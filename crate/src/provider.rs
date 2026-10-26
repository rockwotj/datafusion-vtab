//! DataFusion table provider backed by JavaScript callbacks.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use serde_json::json;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::filter;

#[wasm_bindgen]
extern "C" {
    /// Callbacks the TypeScript wrapper builds around a user's virtual table.
    pub type TableCallbacks;

    /// Takes a JSON array of filters. Resolves to a JSON array with one of
    /// "exact", "inexact" or "unsupported" per filter.
    #[wasm_bindgen(method, catch, js_name = supportsFilters)]
    fn supports_filters(this: &TableCallbacks, filters_json: &str) -> Result<js_sys::Promise, JsValue>;

    /// Takes a JSON scan request and returns an iterator over IPC batches.
    #[wasm_bindgen(method, catch)]
    fn scan(this: &TableCallbacks, request_json: &str) -> Result<ScanIterator, JsValue>;

    pub type ScanIterator;

    /// Resolves to an Arrow IPC stream (`Uint8Array`), or null at the end.
    #[wasm_bindgen(method, catch)]
    fn next(this: &ScanIterator) -> Result<js_sys::Promise, JsValue>;

    /// Stops the scan early. Runs the generator's `finally` blocks.
    #[wasm_bindgen(method)]
    fn close(this: &ScanIterator);
}

/// Wraps a value that is not `Send`/`Sync`. The engine runs on one thread, and
/// DataFusion's traits require both bounds.
pub struct SingleThreaded<T>(pub T);

unsafe impl<T> Send for SingleThreaded<T> {}
unsafe impl<T> Sync for SingleThreaded<T> {}

/// Error raised by JavaScript code, with the table it came from.
#[derive(Debug)]
pub struct JsCallbackError {
    pub table: String,
    pub action: &'static str,
    pub message: String,
}

impl fmt::Display for JsCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} on table \"{}\" failed: {}", self.action, self.table, self.message)
    }
}

impl std::error::Error for JsCallbackError {}

pub fn js_error_message(value: &JsValue) -> String {
    if let Some(err) = value.dyn_ref::<js_sys::Error>() {
        return String::from(err.message());
    }
    if let Some(s) = value.as_string() {
        return s;
    }
    js_sys::JSON::stringify(value)
        .ok()
        .and_then(|s| s.as_string())
        .unwrap_or_else(|| "unknown JavaScript error".to_string())
}

fn callback_error(table: &str, action: &'static str, value: &JsValue) -> DataFusionError {
    DataFusionError::External(Box::new(JsCallbackError {
        table: table.to_string(),
        action,
        message: js_error_message(value),
    }))
}

#[derive(Default)]
struct FilterAnswers {
    /// While true, `supports_filters_pushdown` records filters instead of
    /// answering from `answers`.
    collecting: bool,
    pending: Vec<String>,
    answers: HashMap<String, TableProviderFilterPushDown>,
}

pub struct JsTable {
    name: String,
    schema: SchemaRef,
    /// False when the JavaScript table has no `supportsFilters`.
    filter_pushdown: bool,
    callbacks: SingleThreaded<TableCallbacks>,
    filters: Mutex<FilterAnswers>,
}

impl JsTable {
    pub fn new(name: String, schema: SchemaRef, filter_pushdown: bool, callbacks: TableCallbacks) -> Self {
        JsTable {
            name,
            schema,
            filter_pushdown,
            callbacks: SingleThreaded(callbacks),
            filters: Mutex::new(FilterAnswers::default()),
        }
    }

    pub fn begin_collecting(&self) {
        let mut f = self.filters.lock().unwrap();
        f.collecting = true;
        f.pending.clear();
    }

    pub fn end_collecting(&self) -> Vec<String> {
        let mut f = self.filters.lock().unwrap();
        f.collecting = false;
        std::mem::take(&mut f.pending)
    }

    /// Asks JavaScript how it supports each filter and stores the answers for
    /// the next planning pass.
    pub async fn resolve_filters(&self, filters: Vec<String>) -> Result<()> {
        if filters.is_empty() {
            return Ok(());
        }
        let filters_json = format!("[{}]", filters.join(","));
        let promise = self
            .callbacks
            .0
            .supports_filters(&filters_json)
            .map_err(|e| callback_error(&self.name, "supportsFilters", &e))?;
        let answer = JsFuture::from(promise)
            .await
            .map_err(|e| callback_error(&self.name, "supportsFilters", &e))?;
        let answer: Vec<String> = answer
            .as_string()
            .and_then(|s| serde_json::from_str(&s).ok())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "supportsFilters on table \"{}\" returned an invalid answer",
                    self.name
                ))
            })?;
        if answer.len() != filters.len() {
            return Err(DataFusionError::Plan(format!(
                "supportsFilters on table \"{}\" returned {} answers for {} filters",
                self.name,
                answer.len(),
                filters.len()
            )));
        }
        let mut f = self.filters.lock().unwrap();
        for (filter, answer) in filters.into_iter().zip(answer) {
            let support = match answer.as_str() {
                "exact" => TableProviderFilterPushDown::Exact,
                "inexact" => TableProviderFilterPushDown::Inexact,
                "unsupported" => TableProviderFilterPushDown::Unsupported,
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "supportsFilters on table \"{}\" returned unknown answer \"{other}\"",
                        self.name
                    )));
                }
            };
            f.answers.insert(filter, support);
        }
        Ok(())
    }
}

impl fmt::Debug for JsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsTable").field("name", &self.name).finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for JsTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        if !self.filter_pushdown {
            return Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()]);
        }
        let mut state = self.filters.lock().unwrap();
        Ok(filters
            .iter()
            .map(|expr| {
                let Some(json) = filter::to_json(expr).map(|v| v.to_string()) else {
                    return TableProviderFilterPushDown::Unsupported;
                };
                if state.collecting {
                    if !state.pending.contains(&json) {
                        state.pending.push(json);
                    }
                    // Inexact keeps the filter in the plan and still offers
                    // it to the scan, so the collecting pass sees every filter
                    // the final pass can.
                    TableProviderFilterPushDown::Inexact
                } else {
                    state
                        .answers
                        .get(&json)
                        .cloned()
                        .unwrap_or(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };
        let columns: Vec<&str> = projected_schema.fields().iter().map(|f| f.name().as_str()).collect();
        // DataFusion only passes filters this table accepted, and only passes a
        // limit when no filter remains to be applied above the scan.
        let filters: Vec<serde_json::Value> = filters.iter().filter_map(filter::to_json).collect();
        let request = json!({
            "projection": columns,
            "filters": filters,
            "limit": limit,
        });
        Ok(Arc::new(JsScanExec {
            table: self.name.clone(),
            callbacks: SingleThreaded(self.callbacks.0.clone().unchecked_into()),
            request: request.to_string(),
            limit,
            schema: Arc::clone(&projected_schema),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

struct JsScanExec {
    table: String,
    callbacks: SingleThreaded<TableCallbacks>,
    request: String,
    limit: Option<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for JsScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsScanExec").field("table", &self.table).finish_non_exhaustive()
    }
}

impl DisplayAs for JsScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JsScanExec: table={}, request={}", self.table, self.request)
    }
}

impl ExecutionPlan for JsScanExec {
    fn name(&self) -> &str {
        "JsScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal("JsScanExec has no children".into()))
        }
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "JsScanExec has one partition, got partition {partition}"
            )));
        }
        Ok(Box::pin(JsScanStream {
            table: self.table.clone(),
            callbacks: SingleThreaded(self.callbacks.0.clone().unchecked_into()),
            request: self.request.clone(),
            remaining: self.limit,
            schema: Arc::clone(&self.schema),
            iterator: None,
            pending: None,
            ready: VecDeque::new(),
            done: false,
        }))
    }
}

struct JsScanStream {
    table: String,
    callbacks: SingleThreaded<TableCallbacks>,
    request: String,
    /// Rows still wanted when a limit was pushed down.
    remaining: Option<usize>,
    schema: SchemaRef,
    iterator: Option<SingleThreaded<ScanIterator>>,
    pending: Option<SingleThreaded<JsFuture>>,
    ready: VecDeque<RecordBatch>,
    done: bool,
}

impl JsScanStream {
    fn fail(&mut self, action: &'static str, err: &JsValue) -> Poll<Option<Result<RecordBatch>>> {
        self.finish();
        Poll::Ready(Some(Err(callback_error(&self.table, action, err))))
    }

    fn finish(&mut self) {
        self.done = true;
        self.pending = None;
        self.ready.clear();
        if let Some(iterator) = self.iterator.take() {
            iterator.0.close();
        }
    }

    fn decode(&mut self, ipc: &[u8]) -> Result<()> {
        let reader = StreamReader::try_new(Cursor::new(ipc), None)?;
        for batch in reader {
            let batch = conform(batch?, &self.schema, &self.table)?;
            self.ready.push_back(batch);
        }
        Ok(())
    }
}

impl Stream for JsScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(mut batch) = self.ready.pop_front() {
                if let Some(remaining) = self.remaining {
                    if batch.num_rows() >= remaining {
                        batch = batch.slice(0, remaining);
                        self.finish();
                    } else {
                        self.remaining = Some(remaining - batch.num_rows());
                    }
                }
                return Poll::Ready(Some(Ok(batch)));
            }
            if self.done {
                return Poll::Ready(None);
            }
            if self.iterator.is_none() {
                match self.callbacks.0.scan(&self.request) {
                    Ok(iterator) => self.iterator = Some(SingleThreaded(iterator)),
                    Err(e) => return self.fail("scan", &e),
                }
            }
            if self.pending.is_none() {
                let next = self.iterator.as_ref().unwrap().0.next();
                match next {
                    Ok(promise) => self.pending = Some(SingleThreaded(JsFuture::from(promise))),
                    Err(e) => return self.fail("scan", &e),
                }
            }
            let polled = Pin::new(&mut self.pending.as_mut().unwrap().0).poll(cx);
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return self.fail("scan", &e),
                Poll::Ready(Ok(value)) => {
                    self.pending = None;
                    if value.is_null() || value.is_undefined() {
                        // The iterator finished on its own; no need to close it.
                        self.iterator = None;
                        self.done = true;
                        continue;
                    }
                    let bytes = js_sys::Uint8Array::new(&value).to_vec();
                    if let Err(e) = self.decode(&bytes) {
                        self.finish();
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for JsScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for JsScanStream {
    fn drop(&mut self) {
        if let Some(iterator) = self.iterator.take() {
            iterator.0.close();
        }
    }
}

/// Picks the projected columns out of a batch by name and casts them to the
/// declared types. apache-arrow in JavaScript may encode strings as
/// dictionaries, and scans may return more columns than were asked for.
fn conform(batch: RecordBatch, schema: &SchemaRef, table: &str) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let index = batch.schema().index_of(field.name()).map_err(|_| {
                DataFusionError::Execution(format!(
                    "scan on table \"{table}\" returned a batch without column \"{}\"",
                    field.name()
                ))
            })?;
            let column = batch.column(index);
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)?)
}
