//! Apache DataFusion for WebAssembly with JavaScript virtual tables.
//!
//! The TypeScript package in `src/` wraps these bindings. Data crosses the
//! boundary as Arrow IPC streams, and filters and scan requests as JSON.

mod filter;
mod heap;
mod provider;
mod task;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use wasm_bindgen::prelude::*;

use crate::provider::{JsTable, TableCallbacks};
use crate::task::Task;

#[global_allocator]
static ALLOCATOR: heap::CappedAlloc = heap::CappedAlloc;

#[wasm_bindgen(start)]
fn start() {
    heap::install_panic_hook();
}

fn to_js(err: DataFusionError) -> JsValue {
    js_sys::Error::new(&err.to_string()).into()
}

#[wasm_bindgen]
pub struct Context {
    session: SessionContext,
    pool: Arc<dyn MemoryPool>,
    tables: Rc<RefCell<BTreeMap<String, Arc<JsTable>>>>,
}

#[wasm_bindgen]
impl Context {
    /// Creates a session. `memory_limit` is the DataFusion memory pool size in
    /// bytes. `batch_size` is the target number of rows per batch.
    #[wasm_bindgen(constructor)]
    pub fn new(memory_limit: f64, batch_size: u32) -> Result<Context, JsValue> {
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_limit as usize),
            NonZeroUsize::new(5).unwrap(),
        ));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            // There is no file system to spill to.
            .with_disk_manager_builder(DiskManagerBuilder::default().with_max_temp_directory_size(0))
            .build_arc()
            .map_err(to_js)?;
        let mut config = SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(1)
            .with_batch_size(batch_size as usize)
            .with_repartition_joins(false)
            .with_repartition_aggregations(false)
            .with_repartition_sorts(false)
            .with_repartition_windows(false);
        config.options_mut().sql_parser.dialect = datafusion::config::Dialect::PostgreSQL;
        let mut session = SessionContext::new_with_config_rt(config, runtime);
        datafusion_functions_json::register_all(&mut session).map_err(to_js)?;
        Ok(Context {
            session,
            pool,
            tables: Rc::new(RefCell::new(BTreeMap::new())),
        })
    }

    /// Registers a virtual table. `schema_ipc` is an Arrow IPC stream that
    /// holds the table's schema and no batches.
    #[wasm_bindgen(js_name = registerTable)]
    pub fn register_table(
        &self,
        name: &str,
        schema_ipc: &[u8],
        pushdown: bool,
        callbacks: TableCallbacks,
    ) -> Result<(), JsValue> {
        let reader = StreamReader::try_new(Cursor::new(schema_ipc), None)
            .map_err(|e| JsValue::from(js_sys::Error::new(&format!("invalid schema: {e}"))))?;
        let schema = reader.schema();
        let table = Arc::new(JsTable::new(name.to_string(), schema, pushdown, callbacks));
        self.session
            .register_table(name, Arc::clone(&table) as _)
            .map_err(to_js)?;
        self.tables.borrow_mut().insert(name.to_string(), table);
        Ok(())
    }

    /// Removes a table. Returns false if no table had that name.
    #[wasm_bindgen(js_name = deregisterTable)]
    pub fn deregister_table(&self, name: &str) -> Result<bool, JsValue> {
        self.tables.borrow_mut().remove(name);
        Ok(self.session.deregister_table(name).map_err(to_js)?.is_some())
    }

    /// Bytes currently reserved in the DataFusion memory pool.
    #[wasm_bindgen(js_name = memoryReserved)]
    pub fn memory_reserved(&self) -> f64 {
        self.pool.reserved() as f64
    }

    /// Runs a query. The task resolves to `{ ipc: Uint8Array, truncated: boolean }`.
    pub fn query(&self, sql: String, max_rows: Option<f64>, readonly: bool) -> Task {
        let session = self.session.clone();
        let tables: Vec<Arc<JsTable>> = self.tables.borrow().values().cloned().collect();
        let max_rows = max_rows.map(|n| n as usize);
        Task::new(async move {
            let (ipc, truncated) = run_query(&session, &tables, &sql, max_rows, readonly)
                .await
                .map_err(to_js)?;
            let result = js_sys::Object::new();
            js_sys::Reflect::set(&result, &"ipc".into(), &js_sys::Uint8Array::from(ipc.as_slice()))?;
            js_sys::Reflect::set(&result, &"truncated".into(), &truncated.into())?;
            Ok(result.into())
        })
    }
}

async fn run_query(
    session: &SessionContext,
    tables: &[Arc<JsTable>],
    sql: &str,
    max_rows: Option<usize>,
    readonly: bool,
) -> Result<(Vec<u8>, bool)> {
    let options = SQLOptions::new()
        .with_allow_ddl(!readonly)
        .with_allow_dml(!readonly)
        .with_allow_statements(!readonly);
    let mut df = session.sql_with_options(sql, options).await?;
    df = json_unions_to_text(df)?;
    if let Some(n) = max_rows {
        // One extra row tells us whether the result was truncated.
        df = df.limit(0, Some(n + 1))?;
    }
    let (state, plan) = df.into_parts();

    // supports_filters_pushdown is synchronous, but JavaScript tables answer
    // asynchronously. Optimize once to collect the filters offered to each
    // table, ask JavaScript about them, then plan for real with the answers.
    for table in tables {
        table.begin_collecting();
    }
    let collected = state.optimize(&plan);
    let pending: Vec<Vec<String>> = tables.iter().map(|t| t.end_collecting()).collect();
    collected?;
    for (table, filters) in tables.iter().zip(pending) {
        table.resolve_filters(filters).await?;
    }

    let physical = state.create_physical_plan(&plan).await?;
    let schema = js_compatible_schema(&physical.schema());
    let mut stream = execute_stream(physical, session.task_ctx())?;

    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    let mut rows = 0usize;
    let mut truncated = false;
    while let Some(batch) = stream.next().await {
        let mut batch = batch?;
        if let Some(max) = max_rows {
            if rows + batch.num_rows() > max {
                batch = batch.slice(0, max - rows);
                truncated = true;
            }
        }
        rows += batch.num_rows();
        if batch.num_rows() > 0 {
            writer.write(&js_compatible_batch(batch, &schema)?)?;
        }
        if truncated {
            break;
        }
    }
    drop(stream);
    writer.finish()?;
    drop(writer);
    Ok((buf, truncated))
}

/// `->` and `json_get` return a union type. Convert top-level union columns
/// to JSON text so results are readable in JavaScript.
fn json_unions_to_text(df: datafusion::dataframe::DataFrame) -> Result<datafusion::dataframe::DataFrame> {
    let schema = df.schema().clone();
    if !schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Union(..)))
    {
        return Ok(df);
    }
    let exprs: Vec<Expr> = schema
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
            if datafusion_functions_json::JSON_UNION_DATA_TYPE.equals_datatype(field.data_type()) {
                datafusion_functions_json::functions::json_union_to_text(column).alias(field.name())
            } else {
                column
            }
        })
        .collect();
    df.select(exprs)
}

/// apache-arrow for JavaScript cannot read view types. Map them to the
/// equivalent offset-based types.
fn js_compatible_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::ListView(f) | DataType::List(f) => DataType::List(js_compatible_field(f)),
        DataType::LargeListView(f) | DataType::LargeList(f) => DataType::LargeList(js_compatible_field(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(js_compatible_field(f), *n),
        DataType::Struct(fields) => DataType::Struct(
            fields.iter().map(|f| js_compatible_field(f)).collect::<Fields>(),
        ),
        other => other.clone(),
    }
}

fn js_compatible_field(field: &Arc<Field>) -> Arc<Field> {
    Arc::new(field.as_ref().clone().with_data_type(js_compatible_type(field.data_type())))
}

fn js_compatible_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema.fields().iter().map(js_compatible_field).collect::<Fields>(),
        schema.metadata().clone(),
    ))
}

fn js_compatible_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<std::result::Result<Vec<ArrayRef>, _>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}
