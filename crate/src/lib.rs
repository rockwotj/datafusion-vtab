//! Apache DataFusion for WebAssembly with JavaScript virtual tables.
//!
//! The TypeScript package in `src/` wraps these bindings. Data crosses the
//! boundary as Arrow IPC streams, and filters and scan requests as JSON.

mod filter;
mod heap;
mod provider;
mod task;

use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::format::ExplainFormat;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, execute_stream};
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
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(0),
            )
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
        filter_pushdown: bool,
        callbacks: TableCallbacks,
    ) -> Result<(), JsValue> {
        let reader = StreamReader::try_new(Cursor::new(schema_ipc), None)
            .map_err(|e| JsValue::from(js_sys::Error::new(&format!("invalid schema: {e}"))))?;
        let schema = reader.schema();
        let table = Arc::new(JsTable::new(
            name.to_string(),
            schema,
            filter_pushdown,
            callbacks,
        ));
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
        Ok(self
            .session
            .deregister_table(name)
            .map_err(to_js)?
            .is_some())
    }

    /// Bytes currently reserved in the DataFusion memory pool.
    #[wasm_bindgen(js_name = memoryReserved)]
    pub fn memory_reserved(&self) -> f64 {
        self.pool.reserved() as f64
    }

    /// Runs a query. The task resolves to `{ ipc: Uint8Array, truncated: boolean }`.
    pub fn query(&self, sql: String, max_rows: Option<f64>) -> Task {
        let session = self.session.clone();
        let tables: Vec<Arc<JsTable>> = self.tables.borrow().values().cloned().collect();
        let max_rows = max_rows.map(|n| n as usize);
        Task::new(async move {
            let (ipc, truncated) = run_query(&session, &tables, &sql, max_rows)
                .await
                .map_err(to_js)?;
            let result = js_sys::Object::new();
            js_sys::Reflect::set(
                &result,
                &"ipc".into(),
                &js_sys::Uint8Array::from(ipc.as_slice()),
            )?;
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
) -> Result<(Vec<u8>, bool)> {
    // Queries cannot change the session: no CREATE, INSERT or SET. SET could
    // otherwise raise the session's memory limit.
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let mut df = session.sql_with_options(sql, options).await?;
    df = json_unions_to_text(df)?;
    if let Some(n) = max_rows {
        // DataFusion requires EXPLAIN, EXPLAIN ANALYZE and DESCRIBE to be the
        // root of the plan, so they cannot be wrapped in a limit. Their output
        // is still truncated to max_rows by the loop below, which also sets
        // `truncated`.
        let root_only = matches!(
            df.logical_plan(),
            LogicalPlan::Explain(_) | LogicalPlan::Analyze(_) | LogicalPlan::DescribeTable(_)
        );
        if !root_only {
            // One extra row tells us whether the result was truncated.
            df = df.limit(0, Some(n + 1))?;
        }
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
    let mut stream = match (physical.as_ref() as &dyn Any).downcast_ref::<AnalyzeExec>() {
        Some(analyze) => {
            let batch = run_analyze(analyze, &plan, session.task_ctx()).await?;
            Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter([Ok(batch)]),
            ))
        }
        None => execute_stream(physical, session.task_ctx())?,
    };

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

/// Runs `EXPLAIN ANALYZE` on the current thread. `AnalyzeExec::execute`
/// spawns a Tokio task per input partition, and WebAssembly has no Tokio
/// runtime, so this drains the input here and builds the same output batch.
async fn run_analyze(
    analyze: &AnalyzeExec,
    plan: &LogicalPlan,
    task_ctx: Arc<TaskContext>,
) -> Result<RecordBatch> {
    let LogicalPlan::Analyze(logical) = plan else {
        return internal_err!("AnalyzeExec planned from {}", plan.display());
    };
    let metric_types = logical
        .analyze_level
        .unwrap_or(task_ctx.session_config().options().explain.analyze_level)
        .included_types();
    let metric_categories = analyze.metric_categories().map(<[_]>::to_vec);
    let input = analyze.input();
    let verbose = analyze.verbose();

    // std::time::Instant panics on wasm32-unknown-unknown.
    let start = js_sys::Date::now();
    let mut total_rows = 0usize;
    for partition in 0..input.output_partitioning().partition_count() {
        let mut stream = input.execute(partition, Arc::clone(&task_ctx))?;
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
    }
    let duration = Duration::from_secs_f64((js_sys::Date::now() - start).max(0.0) / 1000.0);

    let mut plan_types = Vec::new();
    let mut plans = Vec::new();
    match analyze.format() {
        ExplainFormat::Indent => {
            plan_types.push("Plan with Metrics".to_string());
            plans.push(
                DisplayableExecutionPlan::with_metrics(input.as_ref())
                    .set_metric_types(metric_types.clone())
                    .set_metric_categories(metric_categories.clone())
                    .set_show_statistics(analyze.show_statistics())
                    .indent(verbose)
                    .to_string(),
            );
            if verbose {
                plan_types.push("Plan with Full Metrics".to_string());
                plans.push(
                    DisplayableExecutionPlan::with_full_metrics(input.as_ref())
                        .set_metric_types(metric_types)
                        .set_metric_categories(metric_categories)
                        .set_show_statistics(analyze.show_statistics())
                        .indent(verbose)
                        .to_string(),
                );
                plan_types.push("Output Rows".to_string());
                plans.push(total_rows.to_string());
                plan_types.push("Duration".to_string());
                plans.push(format!("{duration:?}"));
            }
        }
        ExplainFormat::PostgresJSON => {
            let mut displayable = if verbose {
                DisplayableExecutionPlan::with_full_metrics(input.as_ref())
            } else {
                DisplayableExecutionPlan::with_metrics(input.as_ref())
            };
            displayable = displayable
                .set_metric_types(metric_types)
                .set_metric_categories(metric_categories);
            if verbose {
                displayable = displayable.set_summary(Some(total_rows), Some(duration));
            }
            plan_types.push("Plan with Metrics".to_string());
            plans.push(displayable.pgjson(verbose).to_string());
        }
        format => return internal_err!("EXPLAIN ANALYZE does not support {format} output format"),
    }
    Ok(RecordBatch::try_new(
        analyze.schema(),
        vec![
            Arc::new(StringArray::from(plan_types)),
            Arc::new(StringArray::from(plans)),
        ],
    )?)
}

/// `->` and `json_get` return a union type. Convert top-level union columns
/// to JSON text so results are readable in JavaScript.
fn json_unions_to_text(
    df: datafusion::dataframe::DataFrame,
) -> Result<datafusion::dataframe::DataFrame> {
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
        DataType::LargeListView(f) | DataType::LargeList(f) => {
            DataType::LargeList(js_compatible_field(f))
        }
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(js_compatible_field(f), *n),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| js_compatible_field(f))
                .collect::<Fields>(),
        ),
        other => other.clone(),
    }
}

fn js_compatible_field(field: &Arc<Field>) -> Arc<Field> {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(js_compatible_type(field.data_type())),
    )
}

fn js_compatible_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(js_compatible_field)
            .collect::<Fields>(),
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
