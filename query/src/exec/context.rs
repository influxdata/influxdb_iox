//! This module contains plumbing to connect InfluxDB IOx extensions to
//! DataFusion

use std::{fmt, sync::Arc};

use arrow_deps::{
    arrow::record_batch::RecordBatch,
    datafusion::{
        execution::context::{ExecutionContextState, QueryPlanner},
        logical_plan::{LogicalPlan, UserDefinedLogicalNode},
        physical_plan::{
            collect,
            merge::MergeExec,
            planner::{DefaultPhysicalPlanner, ExtensionPlanner},
            ExecutionPlan, PhysicalPlanner, SendableRecordBatchStream,
        },
        prelude::*,
    },
};

use crate::exec::schema_pivot::{SchemaPivotExec, SchemaPivotNode};

use observability_deps::tracing::debug;

// Reuse DataFusion error and Result types for this module
pub use arrow_deps::datafusion::error::{DataFusionError as Error, Result};

use super::counters::ExecutionCounters;

// The default catalog name - this impacts what SQL queries use if not specified
pub const DEFAULT_CATALOG: &str = "public";
// The default schema name - this impacts what SQL queries use if not specified
pub const DEFAULT_SCHEMA: &str = "iox";

/// This structure implements the DataFusion notion of "query planner"
/// and is needed to create plans with the IOx extension nodes.
struct IOxQueryPlanner {}

impl QueryPlanner for IOxQueryPlanner {
    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan SchemaPivot nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(IOxExtensionPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Physical planner for InfluxDB IOx extension plans
struct IOxExtensionPlanner {}

impl ExtensionPlanner for IOxExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        node.as_any()
            .downcast_ref::<SchemaPivotNode>()
            .map(|schema_pivot| {
                assert_eq!(inputs.len(), 1, "Inconsistent number of inputs");
                let execution_plan = Arc::new(SchemaPivotExec::new(
                    Arc::clone(&inputs[0]),
                    schema_pivot.schema().as_ref().clone().into(),
                ));
                Ok(execution_plan as _)
            })
            .transpose()
    }
}

/// This is an execution context for planning in IOx.
/// It wraps a DataFusion execution context and incudes
/// statistical counters.
///
/// Eventually we envision this as also managing resources
/// and providing visibility into what plans are running
pub struct IOxExecutionContext {
    counters: Arc<ExecutionCounters>,
    inner: ExecutionContext,
}

impl fmt::Debug for IOxExecutionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOxExecutionContext")
            .field("counters", &self.counters)
            .field("inner", &"<DataFusion ExecutionContext>")
            .finish()
    }
}

impl IOxExecutionContext {
    /// Create an ExecutionContext suitable for executing DataFusion plans
    ///
    /// The config is created with a default catalog and schema, but this
    /// can be overridden at a later date
    pub fn new(counters: Arc<ExecutionCounters>) -> Self {
        const BATCH_SIZE: usize = 1000;

        // TBD: Should we be reusing an execution context across all executions?
        let config = ExecutionConfig::new()
            .with_batch_size(BATCH_SIZE)
            .create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA)
            .with_query_planner(Arc::new(IOxQueryPlanner {}));

        let inner = ExecutionContext::with_config(config);

        Self { counters, inner }
    }

    /// returns a reference to the inner datafusion execution context
    pub fn inner(&self) -> &ExecutionContext {
        &self.inner
    }

    /// returns a mutable reference to the inner datafusion execution context
    pub fn inner_mut(&mut self) -> &mut ExecutionContext {
        &mut self.inner
    }

    /// Prepare a SQL statement for execution. This assumes that any
    /// tables referenced in the SQL have been registered with this context
    pub async fn prepare_sql(&mut self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.inner.sql(sql)?.to_logical_plan();
        self.prepare_plan(&logical_plan).await
    }

    /// Prepare (optimize + plan) a pre-created logical plan for execution
    pub async fn prepare_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(
            "Creating plan: Initial plan\n----\n{}\n{}\n----",
            plan.display_indent_schema(),
            plan.display_graphviz(),
        );

        let plan = self.inner.optimize(&plan)?;

        debug!(
            "Creating plan: Optimized plan\n----\n{}\n{}\n----",
            plan.display_indent_schema(),
            plan.display_graphviz(),
        );

        self.inner.create_physical_plan(&plan)
    }

    /// Executes the logical plan using DataFusion on a separate
    /// thread pool and produces RecordBatches
    pub async fn collect(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        self.counters.inc_plans_run();

        // DataFusion plans are "CPU" bound and thus can consume tokio
        // executors threads for extended periods of time. Thus use a
        // dedicated tokio runtime to run them.



        // According to stack overflow, to get a new runtime, we need to spawn our own thread
        // https://stackoverflow.com/questions/62536566/how-can-i-create-a-tokio-runtime-inside-another-tokio-runtime-without-getting-th
        // If you try to do thus from a async context you see:
        // thread 'plan::stringset::tests::test_builder_plan' panicked at 'Cannot drop a runtime in a context where blocking is not allowed. This happens when a runtime is dropped from within an asynchronous context.', /Users/alamb/.cargo/registry/src/github.com-1ecc6299db9ec823/tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21

        use std::thread;

        // This is definitely not cool long term, but short term just
        // block this tokio executor while the plan is running
        println!("Starting to run plan on separate thread...");

        thread::spawn(|| {
            // build runtime
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name("datafusion-executor")
                .build()
                .expect("Creating tokio runtime");

            // By entering the context, we all `tokio::spawn` to this executor.
            let _guard = runtime.enter();
            debug!("Running plan, physical:\n{:?}", physical_plan);

            println!("Starting collect invocation on executor thread");
            let res = runtime.block_on(collect(physical_plan));
            println!("collect invocation complete on executor thread");
            res
        }).join().expect("executor Thread panicked")

    }

    /// Executes the physical plan and produces a RecordBatchStream to stream
    /// over the result that iterates over the results.
    pub async fn execute(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        if physical_plan.output_partitioning().partition_count() <= 1 {
            physical_plan.execute(0).await
        } else {
            // merge into a single partition
            let plan = MergeExec::new(physical_plan);
            // MergeExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0).await
        }
    }
}
