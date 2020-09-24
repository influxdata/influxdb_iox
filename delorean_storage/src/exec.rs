//! This module handles the manipulation / execution of storage
//! plans. This is currently implemented using DataFusion, and this
//! interface abstracts away many of the details

use std::{collections::BTreeSet, sync::atomic::AtomicU64, sync::atomic::Ordering, sync::Arc};

use datafusion::prelude::ExecutionConfig;
use delorean_arrow::{
    arrow::{
        array::{Array, StringArray, StringArrayOps},
        datatypes::DataType,
        record_batch::RecordBatch,
    },
    datafusion::{
        self,
        logical_plan::{Expr, LogicalPlan},
    },
};

use tracing::debug;

use plans::make_exec_context;
use snafu::{ResultExt, Snafu};
pub mod plans;

#[derive(Debug, Snafu)]
/// Opaque error type
pub enum Error {
    #[snafu(display("Plan Execution Error: {}", source))]
    Execution {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Internal error optimizing plan: {}", source))]
    DataFusionOptimizationError {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error during physical planning: {}", source))]
    DataFusionPhysicalPlanningError {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error executing plan: {}", source))]
    DataFusionExecutionError {
        source: datafusion::error::ExecutionError,
    },

    #[snafu(display("Internal error extracting results from Record Batches: {}", message))]
    ResultsExtractionGeneral { message: String },

    #[snafu(display("Joining execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type StringSet = BTreeSet<String>;
pub type StringSetRef = Arc<StringSet>;

/// Represents a general purpose predicate for evaluation.
///
/// TBD can this predicate represent predicates for multiple tables?
#[derive(Clone, Debug)]
pub struct Predicate {
    /// An expresson using the DataFusion expression operations.
    pub expr: Expr,
}

/// A plan which produces a logical set of Strings (e.g. tag
/// values)
#[derive(Debug)]
pub enum StringSetPlan {
    // If the results are known without having to run an actual datafusion plan
    Known(Result<StringSetRef>),
    // A datafusion plan(s) to execute. Each plan must produce
    // RecordBatches with exactly one String column
    Plan(Vec<LogicalPlan>),
}

impl StringSetPlan {
    /// Create a plan from a known result, wrapping the error type
    /// appropriately
    pub fn from_result<E>(result: Result<StringSetRef, E>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        match result {
            Ok(set) => Self::Known(Ok(set)),
            Err(e) => Self::Known(Err(Error::Execution {
                source: Box::new(e),
            })),
        }
    }

    /// create a plan from a DataFusion LogicalPlan node
    pub fn from_plans(plans: Vec<LogicalPlan>) -> Self {
        Self::Plan(plans)
    }
}

/// Handles executing plans, and marshalling the results into rust
/// native structures.
#[derive(Debug, Default)]
pub struct Executor {
    counters: Arc<ExecutionCounters>,
}

impl Executor {
    /// Executes this plan and returns the resulting set of strings
    pub async fn to_string_set(&self, plan: StringSetPlan) -> Result<StringSetRef> {
        match plan {
            StringSetPlan::Known(res) => res,
            StringSetPlan::Plan(plans) => run_logical_plans(self.counters.clone(), plans)
                .await?
                .into_stringset(),
        }
    }
}

// Various statistics for execution
#[derive(Debug, Default)]
pub struct ExecutionCounters {
    pub plans_run: AtomicU64,
}

impl ExecutionCounters {
    fn inc_plans_run(&self) {
        self.plans_run.fetch_add(1, Ordering::Relaxed);
    }
}

/// plans and runs the plans in parallel and collects the results
/// run each plan in parallel and collect the results
async fn run_logical_plans(
    counters: Arc<ExecutionCounters>,
    plans: Vec<LogicalPlan>,
) -> Result<Vec<RecordBatch>> {
    let value_futures = plans
        .into_iter()
        .map(|plan| {
            let counters = counters.clone();
            // TODO run these on some executor other than the main tokio pool
            tokio::task::spawn(async move { run_logical_plan(counters, plan) })
        })
        .collect::<Vec<_>>();

    // now, wait for all the values to resolve and collect them together
    let mut results = Vec::new();
    for join_handle in value_futures.into_iter() {
        let mut plan_result = join_handle.await.context(JoinError)??;

        results.append(&mut plan_result);
    }
    Ok(results)
}

/// Executes the logical plan using DataFusion and produces RecordBatches
fn run_logical_plan(
    counters: Arc<ExecutionCounters>,
    plan: LogicalPlan,
) -> Result<Vec<RecordBatch>> {
    counters.inc_plans_run();

    const BATCH_SIZE: usize = 1000;

    // TBD: Should we be reusing an execution context across all executions?
    let config = ExecutionConfig::new().with_batch_size(BATCH_SIZE);
    let ctx = make_exec_context(config);

    debug!("Running plan, input:\n{:?}", plan);
    // TODO the otimier was removing filters..
    //let logical_plan = ctx.optimize(&plan).context(DataFusionOptimizationError)?;
    let logical_plan = plan.clone();
    debug!("Running plan, optimized:\n{:?}", logical_plan);

    let physical_plan = ctx
        .create_physical_plan(&logical_plan)
        .context(DataFusionPhysicalPlanningError)?;

    debug!("Running plan, physical:\n{:?}", physical_plan);

    // This executes the query, using its own threads
    // internally. TODO figure out a better way to control
    // concurrency / plan admission
    ctx.collect(physical_plan).context(DataFusionExecutionError)
}

trait IntoStringSet {
    fn into_stringset(self) -> Result<StringSetRef>;
}

/// Converts record batches into StringSets. Assumes that the record
/// batches each have a single string column
impl IntoStringSet for Vec<RecordBatch> {
    fn into_stringset(self) -> Result<StringSetRef> {
        let mut strings = StringSet::new();

        // process the record batches one by one
        for record_batch in self.into_iter() {
            let num_rows = record_batch.num_rows();
            let schema = record_batch.schema();
            let fields = schema.fields();
            if fields.len() != 1 {
                return ResultsExtractionGeneral {
                    message: format!(
                        "Expected exactly 1 field in StringSet schema, found {} field in {:?}",
                        fields.len(),
                        schema
                    ),
                }
                .fail();
            }
            let field = &fields[0];

            if *field.data_type() != DataType::Utf8 {
                return ResultsExtractionGeneral {
                    message: format!(
                        "Expected StringSet schema field to be Utf8, instead it was {:?}",
                        field.data_type()
                    ),
                }
                .fail();
            }

            let array = record_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>();

            match array {
                Some(array) => add_utf8_array_to_stringset(&mut strings, array, num_rows)?,
                None => {
                    return ResultsExtractionGeneral {
                        message: format!("Failed to downcast field {:?} to StringArray", field),
                    }
                    .fail()
                }
            }
        }
        Ok(StringSetRef::new(strings))
    }
}

fn add_utf8_array_to_stringset(
    dest: &mut StringSet,
    src: &StringArray,
    num_rows: usize,
) -> Result<()> {
    // TODO: could make a special case loop if there are no null values
    for i in 0..num_rows {
        if !src.is_null(i) {
            let src_value = src.value(i);
            if !dest.contains(src_value) {
                dest.insert(src_value.into());
            }
        }
    }
    Ok(())
}
