//! This module contains Delorean DataFusion extension execution plan types

use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
    sync::Mutex,
};

use delorean_arrow::{
    arrow::array::StringArray,
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    arrow::record_batch::RecordBatch,
    arrow::record_batch::RecordBatchReader,
    datafusion::execution::context::ExecutionContextState,
    datafusion::execution::context::QueryPlanner,
    datafusion::logical_plan::{self, Expr, LogicalPlan, UserDefinedLogicalNode},
    datafusion::physical_plan::common::RecordBatchIterator,
    datafusion::physical_plan::planner::DefaultPhysicalPlanner,
    datafusion::physical_plan::planner::ExtensionPlanner,
    datafusion::physical_plan::ExecutionPlan,
    datafusion::physical_plan::{Distribution, Partitioning, PhysicalPlanner},
    datafusion::prelude::*,
arrow::array::Array};

// publically export an opaque error type
pub use delorean_arrow::datafusion::error::{ExecutionError, ExecutionError as Error, Result};

/// Implements a SchemaPivot node.
///
/// It takes an arbitrary input like
///  ColA | ColB | ColC
/// ------+------+------
///   1   | NULL | NULL
///   2   | 2    | NULL
///   3   | 2    | NULL
///
/// And pivots it to a table with a single string column:
///
///   non_null_column
///  -----------------
///   "ColA"
///   "ColB"
///
/// This operation can be used to implement the tag_keys metadata query
pub fn make_schema_pivot(input: LogicalPlan) -> LogicalPlan {
    let node = Arc::new(SchemaPivotNode::new(input));

    LogicalPlan::Extension { node }
}

/// Implementes the SchemaPivot operation described in make_schema_pivot,
struct SchemaPivotNode {
    input: LogicalPlan,
    schema: SchemaRef,
    // these expressions represent what columns are "used" by this
    // node (in this case all of them) -- columns that are not used
    // are optimzied away by datafusion.
    exprs: Vec<Expr>,
}

impl SchemaPivotNode {
    pub fn new(input: LogicalPlan) -> Self {
        // output schema is a single string field
        let schema = Schema::new(vec![Field::new("non_null_column", DataType::Utf8, false)]);
        let schema = SchemaRef::new(schema);

        // Form exprs that refer to all of our input columns (so that
        // datafusion doesn't opimize them away)
        let exprs = input
            .schema()
            .fields()
            .iter()
            .map(|field| logical_plan::col(field.name()))
            .collect::<Vec<_>>();

        Self {
            input,
            schema,
            exprs,
        }
    }
}

impl Debug for SchemaPivotNode {
    /// Use explain format for the Debug format.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for SchemaPivotNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for Pivot is a single string
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    /// For example: `SchemaPivot`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaPivot")
    }

    fn from_template(
        &self,
        exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "input size inconistent");
        assert_eq!(exprs.len(), self.exprs.len(), "expression size inconistent");
        Arc::new(SchemaPivotNode::new(inputs[0].clone()))
    }
}

// ------ The implementation of SchemaPivot code follows -----

/// Physical operator that implements TopK for u64 data types. This
/// code is not general and is meant as an illustration only
struct SchemaPivotExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl Debug for SchemaPivotExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaPivotExec")
    }
}

impl ExecutionPlan for SchemaPivotExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(SchemaPivotExec {
                input: children[0].clone(),
                schema: self.schema.clone(),
            })),
            _ => Err(ExecutionError::General(
                "SchemaPivotExec wrong number of children".to_string(),
            )),
        }
    }

    /// Execute one partition and return an iterator over RecordBatch
    fn execute(&self, partition: usize) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "SchemaPivotExec invalid partition {}",
                partition
            )));
        }

        println!("AAL Schema pivot executing, schema is {:?}", self.schema());

        let input_reader = self.input.execute(partition)?;

        // the algorithm here is simple and generic - for each column we
        // haven't seen a value yet for, check each input row.  in
        // most cases, we'll have values for most columns and so
        // terminating early is a good idea.
        let input_schema = self.input.schema();
        println!("AAL input schema is: {:#?}", input_schema);
        let input_fields = input_schema.fields();
        let num_fields = input_fields.len();
        let mut field_indexes_with_seen_values = vec![false; num_fields];

        // use a loop so that we release the mutex once we have read each input_batch
        let mut keep_searching = true;
        while keep_searching {
            let input_batch = input_reader
                .lock()
                .expect("locked input mutex")
                .next_batch()?;

            keep_searching = match input_batch {
                Some(input_batch) => {
                    let num_rows = input_batch.num_rows();
                    println!("AAL: Got an input batch of {} rows", num_rows);
                    println!("     field_indexes_with_seen_values: {:?}",
                        field_indexes_with_seen_values
                    );

                    // track if we had to look at any column values,
                    // and if we have nothing else to do, stop
                    // iterating (no need to evaluate any more input)
                    let mut found_values = false;
                    for i in 0..field_indexes_with_seen_values.len() {
                        // only check fields we haven't seen values for
                        if !field_indexes_with_seen_values[i] {
                            let column = input_batch.column(i);
                            println!("AAL: checking column[{}] '{}': null_count {}, num_rows {}: {:?}",
                                     i , input_fields[i].name(), column.null_count(), num_rows, column);

                            let has_values = !column.is_empty() &&
                                column.null_count() < num_rows &&
                                check_for_string_nulls(column, num_rows);



                            println!("AAL has values: {}", has_values);
                            if has_values {
                                field_indexes_with_seen_values[i] = true;
                            } else {
                                found_values = false;
                            }
                        }
                    }
                    // only keep going if we found any values
                    found_values
                }
                // no more input
                None => false,
            };
        }

        println!("AAL DONE: field_indexes_with_seen_values: {:?}",
                 field_indexes_with_seen_values
        );


        // now, output a string for each column in the input schema
        // that we saw values for
        let mut column_name_builder = StringArray::builder(num_fields);
        field_indexes_with_seen_values
            .iter()
            .enumerate()
            .filter_map(|(field_index, has_values)| {
                if *has_values {
                    Some(input_fields[field_index].name())
                } else {
                    None
                }
            })
            .map(|field_name| {
                column_name_builder
                    .append_value(field_name)
                    .map_err(|e| Error::ArrowError(e))
            })
            .collect::<Result<_>>()?;

        let batch = RecordBatch::try_new(
            self.schema().clone(),
            vec![Arc::new(column_name_builder.finish())],
        )?;

        let batches = vec![Arc::new(batch)];
        Ok(Arc::new(Mutex::new(RecordBatchIterator::new(
            self.schema(),
            batches,
        ))))
    }
}

// this is a hack -- check for empty strings and treat them as null
// (the write buffer produces them as empty strings right now).
//
// Return true if there are any non-empty string values
fn check_for_string_nulls(column: &Arc<dyn Array>, num_rows:usize) -> bool {
    use delorean_arrow::arrow::array::StringArrayOps;
    let string_array = column.as_any().downcast_ref::<StringArray>();

    if let Some(string_array) = string_array {
        for i in 0..num_rows {
            if string_array.value(i) != "" {
                return true
            }
        }
    }

    false
}


// ---- Planning plumbing

struct DeloreanQueryPlanner {}

impl QueryPlanner for DeloreanQueryPlanner {
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // TODO ??
        Ok(plan)
    }

    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("Creating physical plan for: {:#?}", logical_plan);
        // Teach the default physical planner how to plan SchemaPivot nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planner(Arc::new(DeloreanExtensionPlanner {}));
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Physical planner for Delorean extension plans
struct DeloreanExtensionPlanner {}

impl ExtensionPlanner for DeloreanExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        _ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(schema_pivot) = node.as_any().downcast_ref::<SchemaPivotNode>() {
            assert_eq!(inputs.len(), 1, "Inconsistent number of inputs");
            Ok(Arc::new(SchemaPivotExec {
                input: inputs[0].clone(),
                schema: schema_pivot.schema.clone(),
            }))
        } else {
            Err(ExecutionError::General(format!(
                "Unknown extension node type {:?}",
                node
            )))
        }
    }
}

/// Create an ExecutionContext suitable for executing DataFusion plans
pub fn make_exec_context(config: ExecutionConfig) -> ExecutionContext {
    let config = config.with_query_planner(Arc::new(DeloreanQueryPlanner {}));

    ExecutionContext::with_config(config)
}
