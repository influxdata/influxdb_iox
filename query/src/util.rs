//! This module contains DataFusion utility functions and helpers

use std::{collections::HashSet, sync::Arc};

use arrow_deps::{
    arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef},
    arrow::record_batch::RecordBatch,
    datafusion::{
        error::DataFusionError,
        logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder},
        optimizer::utils::expr_to_column_names,
    },
};
use internal_types::schema::Schema;

/// Create a logical plan that produces the record batch
pub fn make_scan_plan(batch: RecordBatch) -> std::result::Result<LogicalPlan, DataFusionError> {
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    let projection = None; // scan all columns
    LogicalPlanBuilder::scan_memory(partitions, schema, projection)?.build()
}

/// Given the requested projection (set of requested columns),
/// returns the schema of selecting just those columns
///
/// TODO contribute this back upstream in arrow's Schema so we can
/// avoid the copy of fields
pub fn project_schema(
    arrow_schema: ArrowSchemaRef,
    projection: &Option<Vec<usize>>,
) -> ArrowSchemaRef {
    match projection {
        None => arrow_schema,
        Some(projection) => {
            let new_fields = projection
                .iter()
                .map(|&i| arrow_schema.field(i))
                .cloned()
                .collect();
            Arc::new(ArrowSchema::new(new_fields))
        }
    }
}

/// Returns true if all columns referred to in schema are present, false
/// otherwise
pub fn schema_has_all_expr_columns(schema: &Schema, expr: &Expr) -> bool {
    let mut predicate_columns = HashSet::new();
    expr_to_column_names(expr, &mut predicate_columns).unwrap();

    predicate_columns
        .into_iter()
        .all(|col_name| schema.find_index_of(&col_name).is_some())
}

#[cfg(test)]
mod tests {
    use arrow_deps::datafusion::prelude::*;
    use internal_types::schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_schema_has_all_exprs_() {
        let schema = SchemaBuilder::new().tag("t1").timestamp().build().unwrap();

        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(lit("foo"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t2").eq(lit("foo"))
        ));
        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time2"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time")).and(col("t3").lt(col("time")))
        ));
    }
}
