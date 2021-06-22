//! planning for physical reorganization operations (e.g. MERGE)

use std::sync::Arc;

use datafusion::logical_plan::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_util::AsExpr;
use internal_types::schema::sort::SortKey;
use observability_deps::tracing::debug;

use crate::{provider::ProviderBuilder, QueryChunk};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunk schema not compatible for merge plan: {}", source))]
    ChunkSchemaNotCompatible {
        source: internal_types::schema::merge::Error,
    },

    #[snafu(display("Reorg planner got error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Reorg planner got error adding chunk for table {}: {}",
        table_name,
        source
    ))]
    CreatingProvider {
        table_name: String,
        source: crate::provider::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Planner for physically rearranging chunk data
#[derive(Debug, Default)]
pub struct ReorgPlanner {}

impl ReorgPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an execution plan for the MERGE operations which does the following:
    ///
    /// 1. merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested key
    ///
    /// The plan looks like:
    ///
    /// (Sort on output_sort)
    ///   (Scan chunks) <-- any needed deduplication happens here
    pub fn merge_plan<C>(
        &self,
        chunks: Vec<Arc<C>>,
        output_sort: SortKey<'_>,
    ) -> Result<LogicalPlan>
    where
        C: QueryChunk + 'static,
    {
        assert!(!chunks.is_empty(), "No chunks provided to merge plan");
        let table_name = chunks[0].table_name().to_string();
        let table_name = &table_name;

        debug!(%table_name, "planning merge_plan");

        // Prepare the plan for the table
        let mut builder = ProviderBuilder::new(table_name);

        // There are no predicates in these plans, so no need to prune them
        builder.add_no_op_pruner();

        for chunk in chunks {
            // check that it is consistent with this table_name
            assert_eq!(
                chunk.table_name(),
                table_name,
                "Chunk {} expected table mismatch",
                chunk.id(),
            );

            builder
                .add_chunk(chunk)
                .context(CreatingProvider { table_name })?;
        }

        let provider = builder.build().context(CreatingProvider { table_name })?;
        let provider = Arc::new(provider);

        // Scan all columns
        let projection = None;

        // figure out the sort expression
        let sort_exprs = output_sort
            .iter()
            .map(|(column_name, sort_options)| Expr::Sort {
                expr: Box::new(column_name.as_expr()),
                asc: !sort_options.descending,
                nulls_first: sort_options.nulls_first,
            });

        LogicalPlanBuilder::scan(table_name, provider, projection)
            .context(BuildingPlan)?
            // Add the appropriate sort
            .sort(sort_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)
    }

    /// Creates an execution plan for the SPLIT operations which does the following:
    ///
    /// 1. merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested key
    /// 4. Splits the stream on a specified timestamp:
    ///    `time` columns before that timestamp and after
    ///
    /// The plan looks like:
    ///
    /// (Split on Time)
    ///   (Sort on output_sort)
    ///     (Scan chunks) <-- any needed deduplication happens here
    ///
    /// The output execution plan has two "output streams" (DataFusion partition):
    /// 1. Rows that have `time` *before* the split_time
    /// 2. Rows that have `time` *after* the split_time
    ///
    /// For example:
    ///
    ///
    pub fn split_plan<C>(
        &self,
        chunks: Vec<Arc<C>>,
        output_sort: SortKey<'_>,
        _split_time: i64,
    ) -> Result<LogicalPlan>
    where
        C: QueryChunk + 'static,
    {
        let _base_plan = self.merge_plan(chunks, output_sort)?;
        todo!("Add in the split node and return");
    }
}
