use std::sync::Arc;

use arrow_deps::{datafusion::logical_plan::LogicalPlan, util::str_iter_to_batch};
use data_types::TABLE_NAMES_COLUMN_NAME;

use crate::{
    exec::stringset::{StringSet, StringSetRef},
    util::make_scan_plan,
};

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal error converting to arrow: {}", source))]
    InternalConvertingToArrow {
        source: arrow_deps::arrow::error::ArrowError,
    },

    #[snafu(display("Internal error creating a plan for stringset: {}", source))]
    InternalPlanningStringSet {
        source: arrow_deps::datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A plan which produces a logical set of Strings (e.g. tag
/// values). This includes variants with pre-calculated results as
/// well a variant that runs a full on DataFusion plan.
#[derive(Debug)]
pub enum StringSetPlan {
    /// The plan errored and we are delaying reporting the results
    KnownError(Box<dyn std::error::Error + Send + Sync + 'static>),

    /// The results are known from metadata only without having to run
    /// an actual datafusion plan
    KnownOk(StringSetRef),

    /// A DataFusion plan(s) to execute. Each plan must produce
    /// RecordBatches with exactly one String column, though the
    /// values produced by the plan may be repeated
    ///
    /// TODO: it would be cool to have a single datafusion LogicalPlan
    /// that merged all the results together. However, no such Union
    /// node exists at the time of writing, so we do the unioning in IOx
    Plan(Vec<LogicalPlan>),
}

impl From<StringSetRef> for StringSetPlan {
    /// Create a StringSetPlan from a StringSetRef
    fn from(set: StringSetRef) -> Self {
        Self::KnownOk(set)
    }
}

impl From<StringSet> for StringSetPlan {
    /// Create a StringSetPlan from a StringSet result, wrapping the error type
    /// appropriately
    fn from(set: StringSet) -> Self {
        Self::KnownOk(StringSetRef::new(set))
    }
}

impl<E> From<Result<StringSetRef, E>> for StringSetPlan
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// Create a `StringSetPlan` from a `Result<StringSetRef>`, wrapping the
    /// error type appropriately
    fn from(result: Result<StringSetRef, E>) -> Self {
        match result {
            Ok(set) => Self::KnownOk(set),
            Err(e) => Self::KnownError(Box::new(e)),
        }
    }
}

impl From<Vec<LogicalPlan>> for StringSetPlan {
    /// Create a DataFusion LogicalPlan node, each of which must
    /// produce a single output Utf8 column. The output of each plan
    /// will be included into the final set.
    fn from(plans: Vec<LogicalPlan>) -> Self {
        Self::Plan(plans)
    }
}

/// Builder for StringSet plans for appending multiple plans together
///
/// If the values are known beforehand, the builder merges the
/// strings, otherwise it falls back to generic plans
#[derive(Debug, Default)]
pub struct StringSetPlanBuilder {
    /// If there was any error
    error: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,

    /// Known strings
    strings: StringSet,
    /// General plans
    plans: Vec<LogicalPlan>,
}

impl StringSetPlanBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a reference to any strings already known to be in this
    /// set
    pub fn known_strings(&self) -> &StringSet {
        &self.strings
    }

    /// Append the strings from the passed plan into ourselves if possible, or
    /// passes on the plan
    pub fn append(mut self, other: StringSetPlan) -> Self {
        match other {
            StringSetPlan::KnownError(err) => self.error = Some(err),
            StringSetPlan::KnownOk(ssref) => match Arc::try_unwrap(ssref) {
                Ok(mut ss) => {
                    self.strings.append(&mut ss);
                }
                Err(ssref) => {
                    for s in ssref.iter() {
                        self.strings.insert(s.clone());
                    }
                }
            },
            StringSetPlan::Plan(mut other_plans) => self.plans.append(&mut other_plans),
        }

        self
    }

    /// Create a StringSetPlan that produces the deduplicated (union)
    /// of all plans `append`ed to this builder.
    pub fn build(self) -> Result<StringSetPlan> {
        let Self {
            error,
            strings,
            mut plans,
        } = self;

        if let Some(err) = error {
            // is an error, so nothing else matters
            Ok(StringSetPlan::KnownError(err))
        } else if plans.is_empty() {
            // only a known set of strings
            Ok(StringSetPlan::KnownOk(Arc::new(strings)))
        } else {
            // Had at least one general plan, so need to use general
            // purpose plan for the known strings
            if !strings.is_empty() {
                let batch =
                    str_iter_to_batch(TABLE_NAMES_COLUMN_NAME, strings.into_iter().map(Some))
                        .context(InternalConvertingToArrow)?;

                let plan = make_scan_plan(batch).context(InternalPlanningStringSet)?;

                plans.push(plan)
            }

            Ok(StringSetPlan::Plan(plans))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::Executor;

    use super::*;

    #[test]
    fn test_builder_empty() {
        let plan = StringSetPlanBuilder::new().build().unwrap();
        let empty_ss = StringSet::new().into();
        if let StringSetPlan::KnownOk(ss) = plan {
            assert_eq!(ss, empty_ss)
        } else {
            panic!("unexpected type: {:?}", plan)
        }
    }

    #[test]
    fn test_builder_strings_only() {
        let plan = StringSetPlanBuilder::new()
            .append(to_string_set(&["foo", "bar"]).into())
            .append(to_string_set(&["bar", "baz"]).into())
            .build()
            .unwrap();

        let expected_ss = to_string_set(&["foo", "bar", "baz"]).into();

        if let StringSetPlan::KnownOk(ss) = plan {
            assert_eq!(ss, expected_ss)
        } else {
            panic!("unexpected type: {:?}", plan)
        }
    }

    #[derive(Debug)]
    struct TestError {}

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "this is an error")
        }
    }

    impl std::error::Error for TestError {}

    #[test]
    fn test_builder_error() {
        let err = Box::new(TestError {});
        let error = StringSetPlan::KnownError(err);

        // when an error is generated, ensure it gets returned
        let plan = StringSetPlanBuilder::new()
            .append(to_string_set(&["foo", "bar"]).into())
            .append(error)
            .append(to_string_set(&["bar", "baz"]).into())
            .build()
            .unwrap();

        if let StringSetPlan::KnownError(err) = plan {
            assert_eq!(err.to_string(), "this is an error")
        } else {
            panic!("unexpected type: {:?}", plan)
        }
    }

    #[tokio::test]
    async fn test_builder_plan() {
        let batch = str_iter_to_batch("column_name", vec![Some("from_a_plan")]).unwrap();
        let df_plan = make_scan_plan(batch).unwrap();

        // when a df plan is appended the whole plan should be different
        let plan = StringSetPlanBuilder::new()
            .append(to_string_set(&["foo", "bar"]).into())
            .append(vec![df_plan].into())
            .append(to_string_set(&["baz"]).into())
            .build()
            .unwrap();

        let expected_ss = to_string_set(&["foo", "bar", "baz", "from_a_plan"]).into();

        assert!(matches!(plan, StringSetPlan::Plan(_)));
        let executor = Executor::new();
        let ss = executor.to_string_set(plan).await.unwrap();
        assert_eq!(ss, expected_ss);
    }

    fn to_string_set(v: &[&str]) -> StringSet {
        v.iter().map(|s| s.to_string()).collect::<StringSet>()
    }
}
