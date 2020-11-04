use std::collections::BTreeSet;

use delorean_arrow::datafusion::logical_plan::Expr;

use crate::util::AndExprBuilder;

/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and delorean in particular, they handled specially
#[derive(Clone, PartialEq, Copy, Debug)]
pub struct TimestampRange {
    /// Start defines the inclusive lower bound.
    pub start: i64,
    /// End defines the exclusive upper bound.
    pub end: i64,
}

impl TimestampRange {
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains(&self, v: i64) -> bool {
        self.start <= v && v < self.end
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains_opt(&self, v: Option<i64>) -> bool {
        Some(true) == v.map(|ts| self.contains(ts))
    }
}

/// Represents a general purpose predicate for evaluation in the
/// Delorean storage system. Certain parts of the predicate are
/// special cased
#[derive(Clone, Debug, Default)]
pub struct Predicate {
    /// Optional filter. If present, restrict the request to just
    /// those tables whose names are in table_names
    pub table_names: Option<BTreeSet<String>>,

    // Optional field column selection. If present, further restrict any
    // field columns returned to only those named
    pub field_columns: Option<BTreeSet<String>>,

    /// General DataFusion expressions (arbitrary predicates) applied
    /// as a filter using logical conjuction (aka are 'AND'ed
    /// together). Only rows that evaluate to TRUE for all these
    /// expressions should be returned.
    pub exprs: Vec<Expr>,

    /// Timestamp range: only rows within this range should be considered
    pub range: Option<TimestampRange>,
}

impl Predicate {
    /// Return true if this predicate has any general purpose predicates
    pub fn has_exprs(&self) -> bool {
        !self.exprs.is_empty()
    }

    /// TEMP: return a single Expr that represents all the
    /// general purpose predicates AND'd together.
    pub fn combined_expr(&self) -> Option<Expr> {
        self.exprs
            .iter()
            .fold(AndExprBuilder::default(), |builder, expr| {
                builder.append_expr(expr.clone())
            })
            .build()
    }

    /// adds an additional filter such that all rows must be in one of the table names specified
    pub fn add_filter_table_names(&mut self, table_name_filter: Vec<String>) {
        self.table_names = match self.table_names.take() {
            None => Some(table_name_filter.into_iter().collect::<BTreeSet<_>>()),
            Some(table_names) => {
                // already had table names, so only keep tables that match all predicates
                let intersection = table_name_filter
                    .into_iter()
                    .filter(|t| table_names.contains(t))
                    .collect::<BTreeSet<_>>();

                Some(intersection)
            }
        };
    }
}

#[derive(Debug, Default)]
pub struct PredicateBuilder {
    inner: Predicate,
}

impl PredicateBuilder {
    pub fn from_predicate(predicate: Predicate) -> Self {
        Self { inner: predicate }
    }

    /// Sets the timestamp range
    pub fn timestamp_range(mut self, start: i64, end: i64) -> Self {
        self.inner.range = Some(TimestampRange { start, end });
        self
    }

    /// sets the optional timestamp range, if any
    pub fn timestamp_range_option(mut self, range: Option<TimestampRange>) -> Self {
        self.inner.range = range;
        self
    }

    /// Adds an expression to the list of general purpose predicates
    pub fn add_expr(mut self, expr: Expr) -> Self {
        self.inner.exprs.push(expr);
        self
    }

    /// Adds an optional table name restriction to the existing list
    pub fn table_option(self, table: Option<String>) -> Self {
        if let Some(table) = table {
            self.tables(vec![table])
        } else {
            self
        }
    }

    /// Set the table restriction to [table]
    pub fn table(self, table: impl Into<String>) -> Self {
        self.tables(vec![table.into()])
    }

    /// Sets table name restrictions
    pub fn tables(mut self, tables: Vec<String>) -> Self {
        // We need to distinguish predicates like `table_name In
        // (foo, bar)` and `table_name = foo and table_name = bar` in order to handle this
        assert!(
            self.inner.table_names.is_none(),
            "Multiple table predicate specification not yet supported"
        );

        let table_names = tables.into_iter().collect::<BTreeSet<_>>();
        self.inner.table_names = Some(table_names);
        self
    }

    /// Sets field_column restriction
    pub fn field_columns(mut self, columns: Vec<String>) -> Self {
        // We need to distinguish predicates like `column_name In
        // (foo, bar)` and `column_name = foo and column_name = bar` in order to handle this
        assert!(
            self.inner.field_columns.is_none(),
            "Multiple table predicate specification not yet supported"
        );

        let column_names = columns.into_iter().collect::<BTreeSet<_>>();
        self.inner.field_columns = Some(column_names);
        self
    }

    /// Create a predicate, consuming this builder
    pub fn build(self) -> Predicate {
        self.inner
    }
}
