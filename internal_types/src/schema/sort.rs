use std::str::FromStr;

use indexmap::{map::Iter, IndexMap};
use itertools::Itertools;
use snafu::Snafu;

use crate::schema::TIME_COLUMN_NAME;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid column sort: {}", value))]
    InvalidColumnSort { value: String },

    #[snafu(display("invalid sort ordinal: {}", value))]
    InvalidSortOrdinal { value: String },

    #[snafu(display("invalid descending value: {}", value))]
    InvalidDescending { value: String },

    #[snafu(display("invalid nulls first value: {}", value))]
    InvalidNullsFirst { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Temporary - https://github.com/apache/arrow-rs/pull/425
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            nulls_first: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ColumnSort {
    /// Position of this column in the sort key
    pub sort_ordinal: usize,
    pub options: SortOptions,
}

impl FromStr for ColumnSort {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (sort, descending, nulls) =
            s.split('/')
                .collect_tuple()
                .ok_or(Error::InvalidColumnSort {
                    value: s.to_string(),
                })?;

        Ok(Self {
            sort_ordinal: sort.parse().map_err(|_| Error::InvalidSortOrdinal {
                value: sort.to_string(),
            })?,
            options: SortOptions {
                descending: descending.parse().map_err(|_| Error::InvalidDescending {
                    value: descending.to_string(),
                })?,
                nulls_first: nulls.parse().map_err(|_| Error::InvalidNullsFirst {
                    value: nulls.to_string(),
                })?,
            },
        })
    }
}

impl std::fmt::Display for ColumnSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.sort_ordinal, self.options.descending, self.options.nulls_first
        )
    }
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct SortKey<'a> {
    columns: IndexMap<&'a str, SortOptions>,
}

impl<'a> SortKey<'a> {
    /// Create a new empty sort key that can store `capacity` columns without allocating
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: IndexMap::with_capacity(capacity),
        }
    }

    /// Creates a new sort key based on the provided tag key cardinalities
    /// starting from lowest to highest cardinality, followed by the time column
    pub fn new_from_cardinalities(mut cardinalities: Vec<(&str, usize)>) -> SortKey<'_> {
        cardinalities.sort_by_key(|x| x.1);

        let mut sort_key = SortKey::with_capacity(cardinalities.len() + 1);
        for (col_name, _) in cardinalities {
            sort_key.push(col_name, Default::default());
        }

        sort_key.push(TIME_COLUMN_NAME, Default::default());
        sort_key
    }

    /// Merge any new columns in `other` into this sort key
    ///
    /// For each column in `other` not present in `self`, find the first
    /// column that comes after it in `other` that is also present in `self`
    /// and insert it into `self` before that column
    pub fn merge(&mut self, other: &SortKey<'a>) {
        let mut insert_idx = self.columns.len();
        let mut to_insert = Vec::new();

        for (col, options) in other.columns.iter().rev() {
            match self.get(col) {
                Some(sort) => insert_idx = sort.sort_ordinal,
                None => to_insert.push((insert_idx, (*col, *options))),
            }
        }

        if to_insert.is_empty() {
            return;
        }

        // Ensure new columns are in the order they appeared in `other`
        to_insert.reverse();

        // Add the existing columns after those added from `other`
        to_insert.extend(self.columns.drain(..).enumerate());

        // Stable sort based on insert index
        //
        // Where new columns have the same insert index, it will preserve
        // the order in `other` and follow these by the column from `self`
        to_insert.sort_by_key(|x| x.0);

        // Reconstruct the indexmap
        self.columns = to_insert.drain(..).map(|x| x.1).collect();
    }

    /// Adds a new column to the end of this sort key
    pub fn push(&mut self, column: &'a str, options: SortOptions) {
        self.columns.insert(column, options);
    }

    /// Gets the ColumnSort for a given column name
    pub fn get(&self, column: &str) -> Option<ColumnSort> {
        let (sort_ordinal, _, options) = self.columns.get_full(column)?;
        Some(ColumnSort {
            sort_ordinal,
            options: *options,
        })
    }

    /// Returns an iterator over the columns in this key
    pub fn iter(&self) -> Iter<'_, &'a str, SortOptions> {
        self.columns.iter()
    }

    /// Returns the length of the sort key
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns if this sort key is empty
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        assert_eq!(
            ColumnSort::from_str("23/true/false").unwrap(),
            ColumnSort {
                sort_ordinal: 23,
                options: SortOptions {
                    descending: true,
                    nulls_first: false
                }
            }
        );

        assert!(matches!(
            ColumnSort::from_str("///").unwrap_err(),
            Error::InvalidColumnSort { value } if &value == "///"
        ));

        assert!(matches!(
            ColumnSort::from_str("-1/true/false").unwrap_err(),
            Error::InvalidSortOrdinal { value } if &value == "-1"
        ));

        assert!(matches!(
            ColumnSort::from_str("34/sdf d/false").unwrap_err(),
            Error::InvalidDescending {value } if &value == "sdf d"
        ));

        assert!(matches!(
            ColumnSort::from_str("34/true/s=,ds").unwrap_err(),
            Error::InvalidNullsFirst { value } if &value == "s=,ds"
        ));
    }

    #[test]
    fn test_basic() {
        let mut key = SortKey::with_capacity(3);
        key.push("a", Default::default());
        key.push("c", Default::default());
        key.push("b", Default::default());

        assert_eq!(key.len(), 3);
        assert!(!key.is_empty());

        assert_eq!(key.get("foo"), None);
        assert_eq!(
            key.get("a"),
            Some(ColumnSort {
                sort_ordinal: 0,
                options: Default::default()
            })
        );
        assert_eq!(
            key.get("b"),
            Some(ColumnSort {
                sort_ordinal: 2,
                options: Default::default()
            })
        );
        assert_eq!(
            key.get("c"),
            Some(ColumnSort {
                sort_ordinal: 1,
                options: Default::default()
            })
        );
    }

    #[test]
    fn test_merge() {
        let mut k1 = SortKey::with_capacity(20);
        k1.push("a", Default::default());
        k1.push("b", Default::default());
        k1.push("c", Default::default());

        let mut k2 = SortKey::with_capacity(20);
        k2.push("c", Default::default());
        k2.push("apples", Default::default());
        k2.push("a", Default::default());
        k2.push("bananas", Default::default());
        k2.push("cupcakes", Default::default());
        k2.push("b", Default::default());
        k2.push("f", Default::default());

        k1.merge(&k2);

        let k1_columns: Vec<_> = k1.columns.iter().map(|x| *x.0).collect();

        assert_eq!(
            k1_columns,
            vec!["apples", "a", "bananas", "cupcakes", "b", "c", "f"]
        )
    }

    #[test]
    fn test_from_cardinalities() {
        let k1 = SortKey::new_from_cardinalities(vec![
            ("apples", 20),
            ("bananas", 100),
            ("coconut", 2),
            ("damson", 50),
        ]);

        let k1_columns: Vec<_> = k1.columns.iter().map(|x| *x.0).collect();

        assert_eq!(
            k1_columns,
            vec!["coconut", "apples", "damson", "bananas", "time"]
        )
    }
}
