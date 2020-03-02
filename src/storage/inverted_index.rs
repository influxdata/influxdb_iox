use crate::delorean::{Predicate, Tag};
use crate::line_parser::PointType;
use crate::storage::{SeriesDataType, StorageError};

use std::collections::BTreeMap;

pub trait InvertedIndex: Sync + Send {
    fn get_or_create_series_ids_for_points(
        &self,
        bucket_id: u32,
        points: &mut [PointType],
    ) -> Result<(), StorageError>;

    fn read_series_matching(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = SeriesFilter> + Send>, StorageError>;

    fn get_tag_keys(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError>;

    fn get_tag_values(
        &self,
        bucket_id: u32,
        tag_key: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct SeriesFilter {
    pub id: u64,
    pub key: String,
    pub value_predicate: Option<Predicate>,
    pub series_type: SeriesDataType,
}

impl SeriesFilter {
    // TODO: Handle escaping of ',', '=', and '\t'
    // TODO: Better error handling
    /// Returns the `Tag` keys and values for this `SeriesFilter`, sorted by key.
    pub fn tags(&self) -> Vec<Tag> {
        self.tag_string_slices()
            .iter()
            .map(|(key, value)| Tag::new(key, value))
            .collect()
    }

    fn tag_string_slices(&self) -> BTreeMap<&str, &str> {
        let mut tags = BTreeMap::new();
        let mut split_on_tab = self.key.splitn(2, '\t');

        let before_tab = split_on_tab
            .next()
            .expect("SeriesFilter key did not contain a tab");

        let mut before_tab_split_on_comma = before_tab.split(',');

        let m = before_tab_split_on_comma
            .next()
            .expect("SeriesFilter key did not contain a comma");
        tags.insert("_measurement", m);

        for kv in before_tab_split_on_comma {
            let mut parts = kv.splitn(2, '=');
            let key = parts
                .next()
                .expect("SeriesFilter key did not contain expected parts");
            let value = parts
                .next()
                .expect("SeriesFilter key did not contain expected parts");
            tags.insert(key, value);
        }

        let f = split_on_tab
            .next()
            .expect("SeriesFilter key did not contain a tab");
        tags.insert("_field", f);

        tags
    }

    /// Returns the `Tag` value associated with the provided key.
    pub fn tag_with_key(&self, key: &str) -> Option<&str> {
        self.tag_string_slices().get(key).map(|val| *val)
    }
}

impl Tag {
    fn new(key: &str, value: &str) -> Tag {
        Tag {
            key: key.bytes().collect(),
            value: value.bytes().collect(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::delorean::Tag;
    use crate::line_parser::PointType;
    use crate::storage::inverted_index::{InvertedIndex, SeriesFilter};
    use crate::storage::predicate::parse_predicate;
    use crate::storage::SeriesDataType;

    use std::str;

    // Test helpers for other implementations to run

    pub fn series_id_indexing(index: Box<dyn InvertedIndex>) {
        let bucket_id = 1;
        let bucket_2 = 2;
        let p1 = PointType::new_i64("one".to_string(), 1, 0);
        let p2 = PointType::new_i64("two".to_string(), 23, 40);
        let p3 = PointType::new_i64("three".to_string(), 33, 86);

        let mut points = vec![p1.clone(), p2];
        index
            .get_or_create_series_ids_for_points(bucket_id, &mut points)
            .unwrap();
        assert_eq!(points[0].series_id(), Some(1));
        assert_eq!(points[1].series_id(), Some(2));

        // now put series in a different bucket, but make sure the IDs start from the beginning
        let mut points = vec![p1.clone()];
        index
            .get_or_create_series_ids_for_points(bucket_2, &mut points)
            .unwrap();
        assert_eq!(points[0].series_id(), Some(1));

        // now insert a new series in the first bucket and make sure it shows up
        let mut points = vec![p1, p3];
        index
            .get_or_create_series_ids_for_points(bucket_id, &mut points)
            .unwrap();
        assert_eq!(points[0].series_id(), Some(1));
        assert_eq!(points[1].series_id(), Some(3));
    }

    pub fn series_metadata_indexing(index: Box<dyn InvertedIndex>) {
        let bucket_id = 1;
        let p1 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 0);
        let p2 = PointType::new_i64("cpu,host=a,region=west\tusage_system".to_string(), 1, 0);
        let p3 = PointType::new_i64("cpu,host=a,region=west\tusage_user".to_string(), 1, 0);
        let p4 = PointType::new_i64("mem,host=b,region=west\tfree".to_string(), 1, 0);

        let mut points = vec![p1, p2, p3, p4];
        index
            .get_or_create_series_ids_for_points(bucket_id, &mut points)
            .unwrap();

        let tag_keys: Vec<String> = index.get_tag_keys(bucket_id, None).unwrap().collect();
        assert_eq!(tag_keys, vec!["_f", "_m", "host", "region"]);

        let tag_values: Vec<String> = index
            .get_tag_values(bucket_id, "host", None)
            .unwrap()
            .collect();
        assert_eq!(tag_values, vec!["a", "b"]);

        // get all series

        // get series with measurement = mem
        let pred = parse_predicate(r#"_m = "cpu""#).unwrap();
        let series: Vec<SeriesFilter> = index
            .read_series_matching(bucket_id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![
                SeriesFilter {
                    id: 1,
                    key: "cpu,host=b,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 2,
                    key: "cpu,host=a,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 3,
                    key: "cpu,host=a,region=west\tusage_user".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
            ]
        );

        // get series with host = a
        let pred = parse_predicate(r#"host = "a""#).unwrap();
        let series: Vec<SeriesFilter> = index
            .read_series_matching(bucket_id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![
                SeriesFilter {
                    id: 2,
                    key: "cpu,host=a,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 3,
                    key: "cpu,host=a,region=west\tusage_user".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
            ]
        );

        // get series with measurement = cpu and host = b
        let pred = parse_predicate(r#"_m = "cpu" and host = "b""#).unwrap();
        let series: Vec<SeriesFilter> = index
            .read_series_matching(bucket_id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![SeriesFilter {
                id: 1,
                key: "cpu,host=b,region=west\tusage_system".to_string(),
                value_predicate: None,
                series_type: SeriesDataType::I64
            },]
        );

        let pred = parse_predicate(r#"host = "a" OR _m = "mem""#).unwrap();
        let series: Vec<SeriesFilter> = index
            .read_series_matching(bucket_id, Some(&pred))
            .unwrap()
            .collect();
        assert_eq!(
            series,
            vec![
                SeriesFilter {
                    id: 2,
                    key: "cpu,host=a,region=west\tusage_system".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 3,
                    key: "cpu,host=a,region=west\tusage_user".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
                SeriesFilter {
                    id: 4,
                    key: "mem,host=b,region=west\tfree".to_string(),
                    value_predicate: None,
                    series_type: SeriesDataType::I64
                },
            ]
        );
    }

    pub fn tags_as_strings(tags: &[Tag]) -> Vec<(&str, &str)> {
        tags.iter()
            .map(|t| {
                (
                    str::from_utf8(&t.key).unwrap(),
                    str::from_utf8(&t.value).unwrap(),
                )
            })
            .collect()
    }

    // Unit tests for SeriesFilter

    #[test]
    fn series_filter_tag_parsing() {
        let sf = SeriesFilter {
            id: 1,
            key: "cpu,host=b,region=west\tusage_system".to_string(),
            value_predicate: None,
            series_type: SeriesDataType::I64,
        };

        assert_eq!(
            tags_as_strings(&sf.tags()),
            vec![
                ("_field", "usage_system"),
                ("_measurement", "cpu"),
                ("host", "b"),
                ("region", "west"),
            ]
        );
    }

    #[test]
    fn series_filter_tag_lookup() {
        let sf = SeriesFilter {
            id: 1,
            key: "cpu,host=b,region=west\tusage_system".to_string(),
            value_predicate: None,
            series_type: SeriesDataType::I64,
        };

        assert_eq!(sf.tag_with_key("_measurement"), Some("cpu"));
        assert_eq!(sf.tag_with_key("host"), Some("b"));
        assert_eq!(sf.tag_with_key("region"), Some("west"));
        assert_eq!(sf.tag_with_key("_field"), Some("usage_system"));
        assert_eq!(sf.tag_with_key("not_present"), None);
    }
}
