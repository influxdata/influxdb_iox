use snafu::{ensure, Snafu};
use std::{collections::BTreeMap, io};

/// Errors that occur while building `DataPoint`s
#[derive(Debug, Snafu)]
pub enum DataPointError {
    /// Returned when calling `build` on a `DataPointBuilder` that has no fields.
    #[snafu(display(
        "All `DataPoints` must have at least one field. Builder contains: {:?}",
        data_point_builder
    ))]
    AtLeastOneFieldRequired {
        /// The current state of the `DataPointBuilder`
        data_point_builder: DataPointBuilder,
    },
}

/// Incrementally constructs a `DataPoint`.
///
/// Create this via `DataPoint::builder`.
#[derive(Debug)]
pub struct DataPointBuilder {
    measurement: String,
    // Keeping the tags sorted improves performance on the server side
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

impl DataPointBuilder {
    fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Default::default(),
            fields: Default::default(),
            timestamp: Default::default(),
        }
    }

    /// Sets a tag, replacing any existing tag of the same name.
    pub fn tag(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(name.into(), value.into());
        self
    }

    /// Sets a field, replacing any existing field of the same name.
    pub fn field(mut self, name: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(name.into(), value.into());
        self
    }

    /// Sets the timestamp, replacing any existing timestamp.
    ///
    /// The value is treated as the number of nanoseconds since the
    /// UNIX epoch.
    pub fn timestamp(mut self, value: i64) -> Self {
        self.timestamp = Some(value);
        self
    }

    /// Constructs the data point
    pub fn build(self) -> Result<DataPoint, DataPointError> {
        ensure!(
            !self.fields.is_empty(),
            AtLeastOneFieldRequired {
                data_point_builder: self
            }
        );

        let Self {
            measurement,
            tags,
            fields,
            timestamp,
        } = self;

        Ok(DataPoint {
            measurement,
            tags,
            fields,
            timestamp,
        })
    }
}

/// A single point of information to send to InfluxDB.
// TODO: If we want to support non-UTF-8 data, all `String`s stored in `DataPoint` would need
// to be `Vec<u8>` instead, the API for creating a `DataPoint` would need some more consideration,
// and there would need to be more `Write*` trait implementations. Because the `Write*` traits work
// on a writer of bytes, that part of the design supports non-UTF-8 data now.
#[derive(Debug)]
pub struct DataPoint {
    measurement: String,
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, FieldValue>,
    timestamp: Option<i64>,
}

impl DataPoint {
    /// Create a builder to incrementally construct a `DataPoint`.
    pub fn builder(measurement: impl Into<String>) -> DataPointBuilder {
        DataPointBuilder::new(measurement)
    }
}

impl WriteDataPoint for DataPoint {
    fn write_data_point_to<W>(&self, mut w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        self.measurement.write_measurement_to(&mut w)?;

        for (k, v) in &self.tags {
            w.write_all(b",")?;
            k.write_tag_key_to(&mut w)?;
            w.write_all(b"=")?;
            v.write_tag_value_to(&mut w)?;
        }

        for (i, (k, v)) in self.fields.iter().enumerate() {
            let d = if i == 0 { b" " } else { b"," };

            w.write_all(d)?;
            k.write_field_key_to(&mut w)?;
            w.write_all(b"=")?;
            v.write_field_value_to(&mut w)?;
        }

        if let Some(ts) = self.timestamp {
            w.write_all(b" ")?;
            ts.write_timestamp_to(&mut w)?;
        }

        w.write_all(b"\n")?;

        Ok(())
    }
}

/// Possible value types
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// A true or false value
    Bool(bool),
    /// A 64-bit floating point number
    F64(f64),
    /// A 64-bit signed integer number
    I64(i64),
    /// A string value
    String(String),
}

impl From<bool> for FieldValue {
    fn from(other: bool) -> Self {
        Self::Bool(other)
    }
}

impl From<f64> for FieldValue {
    fn from(other: f64) -> Self {
        Self::F64(other)
    }
}

impl From<i64> for FieldValue {
    fn from(other: i64) -> Self {
        Self::I64(other)
    }
}

impl From<&str> for FieldValue {
    fn from(other: &str) -> Self {
        Self::String(other.into())
    }
}

impl From<String> for FieldValue {
    fn from(other: String) -> Self {
        Self::String(other)
    }
}

impl WriteFieldValue for FieldValue {
    fn write_field_value_to<W>(&self, w: W) -> io::Result<()>
    where
        W: io::Write,
    {
        use FieldValue::*;

        match self {
            Bool(v) => v.write_field_value_to(w),
            F64(v) => v.write_field_value_to(w),
            I64(v) => v.write_field_value_to(w),
            String(v) => v.write_field_value_to(w),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    fn assert_utf8_strings_eq(left: &[u8], right: &[u8]) -> Result {
        assert_eq!(
            left,
            right,
            "\n\nleft string value:  `{}`,\nright string value: `{}`",
            str::from_utf8(left)?,
            str::from_utf8(right)?
        );
        Ok(())
    }

    #[test]
    fn point_builder_allows_setting_tags_and_fields() -> Result {
        let point = DataPoint::builder("swap")
            .tag("host", "server01")
            .tag("name", "disk0")
            .field("in", 3_i64)
            .field("out", 4_i64)
            .timestamp(1)
            .build()?;

        assert_utf8_strings_eq(
            &point.data_point_to_vec()?,
            b"swap,host=server01,name=disk0 in=3i,out=4i 1\n".as_ref(),
        )?;

        Ok(())
    }

    #[test]
    fn no_tags_or_timestamp() -> Result {
        let point = DataPoint::builder("m0")
            .field("f0", 1.0)
            .field("f1", 2_i64)
            .build()?;

        assert_utf8_strings_eq(&point.data_point_to_vec()?, b"m0 f0=1,f1=2i\n".as_ref())?;

        Ok(())
    }

    #[test]
    fn no_timestamp() -> Result {
        let point = DataPoint::builder("m0")
            .tag("t0", "v0")
            .tag("t1", "v1")
            .field("f1", 2_i64)
            .build()?;

        assert_utf8_strings_eq(
            &point.data_point_to_vec()?,
            b"m0,t0=v0,t1=v1 f1=2i\n".as_ref(),
        )?;

        Ok(())
    }

    #[test]
    fn no_field() {
        let point_result = DataPoint::builder("m0").build();

        assert!(point_result.is_err());
    }

    const ALL_THE_DELIMITERS: &str = r#"alpha,beta=delta gamma"epsilon"#;

    #[test]
    fn special_characters_are_escaped_in_measurements() -> Result {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.measurement_to_vec()?,
            br#"alpha\,beta=delta\ gamma"epsilon"#.as_ref(),
        )?;
        Ok(())
    }

    #[test]
    fn special_characters_are_escaped_in_tag_keys() -> Result {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.tag_key_to_vec()?,
            br#"alpha\,beta\=delta\ gamma"epsilon"#.as_ref(),
        )?;
        Ok(())
    }

    #[test]
    fn special_characters_are_escaped_in_tag_values() -> Result {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.tag_value_to_vec()?,
            br#"alpha\,beta\=delta\ gamma"epsilon"#.as_ref(),
        )?;
        Ok(())
    }

    #[test]
    fn special_characters_are_escaped_in_field_keys() -> Result {
        assert_utf8_strings_eq(
            &ALL_THE_DELIMITERS.field_key_to_vec()?,
            br#"alpha\,beta\=delta\ gamma"epsilon"#.as_ref(),
        )?;
        Ok(())
    }

    #[test]
    fn special_characters_are_escaped_in_field_values_of_strings() -> Result {
        assert_utf8_strings_eq(
            &FieldValue::from(ALL_THE_DELIMITERS).field_value_to_vec()?,
            br#""alpha,beta=delta gamma\"epsilon""#.as_ref(),
        )?;
        Ok(())
    }

    #[test]
    fn field_value_of_bool() -> Result {
        let e = FieldValue::from(true);
        assert_utf8_strings_eq(&e.field_value_to_vec()?, b"t")?;

        let e = FieldValue::from(false);
        assert_utf8_strings_eq(&e.field_value_to_vec()?, b"f")?;

        Ok(())
    }

    #[test]
    fn field_value_of_float() -> Result {
        let e = FieldValue::from(42_f64);
        assert_utf8_strings_eq(&e.field_value_to_vec()?, b"42")?;
        Ok(())
    }

    #[test]
    fn field_value_of_integer() -> Result {
        let e = FieldValue::from(42_i64);
        assert_utf8_strings_eq(&e.field_value_to_vec()?, b"42i")?;
        Ok(())
    }

    #[test]
    fn field_value_of_string() -> Result {
        let e = FieldValue::from("hello");
        assert_utf8_strings_eq(&e.field_value_to_vec()?, br#""hello""#)?;
        Ok(())
    }

    // Clears up the boilerplate of writing to a vector from the tests
    macro_rules! test_extension_traits {
        ($($ext_name:ident :: $ext_fn_name:ident -> $base_name:ident :: $base_fn_name:ident,)*) => {
            $(
                trait $ext_name: $base_name {
                    fn $ext_fn_name(&self) -> io::Result<Vec<u8>> {
                        let mut v = Vec::new();
                        self.$base_fn_name(&mut v)?;
                        Ok(v)
                    }
                }
                impl<T: $base_name + ?Sized> $ext_name for T {}
            )*
        }
    }

    test_extension_traits! {
        WriteDataPointExt::data_point_to_vec -> WriteDataPoint::write_data_point_to,
        WriteMeasurementExt::measurement_to_vec -> WriteMeasurement::write_measurement_to,
        WriteTagKeyExt::tag_key_to_vec -> WriteTagKey::write_tag_key_to,
        WriteTagValueExt::tag_value_to_vec -> WriteTagValue::write_tag_value_to,
        WriteFieldKeyExt::field_key_to_vec -> WriteFieldKey::write_field_key_to,
        WriteFieldValueExt::field_value_to_vec -> WriteFieldValue::write_field_value_to,
    }

    // TODO: What should the names of the attributes and traits be? influx? influxdb?
    // TODO: Should the default for a struct field be influx tag or field?
    // TODO: ensure tags are sorted
    // TODO: add fixed tags (fields?) to the struct
    #[derive(Debug, DataPoint)]
    #[influx(measurement = "cpu_load_short")]
    struct Load {
        #[influx(tag)]
        region: Region,
        #[influx(tag)]
        host: String,
        #[influx(field)]
        value: f64,
        #[influx(timestamp)]
        timestamp: i64,
    }

    //#[influx]
    #[allow(dead_code)]
    #[derive(Debug)]
    enum Region {
        West,
        East,
    }

    // TODO: have the `derive` crate generate this
    impl WriteTagValue for Region {
        fn write_tag_value_to<W>(&self, mut w: W) -> io::Result<()>
        where
            W: io::Write,
        {
            match self {
                Region::East => w.write_all(b"East")?,
                Region::West => w.write_all(b"West")?,
            };
            Ok(())
        }
    }

    // Actual test

    #[test]
    fn derive_based_implementationx() -> Result {
        let load = Load {
            region: Region::East,
            host: "something".into(),
            value: 42.42,
            timestamp: 900,
        };

        let x = load.data_point_to_vec()?;

        dbg!(String::from_utf8_lossy(&x));

        assert_eq!(
            x,
            b"cpu_load_short,host=something,region=East value=42.42 900".as_ref(),
        );
        Ok(())
    }
}
