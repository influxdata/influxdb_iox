use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Clone, Debug)]
pub struct ParsedTSMKey {
    pub measurement: String,
    pub tagset: Vec<(String, String)>,
    pub field_key: String,
}

/// Public error type that wraps the underlying data parsing error
/// with the actual key value being parsed.
#[derive(Debug, Snafu, PartialEq)]
pub enum Error {
    #[snafu(display(r#"Error while parsing tsm tag key '{}': {}"#, key, source))]
    ParsingTSMKey { key: String, source: DataError },
}

#[derive(Debug, Snafu, PartialEq)]
pub enum DataError {
    #[snafu(display(r#"No measurement found (expected to find in tag field \x00)"#))]
    NoMeasurement {},

    #[snafu(display(r#"No field key (expected to find in tag field \xff)"#))]
    NoFieldKey {},

    #[snafu(display(
        r#"Found new measurement '{}' after the first '{}'"#,
        new_measurement,
        old_measurement
    ))]
    MultipleMeasurements {
        new_measurement: String,
        old_measurement: String,
    },

    #[snafu(display(
        r#"Found new field key '{}' after the first '{}'"#,
        new_field,
        old_field
    ))]
    MultipleFields {
        new_field: String,
        old_field: String,
    },

    #[snafu(display(r#"Error parsing tsm tag key: {}"#, description))]
    ParsingTSMTagKey { description: String },

    #[snafu(display(
        r#"Error parsing tsm tag value for key '{}': {}"#,
        tag_key,
        description
    ))]
    ParsingTSMTagValue {
        tag_key: String,
        description: String,
    },

    #[snafu(display(r#"Error parsing tsm field key: {}"#, description))]
    ParsingTSMFieldKey { description: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// parses the the measurement, field key and tag
/// set from a tsm index key
///
/// It does not provide access to the org and bucket ids on the key, these can
/// be accessed via org_id() and bucket_id() respectively.
///
/// Loosely based on [points.go](https://github.com/influxdata/influxdb/blob/751d70a213e5fdae837eda13d7ecb37763e69abb/models/points.go#L462)
///
/// The format looks roughly like:
///
/// <org_id bucket_id>,\x00=<measurement>,<tag_keys_str>,\xff=<field_key_str>#!~#<field_key_str>
///
/// For example:
/// <org_id bucket_id>,\x00=http_api_request_duration_seconds,status=2XX,\xff=sum#!~#sum
///
///    measurement = "http_api_request"
///    tags = [("status", "2XX")]
///    field = "sum"
pub fn parse_tsm_key(key: &[u8]) -> Result<ParsedTSMKey, Error> {
    // Wrap in an internal function to translate error types and add key context
    parse_tsm_key_internal(key).context(ParsingTSMKey {
        key: String::from_utf8_lossy(key),
    })
}

fn parse_tsm_key_internal(key: &[u8]) -> Result<ParsedTSMKey, DataError> {
    // skip over org id, bucket id, comma
    // The next n-1 bytes are the measurement name, where the nᵗʰ byte is a `,`.
    let mut rem_key = key.iter().copied().skip(8 + 8 + 1);

    let mut tagset = Vec::with_capacity(10);
    let mut measurement = None;
    let mut field_key = None;

    loop {
        let tag_key = parse_tsm_tag_key(&mut rem_key)?;
        let (has_more_tags, tag_value) = parse_tsm_tag_value(&tag_key, &mut rem_key)?;

        match tag_key {
            KeyType::Tag(tag_key) => {
                tagset.push((tag_key, tag_value));
            }

            KeyType::Measurement => match measurement {
                Some(measurement) => {
                    return MultipleMeasurements {
                        new_measurement: tag_value,
                        old_measurement: measurement,
                    }
                    .fail()
                }
                None => measurement = Some(tag_value),
            },

            KeyType::Field => match field_key {
                Some(field_key) => {
                    return MultipleFields {
                        old_field: field_key,
                        new_field: tag_value,
                    }
                    .fail()
                }
                None => field_key = Some(parse_tsm_field_key(&tag_value)?),
            },
        };

        if !has_more_tags {
            break;
        }
    }

    Ok(ParsedTSMKey {
        measurement: measurement.context(NoMeasurement)?,
        tagset,
        field_key: field_key.context(NoFieldKey)?,
    })
}

/// Parses the field value stored in a TSM key into a field name.
/// fields are stored on the series keys in TSM indexes as follows:
///
/// <field_key><4-byte delimiter><field_key>
///
/// Example: sum#!~#sum means 'sum' field key
fn parse_tsm_field_key(value: &str) -> Result<String, DataError> {
    const DELIM: &str = "#!~#";

    if value.len() < 6 {
        return ParsingTSMFieldKey {
            description: "field key too short",
        }
        .fail();
    }

    let field_trim_length = (value.len() - 4) / 2;
    let (field, _) = value.split_at(field_trim_length);

    let (a, b) = value.split_at(field.len());
    let (b, c) = b.split_at(DELIM.len());

    // Expect exactly <field><delim><field>
    if !a.starts_with(field) || !b.starts_with(DELIM) || !c.starts_with(field) {
        return ParsingTSMFieldKey {
            description: format!(
                "Invalid field key format '{}', expected '{}{}{}'",
                value, field, DELIM, field
            ),
        }
        .fail();
    }

    Ok(field.into())
}

#[derive(Debug, PartialEq)]

/// Represents the 'type' of the tag.
///
/// This is used to represent the
/// the way the 'measurement name' and the `field name` are stored in
/// TSM OSS 2.0 files, which is different than where line protocol has the
/// measurement and field names.
///
/// Specifically, the measurement name and field names are stored as
/// 'tag's with the special keys \x00 and \xff, respectively.
enum KeyType {
    Tag(String),
    /// the measurement name is encoded in the tsm key as the value of a
    /// special tag key '\x00'.
    ///
    /// For example,the tsm key
    /// "\x00=foo" has the measurement name "foo"
    Measurement,
    /// the field name is encoded in the tsm key as the value of a
    /// special tag key '\xff'.
    ///
    /// For example,the tsm key
    /// "user_agent=Firefox,\xff=sum#!~#sum" has a 'user_agent` tag
    /// key with value Firefix and a field named 'sum')
    Field,
}

impl From<&KeyType> for String {
    fn from(item: &KeyType) -> Self {
        match item {
            KeyType::Tag(s) => s.clone(),
            KeyType::Measurement => "<measurement>".to_string(),
            KeyType::Field => "<field>".to_string(),
        }
    }
}

/// Parses bytes from the `rem_key` input stream until the end of the
/// next key value (=). Consumes the '='
fn parse_tsm_tag_key(rem_key: impl Iterator<Item = u8>) -> Result<KeyType, DataError> {
    enum State {
        Data,
        Measurement,
        Field,
        Escape,
    };

    let mut state = State::Data;
    let mut key = String::with_capacity(250);

    // Examine each character in the tag key until we hit an unescaped
    // equals (the tag value), or we hit an error (i.e., unescaped
    // space or comma).
    for byte in rem_key {
        match state {
            State::Data => match byte {
                b'\x00' => {
                    state = State::Measurement;
                }
                b'\xff' => {
                    state = State::Field;
                }
                b'=' => return Ok(KeyType::Tag(key)),
                b',' => {
                    return ParsingTSMTagKey {
                        description: "unescaped comma",
                    }
                    .fail();
                }
                b' ' => {
                    return ParsingTSMTagKey {
                        description: "unescaped space",
                    }
                    .fail();
                }
                b'\\' => state = State::Escape,
                _ => key.push(byte as char),
            },
            State::Measurement => match byte {
                b'=' => {
                    return Ok(KeyType::Measurement);
                }
                _ => {
                    return ParsingTSMTagKey {
                        description: "extra data after special 0x00",
                    }
                    .fail();
                }
            },
            State::Field => match byte {
                b'=' => {
                    return Ok(KeyType::Field);
                }
                _ => {
                    return ParsingTSMTagKey {
                        description: "extra data after special 0xff",
                    }
                    .fail();
                }
            },
            State::Escape => {
                state = State::Data;
                key.push(byte as char);
            }
        }
    }

    ParsingTSMTagKey {
        description: "unexpected end of data",
    }
    .fail()
}

/// Parses bytes from the `rem_key` input stream until the end of a
/// tag value
///
/// Returns a tuple `(has_more_tags, tag_value)`
///
/// Examples:
///
/// "val1,tag2=val --> Ok((true, "val1")));
/// "val1" --> Ok((False, "val1")));
fn parse_tsm_tag_value(
    tag_key: &KeyType,
    rem_key: impl Iterator<Item = u8>,
) -> Result<(bool, String), DataError> {
    #[derive(Debug)]
    enum State {
        Start,
        Data,
        Escaped,
    };

    let mut state = State::Start;
    let mut tag_value = String::with_capacity(100);

    // Examine each character in the tag value until we hit an unescaped
    // comma (move onto next tag key), or we error out.
    //
    // In line protocol, it is an error to have an unescaped space in
    // a tag value.  However, in the TSM index, it is possible to have
    // have such unescaped spaces values. For example, in
    // https://github.com/influxdata/delorean/issues/228: we saw one
    // like the following (note the combination of escaped and
    // unescaped strings):
    //
    // memoryUsageAfterGc.Code\ Cache#!~#memoryUsageAfterGc.Code Cache
    for byte in rem_key {
        match state {
            State::Start => {
                match byte {
                    // An unescaped equals sign is an invalid tag value.
                    // cpu,tag={'=', 'fo=o'}
                    b'=' => {
                        return ParsingTSMTagValue {
                            tag_key,
                            description: "invalid unescaped '='",
                        }
                        .fail()
                    }
                    // An unescaped space is an invalid tag value.
                    b',' => {
                        return ParsingTSMTagValue {
                            tag_key,
                            description: "missing tag value",
                        }
                        .fail()
                    }
                    b'\\' => state = State::Escaped,
                    _ => {
                        state = State::Data;
                        tag_value.push(byte as char);
                    }
                }
            }
            State::Data => {
                match byte {
                    // An unescaped equals sign is an invalid tag value.
                    // cpu,tag={'=', 'fo=o'}
                    b'=' => {
                        return ParsingTSMTagValue {
                            tag_key,
                            description: "invalid unescaped '='",
                        }
                        .fail()
                    }
                    // cpu,tag=foo,
                    b',' => return Ok((true, tag_value)),
                    // start of escape value
                    b'\\' => state = State::Escaped,
                    _ => {
                        tag_value.push(byte as char);
                    }
                }
            }
            State::Escaped => {
                tag_value.push(byte as char);
                state = State::Data;
            }
        }
    }

    // Tag value cannot be empty.
    match state {
        State::Start => ParsingTSMTagValue {
            tag_key,
            description: "missing tag value",
        }
        .fail(),
        State::Escaped => ParsingTSMTagValue {
            tag_key,
            description: "tag value ends in escape",
        }
        .fail(),
        _ => Ok((false, tag_value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tsm_field_key() {
        // test the operation of parse_tsm_field_key
        assert_eq!(parse_tsm_field_key("sum#!~#sum"), Ok("sum".into()));

        let err_str = parse_tsm_field_key("#!~#")
            .expect_err("expect parsing error")
            .to_string();
        assert!(err_str.contains("field key too short"), err_str);

        let err_str = parse_tsm_field_key("foo#!~#fpp")
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("Invalid field key format \'foo#!~#fpp\', expected \'foo#!~#foo\'"),
            err_str
        );

        let err_str = parse_tsm_field_key("foo#!~#naaa")
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("Invalid field key format \'foo#!~#naaa\', expected \'foo#!~#foo\'"),
            err_str
        );

        let err_str = parse_tsm_field_key("foo#!~#")
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("Invalid field key format \'foo#!~#\', expected \'f#!~#f\'"),
            err_str
        );

        let err_str = parse_tsm_field_key("foo####foo")
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("Invalid field key format \'foo####foo\', expected \'foo#!~#foo\'"),
            err_str
        );
    }

    #[test]
    fn test_parse_tsm_tag_key() {
        do_test_parse_tsm_tag_key_error("", "", "unexpected end of data");
        do_test_parse_tsm_tag_key_good("foo=bar", "bar", KeyType::Tag("foo".into()));
        do_test_parse_tsm_tag_key_good("foo=", "", KeyType::Tag("foo".into()));
        do_test_parse_tsm_tag_key_error("foo", "", "unexpected end of data");
        do_test_parse_tsm_tag_key_error("foo,=bar", "=bar", "unescaped comma");
        do_test_parse_tsm_tag_key_error("foo =bar", "=bar", "unescaped space");

        do_test_parse_tsm_tag_key_good(r"\ foo=bar", "bar", KeyType::Tag(" foo".into()));
        do_test_parse_tsm_tag_key_good(r"\=foo=bar", "bar", KeyType::Tag("=foo".into()));
        do_test_parse_tsm_tag_key_good(r"\,foo=bar", "bar", KeyType::Tag(",foo".into()));
        do_test_parse_tsm_tag_key_good(r"\foo=bar", "bar", KeyType::Tag("foo".into()));
        do_test_parse_tsm_tag_key_good(r"\\foo=bar", "bar", KeyType::Tag(r"\foo".into()));

        do_test_parse_tsm_tag_key_good(r"f\ oo=bar", "bar", KeyType::Tag("f oo".into()));
        do_test_parse_tsm_tag_key_good(r"f\=oo=bar", "bar", KeyType::Tag("f=oo".into()));
        do_test_parse_tsm_tag_key_good(r"f\,oo=bar", "bar", KeyType::Tag("f,oo".into()));
        do_test_parse_tsm_tag_key_good(r"f\oo=bar", "bar", KeyType::Tag("foo".into()));
    }

    #[test]
    fn test_parse_tsm_tag_value() {
        do_test_parse_tsm_tag_value_error("", "", "missing tag value");
        do_test_parse_tsm_tag_value_good(
            "val1,tag2=val2 value=1",
            "tag2=val2 value=1",
            (true, "val1".into()),
        );
        do_test_parse_tsm_tag_value_good("val1", "", (false, "val1".into()));
        do_test_parse_tsm_tag_value_good(r"\ val1", "", (false, " val1".into()));
        do_test_parse_tsm_tag_value_good(r"val\ 1", "", (false, "val 1".into()));
        do_test_parse_tsm_tag_value_good(r"val1\ ", "", (false, "val1 ".into()));
        do_test_parse_tsm_tag_value_error(r"val1\", "", "tag value ends in escape");
        do_test_parse_tsm_tag_value_error(r"=b", "b", "invalid unescaped '='");
        do_test_parse_tsm_tag_value_error(r"f=b", "b", "invalid unescaped '='");
        do_test_parse_tsm_tag_value_good(r" v", "", (false, " v".into()));
        do_test_parse_tsm_tag_value_good(r"v ", "", (false, "v ".into()));
        // Combination of escaped and unescaped strings
        do_test_parse_tsm_tag_value_good(r"v x\ y z", "", (false, "v x y z".into()));
    }

    // create key in this form:
    //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
    fn make_tsm_key_prefix(measurement: &str, tag_keys_str: &str) -> Vec<u8> {
        let mut key = Vec::new();

        let org = b"12345678";
        let bucket = b"87654321";

        // 8 bytes of ORG
        key.extend_from_slice(org);

        // 8 bytes of BUCKET
        key.extend_from_slice(bucket);

        key.push(b',');

        // 2 bytes: special measurement tag key \x00=
        key.push(b'\x00');
        key.push(b'=');
        key.extend_from_slice(measurement.as_bytes());

        key.push(b',');
        key.extend_from_slice(tag_keys_str.as_bytes());

        key
    }

    // add this to the key: ,\xff=<field_key_str>#!~#<field_key_str>
    fn add_field_key(mut key: Vec<u8>, field_key_str: &str) -> Vec<u8> {
        key.push(b',');
        key.push(b'\xff');
        key.push(b'=');
        key.extend_from_slice(field_key_str.as_bytes());
        key.extend_from_slice(b"#!~#");
        key.extend_from_slice(field_key_str.as_bytes());
        key
    }

    #[test]
    fn parse_tsm_key_good() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>,\xff=<field_key_str>#!~#<field_key_str>
        let mut key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");
        key = add_field_key(key, "f");

        let parsed_key = super::parse_tsm_key(&key).unwrap();
        assert_eq!(parsed_key.measurement, String::from("m"));
        let exp_tagset = vec![
            (String::from("tag1"), String::from("val1")),
            (String::from("tag2"), String::from("val2")),
        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("f"));
    }

    #[test]
    fn parse_tsm_error_has_key() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
        let key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");

        let err_str = parse_tsm_key(&key)
            .expect_err("expect parsing error")
            .to_string();
        // expect that a representation of the actual TSM key is in the error message
        assert!(
            err_str.contains(
                "Error while parsing tsm tag key '1234567887654321,\x00=m,tag1=val1,tag2=val2':"
            ),
            err_str
        );
    }

    #[test]
    fn parse_tsm_key_no_field() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
        let key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");

        let err_str = parse_tsm_key(&key)
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("No field key (expected to find in tag field \\xff)"),
            err_str
        );
    }

    #[test]
    fn parse_tsm_key_two_fields() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>\xff=<field-key_str>#!~#<field_key_str>\xff=<field-key_str>#!~#<field_key_str>
        let mut key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");
        key = add_field_key(key, "f");
        key = add_field_key(key, "f2");

        let err_str = parse_tsm_key(&key)
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("Found new field key 'f2#!~#f2' after the first 'f'"),
            err_str
        );
    }

    #[test]
    fn test_parse_tsm_key() {
        //<org_id bucket_id>,\x00=http_api_request_duration_seconds,handler=platform,method=POST,path=/api/v2/setup,status=2XX,user_agent=Firefox,\xff=sum#!~#sum
        let buf = "05C19117091A100005C19117091A10012C003D68747470\
             5F6170695F726571756573745F6475726174696F6E5F73\
             65636F6E64732C68616E646C65723D706C6174666F726D\
             2C6D6574686F643D504F53542C706174683D2F6170692F\
             76322F73657475702C7374617475733D3258582C757365\
             725F6167656E743D46697265666F782CFF3D73756D2321\
             7E2373756D";
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(&tsm_key).unwrap();
        assert_eq!(
            parsed_key.measurement,
            String::from("http_api_request_duration_seconds")
        );

        let exp_tagset = vec![
            (String::from("handler"), String::from("platform")),
            (String::from("method"), String::from("POST")),
            (String::from("path"), String::from("/api/v2/setup")),
            (String::from("status"), String::from("2XX")),
            (String::from("user_agent"), String::from("Firefox")),
        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("sum"));
    }

    #[test]
    fn parse_tsm_key_escaped() {
        //<org_id bucket_id>,\x00=query_log,env=prod01-eu-central-1,error=memory\ allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\ 6946816;\ memory\ allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\ 6946816,errorCode=invalid,errorType=user,host=queryd-algow-rw-76d68d5968-fzgwr,hostname=queryd-algow-rw-76d68d5968-fzgwr,nodename=ip-10-153-10-221.eu-central-1.compute.internal,orgID=0b6e852e272ffdd9,ot_trace_sampled=false,role=queryd-algow-rw,source=hackney,\xff=responseSize#!~#responseSize
        let buf = "844910ECE80BE8BC3C0BD4C89186CA892C\
             003D71756572795F6C6F672C656E763D70726F6430312D65752D63656E747261\
             6C2D312C6572726F723D6D656D6F72795C20616C6C6F636174696F6E5C206C69\
             6D69745C20726561636865643A5C206C696D69745C203734303030303030305C\
             2062797465735C2C5C20616C6C6F63617465643A5C203733393834393038385C2\
             C5C2077616E7465643A5C20363934363831363B5C206D656D6F72795C20616C6C\
             6F636174696F6E5C206C696D69745C20726561636865643A5C206C696D69745C2\
             03734303030303030305C2062797465735C2C5C20616C6C6F63617465643A5C20\
             3733393834393038385C2C5C2077616E7465643A5C20363934363831362C65727\
             26F72436F64653D696E76616C69642C6572726F72547970653D757365722C686F\
             73743D7175657279642D616C676F772D72772D373664363864353936382D667A6\
             777722C686F73746E616D653D7175657279642D616C676F772D72772D37366436\
             3864353936382D667A6777722C6E6F64656E616D653D69702D31302D3135332D3\
             1302D3232312E65752D63656E7472616C2D312E636F6D707574652E696E746572\
             6E616C2C6F726749443D306236653835326532373266666464392C6F745F74726\
             163655F73616D706C65643D66616C73652C726F6C653D7175657279642D616C67\
             6F772D72772C736F757263653D6861636B6E65792CFF3D726573706F6E7365536\
             97A6523217E23726573706F6E736553697A65";
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(&tsm_key).unwrap();
        assert_eq!(parsed_key.measurement, String::from("query_log"));

        let exp_tagset = vec![
            (String::from("env"), String::from("prod01-eu-central-1")),
            (String::from("error"), String::from("memory allocation limit reached: limit 740000000 bytes, allocated: 739849088, wanted: 6946816; memory allocation limit reached: limit 740000000 bytes, allocated: 739849088, wanted: 6946816")),
            (String::from("errorCode"), String::from("invalid")),
            (String::from("errorType"), String::from("user")),
            (String::from("host"), String::from("queryd-algow-rw-76d68d5968-fzgwr")),
            (String::from("hostname"), String::from("queryd-algow-rw-76d68d5968-fzgwr")),
            (String::from("nodename"), String::from("ip-10-153-10-221.eu-central-1.compute.internal")),
            (String::from("orgID"), String::from("0b6e852e272ffdd9")),
            (String::from("ot_trace_sampled"), String::from("false")),
            (String::from("role"), String::from("queryd-algow-rw")),
            (String::from("source"), String::from("hackney")),

        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("responseSize"));
    }

    fn do_test_parse_tsm_tag_key_good(
        input: &str,
        expected_remaining_input: &str,
        expected_tag_key: KeyType,
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_key(&mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_key) => {
                assert_eq!(tag_key, expected_tag_key, "while parsing input '{}'", input);
            }
            Err(err) => {
                panic!(
                    "Got error '{}', expected parsed tag key: '{:?}' while parsing '{}'",
                    err, expected_tag_key, input
                );
            }
        }
        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{}'",
            input
        );
    }

    fn do_test_parse_tsm_tag_key_error(
        input: &str,
        expected_remaining_input: &str,
        expected_error: &str,
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_key(&mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_key) => {
                panic!(
                    "Got parsed key {:?}, expected failure {} while parsing input '{}'",
                    tag_key, expected_error, input
                );
            }
            Err(err) => {
                let err_str = err.to_string();
                assert!(
                    err_str.contains(expected_error),
                    "Did not find expected error '{}' in actual error '{}'",
                    expected_error,
                    err_str
                );
            }
        }
        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{}'",
            input
        );
    }

    fn do_test_parse_tsm_tag_value_good(
        input: &str,
        expected_remaining_input: &str,
        expected_tag_value: (bool, String),
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_value(&KeyType::Field, &mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_value) => {
                assert_eq!(
                    tag_value, expected_tag_value,
                    "while parsing input '{}'",
                    input
                );
            }
            Err(err) => {
                panic!(
                    "Got error '{}', expected parsed tag_value: '{:?}' while parsing input '{}",
                    err, expected_tag_value, input
                );
            }
        }

        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{}'",
            input
        );
    }

    fn do_test_parse_tsm_tag_value_error(
        input: &str,
        expected_remaining_input: &str,
        expected_error: &str,
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_value(&KeyType::Field, &mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_value) => {
                panic!(
                    "Got parsed tag_value {:?}, expected failure {} while parsing input '{}'",
                    tag_value, expected_error, input
                );
            }
            Err(err) => {
                let err_str = err.to_string();
                assert!(
                    err_str.contains(expected_error),
                    "Did not find expected error '{}' in actual error '{}'",
                    expected_error,
                    err_str
                );
            }
        }

        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{}'",
            input
        );
    }
}
