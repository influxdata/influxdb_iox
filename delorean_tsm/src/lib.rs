#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

pub mod encoders;
pub mod mapper;
pub mod reader;

use std::convert::TryFrom;
use std::error;
use std::fmt;
use std::io;

#[derive(Clone, Debug)]
pub struct ParsedTSMKey {
    pub measurement: String,
    pub tagset: Vec<(String, String)>,
    pub field_key: String,
}

/// parse_tsm_key parses from the series key the measurement, field key and tag
/// set.
///
/// It does not provide access to the org and bucket ids on the key, these can
/// be accessed via org_id() and bucket_id() respectively.
///
/// TODO: handle escapes in the series key for , = and \t
///
fn parse_tsm_key(mut key: Vec<u8>) -> Result<ParsedTSMKey, TSMError> {
    // skip over org id, bucket id, comma, null byte (measurement) and =
    // The next n-1 bytes are the measurement name, where the nᵗʰ byte is a `,`.
    key = key.drain(8 + 8 + 1 + 1 + 1..).collect::<Vec<u8>>();
    let mut i = 0;
    // TODO(edd): can we make this work with take_while?
    while i != key.len() {
        if key[i] == b',' {
            break;
        }
        i += 1;
    }

    let mut rem_key = key.drain(i..).collect::<Vec<u8>>();
    let measurement = String::from_utf8(key).map_err(|e| TSMError {
        description: e.to_string(),
    })?;

    let mut tagset = Vec::<(String, String)>::with_capacity(10);
    let mut reading_key = true;
    let mut key = String::with_capacity(100);
    let mut value = String::with_capacity(100);

    // skip the comma separating measurement tag
    for byte in rem_key.drain(1..) {
        match byte {
            44 => {
                // ,
                reading_key = true;
                tagset.push((key, value));
                key = String::with_capacity(250);
                value = String::with_capacity(250);
            }
            61 => {
                // =
                reading_key = false;
            }
            _ => {
                if reading_key {
                    key.push(byte as char);
                } else {
                    value.push(byte as char);
                }
            }
        }
    }

    // fields are stored on the series keys in TSM indexes as follows:
    //
    // <field_key><4-byte delimiter><field_key>
    //
    // so we can trim the parsed value.
    let field_trim_length = (value.len() - 4) / 2;
    let (field, _) = value.split_at(field_trim_length);
    Ok(ParsedTSMKey {
        measurement,
        tagset,
        field_key: field.to_string(),
    })
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub enum BlockType {
    Float,
    Integer,
    Bool,
    Str,
    Unsigned,
}

impl TryFrom<u8> for BlockType {
    type Error = TSMError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Float),
            1 => Ok(Self::Integer),
            2 => Ok(Self::Bool),
            3 => Ok(Self::Str),
            4 => Ok(Self::Unsigned),
            _ => Err(TSMError {
                description: format!("{:?} is invalid block type", value),
            }),
        }
    }
}

/// `Block` holds information about location and time range of a block of data.
#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub struct Block {
    pub min_time: i64,
    pub max_time: i64,
    pub offset: u64,
    pub size: u32,
    pub typ: BlockType,
}

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
const MAX_BLOCK_VALUES: usize = 1000;

/// `BlockData` describes the various types of block data that can be held within
/// a TSM file.
#[derive(Debug, Clone)]
pub enum BlockData {
    Float { ts: Vec<i64>, values: Vec<f64> },
    Integer { ts: Vec<i64>, values: Vec<i64> },
    Bool { ts: Vec<i64>, values: Vec<bool> },
    Str { ts: Vec<i64>, values: Vec<Vec<u8>> },
    Unsigned { ts: Vec<i64>, values: Vec<u64> },
}

impl BlockData {
    pub fn is_empty(&self) -> bool {
        match &self {
            Self::Float { ts, values: _ } => ts.is_empty(),
            Self::Integer { ts, values: _ } => ts.is_empty(),
            Self::Bool { ts, values: _ } => ts.is_empty(),
            Self::Str { ts, values: _ } => ts.is_empty(),
            Self::Unsigned { ts, values: _ } => ts.is_empty(),
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// `InfluxID` represents an InfluxDB ID used in InfluxDB 2.x to represent
/// organization and bucket identifiers.
pub struct InfluxID(u64);

#[allow(dead_code)]
impl InfluxID {
    fn new_str(s: &str) -> Result<Self, TSMError> {
        let v = u64::from_str_radix(s, 16).map_err(|e| TSMError {
            description: e.to_string(),
        })?;
        Ok(Self(v))
    }

    fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for InfluxID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:016x}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct TSMError {
    pub description: String,
}

impl fmt::Display for TSMError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for TSMError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl From<io::Error> for TSMError {
    fn from(e: io::Error) -> Self {
        Self {
            description: format!("TODO - io error: {} ({:?})", e, e),
        }
    }
}

impl From<std::str::Utf8Error> for TSMError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self {
            description: format!("TODO - utf8 error: {} ({:?})", e, e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tsm_key() {
        //<org_id bucket_id>,\x00=http_api_request_duration_seconds,handler=platform,method=POST,path=/api/v2/setup,status=2XX,user_agent=Firefox,\xff=sum#!~#sum
        let buf = vec![
            "05C19117091A100005C19117091A10012C003D68747470",
            "5F6170695F726571756573745F6475726174696F6E5F73",
            "65636F6E64732C68616E646C65723D706C6174666F726D",
            "2C6D6574686F643D504F53542C706174683D2F6170692F",
            "76322F73657475702C7374617475733D3258582C757365",
            "725F6167656E743D46697265666F782CFF3D73756D2321",
            "7E2373756D",
        ]
        .join("");
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(tsm_key).unwrap();
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
        //<org_id bucket_id>,\x00=query_log,env=prod01-eu-central-1,error=memory\ allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\ 6946816;\ memory\ allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\ 6946816,errorCode=invalid,errorType=user,host=queryd-algow-rw-76d68d5968-fzgwr,hostname=queryd-algow-rw-76d68d5968-fzgwr,nodename=ip-10-153-10-221.eu-central-1.compute.internal,orgID=0b6e852e272ffdd9,ot_trace_sampled=false,role=queryd-algow-rw,source=hackney,ÿ=responseSize#!~#responseSize
        let buf = vec![
            "844910ECE80BE8BC3C0BD4C89186CA892C",
            "003D71756572795F6C6F672C656E763D70726F6430312D65752D63656E747261",
            "6C2D312C6572726F723D6D656D6F72795C20616C6C6F636174696F6E5C206C69",
            "6D69745C20726561636865643A5C206C696D69745C203734303030303030305C",
            "2062797465735C2C5C20616C6C6F63617465643A5C203733393834393038385C2",
            "C5C2077616E7465643A5C20363934363831363B5C206D656D6F72795C20616C6C",
            "6F636174696F6E5C206C696D69745C20726561636865643A5C206C696D69745C2",
            "03734303030303030305C2062797465735C2C5C20616C6C6F63617465643A5C20",
            "3733393834393038385C2C5C2077616E7465643A5C20363934363831362C65727",
            "26F72436F64653D696E76616C69642C6572726F72547970653D757365722C686F",
            "73743D7175657279642D616C676F772D72772D373664363864353936382D667A6",
            "777722C686F73746E616D653D7175657279642D616C676F772D72772D37366436",
            "3864353936382D667A6777722C6E6F64656E616D653D69702D31302D3135332D3",
            "1302D3232312E65752D63656E7472616C2D312E636F6D707574652E696E746572",
            "6E616C2C6F726749443D306236653835326532373266666464392C6F745F74726",
            "163655F73616D706C65643D66616C73652C726F6C653D7175657279642D616C67",
            "6F772D72772C736F757263653D6861636B6E65792CFF3D726573706F6E7365536",
            "97A6523217E23726573706F6E736553697A65",
        ]
        .join("");
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(tsm_key).unwrap();
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
        assert_eq!(parsed_key.field_key, String::from("requeueDuration"));
    }

    #[test]
    fn influx_id() {
        let id = InfluxID::new_str("20aa9b0").unwrap();
        assert_eq!(id, InfluxID(34_253_232));
        assert_eq!(format!("{}", id), "00000000020aa9b0");
    }
}
