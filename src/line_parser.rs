use either::Either;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::digit1,
    combinator::{map, opt, recognize},
    sequence::{preceded, separated_pair, terminated, tuple},
};
use smallvec::{smallvec, SmallVec};
use snafu::{ResultExt, Snafu};
use std::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap},
    convert::TryFrom,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Must not contain duplicate tags, but "{}" was repeated"#, tag_key))]
    DuplicateTag { tag_key: String },

    #[snafu(display(r#"No fields were provided"#))]
    FieldSetMissing,

    #[snafu(display(r#"Unable to parse integer value '{}'"#, value))]
    IntegerValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse floating-point value '{}'"#, value))]
    FloatValueInvalid {
        source: std::num::ParseFloatError,
        value: String,
    },

    #[snafu(display(r#"Unable to parse timestamp value '{}'"#, value))]
    TimestampValueInvalid {
        source: std::num::ParseIntError,
        value: String,
    },

    // This error is for compatibility with the Go parser
    #[snafu(display(
        r#"Measurements, tag keys and values, and field keys may not end with a backslash"#
    ))]
    EndsWithBackslash,

    // TODO: Replace this with specific failures.
    #[snafu(display(r#"A generic parsing error occurred: {:?}"#, kind))]
    GenericParsingError {
        kind: nom::error::ErrorKind,
        trace: Vec<Error>,
    },
}

impl nom::error::ParseError<&str> for Error {
    fn from_error_kind(_input: &str, kind: nom::error::ErrorKind) -> Self {
        GenericParsingError {
            kind,
            trace: vec![],
        }
        .build()
    }

    fn append(_input: &str, kind: nom::error::ErrorKind, other: Self) -> Self {
        GenericParsingError {
            kind,
            trace: vec![other],
        }
        .build()
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
type IResult<I, T, E = Error> = nom::IResult<I, T, E>;

/// Represents a single typed point of timeseries data
///
/// A Point consists of a series identifier, a timestamp, and a value.
///
/// The series identifier is a string that concatenates the
/// measurement name, tag name=value pairs and field name. These tags
/// are unique and sorted.
///
/// For example, a Point containing a f64 value representing
/// `cpu,host=A,region=west usage_system=64.2 1590488773254420000` could
/// be represented as point like this:
///
/// ```
/// use delorean::line_parser::Point;
///
/// let p = Point {
///     series: "cpu,host=A,region=west\tusage_system".to_string(),
///     series_id: None,
///     value: 64.2,
///     time: 1590488773254420000,
/// };
/// ```
#[derive(Debug, PartialEq, Clone)]
pub struct Point<T> {
    pub series: String,
    pub series_id: Option<u64>,
    pub time: i64,
    pub value: T,
}

impl<T> Point<T> {
    pub fn index_pairs(&self) -> Vec<Pair> {
        index_pairs(&self.series)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum PointType {
    I64(Point<i64>),
    F64(Point<f64>),
}

impl PointType {
    pub fn new_i64(series: String, value: i64, time: i64) -> PointType {
        PointType::I64(Point {
            series,
            series_id: None,
            value,
            time,
        })
    }

    pub fn new_f64(series: String, value: f64, time: i64) -> PointType {
        PointType::F64(Point {
            series,
            series_id: None,
            value,
            time,
        })
    }

    pub fn series(&self) -> &String {
        match self {
            PointType::I64(p) => &p.series,
            PointType::F64(p) => &p.series,
        }
    }

    pub fn time(&self) -> i64 {
        match self {
            PointType::I64(p) => p.time,
            PointType::F64(p) => p.time,
        }
    }

    pub fn set_time(&mut self, t: i64) {
        match self {
            PointType::I64(p) => p.time = t,
            PointType::F64(p) => p.time = t,
        }
    }

    pub fn series_id(&self) -> Option<u64> {
        match self {
            PointType::I64(p) => p.series_id,
            PointType::F64(p) => p.series_id,
        }
    }

    pub fn set_series_id(&mut self, id: u64) {
        match self {
            PointType::I64(p) => p.series_id = Some(id),
            PointType::F64(p) => p.series_id = Some(id),
        }
    }

    pub fn i64_value(&self) -> Option<i64> {
        match self {
            PointType::I64(p) => Some(p.value),
            _ => None,
        }
    }

    pub fn f64_value(&self) -> Option<f64> {
        match self {
            PointType::F64(p) => Some(p.value),
            _ => None,
        }
    }

    pub fn index_pairs(&self) -> Vec<Pair> {
        match self {
            PointType::I64(p) => p.index_pairs(),
            PointType::F64(p) => p.index_pairs(),
        }
    }
}

// TODO: handle escapes in the line protocol for , = and \t
/// index_pairs parses the series key into key value pairs for insertion into the index. In
/// cases where this series is already in the database, this parse step can be skipped entirely.
/// The measurement is represented as a _m key and field as _f.
pub fn index_pairs(key: &str) -> Vec<Pair> {
    let chars = key.chars();
    let mut pairs = vec![];
    let mut key = "_m".to_string();
    let mut value = String::with_capacity(250);
    let mut reading_key = false;

    for ch in chars {
        match ch {
            ',' => {
                reading_key = true;
                pairs.push(Pair { key, value });
                key = String::with_capacity(250);
                value = String::with_capacity(250);
            }
            '=' => {
                reading_key = false;
            }
            '\t' => {
                reading_key = false;
                pairs.push(Pair { key, value });
                key = "_f".to_string();
                value = String::with_capacity(250);
            }
            _ => {
                if reading_key {
                    key.push(ch);
                } else {
                    value.push(ch);
                }
            }
        }
    }
    pairs.push(Pair { key, value });

    pairs
}

// TODO: Could `Pair` hold `Cow` strings?
#[derive(Debug, PartialEq)]
pub struct Pair {
    pub key: String,
    pub value: String,
}

/// Represents an sequence of effectively unescaped strings.
///
/// If we had the input string `"a\nb"`, the `EscapedStr` will hold ["a", "b"].
/// If we had `a\b`, this will also hold ["a", "b"].
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EscapedStr<'a>(SmallVec<[&'a str; 1]>);

impl fmt::Display for EscapedStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for p in &self.0 {
            p.fmt(f)?;
        }
        Ok(())
    }
}

impl<'a> EscapedStr<'a> {
    fn is_escaped(&self) -> bool {
        self.0.len() > 1
    }

    fn ends_with(&self, needle: &str) -> bool {
        self.0.last().map_or(false, |s| s.ends_with(needle))
    }
}

impl From<EscapedStr<'_>> for String {
    fn from(other: EscapedStr<'_>) -> Self {
        other.to_string()
    }
}

impl<'a> From<&'a str> for EscapedStr<'a> {
    fn from(other: &'a str) -> Self {
        Self(smallvec![other])
    }
}

impl PartialEq<&str> for EscapedStr<'_> {
    fn eq(&self, other: &&str) -> bool {
        let mut head = *other;
        for p in &self.0 {
            if head.starts_with(p) {
                head = &head[p.len()..];
            } else {
                return false;
            }
        }
        head.is_empty()
    }
}

type TagSet<'a> = SmallVec<[(EscapedStr<'a>, EscapedStr<'a>); 8]>;
type FieldSet<'a> = SmallVec<[(EscapedStr<'a>, FieldValue); 4]>;

#[derive(Debug)]
/// Represents a single parsed line of line protocol data
///
/// For example, the data formatted in line protocol format
/// `cpu,host=A,region=west usage_system=64.2 1590488773254420000` can
/// be represented as a `ParsedLine` in the following way.
///
/// TODO figure out how to encode this as an actual example code (too
/// many of these structs are private). For now, just use quasi code
///
/// ``` ignore
/// ParsedLine {
///     series: Series {
///         raw_input: "cpu,host=A,region=west",
///         measurement: EscapedStr(["cpu"]),
///         tag_set: Some([
///             (EscapedStr(["host"]), EscapedStr(["A"])),
///             (EscapedStr(["region"]), EscapedStr(["west"]))
///         ])
///     },
///     field_set: [
///         (EscapedStr(["usage_system"]), F64(64.2))
///     ],
///     timestamp: Some(1590488773254420000)
///  }
/// ```
struct ParsedLine<'a> {
    series: Series<'a>,
    field_set: FieldSet<'a>,
    timestamp: Option<i64>,
}

/// Represents a series identifier (measurement, tagset) for line protocol data
#[derive(Debug)]
struct Series<'a> {
    raw_input: &'a str,
    measurement: EscapedStr<'a>,
    tag_set: Option<TagSet<'a>>,
}

impl<'a> Series<'a> {
    pub fn generate_base(self) -> Result<Cow<'a, str>> {
        match (!self.is_escaped(), self.is_sorted_and_unique()) {
            (true, true) => Ok(self.raw_input.into()),
            (_, true) => self.generate_base_with_escaping().map(Into::into),
            (_, _) => self
                .generate_base_with_escaping_sorting_deduplicating()
                .map(Into::into),
        }
    }

    fn generate_base_with_escaping(self) -> Result<String> {
        let mut series_base = self.measurement.to_string();
        for (tag_key, tag_value) in self.tag_set.unwrap_or_default() {
            use std::fmt::Write;
            write!(&mut series_base, ",{}={}", tag_key, tag_value)
                .expect("Could not append string");
        }
        Ok(series_base)
    }

    fn generate_base_with_escaping_sorting_deduplicating(self) -> Result<String> {
        let mut unique_sorted_tag_set = BTreeMap::new();
        for (tag_key, tag_value) in self.tag_set.unwrap_or_default() {
            match unique_sorted_tag_set.entry(tag_key) {
                Entry::Vacant(e) => {
                    e.insert(tag_value);
                }
                Entry::Occupied(e) => {
                    let (tag_key, _) = e.remove_entry();
                    return DuplicateTag { tag_key }.fail();
                }
            }
        }

        let mut series_base = self.measurement.to_string();
        for (tag_key, tag_value) in unique_sorted_tag_set {
            use std::fmt::Write;
            write!(&mut series_base, ",{}={}", tag_key, tag_value)
                .expect("Could not append string");
        }

        Ok(series_base)
    }

    fn is_escaped(&self) -> bool {
        self.measurement.is_escaped() || {
            match &self.tag_set {
                None => false,
                Some(tag_set) => tag_set
                    .iter()
                    .any(|(tag_key, tag_value)| tag_key.is_escaped() || tag_value.is_escaped()),
            }
        }
    }

    fn is_sorted_and_unique(&self) -> bool {
        match &self.tag_set {
            None => true,
            Some(tag_set) => {
                let mut i = tag_set.iter().zip(tag_set.iter().skip(1));
                i.all(|((last_tag_key, _), (this_tag_key, _))| last_tag_key < this_tag_key)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
/// Allowed field types for a `ParsedLine`
enum FieldValue {
    I64(i64),
    F64(f64),
}

// TODO: Return an error for invalid inputs
pub fn parse(input: &str) -> Result<Vec<PointType>> {
    let since_the_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let now_ns = i64::try_from(since_the_epoch.as_nanos()).expect("Time does not fit");

    parse_full(input, now_ns)
}

fn parse_full(input: &str, now_ns: i64) -> Result<Vec<PointType>> {
    parse_lines(input)
        .flat_map(|parsed_line| match parsed_line {
            Ok(parsed_line) => match line_to_points(parsed_line, now_ns) {
                Ok(i) => Either::Left(i.map(Ok)),
                Err(e) => Either::Right(std::iter::once(Err(e))),
            },
            Err(e) => Either::Right(std::iter::once(Err(e))),
        })
        .collect()
}

fn line_to_points(
    parsed_line: ParsedLine<'_>,
    now: i64,
) -> Result<impl Iterator<Item = PointType> + '_> {
    let ParsedLine {
        series,
        field_set,
        timestamp,
    } = parsed_line;

    let series_base = series.generate_base()?;
    let timestamp = timestamp.unwrap_or(now);

    Ok(field_set.into_iter().map(move |(field_key, field_value)| {
        let series = format!("{}\t{}", series_base, field_key);

        match field_value {
            FieldValue::I64(value) => PointType::new_i64(series, value, timestamp),
            FieldValue::F64(value) => PointType::new_f64(series, value, timestamp),
        }
    }))
}

fn parse_lines(mut i: &str) -> impl Iterator<Item = Result<ParsedLine<'_>>> {
    std::iter::from_fn(move || {
        let (remaining, _) = line_whitespace(i).expect("Cannot fail to parse whitespace");
        i = remaining;

        if i.is_empty() {
            return None;
        }

        match parse_line(i) {
            Ok((remaining, line)) => {
                i = remaining;
                Some(Ok(line))
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Some(Err(e)),
            Err(nom::Err::Incomplete(_)) => unreachable!("Cannot have incomplete data"), // Only streaming parsers have this
        }
    })
}

fn parse_line(i: &str) -> IResult<&str, ParsedLine<'_>> {
    let field_set = preceded(whitespace, field_set);
    let timestamp = preceded(whitespace, timestamp);

    let line = tuple((series, field_set, opt(timestamp)));

    map(line, |(series, field_set, timestamp)| ParsedLine {
        series,
        field_set,
        timestamp,
    })(i)
}

fn series(i: &str) -> IResult<&str, Series<'_>> {
    let tag_set = preceded(tag(","), tag_set);
    let series = tuple((measurement, opt(tag_set)));

    let series_and_raw_input = parse_and_recognize(series);

    map(
        series_and_raw_input,
        |(raw_input, (measurement, tag_set))| Series {
            raw_input,
            measurement,
            tag_set,
        },
    )(i)
}

fn measurement(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != ',' && c != '\\');

    let space = map(tag(" "), |_| " ");
    let comma = map(tag(","), |_| ",");
    let backslash = map(tag("\\"), |_| "\\");

    let escaped = alt((space, comma, backslash));

    escape_or_fallback(normal_char, "\\", escaped)(i)
}

fn tag_set(i: &str) -> IResult<&str, TagSet<'_>> {
    let one_tag = separated_pair(tag_key, tag("="), tag_value);
    parameterized_separated_list(tag(","), one_tag, SmallVec::new, |v, i| v.push(i))(i)
}

fn tag_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != '=' && c != '\\');

    escaped_value(normal_char)(i)
}

fn tag_value(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != ',' && c != '\\');
    escaped_value(normal_char)(i)
}

fn field_set(i: &str) -> IResult<&str, FieldSet<'_>> {
    let one_field = separated_pair(field_key, tag("="), field_value);
    let sep = tag(",");

    match parameterized_separated_list1(sep, one_field, SmallVec::new, |v, i| v.push(i))(i) {
        Err(nom::Err::Error(_)) => FieldSetMissing.fail().map_err(nom::Err::Error),
        other => other,
    }
}

fn field_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != '=' && c != '\\');
    escaped_value(normal_char)(i)
}

fn field_value(i: &str) -> IResult<&str, FieldValue> {
    let int = map(integer_value, FieldValue::I64);
    let float = map(float_value, FieldValue::F64);

    alt((int, float))(i)
}

fn integer_value(i: &str) -> IResult<&str, i64> {
    let tagged_value = terminated(integral_value_common, tag("i"));
    map_fail(tagged_value, |value| {
        value.parse().context(IntegerValueInvalid { value })
    })(i)
}

fn float_value(i: &str) -> IResult<&str, f64> {
    let value = alt((float_value_with_decimal, float_value_no_decimal));
    map_fail(value, |value| {
        value.parse().context(FloatValueInvalid { value })
    })(i)
}

fn float_value_with_decimal(i: &str) -> IResult<&str, &str> {
    recognize(separated_pair(integral_value_common, tag("."), digit1))(i)
}

fn float_value_no_decimal(i: &str) -> IResult<&str, &str> {
    integral_value_common(i)
}

fn integral_value_common(i: &str) -> IResult<&str, &str> {
    recognize(preceded(opt(tag("-")), digit1))(i)
}

fn timestamp(i: &str) -> IResult<&str, i64> {
    map_fail(integral_value_common, |value| {
        value.parse().context(TimestampValueInvalid { value })
    })(i)
}

/// Consumes all whitespace at the beginning / end of lines, including
/// completely commented-out lines
fn line_whitespace(mut i: &str) -> IResult<&str, ()> {
    loop {
        let offset = i
            .find(|c| !is_whitespace_boundary_char(c))
            .unwrap_or_else(|| i.len());
        i = &i[offset..];

        if i.starts_with('#') {
            let offset = i.find('\n').unwrap_or_else(|| i.len());
            i = &i[offset..];
        } else {
            break Ok((i, ()));
        }
    }
}

fn whitespace(i: &str) -> IResult<&str, &str> {
    take_while1(|c| c == ' ')(i)
}

fn is_whitespace_boundary_char(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\n'
}

/// While not all of these escape characters are required to be
/// escaped, we support the client escaping them proactively to
/// provide a common experience.
fn escaped_value<'a>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, EscapedStr<'a>> {
    move |i| {
        let backslash = map(tag("\\"), |_| "\\");
        let comma = map(tag(","), |_| ",");
        let equal = map(tag("="), |_| "=");
        let space = map(tag(" "), |_| " ");

        let escaped = alt((backslash, comma, equal, space));

        escape_or_fallback(normal, "\\", escaped)(i)
    }
}

/// Parse an unescaped piece of text, interspersed with
/// potentially-escaped characters. If the character *isn't* escaped,
/// treat it as a literal character.
fn escape_or_fallback<'a>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str>,
    escape_char: &'static str,
    escaped: impl Fn(&'a str) -> IResult<&'a str, &'a str>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, EscapedStr<'a>> {
    move |i| {
        let (remaining, s) = escape_or_fallback_inner(normal, escape_char, escaped)(i)?;

        if s.ends_with("\\") {
            EndsWithBackslash.fail().map_err(nom::Err::Failure)
        } else {
            Ok((remaining, s))
        }
    }
}

fn escape_or_fallback_inner<'a, Error>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
    escape_char: &'static str,
    escaped: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
) -> impl Fn(&'a str) -> IResult<&'a str, EscapedStr<'a>, Error>
where
    Error: nom::error::ParseError<&'a str>,
{
    move |i| {
        let mut result = SmallVec::new();
        let mut head = i;

        loop {
            match normal(head) {
                Ok((remaining, parsed)) => {
                    result.push(parsed);
                    head = remaining;
                }
                Err(nom::Err::Error(_)) => {
                    // FUTURE: https://doc.rust-lang.org/std/primitive.str.html#method.strip_prefix
                    if head.starts_with(escape_char) {
                        let after = &head[escape_char.len()..];

                        match escaped(after) {
                            Ok((remaining, parsed)) => {
                                result.push(parsed);
                                head = remaining;
                            }
                            Err(nom::Err::Error(_)) => {
                                result.push(escape_char);
                                head = after;

                                // The Go parser assumes that *any* unknown escaped character is valid.
                                match head.chars().next() {
                                    Some(c) => {
                                        let (escaped, remaining) = head.split_at(c.len_utf8());
                                        result.push(escaped);
                                        head = remaining;
                                    }
                                    None => return Ok((head, EscapedStr(result))),
                                }
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        // have we parsed *anything*?
                        if head == i {
                            return Err(nom::Err::Error(Error::from_error_kind(
                                head,
                                nom::error::ErrorKind::EscapedTransform,
                            )));
                        } else {
                            return Ok((head, EscapedStr(result)));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// This is a copied version of nom's `separated_list` that allows
/// parameterizing the created collection via closures.
pub fn parameterized_separated_list<I, O, O2, E, F, G, Ret>(
    sep: G,
    f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: Fn(I) -> IResult<I, O, E>,
    G: Fn(I) -> IResult<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = cre();

        match f(i.clone()) {
            Err(nom::Err::Error(_)) => return Ok((i, res)),
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                if i1 == i {
                    return Err(nom::Err::Error(E::from_error_kind(
                        i1,
                        nom::error::ErrorKind::SeparatedList,
                    )));
                }

                add(&mut res, o);
                i = i1;
            }
        }

        loop {
            match sep(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    if i1 == i {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f(i1.clone()) {
                        Err(nom::Err::Error(_)) => return Ok((i, res)),
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            if i2 == i {
                                return Err(nom::Err::Error(E::from_error_kind(
                                    i2,
                                    nom::error::ErrorKind::SeparatedList,
                                )));
                            }

                            add(&mut res, o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

pub fn parameterized_separated_list1<I, O, O2, E, F, G, Ret>(
    sep: G,
    f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: Fn(I) -> IResult<I, O, E>,
    G: Fn(I) -> IResult<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |i| {
        let (rem, first) = f(i)?;

        let mut res = cre();
        add(&mut res, first);

        match sep(rem.clone()) {
            Ok((rem, _)) => parameterized_separated_list(sep, f, move || res, add)(rem),
            Err(nom::Err::Error(_)) => Ok((rem, res)),
            Err(e) => Err(e),
        }
    }
}

/// This is a copied version of nom's `recognize` that runs the parser
/// **and** returns the entire matched input.
pub fn parse_and_recognize<
    I: Clone + nom::Offset + nom::Slice<std::ops::RangeTo<usize>>,
    O,
    E: nom::error::ParseError<I>,
    F,
>(
    parser: F,
) -> impl Fn(I) -> IResult<I, (I, O), E>
where
    F: Fn(I) -> IResult<I, O, E>,
{
    move |input: I| {
        let i = input.clone();
        match parser(i) {
            Ok((i, o)) => {
                let index = input.offset(&i);
                Ok((i, (input.slice(..index), o)))
            }
            Err(e) => Err(e),
        }
    }
}

/// This is very similar to nom's `map_res`, but creates a
/// `nom::Err::Failure` instead.
fn map_fail<'a, R1, R2>(
    first: impl Fn(&'a str) -> IResult<&'a str, R1>,
    second: impl FnOnce(R1) -> Result<R2, Error>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, R2> {
    move |i| {
        let (remaining, value) = first(i)?;

        match second(value) {
            Ok(v) => Ok((remaining, v)),
            Err(e) => Err(nom::Err::Failure(e)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use delorean_test_helpers::approximately_equal;
    use std::convert::From;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[test]
    fn escaped_str_basic() -> Result {
        // Demonstrate how strings without any escapes are handled.
        let es = EscapedStr::from("Foo");
        assert_eq!(es.to_string(), "Foo".to_string());
        assert!(!es.is_escaped(), "There are no escaped values");
        assert!(!es.ends_with("F"));
        assert!(!es.ends_with("z"));
        assert!(!es.ends_with("zz"));
        assert!(es.ends_with("o"));
        assert!(es.ends_with("oo"));
        assert!(es.ends_with("Foo"));
        Ok(())
    }

    // TODO Add test / figure out how strings with escape strings are actually handled

    #[test]
    fn test_parsed_line_construction() -> Result {
        // This test is mostly to document how to construct one of
        // these structures.  I had this in the documentation as an
        // example, but at the time of writing, too many structures
        // needed to be declared as public to do so effectively.

        let mut tag_set: TagSet = SmallVec::new();
        tag_set.push((EscapedStr::from("host"), EscapedStr::from("A")));
        tag_set.push((EscapedStr::from("region"), EscapedStr::from("west")));

        let s = Series {
            raw_input: "cpu,host=A,region=west",
            measurement: EscapedStr::from("cpu"),
            tag_set: Some(tag_set),
        };

        let mut field_set: FieldSet = SmallVec::new();
        field_set.push((EscapedStr::from("usage_system"), FieldValue::F64(64.2)));

        let _l = ParsedLine {
            series: s,
            field_set,
            timestamp: Some(1_590_488_773_254_420_000),
        };

        Ok(())
    }

    #[test]
    fn parse_line_basic() -> Result {
        // Demonstrate parsing a line of data and retrieving the results
        let input = "cpu,host=A,region=west usage_system=64.2 1590488773254420000";
        let (next_slice, parsed_line) = parse_line(input).expect("parse failed");

        // the entire input was consumed
        assert_eq!(next_slice.len(), 0);

        // The series were parsed correctly
        assert_eq!(parsed_line.series.raw_input, "cpu,host=A,region=west");
        assert_eq!(parsed_line.series.measurement, "cpu", "{:?}", parsed_line);

        let tag_set = parsed_line.series.tag_set.expect("had no tags");
        assert_eq!(tag_set.len(), 2);
        assert_eq!(
            (EscapedStr::from("host"), EscapedStr::from("A")),
            tag_set[0]
        );
        assert_eq!(
            (EscapedStr::from("region"), EscapedStr::from("west")),
            tag_set[1]
        );

        // The fields were parsed correctly
        let field_set = parsed_line.field_set;
        assert_eq!(field_set.len(), 1);
        assert_eq!(EscapedStr::from("usage_system"), field_set[0].0);
        assert_eq!(FieldValue::F64(64.2), field_set[0].1);

        assert_eq!(
            parsed_line.timestamp.expect("had timestamp"),
            1_590_488_773_254_420_000
        );

        Ok(())
    }

    // TODO: test that only one line is consumed

    // TODO: test lines that have no timestamp, no tags, no fields, etc

    #[test]
    fn parse_line_empty() -> Result {
        let input = "";
        // can't parse this line
        assert!(matches!(parse_line(input), Err(_)));
        Ok(())
    }

    #[test]
    fn parse_no_fields() -> Result {
        let input = "foo 1234";
        let vals = parse(input);

        assert!(matches!(vals, Err(super::Error::FieldSetMissing)));

        Ok(())
    }

    #[test]
    fn parse_single_field_integer() -> Result {
        let input = "foo asdf=23i 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert_eq!(vals[0].i64_value().unwrap(), 23);

        Ok(())
    }

    #[test]
    fn parse_single_field_float_no_decimal() -> Result {
        let input = "foo asdf=44 546";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 546);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 44.0));

        Ok(())
    }

    #[test]
    fn parse_single_field_float_with_decimal() -> Result {
        let input = "foo asdf=3.74 123";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 123);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 3.74));

        Ok(())
    }

    #[test]
    fn parse_two_fields_integer() -> Result {
        let input = "foo asdf=23i,bar=5i 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert_eq!(vals[0].i64_value().unwrap(), 23);

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert_eq!(vals[1].i64_value().unwrap(), 5);

        Ok(())
    }

    #[test]
    fn parse_two_fields_float() -> Result {
        let input = "foo asdf=23.1,bar=5 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert!(approximately_equal(vals[1].f64_value().unwrap(), 5.0));

        Ok(())
    }

    #[test]
    fn parse_mixed_float_and_integer() -> Result {
        let input = "foo asdf=23.1,bar=5i 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert_eq!(vals[1].i64_value().unwrap(), 5);

        Ok(())
    }

    #[test]
    fn parse_negative_integer() -> Result {
        let input = "m0 field=-1i 99";
        let vals = parse(input)?;

        assert_eq!(vals.len(), 1);
        assert_eq!(vals[0].i64_value().unwrap(), -1);

        Ok(())
    }

    #[test]
    fn parse_negative_float() -> Result {
        let input = "m0 field2=-1 99";
        let vals = parse(input)?;

        assert_eq!(vals.len(), 1);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), -1.0));

        Ok(())
    }

    #[test]
    fn parse_out_of_range_integer() -> Result {
        let input = "m0 field=99999999999999999999999999999999i 99";
        let parsed = parse(input);

        assert!(
            matches!(parsed, Err(super::Error::IntegerValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        Ok(())
    }

    #[test]
    fn parse_out_of_range_float() -> Result {
        let input = format!("m0 field={val}.{val} 99", val = "9".repeat(200));
        let parsed = parse(&input);

        assert!(
            matches!(parsed, Err(super::Error::FloatValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        Ok(())
    }

    #[test]
    fn parse_tag_set_included_in_series() -> Result {
        let input = "foo,tag1=1,tag2=2 value=1 123";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo,tag1=1,tag2=2\tvalue");

        Ok(())
    }

    #[test]
    fn parse_tag_set_unsorted() -> Result {
        let input = "foo,tag2=2,tag1=1 value=1 123";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo,tag1=1,tag2=2\tvalue");

        Ok(())
    }

    #[test]
    fn parse_tag_set_duplicate_tags() -> Result {
        let input = "foo,tag=1,tag=2 value=1 123";
        let err = parse(input).expect_err("Parsing duplicate tags should fail");

        assert_eq!(
            err.to_string(),
            r#"Must not contain duplicate tags, but "tag" was repeated"#
        );

        Ok(())
    }

    #[test]
    fn parse_multiple_lines_become_multiple_points() -> Result {
        let input = r#"foo value1=1i 123
foo value2=2i 123"#;
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tvalue1");
        assert_eq!(vals[0].time(), 123);
        assert_eq!(vals[0].i64_value().unwrap(), 1);

        assert_eq!(vals[1].series(), "foo\tvalue2");
        assert_eq!(vals[1].time(), 123);
        assert_eq!(vals[1].i64_value().unwrap(), 2);

        Ok(())
    }

    #[test]
    fn parse_without_a_timestamp_uses_the_default() -> Result {
        let input = r#"foo value1=1i"#;
        let vals = parse_full(input, 555)?;

        assert_eq!(vals[0].series(), "foo\tvalue1");
        assert_eq!(vals[0].time(), 555);
        assert_eq!(vals[0].i64_value().unwrap(), 1);

        Ok(())
    }

    #[test]
    fn parse_negative_timestamp() -> Result {
        let input = r#"foo value1=1i -123"#;
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tvalue1");
        assert_eq!(vals[0].time(), -123);
        assert_eq!(vals[0].i64_value().unwrap(), 1);

        Ok(())
    }

    #[test]
    fn parse_out_of_range_timestamp() -> Result {
        let input = "m0 field=1i 99999999999999999999999999999999";
        let parsed = parse(input);

        assert!(
            matches!(parsed, Err(super::Error::TimestampValueInvalid { .. })),
            "Wrong error: {:?}",
            parsed,
        );

        Ok(())
    }

    #[test]
    fn parse_blank_lines_are_ignored() -> Result {
        let input = "\n\n\n";
        let vals = parse(input)?;

        assert!(vals.is_empty());

        Ok(())
    }

    #[test]
    fn parse_commented_lines_are_ignored() -> Result {
        let input = "# comment";
        let vals = parse(input)?;

        assert!(vals.is_empty());

        Ok(())
    }

    #[test]
    fn parse_multiple_whitespace_between_elements_is_allowed() -> Result {
        let input = "  measurement  a=1i  123  ";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "measurement\ta");
        assert_eq!(vals[0].time(), 123);
        assert_eq!(vals[0].i64_value().unwrap(), 1);

        Ok(())
    }

    macro_rules! assert_fully_parsed {
        ($parse_result:expr, $output:expr $(,)?) => {{
            let (remaining, parsed) = $parse_result?;

            assert!(
                remaining.is_empty(),
                "Some input remained to be parsed: {:?}",
                remaining,
            );
            assert_eq!(parsed, $output, "Did not parse the expected output");

            Ok(())
        }};
    }

    #[test]
    fn measurement_allows_escaping_comma() -> Result {
        assert_fully_parsed!(measurement(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn measurement_allows_escaping_space() -> Result {
        assert_fully_parsed!(measurement(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn measurement_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(measurement(r#"\\wea\\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn measurement_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(measurement(r#"\wea\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn measurement_allows_literal_newline_as_unknown_escape() -> Result {
        assert_fully_parsed!(
            measurement(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        )
    }

    #[test]
    fn measurement_disallows_literal_newline() -> Result {
        let (remaining, parsed) = measurement(
            r#"weat
her"#,
        )?;
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");

        Ok(())
    }

    #[test]
    fn measurement_disallows_ending_in_backslash() -> Result {
        let parsed = measurement(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));

        Ok(())
    }

    #[test]
    fn tag_key_allows_escaping_comma() -> Result {
        assert_fully_parsed!(tag_key(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn tag_key_allows_escaping_equal() -> Result {
        assert_fully_parsed!(tag_key(r#"wea\=ther"#), r#"wea=ther"#)
    }

    #[test]
    fn tag_key_allows_escaping_space() -> Result {
        assert_fully_parsed!(tag_key(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn tag_key_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(tag_key(r#"\\wea\\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn tag_key_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(tag_key(r#"\wea\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn tag_key_allows_literal_newline_as_unknown_escape() -> Result {
        assert_fully_parsed!(
            tag_key(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        )
    }

    #[test]
    fn tag_key_disallows_literal_newline() -> Result {
        let (remaining, parsed) = tag_key(
            r#"weat
her"#,
        )?;
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");

        Ok(())
    }

    #[test]
    fn tag_key_disallows_ending_in_backslash() -> Result {
        let parsed = tag_key(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));

        Ok(())
    }

    #[test]
    fn tag_value_allows_escaping_comma() -> Result {
        assert_fully_parsed!(tag_value(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn tag_value_allows_escaping_equal() -> Result {
        assert_fully_parsed!(tag_value(r#"wea\=ther"#), r#"wea=ther"#)
    }

    #[test]
    fn tag_value_allows_escaping_space() -> Result {
        assert_fully_parsed!(tag_value(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn tag_value_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(tag_value(r#"\\wea\\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn tag_value_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(tag_value(r#"\wea\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn tag_value_allows_literal_newline_as_unknown_escape() -> Result {
        assert_fully_parsed!(
            tag_value(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        )
    }

    #[test]
    fn tag_value_disallows_literal_newline() -> Result {
        let (remaining, parsed) = tag_value(
            r#"weat
her"#,
        )?;
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");

        Ok(())
    }

    #[test]
    fn tag_value_disallows_ending_in_backslash() -> Result {
        let parsed = tag_value(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));

        Ok(())
    }

    #[test]
    fn field_key_allows_escaping_comma() -> Result {
        assert_fully_parsed!(field_key(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn field_key_allows_escaping_equal() -> Result {
        assert_fully_parsed!(field_key(r#"wea\=ther"#), r#"wea=ther"#)
    }

    #[test]
    fn field_key_allows_escaping_space() -> Result {
        assert_fully_parsed!(field_key(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn field_key_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(field_key(r#"\\wea\\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn field_key_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(field_key(r#"\wea\ther"#), r#"\wea\ther"#)
    }

    #[test]
    fn field_key_allows_literal_newline_as_unknown_escape() -> Result {
        assert_fully_parsed!(
            field_key(
                r#"weat\
her"#
            ),
            "weat\\\nher",
        )
    }

    #[test]
    fn field_key_disallows_literal_newline() -> Result {
        let (remaining, parsed) = field_key(
            r#"weat
her"#,
        )?;
        assert_eq!(parsed, "weat");
        assert_eq!(remaining, "\nher");

        Ok(())
    }

    #[test]
    fn field_key_disallows_ending_in_backslash() -> Result {
        let parsed = field_key(r#"weather\"#);
        assert!(matches!(
            parsed,
            Err(nom::Err::Failure(super::Error::EndsWithBackslash))
        ));

        Ok(())
    }

    #[test]
    fn parse_no_time() -> Result {
        let input = "foo asdf=23.1,bar=5i";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        // Just test that we haven't traveled back in time
        assert!(vals[0].time() > 1_583_443_428_970_606_000);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert!(vals[1].time() > 1_583_443_428_970_606_000);
        assert_eq!(vals[1].i64_value().unwrap(), 5);

        Ok(())
    }

    #[test]
    fn parse_measurement_but_no_time() -> Result {
        let input = "m0,tag0=value1 v0=1";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "m0,tag0=value1\tv0");
        // Just test that we haven't traveled back in time
        assert!(vals[0].time() > 1_583_443_428_970_606_000);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 1.0));

        Ok(())
    }

    #[test]
    fn index_pairs() {
        let p = Point {
            series: "cpu,host=A,region=west\tusage_system".to_string(),
            series_id: None,
            value: 0,
            time: 0,
        };
        let pairs = p.index_pairs();
        assert_eq!(
            pairs,
            vec![
                Pair {
                    key: "_m".to_string(),
                    value: "cpu".to_string()
                },
                Pair {
                    key: "host".to_string(),
                    value: "A".to_string()
                },
                Pair {
                    key: "region".to_string(),
                    value: "west".to_string()
                },
                Pair {
                    key: "_f".to_string(),
                    value: "usage_system".to_string()
                },
            ]
        );
    }
}
