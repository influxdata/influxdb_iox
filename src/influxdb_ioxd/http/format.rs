//! Output formatting utilities for query endpoint

use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::io::{BufWriter, Read, Seek};

use arrow_deps::arrow::{self, csv::WriterBuilder, error::ArrowError, record_batch::RecordBatch};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Arrow pretty printing error {}", source))]
    PrettyArrow { source: ArrowError },

    #[snafu(display("Arrow csv printing error {}", source))]
    CsvArrow { source: ArrowError },

    #[snafu(display("Arrow json printing error {}", source))]
    JsonArrow { source: ArrowError },

    #[snafu(display("Error creating temp file: {}", source))]
    TempFileCreation { source: std::io::Error },

    #[snafu(display("Temp file error while {}: {}", note, source))]
    TempFile {
        note: String,
        source: std::io::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Deserialize, Debug, Copy, Clone, PartialEq)]
/// Requested output format for the query endpoint
pub enum QueryOutputFormat {
    /// Arrow pretty printer format (default)
    #[serde(rename = "pretty")]
    Pretty,
    /// Comma separated values
    #[serde(rename = "csv")]
    CSV,
    /// Arrow JSON format
    #[serde(rename = "json")]
    JSON,
}

impl Default for QueryOutputFormat {
    fn default() -> Self {
        Self::Pretty
    }
}

impl QueryOutputFormat {
    /// Format the [`RecordBatch`]es into a String in one of the following
    /// formats:
    ///
    /// Pretty:
    /// ```text
    /// +----------------+--------------+-------+-----------------+------------+
    /// | bottom_degrees | location     | state | surface_degrees | time       |
    /// +----------------+--------------+-------+-----------------+------------+
    /// | 50.4           | santa_monica | CA    | 65.2            | 1568756160 |
    /// +----------------+--------------+-------+-----------------+------------+
    /// ```
    ///
    /// CSV:
    /// ```text
    /// bottom_degrees,location,state,surface_degrees,time
    /// 50.4,santa_monica,CA,65.2,1568756160
    /// ```
    ///
    /// JSON:
    ///
    /// Example:
    /// ```text
    /// {"bottom_degrees":50.4,"location":"santa_monica","state":"CA","surface_degrees":65.2,"time":1568756160}
    /// {"location":"Boston","state":"MA","surface_degrees":50.2,"time":1568756160}
    /// ```
    pub fn format(&self, batches: &[RecordBatch]) -> Result<String> {
        match self {
            Self::Pretty => batches_to_pretty(&batches),
            Self::CSV => batches_to_csv(&batches),
            Self::JSON => batches_to_json(&batches),
        }
    }
}

fn batches_to_pretty(batches: &[RecordBatch]) -> Result<String> {
    arrow::util::pretty::pretty_format_batches(batches).context(PrettyArrow)
}

fn batches_to_csv(batches: &[RecordBatch]) -> Result<String> {
    let mut tmp = tempfile::tempfile().context(TempFileCreation)?;

    let tmp_writer = BufWriter::new(tmp.try_clone().context(TempFile {
        note: "cloning filehandle",
    })?);

    let mut writer = WriterBuilder::new().has_headers(true).build(tmp_writer);

    for batch in batches {
        writer.write(batch).context(CsvArrow)?;
    }
    // drop the write to ensure we have flushed all data
    std::mem::drop(writer);

    tmp.seek(std::io::SeekFrom::Start(0)).context(TempFile {
        note: "seeking to start",
    })?;

    let mut csv = String::new();
    tmp.read_to_string(&mut csv).context(TempFile {
        note: "reading as string",
    })?;

    Ok(csv)
}

fn batches_to_json(batches: &[RecordBatch]) -> Result<String> {
    let mut tmp = tempfile::tempfile().context(TempFileCreation)?;

    let tmp_writer = BufWriter::new(tmp.try_clone().context(TempFile {
        note: "cloning filehandle",
    })?);

    let mut writer = arrow::json::Writer::new(tmp_writer);
    writer.write_batches(batches).context(JsonArrow)?;

    // drop the write to ensure we have flushed all data
    std::mem::drop(writer);

    tmp.seek(std::io::SeekFrom::Start(0)).context(TempFile {
        note: "seeking to start",
    })?;

    let mut json = String::new();
    tmp.read_to_string(&mut json).context(TempFile {
        note: "reading as string",
    })?;

    Ok(json)
}
