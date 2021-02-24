//! Output formatting utilities for query endpoint

use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::io::{BufWriter, Read, Seek, Write};

use serde_json::Value;

use arrow_deps::arrow::{
    self, csv::WriterBuilder, error::ArrowError, json::writer::record_batches_to_json_rows,
    record_batch::RecordBatch,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Arrow pretty printing error: {}", source))]
    PrettyArrow { source: ArrowError },

    #[snafu(display("Arrow csv printing error: {}", source))]
    CsvArrow { source: ArrowError },

    #[snafu(display("Arrow json printing error: {}", source))]
    JsonArrow { source: ArrowError },

    #[snafu(display("Json conversion error: {}", source))]
    JsonConversion { source: serde_json::Error },

    #[snafu(display("IO error during Json conversion: {}", source))]
    JsonWrite { source: std::io::Error },

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
    /// Return the content type of the relevant format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Pretty => "text/plain",
            Self::CSV => "text/csv",
            Self::JSON => "application/json",
        }
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
    /// Example (newline added for clarity):
    /// ```text
    /// [
    ///  {"bottom_degrees":50.4,"location":"santa_monica","state":"CA","surface_degrees":65.2,"time":1568756160},
    ///  {"location":"Boston","state":"MA","surface_degrees":50.2,"time":1568756160}
    /// ]
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

/// Writes out well formed JSON arays in a streaming fashion
///
/// [{"foo": "bar"}, {"foo": "baz"}]
///
/// This is based on the arrow JSON writer (json::writer::Writer)
///
/// TODO contribute this back to arrow) alongside  (or maybe have it be an
/// option)

struct JsonArrayWriter<W>
where
    W: Write,
{
    started: bool,
    finished: bool,
    writer: W,
}

impl<W> JsonArrayWriter<W>
where
    W: Write,
{
    fn new(writer: W) -> Self {
        Self {
            writer,
            started: false,
            finished: false,
        }
    }

    pub fn write_row(&mut self, row: &Value) -> Result<()> {
        if !self.started {
            self.writer.write_all(b"[").context(JsonWrite)?;
            self.started = true;
        } else {
            self.writer.write_all(b",").context(JsonWrite)?;
        }
        self.writer
            .write_all(&serde_json::to_vec(row).context(JsonConversion)?)
            .context(JsonWrite)?;
        Ok(())
    }

    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for row in record_batches_to_json_rows(batches) {
            self.write_row(&Value::Object(row))?;
        }
        Ok(())
    }

    /// tell the writer there are is no more data to come so it can
    /// write the final `'['`
    pub fn finish(&mut self) -> Result<()> {
        if self.started && !self.finished {
            self.writer.write_all(b"]").context(JsonWrite)?;
            self.finished = true;
        }
        Ok(())
    }
}

fn batches_to_json(batches: &[RecordBatch]) -> Result<String> {
    let mut tmp = tempfile::tempfile().context(TempFileCreation)?;

    let tmp_writer = BufWriter::new(tmp.try_clone().context(TempFile {
        note: "cloning filehandle",
    })?);

    let mut writer = JsonArrayWriter::new(tmp_writer);
    writer.write_batches(batches)?;
    writer.finish()?;

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
