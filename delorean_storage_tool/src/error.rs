use snafu::Snafu;

use delorean::storage::StorageError;
use delorean_parquet::writer::Error as DeloreanTableWriterError;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"IO Error: {} ({})"#, message, source))]
    IO {
        message: String,
        source: std::io::Error,
    },
    #[snafu(display(r#"Error reading TSM data: ({})"#, source))]
    TSM { source: StorageError },
    #[snafu(display(r#"Error parsing data: ({})"#, source))]
    Parsing { source: delorean_line_parser::Error },
    #[snafu(display(r#"Error converting data: ({})"#, source))]
    Conversion { source: delorean_ingest::Error },
    #[snafu(display(r#"Error writing table data: ({})"#, source))]
    Writing { source: DeloreanTableWriterError },
    #[snafu(display(r#"Can not determine input file type: ({})"#, message))]
    UnknownInputType { message: String },
    #[snafu(display(r#"Not yet implemented: ({})"#, message))]
    NotImplemented { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<delorean_line_parser::Error> for Error {
    fn from(other: delorean_line_parser::Error) -> Self {
        Error::Parsing { source: other }
    }
}

impl From<delorean_ingest::Error> for Error {
    fn from(other: delorean_ingest::Error) -> Self {
        Error::Conversion { source: other }
    }
}

impl From<DeloreanTableWriterError> for Error {
    fn from(other: DeloreanTableWriterError) -> Self {
        Error::Writing { source: other }
    }
}

impl From<delorean::storage::StorageError> for Error {
    fn from(other: delorean::storage::StorageError) -> Self {
        Error::TSM { source: other }
    }
}
