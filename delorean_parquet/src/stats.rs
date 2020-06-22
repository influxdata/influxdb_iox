//! Provide storage statistics for parquet files
use std::collections::BTreeMap;
use std::io::{Read, Seek};

use log::debug;
use parquet::file::reader::{FileReader, SerializedFileReader};

use delorean_table::stats::{ColumnStats, ColumnStatsBuilder};

use crate::{
    error::{Error, Result},
    metadata::data_type_from_parquet_type,
    InputReaderAdapter,
};

/// Calculate storage statistics for a particular parquet file that can
/// be read from `input`, with a total size of `input_size` byes
///
/// Returns a Vec of ColumnStats, one for each column in the input
pub fn col_stats<R: 'static>(input: R, input_size: u64) -> Result<Vec<ColumnStats>>
where
    R: Read + Seek,
{
    let input_adapter = InputReaderAdapter::new(input, input_size);

    let reader =
        SerializedFileReader::new(input_adapter).map_err(|e| Error::ParquetLibraryError {
            message: String::from("Creating parquet reader"),
            source: e,
        })?;

    let mut stats_builders = BTreeMap::new();

    let parquet_metadata = reader.metadata();
    for (rg_idx, rg_metadata) in parquet_metadata.row_groups().iter().enumerate() {
        debug!(
            "Looking at Row Group [{}] (total uncompressed byte size {})",
            rg_idx,
            rg_metadata.total_byte_size()
        );

        for (cc_idx, cc_metadata) in rg_metadata.columns().iter().enumerate() {
            let col_path = cc_metadata.column_path();
            let builder = stats_builders.entry(col_path.string()).or_insert_with(|| {
                let data_type = data_type_from_parquet_type(cc_metadata.column_type());
                ColumnStatsBuilder::new(col_path.string(), cc_idx, data_type)
            });

            use std::convert::TryInto;

            let compression_string = format!(
                "{:?}; {:?}",
                cc_metadata.encodings(),
                cc_metadata.compression()
            );

            builder
                .compression(&compression_string)
                .add_rows(
                    cc_metadata
                        .num_values()
                        .try_into()
                        .expect("positive number of values"),
                )
                .add_compressed_bytes(
                    cc_metadata
                        .compressed_size()
                        .try_into()
                        .expect("positive compressed size"),
                )
                .add_uncompressed_bytes(
                    cc_metadata
                        .uncompressed_size()
                        .try_into()
                        .expect("positive uncompressed size"),
                );
        }
    }

    // now, marshal up all the results
    let mut v = stats_builders
        .into_iter()
        .map(|(_k, b)| b.build())
        .collect::<Vec<_>>();

    // ensure output is not sorted by column name, but by column index
    v.sort_by_key(|stats| stats.column_index);

    Ok(v)
}
