use arrayref::array_ref;
use crc32fast::Hasher;
use snafu::{ensure, ResultExt, Snafu};
use std::{
    convert::TryFrom,
    io::{ErrorKind, Read},
    mem::size_of,
};

type Result<T, E = PayloadError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum PayloadError {
    ChunkSizeTooLarge {
        source: std::num::TryFromIntError,
        actual: usize,
    },

    UnableToCompressData {
        source: snap::Error,
    },

    UnableToDecompressData {
        source: snap::Error,
    },

    UnableToReadHeader {
        source: std::io::Error,
    },

    UnableToReadData {
        source: std::io::Error,
    },

    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },

    ReaderAtEof,

    ReadEmptyPayload,
}

/// A single write to a WAL file
#[derive(Debug)]
pub struct Payload {
    header: Header,
    bytes: Vec<u8>,
}

impl Payload {
    /// Creates a payload, compresses the data, and computes its CRC.
    pub fn encode(uncompressed_data: &[u8]) -> Result<Self> {
        // Only designed to support chunks up to `u32::max` bytes long.
        let uncompressed_len = uncompressed_data.len();
        let _ = u32::try_from(uncompressed_len).context(ChunkSizeTooLarge {
            actual: uncompressed_len,
        })?;

        let mut compressed_data =
            vec![0u8; Header::LEN + snap::raw::max_compress_len(uncompressed_data.len())];
        let mut encoder = snap::raw::Encoder::new();
        let written = encoder
            .compress(&uncompressed_data, &mut compressed_data[Header::LEN..])
            .context(UnableToCompressData)?;
        compressed_data.truncate(Header::LEN + written);
        let actual_compressed_len = compressed_data.len() - Header::LEN;
        let actual_compressed_len =
            u32::try_from(actual_compressed_len).context(ChunkSizeTooLarge {
                actual: actual_compressed_len,
            })?;

        // Check total payload size fits in u32
        let payload_size = compressed_data.len();
        let _ = u32::try_from(payload_size).context(ChunkSizeTooLarge {
            actual: payload_size,
        })?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data[Header::LEN..]);
        let checksum = hasher.finalize();

        let header = Header {
            magic: Header::MAGIC,
            checksum,
            len: actual_compressed_len,
        };

        compressed_data[..Header::LEN].copy_from_slice(&header.as_bytes());

        Ok(Self {
            header,
            bytes: compressed_data,
        })
    }

    pub fn decode<R: Read>(mut reader: R) -> Result<Vec<u8>> {
        let mut header_bytes = [0u8; Header::LEN];
        Self::handle_read_error(reader.read_exact(&mut header_bytes))?;

        let header = Header::from_bytes(header_bytes);
        ensure!(header.magic != 0, ReadEmptyPayload);

        let len =
            usize::try_from(header.len).expect("Only designed to run on 32-bit systems or higher");

        let mut compressed_data = vec![0; len];

        Self::handle_read_error(reader.read_exact(&mut compressed_data))?;

        let mut hasher = Hasher::new();
        hasher.update(&compressed_data);
        let actual_checksum = hasher.finalize();

        ensure!(
            header.checksum == actual_checksum,
            ChecksumMismatch {
                expected: header.checksum,
                actual: actual_checksum
            }
        );

        let mut decoder = snap::raw::Decoder::new();
        let data = decoder
            .decompress_vec(&compressed_data)
            .context(UnableToDecompressData)?;

        Ok(data)
    }

    fn handle_read_error<T>(result: Result<T, std::io::Error>) -> Result<T> {
        match result {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Err(PayloadError::ReaderAtEof),
            _ => result.context(UnableToReadData),
        }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn header_bytes(&self) -> &[u8] {
        &self.bytes[..Header::LEN]
    }

    pub fn data_bytes(&self) -> &[u8] {
        &self.bytes[Header::LEN..]
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn size(&self) -> u32 {
        self.bytes.len() as u32
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Header {
    magic: u8,
    checksum: u32,
    len: u32,
}

impl Header {
    pub const LEN: usize = (size_of::<u8>() + size_of::<u32>() + size_of::<u32>()) as usize;
    pub const MAGIC: u8 = u8::MAX;

    pub fn from_bytes(bytes: [u8; Self::LEN]) -> Self {
        Self {
            magic: u8::from_le_bytes(*array_ref![bytes, 0, 1]),
            checksum: u32::from_le_bytes(*array_ref![bytes, 1, 4]),
            len: u32::from_le_bytes(*array_ref![bytes, 5, 4]),
        }
    }

    pub fn as_bytes(&self) -> [u8; Self::LEN] {
        let mut bytes = [0u8; Self::LEN];
        bytes[0..1].copy_from_slice(&self.magic.to_le_bytes());
        bytes[1..5].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[5..9].copy_from_slice(&self.len.to_le_bytes());
        bytes
    }
}
