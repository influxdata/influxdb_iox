#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use crate::payload::{Header, Payload};
use crossbeam_epoch::{self as epoch, Atomic, Owned};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::ffi::OsStr;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, ErrorKind, Seek, SeekFrom},
    path::PathBuf,
    sync::atomic::{AtomicU16, AtomicU64, Ordering::*},
};

mod io;
pub mod payload;

const U48_MAX: u64 = (1 << 48) - 1;
const U47_MAX: u64 = (1 << 47) - 1;
static WAL_FILENAME_PATTERN: Lazy<Regex> = Lazy::new(|| {
    let pattern = format!(r"^{}([0-9a-f]{{8}})$", Wal::FILE_PREFIX);
    Regex::new(&pattern).expect("Hardcoded regex should be valid")
});

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Opaque public `Error` type
#[derive(Debug, Snafu)]
pub struct Error(InternalError);

#[derive(Debug, Snafu)]
enum InternalError {
    PayloadError {
        source: crate::payload::PayloadError,
    },

    UnableToWritePayload {
        source: crate::io::IoError,
        path: PathBuf,
    },

    UnableToCreateWal {
        source: std::io::Error,
        path: PathBuf,
    },

    UnableToSyncWal {
        source: std::io::Error,
        path: PathBuf,
    },

    UnableToOpenWal {
        source: std::io::Error,
        path: PathBuf,
    },

    UnableToReadWalDirectory {
        source: std::io::Error,
        path: PathBuf,
    },

    UnableToReadWal {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Error serializing metadata: {}", source))]
    SerializeMetadata {
        source: serde_json::error::Error,
    },

    #[snafu(display("Error writing metadata to '{:?}': {}", metadata_path, source))]
    WritingMetadata {
        source: std::io::Error,
        metadata_path: PathBuf,
    },

    EntireWalIsEmpty {
        path: PathBuf,
    },
}

/// SequenceNumber is a u64 monotonically increasing number for each WAL entry.
/// Most significant 16 bits are the WAL file id.
/// Lest significant 48 bits are the offset within the WAL file.
#[derive(Copy, Clone, Debug)]
pub struct SequenceNumber {
    wal_id: u16,
    offset: u64,
}

impl SequenceNumber {
    pub fn new(wal_id: u16, offset: u64) -> Self {
        Self { wal_id, offset }
    }

    pub fn as_u64(&self) -> u64 {
        let shifted = (self.wal_id as u64) << 48;
        shifted | self.offset.min(U48_MAX)
    }
}

/// Metadata about this particular WAL
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct WalMetadata {
    pub format: WalFormat,
}

impl Default for WalMetadata {
    fn default() -> Self {
        Self {
            format: WalFormat::FlatBuffers,
        }
    }
}

/// Supported WAL formats that can be restored
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum WalFormat {
    FlatBuffers,
    #[serde(other)]
    Unknown,
}

#[derive(Debug)]
struct WalFile {
    id: u16,
    file: File,
    size: AtomicU64,
}

impl WalFile {
    pub fn create(root: &PathBuf, id: u16) -> Result<Self> {
        let file_path = Self::id_to_path(root, id);
        Ok(Self {
            id,
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&file_path)
                .context(UnableToCreateWal { path: file_path })?,
            size: AtomicU64::new(0),
        })
    }

    pub fn open(root: &PathBuf, id: u16) -> Result<Self> {
        let file_path = Self::id_to_path(root, id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .context(UnableToOpenWal {
                path: file_path.clone(),
            })?;
        Ok(Self {
            id,
            size: AtomicU64::new(
                file.metadata()
                    .context(UnableToOpenWal { path: file_path })?
                    .len(),
            ),
            file,
        })
    }

    pub fn id_to_path(root: &PathBuf, id: u16) -> PathBuf {
        let mut path = root.join(format!("{}{:08x}", Wal::FILE_PREFIX, id));
        path.set_extension(Wal::FILE_EXTENSION);
        path
    }

    pub fn id_from_path(path: &PathBuf) -> Option<u16> {
        path.file_stem().and_then(|file_stem| {
            let file_stem = file_stem.to_string_lossy();
            u16::from_str_radix(
                WAL_FILENAME_PATTERN.captures(&file_stem)?.get(1)?.as_str(),
                16,
            )
            .ok()
        })
    }
}

#[derive(Debug, Copy, Clone)]
pub struct WalOptions {
    rollover_size: u64,
    sync_writes: bool,
}

impl WalOptions {
    pub fn rollover_size(mut self, rollover_size: u64) -> Self {
        self.rollover_size = rollover_size.min(Wal::MAX_FILE_ROLLOVER_SIZE_BYTES);
        self
    }

    pub fn sync_writes(mut self, sync_writes: bool) -> Self {
        self.sync_writes = sync_writes;
        self
    }
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            rollover_size: Wal::DEFAULT_FILE_ROLLOVER_SIZE_BYTES,
            sync_writes: true,
        }
    }
}

#[derive(Debug)]
pub struct Wal {
    root: PathBuf,
    active: Atomic<WalFile>,
    total_size: AtomicU64,
    next_id: AtomicU16,
    metadata: WalMetadata,
    options: WalOptions,
}

impl Wal {
    /// The default size to create new WAL files at. Currently 10GiB.
    pub const DEFAULT_FILE_ROLLOVER_SIZE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
    /// The theoretical max is u48::MAX but u47::MAX is used to give room for late writes
    pub const MAX_FILE_ROLLOVER_SIZE_BYTES: u64 = U47_MAX;
    pub const FILE_PREFIX: &'static str = "wal_";
    pub const FILE_EXTENSION: &'static str = "db";
    pub const METADATA_FILE: &'static str = "wal.metadata";

    pub fn new(root: PathBuf) -> Result<Self> {
        let options = WalOptions::default();
        Self::with_options(root, options)
    }

    pub fn with_options(root: PathBuf, options: WalOptions) -> Result<Self> {
        let wal_files = WalReader::new(root.clone()).list_wal_files()?;

        let total_size = wal_files
            .iter()
            .map(|(_, path)| path.metadata().map(|m| m.len()).unwrap_or(0))
            .sum();

        let active_file = match wal_files.last() {
            Some((id, path)) => {
                if path.metadata().context(UnableToOpenWal { path })?.len() < options.rollover_size
                {
                    WalFile::open(&root, *id)?
                } else {
                    WalFile::create(&root, id + 1)?
                }
            }
            None => WalFile::create(&root, 0)?,
        };

        Ok(Self {
            root,
            next_id: AtomicU16::new(active_file.id + 1),
            active: Atomic::new(active_file),
            total_size: AtomicU64::new(total_size),
            metadata: Default::default(),
            options,
        })
    }

    pub fn append(&self, data: &[u8]) -> Result<SequenceNumber> {
        let guard = epoch::pin();
        // SAFETY: file is properly synchronized (Acquire ordering)
        // so we won't be dereferencing uninitialized data
        let active_wal = self.active.load(Acquire, &guard);
        let active_wal_ref = unsafe { active_wal.as_ref() }.expect("active file was null!");

        let payload = Payload::encode(data).context(PayloadError)?;
        let header_bytes = payload.header().as_bytes();
        let data_bytes = payload.data();
        let payload_size = payload.size() as u64;

        // Optimistically increment file size even if things could still go wrong
        let offset = active_wal_ref.size.fetch_add(payload_size, Relaxed);
        self.total_size.fetch_add(payload_size, Relaxed);
        let new_wal_size = offset + payload_size;

        io::write(&active_wal_ref.file, &header_bytes, data_bytes, offset).context(
            UnableToWritePayload {
                path: WalFile::id_to_path(&self.root, active_wal_ref.id),
            },
        )?;

        if self.options.sync_writes {
            active_wal_ref.file.sync_all().context(UnableToSyncWal {
                path: WalFile::id_to_path(&self.root, active_wal_ref.id),
            })?;
        }

        // We are writing past the `file_rollover_size`, let's make sure the file is being rolled over
        if new_wal_size > self.options.rollover_size {
            let already_marked = self.active.fetch_or(1, Relaxed, &guard).tag() == 1;
            if !already_marked {
                // We were the ones who marked the active file so we will now roll it over
                let new_id = self.next_id.fetch_add(1, Relaxed);
                let new_active_wal = Owned::new(WalFile::create(&self.root, new_id)?);
                // This could be a store in theory but for some practical correctness let's just use a CAS with a panic
                self.active
                    .compare_and_set(active_wal.with_tag(1), new_active_wal, Release, &guard)
                    .expect("CAS on active wal failed!");
                // SAFETY: active_wal has been successfully replaced and is no longer reachable.
                // Once the current epoch is over it's impossible to get a ref to active_wal
                // making this safe to destroy after this epoch is over.
                unsafe { guard.defer_destroy(active_wal) };
            }
        }

        Ok(SequenceNumber::new(active_wal_ref.id, offset))
    }

    pub async fn write_metadata(&self) -> Result<()> {
        let metadata_path = self.root.join(Self::METADATA_FILE);
        Ok(tokio::fs::write(
            &metadata_path,
            serde_json::to_string(&self.metadata).context(SerializeMetadata)?,
        )
        .await
        .context(WritingMetadata {
            metadata_path: &metadata_path,
        })?)
    }

    pub fn reader(&self) -> WalReader {
        WalReader::new(self.root.clone())
    }
}

#[derive(Debug)]
pub struct WalReader {
    root: PathBuf,
}

impl WalReader {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn list_wal_files(&self) -> Result<Vec<(u16, PathBuf)>> {
        let mut wal_files: Vec<_> = fs::read_dir(&self.root)
            .context(UnableToReadWalDirectory { path: &self.root })?
            .flatten() // Discard errors
            .map(|e| e.path())
            .filter(|path| path.extension() == Some(OsStr::new(Wal::FILE_EXTENSION)))
            .filter_map(|path| WalFile::id_from_path(&path).map(|id| (id, path)))
            .collect();

        wal_files.sort_by_key(|(id, _)| *id);

        Ok(wal_files)
    }

    pub fn read(&self, sequence: SequenceNumber) -> Result<Vec<u8>> {
        let path = WalFile::id_to_path(&self.root, sequence.wal_id);
        let mut reader = BufReader::new(WalFile::open(&self.root, sequence.wal_id)?.file);
        reader
            .seek(SeekFrom::Start(sequence.offset))
            .context(UnableToReadWal { path })?;
        Ok(Payload::decode(reader).context(PayloadError)?)
    }

    pub fn read_wal_file(&self, wal_id: u16) -> Result<impl Iterator<Item = Result<Vec<u8>>>> {
        struct Iter {
            reader: BufReader<File>,
        }

        impl Iterator for Iter {
            type Item = Result<Vec<u8>>;

            fn next(&mut self) -> Option<Self::Item> {
                use crate::payload::PayloadError;

                loop {
                    return match Payload::decode(&mut self.reader) {
                        Ok(payload) => Some(Ok(payload)),
                        Err(PayloadError::ReadEmptyPayload) => {
                            match skip_until(&mut self.reader, Header::MAGIC) {
                                Ok(_) => continue,
                                Err(_) => None,
                            }
                        }
                        Err(PayloadError::ReaderAtEof) => None,
                        Err(e) => Some(Err(Error(InternalError::PayloadError { source: e }))),
                    };
                }
            }
        }

        Ok(Iter {
            reader: BufReader::new(WalFile::open(&self.root, wal_id)?.file),
        })
    }

    pub fn read_entire_wal(&self) -> Result<impl Iterator<Item = Result<Vec<u8>>>> {
        struct Iter<T, I>
        where
            T: Iterator<Item = I>,
            I: Iterator<Item = Result<Vec<u8>>>,
        {
            wal_iters: T,
            current_iter: I,
        }

        impl<T, I> Iterator for Iter<T, I>
        where
            T: Iterator<Item = I>,
            I: Iterator<Item = Result<Vec<u8>>>,
        {
            type Item = Result<Vec<u8>>;

            fn next(&mut self) -> Option<Self::Item> {
                loop {
                    match self.current_iter.next() {
                        Some(item) => return Some(item),
                        None => self.current_iter = self.wal_iters.next()?,
                    }
                }
            }
        }

        let mut wal_iters = Vec::new();

        for (id, _) in self.list_wal_files()? {
            wal_iters.push(self.read_wal_file(id)?)
        }

        let mut wal_iters = wal_iters.into_iter();
        let current_iter = match wal_iters.next() {
            Some(iter) => iter,
            None => {
                return Err(Error(InternalError::EntireWalIsEmpty {
                    path: self.root.clone(),
                }))
            }
        };

        Ok(Iter {
            current_iter,
            wal_iters,
        })
    }
}

fn skip_until<R: BufRead + ?Sized>(r: &mut R, delim: u8) -> std::io::Result<usize> {
    let mut read = 0;
    loop {
        let (done, used) = {
            let available = match r.fill_buf() {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            match memchr::memchr(delim, available) {
                Some(i) => (true, i),
                None => (false, available.len()),
            }
        };
        r.consume(used);
        read += used;
        if done || used == 0 {
            return Ok(read);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use tempdir::TempDir;

    fn rand_vec(n: usize) -> Vec<u8> {
        (0..n).map(|_| rand::random::<u8>()).collect()
    }

    fn append_read_many<F: Fn() -> Vec<u8>>(wal: &Wal, gen: F) {
        let mut inputs = Vec::new();

        for _ in 0..1_000 {
            let bytes = gen();
            let sequence = wal.append(&bytes).unwrap();
            inputs.push((sequence, bytes));
        }

        let reader = wal.reader();
        for (sequence, bytes) in inputs {
            assert_eq!(reader.read(sequence).unwrap(), bytes)
        }
    }

    #[test]
    fn sequence_numbers() {
        assert_eq!(SequenceNumber::new(0, u64::MAX).as_u64(), U48_MAX);
        assert_eq!(SequenceNumber::new(u16::MAX, u64::MAX).as_u64(), u64::MAX);
        assert_eq!(
            SequenceNumber::new(u16::MAX, 0).as_u64(),
            (u16::MAX as u64) << 48
        );
        assert_eq!(SequenceNumber::new(1, u64::MAX).as_u64(), (1 << 49) - 1);
    }

    #[test]
    fn wal_id_conversions() {
        let path = WalFile::id_to_path(&"/".into(), u16::MAX);
        assert_eq!(WalFile::id_from_path(&path), Some(u16::MAX));
        let path = WalFile::id_to_path(&"/".into(), u16::MIN);
        assert_eq!(WalFile::id_from_path(&path), Some(u16::MIN));
    }

    #[test]
    fn append_read_many_random() {
        let temp_dir = TempDir::new("wal").unwrap();
        let wal = Wal::with_options(
            temp_dir.path().to_path_buf(),
            WalOptions::default().rollover_size(128),
        )
        .unwrap();
        append_read_many(&wal, || rand_vec(32));
    }

    #[test]
    fn append_read_many_empty() {
        let temp_dir = TempDir::new("wal").unwrap();
        let wal = Wal::with_options(
            temp_dir.path().to_path_buf(),
            WalOptions::default().rollover_size(128),
        )
        .unwrap();
        append_read_many(&wal, Vec::new);
    }

    #[test]
    #[cfg(unix)]
    fn wal_iter() {
        use std::os::unix::fs::FileExt;

        let temp_dir = TempDir::new("wal").unwrap();
        let wal = Wal::with_options(
            temp_dir.path().to_path_buf(),
            WalOptions::default().rollover_size(128),
        )
        .unwrap();

        let mut inputs = Vec::new();

        for _ in 0..1_000 {
            let file = unsafe { wal.active.load(Acquire, epoch::unprotected()).deref() };
            let offset = file.size.fetch_add(Header::LEN as u64, Relaxed);
            file.file.write_all_at(&[0u8; Header::LEN], offset).unwrap();

            let bytes = if rand::thread_rng().gen_bool(1.0 / 3.0) {
                Vec::new()
            } else {
                rand_vec(16)
            };

            let sequence = wal.append(&bytes).unwrap();
            inputs.push((sequence, bytes));

            let file = unsafe { wal.active.load(Acquire, epoch::unprotected()).deref() };
            let offset = file.size.fetch_add(Header::LEN as u64, Relaxed);
            file.file.write_all_at(&[0u8; Header::LEN], offset).unwrap();
        }

        let wal_iter = wal.reader().read_entire_wal().unwrap();
        let mut input_iter = inputs.into_iter();
        for payload in wal_iter {
            let payload = payload.unwrap();
            let (sequence, bytes) = input_iter.next().unwrap();
            assert_eq!(payload, bytes);
            assert_eq!(wal.reader().read(sequence).unwrap(), bytes);
        }
    }
}
