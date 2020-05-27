#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, missing_copy_implementations)]
#![warn(missing_docs)]

//! # delorean_wal
//!
//! This crate provides a WAL tailored for delorean `Partition`s to optionally use.
//!
//! Work remaining:
//!
//! - More testing for correctness; the existing tests mostly demonstrate possible usages.
//! - Error handling

use arc_swap::ArcSwap;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Bytes, BytesMut};
use crc32fast::Hasher;
use once_cell::sync::Lazy;
use regex::Regex;
use snafu::{ensure, ResultExt, Snafu};
use std::{
    convert::TryFrom,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Seek, SeekFrom, Write},
    iter, mem, num,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

/// Opaque public `Error` type
#[derive(Debug, Snafu, Clone)]
pub struct Error(InternalError);

#[derive(Debug, Snafu, Clone)]
enum InternalError {
    UnableToReadFileMetadata {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToReadSequenceNumber {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToReadChecksum {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToReadLength {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToReadData {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    LengthMismatch {
        expected: usize,
        actual: usize,
    },

    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },

    ChunkSizeTooLarge {
        source: num::TryFromIntError,
        actual: usize,
    },

    UnableToWriteSequenceNumber {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToWriteChecksum {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToWriteLength {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToWriteData {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToSync {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
    },

    UnableToOpenFile {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        path: PathBuf,
    },

    UnableToCreateFile {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        path: PathBuf,
    },

    UnableToCopyFileContents {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        src: PathBuf,
        dst: PathBuf,
    },

    UnableToReadDirectoryContents {
        #[snafu(source(from(io::Error, Arc::new)))]
        source: Arc<io::Error>,
        path: PathBuf,
    },
}

/// A specialized `Result` for WAL-related errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Build a Wal rooted at a directory.
///
/// May take more configuration options in the future.
#[derive(Debug, Clone)]
pub struct WalBuilder {
    root: PathBuf,
    file_rollover_size: u64,
}

impl WalBuilder {
    /// The default size to create new WAL files at. Currently 10MiB.
    ///
    /// See [WalBuilder::file_rollover_size]
    pub const DEFAULT_FILE_ROLLOVER_SIZE_BYTES: u64 = 10 * 1024 * 1024;

    /// Create a new WAL rooted at the provided directory on disk.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        // TODO: Error if `root` is not a directory?
        let root = root.into();
        Self {
            root,
            file_rollover_size: Self::DEFAULT_FILE_ROLLOVER_SIZE_BYTES,
        }
    }

    /// Set the size (in bytes) of each WAL file that should prompt a file rollover when it is
    /// exceeded.
    ///
    /// File rollover happens per sync batch. If the file is underneath this file size limit at the
    /// start of a sync operation, the entire sync batch will be written to that file even if
    /// some of the entries in the batch cause the file to exceed the file size limit.
    ///
    /// See [WalBuilder::DEFAULT_FILE_ROLLOVER_SIZE_BYTES]
    pub fn file_rollover_size(mut self, file_rollover_size: u64) -> Self {
        self.file_rollover_size = file_rollover_size;
        self
    }

    /// Consume the builder and create a `Wal`.
    ///
    /// # Generics
    ///
    /// - `N` is a type that will be returned when this batch is synchronized to
    ///   disk (or attempted to). It should be cheaply clonable.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    pub fn wal<N: Clone>(self) -> Result<Wal<N>> {
        Wal::new(self.file_locator())
    }

    /// Consume the builder to get an iterator of all entries in this
    /// WAL that have been persisted to disk.
    ///
    /// Sequence numbers on the entries will be in increasing order, but if files have been
    /// modified or deleted since getting this iterator, there may be gaps in the sequence.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    pub fn entries(self) -> Result<impl Iterator<Item = Result<Entry>>> {
        Loader::load(self.file_locator())
    }

    fn file_locator(self) -> FileLocator {
        FileLocator {
            root: self.root,
            file_rollover_size: self.file_rollover_size,
        }
    }
}

/// The main WAL type to interact with.
///
/// Can be used in single-threaded, multi-threaded, and asynchronous contexts.
///
/// # Example
///
/// This demonstrates using the WAL with the Tokio asynchronous runtime.
///
/// ```
/// # fn example(root_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
/// use delorean_wal::WalBuilder;
/// use futures::{SinkExt, StreamExt, channel::mpsc};
/// use std::{io::Write, time::Duration};
/// use tokio::{task, time};
///
/// // This value should be cloned and used across multiple tasks, such as per
/// // HTTP request.
/// let wal = WalBuilder::new(root_path).wal::<mpsc::Sender<delorean_wal::Result<_>>>()?;
///
/// // We will mimic a HTTP request as a Tokio task.
/// // There can be many of these.
/// let wal_for_http_request = wal.clone();
/// tokio::spawn(async move {
///     let mut batch = wal_for_http_request.append();
///     batch.write_all(b"Pretend this came from the HTTP request").unwrap();
///
///     // The task that periodically syncs will notify us through this channel
///     // once our bytes are safely on disk.
///     let (batch_synced_tx, mut batch_synced_rx) = mpsc::channel(1);
///     batch.finalize(batch_synced_tx);
///
///     // Once this future resolves, we know the data has been written to disk
///     batch_synced_rx.next().await.unwrap().unwrap();
/// });
///
/// let wal_for_syncing = wal;
/// tokio::spawn(async move {
///     loop {
///         tokio::time::delay_for(Duration::from_millis(100)).await;
///
///         // Syncing the WAL performs **blocking IO** and we need to notify
///         // the asynchronous runtime about that.
///         let (tasks_to_notify, outcome) = task::block_in_place(|| wal_for_syncing.sync());
///
///         for mut waiting_http_task in tasks_to_notify {
///             waiting_http_task.send(outcome.clone()).await.unwrap();
///         }
///     }
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Wal<N>
where
    N: Clone,
{
    files: FileLocator,
    sequence_number: Arc<AtomicU64>,
    pending: Arc<ArcSwap<Vec<Pending<N>>>>,
    total_size: Arc<AtomicU64>,
}

impl<N> Wal<N>
where
    N: Clone,
{
    fn new(files: FileLocator) -> Result<Self> {
        let last_sequence_number = Loader::last_sequence_number(files.clone())?;
        let sequence_number = last_sequence_number.map_or(0, |last| last + 1);
        let sequence_number = Arc::new(sequence_number.into());

        let total_size = files.total_size();

        Ok(Self {
            files,
            sequence_number,
            pending: Default::default(),
            total_size: Arc::new(total_size.into()),
        })
    }

    /// A path to a file for storing arbitrary metadata about this WAL, guaranteed not to collide
    /// with the data files.
    pub fn metadata_path(&self) -> PathBuf {
        self.files.root.join("metadata")
    }

    /// Start appending data to this WAL.
    ///
    /// It is required to call `finalize` on the `Append` instance when the
    /// appending is completed, or the written data will not be synced and will
    /// be lost.
    ///
    /// # Asynchronous considerations
    ///
    /// This method may be called in an asynchronous context with no special
    /// handling.
    pub fn append(&self) -> Append<'_, N> {
        Append::new(self)
    }

    /// Total size, in bytes, of all the data in all the files in the WAL. If files are deleted
    /// from disk without deleting them through the WAL, the size won't reflect that deletion
    /// until the WAL is recreated.
    pub fn total_size(&self) -> u64 {
        self.total_size.load(Ordering::SeqCst)
    }

    /// Deletes files up to, but not including, the file that contains the entry number specified
    pub fn delete_up_to_entry(&self, entry_number: u64) -> Result<()> {
        let mut iter = self.files.existing_filenames()?.peekable();
        let hypothetical_filename = self
            .files
            .filename_starting_at_sequence_number(entry_number);

        while let Some(inner_path) = iter.next() {
            if iter.peek().map_or(false, |p| p < &hypothetical_filename) {
                // Intentionally ignore failures. Should we collect them for reporting instead?
                let _ = fs::remove_file(inner_path);
            } else {
                break;
            }
        }

        Ok(())
    }

    // TODO: Maybe a third struct?
    /// Flush all pending bytes to disk.
    ///
    /// Returns clients that can be notified that their data has been persisted.
    /// Intended to be called periodically to batch disk operations.
    ///
    /// # Asynchronous considerations
    ///
    /// This method performs blocking IO and care should be taken when using
    /// it in an asynchronous context.
    ///
    /// # Multiprocessing considerations
    ///
    /// Data corruption should not occur if this method is called
    /// multiple times concurrently, but an error might be returned or
    /// the order that data is written to disk might not correspond to
    /// your intuition.
    pub fn sync(&self) -> (Vec<N>, Result<()>) {
        // Atomically get all pending operations and start a new list.
        let pending = self.pending.swap(Default::default());
        let pending = &**pending;

        let n_pending = pending.len();
        let n_pending =
            u64::try_from(n_pending).expect("Only designed to run on 64-bit systems or lower");

        let sequence_number_base = self.sequence_number.fetch_add(n_pending, Ordering::SeqCst);

        let to_notify: Vec<_> = pending.iter().map(|p| N::clone(&p.notify)).collect();

        let outcome = (move || {
            // Get the file to write to
            let mut file = self.files.open_file_for_append(sequence_number_base)?;

            let len_before = file.metadata().context(UnableToReadFileMetadata)?.len();

            // TODO: Would a user ever wish to resume or retry some failed IO?
            for (i, pending) in pending.iter().enumerate() {
                let i = u64::try_from(i).expect("Only designed to run on 64-bit systems or lower");
                let sequence_number = sequence_number_base + i;

                let Pending {
                    checksum,
                    ref data,
                    len,
                    ..
                } = *pending;

                let header = Header {
                    sequence_number,
                    checksum,
                    len,
                };

                header.write(&mut *file)?;
                file.write_all(&data).context(UnableToWriteData)?;
            }

            let len_after = file.metadata().context(UnableToReadFileMetadata)?.len();
            file.sync_all_and_rename().context(UnableToSync)?;

            self.total_size
                .fetch_add(len_after - len_before, Ordering::SeqCst);

            Ok(())
        })();

        (to_notify, outcome)
    }

    /// # Asynchronous considerations
    ///
    /// This method may be called in an asynchronous context with no special
    /// handling.
    fn append_pending(&self, pending: Pending<N>) {
        self.pending.rcu(|p| {
            let mut p = Vec::<Pending<_>>::clone(p);
            p.push(Pending::clone(&pending));
            p
        });
    }
}

// Manages files within the WAL directory
#[derive(Debug, Clone)]
struct FileLocator {
    root: PathBuf,
    file_rollover_size: u64,
}

impl FileLocator {
    const PREFIX: &'static str = "wal_";
    const EXTENSION: &'static str = "db";
    const TEMP_EXTENSION: &'static str = "tmpdb";

    fn open_files_for_read(&self) -> Result<impl Iterator<Item = Result<Option<File>>> + '_> {
        Ok(self
            .existing_filenames()?
            .map(move |path| self.open_file_for_read(&path)))
    }

    fn total_size(&self) -> u64 {
        self.existing_filenames()
            .map(|files| {
                files
                    .map(|file| {
                        fs::metadata(file)
                            .map(|metadata| metadata.len())
                            .unwrap_or(0)
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    fn open_file_for_read(&self, path: &Path) -> Result<Option<File>> {
        let r = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path);

        match r {
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            r => r
                .map(Some)
                .context(UnableToOpenFile { path })
                .map_err(Into::into),
        }
    }

    fn open_file_for_append(&self, starting_sequence_number: u64) -> Result<SideFile> {
        // TODO: create directories?

        // Is there an existing file?
        let existing_file = self.active_filename()?;

        let final_destination = existing_file
            .filter(|existing| {
                // If there is an existing file, check its size.
                fs::metadata(&existing)
                    // Use the existing file if its size is under the file size limit.
                    .map(|metadata| metadata.len() < self.file_rollover_size)
                    .unwrap_or(false)
            })
            // If there is no file or the file is over the file size limit, start a new file.
            .unwrap_or_else(|| self.filename_starting_at_sequence_number(starting_sequence_number));

        // Temporary file for the purposes of doing an atomic-ish write
        let mut temporary_destination = final_destination.clone();
        temporary_destination.set_extension(Self::TEMP_EXTENSION);

        SideFile::new(final_destination, temporary_destination)
    }

    fn active_filename(&self) -> Result<Option<PathBuf>> {
        Ok(self.existing_filenames()?.last())
    }

    fn existing_filenames(&self) -> Result<impl Iterator<Item = PathBuf>> {
        static FILENAME_PATTERN: Lazy<Regex> = Lazy::new(|| {
            let pattern = format!(r"^{}[0-9a-f]{{16}}$", FileLocator::PREFIX);
            Regex::new(&pattern).expect("Hardcoded regex should be valid")
        });

        let mut wal_paths: Vec<_> = fs::read_dir(&self.root)
            .context(UnableToReadDirectoryContents { path: &self.root })?
            .flatten() // Discard errors
            .map(|e| e.path())
            .filter(|path| path.extension() == Some(OsStr::new(Self::EXTENSION)))
            .filter(|path| {
                path.file_stem().map_or(false, |file_stem| {
                    let file_stem = file_stem.to_string_lossy();
                    FILENAME_PATTERN.is_match(&file_stem)
                })
            })
            .collect();

        wal_paths.sort();

        Ok(wal_paths.into_iter())
    }

    fn filename_starting_at_sequence_number(&self, starting_sequence_number: u64) -> PathBuf {
        let file_stem = format!("{}{:016x}", Self::PREFIX, starting_sequence_number);
        let mut filename = self.root.join(file_stem);
        filename.set_extension(Self::EXTENSION);
        filename
    }
}

/// Provides atomic file writing capabilities.
///
/// It is *required* to call `SideFile::sync_all_and_rename` when done with the
/// file to ensure that the appending happens.
///
/// # Discussion
///
/// Operating systems / filesystems don't provide APIs for atomic writes. The
/// workaround is to copy the existing file to a temporary location, perform
/// non-atomic writes, then rename the temporary file over the original.
///
/// File creation and renaming are atomic, so consumers of the non-temporary
/// files will only ever see the data there or not.
///
/// Without this mechanism, a consumer could see a file in a state where we've
/// written some but not all of the data and the file would look corrupted or
/// otherwise be invalid.
#[derive(Debug)]
struct SideFile {
    final_destination: PathBuf,
    temporary_destination: PathBuf,
    temporary_file: File,
}

impl SideFile {
    fn new(final_destination: PathBuf, temporary_destination: PathBuf) -> Result<Self> {
        // Creating the temporary file will fail if there's already an existing
        // temp file with this name. This should never happen as long as only
        // one task is performing syncing.
        let mut temporary_file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(&temporary_destination)
            .context(UnableToCreateFile {
                path: &temporary_destination,
            })?;

        // Don't worry about errors opening the original file to read from; it
        // might not exist if no data has been written to the WAL.
        if let Ok(mut src) = File::open(&final_destination) {
            // Copy the contents from the existing file to the temp file.
            io::copy(&mut src, &mut temporary_file).context(UnableToCopyFileContents {
                src: &final_destination,
                dst: &temporary_destination,
            })?;
        }

        Ok(SideFile {
            final_destination,
            temporary_destination,
            temporary_file,
        })
    }

    fn sync_all_and_rename(self) -> io::Result<()> {
        self.temporary_file.sync_all()?;
        fs::rename(self.temporary_destination, self.final_destination)
    }
}

impl Deref for SideFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.temporary_file
    }
}

impl DerefMut for SideFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.temporary_file
    }
}

/// Produces an iterator over the on-disk entries in the WAL.
///
/// # Asynchronous considerations
///
/// This type performs blocking IO and care should be taken when using
/// it in an asynchronous context.
#[derive(Debug)]
struct Loader;

impl Loader {
    fn last_sequence_number(files: FileLocator) -> Result<Option<u64>> {
        let last = Self::headers(files)?.last().transpose()?;
        Ok(last.map(|h| h.sequence_number))
    }

    fn headers(files: FileLocator) -> Result<impl Iterator<Item = Result<Header>>> {
        let r = files
            .open_files_for_read()?
            .flat_map(|result_option_file| result_option_file.transpose())
            .map(|result_file| result_file.and_then(Self::headers_from_one_file));

        itertools::process_results(r, |iterator_of_iterators_of_result_headers| {
            iterator_of_iterators_of_result_headers
                .flatten()
                .collect::<Vec<_>>()
                .into_iter()
        })
    }

    fn headers_from_one_file(mut file: File) -> Result<impl Iterator<Item = Result<Header>>> {
        let metadata = file.metadata().context(UnableToReadFileMetadata)?;
        let mut length_remaining = metadata.len();

        Ok(Box::new(iter::from_fn(move || {
            if length_remaining == 0 {
                return None;
            }

            match Header::read(&mut file) {
                Ok(header) => {
                    let data_len = i64::from(header.len);
                    file.seek(SeekFrom::Current(data_len)).unwrap();

                    length_remaining -= Header::LEN + u64::from(header.len);

                    Some(Ok(header))
                }
                Err(e) => Some(Err(e)),
            }
        })))
    }

    fn load(files: FileLocator) -> Result<impl Iterator<Item = Result<Entry>>> {
        let r = files
            .open_files_for_read()?
            .flat_map(|result_option_file| result_option_file.transpose())
            .map(|result_file| result_file.and_then(Self::load_from_one_file));

        itertools::process_results(r, |iterator_of_iterators_of_result_entries| {
            iterator_of_iterators_of_result_entries
                .flatten()
                .collect::<Vec<_>>()
                .into_iter()
        })
    }

    fn load_from_one_file(mut file: File) -> Result<impl Iterator<Item = Result<Entry>>> {
        let metadata = file.metadata().context(UnableToReadFileMetadata)?;
        let mut length_remaining = metadata.len();

        Ok(Box::new(iter::from_fn(move || {
            if length_remaining == 0 {
                return None;
            }

            match Self::load_one(&mut file) {
                Ok((entry, bytes_read)) => {
                    length_remaining -= bytes_read;

                    Some(Ok(entry))
                }
                Err(e) => Some(Err(e)),
            }
        })))
    }

    fn load_one(file: &mut File) -> Result<(Entry, u64)> {
        let header = Header::read(&mut *file)?;

        let expected_len_us =
            usize::try_from(header.len).expect("Only designed to run on 32-bit systems or higher");

        let mut data = Vec::with_capacity(expected_len_us);
        let actual_len = file
            .take(u64::from(header.len))
            .read_to_end(&mut data)
            .context(UnableToReadData)?;

        ensure!(
            expected_len_us == actual_len,
            LengthMismatch {
                expected: expected_len_us,
                actual: actual_len
            }
        );

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let actual_checksum = hasher.finalize();

        ensure!(
            header.checksum == actual_checksum,
            ChecksumMismatch {
                expected: header.checksum,
                actual: actual_checksum
            }
        );

        let entry = Entry {
            sequence_number: header.sequence_number,
            data,
        };

        let bytes_read = Header::LEN + u64::from(header.len);

        Ok((entry, bytes_read))
    }
}

#[derive(Debug)]
struct Header {
    sequence_number: u64,
    checksum: u32,
    len: u32,
}

impl Header {
    const LEN: u64 = (mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>()) as u64;

    fn read(mut r: impl Read) -> Result<Self> {
        let sequence_number = r
            .read_u64::<LittleEndian>()
            .context(UnableToReadSequenceNumber)?;
        let checksum = r.read_u32::<LittleEndian>().context(UnableToReadChecksum)?;
        let len = r.read_u32::<LittleEndian>().context(UnableToReadLength)?;

        Ok(Self {
            sequence_number,
            checksum,
            len,
        })
    }

    fn write(&self, mut w: impl Write) -> Result<()> {
        w.write_u64::<LittleEndian>(self.sequence_number)
            .context(UnableToWriteSequenceNumber)?;
        w.write_u32::<LittleEndian>(self.checksum)
            .context(UnableToWriteChecksum)?;
        w.write_u32::<LittleEndian>(self.len)
            .context(UnableToWriteLength)?;
        Ok(())
    }
}

/// One batch of data read from the WAL.
///
/// This corresponds to one call to `Wal::append`.
#[derive(Debug)]
pub struct Entry {
    sequence_number: u64,
    data: Vec<u8>,
}

impl Entry {
    /// Gets the unique, increasing sequence number associated with this data
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    /// Gets a reference to the entry's data
    pub fn as_data(&self) -> &[u8] {
        &self.data
    }

    /// Gets the entry's data
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

/// One batch of data waiting to be appended to the current WAL file.
///
/// # Generics
///
/// - `N` is a type that will be returned when this batch is synchronized to
///   disk (or attempted to). It should be cheaply clonable.
#[derive(Debug, Clone)]
struct Pending<N>
where
    N: Clone,
{
    checksum: u32,
    data: Bytes,
    len: u32,
    notify: N,
}

/// An in-memory batch of writes to treat atomically.
///
/// Use the `io::Write` interface and then call `finalize` to signify a batch's
/// end.
///
/// # Generics
///
/// - `N` is a type that will be returned when this batch is synchronized to
///   disk (or attempted to). It should be cheaply clonable.
#[derive(Debug)]
pub struct Append<'a, N>
where
    N: Clone,
{
    wal: &'a Wal<N>,
    buffer: BytesMut,
}

impl<'a, N> Append<'a, N>
where
    N: Clone,
{
    fn new(wal: &'a Wal<N>) -> Self {
        Self {
            wal,
            buffer: Default::default(),
        }
    }

    /// Signal that writing of the batch has completed.
    ///
    /// # Asynchronous considerations
    ///
    /// This function may be called in an asynchronous context with no special
    /// handling.
    pub fn finalize(self, notify: N) -> Result<()> {
        let data = self.buffer.freeze();

        // Only designed to support chunks up to `u32::max` bytes long.
        let len = data.len();
        let len = u32::try_from(len).context(ChunkSizeTooLarge { actual: len })?;

        let mut hasher = Hasher::new();
        hasher.update(&data);
        let checksum = hasher.finalize();

        let pending = Pending {
            checksum,
            data,
            len,
            notify,
        };

        self.wal.append_pending(pending);
        Ok(())
    }
}

impl<'a, N> Write for Append<'a, N>
where
    N: Clone,
{
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn side_file_basic() -> Result {
        let dir = delorean_test_helpers::tmp_dir()?;
        let existing_path = dir.as_ref().join("existing");
        let temp_path = dir.as_ref().join("temp");

        let mut side_file = SideFile::new(existing_path.clone(), temp_path)?;

        side_file.write_all(b"hello")?;
        side_file.write_all(b"world")?;

        side_file.sync_all_and_rename()?;

        let s = fs::read_to_string(existing_path)?;

        assert_eq!("helloworld", s);

        Ok(())
    }

    #[test]
    fn sequence_numbers_are_persisted() -> Result {
        let dir = delorean_test_helpers::tmp_dir()?;
        let builder = WalBuilder::new(dir.as_ref());
        let mut wal;

        // Create one in-memory WAL and sync it
        {
            wal = builder.clone().wal()?;

            let mut w = wal.append();
            w.write_all(b"some")?;
            w.write_all(b"data")?;
            w.finalize(())?;

            let (to_notify, outcome) = wal.sync();
            assert!(matches!(outcome, Ok(())));
            assert_eq!(1, to_notify.len());
        }

        // Pretend the process restarts
        {
            wal = builder.wal()?;

            assert_eq!(1, wal.sequence_number.load(Ordering::SeqCst));
        }

        Ok(())
    }

    #[test]
    fn sequence_numbers_increase_by_number_of_pending_entries() -> Result {
        let dir = delorean_test_helpers::tmp_dir()?;
        let builder = WalBuilder::new(dir.as_ref());
        let wal = builder.wal()?;

        // Write 1 entry then sync
        let mut w = wal.append();
        w.write_all(b"some")?;
        w.finalize(())?;
        let (to_notify, outcome) = wal.sync();
        assert!(matches!(outcome, Ok(())));
        assert_eq!(1, to_notify.len());

        // Sequence number should increase by 1
        assert_eq!(1, wal.sequence_number.load(Ordering::SeqCst));

        // Write 2 entries then sync
        let mut w = wal.append();
        w.write_all(b"other")?;
        w.finalize(())?;
        let mut w = wal.append();
        w.write_all(b"again")?;
        w.finalize(())?;
        let (to_notify, outcome) = wal.sync();
        assert!(matches!(outcome, Ok(())));
        assert_eq!(2, to_notify.len());

        // Sequence number should increase by 2
        assert_eq!(3, wal.sequence_number.load(Ordering::SeqCst));

        Ok(())
    }
}
