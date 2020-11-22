use once_cell::sync::Lazy;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use std::fs::File;

#[allow(dead_code)]
static MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Debug, Snafu)]
pub enum IoError {
    FailedToWriteDataUnix { source: std::io::Error },
    FailedToWriteDataOther { source: std::io::Error },
    FailedToCloneFile { source: std::io::Error },
    FailedToSeek { source: std::io::Error },
}

pub fn write(
    file: &File,
    header_bytes: &[u8],
    data_bytes: &[u8],
    offset: u64,
) -> Result<(), IoError> {
    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};

        let _ = MUTEX.lock();
        let mut file = file.try_clone().context(FailedToCloneFile)?;
        file.seek(SeekFrom::Start(offset)).context(FailedToSeek)?;
        file.write_all(header_bytes)
            .context(FailedToWriteDataOther)?;
        file.write_all(data_bytes).context(FailedToWriteDataOther)?;
        Ok(())
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;

        let mut vec = Vec::with_capacity(header_bytes.len() + data_bytes.len());
        vec.extend_from_slice(header_bytes);
        vec.extend_from_slice(data_bytes);

        file.write_all_at(&vec, offset)
            .context(FailedToWriteDataUnix)?;

        Ok(())
    }
}
