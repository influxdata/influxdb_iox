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

pub fn write(file: &File, bytes: &[u8], offset: u64) -> Result<(), IoError> {
    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};

        let _ = MUTEX.lock();
        let mut file = file.try_clone().context(FailedToCloneFile)?;
        file.seek(SeekFrom::Start(offset)).context(FailedToSeek)?;
        file.write_all(bytes).context(FailedToWriteDataOther)?;
        Ok(())
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;

        file.write_all_at(bytes, offset)
            .context(FailedToWriteDataUnix)?;

        Ok(())
    }
}
