use once_cell::sync::Lazy;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use std::{fs::File, os::unix::io::AsRawFd};

#[derive(Debug, Snafu)]
pub enum IoError {
    FailedToWriteDataUnix { source: nix::Error },
    FailedToWriteDataOther { source: nix::Error },
    FailedToCloneFile { source: std::io::Error },
    FailedToSeek { source: std::io::Error },
}

#[cfg(any(
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "linux",
    target_os = "netbsd",
    target_os = "openbsd"
))]
pub fn write(
    file: &File,
    header_bytes: &[u8],
    data_bytes: &[u8],
    offset: u64,
) -> Result<(), IoError> {
    use nix::sys::uio::{pwritev, IoVec};

    let iovec = [
        IoVec::from_slice(header_bytes),
        IoVec::from_slice(data_bytes),
    ];

    pwritev(file.as_raw_fd(), &iovec, offset as i64).context(FailedToWriteDataUnix)?;

    Ok(())
}

#[cfg(target_os = "macos")]
pub fn write(
    file: &File,
    header_bytes: &[u8],
    data_bytes: &[u8],
    offset: u64,
) -> Result<(), IoError> {
    use crate::payload::Header;
    use nix::sys::uio::pwrite;

    pwrite(file.as_raw_fd(), &header_bytes, offset as i64).context(FailedToWriteDataUnix)?;
    pwrite(file.as_raw_fd(), &data_bytes, offset + Header::LEN as i64)
        .context(FailedToWriteDataUnix)?;

    Ok(())
}

#[allow(dead_code)]
static MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[cfg(not(any(
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "linux",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "macos"
)))]
pub fn write(
    file: &File,
    header_bytes: &[u8],
    data_bytes: &[u8],
    offset: u64,
) -> Result<(), IoError> {
    use std::io::SeekFrom;

    let _ = MUTEX.lock();
    let mut file = file.try_clone().context(FailedToCloneFile)?;
    file.seek(SeekFrom::Start(offset)).context(FailedToSeek)?;
    file.write_all(header_bytes)
        .context(FailedToWriteDataOther)?;
    file.write_all(data_bytes).context(FailedToWriteDataOther)?;
    Ok(())
}
