use super::{ApiErrorCode, HttpError, ServerErrorResponse};
use crate::errors::ClientError;
use thiserror::Error;

/// Error responses when creating a new IOx database.
#[derive(Debug, Error)]
pub enum WalMetadataError {
    /// The database doesn't exist
    #[error("the database does not exist")]
    DatabaseNotFound,

    /// The database name contains an invalid character.
    #[error("the database does not have a WAL")]
    WalNotFound,

    /// An unknown server error occured.
    ///
    /// The error string contains the error string returned by the server.
    #[error(transparent)]
    ServerError(ServerErrorResponse),

    /// A non-application HTTP request/response error occurred.
    #[error(transparent)]
    HttpError(#[from] HttpError),

    /// An error occurred in the client.
    #[error(transparent)]
    ClientError(#[from] ClientError),
}

/// Convert a [`ServerErrorResponse`] into a [`WalMetadataError`].
///
/// This conversion plucks any errors with API error codes that are applicable
/// to [`WalMetadataError`] types, and everything else becomes a
/// `ServerError`.
impl From<ServerErrorResponse> for WalMetadataError {
    fn from(err: ServerErrorResponse) -> Self {
        match err.error_code() {
            Some(c) if c == ApiErrorCode::DB_NOT_FOUND as u32 => Self::WalNotFound,
            Some(c) if c == ApiErrorCode::WAL_NOT_FOUND as u32 => Self::WalNotFound,
            _ => Self::ServerError(err),
        }
    }
}

/// Convert errors from the underlying HTTP client into `HttpError` instances.
impl From<reqwest::Error> for WalMetadataError {
    fn from(err: reqwest::Error) -> Self {
        Self::HttpError(err.into())
    }
}
