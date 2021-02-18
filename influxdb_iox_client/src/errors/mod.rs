//! If using the Arrow Flight API, errors from gRPC requests will be converted
//! into a [`GrpcError`] containing details of the failed request.

#[cfg(feature = "flight")]
mod grpc_error;
#[cfg(feature = "flight")]
pub use grpc_error::*;

#[cfg(feature = "flight")]
mod grpc_query_error;
#[cfg(feature = "flight")]
pub use grpc_query_error::*;

// TODO: Remove these error codes and corresponding HTTP routes

/// Constants used in API error codes.
///
/// Expressing this as a enum prevents reuse of discriminants, and as they're
/// effectively consts this uses UPPER_SNAKE_CASE.
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub enum ApiErrorCode {
    /// An unknown/unhandled error
    UNKNOWN = 100,

    /// The database name in the request is invalid.
    DB_INVALID_NAME = 101,

    /// The database referenced already exists.
    DB_ALREADY_EXISTS = 102,

    /// The database referenced does not exist.
    DB_NOT_FOUND = 103,
}

impl From<ApiErrorCode> for u32 {
    fn from(v: ApiErrorCode) -> Self {
        v as u32
    }
}
