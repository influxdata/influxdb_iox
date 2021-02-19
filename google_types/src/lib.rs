// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![allow(
    unused_imports,
    clippy::redundant_static_lifetimes,
    clippy::redundant_closure,
    clippy::redundant_field_names,
    clippy::clone_on_ref_ptr
)]

mod pb {
    pub mod google {
        pub mod protobuf {
            use std::convert::{TryFrom, TryInto};

            include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));

            impl TryFrom<Duration> for std::time::Duration {
                type Error = std::num::TryFromIntError;

                fn try_from(value: Duration) -> Result<Self, Self::Error> {
                    Ok(std::time::Duration::new(
                        value.seconds.try_into()?,
                        value.nanos.try_into()?,
                    ))
                }
            }

            impl From<std::time::Duration> for Duration {
                fn from(value: std::time::Duration) -> Self {
                    Self {
                        seconds: value.as_secs() as _,
                        nanos: value.subsec_nanos() as _,
                    }
                }
            }
        }

        pub mod rpc {
            include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
        }
    }
}

pub use pb::google::*;

use pb::google::protobuf::Any;
use prost::{
    bytes::{Bytes, BytesMut},
    Message,
};
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::iter::FromIterator;

fn encode_status(code: tonic::Code, message: String, details: Option<Any>) -> tonic::Status {
    if let Some(details) = details {
        let mut buffer = BytesMut::new();
        let status = pb::google::rpc::Status {
            code: tonic::Code::InvalidArgument as i32,
            message: message.clone(),
            details: vec![details],
        };

        if status.encode(&mut buffer).is_ok() {
            return tonic::Status::with_details(
                tonic::Code::InvalidArgument,
                message,
                buffer.freeze(),
            );
        }
    }
    tonic::Status::new(code, message)
}

#[derive(Debug, Default, Clone)]
pub struct FieldViolation {
    pub field: String,
    pub description: String,
}

impl FieldViolation {
    pub fn required(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            description: "Field is required".to_string(),
        }
    }

    /// Re-scopes this error as the child of another field
    pub fn scope(self, field: impl Into<String>) -> Self {
        let field = if self.field.is_empty() {
            field.into()
        } else {
            [field.into(), self.field].join(".")
        };

        Self {
            field,
            description: self.description,
        }
    }
}

fn encode_bad_request(violation: Vec<FieldViolation>) -> Result<Any, prost::EncodeError> {
    let mut buffer = BytesMut::new();

    pb::google::rpc::BadRequest {
        field_violations: violation
            .into_iter()
            .map(|f| pb::google::rpc::bad_request::FieldViolation {
                field: f.field,
                description: f.description,
            })
            .collect(),
    }
    .encode(&mut buffer)?;

    Ok(Any {
        type_url: "type.googleapis.com/google.rpc.BadRequest".to_string(),
        value: buffer.freeze(),
    })
}

impl From<FieldViolation> for tonic::Status {
    fn from(f: FieldViolation) -> Self {
        let message = format!("Violation for field \"{}\": {}", f.field, f.description);
        encode_status(
            tonic::Code::InvalidArgument,
            message,
            encode_bad_request(vec![f]).ok(),
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct InternalError {}

impl From<InternalError> for tonic::Status {
    fn from(_: InternalError) -> Self {
        tonic::Status::new(tonic::Code::Internal, "Internal Error")
    }
}

#[derive(Debug, Default, Clone)]
pub struct AlreadyExists {
    pub resource_type: String,
    pub resource_name: String,
    pub owner: String,
    pub description: String,
}

fn encode_resource_info(
    resource_type: String,
    resource_name: String,
    owner: String,
    description: String,
) -> Result<Any, prost::EncodeError> {
    let mut buffer = BytesMut::new();

    pb::google::rpc::ResourceInfo {
        resource_type,
        resource_name,
        owner,
        description,
    }
    .encode(&mut buffer)?;

    Ok(Any {
        type_url: "type.googleapis.com/google.rpc.ResourceInfo".to_string(),
        value: buffer.freeze(),
    })
}

impl From<AlreadyExists> for tonic::Status {
    fn from(exists: AlreadyExists) -> Self {
        let message = format!(
            "Resource {}/{} already exists",
            exists.resource_type, exists.resource_name
        );
        encode_status(
            tonic::Code::AlreadyExists,
            message,
            encode_resource_info(
                exists.resource_type,
                exists.resource_name,
                exists.owner,
                exists.description,
            )
            .ok(),
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct NotFound {
    pub resource_type: String,
    pub resource_name: String,
    pub owner: String,
    pub description: String,
}

impl From<NotFound> for tonic::Status {
    fn from(not_found: NotFound) -> Self {
        let message = format!(
            "Resource {}/{} not found",
            not_found.resource_type, not_found.resource_name
        );
        encode_status(
            tonic::Code::AlreadyExists,
            message,
            encode_resource_info(
                not_found.resource_type,
                not_found.resource_name,
                not_found.owner,
                not_found.description,
            )
            .ok(),
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct PreconditionViolation {
    pub category: String,
    pub subject: String,
    pub description: String,
}

fn encode_precondition_failure(
    violations: Vec<PreconditionViolation>,
) -> Result<Any, prost::EncodeError> {
    use pb::google::rpc::precondition_failure::Violation;

    let mut buffer = BytesMut::new();

    pb::google::rpc::PreconditionFailure {
        violations: violations
            .into_iter()
            .map(|x| Violation {
                r#type: x.category,
                subject: x.subject,
                description: x.description,
            })
            .collect(),
    }
    .encode(&mut buffer)?;

    Ok(Any {
        type_url: "type.googleapis.com/google.rpc.PreconditionFailure".to_string(),
        value: buffer.freeze(),
    })
}

impl From<PreconditionViolation> for tonic::Status {
    fn from(violation: PreconditionViolation) -> Self {
        let message = format!(
            "Precondition violation {} - {}: {}",
            violation.subject, violation.category, violation.description
        );
        encode_status(
            tonic::Code::FailedPrecondition,
            message,
            encode_precondition_failure(vec![violation]).ok(),
        )
    }
}

/// An extension trait that adds the ability to convert an error
/// that can be converted to a String to a FieldViolation
pub trait FieldViolationExt {
    type Output;

    fn field(self, field: &'static str) -> Result<Self::Output, FieldViolation>;
}

impl<T, E> FieldViolationExt for Result<T, E>
where
    E: ToString,
{
    type Output = T;

    fn field(self, field: &'static str) -> Result<T, FieldViolation> {
        self.map_err(|e| FieldViolation {
            field: field.to_string(),
            description: e.to_string(),
        })
    }
}
