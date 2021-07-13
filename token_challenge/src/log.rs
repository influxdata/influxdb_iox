//! Utils to emit tokens via server logs.
//!
//! The assumption here is that an admin can trigger an action w/o a token (that will fail), then look at the logs,
//! fetch the token and trigger the action for real. That should prevent fat fingers for a certain extend.

use chrono::{DateTime, Utc};
use observability_deps::tracing::info;
use snafu::Snafu;

use crate::hmac::HmacTokenGenerator;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot verify token for {}: {}", description, source))]
    VerificationError {
        description: String,
        source: crate::hmac::VerificationError,

        // new token (not included in display representation)
        new_token: String,
    },

    #[snafu(display("No token provided for {}", description))]
    NoTokenProvided {
        description: String,

        // new token (not included in display representation)
        new_token: String,
    },
}

impl Error {
    /// Description of the action that lead to the error.
    pub fn description(&self) -> &str {
        match self {
            Error::VerificationError { description, .. } => description,
            Error::NoTokenProvided { description, .. } => description,
        }
    }

    /// New token that can be used to complete the action.
    pub fn new_token(&self) -> &str {
        match self {
            Error::VerificationError { new_token, .. } => new_token,
            Error::NoTokenProvided { new_token, .. } => new_token,
        }
    }
}

/// Verify provided token or log a new one if token is invalid or none was provided.
///
/// Logging is done at info-level. For the description of how to use `scope`, see [`HmacTokenGenerator`].
pub fn verify_or_log_token(
    gen: &HmacTokenGenerator,
    scope: &str,
    description: &str,
    token: Option<&str>,
) -> std::result::Result<(), Error> {
    let now = Utc::now();

    if let Some(token) = token {
        match gen.verify(scope, now, token) {
            Ok(()) => Ok(()),
            Err(e) => {
                let new_token = log_token(gen, scope, description, now);
                Err(Error::VerificationError {
                    description: description.to_string(),
                    source: e,
                    new_token,
                })
            }
        }
    } else {
        let new_token = log_token(gen, scope, description, now);
        Err(Error::NoTokenProvided {
            description: description.to_string(),
            new_token,
        })
    }
}

/// Generate new token and log it.
fn log_token(
    gen: &HmacTokenGenerator,
    scope: &str,
    description: &str,
    now: DateTime<Utc>,
) -> String {
    let token = gen.generate(scope, now);
    let token_with_markers = format!("Token(\"{}\")", token);
    info!(token = %token_with_markers, "New token to {}", description);
    token
}

pub mod test_helpers {
    /// Extract last token from logs (e.g. stdout, `TracingCapture`).
    pub fn extract_last_token_from_logs(logs: &str) -> Option<String> {
        let start_tag = "Token(\"";
        let end_tag = "\")";

        if let Some(start) = logs.rfind(start_tag) {
            let search_scope = &logs[start + start_tag.len()..];
            if let Some(end) = search_scope.find(end_tag) {
                return Some(search_scope[..end].to_string());
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use ::test_helpers::tracing::TracingCapture;

    use super::test_helpers::*;
    use super::*;

    #[test]
    fn test_verify_or_log_token() {
        let capture = TracingCapture::new();
        let gen = HmacTokenGenerator::new(60);

        // no token provided yet
        let err = verify_or_log_token(&gen, "scope1", "drop everything", None).unwrap_err();
        assert!(matches!(err, Error::NoTokenProvided { .. }));
        assert_eq!(err.description(), "drop everything");

        // extract token from logs and try again
        let token = extract_last_token_from_logs(&capture.to_string()).unwrap();
        assert_eq!(token, err.new_token());
        verify_or_log_token(&gen, "scope1", "drop everything", Some(&token)).unwrap();

        // change scope
        let err =
            verify_or_log_token(&gen, "scope2", "create everything", Some(&token)).unwrap_err();
        assert!(matches!(err, Error::VerificationError { .. }));
        assert_eq!(err.description(), "create everything");
        let token = extract_last_token_from_logs(&capture.to_string()).unwrap();
        assert_eq!(token, err.new_token());
        verify_or_log_token(&gen, "scope2", "create everything", Some(&token)).unwrap();
    }
}
