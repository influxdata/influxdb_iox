//! [HMAC]-based token generator.
//!
//! [HMAC]: https://en.wikipedia.org/wiki/HMAC
use std::{convert::TryInto, fmt::Debug};

use chrono::{DateTime, TimeZone, Utc};
use hmac::{Hmac, Mac, NewMac};
use rand::random;
use sha2::Sha256;
use snafu::{ResultExt, Snafu};

type IoxHmac = Hmac<Sha256>;

#[derive(Debug, Snafu)]
pub enum VerificationError {
    #[snafu(display("Failed to decode base64: {}", source))]
    Base64Error { source: base64::DecodeError },

    #[snafu(display("Token is too short"))]
    TokenTooShort,

    #[snafu(display("Failed to verify MAC: {}", inner))]
    MacError { inner: hmac::crypto_mac::MacError },

    #[snafu(display("Token is valid in the future"))]
    FutureTokenError,

    #[snafu(display("Token has expired"))]
    ExpiredError,
}

/// [HMAC]-based token generator.
///
/// # Security Scope
/// **Only use this generator as a second factor to prevent fat fingers. This MUST NOT be used as a primary factor to
///  check access / permissions!**
///
/// Generators are unique to the server. The private key they contain will NOT survive restarts. The scopes used here
/// should be as specific as possible and must contain they action as well as the object they protect. For example to
/// wipe to drop a table from a database you may use `drop@database=my_db,table=my_table`. The following tables contains
/// BAD example that SHOULD NOT be used:
///
/// | Anti-Example                    | Why it is bad                                                     |
/// | ------------------------------- | ----------------------------------------------------------------- |
/// | `database=my_db,table=my_table` | Action missing (might also be `create`)                           |
/// | `drop@database=my_db`           | Table is missing (might drop any table)                           |
/// | `drop@table=my_table`           | Database is missing (might drop same table from another database) |
///
/// # Example
/// ```
/// use chrono::Utc;
/// use token_challenge::hmac::HmacTokenGenerator;
///
/// let gen = HmacTokenGenerator::new(60);
/// let token = gen.generate("bake_pizza", Utc::now());
/// gen.verify("bake_pizza", Utc::now(), &token).unwrap();
/// gen.verify("get_money", Utc::now(), &token).unwrap_err();
/// ```
///
/// [HMAC]: https://en.wikipedia.org/wiki/HMAC
#[allow(missing_copy_implementations)]
pub struct HmacTokenGenerator {
    /// 256 bit private key
    key: [u8; 32],

    /// token validity in seconds
    token_seconds: u32,
}

impl HmacTokenGenerator {
    /// Create new generator that allows tokens to be valid for the given number of seconds.
    ///
    /// Note that calling this method twice with the same arguments will lead to different generators with different
    /// private keys.
    pub fn new(token_seconds: u32) -> Self {
        Self {
            key: random(),
            token_seconds,
        }
    }

    /// Generate token for the given scope.
    pub fn generate(&self, scope: &str, now: DateTime<Utc>) -> String {
        let ts_bytes = now.timestamp().to_be_bytes();

        // build MAC
        let mut mac = self.mac();
        mac.update(scope.as_bytes());
        mac.update(&ts_bytes);
        let mac_bytes = mac.finalize().into_bytes();

        // concat timestamp and MAC
        let mut token_bytes = Vec::with_capacity(ts_bytes.len() + mac_bytes.len());
        token_bytes.extend_from_slice(&ts_bytes);
        token_bytes.extend_from_slice(&mac_bytes);

        base64::encode(&token_bytes)
    }

    /// Verify the given token.
    pub fn verify(
        &self,
        scope: &str,
        now: DateTime<Utc>,
        token: &str,
    ) -> std::result::Result<(), VerificationError> {
        let token_bytes = base64::decode(token).context(Base64Error)?;

        // split timestamp and MAC
        if token_bytes.len() < 8 {
            return Err(VerificationError::TokenTooShort);
        }
        let ts_bytes = &token_bytes[..8];
        let mac_bytes = &token_bytes[8..];

        // verify MAC
        let mut mac = self.mac();
        mac.update(scope.as_bytes());
        mac.update(ts_bytes);
        if let Err(e) = mac.verify(&mac_bytes) {
            return Err(VerificationError::MacError { inner: e });
        }

        // deserialize and check timestamp AFTER verifying MAC
        let ts = Utc.timestamp(
            i64::from_be_bytes(ts_bytes.try_into().expect("size was checked")),
            0,
        );
        let age_seconds = (now - ts).num_seconds();
        if age_seconds < 0 {
            return Err(VerificationError::FutureTokenError);
        }
        if age_seconds > self.token_seconds as i64 {
            return Err(VerificationError::ExpiredError);
        }

        Ok(())
    }

    /// Create empty MAC using generator-specific private key.
    fn mac(&self) -> IoxHmac {
        IoxHmac::new_from_slice(&self.key).expect("HMAC can take key of any size")
    }
}

impl Debug for HmacTokenGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmacTokenGenerator")
            .field("token_seconds", &self.token_seconds)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;

    #[test]
    fn test_pass() {
        let gen = HmacTokenGenerator::new(60);
        let scope = "foo";

        let token = gen.generate(scope, Utc::now());
        gen.verify(scope, Utc::now(), &token).unwrap();
    }

    #[test]
    fn test_token_len() {
        let gen = HmacTokenGenerator::new(60);

        let token = gen.generate("foo", Utc::now());
        assert_eq!(token.len(), 56);
    }

    #[test]
    fn test_fail_invalid_base64() {
        let gen = HmacTokenGenerator::new(60);
        let scope = "foo";

        assert!(matches!(
            gen.verify(scope, Utc::now(), "x").unwrap_err(),
            VerificationError::Base64Error { .. }
        ));
    }

    #[test]
    fn test_fail_invalid_empty() {
        let gen = HmacTokenGenerator::new(60);
        let scope = "foo";

        assert!(matches!(
            gen.verify(scope, Utc::now(), "").unwrap_err(),
            VerificationError::TokenTooShort
        ));
    }

    #[test]
    fn test_fail_different_gen() {
        let gen1 = HmacTokenGenerator::new(60);
        let gen2 = HmacTokenGenerator::new(60);
        let scope = "foo";

        let token = gen1.generate(scope, Utc::now());
        assert!(matches!(
            gen2.verify(scope, Utc::now(), &token).unwrap_err(),
            VerificationError::MacError { .. }
        ));
    }

    #[test]
    fn test_fail_different_scope() {
        let gen = HmacTokenGenerator::new(60);

        let token = gen.generate("foo", Utc::now());
        assert!(matches!(
            gen.verify("bar", Utc::now(), &token).unwrap_err(),
            VerificationError::MacError { .. }
        ));
    }

    #[test]
    fn test_ts_validation() {
        let gen = HmacTokenGenerator::new(60);
        let scope = "foo";
        let now = Utc::now();

        let token = gen.generate(scope, now);
        assert!(matches!(
            gen.verify(scope, now - Duration::seconds(2), &token)
                .unwrap_err(),
            VerificationError::FutureTokenError
        ));
        gen.verify(scope, now, &token).unwrap();
        gen.verify(scope, now + Duration::seconds(60), &token)
            .unwrap();
        assert!(matches!(
            gen.verify(scope, now + Duration::seconds(61), &token)
                .unwrap_err(),
            VerificationError::ExpiredError
        ));
    }

    #[test]
    fn test_fail_modify_bits() {
        let gen = HmacTokenGenerator::new(60);
        let scope = "foo";
        let now = Utc::now();

        let token = gen.generate(scope, now);
        let token_bytes = base64::decode(token).unwrap();
        for i_byte in 0..token_bytes.len() {
            for i_bit in 0..8 {
                let mut token_bytes = token_bytes.clone();
                token_bytes[i_byte] ^= 1 << i_bit;
                let token = base64::encode(token_bytes);
                gen.verify(scope, now, &token).unwrap_err();
            }
        }
    }

    #[test]
    fn test_dbg_does_not_leak_key() {
        let gen = HmacTokenGenerator::new(60);
        assert_eq!(
            format!("{:?}", gen),
            "HmacTokenGenerator { token_seconds: 60 }"
        );
    }

    #[test]
    fn test_token_generation_does_not_invalidate() {
        // ensure that generating a new token does not invalidate the old one (e.g. their is no counter attached)
        let gen = HmacTokenGenerator::new(60);
        let scope = "foo";

        let token1 = gen.generate(scope, Utc::now());
        let token2 = gen.generate(scope, Utc::now());
        gen.verify(scope, Utc::now(), &token1).unwrap();
        gen.verify(scope, Utc::now(), &token2).unwrap();
    }
}
