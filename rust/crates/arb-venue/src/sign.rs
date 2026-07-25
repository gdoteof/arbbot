//! Request signing — byte-identical to the Python gateways.
//!
//! Both venues sign the SAME canonical message shape: `timestamp_ms + METHOD +
//! path`, where `path` includes the API version prefix and EXCLUDES any query
//! string (Kalshi excludes the query; PM-US paths carry none). The millisecond
//! timestamp is INJECTED by the caller — this module never reads a clock, so
//! signing is a pure function of (key, ts, method, path).
//!
//! Python references replicated here:
//! - Kalshi  `src/arbbot/record/kalshi.py::sign_headers`
//!   RSA-PSS(SHA256), salt_length = DIGEST_LENGTH (32), headers
//!   `KALSHI-ACCESS-KEY / -TIMESTAMP / -SIGNATURE` (base64), in that order.
//! - PM-US   `src/arbbot/exec/polymarket_us_gateway.py::_headers`
//!   Ed25519, headers `X-PM-Access-Key / -Timestamp / -Signature` (base64)
//!   + `Content-Type: application/json`, in that order.

use base64::Engine as _;

use crate::error::VenueError;

const B64: base64::engine::general_purpose::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// The canonical signed message. Identical composition for both venues:
/// `ts_ms + method + path`. No separators (matches Python `(ts_ms + method +
/// path).encode()` and the f-string `f"{ts}{method}{path}"`).
pub fn canonical_message(ts_ms: &str, method: &str, path: &str) -> String {
    format!("{ts_ms}{method}{path}")
}

// ------------------------------------------------------------------ Kalshi ---

pub const KALSHI_HDR_KEY: &str = "KALSHI-ACCESS-KEY";
pub const KALSHI_HDR_TS: &str = "KALSHI-ACCESS-TIMESTAMP";
pub const KALSHI_HDR_SIG: &str = "KALSHI-ACCESS-SIGNATURE";

/// Kalshi RSA-PSS-SHA256 signer. `salt_len` matches Python's
/// `padding.PSS.DIGEST_LENGTH` (== SHA256 output == 32) via the `rsa` crate's
/// default `SigningKey::new`. Signatures are salted/nondeterministic — verify
/// them, never byte-compare.
pub struct KalshiSigner {
    api_key_id: String,
    signing_key: rsa::pss::SigningKey<sha2::Sha256>,
}

impl KalshiSigner {
    /// Load from a PKCS#8 PEM private key (the on-disk `kalshi_private_key.pem`
    /// shape, loaded once — never re-parsed per request, mirroring the Python
    /// `load_private_key` contract).
    pub fn from_pkcs8_pem(api_key_id: impl Into<String>, pem: &str) -> Result<Self, VenueError> {
        use rsa::pkcs8::DecodePrivateKey;
        let key = rsa::RsaPrivateKey::from_pkcs8_pem(pem)
            .map_err(|e| VenueError::Sign(format!("bad kalshi PKCS#8 PEM: {e}")))?;
        Ok(Self {
            api_key_id: api_key_id.into(),
            signing_key: rsa::pss::SigningKey::<sha2::Sha256>::new(key),
        })
    }

    /// Raw signature bytes over the canonical message. Salted, so two calls with
    /// the same inputs differ — verification is the parity check.
    pub fn sign(&self, ts_ms: &str, method: &str, path: &str) -> Vec<u8> {
        use rsa::signature::{RandomizedSigner, SignatureEncoding};
        let msg = canonical_message(ts_ms, method, path);
        let sig = self
            .signing_key
            .sign_with_rng(&mut rand_core::OsRng, msg.as_bytes());
        sig.to_vec()
    }

    /// The three auth headers, in Python dict-insertion order.
    pub fn headers(&self, ts_ms: &str, method: &str, path: &str) -> Vec<(&'static str, String)> {
        let sig = self.sign(ts_ms, method, path);
        vec![
            (KALSHI_HDR_KEY, self.api_key_id.clone()),
            (KALSHI_HDR_TS, ts_ms.to_string()),
            (KALSHI_HDR_SIG, B64.encode(sig)),
        ]
    }
}

/// Kalshi public-key verifier — the parity harness verifies Python-produced
/// (salted) signatures with this.
pub struct KalshiVerifier {
    verifying_key: rsa::pss::VerifyingKey<sha2::Sha256>,
}

impl KalshiVerifier {
    pub fn from_public_key_pem(pem: &str) -> Result<Self, VenueError> {
        use rsa::pkcs8::DecodePublicKey;
        let key = rsa::RsaPublicKey::from_public_key_pem(pem)
            .map_err(|e| VenueError::Sign(format!("bad kalshi SPKI PEM: {e}")))?;
        Ok(Self {
            verifying_key: rsa::pss::VerifyingKey::<sha2::Sha256>::new(key),
        })
    }

    pub fn verify(&self, ts_ms: &str, method: &str, path: &str, sig: &[u8]) -> bool {
        use rsa::signature::Verifier;
        let msg = canonical_message(ts_ms, method, path);
        match rsa::pss::Signature::try_from(sig) {
            Ok(s) => self.verifying_key.verify(msg.as_bytes(), &s).is_ok(),
            Err(_) => false,
        }
    }
}

// ------------------------------------------------------------------- PM-US ---

pub const PMUS_HDR_KEY: &str = "X-PM-Access-Key";
pub const PMUS_HDR_TS: &str = "X-PM-Timestamp";
pub const PMUS_HDR_SIG: &str = "X-PM-Signature";
pub const PMUS_HDR_CT: &str = "Content-Type";
pub const PMUS_CONTENT_TYPE: &str = "application/json";

/// PM-US Ed25519 signer. Deterministic (RFC 8032), so signatures byte-compare
/// against the Python fixture. Python derives the seed as the first 32 bytes of
/// the base64-decoded secret (`from_private_bytes(b64decode(secret)[:32])`).
pub struct PmusSigner {
    api_key_id: String,
    signing_key: ed25519_dalek::SigningKey,
}

impl PmusSigner {
    pub fn from_seed(api_key_id: impl Into<String>, seed: &[u8; 32]) -> Self {
        Self {
            api_key_id: api_key_id.into(),
            signing_key: ed25519_dalek::SigningKey::from_bytes(seed),
        }
    }

    /// Mirror Python exactly: base64-decode the secret, take the first 32 bytes
    /// as the Ed25519 seed. A shorter decode is a hard error (no silent pad).
    pub fn from_secret_b64(
        api_key_id: impl Into<String>,
        secret_b64: &str,
    ) -> Result<Self, VenueError> {
        let raw = B64
            .decode(secret_b64.trim())
            .map_err(|e| VenueError::Sign(format!("bad PM secret base64: {e}")))?;
        if raw.len() < 32 {
            return Err(VenueError::Sign(format!(
                "PM secret decodes to {} bytes, need >= 32",
                raw.len()
            )));
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&raw[..32]);
        Ok(Self::from_seed(api_key_id, &seed))
    }

    /// 64-byte deterministic Ed25519 signature over the canonical message.
    pub fn sign(&self, ts_ms: &str, method: &str, path: &str) -> [u8; 64] {
        use ed25519_dalek::Signer;
        let msg = canonical_message(ts_ms, method, path);
        self.signing_key.sign(msg.as_bytes()).to_bytes()
    }

    /// The four headers, in Python dict-insertion order (Content-Type last).
    pub fn headers(&self, ts_ms: &str, method: &str, path: &str) -> Vec<(&'static str, String)> {
        let sig = self.sign(ts_ms, method, path);
        vec![
            (PMUS_HDR_KEY, self.api_key_id.clone()),
            (PMUS_HDR_TS, ts_ms.to_string()),
            (PMUS_HDR_SIG, B64.encode(sig)),
            (PMUS_HDR_CT, PMUS_CONTENT_TYPE.to_string()),
        ]
    }
}
