//! Venue auth signing.
//!
//! Kalshi: RSA-PSS(SHA256, salt = digest length) over `{ts_ms}{METHOD}{path}`
//! (record/kalshi.py sign_headers). Read-only recorder key ONLY — this
//! binary contains no order-placement code path.
//!
//! PM-US: Ed25519 over `{ts_ms}{METHOD}{path}`; key file is base64 and the
//! SEED is the FIRST 32 BYTES of the decoded blob (venues/pmus.py — signing
//! with the full blob yields opaque 401s).

use anyhow::{Context, Result};
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use sha2::Sha256;

fn now_ms() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis()
        .to_string()
}

pub struct KalshiSigner {
    key_id: String,
    key: SigningKey<Sha256>,
}

impl KalshiSigner {
    pub fn new(key_id: String, pem: &[u8]) -> Result<Self> {
        let pem_str = std::str::from_utf8(pem).context("key pem utf8")?;
        let rsa = RsaPrivateKey::from_pkcs8_pem(pem_str)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem_str))
            .context("parse kalshi private key")?;
        // salt_length = digest length (32) — matches cryptography's
        // PSS.DIGEST_LENGTH used by the Python signer
        Ok(Self { key_id, key: SigningKey::new(rsa) })
    }

    pub fn headers(&self, method: &str, path: &str) -> Vec<(String, String)> {
        let ts = now_ms();
        let msg = format!("{ts}{method}{path}");
        let sig = self.key.sign_with_rng(&mut rsa::rand_core::OsRng, msg.as_bytes());
        vec![
            ("KALSHI-ACCESS-KEY".into(), self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP".into(), ts),
            (
                "KALSHI-ACCESS-SIGNATURE".into(),
                base64::engine::general_purpose::STANDARD.encode(sig.to_bytes()),
            ),
        ]
    }
}

pub struct PmusSigner {
    key_id: String,
    key: ed25519_dalek::SigningKey,
}

impl PmusSigner {
    pub fn new(key_id: String, secret_key_b64: &str) -> Result<Self> {
        let blob = base64::engine::general_purpose::STANDARD
            .decode(secret_key_b64.trim())
            .context("pmus key b64")?;
        let seed: [u8; 32] = blob
            .get(..32)
            .context("pmus key too short")?
            .try_into()
            .expect("32-byte slice");
        Ok(Self { key_id, key: ed25519_dalek::SigningKey::from_bytes(&seed) })
    }

    pub fn headers(&self, method: &str, path: &str) -> Vec<(String, String)> {
        use ed25519_dalek::Signer;
        let ts = now_ms();
        let sig = self.key.sign(format!("{ts}{method}{path}").as_bytes());
        vec![
            ("X-PM-Access-Key".into(), self.key_id.clone()),
            ("X-PM-Timestamp".into(), ts),
            (
                "X-PM-Signature".into(),
                base64::engine::general_purpose::STANDARD.encode(sig.to_bytes()),
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Verifier;

    #[test]
    fn pmus_signature_verifies_and_uses_seed_prefix() {
        // 64-byte blob: seed || garbage — only the first 32 bytes are the seed
        let mut blob = [7u8; 64];
        blob[32..].fill(9);
        let b64 = base64::engine::general_purpose::STANDARD.encode(blob);
        let signer = PmusSigner::new("kid".into(), &b64).unwrap();
        let headers = signer.headers("GET", "/v1/ws/markets");
        let ts = &headers[1].1;
        let sig_b64 = &headers[2].1;
        let expected_key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let sig = ed25519_dalek::Signature::from_bytes(
            &base64::engine::general_purpose::STANDARD
                .decode(sig_b64)
                .unwrap()
                .try_into()
                .unwrap(),
        );
        expected_key
            .verifying_key()
            .verify(format!("{ts}GET/v1/ws/markets").as_bytes(), &sig)
            .expect("signature must verify against seed-derived key");
    }
}
