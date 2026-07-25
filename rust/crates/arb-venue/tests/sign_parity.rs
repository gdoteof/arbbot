//! Signing + header-composition parity against the Python-produced fixtures.
//!
//! - Ed25519 (PM-US) is deterministic → byte-compare the Rust signature.
//! - RSA-PSS (Kalshi) is salted → (a) verify the Python signature in Rust,
//!   (b) sign in Rust and self-verify, (c) write one Rust signature to
//!   target/tmp for `scripts/verify_rust_sigs.py` to verify with the Python
//!   stack.
//! - Header names/order/values must byte-match the Python clients.

use std::collections::BTreeSet;
use std::path::PathBuf;

use base64::Engine as _;
use serde_json::Value;

use arb_venue::sign::{
    canonical_message, KALSHI_HDR_KEY, KALSHI_HDR_SIG, KALSHI_HDR_TS, PMUS_HDR_CT, PMUS_HDR_KEY,
    PMUS_HDR_SIG, PMUS_HDR_TS,
};
use arb_venue::{KalshiSigner, KalshiVerifier, PmusSigner};

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn fixture() -> Value {
    let raw = include_str!("fixtures/venue/sigs.json");
    serde_json::from_str(raw).expect("fixture parses")
}

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or_else(|| panic!("missing str {k}"))
}

// ------------------------------------------------------------------ Kalshi ---

#[test]
fn kalshi_message_composition_matches_fixture() {
    let fx = fixture();
    for case in fx["kalshi"]["cases"].as_array().unwrap() {
        let got = canonical_message(s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
        assert_eq!(got, s(case, "message"), "message = ts+METHOD+path");
    }
}

#[test]
fn kalshi_rust_verifies_python_pss_signature() {
    let fx = fixture();
    let verifier =
        KalshiVerifier::from_public_key_pem(s(&fx["kalshi"], "public_key_spki_pem")).unwrap();
    for case in fx["kalshi"]["cases"].as_array().unwrap() {
        let sig = B64.decode(s(case, "signature_b64")).unwrap();
        assert!(
            verifier.verify(s(case, "timestamp_ms"), s(case, "method"), s(case, "path"), &sig),
            "Rust must verify the Python RSA-PSS signature for `{}`",
            s(case, "name"),
        );
        // wrong message must NOT verify (guards against a no-op verifier)
        assert!(!verifier.verify("0", "GET", "/wrong", &sig));
    }
}

#[test]
fn kalshi_rust_sign_then_self_verify() {
    let fx = fixture();
    let signer =
        KalshiSigner::from_pkcs8_pem(s(&fx["kalshi"], "api_key_id"), s(&fx["kalshi"], "private_key_pkcs8_pem"))
            .unwrap();
    let verifier =
        KalshiVerifier::from_public_key_pem(s(&fx["kalshi"], "public_key_spki_pem")).unwrap();
    for case in fx["kalshi"]["cases"].as_array().unwrap() {
        let (ts, m, p) = (s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
        let sig = signer.sign(ts, m, p);
        assert!(verifier.verify(ts, m, p, &sig), "Rust sig self-verifies");
    }
}

#[test]
fn kalshi_header_composition_matches_python() {
    let fx = fixture();
    let kid = s(&fx["kalshi"], "api_key_id");
    let signer = KalshiSigner::from_pkcs8_pem(kid, s(&fx["kalshi"], "private_key_pkcs8_pem")).unwrap();
    let verifier =
        KalshiVerifier::from_public_key_pem(s(&fx["kalshi"], "public_key_spki_pem")).unwrap();
    for case in fx["kalshi"]["cases"].as_array().unwrap() {
        let (ts, m, p) = (s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
        let hdrs = signer.headers(ts, m, p);
        // exact names, in Python dict-insertion order
        let names: Vec<&str> = hdrs.iter().map(|(k, _)| *k).collect();
        assert_eq!(names, vec![KALSHI_HDR_KEY, KALSHI_HDR_TS, KALSHI_HDR_SIG]);
        // Python emitted the same header key set
        let py_keys: BTreeSet<&str> =
            case["headers"].as_object().unwrap().keys().map(String::as_str).collect();
        let rust_keys: BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(py_keys, rust_keys, "header name set matches Python");
        // KEY and TIMESTAMP values byte-match Python (signature is salted → verify)
        assert_eq!(hdrs[0].1, kid);
        assert_eq!(hdrs[1].1, ts);
        assert_eq!(hdrs[1].1, s(&case["headers"], "KALSHI-ACCESS-TIMESTAMP"));
        let sig = B64.decode(&hdrs[2].1).unwrap();
        assert!(verifier.verify(ts, m, p, &sig), "signature header verifies");
    }
}

/// (c) Emit one Rust-produced RSA-PSS signature for the cross-check by
/// `scripts/verify_rust_sigs.py` (Python verifies it).
#[test]
fn kalshi_emit_rust_signature_for_python_crosscheck() {
    let fx = fixture();
    let signer = KalshiSigner::from_pkcs8_pem(
        s(&fx["kalshi"], "api_key_id"),
        s(&fx["kalshi"], "private_key_pkcs8_pem"),
    )
    .unwrap();
    let case = &fx["kalshi"]["cases"][0]; // "place"
    let (ts, m, p) = (s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
    let sig = signer.sign(ts, m, p);
    let out = serde_json::json!({
        "message": canonical_message(ts, m, p),
        "signature_b64": B64.encode(&sig),
        "public_key_spki_pem": s(&fx["kalshi"], "public_key_spki_pem"),
    });
    // workspace target dir: <crate>/../../target/tmp
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.push("../../target/tmp");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("rust_kalshi_sig.json"), serde_json::to_vec_pretty(&out).unwrap())
        .unwrap();
}

// ------------------------------------------------------------------- PM-US ---

#[test]
fn pmus_message_composition_matches_fixture() {
    let fx = fixture();
    for case in fx["pmus"]["cases"].as_array().unwrap() {
        let got = canonical_message(s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
        assert_eq!(got, s(case, "message"));
    }
}

#[test]
fn pmus_ed25519_signature_byte_matches_python() {
    let fx = fixture();
    let seed = hex::decode(s(&fx["pmus"], "seed_hex")).unwrap();
    let mut seed32 = [0u8; 32];
    seed32.copy_from_slice(&seed);
    let signer = PmusSigner::from_seed(s(&fx["pmus"], "api_key_id"), &seed32);
    for case in fx["pmus"]["cases"].as_array().unwrap() {
        let sig = signer.sign(s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
        assert_eq!(
            hex::encode(sig),
            s(case, "signature_hex"),
            "deterministic Ed25519 sig must byte-match Python for `{}`",
            s(case, "name"),
        );
        assert_eq!(B64.encode(sig), s(case, "signature_b64"));
    }
}

#[test]
fn pmus_header_composition_matches_python() {
    let fx = fixture();
    let seed = hex::decode(s(&fx["pmus"], "seed_hex")).unwrap();
    let mut seed32 = [0u8; 32];
    seed32.copy_from_slice(&seed);
    let kid = s(&fx["pmus"], "api_key_id");
    let signer = PmusSigner::from_seed(kid, &seed32);
    for case in fx["pmus"]["cases"].as_array().unwrap() {
        let (ts, m, p) = (s(case, "timestamp_ms"), s(case, "method"), s(case, "path"));
        let hdrs = signer.headers(ts, m, p);
        let names: Vec<&str> = hdrs.iter().map(|(k, _)| *k).collect();
        assert_eq!(names, vec![PMUS_HDR_KEY, PMUS_HDR_TS, PMUS_HDR_SIG, PMUS_HDR_CT]);
        let py_keys: BTreeSet<&str> =
            case["headers"].as_object().unwrap().keys().map(String::as_str).collect();
        let rust_keys: BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(py_keys, rust_keys, "header name set matches Python");
        // all four header values byte-match Python (Ed25519 is deterministic)
        assert_eq!(hdrs[0].1, s(&case["headers"], "X-PM-Access-Key"));
        assert_eq!(hdrs[1].1, s(&case["headers"], "X-PM-Timestamp"));
        assert_eq!(hdrs[2].1, s(&case["headers"], "X-PM-Signature"));
        assert_eq!(hdrs[3].1, s(&case["headers"], "Content-Type"));
        assert_eq!(hdrs[3].1, "application/json");
    }
}

#[test]
fn pmus_from_secret_b64_matches_seed_derivation() {
    // Python derives the seed as b64decode(secret)[:32]; a 32-byte secret
    // round-trips. Assert the two constructors agree.
    let fx = fixture();
    let seed = hex::decode(s(&fx["pmus"], "seed_hex")).unwrap();
    let mut seed32 = [0u8; 32];
    seed32.copy_from_slice(&seed);
    let secret_b64 = B64.encode(seed32);
    let a = PmusSigner::from_seed("k", &seed32);
    let b = PmusSigner::from_secret_b64("k", &secret_b64).unwrap();
    assert_eq!(a.sign("1", "GET", "/x"), b.sign("1", "GET", "/x"));
}
