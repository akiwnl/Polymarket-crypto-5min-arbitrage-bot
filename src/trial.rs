//! License file authorization: the program only runs with a valid license.
//! Licenses are issued by the author as encrypted expiry timestamps. Deleting the license prevents execution; file deletion cannot reset the trial.

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default license filename (in the program's working directory or specified via env var)
const LICENSE_FILENAME: &str = "license.key";

/// Env var: license file path (optional). Falls back to license.key in the current directory
const LICENSE_PATH_ENV: &str = "POLY_15MIN_BOT_LICENSE";

/// Key derivation seed (used only for deriving the encryption key; same seed for license generation)
const TRIAL_KEY_SEED: &[u8] = b"poly_15min_bot_trial_seed_2025";

/// AES-GCM nonce length (12 bytes)
const NONCE_LEN: usize = 12;

/// Resolve license file path: prefer env var, otherwise license.key in the current directory
fn license_file_path() -> PathBuf {
    std::env::var(LICENSE_PATH_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(LICENSE_FILENAME))
}

/// Derive 256-bit key from seed (SHA-256)
fn derive_key() -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(TRIAL_KEY_SEED);
    let digest = hasher.finalize();
    digest.into()
}

/// Current Unix timestamp (seconds)
fn now_secs() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .context("system time error")
}

/// Encrypt a u64 timestamp: outputs base64(nonce || ciphertext) with authentication tag to prevent tampering.
fn encrypt_timestamp(ts_secs: u64) -> Result<String> {
    let key = derive_key();
    let cipher = Aes256Gcm::new_from_slice(&key).context("encryption initialization failed")?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let plaintext = ts_secs.to_le_bytes();
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_ref())
        .map_err(|e| anyhow::anyhow!("encryption failed: {}", e))?;
    let mut payload = nonce.to_vec();
    payload.extend_from_slice(&ciphertext);
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &payload,
    ))
}

/// Decrypt license/trial content, returns u64 timestamp. Returns error on decryption failure or tampering.
fn decrypt_timestamp(encoded: &str) -> Result<u64> {
    let payload = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        encoded.trim(),
    )
    .context("invalid license format (base64 decode failed)")?;
    if payload.len() < NONCE_LEN {
        anyhow::bail!("invalid or tampered license (data too short)");
    }
    let (nonce_bytes, ciphertext) = payload.split_at(NONCE_LEN);
    let nonce_arr: [u8; NONCE_LEN] = nonce_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid or tampered license (invalid nonce length)"))?;
    let nonce = Nonce::from(nonce_arr);
    let key = derive_key();
    let cipher = Aes256Gcm::new_from_slice(&key).context("decryption initialization failed")?;
    let plaintext = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|_| anyhow::anyhow!("invalid or tampered license (decryption or verification failed)"))?;
    if plaintext.len() != 8 {
        anyhow::bail!("invalid or tampered license (invalid content length)");
    }
    let mut bytes: [u8; 8] = [0; 8];
    bytes.copy_from_slice(&plaintext[..8]);
    Ok(u64::from_le_bytes(bytes))
}

/// Generate license string (encrypted expiry timestamp as base64).
/// For author use: generate license via gen_license binary or this function, write to file for trial users.
pub fn create_license(expiry_secs: u64) -> Result<String> {
    encrypt_timestamp(expiry_secs)
}

/// Verify license file: file must exist and not be expired, otherwise returns error.
/// Deleting the license prevents execution; file deletion cannot reset the trial.
pub fn check_license() -> Result<()> {
    let path = license_file_path();
    let now = now_secs()?;

    if !path.exists() {
        anyhow::bail!(
            "License file not found. Place the author-provided {} in the program directory, or set env var {} to specify the path.",
            LICENSE_FILENAME,
            LICENSE_PATH_ENV
        );
    }

    let content = fs::read_to_string(&path).context("failed to read license file")?;
    let expiry_secs = decrypt_timestamp(&content)?;

    if now >= expiry_secs {
        anyhow::bail!(
            "License expired. Contact the author for a new license to continue using the program."
        );
    }

    let remaining_secs = expiry_secs - now;
    tracing::info!(
        remaining_hours = (remaining_secs as f64) / 3600.0,
        "License valid, approximately {:.1} hours remaining",
        (remaining_secs as f64) / 3600.0
    );
    Ok(())
}
