//! License generation tool (author use only): generate license.key content based on expiry time.
//!
//! Usage examples:
//!   cargo run --bin gen_license -- --hours 24
//!   cargo run --bin gen_license -- --until "2025-02-03 00:00:00"
//!   cargo run --bin gen_license -- --hours 24 --out license.key

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut hours: Option<u64> = None;
    let mut until: Option<String> = None;
    let mut out_path: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--hours" => {
                i += 1;
                hours = Some(
                    args.get(i)
                        .context("--hours requires an argument")?
                        .parse()
                        .context("--hours must be a positive integer")?,
                );
                i += 1;
            }
            "--until" => {
                i += 1;
                until = Some(
                    args.get(i)
                        .context("--until requires an argument (e.g. 2025-02-03 00:00:00)")?
                        .clone(),
                );
                i += 1;
            }
            "--out" => {
                i += 1;
                out_path = Some(
                    args.get(i)
                        .context("--out requires an argument")?
                        .into(),
                );
                i += 1;
            }
            _ => {
                eprintln!("Usage: gen_license --hours <N> | --until \"<datetime>\" [--out license.key]");
                eprintln!("  --hours N    Expires N hours from now");
                eprintln!("  --until \"...\" Specify expiry time (UTC), format: 2025-02-03 00:00:00");
                eprintln!("  --out FILE   Write to file, outputs to stdout if not specified");
                std::process::exit(1);
            }
        }
    }

    let expiry_secs: u64 = if let Some(h) = hours {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now + h * 3600
    } else if let Some(ref dt_str) = until {
        let dt_utc: DateTime<Utc> = DateTime::parse_from_rfc3339(dt_str)
            .map(|d| d.with_timezone(&Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S")
                    .map(|n| n.and_utc())
            })
            .context("Failed to parse --until time, use 2025-02-03 00:00:00 or RFC3339 format")?;
        dt_utc.timestamp() as u64
    } else {
        anyhow::bail!("Please specify --hours <N> or --until \"<datetime>\"");
    };

    let license = poly_5min_bot::trial::create_license(expiry_secs)?;

    if let Some(path) = out_path {
        fs::write(&path, &license).context("Failed to write license file")?;
        eprintln!("Written to: {}", path.display());
    } else {
        io::stdout().write_all(license.as_bytes())?;
        io::stdout().flush()?;
    }
    Ok(())
}
