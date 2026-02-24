use anyhow::Result;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

use super::discoverer::{MarketDiscoverer, MarketInfo};

pub struct MarketScheduler {
    discoverer: MarketDiscoverer,
    refresh_advance_secs: u64,
}

impl MarketScheduler {
    pub fn new(discoverer: MarketDiscoverer, refresh_advance_secs: u64) -> Self {
        Self {
            discoverer,
            refresh_advance_secs,
        }
    }

    /// Calculate wait time until next 5-minute window
    pub fn calculate_wait_time(&self, now: DateTime<Utc>) -> Duration {
        let next_window_ts = MarketDiscoverer::calculate_next_window_timestamp(now);
        let next_window = DateTime::from_timestamp(next_window_ts, 0)
            .expect("Invalid timestamp");

        // Query a few seconds early to ensure markets are created
        let wait_duration = next_window
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::ZERO)
            .saturating_sub(Duration::from_secs(self.refresh_advance_secs));

        wait_duration.max(Duration::ZERO)
    }

    /// Get markets for current window immediately, or wait for next window on failure
    pub async fn get_markets_immediately_or_wait(&self) -> Result<Vec<MarketInfo>> {
        // First try to get markets for current window
        let now = Utc::now();
        let current_timestamp = MarketDiscoverer::calculate_current_window_timestamp(now);
        let next_timestamp = MarketDiscoverer::calculate_next_window_timestamp(now);

        // If current and next windows are the same (theoretically won't happen for 5m), use wait logic
        if current_timestamp == next_timestamp {
            return self.wait_for_next_window().await;
        }

        info!("attempting to get markets for current window");
        match self.discoverer.get_markets_for_timestamp(current_timestamp).await {
            Ok(markets) => {
                if !markets.is_empty() {
                    info!(count = markets.len(), "found markets for current window");
                    return Ok(markets);
                }
                // No markets in current window: new markets may not be created yet, retry at short intervals (5m markets usually ready within seconds)
                // Calling wait_for_next_window directly would wait until next window boundary, causing current window to be skipped
                const RETRY_SECS: u64 = 2;
                const MAX_RETRY_SECS: u64 = 90; // Max retry ~90 seconds
                let mut elapsed = 0u64;
                while elapsed < MAX_RETRY_SECS {
                    info!("current window markets empty, retrying in {} seconds (waited {} seconds)", RETRY_SECS, elapsed);
                    sleep(Duration::from_secs(RETRY_SECS)).await;
                    elapsed += RETRY_SECS;
                    match self.discoverer.get_markets_for_timestamp(current_timestamp).await {
                        Ok(markets) if !markets.is_empty() => {
                            info!(count = markets.len(), "retry succeeded, found markets for current window");
                            return Ok(markets);
                        }
                        _ => {}
                    }
                }
                // Retry timeout, wait for next window
                warn!("no markets after {} seconds of retries, waiting for next window", MAX_RETRY_SECS);
                self.wait_for_next_window().await
            }
            Err(e) => {
                warn!(error = %e, "failed to get current window markets, waiting for next window");
                self.wait_for_next_window().await
            }
        }
    }

    /// Wait until next 5-minute window starts and get markets
    pub async fn wait_for_next_window(&self) -> Result<Vec<MarketInfo>> {
        loop {
            let wait_time = self.calculate_wait_time(Utc::now());
            if wait_time > Duration::ZERO {
                info!(
                    wait_secs = wait_time.as_secs(),
                    "waiting for next 5-minute window"
                );
                sleep(wait_time).await;
            }

            // Query markets for current window
            let now = Utc::now();
            let timestamp = MarketDiscoverer::calculate_current_window_timestamp(now);
            match self.discoverer.get_markets_for_timestamp(timestamp).await {
                Ok(markets) => {
                    if !markets.is_empty() {
                        info!(count = markets.len(), "found new markets");
                        return Ok(markets);
                    }
                    // If markets not yet created, wait and retry
                    info!("markets not yet created, waiting to retry...");
                    sleep(Duration::from_secs(2)).await;
                }
                Err(e) => {
                    error!(error = %e, "failed to get markets, retrying...");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}
