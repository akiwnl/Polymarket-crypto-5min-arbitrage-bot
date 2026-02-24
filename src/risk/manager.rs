use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::types::{B256, Decimal, U256};
use rust_decimal_macros::dec;
use tracing::{debug, error, info};

use super::positions::PositionTracker;
use super::recovery::{RecoveryAction, RecoveryStrategy};
use crate::config::Config as BotConfig;
use crate::trading::executor::OrderPairResult;

#[derive(Debug, Clone, PartialEq)]
pub enum PairStatus {
    Submitted,
    BothFilled,
    PartiallyFilled,
    OneFailed,
    BothFailed,
    Recovering,
}

#[derive(Debug, Clone)]
pub struct OrderPair {
    pub pair_id: String,
    pub market_id: B256,
    pub yes_order_id: String,
    pub no_order_id: String,
    pub yes_token_id: U256,
    pub no_token_id: U256,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub yes_filled: Decimal,
    pub no_filled: Decimal,
    pub status: PairStatus,
    pub created_at: DateTime<Utc>,
}

pub struct RiskManager {
    _clob_client: Option<Client<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>,
    pending_pairs: DashMap<String, OrderPair>,
    position_tracker: std::sync::Arc<PositionTracker>,
    recovery_strategy: RecoveryStrategy,
}

impl RiskManager {
    pub fn new(
        clob_client: Option<Client<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>,
        config: &BotConfig,
    ) -> Self {
        Self {
            _clob_client: clob_client,
            pending_pairs: DashMap::new(),
            position_tracker: std::sync::Arc::new(PositionTracker::new(
                Decimal::try_from(config.risk_max_exposure_usdc).unwrap_or(dec!(1000.0)),
            )),
            recovery_strategy: RecoveryStrategy::new(
                config.risk_imbalance_threshold,
                config.hedge_take_profit_pct,
                config.hedge_stop_loss_pct,
            ),
        }
    }

    /// Register a new order pair
    /// yes_price: YES order buy price
    /// no_price: NO order buy price
    pub fn register_order_pair(
        &self,
        result: OrderPairResult,
        market_id: B256,
        yes_token: U256,
        no_token: U256,
        yes_price: Decimal,
        no_price: Decimal,
    ) {
        let status = if result.yes_filled == result.yes_size && result.no_filled == result.no_size {
            PairStatus::BothFilled
        } else if result.yes_filled > dec!(0) && result.no_filled > dec!(0) {
            PairStatus::PartiallyFilled
        } else if result.yes_filled > dec!(0) && result.no_filled == dec!(0) {
            PairStatus::OneFailed
        } else if result.yes_filled == dec!(0) && result.no_filled > dec!(0) {
            PairStatus::OneFailed
        } else {
            PairStatus::BothFailed
        };

        let pair = OrderPair {
            pair_id: result.pair_id.clone(),
            market_id,
            yes_order_id: result.yes_order_id,
            no_order_id: result.no_order_id,
            yes_token_id: yes_token,
            no_token_id: no_token,
            yes_size: result.yes_size,
            no_size: result.no_size,
            yes_filled: result.yes_filled,
            no_filled: result.no_filled,
            status: status.clone(),
            created_at: Utc::now(),
        };

        // Update positions (exposure already increased by order cost during arbitrage execution, not updated by fill here)
        self.position_tracker.update_position(yes_token, pair.yes_filled);
        self.position_tracker.update_position(no_token, pair.no_filled);

        // This log is already printed in executor, no need to repeat here
        debug!(
            pair_id = %pair.pair_id,
            status = ?status,
            yes_filled = %pair.yes_filled,
            no_filled = %pair.no_filled,
            "registered order pair"
        );

        // Clone pair.pair_id for insertion since DashMap requires ownership
        self.pending_pairs.insert(pair.pair_id.clone(), pair);
    }

    /// Handle order pair and decide recovery strategy
    pub async fn handle_order_pair(&self, pair_id: &str) -> Result<RecoveryAction> {
        let pair = self
            .pending_pairs
            .get(pair_id)
            .ok_or_else(|| anyhow::anyhow!("order pair {} not found", pair_id))?
            .clone();

        match pair.status {
            PairStatus::BothFilled => {
                info!(pair_id = %pair.pair_id, "both orders fully filled, no recovery needed");
                Ok(RecoveryAction::None)
            }
            PairStatus::PartiallyFilled => {
                self.recovery_strategy
                    .handle_partial_fill(&pair, &self.position_tracker)
                    .await
            }
            PairStatus::OneFailed => {
                self.recovery_strategy
                    .handle_one_sided_fill(&pair, &self.position_tracker)
                    .await
            }
            PairStatus::BothFailed => {
                error!(
                    "❌ Arbitrage failed | both YES and NO orders unfilled, possible cause: price changed or insufficient liquidity"
                );
                Ok(RecoveryAction::ManualIntervention {
                    reason: "both orders failed".to_string(),
                })
            }
            _ => Ok(RecoveryAction::None),
        }
    }

    /// Get position tracker (Arc reference)
    pub fn position_tracker(&self) -> std::sync::Arc<PositionTracker> {
        self.position_tracker.clone()
    }
}
