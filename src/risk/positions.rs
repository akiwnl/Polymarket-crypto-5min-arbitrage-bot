use anyhow::Result;
use dashmap::DashMap;
use polymarket_client_sdk::types::{Decimal, U256};
use rust_decimal_macros::dec;
use tracing::{debug, info, trace};

use poly_5min_bot::positions::{get_positions, Position};

pub struct PositionTracker {
    positions: DashMap<U256, Decimal>, // token_id -> quantity (positive=long, negative=short)
    exposure_costs: DashMap<U256, Decimal>, // token_id -> cost (USD), used for tracking risk exposure
    max_exposure: Decimal,
}

impl PositionTracker {
    pub fn new(max_exposure: Decimal) -> Self {
        Self {
            positions: DashMap::new(),
            exposure_costs: DashMap::new(),
            max_exposure,
        }
    }

    pub fn update_position(&self, token_id: U256, delta: Decimal) {
        trace!("update_position: start | token_id:{} | delta:{}", token_id, delta);
        
        trace!("update_position: acquiring positions write lock");
        let mut entry = self.positions.entry(token_id).or_insert(dec!(0));
        trace!("update_position: positions write lock acquired");
        *entry += delta;
        trace!("update_position: position updated, new value:{}", *entry);

        // If position becomes 0 or near 0, clean up
        // Key fix: release positions write lock before accessing exposure_costs
        // This avoids deadlock with update_exposure_cost
        let should_remove = entry.abs() < dec!(0.0001);
        trace!("update_position: should_remove:{}", should_remove);
        if should_remove {
            *entry = dec!(0);
            trace!("update_position: position cleared to zero");
        }
        // Release positions lock
        drop(entry);
        trace!("update_position: positions write lock released");
        
        // Now safe to access exposure_costs
        if should_remove {
            trace!("update_position: removing exposure_costs");
            self.exposure_costs.remove(&token_id);
            trace!("update_position: exposure_costs removed");
        }
        
        trace!("update_position: done");
    }

    /// Update risk exposure cost (USD)
    /// price: buy price
    /// delta: position change (positive=buy, negative=sell)
    pub fn update_exposure_cost(&self, token_id: U256, price: Decimal, delta: Decimal) {
        trace!("update_exposure_cost: start | token_id:{} | price:{} | delta:{}", token_id, price, delta);
        
        if delta == dec!(0) {
            trace!("update_exposure_cost: delta is 0, returning");
            return; // No change, no update needed
        }
        
        trace!("update_exposure_cost: acquiring positions read lock");
        // Key fix: acquire positions read lock first, release it, then acquire exposure_costs write lock
        // This avoids deadlock with update_position (which acquires positions write lock first, then accesses exposure_costs)
        let current_pos = if delta < dec!(0) {
            trace!("update_exposure_cost: sell operation, acquiring positions read lock");
            // On sell, need current position to calculate ratio
            let pos = self.positions.get(&token_id);
            trace!("update_exposure_cost: positions read lock acquired");
            let result = pos.map(|v| *v.value()).unwrap_or(dec!(0));
            trace!("update_exposure_cost: positions read lock released, current_pos:{}", result);
            result
        } else {
            trace!("update_exposure_cost: buy operation, no positions lock needed");
            dec!(0) // Not needed for buy
        };
        
        trace!("update_exposure_cost: acquiring exposure_costs write lock");
        // positions lock released, safe to acquire exposure_costs write lock
        let mut entry = self.exposure_costs.entry(token_id).or_insert(dec!(0));
        trace!("update_exposure_cost: exposure_costs write lock acquired");
        
        if delta > dec!(0) {
            trace!("update_exposure_cost: buy branch, calculating cost_delta");
            // Buy: increase risk exposure (cost = price * quantity)
            let cost_delta = price * delta;
            *entry += cost_delta;
            trace!("update_exposure_cost: buy done, new cost:{}", *entry);
        } else {
            trace!("update_exposure_cost: sell branch, current_pos:{}", current_pos);
            // Sell: reduce risk exposure (proportional reduction)
            if current_pos > dec!(0) {
                trace!("update_exposure_cost: calculating sell ratio");
                // Calculate sell ratio
                let sell_amount = (-delta).min(current_pos);
                let reduction_ratio = sell_amount / current_pos;
                trace!("update_exposure_cost: sell_amount:{} | reduction_ratio:{} | current cost:{}", sell_amount, reduction_ratio, *entry);
                // Reduce cost proportionally
                *entry = (*entry * (dec!(1) - reduction_ratio)).max(dec!(0));
                trace!("update_exposure_cost: sell done, new cost:{}", *entry);
            } else {
                trace!("update_exposure_cost: current_pos is 0, clearing to zero");
                *entry = dec!(0);
            }
        }
        
        trace!("update_exposure_cost: checking if cleanup needed, current cost:{}", *entry);
        // If cost near 0, clean up
        if *entry < dec!(0.01) {
            trace!("update_exposure_cost: cost near 0, cleaning up");
            *entry = dec!(0);
            drop(entry); // Explicitly release write lock
            trace!("update_exposure_cost: write lock released, removing");
            self.exposure_costs.remove(&token_id);
            trace!("update_exposure_cost: remove done");
        } else {
            trace!("update_exposure_cost: cost non-zero, keeping entry");
            drop(entry); // Explicitly release write lock
        }
        
        trace!("update_exposure_cost: done");
    }

    /// Get max risk exposure limit
    pub fn max_exposure(&self) -> Decimal {
        self.max_exposure
    }

    /// Reset risk exposure (called at start of new round, clears cost cache for re-accumulation from 0)
    pub fn reset_exposure(&self) {
        self.exposure_costs.clear();
        info!("🔄 Risk exposure reset (new round)");
    }

    pub fn get_position(&self, token_id: U256) -> Decimal {
        self.positions
            .get(&token_id)
            .map(|v| *v.value())
            .unwrap_or(dec!(0))
    }

    /// Calculate position imbalance (0.0 = fully balanced, 1.0 = fully imbalanced)
    pub fn calculate_imbalance(&self, yes_token: U256, no_token: U256) -> Decimal {
        let yes_pos = self.get_position(yes_token);
        let no_pos = self.get_position(no_token);

        let total = yes_pos + no_pos;
        if total == dec!(0) {
            return dec!(0); // Fully balanced
        }

        // Imbalance = abs(yes - no) / (yes + no)
        let imbalance = (yes_pos - no_pos).abs() / total;
        imbalance
    }

    /// Calculate current total risk exposure (USD)
    /// Based on total cost of all positions
    pub fn calculate_exposure(&self) -> Decimal {
        // Calculate total risk exposure (sum of all position costs)
        // Collect to Vec first to avoid holding the lock for too long
        let costs: Vec<Decimal> = self.exposure_costs
            .iter()
            .map(|entry| *entry.value())
            .collect();
        costs.iter().sum()
    }

    pub fn is_within_limits(&self) -> bool {
        self.calculate_exposure() <= self.max_exposure
    }

    /// Check if executing a new order would exceed risk exposure limit
    /// yes_cost: YES order cost (price * quantity)
    /// no_cost: NO order cost (price * quantity)
    pub fn would_exceed_limit(&self, yes_cost: Decimal, no_cost: Decimal) -> bool {
        let current_exposure = self.calculate_exposure();
        let new_order_cost = yes_cost + no_cost;
        (current_exposure + new_order_cost) > self.max_exposure
    }

    /// Get all positions (token_id -> quantity)
    pub fn get_all_positions(&self) -> Vec<(U256, Decimal)> {
        self.positions
            .iter()
            .filter(|entry| *entry.value() > dec!(0))
            .map(|entry| (*entry.key(), *entry.value()))
            .collect()
    }

    /// Get YES and NO positions
    pub fn get_pair_positions(&self, yes_token: U256, no_token: U256) -> (Decimal, Decimal) {
        (self.get_position(yes_token), self.get_position(no_token))
    }

    /// Sync positions from Data API, fully overwriting local cache
    /// This method fetches latest positions from API, clears and rebuilds local positions map
    /// Used for scheduled sync to ensure local cache matches on-chain positions
    pub async fn sync_from_api(&self) -> Result<Vec<Position>> {
        use std::collections::HashMap;
        use polymarket_client_sdk::types::B256;
        
        let positions = get_positions().await?;
        
        // Clear existing positions (exposure only increases during arbitrage execution, decreases during merge, not backfilled from API)
        self.positions.clear();
        
        // Update local cache with positions from API
        let mut updated_count = 0;
        let mut valid_positions = Vec::new();
        
        for pos in positions {
            if pos.size > dec!(0) {
                // Position.asset is the token_id
                self.positions.insert(pos.asset, pos.size);
                valid_positions.push(pos);
                updated_count += 1;
            }
        }
        
        // Print positions grouped by market
        if !valid_positions.is_empty() {
            let mut by_market: HashMap<B256, Vec<&Position>> = HashMap::new();
            for pos in &valid_positions {
                by_market.entry(pos.condition_id).or_default().push(pos);
            }
            
            info!("📊 Position sync complete | {} positions, {} markets", updated_count, by_market.len());
            
            // Print by market, one line per market
            for (_condition_id, market_positions) in by_market.iter() {
                let mut yes_pos = dec!(0);
                let mut no_pos = dec!(0);
                let mut market_title = "";
                
                for pos in market_positions {
                    if pos.outcome_index == 0 {
                        yes_pos = pos.size;
                    } else if pos.outcome_index == 1 {
                        no_pos = pos.size;
                    }
                    if market_title.is_empty() {
                        market_title = &pos.title;
                    }
                }
                
                // Truncate long titles
                let title_display = if market_title.len() > 40 {
                    format!("{}...", &market_title[..37])
                } else {
                    market_title.to_string()
                };
                
                info!(
                    "  📈 {} | YES:{} NO:{}",
                    title_display,
                    yes_pos,
                    no_pos
                );
            }
        } else {
            info!("📊 Position sync complete | no current positions");
        }
        
        Ok(valid_positions)
    }
}
