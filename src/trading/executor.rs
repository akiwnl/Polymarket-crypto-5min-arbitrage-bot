use anyhow::Result;
use alloy::signers::Signer;
use alloy::signers::local::LocalSigner;
use chrono::Utc;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::types::{Address, Decimal, U256};
use polymarket_client_sdk::POLYGON;
use rust_decimal_macros::dec;
use std::str::FromStr;
use std::time::Instant;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::monitor::arbitrage::ArbitrageOpportunity;

pub struct OrderPairResult {
    pub pair_id: String,
    pub yes_order_id: String,
    pub no_order_id: String,
    pub yes_filled: Decimal,
    pub no_filled: Decimal,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub success: bool,
}

pub struct TradingExecutor {
    client: Option<Client<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>>,
    private_key: String,
    max_order_size: Decimal,
    slippage: [Decimal; 2], // [first, second], down side uses second, up/flat uses first
    gtd_expiration_secs: u64,
    arbitrage_order_type: OrderType,
    dry_run: bool,
}

impl TradingExecutor {
    pub async fn new(
        private_key: String,
        max_order_size_usdc: f64,
        proxy_address: Option<Address>,
        slippage: [f64; 2],
        gtd_expiration_secs: u64,
        arbitrage_order_type: OrderType,
        dry_run: bool,
    ) -> Result<Self> {
        if dry_run {
            info!("[DRY RUN] Trading executor started in simulation mode, no real trades will be executed");
            return Ok(Self {
                client: None,
                private_key,
                max_order_size: Decimal::try_from(max_order_size_usdc)
                    .unwrap_or(rust_decimal_macros::dec!(100.0)),
                slippage: [
                    Decimal::try_from(slippage[0]).unwrap_or(dec!(0.0)),
                    Decimal::try_from(slippage[1]).unwrap_or(dec!(0.01)),
                ],
                gtd_expiration_secs,
                arbitrage_order_type,
                dry_run,
            });
        }

        // Validate private key format
        let signer = LocalSigner::from_str(&private_key)
            .map_err(|e| anyhow::anyhow!("invalid private key format: {}. Ensure the key is a 64-char hex string (without 0x prefix)", e))?
            .with_chain_id(Some(POLYGON));

        let config = Config::builder().use_server_time(false).build();
        let mut auth_builder = Client::new("https://clob.polymarket.com", config)
            .map_err(|e| anyhow::anyhow!("failed to create CLOB client: {}", e))?
            .authentication_builder(&signer);

        // If proxy_address provided, set funder and signature_type (following Python SDK pattern)
        if let Some(funder) = proxy_address {
            auth_builder = auth_builder
                .funder(funder)
                .signature_type(SignatureType::Proxy);
        }

        let client = auth_builder
            .authenticate()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "API authentication failed: {}. Possible causes: 1) invalid private key 2) network issue 3) Polymarket API unavailable",
                    e
                )
            })?;

        Ok(Self {
            client: Some(client),
            private_key,
            max_order_size: Decimal::try_from(max_order_size_usdc)
                .unwrap_or(rust_decimal_macros::dec!(100.0)),
            slippage: [
                Decimal::try_from(slippage[0]).unwrap_or(dec!(0.0)),
                Decimal::try_from(slippage[1]).unwrap_or(dec!(0.01)),
            ],
            gtd_expiration_secs,
            arbitrage_order_type,
            dry_run,
        })
    }

    /// Verify authentication succeeded - using api_keys() per official example
    pub async fn verify_authentication(&self) -> Result<()> {
        if self.dry_run {
            info!("[DRY RUN] Skipping authentication verification");
            return Ok(());
        }
        // Per official example, use api_keys() to verify authentication status
        self.client.as_ref().unwrap().api_keys().await
            .map_err(|e| anyhow::anyhow!("authentication verification failed: API call returned error: {}", e))?;
        Ok(())
    }

    /// Cancel all pending orders for this account (used during wind-down)
    pub async fn cancel_all_orders(&self) -> Result<()> {
        if self.dry_run {
            info!("[DRY RUN] Simulating cancel all pending orders");
            return Ok(());
        }
        self.client.as_ref().unwrap()
            .cancel_all_orders()
            .await
            .map_err(|e| anyhow::anyhow!("failed to cancel all pending orders: {}", e))?;
        Ok(())
    }

    /// Place GTC sell order at specified price (market-intent sell for single-leg position during wind-down)
    pub async fn sell_at_price(
        &self,
        token_id: U256,
        price: Decimal,
        size: Decimal,
    ) -> Result<()> {
        if self.dry_run {
            info!(
                "[DRY RUN] Simulated sell | token_id={:#x} | price:{:.4} | size:{}",
                token_id, price, size
            );
            return Ok(());
        }
        let client = self.client.as_ref().unwrap();
        let signer = LocalSigner::from_str(&self.private_key)?
            .with_chain_id(Some(POLYGON));
        let order = client
            .limit_order()
            .token_id(token_id)
            .side(Side::Sell)
            .price(price)
            .size(size)
            .order_type(OrderType::GTC)
            .build()
            .await?;
        let signed = client.sign(&signer, order).await?;
        client
            .post_order(signed)
            .await
            .map_err(|e| anyhow::anyhow!("sell order submission failed: {}", e))?;
        Ok(())
    }

    /// Get slippage by direction: down(↓) uses second, up(↑) and flat use first
    fn slippage_for_direction(&self, dir: &str) -> Decimal {
        if dir == "↓" {
            self.slippage[1]
        } else {
            self.slippage[0]
        }
    }

    /// Execute arbitrage trade (batch submit YES and NO orders via post_orders; order type configured by arbitrage_order_type, GTD uses gtd_expiration_secs)
    /// yes_dir / no_dir: price direction "↑" "↓" "−" or "", used to assign slippage by direction (down=second, up/flat=first)
    pub async fn execute_arbitrage_pair(
        &self,
        opp: &ArbitrageOpportunity,
        yes_dir: &str,
        no_dir: &str,
    ) -> Result<OrderPairResult> {
        // Performance timing: total start
        let total_start = Instant::now();
        
        // This log is already printed in main.rs, no need to repeat here
        let expiry_info = if matches!(self.arbitrage_order_type, OrderType::GTD) {
            format!("expiry:{}s", self.gtd_expiration_secs)
        } else {
            "no expiry".to_string()
        };
        debug!(
            market_id = %opp.market_id,
            profit_pct = %opp.profit_percentage,
            order_type = %self.arbitrage_order_type,
            "starting arbitrage trade (batch orders, type:{}, {})",
            self.arbitrage_order_type,
            expiry_info
        );

        // Calculate actual order size (considering max order limit)
        let yes_token_id = U256::from_str(&opp.yes_token_id.to_string())?;
        let no_token_id = U256::from_str(&opp.no_token_id.to_string())?;

        let order_size = opp.yes_size.min(opp.no_size).min(self.max_order_size);

        // Generate order pair ID
        let pair_id = Uuid::new_v4().to_string();

        // Calculate expiration: current time + configured expiration
        let expiration = Utc::now() + chrono::Duration::seconds(self.gtd_expiration_secs as i64);

        // Slippage by direction: up=first, down/flat=second
        let yes_slippage_apply = self.slippage_for_direction(yes_dir);
        let no_slippage_apply = self.slippage_for_direction(no_dir);
        let yes_price_with_slippage = (opp.yes_ask_price + yes_slippage_apply).min(dec!(1.0));
        let no_price_with_slippage = (opp.no_ask_price + no_slippage_apply).min(dec!(1.0));
        
        // Print level selection info (price with slippage)
        info!(
            "📋 Level | YES {:.4}×{:.2} NO {:.4}×{:.2}",
            yes_price_with_slippage, order_size,
            no_price_with_slippage, order_size
        );
        
        let expiry_suffix = if matches!(self.arbitrage_order_type, OrderType::GTD) {
            format!(" | GTD {}s", self.gtd_expiration_secs)
        } else {
            String::new()
        };
        info!(
            "📤 Order | YES {:.4}→{:.4}×{} NO {:.4}→{:.4}×{} | {}{}",
            opp.yes_ask_price, yes_price_with_slippage, order_size,
            opp.no_ask_price, no_price_with_slippage, order_size,
            self.arbitrage_order_type, expiry_suffix
        );

        // Pre-order check: both sides must be > $1 (exchange minimum)
        let yes_amount_usd = yes_price_with_slippage * order_size;
        let no_amount_usd = no_price_with_slippage * order_size;
        if yes_amount_usd <= dec!(1) || no_amount_usd <= dec!(1) {
            warn!(
                "⏭️ Skipping order | YES amount:{:.2} USD NO amount:{:.2} USD | both sides must be > $1",
                yes_amount_usd, no_amount_usd
            );
            return Err(anyhow::anyhow!(
                "order amount below exchange minimum: YES {:.2} USD, NO {:.2} USD, both sides must be > $1",
                yes_amount_usd, no_amount_usd
            ));
        }

        // Dry run: simulate full fill, no actual orders
        if self.dry_run {
            info!(
                "[DRY RUN] Simulated arbitrage | market:{} | YES price:{:.4} (with slippage:{:.4}) | NO price:{:.4} (with slippage:{:.4}) | size:{} | order type:{}",
                opp.market_id,
                opp.yes_ask_price, yes_price_with_slippage,
                opp.no_ask_price, no_price_with_slippage,
                order_size,
                self.arbitrage_order_type
            );
            let yes_order_id = format!("dry-run-yes-{}", &pair_id[..8]);
            let no_order_id = format!("dry-run-no-{}", &pair_id[..8]);
            return Ok(OrderPairResult {
                pair_id,
                yes_order_id,
                no_order_id,
                yes_filled: order_size,
                no_filled: order_size,
                yes_size: order_size,
                no_size: order_size,
                success: true,
            });
        }

        let client = self.client.as_ref().unwrap();

        // Performance timing: parallel YES/NO order building start
        let build_start = Instant::now();

        // Build YES and NO orders in parallel; only set expiration for GTD (SDK requires no expiry for non-GTD)
        let (yes_order, no_order) = tokio::join!(
            async {
                let b = client
                    .limit_order()
                    .token_id(yes_token_id)
                    .side(Side::Buy)
                    .price(yes_price_with_slippage)
                    .size(order_size)
                    .order_type(self.arbitrage_order_type.clone());
                if matches!(&self.arbitrage_order_type, OrderType::GTD) {
                    b.expiration(expiration).build().await
                } else {
                    b.build().await
                }
            },
            async {
                let b = client
                    .limit_order()
                    .token_id(no_token_id)
                    .side(Side::Buy)
                    .price(no_price_with_slippage)
                    .size(order_size)
                    .order_type(self.arbitrage_order_type.clone());
                if matches!(&self.arbitrage_order_type, OrderType::GTD) {
                    b.expiration(expiration).build().await
                } else {
                    b.build().await
                }
            }
        );
        
        let yes_order = yes_order?;
        let no_order = no_order?;
        let build_elapsed = build_start.elapsed().as_millis();

        // Performance timing: parallel signing start
        let sign_start = Instant::now();
        
        // Create signer
        let signer = LocalSigner::from_str(&self.private_key)?
            .with_chain_id(Some(POLYGON));
        
        // Sign YES and NO orders in parallel
        let (signed_yes_result, signed_no_result) = tokio::join!(
            client.sign(&signer, yes_order),
            client.sign(&signer, no_order)
        );
        
        let signed_yes = signed_yes_result?;
        let signed_no = signed_no_result?;
        let sign_elapsed = sign_start.elapsed().as_millis();

        // Performance timing: order send start
        let send_start = Instant::now();
        
        // Higher unit price first; parse yes_result/no_result from results in same order after submission
        let yes_first = yes_price_with_slippage >= no_price_with_slippage;
        let orders_to_send: Vec<_> = if yes_first {
            vec![signed_yes, signed_no]
        } else {
            vec![signed_no, signed_yes]
        };
        let results = match client.post_orders(orders_to_send).await {
            Ok(results) => {
                let send_elapsed = send_start.elapsed().as_millis();
                let total_elapsed = total_start.elapsed().as_millis();
                
                info!(
                    "⏱️ Timing | {} | build {}ms sign {}ms send {}ms total {}ms",
                    &pair_id[..8], build_elapsed, sign_elapsed, send_elapsed, total_elapsed
                );
                
                results
            }
            Err(e) => {
                let send_elapsed = send_start.elapsed().as_millis();
                let total_elapsed = total_start.elapsed().as_millis();
                
                error!(
                    "❌ Batch order API call failed | pair ID:{} | YES price:{} (with slippage) | NO price:{} (with slippage) | size:{} | build:{}ms | sign:{}ms | send:{}ms | total:{}ms | error:{}",
                    &pair_id[..8],
                    yes_price_with_slippage,
                    no_price_with_slippage,
                    order_size,
                    build_elapsed,
                    sign_elapsed,
                    send_elapsed,
                    total_elapsed,
                    e
                );
                return Err(anyhow::anyhow!("batch order API call failed: {}", e));
            }
        };
        
        // Verify result count
        if results.len() != 2 {
            error!(
                "❌ Batch order returned incorrect result count | pair ID:{} | expected:2 | actual:{}",
                &pair_id[..8],
                results.len()
            );
            return Err(anyhow::anyhow!(
                "batch order returned incorrect result count | expected:2 | actual:{}",
                results.len()
            ));
        }
        
        // Extract YES and NO order results (higher unit price submitted first, map using yes_first)
        let (yes_result, no_result) = if yes_first {
            (&results[0], &results[1])
        } else {
            (&results[1], &results[0])
        };

        // Order result details removed, only key info kept in subsequent logs

        // Check fill amounts (key metric for GTD orders)
        let yes_filled = yes_result.taking_amount;
        let no_filled = no_result.taking_amount;

        // For GTD orders, if not fully filled within 90s, orders cancel on expiry
        // Check actual fill amounts instead of success field
        // Only return error when both orders have zero fills
        if yes_filled == dec!(0) && no_filled == dec!(0) {
            // Extract simplified error messages
            let yes_error_msg = yes_result
                .error_msg
                .as_deref()
                .unwrap_or("unknown error");
            let no_error_msg = no_result
                .error_msg
                .as_deref()
                .unwrap_or("unknown error");
            
            // Simplify error messages, remove technical details
            let yes_error_simple = if yes_error_msg.contains("no orders found to match") {
                "no matching orders in orderbook"
            } else if yes_error_msg.contains("GTD") || yes_error_msg.contains("FOK") || yes_error_msg.contains("FAK") || yes_error_msg.contains("GTC") {
                "order could not be filled"
            } else {
                yes_error_msg
            };
            
            let no_error_simple = if no_error_msg.contains("no orders found to match") {
                "no matching orders in orderbook"
            } else if no_error_msg.contains("GTD") || no_error_msg.contains("FOK") || no_error_msg.contains("FAK") || no_error_msg.contains("GTC") {
                "order could not be filled"
            } else {
                no_error_msg
            };

            error!(
                "❌ Arbitrage trade failed | pair ID:{} | YES order:{} | NO order:{}",
                &pair_id[..8], // Show first 8 chars only
                yes_error_simple,
                no_error_simple
            );

            // Detailed error info logged at debug level
            debug!(
                pair_id = %pair_id,
                yes_order_id = ?yes_result.order_id,
                no_order_id = ?no_result.order_id,
                yes_success = yes_result.success,
                no_success = no_result.success,
                yes_error = %yes_error_msg,
                no_error = %no_error_msg,
                "both orders unfilled (details)"
            );

            return Err(anyhow::anyhow!(
                "arbitrage failed: both YES and NO orders unfilled | YES: {}, NO: {}",
                yes_error_simple,
                no_error_simple
            ));
        }

        // If at least one order filled, log warning but don't return error
        // Let subsequent risk manager handle one-sided fill
        if !yes_result.success || !no_result.success {
            let yes_error_msg = yes_result
                .error_msg
                .as_deref()
                .unwrap_or("unknown error");
            let no_error_msg = no_result
                .error_msg
                .as_deref()
                .unwrap_or("unknown error");

            // Simplify error messages
            let yes_error_simple = if yes_error_msg.contains("no orders found to match") {
                "partially unfilled (pending)"
            } else if yes_error_msg.contains("GTD") || yes_error_msg.contains("FOK") || yes_error_msg.contains("FAK") || yes_error_msg.contains("GTC") {
                "partially unfilled (pending)"
            } else {
                "status abnormal"
            };
            
            let no_error_simple = if no_error_msg.contains("no orders found to match") {
                "partially unfilled (pending)"
            } else if no_error_msg.contains("GTD") || no_error_msg.contains("FOK") || no_error_msg.contains("FAK") || no_error_msg.contains("GTC") {
                "partially unfilled (pending)"
            } else {
                "status abnormal"
            };

            warn!(
                "⚠️ Partial order status abnormal | pair ID:{} | YES:{} (filled:{} shares) | NO:{} (filled:{} shares) | risk management activated",
                &pair_id[..8],
                yes_error_simple,
                yes_filled,
                no_error_simple,
                no_filled
            );

            // Detailed error info logged at debug level
            debug!(
                pair_id = %pair_id,
                yes_order_id = ?yes_result.order_id,
                no_order_id = ?no_result.order_id,
                yes_success = yes_result.success,
                no_success = no_result.success,
                yes_error = %yes_error_msg,
                no_error = %no_error_msg,
                "order submission status abnormal details"
            );
        }

        // Print different logs based on fill status
        if yes_filled > dec!(0) && no_filled > dec!(0) {
            info!(
                "✅ Arbitrage trade succeeded | pair ID:{} | YES filled:{} shares | NO filled:{} shares | total filled:{} shares",
                &pair_id[..8],
                yes_filled,
                no_filled,
                yes_filled.min(no_filled)
            );
        } else if yes_filled > dec!(0) || no_filled > dec!(0) {
            let side = if yes_filled > dec!(0) { "YES" } else { "NO" };
            let filled = if yes_filled > dec!(0) { yes_filled } else { no_filled };
            let other_side = if yes_filled > dec!(0) { "NO" } else { "YES" };
            warn!(
                "⚠️ One-sided fill | {} | {} filled {} shares, {} unfilled (forwarded to risk management)",
                &pair_id[..8], side, filled, other_side
            );
        } else {
            warn!(
                "❌ Arbitrage failed | pair ID:{} | both YES and NO unfilled",
                &pair_id[..8]
            );
        }

        Ok(OrderPairResult {
            pair_id,
            yes_order_id: yes_result.order_id.clone(),
            no_order_id: no_result.order_id.clone(),
            yes_filled,
            no_filled,
            yes_size: order_size,
            no_size: order_size,
            success: true,
        })
    }
}
