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

    /// Execute arbitrage trade: illiquid side first (FOK), then liquid side (GTD).
    /// Illiquid = side with smaller available size. If FOK doesn't fill, skip trade (loss $0).
    /// yes_dir / no_dir: price direction "↑" "↓" "−" or "", used to assign slippage by direction (down=second, up/flat=first)
    pub async fn execute_arbitrage_pair(
        &self,
        opp: &ArbitrageOpportunity,
        yes_dir: &str,
        no_dir: &str,
    ) -> Result<OrderPairResult> {
        // Performance timing: total start
        let total_start = Instant::now();

        debug!(
            market_id = %opp.market_id,
            profit_pct = %opp.profit_percentage,
            "starting arbitrage trade (illiquid FOK → liquid GTD)"
        );

        // Calculate actual order size (considering max order limit)
        let yes_token_id = U256::from_str(&opp.yes_token_id.to_string())?;
        let no_token_id = U256::from_str(&opp.no_token_id.to_string())?;

        let order_size = opp.yes_size.min(opp.no_size).min(self.max_order_size);

        // Generate order pair ID
        let pair_id = Uuid::new_v4().to_string();

        // Slippage by direction: up=first, down/flat=second
        let yes_slippage_apply = self.slippage_for_direction(yes_dir);
        let no_slippage_apply = self.slippage_for_direction(no_dir);
        let yes_price_with_slippage = (opp.yes_ask_price + yes_slippage_apply).min(dec!(1.0));
        let no_price_with_slippage = (opp.no_ask_price + no_slippage_apply).min(dec!(1.0));

        // Determine illiquid side: smaller available size = less liquidity
        let yes_is_illiquid = opp.yes_size <= opp.no_size;
        let (illiquid_label, liquid_label) = if yes_is_illiquid { ("YES", "NO") } else { ("NO", "YES") };
        let (illiquid_token, liquid_token) = if yes_is_illiquid {
            (yes_token_id, no_token_id)
        } else {
            (no_token_id, yes_token_id)
        };
        let (illiquid_price, liquid_price) = if yes_is_illiquid {
            (yes_price_with_slippage, no_price_with_slippage)
        } else {
            (no_price_with_slippage, yes_price_with_slippage)
        };
        let (illiquid_size, liquid_size) = if yes_is_illiquid {
            (opp.yes_size, opp.no_size)
        } else {
            (opp.no_size, opp.yes_size)
        };

        // Print level selection info (price with slippage)
        info!(
            "📋 Level | YES {:.4}×{:.2} NO {:.4}×{:.2} | illiquid:{} (size:{})",
            yes_price_with_slippage, order_size,
            no_price_with_slippage, order_size,
            illiquid_label, illiquid_size
        );

        info!(
            "📤 Order | {} FOK {:.4}×{} → {} GTD {:.4}×{} | expiry:{}s",
            illiquid_label, illiquid_price, order_size,
            liquid_label, liquid_price, order_size,
            self.gtd_expiration_secs
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
                "[DRY RUN] Simulated arbitrage | market:{} | illiquid:{} FOK {:.4} | liquid:{} GTD {:.4} | size:{}",
                opp.market_id,
                illiquid_label, illiquid_price,
                liquid_label, liquid_price,
                order_size
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
        let signer = LocalSigner::from_str(&self.private_key)?
            .with_chain_id(Some(POLYGON));

        // ── Step 1: Build, sign, and send illiquid side with FOK ──
        let step1_start = Instant::now();

        let illiquid_order = client
            .limit_order()
            .token_id(illiquid_token)
            .side(Side::Buy)
            .price(illiquid_price)
            .size(order_size)
            .order_type(OrderType::FOK)
            .build()
            .await?;

        let signed_illiquid = client.sign(&signer, illiquid_order).await?;

        let illiquid_results = match client.post_orders(vec![signed_illiquid]).await {
            Ok(results) => results,
            Err(e) => {
                let elapsed = step1_start.elapsed().as_millis();
                error!(
                    "❌ Illiquid FOK order failed | {} | pair:{} | price:{} | size:{} | {}ms | error:{}",
                    illiquid_label, &pair_id[..8], illiquid_price, order_size, elapsed, e
                );
                return Err(anyhow::anyhow!("illiquid FOK order API call failed: {}", e));
            }
        };

        if illiquid_results.is_empty() {
            return Err(anyhow::anyhow!("illiquid FOK order returned empty result"));
        }

        let illiquid_result = &illiquid_results[0];
        let illiquid_filled = illiquid_result.taking_amount;
        let step1_elapsed = step1_start.elapsed().as_millis();

        // If FOK didn't fill → skip trade entirely (loss $0)
        if illiquid_filled == dec!(0) {
            let error_msg = illiquid_result.error_msg.as_deref().unwrap_or("no fill");
            info!(
                "⏭️ Illiquid side not filled, skipping trade | {} FOK | pair:{} | reason:{} | {}ms",
                illiquid_label, &pair_id[..8], error_msg, step1_elapsed
            );
            return Ok(OrderPairResult {
                pair_id,
                yes_order_id: String::new(),
                no_order_id: String::new(),
                yes_filled: dec!(0),
                no_filled: dec!(0),
                yes_size: order_size,
                no_size: order_size,
                success: false,
            });
        }

        info!(
            "✅ Illiquid side filled | {} FOK | pair:{} | filled:{} shares | {}ms",
            illiquid_label, &pair_id[..8], illiquid_filled, step1_elapsed
        );

        // ── Step 2: Build, sign, and send liquid side with GTD ──
        let step2_start = Instant::now();
        let expiration = Utc::now() + chrono::Duration::seconds(self.gtd_expiration_secs as i64);

        let liquid_order = client
            .limit_order()
            .token_id(liquid_token)
            .side(Side::Buy)
            .price(liquid_price)
            .size(order_size)
            .order_type(OrderType::GTD)
            .expiration(expiration)
            .build()
            .await?;

        let signed_liquid = client.sign(&signer, liquid_order).await?;

        let liquid_results = match client.post_orders(vec![signed_liquid]).await {
            Ok(results) => results,
            Err(e) => {
                let step2_elapsed = step2_start.elapsed().as_millis();
                let total_elapsed = total_start.elapsed().as_millis();
                error!(
                    "❌ Liquid GTD order failed | {} | pair:{} | price:{} | size:{} | step2:{}ms | total:{}ms | error:{}",
                    liquid_label, &pair_id[..8], liquid_price, order_size, step2_elapsed, total_elapsed, e
                );
                // Illiquid side already filled — this is a one-sided fill, return it so risk manager can handle
                let (yes_filled, no_filled) = if yes_is_illiquid {
                    (illiquid_filled, dec!(0))
                } else {
                    (dec!(0), illiquid_filled)
                };
                warn!(
                    "⚠️ One-sided fill | {} | {} filled {} shares, {} order failed (forwarded to risk management)",
                    &pair_id[..8], illiquid_label, illiquid_filled, liquid_label
                );
                return Ok(OrderPairResult {
                    pair_id,
                    yes_order_id: if yes_is_illiquid { illiquid_result.order_id.clone() } else { String::new() },
                    no_order_id: if yes_is_illiquid { String::new() } else { illiquid_result.order_id.clone() },
                    yes_filled,
                    no_filled,
                    yes_size: order_size,
                    no_size: order_size,
                    success: true,
                });
            }
        };

        if liquid_results.is_empty() {
            let (yes_filled, no_filled) = if yes_is_illiquid {
                (illiquid_filled, dec!(0))
            } else {
                (dec!(0), illiquid_filled)
            };
            warn!(
                "⚠️ One-sided fill | {} | {} filled {} shares, {} returned empty (forwarded to risk management)",
                &pair_id[..8], illiquid_label, illiquid_filled, liquid_label
            );
            return Ok(OrderPairResult {
                pair_id,
                yes_order_id: if yes_is_illiquid { illiquid_result.order_id.clone() } else { String::new() },
                no_order_id: if yes_is_illiquid { String::new() } else { illiquid_result.order_id.clone() },
                yes_filled,
                no_filled,
                yes_size: order_size,
                no_size: order_size,
                success: true,
            });
        }

        let liquid_result = &liquid_results[0];
        let liquid_filled = liquid_result.taking_amount;
        let step2_elapsed = step2_start.elapsed().as_millis();
        let total_elapsed = total_start.elapsed().as_millis();

        info!(
            "⏱️ Timing | {} | step1(FOK) {}ms step2(GTD) {}ms total {}ms",
            &pair_id[..8], step1_elapsed, step2_elapsed, total_elapsed
        );

        // Map back to YES/NO
        let (yes_filled, no_filled) = if yes_is_illiquid {
            (illiquid_filled, liquid_filled)
        } else {
            (liquid_filled, illiquid_filled)
        };
        let (yes_order_id, no_order_id) = if yes_is_illiquid {
            (illiquid_result.order_id.clone(), liquid_result.order_id.clone())
        } else {
            (liquid_result.order_id.clone(), illiquid_result.order_id.clone())
        };

        // Print fill status
        if yes_filled > dec!(0) && no_filled > dec!(0) {
            info!(
                "✅ Arbitrage trade succeeded | pair ID:{} | YES filled:{} shares | NO filled:{} shares | total filled:{} shares",
                &pair_id[..8],
                yes_filled,
                no_filled,
                yes_filled.min(no_filled)
            );
        } else {
            // Illiquid filled but liquid didn't — one-sided fill (rare with GTD, handled by risk manager)
            let unfilled_side = if yes_filled == dec!(0) { "YES" } else { "NO" };
            warn!(
                "⚠️ One-sided fill | {} | {} unfilled (GTD pending, forwarded to risk management)",
                &pair_id[..8], unfilled_side
            );
        }

        Ok(OrderPairResult {
            pair_id,
            yes_order_id,
            no_order_id,
            yes_filled,
            no_filled,
            yes_size: order_size,
            no_size: order_size,
            success: true,
        })
    }
}
