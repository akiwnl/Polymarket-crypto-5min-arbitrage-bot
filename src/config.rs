use anyhow::Result;
use polymarket_client_sdk::clob::types::OrderType;
use std::env;

use polymarket_client_sdk::types::Address;

/// Parse arbitrage order type: GTC, GTD, FOK, FAK (case-insensitive). Invalid/unknown values default to GTD.
fn parse_arbitrage_order_type(s: &str) -> OrderType {
    match s.trim().to_uppercase().as_str() {
        "GTC" => OrderType::GTC,
        "GTD" => OrderType::GTD,
        "FOK" => OrderType::FOK,
        "FAK" => OrderType::FAK,
        _ => OrderType::GTD,
    }
}

/// Parse slippage array: comma-separated, e.g. "-0.02,0.0".
/// Index 0 = up/flat side slippage, 1 = down side only. A single value applies to both. Default "0,0.01".
fn parse_slippage(s: &str) -> [f64; 2] {
    let parts: Vec<f64> = s
        .split(',')
        .map(|x| x.trim().parse().unwrap_or(0.0))
        .collect();
    match parts.len() {
        0 => [0.0, 0.01],
        1 => [parts[0], parts[0]],
        _ => [parts[0], parts[1]],
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub private_key: String,
    pub proxy_address: Option<Address>, // Polymarket Proxy address (if using Email/Magic or Browser Wallet login)
    pub min_profit_threshold: f64,
    pub max_order_size_usdc: f64,
    pub crypto_symbols: Vec<String>,
    pub market_refresh_advance_secs: u64,
    pub risk_max_exposure_usdc: f64,
    pub risk_imbalance_threshold: f64,
    pub hedge_take_profit_pct: f64, // Hedge take-profit percentage (e.g. 0.05 = 5%)
    pub hedge_stop_loss_pct: f64,   // Hedge stop-loss percentage (e.g. 0.05 = 5%)
    pub arbitrage_execution_spread: f64, // Arbitrage execution spread: execute when yes+no <= 1 - spread
    /// Slippage [first, second]: down side uses second, up/flat uses first. E.g. "-0.02,0.0"
    pub slippage: [f64; 2],
    pub gtd_expiration_secs: u64, // GTD order expiration (seconds), default 300s (5 min); only effective when arbitrage_order_type=GTD
    /// Order type for arbitrage: GTC (good-til-cancelled), GTD (with gtd_expiration_secs), FOK (fill-or-kill), FAK (fill-and-kill)
    pub arbitrage_order_type: OrderType,
    pub stop_arbitrage_before_end_minutes: u64, // Stop arbitrage N minutes before market end, default 0 (no stop)
    /// Scheduled merge interval (minutes), 0 = disabled. CONDITION_ID is obtained from the current window market like the orderbook.
    pub merge_interval_minutes: u64,
    /// YES price threshold: only execute arbitrage when YES price >= this value, default 0.0 (no limit)
    pub min_yes_price_threshold: f64,
    /// NO price threshold: only execute arbitrage when NO price >= this value, default 0.0 (no limit)
    pub min_no_price_threshold: f64,
    /// Position sync interval (seconds), default 10s (fetch latest positions from API to overwrite local cache)
    pub position_sync_interval_secs: u64,
    /// Position balance check interval (seconds), default 60s
    pub position_balance_interval_secs: u64,
    /// Imbalance threshold: only cancel pending orders when position difference >= this value, default 2.0
    pub position_balance_threshold: f64,
    /// Minimum total position required: only execute balancing when total position >= this value, default 5.0
    pub position_balance_min_total: f64,
    /// Wind-down before window end: minutes before current 5-min window ends to trigger wind-down (cancel orders -> merge -> market-sell remaining). 0 = disabled.
    pub wind_down_before_window_end_minutes: u64,
    /// Limit price for single-leg sell during wind-down (for fast execution), default 0.01
    pub wind_down_sell_price: f64,
    /// Dry run mode: no real trades, logging only
    pub dry_run: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let dry_run = env::var("DRY_RUN")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .to_lowercase()
            == "true";

        // Parse proxy_address (optional)
        let proxy_address: Option<Address> = env::var("POLYMARKET_PROXY_ADDRESS")
            .ok()
            .and_then(|addr| addr.parse().ok());

        // Private key not required in dry run mode: uses dummy key when not set
        let private_key = match env::var("POLYMARKET_PRIVATE_KEY") {
            Ok(key) => key,
            Err(_) if dry_run => {
                "0000000000000000000000000000000000000000000000000000000000000001"
                    .to_string()
            }
            Err(_) => panic!("POLYMARKET_PRIVATE_KEY must be set"),
        };

        Ok(Config {
            private_key,
            proxy_address,
            min_profit_threshold: env::var("MIN_PROFIT_THRESHOLD")
                .unwrap_or_else(|_| "0.001".to_string())
                .parse()
                .unwrap_or(0.001),
            max_order_size_usdc: env::var("MAX_ORDER_SIZE_USDC")
                .unwrap_or_else(|_| "100.0".to_string())
                .parse()
                .unwrap_or(100.0),
            crypto_symbols: env::var("CRYPTO_SYMBOLS")
                .unwrap_or_else(|_| "btc,eth,xrp,sol".to_string())
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .collect(),
            market_refresh_advance_secs: env::var("MARKET_REFRESH_ADVANCE_SECS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            risk_max_exposure_usdc: env::var("RISK_MAX_EXPOSURE_USDC")
                .unwrap_or_else(|_| "1000.0".to_string())
                .parse()
                .unwrap_or(1000.0),
            risk_imbalance_threshold: env::var("RISK_IMBALANCE_THRESHOLD")
                .unwrap_or_else(|_| "0.1".to_string())
                .parse()
                .unwrap_or(0.1),
            hedge_take_profit_pct: env::var("HEDGE_TAKE_PROFIT_PCT")
                .unwrap_or_else(|_| "0.05".to_string())
                .parse()
                .unwrap_or(0.05), // default 5% take-profit
            hedge_stop_loss_pct: env::var("HEDGE_STOP_LOSS_PCT")
                .unwrap_or_else(|_| "0.05".to_string())
                .parse()
                .unwrap_or(0.05), // default 5% stop-loss
            arbitrage_execution_spread: env::var("ARBITRAGE_EXECUTION_SPREAD")
                .unwrap_or_else(|_| "0.01".to_string())
                .parse()
                .unwrap_or(0.01), // default 0.01
            slippage: parse_slippage(&env::var("SLIPPAGE").unwrap_or_else(|_| "0,0.01".to_string())),
            gtd_expiration_secs: env::var("GTD_EXPIRATION_SECS")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .unwrap_or(300), // default 300s (5 min)
            arbitrage_order_type: parse_arbitrage_order_type(
                &env::var("ARBITRAGE_ORDER_TYPE").unwrap_or_else(|_| "GTD".to_string()),
            ),
            stop_arbitrage_before_end_minutes: env::var("STOP_ARBITRAGE_BEFORE_END_MINUTES")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0), // default 0 (no stop)
            merge_interval_minutes: env::var("MERGE_INTERVAL_MINUTES")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0), // 0 = disabled
            min_yes_price_threshold: env::var("MIN_YES_PRICE_THRESHOLD")
                .unwrap_or_else(|_| "0.0".to_string())
                .parse()
                .unwrap_or(0.0), // default 0.0 (no limit)
            min_no_price_threshold: env::var("MIN_NO_PRICE_THRESHOLD")
                .unwrap_or_else(|_| "0.0".to_string())
                .parse()
                .unwrap_or(0.0), // default 0.0 (no limit)
            position_sync_interval_secs: env::var("POSITION_SYNC_INTERVAL_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10), // default 10s
            position_balance_interval_secs: env::var("POSITION_BALANCE_INTERVAL_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60), // default 60s
            position_balance_threshold: env::var("POSITION_BALANCE_THRESHOLD")
                .unwrap_or_else(|_| "2.0".to_string())
                .parse()
                .unwrap_or(2.0), // default 2.0
            position_balance_min_total: env::var("POSITION_BALANCE_MIN_TOTAL")
                .unwrap_or_else(|_| "5.0".to_string())
                .parse()
                .unwrap_or(5.0), // default 5.0
            wind_down_before_window_end_minutes: env::var("WIND_DOWN_BEFORE_WINDOW_END_MINUTES")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .unwrap_or(0), // 0 = disabled
            wind_down_sell_price: env::var("WIND_DOWN_SELL_PRICE")
                .unwrap_or_else(|_| "0.01".to_string())
                .parse()
                .unwrap_or(0.01), // default 0.01
            dry_run,
        })
    }
}
