//! Fetch user's current positions (Data API)

use anyhow::{Context, Result};
use polymarket_client_sdk::data::types::request::PositionsRequest;
use polymarket_client_sdk::data::Client;
use polymarket_client_sdk::types::Address;

/// Position struct returned by Data API, re-exported for caller convenience
pub use polymarket_client_sdk::data::types::response::Position;

/// Read user address from `POLYMARKET_PROXY_ADDRESS` env var and call Data API to get current open positions.
///
/// # Environment Variables
///
/// - `POLYMARKET_PROXY_ADDRESS`: Required, Polymarket proxy wallet address (or EOA address)
///
/// # Errors
///
/// - `POLYMARKET_PROXY_ADDRESS` not set
/// - Invalid address format
/// - Data API call failed
///
/// # Example
///
/// ```ignore
/// use poly_15min_bot::positions::{get_positions, Position};
///
/// let positions = get_positions().await?;
/// for p in positions {
///     println!("{}: {} @ {}", p.title, p.size, p.cur_price);
/// }
/// ```
pub async fn get_positions() -> Result<Vec<Position>> {
    dotenvy::dotenv().ok();
    let addr = std::env::var("POLYMARKET_PROXY_ADDRESS")
        .context("POLYMARKET_PROXY_ADDRESS not set")?;
    let user: Address = addr
        .parse()
        .context("POLYMARKET_PROXY_ADDRESS invalid format")?;
    let client = Client::default();
    let req = PositionsRequest::builder().user(user).build();
    client.positions(&req).await.context("failed to fetch positions")
}
