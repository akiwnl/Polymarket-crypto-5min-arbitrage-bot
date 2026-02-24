use anyhow::Result;
use dashmap::DashMap;
use futures::Stream;
use futures::StreamExt;
use polymarket_client_sdk::clob::ws::{Client as WsClient, types::response::BookUpdate};
use polymarket_client_sdk::types::{B256, U256};
use std::collections::HashMap;
use std::pin::Pin;
use tracing::{debug, info};

use crate::market::MarketInfo;

/// Shorten B256 for logging: keep 0x + first 8 hex chars, e.g. 0xb91126b7..
#[inline]
fn short_b256(b: &B256) -> String {
    let s = format!("{b}");
    if s.len() > 12 { format!("{}..", &s[..10]) } else { s }
}

/// Shorten U256 for logging: keep last 8 digits, e.g. ..67033653
#[inline]
fn short_u256(u: &U256) -> String {
    let s = format!("{u}");
    if s.len() > 12 {
        format!("..{}", &s[s.len().saturating_sub(8)..])
    } else {
        s
    }
}

pub struct OrderBookMonitor {
    ws_client: WsClient,
    books: DashMap<U256, BookUpdate>,
    market_map: HashMap<B256, (U256, U256)>, // market_id -> (yes_token_id, no_token_id)
}

pub struct OrderBookPair {
    pub yes_book: BookUpdate,
    pub no_book: BookUpdate,
    pub market_id: B256,
}

impl OrderBookMonitor {
    pub fn new() -> Self {
        Self {
            // Use unauthenticated client: orderbook subscription doesn't require auth, it's public data
            // Only user data subscriptions (e.g. user orders, trades) require authentication
            ws_client: WsClient::default(),
            books: DashMap::new(),
            market_map: HashMap::new(),
        }
    }

    /// Subscribe to a new market
    pub fn subscribe_market(&mut self, market: &MarketInfo) -> Result<()> {
        // Record market mapping
        self.market_map.insert(
            market.market_id,
            (market.yes_token_id, market.no_token_id),
        );

        info!(
            market_id = short_b256(&market.market_id),
            yes = short_u256(&market.yes_token_id),
            no = short_u256(&market.no_token_id),
            "subscribed to market orderbook"
        );

        Ok(())
    }

    /// Create orderbook subscription stream
    ///
    /// Note: Orderbook subscription uses an unauthenticated WebSocket client since orderbook data is public.
    /// Only user-related data subscriptions (e.g. order status, trade history) require authentication.
    pub fn create_orderbook_stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<BookUpdate>> + Send + '_>>> {
        // Collect all token_ids to subscribe
        let token_ids: Vec<U256> = self
            .market_map
            .values()
            .flat_map(|(yes, no)| [*yes, *no])
            .collect();

        if token_ids.is_empty() {
            return Err(anyhow::anyhow!("no markets to subscribe"));
        }

        info!(token_count = token_ids.len(), "creating orderbook subscription stream (unauthenticated)");

        // subscribe_orderbook doesn't require auth, unauthenticated client is sufficient
        let stream = self.ws_client.subscribe_orderbook(token_ids)?;
        // Convert SDK Error to anyhow::Error
        let stream = stream.map(|result| result.map_err(|e| anyhow::anyhow!("{}", e)));
        Ok(Box::pin(stream))
    }

    /// Handle orderbook update
    pub fn handle_book_update(&self, book: BookUpdate) -> Option<OrderBookPair> {

        // Print top 5 bid/ask levels (for debugging)
        if !book.bids.is_empty() {
            let top_bids: Vec<String> = book.bids.iter()
                .take(5)
                .map(|b| format!("{}@{}", b.size, b.price))
                .collect();
            debug!(
                asset_id = %book.asset_id,
                "top 5 bids: {}",
                top_bids.join(", ")
            );
        }
        if !book.asks.is_empty() {
            let top_asks: Vec<String> = book.asks.iter()
                .take(5)
                .map(|a| format!("{}@{}", a.size, a.price))
                .collect();
            debug!(
                asset_id = short_u256(&book.asset_id),
                "top 5 asks: {}",
                top_asks.join(", ")
            );
        }

        // Update orderbook cache
        self.books.insert(book.asset_id, book.clone());

        // Find which market this token belongs to; either side (YES or NO) update returns OrderBookPair for timely arbitrage response
        for (market_id, (yes_token, no_token)) in &self.market_map {
            if book.asset_id == *yes_token {
                if let Some(no_book) = self.books.get(no_token) {
                    return Some(OrderBookPair {
                        yes_book: book.clone(),
                        no_book: no_book.clone(),
                        market_id: *market_id,
                    });
                }
            } else if book.asset_id == *no_token {
                if let Some(yes_book) = self.books.get(yes_token) {
                    return Some(OrderBookPair {
                        yes_book: yes_book.clone(),
                        no_book: book.clone(),
                        market_id: *market_id,
                    });
                }
            }
        }

        None
    }

    /// Get orderbook (if exists)
    pub fn get_book(&self, token_id: U256) -> Option<BookUpdate> {
        self.books.get(&token_id).map(|b| b.clone())
    }

    /// Clear all subscriptions
    pub fn clear(&mut self) {
        self.books.clear();
        self.market_map.clear();
    }
}
