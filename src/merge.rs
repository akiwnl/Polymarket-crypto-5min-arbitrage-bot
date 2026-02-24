//! CTF Merge module: merge equal amounts of YES/NO tokens back to USDC.
//!
//! Supports **Gnosis Safe** (execTransaction) and **Magic/Email EIP-1167** (Polymarket Relayer).
//! Merge amount is automatically `min(YES balance, NO balance)`, no input needed.
//!
//! ## Usage Example
//!
//! ```ignore
//! use alloy::primitives::B256;
//! use polymarket_client_sdk::types::Address;
//!
//! let tx = poly_15min_bot::merge::merge_max(
//!     condition_id,
//!     proxy,
//!     &private_key,
//!     Some("https://polygon-rpc.com"),
//! ).await?;
//! ```

use std::env;

use alloy::primitives::{keccak256, Address, B256, Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::LocalSigner;
use alloy::signers::Signer as _;
use alloy::sol_types::SolCall;
use anyhow::Result;
use polymarket_client_sdk::ctf::types::{CollectionIdRequest, MergePositionsRequest, PositionIdRequest};
use polymarket_client_sdk::ctf::Client;
use polymarket_client_sdk::types::address;
use polymarket_client_sdk::{contract_config, POLYGON};
use std::str::FromStr as _;
use tracing::{info, warn};

use alloy::sol;
sol! {
    #[sol(rpc)]
    interface IERC1155Balance {
        function balanceOf(address account, uint256 id) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IGnosisSafe {
        function nonce() external view returns (uint256);
        function encodeTransactionData(
            address to,
            uint256 value,
            bytes memory data,
            uint8 operation,
            uint256 safeTxGas,
            uint256 baseGas,
            uint256 gasPrice,
            address gasToken,
            address refundReceiver,
            uint256 _nonce
        ) external view returns (bytes memory);
        function execTransaction(
            address to,
            uint256 value,
            bytes memory data,
            uint8 operation,
            uint256 safeTxGas,
            uint256 baseGas,
            uint256 gasPrice,
            address gasToken,
            address refundReceiver,
            bytes memory signatures
        ) external payable returns (bool success);
    }
}

sol! {
    struct ProxyCallTuple {
        uint8 typeCode;
        address to;
        uint256 value;
        bytes data;
    }
    function proxy(ProxyCallTuple[] calls) external payable returns (bytes[] returnValues);
}

const RPC_URL_DEFAULT: &str = "https://polygon-bor-rpc.publicnode.com";
const RELAYER_URL_DEFAULT: &str = "https://relayer-v2.polymarket.com";
const USDC_POLYGON: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

const RELAYER_GET_RELAY_PAYLOAD: &str = "/relay-payload";
const RELAYER_SUBMIT: &str = "/submit";

const PROXY_FACTORY: Address = address!("0xaB45c5A4B0c941a2F231C04C3f49182e1A254052");
const RELAY_HUB: Address = address!("0xD216153c06E857cD7f72665E0aF1d7D82172F494");
const PROXY_INIT_CODE_HASH: [u8; 32] = [
    0xd2, 0x1d, 0xf8, 0xdc, 0x65, 0x88, 0x0a, 0x86, 0x06, 0xf0, 0x9f, 0xe0, 0xce, 0x3d, 0xf9, 0xb8,
    0x86, 0x92, 0x87, 0xab, 0x0b, 0x05, 0x8b, 0xe0, 0x5a, 0xa9, 0xe8, 0xaf, 0x63, 0x30, 0xa0, 0x0b,
];
const PROXY_DEFAULT_GAS: u64 = 160_000;

/// Shorten a long 0x-prefixed hex to `0x` + first 8 chars + `..` + last 6 chars for logging.
pub fn short_hex(s: &str) -> String {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() > 14 {
        let lo = hex.len().saturating_sub(6);
        format!("0x{}..{}", &hex[..8.min(hex.len())], &hex[lo..])
    } else {
        format!("0x{}", hex)
    }
}

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;

fn encode_merge_calldata(req: &MergePositionsRequest) -> Vec<u8> {
    let sel = &keccak256(b"mergePositions(address,bytes32,bytes32,uint256[],uint256)")[..4];
    let mut out = Vec::from(sel);
    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(req.collateral_token.as_slice());
    out.extend_from_slice(req.parent_collection_id.as_slice());
    out.extend_from_slice(req.condition_id.as_slice());
    out.extend_from_slice(&U256::from(160u64).to_be_bytes::<32>());
    out.extend_from_slice(&req.amount.to_be_bytes::<32>());
    out.extend_from_slice(&U256::from(req.partition.len()).to_be_bytes::<32>());
    for p in &req.partition {
        out.extend_from_slice(&p.to_be_bytes::<32>());
    }
    out
}

fn derive_proxy_wallet(eoa: Address, proxy_factory: Address) -> Address {
    let salt = keccak256(eoa.as_slice());
    let mut buf = [0u8; 1 + 20 + 32 + 32];
    buf[0] = 0xff;
    buf[1..21].copy_from_slice(proxy_factory.as_slice());
    buf[21..53].copy_from_slice(salt.as_slice());
    buf[53..85].copy_from_slice(&PROXY_INIT_CODE_HASH);
    let h = keccak256(buf);
    Address::from_slice(&h.as_slice()[12..32])
}

fn to_hex_0x(b: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for &x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 0xf) as usize] as char);
    }
    s
}

fn build_hmac_signature(secret: &[u8], timestamp: u64, method: &str, path: &str, body: &str) -> String {
    let msg = format!("{}{}{}{}", timestamp, method, path, body);
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC key");
    mac.update(msg.as_bytes());
    let sig = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    sig.replace('+', "-").replace('/', "_")
}

async fn get_relay_payload(client: &reqwest::Client, base: &str, eoa: Address) -> Result<(Address, String)> {
    let url = format!("{}{}", base.trim_end_matches('/'), RELAYER_GET_RELAY_PAYLOAD);
    let resp = client
        .get(&url)
        .query(&[("address", format!("{:#x}", eoa)), ("type", "PROXY".to_string())])
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("GET /relay-payload failed status={} body={}", status, text);
    }
    let j: serde_json::Value = serde_json::from_str(&text)?;
    let addr = j.get("address").and_then(|v| v.as_str()).ok_or_else(|| anyhow::anyhow!("relay-payload missing address"))?;
    let nonce = j
        .get("nonce")
        .map(|v| {
            v.as_str()
                .map(String::from)
                .or_else(|| v.as_u64().map(|n| n.to_string()))
                .unwrap_or_else(|| "0".into())
        })
        .unwrap_or_else(|| "0".into());
    let relay = addr.trim().parse::<Address>().map_err(|e| anyhow::anyhow!("relay address parse failed: {}", e))?;
    Ok((relay, nonce.to_string()))
}

fn encode_proxy_call(ctf: Address, data: &[u8]) -> Vec<u8> {
    let t = ProxyCallTuple {
        typeCode: 1u8,
        to: ctf,
        value: U256::ZERO,
        data: Bytes::from(data.to_vec()),
    };
    proxyCall { calls: vec![t] }.abi_encode().to_vec()
}

fn create_struct_hash(
    from: Address,
    to: Address,
    data: &[u8],
    tx_fee: u64,
    gas_price: u64,
    gas_limit: u64,
    nonce: &str,
    relay_hub: Address,
    relay: Address,
) -> B256 {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"rlx:");
    buf.extend_from_slice(from.as_slice());
    buf.extend_from_slice(to.as_slice());
    buf.extend_from_slice(data);
    buf.extend_from_slice(&U256::from(tx_fee).to_be_bytes::<32>());
    buf.extend_from_slice(&U256::from(gas_price).to_be_bytes::<32>());
    buf.extend_from_slice(&U256::from(gas_limit).to_be_bytes::<32>());
    let n: u64 = nonce.parse().unwrap_or(0);
    buf.extend_from_slice(&U256::from(n).to_be_bytes::<32>());
    buf.extend_from_slice(relay_hub.as_slice());
    buf.extend_from_slice(relay.as_slice());
    keccak256(buf)
}

fn eip191_hash(struct_hash: B256) -> B256 {
    let mut msg = b"\x19Ethereum Signed Message:\n32".to_vec();
    msg.extend_from_slice(struct_hash.as_slice());
    keccak256(msg)
}

async fn relayer_execute_merge(
    merge_calldata: &[u8],
    ctf_address: Address,
    proxy_wallet: Address,
    signer: &impl alloy::signers::Signer,
    builder_key: &str,
    builder_secret: &str,
    builder_passphrase: &str,
    relayer_url: &str,
) -> Result<String> {
    let client = reqwest::Client::new();
    let eoa = signer.address();
    let base = relayer_url.trim_end_matches('/');

    let (relay, nonce) = get_relay_payload(&client, base, eoa).await?;
    let proxy_data = encode_proxy_call(ctf_address, merge_calldata);
    let gas_limit: u64 = env::var("MERGE_PROXY_GAS_LIMIT")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(PROXY_DEFAULT_GAS);

    if env::var("MERGE_PROXY_TO").map(|s| s.trim().eq_ignore_ascii_case("PROXY_WALLET")).unwrap_or(false) {
        info!("ℹ️ MERGE_PROXY_TO=PROXY_WALLET ignored, using to=PROXY_FACTORY");
    }
    let to = PROXY_FACTORY;
    let struct_hash = create_struct_hash(eoa, to, &proxy_data, 0, 0, gas_limit, &nonce, RELAY_HUB, relay);
    let to_sign = eip191_hash(struct_hash);
    let sig = signer.sign_hash(&to_sign).await.map_err(|e| anyhow::anyhow!("EOA signing failed: {}", e))?;
    let mut sig_bytes = sig.as_bytes().to_vec();
    if sig_bytes.len() == 65 && (sig_bytes[64] == 0 || sig_bytes[64] == 1) {
        sig_bytes[64] += 27;
    }
    let signature_hex = to_hex_0x(&sig_bytes);

    let signature_params = serde_json::json!({
        "gasPrice": "0",
        "gasLimit": gas_limit.to_string(),
        "relayerFee": "0",
        "relayHub": format!("{:#x}", RELAY_HUB),
        "relay": format!("{:#x}", relay)
    });
    let body = serde_json::json!({
        "from": format!("{:#x}", eoa),
        "to": format!("{:#x}", to),
        "proxyWallet": format!("{:#x}", proxy_wallet),
        "data": to_hex_0x(&proxy_data),
        "nonce": nonce,
        "signature": signature_hex,
        "signatureParams": signature_params,
        "type": "PROXY",
        "metadata": "Merge positions"
    });
    let body_str = serde_json::to_string(&body)?;

    let path = RELAYER_SUBMIT;
    let method = "POST";
    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;
    // Support both standard Base64 (+/) and Base64URL (-_) formats
    let secret_b64 = builder_secret
        .trim()
        .replace('-', "+")
        .replace('_', "/");
    let secret_bytes = base64::engine::general_purpose::STANDARD
        .decode(&secret_b64)
        .map_err(|e| anyhow::anyhow!("POLY_BUILDER_SECRET base64 decode failed: {}", e))?;
    let sig_hmac = build_hmac_signature(&secret_bytes, timestamp, method, path, &body_str);

    let url = format!("{}{}", base, path);
    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("POLY_BUILDER_API_KEY", builder_key)
        .header("POLY_BUILDER_TIMESTAMP", timestamp.to_string())
        .header("POLY_BUILDER_PASSPHRASE", builder_passphrase)
        .header("POLY_BUILDER_SIGNATURE", sig_hmac)
        .body(body_str)
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("Relayer request failed status={} body={}", status, text);
    }
    let json: serde_json::Value = serde_json::from_str(&text)?;
    let hash = json
        .get("transactionHash")
        .or_else(|| json.get("transaction_hash"))
        .and_then(|v| v.as_str())
        .map(String::from);
    Ok(hash.unwrap_or_else(|| text))
}

/// Merge max available YES+NO to USDC for the given `condition_id` on `proxy`.
///
/// Merge amount is `min(YES balance, NO balance)`. Supports Gnosis Safe (execTransaction) and Magic/Email (Relayer).
///
/// - `condition_id`: Market condition ID (32-byte hex)
/// - `proxy`: Proxy address (Gnosis Safe or EIP-1167)
/// - `private_key`: EOA private key
/// - `rpc_url`: Polygon RPC, `None` uses `https://polygon-rpc.com`
///
/// Magic/Email path reads from env vars: `POLY_BUILDER_API_KEY`, `POLY_BUILDER_SECRET`, `POLY_BUILDER_PASSPHRASE`, `RELAYER_URL` (optional).
///
/// Returns the transaction hash (hex string).
pub async fn merge_max(
    condition_id: B256,
    proxy: Address,
    private_key: &str,
    rpc_url: Option<&str>,
) -> Result<String> {
    let rpc = rpc_url.unwrap_or(RPC_URL_DEFAULT);
    let chain = POLYGON;
    let signer = LocalSigner::from_str(private_key)?.with_chain_id(Some(chain));
    let wallet = signer.address();

    let provider = ProviderBuilder::new().wallet(signer.clone()).connect(rpc).await?;
    let client = Client::new(provider.clone(), chain)?;
    let config = contract_config(chain, false).ok_or_else(|| anyhow::anyhow!("unsupported chain_id: {}", chain))?;
    let prov_read = ProviderBuilder::new().connect(rpc).await?;
    let erc1155 = IERC1155Balance::new(config.conditional_tokens, prov_read);
    let ctf = config.conditional_tokens;

    let req_col_yes = CollectionIdRequest::builder().parent_collection_id(B256::ZERO).condition_id(condition_id).index_set(U256::from(1)).build();
    let req_col_no = CollectionIdRequest::builder().parent_collection_id(B256::ZERO).condition_id(condition_id).index_set(U256::from(2)).build();
    let col_yes = client.collection_id(&req_col_yes).await?;
    let col_no = client.collection_id(&req_col_no).await?;

    let req_pos_yes = PositionIdRequest::builder().collateral_token(USDC_POLYGON).collection_id(col_yes.collection_id).build();
    let req_pos_no = PositionIdRequest::builder().collateral_token(USDC_POLYGON).collection_id(col_no.collection_id).build();
    let pos_yes = client.position_id(&req_pos_yes).await?;
    let pos_no = client.position_id(&req_pos_no).await?;

    let b_yes: U256 = erc1155.balanceOf(proxy, pos_yes.position_id).call().await.unwrap_or(U256::ZERO);
    let b_no: U256 = erc1155.balanceOf(proxy, pos_no.position_id).call().await.unwrap_or(U256::ZERO);

    let merge_amount = b_yes.min(b_no);
    if merge_amount == U256::ZERO {
        anyhow::bail!("no shares available for merge: YES={} NO={}, at least one side is 0.", b_yes, b_no);
    }
    info!("🔄 Merge amount: {} ({} USDC)", merge_amount, merge_amount / U256::from(1_000_000));

    let merge_req = MergePositionsRequest::for_binary_market(USDC_POLYGON, condition_id, merge_amount);
    let merge_calldata = encode_merge_calldata(&merge_req);
    let code = provider.get_code_at(proxy).await.unwrap_or_default();

    if code.len() < 150 {
        let derived = derive_proxy_wallet(wallet, PROXY_FACTORY);
        let try_anyway = env::var("MERGE_TRY_ANYWAY").map(|s| s.trim() == "1" || s.trim().eq_ignore_ascii_case("true")).unwrap_or(false);
        if derived != proxy {
            if !try_anyway {
                anyhow::bail!(
                    "POLYMARKET_PROXY_ADDRESS ({:?}) does not match ProxyFactory CREATE2 derivation ({:?}).\
                     Use Polymarket web merge instead, or set MERGE_TRY_ANYWAY=1 to force attempt.",
                    proxy, derived
                );
            }
            warn!("MERGE_TRY_ANYWAY=1: derive != proxy, still sending Relayer request.");
        }
        let builder_key = env::var("POLY_BUILDER_API_KEY").ok();
        let builder_secret = env::var("POLY_BUILDER_SECRET").ok();
        let builder_passphrase = env::var("POLY_BUILDER_PASSPHRASE").ok();
        let relayer_url = env::var("RELAYER_URL").unwrap_or_else(|_| RELAYER_URL_DEFAULT.to_string());
        match (builder_key.as_deref(), builder_secret.as_deref(), builder_passphrase.as_deref()) {
            (Some(k), Some(s), Some(p)) => {
                let out = relayer_execute_merge(&merge_calldata, ctf, proxy, &signer, k, s, p, &relayer_url).await?;
                info!("✅ Relayer submitted tx: {}", out);
                return Ok(out);
            }
            _ => anyhow::bail!(
                "Magic/Email requires POLY_BUILDER_API_KEY, POLY_BUILDER_SECRET, POLY_BUILDER_PASSPHRASE; or use web merge instead.",
            ),
        }
    }

    let safe = IGnosisSafe::new(proxy, provider);
    let nonce: U256 = safe.nonce().call().await.map_err(|e| {
        let msg = e.to_string();
        let hint = if msg.contains("revert") || msg.contains("reverted") {
            " This address may not be a Gnosis Safe; for Magic/Email use Relayer or web merge."
        } else { "" };
        anyhow::anyhow!("failed to read Safe nonce: {}{}", msg, hint)
    })?;

    let tx_hash_data = safe
        .encodeTransactionData(ctf, U256::ZERO, merge_calldata.clone().into(), 0u8, U256::ZERO, U256::ZERO, U256::ZERO, Address::ZERO, Address::ZERO, nonce)
        .call().await.map_err(|e| anyhow::anyhow!("Safe.encodeTransactionData failed: {}", e))?.0;

    let tx_hash = keccak256(tx_hash_data.as_ref());
    let sig = signer.sign_hash(&tx_hash).await.map_err(|e| anyhow::anyhow!("signing failed: {}", e))?;
    let mut sig_bytes = sig.as_bytes().to_vec();
    if sig_bytes.len() == 65 && (sig_bytes[64] == 0 || sig_bytes[64] == 1) {
        sig_bytes[64] += 27;
    }

    let pending = safe
        .execTransaction(ctf, U256::ZERO, merge_calldata.into(), 0u8, U256::ZERO, U256::ZERO, U256::ZERO, Address::ZERO, Address::ZERO, sig_bytes.into())
        .send().await.map_err(|e| anyhow::anyhow!("Safe.execTransaction failed: {}", e))?;

    let tx_hash_out = *pending.tx_hash();
    let _receipt = pending.get_receipt().await.map_err(|e| anyhow::anyhow!("waiting for receipt failed: {}", e))?;
    info!("✅ Merge succeeded (Safe) tx: {:#x}", tx_hash_out);
    Ok(format!("{:#x}", tx_hash_out))
}
