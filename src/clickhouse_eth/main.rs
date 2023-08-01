use ethers::providers::{Http, Provider, Ws};
use klickhouse::{u256, Bytes, Row};
use klickhouse::{Client, ClientOptions};
use std::error::Error;
use url::Url;

use crate::ClapActionType;

use super::{init_http, init_ws, sync_ws};

extern crate pretty_env_logger;

#[derive(Row, Clone, Debug, Default)]
pub struct BlockRow {
    pub hash: Bytes,
    pub number: u64,
    pub parentHash: Bytes,
    pub uncles: Vec<Bytes>,
    pub sha3Uncles: Bytes,
    pub totalDifficulty: u256,
    pub difficulty: u256,
    pub miner: Bytes,
    pub nonce: Bytes,
    pub mixHash: Bytes,
    pub baseFeePerGas: Option<u256>,
    pub gasLimit: u256,
    pub gasUsed: u256,
    pub stateRoot: Bytes,
    pub transactionsRoot: Bytes,
    pub receiptsRoot: Bytes,
    pub logsBloom: Bytes,
    pub withdrawlsRoot: Option<Bytes>,
    pub extraData: Bytes,
    pub timestamp: u256,
    pub size: u256,
}

#[derive(Row, Clone, Debug, Default)]
pub struct TransactionRow {
    pub hash: Bytes,
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub transactionIndex: u64,
    pub chainId: Option<u256>,
    pub r#type: Option<u64>,
    pub from: Bytes,
    pub to: Option<Bytes>,
    pub value: u256,
    pub nonce: u256,
    pub input: Bytes,
    pub gas: u256,
    pub gasPrice: Option<u256>,
    pub maxFeePerGas: Option<u256>,
    pub maxPriorityFeePerGas: Option<u256>,
    pub r: u256,
    pub s: u256,
    pub v: u64,
    pub accessList: Option<String>,
    pub contractAddress: Option<Bytes>,
    pub cumulativeGasUsed: u256,
    pub effectiveGasPrice: Option<u256>,
    pub gasUsed: u256,
    pub logsBloom: Bytes,
    pub root: Option<Bytes>,
    pub status: Option<u64>,
}

#[derive(Row, Clone, Debug, Default)]
pub struct EventRow {
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub transactionHash: Bytes,
    pub transactionIndex: u64,
    pub logIndex: u256,
    pub removed: bool,
    pub topics: Vec<Bytes>,
    pub data: Bytes,
    pub address: Bytes,
}

#[derive(Row, Clone, Debug, Default)]
pub struct WithdrawalRow {
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub index: u64,
    pub validatorIndex: u64,
    pub address: Bytes,
    pub amount: u256,
}

#[derive(Row, Clone, Debug)]
pub struct TraceRow {
    pub blockPos: u64,
    /// Block Number
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    /// Block Hash
    pub blockHash: Bytes,

    /// Trace address, The list of addresses where the call was executed, the address of the parents, and the order of the current sub call
    pub traceAddress: Vec<u64>,
    /// Subtraces
    pub subtraces: u64,
    /// Transaction position
    pub transactionPosition: Option<u64>,
    /// Transaction hash
    pub transactionHash: Option<Bytes>,

    /// Error, See also [`TraceError`]
    pub error: Option<String>,

    /// Action
    ///
    // pub action: Action, // call create suicide reward
    pub actionType: String, // Enum('Call', 'Create', 'Suicide', 'Reward')
    /// Sender
    pub actionCallFrom: Option<Bytes>,
    /// Recipient
    pub actionCallTo: Option<Bytes>,
    /// Transferred Value
    pub actionCallValue: Option<u256>,
    /// Input data
    pub actionCallInput: Option<Bytes>,
    pub actionCallGas: Option<u256>,
    /// The type of the call.
    pub actionCallType: String, // none call callcode delegatecall staticcall
    pub actionCreateFrom: Option<Bytes>,
    pub actionCreateValue: Option<u256>,
    pub actionCreateInit: Option<Bytes>,
    pub actionCreateGas: Option<u256>,
    pub actionSuicideAddress: Option<Bytes>,
    pub actionSuicideRefundAddress: Option<Bytes>,
    pub actionSuicideBalance: Option<u256>,
    pub actionRewardAuthor: Option<Bytes>,
    pub actionRewardValue: Option<u256>,
    pub actionRewardType: String, // LowCardinality ('block', 'uncle', 'emptyStep', 'external')
    /// Result
    //  pub result: Option<Res>, // call {gasused, output} create {gas_used, code, address} none
    pub resultType: String, // LowCardinality ('none', 'call', 'create')
    pub resultCallGasUsed: Option<u256>,
    pub resultCallOutput: Option<Bytes>,
    pub resultCreateGasUsed: Option<u256>,
    pub resultCreateCode: Option<Bytes>,
    pub resultCreateAddress: Option<Bytes>,
}

pub async fn main(
    clickhouse_uri: &str,
    eth_uri: &str,
    action_type: &ClapActionType,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(clickhouse_uri).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default" || clickhouse_url.username().len() > 0 {
        ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url.path().to_string(),
        }
    } else {
        ClientOptions::default()
    };



    match action_type {
        ClapActionType::Init {
            from,
            init_trace,
            batch,
        } => {
            let clickhouse_client = Client::connect(
                format!(
                    "{}:{}",
                    clickhouse_url.host().unwrap(),
                    clickhouse_url.port().unwrap()
                ),
                options.clone(),
            )
            .await?;
        
            let provider_url = Url::parse(eth_uri)?;

            if !init_trace {
                let provider = Provider::<Ws>::connect(eth_uri).await?;

                init_ws::init_ws(clickhouse_client, provider, *from, *init_trace, *batch).await?;
            } else {
                let provider = Provider::<Http>::try_from(eth_uri)?;

                init_http::init_http(clickhouse_client, provider, *from, *init_trace, *batch)
                    .await?;
            }
        }
        ClapActionType::Sync { sync_trace } => {
            if !sync_trace {
                sync_ws::sync_ws(clickhouse_url.clone(), options.clone(), eth_uri.to_owned()).await?;
            }
        },
        ClapActionType::GraphQL {} => todo!(),
    }

    Ok(())
}
