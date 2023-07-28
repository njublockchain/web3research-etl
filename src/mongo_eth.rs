use std::{collections::HashMap, error::Error, future::Future};

#[cfg(not(feature = "celo"))]
use ethers::types::Withdrawal;
use ethers::{
    providers::{Middleware, Provider, ProviderError, Ws},
    types::{
        transaction::eip2930::AccessList, Action, Address, Block, Bloom, Bytes, OtherFields, Res,
        Trace, Transaction, TransactionReceipt, H256, H64, U256, U64,
    },
};
use mongodb::{
    options::{ClientOptions, InsertManyOptions, TransactionOptions},
    results::InsertManyResult,
    Client, Collection, Database,
};
use tokio::{join, try_join};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

use crate::{helpers, ClapActionType};
use log::{info, warn, error};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
extern crate pretty_env_logger;

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Deserialize, Serialize)]
struct EthBlock {
    /// Hash of the block
    pub hash: H256,
    /// Hash of the parent
    #[serde(default, rename = "parentHash")]
    pub parent_hash: H256,
    /// Hash of the uncles

    #[serde(default, rename = "sha3Uncles")]
    pub uncles_hash: H256,
    /// Miner/author's address. None if pending.
    #[serde(default, rename = "miner")]
    pub author: Address,
    /// State root hash
    #[serde(default, rename = "stateRoot")]
    pub state_root: H256,
    /// Transactions root hash
    #[serde(default, rename = "transactionsRoot")]
    pub transactions_root: H256,
    /// Transactions receipts root hash
    #[serde(default, rename = "receiptsRoot")]
    pub receipts_root: H256,
    /// Block number. None if pending.
    #[serde(with = "helpers::u64")]
    pub number: U64,
    /// Gas Used
    #[serde(default, rename = "gasUsed")]
    pub gas_used: U256,
    /// Gas Limit

    #[serde(default, rename = "gasLimit")]
    pub gas_limit: U256,
    /// Extra data
    #[serde(default, rename = "extraData")]
    pub extra_data: Bytes,
    /// Logs bloom
    #[serde(rename = "logsBloom")]
    pub logs_bloom: Option<Bloom>,
    /// Timestamp
    #[serde(default, with = "helpers::u256as64")]
    pub timestamp: U256,
    /// Difficulty

    #[serde(default)]
    pub difficulty: U256,
    /// Total difficulty
    #[serde(rename = "totalDifficulty")]
    pub total_difficulty: Option<U256>,
    /// Seal fields
    #[serde(
        default,
        rename = "sealFields",
        deserialize_with = "deserialize_null_default"
    )]
    pub seal_fields: Vec<Bytes>,
    /// Uncles' hashes

    #[serde(default)]
    pub uncles: Vec<H256>,

    /// Size in bytes
    #[serde(with = "helpers::u256as64")]
    pub size: U256,
    /// Mix Hash
    #[serde(rename = "mixHash")]
    pub mix_hash: Option<H256>,

    /// Nonce
    pub nonce: Option<H64>,
    /// Base fee per unit of gas (if past London)
    #[serde(rename = "baseFeePerGas")]
    pub base_fee_per_gas: Option<U256>,
    /// Withdrawals root hash (if past Shanghai)
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "withdrawalsRoot"
    )]
    pub withdrawals_root: Option<H256>,
    /// Withdrawals (if past Shanghai)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawals: Option<Vec<Withdrawal>>,

    /// Captures unknown fields such as additional fields used by L2s
    #[serde(flatten)]
    pub other: OtherFields,
}

/// Details of a signed transaction
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct TransactionWithReceipt {
    /// The transaction's hash
    pub hash: H256,

    /// The transaction's nonce
    pub nonce: U256,

    /// Block hash. None when pending.
    #[serde(default, rename = "blockHash")]
    pub block_hash: H256,

    /// Block number. None when pending.
    #[serde(default, rename = "blockNumber")]
    #[serde(with = "helpers::u64")]
    pub block_number: U64,

    /// Transaction Index. None when pending.
    #[serde(default, rename = "transactionIndex")]
    #[serde(with = "helpers::u64")]
    pub transaction_index: U64,

    /// Sender
    #[serde(default = "Address::zero")]
    pub from: Address,

    /// Recipient (None when contract creation)
    #[serde(default)]
    pub to: Option<Address>,

    /// Transferred value
    pub value: U256,

    /// Gas Price, null for Type 2 transactions
    #[serde(rename = "gasPrice")]
    pub gas_price: Option<U256>,

    /// Gas amount
    pub gas: U256,

    /// Input data
    pub input: Bytes,

    /// ECDSA recovery id
    pub v: U64,

    /// ECDSA signature r
    pub r: U256,

    /// ECDSA signature s
    pub s: U256,

    // EIP2718
    /// Transaction type, Some(2) for EIP-1559 transaction,
    /// Some(1) for AccessList transaction, None for Legacy
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub transaction_type: Option<U64>,

    // EIP2930
    #[serde(
        rename = "accessList",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub access_list: Option<AccessList>,

    #[serde(
        rename = "maxPriorityFeePerGas",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Represents the maximum tx fee that will go to the miner as part of the user's
    /// fee payment. It serves 3 purposes:
    /// 1. Compensates miners for the uncle/ommer risk + fixed costs of including transaction in a
    /// block; 2. Allows users with high opportunity costs to pay a premium to miners;
    /// 3. In times where demand exceeds the available block space (i.e. 100% full, 30mm gas),
    /// this component allows first price auctions (i.e. the pre-1559 fee model) to happen on the
    /// priority fee.
    ///
    /// More context [here](https://hackmd.io/@q8X_WM2nTfu6nuvAzqXiTQ/1559-wallets)
    pub max_priority_fee_per_gas: Option<U256>,

    #[serde(
        rename = "maxFeePerGas",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    /// Represents the maximum amount that a user is willing to pay for their tx (inclusive of
    /// baseFeePerGas and maxPriorityFeePerGas). The difference between maxFeePerGas and
    /// baseFeePerGas + maxPriorityFeePerGas is “refunded” to the user.
    pub max_fee_per_gas: Option<U256>,

    #[serde(rename = "chainId", default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<U256>,

    /// Cumulative gas used within the block after this was executed.
    #[serde(rename = "cumulativeGasUsed")]
    pub cumulative_gas_used: U256,
    /// Gas used by this transaction alone.
    ///
    /// Gas used is `None` if the the client is running in light client mode.
    #[serde(rename = "gasUsed")]
    pub gas_used: Option<U256>,
    /// Contract address created, or `None` if not a deployment.
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<Address>,

    /// Logs generated within this transaction.
    pub logs: Vec<Log>,

    /// Status: either 1 (success) or 0 (failure). Only present after activation of [EIP-658](https://eips.ethereum.org/EIPS/eip-658)
    pub status: Option<U64>,

    /// State root. Only present before activation of [EIP-658](https://eips.ethereum.org/EIPS/eip-658)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<H256>,

    /// Logs bloom
    #[serde(rename = "logsBloom")]
    pub logs_bloom: Bloom,

    /// The price paid post-execution by the transaction (i.e. base fee + priority fee).
    /// Both fields in 1559-style transactions are *maximums* (max fee + max priority fee), the
    /// amount that's actually paid by users can only be determined post-execution
    #[serde(
        rename = "effectiveGasPrice",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub effective_gas_price: Option<U256>,

    /// Captures unknown fields such as additional fields used by L2s
    #[serde(flatten)]
    pub other_pre: OtherFields,

    #[serde(flatten)]
    pub other_post: OtherFields,
}

/// A log produced by a transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Log {
    /// H160. the contract that emitted the log
    pub address: Address,

    /// topics: Array of 0 to 4 32 Bytes of indexed log arguments.
    /// (In solidity: The first topic is the hash of the signature of the event
    /// (e.g. `Deposit(address,bytes32,uint256)`), except you declared the event
    /// with the anonymous specifier.)
    pub topics: Vec<H256>,

    /// Data
    pub data: Bytes,

    /// Block Hash
    #[serde(rename = "blockHash")]
    pub block_hash: H256,

    /// Block Number
    #[serde(rename = "blockNumber")]
    #[serde(with = "helpers::u64")]
    pub block_number: U64,

    /// Transaction Hash
    #[serde(rename = "transactionHash")]
    pub transaction_hash: H256,

    /// Transaction Index
    #[serde(rename = "transactionIndex")]
    #[serde(with = "helpers::u64")]
    pub transaction_index: U64,

    /// Integer of the log index position in the block. None if it's a pending log.
    #[serde(rename = "logIndex")]
    #[serde(with = "helpers::u256as64")]
    pub log_index: U256,

    /// Integer of the transactions index position log was created from.
    /// None when it's a pending log.
    #[serde(rename = "transactionLogIndex")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(with = "helpers::u256as64::option")]
    pub transaction_log_index: Option<U256>,

    /// Log Type
    #[serde(rename = "logType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_type: Option<String>,

    /// True when the log was removed, due to a chain reorganization.
    /// false if it's a valid log.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed: Option<bool>,
}

pub async fn main(
    mongo_uri: &str,
    eth_uri: &str,
    action_type: &ClapActionType,
) -> Result<(), Box<dyn Error>> {
    // Parse a connection string into an options struct.
    let mut client_options = ClientOptions::parse(mongo_uri).await?;

    // Manually set an option.
    client_options.app_name = Some("KylinKit.mongo_eth".to_string());

    // Get a handle to the deployment.
    let mongo_client = Client::with_options(client_options)?;

    let provider = Provider::<Ws>::connect(eth_uri).await?;

    match action_type {
        ClapActionType::Init {from, init_trace } => {
            init(mongo_client, provider, *from, *init_trace).await?;
        }
        ClapActionType::Sync {} => todo!(),
        ClapActionType::GraphQL {} => todo!(),
    }

    Ok(())
}

async fn init(client: Client, provider: Provider<Ws>, from: u64, init_trace: bool) -> Result<(), Box<dyn Error>> {
    // let mut session = client.start_session(None).await.unwrap();
    // session.start_transaction(None).await?;

    let db = client.database("ethereum");

    // TODO: try get latest from mongo
    let num = 0_u64;

    let latest: u64 = provider.get_block_number().await?.as_u64();
    let to = latest / 1_000 * 1_000;

    warn!("target: {}", to);

    let retry_strategy = ExponentialBackoff::from_millis(1000)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    async fn get_data(
        provider: &Provider<Ws>,
        num: u64,
    ) -> Result<
        (
            Option<Block<Transaction>>,
            Vec<TransactionReceipt>,
            Vec<Trace>,
        ),
        ProviderError,
    > {
        let result = tokio::try_join!(
            provider.get_block_with_txs(num),
            provider.get_block_receipts(num),
            provider.trace_block(num.into())
        );
        if result.is_err() {
            error!("{:?}", result);
        }
        
        result
    }

    for num in from..=to {
        // provider.debug_trace_block_by_number(num, trace_options);

        let (block, receipts, traces) = Retry::spawn(retry_strategy.clone(), || get_data(&provider, num)).await?;

        let block = block.as_ref().unwrap();

        let block_coll: Collection<EthBlock> = db.collection("blocks");
        let transaction_coll: Collection<TransactionWithReceipt> = db.collection("transactions");
        let trace_coll: Collection<Trace> = db.collection("traces");

        let mut txs = Vec::with_capacity(block.transactions.len());
        for (i, transaction) in block.transactions.iter().enumerate() {
            let receipt = &receipts[i];
            let tx = TransactionWithReceipt {
                hash: transaction.hash,
                nonce: transaction.nonce,
                block_hash: transaction.block_hash.unwrap(),
                block_number: transaction.block_number.unwrap(),
                transaction_index: transaction.transaction_index.unwrap(),
                from: transaction.from,
                to: transaction.to,
                value: transaction.value,
                gas_price: transaction.gas_price,
                gas: transaction.gas,
                input: transaction.input.clone(),
                v: transaction.v,
                r: transaction.r,
                s: transaction.s,
                transaction_type: transaction.transaction_type,
                access_list: transaction.access_list.clone(),
                max_priority_fee_per_gas: transaction.max_priority_fee_per_gas,
                max_fee_per_gas: transaction.max_fee_per_gas,
                chain_id: transaction.chain_id,
                cumulative_gas_used: receipt.cumulative_gas_used,
                gas_used: receipt.gas_used,
                contract_address: receipt.contract_address,
                logs: receipt
                    .logs
                    .iter()
                    .map(|log| Log {
                        address: log.address,
                        topics: log.topics.clone(),
                        data: log.data.clone(),
                        block_hash: log.block_hash.unwrap(),
                        block_number: log.block_number.unwrap(),
                        transaction_hash: log.transaction_hash.unwrap(),
                        transaction_index: log.transaction_index.unwrap(),
                        log_index: log.log_index.unwrap(),
                        transaction_log_index: log.transaction_log_index,
                        log_type: log.log_type.clone(),
                        removed: log.removed,
                    })
                    .collect(),
                status: receipt.status,
                root: receipt.root,
                logs_bloom: receipt.logs_bloom,
                effective_gas_price: receipt.effective_gas_price,
                other_pre: transaction.other.clone(),
                other_post: receipt.other.clone(),
            };
            txs.push(tx);
        }

        let tx_fu = async move {
            if txs.len() > 0 {
                Some(transaction_coll.insert_many(txs, None).await)
            } else {
                None
            }
        };

        let trace_fu = async move { if traces.len() > 0 {
            Some(trace_coll.insert_many(traces, None).await) 
        } else {
            None
        }};

        let block_fu = block_coll.insert_one(
            EthBlock {
                hash: block.hash.unwrap(),
                parent_hash: block.parent_hash,
                uncles_hash: block.uncles_hash,
                author: block.author.unwrap(),
                state_root: block.state_root,
                transactions_root: block.transactions_root,
                receipts_root: block.receipts_root,
                number: block.number.unwrap(),
                gas_used: block.gas_used,
                gas_limit: block.gas_limit,
                extra_data: block.extra_data.clone(),
                logs_bloom: block.logs_bloom,
                timestamp: block.timestamp,
                difficulty: block.difficulty,
                total_difficulty: block.total_difficulty,
                seal_fields: block.seal_fields.clone(),
                uncles: block.uncles.clone(),
                size: block.size.unwrap(),
                mix_hash: block.mix_hash,
                nonce: block.nonce,
                base_fee_per_gas: block.base_fee_per_gas,
                withdrawals_root: block.withdrawals_root,
                withdrawals: block.withdrawals.clone(),
                other: block.other.clone(),
            },
            None,
        );

        let (tx_result, trace_result, block_result) = join!(tx_fu, trace_fu, block_fu);
        if let Some(tx_result) = tx_result {
            let _ = tx_result?;
        }
        if let Some(trace_result) = trace_result {
            let _ = trace_result?;
        }

        let _ = block_result?;

        if num % 1000 == 0 {
            info!("{} done", num)
        }
    }

    // session.commit_transaction().await?;

    Ok(())
}
