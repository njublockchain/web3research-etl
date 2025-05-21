use clickhouse::Row;
use documented::Documented;
use ethers::{abi::AbiEncode, types::{Action, Block, Log, Res, Trace, Transaction, TransactionReceipt, Withdrawal, U256}, utils::hex::ToHexExt};
use serde::{Deserialize, Serialize};
use serde_variant::to_variant_name;

use super::utils;

mod u256 {
    use ethers::types::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(u: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        u.0.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: [u64; 4] = Deserialize::deserialize(deserializer)?;
        Ok(U256(u))
    }
}

mod optional_u256 {
    use ethers::types::U256;
    use serde::{
        de::{Deserialize, Deserializer},
        ser::{Serialize, Serializer},
    };

    pub fn serialize<S: Serializer>(u: &Option<U256>, serializer: S) -> Result<S::Ok, S::Error> {
        match u {
            Some(u) => u.0.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<U256>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let u: Option<[u64; 4]> = Deserialize::deserialize(deserializer)?;
        Ok(u.map(U256))
    }
}

/** CREATE TABLE IF NOT EXISTS blocks (
    hash             String,
    number           UInt64,
    parentHash       String,
    uncles           Array(String),
    totalDifficulty  Nullable(String),
    miner            Nullable(String),
    difficulty       String,
    nonce            String,
    baseFeePerGas    Nullable(String),
    gasLimit         String,
    gasUsed          String,
    extraData        String,
    timestamp        UInt64,
    size             Nullable(UInt64)
) ENGINE=ReplacingMergeTree
ORDER BY (hash, number);
*/
#[derive(Clone, Debug, Default, Documented, Row, Deserialize, Serialize)]
pub struct BlockRow {
    pub hash: String,
    pub number: u64,
    #[serde(rename = "parentHash")]
    pub parent_hash: String,
    pub uncles: Vec<String>,
    #[serde(rename = "totalDifficulty")]
    pub total_difficulty: Option<String>,
    pub difficulty: String,
    pub miner: Option<String>,
    pub nonce: Option<String>,
    #[serde(rename = "baseFeePerGas")]
    pub base_fee_per_gas: Option<String>,
    #[serde(rename = "gasLimit")]
    pub gas_limit: String,
    #[serde(rename = "gasUsed")]
    pub gas_used: String,
    #[serde(rename = "extraData")]
    pub extra_data: String,
    pub timestamp: u64,
    pub size: Option<u64>,
}

impl BlockRow {
    pub fn from_ethers<T>(block: &Block<T>) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            hash: block.hash.unwrap().encode_hex(),
            number: block.number.unwrap().as_u64(),
            parent_hash: block.parent_hash.encode_hex(),
            uncles: block
                .uncles
                .iter()
                .map(|uncle| uncle.encode_hex())
                .collect(),
            total_difficulty: block.total_difficulty.map(|td| td.encode_hex()),
            difficulty: block.difficulty.encode_hex(),
            miner: block.author.map(|author| author.encode_hex()),
            nonce: block.nonce.map(|nonce| nonce.encode_hex()),
            base_fee_per_gas: block.base_fee_per_gas.map(|fee| fee.encode_hex()),
            gas_limit: block.gas_limit.encode_hex(),
            gas_used: block.gas_used.encode_hex(),
            extra_data: block.extra_data.clone().encode_hex(),
            timestamp: block.timestamp.as_u64(),
            size: block.size.map(|size| size.as_u64()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS transactions (
    hash             FixedString(66),
    blockHash        FixedString(66),
    blockNumber      UInt64,
    blockTimestamp   UInt256,
    transactionIndex UInt64,
    chainId          Nullable(UInt256),
    type             Nullable(UInt64),
    from             String,
    to               Nullable(String),
    value            UInt256,
    nonce            UInt256,
    input            String,
    gas              UInt256,
    gasPrice         Nullable(UInt256),
    maxFeePerGas     Nullable(UInt256),
    maxPriorityFeePerGas Nullable(UInt256),
    r                UInt256,
    s                UInt256,
    v                UInt64,
    accessList       Nullable(String),
    contractAddress  Nullable(String),
    cumulativeGasUsed UInt256,
    effectiveGasPrice Nullable(UInt256),
    gasUsed          UInt256,
    logsBloom        String,
    root             Nullable(FixedString(66)),
    status           Nullable(UInt64)
) ENGINE=ReplacingMergeTree
ORDER BY (blockNumber, blockTimestamp, blockHash, from, nonce, to, transactionIndex, hash)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Clone, Debug, Default, Documented, Row, Deserialize, Serialize)]
pub struct TransactionRow {
    pub hash: String,
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "blockTimestamp")]
    #[serde(with = "u256")]
    pub block_timestamp: U256,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: u64,
    #[serde(rename = "chainId")]
    pub chain_id: Option<String>,
    #[serde(rename = "type")]
    pub r#type: Option<u64>,
    pub from: String,
    pub to: Option<String>,
    #[serde(with = "u256")]
    pub value: U256,
    #[serde(with = "u256")]
    pub nonce: U256,
    pub input: String,
    #[serde(with = "u256")]
    pub gas: U256,
    #[serde(rename = "gasPrice")]
    pub gas_price: Option<String>,
    #[serde(rename = "maxFeePerGas")]
    pub max_fee_per_gas: Option<String>,
    #[serde(rename = "maxPriorityFeePerGas")]
    pub max_priority_fee_per_gas: Option<String>,
    #[serde(with = "u256")]
    pub r: U256,
    #[serde(with = "u256")]
    pub s: U256,
    pub v: u64,
    #[serde(rename = "accessList")]
    pub access_list: Option<String>,
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<String>,
    #[serde(rename = "cumulativeGasUsed")]
    #[serde(with = "u256")]
    pub cumulative_gas_used: U256,
    #[serde(rename = "effectiveGasPrice")]
    pub effective_gas_price: Option<String>,
    #[serde(rename = "gasUsed")]
    #[serde(with = "u256")]
    pub gas_used: U256,
    #[serde(rename = "logsBloom")]
    pub logs_bloom: String,
    pub root: Option<String>,
    pub status: Option<u64>,
}

impl TransactionRow {
    pub fn from_ethers<T>(
        block: &Block<T>,
        transaction: &Transaction,
        receipt: &TransactionReceipt,
    ) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            hash: utils::bytes_to_eth_hex(&transaction.hash.0),
            block_hash: utils::bytes_to_eth_hex(&transaction.block_hash.unwrap().0),
            block_number: transaction.block_number.unwrap().as_u64(),
            block_timestamp: block.timestamp,
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            chain_id: transaction.chain_id.map(|id| id.to_string()),
            r#type: transaction.transaction_type.map(|t| t.as_u64()),
            from: utils::bytes_to_eth_hex(&transaction.from.0),
            to: transaction.to.map(|to| utils::bytes_to_eth_hex(&to.0)),
            value: transaction.value,
            nonce: transaction.nonce,
            input: utils::bytes_to_eth_hex(&transaction.input),
            gas: transaction.gas,
            gas_price: transaction.gas_price.map(|price| price.to_string()),
            max_fee_per_gas: transaction.max_fee_per_gas.map(|fee| fee.to_string()),
            max_priority_fee_per_gas: transaction
                .max_priority_fee_per_gas
                .map(|fee| fee.to_string()),
            r: transaction.r,
            s: transaction.s,
            v: transaction.v.as_u64(),
            access_list: transaction
                .access_list
                .as_ref()
                .map(|al| serde_json::to_string(&al.clone().to_owned()).unwrap()),
            contract_address: receipt
                .contract_address
                .map(|contract| utils::bytes_to_eth_hex(&contract.0)),
            cumulative_gas_used: receipt.cumulative_gas_used,
            effective_gas_price: receipt.effective_gas_price.map(|price| price.to_string()),
            gas_used: receipt.gas_used.unwrap(),
            logs_bloom: utils::bytes_to_eth_hex(&receipt.logs_bloom.0),
            root: receipt.root.map(|root| utils::bytes_to_eth_hex(&root.0)), // Only present before activation of [EIP-658]
            status: receipt.status.map(|status| status.as_u64()), // Only present after activation of [EIP-658]
        }
    }
}

/** CREATE TABLE IF NOT EXISTS events (
    `address` String,
    `blockHash` FixedString(66),
    `blockNumber` UInt64,
    `blockTimestamp` UInt256,
    `transactionHash` FixedString(66),
    `transactionIndex` UInt64,
    `logIndex` UInt256,
    `removed` Bool,
    `topic0` Nullable(FixedString(66)),
    `topic1` Nullable(FixedString(66)),
    `topic2` Nullable(FixedString(66)),
    `topic3` Nullable(FixedString(66)),
    `data` String
)
ENGINE = ReplacingMergeTree
ORDER BY (removed, address, topic0, topic1, topic2, topic3, transactionHash, logIndex)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Clone, Debug, Default, Documented, Row, Deserialize, Serialize)]
pub struct EventRow {
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "blockTimestamp")]
    #[serde(with = "u256")]
    pub block_timestamp: U256,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: u64,
    #[serde(rename = "logIndex")]
    #[serde(with = "u256")]
    pub log_index: U256,
    pub removed: bool,
    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub data: String,
    pub address: String,
}

impl EventRow {
    pub fn from_ethers<T>(block: &Block<T>, transaction: &Transaction, log: &Log) -> Self
    where
        T: serde::ser::Serialize,
    {
        let topics: Vec<String> = log
            .topics
            .iter()
            .map(|topic| utils::bytes_to_eth_hex(&topic.0))
            .collect();

        Self {
            block_hash: utils::bytes_to_eth_hex(&log.block_hash.unwrap().0),
            block_number: log.block_number.unwrap().as_u64(),
            block_timestamp: block.timestamp,
            transaction_hash: utils::bytes_to_eth_hex(&transaction.hash.0),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            log_index: log.log_index.unwrap(),
            removed: log.removed.unwrap(),
            topic0: topics.get(0).cloned(),
            topic1: topics.get(1).cloned(),
            topic2: topics.get(2).cloned(),
            topic3: topics.get(3).cloned(),
            data: utils::bytes_to_eth_hex(&log.data),
            address: utils::bytes_to_eth_hex(&log.address.0),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS withdraws (
    blockHash String,
    blockNumber UInt64,
    blockTimestamp UInt256,
    `index` UInt64,
    validatorIndex UInt64,
    address String,
    amount UInt256
) ENGINE=ReplacingMergeTree
ORDER BY (blockHash, index);
*/
#[derive(Clone, Debug, Default, Documented, Row, Deserialize, Serialize)]
pub struct WithdrawalRow {
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "blockTimestamp")]
    #[serde(with = "u256")]
    pub block_timestamp: U256,
    pub index: u64,
    #[serde(rename = "validatorIndex")]
    pub validator_index: u64,
    pub address: String,
    #[serde(with = "u256")]
    pub amount: U256,
}

impl WithdrawalRow {
    pub fn from_ethers<T>(block: &Block<T>, withdraw: &Withdrawal) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            block_hash: utils::bytes_to_eth_hex(&block.hash.unwrap().0),
            block_number: block.number.unwrap().as_u64(),
            block_timestamp: block.timestamp,
            index: withdraw.index.as_u64(),
            validator_index: withdraw.validator_index.as_u64(),
            address: utils::bytes_to_eth_hex(&withdraw.address.0),
            amount: withdraw.amount,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS traces (
    `blockPos`    UInt64,
    `blockNumber` UInt64,
    `blockTimestamp` UInt256,
    `blockHash` FixedString(66),
    `transactionHash` Nullable(FixedString(66)),
    `traceAddress` Array(UInt64),
    `subtraces` UInt64,
    `transactionPosition` Nullable(UInt64),
    `error` Nullable(String),
    `actionType` LowCardinality(String),
    `actionCallFrom` Nullable(String),
    `actionCallTo` Nullable(String),
    `actionCallValue` Nullable(UInt256),
    `actionCallInput` Nullable(String),
    `actionCallGas` Nullable(UInt256),
    `actionCallType` LowCardinality(String),
    `actionCreateFrom` Nullable(String),
    `actionCreateValue` Nullable(UInt256),
    `actionCreateInit` Nullable(String),
    `actionCreateGas` Nullable(UInt256),
    `actionSuicideAddress` Nullable(String),
    `actionSuicideRefundAddress` Nullable(String),
    `actionSuicideBalance` Nullable(UInt256),
    `actionRewardAuthor` Nullable(String),
    `actionRewardValue` Nullable(UInt256),
    `actionRewardType` LowCardinality(String),
    `resultType` LowCardinality(String),
    `resultCallGasUsed` Nullable(UInt256),
    `resultCallOutput` Nullable(String),
    `resultCreateGasUsed` Nullable(UInt256),
    `resultCreateCode` Nullable(String),
    `resultCreateAddress` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (blockNumber, blockPos);
*/
#[derive(Clone, Debug, Documented, Row, Deserialize, Serialize)]
pub struct TraceRow {
    #[serde(rename = "blockPos")]
    pub block_pos: u64,
    /// Block Number
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "blockTimestamp")]
    #[serde(with = "u256")]
    pub block_timestamp: U256,
    /// Block Hash
    #[serde(rename = "blockHash")]
    pub block_hash: String,

    /// Trace address, The list of addresses where the call was executed, the address of the parents, and the order of the current sub call
    #[serde(rename = "traceAddress")]
    pub trace_address: Vec<u64>,
    /// Subtraces
    pub subtraces: u64,
    /// Transaction position
    #[serde(rename = "transactionPosition")]
    pub transaction_position: Option<u64>,
    /// Transaction hash
    #[serde(rename = "transactionHash")]
    pub transaction_hash: Option<String>,

    /// Error, See also [`TraceError`]
    pub error: Option<String>,

    /// Action
    ///
    // pub action: Action, // call create suicide reward
    #[serde(rename = "actionType")]
    pub action_type: String, // Enum('Call', 'Create', 'Suicide', 'Reward')
    /// Sender
    #[serde(rename = "actionCallFrom")]
    pub action_call_from: Option<String>,
    /// Recipient
    #[serde(rename = "actionCallTo")]
    pub action_call_to: Option<String>,
    /// Transferred Value
    #[serde(rename = "actionCallValue")]
    #[serde(with = "optional_u256")]
    pub action_call_value: Option<U256>,
    /// Input data
    #[serde(rename = "actionCallInput")]
    pub action_call_input: Option<String>,
    #[serde(rename = "actionCallGas")]
    #[serde(with = "optional_u256")]
    pub action_call_gas: Option<U256>,
    /// The type of the call.
    #[serde(rename = "actionCallType")]
    pub action_call_type: String, // none call callcode delegatecall staticcall
    #[serde(rename = "actionCreateFrom")]
    pub action_create_from: Option<String>,
    #[serde(rename = "actionCreateValue")]
    #[serde(with = "optional_u256")]
    pub action_create_value: Option<U256>,
    #[serde(rename = "actionCreateInit")]
    pub action_create_init: Option<String>,
    #[serde(rename = "actionCreateGas")]
    #[serde(with = "optional_u256")]
    pub action_create_gas: Option<U256>,
    #[serde(rename = "actionSuicideAddress")]
    pub action_suicide_address: Option<String>,
    #[serde(rename = "actionSuicideRefundAddress")]
    pub action_suicide_refund_address: Option<String>,
    #[serde(rename = "actionSuicideBalance")]
    #[serde(with = "optional_u256")]
    pub action_suicide_balance: Option<U256>,
    #[serde(rename = "actionRewardAuthor")]
    pub action_reward_author: Option<String>,
    #[serde(rename = "actionRewardValue")]
    #[serde(with = "optional_u256")]
    pub action_reward_value: Option<U256>,
    #[serde(rename = "actionRewardType")]
    pub action_reward_type: String, // LowCardinality ('block', 'uncle', 'emptyStep', 'external')
    /// Result
    //  pub result: Option<Res>, // call {gasused, output} create {gas_used, code, address} none
    #[serde(rename = "resultType")]
    pub result_type: String, // LowCardinality ('none', 'call', 'create')
    #[serde(rename = "resultCallGasUsed")]
    #[serde(with = "optional_u256")]
    pub result_call_gas_used: Option<U256>,
    #[serde(rename = "resultCallOutput")]
    pub result_call_output: Option<String>,
    #[serde(rename = "resultCreateGasUsed")]
    #[serde(with = "optional_u256")]
    pub result_create_gas_used: Option<U256>,
    #[serde(rename = "resultCreateCode")]
    pub result_create_code: Option<String>,
    #[serde(rename = "resultCreateAddress")]
    pub result_create_address: Option<String>,
}

impl TraceRow {
    pub fn from_ethers<T>(block: &Block<T>, trace: &Trace, index: usize) -> Self
    where
        T: serde::ser::Serialize,
    {
        let mut trace_row = Self {
            block_pos: index as u64,
            action_type: to_variant_name(&trace.action_type).unwrap().to_string(),
            action_call_from: None,
            action_call_to: None,
            action_call_value: None,
            action_call_input: None,
            action_call_gas: None,
            action_call_type: "".to_owned(),
            action_create_from: None,
            action_create_value: None,
            action_create_init: None,
            action_create_gas: None,
            action_suicide_address: None,
            action_suicide_refund_address: None,
            action_suicide_balance: None,
            action_reward_author: None,
            action_reward_value: None,
            action_reward_type: "".to_owned(),
            result_type: "".to_owned(),
            result_call_gas_used: None,
            result_call_output: None,
            result_create_gas_used: None,
            result_create_code: None,
            result_create_address: None,
            trace_address: trace.trace_address.iter().map(|t| *t as u64).collect(),
            subtraces: trace.subtraces as u64,
            transaction_position: trace.transaction_position.map(|pos| pos as u64),
            transaction_hash: trace
                .transaction_hash
                .map(|h| utils::bytes_to_eth_hex(&h.0)),
            block_number: trace.block_number,
            block_timestamp: block.timestamp,
            block_hash: utils::bytes_to_eth_hex(&trace.block_hash.0),
            error: trace.error.clone(),
        };

        // fill action
        match &trace.action {
            Action::Call(call) => {
                trace_row.action_call_from = Some(utils::bytes_to_eth_hex(&call.from.0));
                trace_row.action_call_to = Some(utils::bytes_to_eth_hex(&call.to.0));
                trace_row.action_call_type = to_variant_name(&call.call_type).unwrap().to_string();
                trace_row.action_call_gas = Some(call.gas);
                trace_row.action_call_input = Some(utils::bytes_to_eth_hex(&call.input.0));
                trace_row.action_call_value = Some(call.value);
            }
            Action::Create(create) => {
                trace_row.action_create_from = Some(utils::bytes_to_eth_hex(&create.from.0));
                trace_row.action_create_init = Some(utils::bytes_to_eth_hex(&create.init.0));
                trace_row.action_create_value = Some(create.value);
                trace_row.action_create_gas = Some(create.gas);
            }
            Action::Suicide(suicide) => {
                trace_row.action_suicide_address =
                    Some(utils::bytes_to_eth_hex(&suicide.address.0));
                trace_row.action_suicide_balance = Some(suicide.balance);
                trace_row.action_suicide_refund_address =
                    Some(utils::bytes_to_eth_hex(&suicide.refund_address.0));
            }
            Action::Reward(reward) => {
                trace_row.action_reward_author = Some(utils::bytes_to_eth_hex(&reward.author.0));
                trace_row.action_reward_type =
                    to_variant_name(&reward.reward_type).unwrap().to_string();
                trace_row.action_reward_value = Some(reward.value);
            }
        }

        match &trace.result {
            Some(result) => match result {
                Res::Call(call) => {
                    trace_row.result_type = "call".to_owned();
                    trace_row.result_call_gas_used = Some(call.gas_used);
                    trace_row.result_call_output = Some(utils::bytes_to_eth_hex(&call.output.0));
                }
                Res::Create(create) => {
                    trace_row.result_type = "create".to_owned();
                    trace_row.result_create_address =
                        Some(utils::bytes_to_eth_hex(&create.address.0));
                    trace_row.result_create_code = Some(utils::bytes_to_eth_hex(&create.code.0));
                    trace_row.result_create_gas_used = Some(create.gas_used)
                }
                Res::None => {
                    trace_row.result_type = "none".to_owned();
                }
            },
            None => {
                trace_row.result_type = "none".to_owned();
            }
        }

        trace_row
    }
}
