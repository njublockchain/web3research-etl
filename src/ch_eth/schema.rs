use clickhouse::Row;
use documented::Documented;
use ethers::types::{Action, Block, Log, Res, Trace, Transaction, TransactionReceipt, Withdrawal};
use serde::{Deserialize, Serialize};
use serde_variant::to_variant_name;

use super::utils;

/** CREATE TABLE IF NOT EXISTS blocks (
    hash             FixedString(32),
    number           UInt64,
    parentHash       FixedString(32),
    uncles           Array(String),
    sha3Uncles       FixedString(32),
    totalDifficulty  UInt256,
    miner            FixedString(20),
    difficulty       UInt256,
    nonce            FixedString(8),
    mixHash          FixedString(32),
    baseFeePerGas    Nullable(UInt256),
    gasLimit         UInt256,
    gasUsed          UInt256,
    stateRoot        FixedString(32),
    transactionsRoot FixedString(32),
    receiptsRoot     FixedString(32),
    logsBloom        String,
    withdrawlsRoot   Nullable(FixedString(32)),
    extraData        String,
    timestamp        UInt256,
    size             UInt256
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
    #[serde(rename = "sha3Uncles")]
    pub sha3_uncles: String,
    #[serde(rename = "totalDifficulty")]
    pub total_difficulty: String,
    pub difficulty: String,
    pub miner: String,
    pub nonce: String,
    #[serde(rename = "mixHash")]
    pub mix_hash: String,
    #[serde(rename = "baseFeePerGas")]
    pub base_fee_per_gas: Option<String>,
    #[serde(rename = "gasLimit")]
    pub gas_limit: String,
    #[serde(rename = "gasUsed")]
    pub gas_used: String,
    #[serde(rename = "stateRoot")]
    pub state_root: String,
    #[serde(rename = "transactionsRoot")]
    pub transactions_root: String,
    #[serde(rename = "receiptsRoot")]
    pub receipts_root: String,
    #[serde(rename = "logsBloom")]
    pub logs_bloom: String,
    #[serde(rename = "withdrawlsRoot")]
    pub withdrawls_root: Option<String>,
    #[serde(rename = "extraData")]
    pub extra_data: String,
    pub timestamp: String,
    pub size: String,
}

impl BlockRow {
    pub fn from_ethers<T>(block: &Block<T>) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            hash: utils::bytes_to_eth_hex(&block.hash.unwrap().0),
            number: block.number.unwrap().as_u64(),
            parent_hash: utils::bytes_to_eth_hex(&block.parent_hash.0),
            uncles: block
                .uncles
                .iter()
                .map(|uncle| utils::bytes_to_eth_hex(&uncle.0))
                .collect(),
            sha3_uncles: utils::bytes_to_eth_hex(&block.uncles_hash.0),
            total_difficulty: block.total_difficulty.unwrap_or_default().to_string(),
            difficulty: block.difficulty.to_string(),
            miner: utils::bytes_to_eth_hex(&block.author.unwrap().0),
            nonce: utils::bytes_to_eth_hex(&block.nonce.unwrap().0),
            mix_hash: utils::bytes_to_eth_hex(&block.mix_hash.unwrap().0),
            base_fee_per_gas: block.base_fee_per_gas.map(|fee| fee.to_string()),
            gas_limit: block.gas_limit.to_string(),
            gas_used: block.gas_used.to_string(),
            state_root: utils::bytes_to_eth_hex(&block.state_root.0),
            transactions_root: utils::bytes_to_eth_hex(&block.transactions_root.0),
            receipts_root: utils::bytes_to_eth_hex(&block.receipts_root.0),
            logs_bloom: utils::bytes_to_eth_hex(&block.logs_bloom.unwrap().0),
            withdrawls_root: block
                .withdrawals_root
                .map(|root| utils::bytes_to_eth_hex(&root.0)),
            extra_data: utils::bytes_to_eth_hex(&block.extra_data),
            timestamp: block.timestamp.to_string(),
            size: block.size.unwrap().to_string(),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS transactions (
    hash             FixedString(32),
    blockHash        FixedString(32),
    blockNumber      UInt64,
    blockTimestamp   UInt256,
    transactionIndex UInt64,
    chainId          Nullable(UInt256),
    type             Nullable(UInt64),
    from             FixedString(20),
    to               Nullable(FixedString(20)),
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
    contractAddress  Nullable(FixedString(20)),
    cumulativeGasUsed UInt256,
    effectiveGasPrice Nullable(UInt256),
    gasUsed          UInt256,
    logsBloom        String,
    root             Nullable(FixedString(32)),
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
    pub block_timestamp: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: u64,
    #[serde(rename = "chainId")]
    pub chain_id: Option<String>,
    #[serde(rename = "type")]
    pub r#type: Option<u64>,
    pub from: String,
    pub to: Option<String>,
    pub value: String,
    pub nonce: String,
    pub input: String,
    pub gas: String,
    #[serde(rename = "gasPrice")]
    pub gas_price: Option<String>,
    #[serde(rename = "maxFeePerGas")]
    pub max_fee_per_gas: Option<String>,
    #[serde(rename = "maxPriorityFeePerGas")]
    pub max_priority_fee_per_gas: Option<String>,
    pub r: String,
    pub s: String,
    pub v: u64,
    #[serde(rename = "accessList")]
    pub access_list: Option<String>,
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<String>,
    #[serde(rename = "cumulativeGasUsed")]
    pub cumulative_gas_used: String,
    #[serde(rename = "effectiveGasPrice")]
    pub effective_gas_price: Option<String>,
    #[serde(rename = "gasUsed")]
    pub gas_used: String,
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
            block_timestamp: block.timestamp.to_string(),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            chain_id: transaction.chain_id.map(|id| id.to_string()),
            r#type: transaction.transaction_type.map(|t| t.as_u64()),
            from: utils::bytes_to_eth_hex(&transaction.from.0),
            to: transaction.to.map(|to| utils::bytes_to_eth_hex(&to.0)),
            value: transaction.value.to_string(),
            nonce: transaction.nonce.to_string(),
            input: utils::bytes_to_eth_hex(&transaction.input),
            gas: transaction.gas.to_string(),
            gas_price: transaction.gas_price.map(|price| price.to_string()),
            max_fee_per_gas: transaction.max_fee_per_gas.map(|fee| fee.to_string()),
            max_priority_fee_per_gas: transaction
                .max_priority_fee_per_gas
                .map(|fee| fee.to_string()),
            r: transaction.r.to_string(),
            s: transaction.s.to_string(),
            v: transaction.v.as_u64(),
            access_list: transaction
                .access_list
                .as_ref()
                .map(|al| serde_json::to_string(&al.clone().to_owned()).unwrap()),
            contract_address: receipt
                .contract_address
                .map(|contract| utils::bytes_to_eth_hex(&contract.0)),
            cumulative_gas_used: receipt.cumulative_gas_used.to_string(),
            effective_gas_price: receipt.effective_gas_price.map(|price| price.to_string()),
            gas_used: receipt.gas_used.unwrap().to_string(),
            logs_bloom: utils::bytes_to_eth_hex(&receipt.logs_bloom.0),
            root: receipt.root.map(|root| utils::bytes_to_eth_hex(&root.0)), // Only present before activation of [EIP-658]
            status: receipt.status.map(|status| status.as_u64()), // Only present after activation of [EIP-658]
        }
    }
}

/** CREATE TABLE IF NOT EXISTS events (
    `address` FixedString(20),
    `blockHash` FixedString(32),
    `blockNumber` UInt64,
    `blockTimestamp` UInt256,
    `transactionHash` FixedString(32),
    `transactionIndex` UInt64,
    `logIndex` UInt256,
    `removed` Bool,
    `topic0` Nullable(FixedString(32)),
    `topic1` Nullable(FixedString(32)),
    `topic2` Nullable(FixedString(32)),
    `topic3` Nullable(FixedString(32)),
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
    pub block_timestamp: String,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: u64,
    #[serde(rename = "logIndex")]
    pub log_index: String,
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
            block_timestamp: block.timestamp.to_string(),
            transaction_hash: utils::bytes_to_eth_hex(&transaction.hash.0),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            log_index: log.log_index.unwrap().to_string(),
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
    address FixedString(20),
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
    pub block_timestamp: String,
    pub index: u64,
    #[serde(rename = "validatorIndex")]
    pub validator_index: u64,
    pub address: String,
    pub amount: String,
}

impl WithdrawalRow {
    pub fn from_ethers<T>(block: &Block<T>, withdraw: &Withdrawal) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            block_hash: utils::bytes_to_eth_hex(&block.hash.unwrap().0),
            block_number: block.number.unwrap().as_u64(),
            block_timestamp: block.timestamp.to_string(),
            index: withdraw.index.as_u64(),
            validator_index: withdraw.validator_index.as_u64(),
            address: utils::bytes_to_eth_hex(&withdraw.address.0),
            amount: withdraw.amount.to_string(),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS traces (
    `blockPos`    UInt64,
    `blockNumber` UInt64,
    `blockTimestamp` UInt256,
    `blockHash` FixedString(32),
    `transactionHash` Nullable(FixedString(32)),
    `traceAddress` Array(UInt64),
    `subtraces` UInt64,
    `transactionPosition` Nullable(UInt64),
    `error` Nullable(String),
    `actionType` LowCardinality(String),
    `actionCallFrom` Nullable(FixedString(20)),
    `actionCallTo` Nullable(FixedString(20)),
    `actionCallValue` Nullable(UInt256),
    `actionCallInput` Nullable(String),
    `actionCallGas` Nullable(UInt256),
    `actionCallType` LowCardinality(String),
    `actionCreateFrom` Nullable(FixedString(20)),
    `actionCreateValue` Nullable(UInt256),
    `actionCreateInit` Nullable(String),
    `actionCreateGas` Nullable(UInt256),
    `actionSuicideAddress` Nullable(FixedString(20)),
    `actionSuicideRefundAddress` Nullable(FixedString(20)),
    `actionSuicideBalance` Nullable(UInt256),
    `actionRewardAuthor` Nullable(FixedString(20)),
    `actionRewardValue` Nullable(UInt256),
    `actionRewardType` LowCardinality(String),
    `resultType` LowCardinality(String),
    `resultCallGasUsed` Nullable(UInt256),
    `resultCallOutput` Nullable(String),
    `resultCreateGasUsed` Nullable(UInt256),
    `resultCreateCode` Nullable(String),
    `resultCreateAddress` Nullable(FixedString(20))
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
    pub block_timestamp: String,
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
    pub action_call_value: Option<String>,
    /// Input data
    #[serde(rename = "actionCallInput")]
    pub action_call_input: Option<String>,
    #[serde(rename = "actionCallGas")]
    pub action_call_gas: Option<String>,
    /// The type of the call.
    #[serde(rename = "actionCallType")]
    pub action_call_type: String, // none call callcode delegatecall staticcall
    #[serde(rename = "actionCreateFrom")]
    pub action_create_from: Option<String>,
    #[serde(rename = "actionCreateValue")]
    pub action_create_value: Option<String>,
    #[serde(rename = "actionCreateInit")]
    pub action_create_init: Option<String>,
    #[serde(rename = "actionCreateGas")]
    pub action_create_gas: Option<String>,
    #[serde(rename = "actionSuicideAddress")]
    pub action_suicide_address: Option<String>,
    #[serde(rename = "actionSuicideRefundAddress")]
    pub action_suicide_refund_address: Option<String>,
    #[serde(rename = "actionSuicideBalance")]
    pub action_suicide_balance: Option<String>,
    #[serde(rename = "actionRewardAuthor")]
    pub action_reward_author: Option<String>,
    #[serde(rename = "actionRewardValue")]
    pub action_reward_value: Option<String>,
    #[serde(rename = "actionRewardType")]
    pub action_reward_type: String, // LowCardinality ('block', 'uncle', 'emptyStep', 'external')
    /// Result
    //  pub result: Option<Res>, // call {gasused, output} create {gas_used, code, address} none
    #[serde(rename = "resultType")]
    pub result_type: String, // LowCardinality ('none', 'call', 'create')
    #[serde(rename = "resultCallGasUsed")]
    pub result_call_gas_used: Option<String>,
    #[serde(rename = "resultCallOutput")]
    pub result_call_output: Option<String>,
    #[serde(rename = "resultCreateGasUsed")]
    pub result_create_gas_used: Option<String>,
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
            block_timestamp: block.timestamp.to_string(),
            block_hash: utils::bytes_to_eth_hex(&trace.block_hash.0),
            error: trace.error.clone(),
        };

        // fill action
        match &trace.action {
            Action::Call(call) => {
                trace_row.action_call_from = Some(utils::bytes_to_eth_hex(&call.from.0));
                trace_row.action_call_to = Some(utils::bytes_to_eth_hex(&call.to.0));
                trace_row.action_call_type = to_variant_name(&call.call_type).unwrap().to_string();
                trace_row.action_call_gas = Some(call.gas.to_string());
                trace_row.action_call_input = Some(utils::bytes_to_eth_hex(&call.input.0));
                trace_row.action_call_value = Some(call.value.to_string());
            }
            Action::Create(create) => {
                trace_row.action_create_from = Some(utils::bytes_to_eth_hex(&create.from.0));
                trace_row.action_create_init = Some(utils::bytes_to_eth_hex(&create.init.0));
                trace_row.action_create_value = Some(create.value.to_string());
                trace_row.action_create_gas = Some(create.gas.to_string());
            }
            Action::Suicide(suicide) => {
                trace_row.action_suicide_address =
                    Some(utils::bytes_to_eth_hex(&suicide.address.0));
                trace_row.action_suicide_balance = Some(suicide.balance.to_string());
                trace_row.action_suicide_refund_address =
                    Some(utils::bytes_to_eth_hex(&suicide.refund_address.0));
            }
            Action::Reward(reward) => {
                trace_row.action_reward_author = Some(utils::bytes_to_eth_hex(&reward.author.0));
                trace_row.action_reward_type =
                    to_variant_name(&reward.reward_type).unwrap().to_string();
                trace_row.action_reward_value = Some(reward.value.to_string());
            }
        }

        match &trace.result {
            Some(result) => match result {
                Res::Call(call) => {
                    trace_row.result_type = "call".to_owned();
                    trace_row.result_call_gas_used = Some(call.gas_used.to_string());
                    trace_row.result_call_output = Some(utils::bytes_to_eth_hex(&call.output.0));
                }
                Res::Create(create) => {
                    trace_row.result_type = "create".to_owned();
                    trace_row.result_create_address =
                        Some(utils::bytes_to_eth_hex(&create.address.0));
                    trace_row.result_create_code = Some(utils::bytes_to_eth_hex(&create.code.0));
                    trace_row.result_create_gas_used = Some(create.gas_used.to_string())
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
