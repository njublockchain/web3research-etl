use documented::Documented;
use ethers::{
    types::{
        transaction::eip2930::AccessListItem, Action, Block, Log, Res, Trace, Transaction,
        TransactionReceipt, Withdrawal,
    },
    utils::hex::ToHexExt,
};
use klickhouse::{u256, Row};
use serde_variant::to_variant_name;

/** CREATE TABLE IF NOT EXISTS blocks (
    hash             FixedString(66),
    number           UInt64,
    parentHash       FixedString(66),
    uncles           Array(FixedString(66)),
    totalDifficulty  UInt256,
    miner            String,
    difficulty       UInt256,
    nonce            String,
    baseFeePerGas    Nullable(UInt256),
    gasLimit         UInt256,
    gasUsed          UInt256,
    extraData        String,
    timestamp        UInt64,
    size             UInt64
) ENGINE=ReplacingMergeTree
ORDER BY (hash, number);
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
    pub hash: String,
    pub number: u64,
    pub parent_hash: String,
    pub uncles: Vec<String>,
    pub total_difficulty: u256,
    pub difficulty: u256,
    pub miner: String,
    pub nonce: String,
    pub base_fee_per_gas: Option<u256>,
    pub gas_limit: u256,
    pub gas_used: u256,
    pub extra_data: String,
    pub timestamp: u64,
    pub size: u64,
}

impl BlockRow {
    pub fn from_ethers<T>(block: &Block<T>) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            hash: block.hash.unwrap().encode_hex_with_prefix(),
            number: block.number.unwrap().as_u64(),
            parent_hash: block.parent_hash.encode_hex_with_prefix(),
            uncles: block
                .uncles
                .iter()
                .map(|uncle| uncle.encode_hex_with_prefix())
                .collect(),
            total_difficulty: u256(block.total_difficulty.unwrap_or_default().into()),
            difficulty: u256(block.difficulty.into()),
            miner: block.author.unwrap().encode_hex_with_prefix(),
            nonce: block.nonce.unwrap().encode_hex_with_prefix(),
            base_fee_per_gas: block.base_fee_per_gas.map(|fee| u256(fee.into())),
            gas_limit: u256(block.gas_limit.into()),
            gas_used: u256(block.gas_used.into()),
            extra_data: block.extra_data.encode_hex_with_prefix(),
            timestamp: block.timestamp.as_u64(),
            size: block.size.unwrap().as_u64(),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS transactions (
    hash             FixedString(66),
    blockNumber      UInt64,
    blockTimestamp   UInt64,
    transactionIndex UInt64,
    chainId          Nullable(UInt64),
    type             Nullable(UInt64),
    from             String,
    to               Nullable(String),
    value            UInt256,
    nonce            UInt64,
    input            String CODEC(ZSTD(6)),
    gas              UInt256,
    gasPrice         Nullable(UInt256),
    maxFeePerGas     Nullable(UInt256),
    maxPriorityFeePerGas Nullable(UInt256),
    -- r                UInt256,
    -- s                UInt256,
    -- v                UInt64,
    contractAddress  Nullable(String),
    cumulativeGasUsed UInt256,
    effectiveGasPrice Nullable(UInt256),
    gasUsed          UInt256,
    status           Nullable(UInt64)
) ENGINE=ReplacingMergeTree
ORDER BY (blockNumber, blockTimestamp, from, nonce, to, transactionIndex, hash)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransactionRow {
    pub hash: String,
    // pub block_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_index: u64,
    pub chain_id: Option<u64>,
    pub r#type: Option<u64>,
    pub from: String,
    pub to: Option<String>,
    pub value: u256,
    pub nonce: u64,
    pub input: String,
    pub gas: u256,
    pub gas_price: Option<u256>,
    pub max_fee_per_gas: Option<u256>,
    pub max_priority_fee_per_gas: Option<u256>,
    // pub r: u256,
    // pub s: u256,
    // pub v: u64,
    pub contract_address: Option<String>,
    pub cumulative_gas_used: u256,
    pub effective_gas_price: Option<u256>,
    pub gas_used: u256,
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
            hash: transaction.hash.encode_hex_with_prefix(),
            // block_hash: transaction.block_hash.unwrap().encode_hex_with_prefix(),
            block_number: transaction.block_number.unwrap().as_u64(),
            block_timestamp: block.timestamp.as_u64(),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            chain_id: transaction.chain_id.map(|id| id.as_u64()),
            r#type: transaction.transaction_type.map(|t| t.as_u64()),
            from: transaction.from.encode_hex_with_prefix(),
            to: transaction.to.map(|to| to.encode_hex_with_prefix()),
            value: u256(transaction.value.into()),
            nonce: transaction.nonce.as_u64(),
            input: transaction.input.encode_hex_with_prefix(),
            gas: u256(transaction.gas.into()),
            gas_price: transaction.gas_price.map(|price| u256(price.into())),
            max_fee_per_gas: transaction.max_fee_per_gas.map(|fee| u256(fee.into())),
            max_priority_fee_per_gas: transaction
                .max_priority_fee_per_gas
                .map(|fee| u256(fee.into())),
            // r: u256(transaction.r.into()),
            // s: u256(transaction.s.into()),
            // v: transaction.v.as_u64(),
            contract_address: receipt
                .contract_address
                .map(|contract| contract.encode_hex_with_prefix()),
            cumulative_gas_used: u256(receipt.cumulative_gas_used.into()),
            effective_gas_price: receipt.effective_gas_price.map(|price| u256(price.into())),
            gas_used: u256(receipt.gas_used.unwrap().into()),
            status: receipt.status.map(|status| status.as_u64()), // Only present after activation of [EIP-658]
        }
    }
}

/**
CREATE TABLE IF NOT EXISTS accessListItems (
    blockNumber      UInt64,
    blockTimestamp   UInt64,
    transactionIndex UInt64,
    transactionHash  FixedString(66),
    itemIndex        UInt64,
    address          String,
    storageKey       Array(FixedString(66))
)
ENGINE = ReplacingMergeTree
ORDER BY (blockNumber, transactionIndex, transactionHash, itemIndex)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
 */
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccessListItemRow {
    // pub block_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_index: u64,
    pub transaction_hash: String,
    pub item_index: u64,
    pub address: String,
    pub storage_key: Vec<String>,
}

impl AccessListItemRow {
    pub fn from_ethers<T>(
        block: &Block<T>,
        transaction: &Transaction,
        index: u64,
        access_list_item: &AccessListItem,
    ) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            // block_hash: transaction.block_hash.unwrap().encode_hex_with_prefix(),
            block_number: transaction.block_number.unwrap().as_u64(),
            block_timestamp: block.timestamp.as_u64(),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            transaction_hash: transaction.hash.encode_hex_with_prefix(),
            item_index: index,
            address: access_list_item.address.encode_hex_with_prefix(),
            storage_key: access_list_item
                .storage_keys
                .iter()
                .map(|key| key.encode_hex_with_prefix())
                .collect(),
        }
    }
}
/** CREATE TABLE IF NOT EXISTS events (
    `blockNumber` UInt64,
    `blockTimestamp` UInt64,
    `transactionHash` FixedString(66),
    `transactionIndex` UInt64,
    `logIndex` UInt64,
    `removed` Bool,
    `address` String,
    `topic0` Nullable(FixedString(66)),
    `topic1` Nullable(FixedString(66)),
    `topic2` Nullable(FixedString(66)),
    `topic3` Nullable(FixedString(66)),
    `data` String CODEC(ZSTD(6))
)
ENGINE = ReplacingMergeTree
ORDER BY (removed, address, topic0, topic1, topic2, topic3, transactionHash, logIndex)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct EventRow {
    // pub block_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
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
            .map(|topic| topic.encode_hex_with_prefix())
            .collect();

        Self {
            // block_hash: log.block_hash.unwrap().encode_hex_with_prefix(),
            block_number: log.block_number.unwrap().as_u64(),
            block_timestamp: block.timestamp.as_u64(),
            transaction_hash: transaction.hash.encode_hex_with_prefix(),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            log_index: log.log_index.unwrap().as_u64(),
            removed: log.removed.unwrap(),
            topic0: topics.get(0).cloned(),
            topic1: topics.get(1).cloned(),
            topic2: topics.get(2).cloned(),
            topic3: topics.get(3).cloned(),
            data: log.data.encode_hex_with_prefix(),
            address: log.address.encode_hex_with_prefix(),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS withdraws (
    `blockNumber`    UInt64,
    `blockTimestamp` UInt64,
    `index`          UInt64,
    `validatorIndex` UInt64,
    `address`        String,
    `amount`         UInt256
) ENGINE=ReplacingMergeTree
ORDER BY (blockNumber, index)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Row, Clone, Debug, Default, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct WithdrawalRow {
    // pub block_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub index: u64,
    pub validator_index: u64,
    pub address: String,
    pub amount: u256,
}

impl WithdrawalRow {
    pub fn from_ethers<T>(block: &Block<T>, withdraw: &Withdrawal) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            // block_hash: block.hash.unwrap().encode_hex_with_prefix(),
            block_number: block.number.unwrap().as_u64(),
            block_timestamp: block.timestamp.as_u64(),
            index: withdraw.index.as_u64(),
            validator_index: withdraw.validator_index.as_u64(),
            address: withdraw.address.encode_hex_with_prefix(),
            amount: u256(withdraw.amount.into()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS traces (
    `blockPosition`    UInt64,
    `blockNumber` UInt64,
    `blockTimestamp` UInt64,
    `transactionHash` Nullable(FixedString(66)),
    `traceAddress` Array(UInt64),
    `subtraces` UInt64,
    `transactionPosition` Nullable(UInt64),
    `error` Nullable(String) CODEC(ZSTD(6)),
    `actionType` LowCardinality(String),
    `actionCallFrom` Nullable(String),
    `actionCallTo` Nullable(String),
    `actionCallValue` Nullable(UInt256),
    `actionCallInput` Nullable(String) CODEC(ZSTD(6)),
    `actionCallGas` Nullable(UInt256),
    `actionCallType` LowCardinality(String),
    `actionCreateFrom` Nullable(String),
    `actionCreateValue` Nullable(UInt256),
    `actionCreateInit` Nullable(String) CODEC(ZSTD(6)),
    `actionCreateGas` Nullable(UInt256),
    `actionSuicideAddress` Nullable(String),
    `actionSuicideRefundAddress` Nullable(String),
    `actionSuicideBalance` Nullable(UInt256),
    `actionRewardAuthor` Nullable(String),
    `actionRewardValue` Nullable(UInt256),
    `actionRewardType` LowCardinality(String),
    `resultType` LowCardinality(String),
    `resultCallGasUsed` Nullable(UInt256),
    `resultCallOutput` Nullable(String) CODEC(ZSTD(6)),
    `resultCreateGasUsed` Nullable(UInt256),
    `resultCreateCode` Nullable(String) CODEC(ZSTD(6)),
    `resultCreateAddress` Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (blockNumber, transactionHash, blockPosition, transactionPosition)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Row, Clone, Debug, Documented)]
#[klickhouse(rename_all = "camelCase")]
pub struct TraceRow {
    pub block_position: u64,
    /// Block Number
    pub block_number: u64,
    pub block_timestamp: u64,
    /// Block Hash
    // pub block_hash: String,

    /// Trace address, The list of addresses where the call was executed, the address of the parents, and the order of the current sub call
    pub trace_address: Vec<u64>,
    /// Subtraces
    pub subtraces: u64,
    /// Transaction position
    pub transaction_position: Option<u64>,
    /// Transaction hash
    pub transaction_hash: Option<String>,

    /// Error, See also [`TraceError`]
    pub error: Option<String>,

    /// Action
    ///
    // pub action: Action, // call create suicide reward
    pub action_type: String, // Enum('Call', 'Create', 'Suicide', 'Reward')
    /// Sender
    pub action_call_from: Option<String>,
    /// Recipient
    pub action_call_to: Option<String>,
    /// Transferred Value
    pub action_call_value: Option<u256>,
    /// Input data
    pub action_call_input: Option<String>,
    pub action_call_gas: Option<u256>,
    /// The type of the call.
    pub action_call_type: String, // none call callcode delegatecall staticcall
    pub action_create_from: Option<String>,
    pub action_create_value: Option<u256>,
    pub action_create_init: Option<String>,
    pub action_create_gas: Option<u256>,
    pub action_suicide_address: Option<String>,
    pub action_suicide_refund_address: Option<String>,
    pub action_suicide_balance: Option<u256>,
    pub action_reward_author: Option<String>,
    pub action_reward_value: Option<u256>,
    pub action_reward_type: String, // LowCardinality ('block', 'uncle', 'emptyStep', 'external')
    /// Result
    //  pub result: Option<Res>, // call {gasused, output} create {gas_used, code, address} none
    pub result_type: String, // LowCardinality ('none', 'call', 'create')
    pub result_call_gas_used: Option<u256>,
    pub result_call_output: Option<String>,
    pub result_create_gas_used: Option<u256>,
    pub result_create_code: Option<String>,
    pub result_create_address: Option<String>,
}

impl TraceRow {
    pub fn from_ethers<T>(block: &Block<T>, trace: &Trace, index: usize) -> Self
    where
        T: serde::ser::Serialize,
    {
        let mut trace_row = Self {
            block_position: index as u64,
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
            transaction_hash: trace.transaction_hash.map(|h| h.encode_hex_with_prefix()),
            block_number: trace.block_number,
            block_timestamp: block.timestamp.as_u64(),
            // block_hash: trace.block_hash.encode_hex_with_prefix(),
            error: trace.error.clone(),
        };

        // fill action
        match &trace.action {
            Action::Call(call) => {
                trace_row.action_call_from = Some(call.from.encode_hex_with_prefix());
                trace_row.action_call_to = Some(call.to.encode_hex_with_prefix());
                trace_row.action_call_type = to_variant_name(&call.call_type).unwrap().to_string();
                trace_row.action_call_gas = Some(u256(call.gas.into()));
                trace_row.action_call_input = Some(call.input.encode_hex_with_prefix());
            }
            Action::Create(create) => {
                trace_row.action_create_from = Some(create.from.encode_hex_with_prefix());
                trace_row.action_create_init = Some(create.init.encode_hex_with_prefix());
                trace_row.action_create_value = Some(u256(create.value.into()));
                trace_row.action_create_gas = Some(u256(create.gas.into()));
            }
            Action::Suicide(suicide) => {
                trace_row.action_suicide_address = Some(suicide.address.encode_hex_with_prefix());
                trace_row.action_suicide_balance = Some(u256(suicide.balance.into()));
                trace_row.action_suicide_refund_address =
                    Some(suicide.refund_address.encode_hex_with_prefix());
            }
            Action::Reward(reward) => {
                trace_row.action_reward_author = Some(reward.author.encode_hex_with_prefix());
                trace_row.action_reward_type =
                    to_variant_name(&reward.reward_type).unwrap().to_string();
                trace_row.action_reward_value = Some(u256(reward.value.into()));
            }
        }

        match &trace.result {
            Some(result) => match result {
                Res::Call(call) => {
                    trace_row.result_type = "call".to_owned();
                    trace_row.result_call_gas_used = Some(u256(call.gas_used.into()));
                    trace_row.result_call_output = Some(call.output.encode_hex_with_prefix());
                }
                Res::Create(create) => {
                    trace_row.result_type = "create".to_owned();
                    trace_row.result_create_address = Some(create.address.encode_hex_with_prefix());
                    trace_row.result_create_code = Some(create.code.encode_hex_with_prefix());
                    trace_row.result_create_gas_used = Some(u256(create.gas_used.into()))
                }
                Res::None => {
                    // trace_row.resultType =
                }
            },
            None => {} //trace_row.resultType = "none".to_owned(),
        }

        trace_row
    }
}
