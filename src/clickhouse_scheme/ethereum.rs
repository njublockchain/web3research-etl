use ethers::types::{Action, Block, Log, Res, Trace, Transaction, TransactionReceipt, Withdrawal};
use klickhouse::{u256, Bytes, Row};
use serde_variant::to_variant_name;

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
    pub hash: Bytes,
    pub number: u64,
    pub parent_hash: Bytes,
    pub uncles: Vec<Bytes>,
    pub sha3_uncles: Bytes,
    pub total_difficulty: u256,
    pub difficulty: u256,
    pub miner: Bytes,
    pub nonce: Bytes,
    pub mix_hash: Bytes,
    pub base_fee_per_gas: Option<u256>,
    pub gas_limit: u256,
    pub gas_used: u256,
    pub state_root: Bytes,
    pub transactions_root: Bytes,
    pub receipts_root: Bytes,
    pub logs_bloom: Bytes,
    pub withdrawls_root: Option<Bytes>,
    pub extra_data: Bytes,
    pub timestamp: u256,
    pub size: u256,
}

impl BlockRow {
    pub fn from_ethers<T>(block: &Block<T>) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            hash: block.hash.unwrap().0.to_vec().into(), //block.hash.unwrap()),
            number: block.number.unwrap().as_u64(),
            parent_hash: block.parent_hash.0.to_vec().into(),
            uncles: block
                .uncles
                .iter()
                .map(|uncle| uncle.0.to_vec().into())
                .collect(),
            sha3_uncles: block.uncles_hash.0.to_vec().into(),
            total_difficulty: u256(block.total_difficulty.unwrap().into()),
            difficulty: u256(block.difficulty.into()),
            miner: block.author.unwrap().0.to_vec().into(),
            nonce: block.nonce.unwrap().0.to_vec().into(),
            mix_hash: block.mix_hash.unwrap().0.to_vec().into(),
            base_fee_per_gas: block
                .base_fee_per_gas.map(|fee| u256(fee.into())),
            gas_limit: u256(block.gas_limit.into()),
            gas_used: u256(block.gas_used.into()),
            state_root: block.state_root.0.to_vec().into(),
            transactions_root: block.transactions_root.0.to_vec().into(),
            receipts_root: block.receipts_root.0.to_vec().into(),
            logs_bloom: block.logs_bloom.unwrap().0.to_vec().into(),
            withdrawls_root: block
                .withdrawals_root.map(|root| root.0.to_vec().into()),
            extra_data: block.extra_data.to_vec().into(),
            timestamp: u256(block.timestamp.into()),
            size: u256(block.size.unwrap().into()),
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransactionRow {
    pub hash: Bytes,
    pub block_hash: Bytes,
    pub block_number: u64,
    pub block_timestamp: u256,
    pub transaction_index: u64,
    pub chain_id: Option<u256>,
    pub r#type: Option<u64>,
    pub from: Bytes,
    pub to: Option<Bytes>,
    pub value: u256,
    pub nonce: u256,
    pub input: Bytes,
    pub gas: u256,
    pub gas_price: Option<u256>,
    pub max_fee_per_gas: Option<u256>,
    pub max_priority_fee_per_gas: Option<u256>,
    pub r: u256,
    pub s: u256,
    pub v: u64,
    pub access_list: Option<String>,
    pub contract_address: Option<Bytes>,
    pub cumulative_gas_used: u256,
    pub effective_gas_price: Option<u256>,
    pub gas_used: u256,
    pub logs_bloom: Bytes,
    pub root: Option<Bytes>,
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
            hash: transaction.hash.0.to_vec().into(),
            block_hash: transaction.block_hash.unwrap().0.to_vec().into(),
            block_number: transaction.block_number.unwrap().as_u64(),
            block_timestamp: u256(block.timestamp.into()),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            chain_id: transaction.chain_id.map(|id| u256(id.into())),
            r#type: transaction.transaction_type.map(|t| t.as_u64()),
            from: transaction.from.0.to_vec().into(),
            to: transaction.to.map(|to| to.0.to_vec().into()),
            value: u256(transaction.value.into()),
            nonce: u256(transaction.nonce.into()),
            input: transaction.input.to_vec().into(),
            gas: u256(transaction.gas.into()),
            gas_price: transaction
                .gas_price.map(|price| u256(price.into())),
            max_fee_per_gas: transaction
                .max_fee_per_gas.map(|fee| u256(fee.into())),
            max_priority_fee_per_gas: transaction
                .max_priority_fee_per_gas.map(|fee| u256(fee.into())),
            r: u256(transaction.r.into()),
            s: u256(transaction.s.into()),
            v: transaction.v.as_u64(),
            access_list: transaction
                .access_list
                .as_ref().map(|al| serde_json::to_string(&al.clone().to_owned()).unwrap()),
            contract_address: receipt
                .contract_address.map(|contract| contract.0.to_vec().into()),
            cumulative_gas_used: u256(receipt.cumulative_gas_used.into()),
            effective_gas_price: receipt
                .effective_gas_price.map(|price| u256(price.into())),
            gas_used: u256(receipt.gas_used.unwrap().into()),
            logs_bloom: receipt.logs_bloom.0.to_vec().into(),
            root: receipt.root.map(|root| root.0.to_vec().into()), // Only present before activation of [EIP-658]
            status: receipt.status.map(|status| status.as_u64()), // Only present after activation of [EIP-658]
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct EventRow {
    pub block_hash: Bytes,
    pub block_number: u64,
    pub block_timestamp: u256,
    pub transaction_hash: Bytes,
    pub transaction_index: u64,
    pub log_index: u256,
    pub removed: bool,
    pub topics: Vec<Bytes>,
    pub data: Bytes,
    pub address: Bytes,
}

impl EventRow {
    pub fn from_ethers<T>(block: &Block<T>, transaction: &Transaction, log: &Log) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            block_hash: log.block_hash.unwrap().0.to_vec().into(),
            block_number: log.block_number.unwrap().as_u64(),
            block_timestamp: u256(block.timestamp.into()),
            transaction_hash: transaction.hash.0.to_vec().into(),
            transaction_index: transaction.transaction_index.unwrap().as_u64(),
            log_index: u256(log.log_index.unwrap().into()),
            removed: log.removed.unwrap(),
            topics: log
                .topics
                .iter()
                .map(|topic| topic.0.to_vec().into())
                .collect(),
            data: log.data.to_vec().into(),
            address: log.address.0.to_vec().into(),
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WithdrawalRow {
    pub block_hash: Bytes,
    pub block_number: u64,
    pub block_timestamp: u256,
    pub index: u64,
    pub validator_index: u64,
    pub address: Bytes,
    pub amount: u256,
}

impl WithdrawalRow {
    pub fn from_ethers<T>(block: &Block<T>, withdraw: &Withdrawal) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            block_hash: block.hash.unwrap().0.to_vec().into(),
            block_number: block.number.unwrap().as_u64(),
            block_timestamp: u256(block.timestamp.into()),
            index: withdraw.index.as_u64(),
            validator_index: withdraw.validator_index.as_u64(),
            address: withdraw.address.0.to_vec().into(),
            amount: u256(withdraw.amount.into()),
        }
    }
}

#[derive(Row, Clone, Debug)]
#[klickhouse(rename_all = "camelCase")]
pub struct TraceRow {
    pub block_pos: u64,
    /// Block Number
    pub block_number: u64,
    pub block_timestamp: u256,
    /// Block Hash
    pub block_hash: Bytes,

    /// Trace address, The list of addresses where the call was executed, the address of the parents, and the order of the current sub call
    pub trace_address: Vec<u64>,
    /// Subtraces
    pub subtraces: u64,
    /// Transaction position
    pub transaction_position: Option<u64>,
    /// Transaction hash
    pub transaction_hash: Option<Bytes>,

    /// Error, See also [`TraceError`]
    pub error: Option<String>,

    /// Action
    ///
    // pub action: Action, // call create suicide reward
    pub action_type: String, // Enum('Call', 'Create', 'Suicide', 'Reward')
    /// Sender
    pub action_call_from: Option<Bytes>,
    /// Recipient
    pub action_call_to: Option<Bytes>,
    /// Transferred Value
    pub action_call_value: Option<u256>,
    /// Input data
    pub action_call_input: Option<Bytes>,
    pub action_call_gas: Option<u256>,
    /// The type of the call.
    pub action_call_type: String, // none call callcode delegatecall staticcall
    pub action_create_from: Option<Bytes>,
    pub action_create_value: Option<u256>,
    pub action_create_init: Option<Bytes>,
    pub action_create_gas: Option<u256>,
    pub action_suicide_address: Option<Bytes>,
    pub action_suicide_refund_address: Option<Bytes>,
    pub action_suicide_balance: Option<u256>,
    pub action_reward_author: Option<Bytes>,
    pub action_reward_value: Option<u256>,
    pub action_reward_type: String, // LowCardinality ('block', 'uncle', 'emptyStep', 'external')
    /// Result
    //  pub result: Option<Res>, // call {gasused, output} create {gas_used, code, address} none
    pub result_type: String, // LowCardinality ('none', 'call', 'create')
    pub result_call_gas_used: Option<u256>,
    pub result_call_output: Option<Bytes>,
    pub result_create_gas_used: Option<u256>,
    pub result_create_code: Option<Bytes>,
    pub result_create_address: Option<Bytes>,
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
                .transaction_hash.map(|h| h.0.to_vec().into()),
            block_number: trace.block_number,
            block_timestamp: u256(block.timestamp.into()),
            block_hash: trace.block_hash.0.to_vec().into(),
            error: trace.error.clone(),
        };

        // fill action
        match &trace.action {
            Action::Call(call) => {
                trace_row.action_call_from = Some(call.from.0.to_vec().into());
                trace_row.action_call_to = Some(call.to.0.to_vec().into());
                trace_row.action_call_type = to_variant_name(&call.call_type).unwrap().to_string();
                trace_row.action_call_gas = Some(u256(call.gas.into()));
                trace_row.action_call_input = Some(call.input.0.to_vec().into());
            }
            Action::Create(create) => {
                trace_row.action_create_from = Some(create.from.0.to_vec().into());
                trace_row.action_create_init = Some(create.init.0.to_vec().into());
                trace_row.action_create_value = Some(u256(create.value.into()));
                trace_row.action_create_gas = Some(u256(create.gas.into()));
            }
            Action::Suicide(suicide) => {
                trace_row.action_suicide_address = Some(suicide.address.0.to_vec().into());
                trace_row.action_suicide_balance = Some(u256(suicide.balance.into()));
                trace_row.action_suicide_refund_address =
                    Some(suicide.refund_address.0.to_vec().into());
            }
            Action::Reward(reward) => {
                trace_row.action_reward_author = Some(reward.author.0.to_vec().into());
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
                    trace_row.result_call_output = Some(call.output.0.to_vec().into());
                }
                Res::Create(create) => {
                    trace_row.result_type = "create".to_owned();
                    trace_row.result_create_address = Some(create.address.0.to_vec().into());
                    trace_row.result_create_code = Some(create.code.0.to_vec().into());
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
