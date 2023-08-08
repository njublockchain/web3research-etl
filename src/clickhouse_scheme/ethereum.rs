use ethers::types::{Action, Block, Log, Res, Trace, Transaction, TransactionReceipt, Withdrawal};
use klickhouse::{u256, Bytes, Row};

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

impl BlockRow {
    pub fn from_ethers<T>(block: &Block<T>) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            hash: block.hash.unwrap().0.to_vec().into(), //block.hash.unwrap()),
            number: block.number.unwrap().as_u64(),
            parentHash: block.parent_hash.0.to_vec().into(),
            uncles: block
                .uncles
                .iter()
                .map(|uncle| uncle.0.to_vec().into())
                .collect(),
            sha3Uncles: block.uncles_hash.0.to_vec().into(),
            totalDifficulty: u256(block.total_difficulty.unwrap().into()),
            difficulty: u256(block.difficulty.into()),
            miner: block.author.unwrap().0.to_vec().into(),
            nonce: block.nonce.unwrap().0.to_vec().into(),
            mixHash: block.mix_hash.unwrap().0.to_vec().into(),
            baseFeePerGas: block
                .base_fee_per_gas
                .and_then(|fee| Some(u256(fee.into()))),
            gasLimit: u256(block.gas_limit.into()),
            gasUsed: u256(block.gas_used.into()),
            stateRoot: block.state_root.0.to_vec().into(),
            transactionsRoot: block.transactions_root.0.to_vec().into(),
            receiptsRoot: block.receipts_root.0.to_vec().into(),
            logsBloom: block.logs_bloom.unwrap().0.to_vec().into(),
            withdrawlsRoot: block
                .withdrawals_root
                .and_then(|root| Some(root.0.to_vec().into())),
            extraData: block.extra_data.to_vec().into(),
            timestamp: u256(block.timestamp.into()),
            size: u256(block.size.unwrap().into()),
        }
    }
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
            blockHash: transaction.block_hash.unwrap().0.to_vec().into(),
            blockNumber: transaction.block_number.unwrap().as_u64(),
            blockTimestamp: u256(block.timestamp.into()),
            transactionIndex: transaction.transaction_index.unwrap().as_u64(),
            chainId: transaction.chain_id.and_then(|id| Some(u256(id.into()))),
            r#type: transaction.transaction_type.and_then(|t| Some(t.as_u64())),
            from: transaction.from.0.to_vec().into(),
            to: transaction.to.and_then(|to| Some(to.0.to_vec().into())),
            value: u256(transaction.value.into()),
            nonce: u256(transaction.nonce.into()),
            input: transaction.input.to_vec().into(),
            gas: u256(transaction.gas.into()),
            gasPrice: transaction
                .gas_price
                .and_then(|price| Some(u256(price.into()))),
            maxFeePerGas: transaction
                .max_fee_per_gas
                .and_then(|fee| Some(u256(fee.into()))),
            maxPriorityFeePerGas: transaction
                .max_priority_fee_per_gas
                .and_then(|fee| Some(u256(fee.into()))),
            r: u256(transaction.r.into()),
            s: u256(transaction.s.into()),
            v: transaction.v.as_u64(),
            accessList: transaction
                .access_list
                .as_ref()
                .and_then(|al| Some(serde_json::to_string(&al.clone().to_owned()).unwrap())),
            contractAddress: receipt
                .contract_address
                .and_then(|contract| Some(contract.0.to_vec().into())),
            cumulativeGasUsed: u256(receipt.cumulative_gas_used.into()),
            effectiveGasPrice: receipt
                .effective_gas_price
                .and_then(|price| Some(u256(price.into()))),
            gasUsed: u256(receipt.gas_used.unwrap().into()),
            logsBloom: receipt.logs_bloom.0.to_vec().into(),
            root: receipt.root.and_then(|root| Some(root.0.to_vec().into())), // Only present before activation of [EIP-658]
            status: receipt.status.and_then(|status| Some(status.as_u64())), // Only present after activation of [EIP-658]
        }
    }
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

impl EventRow {
    pub fn from_ethers<T>(block: &Block<T>, transaction: &Transaction, log: &Log) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            blockHash: log.block_hash.unwrap().0.to_vec().into(),
            blockNumber: log.block_number.unwrap().as_u64(),
            blockTimestamp: u256(block.timestamp.into()),
            transactionHash: transaction.hash.0.to_vec().into(),
            transactionIndex: transaction.transaction_index.unwrap().as_u64(),
            logIndex: u256(log.log_index.unwrap().into()),
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
pub struct WithdrawalRow {
    pub blockHash: Bytes,
    pub blockNumber: u64,
    pub blockTimestamp: u256,
    pub index: u64,
    pub validatorIndex: u64,
    pub address: Bytes,
    pub amount: u256,
}

impl WithdrawalRow {
    pub fn from_ethers<T>(block: &Block<T>, withdraw: &Withdrawal) -> Self
    where
        T: serde::ser::Serialize,
    {
        Self {
            blockHash: block.hash.unwrap().0.to_vec().into(),
            blockNumber: block.number.unwrap().as_u64(),
            blockTimestamp: u256(block.timestamp.into()),
            index: withdraw.index.as_u64(),
            validatorIndex: withdraw.validator_index.as_u64(),
            address: withdraw.address.0.to_vec().into(),
            amount: u256(withdraw.amount.into()),
        }
    }
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

impl TraceRow {
    pub fn from_ethers<T>(block: &Block<T>, trace: &Trace, index: usize) -> Self
    where
        T: serde::ser::Serialize,
    {
        let mut trace_row = Self {
            blockPos: index as u64,
            actionType: serde_json::to_string(&trace.action_type).unwrap(),
            actionCallFrom: None,
            actionCallTo: None,
            actionCallValue: None,
            actionCallInput: None,
            actionCallGas: None,
            actionCallType: "".to_owned(),
            actionCreateFrom: None,
            actionCreateValue: None,
            actionCreateInit: None,
            actionCreateGas: None,
            actionSuicideAddress: None,
            actionSuicideRefundAddress: None,
            actionSuicideBalance: None,
            actionRewardAuthor: None,
            actionRewardValue: None,
            actionRewardType: "".to_owned(),
            resultType: "".to_owned(),
            resultCallGasUsed: None,
            resultCallOutput: None,
            resultCreateGasUsed: None,
            resultCreateCode: None,
            resultCreateAddress: None,
            traceAddress: trace.trace_address.iter().map(|t| *t as u64).collect(),
            subtraces: trace.subtraces as u64,
            transactionPosition: trace.transaction_position.and_then(|pos| Some(pos as u64)),
            transactionHash: trace
                .transaction_hash
                .and_then(|h| Some(h.0.to_vec().into())),
            blockNumber: trace.block_number,
            blockTimestamp: u256(block.timestamp.into()),
            blockHash: trace.block_hash.0.to_vec().into(),
            error: trace.error.clone(),
        };

        // fill action
        match &trace.action {
            Action::Call(call) => {
                trace_row.actionCallFrom = Some(call.from.0.to_vec().into());
                trace_row.actionCallTo = Some(call.to.0.to_vec().into());
                trace_row.actionCallType = serde_json::to_string(&call.call_type).unwrap();
                trace_row.actionCallGas = Some(u256(call.gas.into()));
                trace_row.actionCallInput = Some(call.input.0.to_vec().into());
            }
            Action::Create(create) => {
                trace_row.actionCreateFrom = Some(create.from.0.to_vec().into());
                trace_row.actionCreateInit = Some(create.init.0.to_vec().into());
                trace_row.actionCreateValue = Some(u256(create.value.into()));
                trace_row.actionCreateGas = Some(u256(create.gas.into()));
            }
            Action::Suicide(suicide) => {
                trace_row.actionSuicideAddress = Some(suicide.address.0.to_vec().into());
                trace_row.actionSuicideBalance = Some(u256(suicide.balance.into()));
                trace_row.actionSuicideRefundAddress =
                    Some(suicide.refund_address.0.to_vec().into());
            }
            Action::Reward(reward) => {
                trace_row.actionRewardAuthor = Some(reward.author.0.to_vec().into());
                trace_row.actionRewardType = serde_json::to_string(&reward.reward_type).unwrap();
                trace_row.actionRewardValue = Some(u256(reward.value.into()));
            }
        }

        match &trace.result {
            Some(result) => match result {
                Res::Call(call) => {
                    trace_row.resultType = "call".to_owned();
                    trace_row.resultCallGasUsed = Some(u256(call.gas_used.into()));
                    trace_row.resultCallOutput = Some(call.output.0.to_vec().into());
                }
                Res::Create(create) => {
                    trace_row.resultType = "create".to_owned();
                    trace_row.resultCreateAddress = Some(create.address.0.to_vec().into());
                    trace_row.resultCreateCode = Some(create.code.0.to_vec().into());
                    trace_row.resultCreateGasUsed = Some(u256(create.gas_used.into()))
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
