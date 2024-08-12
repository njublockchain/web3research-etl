use std::collections::HashMap;

use documented::Documented;
use itertools::MultiUnzip;
use klickhouse::{Bytes, Row};
use log::warn;
use tron_grpc::{
    transaction_info::Log, AccountCreateContract, AccountPermissionUpdateContract,
    AccountUpdateContract, AssetIssueContract, BlockExtention, CancelAllUnfreezeV2Contract,
    ClearAbiContract, CreateSmartContract, DelegateResourceContract, ExchangeCreateContract,
    ExchangeInjectContract, ExchangeTransactionContract, ExchangeWithdrawContract,
    FreezeBalanceContract, FreezeBalanceV2Contract, InternalTransaction, MarketCancelOrderContract,
    MarketSellAssetContract, ParticipateAssetIssueContract, ProposalApproveContract,
    ProposalCreateContract, ProposalDeleteContract, SetAccountIdContract, ShieldedTransferContract,
    TransactionExtention, TransactionInfo, TransferAssetContract, TransferContract,
    TriggerSmartContract, UnDelegateResourceContract, UnfreezeAssetContract,
    UnfreezeBalanceContract, UnfreezeBalanceV2Contract, UpdateAssetContract,
    UpdateBrokerageContract, UpdateEnergyLimitContract, UpdateSettingContract, VoteAssetContract,
    VoteWitnessContract, WithdrawBalanceContract, WithdrawExpireUnfreezeContract,
    WitnessCreateContract, WitnessUpdateContract,
};

pub fn len_20_addr_from_any_vec(any_addr: Vec<u8>) -> Bytes {
    // consider a EVM address
    if any_addr.len() == 32 {
        assert!(
            any_addr.starts_with(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            "32-length EVM address should start with 12*00: {:X?}",
            any_addr
        ); // 12 bytes prefix + 20 bytes addr
        return Bytes(any_addr[12..].to_vec());
    }

    // consider a TRON address
    if any_addr.len() == 21 {
        assert!(
            any_addr.starts_with(&[0x41]),
            "21-length TRON address should start with 41: {:X?}",
            any_addr
        );
        return Bytes(any_addr[1..].to_vec());
    }

    // consider a ETH address
    if any_addr.len() == 20 {
        return Bytes(any_addr);
    }

    // remove the T prefix 
    if any_addr.starts_with(&[0x41]) {
        let cut_addr = any_addr[1..].to_vec();
        assert!(cut_addr.len() == 20);
        return Bytes(cut_addr);
    }

    // if null
    if any_addr.is_empty() {
        return Bytes::default();
    }

    // fallback
    warn!(
        "Address length {} is not considered yet! {:X?}",
        any_addr.len(),
        any_addr
    );
    return Bytes(any_addr);
}

/** CREATE TABLE IF NOT EXISTS blocks
(
    `hash` FixedString(32),
    `timestamp` Int64,
    `txTrieRoot` FixedString(32),
    `parentHash` FixedString(32),
    `number` Int64,
    `witnessId` Int64,
    `witnessAddress` String,
    `version` Int32,
    `accountStateRoot` FixedString(32),
    `witnessSignature` String,
    `transactionCount` Int32
) ENGINE = ReplacingMergeTree
ORDER BY (number, timestamp, hash)
SETTINGS index_granularity = 8192; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
    pub hash: Bytes,
    pub timestamp: i64,
    pub tx_trie_root: Bytes,
    pub parent_hash: Bytes,
    pub number: i64,
    pub witness_id: i64,
    pub witness_address: Bytes,
    pub version: i32,
    pub account_state_root: Bytes,
    pub witness_signature: Bytes,
}

impl BlockRow {
    pub fn from_grpc(block: &BlockExtention) -> Self {
        let header = block.block_header.clone().unwrap();
        let header_raw_data = header.raw_data.unwrap();

        Self {
            hash: Bytes(block.blockid.clone()),
            timestamp: header_raw_data.timestamp,
            tx_trie_root: Bytes(header_raw_data.tx_trie_root),
            parent_hash: Bytes(header_raw_data.parent_hash),
            number: header_raw_data.number,
            witness_id: header_raw_data.witness_id,
            witness_address: Bytes(header_raw_data.witness_address),
            version: header_raw_data.version,
            account_state_root: klickhouse::Bytes(header_raw_data.account_state_root),
            witness_signature: klickhouse::Bytes(header.witness_signature),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS transactions
(
    `hash` FixedString(32),
    `blockNum` Int64,
    `index` Int64,
    `expiration` Int64,
    `authorityAccountNames` Array(LowCardinality(String)),
    `authorityAccountAddresses` Array(String),
    `authorityPermissionNames` Array(LowCardinality(String)),
    `data` String,
    `contractType` LowCardinality(String),
    `contractParameter` Nullable(String),
    `contractProvider` Nullable(String),
    `contractName` Nullable(String),
    `contractPermissionId` Nullable(Int32),
    `scripts` String,
    `timestamp` Int64,
    `feeLimit` Int64,
    `signature` Array(String),
    `constantResult` String,
    `fee` Int64,
    `blockTimeStamp` Int64,
    `contractResult` Nullable(String),
    `contractAddress` Nullable(String),
    `energyUsage` Int64,
    `energyFee` Int64,
    `originEnergyUsage` Int64,
    `energyUsageTotal` Int64,
    `netUsage` Int64,
    `netFee` Int64,
    `receiptResult` LowCardinality(String),
    `result` LowCardinality(String),
    `resMessage` String,
    `assetIssueId` String,
    `withdrawAmount` Int64,
    `unfreezeAmount` Int64,
    `exchangeReceivedAmount` Int64,
    `exchangeInjectAnotherAmount` Int64,
    `exchangeWithdrawAnotherAmount` Int64,
    `exchangeId` Int64,
    `shieldedTransactionFee` Int64,
    `orderId` FixedString(32),
    `orderDetails` Nested(
        `makerOrderId` FixedString(32),
        `takerOrderId` FixedString(32),
        `fillSellQuantity` Int64,
        `fillBuyQuantity` Int64
    ),
    `packingFee` Int64,
    `withdrawExpireAmount` Int64,
    cancelUnfreezeV2Amount Map(String, Int64)
)
ENGINE = ReplacingMergeTree
ORDER BY (blockNum, contractType, contractAddress, exchangeId, orderId, result, hash)
SETTINGS index_granularity = 8192, allow_nullable_key=1; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransactionRow {
    pub hash: Bytes,
    pub block_num: i64,
    pub index: i64,

    pub expiration: i64,
    pub authority_account_names: Vec<Bytes>,
    pub authority_account_addresses: Vec<Bytes>,
    pub authority_permission_names: Vec<Bytes>,
    pub data: Bytes,

    pub contract_type: String,
    pub contract_parameter: Option<Bytes>,
    pub contract_provider: Option<Bytes>,
    pub contract_name: Option<Bytes>,
    pub contract_permission_id: Option<i32>,

    pub scripts: Bytes,
    pub timestamp: i64,
    pub fee_limit: i64,

    pub signature: Vec<Bytes>,
    pub constant_result: Bytes,

    pub fee: i64,
    pub block_time_stamp: i64,
    pub contract_result: Option<Bytes>,
    pub contract_address: Option<Bytes>,

    pub energy_usage: i64,
    pub energy_fee: i64,
    pub origin_energy_usage: i64,
    pub energy_usage_total: i64,
    pub net_usage: i64,
    pub net_fee: i64,
    pub receipt_result: String,

    pub result: String,
    pub res_message: Bytes,

    pub asset_issue_id: String,
    pub withdraw_amount: i64,
    pub unfreeze_amount: i64,
    pub exchange_received_amount: i64,
    pub exchange_inject_another_amount: i64,
    pub exchange_withdraw_another_amount: i64,
    pub exchange_id: i64,
    pub shielded_transaction_fee: i64,

    pub order_id: Bytes,
    // pub order_details: Vec<MarketOrderDetailField>,
    #[klickhouse(rename = "orderDetails.makerOrderId")]
    pub order_detail_maker_order_id: Vec<Bytes>,
    #[klickhouse(rename = "orderDetails.takerOrderId")]
    pub order_detail_taker_order_id: Vec<Bytes>,
    #[klickhouse(rename = "orderDetails.fillSellQuantity")]
    pub order_detail_fill_sell_quantity: Vec<i64>,
    #[klickhouse(rename = "orderDetails.fillBuyQuantity")]
    pub order_detail_fill_buy_quantity: Vec<i64>,
    pub packing_fee: i64,
    pub withdraw_expire_amount: i64,
    pub cancel_unfreeze_v2_amount: HashMap<String, i64>,
}

impl TransactionRow {
    pub fn from_grpc(
        block: &BlockExtention,
        index: i64,
        transaction: &TransactionExtention,
        transaction_info: Option<&TransactionInfo>,
    ) -> Self {
        let header = block.block_header.clone().unwrap();
        let header_raw_data = header.raw_data.unwrap();

        let tx = transaction.transaction.clone().unwrap();
        let tx_raw_data = tx.raw_data.unwrap();

        let contract = if tx_raw_data.contract.is_empty() {
            None
        } else {
            assert!(tx_raw_data.contract.len() == 1);
            Some(&tx_raw_data.contract[0])
        };

        let receipt =
            transaction_info.map(|transaction_info| transaction_info.receipt.clone().unwrap());

        let (
            order_detail_maker_order_id,
            order_detail_taker_order_id,
            order_detail_fill_sell_quantity,
            order_detail_fill_buy_quantity,
        ) = if transaction_info.is_some() {
            transaction_info
                .unwrap()
                .order_details
                .iter()
                .map(|order_detail| {
                    (
                        Bytes(order_detail.maker_order_id.clone()),
                        Bytes(order_detail.taker_order_id.clone()),
                        order_detail.fill_sell_quantity,
                        order_detail.fill_buy_quantity,
                    )
                })
                .multiunzip()
        } else {
            (vec![], vec![], vec![], vec![])
        };

        Self {
            hash: Bytes(transaction.txid.clone()),
            block_num: header_raw_data.number,
            index,
            expiration: tx_raw_data.expiration,
            authority_account_names: tx_raw_data
                .auths
                .iter()
                .map(|auth| auth.account.clone().unwrap().name.into())
                .collect(),
            authority_account_addresses: tx_raw_data
                .auths
                .iter()
                .map(|auth| auth.account.clone().unwrap().address.into())
                .collect(),
            authority_permission_names: tx_raw_data
                .auths
                .iter()
                .map(|auth| auth.permission_name.clone().into())
                .collect(),
            data: Bytes(tx_raw_data.data),
            contract_type: contract.map_or("".to_owned(), |contract| {
                contract.r#type().as_str_name().to_owned()
            }),
            contract_parameter: contract
                .map(|contract| Bytes(contract.parameter.clone().unwrap().value)),
            contract_provider: contract.map(|contract| Bytes(contract.provider.clone())),
            contract_name: contract.map(|contract| Bytes(contract.contract_name.clone())),
            contract_permission_id: contract.map(|contract| contract.permission_id),
            scripts: Bytes(tx_raw_data.scripts),
            timestamp: tx_raw_data.timestamp,
            fee_limit: tx_raw_data.fee_limit,
            signature: tx.signature.iter().map(|sig| Bytes(sig.clone())).collect(),
            constant_result: if transaction.constant_result.is_empty() {
                Bytes::default()
            } else {
                assert!(transaction.constant_result.len() == 1);
                Bytes(transaction.constant_result[0].clone())
            }, // the return of contract call

            // result -> txInfo
            fee: transaction_info.map_or(0, |transaction_info| transaction_info.fee),
            block_time_stamp: transaction_info
                .map_or(0, |transaction_info| transaction_info.block_time_stamp),
            contract_result: transaction_info.map_or(None, |transaction_info| {
                Some(Bytes(transaction_info.contract_result[0].clone()))
            }),
            contract_address: transaction_info.map_or(None, |transaction_info| {
                Some(len_20_addr_from_any_vec(
                    transaction_info.contract_address.clone(),
                ))
            }),
            energy_usage: receipt.clone().map_or(0, |receipt| receipt.energy_usage),
            energy_fee: receipt.clone().map_or(0, |receipt| receipt.energy_fee),
            origin_energy_usage: receipt
                .clone()
                .map_or(0, |receipt| receipt.origin_energy_usage),
            energy_usage_total: receipt
                .clone()
                .map_or(0, |receipt| receipt.energy_usage_total),
            net_usage: receipt.clone().map_or(0, |receipt| receipt.net_usage),
            net_fee: receipt.clone().map_or(0, |receipt| receipt.net_fee),
            receipt_result: receipt.clone().map_or("".to_owned(), |receipt| {
                receipt.result().as_str_name().to_owned()
            }),
            result: transaction_info.map_or("".to_owned(), |transaction_info| {
                transaction_info.result().as_str_name().to_owned()
            }),
            res_message: transaction_info.map_or(Bytes::default(), |transaction_info| {
                Bytes(transaction_info.res_message.clone())
            }),
            asset_issue_id: transaction_info.map_or("".to_owned(), |transaction_info| {
                transaction_info.asset_issue_id.clone()
            }),
            withdraw_amount: transaction_info
                .map_or(0, |transaction_info| transaction_info.withdraw_amount),
            unfreeze_amount: transaction_info
                .map_or(0, |transaction_info| transaction_info.unfreeze_amount),
            exchange_received_amount: transaction_info.map_or(0, |transaction_info| {
                transaction_info.exchange_received_amount
            }),
            exchange_inject_another_amount: transaction_info.map_or(0, |transaction_info| {
                transaction_info.exchange_inject_another_amount
            }),
            exchange_withdraw_another_amount: transaction_info.map_or(0, |transaction_info| {
                transaction_info.exchange_withdraw_another_amount
            }),
            exchange_id: transaction_info
                .map_or(0, |transaction_info| transaction_info.exchange_id),
            shielded_transaction_fee: transaction_info.map_or(0, |transaction_info| {
                transaction_info.shielded_transaction_fee
            }),
            order_id: transaction_info.map_or(Bytes::default(), |transaction_info| {
                Bytes(transaction_info.order_id.clone())
            }),

            order_detail_maker_order_id,
            order_detail_taker_order_id,
            order_detail_fill_sell_quantity,
            order_detail_fill_buy_quantity,

            packing_fee: transaction_info
                .map_or(0, |transaction_info| transaction_info.packing_fee),
            withdraw_expire_amount: transaction_info.map_or(0, |transaction_info| {
                transaction_info.withdraw_expire_amount
            }),
            cancel_unfreeze_v2_amount: transaction_info
                .map_or(HashMap::new(), |transaction_info| {
                    transaction_info.cancel_unfreeze_v2_amount.clone()
                }),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS events
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `logIndex` Int32,
    `address` String,
    topic0 Nullable(FixedString(32)),
    topic1 Nullable(FixedString(32)),
    topic2 Nullable(FixedString(32)),
    topic3 Nullable(FixedString(32)),
    `data` String
)
ENGINE = ReplacingMergeTree
ORDER BY (topic0, topic1, topic2, topic3, blockNum, transactionHash, logIndex)
SETTINGS index_granularity = 8192, allow_nullable_key=1; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct LogRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub log_index: i32,
    pub address: Bytes,

    pub topic0: Option<Bytes>,
    pub topic1: Option<Bytes>,
    pub topic2: Option<Bytes>,
    pub topic3: Option<Bytes>,

    pub data: Bytes,
}

impl LogRow {
    pub fn from_grpc(block_num: i64, transaction_hash: Vec<u8>, log_index: i32, log: &Log) -> Self {
        let topics: Vec<Bytes> = log
            .topics
            .iter()
            .map(|topic| topic.to_vec().into())
            .collect();

        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            log_index,
            address: len_20_addr_from_any_vec(log.address.clone()),
            topic0: topics.get(0).cloned(),
            topic1: topics.get(1).cloned(),
            topic2: topics.get(2).cloned(),
            topic3: topics.get(3).cloned(),
            data: Bytes(log.data.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS internals
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `internalIndex` Int32,
    `hash` FixedString(32),
    `callerAddress` String,
    `transferToAddress` String,
    `callValueInfos` Nested(
        tokenId String, 
        callValue Int64
    ),
    `note` String,
    `rejected` Bool,
    `extra` String
)
ENGINE = ReplacingMergeTree
ORDER BY (callerAddress, transferToAddress, blockNum, transactionHash, internalIndex)
SETTINGS index_granularity = 8192; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct InternalTransactionRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub internal_index: i32,

    pub hash: Bytes,
    pub caller_address: Bytes,
    pub transfer_to_address: Bytes,

    #[klickhouse(rename = "callValueInfos.tokenId")]
    pub call_value_infos_token_id: Vec<String>,
    #[klickhouse(rename = "callValueInfos.callValue")]
    pub call_value_infos_call_value: Vec<i64>,
    pub note: Bytes,
    pub rejected: bool,
    pub extra: String,
}

impl InternalTransactionRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        internal_index: i32,
        internal: &InternalTransaction,
    ) -> Self {
        let (call_value_infos_token_id, call_value_infos_call_value) = internal
            .call_value_info
            .iter()
            .map(|call_value_info| (call_value_info.token_id.clone(), call_value_info.call_value))
            .unzip();

        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            internal_index,

            hash: Bytes(internal.hash.clone()),
            caller_address: len_20_addr_from_any_vec(internal.caller_address.clone()),
            transfer_to_address: len_20_addr_from_any_vec(internal.transfer_to_address.clone()),
            call_value_infos_token_id,
            call_value_infos_call_value,
            note: Bytes(internal.note.clone()),
            rejected: internal.rejected,
            extra: internal.extra.clone(),
        }
    }
}

/////////////////////////////////////////////////////////////////

/** CREATE TABLE IF NOT EXISTS accountCreateContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
    `accountAddress` String,
    `type` Int32
) ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, accountAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccountCreateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub account_address: Bytes,
    pub r#type: i32,
}

impl AccountCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &AccountCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            account_address: len_20_addr_from_any_vec(call.account_address.clone()),
            r#type: call.r#type,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS transferContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
    `toAddress` String,
    `amount` Int64,
)
ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, toAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransferContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub to_address: Bytes,
    pub amount: i64,
}

impl TransferContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &TransferContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            to_address: len_20_addr_from_any_vec(call.to_address.clone()),
            amount: call.amount,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS transferAssetContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `assetName` String,
    `ownerAddress` String,
    `toAddress` String,
    `amount` Int64,
)
ENGINE = ReplacingMergeTree
ORDER BY (assetName, ownerAddress, toAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransferAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub asset_name: Bytes,
    pub owner_address: Bytes,
    pub to_address: Bytes,
    pub amount: i64,
}

impl TransferAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &TransferAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            asset_name: klickhouse::Bytes(call.asset_name.clone()),
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            to_address: len_20_addr_from_any_vec(call.to_address.clone()),
            amount: call.amount,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS voteAssetContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
    `voteAddress` Array(String),
    `support` Bool,
    `count` Int32,
) ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, voteAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192; */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct VoteAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub vote_address: Vec<Bytes>,
    pub support: bool,
    pub count: i32,
}

impl VoteAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &VoteAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            vote_address: call
                .vote_address
                .iter()
                .map(|vote| len_20_addr_from_any_vec(vote.clone()))
                .collect(),
            support: call.support,
            count: call.count,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS voteWitnessContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  votes Nested(
    voteAddress String,
    voteCount Int64
  ),
  support bool,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;  */
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct VoteWitnessContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    #[klickhouse(rename = "votes.voteAddress")]
    pub votes_vote_address: Vec<Bytes>,
    #[klickhouse(rename = "votes.voteCount")]
    pub votes_vote_count: Vec<i64>,
    pub support: bool,
}

impl VoteWitnessContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &VoteWitnessContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            votes_vote_address: call
                .votes
                .iter()
                .map(|vote| len_20_addr_from_any_vec(vote.vote_address.clone()))
                .collect(),
            votes_vote_count: call.votes.iter().map(|vote| vote.vote_count).collect(),
            support: call.support,
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct Vote {
    vote_address: Bytes,
    vote_count: i64,
}

impl Vote {
    pub fn from_grpc(vote: tron_grpc::Vote) -> Self {
        Self {
            vote_address: len_20_addr_from_any_vec(vote.vote_address),
            vote_count: vote.vote_count,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS witnessCreateContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  url String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, url, blockNum, transactionHash, contractIndex)
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WitnessCreateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub url: Bytes,
}

impl WitnessCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &WitnessCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            url: Bytes(call.url.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS assetIssueContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  id String,
  ownerAddress String,
  name String,
  abbr String,
  totalSupply Int64,
  trxNum Int32,
  precision Int32,
  num Int32,
  startTime Int64,
  endTime Int64,
  order Int64,
  voteScore Int32,
  description String,
  url String,
  freeAssetNetLimit Int64,
  publicFreeAssetNetLimit Int64,
  publicFreeAssetNetUsage Int64,
  publicLatestFreeNetTime Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex, name, abbr, url, id)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AssetIssueContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub id: String,
    pub owner_address: Bytes,
    pub name: Bytes,
    pub abbr: Bytes,
    pub total_supply: i64,
    pub trx_num: i32,
    pub precision: i32,
    pub num: i32,
    pub start_time: i64,
    pub end_time: i64,
    pub order: i64, // useless but still added
    pub vote_score: i32,
    pub description: Bytes,
    pub url: Bytes,
    pub free_asset_net_limit: i64,
    pub public_free_asset_net_limit: i64,
    pub public_free_asset_net_usage: i64,
    pub public_latest_free_net_time: i64,
}

impl AssetIssueContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &AssetIssueContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            id: call.id.clone(),
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            name: klickhouse::Bytes(call.name.clone()),
            abbr: klickhouse::Bytes(call.abbr.clone()),
            total_supply: call.total_supply,
            trx_num: call.trx_num,
            precision: call.precision,
            num: call.num,
            start_time: call.start_time,
            end_time: call.end_time,
            order: call.order,
            vote_score: call.vote_score,
            description: klickhouse::Bytes(call.description.clone()),
            url: klickhouse::Bytes(call.url.clone()),
            free_asset_net_limit: call.free_asset_net_limit,
            public_free_asset_net_limit: call.public_free_asset_net_limit,
            public_free_asset_net_usage: call.public_free_asset_net_usage,
            public_latest_free_net_time: call.public_latest_free_net_time,
        }
    }
}

/**
CREATE TABLE IF NOT EXISTS witnessUpdateContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  updateUrl String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WitnessUpdateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub update_url: Bytes,
}

impl WitnessUpdateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &WitnessUpdateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            update_url: klickhouse::Bytes(call.update_url.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS participateAssetIssueContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  toAddress String,
  assetName String,
  amount Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, toAddress, assetName, blockNum, transactionHash, contractIndex)
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ParticipateAssetIssueContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub to_address: Bytes,
    pub asset_name: Bytes,
    pub amount: i64,
}

impl ParticipateAssetIssueContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ParticipateAssetIssueContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            to_address: len_20_addr_from_any_vec(call.to_address.clone()),
            asset_name: klickhouse::Bytes(call.asset_name.clone()),
            amount: call.amount,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS accountUpdateContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  accountName String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, accountName, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccountUpdateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub account_name: Bytes,
}

impl AccountUpdateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &AccountUpdateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            account_name: klickhouse::Bytes(call.account_name.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS freezeBalanceContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  frozenBalance Int64,
  frozenDuration Int64,
  resource Int32,
  receiverAddress String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, receiverAddress, resource, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct FreezeBalanceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub frozen_balance: i64,
    pub frozen_duration: i64,
    pub resource: i32,
    pub receiver_address: Bytes,
}
impl FreezeBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &FreezeBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            frozen_balance: call.frozen_balance,
            frozen_duration: call.frozen_duration,
            resource: call.resource,
            receiver_address: len_20_addr_from_any_vec(call.receiver_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS unfreezeBalanceContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  resource Int32,
  receiverAddress String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, receiverAddress, resource, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnfreezeBalanceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub resource: i32,
    pub receiver_address: Bytes,
}

impl UnfreezeBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UnfreezeBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            resource: call.resource,
            receiver_address: len_20_addr_from_any_vec(call.receiver_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS withdrawBalanceContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WithdrawBalanceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
}

impl WithdrawBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &WithdrawBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS unfreezeAssetContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnfreezeAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
}

impl UnfreezeAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UnfreezeAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS updateAssetContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  description String,
  url String,
  newLimit Int64,
  newPublicLimit Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, url, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub description: Bytes,
    pub url: Bytes,
    pub new_limit: i64,
    pub new_public_limit: i64,
}

impl UpdateAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            description: klickhouse::Bytes(call.description.clone()),
            url: klickhouse::Bytes(call.url.clone()),
            new_limit: call.new_limit,
            new_public_limit: call.new_public_limit,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS proposalCreateContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  parameters Map(Int64, Int64) COMMENT 'key -> value',
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ProposalCreateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub parameters: HashMap<i64, i64>
}

impl ProposalCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ProposalCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            parameters: call.parameters.clone(),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS proposalApproveContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  proposalId Int64,
  isAddApproval Bool,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ProposalApproveContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub proposal_id: i64,
    pub is_add_approval: bool,
}

impl ProposalApproveContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ProposalApproveContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            proposal_id: call.proposal_id,
            is_add_approval: call.is_add_approval,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS proposalDeleteContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  proposalId Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ProposalDeleteContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub proposal_id: i64,
}

impl ProposalDeleteContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ProposalDeleteContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            proposal_id: call.proposal_id,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS setAccountIdContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  accountId String,
  ownerAddress String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct SetAccountIdContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub account_id: Bytes,
    pub owner_address: Bytes,
}

impl SetAccountIdContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &SetAccountIdContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            transaction_index,
            contract_index,

            account_id: Bytes(call.account_id.clone()),
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS createSmartContracts (
    blockNum Int64,
    transactionHash FixedString(32),
    transactionIndex Int64,
    contractIndex Int64,

    ownerAddress String,

    originAddress Nullable(String),
    contractAddress Nullable(String),
    abi Nullable(String),
    bytecode Nullable(String),
    callValue Nullable(Int64),
    consumeUserResourcePercent Nullable(Int64),
    name Nullable(String),
    originEnergyLimit Nullable(Int64),
    codeHash Nullable(FixedString(32)),
    trxHash Nullable(FixedString(32)),
    version Nullable(Int32),

    callTokenValue Int64,
    tokenId Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, originAddress, contractAddress, name, blockNum, transactionHash, trxHash, codeHash, contractIndex)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct CreateSmartContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    #[klickhouse(flatten)]
    pub new_contract: SmartContractField,
    pub call_token_value: i64,
    pub token_id: i64,
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct SmartContractField {
    pub origin_address: Option<Bytes>,
    pub contract_address: Option<Bytes>,
    pub abi: Option<String>, // save as string
    pub bytecode: Option<Bytes>,
    pub call_value: Option<i64>,
    pub consume_user_resource_percent: Option<i64>,
    pub name: Option<String>,
    pub origin_energy_limit: Option<i64>,
    pub code_hash: Option<Bytes>,
    pub trx_hash: Option<Bytes>,
    pub version: Option<i32>,
}

impl CreateSmartContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &CreateSmartContract,
    ) -> Self {
        let new_contract = call.new_contract.clone();
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            new_contract: SmartContractField {
                origin_address: new_contract.as_ref().map_or(None, |sc| {
                    Some(len_20_addr_from_any_vec(sc.origin_address.clone()))
                }),
                contract_address: new_contract.as_ref().map_or(None, |sc| {
                    Some(len_20_addr_from_any_vec(sc.contract_address.clone()))
                }),
                abi: new_contract.as_ref().map_or(None, |sc| {
                    sc.abi
                        .clone()
                        .map_or(None, |abi| Some(serde_json::to_string(&abi).unwrap()))
                }),
                bytecode: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(klickhouse::Bytes(sc.bytecode.clone()))),
                call_value: new_contract.as_ref().map_or(None, |sc| Some(sc.call_value)),
                consume_user_resource_percent: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(sc.consume_user_resource_percent)),
                name: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(sc.name.clone())),
                origin_energy_limit: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(sc.origin_energy_limit)),
                code_hash: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(klickhouse::Bytes(sc.code_hash.clone()))),
                trx_hash: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(klickhouse::Bytes(sc.trx_hash.clone()))),
                version: new_contract.as_ref().map_or(None, |sc| Some(sc.version)),
            },
            call_token_value: call.call_token_value,
            token_id: call.token_id,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS triggerSmartContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  contractAddress String,
  callValue Int64,
  data String,
  callTokenValue Int64,
  tokenId Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, contractAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TriggerSmartContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub contract_address: Bytes,
    pub call_value: i64,
    pub data: Bytes,
    pub call_token_value: i64,
    pub token_id: i64,
}

impl TriggerSmartContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &TriggerSmartContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            contract_address: len_20_addr_from_any_vec(call.contract_address.clone()),
            call_value: call.call_value,
            data: klickhouse::Bytes(call.data.clone()),
            call_token_value: call.call_token_value,
            token_id: call.token_id,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS updateSettingContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  contractAddress String,
  consumeUserResourcePercent Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, contractAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateSettingContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub contract_address: Bytes,
    pub consume_user_resource_percent: i64,
}

impl UpdateSettingContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateSettingContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            contract_address: len_20_addr_from_any_vec(call.contract_address.clone()),
            consume_user_resource_percent: call.consume_user_resource_percent,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS exchangeCreateContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  firstTokenId String,
  firstTokenBalance Int64,
  secondTokenId String,
  secondTokenBalance Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, firstTokenId, secondTokenId, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeCreateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub first_token_id: Bytes,
    pub first_token_balance: i64,
    pub second_token_id: Bytes,
    pub second_token_balance: i64,
}

impl ExchangeCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            first_token_id: klickhouse::Bytes(call.first_token_id.clone()),
            first_token_balance: call.first_token_balance,
            second_token_id: klickhouse::Bytes(call.second_token_id.clone()),
            second_token_balance: call.second_token_balance,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS exchangeInjectContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  exchangeId Int64,
  tokenId String,
  quant Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, exchangeId, tokenId, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeInjectContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub exchange_id: i64,
    pub token_id: Bytes,
    pub quant: i64,
}

impl ExchangeInjectContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeInjectContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            exchange_id: call.exchange_id,
            token_id: klickhouse::Bytes(call.token_id.clone()),
            quant: call.quant,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS exchangeWithdrawContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  exchangeId Int64,
  tokenId String,
  quant Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, exchangeId, tokenId, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeWithdrawContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub exchange_id: i64,
    pub token_id: Bytes,
    pub quant: i64,
}

impl ExchangeWithdrawContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeWithdrawContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            exchange_id: call.exchange_id,
            token_id: klickhouse::Bytes(call.token_id.clone()),
            quant: call.quant,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS exchangeTransactionContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  exchangeId Int64,
  tokenId String,
  quant Int64,
  expected Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, exchangeId, tokenId, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeTransactionContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub exchange_id: i64,
    pub token_id: Bytes,
    pub quant: i64,
    pub expected: i64,
}

impl ExchangeTransactionContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeTransactionContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            exchange_id: call.exchange_id,
            token_id: klickhouse::Bytes(call.token_id.clone()),
            quant: call.quant,
            expected: call.expected,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS updateEnergyLimitContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  contractAddress String,
  originEnergyLimit Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, contractAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateEnergyLimitContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub contract_address: Bytes,
    pub origin_energy_limit: i64,
}

impl UpdateEnergyLimitContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateEnergyLimitContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            contract_address: len_20_addr_from_any_vec(call.contract_address.clone()),
            origin_energy_limit: call.origin_energy_limit,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS accountPermissionUpdateContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,

  ownerPermissionType Nullable(Int32),
  ownerPermissionId Nullable(Int32),
  ownerPermissionName Nullable(String),
  ownerThreshold Nullable(Int64),
  ownerParentId Nullable(Int32),
  ownerKeys Map(String, Int64),
  ownerOperations String,

  witnessPermissionType Nullable(Int32),
  witnessPermissionId Nullable(Int32),
  witnessPermissionName Nullable(String),
  witnessThreshold Nullable(Int64),
  witnessParentId Nullable(Int32),
  witnessKeys Map(String, Int64),
  witnessOperations String,

  actives Nested(
    permissionType Int32,
    permissionId Int32,
    permissionName String,
    threshold Int64,
    parentId Int32,
    keys Map(String, Int64),
    operations String
  )
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192, allow_nullable_key=1;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccountPermissionUpdateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,

    pub owner_permission_type: Option<i32>,
    pub owner_permission_id: Option<i32>,
    pub owner_permission_name: Option<String>,
    pub owner_threshold: Option<i64>,
    pub owner_parent_id: Option<i32>,
    pub owner_keys: HashMap<Bytes, i64>,
    pub owner_operations: Bytes,

    pub witness_permission_type: Option<i32>,
    pub witness_permission_id: Option<i32>,
    pub witness_permission_name: Option<String>,
    pub witness_threshold: Option<i64>,
    pub witness_parent_id: Option<i32>,
    pub witness_keys: HashMap<Bytes, i64>,
    pub witness_operations: Bytes,

    #[klickhouse(rename = "actives.permissionType")]
    pub actives_permission_type: Vec<i32>,
    #[klickhouse(rename = "actives.permissionId")]
    pub actives_permission_id: Vec<i32>,
    #[klickhouse(rename = "actives.permissionName")]
    pub actives_permission_name: Vec<String>,
    #[klickhouse(rename = "actives.threshold")]
    pub actives_threshold: Vec<i64>,
    #[klickhouse(rename = "actives.parentId")]
    pub actives_parent_id: Vec<i32>,
    #[klickhouse(rename = "actives.keys")]
    pub actives_keys: Vec<HashMap<Bytes, i64>>,
    #[klickhouse(rename = "actives.operations")]
    pub actives_operations: Vec<Bytes>,
}

impl AccountPermissionUpdateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &AccountPermissionUpdateContract,
    ) -> Self {
        let (
            owner_permission_type,
            owner_permission_id,
            owner_permission_name,
            owner_threshold,
            owner_parent_id,
            owner_keys,
            owner_operations,
        ) = match &call.owner {
            Some(owner) => {
                let keys =
                    owner
                        .keys
                        .clone()
                        .into_iter()
                        .fold(HashMap::default(), |mut keys, key| {
                            keys.insert(len_20_addr_from_any_vec(key.address), key.weight);
                            keys
                        });
                (
                    Some(owner.r#type),
                    Some(owner.id),
                    Some(owner.permission_name.clone()),
                    Some(owner.threshold),
                    Some(owner.parent_id),
                    keys,
                    Bytes(owner.operations.clone()),
                )
            }
            None => (
                None,
                None,
                None,
                None,
                None,
                HashMap::default(),
                Bytes::default(),
            ),
        };

        let (
            witness_permission_type,
            witness_permission_id,
            witness_permission_name,
            witness_threshold,
            witness_parent_id,
            witness_keys,
            witness_operations,
        ) =
            match &call.witness {
                Some(witness) => {
                    let keys = witness.keys.clone().into_iter().fold(
                        HashMap::default(),
                        |mut keys, key| {
                            keys.insert(len_20_addr_from_any_vec(key.address), key.weight);
                            keys
                        },
                    );
                    (
                        Some(witness.r#type),
                        Some(witness.id),
                        Some(witness.permission_name.clone()),
                        Some(witness.threshold),
                        Some(witness.parent_id),
                        keys,
                        Bytes(witness.operations.clone()),
                    )
                }
                None => (
                    None,
                    None,
                    None,
                    None,
                    None,
                    HashMap::default(),
                    Bytes::default(),
                ),
            };

        let (
            actives_permission_type,
            actives_permission_id,
            actives_permission_name,
            actives_threshold,
            actives_parent_id,
            actives_keys,
            actives_operations,
        ) =
            call.actives.iter().fold(
                (
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                ),
                |(
                    mut actives_permission_type,
                    mut actives_permission_id,
                    mut actives_permission_name,
                    mut actives_threshold,
                    mut actives_parent_id,
                    mut actives_keys,
                    mut actives_operations,
                ),
                 active| {
                    let keys = active.keys.clone().into_iter().fold(
                        HashMap::default(),
                        |mut keys, key| {
                            keys.insert(len_20_addr_from_any_vec(key.address), key.weight);
                            keys
                        },
                    );
                    actives_permission_type.push(active.r#type);
                    actives_permission_id.push(active.id);
                    actives_permission_name.push(active.permission_name.clone());
                    actives_threshold.push(active.threshold);
                    actives_parent_id.push(active.parent_id);
                    actives_keys.push(keys);
                    actives_operations.push(Bytes(active.operations.clone()));
                    (
                        actives_permission_type,
                        actives_permission_id,
                        actives_permission_name,
                        actives_threshold,
                        actives_parent_id,
                        actives_keys,
                        actives_operations,
                    )
                },
            );

        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),

            owner_permission_id,
            owner_permission_type,
            owner_permission_name,
            owner_threshold,
            owner_parent_id,
            owner_keys,
            owner_operations,

            witness_permission_id,
            witness_permission_type,
            witness_permission_name,
            witness_threshold,
            witness_parent_id,
            witness_keys,
            witness_operations,

            actives_permission_type,
            actives_permission_id,
            actives_permission_name,
            actives_threshold,
            actives_parent_id,
            actives_keys,
            actives_operations,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS clearAbiContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  contractAddress String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, contractAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ClearAbiContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub contract_address: Bytes,
}

impl ClearAbiContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ClearAbiContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            contract_address: len_20_addr_from_any_vec(call.contract_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS updateBrokerageContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  brokerage Int32,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateBrokerageContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub brokerage: i32,
}

impl UpdateBrokerageContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateBrokerageContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            brokerage: call.brokerage,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS shieldedTransferContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  transparentFromAddress String,
  fromAmount Int64,

  spendValueCommitment String,
  anchor String,
  nullifier String,
  rk String,
  spendZkproof String,
  spendAuthoritySignature String,

  receiveValueCommitment String,
  noteCommitment String,
  epk String,
  cEnc String,
  cOut String,
  receiveZkproof String,

  bindingSignature FixedString(64),
  transparentToAddress String,
  toAmount Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (transparentFromAddress, transparentToAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ShieldedTransferContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub transparent_from_address: Bytes,
    pub from_amount: i64,
    #[klickhouse(rename = "spendDescription.valueCommitment")]
    pub spend_description_value_commitment: Vec<Bytes>,
    #[klickhouse(rename = "spendDescription.anchor")]
    pub spend_description_anchor: Vec<Bytes>,
    #[klickhouse(rename = "spendDescription.nullifier")]
    pub spend_description_nullifier: Vec<Bytes>,
    #[klickhouse(rename = "spendDescription.rk")]
    pub spend_description_rk: Vec<Bytes>,
    #[klickhouse(rename = "spendDescription.zkproof")]
    pub spend_description_zkproof: Vec<Bytes>,
    #[klickhouse(rename = "spendDescription.authoritySignature")]
    pub spend_description_authority_signature: Vec<Bytes>,

    #[klickhouse(rename = "receiveDescription.valueCommitment")]
    pub receive_description_value_commitment: Vec<Bytes>,
    #[klickhouse(rename = "receiveDescription.noteCommitment")]
    pub receive_description_note_commitment: Vec<Bytes>,
    #[klickhouse(rename = "receiveDescription.epk")]
    pub receive_description_epk: Vec<Bytes>,
    #[klickhouse(rename = "receiveDescription.cEnc")]
    pub receive_description_c_enc: Vec<Bytes>,
    #[klickhouse(rename = "receiveDescription.cOut")]
    pub receive_description_c_out: Vec<Bytes>,
    #[klickhouse(rename = "receiveDescription.zkproof")]
    pub receive_description_zkproof: Vec<Bytes>,

    pub binding_signature: Bytes,
    pub transparent_to_address: Bytes,
    pub to_amount: i64,
}

impl ShieldedTransferContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &ShieldedTransferContract,
    ) -> Self {
        let (
            spend_description_value_commitment,
            spend_description_anchor,
            spend_description_nullifier,
            spend_description_rk,
            spend_description_zkproof,
            spend_description_authority_signature,
        ) = call.spend_description.clone().into_iter().fold(
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            |(
                mut spend_description_value_commitment,
                mut spend_description_anchor,
                mut spend_description_nullifier,
                mut spend_description_rk,
                mut spend_description_zkproof,
                mut spend_description_authority_signature,
            ),
             spend_description| {
                spend_description_value_commitment
                    .push(klickhouse::Bytes(spend_description.value_commitment));
                spend_description_anchor.push(klickhouse::Bytes(spend_description.anchor));
                spend_description_nullifier.push(klickhouse::Bytes(spend_description.nullifier));
                spend_description_rk.push(klickhouse::Bytes(spend_description.rk));
                spend_description_zkproof.push(klickhouse::Bytes(spend_description.zkproof));
                spend_description_authority_signature.push(klickhouse::Bytes(
                    spend_description.spend_authority_signature,
                ));
                (
                    spend_description_value_commitment,
                    spend_description_anchor,
                    spend_description_nullifier,
                    spend_description_rk,
                    spend_description_zkproof,
                    spend_description_authority_signature,
                )
            },
        );

        let (
            receive_description_value_commitment,
            receive_description_note_commitment,
            receive_description_epk,
            receive_description_c_enc,
            receive_description_c_out,
            receive_description_zkproof,
        ) = call.receive_description.clone().into_iter().fold(
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            |(
                mut receive_description_value_commitment,
                mut receive_description_note_commitment,
                mut receive_description_epk,
                mut receive_description_c_enc,
                mut receive_description_c_out,
                mut receive_description_zkproof,
            ),
             receive_description| {
                receive_description_value_commitment
                    .push(klickhouse::Bytes(receive_description.value_commitment));
                receive_description_note_commitment
                    .push(klickhouse::Bytes(receive_description.note_commitment));
                receive_description_epk.push(klickhouse::Bytes(receive_description.epk));
                receive_description_c_enc.push(klickhouse::Bytes(receive_description.c_enc));
                receive_description_c_out.push(klickhouse::Bytes(receive_description.c_out));
                receive_description_zkproof.push(klickhouse::Bytes(receive_description.zkproof));
                (
                    receive_description_value_commitment,
                    receive_description_note_commitment,
                    receive_description_epk,
                    receive_description_c_enc,
                    receive_description_c_out,
                    receive_description_zkproof,
                )
            },
        );

        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            transparent_from_address: len_20_addr_from_any_vec(
                call.transparent_from_address.clone(),
            ),
            from_amount: call.from_amount,
            spend_description_value_commitment,
            spend_description_anchor,
            spend_description_nullifier,
            spend_description_rk,
            spend_description_zkproof,
            spend_description_authority_signature,
            receive_description_value_commitment,
            receive_description_note_commitment,
            receive_description_epk,
            receive_description_c_enc,
            receive_description_c_out,
            receive_description_zkproof,
            binding_signature: klickhouse::Bytes(call.binding_signature.clone()),
            transparent_to_address: len_20_addr_from_any_vec(call.transparent_to_address.clone()),
            to_amount: call.to_amount,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS marketSellAssetContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  sellTokenId String,
  sellTokenQuantity Int64,
  buyTokenId String,
  buyTokenQuantity Int64,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, sellTokenId, buyTokenId, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct MarketSellAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub sell_token_id: Bytes,
    pub sell_token_quantity: i64,
    pub buy_token_id: Bytes,
    pub buy_token_quantity: i64,
}

impl MarketSellAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &MarketSellAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            sell_token_id: klickhouse::Bytes(call.sell_token_id.clone()),
            sell_token_quantity: call.sell_token_quantity,
            buy_token_id: klickhouse::Bytes(call.buy_token_id.clone()),
            buy_token_quantity: call.buy_token_quantity,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS marketCancelOrderContracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  orderId String,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, orderId, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct MarketCancelOrderContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub order_id: Bytes,
}

impl MarketCancelOrderContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &MarketCancelOrderContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            order_id: klickhouse::Bytes(call.order_id.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS freezeBalanceV2Contracts (
  blockNum Int64,
  transactionHash FixedString(32),
  transactionIndex Int64,
  contractIndex Int64,

  ownerAddress String,
  frozenBalance Int64,
  resource Int32,
) ENGINE = ReplacingMergeTree()
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct FreezeBalanceV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub frozen_balance: i64,
    pub resource: i32,
}

impl FreezeBalanceV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &FreezeBalanceV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            frozen_balance: call.frozen_balance,
            resource: call.resource,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS unfreezeBalanceV2Contracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
    `unfreezeBalance` Int64,
    `resource` Int32,
) ENGINE = ReplacingMergeTree
ORDER BY (blockNum, ownerAddress, resource, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnfreezeBalanceV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub unfreeze_balance: i64,
    pub resource: i32,
}

impl UnfreezeBalanceV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UnfreezeBalanceV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            unfreeze_balance: call.unfreeze_balance,
            resource: call.resource,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS withdrawExpireUnfreezeContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
) ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WithdrawExpireUnfreezeContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
}

impl WithdrawExpireUnfreezeContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &WithdrawExpireUnfreezeContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS delegateResourceContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
    `resource` Int32,
    `balance` Int64,
    `receiverAddress` String,
    `lock` Boolean,
    `lockPeriod` Int64,
) ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, resource, receiverAddress, lock, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct DelegateResourceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub resource: i32,
    pub balance: i64,
    pub receiver_address: Bytes,
    pub lock: bool,
    pub lock_period: i64,
}

impl DelegateResourceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &DelegateResourceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            resource: call.resource,
            balance: call.balance,
            receiver_address: len_20_addr_from_any_vec(call.receiver_address.clone()),
            lock: call.lock,
            lock_period: call.lock_period,
        }
    }
}

/** CREATE TABLE IF NOT EXISTS undelegateResourceContracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
    `resource` Int32,
    `balance` Int64,
    `receiverAddress` String,
) ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, resource, receiverAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UndelegateResourceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
    pub resource: i32,
    pub balance: i64,
    pub receiver_address: Bytes,
}

impl UndelegateResourceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &UnDelegateResourceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
            resource: call.resource,
            balance: call.balance,
            receiver_address: len_20_addr_from_any_vec(call.receiver_address.clone()),
        }
    }
}

/** CREATE TABLE IF NOT EXISTS cancelAllUnfreezeV2Contracts
(
    `blockNum` Int64,
    `transactionHash` FixedString(32),
    `transactionIndex` Int64,
    `contractIndex` Int64,

    `ownerAddress` String,
) ENGINE = ReplacingMergeTree
ORDER BY (ownerAddress, blockNum, transactionHash, contractIndex)
SETTINGS index_granularity = 8192;
*/
#[derive(Row, Documented, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct CancelAllUnfreezeV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: Bytes,
}

impl CancelAllUnfreezeV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: &CancelAllUnfreezeV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,
            owner_address: len_20_addr_from_any_vec(call.owner_address.clone()),
        }
    }
}
