use std::collections::HashMap;

use clickhouse::Row;
use documented::Documented;
use itertools::MultiUnzip;
use serde::{Deserialize, Serialize};
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

use crate::ch_tron::utils;

use super::utils::format_tron_address;

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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct BlockRow {
    pub hash: String,
    pub timestamp: i64,
    #[serde(rename = "txTrieRoot")]
    pub tx_trie_root: String,
    #[serde(rename = "parentHash")]
    pub parent_hash: String,
    pub number: i64,
    #[serde(rename = "witnessId")]
    pub witness_id: i64,
    #[serde(rename = "witnessAddress")]
    pub witness_address: String,
    pub version: i32,
    #[serde(rename = "accountStateRoot")]
    pub account_state_root: String,
    #[serde(rename = "witnessSignature")]
    pub witness_signature: String,
}

impl BlockRow {
    pub fn from_grpc(block: &BlockExtention) -> Self {
        let header = block.block_header.clone().unwrap();
        let header_raw_data = header.raw_data.unwrap();

        Self {
            hash: hex::encode(&block.blockid),
            timestamp: header_raw_data.timestamp,
            tx_trie_root: hex::encode(&header_raw_data.tx_trie_root),
            parent_hash: hex::encode(&header_raw_data.parent_hash),
            number: header_raw_data.number,
            witness_id: header_raw_data.witness_id,
            witness_address: if header_raw_data.witness_address.starts_with(&[0x41]) {
                format_tron_address(header_raw_data.witness_address.clone())
            } else {
                format_tron_address(header_raw_data.witness_address.clone()) // for the genesis phase
            },
            version: header_raw_data.version,
            account_state_root: hex::encode(&header_raw_data.account_state_root),
            witness_signature: hex::encode(&header.witness_signature),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransactionRow {
    pub hash: String,
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    pub index: i64,

    pub expiration: i64,
    #[serde(rename = "authorityAccountNames")]
    pub authority_account_names: Vec<String>,
    #[serde(rename = "authorityAccountAddresses")]
    pub authority_account_addresses: Vec<String>,
    #[serde(rename = "authorityPermissionNames")]
    pub authority_permission_names: Vec<String>,
    pub data: String,

    #[serde(rename = "contractType")]
    pub contract_type: String,
    #[serde(rename = "contractProvider")]
    pub contract_provider: Option<String>,
    #[serde(rename = "contractName")]
    pub contract_name: Option<String>,
    #[serde(rename = "contractPermissionId")]
    pub contract_permission_id: Option<i32>,

    pub scripts: String,
    pub timestamp: i64,
    #[serde(rename = "feeLimit")]
    pub fee_limit: i64,

    pub signature: Vec<String>,
    #[serde(rename = "constantResult")]
    pub constant_result: String,

    pub fee: i64,
    #[serde(rename = "blockTimeStamp")]
    pub block_time_stamp: i64,
    #[serde(rename = "contractResult")]
    pub contract_result: Option<String>,
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<String>,

    #[serde(rename = "energyUsage")]
    pub energy_usage: i64,
    #[serde(rename = "energyFee")]
    pub energy_fee: i64,
    #[serde(rename = "originEnergyUsage")]
    pub origin_energy_usage: i64,
    #[serde(rename = "energyUsageTotal")]
    pub energy_usage_total: i64,
    #[serde(rename = "netUsage")]
    pub net_usage: i64,
    #[serde(rename = "netFee")]
    pub net_fee: i64,
    #[serde(rename = "receiptResult")]
    pub receipt_result: String,

    pub result: String,
    #[serde(rename = "resMessage")]
    pub res_message: String,

    #[serde(rename = "assetIssueId")]
    pub asset_issue_id: String,
    #[serde(rename = "withdrawAmount")]
    pub withdraw_amount: i64,
    #[serde(rename = "unfreezeAmount")]
    pub unfreeze_amount: i64,
    #[serde(rename = "exchangeReceivedAmount")]
    pub exchange_received_amount: i64,
    #[serde(rename = "exchangeInjectAnotherAmount")]
    pub exchange_inject_another_amount: i64,
    #[serde(rename = "exchangeWithdrawAnotherAmount")]
    pub exchange_withdraw_another_amount: i64,
    #[serde(rename = "exchangeId")]
    pub exchange_id: i64,
    #[serde(rename = "shieldedTransactionFee")]
    pub shielded_transaction_fee: i64,

    #[serde(rename = "orderId")]
    pub order_id: String,
    // pub order_details: Vec<MarketOrderDetailField>,
    #[serde(rename = "orderDetails.makerOrderId")]
    pub order_detail_maker_order_id: Vec<String>,
    #[serde(rename = "orderDetails.takerOrderId")]
    pub order_detail_taker_order_id: Vec<String>,
    #[serde(rename = "orderDetails.fillSellQuantity")]
    pub order_detail_fill_sell_quantity: Vec<i64>,
    #[serde(rename = "orderDetails.fillBuyQuantity")]
    pub order_detail_fill_buy_quantity: Vec<i64>,
    #[serde(rename = "packingFee")]
    pub packing_fee: i64,
    #[serde(rename = "withdrawExpireAmount")]
    pub withdraw_expire_amount: i64,
    #[serde(rename = "cancelUnfreezeV2Amount")]
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
                        hex::encode(&order_detail.maker_order_id),
                        hex::encode(&order_detail.taker_order_id),
                        order_detail.fill_sell_quantity,
                        order_detail.fill_buy_quantity,
                    )
                })
                .multiunzip()
        } else {
            (vec![], vec![], vec![], vec![])
        };

        Self {
            hash: hex::encode(&transaction.txid),
            block_num: header_raw_data.number,
            index,
            expiration: tx_raw_data.expiration,
            authority_account_names: tx_raw_data
                .auths
                .iter()
                .map(|auth| {
                    String::from_utf8_lossy(&auth.account.clone().unwrap().name).to_string()
                })
                .collect(),
            authority_account_addresses: tx_raw_data
                .auths
                .iter()
                .map(|auth| format_tron_address(auth.account.clone().unwrap().address))
                .collect(),
            authority_permission_names: tx_raw_data
                .auths
                .iter()
                .map(|auth| String::from_utf8_lossy(&auth.permission_name).to_string())
                .collect(),
            data: hex::encode(&tx_raw_data.data),
            contract_type: contract.map_or("".to_owned(), |contract| {
                contract.r#type().as_str_name().to_owned()
            }),
            contract_provider: contract
                .map(|contract| String::from_utf8_lossy(&contract.provider).to_string()),
            contract_name: contract
                .map(|contract| String::from_utf8_lossy(&contract.contract_name).to_string()),
            contract_permission_id: contract.map(|contract| contract.permission_id),
            scripts: hex::encode(&tx_raw_data.scripts),
            timestamp: tx_raw_data.timestamp,
            fee_limit: tx_raw_data.fee_limit,
            signature: tx.signature.iter().map(|sig| hex::encode(sig)).collect(),
            constant_result: if transaction.constant_result.is_empty() {
                String::new()
            } else {
                assert!(transaction.constant_result.len() == 1);
                hex::encode(&transaction.constant_result[0])
            }, // the return of contract call

            // result -> txInfo
            fee: transaction_info.map_or(0, |transaction_info| transaction_info.fee),
            block_time_stamp: transaction_info
                .map_or(0, |transaction_info| transaction_info.block_time_stamp),
            contract_result: transaction_info.map_or(None, |transaction_info| {
                Some(hex::encode(&transaction_info.contract_result[0]))
            }),
            contract_address: transaction_info.map_or(None, |transaction_info| {
                Some(format_tron_address(
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
            res_message: transaction_info.map_or(String::new(), |transaction_info| {
                hex::encode(&transaction_info.res_message)
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
            order_id: transaction_info.map_or(String::new(), |transaction_info| {
                hex::encode(&transaction_info.order_id)
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct LogRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "logIndex")]
    pub log_index: i32,
    pub address: String,

    pub topic0: Option<String>,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,

    pub data: String,
}

impl LogRow {
    pub fn from_grpc(block_num: i64, transaction_hash: String, log_index: i32, log: &Log) -> Self {
        let topics: Vec<String> = log.topics.iter().map(|topic| hex::encode(topic)).collect();

        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            log_index,
            address: format_tron_address(log.address.clone()),
            topic0: topics.get(0).cloned(),
            topic1: topics.get(1).cloned(),
            topic2: topics.get(2).cloned(),
            topic3: topics.get(3).cloned(),
            data: hex::encode(&log.data),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct InternalTransactionRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "internalIndex")]
    pub internal_index: i32,

    pub hash: String,
    #[serde(rename = "callerAddress")]
    pub caller_address: String,
    #[serde(rename = "transferToAddress")]
    pub transfer_to_address: String,

    #[serde(rename = "callValueInfos.tokenId")]
    pub call_value_infos_token_id: Vec<String>,
    #[serde(rename = "callValueInfos.callValue")]
    pub call_value_infos_call_value: Vec<i64>,
    pub note: String,
    pub rejected: bool,
    pub extra: String,
}

impl InternalTransactionRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
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
            transaction_hash: hex::encode(&transaction_hash),
            internal_index,

            hash: hex::encode(&internal.hash),
            caller_address: format_tron_address(internal.caller_address.clone()),
            transfer_to_address: format_tron_address(internal.transfer_to_address.clone()),
            call_value_infos_token_id,
            call_value_infos_call_value,
            note: String::from_utf8_lossy(&internal.note).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct AccountCreateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "accountAddress")]
    pub account_address: String,
    pub r#type: i32,
}

impl AccountCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &AccountCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            account_address: format_tron_address(call.account_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransferContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "toAddress")]
    pub to_address: String,
    pub amount: i64,
}

impl TransferContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &TransferContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            to_address: format_tron_address(call.to_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransferAssetContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "assetName")]
    pub asset_name: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "toAddress")]
    pub to_address: String,
    pub amount: i64,
}

impl TransferAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &TransferAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            asset_name: String::from_utf8_lossy(&call.asset_name).to_string(),
            owner_address: format_tron_address(call.owner_address.clone()),
            to_address: format_tron_address(call.to_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct VoteAssetContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "voteAddress")]
    pub vote_address: Vec<String>,
    pub support: bool,
    pub count: i32,
}

impl VoteAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &VoteAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            vote_address: call
                .vote_address
                .iter()
                .map(|vote| format_tron_address(vote.clone()))
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct VoteWitnessContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "votes.voteAddress")]
    pub votes_vote_address: Vec<String>,
    #[serde(rename = "votes.voteCount")]
    pub votes_vote_count: Vec<i64>,
    pub support: bool,
}

impl VoteWitnessContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &VoteWitnessContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            votes_vote_address: call
                .votes
                .iter()
                .map(|vote| format_tron_address(vote.vote_address.clone()))
                .collect(),
            votes_vote_count: call.votes.iter().map(|vote| vote.vote_count).collect(),
            support: call.support,
        }
    }
}

#[derive(Row, Clone, Debug, Default, Serialize, Deserialize)]
pub struct Vote {
    #[serde(rename = "voteAddress")]
    vote_address: String,
    #[serde(rename = "voteCount")]
    vote_count: i64,
}

impl Vote {
    pub fn from_grpc(vote: tron_grpc::Vote) -> Self {
        Self {
            vote_address: format_tron_address(vote.vote_address),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct WitnessCreateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub url: String,
}

impl WitnessCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &WitnessCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            url: String::from_utf8_lossy(&call.url).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct AssetIssueContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    pub id: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub name: String,
    pub abbr: String,
    #[serde(rename = "totalSupply")]
    pub total_supply: i64,
    #[serde(rename = "trxNum")]
    pub trx_num: i32,
    pub precision: i32,
    pub num: i32,
    #[serde(rename = "startTime")]
    pub start_time: i64,
    #[serde(rename = "endTime")]
    pub end_time: i64,
    pub order: i64, // useless but still added
    #[serde(rename = "voteScore")]
    pub vote_score: i32,
    pub description: String,
    pub url: String,
    #[serde(rename = "freeAssetNetLimit")]
    pub free_asset_net_limit: i64,
    #[serde(rename = "publicFreeAssetNetLimit")]
    pub public_free_asset_net_limit: i64,
    #[serde(rename = "publicFreeAssetNetUsage")]
    pub public_free_asset_net_usage: i64,
    #[serde(rename = "publicLatestFreeNetTime")]
    pub public_latest_free_net_time: i64,
}

impl AssetIssueContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &AssetIssueContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            id: call.id.clone(),
            owner_address: format_tron_address(call.owner_address.clone()),
            name: String::from_utf8_lossy(&call.name).to_string(),
            abbr: String::from_utf8_lossy(&call.abbr).to_string(),
            total_supply: call.total_supply,
            trx_num: call.trx_num,
            precision: call.precision,
            num: call.num,
            start_time: call.start_time,
            end_time: call.end_time,
            order: call.order,
            vote_score: call.vote_score,
            description: String::from_utf8_lossy(&call.description).to_string(),
            url: String::from_utf8_lossy(&call.url).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct WitnessUpdateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "updateUrl")]
    pub update_url: String,
}

impl WitnessUpdateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &WitnessUpdateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            update_url: String::from_utf8_lossy(&call.update_url).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ParticipateAssetIssueContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "toAddress")]
    pub to_address: String,
    #[serde(rename = "assetName")]
    pub asset_name: String,
    pub amount: i64,
}

impl ParticipateAssetIssueContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ParticipateAssetIssueContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            to_address: format_tron_address(call.to_address.clone()),
            asset_name: String::from_utf8_lossy(&call.asset_name).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct AccountUpdateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "accountName")]
    pub account_name: String,
}

impl AccountUpdateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &AccountUpdateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            account_name: String::from_utf8_lossy(&call.account_name).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct FreezeBalanceContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "frozenBalance")]
    pub frozen_balance: i64,
    #[serde(rename = "frozenDuration")]
    pub frozen_duration: i64,
    pub resource: i32,
    #[serde(rename = "receiverAddress")]
    pub receiver_address: String,
}
impl FreezeBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &FreezeBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            frozen_balance: call.frozen_balance,
            frozen_duration: call.frozen_duration,
            resource: call.resource,
            receiver_address: format_tron_address(call.receiver_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct UnfreezeBalanceContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub resource: i32,
    #[serde(rename = "receiverAddress")]
    pub receiver_address: String,
}

impl UnfreezeBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UnfreezeBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            resource: call.resource,
            receiver_address: format_tron_address(call.receiver_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct WithdrawBalanceContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
}

impl WithdrawBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &WithdrawBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct UnfreezeAssetContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
}

impl UnfreezeAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UnfreezeAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct UpdateAssetContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub description: String,
    pub url: String,
    #[serde(rename = "newLimit")]
    pub new_limit: i64,
    #[serde(rename = "newPublicLimit")]
    pub new_public_limit: i64,
}

impl UpdateAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            description: String::from_utf8_lossy(&call.description).to_string(),
            url: String::from_utf8_lossy(&call.url).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProposalCreateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub parameters: HashMap<i64, i64>,
}

impl ProposalCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ProposalCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProposalApproveContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "proposalId")]
    pub proposal_id: i64,
    #[serde(rename = "isAddApproval")]
    pub is_add_approval: bool,
}

impl ProposalApproveContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ProposalApproveContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProposalDeleteContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "proposalId")]
    pub proposal_id: i64,
}

impl ProposalDeleteContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ProposalDeleteContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct SetAccountIdContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
}

impl SetAccountIdContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &SetAccountIdContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            account_id: String::from_utf8_lossy(&call.account_id).to_string(),
            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct CreateSmartContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(flatten)]
    pub new_contract: SmartContractField,
    #[serde(rename = "callTokenValue")]
    pub call_token_value: i64,
    #[serde(rename = "tokenId")]
    pub token_id: i64,
}

#[derive(Row, Clone, Debug, Default, Serialize, Deserialize)]
pub struct SmartContractField {
    #[serde(rename = "originAddress")]
    pub origin_address: Option<String>,
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<String>,
    pub abi: Option<String>, // save as string
    pub bytecode: Option<String>,
    #[serde(rename = "callValue")]
    pub call_value: Option<i64>,
    #[serde(rename = "consumeUserResourcePercent")]
    pub consume_user_resource_percent: Option<i64>,
    pub name: Option<String>,
    #[serde(rename = "originEnergyLimit")]
    pub origin_energy_limit: Option<i64>,
    #[serde(rename = "codeHash")]
    pub code_hash: Option<String>,
    #[serde(rename = "trxHash")]
    pub trx_hash: Option<String>,
    pub version: Option<i32>,
}

impl CreateSmartContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &CreateSmartContract,
    ) -> Self {
        let new_contract = call.new_contract.clone();
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,
            owner_address: format_tron_address(call.owner_address.clone()),
            new_contract: SmartContractField {
                origin_address: new_contract.as_ref().map_or(None, |sc| {
                    Some(format_tron_address(sc.origin_address.clone()))
                }),
                contract_address: new_contract.as_ref().map_or(None, |sc| {
                    Some(format_tron_address(sc.contract_address.clone()))
                }),
                abi: new_contract.as_ref().map_or(None, |sc| {
                    sc.abi
                        .clone()
                        .map_or(None, |abi| Some(serde_json::to_string(&abi).unwrap()))
                }),
                bytecode: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(hex::encode(&sc.bytecode))),
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
                    .map_or(None, |sc| Some(hex::encode(&sc.code_hash))),
                trx_hash: new_contract
                    .as_ref()
                    .map_or(None, |sc| Some(hex::encode(&sc.trx_hash))),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct TriggerSmartContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "contractAddress")]
    pub contract_address: String,
    #[serde(rename = "callValue")]
    pub call_value: i64,
    pub data: String,
    #[serde(rename = "callTokenValue")]
    pub call_token_value: i64,
    #[serde(rename = "tokenId")]
    pub token_id: i64,
}

impl TriggerSmartContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &TriggerSmartContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            contract_address: format_tron_address(call.contract_address.clone()),
            call_value: call.call_value,
            data: hex::encode(&call.data),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct UpdateSettingContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "contractAddress")]
    pub contract_address: String,
    #[serde(rename = "consumeUserResourcePercent")]
    pub consume_user_resource_percent: i64,
}

impl UpdateSettingContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateSettingContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            contract_address: format_tron_address(call.contract_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExchangeCreateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "firstTokenId")]
    pub first_token_id: String,
    #[serde(rename = "firstTokenBalance")]
    pub first_token_balance: i64,
    #[serde(rename = "secondTokenId")]
    pub second_token_id: String,
    #[serde(rename = "secondTokenBalance")]
    pub second_token_balance: i64,
}

impl ExchangeCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            first_token_id: String::from_utf8_lossy(&call.first_token_id).to_string(),
            first_token_balance: call.first_token_balance,
            second_token_id: String::from_utf8_lossy(&call.second_token_id).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExchangeInjectContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "exchangeId")]
    pub exchange_id: i64,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    pub quant: i64,
}

impl ExchangeInjectContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeInjectContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            exchange_id: call.exchange_id,
            token_id: String::from_utf8_lossy(&call.token_id).to_string(),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExchangeWithdrawContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "exchangeId")]
    pub exchange_id: i64,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    pub quant: i64,
}

impl ExchangeWithdrawContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeWithdrawContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            exchange_id: call.exchange_id,
            token_id: hex::encode(&call.token_id),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExchangeTransactionContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "exchangeId")]
    pub exchange_id: i64,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    pub quant: i64,
    pub expected: i64,
}

impl ExchangeTransactionContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ExchangeTransactionContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            exchange_id: call.exchange_id,
            token_id: hex::encode(&call.token_id),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct UpdateEnergyLimitContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "contractAddress")]
    pub contract_address: String,
    #[serde(rename = "originEnergyLimit")]
    pub origin_energy_limit: i64,
}

impl UpdateEnergyLimitContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateEnergyLimitContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,

            owner_address: format_tron_address(call.owner_address.clone()),
            contract_address: format_tron_address(call.contract_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct AccountPermissionUpdateContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,

    #[serde(rename = "ownerPermissionType")]
    pub owner_permission_type: Option<i32>,
    #[serde(rename = "ownerPermissionId")]
    pub owner_permission_id: Option<i32>,
    #[serde(rename = "ownerPermissionName")]
    pub owner_permission_name: Option<String>,
    #[serde(rename = "ownerThreshold")]
    pub owner_threshold: Option<i64>,
    #[serde(rename = "ownerParentId")]
    pub owner_parent_id: Option<i32>,
    #[serde(rename = "ownerKeys")]
    pub owner_keys: HashMap<String, i64>,
    #[serde(rename = "ownerOperations")]
    pub owner_operations: String,

    #[serde(rename = "witnessPermissionType")]
    pub witness_permission_type: Option<i32>,
    #[serde(rename = "witnessPermissionId")]
    pub witness_permission_id: Option<i32>,
    #[serde(rename = "witnessPermissionName")]
    pub witness_permission_name: Option<String>,
    #[serde(rename = "witnessThreshold")]
    pub witness_threshold: Option<i64>,
    #[serde(rename = "witnessParentId")]
    pub witness_parent_id: Option<i32>,
    #[serde(rename = "witnessKeys")]
    pub witness_keys: HashMap<String, i64>,
    #[serde(rename = "witnessOperations")]
    pub witness_operations: String,

    #[serde(rename = "actives.permissionType")]
    pub actives_permission_type: Vec<i32>,
    #[serde(rename = "actives.permissionId")]
    pub actives_permission_id: Vec<i32>,
    #[serde(rename = "actives.permissionName")]
    pub actives_permission_name: Vec<String>,
    #[serde(rename = "actives.threshold")]
    pub actives_threshold: Vec<i64>,
    #[serde(rename = "actives.parentId")]
    pub actives_parent_id: Vec<i32>,
    #[serde(rename = "actives.keys")]
    pub actives_keys: Vec<HashMap<String, i64>>,
    #[serde(rename = "actives.operations")]
    pub actives_operations: Vec<String>,
}

impl AccountPermissionUpdateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
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
                            keys.insert(format_tron_address(key.address), key.weight);
                            keys
                        });
                (
                    Some(owner.r#type),
                    Some(owner.id),
                    Some(owner.permission_name.clone()),
                    Some(owner.threshold),
                    Some(owner.parent_id),
                    keys,
                    hex::encode(&owner.operations),
                )
            }
            None => (
                None,
                None,
                None,
                None,
                None,
                HashMap::default(),
                String::new(),
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
                            keys.insert(format_tron_address(key.address), key.weight);
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
                        hex::encode(&witness.operations),
                    )
                }
                None => (
                    None,
                    None,
                    None,
                    None,
                    None,
                    HashMap::default(),
                    String::new(),
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
                            keys.insert(format_tron_address(key.address), key.weight);
                            keys
                        },
                    );
                    actives_permission_type.push(active.r#type);
                    actives_permission_id.push(active.id);
                    actives_permission_name.push(active.permission_name.clone());
                    actives_threshold.push(active.threshold);
                    actives_parent_id.push(active.parent_id);
                    actives_keys.push(keys);
                    actives_operations.push(hex::encode(&active.operations));
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
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,
            owner_address: format_tron_address(call.owner_address.clone()),

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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClearAbiContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    #[serde(rename = "contractAddress")]
    pub contract_address: String,
}

impl ClearAbiContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &ClearAbiContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,
            owner_address: format_tron_address(call.owner_address.clone()),
            contract_address: format_tron_address(call.contract_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
pub struct UpdateBrokerageContractRow {
    #[serde(rename = "blockNum")]
    pub block_num: i64,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    pub transaction_index: i64,
    #[serde(rename = "contractIndex")]
    pub contract_index: i64,

    #[serde(rename = "ownerAddress")]
    pub owner_address: String,
    pub brokerage: i32,
}

impl UpdateBrokerageContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UpdateBrokerageContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: hex::encode(&transaction_hash),
            transaction_index,
            contract_index,
            owner_address: format_tron_address(call.owner_address.clone()),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShieldedTransferContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub transparent_from_address: String,
    pub from_amount: i64,
    #[serde(rename = "spendDescription.valueCommitment")]
    pub spend_description_value_commitment: Vec<String>,
    #[serde(rename = "spendDescription.anchor")]
    pub spend_description_anchor: Vec<String>,
    #[serde(rename = "spendDescription.nullifier")]
    pub spend_description_nullifier: Vec<String>,
    #[serde(rename = "spendDescription.rk")]
    pub spend_description_rk: Vec<String>,
    #[serde(rename = "spendDescription.zkproof")]
    pub spend_description_zkproof: Vec<String>,
    #[serde(rename = "spendDescription.authoritySignature")]
    pub spend_description_authority_signature: Vec<String>,

    #[serde(rename = "receiveDescription.valueCommitment")]
    pub receive_description_value_commitment: Vec<String>,
    #[serde(rename = "receiveDescription.noteCommitment")]
    pub receive_description_note_commitment: Vec<String>,
    #[serde(rename = "receiveDescription.epk")]
    pub receive_description_epk: Vec<String>,
    #[serde(rename = "receiveDescription.cEnc")]
    pub receive_description_c_enc: Vec<String>,
    #[serde(rename = "receiveDescription.cOut")]
    pub receive_description_c_out: Vec<String>,
    #[serde(rename = "receiveDescription.zkproof")]
    pub receive_description_zkproof: Vec<String>,

    pub binding_signature: String,
    pub transparent_to_address: String,
    pub to_amount: i64,
}

impl ShieldedTransferContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
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
                spend_description_value_commitment.push(utils::bytes_to_tron_format(
                    &spend_description.value_commitment,
                    false,
                ));
                spend_description_anchor.push(utils::bytes_to_tron_format(
                    &spend_description.anchor,
                    false,
                ));
                spend_description_nullifier.push(utils::bytes_to_tron_format(
                    &spend_description.nullifier,
                    false,
                ));
                spend_description_rk
                    .push(utils::bytes_to_tron_format(&spend_description.rk, false));
                spend_description_zkproof.push(utils::bytes_to_tron_format(
                    &spend_description.zkproof,
                    false,
                ));
                spend_description_authority_signature.push(utils::bytes_to_tron_format(
                    &spend_description.spend_authority_signature,
                    false,
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
                receive_description_value_commitment.push(utils::bytes_to_tron_format(
                    &receive_description.value_commitment,
                    false,
                ));
                receive_description_note_commitment.push(utils::bytes_to_tron_format(
                    &receive_description.note_commitment,
                    false,
                ));
                receive_description_epk
                    .push(utils::bytes_to_tron_format(&receive_description.epk, false));
                receive_description_c_enc.push(utils::bytes_to_tron_format(
                    &receive_description.c_enc,
                    false,
                ));
                receive_description_c_out.push(utils::bytes_to_tron_format(
                    &receive_description.c_out,
                    false,
                ));
                receive_description_zkproof.push(utils::bytes_to_tron_format(
                    &receive_description.zkproof,
                    false,
                ));
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
            transaction_hash,
            transaction_index,
            contract_index,
            transparent_from_address: utils::bytes_to_tron_format(
                &call.transparent_from_address.clone(),
                true,
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
            binding_signature: utils::bytes_to_tron_format(&call.binding_signature.clone(), false),
            transparent_to_address: utils::bytes_to_tron_format(
                &call.transparent_to_address.clone(),
                true,
            ),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSellAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub sell_token_id: String,
    pub sell_token_quantity: i64,
    pub buy_token_id: String,
    pub buy_token_quantity: i64,
}

impl MarketSellAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &MarketSellAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,
            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
            sell_token_id: utils::bytes_to_tron_format(&call.sell_token_id, false),
            sell_token_quantity: call.sell_token_quantity,
            buy_token_id: utils::bytes_to_tron_format(&call.buy_token_id, false),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketCancelOrderContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub order_id: String,
}

impl MarketCancelOrderContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &MarketCancelOrderContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,

            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
            order_id: utils::bytes_to_tron_format(&call.order_id, false),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FreezeBalanceV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub frozen_balance: i64,
    pub resource: i32,
}

impl FreezeBalanceV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &FreezeBalanceV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,

            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnfreezeBalanceV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub unfreeze_balance: i64,
    pub resource: i32,
}

impl UnfreezeBalanceV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UnfreezeBalanceV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,

            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawExpireUnfreezeContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
}

impl WithdrawExpireUnfreezeContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &WithdrawExpireUnfreezeContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,
            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelegateResourceContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub resource: i32,
    pub balance: i64,
    pub receiver_address: String,
    pub lock: bool,
    pub lock_period: i64,
}

impl DelegateResourceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &DelegateResourceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,
            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
            resource: call.resource,
            balance: call.balance,
            receiver_address: utils::bytes_to_tron_format(&call.receiver_address, true),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UndelegateResourceContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub resource: i32,
    pub balance: i64,
    pub receiver_address: String,
}

impl UndelegateResourceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &UnDelegateResourceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,
            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
            resource: call.resource,
            balance: call.balance,
            receiver_address: utils::bytes_to_tron_format(&call.receiver_address, true),
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
#[derive(Row, Documented, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelAllUnfreezeV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
}

impl CancelAllUnfreezeV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: String,
        transaction_index: i64,
        contract_index: i64,
        call: &CancelAllUnfreezeV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash,
            transaction_index,
            contract_index,
            owner_address: utils::bytes_to_tron_format(&call.owner_address, true),
        }
    }
}
