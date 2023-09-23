use klickhouse::{u256, Bytes, Row};
use serde_variant::to_variant_name;
use tron_grpc::{
    transaction_info::Log, BlockExtention, InternalTransaction, MarketOrderDetail,
    TransactionExtention, TransactionInfo,
};

#[derive(Row, Clone, Debug, Default)]
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
            witness_address: klickhouse::Bytes(header_raw_data.witness_address),
            version: header_raw_data.version,
            account_state_root: klickhouse::Bytes(header_raw_data.account_state_root),
            witness_signature: klickhouse::Bytes(header.witness_signature),
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
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
    pub contract_result: Bytes,
    pub contract_address: Bytes,

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
    pub packing_fee: i64,
    pub withdraw_expire_amount: i64,
    // pub cancel_unfreeze_v2_amount: std::collections::HashMap<String, i64>,
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
            contract_result: transaction_info.map_or(Bytes::default(), |transaction_info| {
                Bytes(transaction_info.contract_result[0].clone())
            }),
            contract_address: transaction_info.map_or(Bytes::default(), |transaction_info| {
                Bytes(transaction_info.contract_address.clone())
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
            packing_fee: transaction_info
                .map_or(0, |transaction_info| transaction_info.packing_fee),
            withdraw_expire_amount: transaction_info.map_or(0, |transaction_info| {
                transaction_info.withdraw_expire_amount
            }),
            // cancel_unfreeze_v2_amount: transaction_info.cancel_unfreeze_v2_amount,
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct LogRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub log_index: i32,
    pub address: Bytes,
    pub topics: Vec<Bytes>,
    pub data: Bytes,
}

impl LogRow {
    pub fn from_grpc(block_num: i64, transaction_hash: Vec<u8>, log_index: i32, log: &Log) -> Self {
        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            log_index,
            address: Bytes(log.address.clone()),
            topics: log
                .topics
                .iter()
                .map(|topic| Bytes(topic.to_vec()))
                .collect(),
            data: Bytes(log.data.clone()),
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct InternalTransactionRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub internal_index: i32,

    pub hash: Bytes,
    pub caller_address: Bytes,
    pub transfer_to_address: Bytes,

    #[klickhouse(rename = "callValueInfos.tokenId")]
    pub call_value_infos_token_id: Vec<String>, // _token_ids,call_values
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
        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            internal_index,

            hash: Bytes(internal.hash.clone()),
            caller_address: Bytes(internal.caller_address.clone()),
            transfer_to_address: Bytes(internal.transfer_to_address.clone()),
            call_value_infos_token_id: internal
                .call_value_info
                .iter()
                .map(|call_value_info| call_value_info.token_id.clone())
                .collect(),
            call_value_infos_call_value: internal
                .call_value_info
                .iter()
                .map(|call_value_info| call_value_info.call_value)
                .collect(),
            note: Bytes(internal.note.clone()),
            rejected: internal.rejected,
            extra: internal.extra.clone(),
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct MarketOrderDetailRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub order_index: i32,

    pub order_id: Bytes,
    pub maker_order_id: Bytes,
    pub taker_order_id: Bytes,
    pub fill_sell_quantity: i64,
    pub fill_buy_quantity: i64,
}

impl MarketOrderDetailRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        order_index: i32,
        order_id: Vec<u8>,
        order: &MarketOrderDetail,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: Bytes(transaction_hash),
            order_index,
            order_id: Bytes(order_id),
            maker_order_id: Bytes(order.maker_order_id.clone()),
            taker_order_id: Bytes(order.taker_order_id.clone()),
            fill_sell_quantity: order.fill_sell_quantity,
            fill_buy_quantity: order.fill_buy_quantity,
        }
    }
}

// #[derive(Row, Clone, Debug, Default)]
// #[klickhouse(rename_all = "camelCase")]
// pub struct AccountCreateContractRow {
//     pub owner_address: Bytes,
//     pub account_address: Bytes,
//     pub r#type: String,
// }

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransferContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,

    pub owner_address: Bytes,
    pub to_address: Bytes,
    pub amount: i64,
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransferAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,

    pub asset_name: Vec<u8>,
    pub owner_address: Vec<u8>,
    pub to_address: Vec<u8>,
    pub amount: i64,
}

// #[derive(Row, Clone, Debug, Default)]
// #[klickhouse(rename_all = "camelCase")]
// pub struct AssetIssueContractRow {

// }

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct CreateSmartContractRow {}
