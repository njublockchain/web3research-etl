use klickhouse::{u256, Bytes, Row};
use serde_variant::to_variant_name;
use tron_grpc::{
    transaction_info::Log, AccountCreateContract, BlockExtention, FreezeBalanceContract,
    FreezeBalanceV2Contract, InternalTransaction, MarketOrderDetail, TransactionExtention,
    TransactionInfo, TransferAssetContract, TransferContract, TriggerSmartContract,
    UnfreezeBalanceContract, UnfreezeBalanceV2Contract, VoteWitnessContract,
};

pub fn t_addr_from_21(len_21_addr: Vec<u8>) -> String {
    if len_21_addr.len() == 0 {
        return "".to_owned();
    }

    let len_21_addr = if len_21_addr.len() > 21 {
        len_21_addr[0..21].to_vec()
    } else {
        len_21_addr
    };
    assert!(
        len_21_addr.len() == 20 || len_21_addr.len() == 21 || len_21_addr.len() > 21,
        "{:X?}",
        len_21_addr
    );

    if len_21_addr.len() == 20 {
        let mut addr = [0x41].to_vec();
        addr.extend(len_21_addr);

        bs58::encode(addr).with_check().into_string()
    } else {
        let t_addr = bs58::encode(&len_21_addr).with_check().into_string();
        // assert!(t_addr.starts_with("T"), "{}", t_addr);

        t_addr
    }
}

pub fn t_addr_from_32(len_32_addr: Vec<u8>) -> String {
    if len_32_addr.len() == 0 {
        return "".to_owned();
    }

    assert!(len_32_addr.len() == 32);

    let base = len_32_addr
        .strip_prefix(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        .unwrap()
        .to_vec();
    let mut addr = [0x41].to_vec();
    addr.extend(base);

    let t_addr = bs58::encode(addr).with_check().into_string();
    assert!(t_addr.starts_with("T"));
    t_addr
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct BlockRow {
    pub hash: Bytes,
    pub timestamp: i64,
    pub tx_trie_root: Bytes,
    pub parent_hash: Bytes,
    pub number: i64,
    pub witness_id: i64,
    pub witness_address: String,
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
            witness_address: t_addr_from_21(header_raw_data.witness_address),
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
    pub contract_address: String,

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
            contract_address: transaction_info.map_or("".to_owned(), |transaction_info| {
                t_addr_from_21(transaction_info.contract_address.clone())
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
    pub address: String,
    
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
            address: t_addr_from_21(log.address.clone()),
            topic0: topics.get(0).cloned(),
            topic1: topics.get(1).cloned(),
            topic2: topics.get(2).cloned(),
            topic3: topics.get(3).cloned(),
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
    pub caller_address: String,
    pub transfer_to_address: String,

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
            caller_address: t_addr_from_21(internal.caller_address.clone()),
            transfer_to_address: t_addr_from_21(internal.transfer_to_address.clone()),
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

/////////////////////////////////////////////////////////////////

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccountCreateContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub account_address: String,
    pub r#type: i32,
}

impl AccountCreateContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: AccountCreateContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            account_address: t_addr_from_21(call.account_address),
            r#type: call.r#type,
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransferContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub to_address: String,
    pub amount: i64,
}

impl TransferContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: TransferContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            to_address: t_addr_from_21(call.to_address),
            amount: call.amount,
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TransferAssetContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub asset_name: Bytes,
    pub owner_address: String,
    pub to_address: String,
    pub amount: i64,
}

impl TransferAssetContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: TransferAssetContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            asset_name: klickhouse::Bytes(call.asset_name),
            owner_address: t_addr_from_21(call.owner_address),
            to_address: t_addr_from_21(call.to_address),
            amount: call.amount,
        }
    }
}

// #[derive(Row, Clone, Debug, Default)]
// #[klickhouse(rename_all = "camelCase")]
// pub struct VoteWitnessContractRow {
//     pub block_num: i64,
//     pub transaction_hash: Bytes,
//     pub transaction_index: i64,

//     pub owner_address: Bytes,
//     pub votes: ::prost::alloc::vec::Vec<vote_witness_contract::Vote>,
//     pub support: bool,
// }

// impl VoteWitnessContractRow {
//     pub fn from_grpc(
//         block_num: i64,
//         transaction_hash: Vec<u8>,
//         transaction_index: i64,
//         call: VoteWitnessContract,
//     ) -> Self {
//         Self {
//             block_num,
//             transaction_hash: klickhouse::Bytes(transaction_hash),
//             transaction_index,

//         }
//     }
// }

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WitnessCreateContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AssetIssueContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WitnessUpdateContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ParticipateAssetIssueContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccountUpdateContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct FreezeBalanceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub frozen_balance: i64,
    pub frozen_duration: i64,
    pub resource: i32,
    pub receiver_address: String,
}
impl FreezeBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: FreezeBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            frozen_balance: call.frozen_balance,
            frozen_duration: call.frozen_duration,
            resource: call.resource,
            receiver_address: t_addr_from_21(call.receiver_address),
        }
    }
}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnfreezeBalanceContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub resource: i32,
    pub receiver_address: String,
}
impl UnfreezeBalanceContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: UnfreezeBalanceContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            resource: call.resource,
            receiver_address: t_addr_from_21(call.receiver_address),
        }
    }
}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WithdrawBalanceContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnfreezeAssetContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateAssetContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ProposalCreateContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ProposalApproveContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ProposalDeleteContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct SetAccountIdContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct CreateSmartContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct TriggerSmartContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub contract_address: String,
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
        call: TriggerSmartContract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            contract_address: t_addr_from_21(call.contract_address),
            call_value: call.call_value,
            data: klickhouse::Bytes(call.data),
            call_token_value: call.call_token_value,
            token_id: call.token_id,
        }
    }
}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateSettingContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeCreateContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeInjectContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeWithdrawContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ExchangeTransactionContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateEnergyLimitContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct AccountPermissionUpdateContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ClearAbiContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UpdateBrokerageContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct ShieldedTransferContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct MarketSellAssetContract {}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct MarketCancelOrderContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct FreezeBalanceV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub frozen_balance: i64,
    pub resource: i32,
}
impl FreezeBalanceV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: FreezeBalanceV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            frozen_balance: call.frozen_balance,
            resource: call.resource,
        }
    }
}
#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnfreezeBalanceV2ContractRow {
    pub block_num: i64,
    pub transaction_hash: Bytes,
    pub transaction_index: i64,
    pub contract_index: i64,

    pub owner_address: String,
    pub unfreeze_balance: i64,
    pub resource: i32,
}
impl UnfreezeBalanceV2ContractRow {
    pub fn from_grpc(
        block_num: i64,
        transaction_hash: Vec<u8>,
        transaction_index: i64,
        contract_index: i64,
        call: UnfreezeBalanceV2Contract,
    ) -> Self {
        Self {
            block_num,
            transaction_hash: klickhouse::Bytes(transaction_hash),
            transaction_index,
            contract_index,

            owner_address: t_addr_from_21(call.owner_address),
            unfreeze_balance: call.unfreeze_balance,
            resource: call.resource,
        }
    }
}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct WithdrawExpireUnfreezeContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct DelegateResourceContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct UnDelegateResourceContract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct CancelAllUnfreezeV2Contract {}

#[derive(Row, Clone, Debug, Default)]
#[klickhouse(rename_all = "camelCase")]
pub struct CreateSmartContractRow {}
