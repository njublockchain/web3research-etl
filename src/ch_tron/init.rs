use std::error::Error;

use documented::Documented;
use log::{debug, info, warn};
use tron_grpc::{
    AccountCreateContract, AccountPermissionUpdateContract, AccountUpdateContract,
    AssetIssueContract, CancelAllUnfreezeV2Contract, ClearAbiContract, CreateSmartContract,
    DelegateResourceContract, EmptyMessage, ExchangeCreateContract, ExchangeInjectContract,
    ExchangeTransactionContract, ExchangeWithdrawContract, FreezeBalanceContract,
    FreezeBalanceV2Contract, MarketCancelOrderContract, MarketSellAssetContract, NumberMessage,
    ParticipateAssetIssueContract, ProposalApproveContract, ProposalCreateContract,
    ProposalDeleteContract, SetAccountIdContract, ShieldedTransferContract, TransferAssetContract,
    TransferContract, TriggerSmartContract, UnDelegateResourceContract, UnfreezeAssetContract,
    UnfreezeBalanceContract, UnfreezeBalanceV2Contract, UpdateAssetContract,
    UpdateBrokerageContract, UpdateEnergyLimitContract, UpdateSettingContract, VoteAssetContract,
    VoteWitnessContract, WithdrawBalanceContract, WithdrawExpireUnfreezeContract,
    WitnessCreateContract, WitnessUpdateContract,
};
use url::Url;

use crate::ch_tron::{
    schema::{
        AccountCreateContractRow, AccountPermissionUpdateContractRow, AccountUpdateContractRow,
        AssetIssueContractRow, BlockRow, CancelAllUnfreezeV2ContractRow, ClearAbiContractRow,
        CreateSmartContractRow, DelegateResourceContractRow, ExchangeCreateContractRow,
        ExchangeInjectContractRow, ExchangeTransactionContractRow, ExchangeWithdrawContractRow,
        FreezeBalanceContractRow, FreezeBalanceV2ContractRow, InternalTransactionRow, LogRow,
        MarketCancelOrderContractRow, MarketSellAssetContractRow, ParticipateAssetIssueContractRow,
        ProposalApproveContractRow, ProposalCreateContractRow, ProposalDeleteContractRow,
        SetAccountIdContractRow, ShieldedTransferContractRow, TransactionRow,
        TransferAssetContractRow, TransferContractRow, TriggerSmartContractRow,
        UndelegateResourceContractRow, UnfreezeAssetContractRow, UnfreezeBalanceContractRow,
        UnfreezeBalanceV2ContractRow, UpdateAssetContractRow, UpdateBrokerageContractRow,
        UpdateEnergyLimitContractRow, UpdateSettingContractRow, VoteAssetContractRow,
        VoteWitnessContractRow, WithdrawBalanceContractRow, WithdrawExpireUnfreezeContractRow,
        WitnessCreateContractRow, WitnessUpdateContractRow,
    },
    utils,
};

pub(crate) async fn init(
    db: String,
    provider: String,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    // Create a client with the given database
    let client = clickhouse::Client::default().with_url(&db).with_database(
        Url::parse(&db)
            .unwrap()
            .path()
            .strip_prefix('/')
            .unwrap_or("default"),
    );

    debug!("start initializing schema");

    // init all basics
    client.query(BlockRow::DOCS).execute().await?;
    client.query(TransactionRow::DOCS).execute().await?;
    client.query(LogRow::DOCS).execute().await?;
    client.query(InternalTransactionRow::DOCS).execute().await?;

    // init all contracts
    client
        .query(AccountCreateContractRow::DOCS)
        .execute()
        .await?;
    client.query(TransferContractRow::DOCS).execute().await?;
    client
        .query(TransferAssetContractRow::DOCS)
        .execute()
        .await?;
    client.query(VoteAssetContractRow::DOCS).execute().await?;
    client.query(VoteWitnessContractRow::DOCS).execute().await?;
    client
        .query(WitnessCreateContractRow::DOCS)
        .execute()
        .await?;
    client.query(AssetIssueContractRow::DOCS).execute().await?;
    client
        .query(WitnessUpdateContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ParticipateAssetIssueContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(AccountUpdateContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(FreezeBalanceContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(UnfreezeBalanceContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(WithdrawBalanceContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(UnfreezeAssetContractRow::DOCS)
        .execute()
        .await?;
    client.query(UpdateAssetContractRow::DOCS).execute().await?;
    client
        .query(ProposalCreateContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ProposalApproveContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ProposalDeleteContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(SetAccountIdContractRow::DOCS)
        .execute()
        .await?;
    client.query(CreateSmartContractRow::DOCS).execute().await?;
    client
        .query(TriggerSmartContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(UpdateSettingContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ExchangeCreateContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ExchangeInjectContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ExchangeWithdrawContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ExchangeTransactionContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(UpdateEnergyLimitContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(AccountPermissionUpdateContractRow::DOCS)
        .execute()
        .await?;
    client.query(ClearAbiContractRow::DOCS).execute().await?;
    client
        .query(UpdateBrokerageContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(ShieldedTransferContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(MarketSellAssetContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(MarketCancelOrderContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(FreezeBalanceV2ContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(UnfreezeBalanceV2ContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(WithdrawExpireUnfreezeContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(DelegateResourceContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(UndelegateResourceContractRow::DOCS)
        .execute()
        .await?;
    client
        .query(CancelAllUnfreezeV2ContractRow::DOCS)
        .execute()
        .await?;

    let mut grpc_client = tron_grpc::wallet_client::WalletClient::connect(provider).await?;

    let now = grpc_client.get_now_block2(EmptyMessage {}).await?;
    let to = now
        .into_inner()
        .block_header
        .unwrap()
        .raw_data
        .unwrap()
        .number
        / 1000
        * 1000;
    warn!("target: {}", to);

    let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut internal_row_list = Vec::new();

    // all native contracts from
    // https://github.com/tronprotocol/protocol/blob/2a678934da3992b1a67f975769bbb2d31989451f/core/Tron.proto#L338
    // Transaction.Contract.ContractType:
    //   AccountCreateContract = 0;
    //   TransferContract = 1;
    //   TransferAssetContract = 2;
    //   VoteAssetContract = 3;
    //   VoteWitnessContract = 4;
    //   WitnessCreateContract = 5;
    //   AssetIssueContract = 6;
    //   WitnessUpdateContract = 8;
    //   ParticipateAssetIssueContract = 9;
    //   AccountUpdateContract = 10;
    //   FreezeBalanceContract = 11;
    //   UnfreezeBalanceContract = 12;
    //   WithdrawBalanceContract = 13;
    //   UnfreezeAssetContract = 14;
    //   UpdateAssetContract = 15;
    //   ProposalCreateContract = 16;
    //   ProposalApproveContract = 17;
    //   ProposalDeleteContract = 18;
    //   SetAccountIdContract = 19;
    //   CustomContract = 20;
    //   CreateSmartContract = 30;
    //   TriggerSmartContract = 31;
    //   GetContract = 32;
    //   UpdateSettingContract = 33;
    //   ExchangeCreateContract = 41;
    //   ExchangeInjectContract = 42;
    //   ExchangeWithdrawContract = 43;
    //   ExchangeTransactionContract = 44;
    //   UpdateEnergyLimitContract = 45;
    //   AccountPermissionUpdateContract = 46;
    //   ClearABIContract = 48;
    //   UpdateBrokerageContract = 49;
    //   ShieldedTransferContract = 51;
    //   MarketSellAssetContract = 52;
    //   MarketCancelOrderContract = 53;
    //   FreezeBalanceV2Contract = 54;
    //   UnfreezeBalanceV2Contract = 55;
    //   WithdrawExpireUnfreezeContract = 56;
    //   DelegateResourceContract = 57;
    //   UnDelegateResourceContract = 58;
    //   CancelAllUnfreezeV2Contract = 59;
    let mut account_create_contract_row_list = Vec::new();
    let mut transfer_contract_row_list = Vec::new();
    let mut transfer_asset_contract_row_list = Vec::new();
    let mut vote_asset_contract_row_list = Vec::new();
    let mut vote_witness_contract_row_list = Vec::new();
    let mut witness_create_contract_row_list = Vec::new();
    let mut asset_issue_contract_row_list = Vec::new();
    let mut witness_update_contract_row_list = Vec::new();
    let mut participate_asset_issue_contract_row_list = Vec::new();
    let mut account_update_contract_row_list = Vec::new();
    let mut freeze_balance_contract_row_list = Vec::new();
    let mut unfreeze_balance_contract_row_list = Vec::new();
    let mut withdraw_balance_contract_row_list = Vec::new();
    let mut unfreeze_asset_contract_row_list = Vec::new();
    let mut update_asset_contract_row_list = Vec::new();
    let mut proposal_create_contract_row_list = Vec::new();
    let mut proposal_approve_contract_row_list = Vec::new();
    let mut proposal_delete_contract_row_list = Vec::new();
    let mut set_account_id_contract_row_list = Vec::new();
    // let mut custom_contract_row_list = Vec::new();//TODO
    let mut create_smart_contract_row_list = Vec::new();
    let mut trigger_smart_contract_row_list = Vec::new();
    // let mut get_contract_row_list = Vec::new();//TODO
    let mut update_setting_contract_row_list = Vec::new();
    let mut exchange_create_contract_row_list = Vec::new();
    let mut exchange_inject_contract_row_list = Vec::new();
    let mut exchange_withdraw_contract_row_list = Vec::new();
    let mut exchange_transaction_contract_row_list = Vec::new();
    let mut update_energy_limit_contract_row_list = Vec::new();
    let mut account_permission_update_contract_row_list = Vec::new();
    let mut clear_abi_contract_row_list = Vec::new();
    let mut update_brokerage_contract_row_list = Vec::new();
    let mut shielded_transfer_contract_row_list = Vec::new();
    let mut market_sell_asset_contract_row_list = Vec::new();
    let mut market_cancel_order_contract_row_list = Vec::new();
    let mut freeze_balance_v2_contract_row_list = Vec::new();
    let mut unfreeze_balance_v2_contract_row_list = Vec::new();
    let mut withdraw_expire_unfreeze_contract_row_list = Vec::new();
    let mut delegate_resource_contract_row_list = Vec::new();
    let mut undelegate_resource_contract_row_list = Vec::new();
    let mut cancel_all_unfreeze_v2_contract_row_list = Vec::new();

    let from = from as i64;
    let batch = batch as i64;

    let mut grpc_client_clone = grpc_client.clone();

    for num in from..=to {
        // let cli = client.get_jsonrpc_client();
        let (block, tx_infos) = tokio::try_join!(
            grpc_client.get_block_by_num2(NumberMessage { num }),
            grpc_client_clone.get_transaction_info_by_block_num(NumberMessage { num })
        )
        .unwrap();
        let block = block.into_inner();
        let tx_infos = tx_infos.into_inner().transaction_info;

        let block_row = BlockRow::from_grpc(&block);
        block_row_list.push(block_row);

        for (index, transaction) in block.transactions.iter().enumerate() {
            let transaction_hash = utils::bytes_to_tron_format(&transaction.txid, false);

            // handle genesis
            let transaction_row = if num == 0 {
                TransactionRow::from_grpc(&block, index as i64, transaction, None)
            } else {
                assert!(tx_infos[index].id == transaction.txid);
                let transaction_row = TransactionRow::from_grpc(
                    &block,
                    index as i64,
                    transaction,
                    Some(&tx_infos[index]),
                );
                for (index, log) in tx_infos[index].log.iter().enumerate() {
                    let log_row =
                        LogRow::from_grpc(num, transaction_hash.clone(), index as i32, log);
                    event_row_list.push(log_row);
                }

                for (index, internal) in tx_infos[index].internal_transactions.iter().enumerate() {
                    let internal_row = InternalTransactionRow::from_grpc(
                        num,
                        transaction_hash.clone(),
                        index as i32,
                        internal,
                    );
                    internal_row_list.push(internal_row);
                }

                // if transaction.clone().transaction.unwrap().raw_data.unwrap().contract[0].parameter.unwrap().type_url

                transaction_row
            };

            // start handling parameters
            let parameter = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap();

            let mut parameter_parsed = false;

            if let Ok(msg) = parameter.to_msg::<AccountCreateContract>() {
                parameter_parsed = true;
                let row = AccountCreateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                account_create_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<TransferContract>() {
                parameter_parsed = true;
                let row = TransferContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                transfer_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<TransferAssetContract>() {
                parameter_parsed = true;
                let row = TransferAssetContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                transfer_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<VoteAssetContract>() {
                parameter_parsed = true;
                let row = VoteAssetContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                vote_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<VoteWitnessContract>() {
                parameter_parsed = true;
                let row = VoteWitnessContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                vote_witness_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WitnessCreateContract>() {
                parameter_parsed = true;
                let row = WitnessCreateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                witness_create_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<AssetIssueContract>() {
                parameter_parsed = true;
                let row = AssetIssueContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                asset_issue_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WitnessUpdateContract>() {
                parameter_parsed = true;
                let row = WitnessUpdateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                witness_update_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ParticipateAssetIssueContract>() {
                parameter_parsed = true;
                let row = ParticipateAssetIssueContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                participate_asset_issue_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<AccountUpdateContract>() {
                parameter_parsed = true;
                let row = AccountUpdateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                account_update_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<FreezeBalanceContract>() {
                parameter_parsed = true;
                let row = FreezeBalanceContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                freeze_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnfreezeBalanceContract>() {
                parameter_parsed = true;
                let row = UnfreezeBalanceContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                unfreeze_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WithdrawBalanceContract>() {
                parameter_parsed = true;
                let row = WithdrawBalanceContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                withdraw_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnfreezeAssetContract>() {
                parameter_parsed = true;
                let row = UnfreezeAssetContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                unfreeze_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateAssetContract>() {
                parameter_parsed = true;
                let row = UpdateAssetContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ProposalCreateContract>() {
                parameter_parsed = true;
                let row = ProposalCreateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                proposal_create_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ProposalApproveContract>() {
                parameter_parsed = true;
                let row = ProposalApproveContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                proposal_approve_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ProposalDeleteContract>() {
                parameter_parsed = true;
                let row = ProposalDeleteContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                proposal_delete_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<SetAccountIdContract>() {
                parameter_parsed = true;
                let row = SetAccountIdContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                set_account_id_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<CreateSmartContract>() {
                parameter_parsed = true;
                let row = CreateSmartContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                create_smart_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<TriggerSmartContract>() {
                parameter_parsed = true;
                let row = TriggerSmartContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                trigger_smart_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateSettingContract>() {
                parameter_parsed = true;
                let row = UpdateSettingContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_setting_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeCreateContract>() {
                parameter_parsed = true;
                let row = ExchangeCreateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_create_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeInjectContract>() {
                parameter_parsed = true;
                let row = ExchangeInjectContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_inject_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeWithdrawContract>() {
                parameter_parsed = true;
                let row = ExchangeWithdrawContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_withdraw_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeTransactionContract>() {
                parameter_parsed = true;
                let row = ExchangeTransactionContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_transaction_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateEnergyLimitContract>() {
                parameter_parsed = true;
                let row = UpdateEnergyLimitContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_energy_limit_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<AccountPermissionUpdateContract>() {
                parameter_parsed = true;
                let row = AccountPermissionUpdateContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                account_permission_update_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ClearAbiContract>() {
                parameter_parsed = true;
                let row = ClearAbiContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                clear_abi_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateBrokerageContract>() {
                parameter_parsed = true;
                let row = UpdateBrokerageContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_brokerage_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ShieldedTransferContract>() {
                parameter_parsed = true;
                let row = ShieldedTransferContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                shielded_transfer_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<MarketSellAssetContract>() {
                parameter_parsed = true;
                let row = MarketSellAssetContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                market_sell_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<MarketCancelOrderContract>() {
                parameter_parsed = true;
                let row = MarketCancelOrderContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                market_cancel_order_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<FreezeBalanceV2Contract>() {
                parameter_parsed = true;
                let row = FreezeBalanceV2ContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                freeze_balance_v2_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnfreezeBalanceV2Contract>() {
                parameter_parsed = true;
                let row = UnfreezeBalanceV2ContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                unfreeze_balance_v2_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WithdrawExpireUnfreezeContract>() {
                parameter_parsed = true;
                let row = WithdrawExpireUnfreezeContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                withdraw_expire_unfreeze_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<DelegateResourceContract>() {
                parameter_parsed = true;
                let row = DelegateResourceContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                delegate_resource_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnDelegateResourceContract>() {
                parameter_parsed = true;
                let row = UndelegateResourceContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                undelegate_resource_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<CancelAllUnfreezeV2Contract>() {
                parameter_parsed = true;
                let row = CancelAllUnfreezeV2ContractRow::from_grpc(
                    num,
                    transaction_hash.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                cancel_all_unfreeze_v2_contract_row_list.push(row);
            }

            //TODO: add CustomContract and GetContract (useless)
            if !parameter_parsed {
                warn!(
                    "unknown contract type: {:?} {:X?}",
                    parameter.type_url, transaction.txid
                );
            }

            transaction_row_list.push(transaction_row);
        }

        if (num - from + 1) % batch == 0 || num == to {
            // Insert operations in the same order as list declarations
            if !transaction_row_list.is_empty() {
                let mut inserter = client.insert("transactions")?;
                for row in &transaction_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !event_row_list.is_empty() {
                let mut inserter = client.insert("events")?;
                for row in &event_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !internal_row_list.is_empty() {
                let mut inserter = client.insert("internal_transactions")?;
                for row in &internal_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !account_create_contract_row_list.is_empty() {
                let mut inserter = client.insert("accountCreateContracts")?;
                for row in &account_create_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !transfer_contract_row_list.is_empty() {
                let mut inserter = client.insert("transferContracts")?;
                for row in &transfer_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !transfer_asset_contract_row_list.is_empty() {
                let mut inserter = client.insert("transferAssetContracts")?;
                for row in &transfer_asset_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !vote_asset_contract_row_list.is_empty() {
                let mut inserter = client.insert("voteAssetContracts")?;
                for row in &vote_asset_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !vote_witness_contract_row_list.is_empty() {
                let mut inserter = client.insert("voteWitnessContracts")?;
                for row in &vote_witness_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !witness_create_contract_row_list.is_empty() {
                let mut inserter = client.insert("witnessCreateContracts")?;
                for row in &witness_create_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !asset_issue_contract_row_list.is_empty() {
                let mut inserter = client.insert("assetIssueContracts")?;
                for row in &asset_issue_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !witness_update_contract_row_list.is_empty() {
                let mut inserter = client.insert("witnessUpdateContracts")?;
                for row in &witness_update_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !participate_asset_issue_contract_row_list.is_empty() {
                let mut inserter = client.insert("participateAssetIssueContracts")?;
                for row in &participate_asset_issue_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !account_update_contract_row_list.is_empty() {
                let mut inserter = client.insert("accountUpdateContracts")?;
                for row in &account_update_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !freeze_balance_contract_row_list.is_empty() {
                let mut inserter = client.insert("freezeBalanceContracts")?;
                for row in &freeze_balance_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !unfreeze_balance_contract_row_list.is_empty() {
                let mut inserter = client.insert("unfreezeBalanceContracts")?;
                for row in &unfreeze_balance_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !withdraw_balance_contract_row_list.is_empty() {
                let mut inserter = client.insert("withdrawBalanceContracts")?;
                for row in &withdraw_balance_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !unfreeze_asset_contract_row_list.is_empty() {
                let mut inserter = client.insert("unfreezeAssetContracts")?;
                for row in &unfreeze_asset_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !update_asset_contract_row_list.is_empty() {
                let mut inserter = client.insert("updateAssetContracts")?;
                for row in &update_asset_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !proposal_create_contract_row_list.is_empty() {
                let mut inserter = client.insert("proposalCreateContracts")?;
                for row in &proposal_create_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !proposal_approve_contract_row_list.is_empty() {
                let mut inserter = client.insert("proposalApproveContracts")?;
                for row in &proposal_approve_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !proposal_delete_contract_row_list.is_empty() {
                let mut inserter = client.insert("proposalDeleteContracts")?;
                for row in &proposal_delete_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !set_account_id_contract_row_list.is_empty() {
                let mut inserter = client.insert("setAccountIdContracts")?;
                for row in &set_account_id_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !create_smart_contract_row_list.is_empty() {
                let mut inserter = client.insert("createSmartContracts")?;
                for row in &create_smart_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !trigger_smart_contract_row_list.is_empty() {
                let mut inserter = client.insert("triggerSmartContracts")?;
                for row in &trigger_smart_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !update_setting_contract_row_list.is_empty() {
                let mut inserter = client.insert("updateSettingContracts")?;
                for row in &update_setting_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !exchange_create_contract_row_list.is_empty() {
                let mut inserter = client.insert("exchangeCreateContracts")?;
                for row in &exchange_create_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !exchange_inject_contract_row_list.is_empty() {
                let mut inserter = client.insert("exchangeInjectContracts")?;
                for row in &exchange_inject_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !exchange_withdraw_contract_row_list.is_empty() {
                let mut inserter = client.insert("exchangeWithdrawContracts")?;
                for row in &exchange_withdraw_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !exchange_transaction_contract_row_list.is_empty() {
                let mut inserter = client.insert("exchangeTransactionContracts")?;
                for row in &exchange_transaction_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !update_energy_limit_contract_row_list.is_empty() {
                let mut inserter = client.insert("updateEnergyLimitContracts")?;
                for row in &update_energy_limit_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !account_permission_update_contract_row_list.is_empty() {
                let mut inserter = client.insert("accountPermissionUpdateContracts")?;
                for row in &account_permission_update_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !clear_abi_contract_row_list.is_empty() {
                let mut inserter = client.insert("clearAbiContracts")?;
                for row in &clear_abi_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !update_brokerage_contract_row_list.is_empty() {
                let mut inserter = client.insert("updateBrokerageContracts")?;
                for row in &update_brokerage_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !shielded_transfer_contract_row_list.is_empty() {
                let mut inserter = client.insert("shieldedTransferContracts")?;
                for row in &shielded_transfer_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !market_sell_asset_contract_row_list.is_empty() {
                let mut inserter = client.insert("marketSellAssetContracts")?;
                for row in &market_sell_asset_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !market_cancel_order_contract_row_list.is_empty() {
                let mut inserter = client.insert("marketCancelOrderContracts")?;
                for row in &market_cancel_order_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !freeze_balance_v2_contract_row_list.is_empty() {
                let mut inserter = client.insert("freezeBalanceV2Contracts")?;
                for row in &freeze_balance_v2_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !unfreeze_balance_v2_contract_row_list.is_empty() {
                let mut inserter = client.insert("unfreezeBalanceV2Contracts")?;
                for row in &unfreeze_balance_v2_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !withdraw_expire_unfreeze_contract_row_list.is_empty() {
                let mut inserter = client.insert("withdrawExpireUnfreezeContracts")?;
                for row in &withdraw_expire_unfreeze_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !delegate_resource_contract_row_list.is_empty() {
                let mut inserter = client.insert("delegateResourceContracts")?;
                for row in &delegate_resource_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !undelegate_resource_contract_row_list.is_empty() {
                let mut inserter = client.insert("undelegateResourceContracts")?;
                for row in &undelegate_resource_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            if !cancel_all_unfreeze_v2_contract_row_list.is_empty() {
                let mut inserter = client.insert("cancelAllUnfreezeV2Contracts")?;
                for row in &cancel_all_unfreeze_v2_contract_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            // intently insert blocks at last
            if !block_row_list.is_empty() {
                let mut inserter = client.insert("blocks")?;
                for row in &block_row_list {
                    inserter.write(row).await?;
                }
                inserter.end().await?;
            }

            // Clear all lists in the same order as they were created
            block_row_list.clear();
            transaction_row_list.clear();
            event_row_list.clear();
            internal_row_list.clear();

            account_create_contract_row_list.clear();
            transfer_contract_row_list.clear();
            transfer_asset_contract_row_list.clear();
            vote_asset_contract_row_list.clear();
            vote_witness_contract_row_list.clear();
            witness_create_contract_row_list.clear();
            asset_issue_contract_row_list.clear();
            witness_update_contract_row_list.clear();
            participate_asset_issue_contract_row_list.clear();
            account_update_contract_row_list.clear();
            freeze_balance_contract_row_list.clear();
            unfreeze_balance_contract_row_list.clear();
            withdraw_balance_contract_row_list.clear();
            unfreeze_asset_contract_row_list.clear();
            update_asset_contract_row_list.clear();
            proposal_create_contract_row_list.clear();
            proposal_approve_contract_row_list.clear();
            proposal_delete_contract_row_list.clear();
            set_account_id_contract_row_list.clear();
            create_smart_contract_row_list.clear();
            trigger_smart_contract_row_list.clear();
            update_setting_contract_row_list.clear();
            exchange_create_contract_row_list.clear();
            exchange_inject_contract_row_list.clear();
            exchange_withdraw_contract_row_list.clear();
            exchange_transaction_contract_row_list.clear();
            update_energy_limit_contract_row_list.clear();
            account_permission_update_contract_row_list.clear();
            clear_abi_contract_row_list.clear();
            update_brokerage_contract_row_list.clear();
            shielded_transfer_contract_row_list.clear();
            market_sell_asset_contract_row_list.clear();
            market_cancel_order_contract_row_list.clear();
            freeze_balance_v2_contract_row_list.clear();
            unfreeze_balance_v2_contract_row_list.clear();
            withdraw_expire_unfreeze_contract_row_list.clear();
            delegate_resource_contract_row_list.clear();
            undelegate_resource_contract_row_list.clear();
            cancel_all_unfreeze_v2_contract_row_list.clear();

            info!("{} done blocks & txs", num)
        }
    }

    Ok(())
}
