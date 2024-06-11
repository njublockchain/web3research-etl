use std::error::Error;

use documented::Documented;
use log::{debug, info, warn};
use tron_grpc::{
    AccountCreateContract, AccountPermissionUpdateContract,
    AccountUpdateContract, AssetIssueContract, CancelAllUnfreezeV2Contract,
    ClearAbiContract, CreateSmartContract, DelegateResourceContract, EmptyMessage,
    ExchangeCreateContract, ExchangeInjectContract, ExchangeTransactionContract,
    ExchangeWithdrawContract, FreezeBalanceContract, FreezeBalanceV2Contract,
    MarketCancelOrderContract, MarketSellAssetContract, NumberMessage,
    ParticipateAssetIssueContract, ShieldedTransferContract, TransferAssetContract,
    TransferContract, TriggerSmartContract, UnDelegateResourceContract, UnfreezeAssetContract,
    UnfreezeBalanceContract, UnfreezeBalanceV2Contract, UpdateAssetContract,
    UpdateBrokerageContract, UpdateEnergyLimitContract, UpdateSettingContract, VoteWitnessContract,
    WithdrawBalanceContract, WithdrawExpireUnfreezeContract, WitnessUpdateContract,
};
use url::Url;

use crate::clickhouse_scheme::tron::{
    AccountCreateContractRow, AccountPermissionUpdateContractRow, AccountUpdateContractRow,
    AssetIssueContractRow, BlockRow, CancelAllUnfreezeV2ContractRow, ClearAbiContractRow,
    CreateSmartContractRow, DelegateResourceContractRow, ExchangeCreateContractRow,
    ExchangeInjectContractRow, ExchangeTransactionContractRow, ExchangeWithdrawContractRow,
    FreezeBalanceContractRow, FreezeBalanceV2ContractRow, InternalTransactionRow, LogRow,
    MarketCancelOrderContractRow, MarketSellAssetContractRow, ParticipateAssetIssueContractRow,
    ShieldedTransferContractRow, TransactionRow, TransferAssetContractRow, TransferContractRow,
    TriggerSmartContractRow, UndelegateResourceContractRow, UnfreezeAssetContractRow,
    UnfreezeBalanceContractRow, UnfreezeBalanceV2ContractRow, UpdateAssetContractRow,
    UpdateBrokerageContractRow, UpdateEnergyLimitContractRow, UpdateSettingContractRow,
    VoteWitnessContractRow, WithdrawBalanceContractRow, WithdrawExpireUnfreezeContractRow,
    WitnessUpdateContractRow,
};

pub(crate) async fn init(
    db: String,
    provider: String,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
        warn!("auth enabled for clickhouse");
        klickhouse::ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url
                .path()
                .to_string()
                .strip_prefix('/')
                .unwrap()
                .to_string(),
        }
    } else {
        klickhouse::ClientOptions::default()
    };

    let klient = klickhouse::Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        options.clone(),
    )
    .await?;

    debug!("start initializing schema");

    // init all basics
    klient.execute(BlockRow::DOCS).await.unwrap();
    klient.execute(TransactionRow::DOCS).await.unwrap();
    klient.execute(LogRow::DOCS).await.unwrap();
    klient.execute(InternalTransactionRow::DOCS).await.unwrap();

    // init all contracts
    klient
        .execute(AccountCreateContractRow::DOCS)
        .await
        .unwrap();
    klient.execute(TransferContractRow::DOCS).await.unwrap();
    klient
        .execute(TransferAssetContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(TransferAssetContractRow::DOCS)
        .await
        .unwrap();
    klient.execute(VoteWitnessContractRow::DOCS).await.unwrap();
    klient.execute(AssetIssueContractRow::DOCS).await.unwrap();
    klient
        .execute(WitnessUpdateContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(ParticipateAssetIssueContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(AccountUpdateContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(FreezeBalanceContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(UnfreezeBalanceContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(WithdrawBalanceContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(UnfreezeAssetContractRow::DOCS)
        .await
        .unwrap();
    klient.execute(UpdateAssetContractRow::DOCS).await.unwrap();
    klient.execute(CreateSmartContractRow::DOCS).await.unwrap();
    klient.execute(TriggerSmartContractRow::DOCS).await.unwrap();
    klient
        .execute(UpdateSettingContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(ExchangeCreateContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(ExchangeInjectContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(ExchangeWithdrawContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(ExchangeTransactionContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(UpdateEnergyLimitContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(AccountPermissionUpdateContractRow::DOCS)
        .await
        .unwrap();
    klient.execute(ClearAbiContractRow::DOCS).await.unwrap();
    klient
        .execute(UpdateBrokerageContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(ShieldedTransferContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(MarketSellAssetContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(MarketCancelOrderContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(FreezeBalanceV2ContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(UnfreezeBalanceV2ContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(WithdrawExpireUnfreezeContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(DelegateResourceContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(UndelegateResourceContractRow::DOCS)
        .await
        .unwrap();
    klient
        .execute(CancelAllUnfreezeV2ContractRow::DOCS)
        .await
        .unwrap();

    let mut client = tron_grpc::wallet_client::WalletClient::connect(provider).await?;

    let now = client.get_now_block2(EmptyMessage {}).await?;
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
    let mut log_row_list = Vec::new();
    let mut internal_row_list = Vec::new();

    let mut account_create_contract_row_list = Vec::new();
    let mut transfer_contract_row_list = Vec::new();
    let mut transfer_asset_contract_row_list = Vec::new();
    let mut vote_witness_contract_row_list = Vec::new();
    let mut asset_issue_contract_row_list = Vec::new();
    let mut witness_update_contract_row_list = Vec::new();
    let mut participate_asset_issue_contract_row_list = Vec::new();
    let mut account_update_contract_row_list = Vec::new();
    let mut freeze_balance_contract_row_list = Vec::new();
    let mut unfreeze_balance_contract_row_list = Vec::new();
    let mut withdraw_balance_contract_row_list = Vec::new();
    let mut unfreeze_asset_contract_row_list = Vec::new();
    let mut update_asset_contract_row_list = Vec::new();
    let mut create_smart_contract_row_list = Vec::new();
    let mut trigger_smart_contract_row_list = Vec::new();
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

    let mut client_clone = client.clone();

    for num in from..=to {
        // let cli = client.get_jsonrpc_client();
        let (block, tx_infos) = tokio::try_join!(
            client.get_block_by_num2(NumberMessage { num }),
            client_clone.get_transaction_info_by_block_num(NumberMessage { num })
        )
        .unwrap();
        let block = block.into_inner();
        let tx_infos = tx_infos.into_inner().transaction_info;

        let block_row = BlockRow::from_grpc(&block);
        block_row_list.push(block_row);

        for (index, transaction) in block.transactions.iter().enumerate() {
            let transaction_row = if num == 0 {
                TransactionRow::from_grpc(&block, index as i64, transaction, None)
            // handle genesis
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
                        LogRow::from_grpc(num, transaction_row.hash.to_vec(), index as i32, log);
                    log_row_list.push(log_row);
                }

                for (index, internal) in tx_infos[index].internal_transactions.iter().enumerate() {
                    let internal_row = InternalTransactionRow::from_grpc(
                        num,
                        transaction_row.hash.to_vec(),
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

            if let Ok(msg) = parameter.to_msg::<AccountCreateContract>() {
                let row = AccountCreateContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                account_create_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<TransferContract>() {
                let row = TransferContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                transfer_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<TransferAssetContract>() {
                let row = TransferAssetContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                transfer_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<VoteWitnessContract>() {
                let row = VoteWitnessContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                vote_witness_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<AssetIssueContract>() {
                let row = AssetIssueContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                asset_issue_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WitnessUpdateContract>() {
                let row = WitnessUpdateContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                witness_update_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ParticipateAssetIssueContract>() {
                let row = ParticipateAssetIssueContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                participate_asset_issue_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<AccountUpdateContract>() {
                let row = AccountUpdateContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                account_update_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<FreezeBalanceContract>() {
                let row = FreezeBalanceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                freeze_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnfreezeBalanceContract>() {
                let row = UnfreezeBalanceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                unfreeze_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WithdrawBalanceContract>() {
                let row = WithdrawBalanceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                withdraw_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnfreezeAssetContract>() {
                let row = UnfreezeAssetContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                unfreeze_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateAssetContract>() {
                let row = UpdateAssetContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<CreateSmartContract>() {
                let row = CreateSmartContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                create_smart_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<TriggerSmartContract>() {
                let row = TriggerSmartContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                trigger_smart_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateSettingContract>() {
                let row = UpdateSettingContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_setting_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeCreateContract>() {
                let row = ExchangeCreateContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_create_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeInjectContract>() {
                let row = ExchangeInjectContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_inject_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeWithdrawContract>() {
                let row = ExchangeWithdrawContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_withdraw_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ExchangeTransactionContract>() {
                let row = ExchangeTransactionContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                exchange_transaction_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateEnergyLimitContract>() {
                let row = UpdateEnergyLimitContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_energy_limit_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<AccountPermissionUpdateContract>() {
                let row = AccountPermissionUpdateContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                account_permission_update_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ClearAbiContract>() {
                let row = ClearAbiContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                clear_abi_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UpdateBrokerageContract>() {
                let row = UpdateBrokerageContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                update_brokerage_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<ShieldedTransferContract>() {
                let row = ShieldedTransferContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                shielded_transfer_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<MarketSellAssetContract>() {
                let row = MarketSellAssetContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                market_sell_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<MarketCancelOrderContract>() {
                let row = MarketCancelOrderContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                market_cancel_order_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<FreezeBalanceV2Contract>() {
                let row = FreezeBalanceV2ContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                freeze_balance_v2_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnfreezeBalanceV2Contract>() {
                let row = UnfreezeBalanceV2ContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                unfreeze_balance_v2_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<WithdrawExpireUnfreezeContract>() {
                let row = WithdrawExpireUnfreezeContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                withdraw_expire_unfreeze_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<DelegateResourceContract>() {
                let row = DelegateResourceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                delegate_resource_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<UnDelegateResourceContract>() {
                let row = UndelegateResourceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                undelegate_resource_contract_row_list.push(row);
            }
            if let Ok(msg) = parameter.to_msg::<CancelAllUnfreezeV2Contract>() {
                let row = CancelAllUnfreezeV2ContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    &msg,
                );
                cancel_all_unfreeze_v2_contract_row_list.push(row);
            }

            //TODO: add CustomContract and GetContract (useless)

            transaction_row_list.push(transaction_row);
        }

        if (num - from + 1) % batch == 0 {
            tokio::try_join!(
                klient.insert_native_block(
                    "INSERT INTO blocks FORMAT native",
                    block_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO transactions FORMAT native",
                    transaction_row_list.to_vec()
                ),
                klient.insert_native_block("INSERT INTO events FORMAT native", log_row_list.to_vec()),
                klient.insert_native_block(
                    "INSERT INTO internals FORMAT native",
                    internal_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO accountCreateContracts FORMAT native",
                    account_create_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO transferContracts FORMAT native",
                    transfer_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO transferAssetContracts FORMAT native",
                    transfer_asset_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO voteWitnessContracts FORMAT native",
                    vote_witness_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO assetIssueContracts FORMAT native",
                    asset_issue_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO witnessUpdateContracts FORMAT native",
                    witness_update_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO participateAssetIssueContracts FORMAT native",
                    participate_asset_issue_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO accountUpdateContracts FORMAT native",
                    account_update_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO freezeBalanceContracts FORMAT native",
                    freeze_balance_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO unfreezeBalanceContracts FORMAT native",
                    unfreeze_balance_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO withdrawBalanceContracts FORMAT native",
                    withdraw_balance_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO unfreezeAssetContracts FORMAT native",
                    unfreeze_asset_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO updateAssetContracts FORMAT native",
                    update_asset_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO createSmartContracts FORMAT native",
                    create_smart_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO triggerSmartContracts FORMAT native",
                    trigger_smart_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO updateSettingContracts FORMAT native",
                    update_setting_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO exchangeCreateContracts FORMAT native",
                    exchange_create_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO exchangeInjectContracts FORMAT native",
                    exchange_inject_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO exchangeWithdrawContracts FORMAT native",
                    exchange_withdraw_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO exchangeTransactionContracts FORMAT native",
                    exchange_transaction_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO updateEnergyLimitContracts FORMAT native",
                    update_energy_limit_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO accountPermissionUpdateContracts FORMAT native",
                    account_permission_update_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO clearAbiContracts FORMAT native",
                    clear_abi_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO updateBrokerageContracts FORMAT native",
                    update_brokerage_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO shieldedTransferContracts FORMAT native",
                    shielded_transfer_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO marketSellAssetContracts FORMAT native",
                    market_sell_asset_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO marketCancelOrderContracts FORMAT native",
                    market_cancel_order_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO freezeBalanceV2Contracts FORMAT native",
                    freeze_balance_v2_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO unfreezeBalanceV2Contracts FORMAT native",
                    unfreeze_balance_v2_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO withdrawExpireUnfreezeContracts FORMAT native",
                    withdraw_expire_unfreeze_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO delegateResourceContracts FORMAT native",
                    delegate_resource_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO undelegateResourceContracts FORMAT native",
                    undelegate_resource_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO cancelAllUnfreezeV2Contracts FORMAT native",
                    cancel_all_unfreeze_v2_contract_row_list.to_vec()
                )
            )
            .unwrap();

            block_row_list.clear();
            transaction_row_list.clear();
            log_row_list.clear();
            internal_row_list.clear();

            account_create_contract_row_list.clear();
            transfer_contract_row_list.clear();
            transfer_asset_contract_row_list.clear();
            vote_witness_contract_row_list.clear();
            asset_issue_contract_row_list.clear();
            witness_update_contract_row_list.clear();
            participate_asset_issue_contract_row_list.clear();
            account_update_contract_row_list.clear();
            freeze_balance_contract_row_list.clear();
            unfreeze_balance_contract_row_list.clear();
            withdraw_balance_contract_row_list.clear();
            unfreeze_asset_contract_row_list.clear();
            update_asset_contract_row_list.clear();
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

    tokio::try_join!(
        klient.insert_native_block(
            "INSERT INTO blocks FORMAT native",
            block_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO transactions FORMAT native",
            transaction_row_list.to_vec()
        ),
        klient.insert_native_block("INSERT INTO events FORMAT native", log_row_list.to_vec()),
        klient.insert_native_block(
            "INSERT INTO internals FORMAT native",
            internal_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO accountCreateContracts FORMAT native",
            account_create_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO transferContracts FORMAT native",
            transfer_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO transferAssetContracts FORMAT native",
            transfer_asset_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO voteWitnessContracts FORMAT native",
            vote_witness_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO assetIssueContracts FORMAT native",
            asset_issue_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO witnessUpdateContracts FORMAT native",
            witness_update_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO participateAssetIssueContracts FORMAT native",
            participate_asset_issue_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO accountUpdateContracts FORMAT native",
            account_update_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO freezeBalanceContracts FORMAT native",
            freeze_balance_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO unfreezeBalanceContracts FORMAT native",
            unfreeze_balance_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO withdrawBalanceContracts FORMAT native",
            withdraw_balance_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO unfreezeAssetContracts FORMAT native",
            unfreeze_asset_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO updateAssetContracts FORMAT native",
            update_asset_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO createSmartContracts FORMAT native",
            create_smart_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO triggerSmartContracts FORMAT native",
            trigger_smart_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO updateSettingContracts FORMAT native",
            update_setting_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO exchangeCreateContracts FORMAT native",
            exchange_create_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO exchangeInjectContracts FORMAT native",
            exchange_inject_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO exchangeWithdrawContracts FORMAT native",
            exchange_withdraw_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO exchangeTransactionContracts FORMAT native",
            exchange_transaction_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO updateEnergyLimitContracts FORMAT native",
            update_energy_limit_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO accountPermissionUpdateContracts FORMAT native",
            account_permission_update_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO clearAbiContracts FORMAT native",
            clear_abi_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO updateBrokerageContracts FORMAT native",
            update_brokerage_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO shieldedTransferContracts FORMAT native",
            shielded_transfer_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO marketSellAssetContracts FORMAT native",
            market_sell_asset_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO marketCancelOrderContracts FORMAT native",
            market_cancel_order_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO freezeBalanceV2Contracts FORMAT native",
            freeze_balance_v2_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO unfreezeBalanceV2Contracts FORMAT native",
            unfreeze_balance_v2_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO withdrawExpireUnfreezeContracts FORMAT native",
            withdraw_expire_unfreeze_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO delegateResourceContracts FORMAT native",
            delegate_resource_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO undelegateResourceContracts FORMAT native",
            undelegate_resource_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO cancelAllUnfreezeV2Contracts FORMAT native",
            cancel_all_unfreeze_v2_contract_row_list.to_vec()
        )
    )
    .unwrap();

    Ok(())
}
