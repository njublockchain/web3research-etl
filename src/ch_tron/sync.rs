use klickhouse::{Client, Row};
use log::{info, warn};
use std::error::Error;
use tron_grpc::{
    AccountCreateContract, AccountPermissionUpdateContract, AccountUpdateContract,
    AssetIssueContract, CancelAllUnfreezeV2Contract, ClearAbiContract, CreateSmartContract,
    DelegateResourceContract, EmptyMessage, ExchangeCreateContract, ExchangeInjectContract,
    ExchangeTransactionContract, ExchangeWithdrawContract, FreezeBalanceContract,
    FreezeBalanceV2Contract, MarketCancelOrderContract, MarketSellAssetContract, NumberMessage,
    ParticipateAssetIssueContract, ProposalApproveContract, ProposalCreateContract,
    ProposalDeleteContract, SetAccountIdContract, ShieldedTransferContract, TransactionInfo,
    TransferAssetContract, TransferContract, TriggerSmartContract, UnDelegateResourceContract,
    UnfreezeAssetContract, UnfreezeBalanceContract, UnfreezeBalanceV2Contract, UpdateAssetContract,
    UpdateBrokerageContract, UpdateEnergyLimitContract, UpdateSettingContract, VoteAssetContract,
    VoteWitnessContract, WithdrawBalanceContract, WithdrawExpireUnfreezeContract,
    WitnessCreateContract, WitnessUpdateContract,
};
use url::Url;

use crate::{ch_tron::schema::*, ProviderType};

// Process a single block and insert it into the database with all related data
pub async fn insert_block(
    client: Client,
    tron_provider: &str,
    block_number: i64,
) -> Result<(), Box<dyn Error>> {
    let mut tron_client =
        tron_grpc::wallet_client::WalletClient::connect(tron_provider.to_string()).await?;

    // Get the block by number
    let block = tron_client
        .get_block_by_num2(NumberMessage { num: block_number })
        .await?;
    let tx_infos = tron_client
        .get_transaction_info_by_block_num(NumberMessage { num: block_number })
        .await?;

    let block = block.into_inner();
    let tx_infos = tx_infos.into_inner().transaction_info;

    // If block is missing or has issues
    if block.block_header.is_none() {
        warn!("Empty block header for block {}", block_number);
        return Ok(());
    }

    let block_row = BlockRow::from_grpc(&block);

    // Initialize all contract vector lists
    let mut transaction_row_list = Vec::new();
    let mut log_row_list = Vec::new();
    let mut internal_row_list = Vec::new();

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

    // Process all transactions
    for (index, transaction) in block.transactions.iter().enumerate() {
        let transaction_row = if block_number == 0 {
            TransactionRow::from_grpc(&block_row, index.try_into().unwrap(), transaction, None)
        // handle genesis
        } else {
            if index >= tx_infos.len() {
                warn!(
                    "Transaction info not found for index {} in block {}",
                    index, block_number
                );
                continue;
            }

            if tx_infos[index].id != transaction.txid {
                warn!(
                    "Transaction ID mismatch: {} vs {} in block {}",
                    hex::encode(&tx_infos[index].id),
                    hex::encode(&transaction.txid),
                    block_number
                );
                continue;
            }

            let transaction_row = TransactionRow::from_grpc(
                &block_row,
                index.try_into().unwrap(),
                transaction,
                Some(&tx_infos[index]),
            );

            // Process logs
            for (log_idx, log) in tx_infos[index].log.iter().enumerate() {
                let log_row = LogRow::from_grpc(&block_row, &transaction_row, log_idx as i32, log);
                log_row_list.push(log_row);
            }

            // Process internal transactions
            for (internal_idx, internal) in tx_infos[index].internal_transactions.iter().enumerate()
            {
                let internal_row = InternalTransactionRow::from_grpc(
                    &block_row,
                    &transaction_row,
                    internal_idx as i32,
                    internal,
                );
                internal_row_list.push(internal_row);
            }

            transaction_row
        };

        // Process contract parameters
        if transaction.transaction.is_none()
            || transaction.transaction.as_ref().unwrap().raw_data.is_none()
            || transaction
                .transaction
                .as_ref()
                .unwrap()
                .raw_data
                .as_ref()
                .unwrap()
                .contract
                .is_empty()
        {
            warn!("Invalid transaction structure in block {}", block_number);
            continue;
        }

        let parameter = transaction
            .transaction
            .clone()
            .unwrap()
            .raw_data
            .unwrap()
            .contract[0]
            .parameter
            .clone();

        if parameter.is_none() {
            warn!(
                "Parameter is none in transaction {}",
                hex::encode(&transaction.txid)
            );
            transaction_row_list.push(transaction_row);
            continue;
        }

        let parameter = parameter.unwrap();
        let mut parameter_parsed = false;

        // Try to parse each contract type
        if let Ok(msg) = parameter.to_msg::<AccountCreateContract>() {
            parameter_parsed = true;
            let row = AccountCreateContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            account_create_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<TransferContract>() {
            parameter_parsed = true;
            let row = TransferContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            transfer_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<TransferAssetContract>() {
            parameter_parsed = true;
            let row = TransferAssetContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            transfer_asset_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<VoteAssetContract>() {
            parameter_parsed = true;
            let row = VoteAssetContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            vote_asset_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<VoteWitnessContract>() {
            parameter_parsed = true;
            let row = VoteWitnessContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            vote_witness_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<WitnessCreateContract>() {
            parameter_parsed = true;
            let row = WitnessCreateContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            witness_create_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<AssetIssueContract>() {
            parameter_parsed = true;
            let row = AssetIssueContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            asset_issue_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<WitnessUpdateContract>() {
            parameter_parsed = true;
            let row = WitnessUpdateContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            witness_update_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ParticipateAssetIssueContract>() {
            parameter_parsed = true;
            let row =
                ParticipateAssetIssueContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            participate_asset_issue_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<AccountUpdateContract>() {
            parameter_parsed = true;
            let row = AccountUpdateContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            account_update_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<FreezeBalanceContract>() {
            parameter_parsed = true;
            let row = FreezeBalanceContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            freeze_balance_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UnfreezeBalanceContract>() {
            parameter_parsed = true;
            let row = UnfreezeBalanceContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            unfreeze_balance_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<WithdrawBalanceContract>() {
            parameter_parsed = true;
            let row = WithdrawBalanceContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            withdraw_balance_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UnfreezeAssetContract>() {
            parameter_parsed = true;
            let row = UnfreezeAssetContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            unfreeze_asset_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UpdateAssetContract>() {
            parameter_parsed = true;
            let row = UpdateAssetContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            update_asset_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ProposalCreateContract>() {
            parameter_parsed = true;
            let row = ProposalCreateContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            proposal_create_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ProposalApproveContract>() {
            parameter_parsed = true;
            let row = ProposalApproveContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            proposal_approve_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ProposalDeleteContract>() {
            parameter_parsed = true;
            let row = ProposalDeleteContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            proposal_delete_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<SetAccountIdContract>() {
            parameter_parsed = true;
            let row = SetAccountIdContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            set_account_id_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<CreateSmartContract>() {
            parameter_parsed = true;
            let row = CreateSmartContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            create_smart_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<TriggerSmartContract>() {
            parameter_parsed = true;
            let row = TriggerSmartContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            trigger_smart_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UpdateSettingContract>() {
            parameter_parsed = true;
            let row = UpdateSettingContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            update_setting_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ExchangeCreateContract>() {
            parameter_parsed = true;
            let row = ExchangeCreateContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            exchange_create_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ExchangeInjectContract>() {
            parameter_parsed = true;
            let row = ExchangeInjectContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            exchange_inject_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ExchangeWithdrawContract>() {
            parameter_parsed = true;
            let row = ExchangeWithdrawContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            exchange_withdraw_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ExchangeTransactionContract>() {
            parameter_parsed = true;
            let row =
                ExchangeTransactionContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            exchange_transaction_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UpdateEnergyLimitContract>() {
            parameter_parsed = true;
            let row =
                UpdateEnergyLimitContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            update_energy_limit_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<AccountPermissionUpdateContract>() {
            parameter_parsed = true;
            let row = AccountPermissionUpdateContractRow::from_grpc(
                &block_row,
                &transaction_row,
                0,
                &msg,
            );
            account_permission_update_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ClearAbiContract>() {
            parameter_parsed = true;
            let row = ClearAbiContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            clear_abi_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UpdateBrokerageContract>() {
            parameter_parsed = true;
            let row = UpdateBrokerageContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            update_brokerage_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<ShieldedTransferContract>() {
            parameter_parsed = true;
            let row = ShieldedTransferContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            shielded_transfer_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<MarketSellAssetContract>() {
            parameter_parsed = true;
            let row = MarketSellAssetContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            market_sell_asset_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<MarketCancelOrderContract>() {
            parameter_parsed = true;
            let row =
                MarketCancelOrderContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            market_cancel_order_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<FreezeBalanceV2Contract>() {
            parameter_parsed = true;
            let row = FreezeBalanceV2ContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            freeze_balance_v2_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UnfreezeBalanceV2Contract>() {
            parameter_parsed = true;
            let row =
                UnfreezeBalanceV2ContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            unfreeze_balance_v2_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<WithdrawExpireUnfreezeContract>() {
            parameter_parsed = true;
            let row =
                WithdrawExpireUnfreezeContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            withdraw_expire_unfreeze_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<DelegateResourceContract>() {
            parameter_parsed = true;
            let row = DelegateResourceContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            delegate_resource_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<UnDelegateResourceContract>() {
            parameter_parsed = true;
            let row =
                UndelegateResourceContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            undelegate_resource_contract_row_list.push(row);
        }
        if let Ok(msg) = parameter.to_msg::<CancelAllUnfreezeV2Contract>() {
            parameter_parsed = true;
            let row =
                CancelAllUnfreezeV2ContractRow::from_grpc(&block_row, &transaction_row, 0, &msg);
            cancel_all_unfreeze_v2_contract_row_list.push(row);
        }

        if !parameter_parsed {
            warn!(
                "unknown contract type: {:?} {:X?}",
                parameter.type_url, transaction.txid
            );
        }

        transaction_row_list.push(transaction_row);
    }

    // Insert all data to database
    tokio::try_join!(
        client.insert_native_block(
            "INSERT INTO transactions FORMAT native",
            transaction_row_list
        ),
        client.insert_native_block("INSERT INTO events FORMAT native", log_row_list),
        client.insert_native_block("INSERT INTO internals FORMAT native", internal_row_list),
        client.insert_native_block(
            "INSERT INTO accountCreateContracts FORMAT native",
            account_create_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO transferContracts FORMAT native",
            transfer_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO transferAssetContracts FORMAT native",
            transfer_asset_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO voteAssetContracts FORMAT native",
            vote_asset_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO voteWitnessContracts FORMAT native",
            vote_witness_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO witnessCreateContracts FORMAT native",
            witness_create_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO assetIssueContracts FORMAT native",
            asset_issue_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO witnessUpdateContracts FORMAT native",
            witness_update_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO participateAssetIssueContracts FORMAT native",
            participate_asset_issue_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO accountUpdateContracts FORMAT native",
            account_update_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO freezeBalanceContracts FORMAT native",
            freeze_balance_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO unfreezeBalanceContracts FORMAT native",
            unfreeze_balance_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO withdrawBalanceContracts FORMAT native",
            withdraw_balance_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO unfreezeAssetContracts FORMAT native",
            unfreeze_asset_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO updateAssetContracts FORMAT native",
            update_asset_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO proposalCreateContracts FORMAT native",
            proposal_create_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO proposalApproveContracts FORMAT native",
            proposal_approve_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO proposalDeleteContracts FORMAT native",
            proposal_delete_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO setAccountIdContracts FORMAT native",
            set_account_id_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO createSmartContracts FORMAT native",
            create_smart_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO triggerSmartContracts FORMAT native",
            trigger_smart_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO updateSettingContracts FORMAT native",
            update_setting_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO exchangeCreateContracts FORMAT native",
            exchange_create_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO exchangeInjectContracts FORMAT native",
            exchange_inject_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO exchangeWithdrawContracts FORMAT native",
            exchange_withdraw_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO exchangeTransactionContracts FORMAT native",
            exchange_transaction_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO updateEnergyLimitContracts FORMAT native",
            update_energy_limit_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO accountPermissionUpdateContracts FORMAT native",
            account_permission_update_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO clearAbiContracts FORMAT native",
            clear_abi_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO updateBrokerageContracts FORMAT native",
            update_brokerage_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO shieldedTransferContracts FORMAT native",
            shielded_transfer_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO marketSellAssetContracts FORMAT native",
            market_sell_asset_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO marketCancelOrderContracts FORMAT native",
            market_cancel_order_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO freezeBalanceV2Contracts FORMAT native",
            freeze_balance_v2_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO unfreezeBalanceV2Contracts FORMAT native",
            unfreeze_balance_v2_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO withdrawExpireUnfreezeContracts FORMAT native",
            withdraw_expire_unfreeze_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO delegateResourceContracts FORMAT native",
            delegate_resource_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO undelegateResourceContracts FORMAT native",
            undelegate_resource_contract_row_list
        ),
        client.insert_native_block(
            "INSERT INTO cancelAllUnfreezeV2Contracts FORMAT native",
            cancel_all_unfreeze_v2_contract_row_list
        ),
    )?;

    // Insert block data
    client
        .insert_native_block("INSERT INTO blocks FORMAT native", vec![block_row])
        .await?;

    info!("Successfully inserted block {}", block_number);

    Ok(())
}

// Health check function for Tron blocks
pub async fn health_check(
    client: Client,
    tron_provider: &str,
    block_number: i64,
) -> Result<(), Box<dyn Error>> {
    info!("Health checking block {}", block_number);

    // Check if the block exists in the database
    let block_in_db = client
        .query_one::<BlockHashRow>(format!(
            "SELECT hash FROM blocks WHERE number = {}",
            block_number
        ))
        .await;

    if block_in_db.is_err() {
        // If block is missing in database, insert it
        info!("Block {} missing in database, inserting", block_number);
        insert_block(client, &tron_provider, block_number).await?;
    } else {
        // Block exists, check if hash matches with the blockchain
        let block_hash_in_db = block_in_db.unwrap().hash;

        // Create TRON client and get block from blockchain
        let mut tron_client =
            tron_grpc::wallet_client::WalletClient::connect(tron_provider.to_string()).await?;
        let block = tron_client
            .get_block_by_num2(NumberMessage { num: block_number })
            .await?;
        let block = block.into_inner();

        if block.block_header.is_none() {
            warn!("Block {} not found on chain", block_number);
            return Ok(());
        }

        // Get block hash from the blockchain
        let block_hash_on_chain = hex::encode(block.blockid.clone());

        // Compare hashes
        if block_hash_in_db != block_hash_on_chain {
            warn!(
                "Hash mismatch for block {}: DB has {} but chain has {}",
                block_number, block_hash_in_db, block_hash_on_chain
            );

            // Delete the block and related data from database
            tokio::try_join!(
                client.execute(format!(
                    "DELETE FROM blocks WHERE number = {}",
                    block_number
                )),
                client.execute(format!(
                    "DELETE FROM transactions WHERE block_number = {}",
                    block_number
                )),
                client.execute(format!(
                    "DELETE FROM events WHERE block_number = {}",
                    block_number
                )),
                client.execute(format!(
                    "DELETE FROM internals WHERE block_number = {}",
                    block_number
                ))
            )
            .ok();

            // Re-insert the block with correct data
            insert_block(client, &tron_provider, block_number).await?;
        }
    }

    Ok(())
}

// Sync function for continuous synchronization
pub async fn sync(
    db: String,
    provider_uri: String,
    _trace_provider_uri: Option<String>,
    _provider_type: ProviderType,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();

    // Configure client options for clickhouse
    let options = if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
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

    // Create clickhouse client
    let client = klickhouse::Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        options.clone(),
    )
    .await?;

    // Create TRON client
    let mut tron_client =
        tron_grpc::wallet_client::WalletClient::connect(provider_uri.clone()).await?;

    // Get initial state
    info!("Starting Tron sync from provider: {}", provider_uri);

    // Get the latest block number and hash
    let latest_block = tron_client.get_now_block2(EmptyMessage {}).await?;
    let latest_block_header = latest_block
        .into_inner()
        .block_header
        .unwrap()
        .raw_data
        .unwrap();
    let latest_block_number = latest_block_header.number;

    info!("Latest block on chain: {}", latest_block_number);

    // Get the latest block in the database
    let db_latest_block = client
        .query_one::<BlockQueryRow>("SELECT MAX(number) as number FROM blocks")
        .await;

    let mut prev_block_number = match db_latest_block {
        Ok(row) => {
            info!("Latest block in database: {}", row.number);
            row.number
        }
        Err(_) => {
            info!("No blocks in database, starting from 0");
            0
        }
    };

    // If we're far behind the chain, log the sync gap
    if latest_block_number - prev_block_number > 1000 {
        info!(
            "Large sync gap detected: {} blocks behind. This may take some time to catch up.",
            latest_block_number - prev_block_number
        );
    }

    // Run initial health check to ensure database integrity before syncing
    info!("Running initial health check");
    if prev_block_number > 0 {
        match interval_health_check(client.clone(), &provider_uri).await {
            Ok(_) => info!("Initial health check passed"),
            Err(e) => warn!("Initial health check encountered issues: {}", e),
        }
    }

    // Set up counters for statistics
    let mut blocks_processed = 0;
    let mut last_stats_time = std::time::Instant::now();

    // Main sync loop
    loop {
        // Get the current latest block
        let current_block = tron_client.get_now_block2(EmptyMessage {}).await?;
        let current_block_header = current_block
            .into_inner()
            .block_header
            .unwrap()
            .raw_data
            .unwrap();
        let current_block_number = current_block_header.number;
        let current_block_timestamp = current_block_header.timestamp;

        // If there are new blocks to process
        if current_block_number > prev_block_number {
            // Calculate batch size - more if we're far behind
            let blocks_behind = current_block_number - prev_block_number;
            let batch_end = if blocks_behind > 1000 {
                // Process more blocks at once if we're far behind
                std::cmp::min(prev_block_number + 100, current_block_number)
            } else {
                current_block_number
            };

            info!(
                "Processing blocks from {} to {}",
                prev_block_number + 1,
                batch_end
            );

            // Process blocks in the current batch
            for block_number in (prev_block_number + 1)..=batch_end {
                match insert_block(client.clone(), &provider_uri, block_number).await {
                    Ok(_) => {
                        blocks_processed += 1;
                        prev_block_number = block_number;

                        // Log periodic processing statistics
                        if blocks_processed % 50 == 0 {
                            let elapsed = last_stats_time.elapsed().as_secs_f64();
                            last_stats_time = std::time::Instant::now();
                            info!(
                                "Progress: block {}/{} ({:.2}%), {:.2} blocks/sec",
                                block_number,
                                current_block_number,
                                (block_number as f64 / current_block_number as f64) * 100.0,
                                50.0 / elapsed
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Error processing block {}: {}", block_number, e);

                        // Retry logic for important errors
                        if e.to_string().contains("connection") || e.to_string().contains("timeout")
                        {
                            info!(
                                "Connection issue detected, retrying block {} after delay",
                                block_number
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            match insert_block(client.clone(), &provider_uri, block_number).await {
                                Ok(_) => {
                                    info!(
                                        "Successfully processed block {} after retry",
                                        block_number
                                    );
                                    blocks_processed += 1;
                                    prev_block_number = block_number;
                                }
                                Err(retry_err) => {
                                    warn!(
                                        "Block {} still failed after retry: {}",
                                        block_number, retry_err
                                    );
                                    // Skip this block for now, will be caught in later health checks
                                }
                            }
                        }
                    }
                }
            }

            // Perform a health check periodically
            if prev_block_number % 100 == 0 {
                info!("Running health check at block {}", prev_block_number);
                match interval_health_check(client.clone(), &provider_uri).await {
                    Ok(_) => info!("Health check passed"),
                    Err(e) => warn!("Health check encountered issues: {}", e),
                }
            }

            // If we processed all blocks up to the latest, report sync status
            if prev_block_number == current_block_number {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let chain_delay = now - current_block_timestamp;
                info!(
                    "Sync completed up to block {}. Chain is {} ms behind real-time.",
                    current_block_number, chain_delay
                );
            }
        } else {
            // If we're fully synced, wait a bit before checking for new blocks
            info!(
                "No new blocks to process, latest is {}",
                current_block_number
            );
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}

// Helper structure for database queries
#[derive(Row, Clone, Debug)]
struct BlockQueryRow {
    number: i64,
}

// Helper structure for hash checking
#[derive(Row, Clone, Debug)]
struct BlockHashRow {
    hash: String,
}

// Perform periodic health checks
async fn interval_health_check(client: Client, tron_provider: &str) -> Result<(), Box<dyn Error>> {
    // First, get the latest block in the database
    let db_latest_block = client
        .query_one::<BlockQueryRow>("SELECT MAX(number) as number FROM blocks")
        .await;

    // Check if we have any blocks in the database
    if let Err(_) = db_latest_block {
        info!("No blocks in database to health check");
        return Ok(());
    }

    let latest_number = db_latest_block.unwrap().number;

    // Create TRON client to get blockchain data
    let mut tron_client =
        tron_grpc::wallet_client::WalletClient::connect(tron_provider.to_string()).await?;

    // Get the latest block on chain for reference
    let chain_latest = tron_client.get_now_block2(EmptyMessage {}).await?;
    let chain_latest_number = chain_latest
        .into_inner()
        .block_header
        .unwrap()
        .raw_data
        .unwrap()
        .number;

    info!(
        "Running health check - Database latest: {}, Chain latest: {}",
        latest_number, chain_latest_number
    );

    // Check recent blocks (last 10 or less if we don't have that many)
    let start = if latest_number > 10 {
        latest_number - 10
    } else {
        0
    };

    // First check the most recent blocks
    for num in start..=latest_number {
        health_check(client.clone(), tron_provider, num).await?;
    }

    // Check for potential gaps in the blocks at random intervals
    if latest_number > 100 {
        // Check some older blocks to detect any issues
        for i in 0..5 {
            let random_block = start - (i + 1) * 20;
            if random_block > 0 {
                health_check(client.clone(), tron_provider, random_block).await?;
            }
        }
    }

    Ok(())
}
