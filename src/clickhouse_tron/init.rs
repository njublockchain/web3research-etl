use std::error::Error;

use bitcoincore_rpc::{Client, RpcApi};
use klickhouse::Client as Klient;
use log::{debug, info, warn};
use tron_grpc::{
    AccountCreateContract, BlockExtention, EmptyMessage, FreezeBalanceContract,
    FreezeBalanceV2Contract, NumberMessage, TransferAssetContract, TransferContract,
    TriggerSmartContract, UnfreezeBalanceContract, UnfreezeBalanceV2Contract,
};
use url::Url;

use crate::{
    clickhouse_scheme::tron::{
        AccountCreateContractRow, BlockRow, FreezeBalanceContractRow, FreezeBalanceV2ContractRow,
        InternalTransactionRow, LogRow, MarketOrderDetailRow, TransactionRow,
        TransferAssetContractRow, TransferContractRow, TriggerSmartContractRow,
        UnfreezeBalanceContractRow, UnfreezeBalanceV2ContractRow,
    },
    clickhouse_tron::ddl::{
        ACCOUNT_CREATE_CONTRACT_DDL, FREEZE_BALANCE_CONTRACT_DDL, FREEZE_BALANCE_V2_CONTRACT_DDL,
        TRANSFER_ASSET_CONTRACT_DDL, TRANSFER_CONTRACT_DDL, TRIGGER_SMART_CONTRACT_DDL,
        UNFREEZE_BALANCE_CONTRACT_DDL, UNFREEZE_BALANCE_V2_CONTRACT_DDL,
    },
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
    klient
        .execute(format!("CREATE DATABASE IF NOT EXISTS {}", options.default_database).as_str())
        .await?;

    klient
        .execute(
            "
            -- blocks definition

            CREATE TABLE IF NOT EXISTS blocks
            (
            
                `hash` FixedString(32),
            
                `timestamp` Int64,
            
                `txTrieRoot` FixedString(32),
            
                `parentHash` FixedString(32),
            
                `number` Int64,
            
                `witnessId` Int64,
            
                `witnessAddress` FixedString(34),
            
                `version` Int32,
            
                `accountStateRoot` FixedString(32),
            
                `witnessSignature` String,
            
                `transactionCount` Int32
            )
            ENGINE = ReplacingMergeTree
            ORDER BY number
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- transactions definition

            CREATE TABLE IF NOT EXISTS transactions
            (
            
                `hash` FixedString(32),
            
                `blockNum` Int64,

                `index` Int64,
            
                `expiration` Int64,
            
                `authorityAccountNames` Array(LowCardinality(String)),
            
                `authorityAccountAddresses` Array(FixedString(34)),
            
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
            
                `contractResult` String,
            
                `contractAddress` FixedString(34),
            
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
            
                `packingFee` Int64,
            
                `withdrawExpireAmount` Int64
            )
            ENGINE = ReplacingMergeTree
            ORDER BY hash
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- logs definition

            CREATE TABLE IF NOT EXISTS logs
            (
            
                `blockNum` Int64,
            
                `transactionHash` FixedString(32),
            
                `logIndex` Int32,
            
                `address` FixedString(34),
            
                topic0 Nullable(FixedString(32)),
                topic1 Nullable(FixedString(32)),
                topic2 Nullable(FixedString(32)),
                topic3 Nullable(FixedString(32)),
            
                `data` String
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (transactionHash,
             logIndex)
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- logs definition

            CREATE TABLE IF NOT EXISTS logs
            (
            
                `blockNum` Int64,
            
                `transactionHash` FixedString(32),
            
                `logIndex` Int32,
            
                `address` FixedString(34),
            
                `topics` Array(FixedString(32)),
            
                `data` String
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (transactionHash,
             logIndex)
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- internals definition

            CREATE TABLE IF NOT EXISTS internals
            (
            
                `blockNum` Int64,
            
                `transactionHash` FixedString(32),
            
                `internalIndex` Int32,
            
                `hash` FixedString(32),
            
                `callerAddress` FixedString(34),
            
                `transferToAddress` FixedString(34),
            
                `callValueInfos` Nested(tokenId String, callValue Int64),
                
                `note` String,
            
                `rejected` Bool,
            
                `extra` String
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (transactionHash,
             internalIndex)
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient
        .execute(
            "
            -- orders definition

            CREATE TABLE IF NOT EXISTS orders
            (
            
                `blockNum` Int64,
            
                `transactionHash` FixedString(32),
            
                `orderIndex` Int32,
            
                `orderId` String,
            
                `makerOrderId` FixedString(32),
            
                `takerOrderId` FixedString(32),
            
                `fillSellQuantity` Int64,
            
                `fillBuyQuantity` Int64
            )
            ENGINE = ReplacingMergeTree
            ORDER BY (transactionHash,
             orderIndex)
            SETTINGS index_granularity = 8192;
        ",
        )
        .await
        .unwrap();

    klient.execute(ACCOUNT_CREATE_CONTRACT_DDL).await.unwrap();
    klient.execute(TRANSFER_CONTRACT_DDL).await.unwrap();
    klient.execute(TRANSFER_ASSET_CONTRACT_DDL).await.unwrap();
    klient.execute(FREEZE_BALANCE_CONTRACT_DDL).await.unwrap();
    klient.execute(UNFREEZE_BALANCE_CONTRACT_DDL).await.unwrap();
    klient.execute(TRIGGER_SMART_CONTRACT_DDL).await.unwrap();
    klient
        .execute(FREEZE_BALANCE_V2_CONTRACT_DDL)
        .await
        .unwrap();
    klient
        .execute(UNFREEZE_BALANCE_V2_CONTRACT_DDL)
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
    let mut order_detail_row_list = Vec::new();

    let mut account_create_contract_row_list = Vec::new();
    let mut transfer_contract_row_list = Vec::new();
    let mut transfer_asset_contract_row_list = Vec::new();
    let mut freeze_balance_contract_row_list = Vec::new();
    let mut unfreeze_balance_contract_row_list = Vec::new();
    let mut trigger_smart_contract_row_list = Vec::new();
    let mut freeze_balance_v2_contract_row_list = Vec::new();
    let mut unfreeze_balance_v2_contract_row_list = Vec::new();

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

                for (index, order_detail) in tx_infos[index].order_details.iter().enumerate() {
                    let order_detail_row = MarketOrderDetailRow::from_grpc(
                        num,
                        transaction_row.hash.to_vec(),
                        index as i32,
                        transaction_row.order_id.to_vec(),
                        order_detail,
                    );
                    order_detail_row_list.push(order_detail_row);
                }

                // if transaction.clone().transaction.unwrap().raw_data.unwrap().contract[0].parameter.unwrap().type_url

                transaction_row
            };

            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<AccountCreateContract>()
            {
                let row = AccountCreateContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                account_create_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<TransferContract>()
            {
                let row = TransferContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                transfer_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<TransferAssetContract>()
            {
                let row = TransferAssetContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                transfer_asset_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<FreezeBalanceContract>()
            {
                let row = FreezeBalanceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                freeze_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<UnfreezeBalanceContract>()
            {
                let row = UnfreezeBalanceContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                unfreeze_balance_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<TriggerSmartContract>()
            {
                let row = TriggerSmartContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                trigger_smart_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<FreezeBalanceV2Contract>()
            {
                let row = FreezeBalanceV2ContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                freeze_balance_v2_contract_row_list.push(row);
            }
            if let Ok(msg) = transaction
                .transaction
                .clone()
                .unwrap()
                .raw_data
                .unwrap()
                .contract[0]
                .parameter
                .clone()
                .unwrap()
                .to_msg::<UnfreezeBalanceV2Contract>()
            {
                let row = UnfreezeBalanceV2ContractRow::from_grpc(
                    num,
                    transaction.txid.clone(),
                    index.try_into().unwrap(),
                    0,
                    msg,
                );
                unfreeze_balance_v2_contract_row_list.push(row);
            }

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
                klient.insert_native_block("INSERT INTO logs FORMAT native", log_row_list.to_vec()),
                klient.insert_native_block(
                    "INSERT INTO internals FORMAT native",
                    internal_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO orders FORMAT native",
                    order_detail_row_list.to_vec()
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
                    "INSERT INTO freezeBalanceContracts FORMAT native",
                    freeze_balance_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO unfreezeBalanceContracts FORMAT native",
                    unfreeze_balance_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO triggerSmartContracts FORMAT native",
                    trigger_smart_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO freezeBalanceV2Contracts FORMAT native",
                    freeze_balance_v2_contract_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO unfreezeBalanceV2Contracts FORMAT native",
                    unfreeze_balance_v2_contract_row_list.to_vec()
                ),
            )
            .unwrap();

            block_row_list.clear();
            transaction_row_list.clear();
            log_row_list.clear();
            internal_row_list.clear();
            order_detail_row_list.clear();

            account_create_contract_row_list.clear();
            transfer_contract_row_list.clear();
            transfer_asset_contract_row_list.clear();
            freeze_balance_contract_row_list.clear();
            unfreeze_balance_contract_row_list.clear();
            trigger_smart_contract_row_list.clear();
            freeze_balance_v2_contract_row_list.clear();
            unfreeze_balance_v2_contract_row_list.clear();

            info!("{} done blocks & txs", num)
        }
    }

    tokio::try_join!(
        klient.insert_native_block("INSERT INTO blocks FORMAT native", block_row_list.to_vec()),
        klient.insert_native_block(
            "INSERT INTO transactions FORMAT native",
            transaction_row_list.to_vec()
        ),
        klient.insert_native_block("INSERT INTO logs FORMAT native", log_row_list.to_vec()),
        klient.insert_native_block(
            "INSERT INTO internals FORMAT native",
            internal_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO orders FORMAT native",
            order_detail_row_list.to_vec()
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
            "INSERT INTO freezeBalanceContracts FORMAT native",
            freeze_balance_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO unfreezeBalanceContracts FORMAT native",
            unfreeze_balance_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO triggerSmartContracts FORMAT native",
            trigger_smart_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO freezeBalanceV2Contracts FORMAT native",
            freeze_balance_v2_contract_row_list.to_vec()
        ),
        klient.insert_native_block(
            "INSERT INTO unfreezeBalanceV2Contracts FORMAT native",
            unfreeze_balance_v2_contract_row_list.to_vec()
        ),
    )
    .unwrap();

    Ok(())
}
