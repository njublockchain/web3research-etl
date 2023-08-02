use std::{error::Error, time::Duration};

use ethers::{
    providers::{Middleware, Provider, ProviderError, StreamExt, Ws},
    types::{Block, Transaction, TransactionReceipt, H256},
};
use klickhouse::{u256, Client, Row, Bytes, ClientOptions};
use log::{debug, error, info, warn};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use url::Url;

use super::main::{BlockRow, EventRow, TransactionRow, WithdrawalRow};

async fn get_block_and_tx(
    provider: &Provider<Ws>,
    num: u64,
) -> Result<(Option<Block<Transaction>>, Vec<TransactionReceipt>), ProviderError> {
    let result = tokio::try_join!(
        provider.get_block_with_txs(num),
        provider.get_block_receipts(num),
    );
    if result.is_err() {
        error!("{:?}", num);
        error!("{:?}", result);
    }

    result
}

async fn insert_block(
    client: &Client,
    provider: &Provider<Ws>,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let mut block_row_list = Vec::with_capacity((1_u64) as usize);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut withdraw_row_list = Vec::new();

    let (block, receipts) = Retry::spawn(
        ExponentialBackoff::from_millis(100).map(jitter).take(3),
        || get_block_and_tx(&provider, block_number),
    )
    .await?;

    let block = block.unwrap();

    let block_row = BlockRow {
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
    };
    block_row_list.push(block_row);

    for (transaction_index, transaction) in block.transactions.iter().enumerate() {
        let receipt = &receipts[transaction_index];

        let transaction_row = TransactionRow {
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
        };
        transaction_row_list.push(transaction_row);

        for log in &receipt.logs {
            let mut event_row = EventRow {
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
            };
            event_row_list.push(event_row);
        }
    }

    if let Some(withdraws) = &block.withdrawals {
        for withdraw in withdraws {
            let withdraw_row = WithdrawalRow {
                blockHash: block.hash.unwrap().0.to_vec().into(),
                blockNumber: block.number.unwrap().as_u64(),
                blockTimestamp: u256(block.timestamp.into()),
                index: withdraw.index.as_u64(),
                validatorIndex: withdraw.validator_index.as_u64(),
                address: withdraw.address.0.to_vec().into(),
                amount: u256(withdraw.amount.into()),
            };
            withdraw_row_list.push(withdraw_row);
        }
    }

    let results = tokio::try_join!(
        client.insert_native_block("INSERT INTO ethereum.blocks FORMAT native", block_row_list),
        client.insert_native_block(
            "INSERT INTO ethereum.transactions FORMAT native",
            transaction_row_list
        ),
        client.insert_native_block("INSERT INTO ethereum.events FORMAT native", event_row_list),
        client.insert_native_block(
            "INSERT INTO ethereum.withdraws FORMAT native",
            withdraw_row_list
        )
    )
    .unwrap();

    Ok(())
}

async fn handle_block(client: Client, provider: Provider<Ws>, block: Block<H256>) {
    let num = block.number.unwrap().as_u64();
    tokio::try_join!(
        client.execute(format!(
            "DELETE TABLE FROM ethereum.blocks WHERE number = {} ",
            num
        )),
        client.execute(format!(
            "DELETE TABLE FROM ethereum.transactions WHERE blockNumber = {}') ",
            num
        )),
        client.execute(format!(
            "DELETE TABLE FROM ethereum.events WHERE blockNumber = {}') ",
            num
        )),
        client.execute(format!(
            "DELETE TABLE FROM ethereum.withdraws WHERE blockNumber = {}",
            num
        ))
    )
    .ok();

    insert_block(&client, &provider, num).await.unwrap();
    warn!("inserted block {}", num)
}

async fn listen_updates(client: Client, provider: Provider<Ws>) {
    // if in db, update it
    // https://clickhouse.com/docs/en/guides/developer/deduplication
    let mut stream = provider.subscribe_blocks().await.unwrap();

    while let Some(block) = stream.next().await {
        // handle blocks
        warn!(
            "new block {:#032x} @ {}",
            block.hash.unwrap(),
            block.number.unwrap()
        );
        handle_block(client.clone(), provider.clone(), block).await;
    }
}

async fn interval_health_check(client: Client, provider: Provider<Ws>) -> Result<(), Box<dyn Error>> {
    #[derive(Row, Clone, Debug)]
    struct MaxNumberRow {
        max: u64,
    }

    debug!("start interval update");
    let local_height = client
        .query_one::<MaxNumberRow>("SELECT max(number) as max FROM ethereum.blocks")
        .await?;
    info!("local height {}", local_height.max);
    let latest: u64 = provider.get_block_number().await?.as_u64();
    info!("updating to height {}", latest);
    // let from = local_height.max + 1;
    let from = latest - 100_000;

    #[derive(Row, Clone, Debug)]
    struct BlockHashRow {
        hash: String,
    }

    async fn health_check(client: Client, provider: Provider<Ws>, num: u64) {
        let block = client.query_one::<BlockHashRow>(format!("SELECT hex(hash) FROM ethereum.blocks WHERE number = {}", num)).await;
        if block.is_err() {
            warn!("add missing block: {}, {:?}", num, block);
            insert_block(&client, &provider, num).await.unwrap();
        } else { 
            let block =  block.unwrap();
            let block_on_chain = provider.get_block(num).await.unwrap().unwrap();
            if format!("0x{}", block.hash.to_lowercase()) != format!("{:#032x}", block_on_chain.hash.unwrap()) {
                warn!("fix err block {}: {:?} != {:?}", num,  format!("0x{}", block.hash.to_lowercase()),  format!("{:#032x}", block_on_chain.hash.unwrap()));
                tokio::try_join!(
                    client.execute(format!(
                        "DELETE TABLE FROM ethereum.blocks WHERE number = {} ",
                        num
                    )),
                    client.execute(format!(
                        "DELETE TABLE FROM ethereum.transactions WHERE blockNumber = {}') ",
                        num
                    )),
                    client.execute(format!(
                        "DELETE TABLE FROM ethereum.events WHERE blockNumber = {}') ",
                        num
                    )),
                    client.execute(format!(
                        "DELETE TABLE FROM ethereum.withdraws WHERE blockNumber = {}",
                        num
                    ))
                )
                .ok();
                insert_block(&client, &provider, num).await.unwrap();
            }
        }
    }

    for num in (from..=latest).rev() {
        health_check(client.clone(), provider.clone(), num).await;
    }

    Ok(())
}

pub(crate) async fn sync_ws(clickhouse_url: Url, clickhouse_options: ClientOptions, provider_uri: String) -> Result<(), Box<dyn Error>> {
    debug!("start listening");

    let provider_for_listen = Provider::<Ws>::connect(&provider_uri).await?;

    let clickhouse_client_for_listen = Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        clickhouse_options.clone(),
    )
    .await?;

    tokio::spawn(listen_updates(clickhouse_client_for_listen, provider_for_listen));

    let mut interval = tokio::time::interval(Duration::from_secs(60 * 60 * 12));
    loop {
        interval.tick().await;
        let clickhouse_client_for_health = Client::connect(
            format!(
                "{}:{}",
                clickhouse_url.host().unwrap(),
                clickhouse_url.port().unwrap()
            ),
            clickhouse_options.clone(),
        )
        .await?;

        let provider_for_health = Provider::<Ws>::connect(&provider_uri).await?;

        interval_health_check(clickhouse_client_for_health, provider_for_health).await?;
    }

    // Ok(())
}
