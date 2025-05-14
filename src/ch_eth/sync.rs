use clickhouse::Row;
use ethers::providers::StreamExt;
use log::{debug, error, info, warn};
use std::error::Error;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use url::Url;

use crate::{
    ch_eth::{
        schema::{BlockRow, EventRow, TraceRow, TransactionRow, WithdrawalRow},
        utils::{create_provider, EthProvider},
    },
    ProviderType,
};

use super::init::{get_block_details, insert_batch_data};

async fn insert_block(
    client: &clickhouse::Client,
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    provider_type: ProviderType,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let mut block_row_list = Vec::with_capacity(1);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut withdraw_row_list = Vec::new();
    let mut trace_row_list = Vec::new();

    let (block, receipts, traces) = Retry::spawn(
        ExponentialBackoff::from_millis(100).map(jitter).take(3),
        || {
            get_block_details(
                provider,
                trace_provider,
                provider_type == ProviderType::Erigon,
                block_number,
            )
        },
    )
    .await?;

    let block_row = BlockRow::from_ethers(&block);
    block_row_list.push(block_row);

    for (transaction_index, transaction) in block.transactions.iter().enumerate() {
        let receipt = &receipts[transaction_index];

        let transaction_row = TransactionRow::from_ethers(&block, transaction, receipt);
        transaction_row_list.push(transaction_row);

        for log in &receipt.logs {
            let event_row = EventRow::from_ethers(&block, transaction, log);
            event_row_list.push(event_row);
        }
    }

    if let Some(withdrawals) = &block.withdrawals {
        for withdraw in withdrawals {
            let withdraw_row = WithdrawalRow::from_ethers(&block, withdraw);
            withdraw_row_list.push(withdraw_row);
        }
    }

    if let Some(traces) = traces {
        for (index, trace) in traces.into_iter().enumerate() {
            let trace_row = TraceRow::from_ethers(&block, &trace, index);
            trace_row_list.push(trace_row);
        }
    }

    // Insert data separately instead of using try_join to avoid issues with await
    insert_batch_data(
        client,
        &block_row_list,
        &transaction_row_list,
        &event_row_list,
        &withdraw_row_list,
        &trace_row_list,
    )
    .await?;
    debug!("Inserted block {} into ClickHouse", block_number);

    Ok(())
}

#[derive(Row, Clone, Debug, serde::Deserialize)]
struct BlockNumberRow {
    number: u64,
}

/// check if the block data has been inserted into ClickHouse
/// if not, fetch the block data from the provider and insert it
/// into ClickHouse
pub async fn health_check(
    client: &clickhouse::Client,
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    provider_type: ProviderType,
    num: u64,
) -> Result<(), Box<dyn Error>> {
    let result = client
        .query("SELECT number FROM blocks WHERE number = ?")
        .bind(num)
        .fetch_one::<BlockNumberRow>()
        .await;

    if result.is_err() {
        warn!("Block {} not found, inserting...", num);
        insert_block(client, provider, trace_provider, provider_type, num).await?;
    }

    Ok(())
}

pub(crate) async fn sync(
    db: String,
    provider_ws: String,
    provider_http: Option<String>,
    provider_type: ProviderType,
) -> Result<(), Box<dyn Error>> {
    // Create a client with the given database
    let client = clickhouse::Client::default().with_url(&db).with_database(
        Url::parse(&db)
            .unwrap()
            .path()
            .strip_prefix('/')
            .unwrap_or("default"),
    );

    debug!("start listening");

    // Create provider directly based on URL type (WS or HTTP)
    let provider_for_listen = create_provider(&provider_ws).await?;
    info!(
        "Created main provider of type: {}",
        provider_for_listen.provider_type()
    );

    // Create trace provider directly based on URL type (WS or HTTP)
    let trace_provider_for_listen = match provider_http.clone() {
        Some(http_url) => {
            let provider = create_provider(&http_url).await?;
            info!(
                "Created trace provider of type: {}",
                provider.provider_type()
            );
            Some(provider)
        }
        None => None,
    };

    let latest = match provider_for_listen.get_block_number().await {
        Ok(num) => num.as_u64(),
        Err(err) => {
            error!("Failed to get latest block: {}", err);
            return Err(Box::new(err));
        }
    };

    info!("Starting sync from block: {}", latest);

    // Get the Stream of new blocks
    let mut stream = match provider_for_listen.subscribe_blocks().await {
        Ok(stream) => stream,
        Err(err) => {
            error!("Failed to subscribe to blocks: {}", err);
            return Err(Box::new(err));
        }
    };

    info!("Successfully subscribed to new blocks");

    loop {
        match stream.next().await {
            Some(block) => {
                let block_number = block.number.unwrap().as_u64();
                info!("Got new block: {}", block_number);
                insert_block(
                    &client,
                    &provider_for_listen,
                    &trace_provider_for_listen,
                    provider_type,
                    block_number,
                )
                .await?;
                info!("Inserted block {} into ClickHouse", block_number);
            }
            None => {
                warn!("Block stream ended");
                break;
            }
        }
    }

    Ok(())
}
