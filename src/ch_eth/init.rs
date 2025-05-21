use std::error::Error;

use documented::Documented;
use ethers::{
    providers::ProviderError,
    types::{Block, Transaction, TransactionReceipt},
};
use log::{debug, error, info, warn};
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

// Helper function to insert batch data into ClickHouse
pub async fn insert_batch_data(
    client: &clickhouse::Client,
    block_rows: &[BlockRow],
    transaction_rows: &[TransactionRow],
    event_rows: &[EventRow],
    withdraw_rows: &[WithdrawalRow],
    trace_rows: &[TraceRow],
) -> Result<(), Box<dyn Error>> {
    if !transaction_rows.is_empty() {
        let mut inserter = client.insert("transactions")?;
        for row in transaction_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    warn!("transaction rows: {}", transaction_rows.len());

    if !event_rows.is_empty() {
        let mut inserter = client.insert("events")?;
        for row in event_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    warn!("event rows: {}", event_rows.len());

    if !withdraw_rows.is_empty() {
        let mut inserter = client.insert("withdrawals")?;
        for row in withdraw_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    if !trace_rows.is_empty() {
        let mut inserter = client.insert("traces")?;
        for row in trace_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

    warn!("trace rows: {}", trace_rows.len());

    if !block_rows.is_empty() {
        let mut inserter = client.insert("blocks")?;
        for row in block_rows {
            inserter.write(row).await?;
        }
        inserter.end().await?;
    }

        warn!("block rows: {}", block_rows.len());

    Ok(())
}

pub async fn get_block_details(
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    with_get_block_receipts_rpc: bool,
    num: u64,
) -> Result<
    (
        Block<Transaction>,
        Vec<TransactionReceipt>,
        Option<Vec<ethers::types::Trace>>,
    ),
    ProviderError,
> {
    let get_traces = async move {
        if let Some(trace_provider) = trace_provider {
            let result = trace_provider.trace_block(num.into()).await?;
            return Ok(Some(result));
        } else {
            return Ok(None);
        }
    };

    let get_block_receipts = async move {
        if num == 0 {
            return Ok::<Vec<TransactionReceipt>, ProviderError>(Vec::new());
        } else {
            let result = provider
                .get_block_receipts(ethers::types::BlockNumber::from(num))
                .await?;
            return Ok(result);
        }
    };

    // geth style has no get_block_receipts rpc
    if with_get_block_receipts_rpc {
        let result = tokio::try_join!(
            provider.get_block_with_txs(num),
            get_block_receipts,
            get_traces,
        );

        result.and_then(|result| Ok((result.0.unwrap(), result.1, result.2)))
    } else {
        let result = tokio::try_join!(provider.get_block_with_txs(num), get_traces,);

        match result {
            Ok(result) => {
                // let mut set = JoinSet::new();
                let mut receipts = Vec::new();

                let txs = result.0.clone().unwrap().transactions;
                for tx in txs {
                    // let provider = provider.clone();
                    // set.spawn(async move {

                    let receipt = provider
                        .get_transaction_receipt(tx.hash)
                        .await
                        .unwrap_or_else(|e| {
                            error!("{}", e);
                            None
                        })
                        .unwrap();
                    // });

                    receipts.push(receipt);
                }

                // while let Some(res) = set.join_next().await {
                //     let receipt = res.unwrap();

                // }

                Ok((result.0.clone().unwrap(), receipts, result.1))
            }
            Err(err) => Err(err),
        }
    }
}

pub(crate) async fn init(
    db: String,
    provider_url: String,
    trace_provider_url: Option<String>,
    trace_provider_type: ProviderType,
    from: u64,
    batch_size: u64,
) -> Result<(), Box<dyn Error>> {
    // Create connection to ClickHouse using the official library
    let parsed_db_url = Url::parse(&db).unwrap();
    if parsed_db_url.scheme() != "http" && parsed_db_url.scheme() != "https" {
        return Err("Invalid ClickHouse URL scheme, must be http or https in V2".into());
    }

    let database = parsed_db_url.path().strip_prefix('/').unwrap_or("ethereum");
    let clickhouse_database = if database.is_empty() {
        "ethereum"
    } else {
        database
    };
    let clickhouse_url = format!(
        "{}://{}:{}",
        parsed_db_url.scheme(),
        parsed_db_url.host_str().unwrap(),
        parsed_db_url.port().unwrap_or(8123)
    );
    let clickhouse_username = parsed_db_url.username();
    let clickhouse_password = parsed_db_url.password().unwrap_or("");
    let client = clickhouse::Client::default()
        .with_url(clickhouse_url)
        .with_user(clickhouse_username)
        .with_password(clickhouse_password)
        .with_database(clickhouse_database);

    // Create provider directly based on URL type (WS or HTTP)
    let provider = create_provider(&provider_url).await?;
    info!(
        "Created main provider of type: {} for URL: {}",
        provider.provider_type(),
        provider_url
    );

    // Create trace provider if URL is provided
    let trace_provider = match trace_provider_url {
        Some(url) => {
            let provider = create_provider(&url).await?;
            info!(
                "Created trace provider of type: {}",
                provider.provider_type()
            );
            Some(provider)
        }
        None => None,
    };

    info!("start initializing schema");

    // Create database if it doesn't exist
    client
        .query("CREATE DATABASE IF NOT EXISTS ethereum;")
        // .bind(database)
        .with_option("wait_end_of_query", "1")
        .execute()
        .await?;
    info!("database created");

    // Create tables using the SQL definitions from the code comments
    client.query(BlockRow::DOCS).execute().await?;
    client.query(TransactionRow::DOCS).execute().await?;
    client.query(EventRow::DOCS).execute().await?;
    client.query(WithdrawalRow::DOCS).execute().await?;
    client.query(TraceRow::DOCS).execute().await?;

    info!("schema initialized");

    let latest: u64 = provider.get_block_number().await?.as_u64();
    let to = latest / 1_000 * 1_000;

    warn!("target: {}", to);

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    let mut block_row_list = Vec::with_capacity((batch_size + 1_u64) as usize);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut withdraw_row_list = Vec::new();
    let mut trace_row_list = Vec::new();

    for num in from..=to {
        let (block, receipts, traces) = Retry::spawn(retry_strategy.clone(), || {
            get_block_details(
                &provider,
                &trace_provider,
                trace_provider_type == ProviderType::Erigon,
                num,
            )
        })
        .await?;

        let block = &block;

        let block_row = BlockRow::from_ethers(block);
        block_row_list.push(block_row);

        for (transaction_index, transaction) in block.transactions.iter().enumerate() {
            let receipt = &receipts[transaction_index];

            let transaction_row = TransactionRow::from_ethers(block, transaction, receipt);
            transaction_row_list.push(transaction_row);

            for log in &receipt.logs {
                let event_row = EventRow::from_ethers(block, transaction, log);
                event_row_list.push(event_row);
            }
        }

        if let Some(withdraws) = &block.withdrawals {
            for withdraw in withdraws {
                let withdraw_row = WithdrawalRow::from_ethers(block, withdraw);
                withdraw_row_list.push(withdraw_row);
            }
        }

        if let Some(traces) = traces {
            for (index, trace) in traces.into_iter().enumerate() {
                let trace_row = TraceRow::from_ethers(block, &trace, index);
                trace_row_list.push(trace_row);
            }
        }

        if block_row_list.len() >= batch_size as usize || num == to {
            if !block_row_list.is_empty() {
                // Ensure there's data to insert
                insert_batch_data(
                    &client,
                    &block_row_list,
                    &transaction_row_list,
                    &event_row_list,
                    &withdraw_row_list,
                    &trace_row_list,
                )
                .await?;

                info!(
                    "Inserted batch of {} blocks (up to number {})",
                    block_row_list.len(),
                    num
                );

                block_row_list.clear();
                transaction_row_list.clear();
                event_row_list.clear();
                withdraw_row_list.clear();
                trace_row_list.clear();
            }
        }
    }

    info!("ETH initialization complete!");
    Ok(())
}
