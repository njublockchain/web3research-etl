use ethers::{
    providers::StreamExt,
    types::{Block, H256}, utils::hex::ToHexExt,
};
use klickhouse::{Client, ClientOptions, Row};
use log::{debug, error, info, warn};
use std::{error::Error, time::Duration};
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

use super::init::get_block_details;

async fn insert_block(
    client: &Client,
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    provider_type: ProviderType,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let mut block_row_list = Vec::with_capacity((1_u64) as usize);
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

    if let Some(withdraws) = &block.withdrawals {
        for withdraw in withdraws {
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

    tokio::try_join!(
        client.insert_native_block("INSERT INTO blocks FORMAT native", block_row_list),
        client.insert_native_block(
            "INSERT INTO transactions FORMAT native",
            transaction_row_list
        ),
        client.insert_native_block("INSERT INTO events FORMAT native", event_row_list),
        client.insert_native_block("INSERT INTO withdraws FORMAT native", withdraw_row_list),
        client.insert_native_block("INSERT INTO traces FORMAT native", trace_row_list)
    )?;

    Ok(())
}

async fn handle_block(
    client: Client,
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    provider_type: ProviderType,
    block: Block<H256>,
) {
    let num = block.number.unwrap().as_u64();
    tokio::try_join!(
        client.execute(format!("DELETE FROM blocks WHERE number = {} ", num)),
        client.execute(format!(
            "DELETE FROM transactions WHERE blockNumber = {}') ",
            num
        )),
        client.execute(format!("DELETE FROM events WHERE blockNumber = {}') ", num)),
        client.execute(format!("DELETE FROM withdraws WHERE blockNumber = {}", num)),
        client.execute(format!("DELETE FROM traces WHERE blockNumber = {}", num)),
    )
    .ok();

    insert_block(&client, provider, trace_provider, provider_type, num)
        .await
        .unwrap();
    warn!("inserted block {}", num)
}

async fn listen_updates(
    client: Client,
    provider: EthProvider,
    trace_provider: Option<EthProvider>,
    provider_type: ProviderType,
) {
    // if in db, update it
    // https://clickhouse.com/docs/en/guides/developer/deduplication
    debug!("start listening to new blocks");

    // Try to subscribe to blocks if provider supports it (WebSocket)
    match provider.subscribe_blocks().await {
        Ok(mut stream) => {
            while let Some(block) = stream.next().await {
                // handle blocks
                warn!(
                    "new block {:#032x} @ {}",
                    block.hash.unwrap(),
                    block.number.unwrap()
                );
                handle_block(
                    client.clone(),
                    &provider,
                    &trace_provider,
                    provider_type,
                    block,
                )
                .await;
            }
        }
        Err(e) => {
            // If subscription is not supported (HTTP provider), poll for blocks periodically
            warn!(
                "Block subscription not supported: {}, falling back to polling",
                e
            );
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_block_number = 0;

            loop {
                interval.tick().await;
                match provider.get_block_number().await {
                    Ok(current_block) => {
                        let current_block_number = current_block.as_u64();
                        if current_block_number > last_block_number {
                            for block_num in (last_block_number + 1)..=current_block_number {
                                if let Ok(Some(block)) = provider.get_block(block_num).await {
                                    warn!(
                                        "new block {:#032x} @ {}",
                                        block.hash.unwrap(),
                                        block.number.unwrap()
                                    );
                                    handle_block(
                                        client.clone(),
                                        &provider,
                                        &trace_provider,
                                        provider_type,
                                        block,
                                    )
                                    .await;
                                }
                            }
                            last_block_number = current_block_number;
                        }
                    }
                    Err(e) => error!("Failed to get block number: {}", e),
                }
            }
        }
    }
}

#[derive(Row, Clone, Debug)]
struct BlockHashRow {
    hash: String,
}

#[derive(Row, Clone, Debug)]
struct CountRaw {
    count: u64,
}

pub async fn health_check(
    client: Client,
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    provider_type: ProviderType,
    num: u64,
) {
    let block = client
        .query_one::<BlockHashRow>(format!(
            "SELECT hash FROM blocks WHERE number = {}",
            num
        ))
        .await;
    if block.is_err() {
        warn!("add missing block: {}, {:?}", num, block);
        insert_block(&client, provider, trace_provider, provider_type, num)
            .await
            .unwrap();
    } else {
        let block = block.unwrap();
        let block_on_chain = provider.get_block(num).await.unwrap().unwrap();
        let block_hash_on_chain = block_on_chain.hash.unwrap().encode_hex_with_prefix();
        if block.hash != block_hash_on_chain {
            warn!(
                "fix err block {}: {:?} != {:?}",
                num, block.hash, block_hash_on_chain
            );
            tokio::try_join!(
                client.execute(format!("DELETE FROM blocks WHERE number = {} ", num)),
                client.execute(format!(
                    "DELETE FROM transactions WHERE blockNumber = {}') ",
                    num
                )),
                client.execute(format!("DELETE FROM events WHERE blockNumber = {}') ", num)),
                client.execute(format!("DELETE FROM withdraws WHERE blockNumber = {}", num)),
                client.execute(format!("DELETE FROM traces WHERE blockNumber = {}", num))
            )
            .ok(); // ignore error

            insert_block(&client, provider, trace_provider, provider_type, num)
                .await
                .unwrap();
        } else {
            // check transactions
            let block_events_count = client
                .query_one::<CountRaw>(format!(
                    "SELECT count(*) as count FROM transactions WHERE blockNumber = {}",
                    num
                ))
                .await;
            match block_events_count {
                Ok(c) => {
                    if c.count == 0 {
                        warn!("fix err block {}: no events", num);
                        tokio::try_join!(
                            client.execute(format!("DELETE FROM blocks WHERE number = {} ", num)),
                            client.execute(format!(
                                "DELETE FROM transactions WHERE blockNumber = {}') ",
                                num
                            )),
                            client.execute(format!(
                                "DELETE FROM events WHERE blockNumber = {}') ",
                                num
                            )),
                            client.execute(format!(
                                "DELETE FROM withdraws WHERE blockNumber = {}",
                                num
                            )),
                            client
                                .execute(format!("DELETE FROM traces WHERE blockNumber = {}", num))
                        )
                        .ok(); // ignore error

                        insert_block(&client, provider, trace_provider, provider_type, num)
                            .await
                            .unwrap();
                        return;
                    }
                }
                Err(e) => {
                    error!("{}", e)
                }
            }

            // check events
            let block_events_count = client
                .query_one::<CountRaw>(format!(
                    "SELECT count(*) as count FROM events WHERE blockNumber = {}",
                    num
                ))
                .await;
            match block_events_count {
                Ok(c) => {
                    if c.count == 0 {
                        warn!("fix err block {}: no events", num);
                        tokio::try_join!(
                            client.execute(format!("DELETE FROM blocks WHERE number = {} ", num)),
                            client.execute(format!(
                                "DELETE FROM transactions WHERE blockNumber = {}') ",
                                num
                            )),
                            client.execute(format!(
                                "DELETE FROM events WHERE blockNumber = {}') ",
                                num
                            )),
                            client.execute(format!(
                                "DELETE FROM withdraws WHERE blockNumber = {}",
                                num
                            )),
                            client
                                .execute(format!("DELETE FROM traces WHERE blockNumber = {}", num))
                        )
                        .ok(); // ignore error

                        insert_block(&client, provider, trace_provider, provider_type, num)
                            .await
                            .unwrap();
                        return;
                    }
                }
                Err(e) => {
                    error!("{}", e)
                }
            }

            // check traces
            let block_trace_count = client
                .query_one::<CountRaw>(format!(
                    "SELECT count(*) as count FROM traces WHERE blockNumber = {}",
                    num
                ))
                .await;
            match block_trace_count {
                Ok(c) => {
                    if c.count == 0 {
                        warn!("fix err block {}: no traces", num);
                        tokio::try_join!(
                            client.execute(format!("DELETE FROM blocks WHERE number = {} ", num)),
                            client.execute(format!(
                                "DELETE FROM transactions WHERE blockNumber = {}') ",
                                num
                            )),
                            client.execute(format!(
                                "DELETE FROM events WHERE blockNumber = {}') ",
                                num
                            )),
                            client.execute(format!(
                                "DELETE FROM withdraws WHERE blockNumber = {}",
                                num
                            )),
                            client
                                .execute(format!("DELETE FROM traces WHERE blockNumber = {}", num))
                        )
                        .ok(); // ignore error

                        insert_block(&client, provider, trace_provider, provider_type, num)
                            .await
                            .unwrap();
                        return;
                    }
                }
                Err(e) => {
                    error!("{}", e)
                }
            }
        }
    }
}

async fn interval_health_check(
    client: Client,
    provider: &EthProvider,
    trace_provider: &Option<EthProvider>,
    provider_type: ProviderType,
) -> Result<(), Box<dyn Error>> {
    #[derive(Row, Clone, Debug)]
    struct MaxNumberRow {
        max: u64,
    }

    debug!("start interval update");
    let local_height = client
        .query_one::<MaxNumberRow>("SELECT max(number) as max FROM blocks")
        .await?;
    info!("local height {}", local_height.max);
    let latest: u64 = provider.get_block_number().await?.as_u64();
    info!("updating to height {}", latest);
    // let from = local_height.max + 1;
    let from = latest - 100_000;

    for num in (from..=latest).rev() {
        health_check(client.clone(), provider, trace_provider, provider_type, num).await;
    }

    Ok(())
}

pub(crate) async fn sync(
    db: String,
    provider_ws: String,
    provider_http: Option<String>,
    provider_type: ProviderType,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let clickhouse_options =
        if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
            ClientOptions {
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
            ClientOptions::default()
        };

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

    let clickhouse_client_for_listen = Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        clickhouse_options.clone(),
    )
    .await?;

    tokio::spawn(listen_updates(
        clickhouse_client_for_listen,
        provider_for_listen,
        trace_provider_for_listen,
        provider_type,
    ));

    let mut interval = tokio::time::interval(Duration::from_secs(60 * 60 * 4));
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

        // Create provider directly based on URL type (WS or HTTP)
        let provider_for_health = create_provider(&provider_ws).await?;
        debug!(
            "Created health check provider of type: {}",
            provider_for_health.provider_type()
        );

        // Create trace provider directly based on URL type (WS or HTTP)
        let trace_provider_for_health = match provider_http.clone() {
            Some(http_url) => {
                let provider = create_provider(&http_url).await?;
                debug!(
                    "Created health check trace provider of type: {}",
                    provider.provider_type()
                );
                Some(provider)
            }
            None => None,
        };

        interval_health_check(
            clickhouse_client_for_health,
            &provider_for_health,
            &trace_provider_for_health,
            provider_type,
        )
        .await?;
    }

    // Ok(())
}
