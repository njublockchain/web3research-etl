use std::{error::Error, time::Duration};

use ethers::{
    providers::{Http, Middleware, Provider, StreamExt, Ws},
    types::{Block, H256},
};
use klickhouse::{Client, ClientOptions, Row};
use log::{debug, error, info, warn};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use url::Url;

use crate::clickhouse_scheme::ethereum::{
    BlockRow, EventRow, TraceRow, TransactionRow, WithdrawalRow,
};

use super::init::get_block_details;

async fn insert_block(
    client: &Client,
    provider: &Provider<Ws>,
    trace_provider: &Provider<Http>,
    block_number: u64,
) -> Result<(), Box<dyn Error>> {
    let mut block_row_list = Vec::with_capacity((1_u64) as usize);
    let mut transaction_row_list = Vec::new();
    let mut event_row_list = Vec::new();
    let mut withdraw_row_list = Vec::new();

    let mut trace_row_list = Vec::new();

    let (block, receipts, traces) = Retry::spawn(
        ExponentialBackoff::from_millis(100).map(jitter).take(3),
        || get_block_details(&provider, &trace_provider, block_number),
    )
    .await?;

    let block = block.unwrap();

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

    for (index, trace) in traces.into_iter().enumerate() {
        let trace_row = TraceRow::from_ethers(&block, &trace, index);
        trace_row_list.push(trace_row);
    }

    tokio::try_join!(
        client.insert_native_block("INSERT INTO ethereum.blocks FORMAT native", block_row_list),
        client.insert_native_block(
            "INSERT INTO ethereum.transactions FORMAT native",
            transaction_row_list
        ),
        client.insert_native_block("INSERT INTO ethereum.events FORMAT native", event_row_list),
        client.insert_native_block(
            "INSERT INTO ethereum.withdraws FORMAT native",
            withdraw_row_list
        ),
        client.insert_native_block("INSERT INTO ethereum.traces FORMAT native", trace_row_list)
    )?;

    Ok(())
}

async fn handle_block(
    client: Client,
    provider: &Provider<Ws>,
    trace_provider: &Provider<Http>,
    block: Block<H256>,
) {
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
        )),
        client.execute(format!(
            "DELETE TABLE FROM ethereum.traces WHERE blockNumber = {}",
            num
        )),
    )
    .ok();

    insert_block(&client, &provider, &trace_provider, num)
        .await
        .unwrap();
    warn!("inserted block {}", num)
}

async fn listen_updates(client: Client, provider: Provider<Ws>, trace_provider: Provider<Http>) {
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
        handle_block(client.clone(), &provider, &trace_provider, block).await;
    }
}

#[derive(Row, Clone, Debug)]
struct BlockHashRow {
    hash: String,
}

async fn health_check(
    client: Client,
    provider: &Provider<Ws>,
    trace_provider: &Provider<Http>,
    num: u64,
) {
    let block = client
        .query_one::<BlockHashRow>(format!(
            "SELECT hex(hash) FROM ethereum.blocks WHERE number = {}",
            num
        ))
        .await;
    if block.is_err() {
        warn!("add missing block: {}, {:?}", num, block);
        insert_block(&client, &provider, trace_provider, num)
            .await
            .unwrap();
    } else {
        let block = block.unwrap();
        let block_on_chain = provider.get_block(num).await.unwrap().unwrap();
        if format!("0x{}", block.hash.to_lowercase())
            != format!("{:#032x}", block_on_chain.hash.unwrap())
        {
            warn!(
                "fix err block {}: {:?} != {:?}",
                num,
                format!("0x{}", block.hash.to_lowercase()),
                format!("{:#032x}", block_on_chain.hash.unwrap())
            );
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
                )),
                client.execute(format!(
                    "DELETE TABLE FROM ethereum.traces WHERE blockNumber = {}",
                    num
                ))
            )
            .ok(); // ignore error

            insert_block(&client, &provider, trace_provider, num)
                .await
                .unwrap();
        }
    }
}

async fn interval_health_check(
    client: Client,
    provider: &Provider<Ws>,
    trace_provider: &Provider<Http>,
) -> Result<(), Box<dyn Error>> {
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

    for num in (from..=latest).rev() {
        health_check(client.clone(), provider, trace_provider, num).await;
    }

    Ok(())
}

pub(crate) async fn sync(
    clickhouse_url: Url,
    clickhouse_options: ClientOptions,
    provider_uri: String,
    trace_provider_uri: String,
) -> Result<(), Box<dyn Error>> {
    debug!("start listening");

    let provider_for_listen = Provider::<Ws>::connect(&provider_uri).await?;
    let trace_provider_for_listen = Provider::try_from(&trace_provider_uri)?;

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
    ));

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
        let trace_provider_for_health = Provider::try_from(&trace_provider_uri)?;

        interval_health_check(
            clickhouse_client_for_health,
            &provider_for_health,
            &trace_provider_for_health,
        )
        .await?;
    }

    // Ok(())
}
