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
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

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

    let klient = klickhouse::Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        options.clone(),
    )
    .await?;

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

    debug!("start initializing schema");
    klient
        .execute(format!("CREATE DATABASE IF NOT EXISTS {}", options.default_database).as_str())
        .await
        .unwrap();
    klient.execute(BlockRow::DOCS).await.unwrap();
    klient.execute(TransactionRow::DOCS).await.unwrap();
    klient.execute(EventRow::DOCS).await.unwrap();
    klient.execute(WithdrawalRow::DOCS).await.unwrap();
    klient.execute(TraceRow::DOCS).await.unwrap();
    debug!("schema initialized");

    let latest: u64 = provider.get_block_number().await?.as_u64();
    let to = latest / 1_000 * 1_000;

    warn!("target: {}", to);

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(3); // limit to 3 retries

    let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
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
                klient.insert_native_block(
                    "INSERT INTO events FORMAT native",
                    event_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO withdraws FORMAT native",
                    withdraw_row_list.to_vec()
                ),
                klient.insert_native_block(
                    "INSERT INTO traces FORMAT native",
                    trace_row_list.to_vec()
                )
            )
            .unwrap();

            block_row_list.clear();
            transaction_row_list.clear();
            event_row_list.clear();
            withdraw_row_list.clear();

            info!("{} done blocks & txs", num)
        }

        tokio::try_join!(
            klient.insert_native_block("INSERT INTO blocks FORMAT native", block_row_list.to_vec()),
            klient.insert_native_block(
                "INSERT INTO transactions FORMAT native",
                transaction_row_list.to_vec()
            ),
            klient.insert_native_block("INSERT INTO events FORMAT native", event_row_list.to_vec()),
            klient.insert_native_block(
                "INSERT INTO withdraws FORMAT native",
                withdraw_row_list.to_vec()
            ),
            klient.insert_native_block("INSERT INTO traces FORMAT native", trace_row_list.to_vec())
        )
        .unwrap();
    }

    Ok(())
}
