use std::error::Error;

use ethers::providers::{Provider, Ws, Middleware};
use klickhouse::{Client, Row, ClientOptions};
use log::{info, debug};
use url::Url;

use crate::clickhouse_eth::sync::health_check;

pub(crate) async fn check(
    clickhouse_url: Url,
    clickhouse_options: ClientOptions,
    provider_uri: String,
    trace_provider_uri: String,
    from: u64
) -> Result<(), Box<dyn Error>> {
    debug!("start listening");

    let provider = Provider::<Ws>::connect(&provider_uri).await?;
    let trace_provider = Provider::try_from(&trace_provider_uri)?;

    let client = Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        clickhouse_options.clone(),
    )
    .await?;

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

    for num in from..=latest {
        health_check(client.clone(), &provider, &trace_provider, num).await;
    }

    Ok(())
}
