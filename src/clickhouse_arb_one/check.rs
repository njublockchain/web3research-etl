use std::error::Error;

use ethers::providers::{Middleware, Provider, Ws};
use klickhouse::{Client, ClientOptions, Row};
use log::{debug, info};
use url::Url;

use crate::{clickhouse_arb_one::sync::health_check, ProviderType};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    trace_provider_uri: Option<String>,
    provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let clickhouse_options =
        if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
            ClientOptions {
                username: clickhouse_url.username().to_string(),
                password: clickhouse_url.password().unwrap_or("").to_string(),
                default_database: clickhouse_url.path().to_string().strip_prefix('/').unwrap().to_string(),
            }
        } else {
            ClientOptions::default()
        };

    debug!("start listening");

    let provider = Provider::<Ws>::connect(&provider_uri).await?;
    let trace_provider = trace_provider_uri
        .map(|trace_provider_uri| Provider::try_from(&trace_provider_uri).unwrap());

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
        .query_one::<MaxNumberRow>("SELECT max(number) as max FROM blocks")
        .await?;
    info!("local height {}", local_height.max);
    let latest: u64 = provider.get_block_number().await?.as_u64();
    info!("updating to height {}", latest);
    // let from = local_height.max + 1;

    for num in from..=latest {
        health_check(client.clone(), &provider, &trace_provider, provider_type, num).await;
    }

    Ok(())
}
