use clickhouse::Row;
use log::{debug, info};
use std::error::Error;
use url::Url;

use crate::{
    ch_eth::{sync::health_check, utils::create_provider},
    ProviderType,
};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    trace_provider_uri: Option<String>,
    provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    // Create a client with the given database
    let parsed_url = Url::parse(&db).unwrap();
    let database = parsed_url.path().strip_prefix('/').unwrap_or("default");
    let client = clickhouse::Client::default()
        .with_url(&db)
        .with_database(database);

    debug!("start listening");

    let provider = create_provider(&provider_uri).await?;
    info!(
        "Created check provider of type: {}",
        provider.provider_type()
    );

    let trace_provider = match trace_provider_uri {
        Some(trace_uri) => {
            let provider = create_provider(&trace_uri).await?;
            info!(
                "Created check trace provider of type: {}",
                provider.provider_type()
            );
            Some(provider)
        }
        None => None,
    };

    #[derive(Row, Clone, Debug, serde::Deserialize)]
    struct MaxNumberRow {
        max: u64,
    }

    debug!("start interval update");
    let local_height = client
        .query("SELECT max(number) as max FROM blocks")
        .fetch_one::<MaxNumberRow>()
        .await?;
    info!("local height {}", local_height.max);
    let latest: u64 = provider.get_block_number().await?.as_u64();
    info!("updating to height {}", latest);
    // let from = local_height.max + 1;

    for num in from..=latest {
        health_check(&client, &provider, &trace_provider, provider_type, num).await?;
    }

    Ok(())
}
