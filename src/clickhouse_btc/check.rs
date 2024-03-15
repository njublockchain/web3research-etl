use std::error::Error;

use bitcoin::{hashes::Hash, Address};
use bitcoincore_rpc::RpcApi;
use klickhouse::{Client, ClientOptions, Row};
use log::{debug, info, warn};
use url::Url;

use crate::{
    clickhouse_scheme::bitcoin::{BlockRow, InputRow, OutputRow},
    ProviderType, clickhouse_btc::sync::health_check,
};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    trace_provider_uri: Option<String>,
    provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
        warn!("auth enabled for clickhouse");
        klickhouse::ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url.path().to_string().strip_prefix('/').unwrap().to_string(),
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

    let bitcoin_rpc_url = Url::parse(&provider_uri).unwrap();

    let provider = bitcoincore_rpc::Client::new(
        format!(
            "{}://{}:{}",
            bitcoin_rpc_url.scheme(),
            bitcoin_rpc_url.host_str().unwrap_or("localhost"),
            bitcoin_rpc_url.port_or_known_default().unwrap_or(8332),
        )
        .as_str(),
        bitcoincore_rpc::Auth::UserPass(
            bitcoin_rpc_url.username().to_string(),
            bitcoin_rpc_url.password().unwrap_or("").to_string(),
        ),
    )
    .unwrap();

    #[derive(Row, Clone, Debug)]
    struct MaxNumberRow {
        max: u64,
    }

    debug!("start interval update");
    let local_height = klient
        .query_one::<MaxNumberRow>("SELECT max(height) as max FROM blocks")
        .await?;
    info!("local height {}", local_height.max);
    let latest = provider.get_block_count()? - 1;
    info!("checking from {} to height {}", from, latest);

    for num in from..=latest {
        health_check(klient.clone(), &provider, &None, provider_type, num).await;
    }

    Ok(())
}
