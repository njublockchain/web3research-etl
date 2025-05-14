use bitcoincore_rpc::RpcApi;
use clickhouse::Row;
use log::{debug, info};
use serde::Deserialize;
use std::error::Error;
use url::Url;

use crate::{ch_btc::sync::health_check, ProviderType};

pub(crate) async fn check(
    db: String,
    provider_uri: String,
    _trace_provider: Option<String>,
    provider_type: ProviderType,
    from: u64,
) -> Result<(), Box<dyn Error>> {
    // Create a client with the given database
    let client = clickhouse::Client::default().with_url(&db).with_database(
        Url::parse(&db)
            .unwrap()
            .path()
            .strip_prefix('/')
            .unwrap_or("default"),
    );

    let bitcoin_rpc_url = Url::parse(&provider_uri).unwrap();
    let bitcoin_provider_url_owned = format!(
        "{}://{}:{}",
        bitcoin_rpc_url.scheme(),
        bitcoin_rpc_url.host_str().unwrap_or("localhost"),
        bitcoin_rpc_url.port_or_known_default().unwrap_or(8332),
    );
    let bitcoin_provider_userpass = bitcoincore_rpc::Auth::UserPass(
        bitcoin_rpc_url.username().to_string(),
        bitcoin_rpc_url.password().unwrap_or("").to_string(),
    );
    let provider =
        bitcoincore_rpc::Client::new(&bitcoin_provider_url_owned, bitcoin_provider_userpass)
            .unwrap();

    #[derive(Row, Clone, Debug, Deserialize)]
    struct MaxNumberRow {
        max: u64,
    }

    debug!("start interval update");
    debug!("start interval update");
    let local_height = client
        .query("SELECT max(height) as max FROM blocks")
        .fetch_one::<MaxNumberRow>()
        .await?;
    info!("local height {}", local_height.max);
    let latest = provider.get_block_count()? - 1;
    info!("checking from {} to height {}", from, latest);

    for num in from..=latest {
        health_check(&client, &provider, &None, provider_type, num).await;
    }

    Ok(())
}
