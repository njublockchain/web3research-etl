use ethers::providers::{Provider, Ws};
use klickhouse::{Client, ClientOptions};
use std::error::Error;
use url::Url;

use crate::ClapActionType;

use super::{init, sync};

extern crate pretty_env_logger;

pub async fn main(
    clickhouse_uri: &str,
    eth_uri: &str,
    action_type: &ClapActionType,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(clickhouse_uri).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default" || clickhouse_url.username().len() > 0 {
        ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url.path().to_string(),
        }
    } else {
        ClientOptions::default()
    };

    match action_type {
        ClapActionType::Init {
            from,
            init_trace,
            batch,
        } => {
            let clickhouse_client = Client::connect(
                format!(
                    "{}:{}",
                    clickhouse_url.host().unwrap(),
                    clickhouse_url.port().unwrap()
                ),
                options.clone(),
            )
            .await?;

            let provider_ws = Provider::<Ws>::connect(eth_uri).await?;
            let trace_provider = Provider::try_from(init_trace)?;

            init::init(
                clickhouse_client,
                provider_ws,
                *from,
                trace_provider,
                *batch,
            )
            .await?;
        }
        ClapActionType::Sync { sync_trace } => {
            sync::sync(
                clickhouse_url.clone(),
                options.clone(),
                eth_uri.to_owned(),
                sync_trace.to_owned(),
            )
            .await?;
        }
        ClapActionType::GraphQL {} => todo!(),
    }

    Ok(())
}
