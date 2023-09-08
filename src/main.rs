mod clickhouse_btc;
mod clickhouse_eth;
mod clickhouse_scheme;
mod clickhouse_tron;
mod helpers;
use clap::Parser;
use ethers::providers::{Provider, Ws};
use klickhouse::{Client, ClientOptions};
use log::warn;
use std::error::Error;
use url::Url;

extern crate pretty_env_logger;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// action type
    #[command(subcommand)]
    action_type: ClapActionType,
}

#[derive(clap::Subcommand, PartialEq, Eq, Debug)]
pub enum ClapActionType {
    Init {
        /// Name of the person to greet
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        #[arg(long, default_value = "clickhouse://localhost:9000")]
        db: String,

        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// from block
        #[arg(long, default_value_t = 0)]
        from: u64,

        /// provider uri for init trace
        #[arg(long = "trace", default_value = "http://localhost:8545")]
        init_trace: String,

        /// init batch size
        #[arg(long, default_value_t = 1u64)]
        batch: u64,
    },
    Sync {
        /// Name of the person to greet
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        #[arg(long, default_value = "clickhouse://localhost:9000")]
        db: String,

        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// provider uri for sync trace
        #[arg(long = "trace", default_value = "http://localhost:8545")]
        sync_trace: String,
    },
    Check {
        /// from block
        #[arg(long, default_value_t = 0)]
        from: u64,

        /// Name of the person to greet
        #[arg(short, long, value_enum, default_value_t = SupportedChain::Ethereum)]
        chain: SupportedChain,

        #[arg(long, default_value = "clickhouse://default@localhost:9000")]
        db: String,

        #[arg(short, long, default_value = "ws://localhost:8545")]
        provider: String,

        /// provider uri for sync trace
        #[arg(long = "trace", default_value = "http://localhost:8545")]
        sync_trace: String,
    },
}

#[derive(clap::ValueEnum, Clone, PartialEq, Eq, Debug)]
pub enum SupportedChain {
    Ethereum,
    Bitcoin,
    Tron,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init_timed();
    let args = Args::parse();

    match args.action_type {
        ClapActionType::Init {
            chain,
            db,
            provider,
            from,
            init_trace,
            batch,
        } => {
            match chain {
                SupportedChain::Bitcoin => {
                    let clickhouse_url = Url::parse(&db).unwrap();
                    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

                    let options = if clickhouse_url.path() != "/default"
                        || clickhouse_url.username().len() > 0
                    {
                        warn!("auth enabled for clickhouse");
                        ClientOptions {
                            username: clickhouse_url.username().to_string(),
                            password: clickhouse_url.password().unwrap_or("").to_string(),
                            default_database: clickhouse_url.path().to_string(),
                        }
                    } else {
                        ClientOptions::default()
                    };

                    let clickhouse_client = Client::connect(
                        format!(
                            "{}:{}",
                            clickhouse_url.host().unwrap(),
                            clickhouse_url.port().unwrap()
                        ),
                        options.clone(),
                    )
                    .await?;

                    let bitcoin_rpc_url = Url::parse(&provider).unwrap();

                    let rpc = bitcoincore_rpc::Client::new(
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

                    clickhouse_btc::init::init(clickhouse_client, rpc, from, batch).await?;
                }
                SupportedChain::Ethereum => {
                    let clickhouse_url = Url::parse(&db).unwrap();
                    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

                    let options = if clickhouse_url.path() != "/default"
                        || clickhouse_url.username().len() > 0
                    {
                        ClientOptions {
                            username: clickhouse_url.username().to_string(),
                            password: clickhouse_url.password().unwrap_or("").to_string(),
                            default_database: clickhouse_url.path().to_string(),
                        }
                    } else {
                        ClientOptions::default()
                    };

                    let clickhouse_client = Client::connect(
                        format!(
                            "{}:{}",
                            clickhouse_url.host().unwrap(),
                            clickhouse_url.port().unwrap()
                        ),
                        options.clone(),
                    )
                    .await?;

                    let provider_ws = Provider::<Ws>::connect(provider).await?;
                    let trace_provider = Provider::try_from(init_trace)?;

                    clickhouse_eth::init::init(
                        clickhouse_client,
                        provider_ws,
                        from,
                        trace_provider,
                        batch,
                    )
                    .await?;
                }
                SupportedChain::Tron => {}
            }
        }
        ClapActionType::Sync {
            chain,
            db,
            provider,
            sync_trace,
        } => {
            match chain {
                SupportedChain::Bitcoin => {}
                SupportedChain::Ethereum => {
                    let clickhouse_url = Url::parse(&db).unwrap();
                    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

                    let options = if clickhouse_url.path() != "/default"
                        || clickhouse_url.username().len() > 0
                    {
                        ClientOptions {
                            username: clickhouse_url.username().to_string(),
                            password: clickhouse_url.password().unwrap_or("").to_string(),
                            default_database: clickhouse_url.path().to_string(),
                        }
                    } else {
                        ClientOptions::default()
                    };

                    clickhouse_eth::sync::sync(
                        clickhouse_url.clone(),
                        options.clone(),
                        provider.to_owned(),
                        sync_trace.to_owned(),
                    )
                    .await?;
                }
                SupportedChain::Tron => {}
            }
        }
        ClapActionType::Check {
            from,
            chain,
            db,
            provider,
            sync_trace,
        } => {
            match chain {
                SupportedChain::Bitcoin => {}
                SupportedChain::Ethereum => {
                    let clickhouse_url = Url::parse(&db).unwrap();
                    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

                    let options = if clickhouse_url.path() != "/default"
                        || clickhouse_url.username().len() > 0
                    {
                        ClientOptions {
                            username: clickhouse_url.username().to_string(),
                            password: clickhouse_url.password().unwrap_or("").to_string(),
                            default_database: clickhouse_url.path().to_string(),
                        }
                    } else {
                        ClientOptions::default()
                    };

                    clickhouse_eth::check::check(
                        clickhouse_url.clone(),
                        options.clone(),
                        provider.to_owned(),
                        sync_trace.to_owned(),
                        from,
                    )
                    .await?;
                }
                SupportedChain::Tron => {}
            }
        }
    }
    // if args.db.starts_with("clickhouse") {
    //     clickhouse_eth::main(&args.db, &args.provider, &args.action_type).await?;
    // }

    Ok(())
}
