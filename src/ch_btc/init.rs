use std::error::Error;

use bitcoincore_rpc::RpcApi;
use documented::Documented;
use log::{info, warn};
use url::Url;

use crate::{
    ch_btc::schema::{BlockRow, InputRow, OutputRow},
    ProviderType,
};

pub(crate) async fn init(
    db: String,
    provider: String,
    _provider_type: ProviderType,
    from: u64,
    batch: u64,
) -> Result<(), Box<dyn Error>> {
    let clickhouse_url = Url::parse(&db).unwrap();
    // warn!("db: {} path: {}", format!("{}:{}", clickhouse_url.host().unwrap(), clickhouse_url.port().unwrap()), clickhouse_url.path());

    let options = if clickhouse_url.path() != "/default" || !clickhouse_url.username().is_empty() {
        warn!("auth enabled for clickhouse");
        klickhouse::ClientOptions {
            username: clickhouse_url.username().to_string(),
            password: clickhouse_url.password().unwrap_or("").to_string(),
            default_database: clickhouse_url
                .path()
                .to_string()
                .strip_prefix('/')
                .unwrap_or("default")
                .to_string(),
        }
    } else {
        klickhouse::ClientOptions::default()
    };

    let client = klickhouse::Client::connect(
        format!(
            "{}:{}",
            clickhouse_url.host().unwrap(),
            clickhouse_url.port().unwrap()
        ),
        options.clone(),
    )
    .await?;

    let bitcoin_rpc_url = Url::parse(&provider).unwrap();

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

    info!("start initializing schema");
    client.execute(BlockRow::DOCS).await.unwrap();
    client.execute(InputRow::DOCS).await.unwrap();
    client.execute(OutputRow::DOCS).await.unwrap();

    let latest_height = provider.get_block_count()? - 1;
    let to = latest_height;
    warn!("Initializing blocks from {} to {}", from, to);

    let mut block_row_list = Vec::with_capacity((batch + 1_u64) as usize);
    let mut input_row_list = Vec::new();
    let mut output_row_list = Vec::new();

    for num in from..=to {
        let height = num;
        let hash = provider.get_block_hash(num)?;
        // let cli = client.get_jsonrpc_client();
        let block = provider.get_block(&hash)?;

        let block_row = BlockRow::from_bitcoin_rpc(height, &block);

        block_row_list.push(block_row);

        for tx in &block.txdata {
            for (index, vin) in tx.input.iter().enumerate() {
                let input_row =
                    InputRow::from_bitcoin_rpc(height, &block, tx, index.try_into().unwrap(), vin);

                input_row_list.push(input_row);
            }

            for (index, vout) in tx.output.iter().enumerate() {
                let output_row = OutputRow::from_bitcoin_rpc(
                    height,
                    &block,
                    tx,
                    index.try_into().unwrap(),
                    vout,
                );

                output_row_list.push(output_row);
            }
        }

        if (num - from + 1) % batch == 0 || num == to {
            tokio::try_join!(
                client.insert_native_block(
                    "INSERT INTO inputs FORMAT native",
                    input_row_list.to_vec()
                ),
                client.insert_native_block(
                    "INSERT INTO outputs FORMAT native",
                    output_row_list.to_vec()
                ),
            )
            .unwrap();

            client
                .insert_native_block("INSERT INTO blocks FORMAT native", block_row_list.to_vec())
                .await
                .unwrap();

            warn!(
                "Inserted blocks from {} to {}, with {} inputs, {} outputs",
                num - batch + 1,
                num,
                input_row_list.len(),
                output_row_list.len()
            );

            block_row_list.clear();
            input_row_list.clear();
            output_row_list.clear();
        }
    }

    Ok(())
}
