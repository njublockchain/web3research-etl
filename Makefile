include .env

release:
	cargo build --release
eth-init:
	RUST_LOG=info ./target/release/web3research-etl init -c ethereum --db clickhouse://default@localhost:9000/ethereum -p ws://localhost:8546 --trace http://localhost:8545 --provider-type erigon --from $(FROM) --batch $(BATCH)
eth-sync:
	RUST_LOG=info ./target/release/web3research-etl sync -c ethereum --db clickhouse://default@localhost:9000/ethereum -p ws://localhost:8546 --trace http://localhost:8545 --provider-type erigon
eth-check:
	RUST_LOG=info ./target/release/web3research-etl check -c ethereum --db clickhouse://default@localhost:9000/ethereum -p ws://localhost:8546 --trace http://localhost:8545 --provider-type erigon --from $(FROM)
run-eth-init:
	RUST_LOG=info cargo run -- init -c ethereum --db clickhouse://default@localhost:9000/ethereum -p ws://localhost:8546 --trace http://localhost:8545 --provider-type erigon --from $(FROM) --batch $(BATCH)
run-eth-sync:
	RUST_LOG=info cargo run -- sync -c ethereum --db clickhouse://default@localhost:9000/ethereum -p ws://localhost:8546 --trace http://localhost:8545 --provider-type erigon
run-eth-check:
	RUST_LOG=info cargo run -- check -c ethereum --db clickhouse://default@localhost:9000/ethereum -p ws://localhost:8546 --trace http://localhost:8545 --provider-type erigon --from $(FROM)
tron-init:
	RUST_LOG=info ./target/release/web3research-etl init -c tron --db clickhouse://default@localhost:9000/tron -p http://localhost:50051 --from $(FROM) --batch $(BATCH)
btc-init:
	RUST_LOG=info ./target/release/web3research-etl init -c bitcoin --db clickhouse://default@localhost:9000/bitcoin -p http://${BITCOIN_RPC_USERNAME}:${BITCOIN_RPC_PASSWORD}@localhost:8332 --from $(FROM) --batch $(BATCH)
