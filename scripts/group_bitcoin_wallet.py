import os
import traceback
import uuid
import logging
from typing import Set, Dict, List, Optional
from clickhouse_connect import get_client
import argparse
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BitcoinWalletGrouper:
    def __init__(self, clickhouse_url: str):
        """
        Initialize Bitcoin wallet aggregator

        Args:
            clickhouse_url: ClickHouse connection URL
        """
        self.client = self._create_client(clickhouse_url)

    def _create_client(self, clickhouse_url: str):
        """Create ClickHouse client connection"""
        try:
            parsed_url = urlparse(clickhouse_url)

            # Extract connection parameters
            host = parsed_url.hostname or "localhost"
            port = parsed_url.port or 18123
            username = parsed_url.username or "default"
            password = parsed_url.password or ""
            database = parsed_url.path.lstrip("/") or "default"

            logger.info(f"Connecting to ClickHouse: {host}:{port}, database: {database}")

            # Configure client with extended timeouts and settings for large queries
            return get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database,
                connect_timeout=120,  # 2 minutes connection timeout
                send_receive_timeout=1800,  # 30 minutes for send/receive operations
                client_name="bitcoin_wallet_grouper",
                compress=True,  # Enable compression to reduce network traffic
                settings={
                    "max_query_size": 10485760,  # 10MB max query size
                    "send_timeout": 1800,  # 30 minutes send timeout
                    "receive_timeout": 1800,  # 30 minutes receive timeout
                },
            )
        except Exception as e:
            logger.error(f"Failed to create ClickHouse client: {e}")
            raise

    def create_wallet_tables(self):
        """Create wallet related tables"""
        # Wallet table - stores basic wallet information
        wallet_table_sql = """
        CREATE TABLE IF NOT EXISTS wallets (
            `walletId` FixedString(36),
            `createdAt` DateTime DEFAULT now(),
            `lastUpdatedAt` DateTime DEFAULT now(),
            `addressCount` UInt32,
            `firstSeenBlock` UInt64,
            `lastSeenBlock` UInt64
        ) ENGINE = ReplacingMergeTree(lastUpdatedAt)
        ORDER BY walletId
        SETTINGS index_granularity = 8192;
        """

        # Address to wallet mapping table - for quick lookups
        address_wallet_table_sql = """
        CREATE TABLE IF NOT EXISTS walletAddresses (
            `address` String,
            `walletId` FixedString(36),
            `firstSeenBlock` UInt64,
            `firstSeenTxid` FixedString(64),
            `updatedAt` DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updatedAt)
        ORDER BY address
        SETTINGS index_granularity = 8192;
        """

        # Processing progress table
        progress_table_sql = """
        CREATE TABLE IF NOT EXISTS taskProgress (
            `taskName` String,
            `cursorU64` Nullable(UInt64),
            `cursorStr` Nullable(String),
            `updatedAt` DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updatedAt)
        ORDER BY taskName
        SETTINGS index_granularity = 8192;
        """

        try:
            logger.info("Creating wallet related tables...")
            self.client.command(wallet_table_sql)
            self.client.command(address_wallet_table_sql)
            self.client.command(progress_table_sql)
            logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def get_last_processed_block(self) -> int:
        """Get the last processed block height"""
        try:
            result = self.client.query(
                "SELECT cursorU64 FROM taskProgress WHERE taskName = 'bitcoin_wallet_grouping'"
            )
            if result.result_rows:
                return result.result_rows[0][0]
            return 0
        except Exception:
            return 0

    def update_progress(self, block_height: int):
        """Update processing progress"""
        try:
            self.client.insert(
                "taskProgress",
                [["bitcoin_wallet_grouping", block_height, None]],
                column_names=["taskName", "cursorU64", "cursorStr"],
            )
            logger.info(f"Updated progress to block: {block_height}")
        except Exception as e:
            logger.error(f"Failed to update progress: {e}")

    def batch_get_existing_wallets(self, all_addresses: Set[str]) -> Dict[str, str]:
        """
        Batch retrieve existing wallets for addresses

        Args:
            all_addresses: Set of all addresses to query

        Returns:
            Dictionary with address as key and wallet ID as value
        """
        if not all_addresses:
            return {}

        # Process addresses in chunks to avoid query size limits
        address_chunk_size = 5000  # Process 5000 addresses per batch
        address_list = list(all_addresses)
        total_chunks = (len(address_list) + address_chunk_size - 1) // address_chunk_size
        
        logger.info(f"Querying {len(address_list)} addresses in {total_chunks} chunks")
        
        address_to_wallet = {}
        
        for chunk_index in range(total_chunks):
            start_idx = chunk_index * address_chunk_size
            end_idx = min((chunk_index + 1) * address_chunk_size, len(address_list))
            current_chunk = address_list[start_idx:end_idx]
            
            logger.info(f"Processing address chunk {chunk_index + 1}/{total_chunks} with {len(current_chunk)} addresses")
            
            # Create query for this chunk
            placeholders = ",".join(["%s"] * len(current_chunk))
            query = f"""
            SELECT address, walletId
            FROM walletAddresses 
            WHERE address IN ({placeholders})
            """

            result = self.client.query(query, current_chunk)

            # Process results from this chunk
            for address, wallet_id in result.result_rows:
                wallet_id = wallet_id.decode("utf-8") if isinstance(wallet_id, bytes) else wallet_id
                address_to_wallet[address] = wallet_id

        return address_to_wallet

    def batch_process_wallet_operations(
        self, transactions_data: List[tuple], address_to_wallet: Dict[str, str]
    ):
        """
        Batch process wallet operations, including creating new wallets and merging addresses

        Args:
            transactions_data: List of transaction data, each element is (txid, addresses, block_height)
            address_to_wallet: Mapping from address to wallet ID
        """
        new_wallets = []  # New wallet data
        new_addresses = []  # New address mapping data
        wallet_updates = {}  # Wallet update data

        for txid, addresses, block_height in transactions_data:
            # Find existing wallets associated with these addresses
            involved_wallets = set()
            new_addrs = set()

            for addr in addresses:
                if addr in address_to_wallet:
                    # ensure address is str
                    wallet = address_to_wallet[addr]
                    involved_wallets.add(wallet)
                else:
                    addr = addr.decode("utf-8") if isinstance(addr, bytes) else addr
                    new_addrs.add(addr)

            if not involved_wallets:
                # Create new wallet
                wallet_id = str(uuid.uuid4())
                new_wallets.append(
                    [wallet_id, len(addresses), block_height, block_height]
                )

                # Add address mappings
                for addr in addresses:
                    new_addresses.append([addr, wallet_id, block_height, txid])
                    address_to_wallet[addr] = wallet_id  # Update local mapping

            elif len(involved_wallets) == 1:
                # Merge into existing wallet
                wallet_id = list(involved_wallets)[0]

                # Only add new addresses
                for addr in new_addrs:
                    new_addresses.append([addr, wallet_id, block_height, txid])
                    address_to_wallet[addr] = wallet_id

                # Record wallet updates
                if wallet_id not in wallet_updates:
                    wallet_updates[wallet_id] = {
                        "last_block": block_height,
                        "new_addr_count": 0,
                    }
                wallet_updates[wallet_id]["new_addr_count"] += len(new_addrs)
                wallet_updates[wallet_id]["last_block"] = max(
                    wallet_updates[wallet_id]["last_block"], block_height
                )

            else:
                # Multiple wallet conflicts, need to merge
                logger.info(f"Merging wallets: {involved_wallets} in transaction {txid}")
                primary_wallet = min(involved_wallets)  # Choose the smallest wallet ID as the primary wallet

                # Point all addresses to the primary wallet
                all_addrs = addresses | new_addrs
                for addr in all_addrs:
                    if (
                        addr not in address_to_wallet
                        or address_to_wallet[addr] != primary_wallet
                    ):
                        new_addresses.append([addr, primary_wallet, block_height, txid])
                        address_to_wallet[addr] = primary_wallet

                # Record wallet updates
                if primary_wallet not in wallet_updates:
                    wallet_updates[primary_wallet] = {
                        "last_block": block_height,
                        "new_addr_count": 0,
                    }
                wallet_updates[primary_wallet]["last_block"] = max(
                    wallet_updates[primary_wallet]["last_block"], block_height
                )

        # Batch insert new wallets in chunks
        if new_wallets:
            wallet_chunk_size = 5000
            total_wallet_chunks = (len(new_wallets) + wallet_chunk_size - 1) // wallet_chunk_size
            logger.info(f"Inserting {len(new_wallets)} new wallets in {total_wallet_chunks} chunks")
            
            for chunk_index in range(total_wallet_chunks):
                start_idx = chunk_index * wallet_chunk_size
                end_idx = min((chunk_index + 1) * wallet_chunk_size, len(new_wallets))
                wallet_chunk = new_wallets[start_idx:end_idx]
                
                logger.info(f"Inserting wallet chunk {chunk_index + 1}/{total_wallet_chunks} with {len(wallet_chunk)} wallets")
                
                self.client.insert(
                    "wallets",
                    wallet_chunk,
                    column_names=[
                        "walletId",
                        "addressCount",
                        "firstSeenBlock",
                        "lastSeenBlock",
                    ],
                )
            
            logger.info(f"Batch created {len(new_wallets)} new wallets")

        # Batch insert address mappings in chunks
        if new_addresses:
            address_chunk_size = 5000
            total_address_chunks = (len(new_addresses) + address_chunk_size - 1) // address_chunk_size
            logger.info(f"Inserting {len(new_addresses)} address mappings in {total_address_chunks} chunks")
            
            for chunk_index in range(total_address_chunks):
                start_idx = chunk_index * address_chunk_size
                end_idx = min((chunk_index + 1) * address_chunk_size, len(new_addresses))
                address_chunk = new_addresses[start_idx:end_idx]
                
                logger.info(f"Inserting address chunk {chunk_index + 1}/{total_address_chunks} with {len(address_chunk)} addresses")
                
                self.client.insert(
                    "walletAddresses",
                    address_chunk,
                    column_names=["address", "walletId", "firstSeenBlock", "firstSeenTxid"],
                )
                
            logger.info(f"Batch added {len(new_addresses)} address mappings")
            
        # Batch update wallet information
        if wallet_updates:
            update_data = []
            for wallet_id, update_info in wallet_updates.items():
                # Get current total number of addresses
                wallet_id = (
                    wallet_id.decode("utf-8")
                    if isinstance(wallet_id, bytes)
                    else wallet_id
                )
                # Get existing wallet information including firstSeenBlock
                wallet_info_query = self.client.query(
                    f"SELECT firstSeenBlock FROM wallets WHERE walletId = '{wallet_id}'",
                )
                first_seen_block = (
                    wallet_info_query.result_rows[0][0] 
                    if wallet_info_query.result_rows 
                    else update_info["last_block"]  # Use current block if no record exists
                )
                
                count_result = self.client.query(
                    f"SELECT count() FROM walletAddresses WHERE walletId = '{wallet_id}'",
                )
                total_addresses = (
                    count_result.result_rows[0][0] if count_result.result_rows else 0
                )

                update_data.append(
                    [wallet_id, total_addresses, first_seen_block, update_info["last_block"]]
                )

            if update_data:
                # Process wallet updates in chunks
                update_chunk_size = 5000
                total_update_chunks = (len(update_data) + update_chunk_size - 1) // update_chunk_size
                logger.info(f"Updating {len(update_data)} wallets in {total_update_chunks} chunks")
                
                for chunk_index in range(total_update_chunks):
                    start_idx = chunk_index * update_chunk_size
                    end_idx = min((chunk_index + 1) * update_chunk_size, len(update_data))
                    update_chunk = update_data[start_idx:end_idx]
                    
                    logger.info(f"Updating wallet chunk {chunk_index + 1}/{total_update_chunks} with {len(update_chunk)} wallets")
                    
                    self.client.insert(
                        "wallets",
                        update_chunk,
                        column_names=["walletId", "addressCount", "firstSeenBlock", "lastSeenBlock"],
                    )
                
                logger.info(f"Batch updated {len(update_data)} wallet information")

    def process_transactions_in_range(
        self, start_block: int, end_block: int, batch_size: int = 1000
    ):
        """
        No-JOIN optimized version: Query in two steps to avoid using JOIN

        Args:
            start_block: Start block
            end_block: End block
            batch_size: Batch size
        """
        logger.info(f"Processing block range: {start_block} - {end_block}")

        current_block = start_block

        while current_block <= end_block:
            batch_end = min(current_block + batch_size - 1, end_block)

            logger.info(f"Processing blocks {current_block}-{batch_end}")

            # Step 1: Get transaction input information matching conditions (without using JOIN)
            inputs_query = f"""
            SELECT 
                txid,
                count(txid) as vin_count,
                blockHeight,
                groupArray(prevOutputTxid) as prev_txids,
                groupArray(prevOutputVout) as prev_vouts
            FROM inputs
            PREWHERE blockHeight BETWEEN {current_block} AND {batch_end}
            WHERE prevOutputTxid != '0000000000000000000000000000000000000000000000000000000000000000'
            GROUP BY txid, blockHeight
            HAVING vin_count >= 2
            """

            inputs_result = self.client.query(inputs_query)

            if not inputs_result.result_rows:
                logger.info(f"Blocks {current_block}-{batch_end} have no input transactions to process")
                current_block = batch_end + 1
                continue

            # Collect all output transactions and indices that need to be queried
            prev_outputs = []
            txid_to_inputs = {}

            for (
                txid,
                _,
                block_height,
                prev_txids,
                prev_vouts,
            ) in inputs_result.result_rows:
                # convert bytes to str
                txid = txid.decode("utf-8") if isinstance(txid, bytes) else txid
                prev_txids = [
                    tx.decode("utf-8") if isinstance(tx, bytes) else tx
                    for tx in prev_txids
                ]
                prev_vouts = [
                    vout.decode("utf-8") if isinstance(vout, bytes) else vout
                    for vout in prev_vouts
                ]

                txid_to_inputs[txid] = {
                    "blockHeight": block_height,
                    "prev_outputs": [],
                }

                for i in range(len(prev_txids)):
                    prev_outputs.append((prev_txids[i], prev_vouts[i]))
                    txid_to_inputs[txid]["prev_outputs"].append(
                        (prev_txids[i], prev_vouts[i])
                    )

            if not prev_outputs:
                logger.info(f"Blocks {current_block}-{batch_end} have no valid input transactions")
                current_block = batch_end + 1
                continue

            # Deduplicate to reduce query volume
            unique_prev_outputs = list(set(prev_outputs))

            # Step 2: Batch query all relevant output addresses (in smaller chunks to avoid query size limits)
            all_output_data = {}
            output_chunk_size = 5000  # Process 5000 outputs per batch to keep query size manageable

            logger.info(
                f"Blocks {current_block}-{batch_end} need to query {len(unique_prev_outputs)} unique output transactions"
            )
            
            # Split the list of unique outputs into smaller chunks
            total_chunks = (len(unique_prev_outputs) + output_chunk_size - 1) // output_chunk_size
            logger.info(f"Processing outputs in {total_chunks} chunks of up to {output_chunk_size} items each")
            
            outputs_query = """
                SELECT 
                    txid, 
                    index, 
                    address
                FROM outputs
                WHERE (txid, index) IN (%s)
                """
                
            # Process outputs in chunks
            for chunk_index in range(total_chunks):
                start_idx = chunk_index * output_chunk_size
                end_idx = min((chunk_index + 1) * output_chunk_size, len(unique_prev_outputs))
                chunk = unique_prev_outputs[start_idx:end_idx]
                
                logger.info(f"Processing output chunk {chunk_index + 1}/{total_chunks} with {len(chunk)} items")
                
                # Create placeholders for this chunk
                placeholders = ",".join(["(%s, %s)"] * len(chunk))
                query = outputs_query % placeholders
                
                # Query this chunk of outputs
                outputs_result = self.client.query(
                    query, [item for sublist in chunk for item in sublist]
                )
                
                # Process this chunk's results
                for output_txid, output_index, address in outputs_result.result_rows:
                    output_txid = (
                        output_txid.decode("utf-8")
                        if isinstance(output_txid, bytes)
                        else output_txid
                    )
                    address = (
                        address.decode("utf-8") if isinstance(address, bytes) else address
                    )
                    all_output_data[(output_txid, output_index)] = address

            logger.debug(f"Output data collected: {len(all_output_data)} entries")
            # Step 3: Join data in Python
            transactions_addresses = {}

            for txid, inputs_data in txid_to_inputs.items():
                addresses = set()
                for prev_txid, prev_vout in inputs_data["prev_outputs"]:
                    address = all_output_data.get((prev_txid, prev_vout))
                    if address:
                        addresses.add(address)

                if len(addresses) >= 2:  # Only aggregate with 2 or more addresses
                    transactions_addresses[txid] = addresses

            if not transactions_addresses:
                logger.info(f"Blocks {current_block}-{batch_end} have no transactions to aggregate")
                current_block = batch_end + 1
                continue

            logger.info(f"Found {len(transactions_addresses)} transactions that need aggregation")

            # Collect all involved addresses
            all_addresses = set()
            for addresses in transactions_addresses.values():
                all_addresses.update(addresses)

            # Batch get existing wallet mappings
            address_to_wallet = self.batch_get_existing_wallets(all_addresses)

            # Prepare transaction data
            transactions_data = []
            for txid, addresses in transactions_addresses.items():
                # Get block height
                block_height_result = self.client.query(
                    "SELECT blockHeight FROM inputs WHERE txid = %s LIMIT 1", [txid]
                )
                block_height = (
                    block_height_result.result_rows[0][0]
                    if block_height_result.result_rows
                    else current_block
                )

                transactions_data.append((txid, addresses, block_height))

            # Batch process wallet operations
            self.batch_process_wallet_operations(transactions_data, address_to_wallet)

            # Update progress
            self.update_progress(batch_end)
            current_block = batch_end + 1

    def get_wallet_by_address(self, address: str) -> Optional[str]:
        """
        Query wallet ID by address

        Args:
            address: Bitcoin address

        Returns:
            Wallet ID, or None if it doesn't exist
        """
        result = self.client.query(
            "SELECT walletId FROM walletAddresses WHERE address = %s LIMIT 1",
            [address],
        )

        if result.result_rows:
            return result.result_rows[0][0]

        return None

    def get_wallet_info(self, wallet_id: str) -> Optional[Dict]:
        """
        Get detailed wallet information

        Args:
            wallet_id: Wallet ID

        Returns:
            Wallet information dictionary
        """
        # Get basic wallet information
        wallet_result = self.client.query(
            "SELECT * FROM wallets WHERE walletId = %s LIMIT 1", [wallet_id]
        )

        if not wallet_result.result_rows:
            return None

        wallet_row = wallet_result.result_rows[0]

        # Get wallet address list
        addresses_result = self.client.query(
            "SELECT address FROM walletAddresses WHERE walletId = %s", [wallet_id]
        )

        addresses = [row[0] for row in addresses_result.result_rows]

        return {
            "wallet_id": wallet_row[0],
            "created_at": wallet_row[1],
            "last_updated_at": wallet_row[2],
            "address_count": wallet_row[3],
            "first_seen_block": wallet_row[4],
            "last_seen_block": wallet_row[5],
            "addresses": addresses,
        }

    def run_incremental_update(self, batch_size: int = 1000):
        """
        Run incremental update

        Args:
            batch_size: Batch size
        """
        try:
            # Get latest block height
            latest_block_result = self.client.query("SELECT max(height) FROM blocks")
            if (
                not latest_block_result.result_rows
                or latest_block_result.result_rows[0][0] is None
            ):
                logger.warning("Unable to get latest block height")
                return

            latest_block = latest_block_result.result_rows[0][0]
            last_processed = self.get_last_processed_block()

            if last_processed >= latest_block:
                logger.info("No new blocks to process")
                return

            logger.info(f"Starting incremental update from block {last_processed + 1} to {latest_block}")

            self.process_transactions_in_range(
                last_processed + 1, latest_block, batch_size
            )

            logger.info("Incremental update completed")

        except Exception as e:
            logger.error(f"Incremental update failed: {e}")
            logger.error(traceback.format_exc())
            raise

    def run_full_rebuild(self, batch_size: int = 1000):
        """
        Rebuild all wallet data from scratch

        Args:
            batch_size: Batch size
        """
        try:
            logger.info("Starting wallet data rebuild...")

            # Clear existing data
            self.client.command("TRUNCATE TABLE wallets")
            self.client.command("TRUNCATE TABLE walletAddresses")
            self.client.command("TRUNCATE TABLE wallet_grouping_progress")

            # Get latest block height
            latest_block_result = self.client.query("SELECT max(height) FROM blocks")
            if (
                not latest_block_result.result_rows
                or latest_block_result.result_rows[0][0] is None
            ):
                logger.warning("Unable to get latest block height")
                return

            latest_block = latest_block_result.result_rows[0][0]

            logger.info(f"Rebuilding wallet data, processing blocks 1 to {latest_block}")

            self.process_transactions_in_range(1, latest_block, batch_size)

            logger.info("Rebuild completed")

        except Exception as e:
            logger.error(f"Rebuild failed: {e}")
            logger.error(traceback.format_exc())
            raise


def main():
    parser = argparse.ArgumentParser(description="Bitcoin Wallet Aggregation Tool")
    parser.add_argument("--clickhouse-url", required=True, help="ClickHouse connection URL")
    parser.add_argument(
        "--mode",
        choices=["init", "incremental", "rebuild"],
        default="incremental",
        help="Operation mode",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size")

    args = parser.parse_args()

    try:
        grouper = BitcoinWalletGrouper(args.clickhouse_url)

        if args.mode == "init":
            logger.info("Initializing wallet tables...")
            grouper.create_wallet_tables()
            logger.info("Initialization completed")

        elif args.mode == "incremental":
            logger.info("Running incremental update...")
            grouper.create_wallet_tables()  # Ensure tables exist
            grouper.run_incremental_update(args.batch_size)

        elif args.mode == "rebuild":
            logger.info("Rebuilding wallet data...")
            grouper.create_wallet_tables()
            grouper.run_full_rebuild(args.batch_size)

    except Exception as e:
        logger.error(f"Program execution failed: {e}")
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
