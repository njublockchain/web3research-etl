import time
import clickhouse_connect
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--db", type=str, required=True)
parser.add_argument("--dsn", type=str, required=True)
parser.add_argument("--timeout", type=int, default=600)

if __name__ == "__main__":
    args = parser.parse_args()

    try:
        client = clickhouse_connect.get_client(
            dsn=args.dsn,
            send_receive_timeout=args.timeout,
        )
        result = client.query("SHOW TABLES FROM {}".format(args.db))
        for table in result.result_rows:
            print("Optimizing table {}.{}".format(args.db, table[0]))
            try:
                client = clickhouse_connect.get_client(
                    dsn=args.dsn,
                    send_receive_timeout=args.timeout,
                )
                client.command(
                    "OPTIMIZE TABLE {}.{} FINAL DEDUPLICATE".format(args.db, table[0])
                )
            except Exception as e:
                print("Error optimizing table {}.{}: {}".format(args.db, table[0], e))
    except Exception as e:
        print("Error getting tables from {}: {}".format(args.db, e))

    while True:
        result = client.query(
            "SELECT table, progress FROM system.merges WHERE database = '{}';"
            "".format(args.db)
        )
        if not result.result_rows:
            break

        for row in result.result_rows:
            print(
                "Table {}.{} is being optimzing: {}%".format(
                    args.db, row[0], row[1] * 100
                )
            )

        time.sleep(5)
