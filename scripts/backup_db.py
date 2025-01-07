import argparse
import clickhouse_connect


parser = argparse.ArgumentParser()
parser.add_argument("--db", type=str, required=True)
parser.add_argument("--dsn", type=str, required=True)
parser.add_argument("--disk", type=str, default="backups")
parser.add_argument("--timeout", type=int, default=600)

if __name__ == "__main__":
    args = parser.parse_args()
    client = clickhouse_connect.get_client(
        dsn=args.dsn,
        send_receive_timeout=args.timeout,
    )

    try:
        result = client.query("SHOW TABLES FROM {}".format(args.db))
        for table in result.result_rows:
            filename = "{}_{}.zip".format(args.db, table[0])
            print("Backup table {}.{} to {}".format(args.db, table[0], filename))
            try:
                client.command(
                    "BACKUP TABLE {}.{} TO Disk('{}', '{}') ASYNC".format(
                        args.db, table[0], args.disk, filename
                    )
                )
            except Exception as e:
                print("Error backuping table {}.{}: {}".format(args.db, table[0], e))
    except Exception as e:
        print("Error getting tables from {}: {}".format(args.db, e))
