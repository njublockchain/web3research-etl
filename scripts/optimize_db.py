import clickhouse_connect
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--db', type=str, required=True)
parser.add_argument('--dsn', type=str, required=True)
parser.add_argument('--timeout', type=int, default=600)

if __name__ == '__main__':
    args = parser.parse_args()
    client = clickhouse_connect.get_client(
        dsn=args.dsn,
        send_receive_timeout=args.timeout,
    )
    
    try:
        result = client.query('SHOW TABLES FROM {}'.format(args.db))
        for table in result.result_rows:
            print('Optimizing table {}.{}'.format(args.db, table[0]))
            try:
                client.command('OPTIMIZE TABLE {}.{} DEDUPLICATE'.format(args.db, table[0]))
            except Exception as e:
                print('Error optimizing table {}.{}: {}'.format(args.db, table[0], e))
    except Exception as e:
        print('Error getting tables from {}: {}'.format(args.db, e))
