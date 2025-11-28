"""
CSV-based Kafka producer.

Reads a reviews CSV (like `Tools_and_Home_Improvement_clustered.csv`) and an optional
cluster profile CSV (`cluster_profiles.csv`), merges cluster profile attributes into
each record, and streams records to Kafka as JSON messages.

This implementation uses the built-in `csv` module (no extra dependencies).
"""

import csv
import json
import time
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CSVFileProducer:
    """Producer that reads CSV(s), merges cluster profiles and sends to Kafka."""

    def __init__(self, bootstrap_servers='localhost:9092', topic='review-clusters'):
        self.topic = topic
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=10,
            )
            logger.info(f"Kafka producer initialized for topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to initialize producer: {e}")
            raise

    def _load_cluster_profiles(self, cluster_filepath):
        """Load cluster profiles CSV into a dict keyed by cluster id (int)."""
        if not cluster_filepath:
            return {}

        profiles = {}
        try:
            with open(cluster_filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # cluster may be stored as string; convert to int when possible
                    key = row.get('cluster')
                    if key is None:
                        continue
                    try:
                        c = int(key)
                    except Exception:
                        try:
                            c = int(float(key))
                        except Exception:
                            continue

                    # convert numeric fields where appropriate
                    profile = {}
                    for k, v in row.items():
                        if v is None:
                            profile[k] = None
                            continue
                        v = v.strip()
                        if v == '':
                            profile[k] = None
                            continue
                        # try numeric conversion
                        try:
                            if '.' in v:
                                profile[k] = float(v)
                            else:
                                profile[k] = int(v)
                        except Exception:
                            profile[k] = v

                    profiles[c] = profile

        except FileNotFoundError:
            logger.error(f"Cluster profile file not found: {cluster_filepath}")
            raise
        except Exception as e:
            logger.error(f"Error loading cluster profiles: {e}")
            raise

        logger.info(f"Loaded {len(profiles)} cluster profiles from {cluster_filepath}")
        return profiles

    def _stream_csv(self, data_filepath, max_records=None):
        """Generator to stream rows from a CSV file as dicts (memory efficient)."""
        with open(data_filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if max_records and count >= max_records:
                    break
                count += 1
                yield row

    def send_from_csv(self, data_filepath, cluster_filepath=None, delay=0.1, loop=False, max_records=None):
        """Read data CSV and optional cluster profiles CSV, merge and send to Kafka.

        - `data_filepath` should be a CSV with at minimum fields like `rating`, `cluster`, `text`.
        - `cluster_filepath` is optional; if provided, its fields will be attached as `cluster_profile`.
        """
        sent_count = 0
        error_count = 0
        skipped_count = 0
        start_time = time.time()

        # Load cluster profiles once
        cluster_profiles = self._load_cluster_profiles(cluster_filepath) if cluster_filepath else {}

        try:
            iteration = 0
            while True:
                iteration += 1
                logger.info('=' * 60)
                logger.info(f'Reading data from: {data_filepath}')
                if loop:
                    logger.info(f'Iteration: {iteration}')

                records = self._stream_csv(data_filepath, max_records)

                batch_start = time.time()
                batch_count = 0

                for i, raw in enumerate(records, 1):
                    if max_records and sent_count >= max_records:
                        logger.info(f'Reached max_records limit: {max_records}')
                        break

                    try:
                        if not isinstance(raw, dict):
                            skipped_count += 1
                            continue

                        # Normalize fields
                        record = dict(raw)

                        # Convert rating to number when possible
                        if 'rating' in record:
                            try:
                                record['rating'] = float(record['rating']) if record['rating'] != '' else None
                            except Exception:
                                record['rating'] = None

                        # Convert cluster to int when possible
                        cluster_key = None
                        if 'cluster' in record:
                            try:
                                cluster_key = int(float(record['cluster']))
                                record['cluster'] = cluster_key
                            except Exception:
                                record['cluster'] = record.get('cluster')

                        # Attach cluster profile if available
                        if cluster_key is not None and cluster_key in cluster_profiles:
                            record['cluster_profile'] = cluster_profiles[cluster_key]

                        # Send message
                        self.producer.send(self.topic, record)

                        sent_count += 1
                        batch_count += 1

                        if sent_count % 1000 == 0:
                            elapsed = time.time() - start_time
                            rate = sent_count / elapsed if elapsed > 0 else 0
                            logger.info(
                                f'Sent {sent_count:,} messages ({rate:.1f} msg/s, errors: {error_count}, skipped: {skipped_count})'
                            )

                        if delay > 0:
                            time.sleep(delay)

                    except KafkaError as e:
                        error_count += 1
                        if error_count <= 5:
                            logger.error(f'Failed to send record {i}: {e}')
                        continue
                    except Exception as e:
                        error_count += 1
                        if error_count <= 5:
                            logger.error(f'Error processing record {i}: {e}')
                        continue

                # Flush any remaining messages
                self.producer.flush()

                batch_elapsed = time.time() - batch_start
                batch_rate = batch_count / batch_elapsed if batch_elapsed > 0 else 0

                logger.info('=' * 60)
                logger.info('✅ Batch completed:')
                logger.info(f'   - Records sent: {batch_count:,}')
                logger.info(f'   - Average rate: {batch_rate:.1f} msg/s')
                logger.info(f'   - Errors: {error_count}')
                logger.info(f'   - Skipped: {skipped_count}')
                logger.info('=' * 60)

                if not loop:
                    break

                if max_records and sent_count >= max_records:
                    break

                logger.info(f'Looping - restarting in {delay} seconds...')
                time.sleep(delay)

        except FileNotFoundError as e:
            logger.error(f'❌ File not found: {e}')
            raise
        except KeyboardInterrupt:
            logger.info('\n⚠️  Interrupted by user')
        finally:
            self.close()
            total_elapsed = time.time() - start_time
            avg_rate = sent_count / total_elapsed if total_elapsed > 0 else 0

            logger.info('\n' + '=' * 60)
            logger.info('FINAL STATISTICS:')
            logger.info(f'   - Total sent: {sent_count:,}')
            logger.info(f'   - Total errors: {error_count}')
            logger.info(f'   - Total skipped: {skipped_count}')
            logger.info(f'   - Total time: {total_elapsed:.2f}s')
            logger.info(f'   - Average rate: {avg_rate:.1f} msg/s')
            logger.info('=' * 60)

    def close(self):
        try:
            self.producer.flush()
            self.producer.close()
        except Exception:
            pass
        logger.info('Producer closed')


def main():
    parser = argparse.ArgumentParser(
        description='Send CSV data (reviews + cluster profiles) to Kafka topic',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Send reviews CSV with optional cluster profiles CSV
  python producer.py --data-csv Tools_and_Home_Improvement_clustered.csv --cluster-csv cluster_profiles.csv

  # Send first 1000 records at 10 msg/s
  python producer.py --data-csv Tools_and_Home_Improvement_clustered.csv --cluster-csv cluster_profiles.csv --max-records 1000
        '''
    )

    parser.add_argument('--data-csv', help='Path to reviews CSV (e.g. Tools_and_Home_Improvement_clustered.csv)')
    parser.add_argument('--cluster-csv', help='Path to cluster profiles CSV (optional)')
    parser.add_argument('--topic', default='review-clusters', help='Kafka topic name (default: review-clusters)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between messages in seconds (default: 0.1 = 10 msg/s)')
    parser.add_argument('--loop', action='store_true', help='Continuously loop through the file')
    parser.add_argument('--max-records', type=int, default=None, help='Maximum number of records to send (default: all)')

    args = parser.parse_args()

    if not args.data_csv:
        parser.error('the following arguments are required: --data-csv')

    print('=' * 80)
    print('KAFKA CSV FILE PRODUCER')
    print('=' * 80)
    print(f"Data CSV:           {args.data_csv}")
    print(f"Cluster CSV:        {args.cluster_csv}")
    print(f"Topic:              {args.topic}")
    print(f"Bootstrap servers:  {args.bootstrap_servers}")
    print(f"Delay:              {args.delay}s ({1/args.delay:.1f} msg/s)")
    print(f"Loop:               {args.loop}")
    print(f"Max records:        {args.max_records if args.max_records else 'unlimited'}")
    print('=' * 80)
    print('\nPress Ctrl+C to stop\n')

    try:
        producer = CSVFileProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
        producer.send_from_csv(
            data_filepath=args.data_csv,
            cluster_filepath=args.cluster_csv,
            delay=args.delay,
            loop=args.loop,
            max_records=args.max_records,
        )
    except Exception as e:
        logger.error(f'Fatal error: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()