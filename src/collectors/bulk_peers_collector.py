"""
Bulk Stock Peers Collector
Fetches peer relationships for ALL global symbols from FMP bulk API
Stores in peers_bulk table (no validation, no foreign keys)
Override approach: replaces old peer data with latest
"""
import logging
from io import StringIO
from typing import Dict

import pandas as pd
from sqlalchemy.dialects.postgresql import insert

from src.collectors.base_collector import BaseCollector
from src.database.models import PeersBulk

logger = logging.getLogger(__name__)


class BulkPeersCollector(BaseCollector):
    """Collector for bulk stock peers - unvalidated peer relationships"""

    def get_table_name(self) -> str:
        return "peers_bulk"

    def collect_for_symbol(self, symbol: str) -> bool:
        """Not applicable - use collect_bulk_peers() instead"""
        raise NotImplementedError(
            "BulkPeersCollector works on entire market data. "
            "Use collect_bulk_peers() instead of collect_for_symbol()."
        )

    def collect_bulk_peers(self) -> dict:
        """
        Fetch and store bulk peers data for all symbols

        Returns:
            Dictionary with results:
            {
                'symbols_received': int,
                'symbols_inserted': int,
                'success': bool
            }
        """
        logger.info("="*80)
        logger.info("BULK PEERS COLLECTION")
        logger.info("="*80)

        try:
            # Fetch bulk data (CSV format)
            logger.info("Fetching bulk peers data from FMP API...")
            url = "https://financialmodelingprep.com/stable/peers-bulk"
            params = {}

            response = self._get(url, params)

            # Parse CSV response
            csv_text = response.text

            if not csv_text or len(csv_text) < 100:  # Sanity check
                logger.warning("No bulk peers data returned")
                return {
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'success': False
                }

            # Read CSV into DataFrame
            df = pd.read_csv(StringIO(csv_text))

            if df.empty:
                logger.warning("Empty CSV data returned")
                return {
                    'symbols_received': 0,
                    'symbols_inserted': 0,
                    'success': False
                }

            symbols_received = len(df)
            logger.info(f"[OK] Received {symbols_received:,} symbols from bulk API")

            # Transform to our schema
            records = []
            skipped = 0

            for _, item in df.iterrows():
                try:
                    # Basic sanity check - must have symbol
                    if not item.get('symbol') or pd.isna(item.get('symbol')):
                        skipped += 1
                        continue

                    # Get peers list (could be NaN or empty string)
                    peers_value = item.get('peers')
                    if pd.isna(peers_value):
                        peers_list = None
                    else:
                        peers_list = str(peers_value).strip() if peers_value else None

                    record = {
                        'symbol': str(item['symbol']).strip(),
                        'peers_list': peers_list
                    }

                    records.append(record)

                except (ValueError, KeyError, TypeError) as e:
                    skipped += 1
                    continue

            if skipped > 0:
                logger.warning(f"Skipped {skipped} records with invalid data")

            if not records:
                logger.warning("No valid records to insert")
                return {
                    'symbols_received': symbols_received,
                    'symbols_inserted': 0,
                    'success': False
                }

            # Batch upsert all records (override old data)
            logger.info(f"Upserting {len(records):,} records into database...")
            inserted = self._batch_upsert(records)

            logger.info("="*80)
            logger.info("[SUCCESS] BULK PEERS COLLECTION COMPLETE")
            logger.info(f"  Symbols received: {symbols_received:,}")
            logger.info(f"  Symbols upserted: {inserted:,}")
            logger.info("="*80)

            return {
                'symbols_received': symbols_received,
                'symbols_inserted': inserted,
                'success': True
            }

        except Exception as e:
            logger.error(f"Error collecting bulk peers: {e}")
            self.session.rollback()  # Must rollback before any other DB operation
            self.record_error('peers_bulk', 'bulk_collection', str(e))
            return {
                'symbols_received': 0,
                'symbols_inserted': 0,
                'success': False,
                'error': str(e)
            }

    def _batch_upsert(self, records: list) -> int:
        """
        Batch upsert records into peers_bulk

        Args:
            records: List of peer record dictionaries

        Returns:
            Number of records inserted/updated
        """
        if not records:
            return 0

        # Insert in batches to avoid memory issues
        # Using 1000 instead of 10000 to keep error messages manageable
        batch_size = 1000
        total_upserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]

            try:
                stmt = insert(PeersBulk).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol'],
                    set_={
                        'peers_list': stmt.excluded.peers_list,
                        'collected_at': stmt.excluded.collected_at
                    }
                )

                self.session.execute(stmt)
                self.session.commit()  # Commit successful batch immediately
                total_upserted += len(batch)

                # Log progress for large batches
                if len(records) > batch_size:
                    logger.info(f"  Progress: {total_upserted:,} / {len(records):,}")

            except Exception as batch_error:
                logger.error(f"Error inserting batch {i}-{i+len(batch)}: {batch_error}")
                logger.error(f"First symbol in failed batch: {batch[0].get('symbol') if batch else 'unknown'}")
                self.session.rollback()

                # Try inserting records one by one to identify problem records
                logger.warning(f"Attempting individual inserts for batch {i}-{i+len(batch)}...")
                for idx, record in enumerate(batch):
                    try:
                        single_stmt = insert(PeersBulk).values([record])
                        single_stmt = single_stmt.on_conflict_do_update(
                            index_elements=['symbol'],
                            set_={
                                'peers_list': single_stmt.excluded.peers_list,
                                'collected_at': single_stmt.excluded.collected_at
                            }
                        )
                        self.session.execute(single_stmt)
                        self.session.commit()
                        total_upserted += 1
                    except Exception as single_error:
                        logger.error(f"Failed to insert record: {record.get('symbol')} - {single_error}")
                        self.session.rollback()
                        # Skip this record and continue

        # All commits happen in the loop above
        return total_upserted


if __name__ == "__main__":
    from src.database.connection import get_session

    logging.basicConfig(level=logging.INFO)

    with get_session() as session:
        collector = BulkPeersCollector(session)

        # Test collecting peers data
        result = collector.collect_bulk_peers()

        if result['success']:
            print(f"\n[OK] Successfully collected {result['symbols_inserted']:,} symbols")
        else:
            print(f"\n[FAILED] Collection failed")
            if 'error' in result:
                print(f"Error: {result['error']}")
