"""
Backfill BEA GDP by Industry Data

Downloads and loads GDP by Industry data from the Bureau of Economic Analysis API.

Usage:
    python scripts/backfill_bea_gdpbyindustry.py [options]

Options:
    --tables TABLE1,TABLE2  Specific table IDs to backfill (default: all)
    --frequency A|Q         Data frequency: Annual or Quarterly (default: A)
    --year YEAR_SPEC        Year specification: ALL, or comma-separated years (default: ALL)
    --dry-run               Preview what would be collected without actually collecting
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
import io

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings
from src.database.connection import get_session
from src.bea.bea_client import BEAClient
from src.bea.bea_collector import GDPByIndustryCollector, CollectionProgress

# Create logs directory
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bea_gdpbyindustry_backfill.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def progress_callback(progress: CollectionProgress):
    """Print progress updates"""
    pct = (progress.tables_processed / progress.total_tables * 100) if progress.total_tables > 0 else 0
    logger.info(
        f"Progress: {progress.tables_processed}/{progress.total_tables} tables ({pct:.1f}%), "
        f"{progress.series_processed} industries, {progress.data_points_inserted} data points, "
        f"{progress.api_requests} API requests"
    )


def main():
    parser = argparse.ArgumentParser(description='Backfill BEA GDP by Industry data')
    parser.add_argument('--tables', type=str, help='Comma-separated list of table IDs to backfill')
    parser.add_argument('--frequency', type=str, default='A', choices=['A', 'Q'],
                        help='Data frequency: A=Annual, Q=Quarterly')
    parser.add_argument('--year', type=str, default='ALL',
                        help='Year specification: ALL or comma-separated years')
    parser.add_argument('--dry-run', action='store_true', help='Preview without collecting')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"BEA GDP BY INDUSTRY BACKFILL - {datetime.now()}")
    logger.info("=" * 80)
    logger.info(f"Frequency: {args.frequency}")
    logger.info(f"Year: {args.year}")

    # Parse tables argument
    tables = None
    if args.tables:
        tables = [int(t.strip()) for t in args.tables.split(',')]
        logger.info(f"Tables: {tables}")
    else:
        logger.info("Tables: ALL")

    if args.dry_run:
        logger.info("DRY RUN - No data will be collected")

    # Initialize client
    api_key = settings.api.bea_api_key
    if not api_key or len(api_key) != 36:
        logger.error("Invalid or missing BEA_API_KEY in environment")
        return 1

    client = BEAClient(api_key=api_key)

    with get_session() as session:
        collector = GDPByIndustryCollector(client, session)

        if args.dry_run:
            # Just sync the catalogs and show what would be collected
            logger.info("Syncing GDP by Industry tables catalog...")
            tables_count = collector.sync_tables_catalog()
            logger.info(f"Found {tables_count} GDP by Industry tables available")

            logger.info("Syncing industries catalog...")
            industries_count = collector.sync_industries_catalog()
            logger.info(f"Found {industries_count} industries available")

            if tables:
                logger.info(f"Would collect {len(tables)} specified tables")
            else:
                logger.info(f"Would collect all {tables_count} tables")
            return 0

        # Run backfill
        try:
            progress = collector.backfill_all_tables(
                frequency=args.frequency,
                year=args.year,
                tables=tables,
                progress_callback=progress_callback
            )

            logger.info("=" * 80)
            if progress.errors:
                logger.warning(f"BACKFILL COMPLETED WITH {len(progress.errors)} ERRORS")
                for err in progress.errors[:10]:  # Show first 10 errors
                    logger.warning(f"  - {err}")
            else:
                logger.info("BACKFILL COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Tables processed: {progress.tables_processed}/{progress.total_tables}")
            logger.info(f"Industries processed: {progress.series_processed}")
            logger.info(f"Data points inserted: {progress.data_points_inserted}")
            logger.info(f"API requests: {progress.api_requests}")
            logger.info(f"Duration: {(progress.end_time - progress.start_time).total_seconds():.1f}s")

            return 0 if not progress.errors else 1

        except Exception as e:
            logger.error(f"Backfill failed: {e}", exc_info=True)
            return 1


if __name__ == "__main__":
    sys.exit(main())
