"""
Backfill BEA ITA (International Transactions Accounts) Data

Downloads and loads international transactions data from the Bureau of Economic Analysis API.

Usage:
    python scripts/backfill_bea_ita.py [options]

Options:
    --indicators IND1,IND2  Specific indicators to backfill (default: all)
    --frequency A|QSA|QNSA  Data frequency: Annual, Quarterly SA, or Quarterly NSA (default: A)
    --year YEAR_SPEC        Year specification: ALL, LAST5, LAST10, or comma-separated years (default: ALL)
    --dry-run               Preview what would be collected without actually collecting

Examples:
    # Backfill all annual data
    python scripts/backfill_bea_ita.py

    # Backfill trade balance indicators only
    python scripts/backfill_bea_ita.py --indicators BalGds,BalServ,BalCAcc

    # Backfill quarterly seasonally adjusted data, last 10 years
    python scripts/backfill_bea_ita.py --frequency QSA --year LAST10

    # Preview what would be collected
    python scripts/backfill_bea_ita.py --dry-run
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
from src.bea.bea_collector import ITACollector, CollectionProgress

# Create logs directory
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bea_ita_backfill.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def progress_callback(progress: CollectionProgress):
    """Print progress updates"""
    pct = (progress.tables_processed / progress.total_tables * 100) if progress.total_tables > 0 else 0
    logger.info(
        f"Progress: {progress.tables_processed}/{progress.total_tables} indicators ({pct:.1f}%), "
        f"{progress.series_processed} area combinations, {progress.data_points_inserted} data points, "
        f"{progress.api_requests} API requests"
    )


def main():
    parser = argparse.ArgumentParser(description='Backfill BEA ITA (International Transactions) data')
    parser.add_argument('--indicators', type=str,
                        help='Comma-separated list of indicator codes to backfill (e.g., BalGds,BalServ)')
    parser.add_argument('--frequency', type=str, default='A', choices=['A', 'QSA', 'QNSA'],
                        help='Data frequency: A=Annual, QSA=Quarterly SA, QNSA=Quarterly NSA')
    parser.add_argument('--year', type=str, default='ALL',
                        help='Year specification: ALL, LAST5, LAST10, or comma-separated years')
    parser.add_argument('--dry-run', action='store_true', help='Preview without collecting')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"BEA ITA (INTERNATIONAL TRANSACTIONS) BACKFILL - {datetime.now()}")
    logger.info("=" * 80)
    logger.info(f"Frequency: {args.frequency}")
    logger.info(f"Year: {args.year}")

    # Parse indicators argument
    indicators = None
    if args.indicators:
        indicators = [i.strip() for i in args.indicators.split(',')]
        logger.info(f"Indicators: {indicators}")
    else:
        logger.info("Indicators: ALL")

    if args.dry_run:
        logger.info("DRY RUN - No data will be collected")

    # Initialize client
    api_key = settings.api.bea_api_key
    if not api_key or len(api_key) != 36:
        logger.error("Invalid or missing BEA_API_KEY in environment")
        return 1

    client = BEAClient(api_key=api_key)

    with get_session() as session:
        collector = ITACollector(client, session)

        if args.dry_run:
            # Just sync the catalogs and show what would be collected
            logger.info("Syncing ITA indicators catalog...")
            indicators_count = collector.sync_indicators_catalog()
            logger.info(f"Found {indicators_count} ITA indicators available")

            logger.info("Syncing ITA areas catalog...")
            areas_count = collector.sync_areas_catalog()
            logger.info(f"Found {areas_count} areas/countries available")

            if indicators:
                logger.info(f"Would collect {len(indicators)} specified indicators")
            else:
                logger.info(f"Would collect all {indicators_count} indicators")

            logger.info("")
            logger.info("Common indicators:")
            logger.info("  BalGds    - Balance on Goods")
            logger.info("  BalServ   - Balance on Services")
            logger.info("  BalCAcc   - Balance on Current Account")
            logger.info("  ExpGds    - Exports of Goods")
            logger.info("  ImpGds    - Imports of Goods")
            logger.info("  ExpServ   - Exports of Services")
            logger.info("  ImpServ   - Imports of Services")
            return 0

        # Run backfill
        try:
            progress = collector.backfill_all_indicators(
                frequency=args.frequency,
                year=args.year,
                indicators=indicators,
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
            logger.info(f"Indicators processed: {progress.tables_processed}/{progress.total_tables}")
            logger.info(f"Area combinations: {progress.series_processed}")
            logger.info(f"Data points inserted: {progress.data_points_inserted}")
            logger.info(f"API requests: {progress.api_requests}")
            logger.info(f"Duration: {(progress.end_time - progress.start_time).total_seconds():.1f}s")

            return 0 if not progress.errors else 1

        except Exception as e:
            logger.error(f"Backfill failed: {e}", exc_info=True)
            return 1


if __name__ == "__main__":
    sys.exit(main())
