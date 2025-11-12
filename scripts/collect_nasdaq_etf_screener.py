"""
Collect Nasdaq ETF Screener Data
Downloads and processes daily ETF screener data from Nasdaq
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.nasdaq_etf_screener_collector import NasdaqETFScreenerCollector

# Setup logging with UTF-8 encoding for Windows console
import io

# Create a UTF-8 wrapper for stdout to handle emojis on Windows
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/nasdaq_etf_screener_collect.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Collect Nasdaq ETF screener data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect fresh data (download + process)
  python scripts/collect_nasdaq_etf_screener.py

  # Process existing CSV file
  python scripts/collect_nasdaq_etf_screener.py --csv-file data/nasdaq_etf_screener/nasdaq_etf_screener_20251111_221029.csv

  # Collect only if data is stale (older than 1 day)
  python scripts/collect_nasdaq_etf_screener.py --if-needed

  # Force collection regardless of existing data
  python scripts/collect_nasdaq_etf_screener.py --force
        """
    )

    parser.add_argument(
        '--csv-file',
        metavar='PATH',
        help='Path to existing CSV file (skips download)'
    )

    parser.add_argument(
        '--snapshot-date',
        metavar='YYYY-MM-DD',
        help='Snapshot date for the data (default: today)'
    )

    parser.add_argument(
        '--if-needed',
        action='store_true',
        help='Only collect if latest snapshot is older than 1 day'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force collection even if recent data exists'
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info(f"NASDAQ ETF SCREENER DATA COLLECTION - {datetime.now()}")
    logger.info("="*80)

    # Create logs directory if needed
    Path('logs').mkdir(exist_ok=True)

    try:
        with get_session() as session:
            collector = NasdaqETFScreenerCollector(session)

            # Parse snapshot date if provided
            snapshot_date = None
            if args.snapshot_date:
                snapshot_date = datetime.strptime(args.snapshot_date, '%Y-%m-%d').date()

            # Collect data
            if args.if_needed:
                logger.info("Checking if collection is needed...")
                success = collector.collect_if_needed(max_age_days=1)
            else:
                if args.csv_file:
                    logger.info(f"Processing CSV file: {args.csv_file}")
                    success = collector.collect(csv_path=args.csv_file, snapshot_date=snapshot_date)
                else:
                    logger.info("Downloading and processing fresh data...")
                    success = collector.collect(snapshot_date=snapshot_date)

            if success:
                logger.info("="*80)
                logger.info("✓ COLLECTION SUCCESSFUL")
                logger.info("="*80)
                logger.info(f"Records inserted/updated: {collector.records_inserted:,}")

                # Show latest snapshot info
                latest_date = collector.get_latest_snapshot_date()
                if latest_date:
                    symbol_count = len(collector.get_symbols_for_date(latest_date))
                    logger.info(f"Latest snapshot date: {latest_date}")
                    logger.info(f"Total ETFs in latest snapshot: {symbol_count:,}")

                return 0
            else:
                logger.error("="*80)
                logger.error("✗ COLLECTION FAILED")
                logger.error("="*80)
                return 1

    except Exception as e:
        logger.error("="*80)
        logger.error("✗ COLLECTION FAILED")
        logger.error("="*80)
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("")
        logger.error("Workaround if automated download fails:")
        logger.error("  1. Manually download CSV from: https://www.nasdaq.com/market-activity/etf/screener")
        logger.error("  2. Click 'Download CSV' button and save to data/nasdaq_etf_screener/")
        logger.error("  3. Run: python scripts/collect_nasdaq_etf_screener.py --csv-file <path-to-csv>")
        return 1


if __name__ == "__main__":
    sys.exit(main())
