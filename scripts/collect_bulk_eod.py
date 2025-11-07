"""
Collect Bulk EOD Prices
Fetches end-of-day prices for all global symbols from FMP bulk API
Stores in prices_daily_bulk table as unvalidated data lake
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.bulk_price_collector import BulkPriceCollector

# Create logs directory if needed (BEFORE logging setup)
Path('logs').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bulk_eod_collection.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD format."
        )


def main():
    parser = argparse.ArgumentParser(
        description='Collect bulk EOD prices from FMP API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect yesterday's data (default)
  python scripts/collect_bulk_eod.py

  # Collect specific date
  python scripts/collect_bulk_eod.py --date 2024-11-01

  # Collect date range (backfill)
  python scripts/collect_bulk_eod.py --start-date 2024-11-01 --end-date 2024-11-05

  # Collect last N days
  python scripts/collect_bulk_eod.py --last-days 7
        """
    )

    parser.add_argument(
        '--date',
        type=parse_date,
        help='Specific date to collect (YYYY-MM-DD). Defaults to yesterday.'
    )
    parser.add_argument(
        '--start-date',
        type=parse_date,
        help='Start date for range collection (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=parse_date,
        help='End date for range collection (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--last-days',
        type=int,
        help='Collect last N days of data'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.start_date and not args.end_date:
        parser.error("--start-date requires --end-date")
    if args.end_date and not args.start_date:
        parser.error("--end-date requires --start-date")

    # Determine collection mode
    if args.start_date and args.end_date:
        # Date range mode
        start_date = args.start_date
        end_date = args.end_date
        mode = 'range'
    elif args.last_days:
        # Last N days mode
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.last_days - 1)
        mode = 'range'
    elif args.date:
        # Single date mode
        target_date = args.date
        mode = 'single'
    else:
        # Default: yesterday
        target_date = datetime.now().date() - timedelta(days=1)
        mode = 'single'

    logger.info("="*80)
    logger.info("BULK EOD PRICE COLLECTION")
    logger.info("="*80)

    if mode == 'single':
        logger.info(f"Mode: Single date")
        logger.info(f"Date: {target_date}")
    else:
        logger.info(f"Mode: Date range")
        logger.info(f"Start: {start_date}")
        logger.info(f"End: {end_date}")
        logger.info(f"Days: {(end_date - start_date).days + 1}")

    logger.info("")

    # Create logs directory if needed
    Path('logs').mkdir(exist_ok=True)

    # Run collection
    with get_session() as session:
        collector = BulkPriceCollector(session)

        if mode == 'single':
            # Collect single date
            result = collector.collect_bulk_eod(target_date)

            if result['success']:
                logger.info("\n" + "="*80)
                logger.info("[SUCCESS] COLLECTION SUCCESSFUL")
                logger.info("="*80)
                logger.info(f"Date: {result['date']}")
                logger.info(f"Symbols received: {result['symbols_received']:,}")
                logger.info(f"Symbols inserted: {result['symbols_inserted']:,}")
                logger.info("="*80)
                return 0
            else:
                logger.error("\n" + "="*80)
                logger.error("[FAILED] COLLECTION FAILED")
                logger.error("="*80)
                if 'error' in result:
                    logger.error(f"Error: {result['error']}")
                return 1

        else:
            # Collect date range
            results = collector.collect_bulk_date_range(start_date, end_date)

            logger.info("\n" + "="*80)
            if results['dates_failed'] == 0:
                logger.info("[SUCCESS] COLLECTION SUCCESSFUL")
            else:
                logger.info("[WARNING] COLLECTION COMPLETED WITH ERRORS")
            logger.info("="*80)
            logger.info(f"Date range: {results['start_date']} to {results['end_date']}")
            logger.info(f"Dates processed: {results['dates_processed']}")
            logger.info(f"Successful: {results['dates_successful']}")
            logger.info(f"Failed: {results['dates_failed']}")
            logger.info(f"Total symbols: {results['total_symbols']:,}")
            logger.info("="*80)

            return 0 if results['dates_failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
