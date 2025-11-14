"""
Collect Bulk EOD Prices (SLOW VERSION)
Fetches end-of-day prices with rate limiting and delays between requests
Useful for avoiding API rate limits or reducing server load
"""
import sys
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime, date, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.bulk_price_collector import BulkPriceCollector

# Create logs directory if needed (BEFORE logging setup)
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding for Windows console
import io

# Create a UTF-8 wrapper for stdout to handle emojis on Windows
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bulk_eod_collection_slow.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
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


def is_weekend(target_date: date) -> bool:
    """Check if date is a weekend (Saturday=5, Sunday=6)"""
    return target_date.weekday() >= 5


def collect_with_delay(collector, target_date: date, delay: int, skip_weekends: bool) -> dict:
    """
    Collect data for a single date with pre-delay

    Args:
        collector: BulkPriceCollector instance
        target_date: Date to collect
        delay: Seconds to wait before collection
        skip_weekends: Skip weekend dates

    Returns:
        Result dictionary
    """
    # Check if weekend
    if skip_weekends and is_weekend(target_date):
        logger.info(f"⏭️  Skipping {target_date} (weekend)")
        return {
            'date': target_date,
            'symbols_received': 0,
            'symbols_inserted': 0,
            'success': True,
            'skipped': True
        }

    # Wait before collecting
    if delay > 0:
        logger.info(f"⏳ Waiting {delay} seconds before collecting {target_date}...")
        time.sleep(delay)

    # Collect
    return collector.collect_bulk_eod(target_date)


def main():
    parser = argparse.ArgumentParser(
        description='Collect bulk EOD prices from FMP API (SLOW VERSION with rate limiting)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect yesterday's data (default)
  python scripts/collect_bulk_eod_slow.py

  # Collect specific date with 10 second delay
  python scripts/collect_bulk_eod_slow.py --date 2024-11-01 --delay 10

  # Collect date range with 5 second delays, skip weekends
  python scripts/collect_bulk_eod_slow.py --start-date 2024-11-01 --end-date 2024-11-05 --delay 5 --skip-weekends

  # Collect last 30 days with minimal delay
  python scripts/collect_bulk_eod_slow.py --last-days 30 --delay 2 --skip-weekends
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
    parser.add_argument(
        '--delay',
        type=int,
        default=5,
        help='Seconds to wait between each date collection (default: 5)'
    )
    parser.add_argument(
        '--skip-weekends',
        action='store_true',
        help='Skip weekend dates (Saturday and Sunday)'
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
    logger.info("BULK EOD PRICE COLLECTION (SLOW MODE)")
    logger.info("="*80)

    if mode == 'single':
        logger.info(f"Mode: Single date")
        logger.info(f"Date: {target_date}")
    else:
        logger.info(f"Mode: Date range")
        logger.info(f"Start: {start_date}")
        logger.info(f"End: {end_date}")
        logger.info(f"Days: {(end_date - start_date).days + 1}")

    logger.info(f"Delay between dates: {args.delay} seconds")
    logger.info(f"Skip weekends: {args.skip_weekends}")
    logger.info("")

    # Create logs directory if needed
    Path('logs').mkdir(exist_ok=True)

    # Run collection
    with get_session() as session:
        collector = BulkPriceCollector(session)

        if mode == 'single':
            # Collect single date (no delay for single date)
            result = collect_with_delay(collector, target_date, 0, args.skip_weekends)

            if result.get('skipped'):
                logger.info("\n" + "="*80)
                logger.info("[SKIPPED] DATE SKIPPED")
                logger.info("="*80)
                return 0

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
            # Collect date range with delays
            results = {
                'start_date': start_date,
                'end_date': end_date,
                'dates_processed': 0,
                'dates_successful': 0,
                'dates_failed': 0,
                'dates_skipped': 0,
                'total_symbols': 0
            }

            current_date = start_date
            date_count = 0
            total_dates = (end_date - start_date).days + 1

            while current_date <= end_date:
                date_count += 1
                logger.info(f"\n📅 Processing date {date_count}/{total_dates}: {current_date}")

                # Collect with delay (except for first date)
                delay = args.delay if date_count > 1 else 0
                result = collect_with_delay(collector, current_date, delay, args.skip_weekends)

                results['dates_processed'] += 1

                if result.get('skipped'):
                    results['dates_skipped'] += 1
                elif result['success']:
                    results['dates_successful'] += 1
                    results['total_symbols'] += result['symbols_inserted']
                else:
                    results['dates_failed'] += 1

                # Move to next day
                current_date += timedelta(days=1)

            logger.info("\n" + "="*80)
            if results['dates_failed'] == 0:
                logger.info("[SUCCESS] COLLECTION SUCCESSFUL")
            else:
                logger.info("[WARNING] COLLECTION COMPLETED WITH ERRORS")
            logger.info("="*80)
            logger.info(f"Date range: {results['start_date']} to {results['end_date']}")
            logger.info(f"Dates processed: {results['dates_processed']}")
            logger.info(f"Successful: {results['dates_successful']}")
            logger.info(f"Skipped (weekends): {results['dates_skipped']}")
            logger.info(f"Failed: {results['dates_failed']}")
            logger.info(f"Total symbols: {results['total_symbols']:,}")
            logger.info("="*80)

            return 0 if results['dates_failed'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
