"""
Collect Economic Calendar
Fetches economic data releases calendar from FMP API
Supports upcoming events, specific date ranges, and historical backfill
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.economic_calendar_collector import EconomicCalendarCollector

# Create logs directory if needed (BEFORE logging setup)
Path('logs').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/economic_calendar.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Collect economic calendar data from FMP API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect upcoming 90 days (default)
  python scripts/collect_economic_calendar.py

  # Collect upcoming 30 days
  python scripts/collect_economic_calendar.py --upcoming 30

  # Collect specific date range
  python scripts/collect_economic_calendar.py --from 2024-01-01 --to 2024-03-31

  # Backfill last year
  python scripts/collect_economic_calendar.py --backfill-days 365

  # Backfill from specific date
  python scripts/collect_economic_calendar.py --backfill-from 2020-01-01
        """
    )

    parser.add_argument(
        '--upcoming',
        type=int,
        metavar='DAYS',
        help='Collect upcoming N days (default: 90, max: 90)'
    )

    parser.add_argument(
        '--from',
        dest='from_date',
        type=parse_date,
        metavar='YYYY-MM-DD',
        help='Start date for specific range'
    )

    parser.add_argument(
        '--to',
        dest='to_date',
        type=parse_date,
        metavar='YYYY-MM-DD',
        help='End date for specific range'
    )

    parser.add_argument(
        '--backfill-days',
        type=int,
        metavar='DAYS',
        help='Backfill last N days of historical data'
    )

    parser.add_argument(
        '--backfill-from',
        type=parse_date,
        metavar='YYYY-MM-DD',
        help='Backfill from specific start date to today'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.from_date and not args.to_date:
        parser.error("--to is required when using --from")
    if args.to_date and not args.from_date:
        parser.error("--from is required when using --to")

    if sum([
        args.upcoming is not None,
        args.from_date is not None,
        args.backfill_days is not None,
        args.backfill_from is not None
    ]) > 1:
        parser.error("Only one collection mode can be specified")

    logger.info("="*80)
    logger.info("ECONOMIC CALENDAR COLLECTION")
    logger.info("="*80)

    with get_session() as session:
        collector = EconomicCalendarCollector(session)

        # Determine collection mode
        if args.from_date and args.to_date:
            # Specific date range
            logger.info(f"Mode: Specific date range")
            result = collector.collect_range(args.from_date, args.to_date)

            if result['success']:
                logger.info(f"\n{'='*80}")
                logger.info(f"[SUCCESS] Collection successful")
                logger.info(f"  Date range: {result['from_date']} to {result['to_date']}")
                logger.info(f"  Events received: {result['events_received']:,}")
                logger.info(f"  Events upserted: {result['events_upserted']:,}")
                logger.info(f"{'='*80}")
                return 0
            else:
                logger.error(f"\n{'='*80}")
                logger.error(f"[FAILED] Collection failed")
                logger.error(f"{'='*80}")
                return 1

        elif args.backfill_days:
            # Backfill last N days
            logger.info(f"Mode: Backfill last {args.backfill_days} days")
            start_date = date.today() - timedelta(days=args.backfill_days)
            end_date = date.today()

            result = collector.collect_historical(start_date, end_date)

            logger.info(f"\n{'='*80}")
            logger.info(f"[COMPLETED] Backfill finished")
            logger.info(f"  Chunks successful: {result['chunks_successful']}/{result['chunks_processed']}")
            logger.info(f"  Total events: {result['total_events']:,}")
            logger.info(f"{'='*80}")
            return 0 if result['chunks_failed'] == 0 else 1

        elif args.backfill_from:
            # Backfill from specific date
            logger.info(f"Mode: Backfill from {args.backfill_from}")
            result = collector.collect_historical(args.backfill_from, date.today())

            logger.info(f"\n{'='*80}")
            logger.info(f"[COMPLETED] Backfill finished")
            logger.info(f"  Chunks successful: {result['chunks_successful']}/{result['chunks_processed']}")
            logger.info(f"  Total events: {result['total_events']:,}")
            logger.info(f"{'='*80}")
            return 0 if result['chunks_failed'] == 0 else 1

        else:
            # Default: upcoming events
            days = args.upcoming if args.upcoming else 90
            logger.info(f"Mode: Upcoming {days} days")

            result = collector.collect_upcoming(days=days)

            if result['success']:
                logger.info(f"\n{'='*80}")
                logger.info(f"[SUCCESS] Collection successful")
                logger.info(f"  Date range: {result['from_date']} to {result['to_date']}")
                logger.info(f"  Events received: {result['events_received']:,}")
                logger.info(f"  Events upserted: {result['events_upserted']:,}")
                logger.info(f"{'='*80}")
                return 0
            else:
                logger.error(f"\n{'='*80}")
                logger.error(f"[FAILED] Collection failed")
                logger.error(f"{'='*80}")
                return 1


if __name__ == "__main__":
    sys.exit(main())
