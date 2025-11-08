"""
Backfill Bulk EOD Price Gaps
Detects missing dates in prices_daily_bulk table and backfills them
Useful when bulk EOD endpoint fails for specific dates
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
from sqlalchemy import func, select
from src.database.models import PriceDailyBulk

# Create logs directory if needed
Path('logs').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bulk_eod_gap_fill.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_date_range(session) -> tuple:
    """Get the min and max dates in the prices_daily_bulk table"""
    result = session.query(
        func.min(PriceDailyBulk.date),
        func.max(PriceDailyBulk.date)
    ).first()

    return result[0], result[1]


def get_existing_dates(session, start_date: date, end_date: date) -> set:
    """Get all dates that have data in the table within the date range"""
    stmt = select(PriceDailyBulk.date).distinct().where(
        PriceDailyBulk.date >= start_date,
        PriceDailyBulk.date <= end_date
    )

    result = session.execute(stmt)
    return {row[0] for row in result}


def find_missing_dates(session, max_days: int = 365) -> list:
    """
    Find missing dates in the bulk price data

    Args:
        session: Database session
        max_days: Maximum number of days to check from earliest date

    Returns:
        List of missing dates, sorted chronologically
    """
    # Get date range
    min_date, max_date = get_date_range(session)

    if not min_date or not max_date:
        logger.warning("No data found in prices_daily_bulk table")
        return []

    logger.info(f"Table date range: {min_date} to {max_date}")

    # Limit the search window
    search_end = min(min_date + timedelta(days=max_days), date.today())
    logger.info(f"Searching for gaps: {min_date} to {search_end}")

    # Get all existing dates in the range
    existing_dates = get_existing_dates(session, min_date, search_end)
    logger.info(f"Found {len(existing_dates)} dates with data")

    # Generate expected date range (weekdays only - most major exchanges don't trade weekends)
    # Weekend data is sparse and not worth the API calls for gap-filling
    missing_dates = []
    current = min_date

    while current <= search_end:
        # Only check weekdays (Monday=0 to Friday=4)
        if current.weekday() < 5:
            if current not in existing_dates:
                missing_dates.append(current)
        current += timedelta(days=1)

    logger.info(f"Found {len(missing_dates)} missing weekdays")
    return sorted(missing_dates)


def fill_recent_dates(session, collector, dry_run=False):
    """
    Fill missing dates from the latest date in bulk table up to today

    Args:
        session: Database session
        collector: BulkPriceCollector instance
        dry_run: If True, only show what would be filled

    Returns:
        Number of dates filled
    """
    # Get the latest date in bulk table
    max_date_result = session.query(func.max(PriceDailyBulk.date)).scalar()

    if not max_date_result:
        logger.warning("No data in prices_daily_bulk table - cannot determine recent gap")
        return 0

    latest_bulk_date = max_date_result
    today = date.today()

    # Calculate missing recent dates (weekdays only)
    missing_recent = []
    current = latest_bulk_date + timedelta(days=1)

    while current < today:  # Up to yesterday (today's data may not be available yet)
        if current.weekday() < 5:  # Monday=0 to Friday=4
            missing_recent.append(current)
        current += timedelta(days=1)

    if not missing_recent:
        logger.info(f" Bulk table is current (latest: {latest_bulk_date})")
        return 0

    logger.info(f" Latest bulk date: {latest_bulk_date}")
    logger.info(f" Today: {today}")
    logger.info(f" Missing recent dates: {len(missing_recent)} weekdays")

    for d in missing_recent:
        logger.info(f"    - {d}")

    if dry_run:
        logger.info(f"\n[DRY RUN] Would fill {len(missing_recent)} recent dates")
        return 0

    # Fill recent dates
    logger.info(f"\n{'='*80}")
    logger.info("FILLING RECENT DATES (UP TO TODAY)")
    logger.info(f"{'='*80}")

    filled_count = 0
    failed_dates = []

    for i, missing_date in enumerate(missing_recent, 1):
        logger.info(f"\n[{i}/{len(missing_recent)}] Filling {missing_date}...")

        result = collector.collect_bulk_eod(target_date=missing_date)

        if result['success']:
            logger.info(f"   {missing_date}: {result['symbols_inserted']:,} symbols inserted")
            filled_count += 1
        else:
            logger.error(f"   {missing_date}: Failed to collect")
            failed_dates.append(missing_date)

    logger.info(f"\n{'='*80}")
    logger.info(f"Recent dates filled: {filled_count}/{len(missing_recent)}")
    if failed_dates:
        logger.warning(f"Failed recent dates: {', '.join(str(d) for d in failed_dates)}")
    logger.info(f"{'='*80}\n")

    return filled_count


def backfill_gaps(max_days: int = 365, max_fills: int = 10, dry_run: bool = False):
    """
    Find and backfill missing dates in bulk price data

    Two-phase approach:
    1. First, fill from latest date up to today (get current)
    2. Then, scan for historical gaps and fill them

    Args:
        max_days: Maximum number of days to search from earliest date
        max_fills: Maximum number of dates to backfill in this run
        dry_run: If True, only show what would be filled without actually doing it
    """
    logger.info("="*80)
    logger.info("BULK EOD GAP DETECTION AND BACKFILL")
    logger.info("="*80)
    logger.info(f"Max search window: {max_days} days from earliest date")
    logger.info(f"Max fills per run: {max_fills} dates")
    logger.info(f"Dry run: {dry_run}")
    logger.info("")

    with get_session() as session:
        collector = BulkPriceCollector(session)

        # PHASE 1: Fill recent dates (latest_date to today)
        logger.info("PHASE 1: Checking for recent missing dates...")
        logger.info("-" * 80)
        recent_filled = fill_recent_dates(session, collector, dry_run)

        if dry_run and recent_filled == 0:
            # Continue to show historical gaps even in dry run
            pass

        # PHASE 2: Find and fill historical gaps
        logger.info("\nPHASE 2: Checking for historical gaps...")
        logger.info("-" * 80)
        # Find missing dates
        missing_dates = find_missing_dates(session, max_days)

        if not missing_dates:
            logger.info("No historical gaps found - all historical data is complete!")
            if recent_filled > 0:
                # Show summary even if only phase 1 had work
                logger.info(f"\n{'='*80}")
                logger.info("SUMMARY")
                logger.info(f"{'='*80}")
                logger.info(f"Recent dates filled: {recent_filled}")
                logger.info(f"Historical gaps: 0")
                logger.info(f"{'='*80}")
            return 0

        # Show what we found
        logger.info(f"\nMissing dates to backfill:")
        for i, missing_date in enumerate(missing_dates[:max_fills], 1):
            logger.info(f"  {i}. {missing_date}")

        if len(missing_dates) > max_fills:
            logger.info(f"  ... and {len(missing_dates) - max_fills} more (limited by --max-fills)")

        if dry_run:
            logger.info("\n[DRY RUN] Would backfill the above dates. Use without --dry-run to actually fill.")
            return 0

        # Backfill the missing dates
        logger.info(f"\n{'='*80}")
        logger.info("STARTING HISTORICAL BACKFILL")
        logger.info(f"{'='*80}")

        historical_filled = 0
        failed_dates = []

        for i, missing_date in enumerate(missing_dates[:max_fills], 1):
            logger.info(f"\n[{i}/{min(max_fills, len(missing_dates))}] Backfilling {missing_date}...")

            result = collector.collect_bulk_eod(target_date=missing_date)

            if result['success']:
                logger.info(f"   {missing_date}: {result['symbols_inserted']:,} symbols inserted")
                historical_filled += 1
            else:
                logger.error(f"  {missing_date}: Failed to collect")
                failed_dates.append(missing_date)

        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("BACKFILL COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"PHASE 1 - Recent dates filled: {recent_filled}")
        logger.info(f"PHASE 2 - Historical gaps found: {len(missing_dates)}")
        logger.info(f"PHASE 2 - Historical gaps attempted: {min(max_fills, len(missing_dates))}")
        logger.info(f"PHASE 2 - Historical gaps filled: {historical_filled}")
        logger.info(f"Total dates filled: {recent_filled + historical_filled}")
        logger.info(f"Failed: {len(failed_dates)}")

        if failed_dates:
            logger.warning(f"\nFailed dates (may need manual retry):")
            for d in failed_dates:
                logger.warning(f"  - {d}")

        if len(missing_dates) > max_fills:
            remaining = len(missing_dates) - max_fills
            logger.info(f"\nRemaining gaps: {remaining} dates")
            logger.info(f"Run again to continue backfilling")

        logger.info(f"{'='*80}")

        return 0 if not failed_dates else 1


def main():
    parser = argparse.ArgumentParser(
        description='Find and backfill missing dates in bulk EOD price data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find and show missing dates (dry run)
  python scripts/backfill_bulk_eod_gaps.py --dry-run

  # Backfill up to 10 missing dates (default)
  python scripts/backfill_bulk_eod_gaps.py

  # Backfill up to 5 dates, search last 180 days
  python scripts/backfill_bulk_eod_gaps.py --max-days 180 --max-fills 5

  # Backfill all gaps found in last year
  python scripts/backfill_bulk_eod_gaps.py --max-days 365 --max-fills 100
        """
    )

    parser.add_argument(
        '--max-days',
        type=int,
        default=365,
        metavar='N',
        help='Maximum number of days to search from earliest date (default: 365)'
    )

    parser.add_argument(
        '--max-fills',
        type=int,
        default=10,
        metavar='N',
        help='Maximum number of dates to backfill in this run (default: 10)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be filled without actually doing it'
    )

    args = parser.parse_args()

    return backfill_gaps(
        max_days=args.max_days,
        max_fills=args.max_fills,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    sys.exit(main())
