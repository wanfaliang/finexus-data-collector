"""
Backfill Bulk EOD Price Gaps (RECURSIVE RETRY VERSION)
Detects missing dates in prices_daily_bulk table and backfills them
Keeps retrying failed dates until failures < 3
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
from sqlalchemy import func, select
from src.database.models import PriceDailyBulk

# Create logs directory if needed
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding for Windows console
import io

# Create a UTF-8 wrapper for stdout to handle emojis on Windows
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bulk_eod_gap_fill_recursive.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
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

    # Generate expected date range (weekdays only)
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


def fill_recent_dates(session, collector, retry_delay: int = 3):
    """
    Fill missing dates from the latest date in bulk table up to today

    Args:
        session: Database session
        collector: BulkPriceCollector instance
        retry_delay: Seconds to wait between retries

    Returns:
        Tuple of (filled_count, failed_dates)
    """
    # Get the latest date in bulk table
    max_date_result = session.query(func.max(PriceDailyBulk.date)).scalar()

    if not max_date_result:
        logger.warning("No data in prices_daily_bulk table - cannot determine recent gap")
        return 0, []

    latest_bulk_date = max_date_result
    today = date.today()

    # Calculate missing recent dates (weekdays only)
    missing_recent = []
    current = latest_bulk_date + timedelta(days=1)

    while current < today:  # Up to yesterday
        if current.weekday() < 5:  # Monday=0 to Friday=4
            missing_recent.append(current)
        current += timedelta(days=1)

    if not missing_recent:
        logger.info(f"✓ Bulk table is current (latest: {latest_bulk_date})")
        return 0, []

    logger.info(f"📅 Latest bulk date: {latest_bulk_date}")
    logger.info(f"📅 Today: {today}")
    logger.info(f"📊 Missing recent dates: {len(missing_recent)} weekdays")

    # Fill recent dates with recursive retry
    filled_count, failed_dates = fill_dates_with_retry(
        collector=collector,
        dates_to_fill=missing_recent,
        phase_name="RECENT DATES",
        retry_delay=retry_delay
    )

    return filled_count, failed_dates


def fill_dates_with_retry(collector, dates_to_fill: list, phase_name: str, retry_delay: int = 3, max_retries: int = 10):
    """
    Fill dates with recursive retry until failures < 3

    Args:
        collector: BulkPriceCollector instance
        dates_to_fill: List of dates to fill
        phase_name: Name for logging (e.g., "RECENT DATES", "HISTORICAL GAPS")
        retry_delay: Seconds to wait between retry attempts
        max_retries: Maximum number of retry rounds to prevent infinite loops

    Returns:
        Tuple of (total_filled_count, final_failed_dates)
    """
    if not dates_to_fill:
        return 0, []

    logger.info(f"\n{'='*80}")
    logger.info(f"FILLING {phase_name}")
    logger.info(f"{'='*80}")
    logger.info(f"Total dates to process: {len(dates_to_fill)}")

    total_filled = 0
    failed_dates = dates_to_fill.copy()
    retry_round = 0

    while len(failed_dates) >= 3 and retry_round < max_retries:
        retry_round += 1

        if retry_round == 1:
            logger.info(f"\n🔄 Round {retry_round}: Processing {len(failed_dates)} dates")
        else:
            logger.info(f"\n🔄 Round {retry_round}: Retrying {len(failed_dates)} failed dates")
            if retry_delay > 0:
                logger.info(f"⏳ Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)

        current_round_filled = 0
        current_round_failed = []

        for i, target_date in enumerate(failed_dates, 1):
            logger.info(f"\n  [{i}/{len(failed_dates)}] Processing {target_date}...")

            result = collector.collect_bulk_eod(target_date=target_date)

            if result['success']:
                logger.info(f"    ✓ {target_date}: {result['symbols_inserted']:,} symbols inserted")
                current_round_filled += 1
            else:
                logger.error(f"    ✗ {target_date}: Failed to collect")
                current_round_failed.append(target_date)

        # Update totals
        total_filled += current_round_filled
        failed_dates = current_round_failed

        # Summary for this round
        logger.info(f"\n{'─'*80}")
        logger.info(f"Round {retry_round} Summary:")
        logger.info(f"  Filled: {current_round_filled}")
        logger.info(f"  Failed: {len(failed_dates)}")
        logger.info(f"  Total filled so far: {total_filled}/{len(dates_to_fill)}")
        logger.info(f"{'─'*80}")

        # Check exit condition
        if len(failed_dates) < 3:
            logger.info(f"\n✓ Success threshold reached (failures < 3)")
            break

        if retry_round >= max_retries:
            logger.warning(f"\n⚠️  Maximum retry rounds ({max_retries}) reached")
            break

    # Final summary
    logger.info(f"\n{'='*80}")
    logger.info(f"{phase_name} - FINAL SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Total dates attempted: {len(dates_to_fill)}")
    logger.info(f"Successfully filled: {total_filled}")
    logger.info(f"Final failures: {len(failed_dates)}")
    logger.info(f"Retry rounds: {retry_round}")

    if failed_dates:
        logger.warning(f"\nFailed dates ({len(failed_dates)}):")
        for d in failed_dates:
            logger.warning(f"  - {d}")

    logger.info(f"{'='*80}\n")

    return total_filled, failed_dates


def backfill_gaps_recursive(max_days: int = 365, max_fills: int = 100, retry_delay: int = 3, dry_run: bool = False):
    """
    Find and backfill missing dates in bulk price data with recursive retry

    Two-phase approach:
    1. First, fill from latest date up to today (get current)
    2. Then, scan for historical gaps and fill them

    Args:
        max_days: Maximum number of days to search from earliest date
        max_fills: Maximum number of dates to backfill in historical phase
        retry_delay: Seconds to wait between retry attempts
        dry_run: If True, only show what would be filled without actually doing it
    """
    logger.info("="*80)
    logger.info("BULK EOD GAP DETECTION AND BACKFILL (RECURSIVE RETRY)")
    logger.info("="*80)
    logger.info(f"Max search window: {max_days} days from earliest date")
    logger.info(f"Max historical fills: {max_fills} dates")
    logger.info(f"Retry delay: {retry_delay} seconds")
    logger.info(f"Dry run: {dry_run}")
    logger.info("")

    with get_session() as session:
        collector = BulkPriceCollector(session)

        # PHASE 1: Fill recent dates (latest_date to today)
        logger.info("PHASE 1: Checking for recent missing dates...")
        logger.info("-" * 80)

        if dry_run:
            # Show what would be filled
            max_date_result = session.query(func.max(PriceDailyBulk.date)).scalar()
            if max_date_result:
                latest_bulk_date = max_date_result
                today = date.today()
                missing_recent = []
                current = latest_bulk_date + timedelta(days=1)
                while current < today:
                    if current.weekday() < 5:
                        missing_recent.append(current)
                    current += timedelta(days=1)

                if missing_recent:
                    logger.info(f"[DRY RUN] Would fill {len(missing_recent)} recent dates")
                    for d in missing_recent:
                        logger.info(f"  - {d}")
            recent_filled, recent_failed = 0, []
        else:
            recent_filled, recent_failed = fill_recent_dates(session, collector, retry_delay)

        # PHASE 2: Find and fill historical gaps
        logger.info("\nPHASE 2: Checking for historical gaps...")
        logger.info("-" * 80)

        missing_dates = find_missing_dates(session, max_days)

        if not missing_dates:
            logger.info("✓ No historical gaps found - all historical data is complete!")
            if recent_filled > 0:
                logger.info(f"\n{'='*80}")
                logger.info("OVERALL SUMMARY")
                logger.info(f"{'='*80}")
                logger.info(f"Recent dates filled: {recent_filled}")
                logger.info(f"Recent dates failed: {len(recent_failed)}")
                logger.info(f"Historical gaps: 0")
                logger.info(f"{'='*80}")
            return 0 if len(recent_failed) < 3 else 1

        # Limit to max_fills
        dates_to_fill = missing_dates[:max_fills]

        logger.info(f"\nHistorical gaps found: {len(missing_dates)}")
        logger.info(f"Will process: {len(dates_to_fill)} dates (limited by --max-fills)")

        if dry_run:
            logger.info("\n[DRY RUN] Would backfill the following dates:")
            for i, d in enumerate(dates_to_fill, 1):
                logger.info(f"  {i}. {d}")
            if len(missing_dates) > max_fills:
                logger.info(f"  ... and {len(missing_dates) - max_fills} more")
            logger.info("\nUse without --dry-run to actually fill.")
            return 0

        # Backfill historical gaps with recursive retry
        historical_filled, historical_failed = fill_dates_with_retry(
            collector=collector,
            dates_to_fill=dates_to_fill,
            phase_name="HISTORICAL GAPS",
            retry_delay=retry_delay
        )

        # Overall Summary
        logger.info(f"\n{'='*80}")
        logger.info("OVERALL BACKFILL SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"PHASE 1 - Recent dates filled: {recent_filled}")
        logger.info(f"PHASE 1 - Recent dates failed: {len(recent_failed)}")
        logger.info(f"PHASE 2 - Historical gaps found: {len(missing_dates)}")
        logger.info(f"PHASE 2 - Historical gaps attempted: {len(dates_to_fill)}")
        logger.info(f"PHASE 2 - Historical gaps filled: {historical_filled}")
        logger.info(f"PHASE 2 - Historical gaps failed: {len(historical_failed)}")
        logger.info(f"Total filled: {recent_filled + historical_filled}")
        logger.info(f"Total failed: {len(recent_failed) + len(historical_failed)}")

        if len(missing_dates) > max_fills:
            remaining = len(missing_dates) - max_fills
            logger.info(f"\n📋 Remaining gaps: {remaining} dates")
            logger.info(f"Run again to continue backfilling")

        logger.info(f"{'='*80}")

        total_failures = len(recent_failed) + len(historical_failed)
        return 0 if total_failures < 3 else 1


def main():
    parser = argparse.ArgumentParser(
        description='Find and backfill missing dates in bulk EOD price data (RECURSIVE RETRY)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script automatically retries failed dates until failures < 3.

Examples:
  # Find and show missing dates (dry run)
  python scripts/backfill_bulk_eod_gaps_recursive.py --dry-run

  # Backfill with default settings (up to 100 dates, 3 sec retry delay)
  python scripts/backfill_bulk_eod_gaps_recursive.py

  # Backfill with custom retry delay and search window
  python scripts/backfill_bulk_eod_gaps_recursive.py --max-days 180 --retry-delay 5

  # Backfill many dates with longer retry delay
  python scripts/backfill_bulk_eod_gaps_recursive.py --max-fills 200 --retry-delay 10
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
        default=100,
        metavar='N',
        help='Maximum number of dates to backfill in historical phase (default: 100)'
    )

    parser.add_argument(
        '--retry-delay',
        type=int,
        default=3,
        metavar='SECONDS',
        help='Seconds to wait between retry attempts (default: 3)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be filled without actually doing it'
    )

    args = parser.parse_args()

    return backfill_gaps_recursive(
        max_days=args.max_days,
        max_fills=args.max_fills,
        retry_delay=args.retry_delay,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    sys.exit(main())
