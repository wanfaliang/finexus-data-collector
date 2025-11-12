"""
Backfill prices_daily from prices_daily_bulk

For each symbol in prices_daily:
- Check if prices_daily_bulk has more recent data
- If bulk has newer dates, copy those records to prices_daily
- Maps bulk's adj_close to daily's adj_close (dividend-adjusted)
- Note: Bulk doesn't have adj_open, adj_high, adj_low (set to NULL)
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import date
from decimal import Decimal

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.database.models import PriceDaily, PriceDailyBulk
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

# Create logs directory if needed
Path('logs').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backfill_prices_from_bulk.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_symbols_to_backfill(session, limit=None):
    """
    Get list of symbols that need backfilling
    Returns list of (symbol, daily_max_date, bulk_max_date) tuples
    """
    # Create subqueries first
    daily_subq = session.query(
        PriceDaily.symbol,
        func.max(PriceDaily.date).label('daily_max_date')
    ).group_by(PriceDaily.symbol).subquery('daily')

    bulk_subq = session.query(
        PriceDailyBulk.symbol,
        func.max(PriceDailyBulk.date).label('bulk_max_date')
    ).group_by(PriceDailyBulk.symbol).subquery('bulk')

    # Join to find where bulk is ahead - use explicit select_from()
    results = session.query(
        daily_subq.c.symbol,
        daily_subq.c.daily_max_date,
        bulk_subq.c.bulk_max_date
    ).select_from(daily_subq).join(
        bulk_subq,
        daily_subq.c.symbol == bulk_subq.c.symbol
    ).filter(
        bulk_subq.c.bulk_max_date > daily_subq.c.daily_max_date
    ).all()

    logger.info(f"Found {len(results):,} symbols where bulk is ahead")

    if limit:
        results = results[:limit]
        logger.info(f"Limited to {limit} symbols")

    return results


def get_previous_close(session, symbol, date):
    """Get the previous trading day's adj_close price for a symbol"""
    result = session.query(PriceDaily.adj_close)\
        .filter(PriceDaily.symbol == symbol)\
        .filter(PriceDaily.date < date)\
        .order_by(PriceDaily.date.desc())\
        .first()

    return result[0] if result else None


def backfill_symbol(session, symbol, daily_max_date, bulk_max_date, dry_run=False):
    """
    Backfill prices for a single symbol from bulk to daily

    Returns:
        Number of records inserted
    """
    # Fetch records from bulk that are newer than daily
    bulk_records = session.query(PriceDailyBulk)\
        .filter(PriceDailyBulk.symbol == symbol)\
        .filter(PriceDailyBulk.date > daily_max_date)\
        .filter(PriceDailyBulk.date <= bulk_max_date)\
        .order_by(PriceDailyBulk.date)\
        .all()

    if not bulk_records:
        logger.warning(f"  {symbol}: No bulk records found (query issue?)")
        return 0

    records_to_insert = []

    for bulk_record in bulk_records:
        # Build record for prices_daily (dividend-adjusted)
        # Note: Bulk only has adj_close, not adj_open/high/low
        record = {
            'symbol': symbol,
            'date': bulk_record.date,
            'adj_open': None,  # Bulk doesn't have adj_open
            'adj_high': None,  # Bulk doesn't have adj_high
            'adj_low': None,   # Bulk doesn't have adj_low
            'adj_close': bulk_record.adj_close,
            'volume': bulk_record.volume
        }

        records_to_insert.append(record)

    if dry_run:
        logger.info(f"  {symbol}: Would insert {len(records_to_insert)} records ({bulk_records[0].date} to {bulk_records[-1].date})")
        return 0

    # Insert records using UPSERT
    if records_to_insert:
        stmt = insert(PriceDaily).values(records_to_insert)

        # On conflict, update all fields except primary key
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'date'],
            set_={
                'adj_close': stmt.excluded.adj_close,
                'volume': stmt.excluded.volume
                # Note: adj_open, adj_high, adj_low remain NULL (bulk doesn't have them)
            }
        )

        session.execute(stmt)
        session.commit()

        logger.info(f"  {symbol}: Inserted {len(records_to_insert)} records ({bulk_records[0].date} to {bulk_records[-1].date})")
        return len(records_to_insert)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Backfill prices_daily from prices_daily_bulk',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show what would be backfilled (dry run)
  python scripts/backfill_prices_from_bulk.py --dry-run

  # Backfill all symbols where bulk is ahead
  python scripts/backfill_prices_from_bulk.py

  # Backfill first 100 symbols only
  python scripts/backfill_prices_from_bulk.py --limit 100
        """
    )

    parser.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Limit number of symbols to backfill'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be backfilled without actually doing it'
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("BACKFILL PRICES_DAILY FROM PRICES_DAILY_BULK")
    logger.info("="*80)
    if args.limit:
        logger.info(f"Limit: {args.limit} symbols")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("")

    with get_session() as session:
        # Find symbols to backfill
        symbols_to_backfill = get_symbols_to_backfill(session, args.limit)

        if not symbols_to_backfill:
            logger.info("No symbols need backfilling - prices_daily is up to date!")
            return 0

        # Show summary
        logger.info(f"\nSymbols to backfill:")
        for i, (symbol, daily_max, bulk_max) in enumerate(symbols_to_backfill[:10], 1):
            days_gap = (bulk_max - daily_max).days
            logger.info(f"  {i}. {symbol}: {daily_max} → {bulk_max} ({days_gap} days)")

        if len(symbols_to_backfill) > 10:
            logger.info(f"  ... and {len(symbols_to_backfill) - 10} more symbols")

        if args.dry_run:
            logger.info(f"\n[DRY RUN] Would backfill the above symbols. Use without --dry-run to actually fill.")
            return 0

        # Backfill each symbol
        logger.info(f"\n{'='*80}")
        logger.info("STARTING BACKFILL")
        logger.info(f"{'='*80}")

        total_records = 0
        success_count = 0
        failed_symbols = []

        for i, (symbol, daily_max, bulk_max) in enumerate(symbols_to_backfill, 1):
            logger.info(f"\n[{i}/{len(symbols_to_backfill)}] {symbol}")

            try:
                records = backfill_symbol(session, symbol, daily_max, bulk_max, args.dry_run)
                total_records += records
                success_count += 1
            except Exception as e:
                logger.error(f"  {symbol}: Failed - {e}")
                failed_symbols.append(symbol)
                session.rollback()

        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("BACKFILL COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Symbols processed: {len(symbols_to_backfill)}")
        logger.info(f"Successfully backfilled: {success_count}")
        logger.info(f"Failed: {len(failed_symbols)}")
        logger.info(f"Total records inserted: {total_records:,}")

        if failed_symbols:
            logger.warning(f"\nFailed symbols:")
            for s in failed_symbols:
                logger.warning(f"  - {s}")

        logger.info(f"{'='*80}")

        return 0 if not failed_symbols else 1


if __name__ == "__main__":
    sys.exit(main())
