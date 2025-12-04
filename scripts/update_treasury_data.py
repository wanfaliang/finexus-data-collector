"""
Update Treasury Data - Incremental update script

Checks for new auction results and updates the database.
Can be scheduled to run daily to keep data fresh.

Usage:
    python scripts/update_treasury_data.py [options]

Options:
    --days N            Number of days to look back for updates (default: 30)
    --include-upcoming  Also refresh the upcoming auctions calendar
    --force             Force update even if recently updated
    --dry-run           Preview what would be collected without actually collecting

Examples:
    # Standard daily update
    python scripts/update_treasury_data.py

    # Update with upcoming calendar
    python scripts/update_treasury_data.py --include-upcoming

    # Force update last 60 days
    python scripts/update_treasury_data.py --days 60 --force
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta, UTC
import io

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings
from src.database.connection import get_session
from src.treasury import TreasuryClient, TreasuryCollector
from src.database.treasury_tracking_models import TreasuryDataFreshness

# Create logs directory
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/treasury_updates.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def check_needs_update(session, data_type: str, force: bool = False, min_hours: float = 24) -> bool:
    """Check if Treasury data needs updating based on freshness tracking"""
    if force:
        return True

    freshness = session.query(TreasuryDataFreshness).filter(
        TreasuryDataFreshness.data_type == data_type
    ).first()

    if not freshness:
        logger.info(f"{data_type}: No freshness record, needs update")
        return True

    if freshness.needs_update:
        logger.info(f"{data_type}: Marked as needing update")
        return True

    if freshness.update_in_progress:
        logger.warning(f"{data_type}: Update already in progress, skipping")
        return False

    # Check if last update was more than min_hours ago
    if freshness.last_update_completed:
        hours_since_update = (datetime.now(UTC) - freshness.last_update_completed).total_seconds() / 3600
        if hours_since_update < min_hours:
            logger.info(f"{data_type}: Updated {hours_since_update:.1f} hours ago, skipping")
            return False

    return True


def update_freshness(session, data_type: str, total_records: int, latest_date):
    """Update freshness record after successful collection"""
    freshness = session.query(TreasuryDataFreshness).filter(
        TreasuryDataFreshness.data_type == data_type
    ).first()

    if not freshness:
        freshness = TreasuryDataFreshness(data_type=data_type)
        session.add(freshness)

    freshness.last_update_completed = datetime.now(UTC)
    freshness.needs_update = False
    freshness.update_in_progress = False
    freshness.total_records = total_records
    freshness.latest_data_date = latest_date
    freshness.total_updates = (freshness.total_updates or 0) + 1
    session.commit()


def main():
    parser = argparse.ArgumentParser(description='Update Treasury auction data')
    parser.add_argument('--days', type=int, default=30,
                        help='Number of days to look back for updates (default: 30)')
    parser.add_argument('--include-upcoming', action='store_true',
                        help='Also refresh the upcoming auctions calendar')
    parser.add_argument('--force', action='store_true',
                        help='Force update even if data is recent')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without collecting')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"TREASURY DATA UPDATE - {datetime.now()}")
    logger.info("=" * 80)
    logger.info(f"Days: {args.days}")
    logger.info(f"Include Upcoming: {args.include_upcoming}")
    logger.info(f"Force: {args.force}")

    if args.dry_run:
        logger.info("DRY RUN - No data will be collected")
        logger.info("")
        logger.info("Would collect:")
        logger.info(f"  - Auction results from the last {args.days} days")
        if args.include_upcoming:
            logger.info("  - Upcoming auctions calendar")
        return 0

    # Initialize client
    client = TreasuryClient()
    success = True

    with get_session() as session:
        collector = TreasuryCollector(db_session=session, client=client)

        try:
            start_time = datetime.now()

            # Update upcoming auctions if requested
            if args.include_upcoming:
                if check_needs_update(session, 'upcoming', args.force, min_hours=6):
                    logger.info("-" * 40)
                    logger.info("Refreshing upcoming auctions calendar...")
                    upcoming_count = collector.collect_upcoming_auctions()
                    logger.info(f"Upcoming auctions: {upcoming_count} new entries")

                    # Update freshness
                    from src.database.treasury_models import TreasuryUpcomingAuction
                    from sqlalchemy import func
                    total = session.query(func.count(TreasuryUpcomingAuction.upcoming_id)).scalar() or 0
                    latest = session.query(func.max(TreasuryUpcomingAuction.auction_date)).scalar()
                    update_freshness(session, 'upcoming', total, latest)

            # Update recent auctions
            if check_needs_update(session, 'auctions', args.force):
                logger.info("-" * 40)
                logger.info(f"Updating auction results (last {args.days} days)...")

                inserted, updated = collector.collect_recent_auctions(days=args.days)
                stats = collector.stats

                logger.info(f"Auctions fetched: {stats['auctions_fetched']}")
                logger.info(f"Auctions inserted: {inserted}")
                logger.info(f"Auctions updated: {updated}")

                # Update freshness
                from src.database.treasury_models import TreasuryAuction
                from sqlalchemy import func
                total = session.query(func.count(TreasuryAuction.auction_id)).scalar() or 0
                latest = session.query(func.max(TreasuryAuction.auction_date)).scalar()
                update_freshness(session, 'auctions', total, latest)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Print summary
            db_stats = collector.get_auction_stats()
            logger.info("=" * 80)
            logger.info("UPDATE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Duration: {duration:.1f}s")
            logger.info("")
            logger.info("Database Statistics:")
            logger.info(f"  Total auctions: {db_stats['total_auctions']}")
            logger.info(f"  Date range: {db_stats['earliest_auction']} to {db_stats['latest_auction']}")
            logger.info(f"  Upcoming: {db_stats['upcoming_auctions']}")
            logger.info("  By term:")
            for term, count in sorted(db_stats['by_term'].items()):
                logger.info(f"    {term}: {count}")

        except Exception as e:
            logger.error(f"Update failed: {e}", exc_info=True)
            success = False

    if success:
        logger.info("=" * 80)
        logger.info("UPDATE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        return 0
    else:
        logger.warning("=" * 80)
        logger.warning("UPDATE COMPLETED WITH ERRORS")
        logger.warning("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
