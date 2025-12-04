"""
Backfill Treasury Auction Data

Downloads and loads historical Treasury auction data from the Fiscal Data API.

Usage:
    python scripts/backfill_treasury.py [options]

Options:
    --years N           Number of years to backfill (default: 5)
    --term TERM         Specific security term (e.g., '10-Year', '2-Year')
    --include-upcoming  Also refresh the upcoming auctions calendar
    --dry-run           Preview what would be collected without actually collecting
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
from src.treasury import TreasuryClient, TreasuryCollector

# Create logs directory
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/treasury_backfill.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Backfill Treasury auction data')
    parser.add_argument('--years', type=int, default=5,
                        help='Number of years to backfill (default: 5)')
    parser.add_argument('--term', type=str, default=None,
                        help="Specific security term (e.g., '10-Year', '2-Year')")
    parser.add_argument('--include-upcoming', action='store_true',
                        help='Also refresh the upcoming auctions calendar')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without collecting')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"TREASURY AUCTION BACKFILL - {datetime.now()}")
    logger.info("=" * 80)
    logger.info(f"Years: {args.years}")
    logger.info(f"Term: {args.term or 'ALL'}")
    logger.info(f"Include Upcoming: {args.include_upcoming}")

    if args.dry_run:
        logger.info("DRY RUN - No data will be collected")
        logger.info("")
        logger.info("Would collect:")
        logger.info(f"  - Auction results for the last {args.years} years")
        if args.term:
            logger.info(f"  - Filtered to term: {args.term}")
        if args.include_upcoming:
            logger.info("  - Upcoming auctions calendar")
        logger.info("")
        logger.info("Target security terms: 2-Year, 5-Year, 7-Year, 10-Year, 20-Year, 30-Year")
        return 0

    # Initialize client
    client = TreasuryClient()

    with get_session() as session:
        collector = TreasuryCollector(db_session=session, client=client)

        try:
            start_time = datetime.now()

            # Collect upcoming auctions first if requested
            if args.include_upcoming:
                logger.info("-" * 40)
                logger.info("Refreshing upcoming auctions calendar...")
                upcoming_count = collector.collect_upcoming_auctions()
                logger.info(f"Upcoming auctions: {upcoming_count} new entries")

            # Backfill historical auctions
            logger.info("-" * 40)
            logger.info(f"Backfilling {args.years} years of auction history...")

            inserted, updated = collector.backfill_auctions(
                years=args.years,
                security_term=args.term,
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # Print summary
            stats = collector.stats
            logger.info("=" * 80)
            logger.info("BACKFILL COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Auctions fetched:  {stats['auctions_fetched']}")
            logger.info(f"Auctions inserted: {inserted}")
            logger.info(f"Auctions updated:  {updated}")
            logger.info(f"API requests:      {stats['api_requests']}")
            logger.info(f"Duration:          {duration:.1f}s")

            # Show database stats
            db_stats = collector.get_auction_stats()
            logger.info("")
            logger.info("Database Statistics:")
            logger.info(f"  Total auctions: {db_stats['total_auctions']}")
            logger.info(f"  Date range: {db_stats['earliest_auction']} to {db_stats['latest_auction']}")
            logger.info(f"  Upcoming: {db_stats['upcoming_auctions']}")
            logger.info("  By term:")
            for term, count in sorted(db_stats['by_term'].items()):
                logger.info(f"    {term}: {count}")

            return 0

        except Exception as e:
            logger.error(f"Backfill failed: {e}", exc_info=True)
            return 1


if __name__ == "__main__":
    sys.exit(main())
