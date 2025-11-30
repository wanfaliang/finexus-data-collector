"""
Update BEA Data - Incremental update script

Checks for new data releases and updates only what's needed.
Can be scheduled to run daily to keep data fresh.

Usage:
    python scripts/update_bea_data.py [options]

Options:
    --dataset NIPA|Regional|GDPbyIndustry|all  Dataset to update (default: all)
    --force                                     Force update even if data is recent
    --year YEAR_SPEC                            Year specification for update (default: LAST5)
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
import io

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings
from src.database.connection import get_session
from src.bea.bea_client import BEAClient
from src.bea.bea_collector import NIPACollector, RegionalCollector, GDPByIndustryCollector, BEACollector, CollectionProgress
from src.database.bea_tracking_models import BEADatasetFreshness

# Create logs directory
Path('logs').mkdir(exist_ok=True)

# Setup logging with UTF-8 encoding
utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bea_updates.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def check_needs_update(session, dataset_name: str, force: bool = False) -> bool:
    """Check if a dataset needs updating based on freshness tracking"""
    if force:
        return True

    freshness = session.query(BEADatasetFreshness).filter(
        BEADatasetFreshness.dataset_name == dataset_name
    ).first()

    if not freshness:
        logger.info(f"{dataset_name}: No freshness record, needs full backfill")
        return True

    if freshness.needs_update:
        logger.info(f"{dataset_name}: Marked as needing update")
        return True

    if freshness.update_in_progress:
        logger.warning(f"{dataset_name}: Update already in progress, skipping")
        return False

    # Check if last update was more than 24 hours ago
    if freshness.last_update_completed:
        hours_since_update = (datetime.utcnow() - freshness.last_update_completed).total_seconds() / 3600
        if hours_since_update < 24:
            logger.info(f"{dataset_name}: Updated {hours_since_update:.1f} hours ago, skipping")
            return False

    return True


def progress_callback(progress: CollectionProgress):
    """Print progress updates"""
    pct = (progress.tables_processed / progress.total_tables * 100) if progress.total_tables > 0 else 0
    logger.info(
        f"[{progress.dataset_name}] Progress: {progress.tables_processed}/{progress.total_tables} "
        f"({pct:.1f}%), {progress.data_points_inserted} data points"
    )


def update_nipa(session, client: BEAClient, year: str, force: bool) -> bool:
    """Update NIPA dataset"""
    if not check_needs_update(session, 'NIPA', force):
        return True

    logger.info("Updating NIPA data...")
    collector = NIPACollector(client, session)

    try:
        # Update with recent years only for incremental update
        progress = collector.backfill_all_tables(
            frequency='A',
            year=year,
            progress_callback=progress_callback
        )

        if progress.errors:
            logger.warning(f"NIPA update completed with {len(progress.errors)} errors")
            return False
        else:
            logger.info(f"NIPA update successful: {progress.data_points_inserted} data points")
            return True

    except Exception as e:
        logger.error(f"NIPA update failed: {e}")
        return False


def update_regional(session, client: BEAClient, year: str, force: bool) -> bool:
    """Update Regional dataset"""
    if not check_needs_update(session, 'Regional', force):
        return True

    logger.info("Updating Regional data...")
    collector = RegionalCollector(client, session)

    # Focus on most important tables for incremental updates
    priority_tables = [
        'SAGDP1',   # State GDP Summary
        'CAINC1',   # Personal Income Summary
        'SAINC1',   # State Personal Income Summary
    ]

    try:
        progress = collector.backfill_all_tables(
            geo_fips='STATE',
            year=year,
            tables=priority_tables,
            progress_callback=progress_callback
        )

        if progress.errors:
            logger.warning(f"Regional update completed with {len(progress.errors)} errors")
            return False
        else:
            logger.info(f"Regional update successful: {progress.data_points_inserted} data points")
            return True

    except Exception as e:
        logger.error(f"Regional update failed: {e}")
        return False


def update_gdpbyindustry(session, client: BEAClient, year: str, force: bool) -> bool:
    """Update GDP by Industry dataset"""
    if not check_needs_update(session, 'GDPbyIndustry', force):
        return True

    logger.info("Updating GDP by Industry data...")
    collector = GDPByIndustryCollector(client, session)

    # Focus on most important tables for incremental updates
    # Table 1: Value Added by Industry
    # Table 5: Contributions to Percent Change in Real GDP by Industry
    # Table 6: Real Value Added by Industry
    priority_tables = [1, 5, 6]

    try:
        progress = collector.backfill_all_tables(
            frequency='A',
            year=year,
            tables=priority_tables,
            progress_callback=progress_callback
        )

        if progress.errors:
            logger.warning(f"GDP by Industry update completed with {len(progress.errors)} errors")
            return False
        else:
            logger.info(f"GDP by Industry update successful: {progress.data_points_inserted} data points")
            return True

    except Exception as e:
        logger.error(f"GDP by Industry update failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Update BEA data')
    parser.add_argument('--dataset', type=str, default='all',
                        choices=['NIPA', 'Regional', 'GDPbyIndustry', 'all'],
                        help='Dataset to update')
    parser.add_argument('--force', action='store_true',
                        help='Force update even if data is recent')
    parser.add_argument('--year', type=str, default='LAST5',
                        help='Year specification for update')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info(f"BEA DATA UPDATE - {datetime.now()}")
    logger.info("=" * 80)
    logger.info(f"Dataset: {args.dataset}")
    logger.info(f"Year: {args.year}")
    logger.info(f"Force: {args.force}")

    # Initialize client
    api_key = settings.api.bea_api_key
    if not api_key or len(api_key) != 36:
        logger.error("Invalid or missing BEA_API_KEY in environment")
        return 1

    client = BEAClient(api_key=api_key)
    success = True

    with get_session() as session:
        # Sync dataset catalog first
        collector = BEACollector(client, session)
        collector.sync_dataset_catalog()

        # Update selected datasets
        if args.dataset in ('NIPA', 'all'):
            if not update_nipa(session, client, args.year, args.force):
                success = False

        if args.dataset in ('Regional', 'all'):
            if not update_regional(session, client, args.year, args.force):
                success = False

        if args.dataset in ('GDPbyIndustry', 'all'):
            if not update_gdpbyindustry(session, client, args.year, args.force):
                success = False

    logger.info("=" * 80)
    if success:
        logger.info("UPDATE COMPLETED SUCCESSFULLY")
    else:
        logger.warning("UPDATE COMPLETED WITH ERRORS")
    logger.info("=" * 80)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
