"""
Update Economic Data - Daily scheduled job
Checks tracking table and updates only if needed
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.economic_collector import EconomicCollector

# Create logs directory if needed (BEFORE logging setup)
Path('logs').mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/economic_updates.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Run economic data update"""
    logger.info("=" * 80)
    logger.info(f"ECONOMIC DATA UPDATE - {datetime.now()}")
    logger.info("=" * 80)

    with get_session() as session:
        collector = EconomicCollector(session)

        # Check if update is needed
        from src.database.models import TableUpdateTracking
        tracking = session.query(TableUpdateTracking)\
            .filter(TableUpdateTracking.table_name == 'economic_data')\
            .first()

        if tracking:
            hours_since_update = (datetime.now() - tracking.last_update_timestamp).total_seconds() / 3600
            logger.info(f"Last update: {tracking.last_update_timestamp} ({hours_since_update:.1f} hours ago)")

            if hours_since_update < 23:  # Don't update more than once per day
                logger.info("Data is recent, skipping update")
                return 0

        logger.info("Starting economic data collection...")
        success = collector.collect_all()

        if success:
            logger.info("=" * 80)
            logger.info("✓ UPDATE SUCCESSFUL")
            logger.info("=" * 80)
            logger.info(f"Records inserted/updated: {collector.records_inserted}")
            return 0
        else:
            logger.error("=" * 80)
            logger.error("✗ UPDATE FAILED")
            logger.error("=" * 80)
            return 1


if __name__ == "__main__":
    sys.exit(main())
