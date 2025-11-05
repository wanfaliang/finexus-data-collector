"""
Scheduled Task Runner
Runs economic data updates on a schedule
Requires: pip install schedule
"""
import logging
import schedule
import time
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.economic_collector import EconomicCollector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def update_economic_data():
    """Job: Update economic indicators"""
    logger.info("Running scheduled economic data update...")

    try:
        with get_session() as session:
            collector = EconomicCollector(session)
            success = collector.collect_all()

            if success:
                logger.info(f"✓ Economic update complete: {collector.records_inserted} records")
            else:
                logger.error("✗ Economic update failed")
    except Exception as e:
        logger.error(f"Error in economic update: {e}", exc_info=True)


def main():
    """Run scheduler"""
    logger.info("Starting FinExus scheduler...")

    # Schedule economic data update daily at 8:00 AM
    schedule.every().day.at("08:00").do(update_economic_data)

    logger.info("Scheduled jobs:")
    logger.info("  - Economic data update: Daily at 8:00 AM")
    logger.info("Scheduler running. Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    main()
