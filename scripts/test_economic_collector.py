"""
Test script for Economic Data Collector
Fetches FRED and FMP economic indicators and saves to database
"""
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.economic_collector import EconomicCollector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def main():
    """Test economic data collection"""
    logger.info("=" * 80)
    logger.info("ECONOMIC DATA COLLECTION TEST")
    logger.info("=" * 80)

    with get_session() as session:
        collector = EconomicCollector(session)

        logger.info("\nStarting collection...")
        success = collector.collect_all()

        if success:
            logger.info("\n" + "=" * 80)
            logger.info("✓ COLLECTION SUCCESSFUL")
            logger.info("=" * 80)
            logger.info(f"Records inserted: {collector.records_inserted}")
            logger.info(f"Records updated: {collector.records_updated}")

            # Query results
            from src.database.models import (
                EconomicIndicator, EconomicDataRaw,
                EconomicDataMonthly, EconomicDataQuarterly
            )

            indicator_count = session.query(EconomicIndicator).count()
            raw_count = session.query(EconomicDataRaw).count()
            monthly_count = session.query(EconomicDataMonthly).count()
            quarterly_count = session.query(EconomicDataQuarterly).count()

            logger.info(f"\nDatabase Statistics:")
            logger.info(f"  Indicators: {indicator_count}")
            logger.info(f"  Raw data points: {raw_count}")
            logger.info(f"  Monthly aggregations: {monthly_count}")
            logger.info(f"  Quarterly aggregations: {quarterly_count}")

            # Show some sample indicators
            logger.info(f"\nSample Indicators:")
            indicators = session.query(EconomicIndicator).limit(10).all()
            for ind in indicators:
                logger.info(f"  {ind.indicator_code}: {ind.indicator_name} ({ind.source}, {ind.native_frequency})")

        else:
            logger.error("\n" + "=" * 80)
            logger.error("✗ COLLECTION FAILED")
            logger.error("=" * 80)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
