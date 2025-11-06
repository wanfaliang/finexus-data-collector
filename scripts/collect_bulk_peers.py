"""
Collect Bulk Stock Peers
Fetches peer relationships for all global symbols from FMP bulk API
Stores in peers_bulk table as unvalidated data lake
Override approach: replaces old peer data with latest
"""
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.bulk_peers_collector import BulkPeersCollector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bulk_peers_collection.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    logger.info("="*80)
    logger.info("BULK STOCK PEERS COLLECTION")
    logger.info("="*80)
    logger.info("Fetching peer relationships for all global symbols...")
    logger.info("")

    # Create logs directory if needed
    Path('logs').mkdir(exist_ok=True)

    # Run collection
    with get_session() as session:
        collector = BulkPeersCollector(session)

        result = collector.collect_bulk_peers()

        if result['success']:
            logger.info("\n" + "="*80)
            logger.info("[SUCCESS] COLLECTION SUCCESSFUL")
            logger.info("="*80)
            logger.info(f"Symbols received: {result['symbols_received']:,}")
            logger.info(f"Symbols upserted: {result['symbols_inserted']:,}")
            logger.info("="*80)
            return 0
        else:
            logger.error("\n" + "="*80)
            logger.error("[FAILED] COLLECTION FAILED")
            logger.error("="*80)
            if 'error' in result:
                logger.error(f"Error: {result['error']}")
            return 1


if __name__ == "__main__":
    sys.exit(main())
