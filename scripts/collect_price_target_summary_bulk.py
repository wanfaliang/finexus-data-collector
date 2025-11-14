"""
Collect Price Target Summary Bulk Data
Fetches price target summary for all companies from FMP bulk CSV API
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.price_target_summary_bulk_collector import PriceTargetSummaryBulkCollector

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
        logging.FileHandler('logs/price_target_summary_bulk.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Collect Price Target Summary bulk data from FMP API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script downloads current price target summary for all companies as CSV
and updates the price_target_summary_bulk table.

Examples:
  # Collect latest price target summary for all companies
  python scripts/collect_price_target_summary_bulk.py
        """
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("PRICE TARGET SUMMARY BULK COLLECTION")
    logger.info("="*80)
    logger.info(f"Started at: {datetime.now()}")
    logger.info("")

    try:
        with get_session() as session:
            collector = PriceTargetSummaryBulkCollector(session)

            # Collect bulk data
            result = collector.collect_bulk_price_target_summary()

            if result['success']:
                logger.info("\n" + "="*80)
                logger.info("✓ COLLECTION SUCCESSFUL")
                logger.info("="*80)
                logger.info(f"Symbols received: {result['symbols_received']:,}")
                logger.info(f"Symbols inserted/updated: {result['symbols_inserted']:,}")
                logger.info("="*80)
                return 0
            else:
                logger.error("\n" + "="*80)
                logger.error("✗ COLLECTION FAILED")
                logger.error("="*80)
                if 'error' in result:
                    logger.error(f"Error: {result['error']}")
                logger.error("="*80)
                return 1

    except Exception as e:
        logger.error("\n" + "="*80)
        logger.error("✗ COLLECTION FAILED")
        logger.error("="*80)
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("="*80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
