"""
Collect Company Profile Bulk Data
Fetches company profiles for all companies from FMP bulk CSV API
Downloads data in 4 parts (part=0,1,2,3)
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.collectors.company_profile_bulk_collector import CompanyProfileBulkCollector

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
        logging.FileHandler('logs/company_profile_bulk.log', encoding='utf-8'),
        logging.StreamHandler(utf8_stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Collect Company Profile bulk data from FMP API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script downloads current company profiles for all companies as CSV
from 4 parts (part=0,1,2,3) and updates the company_profile_bulk table.

Examples:
  # Collect latest company profiles for all companies
  python scripts/collect_company_profile_bulk.py
        """
    )

    args = parser.parse_args()

    logger.info("="*80)
    logger.info("COMPANY PROFILE BULK COLLECTION")
    logger.info("="*80)
    logger.info(f"Started at: {datetime.now()}")
    logger.info("")

    try:
        with get_session() as session:
            collector = CompanyProfileBulkCollector(session)

            # Collect bulk data
            result = collector.collect_bulk_company_profiles()

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
