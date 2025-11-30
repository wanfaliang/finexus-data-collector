"""
Test BEA API Connection

Quick test script to verify BEA API key and basic functionality.

Usage:
    python scripts/test_bea_api.py
"""
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("BEA API Connection Test")
    logger.info("=" * 60)

    # Check API key
    api_key = settings.api.bea_api_key
    if not api_key:
        logger.error("ERROR: BEA_API_KEY not found in environment")
        logger.info("\nTo set the API key, add to your .env file:")
        logger.info("  BEA_API_KEY=your-36-character-api-key")
        return 1

    if len(api_key) != 36:
        logger.error(f"ERROR: BEA_API_KEY should be 36 characters, got {len(api_key)}")
        return 1

    logger.info(f"API Key: {api_key[:8]}...{api_key[-4:]} ({len(api_key)} chars)")

    # Test API connection
    from src.bea.bea_client import BEAClient, BEAAPIError

    try:
        client = BEAClient(api_key=api_key)

        # Test 1: Get dataset list
        logger.info("\n1. Testing GetDataSetList...")
        datasets = client.get_dataset_list()
        ds_list = datasets.get('BEAAPI', {}).get('Results', {}).get('Dataset', [])
        logger.info(f"   Found {len(ds_list)} datasets:")
        for ds in ds_list[:5]:
            logger.info(f"   - {ds.get('DatasetName')}: {ds.get('DatasetDescription', '')[:50]}...")
        if len(ds_list) > 5:
            logger.info(f"   ... and {len(ds_list) - 5} more")

        # Test 2: Get NIPA tables
        logger.info("\n2. Testing NIPA table list...")
        tables = client.get_nipa_tables()
        logger.info(f"   Found {len(tables)} NIPA tables:")
        for t in tables[:3]:
            logger.info(f"   - {t.get('TableName')}: {t.get('Description', '')[:50]}...")
        if len(tables) > 3:
            logger.info(f"   ... and {len(tables) - 3} more")

        # Test 3: Get GDP data
        logger.info("\n3. Testing NIPA data fetch (GDP - T10101, 2023)...")
        gdp = client.get_nipa_data("T10101", frequency="A", year="2023")
        data = client._extract_data(gdp)
        logger.info(f"   Received {len(data)} data points")
        if data:
            row = data[0]
            logger.info(f"   Sample: {row.get('LineDescription', '')[:40]}: {row.get('DataValue')}")

        # Test 4: Get Regional tables
        logger.info("\n4. Testing Regional table list...")
        tables = client.get_regional_tables()
        logger.info(f"   Found {len(tables)} Regional tables:")
        for t in tables[:3]:
            logger.info(f"   - {t.get('TableName')}: {t.get('Description', '')[:50]}...")

        # Test 5: Get Regional data
        logger.info("\n5. Testing Regional data fetch (SAGDP1, STATE, 2023)...")
        regional = client.get_regional_data("SAGDP1", line_code=1, geo_fips="STATE", year="2023")
        data = client._extract_data(regional)
        logger.info(f"   Received {len(data)} data points")
        if data:
            row = data[0]
            logger.info(f"   Sample: {row.get('GeoName')}: {row.get('DataValue')}")

        # Show rate limit stats
        stats = client.get_request_stats()
        logger.info(f"\nRate limit stats:")
        logger.info(f"   Requests this minute: {stats['requests_last_minute']}/100")
        logger.info(f"   Data this minute: {stats['data_mb_last_minute']:.2f}/100 MB")

        logger.info("\n" + "=" * 60)
        logger.info("ALL TESTS PASSED - BEA API connection working!")
        logger.info("=" * 60)
        return 0

    except BEAAPIError as e:
        logger.error(f"\nBEA API Error: {e}")
        return 1
    except Exception as e:
        logger.error(f"\nUnexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
