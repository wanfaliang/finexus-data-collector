# test_api_fetch.py
"""Quick test to see what BLS API returns for actual time series data"""
import os
import json
from bls_client import BLSClient

def test_single_series():
    """Test fetching a single series to understand data structure"""
    api_key = os.getenv('BLS_API_KEY')
    client = BLSClient(api_key=api_key)

    # Test with a simple AP series: Ground chuck prices
    series_id = "APU0000703111"
    print(f"Fetching recent data for {series_id} (Ground chuck prices)...")

    # Get last 3 years
    rows = client.get_one(
        series_id,
        start_year=2022,
        end_year=2025,
        catalog=True,
        calculations=False,
        as_dataframe=False
    )

    print(f"\nTotal rows returned: {len(rows)}")
    print("\nFirst 5 data points:")
    for row in rows[:5]:
        print(json.dumps(row, indent=2, default=str))

    print("\nLast 3 data points:")
    for row in rows[-3:]:
        print(json.dumps(row, indent=2, default=str))

    # Check if catalog metadata is included
    if rows and 'catalog' in rows[0]:
        print("\nCatalog metadata:")
        print(json.dumps(rows[0]['catalog'], indent=2))

def test_multiple_series():
    """Test fetching multiple series at once"""
    api_key = os.getenv('BLS_API_KEY')
    client = BLSClient(api_key=api_key)

    # Test with 3 different food items
    series_ids = [
        "APU0000703111",  # Ground chuck
        "APU0000701111",  # Flour
        "APU0000702111",  # White bread
    ]

    print(f"\nFetching multiple series: {series_ids}")
    rows = client.get_many(
        series_ids,
        start_year=2024,
        end_year=2025,
        as_dataframe=False
    )

    print(f"\nTotal rows: {len(rows)}")

    # Group by series
    by_series = {}
    for row in rows:
        sid = row['series_id']
        by_series.setdefault(sid, []).append(row)

    for sid, data in by_series.items():
        print(f"\n{sid}: {len(data)} data points")
        print(f"  Latest: {data[0]['year']}-{data[0]['period']} = ${data[0]['value']}")

if __name__ == "__main__":
    print("=" * 60)
    print("TEST 1: Single Series with Catalog")
    print("=" * 60)
    test_single_series()

    print("\n" + "=" * 60)
    print("TEST 2: Multiple Series")
    print("=" * 60)
    test_multiple_series()
