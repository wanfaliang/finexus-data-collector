"""
Test: Simulate exact insert process to find BigInt overflow
"""
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.data_transform import transform_batch, transform_keys

# US exchanges
US_EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX', 'OTC', 'CBOE', 'PNK', 'BATS', 'NYSEARCA', 'INDEX']
BIGINT_MAX = 9223372036854775807


def test_conversion():
    print("="*80)
    print("SIMULATING EXACT BULK_PROFILE_COLLECTOR PROCESS")
    print("="*80)

    # Read first CSV
    csv_path = Path('data/bulk_csv/profiles/profile_bulk_0.csv')
    df = pd.read_csv(csv_path)
    print(f"\n1. Loaded CSV: {len(df):,} records")

    # Filter for US
    df = df[df['exchange'].isin(US_EXCHANGES)]
    print(f"2. Filtered to US: {len(df):,} records")

    # Remove nulls
    df = df[df['symbol'].notna()]
    print(f"3. Removed null symbols: {len(df):,} records")

    # Transform column names
    records = df.to_dict('records')
    transformed_records = transform_batch(records, transform_keys)
    df = pd.DataFrame(transformed_records)
    print(f"4. Transformed column names: {len(df):,} records")

    # Apply BigInteger capping (like in bulk_profile_collector.py)
    print(f"\n5. Applying BigInteger protection...")
    bigint_cols = ['market_cap', 'volume', 'average_volume']
    for col in bigint_cols:
        if col in df.columns:
            print(f"   Processing {col}...")
            before_max = df[col].max()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or abs(x) > BIGINT_MAX else int(x) if pd.notna(x) else None)
            after_max = df[col].max() if df[col].notna().any() else None
            print(f"     Before: max = {before_max}")
            print(f"     After:  max = {after_max}")

    # Convert to dict (what gets sent to SQLAlchemy)
    print(f"\n6. Converting to dict for insert...")
    final_records = df.to_dict('records')

    # Check the actual values in the dict
    print(f"\n7. Checking final dict values...")
    for col in bigint_cols:
        values = [r.get(col) for r in final_records if r.get(col) is not None]
        if values:
            max_val = max(values)
            print(f"   {col}: max = {max_val:,}")
            if abs(max_val) > BIGINT_MAX:
                print(f"      [ERROR] Value exceeds BigInt max!")
            else:
                print(f"      [OK]")

            # Check data types
            sample = values[0]
            print(f"      Type: {type(sample)}")

    # Test a small batch
    print(f"\n8. Testing first 10 records...")
    test_batch = final_records[:10]

    for idx, record in enumerate(test_batch):
        for col in bigint_cols:
            val = record.get(col)
            if val is not None and abs(val) > BIGINT_MAX:
                print(f"   [ERROR] Record {idx} ({record['symbol']}): {col} = {val} exceeds limit!")

    print(f"\n{'='*80}")
    print("DONE")
    print(f"{'='*80}")


if __name__ == "__main__":
    test_conversion()
