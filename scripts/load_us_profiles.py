"""
Load US Company Profiles Only
Loads company profiles from bulk CSV files, filtering for US exchanges only
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert

from src.database.connection import get_session
from src.database.models import Company
from src.utils.data_transform import transform_batch, transform_keys
from src.utils.bulk_utils import get_bulk_data_path


# Define US exchanges
US_EXCHANGES = [
    'NYSE',      # New York Stock Exchange
    'NASDAQ',    # NASDAQ
    'AMEX',      # American Stock Exchange
    'OTC',       # Over-the-Counter
    'CBOE',      # Chicago Board Options Exchange
    'PNK',       # Pink Sheets
    'BATS',      # BATS Exchange
    'NYSEARCA',  # NYSE Arca
    'INDEX',     # Market Indices
]


def process_file(session, file_path: Path) -> tuple[int, int]:
    """Process a single profile CSV file, filtering for US companies only"""
    print(f"\nProcessing: {file_path.name}")
    print("-" * 80)

    # Read CSV
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df):,} total records")

    # Filter for US exchanges only
    df = df[df['exchange'].isin(US_EXCHANGES)]
    print(f"Filtered to {len(df):,} US companies")

    # Skip null symbols
    initial_count = len(df)
    df = df[df['symbol'].notna()]
    if len(df) < initial_count:
        print(f"Skipped {initial_count - len(df)} rows with null symbols")

    # Remove duplicates
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['symbol'], keep='first')
    if len(df) < before_dedup:
        print(f"Removed {before_dedup - len(df)} duplicate symbols")

    if df.empty:
        print("No valid US records to process")
        return 0, 0

    # Transform column names
    records = df.to_dict('records')
    transformed_records = transform_batch(records, transform_keys)
    df = pd.DataFrame(transformed_records)

    # Data type conversions
    if 'ipo_date' in df.columns:
        df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
        df['ipo_date'] = df['ipo_date'].apply(lambda x: x.date() if pd.notna(x) else None)

    # Cap numeric values to avoid overflow (NUMERIC(10, 4) max is 999,999.9999)
    numeric_10_4_cols = ['beta', 'last_dividend', 'change_percentage']
    for col in numeric_10_4_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or abs(x) > 999999 else x)

    if 'full_time_employees' in df.columns:
        df['full_time_employees'] = df['full_time_employees'].fillna('').astype(str)
        df['full_time_employees'] = df['full_time_employees'].replace('nan', '')

    if 'cik' in df.columns:
        df['cik'] = df['cik'].fillna('').astype(str)
        df['cik'] = df['cik'].replace('nan', '')
        df['cik'] = df['cik'].str.replace('.0', '', regex=False)

    boolean_cols = ['default_image', 'is_etf', 'is_actively_trading', 'is_adr', 'is_fund']
    for col in boolean_cols:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)

    string_cols = ['cusip', 'isin', 'website', 'description', 'ceo', 'phone',
                  'address', 'city', 'state', 'zip', 'image', 'industry',
                  'sector', 'country', 'exchange_full_name']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].where(pd.notna(df[col]), None)

    # Prepare records
    records = df.to_dict('records')

    # Insert in batches
    batch_size = 1000
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]

        try:
            stmt = insert(Company).values(batch)
            update_dict = {
                col.name: col
                for col in stmt.excluded
                if col.name not in ['company_id', 'symbol', 'created_at']
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol'],
                set_=update_dict
            )
            session.execute(stmt)
            session.commit()
            total_inserted += len(batch)
        except Exception as e:
            print(f"Error in batch: {e}")
            session.rollback()
            # Try individual inserts
            for record in batch:
                try:
                    stmt = insert(Company).values([record])
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['symbol'],
                        set_={col.name: col for col in stmt.excluded
                              if col.name not in ['company_id', 'symbol', 'created_at']}
                    )
                    session.execute(stmt)
                    session.commit()
                    total_inserted += 1
                except Exception as e2:
                    session.rollback()
                    print(f"Failed: {record.get('symbol', 'UNKNOWN')}")

    print(f"[OK] Inserted {total_inserted:,} US companies")
    return total_inserted, 0


def main():
    print("="*80)
    print("Loading US Company Profiles Only")
    print("="*80)
    print()

    bulk_path = get_bulk_data_path() / 'profiles'
    files = sorted(bulk_path.glob('profile_bulk_*.csv'))

    if not files:
        print(f"No profile files found in {bulk_path}")
        return 1

    print(f"Found {len(files)} profile files")

    with get_session() as session:
        total_inserted = 0

        for file_path in files:
            inserted, _ = process_file(session, file_path)
            total_inserted += inserted

        print()
        print("="*80)
        print("RESULTS")
        print("="*80)
        print(f"Total US companies inserted: {total_inserted:,}")
        print("="*80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
