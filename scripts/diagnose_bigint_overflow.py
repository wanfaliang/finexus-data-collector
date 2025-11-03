"""
Diagnostic Script: Find BigInteger Overflow Issues
Analyzes CSV data to identify which values are causing the overflow
"""
import pandas as pd
from pathlib import Path
import numpy as np

# PostgreSQL BigInteger limits
BIGINT_MAX = 9223372036854775807
BIGINT_MIN = -9223372036854775808

# US exchanges filter
US_EXCHANGES = ['NYSE', 'NASDAQ', 'AMEX', 'OTC', 'CBOE', 'PNK', 'BATS', 'NYSEARCA', 'INDEX']


def diagnose_file(file_path):
    """Diagnose a single CSV file for BigInteger overflow issues"""
    print(f"\n{'='*80}")
    print(f"Diagnosing: {file_path.name}")
    print(f"{'='*80}")

    # Read CSV
    df = pd.read_csv(file_path)
    print(f"Total records: {len(df):,}")

    # Filter for US exchanges
    df = df[df['exchange'].isin(US_EXCHANGES)]
    print(f"US records: {len(df):,}")

    # Check BigInteger columns
    bigint_cols = ['marketCap', 'volume', 'averageVolume']

    issues_found = []

    for col in bigint_cols:
        if col not in df.columns:
            continue

        print(f"\n--- Checking {col} ---")

        # Convert to numeric
        original_values = pd.to_numeric(df[col], errors='coerce')

        # Find overflow values BEFORE transformation
        overflow_mask = original_values.abs() > BIGINT_MAX
        overflow_count = overflow_mask.sum()

        if overflow_count > 0:
            print(f"  [WARN] Found {overflow_count} values exceeding BigInt max")

            # Get the problematic records
            overflow_records = df[overflow_mask][['symbol', 'exchange', col]].copy()
            overflow_records[f'{col}_numeric'] = original_values[overflow_mask]

            print(f"  Max value: {original_values.max():.2e}")
            print(f"\n  Problematic records:")
            for idx, row in overflow_records.head(10).iterrows():
                symbol = row['symbol']
                exchange = row['exchange']
                value = row[f'{col}_numeric']
                print(f"    {symbol:15} {exchange:10} {value:.2e}")

            issues_found.append({
                'file': file_path.name,
                'column': col,
                'count': overflow_count,
                'max_value': original_values.max(),
                'records': overflow_records
            })
        else:
            print(f"  [OK] No overflow (max: {original_values.max():.2e})")

        # Now apply the fix (like in bulk_profile_collector.py)
        print(f"\n  Applying fix...")
        fixed_values = original_values.apply(
            lambda x: None if pd.isna(x) or abs(x) > BIGINT_MAX else int(x) if pd.notna(x) else None
        )

        # Check if fix worked
        fixed_overflow = 0
        for val in fixed_values:
            if val is not None and abs(val) > BIGINT_MAX:
                fixed_overflow += 1

        if fixed_overflow > 0:
            print(f"  [ERROR] Fix FAILED: {fixed_overflow} values still overflow after fix!")
            print(f"  This is the BUG we need to fix!")
        else:
            print(f"  [OK] Fix worked: All overflow values set to None")

    return issues_found


def main():
    print("="*80)
    print("BIGINT OVERFLOW DIAGNOSTIC")
    print("="*80)

    # Find CSV files
    csv_dir = Path('data/bulk_csv/profiles')
    csv_files = sorted(csv_dir.glob('profile_bulk_*.csv'))

    if not csv_files:
        print(f"ERROR: No CSV files found in {csv_dir}")
        return 1

    print(f"\nFound {len(csv_files)} CSV files")

    all_issues = []

    for file_path in csv_files:
        issues = diagnose_file(file_path)
        all_issues.extend(issues)

    # Summary
    print(f"\n\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")

    if all_issues:
        print(f"\nTotal issues found: {len(all_issues)}")
        print("\nBreakdown by column:")

        for col in ['marketCap', 'volume', 'averageVolume']:
            col_issues = [i for i in all_issues if i['column'] == col]
            if col_issues:
                total_count = sum(i['count'] for i in col_issues)
                print(f"  {col:20} {total_count:>5} overflow values")
    else:
        print("\n[OK] No BigInteger overflow issues found!")

    print(f"\n{'='*80}")

    return 0


if __name__ == "__main__":
    exit(main())
