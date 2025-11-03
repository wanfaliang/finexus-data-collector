"""
Inspect Bulk CSV File
Shows structure and sample data from bulk CSV files
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from pathlib import Path
from src.utils.bulk_utils import get_bulk_data_path
from src.utils.data_transform import transform_keys


def inspect_csv(file_path: Path):
    """Inspect a CSV file and show its structure"""
    print(f"\n{'='*80}")
    print(f"Inspecting: {file_path.name}")
    print(f"{'='*80}\n")

    # Read CSV
    df = pd.read_csv(file_path)

    print(f"Total Rows: {len(df):,}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"\nOriginal Column Names (camelCase from API):")
    print("-" * 80)
    for i, col in enumerate(df.columns, 1):
        print(f"{i:2}. {col}")

    # Transform column names
    if len(df) > 0:
        sample_row = df.iloc[0].to_dict()
        transformed_row = transform_keys(sample_row)

        print(f"\n\nTransformed Column Names (snake_case for database):")
        print("-" * 80)
        for i, col in enumerate(transformed_row.keys(), 1):
            print(f"{i:2}. {col}")

    # Show sample data
    print(f"\n\nSample Data (first 3 rows, first 10 columns):")
    print("-" * 80)
    print(df.iloc[:3, :10].to_string())

    # Show data types
    print(f"\n\nData Types:")
    print("-" * 80)
    print(df.dtypes)

    # Show null counts
    print(f"\n\nNull Values:")
    print("-" * 80)
    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df) * 100).round(2)
    null_summary = pd.DataFrame({
        'Column': null_counts.index,
        'Null Count': null_counts.values,
        'Null %': null_pct.values
    })
    print(null_summary[null_summary['Null Count'] > 0].to_string(index=False))

    if null_summary['Null Count'].sum() == 0:
        print("No null values found!")


def main():
    bulk_path = get_bulk_data_path()

    # Look for profile_bulk files
    profile_files = list(bulk_path.glob("**/profile_bulk*.csv"))

    if not profile_files:
        print(f"No profile_bulk CSV files found in {bulk_path}")
        print("Please ensure the file is in one of these locations:")
        print(f"  - {bulk_path / 'profiles'}")
        print(f"  - {bulk_path}")
        return

    # Inspect each found file
    for file_path in profile_files:
        inspect_csv(file_path)


if __name__ == "__main__":
    main()
