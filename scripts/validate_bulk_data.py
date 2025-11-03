"""
Validate Bulk CSV Data
Checks for potential issues before loading into database
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from pathlib import Path
from src.utils.bulk_utils import get_bulk_data_path


def validate_data():
    """Check for potential data issues"""
    bulk_path = get_bulk_data_path() / 'profiles'
    files = sorted(bulk_path.glob('profile_bulk_*.csv'))

    if not files:
        print("No profile files found")
        return

    print("="*80)
    print("VALIDATING BULK CSV DATA")
    print("="*80)
    print()

    all_issues = []

    for file_path in files:
        print(f"Checking: {file_path.name}")
        print("-"*80)

        df = pd.read_csv(file_path)
        file_issues = []

        # Check string lengths
        string_checks = {
            'symbol': 20,
            'companyName': 200,
            'currency': 10,
            'cik': 20,
            'isin': 20,
            'cusip': 20,
            'exchange': 20,
            'exchangeFullName': 100,
            'industry': 100,
            'sector': 100,
            'country': 100,
            'website': 200,
            'ceo': 100,
            'phone': 50,
            'address': 200,
            'city': 100,
            'state': 50,
            'zip': 20,
            'image': 200,
            'fullTimeEmployees': 20
        }

        for col, max_len in string_checks.items():
            if col in df.columns:
                str_col = df[col].astype(str)
                too_long = str_col[str_col.str.len() > max_len]
                if len(too_long) > 0:
                    msg = f"  [WARN] {col}: {len(too_long)} values exceed {max_len} chars (max: {str_col.str.len().max()})"
                    print(msg)
                    file_issues.append(msg)

        # Check numeric ranges
        numeric_checks = {
            'beta': (10, 4, 999999),
            'lastDividend': (10, 4, 999999),
            'changePercentage': (10, 4, 999999),
            'price': (20, 4, 9999999999999999),
            'change': (20, 4, 9999999999999999),
        }

        for col, (precision, scale, max_val) in numeric_checks.items():
            if col in df.columns:
                num_col = pd.to_numeric(df[col], errors='coerce')
                overflow = num_col[num_col.abs() > max_val]
                if len(overflow) > 0:
                    msg = f"  [WARN] {col}: {len(overflow)} values exceed NUMERIC({precision},{scale}) max"
                    print(msg)
                    file_issues.append(msg)

        # Check for null required fields
        if 'symbol' in df.columns:
            null_symbols = df[df['symbol'].isna()]
            if len(null_symbols) > 0:
                msg = f"  [ERROR] NULL symbols: {len(null_symbols)} rows"
                print(msg)
                file_issues.append(msg)

        if 'companyName' in df.columns:
            null_names = df[df['companyName'].isna()]
            if len(null_names) > 0:
                msg = f"  [ERROR] NULL company names: {len(null_names)} rows"
                print(msg)
                file_issues.append(msg)

        if not file_issues:
            print("  [OK] No issues found")

        all_issues.extend(file_issues)
        print()

    print("="*80)
    print("SUMMARY")
    print("="*80)
    if all_issues:
        print(f"Total issues found: {len(all_issues)}")
        print("\nRecommendations:")
        print("1. Add string truncation for oversized fields")
        print("2. Add numeric overflow protection (set to NULL if exceeds)")
        print("3. Skip rows with NULL symbols or company names")
    else:
        print("No issues found!")
    print("="*80)


if __name__ == "__main__":
    validate_data()
