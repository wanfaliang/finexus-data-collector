"""
Standalone Script: Export Actively Trading Companies to Excel
Fetches from FMP API and marks US vs Non-US companies
"""
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os


def main():
    print("="*80)
    print("EXPORT ACTIVELY TRADING COMPANIES")
    print("="*80)
    print()

    # Load API key from .env
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
    api_key = os.getenv('FMP_API_KEY')

    if not api_key:
        print("ERROR: FMP_API_KEY not found in .env file")
        return 1

    # Fetch data from API
    print("Fetching actively trading companies from FMP...")
    url = "https://financialmodelingprep.com/stable/actively-trading-list"
    params = {"apikey": api_key}

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✓ Fetched {len(data):,} actively trading companies")
    except Exception as e:
        print(f"ERROR: Failed to fetch data: {e}")
        return 1

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Add is_us_company column
    # Logic: Symbols WITHOUT .XXX suffix are US companies
    df['is_us_company'] = ~df['symbol'].str.contains(r'\.[A-Z]+$', regex=True, na=False)

    # Count breakdown
    us_count = df['is_us_company'].sum()
    non_us_count = len(df) - us_count

    print()
    print("Breakdown:")
    print(f"  US Companies:     {us_count:>10,}")
    print(f"  Non-US Companies: {non_us_count:>10,}")
    print(f"  Total:            {len(df):>10,}")

    # Show samples
    print()
    print("Sample US Companies (first 5):")
    print("-"*80)
    us_sample = df[df['is_us_company'] == True].head(5)
    for _, row in us_sample.iterrows():
        print(f"  {row['symbol']:10} {row['name']}")

    print()
    print("Sample Non-US Companies (first 5):")
    print("-"*80)
    non_us_sample = df[df['is_us_company'] == False].head(5)
    for _, row in non_us_sample.iterrows():
        print(f"  {row['symbol']:10} {row['name']}")

    # Sort: US companies first, then by symbol
    df = df.sort_values(['is_us_company', 'symbol'], ascending=[False, True])

    # Export to Excel
    output_dir = Path(__file__).parent.parent / 'data' / 'exports'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'actively_trading_companies.xlsx'

    print()
    print("Exporting to Excel...")
    df.to_excel(output_path, sheet_name='Active Companies', index=False)

    print(f"✓ Exported to: {output_path}")
    print()
    print("="*80)
    print("DONE! Please check the Excel file.")
    print("="*80)

    return 0


if __name__ == "__main__":
    exit(main())
