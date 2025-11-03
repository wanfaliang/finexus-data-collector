"""
Prioritize Companies for Data Collection
Matches actively trading list with database companies and marks priorities
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import requests
from sqlalchemy import func
from src.database.connection import get_session
from src.database.models import Company
from src.config import settings


def fetch_actively_trading_list():
    """Fetch actively trading companies from FMP API"""
    url = "https://financialmodelingprep.com/stable/actively-trading-list"
    api_key = settings.api.fmp_api_key
    params = {"apikey": api_key}

    print("Fetching actively trading companies from FMP...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Filter for US companies only (no .XXX suffix - same logic as check_active_companies.py)
    import re
    us_companies = [
        item for item in data
        if not re.search(r'\.[A-Z]+$', item['symbol'])
    ]

    print(f"Fetched {len(us_companies):,} actively trading US companies")
    return us_companies


def match_with_database(actively_trading_symbols):
    """Match actively trading list with database companies"""
    print("\nMatching with database companies...")

    with get_session() as session:
        # Get all companies from database
        db_companies = session.query(Company.symbol).all()
        db_symbols = set([c.symbol for c in db_companies])

        print(f"Database has {len(db_symbols):,} companies")

        # Find matches
        active_symbols_set = set(actively_trading_symbols)
        matched = db_symbols.intersection(active_symbols_set)
        in_db_not_active = db_symbols - active_symbols_set
        active_not_in_db = active_symbols_set - db_symbols

        print(f"\nResults:")
        print(f"  ✓ Matched (in both):           {len(matched):>10,}")
        print(f"  - In DB but not active:        {len(in_db_not_active):>10,}")
        print(f"  - Active but not in DB:        {len(active_not_in_db):>10,}")

        return {
            'matched': sorted(list(matched)),
            'in_db_not_active': sorted(list(in_db_not_active)),
            'active_not_in_db': sorted(list(active_not_in_db))
        }


def save_priority_lists(results, output_dir='data/priority_lists'):
    """Save prioritized company lists to files"""
    import os
    from pathlib import Path

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Priority 1: Companies in database AND actively trading
    priority1_file = output_path / 'priority1_active_in_db.txt'
    with open(priority1_file, 'w') as f:
        f.write('\n'.join(results['matched']))
    print(f"\n✓ Priority 1 (Active + In DB): {len(results['matched']):,} companies")
    print(f"  Saved to: {priority1_file}")

    # Priority 2: In database but not in active list (may be inactive)
    priority2_file = output_path / 'priority2_in_db_maybe_inactive.txt'
    with open(priority2_file, 'w') as f:
        f.write('\n'.join(results['in_db_not_active']))
    print(f"\n✓ Priority 2 (In DB, maybe inactive): {len(results['in_db_not_active']):,} companies")
    print(f"  Saved to: {priority2_file}")

    # Reference: Active but not in database (for future consideration)
    reference_file = output_path / 'reference_active_not_in_db.txt'
    with open(reference_file, 'w') as f:
        f.write('\n'.join(results['active_not_in_db']))
    print(f"\n✓ Reference (Active but not in DB): {len(results['active_not_in_db']):,} companies")
    print(f"  Saved to: {reference_file}")


def main():
    print("="*80)
    print("PRIORITIZE COMPANIES FOR DATA COLLECTION")
    print("="*80)
    print()

    # Step 1: Fetch actively trading list
    active_companies = fetch_actively_trading_list()
    active_symbols = [c['symbol'] for c in active_companies]

    # Step 2: Match with database
    results = match_with_database(active_symbols)

    # Step 3: Save priority lists
    save_priority_lists(results)

    print()
    print("="*80)
    print("NEXT STEPS:")
    print("="*80)
    print()
    print("1. Start with Priority 1 (matched companies)")
    print("   - These are actively trading AND in your database")
    print("   - Use Phase 1 collectors to populate their financial data")
    print()
    print("2. Then process Priority 2 if needed")
    print("   - These are in database but may be inactive")
    print()
    print("3. Consider adding 'Reference' companies later")
    print("   - These are actively trading but not in your database yet")
    print()
    print("="*80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
