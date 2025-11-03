"""Add Companies to Database"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.connection import get_session
from src.database.models import Company

# Define your companies here
COMPANIES = [
    {'symbol': 'AAPL', 'company_name': 'Apple Inc.'},
    {'symbol': 'MSFT', 'company_name': 'Microsoft Corporation'},

]

def main():
    # Check for duplicates in COMPANIES list
    symbols = [comp['symbol'] for comp in COMPANIES]
    duplicates = [sym for sym in set(symbols) if symbols.count(sym) > 1]

    if duplicates:
        print(f"Warning: Found duplicate symbols in COMPANIES list: {', '.join(duplicates)}")
        print("Removing duplicates...")
        # Deduplicate - keep first occurrence
        seen = set()
        unique_companies = []
        for comp in COMPANIES:
            if comp['symbol'] not in seen:
                seen.add(comp['symbol'])
                unique_companies.append(comp)
        companies_to_process = unique_companies
    else:
        companies_to_process = COMPANIES

    print(f"Adding {len(companies_to_process)} companies to database...")

    with get_session() as session:
        added = 0
        skipped = 0

        for comp in companies_to_process:
            existing = session.query(Company)\
                .filter(Company.symbol == comp['symbol'])\
                .first()

            if existing:
                print(f"  - {comp['symbol']}: Already exists (skipped)")
                skipped += 1
            else:
                company = Company(**comp)
                session.add(company)
                print(f"  + {comp['symbol']}: Added")
                added += 1

        session.commit()

    print(f"\n✓ Complete: {added} added, {skipped} skipped")

if __name__ == "__main__":
    main()
