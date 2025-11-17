#!/usr/bin/env python3
"""
Update EC (Employment Cost Index) data with latest observations from BLS API

This script fetches the latest employment cost data via the BLS API.

Usage:
    # Update all active series (~600 series = ~12 requests)
    python scripts/bls/update_ec_latest.py

    # Update specific compensation types
    python scripts/bls/update_ec_latest.py --comp 1,2 --start-year 2024

    # Update specific groups
    python scripts/bls/update_ec_latest.py --groups 000,101 --start-year 2024

    # Update private industry only
    python scripts/bls/update_ec_latest.py --ownership 2 --start-year 2024

    # Update index series only (not percent changes)
    python scripts/bls/update_ec_latest.py --periodicity I --start-year 2024

    # Test with limited series
    python scripts/bls/update_ec_latest.py --limit 10

Compensation Codes:
    1 = Total compensation
    2 = Wages and salaries
    3 = Benefits

Ownership Codes:
    1 = Civilian
    2 = Private industry
    3 = State and local government

Periodicity Codes:
    I = Index number
    Q = 3 month percent change
    A = 12 month percent change
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Dict, List, cast

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.bls_client import BLSClient
from database.bls_models import ECSeries, ECData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update EC data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--comp', help='Comma-separated compensation codes (1, 2, 3)')
    parser.add_argument('--groups', help='Comma-separated group codes')
    parser.add_argument('--ownership', help='Comma-separated ownership codes (1, 2, 3)')
    parser.add_argument('--periodicity', choices=['I', 'Q', 'A'], help='Filter by periodicity')
    parser.add_argument('--seasonal', choices=['S', 'U'], help='Filter by seasonal adjustment')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING EC (EMPLOYMENT COST INDEX) DATA FROM BLS API")
    print("=" * 80)
    print(f"\nYear range: {args.start_year}-{args.end_year}")

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        if args.series_ids:
            series_ids = [s.strip() for s in args.series_ids.split(',')]
            print(f"Updating {len(series_ids)} specified series")
        else:
            query = session.query(ECSeries.series_id).filter(ECSeries.is_active == True)
            filters_applied = []

            if args.comp:
                comp_list = [c.strip() for c in args.comp.split(',')]
                query = query.filter(ECSeries.comp_code.in_(comp_list))
                filters_applied.append(f"compensation={','.join(comp_list)}")

            if args.groups:
                group_list = [g.strip() for g in args.groups.split(',')]
                query = query.filter(ECSeries.group_code.in_(group_list))
                filters_applied.append(f"groups={','.join(group_list)}")

            if args.ownership:
                ownership_list = [o.strip() for o in args.ownership.split(',')]
                query = query.filter(ECSeries.ownership_code.in_(ownership_list))
                filters_applied.append(f"ownership={','.join(ownership_list)}")

            if args.periodicity:
                query = query.filter(ECSeries.periodicity_code == args.periodicity)
                filters_applied.append(f"periodicity={args.periodicity}")

            if args.seasonal:
                query = query.filter(ECSeries.seasonal == args.seasonal)
                filters_applied.append(f"seasonal={args.seasonal}")

            if args.limit:
                query = query.limit(args.limit)
                filters_applied.append(f"limit={args.limit}")

            series_ids = [row[0] for row in query.all()]

            if filters_applied:
                print(f"Filters: {', '.join(filters_applied)}")
            print(f"Updating {len(series_ids)} active series from database")

        if not series_ids:
            print("No series to update!")
            return

        num_requests = (len(series_ids) + 49) // 50
        print(f"API requests needed: ~{num_requests} ({len(series_ids)} series ÷ 50 per request)")

        if num_requests > 500:
            print(f"\n⚠️  WARNING: {num_requests} requests exceeds daily limit of 500!")
            print("   Consider adding filters (--comp, --ownership, --periodicity)")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                print("Aborted.")
                return

        api_key = settings.api.bls_api_key
        client = BLSClient(api_key=api_key)

        print(f"\nFetching data from BLS API...")
        rows = cast(List[Dict[str, Any]], client.get_many(
            series_ids,
            start_year=args.start_year,
            end_year=args.end_year,
            calculations=False,
            catalog=False,
            as_dataframe=False
        ))

        print(f"Fetched {len(rows)} observations")

        data_to_upsert: List[Dict[str, Any]] = []
        for row in rows:
            data_to_upsert.append({
                'series_id': row['series_id'],
                'year': row['year'],
                'period': row['period'],
                'value': row['value'],
                'footnote_codes': row.get('footnotes'),
            })

        print(f"\nUpserting {len(data_to_upsert)} observations to database...")

        from sqlalchemy.dialects.postgresql import insert
        stmt = insert(ECData).values(data_to_upsert)
        stmt = stmt.on_conflict_do_update(
            index_elements=['series_id', 'year', 'period'],
            set_={
                'value': stmt.excluded.value,
                'footnote_codes': stmt.excluded.footnote_codes,
                'updated_at': datetime.now(UTC),
            }
        )
        session.execute(stmt)
        session.commit()

        print("\n" + "=" * 80)
        print("SUCCESS! EC data updated")
        print(f"  Series updated: {len(series_ids)}")
        print(f"  Observations: {len(data_to_upsert)}")
        print(f"  API requests: ~{num_requests}")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
