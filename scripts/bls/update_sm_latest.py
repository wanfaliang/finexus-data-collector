#!/usr/bin/env python3
"""
Update SM (State and Metro Area Employment) data with latest observations from BLS API

This script fetches the latest employment data for states and metro areas via the BLS API.

Usage:
    # Update all active series (NOT recommended - ~24K series = ~480 requests!)
    python scripts/bls/update_sm_latest.py

    # Update specific states only (RECOMMENDED)
    python scripts/bls/update_sm_latest.py --states 36,06,48 --start-year 2024

    # Update specific metro areas
    python scripts/bls/update_sm_latest.py --areas 35620,31080 --start-year 2024

    # Update seasonally adjusted series only
    python scripts/bls/update_sm_latest.py --seasonal S --start-year 2024

    # Test with limited series
    python scripts/bls/update_sm_latest.py --limit 10

State Codes:
    06 = California
    36 = New York
    48 = Texas
    12 = Florida
    17 = Illinois
    ... (see sm.state file for all codes)

Key Metro Area Codes:
    35620 = New York-Newark-Jersey City, NY-NJ-PA
    31080 = Los Angeles-Long Beach-Anaheim, CA
    16980 = Chicago-Naperville-Elgin, IL-IN-WI
    19100 = Dallas-Fort Worth-Arlington, TX
    ... (see sm.area file for all codes)
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Dict, List, cast

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker
from bls.bls_client import BLSClient
from database.bls_models import SMSeries, SMData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update SM data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--states', help='Comma-separated state codes (06=CA, 36=NY, 48=TX, etc.)')
    parser.add_argument('--areas', help='Comma-separated area codes (35620=NYC, 31080=LA, etc.)')
    parser.add_argument('--seasonal', choices=['S', 'U'], help='Filter by seasonal adjustment')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING SM (STATE AND METRO AREA EMPLOYMENT) DATA FROM BLS API")
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
            query = session.query(SMSeries.series_id).filter(SMSeries.is_active == True)
            filters_applied = []

            if args.states:
                state_list = [s.strip() for s in args.states.split(',')]
                query = query.filter(SMSeries.state_code.in_(state_list))
                filters_applied.append(f"states={','.join(state_list)}")

            if args.areas:
                area_list = [a.strip() for a in args.areas.split(',')]
                query = query.filter(SMSeries.area_code.in_(area_list))
                filters_applied.append(f"areas={','.join(area_list)}")

            if args.seasonal:
                query = query.filter(SMSeries.seasonal_code == args.seasonal)
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
            print("   Consider adding filters (--states, --areas, --seasonal)")
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
        stmt = insert(SMData).values(data_to_upsert)
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
        print("SUCCESS! SM data updated")
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
