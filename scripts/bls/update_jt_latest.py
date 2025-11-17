#!/usr/bin/env python3
"""
Update JT (JOLTS) data with latest observations from BLS API

This script fetches the latest job openings and labor turnover data via the BLS API.

Usage:
    # Update all active series (NOT recommended - ~6K series = ~120 requests)
    python scripts/bls/update_jt_latest.py

    # Update specific data elements (RECOMMENDED)
    python scripts/bls/update_jt_latest.py --elements JO,HI --start-year 2024

    # Update specific industries
    python scripts/bls/update_jt_latest.py --industries 000000,100000 --start-year 2024

    # Update level series only (in thousands, not rates)
    python scripts/bls/update_jt_latest.py --ratelevel L --start-year 2024

    # Test with limited series
    python scripts/bls/update_jt_latest.py --limit 10

Data Element Codes:
    JO = Job openings
    HI = Hires
    TS = Total separations
    QU = Quits
    LD = Layoffs and discharges
    OS = Other separations
    UO = Unemployed per job opening ratio

Industry Codes:
    000000 = Total nonfarm
    100000 = Total private
    ... (see jt.industry file for all codes)

Rate/Level:
    R = Rate (per 100 employees)
    L = Level (in thousands)
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
from database.bls_models import JTSeries, JTData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update JT data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--elements', help='Comma-separated data element codes (JO, HI, TS, QU, LD, OS)')
    parser.add_argument('--industries', help='Comma-separated industry codes (000000, 100000, etc.)')
    parser.add_argument('--states', help='Comma-separated state codes')
    parser.add_argument('--ratelevel', choices=['R', 'L'], help='Filter by rate (R) or level (L)')
    parser.add_argument('--seasonal', choices=['S', 'U'], help='Filter by seasonal adjustment')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING JT (JOLTS - JOB OPENINGS AND LABOR TURNOVER) DATA FROM BLS API")
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
            query = session.query(JTSeries.series_id).filter(JTSeries.is_active == True)
            filters_applied = []

            if args.elements:
                element_list = [e.strip() for e in args.elements.split(',')]
                query = query.filter(JTSeries.dataelement_code.in_(element_list))
                filters_applied.append(f"elements={','.join(element_list)}")

            if args.industries:
                industry_list = [i.strip() for i in args.industries.split(',')]
                query = query.filter(JTSeries.industry_code.in_(industry_list))
                filters_applied.append(f"industries={','.join(industry_list)}")

            if args.states:
                state_list = [s.strip() for s in args.states.split(',')]
                query = query.filter(JTSeries.state_code.in_(state_list))
                filters_applied.append(f"states={','.join(state_list)}")

            if args.ratelevel:
                query = query.filter(JTSeries.ratelevel_code == args.ratelevel)
                filters_applied.append(f"ratelevel={args.ratelevel}")

            if args.seasonal:
                query = query.filter(JTSeries.seasonal == args.seasonal)
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
            print("   Consider adding filters (--elements, --industries, --ratelevel)")
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
        stmt = insert(JTData).values(data_to_upsert)
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
        print("SUCCESS! JT data updated")
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
