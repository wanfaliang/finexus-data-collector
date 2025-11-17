#!/usr/bin/env python3
"""
Update LN (Labor Force Statistics from CPS) data with latest observations from BLS API

This script fetches the latest CPS labor force data via the BLS API.

Usage:
    # Update all active series (WARNING: 67K+ series = 1,345+ requests - exceeds 500/day limit!)
    python scripts/bls/update_ln_latest.py

    # Update specific labor force status
    python scripts/bls/update_ln_latest.py --lfst 20,30,40 --start-year 2024

    # Update by demographics (sex, age, race)
    python scripts/bls/update_ln_latest.py --sexs 1,2 --ages 16,20-24 --start-year 2024

    # Update seasonally adjusted series only
    python scripts/bls/update_ln_latest.py --seasonal S --start-year 2024

    # Test with limited series
    python scripts/bls/update_ln_latest.py --limit 10

Labor Force Status Codes (examples):
    00 = Civilian noninstitutional population
    10 = Civilian labor force
    20 = Employed
    30 = Unemployed
    40 = Unemployment rate
    50 = Not in labor force

Demographics:
    Sex: 0=Total, 1=Men, 2=Women
    Age: 00=Total, 16=16 years and older, 16-19, 20-24, 25-54, 55+, etc.
    Race: 00=Total, 01=White, 02=Black, 03=Asian, etc.
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
from database.bls_models import LNSeries, LNData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update LN data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--lfst', help='Comma-separated labor force status codes (10, 20, 30, 40, etc.)')
    parser.add_argument('--sexs', help='Comma-separated sex codes (0, 1, 2)')
    parser.add_argument('--ages', help='Comma-separated age codes (00, 16, 20-24, etc.)')
    parser.add_argument('--race', help='Comma-separated race codes (00, 01, 02, 03, etc.)')
    parser.add_argument('--education', help='Comma-separated education codes')
    parser.add_argument('--occupation', help='Comma-separated occupation codes')
    parser.add_argument('--indy', help='Comma-separated industry codes')
    parser.add_argument('--seasonal', help='Seasonal adjustment (S or U)')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING LN (LABOR FORCE STATISTICS FROM CPS) DATA FROM BLS API")
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
            query = session.query(LNSeries.series_id).filter(LNSeries.is_active == True)
            filters_applied = []

            if args.lfst:
                lfst_list = [s.strip() for s in args.lfst.split(',')]
                query = query.filter(LNSeries.lfst_code.in_(lfst_list))
                filters_applied.append(f"lfst={','.join(lfst_list)}")

            if args.sexs:
                sexs_list = [s.strip() for s in args.sexs.split(',')]
                query = query.filter(LNSeries.sexs_code.in_(sexs_list))
                filters_applied.append(f"sexs={','.join(sexs_list)}")

            if args.ages:
                ages_list = [a.strip() for a in args.ages.split(',')]
                query = query.filter(LNSeries.ages_code.in_(ages_list))
                filters_applied.append(f"ages={','.join(ages_list)}")

            if args.race:
                race_list = [r.strip() for r in args.race.split(',')]
                query = query.filter(LNSeries.race_code.in_(race_list))
                filters_applied.append(f"race={','.join(race_list)}")

            if args.education:
                education_list = [e.strip() for e in args.education.split(',')]
                query = query.filter(LNSeries.education_code.in_(education_list))
                filters_applied.append(f"education={','.join(education_list)}")

            if args.occupation:
                occupation_list = [o.strip() for o in args.occupation.split(',')]
                query = query.filter(LNSeries.occupation_code.in_(occupation_list))
                filters_applied.append(f"occupation={','.join(occupation_list)}")

            if args.indy:
                indy_list = [i.strip() for i in args.indy.split(',')]
                query = query.filter(LNSeries.indy_code.in_(indy_list))
                filters_applied.append(f"industry={','.join(indy_list)}")

            if args.seasonal:
                query = query.filter(LNSeries.seasonal == args.seasonal.upper())
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
            print(f"\nWARNING: {num_requests} requests exceeds daily limit of 500!")
            print("   Consider adding filters:")
            print("   --lfst 20,30,40 (employed, unemployed, unemployment rate)")
            print("   --sexs 1,2 (men, women)")
            print("   --ages 16,20-24,25-54 (age groups)")
            print("   --seasonal S (seasonally adjusted only)")
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
        stmt = insert(LNData).values(data_to_upsert)
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
        print("SUCCESS! LN data updated")
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
