#!/usr/bin/env python3
"""
Update TU (American Time Use Survey) data with latest observations from BLS API

This script fetches the latest time use data via the BLS API.

Usage:
    # Update all active series (WARNING: 87K+ series = 1,748+ requests - exceeds 500/day limit!)
    python scripts/bls/update_tu_latest.py

    # Update specific activities
    python scripts/bls/update_tu_latest.py --activities 010100,020100 --start-year 2024

    # Update specific statistic type
    python scripts/bls/update_tu_latest.py --stattypes 10100 --start-year 2024

    # Update by demographics (sex, age)
    python scripts/bls/update_tu_latest.py --sex 1,2 --start-year 2024

    # Test with limited series
    python scripts/bls/update_tu_latest.py --limit 10

Activity Codes (examples):
    000000 = Total, all activities
    010100 = Sleeping
    020100 = Housework
    030100 = Caring for household children
    040100 = Caring for household adults
    050100 = Work and work-related activities
    060100 = Education
    110100 = Eating and drinking
    120100 = Socializing and communicating
    130100 = Sports, exercise, and recreation
    140100 = Religious and spiritual activities
    150100 = Volunteer activities
    160100 = Telephone calls
    170100 = Traveling

Statistic Types:
    10100 = Number of persons (in thousands)
    10101 = Average hours per day
    20100 = Number of participants (in thousands)
    20101 = Average hours per day for participants
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
from database.bls_models import TUSeries, TUData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update TU data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--stattypes', help='Comma-separated statistic type codes (10100, 10101, etc.)')
    parser.add_argument('--activities', help='Comma-separated activity codes (010100, 020100, etc.)')
    parser.add_argument('--sex', help='Comma-separated sex codes (0, 1, 2)')
    parser.add_argument('--ages', help='Comma-separated age codes')
    parser.add_argument('--races', help='Comma-separated race codes')
    parser.add_argument('--education', help='Comma-separated education codes')
    parser.add_argument('--lfstat', help='Comma-separated labor force status codes')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING TU (AMERICAN TIME USE SURVEY) DATA FROM BLS API")
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
            query = session.query(TUSeries.series_id).filter(TUSeries.is_active == True)
            filters_applied = []

            if args.stattypes:
                stattype_list = [s.strip() for s in args.stattypes.split(',')]
                query = query.filter(TUSeries.stattype_code.in_(stattype_list))
                filters_applied.append(f"stattypes={','.join(stattype_list)}")

            if args.activities:
                activity_list = [a.strip() for a in args.activities.split(',')]
                query = query.filter(TUSeries.actcode_code.in_(activity_list))
                filters_applied.append(f"activities={','.join(activity_list)}")

            if args.sex:
                sex_list = [s.strip() for s in args.sex.split(',')]
                query = query.filter(TUSeries.sex_code.in_(sex_list))
                filters_applied.append(f"sex={','.join(sex_list)}")

            if args.ages:
                age_list = [a.strip() for a in args.ages.split(',')]
                query = query.filter(TUSeries.age_code.in_(age_list))
                filters_applied.append(f"ages={','.join(age_list)}")

            if args.races:
                race_list = [r.strip() for r in args.races.split(',')]
                query = query.filter(TUSeries.race_code.in_(race_list))
                filters_applied.append(f"races={','.join(race_list)}")

            if args.education:
                educ_list = [e.strip() for e in args.education.split(',')]
                query = query.filter(TUSeries.educ_code.in_(educ_list))
                filters_applied.append(f"education={','.join(educ_list)}")

            if args.lfstat:
                lfstat_list = [l.strip() for l in args.lfstat.split(',')]
                query = query.filter(TUSeries.lfstat_code.in_(lfstat_list))
                filters_applied.append(f"lfstat={','.join(lfstat_list)}")

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
            print("   Consider adding filters:")
            print("   --activities 010100,050100 (sleeping, work)")
            print("   --stattypes 10101 (avg hours per day)")
            print("   --sex 1,2 (men, women)")
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
        stmt = insert(TUData).values(data_to_upsert)
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
        print("SUCCESS! TU data updated")
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
