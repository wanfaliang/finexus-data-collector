#!/usr/bin/env python3
"""
Update PR (Major Sector Productivity and Costs) data with latest observations from BLS API

This script fetches the latest productivity and cost measures via the BLS API.

Usage:
    # Update all active series (recommended)
    python scripts/bls/update_pr_latest.py

    # Update specific sectors
    python scripts/bls/update_pr_latest.py --sectors 8400,8500 --start-year 2024

    # Update specific measures
    python scripts/bls/update_pr_latest.py --measures 09,11 --start-year 2024

    # Test with limited series
    python scripts/bls/update_pr_latest.py --limit 10

Sector Codes:
    8400 = Business
    8500 = Nonfarm Business
    8800 = Nonfinancial Corporations
    3000 = Manufacturing
    3100 = Manufacturing, Durable Goods
    3200 = Manufacturing, Nondurable Goods

Measure Codes:
    09 = Labor productivity (output per hour)
    10 = Hourly compensation
    11 = Unit labor costs
    01 = Employment
    03 = Hours worked
    04 = Real value-added output
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
from database.bls_models import PRSeries, PRData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update PR data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--sectors', help='Comma-separated sector codes (8400, 8500, etc.)')
    parser.add_argument('--measures', help='Comma-separated measure codes (09, 11, etc.)')
    parser.add_argument('--classes', help='Comma-separated class codes (3, 6)')
    parser.add_argument('--durations', help='Comma-separated duration codes (1, 2, 3)')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING PR (MAJOR SECTOR PRODUCTIVITY AND COSTS) DATA FROM BLS API")
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
            query = session.query(PRSeries.series_id).filter(PRSeries.is_active == True)
            filters_applied = []

            if args.sectors:
                sector_list = [s.strip() for s in args.sectors.split(',')]
                query = query.filter(PRSeries.sector_code.in_(sector_list))
                filters_applied.append(f"sectors={','.join(sector_list)}")

            if args.measures:
                measure_list = [m.strip() for m in args.measures.split(',')]
                query = query.filter(PRSeries.measure_code.in_(measure_list))
                filters_applied.append(f"measures={','.join(measure_list)}")

            if args.classes:
                class_list = [c.strip() for c in args.classes.split(',')]
                query = query.filter(PRSeries.class_code.in_(class_list))
                filters_applied.append(f"classes={','.join(class_list)}")

            if args.durations:
                duration_list = [d.strip() for d in args.durations.split(',')]
                query = query.filter(PRSeries.duration_code.in_(duration_list))
                filters_applied.append(f"durations={','.join(duration_list)}")

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
            print("   Consider adding filters (--sectors, --measures)")
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
        stmt = insert(PRData).values(data_to_upsert)
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
        print("SUCCESS! PR data updated")
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
