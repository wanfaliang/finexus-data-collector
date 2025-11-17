#!/usr/bin/env python3
"""
Update IP (Industry Productivity) data with latest observations from BLS API

This script fetches the latest industry productivity measures via the BLS API.

Usage:
    # Update all active series (WARNING: 21K+ series = 424+ requests)
    python scripts/bls/update_ip_latest.py

    # Update specific sectors
    python scripts/bls/update_ip_latest.py --sectors 31-33 --start-year 2024

    # Update specific industries (NAICS)
    python scripts/bls/update_ip_latest.py --industries 311,312 --start-year 2024

    # Update specific measures
    python scripts/bls/update_ip_latest.py --measures 18,19 --start-year 2024

    # Update only U.S. total (no state-level)
    python scripts/bls/update_ip_latest.py --areas 00000 --start-year 2024

    # Test with limited series
    python scripts/bls/update_ip_latest.py --limit 10

Sector Codes (NAICS):
  - A = Agriculture, Forestry, Fishing (336 series)
  - B = Mining, Quarrying, Oil & Gas (364 series)
  - C = Utilities (182 series)
  - D = Construction (160 series)
  - E = Manufacturing ⭐ (4,302 series) - This is what you wanted!
  - G = Wholesale Trade (650 series)
  - H = Retail Trade (2,808 series)
  - I = Transportation & Warehousing (1,034 series)
  - J = Information (644 series)
  - K = Finance & Insurance (212 series)
  - L = Real Estate (216 series)
  - M = Professional Services (332 series)
  - P = Educational Services (240 series)
  - Q = Health Care (144 series)
  - R = Arts & Entertainment (408 series)
  - S = Accommodation & Food (284 series)
  - T = Other Services (320 series)

Measure Codes:
  - L00 = Labor productivity (Index, 2017=100)
  - U10 = Unit labor costs (Index, 2017=100)
  - U12 = Hourly compensation (Index, 2017=100)
  - L01 = Hours worked (Index, 2017=100)
  - T01 = Real sectoral output (Index, 2017=100)
  - W00 for Output per worker
  - M00 for Total factor productivity
  - C00 for Capital productivity
  - etc.

Area Codes:
    00000 = U.S. Total
    01000 = Alabama
    ... (see bls_ip_areas table for full list)
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
from database.bls_models import IPSeries, IPData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update IP data with latest from BLS API")
    parser.add_argument('--start-year', type=int, default=datetime.now().year, help='Start year for update')
    parser.add_argument('--end-year', type=int, default=datetime.now().year, help='End year for update')
    parser.add_argument('--sectors', help='Comma-separated sector codes (31-33, 42, etc.)')
    parser.add_argument('--industries', help='Comma-separated industry codes (311, 312, etc.)')
    parser.add_argument('--measures', help='Comma-separated measure codes (18, 19, etc.)')
    parser.add_argument('--durations', help='Comma-separated duration codes (1, 2)')
    parser.add_argument('--types', help='Comma-separated type codes (I, P, H, etc.)')
    parser.add_argument('--areas', help='Comma-separated area codes (00000 for U.S. total)')
    parser.add_argument('--series-ids', help='Comma-separated list of series IDs to update')
    parser.add_argument('--limit', type=int, help='Limit number of series to update (for testing)')

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING IP (INDUSTRY PRODUCTIVITY) DATA FROM BLS API")
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
            query = session.query(IPSeries.series_id).filter(IPSeries.is_active == True)
            filters_applied = []

            if args.sectors:
                sector_list = [s.strip() for s in args.sectors.split(',')]
                query = query.filter(IPSeries.sector_code.in_(sector_list))
                filters_applied.append(f"sectors={','.join(sector_list)}")

            if args.industries:
                industry_list = [i.strip() for i in args.industries.split(',')]
                query = query.filter(IPSeries.industry_code.in_(industry_list))
                filters_applied.append(f"industries={','.join(industry_list)}")

            if args.measures:
                measure_list = [m.strip() for m in args.measures.split(',')]
                query = query.filter(IPSeries.measure_code.in_(measure_list))
                filters_applied.append(f"measures={','.join(measure_list)}")

            if args.durations:
                duration_list = [d.strip() for d in args.durations.split(',')]
                query = query.filter(IPSeries.duration_code.in_(duration_list))
                filters_applied.append(f"durations={','.join(duration_list)}")

            if args.types:
                type_list = [t.strip() for t in args.types.split(',')]
                query = query.filter(IPSeries.type_code.in_(type_list))
                filters_applied.append(f"types={','.join(type_list)}")

            if args.areas:
                area_list = [a.strip() for a in args.areas.split(',')]
                query = query.filter(IPSeries.area_code.in_(area_list))
                filters_applied.append(f"areas={','.join(area_list)}")

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
            print("   --areas 00000 (U.S. total only)")
            print("   --sectors 31-33 (manufacturing)")
            print("   --measures 18,19 (productivity, unit labor costs)")
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
        stmt = insert(IPData).values(data_to_upsert)
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
        print("SUCCESS! IP data updated")
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
