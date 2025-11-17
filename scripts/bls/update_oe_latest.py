#!/usr/bin/env python3
"""
Update BLS OE (Occupational Employment and Wage Statistics) data via API.

This script fetches the latest OEWS data from the BLS API v2 and updates
the database with new observations for existing series.

NOTE: OE is annual data, typically published in March/April for the prior year.
      Example: March 2025 publishes May 2024 data (period A01 of year 2024)

API Limits:
  - 500 requests per day
  - 50 series per request

Usage:
    # Update all active series from 2020 onwards
    python scripts/bls/update_oe_latest.py --start-year 2020

    # Update specific occupations (SOC codes)
    python scripts/bls/update_oe_latest.py --start-year 2023 \\
        --occupations 15-1252,29-1141,11-1021

    # Update specific industries
    python scripts/bls/update_oe_latest.py --start-year 2023 \\
        --industries 000000,622000

    # Update specific data types
    python scripts/bls/update_oe_latest.py --start-year 2023 \\
        --datatypes 01,03,04  # Employment, hourly mean, annual mean

    # Update specific areas
    python scripts/bls/update_oe_latest.py --start-year 2023 \\
        --areas 0000000,3562000  # National, New York metro

    # Dry run (preview without updating)
    python scripts/bls/update_oe_latest.py --start-year 2023 --dry-run
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import List, Dict

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, select, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from database.bls_models import OESeries, OEData
from bls.bls_client import BLSClient
from config import settings


def get_series_to_update(
    session,
    start_year: int,
    end_year: int,
    occupations: List[str] = None,
    industries: List[str] = None,
    datatypes: List[str] = None,
    areas: List[str] = None,
    areatypes: List[str] = None,
) -> List[str]:
    """Get list of OE series IDs to update based on filters"""

    filters = [OESeries.is_active == True]

    if occupations:
        filters.append(OESeries.occupation_code.in_(occupations))

    if industries:
        filters.append(OESeries.industry_code.in_(industries))

    if datatypes:
        filters.append(OESeries.datatype_code.in_(datatypes))

    if areas:
        filters.append(OESeries.area_code.in_(areas))

    if areatypes:
        filters.append(OESeries.areatype_code.in_(areatypes))

    stmt = select(OESeries.series_id).where(and_(*filters))
    result = session.execute(stmt)
    series_ids = [row[0] for row in result]

    return series_ids


def main():
    parser = argparse.ArgumentParser(
        description='Update OE (OEWS) data from BLS API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update all active series from 2020 onwards
  python scripts/bls/update_oe_latest.py --start-year 2020

  # Update specific occupations (Software Developers, Physicians, Top Executives)
  python scripts/bls/update_oe_latest.py --start-year 2023 \\
      --occupations 15-1252,29-1141,11-1021

  # Update employment and wage data for healthcare industry
  python scripts/bls/update_oe_latest.py --start-year 2023 \\
      --industries 622000 --datatypes 01,03,04

  # Update national-level data only
  python scripts/bls/update_oe_latest.py --start-year 2023 \\
      --areatypes N

Common Occupation Codes:
  - 00-0000 : All Occupations
  - 15-1252 : Software Developers
  - 29-1141 : Registered Nurses
  - 11-1021 : General and Operations Managers
  - 13-2011 : Accountants and Auditors

Common Industry Codes:
  - 000000 : Cross-industry (all industries)
  - 622000 : Hospitals
  - 541500 : Computer Systems Design
  - 236000 : Construction of Buildings

Common Data Type Codes:
  - 01 : Employment
  - 03 : Hourly mean wage
  - 04 : Annual mean wage
  - 08 : Hourly median wage
  - 13 : Annual median wage

Common Area Type Codes:
  - N : National
  - S : State
  - M : Metropolitan area
        """
    )

    parser.add_argument(
        '--start-year',
        type=int,
        required=True,
        help='Start year for data update (e.g., 2020)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        default=datetime.now(UTC).year,
        help='End year for data update (default: current year)'
    )

    parser.add_argument(
        '--occupations',
        help='Comma-separated occupation codes (e.g., 15-1252,29-1141)'
    )

    parser.add_argument(
        '--industries',
        help='Comma-separated industry codes (e.g., 000000,622000)'
    )

    parser.add_argument(
        '--datatypes',
        help='Comma-separated datatype codes (e.g., 01,03,04)'
    )

    parser.add_argument(
        '--areas',
        help='Comma-separated area codes (e.g., 0000000,3562000)'
    )

    parser.add_argument(
        '--areatypes',
        help='Comma-separated area type codes (N=National, S=State, M=Metro)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview series to update without making API calls'
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of series to update (for testing)'
    )

    args = parser.parse_args()

    print("="*80)
    print("UPDATING OE (OCCUPATIONAL EMPLOYMENT AND WAGE STATISTICS) DATA FROM BLS API")
    print("="*80)
    print()

    # Parse filter arguments
    occupation_list = args.occupations.split(',') if args.occupations else None
    industry_list = args.industries.split(',') if args.industries else None
    datatype_list = args.datatypes.split(',') if args.datatypes else None
    area_list = args.areas.split(',') if args.areas else None
    areatype_list = args.areatypes.split(',') if args.areatypes else None

    print(f"Year range: {args.start_year}-{args.end_year}")
    if occupation_list:
        print(f"Occupations: {', '.join(occupation_list)}")
    if industry_list:
        print(f"Industries: {', '.join(industry_list)}")
    if datatype_list:
        print(f"Data types: {', '.join(datatype_list)}")
    if area_list:
        print(f"Areas: {', '.join(area_list)}")
    if areatype_list:
        print(f"Area types: {', '.join(areatype_list)}")
    print()

    # Get series to update
    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        series_ids = get_series_to_update(
            session,
            start_year=args.start_year,
            end_year=args.end_year,
            occupations=occupation_list,
            industries=industry_list,
            datatypes=datatype_list,
            areas=area_list,
            areatypes=areatype_list,
        )

        print(f"Updating {len(series_ids)} active series from database")

        if not series_ids:
            print("No series to update!")
            print("Try adjusting your filters or loading data with:")
            print("  python scripts/bls/load_oe_flat_files.py")
            return

        if args.limit:
            series_ids = series_ids[:args.limit]
            print(f"Limited to first {args.limit} series")

        # Dry run - just preview
        if args.dry_run:
            print()
            print("DRY RUN - Preview of series to update:")
            for i, series_id in enumerate(series_ids[:20], 1):
                print(f"  {i}. {series_id}")
            if len(series_ids) > 20:
                print(f"  ... and {len(series_ids) - 20} more")
            print()
            print("Run without --dry-run to perform the update")
            return

        print()

        num_requests = (len(series_ids) + 49) // 50
        print(f"API requests needed: ~{num_requests} ({len(series_ids)} series ÷ 50 per request)")

        if num_requests > 500:
            print(f"\n⚠️  WARNING: {num_requests} requests exceeds daily limit of 500!")
            print("   Consider adding filters (--occupations, --industries, --datatypes, --areatypes)")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                print("Aborted.")
                return

        api_key = settings.api.bls_api_key
        client = BLSClient(api_key=api_key)

        print(f"\nFetching data from BLS API...")
        rows = client.get_many(
            series_ids,
            start_year=args.start_year,
            end_year=args.end_year,
            calculations=False,
            catalog=False,
            as_dataframe=False
        )

        print(f"Fetched {len(rows)} observations")

        data_to_upsert = []
        for row in rows:
            data_to_upsert.append({
                'series_id': row['series_id'],
                'year': row['year'],
                'period': row['period'],
                'value': row['value'],
                'footnote_codes': row.get('footnotes'),
            })

        print(f"\nUpserting {len(data_to_upsert)} observations to database...")

        stmt = insert(OEData).values(data_to_upsert)
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

        print("\n" + "="*80)
        print("SUCCESS! OE data updated")
        print(f"  Series updated: {len(series_ids)}")
        print(f"  Observations: {len(data_to_upsert)}")
        print(f"  API requests: ~{num_requests}")
        print("="*80)

    finally:
        session.close()


if __name__ == '__main__':
    main()
