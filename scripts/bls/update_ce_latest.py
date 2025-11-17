#!/usr/bin/env python3
"""
Update CE (Current Employment Statistics) data with latest observations from BLS API

This script fetches the latest data points for CE series via the BLS API
and updates the database. Use this for regular monthly updates after initial load.

The CE survey includes the famous monthly jobs report (Total Nonfarm Employment - CES0000000001)
that significantly moves financial markets.

Usage:
    # Update all active series (not recommended - ~22K series = 440 requests!)
    python scripts/bls/update_ce_latest.py

    # Update key employment series only (recommended)
    python scripts/bls/update_ce_latest.py --data-types 01 --start-year 2024

    # Update specific industries
    python scripts/bls/update_ce_latest.py --industries 00000000,30000000 --start-year 2024

    # Update specific series (e.g., Total Nonfarm Employment)
    python scripts/bls/update_ce_latest.py --series-ids CES0000000001

    # Test with limited series
    python scripts/bls/update_ce_latest.py --limit 10

Data Type Codes:
    01 = All employees (thousands) - THE JOBS REPORT
    02 = Average weekly hours
    03 = Average hourly earnings
    06 = Production employees
    07 = Production hours
    08 = Production earnings
    ... (41 total data types)

Key Series:
    CES0000000001 = Total Nonfarm Employment (THE jobs report number!)
    CES0500000001 = Total Private Employment
    CES0600000001 = Goods-Producing
    CES0800000001 = Private Service-Providing
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Dict, List, cast

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.bls_client import BLSClient
from database.bls_models import CESeries, CEData
from config import settings

def main():
    parser = argparse.ArgumentParser(
        description="Update CE data with latest from BLS API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update employment data only (data type 01)
  python scripts/bls/update_ce_latest.py --data-types 01 --start-year 2024

  # Update all data for manufacturing
  python scripts/bls/update_ce_latest.py --industries 30000000 --start-year 2024

  # Update seasonally adjusted series only
  python scripts/bls/update_ce_latest.py --seasonal S --data-types 01 --limit 100

  # Update the jobs report number
  python scripts/bls/update_ce_latest.py --series-ids CES0000000001
        """
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=datetime.now().year,
        help='Start year for update (default: current year)'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=datetime.now().year,
        help='End year for update (default: current year)'
    )
    parser.add_argument(
        '--industries',
        help='Comma-separated industry codes to filter (00000000=total nonfarm, 30000000=manufacturing, etc.)'
    )
    parser.add_argument(
        '--data-types',
        help='Comma-separated data type codes to filter (01=employment, 02=hours, 03=earnings, etc.)'
    )
    parser.add_argument(
        '--seasonal',
        choices=['S', 'U'],
        help='Filter by seasonal adjustment: S=seasonally adjusted, U=not adjusted'
    )
    parser.add_argument(
        '--series-ids',
        help='Comma-separated list of series IDs to update (overrides filters)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of series to update (for testing)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING CE (CURRENT EMPLOYMENT STATISTICS) DATA FROM BLS API")
    print("=" * 80)
    print(f"\nYear range: {args.start_year}-{args.end_year}")

    # Get database session
    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get series IDs to update
        if args.series_ids:
            series_ids = [s.strip() for s in args.series_ids.split(',')]
            print(f"Updating {len(series_ids)} specified series")
        else:
            # Build query with filters
            query = session.query(CESeries.series_id).filter(CESeries.is_active == True)

            filters_applied = []

            # Filter by industries
            if args.industries:
                industry_list = [i.strip() for i in args.industries.split(',')]
                query = query.filter(CESeries.industry_code.in_(industry_list))
                filters_applied.append(f"industries={','.join(industry_list)}")

            # Filter by data types
            if args.data_types:
                dtype_list = [dt.strip() for dt in args.data_types.split(',')]
                query = query.filter(CESeries.data_type_code.in_(dtype_list))
                filters_applied.append(f"data_types={','.join(dtype_list)}")

            # Filter by seasonal adjustment
            if args.seasonal:
                query = query.filter(CESeries.seasonal_code == args.seasonal)
                filters_applied.append(f"seasonal={args.seasonal}")

            # Apply limit if specified
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

        # Calculate number of API requests needed
        num_requests = (len(series_ids) + 49) // 50  # Ceiling division
        print(f"API requests needed: ~{num_requests} ({len(series_ids)} series ÷ 50 per request)")

        # Check if within daily limit
        if num_requests > 500:
            print(f"\n⚠️  WARNING: {num_requests} requests exceeds daily limit of 500!")
            print("   Consider adding filters (--industries, --data-types, --seasonal)")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                print("Aborted.")
                return

        # Get API key from config
        api_key = settings.api.bls_api_key

        # Create BLS client
        client = BLSClient(api_key=api_key)

        # Fetch data from API
        print(f"\nFetching data from BLS API...")
        rows = cast(
            List[Dict[str, Any]],
            client.get_many(
                series_ids,
                start_year=args.start_year,
                end_year=args.end_year,
                calculations=False,
                catalog=False,
                as_dataframe=False
            )
        )

        print(f"Fetched {len(rows)} observations")

        # Convert to database format
        data_to_upsert: List[Dict[str, Any]] = []
        for row in rows:
            data_to_upsert.append({
                'series_id': row['series_id'],
                'year': row['year'],
                'period': row['period'],
                'value': row['value'],
                'footnote_codes': row.get('footnotes'),
            })

        # Upsert to database
        print(f"\nUpserting {len(data_to_upsert)} observations to database...")

        from sqlalchemy.dialects.postgresql import insert
        stmt = insert(CEData).values(data_to_upsert)
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
        print("SUCCESS! CE data updated")
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
