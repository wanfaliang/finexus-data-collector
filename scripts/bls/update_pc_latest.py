#!/usr/bin/env python3
"""
Update PC (Producer Price Index - Industry) data with latest observations from BLS API

This script fetches the latest data points for PC series via the BLS API
and updates the database. Use this for regular monthly updates after initial load.

The PC survey tracks producer prices by industry (NAICS-based). BLS updates
PC data around the 15th of each month for the previous month.

Usage:
    # Update all active series (NOT recommended - ~4,700 series = ~95 requests!)
    python scripts/bls/update_pc_latest.py

    # Update specific industries only (RECOMMENDED)
    python scripts/bls/update_pc_latest.py --industries 113310,311 --start-year 2024

    # Update specific series
    python scripts/bls/update_pc_latest.py --series-ids PCU113310113310,PCU311311

    # Update seasonally adjusted series only
    python scripts/bls/update_pc_latest.py --seasonal S --start-year 2024

    # Test with limited series
    python scripts/bls/update_pc_latest.py --limit 10

Industry Codes (NAICS-based):
    113310 = Logging
    211    = Oil and gas extraction
    212    = Mining (except oil and gas)
    311    = Food manufacturing
    312    = Beverage and tobacco
    321    = Wood product manufacturing
    322    = Paper manufacturing
    324    = Petroleum and coal products
    325    = Chemical manufacturing
    331    = Primary metal manufacturing
    333    = Machinery manufacturing
    334    = Computer and electronic products
    336    = Transportation equipment
    ... and many more

Key Series Examples:
    PCU113310113310 = Logging (not seasonally adjusted)
    PCU311311       = Food manufacturing
    PCU324324       = Petroleum and coal products
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
from database.bls_models import PCSeries, PCData
from config import settings

def main():
    parser = argparse.ArgumentParser(
        description="Update PC data with latest from BLS API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update specific industries only (recommended)
  python scripts/bls/update_pc_latest.py --industries 311,325 --start-year 2024

  # Update seasonally adjusted series only
  python scripts/bls/update_pc_latest.py --seasonal S --start-year 2024

  # Update specific series
  python scripts/bls/update_pc_latest.py --series-ids PCU113310113310,PCU311311

  # Test with limited series
  python scripts/bls/update_pc_latest.py --limit 10
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
        help='Comma-separated industry codes to filter (113310=logging, 311=food mfg, etc.)'
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
    print("UPDATING PC (PRODUCER PRICE INDEX - INDUSTRY) DATA FROM BLS API")
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
            query = session.query(PCSeries.series_id).filter(PCSeries.is_active == True)

            filters_applied = []

            # Filter by industries
            if args.industries:
                industry_list = [i.strip() for i in args.industries.split(',')]
                # Use LIKE to match industry codes (handles partial codes like '311' matching '311310')
                from sqlalchemy import or_
                industry_filters = [PCSeries.industry_code.like(f"{ind}%") for ind in industry_list]
                query = query.filter(or_(*industry_filters))
                filters_applied.append(f"industries={','.join(industry_list)}")

            # Filter by seasonal adjustment
            if args.seasonal:
                query = query.filter(PCSeries.seasonal_code == args.seasonal)
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
            print("   Consider adding filters (--industries, --seasonal)")
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
        stmt = insert(PCData).values(data_to_upsert)
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
        print("SUCCESS! PC data updated")
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
