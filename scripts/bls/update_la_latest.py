#!/usr/bin/env python3
"""
Update LA (Local Area Unemployment Statistics) data with latest observations from BLS API

This script fetches the latest data points for LA series via the BLS API
and updates the database. Use this for regular updates after initial load.

Supports flexible filtering for tiered update strategies:
  - Filter by area types (states, metros, counties, etc.)
  - Filter by seasonal adjustment (seasonally adjusted vs not adjusted)
  - Filter by measure codes (unemployment rate, employment, etc.)

Usage:
    # Update all active series (not recommended - 675 requests!)
    python scripts/bls/update_la_latest.py

    # Monthly update: States + Major Metros only (~50 requests)
    python scripts/bls/update_la_latest.py --area-types A,B --seasonal S

    # Quarterly update: All Metro/Micro areas (~300 requests)
    python scripts/bls/update_la_latest.py --area-types B,D,E

    # Semi-Annual: Counties and Cities (~320 requests)
    python scripts/bls/update_la_latest.py --area-types F,G

    # Update specific series
    python scripts/bls/update_la_latest.py --series-ids LASBS060000000000003,LAUCN040130000000003

    # Test with subset
    python scripts/bls/update_la_latest.py --limit 10

Area Type Codes:
    A = Statewide
    B = Metropolitan areas
    C = Metropolitan divisions
    D = Micropolitan areas
    E = Combined areas
    F = Counties and equivalents
    G = Cities and towns above 25,000 population
    H = Cities and towns below 25,000 population in New England
    I = Towns in New England
    J = Cities and towns below 25,000 population, except New England
    K = Census divisions
    L = Census regions
    M = Multi-entity small labor market areas
    N = Balance of state areas

Seasonal Codes:
    S = Seasonally adjusted
    U = Not seasonally adjusted

Measure Codes:
    03 = Unemployment rate (%)
    04 = Unemployment (persons)
    05 = Employment (persons)
    06 = Labor force (persons)
    07 = Employment-population ratio (%)
    08 = Labor force participation rate (%)
    09 = Civilian noninstitutional population (persons)
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
from database.bls_models import LASeries, LAData
from config import settings

def main():
    parser = argparse.ArgumentParser(
        description="Update LA data with latest from BLS API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monthly: States + Major Metros (seasonally adjusted)
  python scripts/bls/update_la_latest.py --area-types A,B --seasonal S

  # Quarterly: All Metro/Micro areas
  python scripts/bls/update_la_latest.py --area-types B,D,E

  # Semi-Annual: Counties and Cities
  python scripts/bls/update_la_latest.py --area-types F,G

  # Update unemployment rate only
  python scripts/bls/update_la_latest.py --measure-codes 03
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
        '--area-types',
        help='Comma-separated area type codes to filter (A=states, B=metros, F=counties, etc.)'
    )
    parser.add_argument(
        '--seasonal',
        choices=['S', 'U'],
        help='Filter by seasonal adjustment: S=seasonally adjusted, U=not adjusted'
    )
    parser.add_argument(
        '--measure-codes',
        help='Comma-separated measure codes to filter (03=rate, 04=unemployment, 05=employment, etc.)'
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
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without making API calls'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING LA (LOCAL AREA UNEMPLOYMENT) DATA FROM BLS API")
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
            query = session.query(LASeries.series_id).filter(LASeries.is_active == True)

            filters_applied = []

            # Filter by area types
            if args.area_types:
                area_type_list = [t.strip() for t in args.area_types.split(',')]
                query = query.filter(LASeries.area_type_code.in_(area_type_list))
                filters_applied.append(f"area_types={','.join(area_type_list)}")

            # Filter by seasonal adjustment
            if args.seasonal:
                query = query.filter(LASeries.seasonal_code == args.seasonal)
                filters_applied.append(f"seasonal={args.seasonal}")

            # Filter by measure codes
            if args.measure_codes:
                measure_list = [m.strip() for m in args.measure_codes.split(',')]
                query = query.filter(LASeries.measure_code.in_(measure_list))
                filters_applied.append(f"measures={','.join(measure_list)}")

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
            print("   Consider adding filters (--area-types, --seasonal, --measure-codes)")
            if not args.dry_run:
                response = input("Continue anyway? (y/N): ")
                if response.lower() != 'y':
                    print("Aborted.")
                    return

        if args.dry_run:
            print("\n[DRY RUN] Would update the following series:")
            for i, sid in enumerate(series_ids[:10], 1):
                print(f"  {i}. {sid}")
            if len(series_ids) > 10:
                print(f"  ... and {len(series_ids) - 10} more")
            print(f"\nTotal: {len(series_ids)} series, ~{num_requests} API requests")
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
        stmt = insert(LAData).values(data_to_upsert)
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
        print("SUCCESS! LA data updated")
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
