#!/usr/bin/env python3
"""
Update CW (Consumer Price Index - Urban Wage Earners and Clerical Workers) data with latest observations from BLS API

This script fetches the latest data points for CW series via the BLS API
and updates the database. Use this for regular monthly updates after initial load.

Usage:
    python scripts/bls/update_cw_latest.py
    python scripts/bls/update_cw_latest.py --start-year 2024
    python scripts/bls/update_cw_latest.py --limit 100
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
from database.bls_models import CWSeries, CWData
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Update CW data with latest from BLS API")
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
        '--series-ids',
        help='Comma-separated list of series IDs to update (default: all active series)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of series to update (for testing)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("UPDATING CW (CPI-W) DATA FROM BLS API")
    print("=" * 80)
    print(f"\nYear range: {args.start_year}-{args.end_year}")

    # Get API key from config
    api_key = settings.api.bls_api_key

    # Create BLS client
    client = BLSClient(api_key=api_key)

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
            # Get all active series from database
            query = session.query(CWSeries.series_id).filter(CWSeries.is_active == True)
            if args.limit:
                query = query.limit(args.limit)
            series_ids = [row[0] for row in query.all()]
            print(f"Updating {len(series_ids)} active series from database")

        if not series_ids:
            print("No series to update!")
            return

        # Calculate number of API requests needed
        num_requests = (len(series_ids) + 49) // 50  # Ceiling division
        print(f"API requests needed: ~{num_requests} ({len(series_ids)} series ÷ 50 per request)")

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
        stmt = insert(CWData).values(data_to_upsert)
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
        print("SUCCESS! CW data updated")
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
