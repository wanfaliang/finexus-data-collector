#!/usr/bin/env python3
"""
Load AP (Average Price) data from flat files into PostgreSQL

This script parses the BLS flat files downloaded from:
https://download.bls.gov/pub/time.series/ap/

Files expected in data/bls/ap/:
  - ap.area, ap.item, ap.period, ap.series
  - ap.data.0.Current
  - ap.data.1.HouseholdFuels
  - ap.data.2.Gasoline
  - ap.data.3.Food

PREREQUISITE: Run Alembic migrations first!
  alembic revision --autogenerate -m "Add BLS AP tables"
  alembic upgrade head

Usage:
    python scripts/bls/load_ap_flat_files.py
    python scripts/bls/load_ap_flat_files.py --data-dir data/bls/ap
    python scripts/bls/load_ap_flat_files.py --batch-size 5000
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.ap_flat_file_parser import APFlatFileParser
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load AP flat files into database")
    parser.add_argument(
        '--data-dir',
        default='data/bls/ap',
        help='Directory containing AP flat files (default: data/bls/ap)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for database inserts (default: 10000)'
    )
    parser.add_argument(
        '--skip-reference',
        action='store_true',
        help='Skip loading reference tables (areas, items, periods, series)'
    )
    parser.add_argument(
        '--skip-data',
        action='store_true',
        help='Skip loading time series data'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING AP (AVERAGE PRICE) DATA FROM FLAT FILES")
    print("=" * 80)
    print(f"\nData directory: {args.data_dir}")
    print(f"Batch size: {args.batch_size:,}")

    # Get database URL from config
    database_url = settings.database.url

    # Create engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        file_parser = APFlatFileParser(data_dir=args.data_dir)

        # Load reference tables
        if not args.skip_reference:
            file_parser.load_reference_tables(session)
        else:
            print("\nSkipping reference tables (--skip-reference)")

        # Load time series data
        if not args.skip_data:
            file_parser.load_data(session, batch_size=args.batch_size)
        else:
            print("\nSkipping time series data (--skip-data)")

        print("\n" + "=" * 80)
        print("SUCCESS! All AP data loaded to database")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
