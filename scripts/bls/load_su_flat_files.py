#!/usr/bin/env python3
"""
Load SU (Chained Consumer Price Index - All Urban Consumers) data from flat files into PostgreSQL

This script parses the BLS flat files downloaded from:
https://download.bls.gov/pub/time.series/su/

Files expected in data/bls/su/:
  - su.area, su.item, su.periodicity, su.series
  - su.data.0.Current, su.data.1.AllItems

PREREQUISITE: Run Alembic migrations first!
  alembic revision --autogenerate -m "Add BLS SU tables"
  alembic upgrade head

Usage:
    python scripts/bls/load_su_flat_files.py
    python scripts/bls/load_su_flat_files.py --data-files su.data.0.Current,su.data.1.AllItems
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.su_flat_file_parser import SUFlatFileParser
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load SU flat files into database")
    parser.add_argument(
        '--data-dir',
        default='data/bls/su',
        help='Directory containing SU flat files (default: data/bls/su)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load (default: su.data.0.Current,su.data.1.AllItems)'
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
        help='Skip loading reference tables'
    )
    parser.add_argument(
        '--skip-data',
        action='store_true',
        help='Skip loading time series data'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING SU (CHAINED CONSUMER PRICE INDEX - ALL URBAN CONSUMERS) DATA FROM FLAT FILES")
    print("=" * 80)
    print(f"\nData directory: {args.data_dir}")
    print(f"Batch size: {args.batch_size:,}")

    # Parse data files list
    data_files = None
    if args.data_files:
        data_files = [f.strip() for f in args.data_files.split(',')]
        print(f"Data files: {', '.join(data_files)}")
    else:
        print("Data files: su.data.0.Current, su.data.1.AllItems (default)")

    # Get database URL from config
    database_url = settings.database.url

    # Create engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        file_parser = SUFlatFileParser(data_dir=args.data_dir)

        # Load reference tables
        if not args.skip_reference:
            file_parser.load_reference_tables(session)
        else:
            print("\nSkipping reference tables (--skip-reference)")

        # Load time series data
        if not args.skip_data:
            file_parser.load_data(session, data_files=data_files, batch_size=args.batch_size)
        else:
            print("\nSkipping time series data (--skip-data)")

        print("\n" + "=" * 80)
        print("SUCCESS! SU data loaded to database")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
