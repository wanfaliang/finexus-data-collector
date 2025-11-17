#!/usr/bin/env python3
"""
Load CW (Consumer Price Index - Urban Wage Earners and Clerical Workers) data from flat files into PostgreSQL

This script parses the BLS flat files downloaded from:
https://download.bls.gov/pub/time.series/cw/

Files expected in data/bls/cw/:
  - cw.area, cw.item, cw.period, cw.periodicity, cw.series
  - cw.data.0.Current (and optionally other data files)
  - cw.aspect (optional)

PREREQUISITE: Run Alembic migrations first!
  alembic revision --autogenerate -m "Add BLS CW tables"
  alembic upgrade head

Usage:
    python scripts/bls/load_cw_flat_files.py
    python scripts/bls/load_cw_flat_files.py --data-files cw.data.0.Current,cw.data.1.AllItems
    python scripts/bls/load_cw_flat_files.py --load-aspects
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.cw_flat_file_parser import CWFlatFileParser
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load CW flat files into database")
    parser.add_argument(
        '--data-dir',
        default='data/bls/cw',
        help='Directory containing CW flat files (default: data/bls/cw)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load (default: cw.data.0.Current)'
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
    parser.add_argument(
        '--load-aspects',
        action='store_true',
        help='Load aspect data (cw.aspect file)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING CW (CONSUMER PRICE INDEX - WAGE EARNERS) DATA FROM FLAT FILES")
    print("=" * 80)
    print(f"\nData directory: {args.data_dir}")
    print(f"Batch size: {args.batch_size:,}")

    # Parse data files list
    data_files = None
    if args.data_files:
        data_files = [f.strip() for f in args.data_files.split(',')]
        print(f"Data files: {', '.join(data_files)}")
    else:
        print("Data files: cw.data.0.Current (default)")

    # Get database URL from config
    database_url = settings.database.url

    # Create engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        file_parser = CWFlatFileParser(data_dir=args.data_dir)

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

        # Load aspects if requested
        if args.load_aspects:
            file_parser.load_aspects(session, batch_size=args.batch_size)

        print("\n" + "=" * 80)
        print("SUCCESS! CW data loaded to database")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
