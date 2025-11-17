#!/usr/bin/env python3
"""
Load EI (Import/Export Price Indexes) data from flat files into PostgreSQL

This script parses the BLS flat files downloaded from:
https://download.bls.gov/pub/time.series/ei/

Files expected in data/bls/ei/:
  - ei.index, ei.series, ei.period, ei.seasonal
  - ei.data.0.Current, ei.data.1.BEAImport, ei.data.2.BEAExport, etc.

PREREQUISITE: Run Alembic migrations first!
  alembic upgrade head

Usage:
    python scripts/bls/load_ei_flat_files.py
    python scripts/bls/load_ei_flat_files.py --data-files ei.data.0.Current
    python scripts/bls/load_ei_flat_files.py --data-files ei.data.0.Current,ei.data.1.BEAImport,ei.data.2.BEAExport
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.ei_flat_file_parser import EIFlatFileParser
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load EI flat files into database")
    parser.add_argument(
        '--data-dir',
        default='data/bls/ei',
        help='Directory containing EI flat files (default: data/bls/ei)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load (default: ei.data.0.Current)'
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
    print("LOADING EI (IMPORT/EXPORT PRICE INDEXES) DATA FROM FLAT FILES")
    print("=" * 80)
    print(f"\nData directory: {args.data_dir}")
    print(f"Batch size: {args.batch_size:,}")

    # Parse data files list
    data_files = None
    if args.data_files:
        data_files = [f.strip() for f in args.data_files.split(',')]
        print(f"Data files: {', '.join(data_files)}")
    else:
        print("Data files: ei.data.0.Current (default)")

    # Get database URL from config
    database_url = settings.database.url

    # Create engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        file_parser = EIFlatFileParser(data_dir=args.data_dir)

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
        print("SUCCESS! EI data loaded to database")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
