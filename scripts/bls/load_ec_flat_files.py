#!/usr/bin/env python3
"""
Load BLS EC (Employment Cost Index) flat files into PostgreSQL database

This script loads the EC survey data - employment costs by industry, occupation, and ownership.

Usage:
    # Load everything (recommended - uses AllData file)
    python scripts/bls/load_ec_flat_files.py

    # Load only reference tables
    python scripts/bls/load_ec_flat_files.py --skip-data

    # Load specific data files
    python scripts/bls/load_ec_flat_files.py --data-files ec.data.0.Current

File Structure:
- ec.data.1.AllData: ALL historical data (3.2 MB, ~115K rows) - RECOMMENDED
- ec.data.0.Current: Recent/current data only (1.6 MB)
- Reference files: ec.compensation, ec.group, ec.ownership, ec.periodicity, ec.series
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.ec_flat_file_parser import ECFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load EC flat files into database")
    parser.add_argument('--data-dir', default='data/bls/ec', help='Directory containing EC flat files')
    parser.add_argument('--data-files', help='Comma-separated list of data files to load')
    parser.add_argument('--load-all', action='store_true', help='Load ALL data files')
    parser.add_argument('--skip-reference', action='store_true', help='Skip loading reference tables')
    parser.add_argument('--skip-data', action='store_true', help='Skip loading time series data')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for data loading')

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS EC (EMPLOYMENT COST INDEX) DATA")
    print("=" * 80)
    print()

    data_files = None
    if not args.skip_data:
        if args.data_files:
            data_files = [f.strip() for f in args.data_files.split(',')]
            print(f"Data files to load: {', '.join(data_files)}")
        elif args.load_all:
            data_files = get_all_data_files(args.data_dir)
            print(f"Loading ALL {len(data_files)} data files")
        else:
            data_files = ['ec.data.1.AllData']
            print("Loading all historical data: ec.data.1.AllData (~115K observations)")

    print()

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        parser = ECFlatFileParser(data_dir=args.data_dir)

        if not args.skip_reference:
            print("LOADING REFERENCE TABLES")
            print("-" * 80)
            parser.load_reference_tables(session)
            print()

        if not args.skip_data:
            print("LOADING TIME SERIES DATA")
            print("-" * 80)
            parser.load_data(session, data_files=data_files, batch_size=args.batch_size)
            print()

        print("=" * 80)
        print("SUCCESS! EC data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_ec_series LIMIT 10;")
        print("     SELECT * FROM bls_ec_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. For quarterly updates, use:")
        print("     python scripts/bls/update_ec_latest.py")

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
