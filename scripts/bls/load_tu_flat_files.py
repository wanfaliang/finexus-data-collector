#!/usr/bin/env python3
"""
Load BLS TU (American Time Use Survey) flat files into PostgreSQL database

This script loads time use data showing how Americans spend their time.

Usage:
    # Load everything (recommended - uses AllData file + aspect)
    python scripts/bls/load_tu_flat_files.py

    # Load only reference tables
    python scripts/bls/load_tu_flat_files.py --skip-data --skip-aspect

    # Load specific data files
    python scripts/bls/load_tu_flat_files.py --data-files tu.data.0.Current

    # Skip aspect data (standard errors)
    python scripts/bls/load_tu_flat_files.py --skip-aspect

File Structure:
- tu.data.1.AllData: ALL historical data (101 MB, annual 2003-2024) - RECOMMENDED
- tu.data.0.Current: Recent/current data only
- tu.aspect: Standard errors (105 MB)
- Reference files: tu.stattype, tu.actcode, tu.sex, tu.age, tu.race, tu.educ, etc.
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.tu_flat_file_parser import TUFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load TU flat files into database")
    parser.add_argument('--data-dir', default='data/bls/tu', help='Directory containing TU flat files')
    parser.add_argument('--data-files', help='Comma-separated list of data files to load')
    parser.add_argument('--load-all', action='store_true', help='Load ALL data files')
    parser.add_argument('--skip-reference', action='store_true', help='Skip loading reference tables')
    parser.add_argument('--skip-data', action='store_true', help='Skip loading time series data')
    parser.add_argument('--skip-aspect', action='store_true', help='Skip loading aspect data (standard errors)')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for data loading')

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS TU (AMERICAN TIME USE SURVEY) DATA")
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
            data_files = ['tu.data.1.AllData']
            print("Loading all historical data: tu.data.1.AllData (101 MB)")

    if not args.skip_aspect:
        print("Loading aspect data (standard errors): tu.aspect (105 MB)")

    print()

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        tu_parser = TUFlatFileParser(data_dir=args.data_dir)

        if not args.skip_reference:
            print("LOADING REFERENCE TABLES")
            print("-" * 80)
            tu_parser.load_reference_tables(session)
            print()

        if not args.skip_data:
            print("LOADING TIME SERIES DATA")
            print("-" * 80)
            tu_parser.load_data(session, data_files=data_files, batch_size=args.batch_size)
            print()

        if not args.skip_aspect:
            print("LOADING ASPECT DATA (STANDARD ERRORS)")
            print("-" * 80)
            tu_parser.load_aspect(session, batch_size=args.batch_size)
            print()

        print("=" * 80)
        print("SUCCESS! TU data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_tu_series LIMIT 10;")
        print("     SELECT * FROM bls_tu_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. Update with latest data via API:")
        print("     python scripts/bls/update_tu_latest.py")
        print()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download TU flat files first:")
        print("  cd data/bls && ./download_tu.sh")
        sys.exit(1)
    finally:
        session.close()


if __name__ == '__main__':
    main()
