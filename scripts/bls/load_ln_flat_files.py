#!/usr/bin/env python3
"""
Load BLS LN (Labor Force Statistics from CPS) flat files into PostgreSQL database

This script loads CPS labor force data with 67K+ series tracking employment,
unemployment, labor force participation, and detailed demographics.

Usage:
    # Load everything (recommended - uses AllData file)
    python scripts/bls/load_ln_flat_files.py

    # Load only reference tables
    python scripts/bls/load_ln_flat_files.py --skip-data

    # Load specific data files
    python scripts/bls/load_ln_flat_files.py --data-files ln.data.1.AllData

File Structure:
- ln.data.1.AllData: ALL historical data (358 MB) - RECOMMENDED
- Reference files: 33+ mapping files (ln.lfst, ln.ages, ln.sexs, ln.race, ln.education, etc.)
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.ln_flat_file_parser import LNFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load LN flat files into database")
    parser.add_argument('--data-dir', default='data/bls/ln', help='Directory containing LN flat files')
    parser.add_argument('--data-files', help='Comma-separated list of data files to load')
    parser.add_argument('--load-all', action='store_true', help='Load ALL data files')
    parser.add_argument('--skip-reference', action='store_true', help='Skip loading reference tables')
    parser.add_argument('--skip-data', action='store_true', help='Skip loading time series data')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for data loading')

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS LN (LABOR FORCE STATISTICS FROM CPS)")
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
            data_files = ['ln.data.1.AllData']
            print("Loading all historical data: ln.data.1.AllData (358 MB)")

    print()

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        ln_parser = LNFlatFileParser(data_dir=args.data_dir)

        if not args.skip_reference:
            print("LOADING REFERENCE TABLES (33 tables)")
            print("-" * 80)
            ln_parser.load_reference_tables(session)
            print()

        if not args.skip_data:
            print("LOADING TIME SERIES DATA")
            print("-" * 80)
            ln_parser.load_data(session, data_files=data_files, batch_size=args.batch_size)
            print()

        print("=" * 80)
        print("SUCCESS! LN data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_ln_series LIMIT 10;")
        print("     SELECT * FROM bls_ln_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. Update with latest data via API:")
        print("     python scripts/bls/update_ln_latest.py")
        print()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download LN flat files first:")
        print("  cd data/bls && ./download_ln.sh")
        sys.exit(1)
    finally:
        session.close()


if __name__ == '__main__':
    main()
