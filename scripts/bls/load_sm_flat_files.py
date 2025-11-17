#!/usr/bin/env python3
"""
Load BLS SM (State and Metro Area Employment) flat files into PostgreSQL database

This script loads the SM survey data - employment statistics by state and metro area.

Usage:
    # Load everything (recommended - uses AllData file)
    python scripts/bls/load_sm_flat_files.py

    # Load only reference tables
    python scripts/bls/load_sm_flat_files.py --skip-data

    # Load specific state files
    python scripts/bls/load_sm_flat_files.py --data-files sm.data.33a.NewYork,sm.data.5a.California

File Structure:
- sm.data.1.AllData: ALL historical data (526 MB, 10M rows) - RECOMMENDED
- sm.data.0.Current: Recent/current data only (314 MB, 6M rows)
- sm.data.XX.*: Individual state files (for selective loading)
- Reference files: sm.state, sm.area, sm.supersector, sm.industry, sm.series
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.sm_flat_file_parser import SMFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load SM flat files into database")
    parser.add_argument('--data-dir', default='data/bls/sm', help='Directory containing SM flat files')
    parser.add_argument('--data-files', help='Comma-separated list of data files to load')
    parser.add_argument('--load-all', action='store_true', help='Load ALL data files (90+ files)')
    parser.add_argument('--skip-reference', action='store_true', help='Skip loading reference tables')
    parser.add_argument('--skip-data', action='store_true', help='Skip loading time series data')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for data loading')

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS SM (STATE AND METRO AREA EMPLOYMENT) DATA")
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
            data_files = ['sm.data.1.AllData']
            print("Loading all historical data: sm.data.1.AllData (10M observations)")

    print()

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        parser = SMFlatFileParser(data_dir=args.data_dir)

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
        print("SUCCESS! SM data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_sm_series LIMIT 10;")
        print("     SELECT * FROM bls_sm_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. For monthly updates, use:")
        print("     python scripts/bls/update_sm_latest.py")

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
