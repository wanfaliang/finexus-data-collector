#!/usr/bin/env python3
"""
Load BLS OE (Occupational Employment and Wage Statistics) flat files into PostgreSQL.

This script loads OEWS data from flat files downloaded from:
https://download.bls.gov/pub/time.series/oe/

NOTE: OE has very large files (1.8GB total, 1.2GB series file, 12M+ data rows)
      Loading will take significant time - expect 10-30 minutes for full load.

Usage:
    # Load reference tables and all data
    python scripts/bls/load_oe_flat_files.py

    # Load only reference tables
    python scripts/bls/load_oe_flat_files.py --skip-data

    # Load only data files (reference tables already loaded)
    python scripts/bls/load_oe_flat_files.py --skip-reference

    # Load specific data files
    python scripts/bls/load_oe_flat_files.py --skip-reference \\
        --data-files oe.data.0.Current

    # Load all available data files
    python scripts/bls/load_oe_flat_files.py --skip-reference --load-all
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.oe_flat_file_parser import OEFlatFileParser, get_all_data_files
from config import settings


def main():
    parser = argparse.ArgumentParser(description="Load OE flat files into database")
    parser.add_argument('--data-dir', default='data/bls/oe', help='Directory containing OE flat files')
    parser.add_argument('--data-files', help='Comma-separated list of data files to load')
    parser.add_argument('--load-all', action='store_true', help='Load ALL data files')
    parser.add_argument('--skip-reference', action='store_true', help='Skip loading reference tables')
    parser.add_argument('--skip-data', action='store_true', help='Skip loading time series data')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for data loading')

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS OE (OCCUPATIONAL EMPLOYMENT AND WAGE STATISTICS) DATA")
    print("=" * 80)
    print()
    print("WARNING: OE has very large files (1.8GB total, 1.2GB series file, 12M+ rows)")
    print("         Full load may take 10-30 minutes")
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
            data_files = ['oe.data.1.AllData']
            print("Loading all historical data: oe.data.1.AllData (~12M+ observations)")

    print()

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        oe_parser = OEFlatFileParser(data_dir=args.data_dir)

        if not args.skip_reference:
            print("LOADING REFERENCE TABLES (including 1.2GB series file)")
            print("-" * 80)
            oe_parser.load_reference_tables(session)
            print()

        if not args.skip_data:
            print("LOADING TIME SERIES DATA")
            print("-" * 80)
            oe_parser.load_data(session, data_files=data_files, batch_size=args.batch_size)
            print()

        print("=" * 80)
        print("SUCCESS! OE data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_oe_series LIMIT 10;")
        print("     SELECT * FROM bls_oe_data WHERE year >= 2023 LIMIT 10;")
        print()
        print("  2. Update with latest data via API:")
        print("     python scripts/bls/update_oe_latest.py --start-year 2023")
        print()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please download OE flat files first:")
        print("  cd data/bls && ./download_oe.sh")
        sys.exit(1)
    finally:
        session.close()


if __name__ == '__main__':
    main()
