#!/usr/bin/env python3
"""
Load BLS WP (Producer Price Index - Commodities) flat files into PostgreSQL database

This script loads the WP survey data, which includes:
- Commodity group codes (56 groups organized by end use/material)
- Item codes (4,168 commodities organized hierarchically)
- Series metadata (5,498 price index series)
- Time series data (1.31M observations, 1947-2025)

The WP survey tracks producer prices organized by commodity/product type.
It's complementary to PC (industry-based) and critical for tracking commodity inflation.

Usage:
    # Load everything (recommended - uses main file with all data)
    python scripts/bls/load_wp_flat_files.py

    # Load only reference tables (groups, items, series metadata)
    python scripts/bls/load_wp_flat_files.py --skip-data

    # Load specific data files
    python scripts/bls/load_wp_flat_files.py --data-files wp.data.6.Fuels,wp.data.7.Chemicals

    # Load ALL individual data files (35+ files, slower than main file)
    python scripts/bls/load_wp_flat_files.py --load-all

File Structure:
- wp.data.0.Current: Main file with ALL current data (69 MB, 1.31M rows) - RECOMMENDED
- wp.data.XX.*: Individual files by commodity group (smaller, for selective loading)
- wp.group: Commodity group codes and names
- wp.item: Item codes within groups
- wp.series: Series metadata (series IDs, titles, date ranges)

Examples:
    # Initial load (loads everything from main file)
    python scripts/bls/load_wp_flat_files.py

    # Update reference tables only (after BLS updates group/item lists)
    python scripts/bls/load_wp_flat_files.py --skip-data

    # Load fuels commodity group only
    python scripts/bls/load_wp_flat_files.py --data-files wp.data.6.Fuels --skip-reference
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.wp_flat_file_parser import WPFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(
        description="Load WP flat files into database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load everything (recommended - uses main file)
  python scripts/bls/load_wp_flat_files.py

  # Load only reference tables
  python scripts/bls/load_wp_flat_files.py --skip-data

  # Load specific commodity groups
  python scripts/bls/load_wp_flat_files.py --data-files wp.data.6.Fuels,wp.data.7.Chemicals

  # Load ALL individual files (slower, not recommended)
  python scripts/bls/load_wp_flat_files.py --load-all

Data Files:
  Main file (RECOMMENDED):
    - wp.data.0.Current (69 MB, 1.31M observations, all current data)

  Individual files (35+ files, for selective loading):
    - wp.data.6.Fuels (gasoline, oil, natural gas)
    - wp.data.7.Chemicals
    - wp.data.11a.Metals10-103
    - ... and many more

Reference files (always loaded unless --skip-reference):
    - wp.group (56 commodity groups)
    - wp.item (4,168 items)
    - wp.series (5,498 series metadata)
        """
    )
    parser.add_argument(
        '--data-dir',
        default='data/bls/wp',
        help='Directory containing WP flat files (default: data/bls/wp)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load (e.g., wp.data.0.Current,wp.data.6.Fuels)'
    )
    parser.add_argument(
        '--load-all',
        action='store_true',
        help='Load ALL data files (35+ files). Note: main file wp.data.0.Current contains all current data already'
    )
    parser.add_argument(
        '--skip-reference',
        action='store_true',
        help='Skip loading reference tables (groups, items, series metadata)'
    )
    parser.add_argument(
        '--skip-data',
        action='store_true',
        help='Skip loading time series data (only load reference tables)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Batch size for data loading (default: 10000)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS WP (PRODUCER PRICE INDEX - COMMODITIES) DATA")
    print("=" * 80)
    print()

    # Determine which data files to load
    data_files = None
    if not args.skip_data:
        if args.data_files:
            data_files = [f.strip() for f in args.data_files.split(',')]
            print(f"Data files to load: {', '.join(data_files)}")
        elif args.load_all:
            data_files = get_all_data_files(args.data_dir)
            print(f"Loading ALL {len(data_files)} data files")
            print("Note: This is slower than loading just wp.data.0.Current which contains all current data")
        else:
            data_files = ['wp.data.0.Current']  # Default to main file
            print("Loading main data file: wp.data.0.Current (contains all current data)")

    print()

    # Get database session
    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Initialize parser
        parser = WPFlatFileParser(data_dir=args.data_dir)

        # Load reference tables
        if not args.skip_reference:
            print("LOADING REFERENCE TABLES")
            print("-" * 80)
            parser.load_reference_tables(session)
            print()

        # Load data
        if not args.skip_data:
            print("LOADING TIME SERIES DATA")
            print("-" * 80)
            parser.load_data(
                session,
                data_files=data_files,
                batch_size=args.batch_size
            )
            print()

        print("=" * 80)
        print("SUCCESS! WP data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_wp_series LIMIT 10;")
        print("     SELECT * FROM bls_wp_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. For monthly updates, use:")
        print("     python scripts/bls/update_wp_latest.py")

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
