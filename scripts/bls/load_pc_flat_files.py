#!/usr/bin/env python3
"""
Load BLS PC (Producer Price Index - Industry) flat files into PostgreSQL database

This script loads the PC survey data, which includes:
- Industry codes (NAICS-based industry classification)
- Product codes (products within each industry)
- Series metadata (4,746 price index series)
- Time series data (1.18M observations, 1981-2025)

The PC survey tracks producer prices organized by industry (NAICS-based).
It's complementary to WP (commodities) and critical for tracking inflation.

Usage:
    # Load everything (recommended - uses main file with all data)
    python scripts/bls/load_pc_flat_files.py

    # Load only reference tables (industries, products, series metadata)
    python scripts/bls/load_pc_flat_files.py --skip-data

    # Load specific data files
    python scripts/bls/load_pc_flat_files.py --data-files pc.data.14.Chemicals,pc.data.19.Machinery

    # Load ALL individual data files (70+ files, slower than main file)
    python scripts/bls/load_pc_flat_files.py --load-all

File Structure:
- pc.data.0.Current: Main file with ALL current data (62.2 MB, 1.18M rows) - RECOMMENDED
- pc.data.XX.*: Individual files by industry subsector (smaller, for selective loading)
- pc.industry: Industry codes and names
- pc.product: Product codes within industries
- pc.series: Series metadata (series IDs, titles, date ranges)

Examples:
    # Initial load (loads everything from main file)
    python scripts/bls/load_pc_flat_files.py

    # Update reference tables only (after BLS updates industry/product lists)
    python scripts/bls/load_pc_flat_files.py --skip-data

    # Load chemicals industry only
    python scripts/bls/load_pc_flat_files.py --data-files pc.data.14.Chemicals --skip-reference
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.pc_flat_file_parser import PCFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(
        description="Load PC flat files into database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load everything (recommended - uses main file)
  python scripts/bls/load_pc_flat_files.py

  # Load only reference tables
  python scripts/bls/load_pc_flat_files.py --skip-data

  # Load specific industries
  python scripts/bls/load_pc_flat_files.py --data-files pc.data.14.Chemicals,pc.data.19.Machinery

  # Load ALL individual files (slower, not recommended)
  python scripts/bls/load_pc_flat_files.py --load-all

Data Files:
  Main file (RECOMMENDED):
    - pc.data.0.Current (62.2 MB, 1.18M observations, all current data)

  Individual files (70+ files, for selective loading):
    - pc.data.14.Chemicals
    - pc.data.19.Machinery
    - pc.data.22.TransportationEquipment
    - ... and many more

Reference files (always loaded unless --skip-reference):
    - pc.industry (1,058 industries)
    - pc.product (4,746 products)
    - pc.series (4,746 series metadata)
        """
    )
    parser.add_argument(
        '--data-dir',
        default='data/bls/pc',
        help='Directory containing PC flat files (default: data/bls/pc)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load (e.g., pc.data.0.Current,pc.data.14.Chemicals)'
    )
    parser.add_argument(
        '--load-all',
        action='store_true',
        help='Load ALL data files (70+ files). Note: main file pc.data.0.Current contains all current data already'
    )
    parser.add_argument(
        '--skip-reference',
        action='store_true',
        help='Skip loading reference tables (industries, products, series metadata)'
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
    print("LOADING BLS PC (PRODUCER PRICE INDEX - INDUSTRY) DATA")
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
            print("Note: This is slower than loading just pc.data.0.Current which contains all current data")
        else:
            data_files = ['pc.data.0.Current']  # Default to main file
            print("Loading main data file: pc.data.0.Current (contains all current data)")

    print()

    # Get database session
    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Initialize parser
        parser = PCFlatFileParser(data_dir=args.data_dir)

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
        print("SUCCESS! PC data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_pc_series LIMIT 10;")
        print("     SELECT * FROM bls_pc_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. For monthly updates, use:")
        print("     python scripts/bls/update_pc_latest.py")

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
