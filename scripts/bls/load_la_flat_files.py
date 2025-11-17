#!/usr/bin/env python3
"""
Load LA (Local Area Unemployment Statistics) data from flat files into PostgreSQL

This script parses the BLS flat files downloaded from:
https://download.bls.gov/pub/time.series/la/

Files expected in data/bls/la/:
  - la.area, la.measure, la.period, la.series
  - 71 data files (la.data.*.*)

PREREQUISITE: Run Alembic migrations first!
  alembic revision --autogenerate -m "Add BLS LA tables"
  alembic upgrade head

Usage:
    # Load seasonally adjusted current data only (fastest, ~240K obs)
    python scripts/bls/load_la_flat_files.py

    # Load all current data files (CurrentU + CurrentS, ~16M obs)
    python scripts/bls/load_la_flat_files.py --load-current-all

    # Load specific files
    python scripts/bls/load_la_flat_files.py --data-files la.data.1.CurrentS,la.data.2.AllStatesU

    # Load ALL 71 files (complete dataset, ~20M+ obs, takes time!)
    python scripts/bls/load_la_flat_files.py --load-all
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.la_flat_file_parser import LAFlatFileParser
from config import settings

def get_all_data_files(data_dir: Path) -> list:
    """Get all la.data.*.* files"""
    return sorted([f.name for f in data_dir.glob("la.data.*") if f.is_file()])

def get_current_files() -> list:
    """Get current data files (not state-specific)"""
    return [
        'la.data.0.CurrentU00-04',
        'la.data.0.CurrentU05-09',
        'la.data.0.CurrentU10-14',
        'la.data.0.CurrentU15-19',
        'la.data.0.CurrentU20-24',
        'la.data.0.CurrentU25-29',
        'la.data.0.CurrentU90-94',
        'la.data.0.CurrentU95-99',
        'la.data.1.CurrentS',
        'la.data.2.AllStatesU',
    ]

def main():
    parser = argparse.ArgumentParser(description="Load LA flat files into database")
    parser.add_argument(
        '--data-dir',
        default='data/bls/la',
        help='Directory containing LA flat files (default: data/bls/la)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load'
    )
    parser.add_argument(
        '--load-current-all',
        action='store_true',
        help='Load all current data files (CurrentU + CurrentS, ~16M obs)'
    )
    parser.add_argument(
        '--load-all',
        action='store_true',
        help='Load ALL 71 data files (complete dataset, ~20M+ obs)'
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
    print("LOADING LA (LOCAL AREA UNEMPLOYMENT STATISTICS) DATA FROM FLAT FILES")
    print("=" * 80)
    print(f"\nData directory: {args.data_dir}")
    print(f"Batch size: {args.batch_size:,}")

    # Parse data files list
    data_files = None
    if args.load_all:
        data_dir = Path(args.data_dir)
        data_files = get_all_data_files(data_dir)
        print(f"Loading ALL {len(data_files)} data files (complete dataset)")
        print("WARNING: This will load 20M+ observations and may take 30+ minutes!")
    elif args.load_current_all:
        data_files = get_current_files()
        print(f"Loading {len(data_files)} current data files (~16M observations)")
    elif args.data_files:
        data_files = [f.strip() for f in args.data_files.split(',')]
        print(f"Data files: {', '.join(data_files)}")
    else:
        print("Data files: la.data.1.CurrentS (seasonally adjusted current data)")

    # Get database URL from config
    database_url = settings.database.url

    # Create engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        file_parser = LAFlatFileParser(data_dir=args.data_dir)

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
        print("SUCCESS! LA data loaded to database")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
