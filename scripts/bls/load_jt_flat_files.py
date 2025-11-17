#!/usr/bin/env python3
"""
Load BLS JT (Job Openings and Labor Turnover Survey - JOLTS) flat files into PostgreSQL database

This script loads the JT survey data - job openings, hires, separations, quits, and layoffs.

Usage:
    # Load everything (recommended - uses AllItems file)
    python scripts/bls/load_jt_flat_files.py

    # Load only reference tables
    python scripts/bls/load_jt_flat_files.py --skip-data

    # Load specific data files
    python scripts/bls/load_jt_flat_files.py --data-files jt.data.2.JobOpenings,jt.data.3.Hires

File Structure:
- jt.data.1.AllItems: ALL historical data (32 MB, ~1.58M rows) - RECOMMENDED
- jt.data.0.Current: Recent/current data only (19 MB)
- jt.data.2.JobOpenings: Job openings only (5.8 MB)
- jt.data.3.Hires: Hires only (5.8 MB)
- jt.data.4.TotalSeparations: Total separations (5.8 MB)
- jt.data.5.Quits: Quits only (5.8 MB)
- jt.data.6.LayoffsDischarges: Layoffs and discharges (5.8 MB)
- jt.data.7.OtherSeparations: Other separations (2.5 MB)
- jt.data.8.UnemployedPerJobOpeningRatio: Unemployment ratio (827 KB)
- Reference files: jt.dataelement, jt.industry, jt.state, jt.area, jt.sizeclass, jt.ratelevel, jt.series
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.jt_flat_file_parser import JTFlatFileParser, get_all_data_files
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Load JT flat files into database")
    parser.add_argument('--data-dir', default='data/bls/jt', help='Directory containing JT flat files')
    parser.add_argument('--data-files', help='Comma-separated list of data files to load')
    parser.add_argument('--load-all', action='store_true', help='Load ALL data files (9 files)')
    parser.add_argument('--skip-reference', action='store_true', help='Skip loading reference tables')
    parser.add_argument('--skip-data', action='store_true', help='Skip loading time series data')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for data loading')

    args = parser.parse_args()

    print("=" * 80)
    print("LOADING BLS JT (JOB OPENINGS AND LABOR TURNOVER SURVEY - JOLTS) DATA")
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
            data_files = ['jt.data.1.AllItems']
            print("Loading all historical data: jt.data.1.AllItems (~1.58M observations)")

    print()

    database_url = settings.database.url
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        parser = JTFlatFileParser(data_dir=args.data_dir)

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
        print("SUCCESS! JT data loaded into database")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Query the data:")
        print("     SELECT * FROM bls_jt_series LIMIT 10;")
        print("     SELECT * FROM bls_jt_data WHERE year >= 2024 LIMIT 10;")
        print()
        print("  2. For monthly updates, use:")
        print("     python scripts/bls/update_jt_latest.py")

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
