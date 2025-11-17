#!/usr/bin/env python3
"""
Load CE (Current Employment Statistics) data from flat files into PostgreSQL

This script parses the BLS flat files downloaded from:
https://download.bls.gov/pub/time.series/ce/

Files expected in data/bls/ce/:
  - ce.industry, ce.datatype, ce.supersector, ce.series
  - 60+ data files (ce.data.*.*)

PREREQUISITE: Run Alembic migrations first!
  alembic revision --autogenerate -m "Add BLS CE tables"
  alembic upgrade head

Usage:
    # Load from main file only (fastest, all series, ~324MB)
    python scripts/bls/load_ce_flat_files.py

    # Load specific industry files
    python scripts/bls/load_ce_flat_files.py --data-files ce.data.30a.Manufacturing.Employment,ce.data.50a.Information.Employment

    # Load all data files (complete dataset, ~723MB)
    python scripts/bls/load_ce_flat_files.py --load-all

    # Load employment data only (XXa files)
    python scripts/bls/load_ce_flat_files.py --load-employment

    # Load hours and earnings (XXb and XXc files)
    python scripts/bls/load_ce_flat_files.py --load-earnings
"""
import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.ce_flat_file_parser import CEFlatFileParser
from config import settings

def get_all_data_files(data_dir: Path) -> list:
    """Get all ce.data.*.* files"""
    return sorted([f.name for f in data_dir.glob("ce.data.*") if f.is_file() and f.name != "ce.data.Goog"])

def get_employment_files() -> list:
    """Get employment data files (XXa pattern)"""
    files = [
        'ce.data.00a.TotalNonfarm.Employment',
        'ce.data.05a.TotalPrivate.Employment',
        'ce.data.10a.MiningAndLogging.Employment',
        'ce.data.20a.Construction.Employment',
        'ce.data.30a.Manufacturing.Employment',
        'ce.data.31a.ManufacturingDurableGoods.Employment',
        'ce.data.32a.ManufacturingNondurableGoods.Employment',
        'ce.data.40a.TradeTransportationAndUtilities.Employment',
        'ce.data.41a.WholesaleTrade.Employment',
        'ce.data.42a.RetailTrade.Employment',
        'ce.data.43a.TransportationAndWarehousingAndUtilities.Employment',
        'ce.data.50a.Information.Employment',
        'ce.data.55a.FinancialActivities.Employment',
        'ce.data.60a.ProfessionalBusinessServices.Employment',
        'ce.data.65a.EducationAndHealthCare.Employment',
        'ce.data.70a.LeisureAndHospitality.Employment',
        'ce.data.80a.OtherServices.Employment',
        'ce.data.90a.Government.Employment',
    ]
    return files

def get_earnings_files() -> list:
    """Get hours and earnings files (XXb and XXc patterns)"""
    # All employee hours/earnings (XXb) + production employee hours/earnings (XXc)
    data_dir = Path("data/bls/ce")
    return sorted([
        f.name for f in data_dir.glob("ce.data.*")
        if f.is_file() and (
            'AllEmployeeHoursAndEarnings' in f.name or
            'ProductionEmployeeHoursAndEarnings' in f.name
        )
    ])

def main():
    parser = argparse.ArgumentParser(description="Load CE flat files into database")
    parser.add_argument(
        '--data-dir',
        default='data/bls/ce',
        help='Directory containing CE flat files (default: data/bls/ce)'
    )
    parser.add_argument(
        '--data-files',
        help='Comma-separated list of data files to load'
    )
    parser.add_argument(
        '--load-all',
        action='store_true',
        help='Load ALL data files (complete dataset, ~723MB)'
    )
    parser.add_argument(
        '--load-employment',
        action='store_true',
        help='Load employment data files only (XXa pattern)'
    )
    parser.add_argument(
        '--load-earnings',
        action='store_true',
        help='Load hours and earnings files (XXb and XXc patterns)'
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
    print("LOADING CE (CURRENT EMPLOYMENT STATISTICS) DATA FROM FLAT FILES")
    print("=" * 80)
    print(f"\nData directory: {args.data_dir}")
    print(f"Batch size: {args.batch_size:,}")

    # Parse data files list
    data_files = None
    if args.load_all:
        data_dir = Path(args.data_dir)
        data_files = get_all_data_files(data_dir)
        print(f"Loading ALL {len(data_files)} data files (complete dataset, ~723MB)")
        print("WARNING: This may take 30+ minutes!")
    elif args.load_employment:
        data_files = get_employment_files()
        print(f"Loading {len(data_files)} employment data files")
    elif args.load_earnings:
        data_files = get_earnings_files()
        print(f"Loading {len(data_files)} hours and earnings files")
    elif args.data_files:
        data_files = [f.strip() for f in args.data_files.split(',')]
        print(f"Data files: {', '.join(data_files)}")
    else:
        print("Data file: ce.data.0.AllCESSeries (main file, all series, ~324MB)")

    # Get database URL from config
    database_url = settings.database.url

    # Create engine and session
    engine = create_engine(database_url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Create parser
        file_parser = CEFlatFileParser(data_dir=args.data_dir)

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
        print("SUCCESS! CE data loaded to database")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
