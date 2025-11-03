"""
Backfill Historical Data for Priority Companies
Reads from priority list and backfills data in controlled manner
"""
import sys
import os
import argparse
import time
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.connection import get_session
from src.database.models import Company
from src.collectors.company_collector import CompanyCollector
from src.collectors.financial_collector import FinancialCollector
from src.collectors.price_collector import PriceCollector
from src.collectors.analyst_collector import AnalystCollector
from src.collectors.insider_collector import InsiderCollector
from src.collectors.employee_collector import EmployeeCollector
from src.collectors.enterprise_collector import EnterpriseCollector


def load_priority_list(file_path):
    """Load symbols from priority list file (txt or csv)"""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Priority list not found: {file_path}")

    symbols = []

    if path.suffix == '.txt':
        # Plain text file, one symbol per line
        with open(path, 'r') as f:
            symbols = [line.strip() for line in f if line.strip()]

    elif path.suffix == '.csv':
        # CSV file, assume first column is symbol
        import csv
        with open(path, 'r', encoding='utf-8-sig') as f:  # Handle BOM
            reader = csv.DictReader(f)
            # Try common column names
            for row in reader:
                symbol = row.get('symbol') or row.get('Symbol') or row.get('SYMBOL') or row.get('ticker') or row.get('Ticker')
                if symbol:
                    symbols.append(symbol.strip())

    else:
        raise ValueError(f"Unsupported file format: {path.suffix}. Use .txt or .csv")

    print(f"Loaded {len(symbols):,} symbols from {path.name}")
    return symbols


def verify_symbols_in_database(session, symbols):
    """Verify which symbols exist in database (preserves input order)"""
    # Query database for all symbols
    db_query_results = session.query(Company.symbol).filter(Company.symbol.in_(symbols)).all()
    db_symbols_set = set([s[0] for s in db_query_results])

    # Preserve original order from input file
    valid_symbols = [s for s in symbols if s in db_symbols_set]
    missing = set(symbols) - db_symbols_set

    print(f"  ✓ Found in database: {len(valid_symbols):,}")
    if missing:
        print(f"  ⚠ Not in database: {len(missing)} symbols")
        if len(missing) <= 10:
            print(f"    Missing: {', '.join(sorted(missing))}")

    return valid_symbols


def save_progress(progress_file, completed_symbols):
    """Save progress to file for resume capability"""
    with open(progress_file, 'w') as f:
        f.write('\n'.join(completed_symbols))


def load_progress(progress_file):
    """Load previously completed symbols"""
    if not Path(progress_file).exists():
        return set()

    with open(progress_file, 'r') as f:
        return set(line.strip() for line in f if line.strip())


def backfill_symbol(session, symbol, collectors, args):
    """Backfill all data for a single symbol"""
    results = {}

    try:
        # Company profile update
        if 'company' in args.collectors:
            results['company'] = collectors['company'].collect_for_symbol(symbol)

        # Financial statements
        if 'financial' in args.collectors:
            results['financial'] = collectors['financial'].collect_for_symbol(symbol)

        # Prices
        if 'price' in args.collectors:
            results['price'] = collectors['price'].collect_for_symbol(symbol)

        # Analyst data
        if 'analyst' in args.collectors:
            results['analyst'] = collectors['analyst'].collect_for_symbol(symbol)

        # Insider trading
        if 'insider' in args.collectors:
            results['insider'] = collectors['insider'].collect_for_symbol(symbol)

        # Employee history
        if 'employee' in args.collectors:
            results['employee'] = collectors['employee'].collect_for_symbol(symbol)

        # Enterprise values
        if 'enterprise' in args.collectors:
            results['enterprise'] = collectors['enterprise'].collect_for_symbol(symbol)

        return True, results

    except Exception as e:
        print(f"    [ERROR] {symbol}: {e}")
        return False, {'error': str(e)}


def main():
    parser = argparse.ArgumentParser(
        description='Backfill historical data for priority companies',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill Priority 1 companies (all collectors)
  python scripts/backfill_priority_data.py data/priority_lists/priority1_active_in_db.txt

  # Backfill first 100 companies only
  python scripts/backfill_priority_data.py data/priority_lists/priority1_active_in_db.txt --limit 100

  # Only collect financial and price data
  python scripts/backfill_priority_data.py priority.txt --collectors financial,price

  # Resume from where you left off
  python scripts/backfill_priority_data.py priority.txt --resume
        """
    )

    parser.add_argument('priority_file', help='Path to priority list file (.txt or .csv)')
    parser.add_argument('--limit', type=int, help='Limit number of companies to process')
    parser.add_argument('--collectors', default='company,financial,price,analyst,insider,employee,enterprise',
                        help='Comma-separated list of collectors to run (default: all)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume from previous run (skip already completed)')
    parser.add_argument('--progress-file', default='data/progress/backfill_progress.txt',
                        help='File to track progress for resume capability')

    args = parser.parse_args()

    # Parse collectors
    args.collectors = [c.strip() for c in args.collectors.split(',')]

    print("="*80)
    print("BACKFILL PRIORITY DATA")
    print("="*80)
    print()
    print(f"Priority file: {args.priority_file}")
    print(f"Collectors: {', '.join(args.collectors)}")
    if args.limit:
        print(f"Limit: {args.limit:,} companies")
    if args.resume:
        print(f"Resume mode: ON")
    print()

    # Load priority list
    symbols = load_priority_list(args.priority_file)

    # Load progress if resuming
    completed_symbols = set()
    if args.resume:
        completed_symbols = load_progress(args.progress_file)
        if completed_symbols:
            print(f"Resuming: {len(completed_symbols):,} companies already completed")
            symbols = [s for s in symbols if s not in completed_symbols]
            print(f"Remaining: {len(symbols):,} companies")

    if not symbols:
        print("No symbols to process!")
        return 0

    with get_session() as session:
        # Verify symbols exist in database BEFORE applying limit
        print("\nVerifying symbols in database...")
        valid_symbols = verify_symbols_in_database(session, symbols)

        # Apply limit AFTER verification to ensure we get the requested number
        if args.limit and len(valid_symbols) > args.limit:
            valid_symbols = valid_symbols[:args.limit]
            print(f"Limited to: {len(valid_symbols):,} companies")

        if not valid_symbols:
            print("No valid symbols found in database!")
            return 1

        # Initialize collectors
        print("\nInitializing collectors...")
        collectors = {}
        if 'company' in args.collectors:
            collectors['company'] = CompanyCollector(session)
        if 'financial' in args.collectors:
            collectors['financial'] = FinancialCollector(session)
        if 'price' in args.collectors:
            collectors['price'] = PriceCollector(session)
        if 'analyst' in args.collectors:
            collectors['analyst'] = AnalystCollector(session)
        if 'insider' in args.collectors:
            collectors['insider'] = InsiderCollector(session)
        if 'employee' in args.collectors:
            collectors['employee'] = EmployeeCollector(session)
        if 'enterprise' in args.collectors:
            collectors['enterprise'] = EnterpriseCollector(session)

        # Start collection run for proper logging to data_collection_log
        job_name = f"backfill_priority_data_{Path(args.priority_file).stem}"
        if len(collectors) > 0:
            first_collector = list(collectors.values())[0]
            first_collector.start_collection_run(job_name, valid_symbols)

        # Process each symbol
        print("\n" + "="*80)
        print(f"PROCESSING {len(valid_symbols):,} COMPANIES")
        print("="*80)

        success_count = 0
        failed_count = 0
        start_time = time.time()

        # Ensure progress directory exists
        Path(args.progress_file).parent.mkdir(parents=True, exist_ok=True)

        for idx, symbol in enumerate(valid_symbols, 1):
            print(f"\n[{idx}/{len(valid_symbols)}] {symbol}")

            success, results = backfill_symbol(session, symbol, collectors, args)

            if success:
                success_count += 1
                completed_symbols.add(symbol)

                # Show summary for this symbol
                for collector_name, result in results.items():
                    if isinstance(result, dict):
                        print(f"    {collector_name}: {result}")
            else:
                failed_count += 1

            # Save progress every 10 companies
            if idx % 10 == 0:
                save_progress(args.progress_file, completed_symbols)

            # Show progress every 50
            if idx % 50 == 0:
                elapsed = time.time() - start_time
                rate = idx / elapsed
                remaining = (len(valid_symbols) - idx) / rate if rate > 0 else 0
                print(f"\n  Progress: {idx}/{len(valid_symbols)} ({idx/len(valid_symbols)*100:.1f}%)")
                print(f"  Rate: {rate:.2f} companies/sec")
                print(f"  Estimated remaining: {remaining/60:.1f} minutes")

        # Final progress save
        save_progress(args.progress_file, completed_symbols)

        # End collection run for proper logging
        if len(collectors) > 0:
            first_collector = list(collectors.values())[0]
            status = 'success' if failed_count == 0 else ('partial' if success_count > 0 else 'failed')
            first_collector.end_collection_run(status)

        # Summary
        elapsed = time.time() - start_time
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Total processed: {len(valid_symbols):,}")
        print(f"Successful: {success_count:,}")
        print(f"Failed: {failed_count:,}")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print(f"Average rate: {len(valid_symbols)/elapsed:.2f} companies/sec")
        print()
        print(f"Progress saved to: {args.progress_file}")
        print(f"Collection logged to data_collection_log table")
        print("="*80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
