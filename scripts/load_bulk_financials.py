"""
Load Bulk Financial Statements
Loads income statements, balance sheets, and cash flows from bulk CSV files
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.connection import get_session
from src.collectors.bulk_financial_collector import BulkFinancialCollector


def main():
    print("="*80)
    print("Loading Bulk Financial Statements")
    print("="*80)
    print()

    with get_session() as session:
        collector = BulkFinancialCollector(session)

        print("Processing all financial statement files...\n")
        results = collector.process_all_financial_files()

        print(f"\n{'='*80}")
        print("RESULTS")
        print(f"{'='*80}")

        if not results:
            print("No files were processed")
            return 1

        success_count = sum(1 for v in results.values() if v)
        failed_count = len(results) - success_count

        for filename, success in results.items():
            status = "[OK]" if success else "[FAIL]"
            print(f"{status} {filename}")

        print(f"\n{'='*80}")
        print(f"Total files: {len(results)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {failed_count}")
        print(f"Records inserted: {collector.records_inserted:,}")
        print(f"{'='*80}")

        return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
