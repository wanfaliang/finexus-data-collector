"""
Load Bulk Company Profiles
Loads company profiles from bulk CSV file into the database
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.connection import get_session
from src.collectors.bulk_profile_collector import BulkProfileCollector


def main():
    print("="*80)
    print("Loading Bulk Company Profiles")
    print("="*80)
    print()

    with get_session() as session:
        collector = BulkProfileCollector(session)

        print("Processing all profile bulk files...\n")
        results = collector.process_all_profile_files()

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
        print(f"Records updated: {collector.records_updated:,}")
        print(f"{'='*80}")

        return 0 if failed_count == 0 else 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
