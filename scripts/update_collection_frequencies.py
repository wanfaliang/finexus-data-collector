"""
Update Collection Frequencies in Tracking Table

Updates existing table_update_tracking records to reflect new collection frequencies
Run this after changing collector update frequencies in code
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.database.models import TableUpdateTracking  # type: ignore

def update_frequencies():
    """Update tracking records to new frequencies"""

    # Define new frequencies
    updates = {
        # Financial tables: daily → quarterly
        'income_statements_annual': 'quarterly',
        'income_statements_quarter': 'quarterly',
        'balance_sheets_annual': 'quarterly',
        'balance_sheets_quarter': 'quarterly',
        'cash_flows_annual': 'quarterly',
        'cash_flows_quarter': 'quarterly',
        'financial_ratios_annual': 'quarterly',
        'financial_ratios_quarter': 'quarterly',
        'key_metrics_annual': 'quarterly',
        'key_metrics_quarter': 'quarterly',

        # Analyst tables: daily → 15days
        'analyst_estimates': '15days',
        'price_targets': '15days',
    }

    print("="*80)
    print("UPDATE COLLECTION FREQUENCIES")
    print("="*80)
    print()

    with get_session() as session:
        for table_name, new_frequency in updates.items():
            # Find all tracking records for this table
            records = session.query(TableUpdateTracking)\
                .filter(TableUpdateTracking.table_name == table_name)\
                .all()  # type: ignore

            if not records:
                print(f"WARNING: {table_name}: No records found")
                continue

            print(f"{table_name}: Updating {len(records)} symbols -> {new_frequency}")

            # Calculate new next_update_due based on frequency
            now = datetime.now()
            if new_frequency == 'quarterly':
                next_update = now + timedelta(days=90)
            elif new_frequency == '15days':
                next_update = now + timedelta(days=15)
            elif new_frequency == 'weekly':
                next_update = now + timedelta(weeks=1)
            elif new_frequency == 'monthly':
                next_update = now + timedelta(days=30)
            else:
                next_update = now + timedelta(days=1)

            # Update all records using bulk update
            session.query(TableUpdateTracking)\
                .filter(TableUpdateTracking.table_name == table_name)\
                .update({
                    'update_frequency': new_frequency,
                    'next_update_due': next_update
                })  # type: ignore

            session.commit()
            print(f"  SUCCESS: Set next_update_due to {next_update.date()}")
            print()

        print("="*80)
        print("UPDATE COMPLETE")
        print("="*80)
        print()
        print("Verify changes:")
        print("  Financial tables -> quarterly (next update ~90 days)")
        print("  Analyst tables -> 15days (next update ~15 days)")
        print()
        print("Note: Existing data is preserved, only update schedule changed")

    return 0


if __name__ == "__main__":
    sys.exit(update_frequencies())
