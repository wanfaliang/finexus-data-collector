#!/usr/bin/env python3
"""
Reset BLS series update status

Marks series as needing update (clears is_current flag)
Useful for forcing a re-check of all series
"""
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.bls_tracking_models import BLSSeriesUpdateStatus
from config import settings

def main():
    parser = argparse.ArgumentParser(description="Reset BLS series update status")
    parser.add_argument(
        '--surveys',
        required=True,
        help='Comma-separated list of survey codes to reset (e.g., CU,CE)'
    )
    parser.add_argument(
        '--confirm',
        action='store_true',
        help='Confirm the reset operation'
    )
    args = parser.parse_args()

    survey_codes = [s.strip().upper() for s in args.surveys.split(',')]

    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("=" * 80)
    print("RESET BLS SERIES UPDATE STATUS")
    print("=" * 80)

    # Count series to reset
    total = 0
    for survey_code in survey_codes:
        count = session.query(BLSSeriesUpdateStatus).filter(
            BLSSeriesUpdateStatus.survey_code == survey_code
        ).count()
        total += count
        print(f"\n{survey_code}: {count:,} series will be marked for update")

    if total == 0:
        print("\nNo series found for specified surveys.")
        session.close()
        return

    print(f"\nTotal: {total:,} series will be reset")

    if not args.confirm:
        print("\n" + "!" * 80)
        print("This will mark all series as needing update!")
        print("!" * 80)
        response = input("\nAre you sure? (yes/no): ")
        if response.lower() != 'yes':
            print("Reset cancelled.")
            session.close()
            return

    # Reset status
    for survey_code in survey_codes:
        session.query(BLSSeriesUpdateStatus).filter(
            BLSSeriesUpdateStatus.survey_code == survey_code
        ).update({
            'is_current': False
        })

    session.commit()

    print("\n" + "=" * 80)
    print(f"SUCCESS! Reset {total:,} series")
    print("=" * 80)
    print("\nThese series will be updated on next universal_update run.")

    session.close()

if __name__ == "__main__":
    main()
