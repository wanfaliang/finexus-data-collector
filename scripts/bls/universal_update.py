#!/usr/bin/env python3
"""
Universal BLS Update Script

Updates any BLS survey series using the Update Cycle system:
- Soft Update: Resume existing cycle, skip already-updated series
- Force Update: Create new cycle, start fresh

Usage:
    # Update specific surveys (soft - resumes if cycle exists)
    python scripts/bls/universal_update.py --surveys CU,CE,AP

    # Force update (creates new cycle, starts fresh)
    python scripts/bls/universal_update.py --surveys CU --force

    # Check what needs updating (no API calls)
    python scripts/bls/universal_update.py --check-only

    # Check freshness (compares with BLS API)
    python scripts/bls/universal_update.py --check-freshness

    # Set daily limit
    python scripts/bls/universal_update.py --daily-limit 400
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, date

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from bls.bls_client import BLSClient
from bls import update_manager
from bls import freshness_checker
from database.bls_tracking_models import BLSUpdateCycle, BLSAPIUsageLog
from config import settings


def get_remaining_quota(session, daily_limit: int = 500) -> int:
    """Check how many API requests remaining today"""
    from sqlalchemy import func
    today = date.today()
    used_today = session.query(
        func.sum(BLSAPIUsageLog.requests_used)
    ).filter(
        BLSAPIUsageLog.usage_date == today
    ).scalar() or 0

    return max(0, daily_limit - used_today)


def main():
    parser = argparse.ArgumentParser(
        description="Universal BLS Update Script - Update any BLS survey using cycle system"
    )
    parser.add_argument(
        '--surveys',
        help='Comma-separated list of survey codes (e.g., CU,CE,AP). Default: all'
    )
    parser.add_argument(
        '--daily-limit',
        type=int,
        default=500,
        help='Daily API request limit (default: 500)'
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=datetime.now().year - 1,
        help='Start year for update (default: last year)'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=datetime.now().year,
        help='End year for update (default: current year)'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Only check cycle status, do not update'
    )
    parser.add_argument(
        '--check-freshness',
        action='store_true',
        help='Check if BLS has new data (compares API with database)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force update: create new cycle and start fresh'
    )

    args = parser.parse_args()

    # Parse survey codes
    if args.surveys:
        survey_codes = [s.strip().upper() for s in args.surveys.split(',')]
        # Validate
        invalid = [s for s in survey_codes if s not in update_manager.SURVEYS]
        if invalid:
            print(f"ERROR: Invalid survey codes: {', '.join(invalid)}")
            print(f"Valid codes: {', '.join(sorted(update_manager.SURVEYS.keys()))}")
            return
    else:
        survey_codes = list(update_manager.SURVEYS.keys())

    print("=" * 80)
    print("BLS UNIVERSAL UPDATE TOOL")
    print("=" * 80)

    # Get database session
    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Check daily quota
        remaining_quota = get_remaining_quota(session, args.daily_limit)
        used_today = args.daily_limit - remaining_quota

        print(f"\nDaily Quota Status:")
        print(f"  Requests used today: {used_today} / {args.daily_limit}")
        print(f"  Remaining: {remaining_quota} requests (~{remaining_quota * 50:,} series)")

        # Check freshness mode
        if args.check_freshness:
            print("\n" + "=" * 80)
            print("CHECKING BLS FRESHNESS")
            print("=" * 80)

            client = BLSClient(api_key=settings.api.bls_api_key)

            print("\nChecking each survey (50 series per survey)...")
            results = freshness_checker.check_all_surveys(session, client, survey_codes)

            surveys_with_new_data = []
            for result in results:
                status = "NEW DATA" if result.has_new_data else "Current"
                if result.error:
                    status = f"ERROR: {result.error}"

                print(f"\n  {result.survey_code} - {result.survey_name}")
                print(f"    Status: {status}")
                if result.has_new_data:
                    print(f"    Our latest: {result.our_latest}")
                    print(f"    BLS latest: {result.bls_latest}")
                    print(f"    Series with new data: {result.series_with_new_data}/{result.series_checked}")
                    surveys_with_new_data.append(result.survey_code)

            print("\n" + "=" * 80)
            if surveys_with_new_data:
                print(f"SURVEYS WITH NEW DATA: {', '.join(surveys_with_new_data)}")
                print(f"\nTo update these surveys:")
                print(f"  python scripts/bls/universal_update.py --surveys {','.join(surveys_with_new_data)}")
            else:
                print("ALL SURVEYS ARE CURRENT - No new data available from BLS")
            print("=" * 80)
            return

        # Check cycle status
        print(f"\nCycle Status:")

        survey_info = []
        total_need_update = 0
        total_requests_needed = 0

        for survey_code in survey_codes:
            status = update_manager.get_survey_status(session, survey_code)

            if status['has_current_cycle']:
                remaining = status['total_series'] - status['series_updated']
                requests_needed = (remaining + 49) // 50

                cycle_status = "complete" if status['is_complete'] else f"in progress ({status['series_updated']}/{status['total_series']})"
                print(f"  {survey_code:<3}: Cycle #{status['cycle_id']} - {cycle_status}")

                if not status['is_complete']:
                    survey_info.append({
                        'code': survey_code,
                        'series_remaining': remaining,
                        'requests': requests_needed,
                        'cycle_id': status['cycle_id'],
                        'is_new_cycle': False,
                    })
                    total_need_update += remaining
                    total_requests_needed += requests_needed
            else:
                requests_needed = (status['total_series'] + 49) // 50
                print(f"  {survey_code:<3}: No current cycle ({status['total_series']:,} series)")

                survey_info.append({
                    'code': survey_code,
                    'series_remaining': status['total_series'],
                    'requests': requests_needed,
                    'cycle_id': None,
                    'is_new_cycle': True,
                })
                total_need_update += status['total_series']
                total_requests_needed += requests_needed

        print(f"\n  Total: {total_need_update:,} series to update (~{total_requests_needed} requests)")

        if args.check_only:
            print("\n" + "=" * 80)
            print("CHECK COMPLETE (--check-only mode)")
            print("=" * 80)
            return

        if total_need_update == 0:
            print("\n" + "=" * 80)
            print("ALL CYCLES COMPLETE!")
            print("=" * 80)
            print("\nNo updates needed. All current cycles are complete.")
            print("Use --force to start new cycles, or --check-freshness to see if BLS has new data.")
            return

        # Check quota
        if total_requests_needed > remaining_quota:
            print(f"\n" + "!" * 80)
            print(f"WARNING: Need {total_requests_needed} requests but only {remaining_quota} remaining today")
            print(f"!" * 80)
            print(f"\nWill update as many as possible within quota limit.")
            print(f"Remaining series will be updated in next run.")

        # Confirm
        mode = "FORCE UPDATE (new cycles)" if args.force else "SOFT UPDATE (resume cycles)"
        print(f"\n" + "-" * 80)
        print(f"Mode: {mode}")
        response = input("Continue with updates? (Y/N): ")
        if response.upper() != 'Y':
            print("Update cancelled.")
            return
        print("-" * 80)

        # Get BLS client
        client = BLSClient(api_key=settings.api.bls_api_key)

        # Update surveys
        print(f"\nUpdating surveys...")
        total_observations = 0
        total_series_updated = 0
        total_requests_used = 0

        for info in survey_info:
            if info['series_remaining'] == 0:
                continue

            cycle_id = info['cycle_id']
            action_msg = 'Creating new cycle' if args.force or info['is_new_cycle'] else f'Resuming cycle #{cycle_id}'
            print(f"\n[{info['code']}] {action_msg}")
            print(f"  Series to update: {info['series_remaining']:,}")

            try:
                # Define progress callback
                def progress_callback(progress):
                    pct = (progress.series_updated / progress.total_series * 100) if progress.total_series > 0 else 0
                    print(f"  Progress: {progress.series_updated:,}/{progress.total_series:,} ({pct:.1f}%)")

                result = update_manager.update_survey(
                    survey_code=info['code'],
                    session=session,
                    client=client,
                    force=args.force,
                    start_year=args.start_year,
                    end_year=args.end_year,
                    progress_callback=progress_callback
                )

                total_observations += result.observations_added
                total_series_updated += result.series_updated - (0 if info['is_new_cycle'] else (update_manager.get_survey_status(session, info['code'])['series_updated'] - result.series_updated))
                total_requests_used += result.requests_used

                if result.completed:
                    print(f"  [OK] Cycle complete: {result.series_updated:,} series")
                else:
                    print(f"  [PAUSED] {result.series_updated:,}/{result.total_series:,} series updated")

                if result.errors:
                    print(f"  Errors: {len(result.errors)}")
                    for err in result.errors[:3]:  # Show first 3 errors
                        print(f"    - {err[:100]}")

            except KeyboardInterrupt:
                print(f"\n  Update interrupted by user")
                break

            except Exception as e:
                print(f"  ERROR: {e}")
                continue

        # Summary
        print("\n" + "=" * 80)
        print("UPDATE COMPLETE!")
        print("=" * 80)
        print(f"  Series updated: {total_series_updated:,}")
        print(f"  Observations: {total_observations:,}")
        print(f"  API requests used: {total_requests_used}")
        print(f"  Remaining today: {get_remaining_quota(session, args.daily_limit)} requests")
        print("=" * 80)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
