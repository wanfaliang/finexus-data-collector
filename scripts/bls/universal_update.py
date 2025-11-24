#!/usr/bin/env python3
"""
Universal BLS Update Script

Updates any BLS survey series intelligently:
- Checks which series need updates
- Tracks daily API quota usage
- Skips already-current series
- Can resume across multiple runs
- Records all activity

Usage:
    # Update all surveys (respects daily limit)
    python scripts/bls/universal_update.py

    # Update specific surveys
    python scripts/bls/universal_update.py --surveys CU,CE,AP

    # Set daily limit
    python scripts/bls/universal_update.py --daily-limit 400

    # Check status only (no updates)
    python scripts/bls/universal_update.py --check-only

    # Force update even if marked current
    python scripts/bls/universal_update.py --surveys CU --force

    # Update with specific year range
    python scripts/bls/universal_update.py --surveys BD --start-year 2024

    # Update only surveys with detected freshness changes
    python scripts/bls/universal_update.py --fresh-only
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta, UTC
from typing import Any, Dict, List, Set
from collections import defaultdict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from bls.bls_client import BLSClient
from database.bls_tracking_models import BLSSeriesUpdateStatus, BLSAPIUsageLog, BLSSurveyFreshness
from database.bls_models import (
    APSeries, APData, CUSeries, CUData, LASeries, LAData, CESeries, CEData,
    PCSeries, PCData, WPSeries, WPData, SMSeries, SMData, JTSeries, JTData,
    ECSeries, ECData, OESeries, OEData, PRSeries, PRData, TUSeries, TUData,
    IPSeries, IPData, LNSeries, LNData, CWSeries, CWData, SUSeries, SUData,
    BDSeries, BDData, EISeries, EIData
)
from config import settings

# Survey configuration: code -> (SeriesModel, DataModel, survey_name)
SURVEYS = {
    'AP': (APSeries, APData, 'Average Price Data'),
    'CU': (CUSeries, CUData, 'Consumer Price Index'),
    'LA': (LASeries, LAData, 'Local Area Unemployment'),
    'CE': (CESeries, CEData, 'Current Employment Statistics'),
    'PC': (PCSeries, PCData, 'Producer Price Index - Commodity'),
    'WP': (WPSeries, WPData, 'Producer Price Index'),
    'SM': (SMSeries, SMData, 'State and Metro Area Employment'),
    'JT': (JTSeries, JTData, 'JOLTS'),
    'EC': (ECSeries, ECData, 'Employment Cost Index'),
    'OE': (OESeries, OEData, 'Occupational Employment'),
    'PR': (PRSeries, PRData, 'Major Sector Productivity'),
    'TU': (TUSeries, TUData, 'American Time Use Survey'),
    'IP': (IPSeries, IPData, 'Industry Productivity'),
    'LN': (LNSeries, LNData, 'Labor Force Statistics'),
    'CW': (CWSeries, CWData, 'CPI - Urban Wage Earners'),
    'SU': (SUSeries, SUData, 'Chained CPI'),
    'BD': (BDSeries, BDData, 'Business Employment Dynamics'),
    'EI': (EISeries, EIData, 'Import/Export Price Indexes'),
}


def get_remaining_quota(session, daily_limit: int = 500) -> int:
    """Check how many API requests remaining today"""
    today = date.today()
    used_today = session.query(
        func.sum(BLSAPIUsageLog.requests_used)
    ).filter(
        BLSAPIUsageLog.usage_date == today
    ).scalar() or 0

    remaining = daily_limit - used_today
    return max(0, remaining)


def get_series_needing_update(session, survey_code: str, series_model, data_model,
                             force: bool = False) -> List[str]:
    """
    Find series that need updates

    Returns list of series IDs that either:
    - Are not marked as current
    - Have no status record
    - Are marked current but force=True
    """
    # Get all active series for this survey
    active_series = session.query(series_model.series_id).filter(
        series_model.is_active == True
    ).all()
    active_series_ids = [row[0] for row in active_series]

    if force:
        # Force update all active series
        return active_series_ids

    # Get series marked as current (checked within last 24 hours)
    current_threshold = datetime.now() - timedelta(hours=24)
    current_series = session.query(
        BLSSeriesUpdateStatus.series_id
    ).filter(
        BLSSeriesUpdateStatus.survey_code == survey_code,
        BLSSeriesUpdateStatus.is_current == True,
        BLSSeriesUpdateStatus.last_checked_at >= current_threshold
    ).all()
    current_series_ids = set([row[0] for row in current_series])

    # Return series that need updates
    needs_update = [sid for sid in active_series_ids if sid not in current_series_ids]
    return needs_update


def check_if_series_current(session, series_id: str, data_model,
                           start_year: int, end_year: int) -> bool:
    """
    Check if a series has data up to expected timeframe

    A series is considered current if it has data for the most recent
    expected period (accounting for reporting lag)
    """
    # Get latest data point for this series
    latest = session.query(
        func.max(data_model.year).label('max_year'),
        func.max(data_model.period).label('max_period')
    ).filter(
        data_model.series_id == series_id
    ).first()

    if not latest or not latest.max_year:
        return False  # No data, needs update

    # Simple check: if latest year is >= end_year - 1, consider current
    # (accounts for reporting lag)
    return latest.max_year >= end_year - 1


def update_series_batch(session, client: BLSClient, series_ids: List[str],
                       survey_code: str, data_model,
                       start_year: int, end_year: int) -> Dict[str, Any]:
    """
    Update a batch of series and return statistics
    """
    # Fetch from API
    rows = client.get_many(
        series_ids,
        start_year=start_year,
        end_year=end_year,
        calculations=False,
        catalog=False,
        as_dataframe=False
    )

    # Convert to database format
    data_to_upsert = []
    for row in rows:
        data_to_upsert.append({
            'series_id': row['series_id'], # type: ignore
            'year': row['year'], # type: ignore
            'period': row['period'], # type: ignore
            'value': row['value'], # type: ignore
            'footnote_codes': row.get('footnotes'), # type: ignore
        })

    # Upsert to database
    from sqlalchemy.dialects.postgresql import insert
    if data_to_upsert:
        stmt = insert(data_model).values(data_to_upsert)
        stmt = stmt.on_conflict_do_update(
            index_elements=['series_id', 'year', 'period'],
            set_={
                'value': stmt.excluded.value,
                'footnote_codes': stmt.excluded.footnote_codes,
                'updated_at': datetime.now(UTC),
            }
        )
        session.execute(stmt)
        session.commit()

    # Update status for each series
    now = datetime.now()
    for series_id in series_ids:
        is_current = check_if_series_current(session, series_id, data_model,
                                            start_year, end_year)

        # Upsert status
        stmt = insert(BLSSeriesUpdateStatus).values({
            'series_id': series_id,
            'survey_code': survey_code,
            'last_checked_at': now,
            'last_updated_at': now,
            'is_current': is_current,
        })
        stmt = stmt.on_conflict_do_update(
            index_elements=['series_id'],
            set_={
                'last_checked_at': stmt.excluded.last_checked_at,
                'last_updated_at': stmt.excluded.last_updated_at,
                'is_current': stmt.excluded.is_current,
            }
        )
        session.execute(stmt)

    session.commit()

    return {
        'observations': len(data_to_upsert),
        'series_updated': len(series_ids),
    }


def record_api_usage(session, requests_used: int, series_count: int,
                    survey_code: str, script_name: str = 'universal_update'):
    """Record API usage in log"""
    log = BLSAPIUsageLog(
        usage_date=date.today(),
        requests_used=requests_used,
        series_count=series_count,
        survey_code=survey_code,
        script_name=script_name
    )
    session.add(log)
    session.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Universal BLS Update Script - Update any BLS survey intelligently"
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
        help='Only check status, do not update'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force update even if series marked as current'
    )
    parser.add_argument(
        '--fresh-only',
        action='store_true',
        help='Only update surveys that sentinel system detected as needing updates'
    )

    args = parser.parse_args()

    # Parse survey codes
    if args.surveys:
        survey_codes = [s.strip().upper() for s in args.surveys.split(',')]
        # Validate
        invalid = [s for s in survey_codes if s not in SURVEYS]
        if invalid:
            print(f"ERROR: Invalid survey codes: {', '.join(invalid)}")
            print(f"Valid codes: {', '.join(sorted(SURVEYS.keys()))}")
            return
    else:
        survey_codes = list(SURVEYS.keys())

    print("=" * 80)
    print("BLS UNIVERSAL UPDATE TOOL")
    print("=" * 80)

    # Get database session
    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Filter for fresh-only mode
        if args.fresh_only:
            fresh_surveys = session.query(BLSSurveyFreshness.survey_code).filter(
                BLSSurveyFreshness.survey_code.in_(survey_codes),
                BLSSurveyFreshness.needs_full_update == True
            ).all()
            fresh_survey_codes = [row[0] for row in fresh_surveys]

            if not fresh_survey_codes:
                print("\n" + "=" * 80)
                print("FRESH-ONLY MODE: No surveys need updates")
                print("=" * 80)
                print("\nAll sentinel-monitored surveys are up-to-date!")
                print("Run 'python scripts/bls/check_freshness.py' to check for new data")
                return

            original_count = len(survey_codes)
            survey_codes = fresh_survey_codes
            print(f"\nFRESH-ONLY MODE: Filtering to {len(survey_codes)} of {original_count} surveys with detected updates")

    except Exception as e:
        print(f"\nERROR during fresh-only filtering: {e}")
        session.close()
        raise

    try:
        # Check daily quota
        remaining_quota = get_remaining_quota(session, args.daily_limit)
        used_today = args.daily_limit - remaining_quota

        print(f"\nDaily Quota Status:")
        print(f"  Requests used today: {used_today} / {args.daily_limit}")
        print(f"  Remaining: {remaining_quota} requests (~{remaining_quota * 50:,} series)")

        # Check each survey
        print(f"\nSurvey Status:")

        survey_info = []
        total_need_update = 0
        total_requests_needed = 0

        for survey_code in survey_codes:
            series_model, data_model, survey_name = SURVEYS[survey_code]

            series_needing_update = get_series_needing_update(
                session, survey_code, series_model, data_model, force=args.force
            )

            num_series = len(series_needing_update)
            requests_needed = (num_series + 49) // 50

            if num_series > 0 or args.check_only:
                status = "needs update" if num_series > 0 else "all current"
                print(f"  {survey_code:<3}: {num_series:>7,} series {status:15} ({requests_needed:>4} requests)")

                survey_info.append({
                    'code': survey_code,
                    'series': num_series,
                    'requests': requests_needed,
                    'model_data': data_model,
                })

                total_need_update += num_series
                total_requests_needed += requests_needed

        print(f"\n  Total: {total_need_update:,} series need update (~{total_requests_needed} requests)")

        if args.check_only:
            print("\n" + "=" * 80)
            print("CHECK COMPLETE (--check-only mode)")
            print("=" * 80)
            return

        if total_need_update == 0:
            print("\n" + "=" * 80)
            print("ALL SURVEYS UP-TO-DATE!")
            print("=" * 80)
            print("\nNo updates needed. All series are current.")
            return

        # Check quota
        if total_requests_needed > remaining_quota:
            print(f"\n" + "!" * 80)
            print(f"WARNING: Need {total_requests_needed} requests but only {remaining_quota} remaining today")
            print(f"!" * 80)
            print(f"\nWill update as many as possible within quota limit.")
            print(f"Remaining series will be updated in next run.")

        # Confirm
        print("\n" + "-" * 80)
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

        requests_remaining = remaining_quota

        for info in survey_info:
            if info['series'] == 0:
                continue

            if requests_remaining <= 0:
                print(f"\n[{info['code']}] Skipping - daily quota reached")
                continue

            # Calculate how many series we can update
            max_series_for_quota = requests_remaining * 50
            series_to_update = info['series']

            if series_to_update > max_series_for_quota:
                print(f"\n[{info['code']}] Updating {max_series_for_quota:,} of {series_to_update:,} series (quota limit)")
                series_to_update = max_series_for_quota
            else:
                print(f"\n[{info['code']}] Updating {series_to_update:,} series...")

            series_model, data_model, survey_name = SURVEYS[info['code']]
            series_ids = get_series_needing_update(
                session, info['code'], series_model, data_model, force=args.force
            )[:series_to_update]

            # Mark freshness as update in progress
            from sqlalchemy import update as sql_update
            freshness_update = sql_update(BLSSurveyFreshness).where(
                BLSSurveyFreshness.survey_code == info['code']
            ).values(
                full_update_in_progress=True,
                last_full_update_started=datetime.now(),
                series_total_count=len(series_ids),
                series_updated_count=0
            )
            session.execute(freshness_update)
            session.commit()

            # Update in batches of 50
            failed_batches = 0
            for i in range(0, len(series_ids), 50):
                if requests_remaining <= 0:
                    break

                batch = series_ids[i:i+50]

                try:
                    stats = update_series_batch(
                        session, client, batch, info['code'], data_model,
                        args.start_year, args.end_year
                    )

                    total_observations += stats['observations']
                    total_series_updated += stats['series_updated']
                    total_requests_used += 1
                    requests_remaining -= 1

                    # Record usage
                    record_api_usage(session, 1, len(batch), info['code'])

                    # Update freshness progress
                    freshness_update = sql_update(BLSSurveyFreshness).where(
                        BLSSurveyFreshness.survey_code == info['code']
                    ).values(
                        series_updated_count=total_series_updated
                    )
                    session.execute(freshness_update)
                    session.commit()

                    if i + 50 < len(series_ids):
                        print(f"  Progress: {i + 50:,} / {len(series_ids):,} series...")

                except KeyboardInterrupt:
                    print(f"\n  Update interrupted by user")
                    print(f"  Progress saved: {total_series_updated:,} series updated")
                    break

                except Exception as e:
                    failed_batches += 1
                    print(f"  ERROR in batch: {e}")
                    session.rollback()

                    # Check if it's an API limit error
                    error_str = str(e).lower()
                    if 'quota' in error_str or 'limit' in error_str or 'exceeded' in error_str:
                        print(f"  API limit likely exceeded. Stopping.")
                        print(f"  Progress saved: {total_series_updated:,} series updated successfully")
                        break

                    # For other errors, continue with next batch
                    print(f"  Continuing with next batch...")
                    continue

            # Mark freshness update as complete
            freshness_update = sql_update(BLSSurveyFreshness).where(
                BLSSurveyFreshness.survey_code == info['code']
            ).values(
                full_update_in_progress=False,
                last_full_update_completed=datetime.now(),
                needs_full_update=False  # Clear the needs_update flag
            )
            session.execute(freshness_update)
            session.commit()

            if failed_batches > 0:
                print(f"  [WARN] Complete with {failed_batches} failed batches: {total_series_updated:,} series updated")
            else:
                print(f"  [OK] Complete: {min(len(series_ids), series_to_update):,} series updated")

        # Summary
        print("\n" + "=" * 80)
        print("UPDATE COMPLETE!")
        print("=" * 80)
        print(f"  Series updated: {total_series_updated:,}")
        print(f"  Observations: {total_observations:,}")
        print(f"  API requests used: {total_requests_used}")
        print(f"  Remaining today: {requests_remaining} requests")
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
