#!/usr/bin/env python3
"""
Check BLS Data Freshness via Sentinel System

This script checks if BLS has published new data by comparing current values
of sentinel series against stored baseline values.

For each survey:
1. Fetch current values for 50 sentinel series (1 API request)
2. Compare with stored values
3. Detect if any sentinels changed
4. Update freshness status
5. Mark surveys needing full updates

This is much more efficient than checking all series directly.

Usage:
    python scripts/bls/check_freshness.py              # Check all surveys
    python scripts/bls/check_freshness.py --surveys CU,CE  # Check specific surveys
    python scripts/bls/check_freshness.py --verbose    # Show detailed changes
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, date, timedelta, UTC
from typing import Dict, Any, List, Tuple
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from bls.bls_client import BLSClient
from database.bls_tracking_models import BLSSurveySentinel, BLSSurveyFreshness, BLSAPIUsageLog
from config import settings

# Survey names for display
SURVEY_NAMES = {
    'AP': 'Average Price Data',
    'CU': 'Consumer Price Index',
    'LA': 'Local Area Unemployment',
    'CE': 'Current Employment Statistics',
    'PC': 'Producer Price Index - Commodity',
    'WP': 'Producer Price Index',
    'SM': 'State and Metro Area Employment',
    'JT': 'JOLTS',
    'EC': 'Employment Cost Index',
    'OE': 'Occupational Employment',
    'PR': 'Major Sector Productivity',
    'TU': 'American Time Use Survey',
    'IP': 'Industry Productivity',
    'LN': 'Labor Force Statistics',
    'CW': 'CPI - Urban Wage Earners',
    'SU': 'Chained CPI',
    'BD': 'Business Employment Dynamics',
    'EI': 'Import/Export Price Indexes',
}


def get_sentinels_for_survey(session, survey_code: str) -> List[BLSSurveySentinel]:
    """Get all sentinel series for a survey"""
    sentinels = session.query(BLSSurveySentinel).filter(
        BLSSurveySentinel.survey_code == survey_code
    ).order_by(
        BLSSurveySentinel.sentinel_order
    ).all()

    return sentinels


def fetch_current_sentinel_values(client: BLSClient, series_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch current values for sentinel series from BLS API

    Returns dict mapping series_id -> latest observation data
    """
    current_year = datetime.now().year

    # Fetch last 2 years to ensure we get latest
    rows = client.get_many(
        series_ids,
        start_year=current_year - 1,
        end_year=current_year,
        calculations=False,
        catalog=False,
        as_dataframe=False
    )

    # Find latest observation for each series
    latest_by_series: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        series_id = row['series_id']  # type: ignore
        year = row['year']  # type: ignore
        period = row['period']  # type: ignore

        # Create sortable key
        sort_key = (year, period)

        if series_id not in latest_by_series:
            latest_by_series[series_id] = row  # type: ignore
        else:
            current_key = (latest_by_series[series_id]['year'], latest_by_series[series_id]['period'])
            if sort_key > current_key:
                latest_by_series[series_id] = row  # type: ignore

    return latest_by_series


def compare_sentinel_values(sentinel: BLSSurveySentinel, current_data: Dict[str, Any],
                           verbose: bool = False) -> Tuple[bool, str]:
    """
    Compare stored sentinel values with current data

    Returns (changed: bool, reason: str)
    """
    if not current_data:
        return False, "no_current_data"

    # Compare year and period
    if sentinel.last_year != current_data.get('year'):
        if verbose:
            return True, f"year changed: {sentinel.last_year} -> {current_data.get('year')}"
        return True, "year_changed"

    if sentinel.last_period != current_data.get('period'):
        if verbose:
            return True, f"period changed: {sentinel.last_period} -> {current_data.get('period')}"
        return True, "period_changed"

    # Compare value (handle decimal precision)
    current_value = current_data.get('value')
    if current_value is not None and sentinel.last_value is not None:
        # Convert to Decimal for precise comparison
        current_decimal = Decimal(str(current_value))
        stored_decimal = Decimal(str(sentinel.last_value))

        if current_decimal != stored_decimal:
            if verbose:
                return True, f"value changed: {stored_decimal} -> {current_decimal}"
            return True, "value_changed"

    # Compare footnotes
    current_footnotes = current_data.get('footnotes', '')
    if sentinel.last_footnotes != current_footnotes:
        if verbose:
            return True, f"footnotes changed: '{sentinel.last_footnotes}' -> '{current_footnotes}'"
        return True, "footnotes_changed"

    return False, "no_change"


def update_sentinel_after_check(session, sentinel: BLSSurveySentinel,
                                current_data: Dict[str, Any], changed: bool):
    """Update sentinel record after check"""
    now = datetime.now()

    updates = {
        'last_checked_at': now,
        'check_count': sentinel.check_count + 1,
        'updated_at': now,
    }

    if changed:
        # Update stored values to current
        updates.update({
            'last_value': current_data.get('value'),
            'last_year': current_data.get('year'),
            'last_period': current_data.get('period'),
            'last_footnotes': current_data.get('footnotes', ''),
            'last_changed_at': now,
            'change_count': sentinel.change_count + 1,
        })

    # Update in database
    stmt = update(BLSSurveySentinel).where(
        BLSSurveySentinel.survey_code == sentinel.survey_code,
        BLSSurveySentinel.series_id == sentinel.series_id
    ).values(**updates)

    session.execute(stmt)


def update_freshness_status(session, survey_code: str, sentinels_changed: int,
                           sentinels_total: int, freshness: BLSSurveyFreshness):
    """Update survey freshness status after check"""
    now = datetime.now()

    updates = {
        'last_sentinel_check': now,
        'sentinels_changed': sentinels_changed,
        'sentinels_total': sentinels_total,
        'total_checks': freshness.total_checks + 1,
        'updated_at': now,
    }

    # If sentinels changed, mark as needing update
    if sentinels_changed > 0:
        updates.update({
            'needs_full_update': True,
            'last_bls_update_detected': now,
            'total_updates_detected': freshness.total_updates_detected + 1,
        })

        # Calculate update frequency if we have previous detection
        if freshness.last_bls_update_detected:
            days_since_last = (now - freshness.last_bls_update_detected).days
            if freshness.bls_update_frequency_days:
                # Moving average
                current_freq = float(freshness.bls_update_frequency_days)
                new_freq = (current_freq * 0.7) + (days_since_last * 0.3)
                updates['bls_update_frequency_days'] = round(new_freq, 2)
            else:
                updates['bls_update_frequency_days'] = days_since_last

    # Update in database
    stmt = update(BLSSurveyFreshness).where(
        BLSSurveyFreshness.survey_code == survey_code
    ).values(**updates)

    session.execute(stmt)


def check_survey_freshness(session, client: BLSClient, survey_code: str,
                          verbose: bool = False) -> Dict[str, Any]:
    """
    Check freshness for a single survey

    Returns dict with check results
    """
    # Get sentinels
    sentinels = get_sentinels_for_survey(session, survey_code)

    if not sentinels:
        return {
            'survey_code': survey_code,
            'status': 'no_sentinels',
            'message': 'No sentinels configured for this survey',
        }

    series_ids = [s.series_id for s in sentinels]

    # Fetch current values from BLS
    print(f"  Fetching {len(series_ids)} sentinel values from BLS API...")
    current_values = fetch_current_sentinel_values(client, series_ids)

    # Compare each sentinel
    changes_detected = 0
    changes_detail = []

    for sentinel in sentinels:
        current_data = current_values.get(sentinel.series_id)
        changed, reason = compare_sentinel_values(sentinel, current_data, verbose=verbose)

        if changed:
            changes_detected += 1
            changes_detail.append({
                'series_id': sentinel.series_id,
                'order': sentinel.sentinel_order,
                'reason': reason,
            })

        # Update sentinel record
        update_sentinel_after_check(session, sentinel, current_data or {}, changed)

    session.commit()

    # Get freshness record
    freshness = session.query(BLSSurveyFreshness).filter(
        BLSSurveyFreshness.survey_code == survey_code
    ).first()

    if not freshness:
        # Create new freshness record
        freshness = BLSSurveyFreshness(
            survey_code=survey_code,
            last_sentinel_check=datetime.now(),
            sentinels_total=len(sentinels),
            total_checks=0,
            total_updates_detected=0,
        )
        session.add(freshness)
        session.commit()
        session.refresh(freshness)

    # Update freshness status
    update_freshness_status(session, survey_code, changes_detected, len(sentinels), freshness)
    session.commit()

    return {
        'survey_code': survey_code,
        'status': 'changed' if changes_detected > 0 else 'unchanged',
        'sentinels_checked': len(sentinels),
        'sentinels_changed': changes_detected,
        'changes_detail': changes_detail,
        'needs_update': changes_detected > 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Check BLS data freshness via sentinel system"
    )
    parser.add_argument(
        '--surveys',
        help='Comma-separated list of survey codes (e.g., CU,CE,AP). Default: all'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed change information'
    )
    parser.add_argument(
        '--skip-recent',
        type=int,
        default=6,
        help='Skip surveys checked within N hours (default: 6)'
    )

    args = parser.parse_args()

    # Parse survey codes
    if args.surveys:
        survey_codes = [s.strip().upper() for s in args.surveys.split(',')]
    else:
        survey_codes = list(SURVEY_NAMES.keys())

    print("=" * 80)
    print("BLS FRESHNESS CHECK")
    print("=" * 80)
    print(f"\nChecking if BLS has published new data via sentinel system")
    print(f"Surveys to check: {', '.join(survey_codes)}")

    # Get database session
    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get BLS client
        client = BLSClient(api_key=settings.api.bls_api_key)

        # Filter out recently checked surveys
        if args.skip_recent > 0:
            cutoff = datetime.now() - timedelta(hours=args.skip_recent)
            recently_checked = session.query(BLSSurveyFreshness.survey_code).filter(
                BLSSurveyFreshness.survey_code.in_(survey_codes),
                BLSSurveyFreshness.last_sentinel_check >= cutoff
            ).all()
            recently_checked_codes = [row[0] for row in recently_checked]

            if recently_checked_codes:
                print(f"\nSkipping {len(recently_checked_codes)} surveys checked within {args.skip_recent}h:")
                print(f"  {', '.join(recently_checked_codes)}")
                survey_codes = [s for s in survey_codes if s not in recently_checked_codes]

        if not survey_codes:
            print("\nNo surveys to check (all recently checked)")
            print(f"Use --skip-recent 0 to check anyway")
            return

        print(f"\nChecking {len(survey_codes)} surveys...\n")

        # Check each survey
        results = []
        total_requests = 0

        for survey_code in survey_codes:
            survey_name = SURVEY_NAMES.get(survey_code, survey_code)
            print(f"[{survey_code}] {survey_name}")

            result = check_survey_freshness(session, client, survey_code, verbose=args.verbose)
            results.append(result)

            if result['status'] == 'no_sentinels':
                print(f"  [WARN] {result['message']}")
            elif result['status'] == 'changed':
                print(f"  [CHANGED] {result['sentinels_changed']}/{result['sentinels_checked']} sentinels updated")
                print(f"  --> Survey needs full update!")

                if args.verbose and result['changes_detail']:
                    print(f"  Changes:")
                    for change in result['changes_detail'][:5]:
                        print(f"    - {change['series_id']}: {change['reason']}")
                    if len(result['changes_detail']) > 5:
                        print(f"    ... and {len(result['changes_detail']) - 5} more")
            else:
                print(f"  [OK] No changes detected ({result['sentinels_checked']} sentinels checked)")

            if result['status'] != 'no_sentinels':
                total_requests += 1

            # Record API usage
            if result['status'] != 'no_sentinels':
                usage_log = BLSAPIUsageLog(
                    usage_date=date.today(),
                    requests_used=1,
                    series_count=result['sentinels_checked'],
                    survey_code=survey_code,
                    script_name='check_freshness'
                )
                session.add(usage_log)
                session.commit()

            print()

        # Summary
        surveys_with_changes = [r for r in results if r['status'] == 'changed']
        surveys_unchanged = [r for r in results if r['status'] == 'unchanged']

        print("=" * 80)
        print("FRESHNESS CHECK COMPLETE")
        print("=" * 80)
        print(f"  Surveys checked: {len(results)}")
        print(f"  Surveys with changes: {len(surveys_with_changes)}")
        print(f"  Surveys unchanged: {len(surveys_unchanged)}")
        print(f"  API requests used: {total_requests}")

        if surveys_with_changes:
            print(f"\n  Surveys needing updates:")
            for result in surveys_with_changes:
                survey_name = SURVEY_NAMES.get(result['survey_code'], result['survey_code'])
                print(f"    - {result['survey_code']}: {survey_name}")
                print(f"      {result['sentinels_changed']}/{result['sentinels_checked']} sentinels changed")

        print("\n" + "=" * 80)
        print("Next steps:")
        if surveys_with_changes:
            codes = ','.join([r['survey_code'] for r in surveys_with_changes])
            print(f"  1. Run 'python scripts/bls/show_freshness.py' to view detailed status")
            print(f"  2. Update changed surveys:")
            print(f"     python scripts/bls/universal_update.py --surveys {codes}")
            print(f"     OR")
            print(f"     python scripts/bls/universal_update.py --fresh-only")
        else:
            print("  All surveys are up-to-date!")
            print("  Run again later or use --skip-recent 0 to force recheck")
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
