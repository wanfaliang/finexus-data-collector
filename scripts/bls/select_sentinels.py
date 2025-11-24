#!/usr/bin/env python3
"""
Select Sentinel Series for BLS Freshness Detection

This script intelligently selects 50 representative series for each BLS survey
to act as "sentinels" for detecting when BLS publishes new data.

The selection strategy:
1. National/aggregate series (area_code='0000' or equivalent) - 20 series
2. Diverse geographic coverage - 20 series
3. Random sample for edge cases - 10 series

After selection, fetches current values from BLS API to establish baseline.

Usage:
    python scripts/bls/select_sentinels.py              # Select for all surveys
    python scripts/bls/select_sentinels.py --surveys CU,CE  # Select specific surveys
    python scripts/bls/select_sentinels.py --force      # Re-select even if exists
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, UTC
from typing import List, Dict, Any, Tuple
import random

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from bls.bls_client import BLSClient
from database.bls_tracking_models import BLSSurveySentinel, BLSSurveyFreshness
from database.bls_models import (
    APSeries, CUSeries, LASeries, CESeries, PCSeries, WPSeries, SMSeries, JTSeries,
    ECSeries, OESeries, PRSeries, TUSeries, IPSeries, LNSeries, CWSeries, SUSeries,
    BDSeries, EISeries
)
from config import settings

# Survey configuration
SURVEYS = {
    'AP': (APSeries, 'area_code'),
    'CU': (CUSeries, 'area_code'),
    'LA': (LASeries, 'area_code'),
    'CE': (CESeries, 'industry_code'),
    'PC': (PCSeries, 'product_code'),  # Fixed: product_code not item_code
    'WP': (WPSeries, 'item_code'),
    'SM': (SMSeries, 'area_code'),
    'JT': (JTSeries, 'area_code'),
    'EC': (ECSeries, 'group_code'),    # Fixed: group_code not occupation_code
    'OE': (OESeries, 'area_code'),
    'PR': (PRSeries, 'sector_code'),
    'TU': (TUSeries, 'actcode_code'),  # Fixed: actcode_code not activity_code
    'IP': (IPSeries, 'sector_code'),
    'LN': (LNSeries, 'lfst_code'),     # Fixed: lfst_code (labor force status) not area_code
    'CW': (CWSeries, 'area_code'),
    'SU': (SUSeries, 'area_code'),
    'BD': (BDSeries, 'industry_code'),
    'EI': (EISeries, 'index_code'),    # Fixed: index_code not item_code
}

# National/aggregate codes by survey type
NATIONAL_CODES = {
    'area_code': ['0000', '00000'],   # National area
    'industry_code': ['00000000'],     # All industries
    'item_code': ['0'],                # All items (WP)
    'product_code': ['0'],             # All products (PC)
    'group_code': ['0'],               # All groups (EC)
    'sector_code': ['00'],             # Business sector (PR, IP)
    'actcode_code': ['01'],            # All activities (TU)
    'index_code': ['0'],               # All indexes (EI)
    'lfst_code': ['00'],               # All labor force statuses (LN)
}


def get_national_series(session, series_model, grouping_column: str, limit: int = 20) -> List[str]:
    """
    Get national/aggregate series for a survey

    These are usually the most important and reliable series
    """
    national_codes = NATIONAL_CODES.get(grouping_column, [])

    if not national_codes:
        # No specific national codes, try to get series with simplest codes
        series = session.query(series_model.series_id).filter(
            series_model.is_active == True
        ).order_by(
            func.length(getattr(series_model, grouping_column))
        ).limit(limit).all()
        return [row[0] for row in series]

    # Get series with national codes
    column = getattr(series_model, grouping_column)
    series = session.query(series_model.series_id).filter(
        series_model.is_active == True,
        column.in_(national_codes)
    ).limit(limit).all()

    result = [row[0] for row in series]

    # If we don't have enough, get more with shortest codes
    if len(result) < limit:
        additional = session.query(series_model.series_id).filter(
            series_model.is_active == True,
            series_model.series_id.notin_(result)
        ).order_by(
            func.length(column)
        ).limit(limit - len(result)).all()
        result.extend([row[0] for row in additional])

    return result


def get_diverse_geographic_series(session, series_model, grouping_column: str,
                                  exclude: List[str], limit: int = 20) -> List[str]:
    """
    Get geographically diverse series

    Samples from different regions/categories to ensure coverage
    """
    column = getattr(series_model, grouping_column)

    # Get distinct grouping values (excluding already selected)
    distinct_values = session.query(column).filter(
        series_model.is_active == True,
        series_model.series_id.notin_(exclude) if exclude else True
    ).distinct().all()

    distinct_values = [row[0] for row in distinct_values if row[0]]

    if not distinct_values:
        return []

    # Sample diverse grouping values
    num_groups = min(limit, len(distinct_values))
    sampled_groups = random.sample(distinct_values, num_groups)

    # Get one series from each sampled group
    result = []
    for group_value in sampled_groups:
        series = session.query(series_model.series_id).filter(
            series_model.is_active == True,
            column == group_value,
            series_model.series_id.notin_(exclude + result) if (exclude or result) else True
        ).limit(1).first()

        if series:
            result.append(series[0])

    return result


def get_random_sample(session, series_model, exclude: List[str], limit: int = 10) -> List[str]:
    """
    Get random sample of series for edge case coverage
    """
    series = session.query(series_model.series_id).filter(
        series_model.is_active == True,
        series_model.series_id.notin_(exclude) if exclude else True
    ).order_by(
        func.random()
    ).limit(limit).all()

    return [row[0] for row in series]


def select_sentinels_for_survey(session, survey_code: str, series_model,
                                grouping_column: str, force: bool = False) -> Tuple[List[str], int]:
    """
    Select 50 sentinel series for a survey

    Returns tuple of (selected series IDs, total series count)
    """
    # Check if sentinels already exist
    existing_count = session.query(func.count(BLSSurveySentinel.series_id)).filter(
        BLSSurveySentinel.survey_code == survey_code
    ).scalar()

    if existing_count > 0 and not force:
        print(f"  Sentinels already exist for {survey_code} ({existing_count} series)")
        print(f"  Use --force to re-select")
        return ([], 0)

    print(f"\n[{survey_code}] Selecting sentinel series...")

    # Get total count of active series in this survey
    total_series_count = session.query(func.count(series_model.series_id)).filter(
        series_model.is_active == True
    ).scalar() or 0
    print(f"  Total active series in survey: {total_series_count}")

    # Step 1: Get national/aggregate series (20)
    national_series = get_national_series(session, series_model, grouping_column, limit=20)
    print(f"  Selected {len(national_series)} national/aggregate series")

    # Step 2: Get geographically diverse series (20)
    diverse_series = get_diverse_geographic_series(
        session, series_model, grouping_column,
        exclude=national_series, limit=20
    )
    print(f"  Selected {len(diverse_series)} geographically diverse series")

    # Step 3: Get random sample (10)
    random_series = get_random_sample(
        session, series_model,
        exclude=national_series + diverse_series, limit=10
    )
    print(f"  Selected {len(random_series)} random series for coverage")

    # Combine all selections
    all_sentinels = national_series + diverse_series + random_series

    # If we don't have 50, pad with more random
    if len(all_sentinels) < 50:
        additional_needed = 50 - len(all_sentinels)
        additional = get_random_sample(session, series_model, exclude=all_sentinels, limit=additional_needed)
        all_sentinels.extend(additional)
        print(f"  Added {len(additional)} additional random series to reach 50")

    # Trim to exactly 50
    all_sentinels = all_sentinels[:50]

    print(f"  Total sentinels selected: {len(all_sentinels)}")

    return (all_sentinels, total_series_count)


def fetch_initial_values(client: BLSClient, series_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch current values for sentinel series from BLS API

    Returns dict mapping series_id -> latest observation data
    """
    current_year = datetime.now().year

    # Fetch last 2 years of data to ensure we get latest
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

        # Create sortable key (year + period)
        sort_key = (year, period)

        if series_id not in latest_by_series:
            latest_by_series[series_id] = row  # type: ignore
        else:
            current_key = (latest_by_series[series_id]['year'], latest_by_series[series_id]['period'])
            if sort_key > current_key:
                latest_by_series[series_id] = row  # type: ignore

    return latest_by_series


def store_sentinels(session, survey_code: str, series_ids: List[str],
                   initial_values: Dict[str, Dict[str, Any]], total_series_count: int, force: bool = False):
    """
    Store sentinel series and their initial values in database
    """
    now = datetime.now()

    # Delete existing sentinels if force
    if force:
        session.query(BLSSurveySentinel).filter(
            BLSSurveySentinel.survey_code == survey_code
        ).delete()
        session.commit()

    # Insert sentinels
    sentinels_data = []
    for order, series_id in enumerate(series_ids, 1):
        latest = initial_values.get(series_id)

        sentinel = {
            'survey_code': survey_code,
            'series_id': series_id,
            'sentinel_order': order,
            'selection_reason': 'auto_selected',
            'last_value': latest.get('value') if latest else None,
            'last_year': latest.get('year') if latest else None,
            'last_period': latest.get('period') if latest else None,
            'last_footnotes': latest.get('footnotes') if latest else None,
            'last_checked_at': now,
            'last_changed_at': None,
            'check_count': 1,
            'change_count': 0,
            'created_at': now,
            'updated_at': now,
        }
        sentinels_data.append(sentinel)

    # Bulk insert
    stmt = insert(BLSSurveySentinel).values(sentinels_data)
    session.execute(stmt)
    session.commit()

    print(f"  [OK] Stored {len(sentinels_data)} sentinels in database")

    # Update or create freshness record
    freshness_stmt = insert(BLSSurveyFreshness).values({
        'survey_code': survey_code,
        'last_sentinel_check': now,
        'sentinels_changed': 0,
        'sentinels_total': len(sentinels_data),
        'series_total_count': total_series_count,
        'needs_full_update': False,
        'total_checks': 1,
        'created_at': now,
        'updated_at': now,
    })
    freshness_stmt = freshness_stmt.on_conflict_do_update(
        index_elements=['survey_code'],
        set_={
            'sentinels_total': len(sentinels_data),
            'series_total_count': total_series_count,
            'updated_at': now,
        }
    )
    session.execute(freshness_stmt)
    session.commit()

    print(f"  [OK] Updated freshness tracking record")


def main():
    parser = argparse.ArgumentParser(
        description="Select sentinel series for BLS freshness detection"
    )
    parser.add_argument(
        '--surveys',
        help='Comma-separated list of survey codes (e.g., CU,CE,AP). Default: all'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-select sentinels even if they already exist'
    )
    parser.add_argument(
        '--no-fetch',
        action='store_true',
        help='Skip fetching initial values from BLS API (for testing)'
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
    print("BLS SENTINEL SELECTION TOOL")
    print("=" * 80)
    print(f"\nSelecting 50 sentinel series for each survey to enable freshness detection")
    print(f"Surveys to process: {', '.join(survey_codes)}")
    print(f"Mode: {'FORCE (re-select)' if args.force else 'Normal (skip existing)'}")

    # Get database session
    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Get BLS client (if needed)
        client = None if args.no_fetch else BLSClient(api_key=settings.api.bls_api_key)

        total_selected = 0
        total_requests = 0

        for survey_code in survey_codes:
            series_model, grouping_column = SURVEYS[survey_code]

            # Select sentinels
            selected_series, total_series_count = select_sentinels_for_survey(
                session, survey_code, series_model, grouping_column, force=args.force
            )

            if not selected_series:
                continue

            # Fetch initial values
            initial_values = {}
            if not args.no_fetch and client:
                print(f"  Fetching initial values from BLS API...")
                initial_values = fetch_initial_values(client, selected_series)
                total_requests += 1
                print(f"  Retrieved values for {len(initial_values)} series")

            # Store in database
            store_sentinels(session, survey_code, selected_series, initial_values, total_series_count, force=args.force)
            total_selected += len(selected_series)

        # Summary
        print("\n" + "=" * 80)
        print("SENTINEL SELECTION COMPLETE!")
        print("=" * 80)
        print(f"  Surveys processed: {len([s for s in survey_codes if s in SURVEYS])}")
        print(f"  Total sentinels: {total_selected}")
        print(f"  API requests used: {total_requests}")
        print("\n" + "=" * 80)
        print("Next steps:")
        print("  1. Run 'python scripts/bls/check_freshness.py' to check for BLS updates")
        print("  2. Run 'python scripts/bls/show_freshness.py' to view status")
        print("  3. Use 'python scripts/bls/universal_update.py --fresh-only' to update")
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
