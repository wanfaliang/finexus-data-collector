#!/usr/bin/env python3
"""
Show BLS Data Freshness Status

Displays the current freshness status for all BLS surveys based on
sentinel monitoring system.

Shows:
- Last BLS update detected
- Last check time
- Whether survey needs update
- Update frequency statistics
- Sentinel change rates

Usage:
    python scripts/bls/show_freshness.py              # Show all surveys
    python scripts/bls/show_freshness.py --surveys CU,CE  # Show specific surveys
    python scripts/bls/show_freshness.py --needs-update   # Show only surveys needing update
    python scripts/bls/show_freshness.py --detail         # Show detailed sentinel info
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from database.bls_tracking_models import BLSSurveySentinel, BLSSurveyFreshness
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


def format_time_ago(dt: Optional[datetime]) -> str:
    """Format datetime as human-readable time ago"""
    if not dt:
        return "Never"

    now = datetime.now()
    diff = now - dt

    if diff.days > 0:
        if diff.days == 1:
            return "1 day ago"
        elif diff.days < 7:
            return f"{diff.days} days ago"
        elif diff.days < 30:
            weeks = diff.days // 7
            return f"{weeks} week{'s' if weeks > 1 else ''} ago"
        elif diff.days < 365:
            months = diff.days // 30
            return f"{months} month{'s' if months > 1 else ''} ago"
        else:
            years = diff.days // 365
            return f"{years} year{'s' if years > 1 else ''} ago"

    hours = diff.seconds // 3600
    if hours > 0:
        return f"{hours} hour{'s' if hours > 1 else ''} ago"

    minutes = diff.seconds // 60
    if minutes > 0:
        return f"{minutes} minute{'s' if minutes > 1 else ''} ago"

    return "Just now"


def format_datetime(dt: Optional[datetime]) -> str:
    """Format datetime for display"""
    if not dt:
        return "Never"
    return dt.strftime("%Y-%m-%d %H:%M")


def show_survey_freshness(session, survey_code: str, detail: bool = False):
    """Display freshness status for a single survey"""
    survey_name = SURVEY_NAMES.get(survey_code, survey_code)

    # Get freshness record
    freshness = session.query(BLSSurveyFreshness).filter(
        BLSSurveyFreshness.survey_code == survey_code
    ).first()

    # Get sentinel count
    sentinel_count = session.query(func.count(BLSSurveySentinel.series_id)).filter(
        BLSSurveySentinel.survey_code == survey_code
    ).scalar() or 0

    print(f"\n{'=' * 80}")
    print(f"{survey_code}: {survey_name}")
    print(f"{'=' * 80}")

    if not freshness and sentinel_count == 0:
        print("  Status: Not configured")
        print("  Run 'python scripts/bls/select_sentinels.py' to set up monitoring")
        return

    if freshness:
        # Status
        if freshness.needs_full_update:
            status = "[NEEDS UPDATE]"
        elif freshness.full_update_in_progress:
            status = "[UPDATING]"
        else:
            status = "[OK] Up-to-date"

        print(f"  Status: {status}")
        print()

        # Last BLS update
        print(f"  Last BLS Update:")
        if freshness.last_bls_update_detected:
            print(f"    Detected: {format_datetime(freshness.last_bls_update_detected)}")
            print(f"              ({format_time_ago(freshness.last_bls_update_detected)})")
        else:
            print(f"    Detected: No changes detected yet")

        # Last check
        print(f"\n  Last Sentinel Check:")
        if freshness.last_sentinel_check:
            print(f"    Time: {format_datetime(freshness.last_sentinel_check)}")
            print(f"          ({format_time_ago(freshness.last_sentinel_check)})")
            print(f"    Changed: {freshness.sentinels_changed}/{freshness.sentinels_total} sentinels")
        else:
            print(f"    Time: Never checked")

        # Update frequency
        if freshness.bls_update_frequency_days:
            print(f"\n  BLS Update Frequency:")
            freq = float(freshness.bls_update_frequency_days)
            if freq < 1:
                print(f"    ~{freq * 24:.1f} hours")
            elif freq < 7:
                print(f"    ~{freq:.1f} days")
            elif freq < 30:
                print(f"    ~{freq / 7:.1f} weeks")
            else:
                print(f"    ~{freq / 30:.1f} months")

        # Statistics
        print(f"\n  Statistics:")
        print(f"    Total checks: {freshness.total_checks}")
        print(f"    Updates detected: {freshness.total_updates_detected}")
        if freshness.total_checks > 0:
            change_rate = (freshness.total_updates_detected / freshness.total_checks) * 100
            print(f"    Change rate: {change_rate:.1f}%")

        # Update progress
        if freshness.full_update_in_progress:
            print(f"\n  Update Progress:")
            print(f"    Started: {format_datetime(freshness.last_full_update_started)}")
            if freshness.series_total_count > 0:
                pct = (freshness.series_updated_count / freshness.series_total_count) * 100
                print(f"    Progress: {freshness.series_updated_count:,}/{freshness.series_total_count:,} ({pct:.1f}%)")
        elif freshness.last_full_update_completed:
            print(f"\n  Last Full Update:")
            print(f"    Completed: {format_datetime(freshness.last_full_update_completed)}")
            print(f"               ({format_time_ago(freshness.last_full_update_completed)})")
            if freshness.series_total_count > 0:
                print(f"    Series updated: {freshness.series_updated_count:,}/{freshness.series_total_count:,}")

    # Sentinel details if requested
    if detail and sentinel_count > 0:
        print(f"\n  Sentinel Details:")
        sentinels = session.query(BLSSurveySentinel).filter(
            BLSSurveySentinel.survey_code == survey_code
        ).order_by(
            BLSSurveySentinel.change_count.desc()
        ).limit(10).all()

        print(f"    Total sentinels: {sentinel_count}")
        print(f"\n    Top 10 most active sentinels:")
        print(f"    {'Series ID':<20} {'Checks':>8} {'Changes':>8} {'Last Changed':<20}")
        print(f"    {'-' * 60}")

        for sentinel in sentinels:
            last_changed = format_time_ago(sentinel.last_changed_at) if sentinel.last_changed_at else "Never"
            print(f"    {sentinel.series_id:<20} {sentinel.check_count:>8} {sentinel.change_count:>8} {last_changed:<20}")


def show_summary_table(session, survey_codes: List[str], needs_update_only: bool = False):
    """Show summary table of all surveys"""
    print("\n" + "=" * 80)
    print("BLS DATA FRESHNESS STATUS")
    print("=" * 80)

    # Table header
    print(f"\n{'Survey':<6} {'Status':<15} {'Last BLS Update':<20} {'Last Check':<20} {'Sentinels':<10}")
    print("-" * 80)

    for survey_code in survey_codes:
        freshness = session.query(BLSSurveyFreshness).filter(
            BLSSurveyFreshness.survey_code == survey_code
        ).first()

        sentinel_count = session.query(func.count(BLSSurveySentinel.series_id)).filter(
            BLSSurveySentinel.survey_code == survey_code
        ).scalar() or 0

        # Skip if filtering for needs_update only
        if needs_update_only and (not freshness or not freshness.needs_full_update):
            continue

        # Status
        if not freshness:
            status = "Not setup"
            last_update = "N/A"
            last_check = "N/A"
            sentinels = f"{sentinel_count}/50"
        else:
            if freshness.needs_full_update:
                status = "[NEEDS UPDATE]"
            elif freshness.full_update_in_progress:
                pct = 0
                if freshness.series_total_count > 0:
                    pct = (freshness.series_updated_count / freshness.series_total_count) * 100
                status = f"[UPDATING {pct:.0f}%]"
            else:
                status = "[OK] Current"

            last_update = format_time_ago(freshness.last_bls_update_detected)
            last_check = format_time_ago(freshness.last_sentinel_check)
            sentinels = f"{freshness.sentinels_total}/50"

        print(f"{survey_code:<6} {status:<15} {last_update:<20} {last_check:<20} {sentinels:<10}")


def main():
    parser = argparse.ArgumentParser(
        description="Show BLS data freshness status"
    )
    parser.add_argument(
        '--surveys',
        help='Comma-separated list of survey codes (e.g., CU,CE,AP). Default: all'
    )
    parser.add_argument(
        '--needs-update',
        action='store_true',
        help='Show only surveys that need updates'
    )
    parser.add_argument(
        '--detail',
        action='store_true',
        help='Show detailed information including sentinel details'
    )

    args = parser.parse_args()

    # Parse survey codes
    if args.surveys:
        survey_codes = [s.strip().upper() for s in args.surveys.split(',')]
    else:
        survey_codes = list(SURVEY_NAMES.keys())

    # Get database session
    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Show summary table first
        show_summary_table(session, survey_codes, needs_update_only=args.needs_update)

        # Show detailed view if requested
        if args.detail:
            for survey_code in survey_codes:
                # Skip if filtering for needs_update only
                if args.needs_update:
                    freshness = session.query(BLSSurveyFreshness).filter(
                        BLSSurveyFreshness.survey_code == survey_code
                    ).first()
                    if not freshness or not freshness.needs_full_update:
                        continue

                show_survey_freshness(session, survey_code, detail=True)

        # Action recommendations
        needs_update = session.query(BLSSurveyFreshness).filter(
            BLSSurveyFreshness.survey_code.in_(survey_codes),
            BLSSurveyFreshness.needs_full_update == True
        ).all()

        if needs_update:
            print("\n" + "=" * 80)
            print("RECOMMENDED ACTIONS")
            print("=" * 80)
            codes = ','.join([f.survey_code for f in needs_update])
            print(f"\n{len(needs_update)} survey(s) need updates:")
            print(f"\n  Update all:")
            print(f"    python scripts/bls/universal_update.py --surveys {codes}")
            print(f"\n  Or update only fresh surveys:")
            print(f"    python scripts/bls/universal_update.py --fresh-only")
        else:
            print("\n" + "=" * 80)
            print("All monitored surveys are up-to-date!")
            print("=" * 80)

        # Setup recommendations
        not_setup = []
        for survey_code in survey_codes:
            count = session.query(func.count(BLSSurveySentinel.series_id)).filter(
                BLSSurveySentinel.survey_code == survey_code
            ).scalar() or 0
            if count == 0:
                not_setup.append(survey_code)

        if not_setup:
            print(f"\n{len(not_setup)} survey(s) not set up for monitoring:")
            print(f"  {', '.join(not_setup)}")
            print(f"\nTo enable monitoring:")
            print(f"  python scripts/bls/select_sentinels.py --surveys {','.join(not_setup)}")

        print()

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
