#!/usr/bin/env python3
"""
Check BLS API quota usage

Shows today's API usage and remaining quota
"""
import sys
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from database.bls_tracking_models import BLSAPIUsageLog
from config import settings

def main():
    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    today = date.today()
    daily_limit = 500

    # Get today's usage
    usage = session.query(
        func.sum(BLSAPIUsageLog.requests_used).label('total_requests'),
        func.sum(BLSAPIUsageLog.series_count).label('total_series')
    ).filter(
        BLSAPIUsageLog.usage_date == today
    ).first()

    requests_used = usage.total_requests or 0
    series_updated = usage.total_series or 0
    remaining = daily_limit - requests_used
    percentage = (requests_used / daily_limit * 100) if daily_limit > 0 else 0

    print("=" * 80)
    print(f"BLS API QUOTA STATUS - {today}")
    print("=" * 80)
    print(f"\nRequests:")
    print(f"  Used today: {requests_used} / {daily_limit} ({percentage:.1f}%)")
    print(f"  Remaining:  {remaining} requests (~{remaining * 50:,} series)")
    print(f"\nSeries updated today: {series_updated:,}")

    # Show breakdown by survey
    breakdown = session.query(
        BLSAPIUsageLog.survey_code,
        func.sum(BLSAPIUsageLog.requests_used).label('requests'),
        func.sum(BLSAPIUsageLog.series_count).label('series')
    ).filter(
        BLSAPIUsageLog.usage_date == today
    ).group_by(
        BLSAPIUsageLog.survey_code
    ).all()

    if breakdown:
        print(f"\nBreakdown by survey:")
        for row in breakdown:
            survey = row.survey_code or 'Unknown'
            print(f"  {survey:<3}: {row.requests:>4} requests, {row.series:>7,} series")

    print("=" * 80)

    session.close()

if __name__ == "__main__":
    main()
