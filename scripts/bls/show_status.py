#!/usr/bin/env python3
"""
Show BLS series update status

Displays which surveys have current data and which need updates
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from database.bls_tracking_models import BLSSeriesUpdateStatus
from database.bls_models import (
    APSeries, CUSeries, LASeries, CESeries, PCSeries, WPSeries,
    SMSeries, JTSeries, ECSeries, OESeries, PRSeries, TUSeries,
    IPSeries, LNSeries, CWSeries, SUSeries, BDSeries, EISeries
)
from config import settings

SURVEYS = {
    'AP': (APSeries, 'Average Price Data'),
    'CU': (CUSeries, 'Consumer Price Index'),
    'LA': (LASeries, 'Local Area Unemployment'),
    'CE': (CESeries, 'Current Employment Statistics'),
    'PC': (PCSeries, 'Producer Price Index - Commodity'),
    'WP': (WPSeries, 'Producer Price Index'),
    'SM': (SMSeries, 'State and Metro Area Employment'),
    'JT': (JTSeries, 'JOLTS'),
    'EC': (ECSeries, 'Employment Cost Index'),
    'OE': (OESeries, 'Occupational Employment'),
    'PR': (PRSeries, 'Major Sector Productivity'),
    'TU': (TUSeries, 'American Time Use Survey'),
    'IP': (IPSeries, 'Industry Productivity'),
    'LN': (LNSeries, 'Labor Force Statistics'),
    'CW': (CWSeries, 'CPI - Urban Wage Earners'),
    'SU': (SUSeries, 'Chained CPI'),
    'BD': (BDSeries, 'Business Employment Dynamics'),
    'EI': (EISeries, 'Import/Export Price Indexes'),
}

def main():
    parser = argparse.ArgumentParser(description="Show BLS series update status")
    parser.add_argument(
        '--surveys',
        help='Comma-separated list of survey codes (default: all)'
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Show detailed information including last update times'
    )
    args = parser.parse_args()

    # Parse survey codes
    if args.surveys:
        survey_codes = [s.strip().upper() for s in args.surveys.split(',')]
    else:
        survey_codes = list(SURVEYS.keys())

    engine = create_engine(settings.database.url, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("=" * 80)
    print("BLS SERIES UPDATE STATUS")
    print("=" * 80)

    current_threshold = datetime.now() - timedelta(hours=24)

    for survey_code in survey_codes:
        series_model, survey_name = SURVEYS[survey_code]

        # Get total active series
        total_active = session.query(
            func.count(series_model.series_id)
        ).filter(
            series_model.is_active == True
        ).scalar()

        # Get current series count
        current_count = session.query(
            func.count(BLSSeriesUpdateStatus.series_id)
        ).filter(
            BLSSeriesUpdateStatus.survey_code == survey_code,
            BLSSeriesUpdateStatus.is_current == True,
            BLSSeriesUpdateStatus.last_checked_at >= current_threshold
        ).scalar()

        # Calculate percentage
        if total_active > 0:
            percentage = (current_count / total_active * 100)
            status = "✓" if percentage == 100 else "⚠" if percentage > 50 else "✗"
        else:
            percentage = 0
            status = "-"

        print(f"\n{survey_code} - {survey_name}")
        print(f"  Active series: {total_active:,}")
        print(f"  Current: {current_count:,} ({percentage:.1f}%)")
        print(f"  Need update: {total_active - current_count:,}")

        if args.detailed:
            # Show last update info
            last_update = session.query(
                func.max(BLSSeriesUpdateStatus.last_updated_at)
            ).filter(
                BLSSeriesUpdateStatus.survey_code == survey_code
            ).scalar()

            if last_update:
                print(f"  Last updated: {last_update}")
            else:
                print(f"  Last updated: Never")

    print("\n" + "=" * 80)

    session.close()

if __name__ == "__main__":
    main()
