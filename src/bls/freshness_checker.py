"""
BLS Freshness Checker

Simple, stateless freshness checking for BLS surveys.
Compares BLS API data with local database to determine if new data is available.

No persistent sentinel tracking - just on-the-fly comparison.
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from sqlalchemy import func
from sqlalchemy.orm import Session

try:
    from src.bls.bls_client import BLSClient
    from src.database.bls_models import (
        APSeries, APData, CUSeries, CUData, LASeries, LAData, CESeries, CEData,
        PCSeries, PCData, WPSeries, WPData, SMSeries, SMData, JTSeries, JTData,
        ECSeries, ECData, OESeries, OEData, PRSeries, PRData, TUSeries, TUData,
        IPSeries, IPData, LNSeries, LNData, CWSeries, CWData, SUSeries, SUData,
        BDSeries, BDData, EISeries, EIData
    )
    from src.bls.update_manager import record_api_usage
except ImportError:
    from bls.bls_client import BLSClient
    from database.bls_models import (
        APSeries, APData, CUSeries, CUData, LASeries, LAData, CESeries, CEData,
        PCSeries, PCData, WPSeries, WPData, SMSeries, SMData, JTSeries, JTData,
        ECSeries, ECData, OESeries, OEData, PRSeries, PRData, TUSeries, TUData,
        IPSeries, IPData, LNSeries, LNData, CWSeries, CWData, SUSeries, SUData,
        BDSeries, BDData, EISeries, EIData
    )
    from bls.update_manager import record_api_usage


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
    # 'EC': Skip - legacy survey
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

# Number of series to check per survey
SERIES_TO_CHECK = 50


@dataclass
class FreshnessResult:
    """Result of a freshness check for a single survey"""
    survey_code: str
    survey_name: str
    has_new_data: bool
    series_checked: int
    series_with_new_data: int
    our_latest: Optional[str]  # e.g., "2024 M11"
    bls_latest: Optional[str]  # e.g., "2024 M12"
    error: Optional[str] = None
    checked_at: datetime = None

    def __post_init__(self):
        if self.checked_at is None:
            self.checked_at = datetime.now()


def get_sample_series(session: Session, series_model, limit: int = SERIES_TO_CHECK) -> List[str]:
    """
    Get a sample of active series IDs for freshness checking.

    Selects series that have data, prioritizing those with recent updates.
    """
    # Get active series with data, ordered by series_id for consistency
    series = session.query(series_model.series_id).filter(
        series_model.is_active == True
    ).order_by(series_model.series_id).limit(limit).all()

    return [row[0] for row in series]


def get_our_latest_period(session: Session, data_model, series_ids: List[str]) -> Dict[str, tuple]:
    """
    Get the latest (year, period) we have for each series.

    Returns dict: series_id -> (year, period)
    """
    result = {}

    for series_id in series_ids:
        latest = session.query(
            func.max(data_model.year).label('max_year')
        ).filter(
            data_model.series_id == series_id
        ).first()

        if latest and latest.max_year:
            # Get the latest period for that year
            period = session.query(
                func.max(data_model.period)
            ).filter(
                data_model.series_id == series_id,
                data_model.year == latest.max_year
            ).scalar()

            result[series_id] = (latest.max_year, period)

    return result


def check_survey_freshness(
    survey_code: str,
    session: Session,
    client: BLSClient,
    series_to_check: int = SERIES_TO_CHECK
) -> FreshnessResult:
    """
    Check if a survey has new data available from BLS.

    Compares our latest data with BLS API for a sample of series.

    Args:
        survey_code: BLS survey code (e.g., 'CU', 'LA')
        session: Database session
        client: BLS API client
        series_to_check: Number of series to sample (default: 50)

    Returns:
        FreshnessResult with comparison details
    """
    survey_code = survey_code.upper()

    if survey_code not in SURVEYS:
        return FreshnessResult(
            survey_code=survey_code,
            survey_name='Unknown',
            has_new_data=False,
            series_checked=0,
            series_with_new_data=0,
            our_latest=None,
            bls_latest=None,
            error=f"Invalid survey code: {survey_code}"
        )

    series_model, data_model, survey_name = SURVEYS[survey_code]

    try:
        # Get sample series
        series_ids = get_sample_series(session, series_model, series_to_check)

        if not series_ids:
            return FreshnessResult(
                survey_code=survey_code,
                survey_name=survey_name,
                has_new_data=False,
                series_checked=0,
                series_with_new_data=0,
                our_latest=None,
                bls_latest=None,
                error="No active series found"
            )

        # Get our latest periods
        our_latest = get_our_latest_period(session, data_model, series_ids)

        # Fetch from BLS API
        current_year = datetime.now().year
        rows = client.get_many(
            series_ids,
            start_year=current_year - 1,
            end_year=current_year,
            calculations=False,
            catalog=False,
            as_dataframe=False
        )

        # Log API usage for quota tracking
        record_api_usage(session, 1, len(series_ids), survey_code, script_name='freshness_check')

        # Group by series and find latest
        bls_latest = {}
        for row in rows:
            series_id = row['series_id']
            year = row['year']
            period = row['period']

            if series_id not in bls_latest:
                bls_latest[series_id] = (year, period)
            else:
                existing = bls_latest[series_id]
                if (year, period) > existing:
                    bls_latest[series_id] = (year, period)

        # Compare
        series_with_new_data = 0
        sample_our = None
        sample_bls = None

        for series_id in series_ids:
            our = our_latest.get(series_id)
            bls = bls_latest.get(series_id)

            if bls and (not our or bls > our):
                series_with_new_data += 1
                if sample_our is None and our:
                    sample_our = f"{our[0]} {our[1]}"
                if sample_bls is None and bls:
                    sample_bls = f"{bls[0]} {bls[1]}"

        has_new_data = series_with_new_data > 0

        return FreshnessResult(
            survey_code=survey_code,
            survey_name=survey_name,
            has_new_data=has_new_data,
            series_checked=len(series_ids),
            series_with_new_data=series_with_new_data,
            our_latest=sample_our,
            bls_latest=sample_bls,
            error=None
        )

    except Exception as e:
        return FreshnessResult(
            survey_code=survey_code,
            survey_name=survey_name,
            has_new_data=False,
            series_checked=0,
            series_with_new_data=0,
            our_latest=None,
            bls_latest=None,
            error=str(e)
        )


def check_all_surveys(
    session: Session,
    client: BLSClient,
    survey_codes: Optional[List[str]] = None,
    series_to_check: int = SERIES_TO_CHECK
) -> List[FreshnessResult]:
    """
    Check freshness for multiple surveys.

    Args:
        session: Database session
        client: BLS API client
        survey_codes: List of survey codes to check (default: all except EC)
        series_to_check: Number of series to sample per survey

    Returns:
        List of FreshnessResult for each survey
    """
    if survey_codes is None:
        survey_codes = list(SURVEYS.keys())

    results = []
    for code in survey_codes:
        result = check_survey_freshness(code, session, client, series_to_check)
        results.append(result)

    return results


def get_surveys_needing_update(
    session: Session,
    client: BLSClient,
    survey_codes: Optional[List[str]] = None
) -> List[str]:
    """
    Get list of survey codes that have new data available.

    Convenience function that returns only the codes needing update.
    """
    results = check_all_surveys(session, client, survey_codes)
    return [r.survey_code for r in results if r.has_new_data and not r.error]
