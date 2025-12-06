"""
LN Explorer API Endpoints
Labor Force Statistics from the Current Population Survey (CPS)
"""
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc

from src.admin.core.database import get_db
from src.admin.schemas.ln_explorer import (
    LNDimensions,
    LNDimensionItem,
    LNSeriesListResponse,
    LNSeriesInfo,
    LNDataResponse,
    LNSeriesData,
    DataPoint,
    LNOverviewResponse,
    UnemploymentMetric,
    LNDemographicAnalysisResponse,
    DemographicBreakdown,
    LNOverviewTimelineResponse,
    OverviewTimelinePoint,
    LNDemographicTimelineResponse,
    DemographicTimelinePoint,
    LNOccupationAnalysisResponse,
    LNOccupationTimelineResponse,
    LNIndustryAnalysisResponse,
    LNIndustryTimelineResponse,
)
from src.database.bls_models import (
    LNSeries,
    LNData,
    LNLaborForceStatus,
    LNAge,
    LNSex,
    LNRace,
    LNEducation,
    LNOccupation,
    LNIndustry,
    LNMaritalStatus,
    LNVeteran,
    LNDisability,
    LNTelework,
    BLSPeriod,
)

router = APIRouter(prefix="/ln", tags=["LN Explorer"])


# ==================== Helper Functions ====================

def _calculate_unemployment_metrics(series_id: str, dimension_name: str, db: Session) -> Optional[UnemploymentMetric]:
    """Calculate unemployment rate metrics for a series"""
    # Get the last 25 data points for calculations
    data_points = db.query(LNData).filter(
        LNData.series_id == series_id
    ).order_by(
        LNData.year.desc(), LNData.period.desc()
    ).limit(25).all()

    if not data_points or len(data_points) < 1:
        return None

    # Most recent data point
    latest = data_points[0]
    latest_value = float(latest.value) if latest.value else None
    latest_year = latest.year
    latest_period = latest.period

    # Build a dictionary for easier lookups
    data_dict = {(d.year, d.period): float(d.value) if d.value else None for d in data_points}

    # Calculate m/m change (vs previous month)
    month_over_month = None
    prev_month = int(latest_period[1:]) - 1  # Remove 'M' prefix and subtract 1
    prev_year = latest_year
    if prev_month < 1:
        prev_month = 12
        prev_year = latest_year - 1
    prev_period = f"M{prev_month:02d}"
    prev_month_value = data_dict.get((prev_year, prev_period))
    if prev_month_value and latest_value:
        month_over_month = latest_value - prev_month_value  # Absolute change in percentage points

    # Calculate y/y change (vs same month last year)
    year_over_year = None
    year_ago_value = data_dict.get((latest_year - 1, latest_period))
    if year_ago_value and latest_value:
        year_over_year = latest_value - year_ago_value  # Absolute change in percentage points

    return UnemploymentMetric(
        series_id=series_id,
        dimension_name=dimension_name,
        latest_value=latest_value,
        latest_date=f"{latest.year}-{latest.period}",
        month_over_month=round(month_over_month, 1) if month_over_month else None,
        year_over_year=round(year_over_year, 1) if year_over_year else None,
    )


# ==================== API Endpoints ====================

@router.get("/dimensions", response_model=LNDimensions)
def get_dimensions(db: Session = Depends(get_db)):
    """Get all available dimensions for filtering LN data"""

    # Query all dimension tables
    labor_force_statuses = [
        LNDimensionItem(code=row.lfst_code, text=row.lfst_text)
        for row in db.query(LNLaborForceStatus).all()
    ]

    ages = [
        LNDimensionItem(code=row.ages_code, text=row.ages_text)
        for row in db.query(LNAge).all()
    ]

    sexes = [
        LNDimensionItem(code=row.sexs_code, text=row.sexs_text)
        for row in db.query(LNSex).all()
    ]

    races = [
        LNDimensionItem(code=row.race_code, text=row.race_text)
        for row in db.query(LNRace).all()
    ]

    educations = [
        LNDimensionItem(code=row.education_code, text=row.education_text)
        for row in db.query(LNEducation).all()
    ]

    occupations = [
        LNDimensionItem(code=row.occupation_code, text=row.occupation_text)
        for row in db.query(LNOccupation).all()
    ]

    industries = [
        LNDimensionItem(code=row.indy_code, text=row.indy_text)
        for row in db.query(LNIndustry).all()
    ]

    marital_statuses = [
        LNDimensionItem(code=row.mari_code, text=row.mari_text)
        for row in db.query(LNMaritalStatus).all()
    ]

    veteran_statuses = [
        LNDimensionItem(code=row.vets_code, text=row.vets_text)
        for row in db.query(LNVeteran).all()
    ]

    disability_statuses = [
        LNDimensionItem(code=row.disa_code, text=row.disa_text)
        for row in db.query(LNDisability).all()
    ]

    telework_statuses = [
        LNDimensionItem(code=row.tlwk_code, text=row.tlwk_text)
        for row in db.query(LNTelework).all()
    ]

    return LNDimensions(
        labor_force_statuses=labor_force_statuses,
        ages=ages,
        sexes=sexes,
        races=races,
        educations=educations,
        occupations=occupations,
        industries=industries,
        marital_statuses=marital_statuses,
        veteran_statuses=veteran_statuses,
        disability_statuses=disability_statuses,
        telework_statuses=telework_statuses,
    )


@router.get("/series", response_model=LNSeriesListResponse)
def get_series(
    lfst_code: Optional[str] = None,
    ages_code: Optional[str] = None,
    sexs_code: Optional[str] = None,
    race_code: Optional[str] = None,
    education_code: Optional[str] = None,
    occupation_code: Optional[str] = None,
    indy_code: Optional[str] = None,
    mari_code: Optional[str] = None,
    vets_code: Optional[str] = None,
    disa_code: Optional[str] = None,
    tlwk_code: Optional[str] = None,
    seasonal: Optional[str] = None,  # S or U
    active_only: bool = True,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """Get list of LN series with optional filtering by dimensions"""

    query = db.query(LNSeries)

    # Apply filters
    filters = []
    if lfst_code:
        filters.append(LNSeries.lfst_code == lfst_code)
    if ages_code:
        filters.append(LNSeries.ages_code == ages_code)
    if sexs_code:
        filters.append(LNSeries.sexs_code == sexs_code)
    if race_code:
        filters.append(LNSeries.race_code == race_code)
    if education_code:
        filters.append(LNSeries.education_code == education_code)
    if occupation_code:
        filters.append(LNSeries.occupation_code == occupation_code)
    if indy_code:
        filters.append(LNSeries.indy_code == indy_code)
    if mari_code:
        filters.append(LNSeries.mari_code == mari_code)
    if vets_code:
        filters.append(LNSeries.vets_code == vets_code)
    if disa_code:
        filters.append(LNSeries.disa_code == disa_code)
    if tlwk_code:
        filters.append(LNSeries.tlwk_code == tlwk_code)
    if seasonal:
        filters.append(LNSeries.seasonal == seasonal)
    if active_only:
        filters.append(LNSeries.is_active == True)

    if filters:
        query = query.filter(and_(*filters))

    # Get total count
    total = query.count()

    # Get paginated results
    series = query.order_by(LNSeries.series_id).offset(offset).limit(limit).all()

    return LNSeriesListResponse(
        total=total,
        limit=limit,
        offset=offset,
        series=[LNSeriesInfo.from_orm(s) for s in series]
    )


@router.get("/series/{series_id}/data", response_model=LNDataResponse)
def get_series_data(
    series_id: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get time series data for a specific LN series"""

    # Get series metadata
    series = db.query(LNSeries).filter(LNSeries.series_id == series_id).first()
    if not series:
        return LNDataResponse(series=[])

    # Build data query
    query = db.query(LNData).filter(LNData.series_id == series_id)

    # Apply date filters
    if start_year:
        query = query.filter(LNData.year >= start_year)
    if end_year:
        query = query.filter(LNData.year <= end_year)
    if start_period:
        query = query.filter(LNData.period >= start_period)
    if end_period:
        query = query.filter(LNData.period <= end_period)

    # Get data ordered by date
    data = query.order_by(LNData.year, LNData.period).all()

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Format data points
    data_points = [
        DataPoint(
            year=d.year,
            period=d.period,
            period_name=f"{period_map.get(d.period, d.period)} {d.year}",
            value=float(d.value) if d.value else None,
            footnote_codes=d.footnote_codes
        )
        for d in data
    ]

    series_data = LNSeriesData(
        series_id=series.series_id,
        series_title=series.series_title,
        data_points=data_points
    )

    return LNDataResponse(series=[series_data])


@router.get("/overview", response_model=LNOverviewResponse)
def get_overview(db: Session = Depends(get_db)):
    """Get overview dashboard with headline unemployment metrics"""

    # Headline unemployment rate (seasonally adjusted, all persons 16+)
    # Series ID for civilian unemployment rate: LNS14000000
    headline_series_id = "LNS14000000"
    headline = _calculate_unemployment_metrics(headline_series_id, "Unemployment Rate", db)

    # Labor force participation rate (seasonally adjusted, all persons 16+)
    # Series ID: LNS11300000
    lfpr_series_id = "LNS11300000"
    labor_force_participation = _calculate_unemployment_metrics(lfpr_series_id, "Labor Force Participation Rate", db)

    # Employment-population ratio (seasonally adjusted, all persons 16+)
    # Series ID: LNS12300000
    epop_series_id = "LNS12300000"
    employment_population_ratio = _calculate_unemployment_metrics(epop_series_id, "Employment-Population Ratio", db)

    # Get last updated date from most recent data
    last_updated = None
    if headline:
        last_updated = headline.latest_date

    return LNOverviewResponse(
        headline_unemployment=headline,
        labor_force_participation=labor_force_participation,
        employment_population_ratio=employment_population_ratio,
        last_updated=last_updated
    )


@router.get("/demographics", response_model=LNDemographicAnalysisResponse)
def get_demographic_analysis(db: Session = Depends(get_db)):
    """
    Get unemployment breakdown by key demographic dimensions (latest snapshot).
    For historical data, use /series/{series_id}/data endpoint with specific series IDs.
    """

    breakdowns = []

    # Unemployment by Age (seasonally adjusted)
    age_series_map = {
        "03": ("LNS14000012", "16 to 19 years"),
        "12": ("LNS14000089", "20 to 24 years"),
        "13": ("LNS14000091", "25 to 34 years"),
        "14": ("LNS14000093", "35 to 44 years"),
        "15": ("LNS14000095", "45 to 54 years"),
        "16": ("LNS14000097", "55 years and over"),
    }

    age_metrics = []
    for age_code, (series_id, age_name) in age_series_map.items():
        metric = _calculate_unemployment_metrics(series_id, age_name, db)
        if metric:
            age_metrics.append(metric)

    if age_metrics:
        breakdowns.append(DemographicBreakdown(
            dimension_type="age",
            dimension_name="Age Groups",
            metrics=age_metrics
        ))

    # Unemployment by Sex (seasonally adjusted)
    sex_series_map = {
        "01": ("LNS14000001", "Men"),
        "02": ("LNS14000002", "Women"),
    }

    sex_metrics = []
    for sex_code, (series_id, sex_name) in sex_series_map.items():
        metric = _calculate_unemployment_metrics(series_id, sex_name, db)
        if metric:
            sex_metrics.append(metric)

    if sex_metrics:
        breakdowns.append(DemographicBreakdown(
            dimension_type="sex",
            dimension_name="Sex",
            metrics=sex_metrics
        ))

    # Unemployment by Race (seasonally adjusted)
    race_series_map = {
        "01": ("LNS14000003", "White"),
        "02": ("LNS14000006", "Black or African American"),
        "03": ("LNS14000009", "Asian"),
    }

    race_metrics = []
    for race_code, (series_id, race_name) in race_series_map.items():
        metric = _calculate_unemployment_metrics(series_id, race_name, db)
        if metric:
            race_metrics.append(metric)

    if race_metrics:
        breakdowns.append(DemographicBreakdown(
            dimension_type="race",
            dimension_name="Race",
            metrics=race_metrics
        ))

    # Unemployment by Education (seasonally adjusted, 25 years and over)
    education_series_map = {
        "01": ("LNS14027659", "Less than high school diploma"),
        "02": ("LNS14027660", "High school graduates, no college"),
        "03": ("LNS14027689", "Some college or associate degree"),
        "04": ("LNS14027662", "Bachelor's degree and higher"),
    }

    education_metrics = []
    for edu_code, (series_id, edu_name) in education_series_map.items():
        metric = _calculate_unemployment_metrics(series_id, edu_name, db)
        if metric:
            education_metrics.append(metric)

    if education_metrics:
        breakdowns.append(DemographicBreakdown(
            dimension_type="education",
            dimension_name="Educational Attainment",
            metrics=education_metrics
        ))

    return LNDemographicAnalysisResponse(breakdowns=breakdowns)


@router.get("/overview/timeline", response_model=LNOverviewTimelineResponse)
def get_overview_timeline(
    months_back: int = Query(24, ge=0, le=600),
    db: Session = Depends(get_db)
):
    """Get timeline data for overview metrics (headline unemployment, LFPR, emp-pop ratio)"""

    # Define series IDs
    headline_series_id = "LNS14000000"  # Unemployment rate
    lfpr_series_id = "LNS11300000"  # Labor force participation rate
    epop_series_id = "LNS12300000"  # Employment-population ratio

    # Get data for all three series (0 = all time)
    headline_query = db.query(LNData).filter(
        LNData.series_id == headline_series_id
    ).order_by(LNData.year.desc(), LNData.period.desc())
    headline_data = headline_query.all() if months_back == 0 else headline_query.limit(months_back).all()

    lfpr_query = db.query(LNData).filter(
        LNData.series_id == lfpr_series_id
    ).order_by(LNData.year.desc(), LNData.period.desc())
    lfpr_data = lfpr_query.all() if months_back == 0 else lfpr_query.limit(months_back).all()

    epop_query = db.query(LNData).filter(
        LNData.series_id == epop_series_id
    ).order_by(LNData.year.desc(), LNData.period.desc())
    epop_data = epop_query.all() if months_back == 0 else epop_query.limit(months_back).all()

    # Create dictionaries for easier lookup
    headline_dict = {(d.year, d.period): float(d.value) if d.value else None for d in headline_data}
    lfpr_dict = {(d.year, d.period): float(d.value) if d.value else None for d in lfpr_data}
    epop_dict = {(d.year, d.period): float(d.value) if d.value else None for d in epop_data}

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline (reverse to get chronological order)
    timeline = []
    for d in reversed(headline_data):
        timeline.append(OverviewTimelinePoint(
            year=d.year,
            period=d.period,
            period_name=f"{period_map.get(d.period, d.period)} {d.year}",
            headline_value=headline_dict.get((d.year, d.period)),
            lfpr_value=lfpr_dict.get((d.year, d.period)),
            epop_value=epop_dict.get((d.year, d.period)),
        ))

    return LNOverviewTimelineResponse(timeline=timeline)


@router.get("/demographics/timeline", response_model=LNDemographicTimelineResponse)
def get_demographic_timeline(
    dimension_type: str = Query(..., description="Dimension type: age, sex, race, education"),
    months_back: int = Query(24, ge=0, le=600),
    db: Session = Depends(get_db)
):
    """Get timeline data for a specific demographic dimension"""

    # Define series maps for each dimension
    dimension_maps = {
        "age": {
            "dimension_name": "Age Groups",
            "series": {
                "16 to 19 years": "LNS14000012",
                "20 to 24 years": "LNS14000089",
                "25 to 34 years": "LNS14000091",
                "35 to 44 years": "LNS14000093",
                "45 to 54 years": "LNS14000095",
                "55 years and over": "LNS14000097",
            }
        },
        "sex": {
            "dimension_name": "Sex",
            "series": {
                "Men": "LNS14000001",
                "Women": "LNS14000002",
            }
        },
        "race": {
            "dimension_name": "Race",
            "series": {
                "White": "LNS14000003",
                "Black or African American": "LNS14000006",
                "Asian": "LNS14000009",
            }
        },
        "education": {
            "dimension_name": "Educational Attainment",
            "series": {
                "Less than high school diploma": "LNS14027659",
                "High school graduates, no college": "LNS14027660",
                "Some college or associate degree": "LNS14027689",
                "Bachelor's degree and higher": "LNS14027662",
            }
        },
    }

    if dimension_type not in dimension_maps:
        return LNDemographicTimelineResponse(
            dimension_type=dimension_type,
            dimension_name="Unknown",
            timeline=[]
        )

    dim_config = dimension_maps[dimension_type]

    # Get data for all series in this dimension (0 = all time)
    all_series_data = {}
    for name, series_id in dim_config["series"].items():
        query = db.query(LNData).filter(
            LNData.series_id == series_id
        ).order_by(LNData.year.desc(), LNData.period.desc())
        data = query.all() if months_back == 0 else query.limit(months_back).all()
        all_series_data[name] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

    # Get unique time periods from first series
    first_series_id = list(dim_config["series"].values())[0]
    time_query = db.query(LNData).filter(
        LNData.series_id == first_series_id
    ).order_by(LNData.year.desc(), LNData.period.desc())
    time_points = time_query.all() if months_back == 0 else time_query.limit(months_back).all()

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline
    timeline = []
    for tp in reversed(time_points):
        metrics = []
        for name, series_id in dim_config["series"].items():
            value = all_series_data[name].get((tp.year, tp.period))
            if value is not None:
                metrics.append(UnemploymentMetric(
                    series_id=series_id,
                    dimension_name=name,
                    latest_value=value,
                    latest_date=f"{tp.year}-{tp.period}",
                ))

        timeline.append(DemographicTimelinePoint(
            year=tp.year,
            period=tp.period,
            period_name=f"{period_map.get(tp.period, tp.period)} {tp.year}",
            metrics=metrics
        ))

    return LNDemographicTimelineResponse(
        dimension_type=dimension_type,
        dimension_name=dim_config["dimension_name"],
        timeline=timeline
    )


@router.get("/occupations", response_model=LNOccupationAnalysisResponse)
def get_occupation_analysis(db: Session = Depends(get_db)):
    """Get unemployment breakdown by occupation (latest snapshot)"""

    # Key occupation series (seasonally adjusted, 16 years and over)
    occupation_series_map = {
        "Management, professional, and related": "LNS14000000",  # Placeholder - need actual series
        "Service occupations": "LNS14000000",  # Placeholder - need actual series
        "Sales and office occupations": "LNS14000000",  # Placeholder - need actual series
        "Natural resources, construction, and maintenance": "LNS14000000",  # Placeholder - need actual series
        "Production, transportation, and material moving": "LNS14000000",  # Placeholder - need actual series
    }

    occupation_metrics = []
    for occ_name, series_id in occupation_series_map.items():
        metric = _calculate_unemployment_metrics(series_id, occ_name, db)
        if metric:
            occupation_metrics.append(metric)

    return LNOccupationAnalysisResponse(occupations=occupation_metrics)


@router.get("/occupations/timeline", response_model=LNOccupationTimelineResponse)
def get_occupation_timeline(
    months_back: int = Query(24, ge=0, le=600),
    db: Session = Depends(get_db)
):
    """Get timeline data for occupation unemployment"""

    # Key occupation series
    occupation_series_map = {
        "Management, professional, and related": "LNS14000000",  # Placeholder
        "Service occupations": "LNS14000000",  # Placeholder
        "Sales and office occupations": "LNS14000000",  # Placeholder
        "Natural resources, construction, and maintenance": "LNS14000000",  # Placeholder
        "Production, transportation, and material moving": "LNS14000000",  # Placeholder
    }

    # Get data for all occupation series (0 = all time)
    all_series_data = {}
    for name, series_id in occupation_series_map.items():
        query = db.query(LNData).filter(
            LNData.series_id == series_id
        ).order_by(LNData.year.desc(), LNData.period.desc())
        data = query.all() if months_back == 0 else query.limit(months_back).all()
        all_series_data[name] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

    # Get unique time periods from first series
    first_series_id = list(occupation_series_map.values())[0]
    time_query = db.query(LNData).filter(
        LNData.series_id == first_series_id
    ).order_by(LNData.year.desc(), LNData.period.desc())
    time_points = time_query.all() if months_back == 0 else time_query.limit(months_back).all()

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline
    timeline = []
    for tp in reversed(time_points):
        metrics = []
        for name, series_id in occupation_series_map.items():
            value = all_series_data[name].get((tp.year, tp.period))
            if value is not None:
                metrics.append(UnemploymentMetric(
                    series_id=series_id,
                    dimension_name=name,
                    latest_value=value,
                    latest_date=f"{tp.year}-{tp.period}",
                ))

        timeline.append(DemographicTimelinePoint(
            year=tp.year,
            period=tp.period,
            period_name=f"{period_map.get(tp.period, tp.period)} {tp.year}",
            metrics=metrics
        ))

    return LNOccupationTimelineResponse(timeline=timeline)


@router.get("/industries", response_model=LNIndustryAnalysisResponse)
def get_industry_analysis(db: Session = Depends(get_db)):
    """Get unemployment breakdown by industry (latest snapshot)"""

    # Key industry unemployment rate series (not seasonally adjusted - no SA available for industry)
    industry_series_map = {
        "Agriculture and related": "LNU04032244",
        "Mining, quarrying, and oil/gas": "LNU04032230",
        "Construction": "LNU04032231",
        "Manufacturing": "LNU04032232",
        "Wholesale and retail trade": "LNU04032235",
        "Transportation and utilities": "LNU04032236",
        "Information": "LNU04032237",
        "Financial activities": "LNU04032238",
        "Professional and business services": "LNU04032239",
        "Education and health services": "LNU04032240",
        "Leisure and hospitality": "LNU04032241",
        "Other services": "LNU04032242",
    }

    industry_metrics = []
    for ind_name, series_id in industry_series_map.items():
        metric = _calculate_unemployment_metrics(series_id, ind_name, db)
        if metric:
            industry_metrics.append(metric)

    return LNIndustryAnalysisResponse(industries=industry_metrics)


@router.get("/industries/timeline", response_model=LNIndustryTimelineResponse)
def get_industry_timeline(
    months_back: int = Query(24, ge=0, le=600),
    db: Session = Depends(get_db)
):
    """Get timeline data for industry unemployment"""

    # Key industry unemployment rate series (not seasonally adjusted - no SA available for industry)
    industry_series_map = {
        "Agriculture and related": "LNU04032244",
        "Mining, quarrying, and oil/gas": "LNU04032230",
        "Construction": "LNU04032231",
        "Manufacturing": "LNU04032232",
        "Wholesale and retail trade": "LNU04032235",
        "Transportation and utilities": "LNU04032236",
        "Information": "LNU04032237",
        "Financial activities": "LNU04032238",
        "Professional and business services": "LNU04032239",
        "Education and health services": "LNU04032240",
        "Leisure and hospitality": "LNU04032241",
        "Other services": "LNU04032242",
    }

    # Get data for all industry series (0 = all time)
    all_series_data = {}
    for name, series_id in industry_series_map.items():
        query = db.query(LNData).filter(
            LNData.series_id == series_id
        ).order_by(LNData.year.desc(), LNData.period.desc())
        data = query.all() if months_back == 0 else query.limit(months_back).all()
        all_series_data[name] = {(d.year, d.period): float(d.value) if d.value else None for d in data}

    # Get unique time periods from first series
    first_series_id = list(industry_series_map.values())[0]
    time_query = db.query(LNData).filter(
        LNData.series_id == first_series_id
    ).order_by(LNData.year.desc(), LNData.period.desc())
    time_points = time_query.all() if months_back == 0 else time_query.limit(months_back).all()

    # Get period names
    period_map = {p.period_code: p.period_name for p in db.query(BLSPeriod).all()}

    # Build timeline
    timeline = []
    for tp in reversed(time_points):
        metrics = []
        for name, series_id in industry_series_map.items():
            value = all_series_data[name].get((tp.year, tp.period))
            if value is not None:
                metrics.append(UnemploymentMetric(
                    series_id=series_id,
                    dimension_name=name,
                    latest_value=value,
                    latest_date=f"{tp.year}-{tp.period}",
                ))

        timeline.append(DemographicTimelinePoint(
            year=tp.year,
            period=tp.period,
            period_name=f"{period_map.get(tp.period, tp.period)} {tp.year}",
            metrics=metrics
        ))

    return LNIndustryTimelineResponse(timeline=timeline)
