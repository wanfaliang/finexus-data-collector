#!/usr/bin/env python3
"""Check active series counts for all BLS surveys to determine API update feasibility"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from config import settings
from database.bls_models import (
    APSeries, CUSeries, LASeries, CESeries, PCSeries, WPSeries,
    SMSeries, JTSeries, ECSeries, OESeries, PRSeries, TUSeries,
    IPSeries, LNSeries, CWSeries, SUSeries, BDSeries, EISeries
)

engine = create_engine(settings.database.url, echo=False)
Session = sessionmaker(bind=engine)
session = Session()

surveys = [
    ('AP', APSeries, 'Average Price Data'),
    ('CU', CUSeries, 'Consumer Price Index'),
    ('LA', LASeries, 'Local Area Unemployment'),
    ('CE', CESeries, 'Current Employment Statistics'),
    ('PC', PCSeries, 'Producer Price Index - Commodity'),
    ('WP', WPSeries, 'Producer Price Index'),
    ('SM', SMSeries, 'State and Metro Area Employment'),
    ('JT', JTSeries, 'JOLTS'),
    ('EC', ECSeries, 'Employment Cost Index'),
    ('OE', OESeries, 'Occupational Employment'),
    ('PR', PRSeries, 'Major Sector Productivity'),
    ('TU', TUSeries, 'American Time Use Survey'),
    ('IP', IPSeries, 'Industry Productivity'),
    ('LN', LNSeries, 'Labor Force Statistics'),
    ('CW', CWSeries, 'CPI - Urban Wage Earners'),
    ('SU', SUSeries, 'Chained CPI'),
    ('BD', BDSeries, 'Business Employment Dynamics'),
    ('EI', EISeries, 'Import/Export Price Indexes')
]

print('=' * 90)
print('BLS SURVEY SERIES COUNTS AND API UPDATE FEASIBILITY')
print('=' * 90)
print(f'\n{"Survey":<6} | {"Active Series":>15} | {"API Requests":>14} | {"Recommendation":<30}')
print('-' * 90)

# BLS API limits: 500/day with key, 25,000/year
DAILY_LIMIT = 500
ANNUAL_LIMIT = 25000

total_series = 0
total_requests = 0

for code, model, name in surveys:
    count = session.query(func.count(model.series_id)).filter(model.is_active == True).scalar()
    requests = (count + 49) // 50  # Ceiling division for 50 series per request

    # Determine feasibility
    if requests <= 100:  # < 5,000 series
        recommendation = "API updates OK"
    elif requests <= 500:  # < 25,000 series
        recommendation = "API updates (1 day)"
    elif requests <= ANNUAL_LIMIT / 10:  # < 125,000 series
        recommendation = "API updates (few days)"
    else:
        recommendation = "Use flat files only"

    print(f'{code:<6} | {count:>15,} | {requests:>14,} | {recommendation:<30}')
    total_series += count
    total_requests += requests

print('-' * 90)
print(f'{"TOTAL":<6} | {total_series:>15,} | {total_requests:>14,} | ')
print('=' * 90)

print('\nAPI Constraints:')
print(f'  Daily limit:  {DAILY_LIMIT:,} requests/day (with API key)')
print(f'  Annual limit: {ANNUAL_LIMIT:,} requests/year')
print(f'\nDays needed to update all series: {total_requests / DAILY_LIMIT:.1f} days')
print(f'Percentage of annual quota: {total_requests / ANNUAL_LIMIT * 100:.1f}%')

print('\nRecommendations:')
print('  - Surveys with < 100 requests: Safe for regular API updates')
print('  - Surveys with 100-2,500 requests: Feasible but monitor quota')
print('  - Surveys with > 2,500 requests: Use flat file downloads for updates')

session.close()
