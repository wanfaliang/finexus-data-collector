#!/usr/bin/env python3
"""Quick verification script for LA data"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from sqlalchemy import create_engine, text
from config import settings

engine = create_engine(settings.database.url)

with engine.connect() as conn:
    # Count total observations
    result = conn.execute(text('SELECT COUNT(*) FROM bls_la_data'))
    total = result.fetchone()[0]
    print(f"Total LA observations: {total:,}")

    # Count 2024+ observations
    result = conn.execute(text('SELECT COUNT(*) FROM bls_la_data WHERE year >= 2024'))
    recent = result.fetchone()[0]
    print(f"LA observations for 2024+: {recent:,}")

    # Sample recent data
    result = conn.execute(text('''
        SELECT d.series_id, d.year, d.period, d.value, s.series_title
        FROM bls_la_data d
        JOIN bls_la_series s ON d.series_id = s.series_id
        WHERE d.year >= 2024
        ORDER BY d.year DESC, d.period DESC
        LIMIT 5
    '''))

    print("\nSample recent observations:")
    for row in result:
        print(f"  {row[0]} ({row[1]}-{row[2]}): {row[3]} - {row[4][:60]}")
