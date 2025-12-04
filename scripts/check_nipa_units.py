"""Check NIPA series unit strings"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import get_session
from src.database.bea_models import NIPASeries, NIPAData

with get_session() as session:
    # Check T10101 and T10105 line 1 series
    for tname in ['T10101', 'T10105']:
        series = session.query(NIPASeries).filter(
            NIPASeries.table_name == tname,
            NIPASeries.line_number == 1
        ).first()
        if series:
            print(f'{tname} Line 1:')
            print(f'  series_code: {series.series_code}')
            print(f'  line_description: {series.line_description}')
            print(f'  metric_name: {series.metric_name}')
            print(f'  cl_unit: {series.cl_unit}')
            print(f'  unit_mult: {series.unit_mult}')

            # Get a sample data point
            data = session.query(NIPAData).filter(
                NIPAData.series_code == series.series_code
            ).order_by(NIPAData.time_period.desc()).first()
            if data:
                print(f'  Latest value: {data.value} ({data.time_period})')
            print()
