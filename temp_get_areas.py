from sqlalchemy import create_engine, text
from src.config import get_db_url

engine = create_engine(get_db_url())
with engine.connect() as conn:
    result = conn.execute(text('SELECT area_code, area_name FROM cu_areas ORDER BY sort_sequence'))
    areas = result.fetchall()
    for code, name in areas:
        print(f'{code}\t{name}')
