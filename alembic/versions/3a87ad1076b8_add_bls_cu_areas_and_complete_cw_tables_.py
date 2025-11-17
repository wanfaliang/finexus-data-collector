"""Add bls_cu_areas and complete CW tables (items, series, data, aspects)

Revision ID: 3a87ad1076b8
Revises: addd58cd7f11
Create Date: 2025-11-16 20:11:33.808046

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3a87ad1076b8'
down_revision: Union[str, Sequence[str], None] = 'addd58cd7f11'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create bls_cu_areas
    op.create_table('bls_cu_areas',
    sa.Column('area_code', sa.String(length=10), nullable=False),
    sa.Column('area_name', sa.String(length=255), nullable=False),
    sa.Column('display_level', sa.SmallInteger(), nullable=True),
    sa.Column('selectable', sa.String(length=1), nullable=True),
    sa.Column('sort_sequence', sa.SmallInteger(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('area_code')
    )
    op.create_index('ix_bls_cu_areas_display_level', 'bls_cu_areas', ['display_level'], unique=False)
    op.create_index('ix_bls_cu_areas_selectable', 'bls_cu_areas', ['selectable'], unique=False)

    # Copy existing area data from bls_areas for CU area codes
    # This ensures referential integrity when we add the foreign key
    op.execute("""
        INSERT INTO bls_cu_areas (area_code, area_name, display_level, selectable, sort_sequence, created_at, updated_at)
        SELECT DISTINCT
            a.area_code,
            a.area_name,
            0 as display_level,
            'T' as selectable,
            0 as sort_sequence,
            NOW() as created_at,
            NOW() as updated_at
        FROM bls_areas a
        INNER JOIN bls_cu_series s ON s.area_code = a.area_code
        ON CONFLICT (area_code) DO NOTHING
    """)

    # Update bls_cu_series foreign key
    op.drop_constraint(op.f('bls_cu_series_area_code_fkey'), 'bls_cu_series', type_='foreignkey')
    op.create_foreign_key(None, 'bls_cu_series', 'bls_cu_areas', ['area_code'], ['area_code'])

    # Create bls_cw_items
    op.create_table('bls_cw_items',
    sa.Column('item_code', sa.String(length=20), nullable=False),
    sa.Column('item_name', sa.String(length=500), nullable=False),
    sa.Column('display_level', sa.SmallInteger(), nullable=True),
    sa.Column('selectable', sa.String(length=1), nullable=True),
    sa.Column('sort_sequence', sa.SmallInteger(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('item_code')
    )
    op.create_index('ix_bls_cw_items_display_level', 'bls_cw_items', ['display_level'], unique=False)
    op.create_index('ix_bls_cw_items_selectable', 'bls_cw_items', ['selectable'], unique=False)

    # Create bls_cw_series
    op.create_table('bls_cw_series',
    sa.Column('series_id', sa.String(length=20), nullable=False),
    sa.Column('area_code', sa.String(length=10), nullable=True),
    sa.Column('item_code', sa.String(length=20), nullable=True),
    sa.Column('seasonal_code', sa.String(length=1), nullable=True),
    sa.Column('periodicity_code', sa.String(length=5), nullable=True),
    sa.Column('base_code', sa.String(length=10), nullable=True),
    sa.Column('base_period', sa.String(length=50), nullable=True),
    sa.Column('series_title', sa.Text(), nullable=False),
    sa.Column('footnote_codes', sa.String(length=50), nullable=True),
    sa.Column('begin_year', sa.SmallInteger(), nullable=True),
    sa.Column('begin_period', sa.String(length=5), nullable=True),
    sa.Column('end_year', sa.SmallInteger(), nullable=True),
    sa.Column('end_period', sa.String(length=5), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['area_code'], ['bls_cw_areas.area_code'], ),
    sa.ForeignKeyConstraint(['item_code'], ['bls_cw_items.item_code'], ),
    sa.ForeignKeyConstraint(['periodicity_code'], ['bls_periodicity.periodicity_code'], ),
    sa.PrimaryKeyConstraint('series_id')
    )
    op.create_index('ix_bls_cw_series_active', 'bls_cw_series', ['is_active'], unique=False)
    op.create_index('ix_bls_cw_series_area', 'bls_cw_series', ['area_code'], unique=False)
    op.create_index('ix_bls_cw_series_item', 'bls_cw_series', ['item_code'], unique=False)
    op.create_index('ix_bls_cw_series_seasonal', 'bls_cw_series', ['seasonal_code'], unique=False)

    # Create bls_cw_data
    op.create_table('bls_cw_data',
    sa.Column('series_id', sa.String(length=20), nullable=False),
    sa.Column('year', sa.SmallInteger(), nullable=False),
    sa.Column('period', sa.String(length=5), nullable=False),
    sa.Column('value', sa.Numeric(precision=20, scale=6), nullable=True),
    sa.Column('footnote_codes', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['series_id'], ['bls_cw_series.series_id'], ),
    sa.PrimaryKeyConstraint('series_id', 'year', 'period')
    )
    op.create_index('ix_bls_cw_data_series_year', 'bls_cw_data', ['series_id', 'year'], unique=False)
    op.create_index('ix_bls_cw_data_year_period', 'bls_cw_data', ['year', 'period'], unique=False)

    # Create bls_cw_aspects
    op.create_table('bls_cw_aspects',
    sa.Column('series_id', sa.String(length=20), nullable=False),
    sa.Column('year', sa.SmallInteger(), nullable=False),
    sa.Column('period', sa.String(length=5), nullable=False),
    sa.Column('aspect_type', sa.String(length=5), nullable=False),
    sa.Column('value', sa.String(length=100), nullable=True),
    sa.Column('footnote_codes', sa.String(length=50), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.ForeignKeyConstraint(['series_id'], ['bls_cw_series.series_id'], ),
    sa.PrimaryKeyConstraint('series_id', 'year', 'period', 'aspect_type')
    )
    op.create_index('ix_bls_cw_aspects_series_year', 'bls_cw_aspects', ['series_id', 'year'], unique=False)
    op.create_index('ix_bls_cw_aspects_type', 'bls_cw_aspects', ['aspect_type'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop CW tables in reverse order
    op.drop_index('ix_bls_cw_aspects_type', table_name='bls_cw_aspects')
    op.drop_index('ix_bls_cw_aspects_series_year', table_name='bls_cw_aspects')
    op.drop_table('bls_cw_aspects')

    op.drop_index('ix_bls_cw_data_year_period', table_name='bls_cw_data')
    op.drop_index('ix_bls_cw_data_series_year', table_name='bls_cw_data')
    op.drop_table('bls_cw_data')

    op.drop_index('ix_bls_cw_series_seasonal', table_name='bls_cw_series')
    op.drop_index('ix_bls_cw_series_item', table_name='bls_cw_series')
    op.drop_index('ix_bls_cw_series_area', table_name='bls_cw_series')
    op.drop_index('ix_bls_cw_series_active', table_name='bls_cw_series')
    op.drop_table('bls_cw_series')

    op.drop_index('ix_bls_cw_items_selectable', table_name='bls_cw_items')
    op.drop_index('ix_bls_cw_items_display_level', table_name='bls_cw_items')
    op.drop_table('bls_cw_items')

    # Restore CU series foreign key to bls_areas
    op.drop_constraint(None, 'bls_cu_series', type_='foreignkey')
    op.create_foreign_key(op.f('bls_cu_series_area_code_fkey'), 'bls_cu_series', 'bls_areas', ['area_code'], ['area_code'])

    # Drop CU areas
    op.drop_index('ix_bls_cu_areas_selectable', table_name='bls_cu_areas')
    op.drop_index('ix_bls_cu_areas_display_level', table_name='bls_cu_areas')
    op.drop_table('bls_cu_areas')
