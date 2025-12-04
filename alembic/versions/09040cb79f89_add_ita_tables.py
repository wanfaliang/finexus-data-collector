"""add_ita_tables

Revision ID: 09040cb79f89
Revises: a48cb68c4faa
Create Date: 2025-12-01 08:01:32.372777

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '09040cb79f89'
down_revision: Union[str, Sequence[str], None] = 'a48cb68c4faa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - add ITA (International Transactions Accounts) tables."""
    # Create ITA indicators catalog
    op.create_table('bea_ita_indicators',
        sa.Column('indicator_code', sa.String(length=50), nullable=False),
        sa.Column('indicator_description', sa.Text(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('indicator_code')
    )
    op.create_index('ix_bea_ita_indicators_active', 'bea_ita_indicators', ['is_active'], unique=False)

    # Create ITA areas/countries catalog
    op.create_table('bea_ita_areas',
        sa.Column('area_code', sa.String(length=50), nullable=False),
        sa.Column('area_name', sa.String(length=255), nullable=False),
        sa.Column('area_type', sa.String(length=50), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('area_code')
    )
    op.create_index('ix_bea_ita_areas_active', 'bea_ita_areas', ['is_active'], unique=False)
    op.create_index('ix_bea_ita_areas_type', 'bea_ita_areas', ['area_type'], unique=False)

    # Create ITA data table
    op.create_table('bea_ita_data',
        sa.Column('indicator_code', sa.String(length=50), nullable=False),
        sa.Column('area_code', sa.String(length=50), nullable=False),
        sa.Column('frequency', sa.String(length=10), nullable=False),
        sa.Column('time_period', sa.String(length=10), nullable=False),
        sa.Column('value', sa.Numeric(precision=20, scale=6), nullable=True),
        sa.Column('time_series_id', sa.String(length=100), nullable=True),
        sa.Column('time_series_description', sa.Text(), nullable=True),
        sa.Column('cl_unit', sa.String(length=50), nullable=True),
        sa.Column('unit_mult', sa.SmallInteger(), nullable=True),
        sa.Column('note_ref', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['area_code'], ['bea_ita_areas.area_code'], ),
        sa.ForeignKeyConstraint(['indicator_code'], ['bea_ita_indicators.indicator_code'], ),
        sa.PrimaryKeyConstraint('indicator_code', 'area_code', 'frequency', 'time_period')
    )
    op.create_index('ix_bea_ita_data_indicator', 'bea_ita_data', ['indicator_code'], unique=False)
    op.create_index('ix_bea_ita_data_area', 'bea_ita_data', ['area_code'], unique=False)
    op.create_index('ix_bea_ita_data_freq', 'bea_ita_data', ['frequency'], unique=False)
    op.create_index('ix_bea_ita_data_period', 'bea_ita_data', ['time_period'], unique=False)
    op.create_index('ix_bea_ita_data_indicator_area', 'bea_ita_data', ['indicator_code', 'area_code'], unique=False)
    op.create_index('ix_bea_ita_data_indicator_period', 'bea_ita_data', ['indicator_code', 'time_period'], unique=False)


def downgrade() -> None:
    """Downgrade schema - remove ITA tables."""
    op.drop_index('ix_bea_ita_data_indicator_period', table_name='bea_ita_data')
    op.drop_index('ix_bea_ita_data_indicator_area', table_name='bea_ita_data')
    op.drop_index('ix_bea_ita_data_period', table_name='bea_ita_data')
    op.drop_index('ix_bea_ita_data_freq', table_name='bea_ita_data')
    op.drop_index('ix_bea_ita_data_area', table_name='bea_ita_data')
    op.drop_index('ix_bea_ita_data_indicator', table_name='bea_ita_data')
    op.drop_table('bea_ita_data')

    op.drop_index('ix_bea_ita_areas_type', table_name='bea_ita_areas')
    op.drop_index('ix_bea_ita_areas_active', table_name='bea_ita_areas')
    op.drop_table('bea_ita_areas')

    op.drop_index('ix_bea_ita_indicators_active', table_name='bea_ita_indicators')
    op.drop_table('bea_ita_indicators')
