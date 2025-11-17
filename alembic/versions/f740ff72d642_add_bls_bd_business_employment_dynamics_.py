"""add_bls_bd_business_employment_dynamics_tables

Revision ID: f740ff72d642
Revises: 4c03abe80200
Create Date: 2025-11-16 23:04:08.242383

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f740ff72d642'
down_revision: Union[str, Sequence[str], None] = '4c03abe80200'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create reference tables first (no foreign keys)
    op.create_table('bls_bd_states',
        sa.Column('state_code', sa.String(length=5), nullable=False),
        sa.Column('state_name', sa.String(length=100), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('state_code')
    )

    op.create_table('bls_bd_industries',
        sa.Column('industry_code', sa.String(length=10), nullable=False),
        sa.Column('industry_name', sa.String(length=255), nullable=False),
        sa.Column('display_level', sa.SmallInteger(), nullable=True),
        sa.Column('selectable', sa.String(length=1), nullable=True),
        sa.Column('sort_sequence', sa.SmallInteger(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('industry_code')
    )

    op.create_table('bls_bd_dataclasses',
        sa.Column('dataclass_code', sa.String(length=5), nullable=False),
        sa.Column('dataclass_name', sa.String(length=255), nullable=False),
        sa.Column('display_level', sa.SmallInteger(), nullable=True),
        sa.Column('selectable', sa.String(length=1), nullable=True),
        sa.Column('sort_sequence', sa.SmallInteger(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('dataclass_code')
    )

    op.create_table('bls_bd_dataelements',
        sa.Column('dataelement_code', sa.String(length=5), nullable=False),
        sa.Column('dataelement_name', sa.String(length=100), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('dataelement_code')
    )

    op.create_table('bls_bd_sizeclasses',
        sa.Column('sizeclass_code', sa.String(length=5), nullable=False),
        sa.Column('sizeclass_name', sa.String(length=255), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('sizeclass_code')
    )

    op.create_table('bls_bd_ratelevels',
        sa.Column('ratelevel_code', sa.String(length=5), nullable=False),
        sa.Column('ratelevel_name', sa.String(length=50), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('ratelevel_code')
    )

    op.create_table('bls_bd_unitanalysis',
        sa.Column('unitanalysis_code', sa.String(length=5), nullable=False),
        sa.Column('unitanalysis_name', sa.String(length=100), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('unitanalysis_code')
    )

    op.create_table('bls_bd_ownership',
        sa.Column('ownership_code', sa.String(length=5), nullable=False),
        sa.Column('ownership_name', sa.String(length=100), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('ownership_code')
    )

    # Create series table with foreign keys
    op.create_table('bls_bd_series',
        sa.Column('series_id', sa.String(length=30), nullable=False),
        sa.Column('seasonal_code', sa.String(length=1), nullable=True),
        sa.Column('msa_code', sa.String(length=10), nullable=True),
        sa.Column('state_code', sa.String(length=5), nullable=True),
        sa.Column('county_code', sa.String(length=10), nullable=True),
        sa.Column('industry_code', sa.String(length=10), nullable=True),
        sa.Column('unitanalysis_code', sa.String(length=5), nullable=True),
        sa.Column('dataelement_code', sa.String(length=5), nullable=True),
        sa.Column('sizeclass_code', sa.String(length=5), nullable=True),
        sa.Column('dataclass_code', sa.String(length=5), nullable=True),
        sa.Column('ratelevel_code', sa.String(length=5), nullable=True),
        sa.Column('periodicity_code', sa.String(length=5), nullable=True),
        sa.Column('ownership_code', sa.String(length=5), nullable=True),
        sa.Column('series_title', sa.Text(), nullable=False),
        sa.Column('footnote_codes', sa.String(length=500), nullable=True),
        sa.Column('begin_year', sa.SmallInteger(), nullable=True),
        sa.Column('begin_period', sa.String(length=5), nullable=True),
        sa.Column('end_year', sa.SmallInteger(), nullable=True),
        sa.Column('end_period', sa.String(length=5), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['state_code'], ['bls_bd_states.state_code'], ),
        sa.ForeignKeyConstraint(['industry_code'], ['bls_bd_industries.industry_code'], ),
        sa.ForeignKeyConstraint(['unitanalysis_code'], ['bls_bd_unitanalysis.unitanalysis_code'], ),
        sa.ForeignKeyConstraint(['dataelement_code'], ['bls_bd_dataelements.dataelement_code'], ),
        sa.ForeignKeyConstraint(['sizeclass_code'], ['bls_bd_sizeclasses.sizeclass_code'], ),
        sa.ForeignKeyConstraint(['dataclass_code'], ['bls_bd_dataclasses.dataclass_code'], ),
        sa.ForeignKeyConstraint(['ratelevel_code'], ['bls_bd_ratelevels.ratelevel_code'], ),
        sa.ForeignKeyConstraint(['periodicity_code'], ['bls_periodicity.periodicity_code'], ),
        sa.ForeignKeyConstraint(['ownership_code'], ['bls_bd_ownership.ownership_code'], ),
        sa.PrimaryKeyConstraint('series_id')
    )
    op.create_index('ix_bls_bd_series_active', 'bls_bd_series', ['is_active'], unique=False)
    op.create_index('ix_bls_bd_series_dataclass', 'bls_bd_series', ['dataclass_code'], unique=False)
    op.create_index('ix_bls_bd_series_dataelement', 'bls_bd_series', ['dataelement_code'], unique=False)
    op.create_index('ix_bls_bd_series_industry', 'bls_bd_series', ['industry_code'], unique=False)
    op.create_index('ix_bls_bd_series_sizeclass', 'bls_bd_series', ['sizeclass_code'], unique=False)
    op.create_index('ix_bls_bd_series_state', 'bls_bd_series', ['state_code'], unique=False)

    # Create data table
    op.create_table('bls_bd_data',
        sa.Column('series_id', sa.String(length=30), nullable=False),
        sa.Column('year', sa.SmallInteger(), nullable=False),
        sa.Column('period', sa.String(length=5), nullable=False),
        sa.Column('value', sa.Numeric(precision=20, scale=1), nullable=True),
        sa.Column('footnote_codes', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['series_id'], ['bls_bd_series.series_id'], ),
        sa.PrimaryKeyConstraint('series_id', 'year', 'period')
    )
    op.create_index('ix_bls_bd_data_series_year', 'bls_bd_data', ['series_id', 'year'], unique=False)
    op.create_index('ix_bls_bd_data_year_period', 'bls_bd_data', ['year', 'period'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop tables in reverse order
    op.drop_index('ix_bls_bd_data_year_period', table_name='bls_bd_data')
    op.drop_index('ix_bls_bd_data_series_year', table_name='bls_bd_data')
    op.drop_table('bls_bd_data')

    op.drop_index('ix_bls_bd_series_state', table_name='bls_bd_series')
    op.drop_index('ix_bls_bd_series_sizeclass', table_name='bls_bd_series')
    op.drop_index('ix_bls_bd_series_industry', table_name='bls_bd_series')
    op.drop_index('ix_bls_bd_series_dataelement', table_name='bls_bd_series')
    op.drop_index('ix_bls_bd_series_dataclass', table_name='bls_bd_series')
    op.drop_index('ix_bls_bd_series_active', table_name='bls_bd_series')
    op.drop_table('bls_bd_series')

    op.drop_table('bls_bd_ownership')
    op.drop_table('bls_bd_unitanalysis')
    op.drop_table('bls_bd_ratelevels')
    op.drop_table('bls_bd_sizeclasses')
    op.drop_table('bls_bd_dataelements')
    op.drop_table('bls_bd_dataclasses')
    op.drop_table('bls_bd_industries')
    op.drop_table('bls_bd_states')
