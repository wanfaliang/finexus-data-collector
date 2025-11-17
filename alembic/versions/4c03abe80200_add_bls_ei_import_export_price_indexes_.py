"""add_bls_ei_import_export_price_indexes_tables

Revision ID: 4c03abe80200
Revises: ed546fe6ba3a
Create Date: 2025-11-16 22:57:35.534948

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4c03abe80200'
down_revision: Union[str, Sequence[str], None] = 'ed546fe6ba3a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create bls_ei_indexes table
    op.create_table('bls_ei_indexes',
        sa.Column('index_code', sa.String(length=5), nullable=False),
        sa.Column('index_name', sa.String(length=255), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('index_code')
    )

    # Create bls_ei_series table
    op.create_table('bls_ei_series',
        sa.Column('series_id', sa.String(length=30), nullable=False),
        sa.Column('seasonal_code', sa.String(length=1), nullable=True),
        sa.Column('index_code', sa.String(length=5), nullable=True),
        sa.Column('series_name', sa.String(length=255), nullable=True),
        sa.Column('base_period', sa.String(length=50), nullable=True),
        sa.Column('series_title', sa.Text(), nullable=False),
        sa.Column('footnote_codes', sa.String(length=500), nullable=True),
        sa.Column('begin_year', sa.SmallInteger(), nullable=True),
        sa.Column('begin_period', sa.String(length=5), nullable=True),
        sa.Column('end_year', sa.SmallInteger(), nullable=True),
        sa.Column('end_period', sa.String(length=5), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['index_code'], ['bls_ei_indexes.index_code'], ),
        sa.PrimaryKeyConstraint('series_id')
    )
    op.create_index('ix_bls_ei_series_active', 'bls_ei_series', ['is_active'], unique=False)
    op.create_index('ix_bls_ei_series_index', 'bls_ei_series', ['index_code'], unique=False)
    op.create_index('ix_bls_ei_series_seasonal', 'bls_ei_series', ['seasonal_code'], unique=False)

    # Create bls_ei_data table
    op.create_table('bls_ei_data',
        sa.Column('series_id', sa.String(length=30), nullable=False),
        sa.Column('year', sa.SmallInteger(), nullable=False),
        sa.Column('period', sa.String(length=5), nullable=False),
        sa.Column('value', sa.Numeric(precision=20, scale=3), nullable=True),
        sa.Column('footnote_codes', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['series_id'], ['bls_ei_series.series_id'], ),
        sa.PrimaryKeyConstraint('series_id', 'year', 'period')
    )
    op.create_index('ix_bls_ei_data_series_year', 'bls_ei_data', ['series_id', 'year'], unique=False)
    op.create_index('ix_bls_ei_data_year_period', 'bls_ei_data', ['year', 'period'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop tables in reverse order
    op.drop_index('ix_bls_ei_data_year_period', table_name='bls_ei_data')
    op.drop_index('ix_bls_ei_data_series_year', table_name='bls_ei_data')
    op.drop_table('bls_ei_data')

    op.drop_index('ix_bls_ei_series_seasonal', table_name='bls_ei_series')
    op.drop_index('ix_bls_ei_series_index', table_name='bls_ei_series')
    op.drop_index('ix_bls_ei_series_active', table_name='bls_ei_series')
    op.drop_table('bls_ei_series')

    op.drop_table('bls_ei_indexes')
