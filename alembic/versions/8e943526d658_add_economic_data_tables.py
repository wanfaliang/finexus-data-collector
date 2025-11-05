"""Add economic data tables

Revision ID: 8e943526d658
Revises: b6dcd324a613
Create Date: 2025-11-05 10:48:52.541897

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '8e943526d658'
down_revision: Union[str, Sequence[str], None] = 'b6dcd324a613'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Step 1: Drop old economic_indicators table completely (different structure)
    op.drop_index(op.f('ix_economic_indicators_date'), table_name='economic_indicators')
    op.drop_index(op.f('ix_economic_indicators_name_date'), table_name='economic_indicators')
    op.drop_table('economic_indicators')

    # Step 2: Create new economic_indicators table (metadata only, no time series data)
    op.create_table('economic_indicators',
    sa.Column('indicator_code', sa.String(length=100), nullable=False),
    sa.Column('indicator_name', sa.String(length=255), nullable=True),
    sa.Column('source', sa.String(length=20), nullable=True),
    sa.Column('source_series_id', sa.String(length=100), nullable=True),
    sa.Column('native_frequency', sa.String(length=20), nullable=True),
    sa.Column('units', sa.String(length=100), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('indicator_code')
    )

    # Step 3: Now create the data tables with foreign keys
    op.create_table('economic_data_raw',
    sa.Column('indicator_code', sa.String(length=100), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('value', sa.Numeric(precision=20, scale=6), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['indicator_code'], ['economic_indicators.indicator_code'], ),
    sa.PrimaryKeyConstraint('indicator_code', 'date')
    )
    op.create_index('ix_economic_data_raw_date', 'economic_data_raw', ['date'], unique=False)

    op.create_table('economic_data_monthly',
    sa.Column('indicator_code', sa.String(length=100), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('value', sa.Numeric(precision=20, scale=6), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['indicator_code'], ['economic_indicators.indicator_code'], ),
    sa.PrimaryKeyConstraint('indicator_code', 'date')
    )
    op.create_index('ix_economic_data_monthly_date', 'economic_data_monthly', ['date'], unique=False)

    op.create_table('economic_data_quarterly',
    sa.Column('indicator_code', sa.String(length=100), nullable=False),
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('value', sa.Numeric(precision=20, scale=6), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.ForeignKeyConstraint(['indicator_code'], ['economic_indicators.indicator_code'], ),
    sa.PrimaryKeyConstraint('indicator_code', 'date')
    )
    op.create_index('ix_economic_data_quarterly_date', 'economic_data_quarterly', ['date'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Drop new tables
    op.drop_index('ix_economic_data_quarterly_date', table_name='economic_data_quarterly')
    op.drop_table('economic_data_quarterly')
    op.drop_index('ix_economic_data_monthly_date', table_name='economic_data_monthly')
    op.drop_table('economic_data_monthly')
    op.drop_index('ix_economic_data_raw_date', table_name='economic_data_raw')
    op.drop_table('economic_data_raw')

    # Drop new economic_indicators table
    op.drop_table('economic_indicators')

    # Recreate old economic_indicators table structure
    op.create_table('economic_indicators',
    sa.Column('indicator_name', sa.VARCHAR(length=100), nullable=False),
    sa.Column('date', sa.DATE(), nullable=False),
    sa.Column('value', sa.NUMERIC(precision=20, scale=6), nullable=True),
    sa.Column('frequency', sa.VARCHAR(length=20), nullable=True),
    sa.Column('series_id', sa.VARCHAR(length=50), nullable=True),
    sa.Column('units', sa.VARCHAR(length=100), nullable=True),
    sa.Column('seasonal_adjustment', sa.VARCHAR(length=50), nullable=True),
    sa.Column('last_updated', postgresql.TIMESTAMP(), nullable=False),
    sa.PrimaryKeyConstraint('indicator_name', 'date')
    )
    op.create_index(op.f('ix_economic_indicators_date'), 'economic_indicators', ['date'], unique=False)
    op.create_index(op.f('ix_economic_indicators_name_date'), 'economic_indicators', ['indicator_name', 'date'], unique=False)
