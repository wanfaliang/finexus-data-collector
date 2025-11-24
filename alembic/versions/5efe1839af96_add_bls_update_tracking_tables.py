"""add_bls_update_tracking_tables

Revision ID: 5efe1839af96
Revises: f740ff72d642
Create Date: 2025-11-18 22:47:13.278736

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5efe1839af96'
down_revision: Union[str, Sequence[str], None] = 'f740ff72d642'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create series update status table
    op.create_table('bls_series_update_status',
        sa.Column('series_id', sa.String(length=30), nullable=False),
        sa.Column('survey_code', sa.String(length=5), nullable=False),
        sa.Column('last_data_year', sa.SmallInteger(), nullable=True),
        sa.Column('last_data_period', sa.String(length=5), nullable=True),
        sa.Column('last_checked_at', sa.DateTime(), nullable=False),
        sa.Column('last_updated_at', sa.DateTime(), nullable=True),
        sa.Column('is_current', sa.Boolean(), nullable=False, server_default='false'),
        sa.PrimaryKeyConstraint('series_id')
    )
    op.create_index('ix_bls_series_status_survey_current', 'bls_series_update_status',
                    ['survey_code', 'is_current'], unique=False)

    # Create API usage log table
    op.create_table('bls_api_usage_log',
        sa.Column('log_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('usage_date', sa.Date(), nullable=False),
        sa.Column('requests_used', sa.Integer(), nullable=False),
        sa.Column('series_count', sa.Integer(), nullable=False),
        sa.Column('survey_code', sa.String(length=5), nullable=True),
        sa.Column('execution_time', sa.DateTime(), nullable=False, server_default=sa.text('NOW()')),
        sa.Column('script_name', sa.String(length=100), nullable=True),
        sa.PrimaryKeyConstraint('log_id')
    )
    op.create_index('ix_bls_api_usage_date', 'bls_api_usage_log', ['usage_date'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_bls_api_usage_date', table_name='bls_api_usage_log')
    op.drop_table('bls_api_usage_log')

    op.drop_index('ix_bls_series_status_survey_current', table_name='bls_series_update_status')
    op.drop_table('bls_series_update_status')
