"""revamp_bls_update_tracking_with_cycles

Revision ID: a4b3db6964da
Revises: b3d896422414
Create Date: 2025-11-30 14:05:36.982931

This migration:
1. Creates new bls_update_cycles table
2. Creates new bls_update_cycle_series table
3. Drops deprecated tables: bls_series_update_status, bls_survey_sentinels, bls_survey_freshness
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a4b3db6964da'
down_revision: Union[str, Sequence[str], None] = 'b3d896422414'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create new bls_update_cycles table
    op.create_table(
        'bls_update_cycles',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('survey_code', sa.String(5), nullable=False),
        sa.Column('is_current', sa.Boolean(), nullable=False, default=True),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('total_series', sa.Integer(), nullable=False, default=0),
        sa.Column('series_updated', sa.Integer(), nullable=False, default=0),
        sa.Column('requests_used', sa.Integer(), nullable=False, default=0),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_bls_update_cycles_survey_code', 'bls_update_cycles', ['survey_code'])
    op.create_index('ix_bls_update_cycles_is_current', 'bls_update_cycles', ['is_current'])

    # Create new bls_update_cycle_series table
    op.create_table(
        'bls_update_cycle_series',
        sa.Column('cycle_id', sa.Integer(), nullable=False),
        sa.Column('series_id', sa.String(30), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('cycle_id', 'series_id'),
        sa.ForeignKeyConstraint(['cycle_id'], ['bls_update_cycles.id'], ondelete='CASCADE')
    )
    op.create_index('ix_bls_update_cycle_series_cycle_id', 'bls_update_cycle_series', ['cycle_id'])

    # Drop deprecated tables
    op.drop_table('bls_series_update_status')
    op.drop_table('bls_survey_sentinels')
    op.drop_table('bls_survey_freshness')


def downgrade() -> None:
    """Downgrade schema."""
    # Recreate deprecated tables
    op.create_table(
        'bls_series_update_status',
        sa.Column('series_id', sa.String(30), nullable=False),
        sa.Column('survey_code', sa.String(5), nullable=False),
        sa.Column('last_data_year', sa.SmallInteger(), nullable=True),
        sa.Column('last_data_period', sa.String(5), nullable=True),
        sa.Column('last_checked_at', sa.DateTime(), nullable=False),
        sa.Column('last_updated_at', sa.DateTime(), nullable=True),
        sa.Column('is_current', sa.Boolean(), nullable=False, default=False),
        sa.PrimaryKeyConstraint('series_id')
    )
    op.create_index('ix_bls_series_update_status_survey_code', 'bls_series_update_status', ['survey_code'])
    op.create_index('ix_bls_series_update_status_is_current', 'bls_series_update_status', ['is_current'])

    op.create_table(
        'bls_survey_sentinels',
        sa.Column('survey_code', sa.String(5), nullable=False),
        sa.Column('series_id', sa.String(30), nullable=False),
        sa.Column('sentinel_order', sa.Integer(), nullable=False),
        sa.Column('selection_reason', sa.String(50), nullable=True),
        sa.Column('last_value', sa.Numeric(20, 6), nullable=True),
        sa.Column('last_year', sa.SmallInteger(), nullable=True),
        sa.Column('last_period', sa.String(5), nullable=True),
        sa.Column('last_footnotes', sa.String(500), nullable=True),
        sa.Column('last_checked_at', sa.DateTime(), nullable=True),
        sa.Column('last_changed_at', sa.DateTime(), nullable=True),
        sa.Column('check_count', sa.Integer(), nullable=False, default=0),
        sa.Column('change_count', sa.Integer(), nullable=False, default=0),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('survey_code', 'series_id')
    )

    op.create_table(
        'bls_survey_freshness',
        sa.Column('survey_code', sa.String(5), nullable=False),
        sa.Column('last_bls_update_detected', sa.DateTime(), nullable=True),
        sa.Column('last_sentinel_check', sa.DateTime(), nullable=True),
        sa.Column('sentinels_changed', sa.Integer(), nullable=False, default=0),
        sa.Column('sentinels_total', sa.Integer(), nullable=False, default=50),
        sa.Column('needs_full_update', sa.Boolean(), nullable=False, default=False),
        sa.Column('last_full_update_started', sa.DateTime(), nullable=True),
        sa.Column('last_full_update_completed', sa.DateTime(), nullable=True),
        sa.Column('full_update_in_progress', sa.Boolean(), nullable=False, default=False),
        sa.Column('series_updated_count', sa.Integer(), nullable=False, default=0),
        sa.Column('series_total_count', sa.Integer(), nullable=False, default=0),
        sa.Column('bls_update_frequency_days', sa.Numeric(5, 2), nullable=True),
        sa.Column('total_checks', sa.Integer(), nullable=False, default=0),
        sa.Column('total_updates_detected', sa.Integer(), nullable=False, default=0),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('survey_code')
    )
    op.create_index('ix_bls_survey_freshness_last_sentinel_check', 'bls_survey_freshness', ['last_sentinel_check'])
    op.create_index('ix_bls_survey_freshness_needs_full_update', 'bls_survey_freshness', ['needs_full_update'])

    # Drop new tables
    op.drop_table('bls_update_cycle_series')
    op.drop_table('bls_update_cycles')
