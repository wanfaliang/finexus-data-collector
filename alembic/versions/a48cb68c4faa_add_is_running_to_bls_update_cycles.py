"""add_is_running_to_bls_update_cycles

Revision ID: a48cb68c4faa
Revises: a4b3db6964da
Create Date: 2025-12-01 07:41:35.908469

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a48cb68c4faa'
down_revision: Union[str, Sequence[str], None] = 'a4b3db6964da'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add is_running column to track active update operations."""
    op.add_column(
        'bls_update_cycles',
        sa.Column('is_running', sa.Boolean(), nullable=False, server_default='false')
    )
    op.create_index('ix_bls_update_cycles_is_running', 'bls_update_cycles', ['is_running'])


def downgrade() -> None:
    """Remove is_running column."""
    op.drop_index('ix_bls_update_cycles_is_running', table_name='bls_update_cycles')
    op.drop_column('bls_update_cycles', 'is_running')
