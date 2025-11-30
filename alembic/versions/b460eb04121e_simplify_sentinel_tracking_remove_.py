"""simplify sentinel tracking remove change_count

Revision ID: b460eb04121e
Revises: ff4550c62479
Create Date: 2025-11-28 19:08:58.610958

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b460eb04121e'
down_revision: Union[str, Sequence[str], None] = 'ff4550c62479'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('bea_sentinel_series', sa.Column('has_changed', sa.Boolean(), nullable=False, server_default='false'))
    op.drop_column('bea_sentinel_series', 'check_count')
    op.drop_column('bea_sentinel_series', 'change_count')
    op.drop_column('bea_sentinel_series', 'last_changed_at')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column('bea_sentinel_series', sa.Column('last_changed_at', sa.DateTime(), nullable=True))
    op.add_column('bea_sentinel_series', sa.Column('change_count', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('bea_sentinel_series', sa.Column('check_count', sa.Integer(), nullable=False, server_default='0'))
    op.drop_column('bea_sentinel_series', 'has_changed')
