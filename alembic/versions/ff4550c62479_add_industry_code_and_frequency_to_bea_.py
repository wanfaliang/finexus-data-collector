"""add industry_code and frequency to bea_sentinel_series

Revision ID: ff4550c62479
Revises: 448b02687a4e
Create Date: 2025-11-28 16:41:10.257101

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ff4550c62479'
down_revision: Union[str, Sequence[str], None] = '448b02687a4e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('bea_sentinel_series', sa.Column('industry_code', sa.String(length=20), nullable=True))
    op.add_column('bea_sentinel_series', sa.Column('frequency', sa.String(length=1), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('bea_sentinel_series', 'frequency')
    op.drop_column('bea_sentinel_series', 'industry_code')
