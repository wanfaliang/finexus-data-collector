"""increase_ita_indicator_code_size

Revision ID: 5e6889152bca
Revises: 09040cb79f89
Create Date: 2025-12-01 08:51:01.973800

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5e6889152bca'
down_revision: Union[str, Sequence[str], None] = '09040cb79f89'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - increase indicator_code from 50 to 100 chars.

    Some ITA indicator codes are very long, e.g.:
    'DiInvReinvestEarnWithoutCurrCostAdjAssetsHoldExcBank' (52 chars)
    """
    op.alter_column('bea_ita_indicators', 'indicator_code',
               existing_type=sa.VARCHAR(length=50),
               type_=sa.String(length=100),
               existing_nullable=False)
    op.alter_column('bea_ita_data', 'indicator_code',
               existing_type=sa.VARCHAR(length=50),
               type_=sa.String(length=100),
               existing_nullable=False)


def downgrade() -> None:
    """Downgrade schema - revert indicator_code back to 50 chars."""
    op.alter_column('bea_ita_data', 'indicator_code',
               existing_type=sa.String(length=100),
               type_=sa.VARCHAR(length=50),
               existing_nullable=False)
    op.alter_column('bea_ita_indicators', 'indicator_code',
               existing_type=sa.String(length=100),
               type_=sa.VARCHAR(length=50),
               existing_nullable=False)
