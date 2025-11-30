"""add frequency geo_scope year_spec to collection_runs

Revision ID: b3d896422414
Revises: 42a70e14f4f4
Create Date: 2025-11-28 21:29:57.642179

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3d896422414'
down_revision: Union[str, Sequence[str], None] = '42a70e14f4f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('bea_collection_runs', sa.Column('frequency', sa.String(1), nullable=True))
    op.add_column('bea_collection_runs', sa.Column('geo_scope', sa.String(20), nullable=True))
    op.add_column('bea_collection_runs', sa.Column('year_spec', sa.String(50), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('bea_collection_runs', 'year_spec')
    op.drop_column('bea_collection_runs', 'geo_scope')
    op.drop_column('bea_collection_runs', 'frequency')
