"""add bls_cw_areas table and update cw series foreign key

Revision ID: addd58cd7f11
Revises: 7db74d2b38d3
Create Date: 2025-11-16 19:24:13.958304

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'addd58cd7f11'
down_revision: Union[str, Sequence[str], None] = '7db74d2b38d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Only creates bls_cw_areas (this was already applied)
    op.create_table('bls_cw_areas',
    sa.Column('area_code', sa.String(length=10), nullable=False),
    sa.Column('area_name', sa.String(length=255), nullable=False),
    sa.Column('display_level', sa.SmallInteger(), nullable=True),
    sa.Column('selectable', sa.String(length=1), nullable=True),
    sa.Column('sort_sequence', sa.SmallInteger(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('area_code')
    )
    op.create_index('ix_bls_cw_areas_display_level', 'bls_cw_areas', ['display_level'], unique=False)
    op.create_index('ix_bls_cw_areas_selectable', 'bls_cw_areas', ['selectable'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_bls_cw_areas_selectable', table_name='bls_cw_areas')
    op.drop_index('ix_bls_cw_areas_display_level', table_name='bls_cw_areas')
    op.drop_table('bls_cw_areas')
