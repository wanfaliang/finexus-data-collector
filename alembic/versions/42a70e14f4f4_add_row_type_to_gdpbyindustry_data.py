"""add row_type to gdpbyindustry_data

Revision ID: 42a70e14f4f4
Revises: b460eb04121e
Create Date: 2025-11-28 19:35:06.622573

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '42a70e14f4f4'
down_revision: Union[str, Sequence[str], None] = 'b460eb04121e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema.

    Add row_type column to bea_gdpbyindustry_data to support tables 6 & 7
    which have multiple rows per industry (total, compensation, taxes, surplus).
    """
    # 1. Drop the existing primary key constraint
    op.drop_constraint('bea_gdpbyindustry_data_pkey', 'bea_gdpbyindustry_data', type_='primary')

    # 2. Add the row_type column with default 'total' for existing data
    op.add_column('bea_gdpbyindustry_data',
                  sa.Column('row_type', sa.String(20), nullable=False, server_default='total'))

    # 3. Create new primary key with all 5 columns
    op.create_primary_key(
        'bea_gdpbyindustry_data_pkey',
        'bea_gdpbyindustry_data',
        ['table_id', 'industry_code', 'frequency', 'time_period', 'row_type']
    )

    # 4. Add index for row_type
    op.create_index('ix_bea_gdpbyindustry_data_row_type', 'bea_gdpbyindustry_data', ['row_type'])


def downgrade() -> None:
    """Downgrade schema."""
    # 1. Drop the index
    op.drop_index('ix_bea_gdpbyindustry_data_row_type', table_name='bea_gdpbyindustry_data')

    # 2. Drop the primary key
    op.drop_constraint('bea_gdpbyindustry_data_pkey', 'bea_gdpbyindustry_data', type_='primary')

    # 3. Delete non-total rows (they won't fit in the old schema)
    op.execute("DELETE FROM bea_gdpbyindustry_data WHERE row_type != 'total'")

    # 4. Drop the row_type column
    op.drop_column('bea_gdpbyindustry_data', 'row_type')

    # 5. Recreate original primary key
    op.create_primary_key(
        'bea_gdpbyindustry_data_pkey',
        'bea_gdpbyindustry_data',
        ['table_id', 'industry_code', 'frequency', 'time_period']
    )
