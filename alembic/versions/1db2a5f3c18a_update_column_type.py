"""update column type

Revision ID: 1db2a5f3c18a
Revises: 98ac0bc250f4
Create Date: 2025-12-30 17:14:41.916113

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1db2a5f3c18a'
down_revision: Union[str, Sequence[str], None] = '98ac0bc250f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    op.execute("""
        ALTER TABLE data_check
        ALTER COLUMN silver_process
        TYPE BOOLEAN
        USING CASE
            WHEN silver_process = 1 THEN TRUE
            WHEN silver_process = 0 THEN FALSE
            ELSE NULL
        END
    """)


def downgrade():
    op.execute("""
        ALTER TABLE data_check
        ALTER COLUMN silver_process
        TYPE INTEGER
        USING CASE
            WHEN silver_process = TRUE THEN 1
            WHEN silver_process = FALSE THEN 0
            ELSE NULL
        END
    """)