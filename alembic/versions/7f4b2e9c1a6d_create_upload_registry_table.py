"""create upload_registry table

Revision ID: 7f4b2e9c1a6d
Revises: 5c1a0e4f7b2d
Create Date: 2026-03-14 13:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "7f4b2e9c1a6d"
down_revision: Union[str, Sequence[str], None] = "a7b2c8d9e0f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - create upload_registry table."""
    op.create_table(
        "upload_registry",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("org_id", sa.String(length=64), nullable=False),
        sa.Column("workspace_id", sa.String(length=64), nullable=False),
        sa.Column("uploaded_by", sa.String(length=64), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("source_path", sa.Text(), nullable=True),
        sa.Column("column_schema", postgresql.JSONB(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("is_append", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")),
        sa.Column("job_status", sa.String(length=16), nullable=False, server_default=sa.text("'COMPLETED'")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("file_hash", sa.Text(), nullable=True),
    )

    op.create_index("ix_upload_registry_org_id", "upload_registry", ["org_id"])
    op.create_index("ix_upload_registry_workspace_id", "upload_registry", ["workspace_id"])
    op.create_index("ix_upload_registry_uploaded_by", "upload_registry", ["uploaded_by"])


def downgrade() -> None:
    """Downgrade schema - drop upload_registry table."""
    op.drop_index("ix_upload_registry_uploaded_by", table_name="upload_registry")
    op.drop_index("ix_upload_registry_workspace_id", table_name="upload_registry")
    op.drop_index("ix_upload_registry_org_id", table_name="upload_registry")
    op.drop_table("upload_registry")
