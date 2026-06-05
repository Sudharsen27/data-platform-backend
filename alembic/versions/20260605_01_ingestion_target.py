"""add ingestion_jobs.target column

Revision ID: 20260605_01
Revises: 20260602_01
Create Date: 2026-06-05
"""

from typing import Sequence, Union

from alembic import op

revision: str = "20260605_01"
down_revision: Union[str, Sequence[str], None] = "20260602_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS target VARCHAR NOT NULL DEFAULT 'quarantine'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE ingestion_jobs DROP COLUMN IF EXISTS target")
