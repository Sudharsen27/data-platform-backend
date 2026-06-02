"""create stewardship_queue table

Revision ID: 20260509_01
Revises: 20260508_01
Create Date: 2026-05-09

"""

from typing import Sequence, Union

from alembic import op

revision: str = "20260509_01"
down_revision: Union[str, Sequence[str], None] = "20260508_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS stewardship_queue (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL DEFAULT '',
            issue VARCHAR NOT NULL DEFAULT '',
            status VARCHAR NOT NULL DEFAULT 'pending',
            owner_email VARCHAR NOT NULL DEFAULT ''
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS stewardship_queue")
