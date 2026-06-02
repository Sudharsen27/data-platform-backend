"""index stewardship_queue.status for filter/count performance

Revision ID: 20260509_02
Revises: 20260509_01
Create Date: 2026-05-09
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260509_02"
down_revision: Union[str, Sequence[str], None] = "20260509_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_stewardship_queue_status
        ON stewardship_queue (status)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_stewardship_queue_status")
