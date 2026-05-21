"""add stewardship_queue.owner_email

Revision ID: 20260521_01
Revises: 20260509_03
Create Date: 2026-05-21

"""

from alembic import op

revision = "20260521_01"
down_revision = "20260509_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE stewardship_queue ADD COLUMN IF NOT EXISTS owner_email VARCHAR DEFAULT ''"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE stewardship_queue DROP COLUMN IF EXISTS owner_email")
