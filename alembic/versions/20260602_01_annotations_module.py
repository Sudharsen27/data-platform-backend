"""add record annotations and annotation history tables

Revision ID: 20260602_01
Revises: 20260529_01
Create Date: 2026-06-02
"""

from typing import Sequence, Union

from alembic import op

revision: str = "20260602_01"
down_revision: Union[str, Sequence[str], None] = "20260529_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS record_annotations (
            id SERIAL PRIMARY KEY,
            record_id INTEGER NOT NULL,
            comment VARCHAR NOT NULL DEFAULT '',
            status VARCHAR NOT NULL DEFAULT 'needs_review',
            created_by VARCHAR NOT NULL DEFAULT 'unknown',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS annotation_history (
            id SERIAL PRIMARY KEY,
            annotation_id INTEGER NOT NULL,
            action VARCHAR NOT NULL DEFAULT 'create',
            old_value VARCHAR NOT NULL DEFAULT '',
            new_value VARCHAR NOT NULL DEFAULT '',
            acted_by VARCHAR NOT NULL DEFAULT 'unknown',
            acted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_record_annotations_record_id ON record_annotations (record_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_record_annotations_status ON record_annotations (status)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_record_annotations_created_at ON record_annotations (created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_annotation_history_annotation_id ON annotation_history (annotation_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_annotation_history_acted_at ON annotation_history (acted_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_annotation_history_acted_at")
    op.execute("DROP INDEX IF EXISTS ix_annotation_history_annotation_id")
    op.execute("DROP INDEX IF EXISTS ix_record_annotations_created_at")
    op.execute("DROP INDEX IF EXISTS ix_record_annotations_status")
    op.execute("DROP INDEX IF EXISTS ix_record_annotations_record_id")
    op.execute("DROP TABLE IF EXISTS annotation_history")
    op.execute("DROP TABLE IF EXISTS record_annotations")
