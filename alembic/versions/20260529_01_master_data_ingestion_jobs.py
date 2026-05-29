"""create master_data and ingestion_jobs tables

Revision ID: 20260529_01
Revises: 20260521_01
Create Date: 2026-05-29
"""

from typing import Sequence, Union

from alembic import op

revision: str = "20260529_01"
down_revision: Union[str, Sequence[str], None] = "20260521_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS master_data (
            id SERIAL PRIMARY KEY,
            source_queue_id INTEGER NOT NULL,
            name VARCHAR NOT NULL,
            email VARCHAR NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_master_data_id
        ON master_data (id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_master_data_source_queue_id
        ON master_data (source_queue_id)
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_jobs (
            id SERIAL PRIMARY KEY,
            filename VARCHAR NOT NULL DEFAULT '',
            status VARCHAR NOT NULL DEFAULT 'queued',
            total_rows INTEGER NOT NULL DEFAULT 0,
            processed_rows INTEGER NOT NULL DEFAULT 0,
            inserted_rows INTEGER NOT NULL DEFAULT 0,
            error_message VARCHAR NOT NULL DEFAULT '',
            created_by VARCHAR NOT NULL DEFAULT 'system',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP NULL,
            completed_at TIMESTAMP NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_id
        ON ingestion_jobs (id)
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_status
        ON ingestion_jobs (status)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_ingestion_jobs_status")
    op.execute("DROP INDEX IF EXISTS ix_ingestion_jobs_id")
    op.execute("DROP TABLE IF EXISTS ingestion_jobs")
    op.execute("DROP INDEX IF EXISTS ix_master_data_source_queue_id")
    op.execute("DROP INDEX IF EXISTS ix_master_data_id")
    op.execute("DROP TABLE IF EXISTS master_data")
