"""baseline enterprise features

Revision ID: 20260508_01
Revises:
Create Date: 2026-05-08
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "20260508_01"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS lineage_nodes (
            id SERIAL PRIMARY KEY,
            key VARCHAR NOT NULL UNIQUE,
            label VARCHAR NOT NULL,
            node_type VARCHAR NOT NULL DEFAULT 'dataset',
            system VARCHAR NOT NULL DEFAULT '',
            layer VARCHAR NOT NULL DEFAULT ''
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS lineage_edges (
            id SERIAL PRIMARY KEY,
            source_key VARCHAR NOT NULL,
            target_key VARCHAR NOT NULL,
            transformation VARCHAR NOT NULL DEFAULT '',
            criticality VARCHAR NOT NULL DEFAULT 'medium'
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_copilot_action_logs (
            id SERIAL PRIMARY KEY,
            action_key VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL DEFAULT 'unknown',
            status VARCHAR NOT NULL DEFAULT 'success',
            summary VARCHAR NOT NULL DEFAULT '',
            payload VARCHAR NOT NULL DEFAULT '',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ai_copilot_action_logs")
    op.execute("DROP TABLE IF EXISTS lineage_edges")
    op.execute("DROP TABLE IF EXISTS lineage_nodes")
