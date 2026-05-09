"""catalog_assets.lineage_node_key + backfill from lineage_nodes

Revision ID: 20260509_03
Revises: 20260509_02
Create Date: 2026-05-09
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260509_03"
down_revision: Union[str, Sequence[str], None] = "20260509_02"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE catalog_assets
        ADD COLUMN IF NOT EXISTS lineage_node_key VARCHAR NOT NULL DEFAULT ''
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_catalog_assets_lineage_node_key
        ON catalog_assets (lineage_node_key)
        """
    )
    op.execute(
        """
        UPDATE catalog_assets AS ca
        SET lineage_node_key = ca.asset_key
        FROM lineage_nodes AS ln
        WHERE (ca.lineage_node_key IS NULL OR ca.lineage_node_key = '')
          AND ln.key = ca.asset_key
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_catalog_assets_lineage_node_key")
    op.execute("ALTER TABLE catalog_assets DROP COLUMN IF EXISTS lineage_node_key")
