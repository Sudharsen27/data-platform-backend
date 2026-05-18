import os

from sqlalchemy.orm import Session

from app.db.snowflake import get_snowflake_connection
from app.models import QuarantineData, Rule

# Snowflake limits expression lists to 200_000; each quarantine row uses 4 bind values.
_DEFAULT_BATCH_ROWS = 10_000


def _sync_batch_size() -> int:
    try:
        return max(100, min(50_000, int(os.getenv("SNOWFLAKE_SYNC_BATCH_SIZE", str(_DEFAULT_BATCH_ROWS)))))
    except ValueError:
        return _DEFAULT_BATCH_ROWS


def _iter_batches(items: list, batch_size: int):
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def _executemany_batched(cursor, sql: str, rows: list, *, batch_size: int | None = None) -> int:
    size = batch_size if batch_size is not None else _sync_batch_size()
    inserted = 0
    for chunk in _iter_batches(rows, size):
        if chunk:
            cursor.executemany(sql, chunk)
            inserted += len(chunk)
    return inserted


def _create_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantine_data (
            id INTEGER,
            name STRING,
            email STRING,
            error STRING
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER,
            field STRING,
            rule STRING,
            status STRING
        )
        """
    )


def sync_postgres_to_snowflake(db: Session):
    quarantine_rows = db.query(QuarantineData).order_by(QuarantineData.id.asc()).all()
    rule_rows = db.query(Rule).order_by(Rule.id.asc()).all()

    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:
        _create_tables(cursor)

        cursor.execute("TRUNCATE TABLE quarantine_data")
        cursor.execute("TRUNCATE TABLE rules")

        if quarantine_rows:
            _executemany_batched(
                cursor,
                "INSERT INTO quarantine_data (id, name, email, error) VALUES (%s, %s, %s, %s)",
                [(row.id, row.name, row.email, row.error) for row in quarantine_rows],
            )

        if rule_rows:
            _executemany_batched(
                cursor,
                "INSERT INTO rules (id, field, rule, status) VALUES (%s, %s, %s, %s)",
                [(row.id, row.field, row.rule, row.status) for row in rule_rows],
            )

        connection.commit()

        return {
            "message": "Sync completed successfully",
            "quarantine_rows_synced": len(quarantine_rows),
            "rules_synced": len(rule_rows),
        }
    finally:
        cursor.close()
        connection.close()
