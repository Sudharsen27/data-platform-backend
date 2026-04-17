from sqlalchemy.orm import Session

from app.db.snowflake import get_snowflake_connection
from app.models import QuarantineData, Rule


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
            cursor.executemany(
                "INSERT INTO quarantine_data (id, name, email, error) VALUES (%s, %s, %s, %s)",
                [(row.id, row.name, row.email, row.error) for row in quarantine_rows],
            )

        if rule_rows:
            cursor.executemany(
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
