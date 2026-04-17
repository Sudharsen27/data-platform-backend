from app.db.snowflake import get_snowflake_connection


def get_quarantine_analytics():
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM quarantine_data")
        total_records = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM quarantine_data
            WHERE error IS NOT NULL AND TRIM(error) <> ''
            """
        )
        failed_records = cursor.fetchone()[0]

        success_records = total_records - failed_records
        success_rate = 0
        if total_records > 0:
            success_rate = round((success_records / total_records) * 100, 2)

        cursor.execute(
            """
            SELECT error, COUNT(*) as error_count
            FROM quarantine_data
            WHERE error IS NOT NULL AND TRIM(error) <> ''
            GROUP BY error
            ORDER BY error_count DESC
            """
        )
        error_distribution = [
            {"error": row[0], "count": row[1]} for row in cursor.fetchall()
        ]

        return {
            "total_records": total_records,
            "success_records": success_records,
            "failed_records": failed_records,
            "success_rate": success_rate,
            "error_distribution": error_distribution,
        }
    finally:
        cursor.close()
        connection.close()
