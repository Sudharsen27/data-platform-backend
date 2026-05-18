from unittest.mock import MagicMock

from app.services.snowflake_sync import _executemany_batched, _sync_batch_size


def test_sync_batch_size_default():
    assert _sync_batch_size() == 10_000


def test_executemany_batched_splits_large_lists():
    cursor = MagicMock()
    rows = [(i, f"n{i}", f"u{i}@x.com", "") for i in range(25_000)]
    count = _executemany_batched(
        cursor,
        "INSERT INTO quarantine_data (id, name, email, error) VALUES (%s, %s, %s, %s)",
        rows,
        batch_size=10_000,
    )
    assert count == 25_000
    assert cursor.executemany.call_count == 3
