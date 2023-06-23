"""
Disk-based LRU cache using SQLite.
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Optional

# pylint: disable=line-too-long


class DiskLRUCache:
    """Disk-based LRU cache using SQLite."""

    def __init__(self, db_path: str, max_size: str) -> None:
        """Initializes the cache."""
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.closed = False
        self.cursor = self.conn.cursor()
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                timestamp INTEGER,
                value TEXT
            );
        """
        )
        self.cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON cache (timestamp);"
        )
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_key ON cache (key);")
        self.conn.commit()
        self.max_size = max_size

    def get(self, key: str) -> Optional[str]:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        assert not self.closed
        self.cursor.execute("SELECT value FROM cache WHERE key=?", (key,))
        result = self.cursor.fetchone()
        if result is not None:
            self.cursor.execute(
                "UPDATE cache SET timestamp=? WHERE key=?",
                (int(datetime.now().timestamp()), key),
            )
            self.conn.commit()
            return result[0]
        return None

    def get_json(self, key: str) -> Any:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        result = self.get(key)
        if result is not None:
            return json.loads(result)
        return None

    def put(self, key: str, value: str) -> None:
        """Sets the value associated with the given key."""
        assert not self.closed
        self.cursor.execute("SELECT COUNT(*) FROM cache")
        if self.cursor.fetchone()[0] >= self.max_size:
            # Delete the least recently used item
            self.cursor.execute("SELECT key FROM cache ORDER BY timestamp ASC LIMIT 1")
            lru_key = self.cursor.fetchone()[0]
            self.cursor.execute("DELETE FROM cache WHERE key=?", (lru_key,))
            self.conn.commit()
        timestamp = int(datetime.now().timestamp())
        self.cursor.execute(
            "INSERT OR REPLACE INTO cache (key, timestamp, value) VALUES (?, ?, ?)",
            (key, timestamp, value),
        )
        self.conn.commit()

    def put_json(self, key: str, val: Any) -> None:
        """Sets the value associated with the given key."""
        self.put(key, json.dumps(val))

    def delete(self, key) -> None:
        """Deletes the given key from the cache."""
        assert not self.closed
        self.cursor.execute("DELETE FROM cache WHERE key=?", (key,))
        self.conn.commit()

    def purge(self, timestamp) -> None:
        """Purges all elements less than the timestamp."""
        assert not self.closed
        self.cursor.execute("DELETE FROM cache WHERE timestamp<?", (timestamp,))
        self.conn.commit()

    def clear(self) -> None:
        """Clears the cache."""
        assert not self.closed
        self.cursor.execute("DELETE FROM cache")
        self.conn.commit()

    def __del__(self) -> None:
        """Destructor."""
        self.close()

    def close(self) -> None:
        """Closes the connection to the database."""
        if not self.closed:
            self.conn.close()
            self.closed = True
