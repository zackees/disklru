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
        # Combine all creation statements into a single atomic transaction
        self.cursor.executescript(
            """
            BEGIN;
            
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                timestamp INTEGER,
                value BLOB
            );
            
            CREATE INDEX IF NOT EXISTS idx_timestamp ON cache (timestamp);
            CREATE INDEX IF NOT EXISTS idx_key ON cache (key);
            
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value INTEGER
            );
            
            INSERT OR IGNORE INTO metadata (key, value) VALUES ('size', 0);
            
            COMMIT;
        """
        )
        self.max_size = max_size

    def get(self, key: str) -> Optional[str]:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        assert not self.closed
        self.cursor.execute("BEGIN")
        try:
            self.cursor.execute("SELECT value FROM cache WHERE key=?", (key,))
            result = self.cursor.fetchone()
            if result is not None:
                self.cursor.execute(
                    "UPDATE cache SET timestamp=? WHERE key=?",
                    (int(datetime.utcnow().timestamp()), key),
                )
                self.conn.commit()
                return result[0].decode("utf-8")
            self.conn.commit()
            return None
        except:
            self.conn.rollback()
            raise

    def get_json(self, key: str) -> Any:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        result = self.get(key)
        if result is not None:
            return json.loads(result)
        return None

    def put(self, key: str, value: str) -> None:
        """Sets the value associated with the given key."""
        assert not self.closed
        self.cursor.execute("BEGIN")
        try:
            # Check if key already exists
            self.cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
            key_exists = self.cursor.fetchone() is not None

            # Get current size
            self.cursor.execute("SELECT value FROM metadata WHERE key='size'")
            current_size = self.cursor.fetchone()[0]

            if not key_exists and current_size >= self.max_size:
                # Delete the least recently used item
                self.cursor.execute(
                    "SELECT key FROM cache ORDER BY timestamp ASC LIMIT 1"
                )
                lru_key = self.cursor.fetchone()[0]
                self.cursor.execute("DELETE FROM cache WHERE key=?", (lru_key,))
                self.cursor.execute(
                    "UPDATE metadata SET value = value - 1 WHERE key='size'"
                )

            timestamp = int(datetime.utcnow().timestamp())
            self.cursor.execute(
                "INSERT OR REPLACE INTO cache (key, timestamp, value) VALUES (?, ?, ?)",
                (key, timestamp, value.encode("utf-8")),
            )

            # Increment size only if it's a new key
            if not key_exists:
                self.cursor.execute(
                    "UPDATE metadata SET value = value + 1 WHERE key='size'"
                )

            self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def put_json(self, key: str, val: Any) -> None:
        """Sets the value associated with the given key."""
        self.put(key, json.dumps(val))

    def delete(self, key) -> None:
        """Deletes the given key from the cache."""
        assert not self.closed
        self.cursor.execute("BEGIN")
        try:
            self.cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
            if self.cursor.fetchone() is not None:
                self.cursor.execute("DELETE FROM cache WHERE key=?", (key,))
                self.cursor.execute(
                    "UPDATE metadata SET value = value - 1 WHERE key='size'"
                )
                self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def purge(self, timestamp) -> None:
        """Purges all elements less than the timestamp."""
        assert not self.closed
        self.cursor.execute("BEGIN")
        try:
            self.cursor.execute(
                "SELECT COUNT(*) FROM cache WHERE timestamp < ?", (timestamp,)
            )
            to_delete = self.cursor.fetchone()[0]
            self.cursor.execute("DELETE FROM cache WHERE timestamp < ?", (timestamp,))
            self.cursor.execute(
                "UPDATE metadata SET value = value - ? WHERE key='size'", (to_delete,)
            )
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def clear(self) -> None:
        """Clears the cache."""
        assert not self.closed
        self.cursor.execute("BEGIN")
        try:
            self.cursor.execute("DELETE FROM cache")
            self.cursor.execute("UPDATE metadata SET value = 0 WHERE key='size'")
            self.conn.commit()
        except:
            self.conn.rollback()
            raise

    def __del__(self) -> None:
        """Destructor."""
        self.close()

    def close(self) -> None:
        """Closes the connection to the database."""
        if not self.closed:
            self.conn.close()
            self.closed = True
