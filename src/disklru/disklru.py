"""
Disk-based LRU cache using SQLite.
"""

import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, Optional

# pylint: disable=line-too-long


class DiskLRUCache:
    """Disk-based LRU cache using SQLite."""

    _sessions: Dict[int, tuple[sqlite3.Connection, sqlite3.Cursor]] = {}

    def __init__(self, db_path: str, max_size: str) -> None:
        """Initializes the cache."""
        self.db_path = db_path
        self.max_size = max_size
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    def _get_session(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        """Gets or creates a thread-specific database session."""
        thread_id = threading.get_ident()
        pair = self._sessions.get(thread_id)
        if pair is None:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.executescript(
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
            self._sessions[thread_id] = (conn, cursor)
            return (conn, cursor)
        return (pair[0], pair[1])

    def get(self, key: str) -> Optional[str]:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        conn, cursor = self._get_session()
        cursor.execute("BEGIN")
        try:
            cursor.execute("SELECT value FROM cache WHERE key=?", (key,))
            result = cursor.fetchone()
            if result is not None:
                cursor.execute(
                    "UPDATE cache SET timestamp=? WHERE key=?",
                    (int(datetime.utcnow().timestamp()), key),
                )
                conn.commit()
                return result[0].decode("utf-8")
            conn.commit()
            return None
        except:
            conn.rollback()
            raise

    def get_json(self, key: str) -> Any:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        result = self.get(key)
        if result is not None:
            return json.loads(result)
        return None

    def put(self, key: str, value: str) -> None:
        """Sets the value associated with the given key."""
        conn, cursor = self._get_session()
        cursor.execute("BEGIN")
        try:
            # Check if key already exists
            cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
            key_exists = cursor.fetchone() is not None

            # Get current size
            cursor.execute("SELECT value FROM metadata WHERE key='size'")
            current_size = cursor.fetchone()[0]

            if not key_exists and current_size >= self.max_size:
                # Delete the least recently used item
                cursor.execute("SELECT key FROM cache ORDER BY timestamp ASC LIMIT 1")
                lru_key = cursor.fetchone()[0]
                cursor.execute("DELETE FROM cache WHERE key=?", (lru_key,))
                cursor.execute("UPDATE metadata SET value = value - 1 WHERE key='size'")

            timestamp = int(datetime.utcnow().timestamp())
            cursor.execute(
                "INSERT OR REPLACE INTO cache (key, timestamp, value) VALUES (?, ?, ?)",
                (key, timestamp, value.encode("utf-8")),
            )

            # Increment size only if it's a new key
            if not key_exists:
                cursor.execute("UPDATE metadata SET value = value + 1 WHERE key='size'")

            conn.commit()
        except:
            conn.rollback()
            raise

    def put_json(self, key: str, val: Any) -> None:
        """Sets the value associated with the given key."""
        self.put(key, json.dumps(val))

    def delete(self, key) -> None:
        """Deletes the given key from the cache."""
        conn, cursor = self._get_session()
        cursor.execute("BEGIN")
        try:
            cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
            if cursor.fetchone() is not None:
                cursor.execute("DELETE FROM cache WHERE key=?", (key,))
                cursor.execute("UPDATE metadata SET value = value - 1 WHERE key='size'")
                conn.commit()
        except:
            conn.rollback()
            raise

    def purge(self, timestamp) -> None:
        """Purges all elements less than the timestamp."""
        conn, cursor = self._get_session()
        cursor.execute("BEGIN")
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM cache WHERE timestamp < ?", (timestamp,)
            )
            to_delete = cursor.fetchone()[0]
            cursor.execute("DELETE FROM cache WHERE timestamp < ?", (timestamp,))
            cursor.execute(
                "UPDATE metadata SET value = value - ? WHERE key='size'", (to_delete,)
            )
            conn.commit()
        except:
            conn.rollback()
            raise

    def clear(self) -> None:
        """Clears the cache."""
        conn, cursor = self._get_session()
        cursor.execute("BEGIN")
        try:
            cursor.execute("DELETE FROM cache")
            cursor.execute("UPDATE metadata SET value = 0 WHERE key='size'")
            conn.commit()
        except:
            conn.rollback()
            raise

    def __del__(self) -> None:
        """Destructor."""
        self.close()

    def close(self) -> None:
        """Closes all database connections."""
        for conn, _ in self._sessions.values():
            conn.close()
        self._sessions.clear()
