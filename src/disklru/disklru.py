"""
Disk-based LRU cache using SQLite.
"""

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional

# pylint: disable=line-too-long


class DiskLRUCache:
    """Disk-based LRU cache using SQLite."""

    _lock = threading.Lock()
    _local = threading.local()
    _closed: bool = False

    @property
    def closed(self) -> bool:
        """Returns True if the cache is closed."""
        return self._closed

    def __init__(self, db_path: str, max_size: int) -> None:
        """Initializes the cache."""
        self.db_path = db_path
        self.max_size = max_size
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    def _get_session(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        """Gets or creates a thread-specific database session."""
        if not hasattr(self._local, "session"):
            with self._lock:
                conn = sqlite3.connect(
                    self.db_path, check_same_thread=False, timeout=60.0
                )
                cursor = conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")  # Use Write-Ahead Logging
                cursor.execute(
                    "PRAGMA busy_timeout=60000"
                )  # Set busy timeout to 60 seconds

                # Initialize schema
                cursor.executescript(
                    """
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
                    """
                )

                # Initialize size counter if needed
                cursor.execute(
                    "INSERT OR IGNORE INTO metadata (key, value) VALUES ('size', 0)"
                )
                conn.commit()

                self._local.session = (conn, cursor)
        return self._local.session

    def get(self, key: str) -> Optional[str]:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        conn, cursor = self._get_session()
        with self._lock:
            try:
                cursor.execute("SELECT value FROM cache WHERE key=?", (key,))
                result = cursor.fetchone()
                if result is not None:
                    cursor.execute(
                        "UPDATE cache SET timestamp=? WHERE key=?",
                        (int(datetime.now(timezone.utc).timestamp()), key),
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
        with self._lock:
            try:
                # Check if key already exists
                cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
                key_exists = cursor.fetchone() is not None

                # Get current size
                cursor.execute("SELECT value FROM metadata WHERE key='size'")
                current_size = cursor.fetchone()[0]

                if not key_exists and current_size >= self.max_size:
                    # Delete the least recently used item
                    cursor.execute(
                        "SELECT key FROM cache ORDER BY timestamp ASC LIMIT 1"
                    )
                    lru_key = cursor.fetchone()[0]
                    cursor.execute("DELETE FROM cache WHERE key=?", (lru_key,))
                    cursor.execute(
                        "UPDATE metadata SET value = value - 1 WHERE key='size'"
                    )

                # Ensure timestamp is stored as integer
                timestamp = int(datetime.now(timezone.utc).timestamp())
                cursor.execute(
                    "INSERT OR REPLACE INTO cache (key, timestamp, value) VALUES (?, ?, ?)",
                    (key, timestamp, value.encode("utf-8")),
                )

                # Increment size only if it's a new key
                if not key_exists:
                    cursor.execute(
                        "UPDATE metadata SET value = value + 1 WHERE key='size'"
                    )

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
        with self._lock:
            try:
                cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
                if cursor.fetchone() is not None:
                    cursor.execute("DELETE FROM cache WHERE key=?", (key,))
                    cursor.execute(
                        "UPDATE metadata SET value = value - 1 WHERE key='size'"
                    )
                    conn.commit()
            except:
                conn.rollback()
                raise

    def purge(self, timestamp: datetime) -> None:
        """
        Purges all elements less than or equal to the timestamp.

        Args:
            timestamp (datetime): The UTC datetime to purge entries up to
        """
        conn, cursor = self._get_session()
        with self._lock:
            try:
                # Convert datetime to UTC timestamp
                utc_timestamp = int(timestamp.astimezone(timezone.utc).timestamp())

                # Get current count of items to be deleted
                cursor.execute(
                    "SELECT COUNT(*) FROM cache WHERE timestamp <= ?", (utc_timestamp,)
                )
                to_delete = cursor.fetchone()[0]

                # Delete items
                cursor.execute(
                    "DELETE FROM cache WHERE timestamp <= ?", (utc_timestamp,)
                )

                # Update size counter
                if to_delete > 0:
                    cursor.execute(
                        "UPDATE metadata SET value = value - ? WHERE key='size'",
                        (to_delete,),
                    )

                conn.commit()
            except:
                conn.rollback()
                raise

    def clear(self) -> None:
        """Clears the cache."""
        conn, cursor = self._get_session()
        with self._lock:
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

    def get_size(self) -> int:
        """Returns the current number of items in the cache."""
        conn, cursor = self._get_session()
        with self._lock:
            cursor.execute("SELECT value FROM metadata WHERE key='size'")
            return cursor.fetchone()[0]

    def close(self) -> None:
        """Closes all database connections."""
        if hasattr(self._local, "session"):
            conn, _ = self._local.session
            conn.close()
            delattr(self._local, "session")
        self._closed = True
