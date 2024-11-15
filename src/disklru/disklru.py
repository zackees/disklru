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

    _local = threading.local()
    _closed: bool = False
    _connections: dict[int, tuple[sqlite3.Connection, sqlite3.Cursor, float]] = {}
    _connections_lock = threading.Lock()
    MAX_CONNECTIONS = 50

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
        """Gets or creates a thread-specific database session with connection pooling."""
        thread_id = threading.get_ident()
        current_time = datetime.now(timezone.utc).timestamp()

        with self._connections_lock:
            # Clean up old connections if we're at the limit
            if len(self._connections) >= self.MAX_CONNECTIONS:
                current_time = datetime.now(timezone.utc).timestamp()
                # Sort by last access time and remove oldest connections
                sorted_conns = sorted(self._connections.items(), key=lambda x: x[1][2])
                while len(self._connections) >= self.MAX_CONNECTIONS:
                    old_thread_id, (old_conn, old_cursor, _) = sorted_conns.pop(0)
                    old_conn.close()
                    del self._connections[old_thread_id]

            # Get or create connection for current thread
            if thread_id in self._connections:
                conn, cursor, _ = self._connections[thread_id]
                # Update last access time
                self._connections[thread_id] = (conn, cursor, current_time)
                return conn, cursor

            # Create new connection
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=60.0)
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=60000")

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

            # Store in connection pool
            self._connections[thread_id] = (conn, cursor, current_time)
            return conn, cursor

    def get(self, key: str) -> Optional[str]:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        conn, cursor = self._get_session()
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
        try:
            # Start transaction
            cursor.execute("BEGIN IMMEDIATE")

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

            # Ensure timestamp is stored as integer
            timestamp = int(datetime.now(timezone.utc).timestamp())
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
        try:
            cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
            if cursor.fetchone() is not None:
                cursor.execute("DELETE FROM cache WHERE key=?", (key,))
                cursor.execute("UPDATE metadata SET value = value - 1 WHERE key='size'")
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
        try:
            # Convert datetime to UTC timestamp
            utc_timestamp = int(timestamp.astimezone(timezone.utc).timestamp())

            # Get current count of items to be deleted
            cursor.execute(
                "SELECT COUNT(*) FROM cache WHERE timestamp <= ?", (utc_timestamp,)
            )
            to_delete = cursor.fetchone()[0]

            # Delete items
            cursor.execute("DELETE FROM cache WHERE timestamp <= ?", (utc_timestamp,))

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
        cursor.execute("SELECT value FROM metadata WHERE key='size'")
        return cursor.fetchone()[0]

    def close(self) -> None:
        """Closes all database connections."""
        with self._connections_lock:
            for thread_id, (conn, _, _) in self._connections.items():
                conn.close()
            self._connections.clear()
        self._closed = True
