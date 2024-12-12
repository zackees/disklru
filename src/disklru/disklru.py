"""
Disk-based LRU cache using SQLite.
"""

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

# pylint: disable=line-too-long


class DiskLRUCache:
    """Disk-based LRU cache using SQLite."""

    _local = threading.local()
    _closed: bool = False
    _connections: Dict[int, Tuple[sqlite3.Connection, sqlite3.Cursor, float]] = {}
    _connections_lock = threading.Lock()
    MAX_CONNECTIONS = 50

    @property
    def closed(self) -> bool:
        """Returns True if the cache is closed."""
        return self._closed

    def __init__(
        self, db_path: str, max_entries: int, max_connections: int = MAX_CONNECTIONS
    ) -> None:
        """Initializes the cache."""
        self.db_path = db_path
        self.max_entries = max_entries
        self.max_connections = max_connections
        if ":memory:" not in db_path:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

    def _get_session(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        """Gets or creates a thread-specific database session with connection pooling."""
        thread_id = threading.get_ident()
        current_time = datetime.now(timezone.utc).timestamp()

        # Fast path if the connection already exists
        con = self._connections.get(thread_id)
        if con is not None:
            conn, cursor, _ = con
            self._connections[thread_id] = (conn, cursor, current_time)
            return conn, cursor

        # Fast path - check if connection exists without lock
        if thread_id in self._connections:
            conn, cursor, _ = self._connections[thread_id]
            # Update last access time under lock
            with self._connections_lock:
                if thread_id in self._connections:  # Double-check pattern
                    self._connections[thread_id] = (conn, cursor, current_time)
                    return conn, cursor

        # Slow path - need new connection
        with self._connections_lock:
            # Check again in case another thread created it
            if thread_id in self._connections:
                conn, cursor, _ = self._connections[thread_id]
                self._connections[thread_id] = (conn, cursor, current_time)
                return conn, cursor

            # Clean up old connections if we're at the limit
            if len(self._connections) >= self.max_connections:
                # Get oldest connections without creating a full sorted copy
                oldest_threads = sorted(
                    self._connections.items(), key=lambda x: x[1][2], reverse=True
                )[self.max_connections - 1 :]

                for old_thread_id, (_, _, _) in oldest_threads:
                    # Just remove the reference, let the connection close naturally
                    del self._connections[old_thread_id]

            # Create new connection
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=60.0)
            cursor = conn.cursor()

            # Batch execute PRAGMA statements
            cursor.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA busy_timeout=60000;
                
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
            """
            )

            conn.commit()

            # Store in connection pool
            self._connections[thread_id] = (conn, cursor, current_time)
            return conn, cursor

    def get(self, key: str) -> Optional[str]:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        if not isinstance(key, str):
            raise TypeError("key must be str, not " + type(key).__name__)
        result = self.get_bytes(key)
        if result is not None:
            return result.decode("utf-8")
        return None

    def get_json(self, key: str) -> Any:
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        result = self.get(key)
        if result is not None:
            return json.loads(result)
        return None

    def get_bytes(self, key: str) -> Optional[bytes]:
        """Returns the value associated with the given key as bytes, or None if the key is not in the cache."""
        if not isinstance(key, str):
            raise TypeError("key must be str, not " + type(key).__name__)
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
                return result[0]
            conn.commit()
            return None
        except:
            conn.rollback()
            raise

    def put_bytes(self, key: str, value: bytes) -> None:
        """Sets the value associated with the given key as raw bytes."""
        if not isinstance(key, str):
            raise TypeError("key must be str, not " + type(key).__name__)
        if not isinstance(value, bytes):
            raise TypeError("value must be bytes, not " + type(value).__name__)
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

            if not key_exists and current_size >= self.max_entries:
                # Delete the least recently used item
                cursor.execute("SELECT key FROM cache ORDER BY timestamp ASC LIMIT 1")
                lru_key = cursor.fetchone()[0]
                cursor.execute("DELETE FROM cache WHERE key=?", (lru_key,))
                cursor.execute("UPDATE metadata SET value = value - 1 WHERE key='size'")

            # Ensure timestamp is stored as integer
            timestamp = int(datetime.now(timezone.utc).timestamp())
            cursor.execute(
                "INSERT OR REPLACE INTO cache (key, timestamp, value) VALUES (?, ?, ?)",
                (key, timestamp, value),
            )

            # Increment size only if it's a new key
            if not key_exists:
                cursor.execute("UPDATE metadata SET value = value + 1 WHERE key='size'")

            conn.commit()
        except:
            conn.rollback()
            raise

    def put(self, key: str, value: str) -> None:
        """Sets the value associated with the given key."""
        if not isinstance(key, str):
            raise TypeError("key must be str, not " + type(key).__name__)
        if not isinstance(value, str):
            raise TypeError("value must be str, not " + type(value).__name__)
        self.put_bytes(key, value.encode("utf-8"))

    def put_json(self, key: str, val: Any) -> None:
        """Sets the value associated with the given key."""
        self.put(key, json.dumps(val))

    def __contains__(self, key: str) -> bool:
        """Implements 'in' operator to check if a key exists in the cache."""
        if not isinstance(key, str):
            raise TypeError("key must be str, not " + type(key).__name__)
        conn, cursor = self._get_session()
        try:
            cursor.execute("SELECT 1 FROM cache WHERE key=?", (key,))
            return cursor.fetchone() is not None
        except:
            conn.rollback()
            raise

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

    def compare_and_swap(
        self, key: str, prev_val: Optional[str], new_val: Optional[str]
    ) -> Tuple[bool, Optional[str]]:
        if not isinstance(key, str):
            raise TypeError("key must be str, not " + type(key).__name__)
        if prev_val is not None and not isinstance(prev_val, str):
            raise TypeError(
                "prev_val must be str or None, not " + type(prev_val).__name__
            )
        if new_val is not None and not isinstance(new_val, str):
            raise TypeError(
                "new_val must be str or None, not " + type(new_val).__name__
            )

        conn, cursor = self._get_session()
        try:
            timestamp = int(datetime.now(timezone.utc).timestamp())

            # Convert values to bytes for storage
            prev_val_bytes = prev_val.encode("utf-8") if prev_val is not None else None
            new_val_bytes = new_val.encode("utf-8") if new_val is not None else None

            cursor.execute("BEGIN IMMEDIATE")

            if new_val is None:
                # Delete case
                cursor.execute(
                    """
                    DELETE FROM cache
                    WHERE key = ? AND (? IS NULL AND value IS NULL OR value = ?);
                    """,
                    (key, prev_val_bytes, prev_val_bytes),
                )
                if cursor.rowcount > 0:
                    cursor.execute(
                        """
                        UPDATE metadata SET value = value - 1 WHERE key = 'size';
                        """
                    )
            else:
                # Insert or update case
                cursor.execute(
                    """
                    INSERT INTO cache (key, timestamp, value)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        timestamp = excluded.timestamp,
                        value = excluded.value
                    WHERE
                        (cache.value = ?);
                    """,
                    (
                        key,
                        timestamp,
                        new_val_bytes,
                        prev_val_bytes,
                    ),
                )

                # Update metadata if it was an insert
                if cursor.rowcount > 0 and prev_val is None:
                    cursor.execute(
                        """
                        UPDATE metadata SET value = value + 1 WHERE key = 'size';
                        """
                    )

            conn.commit()
            success = cursor.arraysize > 0
            return_str: str | None = None
            if not success:
                # get the previous value
                cursor.execute("SELECT value FROM cache WHERE key=?", (key,))
                result = cursor.fetchone()
                if result is not None:
                    return_str = result[0].encode("utf-8")
            return (
                success,
                return_str,
            )  # Old value retrieval removed for compatibility
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
        """Closes the cache by removing connection references."""
        with self._connections_lock:
            self._connections.clear()
        self._closed = True
