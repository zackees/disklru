"""
Disk-based LRU cache using SQLite.
"""

import sqlite3
from datetime import datetime

# pylint: disable=line-too-long


class DiskLRUCache:
    """Disk-based LRU cache using SQLite."""

    def __init__(self, db_path, max_size):
        """Initializes the cache."""
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

    def get(self, key):
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

    def put(self, key, value):
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

    def delete(self, key):
        """Deletes the given key from the cache."""
        assert not self.closed
        self.cursor.execute("DELETE FROM cache WHERE key=?", (key,))
        self.conn.commit()

    def purge(self, timestamp):
        """Purges all elements less than the timestamp."""
        assert not self.closed
        self.cursor.execute("DELETE FROM cache WHERE timestamp<?", (timestamp,))
        self.conn.commit()

    def clear(self):
        """Clears the cache."""
        assert not self.closed
        self.cursor.execute("DELETE FROM cache")
        self.conn.commit()

    def __del__(self):
        """Destructor."""
        self.close()

    def close(self):
        """Closes the connection to the database."""
        if not self.closed:
            self.conn.close()
            self.closed = True
