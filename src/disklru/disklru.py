"""
Implementation of a disk-based LRU cache using SQLAlchemy and SQLite.
"""

# pylint: disable=line-too-long

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from disklru.models import Base, CacheItem  # type: ignore


class DiskLRUCache:
    """Disk-based LRU cache using SQLAlchemy and SQLite."""

    def __init__(self, db_path, max_size):
        """Initializes the cache."""
        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)  # pylint: disable=invalid-name
        self.max_size = max_size

    def get(self, key):
        """Returns the value associated with the given key, or None if the key is not in the cache."""
        with self.Session() as session:
            item = session.query(CacheItem).filter_by(key=key).first()
            if item:
                item.timestamp = int(datetime.now().timestamp())
                session.commit()
                return item.value
            return None

    def put(self, key, value):
        """Sets the value associated with the given key."""
        with self.Session() as session:
            if session.query(CacheItem).count() >= self.max_size:
                # Delete the least recently used item
                lru_item = (
                    session.query(CacheItem).order_by(CacheItem.timestamp).first()
                )
                session.delete(lru_item)
                session.commit()
            timestamp = int(datetime.now().timestamp())
            session.add(CacheItem(key=key, timestamp=timestamp, value=value))
            session.commit()

    def delete(self, key):
        """Deletes the given key from the cache."""
        with self.Session() as session:
            item = session.query(CacheItem).filter_by(key=key).first()
            if item:
                session.delete(item)
                session.commit()

    def purge(self, timestamp):
        """Purges all elements less than the timestamp."""
        with self.Session() as session:
            session.query(CacheItem).filter(CacheItem.timestamp < timestamp).delete(
                synchronize_session=False
            )
            session.commit()

    def clear(self):
        """Clears the cache."""
        with self.Session() as session:
            session.query(CacheItem).delete()
            session.commit()
