# type: ignore

"""
Models for the disklru package.
"""


from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CacheItem(Base):  # pylint: disable=too-few-public-methods
    """Represents a cache item."""

    __tablename__ = "cache"
    key = Column(String, primary_key=True)
    timestamp = Column(Integer, index=True)
    value = Column(String)


Index("idx_timestamp", CacheItem.timestamp)
