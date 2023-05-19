"""
Unit test file.
"""

import os
import time
import unittest
from datetime import datetime, timedelta

from disklru import DiskLRUCache

HERE = os.path.dirname(__file__)

LRU_CACHE_FILE = os.path.join(HERE, "test.db")


class DskLRUTester(unittest.TestCase):
    """Main tester class."""

    def setUp(self):
        self.cache = DiskLRUCache(LRU_CACHE_FILE, 4)
        # Clean up the database file after running each test
        self.cache.clear()

    def tearDown(self):
        if not self.cache.closed:
            self.cache.close()

    def test_set_and_get(self):
        """Tests setting and getting a value."""
        self.cache.put("key", "value")
        self.assertEqual(self.cache.get("key"), "value")

    def test_clear(self):
        """Tests clearing the cache."""
        self.cache.put("key", "value")
        self.cache.clear()
        self.assertIsNone(self.cache.get("key"))

    def test_purge(self):
        """Tests purging the cache."""
        past_time = int((datetime.now() - timedelta(minutes=5)).timestamp())
        future_time = int((datetime.now() + timedelta(minutes=5)).timestamp())
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.purge(future_time)
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.purge(past_time)
        self.assertIsNotNone(self.cache.get("key1"))  # this fails
        self.assertIsNotNone(self.cache.get("key2"))

    def test_max_elements(self):
        """Tests that the cache evicts the least recently used item when it's full."""
        # First, fill the cache
        self.cache.put("key1", "value1")
        time.sleep(0.1)
        self.cache.put("key2", "value2")
        time.sleep(0.1)
        self.cache.put("key3", "value3")
        time.sleep(0.1)
        self.cache.put("key4", "value4")
        time.sleep(0.1)

        # All keys should be present
        self.assertIsNotNone(self.cache.get("key1"))
        self.assertIsNotNone(self.cache.get("key2"))
        self.assertIsNotNone(self.cache.get("key3"))
        self.assertIsNotNone(self.cache.get("key4"))

        # Now, add one more item. This should evict "key1"
        # because it was the least recently used item.
        self.cache.put("key5", "value5")

        # "key1" should be evicted, and the rest should be present.
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNotNone(self.cache.get("key2"))
        self.assertIsNotNone(self.cache.get("key3"))
        self.assertIsNotNone(self.cache.get("key4"))
        self.assertIsNotNone(self.cache.get("key5"))

    def test_json(self) -> None:
        """Tests that the cache can store and retrieve JSON objects."""
        self.cache.put_json("key", {"foo": "bar"})
        self.assertEqual(self.cache.get_json("key"), {"foo": "bar"})

    def test_close(self) -> None:
        """Tests that the cache can be closed."""
        self.cache.close()
        self.assertTrue(self.cache.closed)


if __name__ == "__main__":
    unittest.main()
