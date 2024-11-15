"""
Unit test file.
"""

import os
import unittest

from disklru import DiskLRUCache

HERE = os.path.dirname(__file__)

LRU_CACHE_FILE = os.path.join(HERE, "test.db")


class DskLRUTesterMultiThreaded(unittest.TestCase):
    """Main tester class."""

    def setUp(self) -> None:
        self.cache = DiskLRUCache(LRU_CACHE_FILE, 16)
        # Clean up the database file after running each test
        self.cache.clear()

    def tearDown(self) -> None:
        if not self.cache.closed:
            self.cache.close()

    def test_multi_threaded_stress_test(self) -> None:
        """Test concurrent access from multiple threads."""
        import random
        import threading

        num_threads = 4
        operations_per_thread = 100

        def worker(thread_id: int) -> None:
            """Worker function that performs random cache operations."""
            for i in range(operations_per_thread):
                operation = random.randint(0, 2)
                key = f"key_{thread_id}_{i}"
                value = f"value_{thread_id}_{i}"

                try:
                    if operation == 0:  # Put
                        self.cache.put(key, value)
                    elif operation == 1:  # Get
                        self.cache.get(key)
                    else:  # Delete
                        self.cache.delete(key)
                except Exception as e:
                    self.fail(f"Thread {thread_id} failed on operation {i}: {str(e)}")

        # Create and start threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

    def test_concurrent_json_operations(self) -> None:
        """Test concurrent JSON operations from multiple threads."""
        import threading

        num_threads = 4
        operations_per_thread = 50

        def worker(thread_id: int) -> None:
            """Worker function that performs random JSON cache operations."""
            for i in range(operations_per_thread):
                key = f"json_key_{thread_id}_{i}"
                value = {"thread": thread_id, "index": i, "data": "test"}

                try:
                    self.cache.put_json(key, value)
                    retrieved = self.cache.get_json(key)
                    if retrieved:
                        self.assertEqual(retrieved["thread"], thread_id)
                        self.assertEqual(retrieved["index"], i)
                except Exception as e:
                    self.fail(f"Thread {thread_id} failed on operation {i}: {str(e)}")

        # Create and start threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

    def test_concurrent_size_limit(self) -> None:
        """Test that size limit is maintained under concurrent access."""
        import threading

        num_threads = 4
        operations_per_thread = 10

        def worker(thread_id: int) -> None:
            """Worker function that adds items to cache."""
            for i in range(operations_per_thread):
                key = f"size_key_{thread_id}_{i}"
                value = f"value_{thread_id}_{i}"
                self.cache.put(key, value)

        # Create and start threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify that cache size hasn't exceeded max_size
        actual_size = self.cache.get_size()
        self.assertLessEqual(actual_size, self.cache.max_size)


if __name__ == "__main__":
    unittest.main()
