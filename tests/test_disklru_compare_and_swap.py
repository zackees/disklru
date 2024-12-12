"""
Unit test file.
"""

import os
import sqlite3
import threading
import time
import unittest

from disklru import DiskLRUCache

HERE = os.path.dirname(__file__)

LRU_CACHE_FILE = os.path.join(HERE, "test.db")


class DskLRUTesterMultiThreaded(unittest.TestCase):
    """Main tester class."""

    @classmethod
    def setUpClass(cls):
        """Set up test configurations"""
        cls.configs = [("file", LRU_CACHE_FILE)]

    def setUp(self) -> None:
        # Run each test for both file and memory configurations
        self._type_name = None

    def run(self, result=None):
        """Run tests for both configurations"""
        original_method_name = self._testMethodName
        original_method = getattr(self, original_method_name)

        for db_type, db_path in self.configs:
            self._type_name = db_type
            self.cache = DiskLRUCache(db_path, 16)
            self.cache.clear()

            try:
                original_method()
            finally:
                if not self.cache.closed:
                    self.cache.close()

        return None

    def test_concurrent_compare_and_swap(self) -> None:
        """Test that compare_and_swap operations work correctly across threads."""
        num_threads = 2
        iterations_per_thread = 10
        shared_key = "shared_counter"

        self.cache.clear()

        # Initialize the counter using compare_and_swap
        success, old = self.cache.compare_and_swap(shared_key, None, "0")
        self.assertTrue(success)

        def worker(thread_id: int, successes: list) -> None:
            """Worker that tries to increment the counter using CAS."""
            local_successes = 0
            count = thread_id

            for _ in range(iterations_per_thread):
                retry_count = 0
                while retry_count < 100:  # Add retry limit
                    time.sleep(0.01)
                    try:
                        # First get the current value
                        # _, current_val = self.cache.compare_and_swap(
                        #    shared_key, None, "0"
                        # )
                        current_val = self.cache.get(shared_key)
                        count += 1
                        next_value = count if current_val else 0

                        # Try to increment with CAS
                        success, _ = self.cache.compare_and_swap(
                            shared_key, current_val, str(next_value)
                        )

                        if success:
                            local_successes += 1
                            break

                        retry_count += 1
                    except sqlite3.OperationalError:
                        # Handle potential database locked errors
                        retry_count += 1
                        continue

                if retry_count >= 100:
                    raise RuntimeError(
                        f"Thread {thread_id} failed to increment after 100 retries"
                    )

            successes[thread_id] = local_successes

        # Track successes per thread
        thread_successes = [0] * num_threads

        # Create and start threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i, thread_successes))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Get final value using compare_and_swap
        # _, final_value = self.cache.compare_and_swap(shared_key, None, None)
        # final_value = int(self.cache.get(shared_key))
        # final_value = int(final_value)
        total_successes = sum(thread_successes)

        # Each success should have incremented the counter exactly once
        # self.assertEqual(final_value, total_successes)

        # Each thread should have succeeded iterations_per_thread times
        self.assertEqual(total_successes, num_threads * iterations_per_thread)

        # Each individual thread should have succeeded iterations_per_thread times
        for successes in thread_successes:
            self.assertEqual(successes, iterations_per_thread)


if __name__ == "__main__":
    unittest.main()
