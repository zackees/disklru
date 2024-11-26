# disklru

`pip install disklru`

Creates a disk based lru (least recently used) cache, backed by sqlite, that you can use in your apps.

Zero dependency package. Only relies on the python standard lib. Cross platform tests.

[![Linting](https://github.com/zackees/disklru/actions/workflows/lint.yml/badge.svg)](https://github.com/zackees/disklru/actions/workflows/lint.yml)

[![MacOS_Tests](https://github.com/zackees/disklru/actions/workflows/push_macos.yml/badge.svg)](https://github.com/zackees/disklru/actions/workflows/push_macos.yml)
[![Ubuntu_Tests](https://github.com/zackees/disklru/actions/workflows/push_ubuntu.yml/badge.svg)](https://github.com/zackees/disklru/actions/workflows/push_ubuntu.yml)
[![Win_Tests](https://github.com/zackees/disklru/actions/workflows/push_win.yml/badge.svg)](https://github.com/zackees/disklru/actions/workflows/push_win.yml)


# Usage

```python
from disklru import DiskLRUCache

LRU_CACHE_FILE = "cache.db"
MAX_ENTRIES = 4
cache = DiskLRUCache(LRU_CACHE_FILE, MAX_ENTRIES)
cache.put("key", "value")
assert cache.get("key1") == "val"
cache.clear()
```

# API

```python
class DiskLRUCache:
    """Disk-based LRU cache using SQLite."""

    def get(self, key: str) -> str | None:
        """Returns the value associated with the given key, or None if the key is not in the cache."""

    def get_bytes(self, key: str) -> bytes | None:
        """Returns the bytes values associated with the given key"""

    def get_json(self, key: str) -> Any:
        """Returns the value associated with the given key, or None if the key is not in the cache."""

    def put(self, key: str, value: str) -> None:
        """Sets the value associated with the given key."""

    def put_bytes(self, key: str, value: bytes) ->: None:
        """Sets the byte value associated with the given key."""

    def put_json(self, key: str, val: Any) -> None:
        """Sets the value associated with the given key."""

    def delete(self, key) -> None:
        """Deletes the given key from the cache."""

    def purge(self, timestamp) -> None:
        """Purges all elements less than the timestamp."""

    def clear(self) -> None:
        """Clears the cache."""

    def __del__(self) -> None:
        """Destructor."""
        self.close()

    def close(self) -> None:
        """Closes the connection to the database."""
```

# Development

First install development dependencies:

```bash
pip install -e ".[dev]"
```

### Windows

This environment requires you to use `git-bash`.

### Linting

Run `./lint.sh` to find linting errors using `pylint`, `flake8`, `mypy` and other tools.


### Releases

  * 2.0.2 - __contains__ operator is now defined so that you can do "key" in disklur
  * 2.0.1 - `max_size` is now `max_entries`
  * 2.0.0 - Overhaul - now allows multithreaded access, connection pool, get/put bytes. purge() now takes in a timestamp aware value for purging.
