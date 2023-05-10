# disklru

Creates a disk based lru that you can use in your apps.

Usefull for caches.

[![Linting](../../actions/workflows/lint.yml/badge.svg)](../../actions/workflows/lint.yml)

[![MacOS_Tests](../../actions/workflows/push_macos.yml/badge.svg)](../../actions/workflows/push_macos.yml)
[![Ubuntu_Tests](../../actions/workflows/push_ubuntu.yml/badge.svg)](../../actions/workflows/push_ubuntu.yml)
[![Win_Tests](../../actions/workflows/push_win.yml/badge.svg)](../../actions/workflows/push_win.yml)


# Usage

```python
LRU_CACHE_FILE = "cache.db"
MAX_FILES = 4
cache = DiskLRUCache(LRU_CACHE_FILE, MAX_FILES)
cache.put("key", "value")
assert cache.get("key1") == "val"
cache.clear()
```

# Windows

This environment requires you to use `git-bash`.

# Linting

Run `./lint.sh` to find linting errors using `pylint`, `flake8` and `mypy`.
