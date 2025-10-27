#!/usr/bin/env python3
"""Remove Python build artifacts and caches."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


CACHE_DIRS = {"__pycache__", ".pytest_cache"}
PYC_SUFFIX = ".pyc"


def prune(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink()
        except OSError:
            pass


def walk_and_clean(root: Path) -> None:
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        current = Path(dirpath)
        if current.name in CACHE_DIRS:
            prune(current)
            continue
        for name in list(dirnames):
            if name in CACHE_DIRS:
                prune(current / name)
        for fname in filenames:
            if fname.endswith(PYC_SUFFIX):
                prune(current / fname)


def main() -> None:
    walk_and_clean(Path(".").resolve())


if __name__ == "__main__":
    main()
