from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def find_project_root(marker_files=("worker.py",)) -> Path:
    """Find project root by looking for marker files."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if any((parent / marker).exists() for marker in marker_files):
            return parent

    return Path("/app")  # Fallback to /app if no marker files found
