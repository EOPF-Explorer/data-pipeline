"""Pytest configuration tweaks for GeoZarr validation suite."""

import os
import sys
from types import SimpleNamespace

import s3fs

# Force s3fs to remain synchronous during tests to avoid background tasks.
os.environ.setdefault("S3FS_ASYNC", "0")
s3fs.core.ASYNC_ENABLED = False


def _suppress_async_unraisable(unraisable: SimpleNamespace) -> None:
    """Ignore noisy async teardown errors emitted by s3fs/zarr."""
    exc = unraisable.exc_value
    obj_repr = repr(unraisable.object)
    if "StorePath.get" in obj_repr and "_context" in str(exc):
        return
    sys.__unraisablehook__(unraisable)


sys.unraisablehook = _suppress_async_unraisable
