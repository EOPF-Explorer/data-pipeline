"""Read a ``processing:software`` version: the one parser for every tool that compares them.

``processing:software`` maps a software name to the version that produced the item, e.g.
``{"EOPF_GEOZARR": "0.13.1", ...}``. The Explorer API returns it as an object in a full item
and as a JSON-encoded string under the ``fields`` extension, so both are read.
"""

import json

from packaging.version import Version


def read_version(properties: dict, key: str) -> Version | None:
    """``properties["processing:software"][key]`` as a PEP 440 version, or None if absent.

    Raises ValueError (``InvalidVersion`` is one) when the entry is there but unreadable:
    ``processing:software`` that is not an object, a value that is not a string, or one that
    is not a version with at least ``major.minor``. That covers data-model's git-describe
    stamp (``v0.13.1-0-g…``) and a bare commit sha, including an all-digit one, which PEP 440
    alone would read as a huge release (``1234567``) and so as newer than every real one.
    """
    software = properties.get("processing:software")
    if isinstance(software, str):
        try:
            software = json.loads(software)
        except json.JSONDecodeError:
            raise ValueError(f"processing:software is not JSON: {software!r:.80}") from None
    if software is None:
        return None
    if not isinstance(software, dict):
        raise ValueError(f"processing:software is not an object: {software!r:.80}")
    value = software.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"processing:software[{key!r}] is not a string: {value!r:.80}")
    version = Version(value)
    if len(version.release) < 2:
        raise ValueError(f"processing:software[{key!r}] is not major.minor[.patch]: {value!r}")
    return version


def is_final(version: Version) -> bool:
    """A release a tool may write from: not a pre-release, a dev build or a local label.

    PEP 440 orders ``0.14.0rc1`` above ``0.13.1`` and ``0.13.0+eodc.1`` above ``0.13.0``, so
    without this a release candidate EODC tries out would replace a final release. A
    post-release (``0.13.1.post1``) is final.
    """
    return not version.is_prerelease and version.local is None
