"""Live integration tests: STAC item writes are single idempotent PUTs (issue #352).

These tests drive the real write functions (not raw HTTP) against a throwaway,
timestamped scratch collection on a real STAC API, proving PUT-replace persists
without duplicating or tearing items.

Double-gated on purpose — a live run needs BOTH:
  - STAC_TEST_URL: the STAC API root to write to (a scratch collection is still
    a real write on that host), and
  - STAC_LIVE=1: explicit opt-in, so an ambiently exported STAC_TEST_URL can
    never fire prod-host writes from a plain `uv run pytest`.

Run:
    STAC_TEST_URL=https://... STAC_LIVE=1 uv run pytest -m integration \
        tests/integration/test_stac_put_idempotency.py
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import pystac
import pytest
import requests

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.environ.get("STAC_TEST_URL") and os.environ.get("STAC_LIVE") == "1"),
        reason="live STAC write test: set STAC_TEST_URL and STAC_LIVE=1 to opt in",
    ),
]

# ---------------------------------------------------------------------------
# Module loading (operator-tools has a hyphen — import by file path)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).parent.parent.parent
OPERATOR_TOOLS = REPO_ROOT / "operator-tools"
SCRIPTS_DIR = REPO_ROOT / "scripts"

for _p in (str(SCRIPTS_DIR), str(OPERATOR_TOOLS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _load(module_name: str, file_path: Path):
    # Reuse an already-loaded instance: replacing sys.modules[module_name] would
    # break patch() targets in other test modules that loaded the same file.
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


manage_item_module = _load("manage_item", OPERATOR_TOOLS / "manage_item.py")
register_v1 = _load("register_v1", SCRIPTS_DIR / "register_v1.py")

# ---------------------------------------------------------------------------
# Scratch-collection guards
# ---------------------------------------------------------------------------
# Only names THIS suite creates — never a broad zzz-* match against a real catalog.
SCRATCH_NAME_RE = re.compile(r"^zzz-(put|upsert)-(e2e|probe)-\d{8}T\d{6}$")


def _assert_scratch(collection_id: str) -> None:
    """Belt-and-braces: refuse to write to anything but a scratch collection."""
    assert collection_id.startswith(
        "zzz-"
    ), f"refusing to touch non-scratch collection {collection_id!r}"


def _collection_dict(collection_id: str) -> dict:
    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": collection_id,
        "description": "Throwaway scratch collection for PUT-idempotency tests (#352)",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [],
    }


def _make_item(item_id: str, collection_id: str) -> pystac.Item:
    return pystac.Item(
        id=item_id,
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        bbox=[0.0, 0.0, 0.0, 0.0],
        datetime=datetime(2024, 1, 15, tzinfo=UTC),
        properties={},
        collection=collection_id,
    )


def _post_item(
    session: requests.Session, api_url: str, collection_id: str, item: pystac.Item
) -> requests.Response:
    _assert_scratch(collection_id)
    return session.post(
        f"{api_url}/collections/{collection_id}/items", json=item.to_dict(), timeout=30
    )


def _get_item_features(session: requests.Session, api_url: str, collection_id: str) -> list[dict]:
    resp = session.get(f"{api_url}/collections/{collection_id}/items", timeout=30)
    resp.raise_for_status()
    return resp.json()["features"]


def _delete_collection(
    session: requests.Session, api_url: str, collection_id: str
) -> requests.Response:
    _assert_scratch(collection_id)
    return session.delete(f"{api_url}/collections/{collection_id}", timeout=30)


def _sweep_stale_scratch_collections(session: requests.Session, api_url: str) -> None:
    """Delete leftovers from earlier crashed runs — strict name match only."""
    resp = session.get(f"{api_url}/collections", timeout=30)
    resp.raise_for_status()
    for collection in resp.json().get("collections", []):
        if SCRATCH_NAME_RE.match(collection.get("id", "")):
            _delete_collection(session, api_url, collection["id"])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def api_url() -> str:
    return os.environ["STAC_TEST_URL"].rstrip("/")


@pytest.fixture(scope="module")
def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    return s


@pytest.fixture(scope="module")
def scratch_collection(session: requests.Session, api_url: str):
    """Timestamped scratch collection, ALWAYS deleted — even mid-test failure."""
    _sweep_stale_scratch_collections(session, api_url)
    collection_id = f"zzz-put-e2e-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
    resp = session.post(f"{api_url}/collections", json=_collection_dict(collection_id), timeout=30)
    assert resp.status_code in (200, 201), f"scratch collection create failed: {resp.text}"
    try:
        yield collection_id
    finally:
        resp = _delete_collection(session, api_url, collection_id)
        assert resp.status_code in (200, 204), f"scratch teardown failed: {resp.text}"


# ---------------------------------------------------------------------------
# Pattern R — pure replace via the file-local _replace_item helper
# ---------------------------------------------------------------------------
class TestPatternRReplace:
    def test_replace_persists_mutation_without_duplicating(
        self, session, api_url, scratch_collection
    ):
        item = _make_item("probe-item-r1", scratch_collection)
        resp = _post_item(session, api_url, scratch_collection, item)
        assert resp.status_code in (200, 201), f"seed POST failed: {resp.text}"

        item.properties["constellation"] = "probe-mutated"
        manage_item_module._replace_item(session, api_url, scratch_collection, item)

        features = _get_item_features(session, api_url, scratch_collection)
        assert len(features) == 1, "PUT must replace, never duplicate"
        stored = features[0]
        assert stored["id"] == "probe-item-r1"
        assert stored["properties"]["constellation"] == "probe-mutated"
        assert stored["geometry"] == {"type": "Point", "coordinates": [0.0, 0.0]}


# ---------------------------------------------------------------------------
# Pattern U — upsert via the real register_v1.upsert_item
# ---------------------------------------------------------------------------
class TestPatternUUpsert:
    def test_upsert_creates_then_replaces(self, session, api_url, scratch_collection):
        import pystac_client

        client = pystac_client.Client.open(api_url)
        item = _make_item("probe-item-u1", scratch_collection)
        _assert_scratch(scratch_collection)

        # Create path: item absent → upsert must POST (a PUT here would 404).
        register_v1.upsert_item(client, scratch_collection, item)
        matching = [
            f
            for f in _get_item_features(session, api_url, scratch_collection)
            if f["id"] == "probe-item-u1"
        ]
        assert len(matching) == 1, "create path must register the item exactly once"

        # Replace path: item present → upsert must PUT the mutation in place.
        item.properties["constellation"] = "probe-mutated"
        register_v1.upsert_item(client, scratch_collection, item)
        matching = [
            f
            for f in _get_item_features(session, api_url, scratch_collection)
            if f["id"] == "probe-item-u1"
        ]
        assert len(matching) == 1, "replace must never duplicate"
        assert matching[0]["properties"]["constellation"] == "probe-mutated"
        assert matching[0]["geometry"] == {"type": "Point", "coordinates": [0.0, 0.0]}
