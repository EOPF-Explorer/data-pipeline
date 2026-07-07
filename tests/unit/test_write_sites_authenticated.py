"""Each STAC write site authenticates via the shared OIDC helper (plan Task 3).

Two proof styles:
- **Behavioral** (session-based sites): with OIDC env set + a mocked token endpoint,
  the outgoing write carries ``Authorization: Bearer …`` — and nothing when env is unset.
- **Delegation** (pystac sites): the write path opens its client through
  ``stac_auth.open_client`` (unit-tested in test_stac_auth to attach the header), not a
  bare ``Client.open``.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))


def _httpx_cm(get_json):
    """A context-manager httpx.Client mock whose .get returns get_json."""
    resp = MagicMock()
    resp.json.return_value = get_json
    resp.raise_for_status.return_value = None
    http = MagicMock()
    http.get.return_value = resp
    http.__enter__ = MagicMock(return_value=http)
    http.__exit__ = MagicMock(return_value=False)
    return http


def _source_item_dict():
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "SRC_ITEM",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "bbox": [0.0, 0.0, 0.0, 0.0],
        "properties": {"datetime": "2020-01-01T00:00:00Z"},
        "links": [],
        "assets": {
            "data": {
                "href": "s3://src/SRC_ITEM.zarr/measurements",
                "type": "application/vnd.zarr",
            }
        },
        "collection": "src-collection",
    }


# --- Behavioral: session-based write sites -----------------------------------


def test_aggregate_items_put_authenticated(oidc_env, token_response):
    import aggregate_items

    captured = {}

    def mock_put(url, json=None, headers=None):  # noqa: ARG001
        captured["headers"] = headers
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    http = _httpx_cm({"id": "c", "links": []})
    http.put = mock_put
    with (
        patch("stac_auth.httpx.post", return_value=token_response()),
        patch("aggregate_items.httpx.Client", return_value=http),
    ):
        aggregate_items.update_collection_links(
            "https://api.test/stac", "c", "https://s3.test", "bucket", "prefix"
        )
    assert captured["headers"]["Authorization"] == "Bearer test-token"


def test_aggregate_items_put_unauthenticated_without_env():
    import aggregate_items

    captured = {}

    def mock_put(url, json=None, headers=None):  # noqa: ARG001
        captured["headers"] = headers
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        return resp

    http = _httpx_cm({"id": "c", "links": []})
    http.put = mock_put
    with patch("aggregate_items.httpx.Client", return_value=http):
        aggregate_items.update_collection_links(
            "https://api.test/stac", "c", "https://s3.test", "bucket", "prefix"
        )
    assert "Authorization" not in captured["headers"]


def _auth_header_on_write(session) -> str | None:
    """Send a request through the session's per-request auth hook, return its header."""
    prepared = session.auth(requests.Request("POST", "https://api.test/stac/collections").prepare())
    return prepared.headers.get("Authorization")


def test_manage_collections_session_authenticated(oidc_env, token_response):
    import manage_collections

    mgr = manage_collections.STACCollectionManager("https://api.test/stac")
    with patch("stac_auth.httpx.post", return_value=token_response()):
        assert _auth_header_on_write(mgr.session) == "Bearer test-token"


def test_manage_collections_session_unauthenticated_without_env():
    import manage_collections

    mgr = manage_collections.STACCollectionManager("https://api.test/stac")
    assert _auth_header_on_write(mgr.session) is None


def test_manage_item_session_authenticated(oidc_env, token_response):
    import manage_item

    mgr = manage_item.STACItemManager("https://api.test/stac")
    with patch("stac_auth.httpx.post", return_value=token_response()):
        assert _auth_header_on_write(mgr.session) == "Bearer test-token"


def test_manage_item_session_unauthenticated_without_env():
    import manage_item

    mgr = manage_item.STACItemManager("https://api.test/stac")
    assert _auth_header_on_write(mgr.session) is None


# --- Delegation: pystac write sites open via the helper ----------------------


def _authed_client_mock():
    client = MagicMock()
    client.self_href = "https://api.test/stac"
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    client._stac_io.session.post.return_value = resp
    return client


def test_register_per_acquisition_opens_via_helper():
    import register_per_acquisition as rpa

    client = _authed_client_mock()
    with patch("register_per_acquisition.stac_auth.open_client", return_value=client) as mock_open:
        rpa._upsert_items(
            "https://api.test/stac",
            "col",
            [{"id": "a", "properties": {"datetime": "2020-01-01T00:00:00Z"}}],
        )
    mock_open.assert_called_once_with("https://api.test/stac")
    client._stac_io.session.post.assert_called_once()


def test_register_v1_s1_rtc_opens_via_helper():
    import register_v1_s1_rtc as mod

    item = MagicMock()
    item.id = "s1-rtc-31TCH"
    with (
        patch(f"{mod.__name__}.build_s1_rtc_stac_item", return_value=item),
        patch(f"{mod.__name__}.warm_thumbnail_cache"),
        patch(f"{mod.__name__}.slice_coverages", return_value=[]),
        patch(f"{mod.__name__}.upsert_item"),
        patch(f"{mod.__name__}.stac_auth.open_client") as mock_open,
    ):
        mod.register(
            "s3://bucket/s1-rtc-31TCH.zarr",
            "sentinel-1-grd-rtc-tests",
            "https://api.test/stac",
            "https://raster.test",
            "https://s3.test",
        )
    mock_open.assert_called_once_with("https://api.test/stac")


def test_register_v0_opens_via_helper():
    import register_v0

    with (
        patch("register_v0.httpx.Client", return_value=_httpx_cm(_source_item_dict())),
        patch("register_v0.stac_auth.open_client") as mock_open,
        patch("register_v0.upsert_item") as mock_upsert,
    ):
        register_v0.run_registration(
            "https://src/SRC_ITEM.json",
            "col",
            "https://api.test/stac",
            "https://raster.test",
            "https://s3.test",
            "bucket",
            "prefix",
        )
    mock_open.assert_called_once_with("https://api.test/stac")
    mock_upsert.assert_called_once()


def test_wipe_s1rtc_open_session_via_helper():
    """wipe_s1rtc_tiles DELETEs through the session from stac_auth.open_client, not a bare
    Client.open — so the wipe authenticates once enforcement is on."""
    import wipe_s1rtc_tiles

    client = MagicMock()
    client.self_href = "https://api.test/stac"
    with patch("wipe_s1rtc_tiles.stac_auth.open_client", return_value=client) as mock_open:
        session, base = wipe_s1rtc_tiles._open_session("https://api.test/stac")
    mock_open.assert_called_once_with("https://api.test/stac")
    assert session is client._stac_io.session
    assert base == "https://api.test/stac"


def test_migrate_runner_session_authenticated(oidc_env, token_response):
    from _migrate_catalog.runner import STACMigrationRunner

    runner = STACMigrationRunner("https://api.test/stac")
    with patch("stac_auth.httpx.post", return_value=token_response()):
        assert _auth_header_on_write(runner.session) == "Bearer test-token"


def test_migrate_runner_session_unauthenticated_without_env():
    from _migrate_catalog.runner import STACMigrationRunner

    runner = STACMigrationRunner("https://api.test/stac")
    assert _auth_header_on_write(runner.session) is None


def test_register_v1_opens_via_helper():
    import register_v1

    with (
        patch("register_v1.httpx.Client", return_value=_httpx_cm(_source_item_dict())),
        patch("register_v1.consolidate_reflectance_assets"),
        patch("register_v1.add_alternate_s3_assets"),
        patch("register_v1.add_thumbnail_asset"),
        patch("register_v1.warm_thumbnail_cache"),
        patch("register_v1.stac_auth.open_client") as mock_open,
        patch("register_v1.upsert_item") as mock_upsert,
    ):
        register_v1.run_registration(
            "https://src/SRC_ITEM.json",
            "col",
            "https://api.test/stac",
            "https://raster.test",
            "https://s3.test",
            "bucket",
            "prefix",
        )
    mock_open.assert_called_once_with("https://api.test/stac")
    mock_upsert.assert_called_once()
