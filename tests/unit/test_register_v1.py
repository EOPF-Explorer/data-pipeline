"""Unit tests for register_v1.py — upsert_item + expires stamping."""

import contextlib
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pystac import Item

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v1 import (  # noqa: E402
    TIMESTAMPS_EXTENSION,
    add_expires,
    resolve_retention_days,
    upsert_item,
)


def _make_client(item_exists: bool, base_url: str = "https://stac.example.com") -> MagicMock:
    """Build a minimal pystac_client.Client mock."""
    client = MagicMock()
    client.self_href = base_url

    if item_exists:
        # get_item() returns normally → exists = True
        client.get_collection.return_value.get_item.return_value = MagicMock()
    else:
        # get_item() raises → exists = False
        client.get_collection.return_value.get_item.side_effect = Exception("not found")

    return client


def _make_response(status_code: int) -> Mock:
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Error", response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


def _make_item(item_id: str = "test-item-001") -> MagicMock:
    item = MagicMock()
    item.id = item_id
    item.to_dict.return_value = {"id": item_id, "type": "Feature"}
    return item


class TestUpsertItemDeleteFailure:
    """DELETE fails → raise_for_status raises → POST must not be called."""

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_raises_on_delete_failure(self, status_code):
        client = _make_client(item_exists=True)
        client._stac_io.session.delete.return_value = _make_response(status_code)
        item = _make_item()

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", item)

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_post_not_called_when_delete_fails(self, status_code):
        client = _make_client(item_exists=True)
        client._stac_io.session.delete.return_value = _make_response(status_code)
        item = _make_item()

        with contextlib.suppress(requests.HTTPError):
            upsert_item(client, "my-collection", item)

        client._stac_io.session.post.assert_not_called()


class TestUpsertItemDeleteSuccess:
    """DELETE succeeds → POST is called with the item payload."""

    def test_delete_then_post_when_item_exists(self):
        client = _make_client(item_exists=True, base_url="https://stac.example.com")
        client._stac_io.session.delete.return_value = _make_response(200)
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("existing-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.delete.assert_called_once_with(
            "https://stac.example.com/collections/my-collection/items/existing-item",
            timeout=30,
        )
        client._stac_io.session.post.assert_called_once()
        post_call = client._stac_io.session.post.call_args
        assert post_call.kwargs["json"] == item.to_dict()


class TestUpsertItemNewItem:
    """Item does not exist → no DELETE, POST called directly."""

    def test_no_delete_when_item_is_new(self):
        client = _make_client(item_exists=False)
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("new-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.delete.assert_not_called()
        client._stac_io.session.post.assert_called_once()

    def test_post_url_for_new_item(self):
        client = _make_client(item_exists=False, base_url="https://stac.example.com")
        client._stac_io.session.post.return_value = _make_response(201)

        upsert_item(client, "sentinel-2", _make_item())

        post_call = client._stac_io.session.post.call_args
        assert post_call.args[0] == "https://stac.example.com/collections/sentinel-2/items"

    def test_raises_on_post_failure(self):
        client = _make_client(item_exists=False)
        client._stac_io.session.post.return_value = _make_response(500)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())


# === expires stamping (coordination#183, Task 2) ===


def _real_item(item_id: str = "S2_test") -> Item:
    """A minimal real pystac Item for exercising add_expires."""
    return Item(
        id=item_id,
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        bbox=[0.0, 0.0, 0.0, 0.0],
        datetime=datetime(2024, 1, 1, tzinfo=UTC),
        properties={},
    )


class TestAddExpires:
    """add_expires stamps properties.expires + the timestamps extension."""

    def test_sets_expires_roughly_retention_days_ahead(self) -> None:
        item = _real_item()
        add_expires(item, 183)

        assert "expires" in item.properties
        expires = datetime.strptime(item.properties["expires"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
        delta = expires - datetime.now(UTC)
        # Allow a small window for wall-clock drift during the test.
        assert timedelta(days=182, hours=23) < delta <= timedelta(days=183)

    def test_expires_is_utc_z_formatted(self) -> None:
        item = _real_item()
        add_expires(item, 183)
        assert item.properties["expires"].endswith("Z")

    def test_appends_timestamps_extension_exactly_once(self) -> None:
        item = _real_item()
        add_expires(item, 183)
        add_expires(item, 183)  # re-stamp must not duplicate the extension URL
        assert item.stac_extensions.count(TIMESTAMPS_EXTENSION) == 1

    def test_zero_retention_is_a_noop(self) -> None:
        item = _real_item()
        add_expires(item, 0)
        assert "expires" not in item.properties
        assert TIMESTAMPS_EXTENSION not in item.stac_extensions

    def test_negative_retention_is_a_noop(self) -> None:
        item = _real_item()
        add_expires(item, -5)
        assert "expires" not in item.properties
        assert TIMESTAMPS_EXTENSION not in item.stac_extensions


class TestResolveRetentionDays:
    """resolve_retention_days reads EXPIRES_RETENTION_DAYS, default 183."""

    def test_defaults_to_183_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EXPIRES_RETENTION_DAYS", raising=False)
        assert resolve_retention_days() == 183

    def test_reads_override_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EXPIRES_RETENTION_DAYS", "30")
        assert resolve_retention_days() == 30

    def test_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("EXPIRES_RETENTION_DAYS", "0")
        assert resolve_retention_days() == 0

    def test_empty_env_falls_back_to_default_not_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # An empty value in a manifest must not crash the registration hot path.
        monkeypatch.setenv("EXPIRES_RETENTION_DAYS", "")
        assert resolve_retention_days() == 183
