"""Unit tests for register_v0.py upsert_item (issue #352).

Mirrors test_register_v1. The create-path test is load-bearing: register_v0's
old existence check treated "collection resolves" as "item exists" (get_item
returns None for an absent item, it does not raise), which under exists→PUT
would turn every first-time registration into a PUT → 404 → crash.
"""

import contextlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v0 import upsert_item  # noqa: E402


def _make_client(item_exists: bool, base_url: str = "https://stac.example.com") -> MagicMock:
    """Build a minimal pystac_client.Client mock."""
    client = MagicMock()
    client.self_href = base_url

    # get_item returns the item if present, None if absent — it does NOT raise.
    client.get_collection.return_value.get_item.return_value = MagicMock() if item_exists else None

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


class TestUpsertItemNewItem:
    """get_item returns None → item is new → POST; no DELETE, no PUT."""

    def test_none_from_get_item_takes_post_path(self):
        client = _make_client(item_exists=False)
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("new-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.post.assert_called_once()
        post_call = client._stac_io.session.post.call_args
        assert post_call.args[0] == "https://stac.example.com/collections/my-collection/items"
        client._stac_io.session.put.assert_not_called()
        client._stac_io.session.delete.assert_not_called()

    def test_raises_on_post_failure(self):
        client = _make_client(item_exists=False)
        client._stac_io.session.post.return_value = _make_response(500)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

    def test_exists_check_error_falls_back_to_post_and_logs(self, caplog):
        """A transient exists-check failure takes the POST path — and says so,
        because an existing item routed to POST surfaces as a confusing 409."""
        import logging

        client = _make_client(item_exists=False)
        client.get_collection.side_effect = Exception("API unreachable")
        client._stac_io.session.post.return_value = _make_response(201)

        with caplog.at_level(logging.DEBUG, logger="register_v0"):
            upsert_item(client, "my-collection", _make_item())

        client._stac_io.session.post.assert_called_once()
        client._stac_io.session.put.assert_not_called()
        assert "exists-check failed" in caplog.text


class TestUpsertItemReplaceExisting:
    """Item exists → single PUT to the item URL; no DELETE, no POST."""

    def test_put_when_item_exists(self):
        client = _make_client(item_exists=True, base_url="https://stac.example.com")
        client._stac_io.session.put.return_value = _make_response(200)
        item = _make_item("existing-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.put.assert_called_once()
        put_call = client._stac_io.session.put.call_args
        assert put_call.args[0] == (
            "https://stac.example.com/collections/my-collection/items/existing-item"
        )
        assert put_call.kwargs["json"] == item.to_dict()
        client._stac_io.session.delete.assert_not_called()
        client._stac_io.session.post.assert_not_called()


class TestUpsertItemPutFailure:
    """PUT fails (incl. ghost-id 404) → raises; POST is never a fallback."""

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_raises_on_put_failure(self, status_code):
        client = _make_client(item_exists=True)
        client._stac_io.session.put.return_value = _make_response(status_code)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

    def test_post_not_called_when_put_fails(self):
        client = _make_client(item_exists=True)
        client._stac_io.session.put.return_value = _make_response(404)

        with contextlib.suppress(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

        client._stac_io.session.post.assert_not_called()
