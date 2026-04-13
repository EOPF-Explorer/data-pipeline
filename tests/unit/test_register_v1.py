"""Unit tests for register_v1.py — upsert_item."""

import contextlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests

# Add scripts directory to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

from register_v1 import upsert_item  # noqa: E402


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
