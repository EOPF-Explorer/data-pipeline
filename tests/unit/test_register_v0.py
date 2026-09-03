"""Unit tests for register_v0.py upsert_item (issue #352).

Mirrors test_register_v1: POST first, and on 409 (item exists) replace via a
single atomic PUT. No client-side existence pre-check (it can mis-read
transient/conformance errors as "absent" and then 409, #186) and never a
DELETE (no code path can leave the item deleted-but-not-recreated, #352).
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


def _make_client(base_url: str = "https://stac.example.com") -> MagicMock:
    """Build a minimal pystac_client.Client mock — only self_href + _stac_io.session used."""
    client = MagicMock()
    client.self_href = base_url
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
    """First POST succeeds (item did not exist) → single POST; no PUT, no DELETE."""

    def test_post_path_for_new_item(self):
        client = _make_client()
        client._stac_io.session.post.return_value = _make_response(201)
        item = _make_item("new-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.post.assert_called_once()
        post_call = client._stac_io.session.post.call_args
        assert post_call.args[0] == "https://stac.example.com/collections/my-collection/items"
        client._stac_io.session.put.assert_not_called()
        client._stac_io.session.delete.assert_not_called()

    def test_raises_on_post_failure_without_put(self):
        """A non-409 POST error (e.g. 500) raises immediately — no PUT, no DELETE."""
        client = _make_client()
        client._stac_io.session.post.return_value = _make_response(500)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

        client._stac_io.session.put.assert_not_called()
        client._stac_io.session.delete.assert_not_called()


class TestUpsertItemReplaceExisting:
    """POST 409 (item exists) → single atomic PUT to the item URL; no DELETE."""

    def test_409_triggers_put_replace(self):
        client = _make_client(base_url="https://stac.example.com")
        client._stac_io.session.post.return_value = _make_response(409)
        client._stac_io.session.put.return_value = _make_response(200)
        item = _make_item("existing-item")

        upsert_item(client, "my-collection", item)

        client._stac_io.session.post.assert_called_once()
        client._stac_io.session.put.assert_called_once()
        put_call = client._stac_io.session.put.call_args
        assert put_call.args[0] == (
            "https://stac.example.com/collections/my-collection/items/existing-item"
        )
        assert put_call.kwargs["json"] == item.to_dict()
        client._stac_io.session.delete.assert_not_called()


class TestUpsertItemPutFailure:
    """POST 409, then PUT fails (incl. ghost-id 404) → raises; re-POST is never a fallback."""

    @pytest.mark.parametrize("status_code", [403, 404, 500, 503])
    def test_raises_on_put_failure(self, status_code):
        client = _make_client()
        client._stac_io.session.post.return_value = _make_response(409)
        client._stac_io.session.put.return_value = _make_response(status_code)

        with pytest.raises(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

    def test_no_repost_when_put_fails(self):
        client = _make_client()
        client._stac_io.session.post.return_value = _make_response(409)
        client._stac_io.session.put.return_value = _make_response(404)

        with contextlib.suppress(requests.HTTPError):
            upsert_item(client, "my-collection", _make_item())

        # only the first POST (the 409) was made; no re-POST after the PUT failed
        assert client._stac_io.session.post.call_count == 1
        client._stac_io.session.delete.assert_not_called()
