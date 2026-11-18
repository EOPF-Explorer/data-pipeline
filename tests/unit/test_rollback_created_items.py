"""Unit tests for scripts/rollback_created_items.py (plan rev 2, T10)."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
from rollback_created_items import (
    EODC_SOURCE_ITEMS,
    main,
    not_ours,
    read_created_ids,
    roll_back,
)

C = "sentinel-2-l2a-samples-zarr3-rollback"
API = "https://stac.example.com"


def _eodc_item(item_id: str) -> dict:
    """What the pipeline creates: data on data.eodc.eu, no S3 copy, derived from EODC."""
    return {
        "id": item_id,
        "assets": {
            "reflectance": {
                "href": "https://data.eodc.eu/x.zarr/r",
                "roles": ["data", "reflectance"],
            },
            "SCL_20m": {"href": "https://data.eodc.eu/x.zarr/scl", "roles": ["data"]},
        },
        "links": [{"rel": "derived_from", "href": EODC_SOURCE_ITEMS + item_id}],
    }


def _converted_item(item_id: str) -> dict:
    """A converted prod item with the same id: our S3 store, alternate.s3."""
    href = "https://s3.explorer.eopf.copernicus.eu/b/x.zarr/r"
    return {
        "id": item_id,
        "assets": {
            "reflectance": {
                "href": href,
                "roles": ["data"],
                "alternate": {"s3": {"href": "s3://b/x.zarr/r"}},
            }
        },
        "links": [],
    }


def _write(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "created-ids.jsonl"
    path.write_text("".join(lines))
    return path


def _line(item_id: str, collection: str = C, **extra: bool) -> str:
    return json.dumps({"id": item_id, "collection": collection, "ts": "t", **extra}) + "\n"


class _Resp:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code = status
        self._body = body

    def json(self) -> dict | None:
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


class _Session:
    """GET answers from `items` (absent ⇒ 404); DELETE answers `delete_status`."""

    def __init__(self, items: dict[str, dict], delete_status: int = 204) -> None:
        self.items = items
        self.delete_status = delete_status
        self.deleted: list[str] = []

    def get(self, url: str, timeout: float) -> _Resp:
        item_id = url.rsplit("/", 1)[1]
        return _Resp(200, self.items[item_id]) if item_id in self.items else _Resp(404)

    def delete(self, url: str, timeout: float) -> _Resp:
        self.deleted.append(url.rsplit("/", 1)[1])
        return _Resp(self.delete_status)


class TestReadCreatedIds:
    def test_uncertain_lines_are_included_and_ids_deduplicated(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _line("a"), _line("b", uncertain=True), _line("a"))
        assert read_created_ids(path, C) == ["a", "b"]

    def test_a_torn_last_line_is_skipped(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _line("a"), '{"id": "b", "coll')
        assert read_created_ids(path, C) == ["a"]

    def test_a_bad_line_before_the_last_refuses_the_list(self, tmp_path: Path) -> None:
        path = _write(tmp_path, '{"id": "a", "coll\n', _line("b"))
        with pytest.raises(SystemExit, match="not JSON"):
            read_created_ids(path, C)

    def test_a_line_for_another_collection_refuses_the_list(self, tmp_path: Path) -> None:
        path = _write(tmp_path, _line("a"), _line("b", collection="sentinel-2-l2a"))
        with pytest.raises(SystemExit, match="sentinel-2-l2a"):
            read_created_ids(path, C)


class TestNotOurs:
    def test_a_pipeline_item_is_ours(self) -> None:
        assert not_ours(_eodc_item("a"), "a") is None

    def test_a_converted_prod_item_is_refused(self) -> None:
        assert not_ours(_converted_item("a"), "a") is not None

    def test_alternate_s3_on_an_eodc_item_is_refused(self) -> None:
        item = _eodc_item("a")
        item["assets"]["SCL_20m"]["alternate"] = {"s3": {"href": "s3://b/x"}}
        assert "alternate.s3" in not_ours(item, "a")

    def test_a_data_asset_elsewhere_is_refused(self) -> None:
        item = _eodc_item("a")
        item["assets"]["SCL_20m"]["href"] = "https://objects.example.com/x.zarr/scl"
        assert "not on data.eodc.eu" in not_ours(item, "a")

    def test_derived_from_another_id_is_refused(self) -> None:
        assert "derived_from" in not_ours(_eodc_item("a"), "b")

    def test_no_data_assets_is_refused(self) -> None:
        item = _eodc_item("a")
        item["assets"] = {
            "thumbnail": {"href": "https://data.eodc.eu/t.png", "roles": ["thumbnail"]}
        }
        assert not_ours(item, "a") == "no data assets"


class TestRollBack:
    def test_a_dry_run_deletes_nothing(self) -> None:
        session = _Session({"a": _eodc_item("a")})
        counts = roll_back(session, API, C, ["a"], apply=False)
        assert session.deleted == []
        assert counts == {"would_delete": 1}

    def test_apply_deletes_ours_refuses_the_rest_and_counts_absent(self) -> None:
        session = _Session({"a": _eodc_item("a"), "p": _converted_item("p")})
        counts = roll_back(session, API, C, ["a", "p", "gone"], apply=True)
        assert session.deleted == ["a"]
        assert counts == {"deleted": 1, "refused": 1, "absent": 1}

    def test_a_delete_answering_404_is_a_failure(self) -> None:
        session = _Session({"a": _eodc_item("a")}, delete_status=404)
        assert roll_back(session, API, C, ["a"], apply=True) == {"failed": 1}


class TestMain:
    def _run(self, tmp_path: Path, session: _Session, *extra: str, typed: str = C) -> int:
        path = _write(tmp_path, _line("a"), _line("b"))
        argv = ["--created-ids", str(path), "--collection", C, "--stac-api-url", API, *extra]
        with (
            patch("rollback_created_items.requests.Session", return_value=session),
            patch("builtins.input", return_value=typed),
        ):
            return main(argv)

    def test_over_max_items_is_refused_before_any_request(self, tmp_path: Path) -> None:
        session = _Session({"a": _eodc_item("a"), "b": _eodc_item("b")})
        with patch.object(session, "get") as get:
            assert self._run(tmp_path, session, "--max-items", "1", "--apply") == 1
        get.assert_not_called()
        assert session.deleted == []

    def test_a_wrong_confirmation_deletes_nothing(self, tmp_path: Path) -> None:
        session = _Session({"a": _eodc_item("a"), "b": _eodc_item("b")})
        assert (
            self._run(tmp_path, session, "--max-items", "2", "--apply", typed="sentinel-2-l2a") == 1
        )
        assert session.deleted == []

    def test_confirmed_apply_deletes_and_exits_0(self, tmp_path: Path) -> None:
        session = _Session({"a": _eodc_item("a"), "b": _eodc_item("b")})
        assert self._run(tmp_path, session, "--max-items", "2", "--apply") == 0
        assert session.deleted == ["a", "b"]

    def test_a_refusal_exits_1(self, tmp_path: Path) -> None:
        session = _Session({"a": _eodc_item("a"), "b": _converted_item("b")})
        assert self._run(tmp_path, session, "--max-items", "2", "--apply") == 1
        assert session.deleted == ["a"]
