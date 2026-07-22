"""Unit tests for scripts/repair_stac_raster_links.py (#371 repair tool)."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
import repair_stac_raster_links as mod  # noqa: E402

API = "https://api.explorer.eopf.copernicus.eu/stac"
CORRUPT = "https://api.explorer.eopf.copernicus.eu/stac/raster"
CLEAN = "https://api.explorer.eopf.copernicus.eu/raster"


def make_item(item_id: str = "item-1", corrupted: bool = True) -> dict:
    base = CORRUPT if corrupted else CLEAN
    return {
        "id": item_id,
        "properties": {"updated": "2026-07-22T10:00:00Z"},
        "links": [
            {"rel": "self", "href": f"{API}/collections/c/items/{item_id}"},
            {"rel": "viewer", "href": f"{base}/collections/c/items/{item_id}/viewer"},
            {
                "rel": "xyz",
                "href": f"{base}/collections/c/items/{item_id}/tiles/{{z}}/{{x}}/{{y}}.png",
            },
            {"rel": "tilejson", "href": f"{base}/collections/c/items/{item_id}/tilejson.json"},
            {"rel": "via", "href": "https://stac.core.eopf.eodc.eu/collections/x"},
        ],
        "assets": {"thumb": {"href": f"{CLEAN}/collections/c/items/{item_id}/preview"}},
    }


def response(json_data=None, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


def make_run(session, tmp_path, max_items=100, apply=True, collection="c"):
    return mod.RepairRun(
        session=session,
        api_url=API,
        collection=collection,
        max_items=max_items,
        apply=apply,
        backup_dir=tmp_path / "backups",
    )


# ---------- transform ----------


def test_repair_links_rewrites_each_corrupted_rel():
    repaired, changed = mod.repair_links(make_item())
    assert changed == 3
    by_rel = {link["rel"]: link["href"] for link in repaired["links"]}
    for rel in ("viewer", "xyz", "tilejson"):
        assert by_rel[rel].startswith(CLEAN + "/")
        assert "/stac/raster/" not in by_rel[rel]


def test_repair_links_idempotent():
    once, _ = mod.repair_links(make_item())
    twice, changed = mod.repair_links(once)
    assert changed == 0
    assert twice == once


def test_repair_links_noop_on_clean_item():
    clean = make_item(corrupted=False)
    snapshot = copy.deepcopy(clean)
    repaired, changed = mod.repair_links(clean)
    assert changed == 0
    assert repaired == snapshot
    assert clean == snapshot  # input never mutated


def test_repair_links_never_touches_assets_or_foreign_hrefs():
    item = make_item()
    item["links"].append({"rel": "odd", "href": "https://other.host/stac/raster/x"})
    item["links"].append({"rel": "mid", "href": f"{API}/x?next={CORRUPT}/y"})
    repaired, _ = mod.repair_links(item)
    assert repaired["assets"] == item["assets"]
    by_rel = {link["rel"]: link["href"] for link in repaired["links"]}
    assert by_rel["odd"] == "https://other.host/stac/raster/x"  # other host untouched
    assert by_rel["mid"] == f"{API}/x?next={CORRUPT}/y"  # mid-string untouched
    assert by_rel["self"] == f"{API}/collections/c/items/item-1"
    assert by_rel["via"] == "https://stac.core.eopf.eodc.eu/collections/x"


# ---------- bound / dry-run / backup ----------


def test_max_items_bounds_writes_exactly(tmp_path):
    ids = [f"i{n}" for n in range(5)]
    session = MagicMock()
    puts: list[str] = []

    def get(url, **kw):
        item_id = url.rsplit("/", 1)[-1]
        # corrupted until PUT, clean afterwards (so verify-GETs pass)
        done = any(p.endswith(f"/{item_id}") for p in puts)
        return response(make_item(item_id, corrupted=not done))

    def put(url, **kw):
        puts.append(url)
        return response(status=200)

    session.get.side_effect = get
    session.put.side_effect = put
    run = make_run(session, tmp_path, max_items=2)
    run.repair(ids)
    assert len(puts) == 2
    assert run.truncated is True
    assert "truncated=True" in run.summary()


def test_dry_run_is_default_and_writes_nothing(tmp_path):
    session = MagicMock()
    session.get.return_value = response(make_item())
    run = make_run(session, tmp_path, apply=False)
    run.repair(["item-1"])
    session.put.assert_not_called()
    assert not (tmp_path / "backups").exists()  # no backup file in dry-run
    assert "DRY-RUN" in run.summary()


def test_backup_written_and_fsynced_before_put(tmp_path):
    session = MagicMock()
    original = make_item("item-1")
    session.get.return_value = response(original)
    session.put.side_effect = RuntimeError("boom mid-write")
    run = make_run(session, tmp_path)
    with pytest.raises(RuntimeError):
        run.repair(["item-1"])
    backups = list((tmp_path / "backups").glob("raster-link-repair-c-*.jsonl"))
    assert len(backups) == 1
    line = json.loads(backups[0].read_text().splitlines()[0])
    assert line["id"] == "item-1"
    assert line["item"] == original  # pre-write doc, corrupted links included


def test_put_404_skips_and_never_posts(tmp_path):
    session = MagicMock()
    session.get.return_value = response(make_item())
    session.put.return_value = response(status=404)
    run = make_run(session, tmp_path)
    run.repair(["item-1"])
    session.post.assert_not_called()  # no resurrection
    assert run.failures == 1


def test_verify_mismatch_counts_failures_and_aborts_after_three(tmp_path):
    session = MagicMock()
    # GET always returns a corrupted item (both discovery-GET and verify-GET)
    session.get.side_effect = lambda url, **kw: response(make_item(url.rsplit("/", 1)[-1]))
    session.put.return_value = response(status=200)
    run = make_run(session, tmp_path)
    run.repair([f"i{n}" for n in range(10)])
    assert run.failures == 3  # aborted at 3 consecutive
    assert run.written == 3


# ---------- restore ----------


def restore_fixture(tmp_path, updated_after="2026-07-22T15:00:00Z"):
    backup = tmp_path / "b.jsonl"
    backup.write_text(json.dumps({"collection": "c", "id": "item-1", "item": make_item()}) + "\n")
    results = tmp_path / "b.results.jsonl"
    results.write_text(json.dumps({"id": "item-1", "updated_after": updated_after}) + "\n")
    return backup


def test_restore_puts_doc_verbatim_and_warns(tmp_path, caplog):
    backup = restore_fixture(tmp_path)
    session = MagicMock()
    session.get.return_value = response(
        {"id": "item-1", "properties": {"updated": "2026-07-22T15:00:00Z"}}
    )
    session.put.return_value = response(status=200)
    run = make_run(session, tmp_path)
    with caplog.at_level("WARNING"):
        run.restore(backup, force=False)
    assert "corrupted /stac/raster links" in caplog.text  # loud by design
    put_body = session.put.call_args.kwargs["json"]
    assert put_body == make_item()  # verbatim, corrupted links included
    assert run.written == 1


def test_restore_staleness_guard_refuses_without_force(tmp_path):
    backup = restore_fixture(tmp_path, updated_after="2026-07-22T15:00:00Z")
    session = MagicMock()
    session.get.return_value = response(
        {"id": "item-1", "properties": {"updated": "2026-07-23T09:00:00Z"}}  # re-registered since
    )
    run = make_run(session, tmp_path)
    run.restore(backup, force=False)
    session.put.assert_not_called()
    assert run.failures == 1


def test_restore_force_overrides_staleness(tmp_path):
    backup = restore_fixture(tmp_path)
    session = MagicMock()
    session.put.return_value = response(status=200)
    run = make_run(session, tmp_path)
    run.restore(backup, force=True)
    session.get.assert_not_called()  # guard skipped entirely
    assert run.written == 1


def test_restore_honors_dry_run_and_max_items(tmp_path):
    backup = tmp_path / "b.jsonl"
    lines = [
        json.dumps({"collection": "c", "id": f"i{n}", "item": make_item(f"i{n}")}) for n in range(3)
    ]
    backup.write_text("\n".join(lines) + "\n")
    session = MagicMock()
    dry = make_run(session, tmp_path, apply=False)
    dry.restore(backup, force=True)
    session.put.assert_not_called()

    session2 = MagicMock()
    session2.put.return_value = response(status=200)
    bounded = make_run(session2, tmp_path, max_items=2)
    bounded.restore(backup, force=True)
    assert session2.put.call_count == 2
    assert bounded.truncated is True


# ---------- discovery ----------


def test_discovery_request_shape_and_client_side_predicate():
    session = MagicMock()
    page = {
        "features": [make_item("bad-1"), make_item("clean-1", corrupted=False)],
        "links": [],
    }
    session.post.return_value = response(page)
    ids = mod.discover_corrupted_ids(session, API, "c", "2026-07-21T11:15:00Z")
    assert ids == ["bad-1"]
    body = session.post.call_args.kwargs["json"]
    assert body["filter-lang"] == "cql2-json"
    # string comparison (pgstac `updated` is text — TIMESTAMP() errors)
    assert body["filter"] == {
        "op": ">=",
        "args": [{"property": "updated"}, "2026-07-21T11:15:00Z"],
    }
    assert body["collections"] == ["c"]
    assert "links" in body["fields"]["include"]


def test_discovery_follows_next_pagination():
    session = MagicMock()
    page1 = {
        "features": [make_item("bad-1")],
        "links": [{"rel": "next", "href": f"{API}/search", "body": {"token": "next:x"}}],
    }
    page2 = {"features": [make_item("bad-2")], "links": []}
    session.post.side_effect = [response(page1), response(page2)]
    ids = mod.discover_corrupted_ids(session, API, "c", "2026-07-21T11:15:00Z")
    assert ids == ["bad-1", "bad-2"]
    assert session.post.call_count == 2


# ---------- session auth ----------


def test_session_wires_bearer_auth_hook():
    session = mod.make_session()
    assert session.auth is mod.stac_auth.bearer_auth
