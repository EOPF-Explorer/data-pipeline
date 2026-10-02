"""Unit tests for operator-tools/fill_stac_item_platform.py (#451 STAC follow-up)."""

import copy
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "operator-tools"))
import fill_stac_item_platform as mod  # noqa: E402
import repair_stac_raster_links as rsrl  # noqa: E402

API = "https://api.explorer.eopf.copernicus.eu/stac"
COLL = "sentinel-1-grd-rtc-acquisitions-staging"


def make_item(item_id: str, platform: str | None = None) -> dict:
    props = {"datetime": "2026-07-02T16:58:56Z", "updated": "2026-07-03T13:40:35Z"}
    if platform is not None:
        props["platform"] = platform
    return {
        "id": item_id,
        "properties": props,
        "links": [
            {"rel": "self", "href": f"{API}/collections/{COLL}/items/{item_id}"},
            {"rel": "xyz", "href": f"https://api.explorer.eopf.copernicus.eu/raster/{item_id}"},
        ],
        "assets": {"vh": {"href": f"s3://bucket/{item_id}.zarr"}},
    }


def response(json_data=None, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


class FakeStac:
    """Items by id; a PUT stores the doc, stamps `updated` and reorders `links` like a real API may."""

    def __init__(self, items, mangle=None):
        self.items = {i["id"]: copy.deepcopy(i) for i in items}
        self.puts: list[str] = []
        self.mangle = mangle

    def get(self, url, **kw):
        return response(copy.deepcopy(self.items[url.rsplit("/", 1)[-1]]))

    def put(self, url, json=None, **kw):
        item_id = url.rsplit("/", 1)[-1]
        self.puts.append(item_id)
        doc = copy.deepcopy(json)
        doc["properties"]["updated"] = f"2026-10-02T12:00:{len(self.puts):02d}Z"
        doc["links"] = list(reversed(doc["links"]))
        if self.mangle:
            self.mangle(doc)
        self.items[item_id] = doc
        return response(status=200)


def session_for(fake: FakeStac) -> MagicMock:
    session = MagicMock()
    session.get.side_effect = fake.get
    session.put.side_effect = fake.put
    return session


def make_run(fake, tmp_path, platforms, max_items=100, apply=True):
    return rsrl.RepairRun(
        session=session_for(fake),
        api_url=API,
        collection=COLL,
        max_items=max_items,
        apply=apply,
        backup_dir=tmp_path / "backups",
        fix=mod.make_fix(platforms),
        check=mod.check,
        label="item-platform-fill",
    )


# ---------- fix / check / plan ----------


def test_fix_sets_a_missing_platform_and_leaves_the_input_alone():
    item = make_item("a")
    doc, changed = mod.make_fix({"a": "sentinel-1c"})(item)
    assert changed == 1
    assert doc["properties"]["platform"] == "sentinel-1c"
    assert "platform" not in item["properties"]
    assert {k: v for k, v in doc.items() if k != "properties"} == {
        k: v for k, v in item.items() if k != "properties"
    }


def test_fix_treats_an_empty_platform_as_missing():
    doc, changed = mod.make_fix({"a": "sentinel-1a"})(make_item("a", platform=""))
    assert (changed, doc["properties"]["platform"]) == (1, "sentinel-1a")


def test_fix_skips_an_item_that_already_holds_the_planned_value():
    item = make_item("a", platform="sentinel-1c")
    assert mod.make_fix({"a": "sentinel-1c"})(item) == (item, 0)


def test_fix_refuses_an_item_with_a_different_platform():
    with pytest.raises(ValueError, match="refusing"):
        mod.make_fix({"a": "sentinel-1c"})(make_item("a", platform="sentinel-1a"))


def test_check_ignores_updated_and_link_order():
    doc = make_item("a", platform="sentinel-1c")
    after = copy.deepcopy(doc)
    after["properties"]["updated"] = "2026-10-02T12:00:00Z"
    after["links"].reverse()
    assert mod.check(after, doc) is None


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d["properties"].update(platform="sentinel-1a"),
        lambda d: d["links"][1].update(
            href="https://api.explorer.eopf.copernicus.eu/stac/raster/x"
        ),
        lambda d: d["assets"].pop("vh"),
        lambda d: d["properties"].update(extra=1),
    ],
)
def test_check_flags_any_other_difference(change):
    doc = make_item("a", platform="sentinel-1c")
    after = copy.deepcopy(doc)
    change(after)
    assert "differs" in mod.check(after, doc)


def write_plan(tmp_path, platforms, **over):
    plan = {"format": mod.PLAN_FORMAT, "collection": COLL, "platform": platforms, **over}
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    return path


def test_load_plan_returns_collection_and_platforms(tmp_path):
    assert mod.load_plan(write_plan(tmp_path, {"a": "sentinel-1c"})) == (COLL, {"a": "sentinel-1c"})


@pytest.mark.parametrize(
    ("platforms", "over", "match"),
    [
        ({"a": "sentinel-1c"}, {"format": "other/1"}, "format"),
        ({"a": "sentinel-1c"}, {"collection": ""}, "collection"),
        ({}, {}, "platform"),
        ({"a": "s1c"}, {}, "sentinel-1"),
        ({"a": None}, {}, "sentinel-1"),
    ],
)
def test_load_plan_rejects_a_bad_plan(tmp_path, platforms, over, match):
    with pytest.raises(ValueError, match=match):
        mod.load_plan(write_plan(tmp_path, platforms, **over))


# ---------- the run ----------


def test_dry_run_writes_nothing(tmp_path):
    fake = FakeStac([make_item("a")])
    run = make_run(fake, tmp_path, {"a": "sentinel-1c"}, apply=False)
    run.repair(["a"])
    assert fake.puts == []
    assert not (tmp_path / "backups").exists()
    assert "[DRY-RUN] scanned=1 clean-skipped=0 writes=0" in run.summary()


def test_apply_fills_missing_skips_filled_and_refuses_different(tmp_path):
    fake = FakeStac([make_item("a"), make_item("b", "sentinel-1c"), make_item("c", "sentinel-1a")])
    platforms = {"a": "sentinel-1c", "b": "sentinel-1c", "c": "sentinel-1c"}
    run = make_run(fake, tmp_path, platforms)
    run.repair(["a", "b", "c"])
    assert fake.puts == ["a"]
    assert fake.items["a"]["properties"]["platform"] == "sentinel-1c"
    assert fake.items["c"]["properties"]["platform"] == "sentinel-1a"  # refused, untouched
    assert (run.written, run.verified, run.skipped_clean, run.failures) == (1, 1, 1, 1)


def test_max_items_bounds_the_puts(tmp_path):
    fake = FakeStac([make_item(i) for i in "abc"])
    run = make_run(fake, tmp_path, dict.fromkeys("abc", "sentinel-1c"), max_items=2)
    run.repair(["a", "b", "c"])
    assert fake.puts == ["a", "b"]
    assert run.truncated is True
    assert "platform" not in fake.items["c"]["properties"]


def test_rerun_is_a_no_op(tmp_path):
    fake = FakeStac([make_item("a")])
    make_run(fake, tmp_path, {"a": "sentinel-1c"}).repair(["a"])
    again = make_run(fake, tmp_path, {"a": "sentinel-1c"})
    again.repair(["a"])
    assert fake.puts == ["a"]
    assert (again.skipped_clean, again.written) == (1, 0)


def test_backup_holds_the_pre_write_item(tmp_path):
    original = make_item("a")
    fake = FakeStac([original])
    make_run(fake, tmp_path, {"a": "sentinel-1c"}).repair(["a"])
    files = (tmp_path / "backups").glob(f"item-platform-fill-{COLL}-*.jsonl")
    (backup,) = [f for f in files if not f.name.endswith(".results.jsonl")]
    line = json.loads(backup.read_text().splitlines()[0])
    assert (line["collection"], line["id"], line["item"]) == (COLL, "a", original)


def test_a_server_side_change_beyond_platform_fails_the_item(tmp_path):
    def corrupt_links(doc):
        doc["links"][0]["href"] = "https://api.explorer.eopf.copernicus.eu/stac/raster/x"

    fake = FakeStac([make_item("a")], mangle=corrupt_links)
    run = make_run(fake, tmp_path, {"a": "sentinel-1c"})
    run.repair(["a"])
    assert (run.written, run.verified, run.failures) == (1, 0, 1)


def test_restore_puts_the_original_back(tmp_path):
    original = make_item("a")
    fake = FakeStac([original])
    run = make_run(fake, tmp_path, {"a": "sentinel-1c"})
    run.repair(["a"])
    undo = make_run(fake, tmp_path / "undo", {})
    undo.restore(run._backup_path, force=False)
    assert undo.failures == 0
    assert "platform" not in fake.items["a"]["properties"]
    assert fake.items["a"]["assets"] == original["assets"]


# ---------- CLI ----------


def run_main(monkeypatch, fake, argv):
    monkeypatch.setattr(rsrl, "make_session", lambda: session_for(fake))
    return mod.main(argv)


def test_main_applies_a_plan_within_the_bound(tmp_path, monkeypatch, capsys):
    fake = FakeStac([make_item("a"), make_item("b")])
    plan = write_plan(tmp_path, {"a": "sentinel-1c", "b": "sentinel-1a"})
    argv = ["--plan", str(plan), "--max-items", "1", "--apply", "--backup-dir", str(tmp_path / "b")]
    assert run_main(monkeypatch, fake, argv) == 0
    assert fake.puts == ["a"]
    assert "truncated=True" in capsys.readouterr().out


def test_main_ids_limit_the_run(tmp_path, monkeypatch):
    fake = FakeStac([make_item("a"), make_item("b")])
    plan = write_plan(tmp_path, {"a": "sentinel-1c", "b": "sentinel-1a"})
    argv = ["--plan", str(plan), "--ids", "b", "--max-items", "5", "--apply"]
    assert run_main(monkeypatch, fake, [*argv, "--backup-dir", str(tmp_path / "b")]) == 0
    assert fake.puts == ["b"]


@pytest.mark.parametrize(
    "extra",
    [
        ["--ids", "zzz"],  # not in the plan
        ["--restore", "x.jsonl"],  # restore takes no plan
        ["--max-items", "0"],
    ],
)
def test_main_rejects_bad_arguments(tmp_path, monkeypatch, extra):
    plan = write_plan(tmp_path, {"a": "sentinel-1c"})
    argv = ["--plan", str(plan), "--max-items", "5", "--backup-dir", str(tmp_path), *extra]
    with pytest.raises(SystemExit):
        run_main(monkeypatch, FakeStac([make_item("a")]), argv)
