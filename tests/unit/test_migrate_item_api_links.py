"""Unit tests for operator-tools/migrate_item_api_links.py (titiler-eopf 0.12 link migration).

The items are real catalogue items (tests/fixtures/item_api_links, from a 2026-10-09 prod sample)
in an in-memory catalogue that answers PUT like stac-fastapi-pgstac: 200 replaces an existing
item, 404 for a missing one, and nothing ever creates one.
"""

import copy
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from pystac import Asset, Item

sys.path.insert(0, str(Path(__file__).parents[2] / "operator-tools"))
import migrate_item_api_links as mod  # noqa: E402
from raster_links import s2_true_colour_links, s2_true_colour_thumbnail  # noqa: E402
from register_proxy import add_proxy_visualization  # noqa: E402
from register_v1 import add_thumbnail_asset, add_visualization_links  # noqa: E402

FIXTURES = Path(__file__).parents[1] / "fixtures" / "item_api_links"
ARCHIVE = json.loads((FIXTURES / "sentinel-2-l2a.json").read_text())
NEW = json.loads((FIXTURES / "sentinel-2-l2a-new.json").read_text())
API = "https://api.explorer.eopf.copernicus.eu/stac"
RASTER = mod.RASTER


def renamed(item: dict, new_id: str, collection: str | None = None) -> dict:
    """The same real item under another id (and, for a scratch copy, another collection)."""
    out = json.loads(json.dumps(item).replace(item["id"], new_id))
    if collection is not None:
        out["collection"] = collection
    return out


def archive_items(n: int) -> list[dict]:
    return [renamed(ARCHIVE, f"S2-{i:03d}") for i in range(n)]


class Catalogue:
    """An in-memory collection answering reads and PUTs; it records every request."""

    def __init__(self, items: list[dict]) -> None:
        self.items = {item["id"]: copy.deepcopy(item) for item in items}
        self.gets: list[str] = []
        self.puts: list[str] = []
        self.posts: list[str] = []
        self.status: dict[str, int] = {}  # forced PUT status per id
        self.raise_on: dict[str, Exception] = {}  # PUTs that raise
        self.on_get = lambda item_id: None  # runs before a GET is answered
        self.on_put = lambda item_id: None  # runs before a PUT is answered

    def fetch(self, ids: list[str] | None) -> list[dict]:
        """The page read: a snapshot, as /search returns it."""
        chosen = list(self.items) if ids is None else [i for i in ids if i in self.items]
        return [copy.deepcopy(self.items[i]) for i in chosen]

    def get(self, url: str, timeout: float) -> SimpleNamespace:
        item_id = url.rsplit("/", 1)[1]
        self.gets.append(item_id)
        self.on_get(item_id)
        item = copy.deepcopy(self.items.get(item_id))
        return SimpleNamespace(status_code=200 if item else 404, json=lambda: item, text="")

    def put(self, url: str, json: dict, timeout: float) -> SimpleNamespace:
        item_id = url.rsplit("/", 1)[1]
        self.puts.append(item_id)
        self.on_put(item_id)
        if item_id in self.raise_on:
            raise self.raise_on[item_id]
        status = self.status.get(item_id, 200 if item_id in self.items else 404)
        if status == 200:
            self.items[item_id] = copy.deepcopy(json)
        return SimpleNamespace(status_code=status, text="")

    def post(self, url: str, **kwargs) -> None:
        self.posts.append(url)
        raise AssertionError("the tool must never POST")

    delete = post


@pytest.fixture(autouse=True)
def no_live_endpoints(monkeypatch):
    """main() builds its reader and writer from these: a test that gets past a refusal it should
    hit fails here instead of opening the real STAC API."""

    def refuse(*args):
        raise AssertionError("unit tests never reach a live endpoint")

    monkeypatch.setattr(mod, "make_fetch", refuse)
    monkeypatch.setattr(mod, "make_session", refuse)


def make_run(
    catalogue,
    tmp_path,
    *,
    max_writes=1000,
    apply=True,
    concurrency=1,
    collection=None,
    any_collection=False,
):
    return mod.LinkRun(
        any_collection=any_collection,
        api_url=API,
        collection=collection or "sentinel-2-l2a",
        fetch=catalogue.fetch,
        make_session=lambda: catalogue,
        max_writes=max_writes,
        apply=apply,
        backup_dir=tmp_path / "pre-images",
        concurrency=concurrency,
    )


def migrate(run, mapping="sentinel-2-l2a", raster=RASTER):
    run.migrate(mod.PASSES[mapping], raster)


def buckets(run) -> int:
    return sum(
        getattr(run, b) for b in ("ok", "due", "gone", "failed", "already", "unrecognised", "stale")
    )


def preimages(run) -> list[dict]:
    if run.backup_path is None:
        return []
    return [json.loads(line) for line in run.backup_path.read_text().splitlines()]


# ---------- link forms: the real items, the builder, the register paths ----------


def test_legacy_form_is_the_real_archive_items_form():
    assert mod._shape(mod.managed(ARCHIVE), False) == mod._shape(
        mod.legacy_s2("sentinel-2-l2a", ARCHIVE["id"]), False
    )


def test_new_items_carry_exactly_the_builders_links_on_rstaging():
    assert mod.managed(NEW) == mod.s2_target(mod.RSTAGING, "sentinel-2-l2a-new", NEW["id"])


def test_target_is_the_012_item_route_form():
    links, thumbnail = mod.s2_target(RASTER, "sentinel-2-l2a", "X")
    hrefs = {link["rel"]: link["href"] for link in links} | {"thumbnail": thumbnail["href"]}
    for rel, href in hrefs.items():
        assert href.startswith(f"{RASTER}/collections/sentinel-2-l2a/items/X/"), rel
        assert "assets=reflectance%7Cbands%3Db04%2Cb03%2Cb02" in href, rel
        assert "variables=" not in href and "bidx" not in href and "expression" not in href, rel
    assert "/WebMercatorQuad/map.html?" in hrefs["viewer"]


def test_migrated_archive_item_has_the_target_links_and_nothing_else_changes(tmp_path):
    catalogue = Catalogue([ARCHIVE])
    migrate(make_run(catalogue, tmp_path))
    after = catalogue.items[ARCHIVE["id"]]
    assert mod.managed(after) == mod.s2_target(RASTER, "sentinel-2-l2a", ARCHIVE["id"])
    # the links keep their positions and every other field is untouched
    assert [link["rel"] for link in after["links"]] == [link["rel"] for link in ARCHIVE["links"]]
    for key in ARCHIVE:
        if key not in ("links", "assets"):
            assert after[key] == ARCHIVE[key], key
    other_links = [link for link in ARCHIVE["links"] if link["rel"] not in mod.VIZ_RELS]
    assert [link for link in after["links"] if link["rel"] not in mod.VIZ_RELS] == other_links
    assert {k: v for k, v in after["assets"].items() if k != "thumbnail"} == {
        k: v for k, v in ARCHIVE["assets"].items() if k != "thumbnail"
    }


@pytest.mark.parametrize("collection", ["sentinel-2-l2a", "sentinel-2-l2a-new"])
def test_register_and_migration_write_identical_links(collection):
    """One builder: a freshly registered S2 L2A item gets the links the migration writes."""
    item = Item(
        id="S2-X",
        geometry=None,
        bbox=None,
        datetime=datetime(2026, 10, 1, tzinfo=UTC),
        properties={},
    )
    bands = [{"name": b} for b in ("b02", "b03", "b04")]
    item.add_asset(
        "reflectance", Asset(href="https://s3.example/x.zarr", extra_fields={"bands": bands})
    )
    if collection == "sentinel-2-l2a":
        add_visualization_links(item, RASTER, collection)
        add_thumbnail_asset(item, RASTER, collection)
    else:
        add_proxy_visualization(item, RASTER, collection)
    registered = mod.managed(item.to_dict(transform_hrefs=False))
    assert registered == mod.s2_target(RASTER, collection, "S2-X")
    assert registered == (
        s2_true_colour_links(RASTER, collection, "S2-X"),
        s2_true_colour_thumbnail(RASTER, collection, "S2-X"),
    )


def test_new_collection_pass_moves_only_the_host(tmp_path):
    catalogue = Catalogue([NEW])
    migrate(make_run(catalogue, tmp_path, collection="sentinel-2-l2a-new"), "sentinel-2-l2a-new")
    before, after = mod.managed(NEW), mod.managed(catalogue.items[NEW["id"]])
    swap = (f"{mod.RSTAGING}/", f"{RASTER}/")
    assert after[0] == [{**link, "href": link["href"].replace(*swap)} for link in before[0]]
    assert after[1] == {**before[1], "href": before[1]["href"].replace(*swap)}


def test_scratch_copy_keeps_its_source_links_and_is_migrated_to_its_own_path(tmp_path):
    """A `migrate_catalog clone` copy still links to the source collection; --mapping covers it."""
    scratch = renamed(ARCHIVE, ARCHIVE["id"], collection="scratch-398")
    catalogue = Catalogue([scratch])
    migrate(make_run(catalogue, tmp_path, collection="scratch-398", any_collection=True))
    after = catalogue.items[ARCHIVE["id"]]
    assert mod.managed(after) == mod.s2_target(RASTER, "scratch-398", ARCHIVE["id"])


# ---------- 1. dry run by default ----------


def test_dry_run_writes_nothing(tmp_path):
    catalogue = Catalogue(archive_items(5))
    run = make_run(catalogue, tmp_path, apply=False)
    migrate(run)
    assert catalogue.puts == []
    assert run.writes == 5 and run.backup_path is None


def test_main_is_a_dry_run_without_apply(tmp_path, monkeypatch, capsys):
    catalogue = Catalogue(archive_items(3))
    monkeypatch.setattr(mod, "make_fetch", lambda api, collection: catalogue.fetch)
    monkeypatch.setattr(mod, "make_session", lambda: catalogue)
    argv = ["--stac-api-url", API, "--collection", "sentinel-2-l2a", "--max-writes", "10"]
    argv += ["--raster-api-url", RASTER, "--backup-dir", str(tmp_path)]
    assert mod.main(argv) == 0
    assert catalogue.puts == []
    assert "[DRY-RUN] sentinel-2-l2a: scanned=3 writes=3" in capsys.readouterr().out


def test_apply_needs_a_backup_dir():
    argv = ["--stac-api-url", API, "--collection", "sentinel-2-l2a", "--max-writes", "1"]
    with pytest.raises(SystemExit):
        mod.main([*argv, "--raster-api-url", RASTER, "--apply"])


@pytest.mark.parametrize("collection", ["sentinel-2-l1c", "landsat-c2-l2"])
def test_out_of_scope_collections_are_refused(collection, capsys):
    argv = ["--stac-api-url", API, "--collection", collection, "--max-writes", "1"]
    with pytest.raises(SystemExit):
        mod.main([*argv, "--raster-api-url", RASTER])
    assert "L1C is refused" in capsys.readouterr().err


# ---------- 2. --max-writes is a hard bound ----------


@pytest.mark.parametrize("concurrency", [1, 4])
@pytest.mark.parametrize("apply", [True, False])
def test_max_writes_stops_before_write_n_plus_1(tmp_path, concurrency, apply):
    catalogue = Catalogue(archive_items(10))
    run = make_run(catalogue, tmp_path, max_writes=3, apply=apply, concurrency=concurrency)
    migrate(run)
    assert run.writes == 3 and run.truncated
    assert len(catalogue.puts) == (3 if apply else 0)
    assert len(preimages(run)) == (3 if apply else 0)


def test_max_writes_equal_to_the_work_is_not_truncated(tmp_path):
    catalogue = Catalogue(archive_items(3))
    run = make_run(catalogue, tmp_path, max_writes=3)
    migrate(run)
    assert run.ok == 3 and not run.truncated


# ---------- 3. pre-image before the PUT ----------


def test_preimage_is_on_disk_and_fsynced_before_each_put(tmp_path, monkeypatch):
    items = archive_items(4)
    catalogue = Catalogue(items)
    run = make_run(catalogue, tmp_path)
    events = []
    fsync = mod._full_fsync
    monkeypatch.setattr(mod, "_full_fsync", lambda fd: (fsync(fd), events.append("fsync")))

    def check(item_id):  # what a crash right now would leave on disk
        on_disk = {e["id"]: e for e in preimages(run)}
        assert on_disk[item_id]["item"] == catalogue.items[item_id]
        events.append(item_id)

    catalogue.on_put = check
    migrate(run)
    assert events == [e for item in items for e in ("fsync", item["id"])]
    entry = preimages(run)[0]
    assert entry["written"] == json.loads(json.dumps(mod.managed(catalogue.items[entry["id"]])))


@pytest.mark.parametrize("failure", ["500", "timeout"])
def test_a_failed_put_still_leaves_its_preimage(tmp_path, failure):
    catalogue = Catalogue(archive_items(2))
    if failure == "500":
        catalogue.status["S2-000"] = 500
    else:
        catalogue.raise_on["S2-000"] = requests.Timeout("read timed out")
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert run.failed == 1 and run.ok == 1
    assert [e["id"] for e in preimages(run)] == ["S2-000", "S2-001"]
    assert catalogue.items["S2-000"] == archive_items(1)[0]  # unchanged


# ---------- 4. update only: 404 is skipped, never POSTed, not counted ----------


def test_items_deleted_mid_run_are_skipped_not_counted_and_never_recreated(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 1)
    catalogue = Catalogue(archive_items(6))
    deleted = {"S2-001", "S2-003", "S2-004"}

    def cleanup(item_id):  # historical-cleanup deletes the item between our read and our PUT
        if item_id in deleted:
            del catalogue.items[item_id]

    catalogue.on_put = cleanup
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.gone, run.failed, run.ok, run.aborted) == (3, 0, 3, False)
    assert catalogue.posts == []
    assert deleted.isdisjoint(catalogue.items)


# ---------- 5. abort budget ----------


@pytest.mark.parametrize("concurrency", [1, 4])
def test_other_failures_stop_the_run_at_the_abort_budget(tmp_path, monkeypatch, concurrency):
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 3)
    catalogue = Catalogue(archive_items(20))
    catalogue.status = {f"S2-{i:03d}": 503 for i in range(20)}
    run = make_run(catalogue, tmp_path, concurrency=concurrency)
    migrate(run)
    assert run.aborted
    # stops at the next item boundary; only the PUTs already in flight may still fail
    assert 3 <= len(catalogue.puts) <= 3 + concurrency - 1


# ---------- 6. idempotent ----------


def test_a_second_run_writes_nothing(tmp_path):
    catalogue = Catalogue(archive_items(5))
    migrate(make_run(catalogue, tmp_path))
    assert len(catalogue.puts) == 5
    again = make_run(catalogue, tmp_path)
    migrate(again)
    assert len(catalogue.puts) == 5
    assert (again.already, again.writes) == (5, 0)


def test_an_unrecognised_item_is_left_alone(tmp_path):
    odd = renamed(ARCHIVE, "S2-odd")
    next(link for link in odd["links"] if link["rel"] == "xyz")["href"] += "&rescale=0,2"
    catalogue = Catalogue([odd, *archive_items(1)])
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.unrecognised, run.ok) == (1, 1)
    assert catalogue.puts == ["S2-000"]


# ---------- 7. --restore with a staleness guard ----------


def test_restore_puts_the_links_back_and_keeps_later_changes(tmp_path):
    items = archive_items(3)
    catalogue = Catalogue(items)
    run = make_run(catalogue, tmp_path)
    migrate(run)
    catalogue.items["S2-001"]["properties"]["expires"] = "2027-01-01T00:00:00Z"  # written since

    back = make_run(catalogue, tmp_path)
    back.restore(run.backup_path)
    assert back.ok == 3
    for item in items:
        assert mod.managed(catalogue.items[item["id"]]) == mod.managed(item)
    assert catalogue.items["S2-001"]["properties"]["expires"] == "2027-01-01T00:00:00Z"


def test_restore_skips_and_reports_items_whose_links_changed_since(tmp_path):
    catalogue = Catalogue(archive_items(3))
    run = make_run(catalogue, tmp_path)
    migrate(run)
    reregistered = copy.deepcopy(catalogue.items["S2-002"])
    next(link for link in reregistered["links"] if link["rel"] == "xyz")["href"] += "&bidx=1"
    catalogue.items["S2-002"] = reregistered

    back = make_run(catalogue, tmp_path)
    back.restore(run.backup_path)
    assert (back.stale, back.ok) == (1, 2)
    assert catalogue.items["S2-002"] == reregistered


def test_restore_never_recreates_a_deleted_item(tmp_path):
    catalogue = Catalogue(archive_items(2))
    run = make_run(catalogue, tmp_path)
    migrate(run)
    del catalogue.items["S2-000"]
    back = make_run(catalogue, tmp_path)
    back.restore(run.backup_path)
    assert (back.gone, back.ok) == (1, 1)
    assert catalogue.puts.count("S2-000") == 1  # the migration's only
    assert "S2-000" not in catalogue.items and catalogue.posts == []


@pytest.mark.parametrize("apply", [True, False])
def test_restore_honours_max_writes_and_dry_run(tmp_path, apply):
    catalogue = Catalogue(archive_items(5))
    run = make_run(catalogue, tmp_path)
    migrate(run)
    puts = len(catalogue.puts)
    back = make_run(catalogue, tmp_path, max_writes=2, apply=apply)
    back.restore(run.backup_path)
    assert back.writes == 2 and back.truncated
    assert len(catalogue.puts) - puts == (2 if apply else 0)


def test_a_restore_can_itself_be_undone(tmp_path):
    catalogue = Catalogue(archive_items(2))
    run = make_run(catalogue, tmp_path)
    migrate(run)
    migrated = copy.deepcopy(catalogue.items)
    back = make_run(catalogue, tmp_path)
    back.restore(run.backup_path)
    assert back.backup_path != run.backup_path
    redo = make_run(catalogue, tmp_path)
    redo.restore(back.backup_path)
    assert catalogue.items == migrated


# ---------- 8. timing ----------


def test_write_rate_is_concurrency_over_the_measured_else_assumed_put_latency():
    assert mod.write_rate(8, None) == pytest.approx(8 / mod.ASSUMED_PUT_SECONDS)
    assert mod.write_rate(8, 0.5) == pytest.approx(16.0)


def test_dry_run_summary_reports_rate_and_projection(tmp_path, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(mod, "monotonic", lambda: now[0])
    catalogue = Catalogue(archive_items(10))
    run = make_run(catalogue, tmp_path, apply=False, concurrency=8)
    migrate(run)
    now[0] = 50.0
    summary = run.summary()
    assert "timing: 50.0 s, 0.2 items/s" in summary
    # 50 s measured (pages and the per-item GETs) + 10 PUTs at 8 / 0.412 s = 50.5 s
    assert "50 s measured (pages and per-item GETs) + 10 PUTs at 19.42/s (ASSUMED" in summary
    assert summary.endswith("= 51 s (0.0 h)")


# ---------- code-review fixes ----------


def main_argv(collection="sentinel-2-l2a", *extra):
    return [
        "--stac-api-url",
        API,
        "--collection",
        collection,
        "--max-writes",
        "1000",
        "--raster-api-url",
        RASTER,
        *extra,
    ]


def wire(monkeypatch, catalogue, fetch=None):
    monkeypatch.setattr(mod, "make_fetch", lambda api, collection: fetch or catalogue.fetch)
    monkeypatch.setattr(mod, "make_session", lambda: catalogue)


def test_a_write_landing_after_the_page_read_is_kept(tmp_path):
    """#1: the PUT body and the pre-image come from a fresh GET, not from the search page."""
    catalogue = Catalogue(archive_items(2))
    catalogue.on_get = lambda i: catalogue.items[i]["properties"].update({"storage:tier": "cold"})
    run = make_run(catalogue, tmp_path)
    migrate(run)
    for item_id in ("S2-000", "S2-001"):
        after = catalogue.items[item_id]
        assert after["properties"]["storage:tier"] == "cold"
        assert mod.managed(after) == mod.s2_target(RASTER, "sentinel-2-l2a", item_id)
    assert all(e["item"]["properties"]["storage:tier"] == "cold" for e in preimages(run))


def test_links_changed_after_the_page_read_are_rechecked_on_the_fresh_copy(tmp_path):
    """#1: an item re-registered since the page read is not overwritten."""
    catalogue = Catalogue(archive_items(2))
    target = mod.s2_target(RASTER, "sentinel-2-l2a", "S2-000")
    catalogue.on_get = lambda i: (
        i == "S2-000" and catalogue.items.update({i: mod.with_managed(catalogue.items[i], target)})
    )
    run = make_run(catalogue, tmp_path, max_writes=1)
    migrate(run)
    assert catalogue.puts == ["S2-001"]  # the reserved write went to the next item
    assert (run.already, run.ok, run.writes) == (1, 1, 1)


def test_restore_checks_staleness_on_the_fresh_copy(tmp_path):
    catalogue = Catalogue(archive_items(2))
    run = make_run(catalogue, tmp_path)
    migrate(run)

    def reregister(item_id):
        next(x for x in catalogue.items[item_id]["links"] if x["rel"] == "xyz")["href"] += "&a=1"

    catalogue.on_get = lambda i: i == "S2-001" and reregister(i)
    back = make_run(catalogue, tmp_path)
    back.restore(run.backup_path)
    assert (back.ok, back.stale) == (1, 1)
    assert catalogue.puts.count("S2-001") == 1


def test_a_dry_run_makes_the_same_per_item_gets(tmp_path):
    """#1 timing: the dry run's measured time includes the GET each write will need."""
    catalogue = Catalogue(archive_items(3))
    run = make_run(catalogue, tmp_path, apply=False)
    migrate(run)
    assert catalogue.gets == ["S2-000", "S2-001", "S2-002"] and catalogue.puts == []
    assert run.due == 3 and "mean GET" in run.summary()


def test_mapping_cannot_borrow_the_s2_entry_for_another_collection(capsys):
    """#2: --mapping is only for a copy whose id starts with the entry's name."""
    argv = main_argv("sentinel-2-l1c", "--mapping", "sentinel-2-l2a")
    with pytest.raises(SystemExit):
        mod.main(argv)
    assert "L1C is refused" in capsys.readouterr().err


def test_mapping_covers_a_staging_copy(monkeypatch, capsys):
    catalogue = Catalogue([renamed(ARCHIVE, "S2-S", collection="sentinel-2-l2a-staging")])
    wire(monkeypatch, catalogue)
    assert mod.main(main_argv("sentinel-2-l2a-staging", "--mapping", "sentinel-2-l2a")) == 0
    assert "writes=1" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--raster-api-url", "api.explorer.eopf.copernicus.eu/raster"),
        ("--raster-api-url", "http://api.explorer.eopf.copernicus.eu/raster"),
        ("--raster-api-url", "/raster"),
        ("--stac-api-url", "api.explorer.eopf.copernicus.eu/stac"),
    ],
)
def test_urls_must_be_absolute_https(flag, value, capsys):
    """#4: a scheme-less raster URL would write relative hrefs."""
    argv = main_argv()
    argv[argv.index(flag) + 1] = value
    with pytest.raises(SystemExit):
        mod.main(argv)
    assert "must be an absolute https URL" in capsys.readouterr().err


def test_new_collection_host_moves_both_ways(tmp_path):
    """#5: the rollback rewrite /raster -> /rstaging is possible, and the round trip is exact."""
    catalogue = Catalogue([NEW])
    for host in (RASTER, mod.RSTAGING):
        run = make_run(catalogue, tmp_path, collection="sentinel-2-l2a-new")
        migrate(run, "sentinel-2-l2a-new", host)
        assert run.ok == 1
        assert mod.managed(catalogue.items[NEW["id"]]) == mod.s2_target(
            host, "sentinel-2-l2a-new", NEW["id"]
        )
    assert catalogue.items[NEW["id"]] == NEW


def test_any_exception_from_a_put_is_a_counted_failure(tmp_path):
    """#6: e.g. stac_auth's token refresh raises RuntimeError."""
    catalogue = Catalogue(archive_items(3))
    catalogue.raise_on["S2-001"] = RuntimeError("token endpoint returned 500")
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.failed, run.ok) == (1, 2)


def test_an_interrupt_still_tallies_the_puts_in_flight(tmp_path, monkeypatch, capsys, oidc_env):
    """#7: Ctrl-C (or any stop) tallies what is in flight and prints the summary."""
    catalogue = Catalogue(archive_items(6))

    def fetch(ids):
        yield from catalogue.fetch(ids)
        raise KeyboardInterrupt

    wire(monkeypatch, catalogue, fetch)
    argv = main_argv(
        "sentinel-2-l2a", "--concurrency", "4", "--apply", "--backup-dir", str(tmp_path)
    )
    assert mod.main(argv) == 130
    out = capsys.readouterr().out
    assert len(catalogue.puts) == 6 and "ok=6" in out


def test_a_failed_page_read_stops_cleanly_with_a_summary(tmp_path, monkeypatch, capsys, oidc_env):
    """#9: pgstac's 'Could not find item using token' after cleanup deleted the page token."""
    catalogue = Catalogue(archive_items(3))

    def fetch(ids):
        yield from catalogue.fetch(ids)
        raise RuntimeError("Could not find item using token")

    wire(monkeypatch, catalogue, fetch)
    argv = main_argv("sentinel-2-l2a", "--apply", "--backup-dir", str(tmp_path))
    assert mod.main(argv) == 1
    out = capsys.readouterr().out
    assert "ok=3" in out and "aborted=True" in out


def test_restore_counts_malformed_lines_as_failures(tmp_path):
    """#14: the rollback path must not crash on a bad line."""
    catalogue = Catalogue(archive_items(2))
    run = make_run(catalogue, tmp_path)
    migrate(run)
    good = run.backup_path.read_text().splitlines()
    bad = [
        "[1, 2]",
        '"a string"',
        '{"format": "item-api-links-preimage/1", "collection": "sentinel-2-l2a"}',
        '{"format": "item-api-links-preimage/1", "collection": "other", "id": "x"}',
        '{"format": "item-api-links-preimage/1", "collec',  # torn
    ]
    mixed = tmp_path / "mixed.jsonl"
    mixed.write_text("\n".join([good[0], *bad, good[1]]) + "\n")
    back = make_run(catalogue, tmp_path)
    back.restore(mixed)
    assert (back.failed, back.ok, back.aborted) == (5, 2, False)


def test_put_404_on_an_item_that_is_still_there_is_a_failure(tmp_path):
    """#8: pgstac's delete-then-insert also 404s on two concurrent PUTs of one item."""
    catalogue = Catalogue(archive_items(2))
    catalogue.status["S2-000"] = 404  # the PUT 404s, the item is still there
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.failed, run.gone, run.ok) == (1, 0, 1)


def test_scattered_failures_do_not_abort(tmp_path, monkeypatch):
    """#10: the budget is consecutive failures, reset by any item that gets through."""
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 2)
    catalogue = Catalogue(archive_items(10))
    catalogue.status = {f"S2-{i:03d}": 500 for i in range(1, 10, 2)}
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.aborted, run.failed, run.ok, len(catalogue.puts)) == (False, 5, 5, 10)


def test_an_item_without_the_rgb_bands_is_unrecognised(tmp_path):
    """#11: links selecting b04,b03,b02 would 4xx on an item that has no such bands."""
    item = renamed(ARCHIVE, "S2-nobands")
    item["assets"]["reflectance"]["bands"] = [{"name": "b08"}]
    catalogue = Catalogue([item])
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.unrecognised, catalogue.puts) == (1, [])


def test_links_to_another_collection_match_the_source_form_only_when_mapping_a_copy(tmp_path):
    """#13: the collection in the path is ignored only for a staging/scratch copy."""
    item = mod.with_managed(
        renamed(ARCHIVE, "S2-X"), mod.legacy_s2("sentinel-2-l2a-staging", "S2-X")
    )
    strict = make_run(Catalogue([item]), tmp_path)
    migrate(strict)
    assert (strict.unrecognised, strict.ok) == (1, 0)
    copy_run = make_run(Catalogue([item]), tmp_path, any_collection=True)
    migrate(copy_run)
    assert copy_run.ok == 1


def test_a_scratch_copy_of_new_is_rewritten_to_its_own_collection_path(tmp_path):
    """Review 2 #3: with --mapping, "already" still compares the collection exactly. A clone of
    -new keeps /rstaging links to sentinel-2-l2a-new; the drill must point them at the clone."""
    scratch = renamed(NEW, NEW["id"], collection="sentinel-2-l2a-new-scratch")
    catalogue = Catalogue([scratch])
    run = make_run(
        catalogue, tmp_path, collection="sentinel-2-l2a-new-scratch", any_collection=True
    )
    migrate(run, "sentinel-2-l2a-new", mod.RSTAGING)
    assert (run.ok, run.already) == (1, 0)
    assert mod.managed(catalogue.items[NEW["id"]]) == mod.s2_target(
        mod.RSTAGING, "sentinel-2-l2a-new-scratch", NEW["id"]
    )


def malformed(item_id: str) -> dict:
    return {"id": item_id, "links": "not a list", "assets": copy.deepcopy(ARCHIVE["assets"])}


def test_page_level_outcomes_reset_the_failure_streak(tmp_path, monkeypatch):
    """Review 2 #1: malformed items scattered among already-migrated ones never abort a run."""
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 3)

    def done(item_id):
        target = mod.s2_target(RASTER, "sentinel-2-l2a", item_id)
        return mod.with_managed(renamed(ARCHIVE, item_id), target)

    page = [done(f"done-{i}") if i % 2 else malformed(f"bad-{i}") for i in range(10)]
    catalogue = Catalogue([*page, *archive_items(2)])
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.failed, run.already, run.ok, run.aborted) == (5, 5, 2, False)


def test_failures_on_the_page_alone_abort_without_a_write(tmp_path, monkeypatch):
    """Review 2 #1: the abort check runs for page-level failures, not only before a dispatch."""
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 3)
    catalogue = Catalogue([malformed(f"bad-{i}") for i in range(10)])
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.aborted, run.failed, run.scanned) == (True, 3, 3)


def test_with_managed_removes_the_thumbnail_when_the_form_has_none():
    """Review 2 #2: a None thumbnail is popped, not written as null."""
    out = mod.with_managed(ARCHIVE, (mod.managed(ARCHIVE)[0], None))
    assert "thumbnail" not in out["assets"]


def test_legacy_form_is_what_register_v1_still_writes_for_l1c():
    """Review 2 #6: one 0.11 S2 query, shared by register_v1's fallback and the migration."""
    item = Item(
        id="S2-L1C",
        geometry=None,
        bbox=None,
        datetime=datetime(2026, 10, 1, tzinfo=UTC),
        properties={},
    )
    add_visualization_links(item, RASTER, "sentinel-2-l1c")
    add_thumbnail_asset(item, RASTER, "sentinel-2-l1c")
    assert mod.managed(item.to_dict(transform_hrefs=False)) == mod.legacy_s2(
        "sentinel-2-l1c", "S2-L1C"
    )


def test_every_scanned_item_lands_in_exactly_one_bucket(tmp_path):
    """#15: the summary adds up, including the item that trips --max-writes."""
    target = renamed(ARCHIVE, "T")
    target = mod.with_managed(target, mod.s2_target(RASTER, "sentinel-2-l2a", "T"))
    odd = renamed(ARCHIVE, "U")
    next(x for x in odd["links"] if x["rel"] == "xyz")["href"] += "&a=1"
    catalogue = Catalogue([target, odd, *archive_items(6)])
    catalogue.on_put = lambda i: i == "S2-000" and catalogue.items.pop(i)  # gone
    catalogue.status["S2-001"] = 500
    run = make_run(catalogue, tmp_path, max_writes=3)
    migrate(run)
    assert run.truncated and run.scanned == buckets(run) == 5


def test_requested_ids_found_is_reported_only_for_a_complete_read(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=mod.logger.name)
    catalogue = Catalogue(archive_items(3))
    wire(monkeypatch, catalogue)
    ids = tmp_path / "ids.txt"
    ids.write_text("S2-000\nS2-001\nS2-002\nS2-999\n")
    argv = main_argv("sentinel-2-l2a", "--ids-file", str(ids))
    argv[argv.index("--max-writes") + 1] = "1"
    mod.main(argv)
    assert "requested ids found" not in caplog.text
    argv[argv.index("--max-writes") + 1] = "10"
    mod.main(argv)
    assert "3 of 4 requested ids found" in caplog.text


@pytest.mark.parametrize("value", ["inf", "nan", "0", "-1"])
def test_put_seconds_must_be_a_positive_number(value, capsys):
    with pytest.raises(SystemExit):
        mod.main([*main_argv(), "--put-seconds", value])
    assert "--put-seconds must be a positive" in capsys.readouterr().err


def test_apply_needs_the_oidc_writer_env(tmp_path, capsys):
    with pytest.raises(SystemExit):
        mod.main([*main_argv(), "--apply", "--backup-dir", str(tmp_path)])
    assert "OIDC" in capsys.readouterr().err


def test_unrecognised_says_which_field_differs(tmp_path, caplog):
    odd = renamed(ARCHIVE, "S2-odd")
    next(x for x in odd["links"] if x["rel"] == "xyz")["href"] += "&rescale=0,2"
    migrate(make_run(Catalogue([odd]), tmp_path))
    assert "UNRECOGNISED S2-odd: xyz href is" in caplog.text


def test_failing_writes_abort_even_with_skips_between_them(tmp_path, monkeypatch):
    """Review wave 1 #1: an "already" item on the page does not reset the write-path streak."""
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 3)
    items = []
    for i in range(10):
        items.append(renamed(ARCHIVE, f"S2-{i:03d}"))
        done = renamed(ARCHIVE, f"done-{i:03d}")
        items.append(mod.with_managed(done, mod.s2_target(RASTER, "sentinel-2-l2a", done["id"])))
    catalogue = Catalogue(items)
    catalogue.status = {f"S2-{i:03d}": 503 for i in range(10)}
    run = make_run(catalogue, tmp_path)
    migrate(run)
    assert (run.aborted, run.failed, len(catalogue.puts)) == (True, 3, 3)


def test_restore_aborts_on_a_streak_of_malformed_lines(tmp_path, monkeypatch):
    """Review wave 1 #3: the bad-line abort on the restore path."""
    monkeypatch.setattr(mod, "MAX_CONSECUTIVE_FAILURES", 3)
    foreign = '{"format": "item-api-links-preimage/1", "collection": "other", "id": "x"}\n'
    preimage = tmp_path / "foreign.jsonl"
    preimage.write_text(foreign * 5)
    run = make_run(Catalogue([]), tmp_path)
    run.restore(preimage)
    assert (run.aborted, run.failed) == (True, 3)


# ---------- S1 RTC and OLCI (link-forms_2026-11-04) ----------

CUBE = json.loads((FIXTURES / "sentinel-1-grd-rtc-staging.json").read_text())  # s1-rtc-31TCH
CUBE_HREFLESS = json.loads((FIXTURES / "sentinel-1-grd-rtc-staging-hrefless.json").read_text())
ACQ = json.loads((FIXTURES / "sentinel-1-grd-rtc-acquisitions-staging.json").read_text())
ACQ_ORPHAN = json.loads(
    (FIXTURES / "sentinel-1-grd-rtc-acquisitions-staging-orphan.json").read_text()
)
OLCI = json.loads((FIXTURES / "sentinel-3-olci-l1-efr-staging.json").read_text())
RST = mod.RSTAGING
CUBES, ACQS, OLCIS = (
    "sentinel-1-grd-rtc-staging",
    "sentinel-1-grd-rtc-acquisitions-staging",
    "sentinel-3-olci-l1-efr-staging",
)

# URLs that returned 200 with the right image on /rstaging 0.12.4 on 4 Nov 2026
# (link-forms_2026-11-04/workflow-result.json probes), minus their `&cb=` cache-buster. The xyz
# probes fetched one tile: the template's {z}/{x}/{y} are filled with its numbers to compare.
_C = f"{RST}/collections/{CUBES}/items/s1-rtc-31TCH"
_CQ = (
    "assets=gamma0-rtc-backscatter-asc%7Cvariables%3Dvv%2Cvh%7Csel%3Dtime%3D2026-06-26T17%3A46%3A10"
    "&expression=b1%3Bb2%3B%28b1%29%2F%28b2%29&rescale=0.0%2C0.4&rescale=0.0%2C0.1&rescale=1.0%2C15.0"
)
_A = f"{RST}/collections/{ACQS}/items/s1-rtc-28RBS-20211109t071038"
_AQ = (
    "assets=gamma0-rtc-backscatter-desc%7Cbands%3Dvv%2Cvh%7Csel%3Dtime%3D2021-11-09T07%3A10%3A38"
    "&expression=b1%3Bb2%3Bb1%2Fb2&rescale=0%2C0.4&rescale=0%2C0.1&rescale=1%2C15"
)
_O = f"{RST}/collections/{OLCIS}/items/{OLCI['id']}/assets/radianceData"
_OQ = (
    "variables=oa08_radiance&variables=oa06_radiance&variables=oa04_radiance&rescale=10%2C300"
    "&rescale=22%2C345&rescale=36%2C390&color_formula=gamma%20rgb%201.3%2C%20sigmoidal%20rgb%206"
    "%200.1%2C%20saturation%201.2&bidx=1"
)
_O_GREENLAND = (
    "S3A_OL_1_EFR____20260721T142227_20260721T142527_20260721T162757_0179_142_039_1620_PS1_O_NR_004"
)
PROVEN = {
    (CUBES, "viewer"): f"{_C}/WebMercatorQuad/map.html?{_CQ}&tilesize=256",
    (CUBES, "tilejson"): f"{_C}/WebMercatorQuad/tilejson.json?{_CQ}&tilesize=256",
    (CUBES, "xyz"): f"{_C}/tiles/WebMercatorQuad/10/515/376.png?{_CQ}&tilesize=256",
    (CUBES, "thumbnail"): f"{_C}/preview?format=webp&max_size=512&{_CQ}",
    (ACQS, "viewer"): f"{_A}/WebMercatorQuad/map.html?{_AQ}",
    (ACQS, "tilejson"): f"{_A}/WebMercatorQuad/tilejson.json?{_AQ}",
    (ACQS, "xyz"): f"{_A}/tiles/WebMercatorQuad/10/461/426.png?{_AQ}",
    (ACQS, "thumbnail"): f"{_A}/preview?format=webp&max_size=512&{_AQ}",
    (OLCIS, "viewer"): f"{RST}/collections/{OLCIS}/items/{_O_GREENLAND}/assets/radianceData"
    f"/WebMercatorQuad/map.html?{_OQ}&minzoom=0",
    (OLCIS, "xyz"): f"{_O}/tiles/WebMercatorQuad/7/51/50.png?{_OQ}",
    (OLCIS, "tilejson"): f"{_O}/WebMercatorQuad/tilejson.json?{_OQ}",
    (OLCIS, "thumbnail"): f"{_O}/preview?format=webp&max_size=512&{_OQ}",
}
XYZ_TILE = {CUBES: "10/515/376", ACQS: "10/461/426", OLCIS: "7/51/50"}


def built(collection: str, rel: str) -> str:
    """The href the tool writes, from the real fixture item when the proof used that item."""
    links, thumbnail = {
        CUBES: lambda: mod.PASSES[CUBES].target(RST, CUBES, CUBE),
        ACQS: lambda: mod.PASSES[ACQS].target(RST, ACQS, ACQ),
        OLCIS: lambda: mod.PASSES[OLCIS].target(RST, OLCIS, OLCI),
    }[collection]()
    if (collection, rel) == (OLCIS, "viewer"):  # proven on the Greenland item, with minzoom=0
        links = mod.olci_links(RST, OLCIS, _O_GREENLAND)
    href = (
        thumbnail["href"]
        if rel == "thumbnail"
        else next(x["href"] for x in links if x["rel"] == rel)
    )
    return href.replace("{z}/{x}/{y}", XYZ_TILE[collection])


@pytest.mark.parametrize(("collection", "rel"), sorted(PROVEN))
def test_each_builder_reproduces_a_proven_url_byte_for_byte(collection, rel):
    assert built(collection, rel) == PROVEN[(collection, rel)]


@pytest.mark.parametrize(("collection", "fixture"), [(CUBES, CUBE), (ACQS, ACQ), (OLCIS, OLCI)])
def test_s1_and_olci_items_in_todays_form_are_migrated_and_nothing_else_changes(
    tmp_path, collection, fixture
):
    catalogue = Catalogue([fixture])
    run = make_run(catalogue, tmp_path, collection=collection)
    migrate(run, collection)
    after = catalogue.items[fixture["id"]]
    assert run.ok == 1
    assert mod.managed(after) == mod.PASSES[collection].target(RASTER, collection, fixture)
    assert [x["rel"] for x in after["links"]] == [x["rel"] for x in fixture["links"]]
    # The write body re-materializes a cube's null datetime, which GET omits and PUT requires
    # (runner._transaction_body); nothing else differs.
    assert after["properties"] == {"datetime": None, **fixture["properties"]}
    for key in fixture:
        if key not in ("links", "assets", "properties"):
            assert after[key] == fixture[key], key
    assert {k: v for k, v in after["assets"].items() if k != "thumbnail"} == {
        k: v for k, v in fixture["assets"].items() if k != "thumbnail"
    }
    # titles and link types unchanged; the thumbnail is the proven WebP, typed as such
    links, thumbnail = mod.managed(after)
    old_links, old_thumbnail = mod.managed(fixture)
    assert [(x["rel"], x["type"], x["title"]) for x in links] == [
        (x["rel"], x["type"], x["title"]) for x in old_links
    ]
    assert thumbnail == {
        "href": PROVEN[(collection, "thumbnail")].replace(RST, RASTER),
        "type": "image/webp",
        "title": old_thumbnail["title"],
        "roles": ["thumbnail"],
    }
    again = make_run(catalogue, tmp_path, collection=collection)
    migrate(again, collection)
    assert (again.already, again.writes) == (1, 0)


def test_a_cube_with_an_hrefless_asset_is_reported_not_rewritten(tmp_path, caplog):
    """The 6 single-orbit cubes list the other orbit's assets with no href; 0.12 fails on them."""
    catalogue = Catalogue([CUBE_HREFLESS])
    run = make_run(catalogue, tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert (run.unrecognised, catalogue.puts) == (1, [])
    assert "UNRECOGNISED s1-rtc-34WET: asset without href" in caplog.text


def test_a_cube_without_renders_rgb_is_still_migrated(tmp_path):
    """The target never reads renders: the stretch and expression are fixed in the builder."""
    cube = copy.deepcopy(CUBE)
    del cube["properties"]["renders"]["rgb"]
    catalogue = Catalogue([cube])
    run = make_run(catalogue, tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert run.ok == 1


def test_a_migrated_cube_without_renders_rgb_is_already(tmp_path):
    cube = mod.with_managed(CUBE, mod.PASSES[CUBES].target(RASTER, CUBES, CUBE))
    del cube["properties"]["renders"]["rgb"]
    run = make_run(Catalogue([cube]), tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert (run.already, run.unrecognised) == (1, 0)


def test_a_migrated_item_is_already_whatever_its_precondition_says(tmp_path):
    """The target comparison comes before the precondition: a migrated item is never re-judged."""
    item = mod.with_managed(ARCHIVE, mod.s2_target(RASTER, "sentinel-2-l2a", ARCHIVE["id"]))
    item["assets"]["reflectance"]["bands"] = []
    run = make_run(Catalogue([item]), tmp_path)
    migrate(run)
    assert (run.already, run.unrecognised) == (1, 0)


def test_a_cube_without_its_orbit_asset_is_not_rewritten(tmp_path):
    """recognisers-2: the target names gamma0-rtc-backscatter-{orbit}; it must exist with an href."""
    cube = copy.deepcopy(CUBE)
    del cube["assets"]["gamma0-rtc-backscatter-asc"]
    catalogue = Catalogue([cube])
    run = make_run(catalogue, tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert (run.unrecognised, catalogue.puts) == (1, [])


def test_a_cube_without_an_orbit_is_reported_not_failed(tmp_path):
    cube = copy.deepcopy(CUBE)
    del cube["properties"]["sat:orbit_state"]
    catalogue = Catalogue([cube])
    run = make_run(catalogue, tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert (run.unrecognised, run.failed, catalogue.puts) == (1, 0, [])


def test_an_acquisition_with_an_hrefless_asset_is_reported_not_rewritten(tmp_path, caplog):
    acq = copy.deepcopy(ACQ)
    acq["assets"]["gamma0-rtc-backscatter-asc"] = {"type": "application/vnd.zarr; version=3"}
    catalogue = Catalogue([acq])
    run = make_run(catalogue, tmp_path, collection=ACQS)
    migrate(run, ACQS)
    assert (run.unrecognised, catalogue.puts) == (1, [])
    assert f"UNRECOGNISED {acq['id']}: asset without href" in caplog.text


def test_an_acquisition_without_a_datetime_is_reported_not_failed(tmp_path):
    acq = copy.deepcopy(ACQ)
    del acq["properties"]["datetime"]
    catalogue = Catalogue([acq])
    run = make_run(catalogue, tmp_path, collection=ACQS)
    migrate(run, ACQS)
    assert (run.unrecognised, run.failed, catalogue.puts) == (1, 0, [])


def test_a_cube_without_a_pinned_slice_gets_no_links(tmp_path):
    """A cube link without sel renders the wrong slice with a 200: never write one."""
    cube = copy.deepcopy(CUBE)
    xyz = next(x for x in cube["links"] if x["rel"] == "xyz")
    xyz["href"] = xyz["href"].split("&sel=")[0]
    catalogue = Catalogue([cube])
    run = make_run(catalogue, tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert (run.unrecognised, run.failed, catalogue.puts) == (1, 0, [])
    with pytest.raises(ValueError, match="needs an orbit and a slice"):
        mod.s1_rtc_cube_links(RASTER, CUBES, "s1-rtc-31TCH", "ascending", "")


def test_the_orbit_comes_from_sat_orbit_state(tmp_path):
    """Links that disagree with the item's orbit are not today's form for that item."""
    cube = copy.deepcopy(CUBE)
    cube["properties"]["sat:orbit_state"] = "descending"  # its links say ascending
    catalogue = Catalogue([cube])
    run = make_run(catalogue, tmp_path, collection=CUBES)
    migrate(run, CUBES)
    assert (run.unrecognised, catalogue.puts) == (1, [])


def test_an_acquisition_links_to_its_own_item_at_its_datetime_to_the_second(tmp_path):
    acq = copy.deepcopy(ACQ)
    acq["properties"]["datetime"] = "2021-11-09T07:10:38.734Z"
    catalogue = Catalogue([acq])
    run = make_run(catalogue, tmp_path, collection=ACQS)
    migrate(run, ACQS)
    assert run.ok == 1
    xyz = next(x["href"] for x in catalogue.items[acq["id"]]["links"] if x["rel"] == "xyz")
    assert xyz.startswith(f"{RASTER}/collections/{ACQS}/items/{acq['id']}/tiles/")
    assert "sel%3Dtime%3D2021-11-09T07%3A10%3A38&" in xyz


def test_an_orphan_acquisition_is_reported_not_rewritten(tmp_path, caplog):
    """The 16 form-B items: tilejson and xyz only, one rescale, a slice no longer in the cube."""
    catalogue = Catalogue([ACQ_ORPHAN])
    run = make_run(catalogue, tmp_path, collection=ACQS)
    migrate(run, ACQS)
    assert (run.unrecognised, catalogue.puts) == (1, [])
    assert f"UNRECOGNISED {ACQ_ORPHAN['id']}: links are ['tilejson', 'xyz']" in caplog.text


def test_an_acquisition_without_vv_vh_bands_is_not_rewritten(tmp_path):
    acq = copy.deepcopy(ACQ)
    acq["assets"]["gamma0-rtc-backscatter-desc"]["bands"] = [{"name": "vv"}]
    catalogue = Catalogue([acq])
    run = make_run(catalogue, tmp_path, collection=ACQS)
    migrate(run, ACQS)
    assert (run.unrecognised, catalogue.puts) == (1, [])


def test_olci_whose_radiance_is_not_the_measurements_group_is_not_rewritten(tmp_path):
    """recognisers-2: bare variable names resolve only in /measurements."""
    item = copy.deepcopy(OLCI)
    href = item["assets"]["radianceData"]["href"]
    item["assets"]["radianceData"]["href"] = href.rsplit("/measurements", 1)[0]  # the store root
    catalogue = Catalogue([item])
    run = make_run(catalogue, tmp_path, collection=OLCIS)
    migrate(run, OLCIS)
    assert (run.unrecognised, catalogue.puts) == (1, [])


def test_olci_without_a_radiance_href_is_not_rewritten(tmp_path):
    item = copy.deepcopy(OLCI)
    del item["assets"]["radianceData"]["href"]
    catalogue = Catalogue([item])
    run = make_run(catalogue, tmp_path, collection=OLCIS)
    migrate(run, OLCIS)
    assert (run.unrecognised, catalogue.puts) == (1, [])
