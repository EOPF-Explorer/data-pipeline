"""Unit tests for the S1 RTC migration fleet driver (`run_fleet`) — Task 2.

The driver's I/O boundaries (`list_cube_items` STAC enumeration, `redrive_store` per-store re-derive)
are patched, so these tests cover only the orchestration: href→s3 resolution, continue-on-error,
bucketing (derived / already-current / skipped / failed), dry-run pass-through, and the
``--item``/``--skip-tiles`` filters. The real S3/STAC I/O is verified in-cluster (plan Task 2 Verify).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import migrate_s1_rtc_datamodel as migrate  # noqa: E402

_GATEWAY = "https://s3.explorer.eopf.copernicus.eu"
_PREFIX = "esa-zarr-sentinel-explorer-fra/tests-output/sentinel-1-grd-rtc-staging"


def _href(tile: str) -> str:
    return f"{_GATEWAY}/{_PREFIX}/s1-rtc-{tile}.zarr"


_ITEMS = [
    ("s1-rtc-30TUM", _href("30TUM")),
    ("s1-rtc-30TWQ", _href("30TWQ")),
    ("s1-rtc-31TDG", _href("31TDG")),
]


def _patch_list(monkeypatch, items=_ITEMS) -> None:
    monkeypatch.setattr(migrate, "list_cube_items", lambda _api, _coll: list(items))


def test_continues_past_a_failing_store_and_records_it(monkeypatch) -> None:
    _patch_list(monkeypatch)

    def fake_redrive(store, *, dry_run=False):
        if "30TWQ" in store:
            raise RuntimeError("unreadable store")
        return migrate.RedriveReport(store=store, orbits=["ascending"])

    monkeypatch.setattr(migrate, "redrive_store", fake_redrive)

    fleet = migrate.run_fleet("http://stac", "coll")

    assert fleet.failed == [("s1-rtc-30TWQ", "unreadable store")]
    assert set(fleet.derived) == {"s1-rtc-30TUM", "s1-rtc-31TDG"}  # the run did not abort


def test_buckets_derived_already_current_and_skipped(monkeypatch) -> None:
    _patch_list(monkeypatch)

    def fake_redrive(store, *, dry_run=False):
        if "30TUM" in store:
            return migrate.RedriveReport(store=store, already_current=True)
        if "30TWQ" in store:
            return migrate.RedriveReport(store=store, skipped_no_border_mask=["descending"])
        return migrate.RedriveReport(store=store, orbits=["ascending"])

    monkeypatch.setattr(migrate, "redrive_store", fake_redrive)

    fleet = migrate.run_fleet("http://stac", "coll")

    assert fleet.already_current == ["s1-rtc-30TUM"]
    assert fleet.skipped_no_border_mask == ["s1-rtc-30TWQ"]
    assert fleet.derived == ["s1-rtc-31TDG"]
    assert fleet.failed == []


def test_resolves_each_href_to_s3_before_redriving(monkeypatch) -> None:
    _patch_list(monkeypatch)
    seen: list[str] = []

    def fake_redrive(store, *, dry_run=False):
        seen.append(store)
        return migrate.RedriveReport(store=store, orbits=["ascending"])

    monkeypatch.setattr(migrate, "redrive_store", fake_redrive)

    migrate.run_fleet("http://stac", "coll")

    assert seen[0] == f"s3://{_PREFIX}/s1-rtc-30TUM.zarr"
    assert all(s.startswith("s3://") for s in seen)


def test_dry_run_threads_through_to_every_store(monkeypatch) -> None:
    _patch_list(monkeypatch)
    seen_dry: list[bool] = []

    def fake_redrive(store, *, dry_run=False):
        seen_dry.append(dry_run)
        return migrate.RedriveReport(store=store, orbits=["ascending"])

    monkeypatch.setattr(migrate, "redrive_store", fake_redrive)

    migrate.run_fleet("http://stac", "coll", dry_run=True)

    assert seen_dry == [True, True, True]


def test_only_item_and_skip_tiles_filter_the_fleet(monkeypatch) -> None:
    _patch_list(monkeypatch)
    seen: list[str] = []

    def fake_redrive(store, *, dry_run=False):
        seen.append(store)
        return migrate.RedriveReport(store=store, orbits=["ascending"])

    monkeypatch.setattr(migrate, "redrive_store", fake_redrive)

    only = migrate.run_fleet("http://stac", "coll", only_item="s1-rtc-31TDG")
    assert only.derived == ["s1-rtc-31TDG"] and len(seen) == 1

    seen.clear()
    skipped = migrate.run_fleet("http://stac", "coll", skip_tiles=("30TWQ",))
    assert {"s1-rtc-30TUM", "s1-rtc-31TDG"} == set(skipped.derived)
    assert all("30TWQ" not in s for s in seen)  # the skipped tile was never opened


def test_main_list_enumerates_without_opening_stores(monkeypatch, capsys) -> None:
    """`--list` prints item id + resolved s3 store from STAC only — never calls redrive_store."""
    _patch_list(monkeypatch)

    def _boom(*_a, **_k):  # redrive must not be touched in --list mode
        raise AssertionError("redrive_store must not be called for --list")

    monkeypatch.setattr(migrate, "redrive_store", _boom)

    rc = migrate.main(["--stac-api-url", "http://stac", "--cube-collection", "coll", "--list"])

    assert rc == 0
    out = capsys.readouterr().out
    assert f"s1-rtc-30TUM\ts3://{_PREFIX}/s1-rtc-30TUM.zarr" in out
    assert out.count("\n") == len(_ITEMS)


def _ok_report(store, *, dry_run=False):
    return migrate.RedriveReport(store=store, orbits=["ascending"])


def _must_not_call(*_a, **_k):
    raise AssertionError("this should not have been called")


def test_backup_prefix_backs_up_each_store_before_redrive(monkeypatch) -> None:
    """Task 3: with a backup prefix, each store is copied (to <prefix>/<item>.zarr) before its redrive."""
    _patch_list(monkeypatch)
    calls: list[tuple] = []
    monkeypatch.setattr(
        migrate.s1_store_meta, "backup_store", lambda s, b: calls.append(("backup", s, b)) or 1
    )

    def fake_redrive(store, *, dry_run=False):
        calls.append(("redrive", store))
        return migrate.RedriveReport(store=store, orbits=["ascending"])

    monkeypatch.setattr(migrate, "redrive_store", fake_redrive)

    migrate.run_fleet("http://stac", "coll", backup_prefix="s3://b/backup")

    assert calls[0] == (
        "backup",
        f"s3://{_PREFIX}/s1-rtc-30TUM.zarr",
        "s3://b/backup/s1-rtc-30TUM.zarr",
    )
    assert calls[1] == (
        "redrive",
        f"s3://{_PREFIX}/s1-rtc-30TUM.zarr",
    )  # backup immediately precedes
    assert [c[0] for c in calls] == ["backup", "redrive"] * len(_ITEMS)


def test_dry_run_with_backup_prefix_does_not_back_up(monkeypatch) -> None:
    """Dry-run writes nothing → no backup even if a prefix is given."""
    _patch_list(monkeypatch)
    monkeypatch.setattr(migrate.s1_store_meta, "backup_store", _must_not_call)
    monkeypatch.setattr(migrate, "redrive_store", _ok_report)

    migrate.run_fleet(
        "http://stac", "coll", dry_run=True, backup_prefix="s3://b/backup"
    )  # no raise


def test_main_refuses_real_run_when_versioning_off_and_no_backup(monkeypatch) -> None:
    """C2 pre-flight: a real run with versioning OFF and no --backup-prefix is refused (rc 2), no writes."""
    _patch_list(monkeypatch)
    monkeypatch.setattr(migrate.s1_store_meta, "s3_versioning_enabled", lambda *a, **k: False)
    monkeypatch.setattr(migrate, "redrive_store", _must_not_call)

    rc = migrate.main(["--stac-api-url", "x", "--cube-collection", "y", "--bucket", "b"])

    assert rc == 2


def test_main_proceeds_when_versioning_on(monkeypatch) -> None:
    """C2 pre-flight: versioning ON → rely on it, proceed with the real run."""
    _patch_list(monkeypatch)
    monkeypatch.setattr(migrate.s1_store_meta, "s3_versioning_enabled", lambda *a, **k: True)
    seen: list[str] = []
    monkeypatch.setattr(
        migrate, "redrive_store", lambda s, *, dry_run=False: seen.append(s) or _ok_report(s)
    )

    rc = migrate.main(["--stac-api-url", "x", "--cube-collection", "y", "--bucket", "b"])

    assert rc == 0
    assert len(seen) == len(_ITEMS)


def test_rollback_restores_each_store_from_backup(monkeypatch) -> None:
    """Task 3 AC: --rollback restores each store from <backup-prefix>/<item>.zarr (never re-derives)."""
    _patch_list(monkeypatch)
    restored: list[tuple] = []
    monkeypatch.setattr(
        migrate.s1_store_meta, "restore_store", lambda b, s: restored.append((b, s)) or 1
    )
    monkeypatch.setattr(migrate, "redrive_store", _must_not_call)

    rc = migrate.main(
        [
            "--stac-api-url",
            "x",
            "--cube-collection",
            "y",
            "--rollback",
            "--backup-prefix",
            "s3://b/bk",
        ]
    )

    assert rc == 0
    assert restored[0] == ("s3://b/bk/s1-rtc-30TUM.zarr", f"s3://{_PREFIX}/s1-rtc-30TUM.zarr")
    assert len(restored) == len(_ITEMS)


def test_unresolvable_store_href_is_recorded_as_failed(monkeypatch) -> None:
    """An href that doesn't resolve to s3:// (https_to_s3 → None) is failed, not crashed on Path(None)."""
    _patch_list(monkeypatch)
    monkeypatch.setattr(
        migrate,
        "https_to_s3",
        lambda href: None if "30TWQ" in href else f"s3://{_PREFIX}/{href.rsplit('/', 1)[-1]}",
    )
    monkeypatch.setattr(migrate, "redrive_store", _ok_report)

    fleet = migrate.run_fleet("http://stac", "coll")

    assert [item for item, _ in fleet.failed] == ["s1-rtc-30TWQ"]
    assert {"s1-rtc-30TUM", "s1-rtc-31TDG"} == set(fleet.derived)


def test_main_allow_no_backup_bypasses_the_gate(monkeypatch) -> None:
    """--allow-no-backup proceeds with a real run despite no versioning + no backup (explicit C2 opt-out);
    the versioning check is not even called."""
    _patch_list(monkeypatch)
    monkeypatch.setattr(migrate.s1_store_meta, "s3_versioning_enabled", _must_not_call)
    seen: list[str] = []
    monkeypatch.setattr(
        migrate, "redrive_store", lambda s, *, dry_run=False: seen.append(s) or _ok_report(s)
    )

    rc = migrate.main(
        ["--stac-api-url", "x", "--cube-collection", "y", "--bucket", "b", "--allow-no-backup"]
    )

    assert rc == 0
    assert len(seen) == len(_ITEMS)  # ran the fleet, gate bypassed
