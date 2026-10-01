"""Tests for operator-tools/fill_coordinate_holes.py, on real damaged chunks read on 1 Oct 2026."""

import base64
import hashlib
import io
import json
from pathlib import Path

import fill_coordinate_holes as fch
import numpy as np
import pytest
import zarr
from botocore.exceptions import ClientError
from zarr.codecs import ZstdCodec

FIXTURES = Path(__file__).parent.parent / "fixtures" / "fill_coordinate_holes"
BUCKET = "bucket"
BASE = "tests-output/sentinel-1-grd-rtc-staging"
UPB = f"{BASE}/s1-rtc-32UPB.zarr"  # ascending/r10m/platform = ['s1a', 's1c', '']
TEL = f"{BASE}/s1-rtc-31TEL.zarr"  # descending/r10m/relative_orbit = [110, 37, 110, 0]
PLAT = {"path": "ascending/r10m/platform", "length": 3, "fill": "", "values": {"2": "s1c"}}
REL = {"path": "descending/r10m/relative_orbit", "length": 4, "fill": 0, "values": {"3": 110}}


def _etag(body: bytes) -> str:
    return f'"{hashlib.md5(body, usedforsecurity=False).hexdigest()}"'


class FakeS3:
    """In-memory stand-in for the boto3 S3 client calls the tool makes."""

    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = dict(objects)
        self.puts: list[str] = []
        self.fail_put_on: str | None = None
        self.mangle_puts = False

    def get_object(self, Bucket: str, Key: str):  # noqa: N803 (boto3 kwarg names)
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(body), "ETag": _etag(body)}

    def head_object(self, Bucket: str, Key: str):  # noqa: N803
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ETag": _etag(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str):  # noqa: N803
        if Key == self.fail_put_on:
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        self.objects[(Bucket, Key)] = Body[:-1] if self.mangle_puts else Body
        self.puts.append(Key)
        return {"ETag": _etag(Body)}


def _fixture(cube: str, prefix: str) -> dict[tuple[str, str], bytes]:
    root = FIXTURES / cube
    return {
        (BUCKET, f"{prefix}/{p.relative_to(root).as_posix()}"): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }


def _fleet() -> dict[tuple[str, str], bytes]:
    return {**_fixture("s1-rtc-32UPB", UPB), **_fixture("s1-rtc-31TEL", TEL)}


def _entries() -> list[dict]:
    return [
        {"store": f"s3://{BUCKET}/{UPB}", "arrays": [PLAT]},
        {"store": f"s3://{BUCKET}/{TEL}", "arrays": [REL]},
    ]


def _decode(fake: FakeS3, prefix: str, path: str, tmp_path: Path) -> list:
    local = tmp_path / "decode" / prefix / path
    (local / "c").mkdir(parents=True, exist_ok=True)
    (local / "zarr.json").write_bytes(fake.objects[(BUCKET, f"{prefix}/{path}/zarr.json")])
    (local / "c" / "0").write_bytes(fake.objects[(BUCKET, f"{prefix}/{path}/c/0")])
    return zarr.open_array(str(local), mode="r", zarr_format=3)[:].tolist()


def _run(fake, tmp_path, entries=None, *, apply=True, max_writes=100):
    run = fch.FillRun(
        fake, max_writes=max_writes, apply=apply, backup_dir=tmp_path / "backups" if apply else None
    )
    run.repair(_entries() if entries is None else entries)
    return run


def _key(prefix: str, path: str) -> str:
    return f"{prefix}/{path}/c/0"


# --- the fill itself -------------------------------------------------------------------------


def test_fixtures_hold_the_damage_seen_on_s3(tmp_path) -> None:
    fake = FakeS3(_fleet())
    assert _decode(fake, UPB, PLAT["path"], tmp_path) == ["s1a", "s1c", ""]
    assert _decode(fake, TEL, REL["path"], tmp_path) == [110, 37, 110, 0]


def test_fills_real_damaged_chunks_and_nothing_else(tmp_path) -> None:
    before = _fleet()
    fake = FakeS3(before)
    run = _run(fake, tmp_path)

    assert run.failures == 0 and run.writes == 2 and run.verified == 2
    assert _decode(fake, UPB, PLAT["path"], tmp_path) == ["s1a", "s1c", "s1c"]
    assert _decode(fake, TEL, REL["path"], tmp_path) == [110, 37, 110, 110]  # the wrong 37 stays
    assert sorted(fake.puts) == sorted([_key(TEL, REL["path"]), _key(UPB, PLAT["path"])])
    unchanged = {k: v for k, v in before.items() if k[1] not in fake.puts}
    assert all(fake.objects[k] == v for k, v in unchanged.items())  # zarr.json untouched


def test_rerun_is_a_noop(tmp_path) -> None:
    fake = FakeS3(_fleet())
    _run(fake, tmp_path)
    again = _run(fake, tmp_path)

    assert len(fake.puts) == 2
    assert again.writes == 0 and again.skipped_clean == 2 and again.failures == 0


def test_dry_run_writes_nothing_and_counts_the_writes(tmp_path) -> None:
    fake = FakeS3(_fleet())
    run = _run(fake, tmp_path, apply=False)

    assert fake.puts == [] and run.writes == 2 and run.verified == 0
    assert not (tmp_path / "backups").exists()


@pytest.mark.parametrize(
    ("spec", "match"),
    [
        ({**PLAT, "values": {"1": "s1a"}}, "not the fill value"),  # index 1 holds 's1c'
        ({**PLAT, "length": 4}, "length 3"),
        ({**PLAT, "fill": "x"}, "fill value"),
        ({**PLAT, "values": {}}, "empty or out of range"),
        ({**PLAT, "values": {"7": "s1c"}}, "empty or out of range"),
        ({**PLAT, "path": "ascending/r10m/nope"}, "no zarr.json"),
    ],
    ids=["not-a-hole", "length", "fill", "empty", "out-of-range", "missing-array"],
)
def test_plan_array_refuses(spec, match, tmp_path) -> None:
    with pytest.raises(fch.PlanError, match=match):
        fch.plan_array(FakeS3(_fleet()), BUCKET, UPB, spec, tmp_path)


def test_a_refused_array_refuses_its_whole_store(tmp_path) -> None:
    fake = FakeS3(_fleet())
    bad = {"store": f"s3://{BUCKET}/{UPB}", "arrays": [{**PLAT, "values": {"1": "s1a"}}]}
    run = _run(fake, tmp_path, [bad])

    assert fake.puts == [] and run.failures == 1


def test_refuses_a_plan_that_leaves_a_hole(tmp_path) -> None:
    prefix = f"{BASE}/s1-rtc-TWO.zarr"
    local = tmp_path / "two"
    arr = zarr.create_array(
        str(local / "descending/r10m/relative_orbit"),
        shape=(3,),
        chunks=(512,),
        dtype="int32",
        fill_value=0,
        compressors=ZstdCodec(level=0),
        zarr_format=3,
    )
    arr[:] = np.array([66, 0, 0], dtype="int32")
    objects = {
        (BUCKET, f"{prefix}/{p.relative_to(local).as_posix()}"): p.read_bytes()
        for p in local.rglob("*")
        if p.is_file()
    }
    spec = {"path": "descending/r10m/relative_orbit", "length": 3, "fill": 0, "values": {"1": 66}}
    with pytest.raises(fch.PlanError, match="leaves fill values at \\[2\\]"):
        fch.plan_array(FakeS3(objects), BUCKET, prefix, spec, tmp_path / "work")


@pytest.mark.parametrize(
    ("sabotage", "match"),
    [
        ("wrong-value", "does not decode to the planned values"),
        ("touch-metadata", "rewrote zarr.json"),
        ("extra-file", "unexpected files"),
    ],
)
def test_post_encode_checks_catch_a_misbehaving_writer(sabotage, match, tmp_path, monkeypatch):
    """The checks after re-encoding guard against zarr itself; prove each one fires."""
    real_open = zarr.open_array

    class Sabotaged:
        def __init__(self, array, path: Path) -> None:
            self.array, self.path, self.fill_value = array, path, array.fill_value

        def __getitem__(self, key):
            return self.array[key]

        def __setitem__(self, key, value) -> None:
            value = value.copy()
            if sabotage == "wrong-value":
                value[0] = value[1]
            self.array[key] = value
            if sabotage == "touch-metadata":
                (self.path / "zarr.json").write_text((self.path / "zarr.json").read_text() + " ")
            if sabotage == "extra-file":
                (self.path / "c" / "1").write_bytes(b"x")

    def open_array(path, mode="r", **kwargs):
        array = real_open(path, mode=mode, **kwargs)
        return Sabotaged(array, Path(path)) if mode == "r+" else array

    monkeypatch.setattr(fch.zarr, "open_array", open_array)
    with pytest.raises(fch.PlanError, match=match):
        fch.plan_array(FakeS3(_fleet()), BUCKET, UPB, PLAT, tmp_path)


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ({"shape": [3, 2]}, "not a 1-D array"),
        ({"shape": [600]}, "not a 1-D array"),
        ({"codecs": [{"name": "sharding_indexed"}]}, "sharded"),
        ({"chunk_key_encoding": {"name": "v2"}}, "not handled"),
        ({"node_type": "group"}, "not a zarr v3 array"),
    ],
    ids=["2-D", "multi-chunk", "sharded", "v2-keys", "group"],
)
def test_chunk_key_refuses_unhandled_layouts(change, match) -> None:
    meta = json.loads((FIXTURES / "s1-rtc-32UPB" / PLAT["path"] / "zarr.json").read_text())
    assert fch.chunk_key(UPB, PLAT["path"], meta) == _key(UPB, PLAT["path"])
    with pytest.raises(fch.PlanError, match=match):
        fch.chunk_key(UPB, PLAT["path"], {**meta, **change})


def test_load_plan_rejects_a_wrong_format_and_duplicates(tmp_path) -> None:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"format": "other", "stores": []}))
    with pytest.raises(ValueError, match="format"):
        fch.load_plan(path)
    store = {"store": f"s3://{BUCKET}/{UPB}", "arrays": [PLAT]}
    path.write_text(json.dumps({"format": fch.PLAN_FORMAT, "stores": [store, store]}))
    with pytest.raises(ValueError, match="twice"):
        fch.load_plan(path)


def test_a_store_planned_twice_with_one_array_is_refused(tmp_path) -> None:
    fake = FakeS3(_fleet())
    run = _run(fake, tmp_path, [{"store": f"s3://{BUCKET}/{UPB}", "arrays": [PLAT, PLAT]}])
    assert fake.puts == [] and run.failures == 1


# --- the write bound -------------------------------------------------------------------------


def _two_array_store() -> tuple[FakeS3, list[dict]]:
    both = f"{BASE}/s1-rtc-BOTH.zarr"
    objects = {**_fleet(), **_fixture("s1-rtc-32UPB", both), **_fixture("s1-rtc-31TEL", both)}
    entries = [
        {"store": f"s3://{BUCKET}/{both}", "arrays": [PLAT, REL]},
        {"store": f"s3://{BUCKET}/{UPB}", "arrays": [PLAT]},
    ]
    return FakeS3(objects), entries


def test_max_writes_stops_between_stores(tmp_path) -> None:
    fake, entries = _two_array_store()
    run = _run(fake, tmp_path, entries, max_writes=2)

    assert run.truncated and run.writes == 2 and len(fake.puts) == 2
    assert all("BOTH" in k for k in fake.puts)


def test_a_store_larger_than_the_budget_is_not_started(tmp_path) -> None:
    fake, entries = _two_array_store()
    run = _run(fake, tmp_path, entries, max_writes=1)

    assert run.truncated and fake.puts == [] and run.writes == 0


def test_dry_run_stops_where_the_apply_would(tmp_path) -> None:
    fake, entries = _two_array_store()
    run = _run(fake, tmp_path, entries, apply=False, max_writes=2)

    assert run.truncated and run.writes == 2 and fake.puts == []


def test_constructor_guards() -> None:
    with pytest.raises(ValueError):
        fch.FillRun(FakeS3({}), max_writes=0, apply=False, backup_dir=None)
    with pytest.raises(ValueError):
        fch.FillRun(FakeS3({}), max_writes=3, apply=True, backup_dir=None)


# --- backups, staleness, failures ------------------------------------------------------------


def test_every_chunk_is_backed_up_before_the_first_put(tmp_path) -> None:
    before = _fleet()
    fake, entries = _two_array_store()
    fake.fail_put_on = f"{BASE}/s1-rtc-BOTH.zarr/{PLAT['path']}/c/0"
    run = _run(fake, tmp_path, entries)

    assert run.failures == 1 and fake.puts == []
    lines = [json.loads(line) for line in run.backup_path.read_text().splitlines()]
    assert len(lines) == 2  # both of the store's chunks, before its first PUT
    for line in lines:
        body = base64.b64decode(line["body_b64"])
        assert line["sha256"] == hashlib.sha256(body).hexdigest()
        assert body in before.values()
    assert run.scanned == 1  # an uncertain PUT stops the run


def test_a_chunk_changed_since_it_was_read_skips_the_store(tmp_path, monkeypatch) -> None:
    fake = FakeS3(_fleet())
    monkeypatch.setattr(fake, "head_object", lambda Bucket, Key: {"ETag": '"changed"'})  # noqa: N803
    run = _run(fake, tmp_path)

    assert fake.puts == [] and run.failures == 2 and run.backup_path is None


def test_a_read_back_mismatch_stops_the_run(tmp_path) -> None:
    fake = FakeS3(_fleet())
    fake.mangle_puts = True
    run = _run(fake, tmp_path)

    assert run.failures == 1 and run.verified == 0 and len(fake.puts) == 1


def test_a_tls_error_while_reading_back_stops_the_run_cleanly(tmp_path, monkeypatch) -> None:
    """urllib3 raises a TLS failure during a body read unwrapped (not a BotoCoreError)."""
    from urllib3.exceptions import ProtocolError

    fake = FakeS3(_fleet())
    real_get = fake.get_object

    class BrokenBody:
        def read(self) -> bytes:
            raise ProtocolError("TLS connection broken")

    def get_object(Bucket, Key):  # noqa: N803
        resp = real_get(Bucket=Bucket, Key=Key)
        return {**resp, "Body": BrokenBody()} if Key in fake.puts else resp

    monkeypatch.setattr(fake, "get_object", get_object)
    run = _run(fake, tmp_path)

    assert run.failures == 1 and run.verified == 0 and run.scanned == 1


def test_refuses_to_write_a_key_that_is_not_a_chunk(tmp_path) -> None:
    run = fch.FillRun(FakeS3({}), max_writes=3, apply=True, backup_dir=tmp_path)
    with pytest.raises(fch.PlanError, match="not a chunk"):
        run._put_verified(BUCKET, f"{UPB}/{PLAT['path']}/zarr.json", b"{}")


def test_three_consecutive_failures_abort(tmp_path) -> None:
    entries = [
        {"store": f"s3://{BUCKET}/{BASE}/missing-{i}.zarr", "arrays": [PLAT]} for i in range(5)
    ]
    run = _run(FakeS3({}), tmp_path, entries)
    assert run.failures == 3 and run.scanned == 3


def test_ten_failures_abort_even_when_not_consecutive(tmp_path) -> None:
    good = {"store": f"s3://{BUCKET}/{UPB}", "arrays": [PLAT]}
    entries = []
    for i in range(12):
        entries += [{"store": f"s3://{BUCKET}/{BASE}/missing-{i}.zarr", "arrays": [PLAT]}, good]
    run = _run(FakeS3(_fleet()), tmp_path, entries, apply=False)
    assert run.failures == 10


# --- restore ---------------------------------------------------------------------------------


def _repaired(tmp_path):
    before = _fleet()
    fake = FakeS3(before)
    run = _run(fake, tmp_path)
    return fake, before, run


def _restore(fake, tmp_path, backup, *, apply=True, force=False, max_writes=10):
    run = fch.FillRun(
        fake, max_writes=max_writes, apply=apply, backup_dir=tmp_path / "undo" if apply else None
    )
    run.restore(backup, force=force)
    return run


def test_restore_puts_the_original_chunks_back(tmp_path) -> None:
    fake, before, repair = _repaired(tmp_path)
    run = _restore(fake, tmp_path, repair.backup_path)

    assert run.failures == 0 and run.verified == 2
    assert fake.objects == before
    assert len(run.backup_path.read_text().splitlines()) == 2  # a restore is reversible too


def test_restore_refuses_a_chunk_changed_since_the_repair(tmp_path) -> None:
    fake, before, repair = _repaired(tmp_path)
    key = (BUCKET, _key(UPB, PLAT["path"]))
    fake.objects[key] = b"someone else"
    run = _restore(fake, tmp_path, repair.backup_path)
    assert run.failures == 1 and fake.objects[key] == b"someone else"

    forced = _restore(fake, tmp_path, repair.backup_path, force=True)
    assert forced.failures == 0 and fake.objects[key] == before[key]


def test_restore_bound_never_stops_mid_store(tmp_path) -> None:
    fake, entries = _two_array_store()
    repair = _run(fake, tmp_path, entries, max_writes=2)  # only the two-array store
    run = _restore(fake, tmp_path, repair.backup_path, max_writes=1)

    assert run.truncated and run.writes == 0


def test_restore_refuses_a_line_that_fails_its_hash(tmp_path) -> None:
    fake, _, repair = _repaired(tmp_path)
    lines = repair.backup_path.read_text().splitlines()
    tampered = json.loads(lines[0]) | {"sha256": "0" * 64}
    repair.backup_path.write_text("\n".join([json.dumps(tampered), *lines[1:]]) + "\n")
    run = _restore(fake, tmp_path, repair.backup_path)

    assert run.failures == 1 and run.verified == 1


def test_restore_dry_run_writes_nothing(tmp_path) -> None:
    fake, _, repair = _repaired(tmp_path)
    puts = list(fake.puts)
    run = _restore(fake, tmp_path, repair.backup_path, apply=False)
    assert fake.puts == puts and run.writes == 2


# --- CLI -------------------------------------------------------------------------------------


def _plan_file(tmp_path: Path, entries=None) -> Path:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"format": fch.PLAN_FORMAT, "stores": entries or _entries()}))
    return path


class _Client(FakeS3):
    class meta:  # noqa: N801 — mimics boto3's client.meta.endpoint_url
        endpoint_url = "https://s3.example"


def test_cli_dry_run_and_only(tmp_path, monkeypatch, capsys) -> None:
    fake = _Client(_fleet())
    monkeypatch.setattr(fch, "make_s3_client", lambda endpoint: fake)
    plan = str(_plan_file(tmp_path))
    rc = fch.main(["--plan", plan, "--max-writes", "5", "--only", f"s3://{BUCKET}/{UPB}"])

    assert rc == 0 and fake.puts == []
    out = capsys.readouterr().out
    assert "[DRY-RUN]" in out and "writes=1" in out


@pytest.mark.parametrize(
    "extra",
    [
        ["--apply", "--s3-endpoint", "https://s3.example"],  # no --backup-dir
        ["--apply", "--backup-dir", "b"],  # no --s3-endpoint
        ["--max-writes", "0"],
        ["--restore", "x.jsonl"],  # with --plan
        ["--only", "s3://bucket/not-in-plan.zarr"],
    ],
    ids=["apply-no-backup", "apply-no-endpoint", "zero-budget", "restore-and-plan", "only-unknown"],
)
def test_cli_refuses(extra, tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(fch, "make_s3_client", lambda endpoint: _Client({}))
    argv = ["--plan", str(_plan_file(tmp_path)), "--max-writes", "5", *extra]
    with pytest.raises(SystemExit):
        fch.main(argv)


def test_cli_needs_a_plan(monkeypatch) -> None:
    monkeypatch.setattr(fch, "make_s3_client", lambda endpoint: _Client({}))
    with pytest.raises(SystemExit):
        fch.main(["--max-writes", "5"])
