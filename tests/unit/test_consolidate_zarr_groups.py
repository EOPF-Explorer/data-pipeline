"""Tests for operator-tools/consolidate_zarr_groups.py (#446)."""

import gzip
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import consolidate_zarr_groups as czg
import pytest
import zarr
from botocore.exceptions import ClientError, ReadTimeoutError
from urllib3.exceptions import SSLError

FIXTURES = Path(__file__).parent.parent / "fixtures" / "consolidate_zarr_groups" / "s1-rtc-32UPB"
BUCKET = "bucket"
S1 = "tests-output/sentinel-1-grd-rtc-staging/s1-rtc-32UPB.zarr"
S1_GROUPS = ["ascending", "descending", ""]


def _etag(body: bytes) -> str:
    return f'"{hashlib.md5(body, usedforsecurity=False).hexdigest()}"'


def _missing(op: str) -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchKey"}}, op)


class FakeS3:
    """In-memory stand-in for the boto3 S3 client calls the tool makes."""

    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = dict(objects)
        self.puts: list[str] = []
        self.gets: list[str] = []
        self.listed: list[str] = []
        self.fail_put_on: str | None = None
        self.garble_readback_of: str | None = None  # a read after a PUT to this key differs
        self.tls_error_on_readback_of: str | None = None  # a read after a PUT to this key raises
        # A PUT to this key is applied, and then the client times out.
        self.timeout_after_put_on: str | None = None
        self.meta = SimpleNamespace(endpoint_url="https://s3.fake")

    def get_object(self, Bucket: str, Key: str):  # noqa: N803 (boto3 kwarg names)
        self.gets.append(Key)
        if (Bucket, Key) not in self.objects:
            raise _missing("GetObject")
        body = self.objects[(Bucket, Key)]
        if Key == self.garble_readback_of and Key in self.puts:
            body += b" "
        if Key == self.tls_error_on_readback_of and Key in self.puts:
            raise SSLError("decryption failed or bad record mac")
        return {"Body": io.BytesIO(body), "ETag": _etag(body)}

    def head_object(self, Bucket: str, Key: str):  # noqa: N803
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ETag": _etag(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str):  # noqa: N803
        if Key == self.fail_put_on:
            raise ClientError({"Error": {"Code": "InternalError"}}, "PutObject")
        self.objects[(Bucket, Key)] = Body
        self.puts.append(Key)
        if Key == self.timeout_after_put_on:
            raise ReadTimeoutError(endpoint_url=self.meta.endpoint_url)
        return {"ETag": _etag(Body)}

    def get_paginator(self, operation: str):
        assert operation == "list_objects_v2"
        return _FakePaginator(self)


class _FakePaginator:
    def __init__(self, fake: FakeS3) -> None:
        self.fake = fake

    def paginate(self, Bucket: str, Prefix: str, Delimiter: str):  # noqa: N803
        assert Delimiter == "/"
        self.fake.listed.append(Prefix)
        children = sorted(
            {
                key[len(Prefix) :].split("/", 1)[0]
                for bucket, key in self.fake.objects
                if bucket == Bucket and key.startswith(Prefix) and "/" in key[len(Prefix) :]
            }
        )
        common = [{"Prefix": f"{Prefix}{child}/"} for child in children]
        # Two pages, to prove pagination is consumed rather than assumed single-page.
        yield {"CommonPrefixes": common[:1]}
        yield {"CommonPrefixes": common[1:]}


def _healthy(name: str) -> dict:
    return json.loads(gzip.decompress((FIXTURES / f"{name}.json.gz").read_bytes()))


def _strip(meta: dict) -> dict:
    return {k: v for k, v in meta.items() if k != "consolidated_metadata"}


_key = czg.zarr_json_key


def stripped_cube(prefix: str = S1) -> dict[tuple[str, str], bytes]:
    """Every node of the healthy 32UPB cube, with the blocks stripped the way the no-op ingest did
    (compact json.dumps), plus one chunk per array that the tool must never read."""
    root = _healthy("root")
    objects = {(BUCKET, _key(prefix, "")): json.dumps(_strip(root)).encode()}
    for path, meta in root["consolidated_metadata"]["metadata"].items():
        objects[(BUCKET, _key(prefix, path))] = json.dumps(_strip(meta)).encode()
        if meta["node_type"] == "array":
            objects[(BUCKET, f"{prefix}/{path}/c/0/0/0")] = b"\x00chunk"
    return objects


def _run(fake, tmp_path, stores, groups=S1_GROUPS, *, apply=True, max_writes=100):
    run = czg.ConsolidateRun(
        fake, max_writes=max_writes, apply=apply, backup_dir=tmp_path / "backups" if apply else None
    )
    run.repair(stores, groups)
    return run


def _doc(fake: FakeS3, prefix: str, group: str) -> dict:
    return json.loads(fake.objects[(BUCKET, _key(prefix, group))])


def _same(a: dict, b: dict) -> bool:
    return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


# --- the repair itself -----------------------------------------------------------------------


def test_fixture_is_a_consistent_healthy_cube() -> None:
    root = _healthy("root")["consolidated_metadata"]["metadata"]
    for orbit in ("ascending", "descending"):
        assert _same(_strip(root[orbit]), _strip(_healthy(orbit)))


def test_repair_reproduces_the_healthy_cube_exactly(tmp_path) -> None:
    fake = FakeS3(stripped_cube())
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert run.failures == 0 and run.writes == 3 and run.verified == 3
    for group, name in (("", "root"), ("ascending", "ascending"), ("descending", "descending")):
        assert _same(_doc(fake, S1, group), _healthy(name)), f"{name} differs from the healthy cube"


def test_only_the_target_documents_are_written_root_last(tmp_path) -> None:
    before = stripped_cube()
    fake = FakeS3(before)
    _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert sorted(fake.puts[:2]) == [_key(S1, "ascending"), _key(S1, "descending")]
    assert fake.puts[2:] == [_key(S1, "")]
    untouched = {k: v for k, v in before.items() if k[1] not in fake.puts}
    assert all(fake.objects[k] == v for k, v in untouched.items())


def test_never_lists_or_reads_inside_an_array(tmp_path) -> None:
    fake = FakeS3(stripped_cube())
    _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"], apply=False)

    arrays = {
        f"{S1}/{p}/"
        for p, m in _healthy("root")["consolidated_metadata"]["metadata"].items()
        if m["node_type"] == "array"
    }
    assert not arrays & set(fake.listed)
    assert not [k for k in fake.gets if "/c/" in k]


def test_olci_layout_consolidates_r0_and_root_only(tmp_path) -> None:
    local = tmp_path / "olci.zarr"
    root = zarr.open_group(str(local), mode="w", zarr_format=3)
    r0 = root.create_group("measurements").create_group("r0", attributes={"proj:code": "EPSG:4326"})
    r0.create_array("oa08_radiance", shape=(4, 4), dtype="float32", dimension_names=("y", "x"))
    root.create_group("conditions").create_array("sza", shape=(2,), dtype="float32")
    prefix = "tests-output/sentinel-3-olci-l1-efr-staging/S3B_X.zarr"
    objects = {
        (BUCKET, f"{prefix}/{p.relative_to(local).as_posix()}"): p.read_bytes()
        for p in local.rglob("zarr.json")
    }
    fake = FakeS3(objects)

    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{prefix}"], ["measurements/r0", ""])

    assert run.failures == 0
    assert fake.puts == [_key(prefix, "measurements/r0"), _key(prefix, "")]
    assert (
        "oa08_radiance"
        in _doc(fake, prefix, "measurements/r0")["consolidated_metadata"]["metadata"]
    )
    assert "consolidated_metadata" not in _doc(fake, prefix, "measurements")


def test_rerun_is_a_noop(tmp_path) -> None:
    fake = FakeS3(stripped_cube())
    _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])
    again = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert len(fake.puts) == 3
    assert again.writes == 0 and again.skipped_clean == 1 and again.failures == 0


def test_dry_run_writes_nothing_and_counts_the_writes(tmp_path) -> None:
    fake = FakeS3(stripped_cube())
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"], apply=False)

    assert fake.puts == [] and run.writes == 3 and run.verified == 0
    assert not (tmp_path / "backups").exists()


def test_missing_group_refuses_the_store(tmp_path) -> None:
    fake = FakeS3(stripped_cube())
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"], ["ascending", "nope", ""])

    assert fake.puts == [] and run.failures == 1


def test_check_only_block_added_rejects_any_other_change() -> None:
    old = json.dumps({"attributes": {"a": 1}, "zarr_format": 3, "node_type": "group"}).encode()
    block = {"kind": "inline", "must_understand": False, "metadata": {"x": {}}}
    ok = {**json.loads(old), "consolidated_metadata": block}
    czg.check_only_block_added(old, json.dumps(ok).encode(), "ok")

    changed = {**ok, "attributes": {"a": 2}}
    with pytest.raises(czg.PlanError, match="attributes"):
        czg.check_only_block_added(old, json.dumps(changed).encode(), "changed")
    empty = {**ok, "consolidated_metadata": {**block, "metadata": {}}}
    with pytest.raises(czg.PlanError, match="empty block"):
        czg.check_only_block_added(old, json.dumps(empty).encode(), "empty")


def _consolidate_then(monkeypatch, edit) -> None:
    """Run zarr's consolidation, then let ``edit(store_dir, group)`` tamper with the local copy."""
    real = czg.zarr.consolidate_metadata

    def consolidate(store, path=None, zarr_format=3):
        real(store, path=path, zarr_format=zarr_format)
        edit(Path(store), path or "")

    monkeypatch.setattr(czg.zarr, "consolidate_metadata", consolidate)


def _edit_zarr_json(path: Path, change) -> None:
    meta = json.loads(path.read_text())
    change(meta)
    path.write_text(json.dumps(meta))


def _refused(tmp_path) -> czg.ConsolidateRun:
    fake = FakeS3(stripped_cube())
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])
    assert fake.puts == [] and run.failures == 1
    return run


def test_a_consolidation_that_changes_attributes_refuses_the_store(tmp_path, monkeypatch) -> None:
    def add_attribute(store: Path, group: str) -> None:
        _edit_zarr_json(store / group / "zarr.json", lambda m: m["attributes"].update(x=1))

    _consolidate_then(monkeypatch, add_attribute)
    _refused(tmp_path)


def test_a_consolidation_that_rewrites_another_node_refuses_the_store(
    tmp_path, monkeypatch
) -> None:
    root = _healthy("root")["consolidated_metadata"]["metadata"]
    array = next(p for p, m in root.items() if m["node_type"] == "array")

    def touch_an_array(store: Path, group: str) -> None:
        if group == "":  # after the last target
            path = store / array / "zarr.json"
            path.write_bytes(path.read_bytes() + b" ")

    _consolidate_then(monkeypatch, touch_an_array)
    _refused(tmp_path)


def test_a_block_that_misses_a_node_refuses_the_store(tmp_path, monkeypatch) -> None:
    def drop_a_member(store: Path, group: str) -> None:
        _edit_zarr_json(store / group / "zarr.json", lambda m: m[czg.BLOCK]["metadata"].popitem())

    _consolidate_then(monkeypatch, drop_a_member)
    _refused(tmp_path)


def test_a_dot_dot_segment_refuses_the_store_before_reading_under_it(tmp_path) -> None:
    objects = stripped_cube()
    objects[(BUCKET, f"{S1}/../escape/zarr.json")] = objects[(BUCKET, _key(S1, ""))]
    fake = FakeS3(objects)
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert fake.puts == [] and run.failures == 1
    assert not [k for k in fake.gets if "/../" in k]


def test_an_empty_block_counts_as_unconsolidated(tmp_path) -> None:
    objects = stripped_cube()
    root_key = (BUCKET, _key(S1, ""))
    root = {**json.loads(objects[root_key]), "consolidated_metadata": {}}
    objects[root_key] = json.dumps(root).encode()
    fake = FakeS3(objects)
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert run.failures == 0 and run.writes == 3
    assert _same(_doc(fake, S1, ""), _healthy("root"))


def test_an_empty_path_segment_refuses_the_store_instead_of_looping(tmp_path) -> None:
    objects = stripped_cube()
    objects[(BUCKET, f"{S1}//ascending/zarr.json")] = objects[(BUCKET, _key(S1, "ascending"))]
    fake = FakeS3(objects)
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert fake.puts == [] and run.failures == 1


# --- the write bound -------------------------------------------------------------------------


def _two_stores() -> tuple[FakeS3, list[str], str]:
    other = S1.replace("32UPB", "31TCH")
    fake = FakeS3({**stripped_cube(S1), **stripped_cube(other)})
    return fake, [f"s3://{BUCKET}/{S1}", f"s3://{BUCKET}/{other}"], other


def test_max_writes_stops_between_stores(tmp_path) -> None:
    fake, stores, other = _two_stores()
    run = _run(fake, tmp_path, stores, max_writes=5)

    assert run.truncated and run.writes == 3 and len(fake.puts) == 3
    assert all(k.startswith(f"{S1}/") for k in fake.puts)
    assert "consolidated_metadata" not in _doc(fake, other, "")


def test_dry_run_stops_where_the_apply_would(tmp_path) -> None:
    fake, stores, _ = _two_stores()
    run = _run(fake, tmp_path, stores, apply=False, max_writes=5)

    assert run.truncated and run.writes == 3 and fake.puts == []


def test_a_store_larger_than_the_budget_is_not_started(tmp_path) -> None:
    fake = FakeS3(stripped_cube())
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"], max_writes=2)

    assert run.truncated and fake.puts == [] and run.writes == 0


def test_max_writes_must_be_positive() -> None:
    with pytest.raises(ValueError):
        czg.ConsolidateRun(FakeS3({}), max_writes=0, apply=False, backup_dir=None)


def test_a_real_run_needs_a_backup_dir() -> None:
    with pytest.raises(ValueError):
        czg.ConsolidateRun(FakeS3({}), max_writes=3, apply=True, backup_dir=None)


# --- backups, staleness, failures ------------------------------------------------------------


def test_every_original_is_backed_up_before_the_first_put(tmp_path) -> None:
    before = stripped_cube()
    fake = FakeS3(before)
    fake.fail_put_on = _key(S1, "ascending")  # the first PUT fails
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert run.failures == 1 and fake.puts == []
    entries = [json.loads(line) for line in run.backup_path.read_text().splitlines()]
    assert {e["key"] for e in entries} == {_key(S1, g) for g in S1_GROUPS}
    for e in entries:
        assert e["body"].encode() == before[(BUCKET, e["key"])]
        assert e["sha256"] == hashlib.sha256(e["body"].encode()).hexdigest()


def test_an_object_changed_since_it_was_read_skips_the_store(tmp_path, monkeypatch) -> None:
    fake = FakeS3(stripped_cube())
    real_head = fake.head_object

    def head(Bucket, Key):  # noqa: N803
        resp = real_head(Bucket=Bucket, Key=Key)
        return {"ETag": '"changed"'} if Key == _key(S1, "") else resp

    monkeypatch.setattr(fake, "head_object", head)
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])

    assert fake.puts == [] and run.failures == 1 and run.backup_path is None


def test_an_uncertain_put_stops_the_run(tmp_path) -> None:
    fake, stores, other = _two_stores()
    fake.timeout_after_put_on = _key(S1, "descending")  # applied server-side, then a timeout
    run = _run(fake, tmp_path, stores)

    assert run.failures == 1 and fake.puts == [_key(S1, "ascending"), _key(S1, "descending")]
    assert "consolidated_metadata" not in _doc(fake, other, "ascending")  # the run stopped
    entries = [json.loads(line) for line in run.backup_path.read_text().splitlines()]
    assert len(entries) == 3 and all("sha256_new" in e for e in entries)


def test_a_read_back_that_differs_stops_the_run(tmp_path) -> None:
    fake, stores, other = _two_stores()
    fake.garble_readback_of = _key(S1, "ascending")
    run = _run(fake, tmp_path, stores)

    assert run.failures == 1 and run.verified == 0 and fake.puts == [_key(S1, "ascending")]


def test_a_tls_error_on_read_back_stops_the_run_cleanly(tmp_path) -> None:
    fake, stores, _ = _two_stores()
    fake.tls_error_on_readback_of = _key(S1, "descending")
    run = _run(fake, tmp_path, stores)

    assert run.failures == 1 and fake.puts == [_key(S1, "ascending"), _key(S1, "descending")]


def test_stale_stores_in_an_apply_abort_after_three(tmp_path, monkeypatch) -> None:
    prefixes = [S1.replace("32UPB", f"3{i}AAA") for i in range(5)]
    fake = FakeS3({k: v for p in prefixes for k, v in stripped_cube(p).items()})
    monkeypatch.setattr(fake, "head_object", lambda Bucket, Key: {"ETag": '"changed"'})  # noqa: N803
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{p}" for p in prefixes])

    assert fake.puts == [] and run.failures == 3 and run.scanned == 3


def test_put_refuses_a_key_that_is_not_a_zarr_json(tmp_path) -> None:
    run = czg.ConsolidateRun(FakeS3({}), max_writes=3, apply=True, backup_dir=tmp_path)
    with pytest.raises(czg.PlanError):
        run._put_verified(BUCKET, f"{S1}/ascending/r10m/vv/c/0/0/0", b"")
    assert run.writes == 0


def test_failures_that_are_not_consecutive_do_not_abort(tmp_path) -> None:
    stores = [
        f"s3://{BUCKET}/missing-{i}.zarr" if i % 2 == 0 else f"s3://{BUCKET}/{S1}" for i in range(6)
    ]
    run = _run(FakeS3(stripped_cube()), tmp_path, stores, apply=False)

    assert run.failures == 3 and run.scanned == 6


def test_ten_failures_in_total_abort(tmp_path) -> None:
    stores = []
    for i in range(12):
        stores += [f"s3://{BUCKET}/missing-{i}.zarr", f"s3://{BUCKET}/{S1}"]
    run = _run(FakeS3(stripped_cube()), tmp_path, stores, apply=False, max_writes=1000)

    assert run.failures == 10 and run.scanned == 19


def test_three_consecutive_failures_abort(tmp_path) -> None:
    fake = FakeS3({})
    stores = [f"s3://{BUCKET}/missing-{i}.zarr" for i in range(5)]
    run = _run(fake, tmp_path, stores)

    assert run.failures == 3 and run.scanned == 3


# --- restore ---------------------------------------------------------------------------------


def _repaired(tmp_path) -> tuple[FakeS3, dict, czg.ConsolidateRun]:
    before = stripped_cube()
    fake = FakeS3(before)
    run = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])
    return fake, before, run


def _restore(fake, tmp_path, backup, *, apply=True, force=False):
    run = czg.ConsolidateRun(
        fake, max_writes=3, apply=apply, backup_dir=tmp_path / "restore" if apply else None
    )
    run.restore(backup, force=force)
    return run


def test_restore_puts_the_original_bytes_back(tmp_path) -> None:
    fake, before, repair = _repaired(tmp_path)
    run = _restore(fake, tmp_path, repair.backup_path)

    assert run.failures == 0 and run.verified == 3
    assert all(
        fake.objects[(BUCKET, _key(S1, g))] == before[(BUCKET, _key(S1, g))] for g in S1_GROUPS
    )
    undo = [json.loads(line) for line in run.backup_path.read_text().splitlines()]
    assert all("consolidated_metadata" in json.loads(e["body"]) for e in undo)


def test_restore_refuses_an_object_changed_since_the_repair(tmp_path) -> None:
    fake, before, repair = _repaired(tmp_path)
    root_key = (BUCKET, _key(S1, ""))
    fake.objects[root_key] = fake.objects[root_key] + b"\n"  # someone wrote it since

    run = _restore(fake, tmp_path, repair.backup_path)
    assert run.failures == 1 and fake.objects[root_key] != before[root_key]

    forced = _restore(fake, tmp_path, repair.backup_path, force=True)
    assert forced.failures == 0 and fake.objects[root_key] == before[root_key]


def test_restore_recognises_a_write_whose_put_timed_out(tmp_path) -> None:
    before = stripped_cube()
    fake = FakeS3(before)
    fake.timeout_after_put_on = _key(S1, "descending")
    repair = _run(fake, tmp_path, [f"s3://{BUCKET}/{S1}"])
    fake.timeout_after_put_on = None

    run = _restore(fake, tmp_path, repair.backup_path)  # no --force needed
    assert run.failures == 0 and run.verified == 2 and run.skipped_clean == 1
    assert all(fake.objects[k] == v for k, v in before.items())


def test_restore_refuses_a_backup_line_that_fails_its_hash(tmp_path) -> None:
    fake, _, repair = _repaired(tmp_path)
    lines = repair.backup_path.read_text().splitlines()
    tampered = json.loads(lines[0])
    tampered["body"] = tampered["body"].replace("zarr_format", "zarr_formaT")
    repair.backup_path.write_text("\n".join([json.dumps(tampered), *lines[1:]]) + "\n")

    run = _restore(fake, tmp_path, repair.backup_path)
    assert run.failures == 1 and run.verified == 2
    assert "zarr_formaT" not in fake.objects[(BUCKET, tampered["key"])].decode()


def test_a_restore_never_writes_into_the_backup_it_restores(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(czg.time, "strftime", lambda *_: "20261001T000000Z")  # same second
    fake, _, repair = _repaired(tmp_path)
    original = repair.backup_path.read_text()
    run = czg.ConsolidateRun(fake, max_writes=3, apply=True, backup_dir=repair.backup_path.parent)
    run.restore(repair.backup_path, force=False)

    assert run.verified == 3 and run.backup_path != repair.backup_path
    assert repair.backup_path.read_text() == original


def test_a_bounded_restore_stops_between_stores(tmp_path) -> None:
    fake, stores, other = _two_stores()
    before = dict(fake.objects)
    repair = _run(fake, tmp_path, stores)
    run = czg.ConsolidateRun(fake, max_writes=4, apply=True, backup_dir=tmp_path / "restore")
    run.restore(repair.backup_path, force=False)

    assert run.truncated and run.writes == 3
    assert all(
        fake.objects[(BUCKET, _key(S1, g))] == before[(BUCKET, _key(S1, g))] for g in S1_GROUPS
    )
    assert all("consolidated_metadata" in _doc(fake, other, g) for g in S1_GROUPS)


def test_a_torn_last_backup_line_still_restores_the_rest(tmp_path) -> None:
    fake, before, repair = _repaired(tmp_path)
    with open(repair.backup_path, "a") as fh:
        fh.write('{"format": 1, "bucket": "bucket", "key": "x/zarr.j')  # cut off by a full disk
    run = _restore(fake, tmp_path, repair.backup_path)

    assert run.failures == 1 and run.verified == 3
    assert all(fake.objects[k] == v for k, v in before.items())


def test_a_backup_line_of_another_format_fails_instead_of_crashing(tmp_path) -> None:
    fake, _, repair = _repaired(tmp_path)
    puts = list(fake.puts)
    lines = [json.loads(line) for line in repair.backup_path.read_text().splitlines()]
    repair.backup_path.write_text("".join(json.dumps({**e, "format": 0}) + "\n" for e in lines))
    run = _restore(fake, tmp_path, repair.backup_path)

    assert run.failures == 3 and fake.puts == puts


def test_a_restore_re_run_needs_only_the_budget_that_is_left(tmp_path) -> None:
    fake, before, repair = _repaired(tmp_path)
    fake.fail_put_on = _key(S1, "descending")
    first = _restore(fake, tmp_path, repair.backup_path)
    assert first.failures == 1 and first.verified == 1
    fake.fail_put_on = None

    run = czg.ConsolidateRun(fake, max_writes=2, apply=True, backup_dir=tmp_path / "again")
    run.restore(repair.backup_path, force=False)
    assert not run.truncated and run.verified == 2 and run.skipped_clean == 1
    assert all(fake.objects[k] == v for k, v in before.items())


def test_restore_dry_run_writes_nothing(tmp_path) -> None:
    fake, _, repair = _repaired(tmp_path)
    puts = list(fake.puts)
    run = _restore(fake, tmp_path, repair.backup_path, apply=False)

    assert fake.puts == puts and run.writes == 3


# --- CLI -------------------------------------------------------------------------------------


def test_cli_dry_run(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeS3(stripped_cube())
    monkeypatch.setattr(czg, "make_s3_client", lambda endpoint: fake)
    args = ["--store", f"s3://{BUCKET}/{S1}", "--max-writes", "3"]
    rc = czg.main(args + ["--group", "ascending", "--group", "descending", "--group", "."])

    assert rc == 0 and fake.puts == []
    assert "[DRY-RUN]" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv",
    [
        [
            *["--store", f"s3://{BUCKET}/{S1}", "--group", ".", "--max-writes", "3"],
            *["--apply", "--s3-endpoint", "https://s3.fake"],
        ],
        [
            *["--store", f"s3://{BUCKET}/{S1}", "--group", ".", "--max-writes", "3"],
            *["--apply", "--backup-dir", "unused"],
        ],
        ["--store", f"s3://{BUCKET}/{S1}", "--group", ".", "--max-writes", "0"],
        ["--store", f"s3://{BUCKET}/{S1}", "--max-writes", "3"],
        ["--group", ".", "--max-writes", "3"],
        ["--restore", "b.jsonl", "--store", f"s3://{BUCKET}/{S1}", "--max-writes", "3"],
        ["--restore", "b.jsonl", "--group", ".", "--max-writes", "3"],
    ],
    ids=[
        "apply-without-backup-dir",
        "apply-without-endpoint",
        "zero-budget",
        "no-group",
        "no-store",
        "restore-with-store",
        "restore-with-group",
    ],
)
def test_cli_refuses_unsafe_or_incomplete_arguments(argv, monkeypatch) -> None:
    monkeypatch.setattr(czg, "make_s3_client", lambda endpoint: FakeS3({}))
    with pytest.raises(SystemExit):
        czg.main(argv)


def _cli(monkeypatch, fake, argv) -> int:
    monkeypatch.setattr(czg, "make_s3_client", lambda endpoint: fake)
    return czg.main(argv)


def test_cli_max_writes_bounds_an_apply(tmp_path, monkeypatch) -> None:
    fake = FakeS3(stripped_cube())
    argv = ["--store", f"s3://{BUCKET}/{S1}", "--group", "ascending", "--group", "descending"]
    argv += ["--group", ".", "--max-writes", "2", "--apply", "--s3-endpoint", "https://s3.fake"]
    rc = _cli(monkeypatch, fake, [*argv, "--backup-dir", str(tmp_path)])

    assert rc == 0 and fake.puts == []


def test_cli_deduplicates_stores_and_groups(tmp_path, monkeypatch, capsys) -> None:
    store = f"s3://{BUCKET}/{S1}"
    argv = ["--store", store, "--store", f"{store}/", "--group", ".", "--group", "/"]
    _cli(monkeypatch, FakeS3(stripped_cube()), [*argv, "--max-writes", "10"])

    assert "scanned=1 clean-skipped=0 writes=1 " in capsys.readouterr().out


def test_cli_expands_a_tilde_in_backup_dir(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    argv = ["--store", f"s3://{BUCKET}/{S1}", "--group", ".", "--max-writes", "1", "--apply"]
    argv += ["--s3-endpoint", "https://s3.fake", "--backup-dir=~/backups"]
    _cli(monkeypatch, FakeS3(stripped_cube()), argv)

    assert len(list((tmp_path / "backups").glob("*.jsonl"))) == 1
