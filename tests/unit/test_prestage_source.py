"""Unit tests for prestage_source.py — S3 mapping, allowlist, copy/verify, cleanup guards.

"S3" here means object storage. Where the *mission* Sentinel-3 is meant, it is spelled
out — the Sentinel-3 cases exist because prestage is mission-agnostic and must stay
that way: ids, buckets and hrefs below are copied from live sentinel-3-olci-l1-efr
items, and staging one through the same code path is what proves the claim.

No network: the S3 clients are replaced with an in-memory fake so the copy,
skip-if-identical, verification and cleanup logic are exercised for real.
"""

from __future__ import annotations

import hashlib
import io
import sys
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import prestage_source  # noqa: E402

# EODC hrefs are Ceph RGW: path-style, explicit :443, and a bucket name that
# literally contains a colon (tenant:container).
EODC_HREF = (
    "https://objects.eodc.eu:443/e05ab01a9d56408d82ac32d69a5aae2a:202607-s02msil2a-eu"
    "/13/products/cpm_v270/S2B_MSIL2A_20260713T104619_N0512_R051_T31UDP_20260713T131840.zarr"
)
EODC_BUCKET = "e05ab01a9d56408d82ac32d69a5aae2a:202607-s02msil2a-eu"
EODC_KEY = "13/products/cpm_v270/S2B_MSIL2A_20260713T104619_N0512_R051_T31UDP_20260713T131840.zarr"
ITEM_ID = "S2B_MSIL2A_20260713T104619_N0512_R051_T31UDP_20260713T131840"
STAC_ITEM_URL = f"https://stac.example.com/collections/sentinel-2-l2a/items/{ITEM_ID}"
GATEWAY_HREF = f"https://s3.explorer.eopf.copernicus.eu/bucket/cpm-manual/{ITEM_ID}.zarr"

# Sentinel-3 (the mission): a live OLCI L1 EFR product. Same host, same Ceph
# tenant:container shape, different tenant container and a very different id.
S3_OLCI_ITEM_ID = (
    "S3A_OL_1_EFR____20260714T222153_20260714T222243_20260715T003629_0050_141_329_1080_PS1_O_NR_004"
)
S3_OLCI_BUCKET = "e05ab01a9d56408d82ac32d69a5aae2a:202607-s03olcefr-eu"
S3_OLCI_KEY = f"14/products/cpm_v270/{S3_OLCI_ITEM_ID}.zarr"
S3_OLCI_HREF = f"https://objects.eodc.eu:443/{S3_OLCI_BUCKET}/{S3_OLCI_KEY}"
S3_OLCI_STAC_ITEM_URL = (
    f"https://stac.core.eopf.eodc.eu/collections/sentinel-3-olci-l1-efr/items/{S3_OLCI_ITEM_ID}"
)


# ---------------------------------------------------------------------------
# In-memory S3 fake
# ---------------------------------------------------------------------------


def _etag(body: bytes) -> str:
    return f'"{hashlib.md5(body, usedforsecurity=False).hexdigest()}"'


class FakeS3:
    """Minimal in-memory stand-in for the boto3 S3 client surface we use."""

    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects: dict[tuple[str, str], bytes] = dict(objects or {})
        self.puts: list[str] = []
        self.gets: list[str] = []
        self.deleted: list[str] = []

    def get_paginator(self, operation: str):
        assert operation == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket: str, Key: str):  # noqa: N803 (boto3 kwarg names)
        self.gets.append(Key)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket: str, Key: str, Body: bytes):  # noqa: N803
        self.objects[(Bucket, Key)] = Body
        self.puts.append(Key)
        return {}

    def delete_objects(self, Bucket: str, Delete: dict):  # noqa: N803
        keys = [o["Key"] for o in Delete["Objects"]]
        for key in keys:
            self.objects.pop((Bucket, key), None)
            self.deleted.append(key)
        return {"Deleted": [{"Key": k} for k in keys]}


class _FakePaginator:
    def __init__(self, fake: FakeS3) -> None:
        self.fake = fake

    def paginate(self, Bucket: str, Prefix: str):  # noqa: N803
        contents = [
            {"Key": key, "Size": len(body), "ETag": _etag(body)}
            for (bucket, key), body in sorted(self.fake.objects.items())
            if bucket == Bucket and key.startswith(Prefix)
        ]
        # Two pages, to prove pagination is consumed rather than assumed single-page.
        yield {"Contents": contents[:1]}
        yield {"Contents": contents[1:]}


# Key layout copied from a live listing of the Fontainebleau product (2026-07-15):
# zarr v2, and `quality/atmosphere/r10m/aot/0.0` really is one 4.6 MB chunk — the
# single-chunk array whose concurrent same-key reads trigger #339.
SOURCE_KEYS = {
    ".zattrs": b"{}",
    "quality/atmosphere/r10m/aot/.zarray": b'{"chunks":[10980,10980]}',
    "quality/atmosphere/r10m/aot/0.0": b"single-10980-squared-chunk",
    "measurements/reflectance/r10m/b08/0.2": b"chunk-b08",
}


def _source_store() -> FakeS3:
    """The source product as it actually lives under the EODC zarr root."""
    return FakeS3({(EODC_BUCKET, f"{EODC_KEY}/{key}"): body for key, body in SOURCE_KEYS.items()})


@pytest.fixture
def clients(monkeypatch):
    """Patch the client factories; return (source, dest) fakes."""
    source, dest = _source_store(), FakeS3()
    monkeypatch.setattr(prestage_source, "_source_client", lambda endpoint: source)
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)
    monkeypatch.setattr(prestage_source, "resolve_zarr_url", lambda url: EODC_HREF)
    return source, dest


def _argv(tmp_path: Path, **overrides: str) -> list[str]:
    args = {
        "--source-url": STAC_ITEM_URL,
        "--dest-bucket": "esa-zarr-sentinel-explorer-fra",
        "--output-dir": str(tmp_path),
        **overrides,
    }
    return [item for pair in args.items() for item in pair]


def _outputs(tmp_path: Path) -> tuple[str, str]:
    return (
        (tmp_path / "convert_source_url").read_text(),
        (tmp_path / "staged").read_text(),
    )


# ---------------------------------------------------------------------------
# href -> S3 mapping
# ---------------------------------------------------------------------------


def test_parse_href_maps_colon_bucket_and_strips_default_port():
    loc = prestage_source.parse_https_s3_href(EODC_HREF)

    assert loc.endpoint == "https://objects.eodc.eu"
    assert loc.bucket == EODC_BUCKET
    assert loc.key == EODC_KEY


def test_parse_href_maps_a_sentinel3_product():
    """Same Ceph shape, different tenant container — no mission-specific parsing."""
    loc = prestage_source.parse_https_s3_href(S3_OLCI_HREF)

    assert loc.endpoint == "https://objects.eodc.eu"
    assert loc.bucket == S3_OLCI_BUCKET
    assert loc.key == S3_OLCI_KEY


def test_parse_href_without_explicit_port():
    loc = prestage_source.parse_https_s3_href(EODC_HREF.replace(":443", ""))

    assert loc.endpoint == "https://objects.eodc.eu"
    assert loc.bucket == EODC_BUCKET


def test_parse_href_strips_trailing_slash():
    assert prestage_source.parse_https_s3_href(EODC_HREF + "/").key == EODC_KEY


def test_parse_href_keeps_non_default_port():
    loc = prestage_source.parse_https_s3_href(EODC_HREF.replace(":443", ":8443"))

    assert loc.endpoint == "https://objects.eodc.eu:8443"


# ---------------------------------------------------------------------------
# staged key naming + guards
# ---------------------------------------------------------------------------


def test_staged_prefix_has_no_zarr_suffix():
    """Convert re-derives item_id from the staged URL, so the staged segment must
    equal item_id exactly — a .zarr suffix here would produce item.zarr.zarr output."""
    assert prestage_source.staged_prefix("source-cache", ITEM_ID) == f"source-cache/{ITEM_ID}/"


def test_staged_url_is_native_s3():
    url = prestage_source.staged_url("my-bucket", "source-cache", ITEM_ID)

    assert url == f"s3://my-bucket/source-cache/{ITEM_ID}"


@pytest.mark.parametrize("item_id", ["", "/", "   "])
def test_staged_prefix_refuses_empty_item_segment(item_id):
    with pytest.raises(ValueError, match="item"):
        prestage_source.staged_prefix("source-cache", item_id)


def test_staged_prefix_refuses_empty_dest_prefix():
    with pytest.raises(ValueError, match="dest_prefix"):
        prestage_source.staged_prefix("", ITEM_ID)


# ---------------------------------------------------------------------------
# passthrough
# ---------------------------------------------------------------------------


def test_passthrough_mode_echoes_source_url_without_network(tmp_path, monkeypatch):
    """Flag off must reproduce today's behaviour exactly: convert gets the original
    STAC item URL and nothing is contacted."""

    def _boom(*_a, **_k):
        raise AssertionError("passthrough must not touch the network")

    monkeypatch.setattr(prestage_source, "_source_client", _boom)
    monkeypatch.setattr(prestage_source, "_dest_client", _boom)
    monkeypatch.setattr(prestage_source, "resolve_zarr_url", _boom)

    rc = prestage_source.main(_argv(tmp_path, **{"--mode": "passthrough"}))

    assert rc == 0
    assert _outputs(tmp_path) == (STAC_ITEM_URL, "false")


def test_non_allowlisted_host_falls_back_to_passthrough(tmp_path, monkeypatch):
    """The nginx-s3-gateway answers ListObjectsV2 with an HTML index, so gateway-hosted
    sources (cpm-manual/) must pass through untouched even with the flag on."""
    dest = FakeS3()
    monkeypatch.setattr(prestage_source, "resolve_zarr_url", lambda url: GATEWAY_HREF)
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)
    monkeypatch.setattr(
        prestage_source,
        "_source_client",
        lambda endpoint: (_ for _ in ()).throw(AssertionError("must not copy")),
    )

    rc = prestage_source.main(_argv(tmp_path, **{"--source-url": GATEWAY_HREF}))

    assert rc == 0
    assert _outputs(tmp_path) == (GATEWAY_HREF, "false")
    assert dest.puts == []


# ---------------------------------------------------------------------------
# copy + verify
# ---------------------------------------------------------------------------


def test_copy_stages_every_object_and_reports_staged_s3_url(tmp_path, clients):
    source, dest = clients

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 0
    staged_keys = {key for (_b, key) in dest.objects}
    assert staged_keys == {f"source-cache/{ITEM_ID}/{key}" for key in SOURCE_KEYS}
    assert _outputs(tmp_path) == (
        f"s3://esa-zarr-sentinel-explorer-fra/source-cache/{ITEM_ID}",
        "true",
    )


def test_copy_stages_a_sentinel3_product_through_the_same_path(tmp_path, monkeypatch):
    """Prestage is mission-agnostic: a Sentinel-3 OLCI product stages with no S2 in sight.

    Guards the reuse claim in the module docstring — Sentinel-3 needs a template, not a
    code change. Uses the live OLCI key layout (measurement groups, not S2's r10m/aot).
    """
    olci_keys = {
        ".zattrs": b"{}",
        "measurements/oa01_radiance/0.0": b"olci-band-1-chunk",
        "measurements/oa21_radiance/0.0": b"olci-band-21-chunk",
        "conditions/geometry/sza/0.0": b"sun-zenith-chunk",
    }
    source = FakeS3({(S3_OLCI_BUCKET, f"{S3_OLCI_KEY}/{k}"): v for k, v in olci_keys.items()})
    dest = FakeS3()
    monkeypatch.setattr(prestage_source, "_source_client", lambda endpoint: source)
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)
    monkeypatch.setattr(prestage_source, "resolve_zarr_url", lambda url: S3_OLCI_HREF)

    rc = prestage_source.main(_argv(tmp_path, **{"--source-url": S3_OLCI_STAC_ITEM_URL}))

    assert rc == 0
    assert {key for (_b, key) in dest.objects} == {
        f"source-cache/{S3_OLCI_ITEM_ID}/{key}" for key in olci_keys
    }
    # The staged URL carries the Sentinel-3 id, so convert's output path and register's
    # geozarr URL agree with it exactly as they do for S2.
    assert _outputs(tmp_path) == (
        f"s3://esa-zarr-sentinel-explorer-fra/source-cache/{S3_OLCI_ITEM_ID}",
        "true",
    )


def test_cleanup_deletes_a_sentinel3_staged_copy(tmp_path, monkeypatch):
    """The delete guards key off item_id alone, so they hold for any mission."""
    bucket = "esa-zarr-sentinel-explorer-fra"
    dest = FakeS3(
        {
            (bucket, f"source-cache/{S3_OLCI_ITEM_ID}/zarr.json"): b"{}",
            (bucket, f"source-cache/{S3_OLCI_ITEM_ID}/measurements/oa01_radiance/0.0"): b"chunk",
            # A different mission staged alongside it must survive.
            (bucket, f"source-cache/{ITEM_ID}/zarr.json"): b"{}",
        }
    )
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)

    rc = prestage_source.main(
        _argv(tmp_path, **{"--source-url": S3_OLCI_STAC_ITEM_URL, "--mode": "cleanup"})
    )

    assert rc == 0
    assert {key for (_b, key) in dest.objects} == {f"source-cache/{ITEM_ID}/zarr.json"}


def test_copy_preserves_bytes(tmp_path, clients):
    source, dest = clients

    prestage_source.main(_argv(tmp_path))

    chunk = "quality/atmosphere/r10m/aot/0.0"
    assert (
        dest.objects[("esa-zarr-sentinel-explorer-fra", f"source-cache/{ITEM_ID}/{chunk}")]
        == (source.objects[(EODC_BUCKET, f"{EODC_KEY}/{chunk}")])
    )


def test_rerun_skips_keys_already_staged_identically(tmp_path, clients):
    """Idempotent retry: a second run re-copies nothing and still exits 0."""
    source, dest = clients
    assert prestage_source.main(_argv(tmp_path)) == 0
    dest.puts.clear()
    source.gets.clear()

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 0
    assert dest.puts == []
    assert source.gets == []


def test_rerun_recopies_key_whose_bytes_differ(tmp_path, clients):
    """A truncated/partial staged object must not be mistaken for a good one."""
    source, dest = clients
    assert prestage_source.main(_argv(tmp_path)) == 0
    chunk = f"source-cache/{ITEM_ID}/quality/atmosphere/r10m/aot/0.0"
    dest.objects[("esa-zarr-sentinel-explorer-fra", chunk)] = b"truncated"
    dest.puts.clear()

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 0
    assert dest.puts == [chunk]


def test_empty_source_listing_exits_3(tmp_path, monkeypatch):
    monkeypatch.setattr(prestage_source, "resolve_zarr_url", lambda url: EODC_HREF)
    monkeypatch.setattr(prestage_source, "_source_client", lambda endpoint: FakeS3())
    monkeypatch.setattr(prestage_source, "_dest_client", FakeS3)

    assert prestage_source.main(_argv(tmp_path)) == 3


def test_verification_mismatch_exits_2_and_writes_no_outputs(tmp_path, clients, monkeypatch):
    """Never declare a stage complete without proving source and dest agree."""
    source, dest = clients
    real_put = dest.put_object

    def _lossy_put(Bucket, Key, Body):  # noqa: N803 — silently drops one object
        if Key.endswith("aot/0.0"):
            return {}
        return real_put(Bucket=Bucket, Key=Key, Body=Body)

    monkeypatch.setattr(dest, "put_object", _lossy_put)

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 2
    assert not (tmp_path / "convert_source_url").exists()


def test_stage_proves_the_staged_copy_is_readable_back(tmp_path, clients):
    """Counting objects proves LIST; convert needs GET. Only a real read proves a stage."""
    _, dest = clients
    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 0
    assert dest.gets, "verified a stage without ever reading one staged object back"


def test_write_only_identity_fails_in_prestage_not_later_in_convert(
    tmp_path, clients, monkeypatch, caplog
):
    """The geozarr-s3-credentials identity now READS the staged copy as well as writing
    the output. If it is ever scoped to write-without-read on this prefix, the copy and
    the object count both still succeed and only convert breaks — as an opaque failure
    that reads like a conversion bug. Prestage must be the thing that fails, and say why.
    """
    _, dest = clients

    def _denied(Bucket, Key):  # noqa: N803 — can PUT and LIST, cannot GET
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "GetObject"
        )

    monkeypatch.setattr(dest, "get_object", _denied)

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 1, "a permission failure is permanent — it must not be retried like a flake"
    assert not (tmp_path / "convert_source_url").exists(), "must not hand convert an unreadable url"
    # The message has to point at the credential, not at the data.
    text = caplog.text.lower()
    assert "read" in text
    assert "credential" in text or "aws_access_key_id" in text


def _deny_listing(fake, monkeypatch, code="AccessDenied"):
    def _boom(operation):
        raise ClientError({"Error": {"Code": code, "Message": code}}, "ListObjectsV2")

    monkeypatch.setattr(fake, "get_paginator", _boom)


@pytest.mark.parametrize("code", ["SlowDown", "ServiceUnavailable", "RequestTimeout"])
def test_a_throttled_listing_is_retryable_and_does_not_blame_permissions(
    tmp_path, clients, monkeypatch, caplog, code
):
    """Not every S3 error is a permission error.

    16 copy threads against EODC is a known throttling risk (the spec says to lower
    --copy-workers if it bites). Treating SlowDown as a permission fault would tell the
    operator to fix IAM, and exit 1 is the one code the template's retry expression
    skips — so a transient throttle would permanently fail a workflow that a 30s backoff
    would have fixed.
    """
    _, dest = clients
    _deny_listing(dest, monkeypatch, code=code)

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == prestage_source.EXIT_TRANSIENT_S3, "a throttle must stay retryable"
    assert rc != 1, "exit 1 is skipped by the template retry — a throttle must not land there"
    text = caplog.text.lower()
    assert "geozarr-s3-credentials" not in text, "must not blame permissions for a throttle"
    assert code.lower() in text


def test_dest_listing_denied_names_the_credential(tmp_path, clients, monkeypatch, caplog):
    """Losing ListBucket must not surface as a bare traceback."""
    _, dest = clients
    _deny_listing(dest, monkeypatch)

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 1
    text = caplog.text.lower()
    assert "traceback" not in text
    assert "geozarr-s3-credentials" in text
    assert not (tmp_path / "convert_source_url").exists()


def test_source_listing_denied_points_at_eodc_access(tmp_path, clients, monkeypatch, caplog):
    """A source denial is a different fix from a dest denial — say which."""
    source, _ = clients
    _deny_listing(source, monkeypatch)

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 1
    text = caplog.text.lower()
    assert "eodc" in text
    assert "anonymous" in text
    assert (
        "geozarr-s3-credentials" not in text
    ), "must not blame the OVH identity for an EODC denial"


def test_an_unexpected_s3_error_is_reported_not_raised(tmp_path, clients, monkeypatch, caplog):
    """Any other S3 failure (here: PutObject denied) still exits cleanly."""
    _, dest = clients

    def _denied_put(Bucket, Key, Body):  # noqa: N803
        raise ClientError({"Error": {"Code": "AccessDenied", "Message": "Denied"}}, "PutObject")

    monkeypatch.setattr(dest, "put_object", _denied_put)

    rc = prestage_source.main(_argv(tmp_path))

    assert rc == 1
    assert "accessdenied" in caplog.text.lower()
    assert not (tmp_path / "convert_source_url").exists()


def test_cleanup_listing_denied_is_reported_cleanly(tmp_path, monkeypatch, caplog):
    """cleanup runs with continueOn.failed, so its message is the only diagnostic."""
    dest = FakeS3()
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)
    _deny_listing(dest, monkeypatch)

    rc = prestage_source.main(_argv(tmp_path, **{"--mode": "cleanup"}))

    assert rc == 1
    assert "traceback" not in caplog.text.lower()


def test_verification_catches_byte_total_mismatch(tmp_path, clients, monkeypatch):
    """Equal object counts but unequal bytes is still a bad stage."""
    source, dest = clients
    real_put = dest.put_object

    def _truncating_put(Bucket, Key, Body):  # noqa: N803
        return real_put(Bucket=Bucket, Key=Key, Body=Body[:1])

    monkeypatch.setattr(dest, "put_object", _truncating_put)

    assert prestage_source.main(_argv(tmp_path)) == 2


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------


def test_cleanup_deletes_only_the_staged_item_prefix(tmp_path, monkeypatch):
    bucket = "esa-zarr-sentinel-explorer-fra"
    dest = FakeS3(
        {
            (bucket, f"source-cache/{ITEM_ID}/zarr.json"): b"{}",
            (bucket, f"source-cache/{ITEM_ID}/b02/c/0/0"): b"chunk",
            (bucket, "source-cache/OTHER_ITEM/zarr.json"): b"{}",
            (bucket, "tests-output/sentinel-2-l2a/live-item.zarr/zarr.json"): b"{}",
        }
    )
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)

    rc = prestage_source.main(_argv(tmp_path, **{"--mode": "cleanup"}))

    assert rc == 0
    assert sorted(dest.deleted) == [
        f"source-cache/{ITEM_ID}/b02/c/0/0",
        f"source-cache/{ITEM_ID}/zarr.json",
    ]
    assert (bucket, "source-cache/OTHER_ITEM/zarr.json") in dest.objects
    assert (bucket, "tests-output/sentinel-2-l2a/live-item.zarr/zarr.json") in dest.objects


def test_cleanup_refuses_empty_item_segment(tmp_path, monkeypatch):
    """A source_url with no basename must never widen into a prefix-wide delete."""
    dest = FakeS3({("esa-zarr-sentinel-explorer-fra", "source-cache/x/zarr.json"): b"{}"})
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)

    rc = prestage_source.main(
        _argv(tmp_path, **{"--mode": "cleanup", "--source-url": "https://stac.example.com/"})
    )

    assert rc == 1
    assert dest.deleted == []


def test_cleanup_refuses_empty_dest_prefix(tmp_path, monkeypatch):
    dest = FakeS3({("esa-zarr-sentinel-explorer-fra", "source-cache/x/zarr.json"): b"{}"})
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)

    rc = prestage_source.main(_argv(tmp_path, **{"--mode": "cleanup", "--dest-prefix": ""}))

    assert rc == 1
    assert dest.deleted == []


def test_cleanup_of_absent_prefix_is_a_no_op(tmp_path, monkeypatch):
    """Cleanup runs with continueOn:failed after register; an already-gone stage is success."""
    dest = FakeS3()
    monkeypatch.setattr(prestage_source, "_dest_client", lambda: dest)

    assert prestage_source.main(_argv(tmp_path, **{"--mode": "cleanup"})) == 0
    assert dest.deleted == []
