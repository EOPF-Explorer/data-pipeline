"""Unit tests for the S3 frame cache (scripts/cache_frames.py, Tasks 5 & 6).

S3 is faked in-memory (no moto dependency): a dict of key -> bytes with the three
operations the module uses (head_object, upload_fileobj, download_fileobj). The
fake's ClientError 404 mirrors boto3 so cache_has()'s miss path is exercised.
"""

import io
import sys
import tarfile
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
import cache_frames as cf  # noqa: E402


# --------------------------------------------------------------------------- #
# Fakes / helpers
# --------------------------------------------------------------------------- #
class FakeS3:
    """Minimal in-memory S3 stand-in (key -> bytes)."""

    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.uploads = 0

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def upload_fileobj(self, Fileobj, Bucket, Key):
        self.store[Key] = Fileobj.read()
        self.uploads += 1

    def download_fileobj(self, Bucket, Key, Fileobj):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject")
        Fileobj.write(self.store[Key])

    def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def delete_object(self, Bucket, Key):
        self.store.pop(Key, None)


VALID_ID = "S1A_IW_GRDH_1SDV_20240101T060000_20240101T060025_052000_064700_ABCD"
VALID_ID_2 = "S1A_IW_GRDH_1SDV_20240113T060000_20240113T060025_052100_064800_EF01"
S1D_ID = "S1D_IW_GRDH_1SDV_20240102T060000_20240102T060025_001000_002000_BEEF"

# The same acquisition served two ways by CDSE: eodag downloads the classic SAFE,
# list_tile_frames lists the COG. They share the first 8 fields (the acquisition key).
CLASSIC_ID = "S1A_IW_GRDH_1SDV_20260618T051026_20260618T051051_065019_0831D4_19B8"
COG_ID = "S1A_IW_GRDH_1SDV_20260618T051026_20260618T051051_065019_0831D4_8DC1_COG"


def _make_safe(data_raw, prod_id, tiff=b"raster-bytes"):
    """Create a minimal extracted SAFE tree on disk (eodag-4 layout: manifest.safe +
    measurement/ live DIRECTLY under data_raw/{prod_id}/, no nested {prod_id}.SAFE/)."""
    safe = Path(data_raw) / prod_id
    (safe / "measurement").mkdir(parents=True)
    (safe / "manifest.safe").write_bytes(b"<xfdu:XFDU/>")
    (safe / "measurement" / "iw-vv.tiff").write_bytes(tiff)
    return safe


# --------------------------------------------------------------------------- #
# Key / id validation (the trust boundary)
# --------------------------------------------------------------------------- #
class TestValidateProdId:
    def test_accepts_real_s1_id(self):
        assert cf.validate_prod_id(VALID_ID) == VALID_ID

    @pytest.mark.parametrize(
        "bad",
        [
            "../etc/passwd",
            "S1A_IW/../../escape",
            "S1A_IW_GRDH/nested",
            "S1A_IW",  # too short
            "s1a_iw_grdh_lowercase_not_allowed_xxxxxxxxxx",
            "X1A_IW_GRDH_1SDV_xxxxxxxxxxxxxxxx",  # wrong mission
            "S1A_IW_GRDH_1SDV_;rm -rf",  # shell metachars
            "",
            "S1A_IW_GRDH_1SDV_with.dot.xxxxxxxxxx",  # '.' is path-ish, rejected
        ],
    )
    def test_rejects_unsafe_ids(self, bad):
        with pytest.raises(ValueError):
            cf.validate_prod_id(bad)

    def test_frame_key_validates_and_formats(self):
        acq = cf.acquisition_key(VALID_ID)
        assert cf.frame_key("frame-cache", VALID_ID) == f"frame-cache/{acq}.tar"
        assert cf.frame_key("frame-cache/", VALID_ID) == f"frame-cache/{acq}.tar"
        with pytest.raises(ValueError):
            cf.frame_key("frame-cache", "../evil")


class TestAcquisitionKey:
    def test_strips_trailing_unique_id(self):
        assert cf.acquisition_key(CLASSIC_ID) == (
            "S1A_IW_GRDH_1SDV_20260618T051026_20260618T051051_065019_0831D4"
        )

    def test_classic_and_cog_map_to_same_key(self):
        # the whole point: eodag's classic id and the STAC's COG id key the same tar.
        assert cf.acquisition_key(CLASSIC_ID) == cf.acquisition_key(COG_ID)

    def test_idempotent_on_a_key(self):
        k = cf.acquisition_key(CLASSIC_ID)
        assert cf.acquisition_key(k) == k

    def test_rejects_invalid_id(self):
        with pytest.raises(ValueError):
            cf.acquisition_key("../evil")

    def test_rejects_too_few_fields(self):
        # valid id grammar (passes validate_prod_id) but not enough fields to name an
        # acquisition — must fail loud rather than silently key on a truncated prefix.
        with pytest.raises(ValueError, match="too few fields"):
            cf.acquisition_key("S1A_IW_GRDH_TOOSHORT")


# --------------------------------------------------------------------------- #
# Pull
# --------------------------------------------------------------------------- #
class TestPull:
    def test_miss_reported_when_not_in_cache(self, tmp_path):
        s3 = FakeS3()
        assert cf.pull_frame(s3, "b", "frame-cache", VALID_ID, tmp_path) == "miss"

    def test_present_is_idempotent_noop(self, tmp_path):
        # SAFE already on disk -> "present", no cache access needed.
        _make_safe(tmp_path, VALID_ID)
        s3 = FakeS3()  # empty cache
        assert cf.pull_frame(s3, "b", "frame-cache", VALID_ID, tmp_path) == "present"

    def test_hit_restores_safe_tree(self, tmp_path):
        # populate from src, pull into a fresh dst, assert the SAFE round-trips.
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        _make_safe(src, VALID_ID, tiff=b"unique-pixels")
        s3 = FakeS3()
        assert cf.populate_frame(s3, "b", "fc", VALID_ID, src) == "uploaded"

        assert cf.pull_frame(s3, "b", "fc", VALID_ID, dst) == "hit"
        restored = dst / VALID_ID
        assert (restored / "manifest.safe").is_file()
        assert (restored / "measurement" / "iw-vv.tiff").read_bytes() == b"unique-pixels"

    def test_cog_id_pulls_classic_cached_frame(self, tmp_path):
        # THE parity fix: eodag downloads the classic SAFE (…_19B8); the next tile lists
        # the same frame as a COG id (…_8DC1_COG). Populating from the classic id must be
        # a cache HIT when pulled by the COG id, restored under the real classic dir name
        # so S1Tiling's scan skips it.
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        _make_safe(src, CLASSIC_ID, tiff=b"classic-pixels")
        s3 = FakeS3()
        assert cf.populate_frame(s3, "b", "fc", CLASSIC_ID, src) == "uploaded"

        assert cf.pull_frame(s3, "b", "fc", COG_ID, dst) == "hit"
        restored = dst / CLASSIC_ID  # real classic dir name, not the COG id
        assert (restored / "manifest.safe").is_file()
        assert (restored / "measurement" / "iw-vv.tiff").read_bytes() == b"classic-pixels"
        assert not (dst / COG_ID).exists()

    def test_present_recognised_across_classic_cog(self, tmp_path):
        # If the classic SAFE is already on disk, a pull by the COG id is a "present" no-op.
        _make_safe(tmp_path, CLASSIC_ID)
        s3 = FakeS3()
        assert cf.pull_frame(s3, "b", "fc", COG_ID, tmp_path) == "present"

    def test_pull_moves_only_matching_acquisition_dir(self, tmp_path):
        # Defence: a (malformed) cache tar carrying an extra unrelated SAFE must restore
        # ONLY the frame we asked for, never splatter the extra into data_raw.
        other_id = "S1A_IW_GRDH_1SDV_20260618T052000_20260618T052025_065019_0831D4_9999"
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        _make_safe(src, CLASSIC_ID, tiff=b"want")
        _make_safe(src, other_id, tiff=b"unwanted")
        s3 = FakeS3()
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            tar.add(src / CLASSIC_ID, arcname=CLASSIC_ID)
            tar.add(src / other_id, arcname=other_id)  # extra, different acquisition
        s3.store[cf.frame_key("fc", CLASSIC_ID)] = buf.getvalue()

        assert cf.pull_frame(s3, "b", "fc", COG_ID, dst) == "hit"
        assert (dst / CLASSIC_ID / "manifest.safe").is_file()
        assert not (dst / other_id).exists()  # unrelated SAFE ignored

    def test_pull_rejects_invalid_id_before_io(self, tmp_path):
        s3 = FakeS3()
        with pytest.raises(ValueError):
            cf.pull_frame(s3, "b", "fc", "../escape", tmp_path)

    def test_corrupt_tar_without_manifest_raises(self, tmp_path):
        # A cache object that extracts but yields no manifest.safe = integrity fail.
        s3 = FakeS3()
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo(f"{VALID_ID}.SAFE/not-a-manifest")
            data = b"x"
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        s3.store[cf.frame_key("fc", VALID_ID)] = buf.getvalue()
        with pytest.raises(RuntimeError):
            cf.pull_frame(s3, "b", "fc", VALID_ID, tmp_path)

    def test_failed_pull_leaves_data_raw_clean(self, tmp_path):
        # A tar that extracts but yields no manifest must raise AND leave no partial
        # SAFE (nor a staging dir) behind — the atomic stage-and-swap contract.
        s3 = FakeS3()
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo(f"{VALID_ID}.SAFE/measurement/x.tiff")
            data = b"partial-no-manifest"
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        s3.store[cf.frame_key("fc", VALID_ID)] = buf.getvalue()
        with pytest.raises(RuntimeError):
            cf.pull_frame(s3, "b", "fc", VALID_ID, tmp_path)
        assert not (tmp_path / VALID_ID).exists()  # no partial product dir
        assert list(tmp_path.glob(".cache-stage-*")) == []  # staging (under data_raw) cleaned up

    def test_failed_pull_via_pull_frames_degrades_to_miss_and_clean(self, tmp_path):
        s3 = FakeS3()
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo(f"{VALID_ID}.SAFE/stray")
            info.size = 1
            tar.addfile(info, io.BytesIO(b"z"))
        s3.store[cf.frame_key("fc", VALID_ID)] = buf.getvalue()
        results = cf.pull_frames(s3, "b", "fc", [VALID_ID], tmp_path)
        assert results[VALID_ID] == "miss"
        assert not (tmp_path / VALID_ID).exists()


class TestPullFramesParallel:
    def test_mixed_hits_and_miss(self, tmp_path):
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        s3 = FakeS3()
        for pid in (VALID_ID, VALID_ID_2):
            _make_safe(src, pid)
            cf.populate_frame(s3, "b", "fc", pid, src)
        results = cf.pull_frames(s3, "b", "fc", [VALID_ID, VALID_ID_2, S1D_ID], dst, max_workers=3)
        assert results[VALID_ID] == "hit"
        assert results[VALID_ID_2] == "hit"
        assert results[S1D_ID] == "miss"

    def test_invalid_id_fails_batch_fast(self, tmp_path):
        s3 = FakeS3()
        with pytest.raises(ValueError):
            cf.pull_frames(s3, "b", "fc", [VALID_ID, "../evil"], tmp_path)

    def test_pull_failure_degrades_to_miss(self, tmp_path):
        # A download that blows up must not fail the run — it becomes a miss.
        class BoomS3(FakeS3):
            def download_fileobj(self, Bucket, Key, Fileobj):
                raise RuntimeError("transient S3 error")

        s3 = BoomS3()
        s3.store[cf.frame_key("fc", VALID_ID)] = b"present-but-unreadable"
        results = cf.pull_frames(s3, "b", "fc", [VALID_ID], tmp_path)
        assert results[VALID_ID] == "miss"


# --------------------------------------------------------------------------- #
# Populate
# --------------------------------------------------------------------------- #
class TestPopulate:
    def test_uploads_new_frame(self, tmp_path):
        _make_safe(tmp_path, VALID_ID)
        s3 = FakeS3()
        assert cf.populate_frame(s3, "b", "fc", VALID_ID, tmp_path) == "uploaded"
        assert cf.frame_key("fc", VALID_ID) in s3.store

    def test_skips_already_cached(self, tmp_path):
        _make_safe(tmp_path, VALID_ID)
        s3 = FakeS3()
        cf.populate_frame(s3, "b", "fc", VALID_ID, tmp_path)
        assert cf.populate_frame(s3, "b", "fc", VALID_ID, tmp_path) == "cached"
        assert s3.uploads == 1  # not re-uploaded

    def test_absent_when_no_safe_on_disk(self, tmp_path):
        s3 = FakeS3()
        assert cf.populate_frame(s3, "b", "fc", VALID_ID, tmp_path) == "absent"

    def test_size_mismatch_raises(self, tmp_path):
        # Simulate a silent partial upload: head reports a wrong length.
        class LyingS3(FakeS3):
            def head_object(self, Bucket, Key):
                if Key not in self.store:
                    raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
                return {"ContentLength": 1}  # wrong

        _make_safe(tmp_path, VALID_ID)
        with pytest.raises(RuntimeError, match="size mismatch"):
            cf.populate_frame(LyingS3(), "b", "fc", VALID_ID, tmp_path)

    def test_overwrite_reuploads(self, tmp_path):
        _make_safe(tmp_path, VALID_ID)
        s3 = FakeS3()
        cf.populate_frame(s3, "b", "fc", VALID_ID, tmp_path)
        assert cf.populate_frame(s3, "b", "fc", VALID_ID, tmp_path, overwrite=True) == "uploaded"
        assert s3.uploads == 2


class TestFetchOnceAcrossSharingTiles:
    """End-to-end proof of the optimisation: across frame-sharing tiles processed
    sequentially (the warm/steady-state case), each distinct frame is fetched from
    CDSE exactly once instead of once per tile (~N x egress drop). Exercises the
    real pull -> miss -> download -> populate -> next-tile-hit path.

    NB: sequential = the warm steady state. The lazy-populate concurrency window
    (>=K simultaneous first-touches double-fetch a frame) is T11's concern, not
    modelled here.
    """

    def test_distinct_frames_fetched_once(self, tmp_path):
        s3 = FakeS3()
        ids = {
            f"F{i}": f"S1A_IW_GRDH_1SDV_2024010{i}T060000_2024010{i}T060025_05200{i}_06470{i}_AB0{i}"
            for i in range(5)
        }
        # 4 contiguous tiles, each sharing a frame with its neighbour (a block).
        tiles = {"T1": ["F0", "F1"], "T2": ["F1", "F2"], "T3": ["F2", "F3"], "T4": ["F3", "F4"]}
        cdse_fetches: list[str] = []

        for tile, frames in tiles.items():
            data_raw = tmp_path / tile
            data_raw.mkdir()
            prod_ids = [ids[f] for f in frames]
            # pre-step: pull what the cache already has
            results = cf.pull_frames(s3, "b", "fc", prod_ids, data_raw)
            # s1processor downloads the misses from CDSE (modelled: write the SAFE)
            for pid, status in results.items():
                if status == "miss":
                    cdse_fetches.append(pid)
                    _make_safe(data_raw, pid)
            # post-step: populate freshly-downloaded frames for the next tile
            cf.populate_frames(s3, "b", "fc", prod_ids, data_raw)

        total_pairs = sum(len(v) for v in tiles.values())  # 8 fetches without a cache
        assert sorted(cdse_fetches) == sorted(ids.values())  # all 5 frames, no dups
        assert len(cdse_fetches) == 5
        assert len(cdse_fetches) < total_pairs == 8  # egress dropped 8 -> 5


class TestDiscover:
    def test_lists_valid_safes_skips_others(self, tmp_path):
        _make_safe(tmp_path, VALID_ID)
        _make_safe(tmp_path, VALID_ID_2)
        (tmp_path / "not-a-frame").mkdir()  # ignored
        (tmp_path / VALID_ID).joinpath("incomplete.SAFE").mkdir()  # no manifest -> ignored elsewhere
        found = cf.discover_downloaded_frames(tmp_path)
        assert set(found) == {VALID_ID, VALID_ID_2}

    def test_empty_dir(self, tmp_path):
        assert cf.discover_downloaded_frames(tmp_path / "missing") == []


# --------------------------------------------------------------------------- #
# Safe extraction (defence against malicious/corrupt cache objects)
# --------------------------------------------------------------------------- #
class TestSafeExtract:
    def test_rejects_path_traversal_member(self, tmp_path):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo("../escape.txt")
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))
        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r") as tar, pytest.raises(ValueError, match="escapes"):
            cf._safe_extract(tar, tmp_path)

    def test_rejects_symlink_member(self, tmp_path):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo("link")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            tar.addfile(info)
        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r") as tar, pytest.raises(ValueError):
            cf._safe_extract(tar, tmp_path)

    def test_rejects_hardlink_member(self, tmp_path):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo("hard")
            info.type = tarfile.LNKTYPE
            info.linkname = "manifest.safe"
            tar.addfile(info)
        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r") as tar, pytest.raises(ValueError):
            cf._safe_extract(tar, tmp_path)

    def test_rejects_absolute_path_member(self, tmp_path):
        # tarfile may strip a leading "/", so use an absolute-looking traversal that
        # escapes regardless: the resolve()+containment check must reject it.
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            info = tarfile.TarInfo("../../../../etc/evil")
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))
        buf.seek(0)
        with tarfile.open(fileobj=buf, mode="r") as tar, pytest.raises(ValueError, match="escapes"):
            cf._safe_extract(tar, tmp_path)

    def test_extracts_normal_tree(self, tmp_path):
        src = tmp_path / "src"
        _make_safe(src, VALID_ID)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            tar.add(src / VALID_ID, arcname=VALID_ID)
        buf.seek(0)
        dest = tmp_path / "dest"
        dest.mkdir()
        with tarfile.open(fileobj=buf, mode="r") as tar:
            cf._safe_extract(tar, dest)
        assert (dest / VALID_ID / "manifest.safe").is_file()


# --------------------------------------------------------------------------- #
# Eviction / retention (T9)
# --------------------------------------------------------------------------- #
from datetime import date  # noqa: E402


def _seed_cache(s3, prefix, *prod_ids):
    for pid in prod_ids:
        s3.store[cf.frame_key(prefix, pid)] = b"tarbytes"


class TestAcqDate:
    def test_parses_start_date(self):
        # VALID_ID acquisition start = 2024-01-01
        assert cf._acq_date(VALID_ID) == date(2024, 1, 1)
        assert cf._acq_date(VALID_ID_2) == date(2024, 1, 13)

    def test_raises_on_no_timestamp(self):
        with pytest.raises(ValueError):
            cf._acq_date("S1A_IW_GRDH_no_timestamp_here_xxxxxxxxxx")


class TestListCachedFrames:
    def test_lists_tar_keys_only(self, tmp_path):
        s3 = FakeS3()
        _seed_cache(s3, "fc", VALID_ID, VALID_ID_2)
        s3.store["fc/not-a-tar.txt"] = b"x"          # ignored (not .tar)
        s3.store["fc/garbage.tar"] = b"x"            # ignored (invalid id)
        # cache objects are keyed by acquisition (the tar name), so list returns those.
        assert sorted(cf.list_cached_frames(s3, "b", "fc")) == sorted(
            [cf.acquisition_key(VALID_ID), cf.acquisition_key(VALID_ID_2)]
        )

    def test_paginates(self):
        class PagedS3(FakeS3):
            def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None):
                keys = sorted(k for k in self.store if k.startswith(Prefix))
                start = int(ContinuationToken or 0)
                page = keys[start:start + 1]
                more = start + 1 < len(keys)
                return {"Contents": [{"Key": k} for k in page],
                        "IsTruncated": more, "NextContinuationToken": str(start + 1)}
        s3 = PagedS3()
        _seed_cache(s3, "fc", VALID_ID, VALID_ID_2, S1D_ID)
        assert sorted(cf.list_cached_frames(s3, "b", "fc")) == sorted(
            [cf.acquisition_key(i) for i in (VALID_ID, VALID_ID_2, S1D_ID)]
        )


class TestEvictStale:
    # cache holds frames acquired 2024-01-01, 2024-01-02, 2024-01-13
    def _seeded(self):
        s3 = FakeS3()
        _seed_cache(s3, "fc", VALID_ID, S1D_ID, VALID_ID_2)
        return s3

    def test_removes_only_stale(self):
        s3 = self._seeded()
        # today=2024-01-20, keep 10 days -> cutoff 2024-01-10: 01-01 & 01-02 stale, 01-13 kept
        res = cf.evict_stale(s3, "b", "fc", keep_days=10, today=date(2024, 1, 20))
        assert res["stale"] == sorted([cf.acquisition_key(VALID_ID), cf.acquisition_key(S1D_ID)])
        assert res["removed"] == sorted([cf.acquisition_key(VALID_ID), cf.acquisition_key(S1D_ID)])
        assert res["kept"] == 1
        assert cf.frame_key("fc", VALID_ID) not in s3.store      # actually deleted
        assert cf.frame_key("fc", VALID_ID_2) in s3.store        # in-window retained

    def test_dry_run_deletes_nothing(self):
        s3 = self._seeded()
        before = dict(s3.store)
        res = cf.evict_stale(s3, "b", "fc", keep_days=10, today=date(2024, 1, 20), dry_run=True)
        assert res["stale"] == sorted([cf.acquisition_key(VALID_ID), cf.acquisition_key(S1D_ID)])
        assert res["removed"] == []
        assert s3.store == before                            # nothing deleted

    def test_keeps_everything_within_window(self):
        s3 = self._seeded()
        res = cf.evict_stale(s3, "b", "fc", keep_days=3650, today=date(2024, 1, 20))
        assert res["stale"] == []
        assert res["kept"] == 3

    def test_unparseable_acq_date_is_kept(self):
        # An on-disk key that passes id validation but whose date can't be parsed must
        # never be deleted (conservative — same principle as pull/T1).
        s3 = FakeS3()
        # valid grammar + >=8 fields (so it forms an acquisition key), but no parseable
        # YYYYMMDDThhmmss so its acquisition date can't be derived.
        weird = "S1A_IW_GRDH_1SDV_NODATE_NOSTOP_052000_064700_ABCD"
        assert cf.validate_prod_id(weird) == weird  # passes the id grammar
        s3.store[cf.frame_key("fc", weird)] = b"x"
        res = cf.evict_stale(s3, "b", "fc", keep_days=1, today=date(2024, 1, 20))
        assert res["stale"] == []
        assert res["kept"] == 1
        assert cf.frame_key("fc", weird) in s3.store


class TestEvictCLI:
    def test_evict_does_not_require_data_raw(self, capsys):
        s3 = FakeS3()
        _seed_cache(s3, "fc", VALID_ID, VALID_ID_2)
        import unittest.mock as mock
        with mock.patch.object(cf, "make_s3_client", return_value=s3):
            rc = cf.main(["evict", "--bucket", "b", "--prefix", "fc",
                          "--keep-days", "10", "--today", "2024-01-20", "--dry-run"])
        assert rc == 0
        out = capsys.readouterr().out
        assert cf.acquisition_key(VALID_ID) in out          # stale (2024-01-01) printed
        assert cf.acquisition_key(VALID_ID_2) not in out    # in-window not printed

    def test_pull_still_requires_data_raw(self):
        s3 = FakeS3()
        import unittest.mock as mock
        with mock.patch.object(cf, "make_s3_client", return_value=s3), pytest.raises(SystemExit):
            cf.main(["pull", "--bucket", "b", "--frames", VALID_ID])
