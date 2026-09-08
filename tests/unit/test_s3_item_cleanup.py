"""Unit tests for scripts/s3_item_cleanup.py.

These cover the shared S3-deletion helpers extracted from
operator-tools/manage_item.py (coordination#183, Task 1):
- URL extraction from STAC item assets
- Zarr-prefix expansion + 1000-key batch deletion
- NoSuchKey-as-deleted tolerance
- object counting for validation

boto3 is fully mocked — no network, no AWS.
"""

import importlib.util
import logging
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from s3_item_cleanup import (
    BAKED_EXCLUDE_FILE,
    DEFAULT_RETENTION_DAYS,
    EXPIRES_TS_FORMAT,
    UnconfinedS3URLError,
    assert_urls_confined,
    check_urls_confined,
    count_s3_objects_for_item,
    delete_s3_objects_for_item,
    env_int,
    extract_s3_urls_from_item,
    format_expires,
    load_exclude_ids,
    parse_s3_prefix,
    parse_stac_timestamp,
    resolve_exclude_ids,
)

BUCKET = "esa-zarr-sentinel-explorer-fra"


# === Module contract ===


def test_default_retention_days_is_183() -> None:
    """Single source of truth for retention shared by register + backfill."""
    assert DEFAULT_RETENTION_DAYS == 183


def test_module_does_not_depend_on_click() -> None:
    """scripts/ is baked into the pipeline image without click; batch
    progress must go through logging, not click.progressbar."""
    spec = importlib.util.find_spec("s3_item_cleanup")
    assert spec is not None and spec.origin is not None
    source = Path(spec.origin).read_text()
    assert "import click" not in source
    assert "click.progressbar" not in source


# === extract_s3_urls_from_item ===


def test_extract_prefers_alternate_s3_href() -> None:
    item = {
        "assets": {
            "data": {
                "href": "https://example.com/data.zarr",
                "alternate": {"s3": {"href": f"s3://{BUCKET}/item/data.zarr/"}},
            }
        }
    }
    assert extract_s3_urls_from_item(item) == {f"s3://{BUCKET}/item/data.zarr/"}


def test_extract_falls_back_to_main_href_when_s3() -> None:
    item = {"assets": {"data": {"href": f"s3://{BUCKET}/item/file.tif"}}}
    assert extract_s3_urls_from_item(item) == {f"s3://{BUCKET}/item/file.tif"}


def test_extract_skips_thumbnail_and_non_s3_assets() -> None:
    item = {
        "assets": {
            "thumb": {
                "href": f"s3://{BUCKET}/item/thumb.png",
                "roles": ["thumbnail"],
            },
            "https": {"href": "https://example.com/data.tif"},
        }
    }
    assert extract_s3_urls_from_item(item) == set()


# === delete_s3_objects_for_item ===


def _paginator_returning(keys: list[str]) -> MagicMock:
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    return paginator


def test_delete_expands_zarr_prefix_and_deletes_listed_objects() -> None:
    """A .zarr/ URL must be expanded to every object under the store root."""
    zarr_keys = [
        "item/data.zarr/.zmetadata",
        "item/data.zarr/B02/0.0",
        "item/data.zarr/B02/0.1",
    ]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(zarr_keys)
    client.delete_objects.return_value = {
        "Deleted": [{"Key": k} for k in zarr_keys],
        "Errors": [],
    }

    deleted, failed = delete_s3_objects_for_item(
        client, {f"s3://{BUCKET}/item/data.zarr/B02/0.0"}, confinement=[(BUCKET, "item/")]
    )

    assert (deleted, failed) == (3, 0)
    # Paginate was scoped to the zarr root prefix, not the single chunk.
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket=BUCKET, Prefix="item/data.zarr/"
    )


def test_delete_batches_in_chunks_of_1000() -> None:
    keys = [f"item/data.zarr/chunk/{i}" for i in range(1200)]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)
    client.delete_objects.return_value = {"Deleted": [], "Errors": []}

    delete_s3_objects_for_item(
        client, {f"s3://{BUCKET}/item/data.zarr/x"}, confinement=[(BUCKET, "item/")]
    )

    batch_sizes = [
        len(kwargs["Delete"]["Objects"]) for _, kwargs in client.delete_objects.call_args_list
    ]
    assert batch_sizes == [1000, 200]  # S3's max keys per delete_objects call


def test_delete_counts_nosuchkey_as_deleted() -> None:
    keys = ["item/data.zarr/a", "item/data.zarr/b"]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)
    client.delete_objects.return_value = {
        "Deleted": [{"Key": "item/data.zarr/a"}],
        "Errors": [{"Key": "item/data.zarr/b", "Code": "NoSuchKey"}],
    }

    deleted, failed = delete_s3_objects_for_item(
        client, {f"s3://{BUCKET}/item/data.zarr/x"}, confinement=[(BUCKET, "item/")]
    )

    assert (deleted, failed) == (2, 0)


def test_delete_counts_other_errors_as_failed() -> None:
    keys = ["item/data.zarr/a", "item/data.zarr/b"]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)
    client.delete_objects.return_value = {
        "Deleted": [{"Key": "item/data.zarr/a"}],
        "Errors": [{"Key": "item/data.zarr/b", "Code": "AccessDenied"}],
    }

    deleted, failed = delete_s3_objects_for_item(
        client, {f"s3://{BUCKET}/item/data.zarr/x"}, confinement=[(BUCKET, "item/")]
    )

    assert (deleted, failed) == (1, 1)


# === count_s3_objects_for_item ===


def test_count_expands_zarr_prefix() -> None:
    keys = ["item/data.zarr/a", "item/data.zarr/b", "item/data.zarr/c"]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)

    count = count_s3_objects_for_item(client, {f"s3://{BUCKET}/item/data.zarr/x"})

    assert count == 3


def test_count_individual_file_via_head_object() -> None:
    client = MagicMock()
    client.head_object.return_value = {}

    count = count_s3_objects_for_item(client, {f"s3://{BUCKET}/item/file.tif"})

    assert count == 1
    client.head_object.assert_called_once_with(Bucket=BUCKET, Key="item/file.tif")


def test_count_returns_zero_when_head_object_missing() -> None:
    from botocore.exceptions import ClientError

    client = MagicMock()
    client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

    count = count_s3_objects_for_item(client, {f"s3://{BUCKET}/item/gone.tif"})

    assert count == 0


# === Shared expires helpers (Task 1 consolidation, review findings 4/5/6) ===


class TestTimestampHelpers:
    def test_format_expires_is_zero_padded_utc_z(self) -> None:
        assert format_expires(datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)) == "2025-01-02T03:04:05Z"

    def test_parse_round_trips_the_canonical_format(self) -> None:
        dt = datetime(2025, 7, 10, 12, 30, 0, tzinfo=UTC)
        assert parse_stac_timestamp(format_expires(dt)) == dt

    def test_parse_accepts_offset_form(self) -> None:
        assert parse_stac_timestamp("2025-01-01T00:00:00+00:00") == datetime(2025, 1, 1, tzinfo=UTC)

    def test_canonical_format_constant_is_stable(self) -> None:
        # Load-bearing: pgstac string-compares expires, so this must not change.
        assert EXPIRES_TS_FORMAT == "%Y-%m-%dT%H:%M:%SZ"


class TestEnvInt:
    def test_unset_returns_default(self, monkeypatch: object) -> None:
        monkeypatch.delenv("X_ENVINT_PROBE", raising=False)  # type: ignore[attr-defined]
        assert env_int("X_ENVINT_PROBE", 183) == 183

    def test_empty_string_returns_default(self, monkeypatch: object) -> None:
        monkeypatch.setenv("X_ENVINT_PROBE", "")  # type: ignore[attr-defined]
        assert env_int("X_ENVINT_PROBE", 183) == 183

    def test_zero_is_honoured(self, monkeypatch: object) -> None:
        monkeypatch.setenv("X_ENVINT_PROBE", "0")  # type: ignore[attr-defined]
        assert env_int("X_ENVINT_PROBE", 183) == 0

    def test_value_is_parsed(self, monkeypatch: object) -> None:
        monkeypatch.setenv("X_ENVINT_PROBE", "30")  # type: ignore[attr-defined]
        assert env_int("X_ENVINT_PROBE", 183) == 30


class TestLoadExcludeIds:
    def test_none_is_empty(self) -> None:
        assert load_exclude_ids(None) == set()

    def test_reads_newline_ids_ignoring_comments_and_blanks(self, tmp_path: Path) -> None:
        f = tmp_path / "exclude.txt"
        f.write_text("item-a\n# a comment\n\nitem-b\n")
        assert load_exclude_ids(str(f)) == {"item-a", "item-b"}


class TestResolveExcludeIds:
    """resolve_exclude_ids makes demo protection unconditional: with nothing
    configured it falls back to the baked demo denylist, so a workflow that
    forgets EXPIRES_EXCLUDE_FILE cannot leave demo scenes unprotected
    (coordination#183). Precedence: explicit path > env > baked file."""

    def test_baked_file_is_present_and_non_empty(self) -> None:
        # The fallback is only real protection if the file is actually there. This
        # guards the source tree; `docker/Dockerfile`'s `COPY scripts/ /app/scripts/`
        # is what carries it into the image (.dockerignore excludes *.md, not *.txt).
        assert BAKED_EXCLUDE_FILE.exists()
        assert load_exclude_ids(str(BAKED_EXCLUDE_FILE))  # non-empty

    def test_unset_falls_back_to_baked(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        assert resolve_exclude_ids() == load_exclude_ids(str(BAKED_EXCLUDE_FILE))

    def test_env_overrides_baked(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        f = tmp_path / "env.txt"
        f.write_text("env-a\nenv-b\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(f))
        assert resolve_exclude_ids() == {"env-a", "env-b"}

    def test_explicit_path_overrides_env_and_baked(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        env_f = tmp_path / "env.txt"
        env_f.write_text("env-a\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(env_f))
        explicit = tmp_path / "explicit.txt"
        explicit.write_text("explicit-a\nexplicit-b\n")
        assert resolve_exclude_ids(str(explicit)) == {"explicit-a", "explicit-b"}

    def test_missing_baked_file_warns_instead_of_silently_unprotecting(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Losing the baked list reverts to the opt-in behaviour this function removes.
        # That must never happen quietly — an operator has to be able to see it.
        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        monkeypatch.setattr("s3_item_cleanup.BAKED_EXCLUDE_FILE", tmp_path / "absent.txt")

        with caplog.at_level(logging.WARNING, logger="s3_item_cleanup"):
            assert resolve_exclude_ids() == set()

        assert "Demo protection is OFF" in caplog.text


def test_consumers_share_one_format_helper() -> None:
    """Regression guard for the load-bearing format: register and cleanup must
    use the shared helper, not a private copy (review finding 4)."""
    import cleanup_expired_items
    import register_v1

    assert register_v1.format_expires is format_expires
    assert cleanup_expired_items.format_expires is format_expires
    assert cleanup_expired_items.parse_stac_timestamp is parse_stac_timestamp


# === Delete confinement (prefix guard) ===

STAGING = (BUCKET, "tests-output/sentinel-2-l2a-staging/")
PROD = (BUCKET, "tests-output/sentinel-2-l2a/")


def test_parse_s3_prefix_normalises_trailing_slash() -> None:
    assert parse_s3_prefix(f"s3://{BUCKET}/tests-output/foo") == (BUCKET, "tests-output/foo/")
    assert parse_s3_prefix(f"s3://{BUCKET}/tests-output/foo/") == (BUCKET, "tests-output/foo/")


def test_parse_s3_prefix_bare_bucket_means_whole_bucket() -> None:
    assert parse_s3_prefix(f"s3://{BUCKET}") == (BUCKET, "")
    assert parse_s3_prefix(f"s3://{BUCKET}/") == (BUCKET, "")


@pytest.mark.parametrize("spec", ["https://example.com/x", "s3://", "/tests-output/foo"])
def test_parse_s3_prefix_rejects_non_s3(spec: str) -> None:
    with pytest.raises(ValueError):
        parse_s3_prefix(spec)


def test_staging_confinement_does_not_match_adjacent_prod_prefix() -> None:
    """The bug this guard exists for: `sentinel-2-l2a` is a string prefix of
    `sentinel-2-l2a-staging`, and both live in one un-versioned bucket."""
    prod_url = f"s3://{BUCKET}/tests-output/sentinel-2-l2a/item/data.zarr/B02/0.0"
    assert check_urls_confined({prod_url}, [STAGING]) == [(prod_url, "outside_confinement")]


def test_prod_confinement_does_not_match_staging_prefix() -> None:
    """And the converse — confinement must not leak across the shared stem."""
    staging_url = f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/item/data.zarr/B02/0.0"
    assert check_urls_confined({staging_url}, [PROD]) == [(staging_url, "outside_confinement")]


def test_in_bounds_urls_pass() -> None:
    urls = {
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/a/data.zarr/B02/0.0",
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/b/thumb.png",
    }
    assert check_urls_confined(urls, [STAGING]) == []


def test_other_bucket_is_out_of_bounds() -> None:
    url = "s3://some-other-bucket/tests-output/sentinel-2-l2a-staging/a/data.zarr/x"
    assert check_urls_confined({url}, [STAGING]) == [(url, "outside_confinement")]


def test_empty_confinement_rejects_everything() -> None:
    """Fail closed: an unconfigured caller deletes nothing, not everything."""
    url = f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/a/data.zarr/x"
    assert check_urls_confined({url}, []) == [(url, "outside_confinement")]


def test_bare_zarr_href_is_rejected_as_orphaning() -> None:
    """`.zarr` with no trailing slash partitions as a single key: the delete
    removes ~nothing, validation sees 0 remaining, and the STAC item is dropped
    while the store survives."""
    url = f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/a/data.zarr"
    assert check_urls_confined({url}, [STAGING]) == [(url, "bare_zarr_store")]


def test_multiple_confinements_are_a_union() -> None:
    urls = {
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/a/data.zarr/x",
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a/b/data.zarr/y",
    }
    assert check_urls_confined(urls, [STAGING, PROD]) == []


def test_assert_urls_confined_raises_with_all_violations() -> None:
    urls = {
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a/one/data.zarr/x",
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a/two/data.zarr/y",
    }
    with pytest.raises(UnconfinedS3URLError) as exc:
        assert_urls_confined(urls, [STAGING], item_id="ITEM_A")
    assert len(exc.value.violations) == 2
    assert exc.value.item_id == "ITEM_A"


def test_delete_refuses_before_issuing_any_call() -> None:
    """The guard runs before _collect_keys_by_bucket, so a rogue href in an
    otherwise-valid item cannot leave a half-deleted store behind."""
    client = MagicMock()
    urls = {
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a-staging/ok/data.zarr/x",
        f"s3://{BUCKET}/tests-output/sentinel-2-l2a/rogue/data.zarr/y",
    }
    with pytest.raises(UnconfinedS3URLError):
        delete_s3_objects_for_item(client, urls, confinement=[STAGING])
    client.get_paginator.assert_not_called()
    client.delete_objects.assert_not_called()


def test_delete_proceeds_when_confined() -> None:
    client = MagicMock()
    paginator = MagicMock()
    key = "tests-output/sentinel-2-l2a-staging/a/data.zarr/B02/0.0"
    paginator.paginate.return_value = [{"Contents": [{"Key": key}]}]
    client.get_paginator.return_value = paginator
    client.delete_objects.return_value = {"Deleted": [{"Key": key}]}

    deleted, failed = delete_s3_objects_for_item(
        client, {f"s3://{BUCKET}/{key}"}, confinement=[STAGING]
    )
    assert (deleted, failed) == (1, 0)


def test_delete_requires_confinement_keyword() -> None:
    """No positional fallback and no default — the bound is part of the call."""
    with pytest.raises(TypeError):
        delete_s3_objects_for_item(MagicMock(), set())  # type: ignore[call-arg]
