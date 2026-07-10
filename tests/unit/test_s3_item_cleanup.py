"""Unit tests for scripts/s3_item_cleanup.py.

These cover the shared S3-deletion helpers extracted from
operator-tools/manage_item.py (coordination#183, Task 1):
- URL extraction from STAC item assets
- Zarr-prefix expansion + 200-key batch deletion
- NoSuchKey-as-deleted tolerance
- object counting for validation

boto3 is fully mocked — no network, no AWS.
"""

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from s3_item_cleanup import (
    DEFAULT_RETENTION_DAYS,
    EXPIRES_TS_FORMAT,
    count_s3_objects_for_item,
    delete_s3_objects_for_item,
    env_int,
    extract_s3_urls_from_item,
    format_expires,
    load_exclude_ids,
    parse_stac_timestamp,
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

    deleted, failed = delete_s3_objects_for_item(client, {f"s3://{BUCKET}/item/data.zarr/B02/0.0"})

    assert (deleted, failed) == (3, 0)
    # Paginate was scoped to the zarr root prefix, not the single chunk.
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket=BUCKET, Prefix="item/data.zarr/"
    )


def test_delete_batches_in_chunks_of_200() -> None:
    keys = [f"item/data.zarr/chunk/{i}" for i in range(250)]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)
    client.delete_objects.return_value = {"Deleted": [], "Errors": []}

    delete_s3_objects_for_item(client, {f"s3://{BUCKET}/item/data.zarr/x"})

    batch_sizes = [
        len(kwargs["Delete"]["Objects"]) for _, kwargs in client.delete_objects.call_args_list
    ]
    assert batch_sizes == [200, 50]


def test_delete_counts_nosuchkey_as_deleted() -> None:
    keys = ["item/data.zarr/a", "item/data.zarr/b"]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)
    client.delete_objects.return_value = {
        "Deleted": [{"Key": "item/data.zarr/a"}],
        "Errors": [{"Key": "item/data.zarr/b", "Code": "NoSuchKey"}],
    }

    deleted, failed = delete_s3_objects_for_item(client, {f"s3://{BUCKET}/item/data.zarr/x"})

    assert (deleted, failed) == (2, 0)


def test_delete_counts_other_errors_as_failed() -> None:
    keys = ["item/data.zarr/a", "item/data.zarr/b"]
    client = MagicMock()
    client.get_paginator.return_value = _paginator_returning(keys)
    client.delete_objects.return_value = {
        "Deleted": [{"Key": "item/data.zarr/a"}],
        "Errors": [{"Key": "item/data.zarr/b", "Code": "AccessDenied"}],
    }

    deleted, failed = delete_s3_objects_for_item(client, {f"s3://{BUCKET}/item/data.zarr/x"})

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


def test_consumers_share_one_format_helper() -> None:
    """Regression guard for the load-bearing format: register and cleanup must
    use the shared helper, not a private copy (review finding 4)."""
    import cleanup_expired_items
    import register_v1

    assert register_v1.format_expires is format_expires
    assert cleanup_expired_items.format_expires is format_expires
    assert cleanup_expired_items.parse_stac_timestamp is parse_stac_timestamp
