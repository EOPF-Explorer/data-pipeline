"""Unit tests for the migrate_catalog package."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _migrate_catalog.history import load_history, record_run, was_migration_run
from _migrate_catalog.migrations.add_xyz_link import add_xyz_link
from _migrate_catalog.migrations.align_visualization_links import align_visualization_links
from _migrate_catalog.migrations.fix_url_encoding import fix_url_encoding
from _migrate_catalog.migrations.fix_zarr_media_type import fix_zarr_media_type
from _migrate_catalog.runner import STACMigrationRunner, compose_migrations
from _migrate_catalog.types import MigrationResult

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "migrate_catalog"


@pytest.fixture
def item_with_plus_urls():
    with open(FIXTURES_DIR / "stac_item_with_plus_urls.json") as f:
        return json.load(f)


@pytest.fixture
def item_with_wrong_media_type():
    with open(FIXTURES_DIR / "stac_item_with_wrong_media_type.json") as f:
        return json.load(f)


@pytest.fixture
def item_clean():
    with open(FIXTURES_DIR / "stac_item_clean.json") as f:
        return json.load(f)


@pytest.fixture
def item_with_v2_zarr():
    with open(FIXTURES_DIR / "stac_item_with_v2_zarr.json") as f:
        return json.load(f)


@pytest.fixture
def migration_result():
    return MigrationResult(
        migration_name="fix_zarr_media_type",
        collection_id="sentinel-2-l2a",
        started_at="2026-03-12T10:00:00+00:00",
        completed_at="2026-03-12T10:05:00+00:00",
        items_processed=100,
        items_modified=50,
        items_skipped=50,
        items_failed=0,
        dry_run=False,
        errors=[],
    )


class TestFixUrlEncoding:
    def test_replaces_plus_in_asset_query_string(self, item_with_plus_urls):
        result = fix_url_encoding(item_with_plus_urls)
        assert result is not None
        query = result["assets"]["thumbnail"]["href"].split("?")[1]
        assert "+" not in query
        assert "%20" in query

    def test_replaces_plus_in_link_href(self, item_with_plus_urls):
        result = fix_url_encoding(item_with_plus_urls)
        assert result is not None
        preview_link = next(link for link in result["links"] if link["rel"] == "preview")
        query = preview_link["href"].split("?")[1]
        assert "+" not in query
        assert "%20" in query

    def test_does_not_touch_path_segment(self, item_with_plus_urls):
        # The data asset has no query string — href must be unchanged
        result = fix_url_encoding(item_with_plus_urls)
        assert result is not None
        assert result["assets"]["data"]["href"] == item_with_plus_urls["assets"]["data"]["href"]

    def test_returns_none_when_no_plus_in_query(self, item_clean):
        assert fix_url_encoding(item_clean) is None

    def test_does_not_mutate_input(self, item_with_plus_urls):
        original_href = item_with_plus_urls["assets"]["thumbnail"]["href"]
        fix_url_encoding(item_with_plus_urls)
        assert item_with_plus_urls["assets"]["thumbnail"]["href"] == original_href

    def test_idempotent(self, item_with_plus_urls):
        result1 = fix_url_encoding(item_with_plus_urls)
        assert result1 is not None
        result2 = fix_url_encoding(result1)
        assert result2 is None


class TestFixZarrMediaType:
    def test_replaces_vnd_plus_zarr_with_version_3(self, item_with_wrong_media_type):
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert result["assets"]["SR_10m"]["type"] == "application/vnd.zarr; version=3"

    def test_replaces_vnd_plus_zarr_with_existing_version_suffix(self, item_with_wrong_media_type):
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert result["assets"]["SR_20m"]["type"] == "application/vnd.zarr; version=3"

    def test_removes_zipped_product_asset(self, item_with_wrong_media_type):
        assert "zipped_product" in item_with_wrong_media_type["assets"]
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert "zipped_product" not in result["assets"]

    def test_does_not_modify_non_zarr_assets(self, item_with_wrong_media_type):
        result = fix_zarr_media_type(item_with_wrong_media_type)
        assert result is not None
        assert result["assets"]["thumbnail"]["type"] == "image/png"

    def test_returns_none_when_already_correct(self, item_clean):
        assert fix_zarr_media_type(item_clean) is None

    def test_does_not_mutate_input(self, item_with_wrong_media_type):
        original_type = item_with_wrong_media_type["assets"]["SR_10m"]["type"]
        fix_zarr_media_type(item_with_wrong_media_type)
        assert item_with_wrong_media_type["assets"]["SR_10m"]["type"] == original_type

    def test_replaces_version2_with_profile(self, item_with_v2_zarr):
        result = fix_zarr_media_type(item_with_v2_zarr)
        assert result is not None
        assert (
            result["assets"]["reflectance"]["type"]
            == "application/vnd.zarr; version=3; profile=multiscales"
        )

    def test_replaces_version2_without_profile(self, item_with_v2_zarr):
        result = fix_zarr_media_type(item_with_v2_zarr)
        assert result is not None
        assert result["assets"]["SR_20m"]["type"] == "application/vnd.zarr; version=3"

    def test_fixes_vnd_plus_zarr_and_version2_together(self, item_with_v2_zarr):
        result = fix_zarr_media_type(item_with_v2_zarr)
        assert result is not None
        assert (
            result["assets"]["SR_60m"]["type"]
            == "application/vnd.zarr; version=3; profile=multiscales"
        )

    def test_fixes_vnd_plus_zarr_version2_without_profile(self):
        item = {
            "id": "test",
            "assets": {
                "data": {
                    "href": "https://example.com/data.zarr",
                    "type": "application/vnd+zarr; version=2",
                }
            },
            "links": [],
        }
        result = fix_zarr_media_type(item)
        assert result is not None
        assert result["assets"]["data"]["type"] == "application/vnd.zarr; version=3"

    def test_does_not_mutate_v2_input(self, item_with_v2_zarr):
        original_type = item_with_v2_zarr["assets"]["reflectance"]["type"]
        fix_zarr_media_type(item_with_v2_zarr)
        assert item_with_v2_zarr["assets"]["reflectance"]["type"] == original_type

    def test_idempotent(self, item_with_wrong_media_type):
        result1 = fix_zarr_media_type(item_with_wrong_media_type)
        assert result1 is not None
        result2 = fix_zarr_media_type(result1)
        assert result2 is None

    def test_idempotent_v2(self, item_with_v2_zarr):
        result1 = fix_zarr_media_type(item_with_v2_zarr)
        assert result1 is not None
        result2 = fix_zarr_media_type(result1)
        assert result2 is None


_TJ_BASE = (
    "https://api.example.com/raster/collections/sentinel-1-grd-rtc-staging"
    "/items/s1-rtc-31TCG/WebMercatorQuad/tilejson.json"
)
_TJ_QUERY = "expression=%2Fdescending%3Avv&rescale=0.0%2C0.2&sel=time=2026-06-07T05%3A52%3A48"
_TJ_HREF = f"{_TJ_BASE}?{_TJ_QUERY}"


def _item_with_tilejson(*, viewer_title=None, render_title=None, extra_links=None) -> dict:
    """A STAC item carrying a well-formed tilejson link (+ optional viewer/renders), no xyz yet."""
    links = [
        {"rel": "self", "href": "https://api.example.com/stac/.../s1-rtc-31TCG"},
        {"rel": "tilejson", "type": "application/json", "href": _TJ_HREF, "title": "tilejson"},
    ]
    if viewer_title is not None:
        links.append(
            {
                "rel": "viewer",
                "type": "text/html",
                "href": f"{_TJ_BASE.replace('tilejson.json', 'map.html')}?{_TJ_QUERY}",
                "title": viewer_title,
            }
        )
    links.extend(extra_links or [])
    item: dict = {"id": "s1-rtc-31TCG", "links": links, "assets": {}}
    if render_title is not None:
        item["properties"] = {"renders": {"rgb": {"title": render_title}}}
    return item


class TestAddXyzLink:
    def test_adds_xyz_after_tilejson(self):
        item = _item_with_tilejson()
        result = add_xyz_link(item)
        assert result is not None
        rels = [lk["rel"] for lk in result["links"]]
        assert rels.count("xyz") == 1
        assert rels.index("xyz") == rels.index("tilejson") + 1
        xyz = next(lk for lk in result["links"] if lk["rel"] == "xyz")
        assert xyz["type"] == "image/png"
        assert "/tiles/WebMercatorQuad/{z}/{x}/{y}.png?" in xyz["href"]

    def test_query_preserved_including_encoded_sel_time(self):
        result = add_xyz_link(_item_with_tilejson())
        assert result is not None
        xyz = next(lk for lk in result["links"] if lk["rel"] == "xyz")
        # query byte-identical to tilejson's (incl. percent-encoded sel=time colons)
        assert xyz["href"].split("?", 1)[1] == _TJ_QUERY

    def test_title_from_viewer_first(self):
        # live per-acq items carry both a viewer title and a renders title — viewer wins for parity
        result = add_xyz_link(
            _item_with_tilejson(
                viewer_title="Sentinel-1 GRD RGB composite", render_title="VV, VH, VV/VH composite"
            )
        )
        assert result is not None
        xyz = next(lk for lk in result["links"] if lk["rel"] == "xyz")
        assert xyz["title"] == "Sentinel-1 GRD RGB composite"

    def test_title_from_renders_when_no_viewer(self):
        result = add_xyz_link(_item_with_tilejson(render_title="VV, VH, VV/VH composite"))
        assert result is not None
        xyz = next(lk for lk in result["links"] if lk["rel"] == "xyz")
        assert xyz["title"] == "VV, VH, VV/VH composite"

    def test_title_fallback_when_no_viewer_or_renders(self):
        result = add_xyz_link(_item_with_tilejson())
        assert result is not None
        xyz = next(lk for lk in result["links"] if lk["rel"] == "xyz")
        assert xyz["title"] == "XYZ tile template"

    def test_skips_when_xyz_already_present(self):
        item = _item_with_tilejson(
            extra_links=[{"rel": "xyz", "type": "image/png", "href": "https://x/{z}/{x}/{y}.png"}]
        )
        assert add_xyz_link(item) is None

    def test_skips_legacy_bare_tilejson(self):
        # the 3 known-legacy items carry .../WebMercatorQuad (no /tilejson.json?) — deriving xyz
        # would emit garbage; they're slated for wipe+re-ingest, so skip them.
        item = {
            "id": "s1-rtc-31TCJ",
            "links": [
                {
                    "rel": "tilejson",
                    "type": "application/json",
                    "href": "https://api.example.com/raster/.../s1-rtc-31TCJ/WebMercatorQuad",
                }
            ],
            "assets": {},
        }
        assert add_xyz_link(item) is None

    def test_skips_when_no_tilejson(self):
        item = {"id": "x", "links": [{"rel": "self", "href": "https://x"}], "assets": {}}
        assert add_xyz_link(item) is None

    def test_does_not_mutate_input(self):
        item = _item_with_tilejson()
        original_rels = [lk["rel"] for lk in item["links"]]
        add_xyz_link(item)
        assert [lk["rel"] for lk in item["links"]] == original_rels

    def test_idempotent(self):
        result1 = add_xyz_link(_item_with_tilejson())
        assert result1 is not None
        assert add_xyz_link(result1) is None


def _nav_links() -> list:
    return [
        {"rel": "collection", "href": "https://x/c"},
        {"rel": "parent", "href": "https://x/p"},
        {"rel": "root", "href": "https://x/r"},
        {"rel": "self", "href": "https://x/s"},
    ]


def _old_acq_item() -> dict:
    """An acquisition item as the OLD register_per_acquisition emitted it: order
    store→tilejson→xyz→viewer, tilejson titled 'tilejson', viewer/xyz hardcoded."""
    q = "expression=a&sel=time=2026-07-01T17%3A54%3A17"
    base = (
        "https://api.example.com/raster/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCG"
    )
    return {
        "id": "s1-rtc-31TCG-20260701t175417",
        "properties": {"renders": {"rgb": {"title": "VV, VH, VV/VH composite"}}},
        "links": [
            *_nav_links(),
            {"rel": "store", "title": "Zarr Store", "href": "https://x/z"},
            {
                "rel": "tilejson",
                "type": "application/json",
                "title": "tilejson",
                "href": f"{base}/WebMercatorQuad/tilejson.json?{q}",
            },
            {
                "rel": "xyz",
                "type": "image/png",
                "title": "Sentinel-1 GRD RGB composite",
                "href": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png?{q}",
            },
            {
                "rel": "viewer",
                "type": "text/html",
                "title": "Sentinel-1 GRD RGB composite",
                "href": f"{base}/WebMercatorQuad/map.html?{q}",
            },
            {"rel": "via", "type": "text/html", "title": "EOPF Explorer", "href": "https://x/e"},
            {
                "rel": "related",
                "type": "application/json",
                "title": "Parent tile datacube",
                "href": "https://x/pt",
            },
        ],
        "assets": {},
    }


def _canonical_cube_item() -> dict:
    """A cube item already in the canonical form register_v1 emits — align must no-op on it."""
    return {
        "id": "s1-rtc-31TCG",
        "properties": {"renders": {"rgb": {"title": "VV, VH, VV/VH composite"}}},
        "links": [
            *_nav_links(),
            {"rel": "store", "title": "Zarr Store", "href": "https://x/z"},
            {"rel": "viewer", "title": "VV, VH, VV/VH composite", "href": "https://x/v"},
            {"rel": "tilejson", "title": "TileJSON for s1-rtc-31TCG", "href": "https://x/t"},
            {"rel": "xyz", "title": "VV, VH, VV/VH composite", "href": "https://x/xyz"},
            {"rel": "via", "title": "EOPF Explorer", "href": "https://x/e"},
            {"rel": "related", "title": "Acquisition A", "href": "https://x/a1"},
            {"rel": "related", "title": "Acquisition B", "href": "https://x/a2"},
        ],
        "assets": {},
    }


class TestAlignVisualizationLinks:
    def test_retitles_viewer_tilejson_xyz(self):
        result = align_visualization_links(_old_acq_item())
        assert result is not None
        by_rel = {lk["rel"]: lk for lk in result["links"]}
        assert by_rel["viewer"]["title"] == "VV, VH, VV/VH composite"
        assert by_rel["xyz"]["title"] == "VV, VH, VV/VH composite"
        assert by_rel["tilejson"]["title"] == "TileJSON for s1-rtc-31TCG-20260701t175417"

    def test_reorders_non_nav_to_canonical(self):
        result = align_visualization_links(_old_acq_item())
        assert result is not None
        rels = [lk["rel"] for lk in result["links"]]
        non_nav = [r for r in rels if r not in {"collection", "parent", "root", "self"}]
        assert non_nav == ["store", "viewer", "tilejson", "xyz", "via", "related"]

    def test_preserves_related_order_and_count(self):
        item = _old_acq_item()
        item["links"].append({"rel": "related", "title": "Second related", "href": "https://x/pt2"})
        result = align_visualization_links(item)
        assert result is not None
        related = [lk for lk in result["links"] if lk["rel"] == "related"]
        assert [lk["title"] for lk in related] == ["Parent tile datacube", "Second related"]

    def test_noop_on_canonical_cube_item(self):
        assert align_visualization_links(_canonical_cube_item()) is None

    def test_skips_item_without_renders(self):
        item = {"id": "s2", "properties": {}, "links": _nav_links(), "assets": {}}
        assert align_visualization_links(item) is None

    def test_retitles_even_when_xyz_absent(self):
        item = _old_acq_item()
        item["links"] = [lk for lk in item["links"] if lk["rel"] != "xyz"]
        result = align_visualization_links(item)
        assert result is not None
        by_rel = {lk["rel"]: lk for lk in result["links"]}
        assert by_rel["tilejson"]["title"] == "TileJSON for s1-rtc-31TCG-20260701t175417"
        assert by_rel["viewer"]["title"] == "VV, VH, VV/VH composite"

    def test_render_title_fallback_when_untitled(self):
        item = _old_acq_item()
        del item["properties"]["renders"]["rgb"]["title"]
        result = align_visualization_links(item)
        assert result is not None
        by_rel = {lk["rel"]: lk for lk in result["links"]}
        assert by_rel["viewer"]["title"] == "Visualization for s1-rtc-31TCG-20260701t175417"

    def test_does_not_mutate_input(self):
        item = _old_acq_item()
        before = [lk["rel"] for lk in item["links"]]
        align_visualization_links(item)
        assert [lk["rel"] for lk in item["links"]] == before

    def test_idempotent(self):
        result1 = align_visualization_links(_old_acq_item())
        assert result1 is not None
        assert align_visualization_links(result1) is None

    def test_composes_with_add_xyz_link(self):
        # Full backfill path: add xyz (item without one) then align → canonical form.
        item = _old_acq_item()
        item["links"] = [lk for lk in item["links"] if lk["rel"] != "xyz"]
        composed = compose_migrations([add_xyz_link, align_visualization_links])
        result = composed(item)
        assert result is not None
        rels = [
            lk["rel"]
            for lk in result["links"]
            if lk["rel"] not in {"collection", "parent", "root", "self"}
        ]
        assert rels == ["store", "viewer", "tilejson", "xyz", "via", "related"]
        by_rel = {lk["rel"]: lk for lk in result["links"]}
        assert by_rel["xyz"]["title"] == "VV, VH, VV/VH composite"


def _make_mock_search(items_dicts: list, total: int | None = None) -> MagicMock:
    """Build a mock pystac_client search that yields one page with the given items."""
    mock_items = []
    for d in items_dicts:
        m = MagicMock()
        m.to_dict.return_value = d
        mock_items.append(m)
    mock_page = MagicMock()
    mock_page.items = mock_items
    mock_search = MagicMock()
    mock_search.matched.return_value = total
    mock_search.pages.return_value = [mock_page] if mock_items else []
    # run_migration consumes raw dicts (robust to items pystac can't model); clone/fetch still
    # use .pages()/.items.
    mock_search.items_as_dicts.return_value = list(items_dicts)
    return mock_search


class TestSTACMigrationRunner:
    def _make_runner(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        runner._update_item = MagicMock()
        return runner

    def test_dry_run_counts_modified_without_updating(self, item_with_wrong_media_type):
        runner = self._make_runner()
        mock_search = _make_mock_search([item_with_wrong_media_type], total=1)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", dry_run=True
            )

        assert result.items_modified == 1
        assert result.items_skipped == 0
        assert result.items_failed == 0
        assert result.dry_run is True
        runner._update_item.assert_not_called()

    def test_applies_migration_with_corrected_item(self, item_with_wrong_media_type):
        runner = self._make_runner()
        mock_search = _make_mock_search([item_with_wrong_media_type], total=1)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_modified == 1
        assert result.items_failed == 0
        runner._update_item.assert_called_once()
        _, _, posted_item = runner._update_item.call_args[0]
        assert posted_item["assets"]["SR_10m"]["type"] == "application/vnd.zarr; version=3"

    def test_skips_items_with_no_changes(self, item_clean):
        runner = self._make_runner()
        mock_search = _make_mock_search([item_clean], total=1)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_skipped == 1
        assert result.items_modified == 0
        runner._update_item.assert_not_called()

    def test_records_failure_on_update_error(self, item_with_wrong_media_type):
        runner = self._make_runner()
        mock_search = _make_mock_search([item_with_wrong_media_type], total=1)
        runner._update_item.side_effect = Exception("API error")

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_failed == 1
        assert result.items_modified == 0
        assert len(result.errors) == 1
        assert result.errors[0]["error"] == "API error"

    def test_processes_multiple_items(self, item_with_wrong_media_type, item_clean):
        runner = self._make_runner()
        mock_search = _make_mock_search([item_with_wrong_media_type, item_clean], total=2)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.items_processed == 2
        assert result.items_modified == 1
        assert result.items_skipped == 1
        runner._update_item.assert_called_once()

    def test_ids_are_passed_to_search(self, item_clean):
        # Canary path: run_migration(ids=[...]) restricts the search to those item ids so the
        # single-tile run uses the exact same code path/recovery/history as the full run.
        runner = self._make_runner()
        mock_search = _make_mock_search([item_clean], total=1)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", ids=["item-a", "item-b"]
            )

        mock_client.open.return_value.search.assert_called_once_with(
            collections=["test-col"], ids=["item-a", "item-b"], max_items=None, limit=100
        )

    def test_no_ids_omits_ids_filter(self, item_clean):
        # Without ids the full-collection search must not pass an ids filter (backcompat).
        runner = self._make_runner()
        mock_search = _make_mock_search([item_clean], total=1)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        mock_client.open.return_value.search.assert_called_once_with(
            collections=["test-col"], max_items=None, limit=100
        )

    def test_post_body_normalized_so_datacube_items_dont_400(self):
        # Datacube items have null datetime, which the STAC GET/search view omits from
        # properties — but the transaction POST requires the key. run_migration must normalize
        # the POST body (via pystac) so it re-materializes properties.datetime.
        runner = self._make_runner()  # _update_item is a MagicMock
        item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "s1-rtc-31TCG",
            "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            "bbox": [0, 0, 1, 1],
            "properties": {  # note: NO "datetime" key (null-datetime datacube), only start/end
                "start_datetime": "2026-06-01T00:00:00Z",
                "end_datetime": "2026-07-01T00:00:00Z",
            },
            "links": [
                {
                    "rel": "tilejson",
                    "type": "application/json",
                    "href": "https://x/WebMercatorQuad/tilejson.json?expression=a",
                }
            ],
            "assets": {},
            "collection": "sentinel-1-grd-rtc-staging",
        }
        mock_search = MagicMock()
        mock_search.matched.return_value = 1
        mock_search.items_as_dicts.return_value = [item]

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            runner.run_migration("test-col", add_xyz_link, "add_xyz_link")

        runner._update_item.assert_called_once()
        _, _, body = runner._update_item.call_args[0]
        assert "datetime" in body["properties"]  # key present (None) for the transaction API
        # xyz stays immediately after tilejson through normalization
        rels = [lk["rel"] for lk in body["links"]]
        assert rels.index("xyz") == rels.index("tilejson") + 1

    def test_reads_raw_dicts_so_unmodelable_items_dont_abort(self):
        # A live item can carry an asset with no href (e.g. s1-rtc-30TWQ) that pystac's
        # Item.from_dict rejects. run_migration must iterate raw dicts (items_as_dicts), not
        # pystac Item objects, so one malformed item can't abort the whole run.
        runner = self._make_runner()
        hrefless = {
            "id": "s1-rtc-30TWQ",
            "assets": {"vv": {"roles": ["data"]}},  # no href — pystac would raise KeyError
            "links": [
                {
                    "rel": "tilejson",
                    "type": "application/json",
                    "href": "https://x/WebMercatorQuad/tilejson.json?expression=a",
                }
            ],
        }
        mock_search = MagicMock()
        mock_search.matched.return_value = 1
        mock_search.items_as_dicts.return_value = [hrefless]
        # Prove the runner does NOT touch the pystac-parsing path.
        mock_search.pages.side_effect = KeyError("href")

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", add_xyz_link, "add_xyz_link")

        assert result.items_processed == 1
        assert result.items_modified == 1
        assert result.items_failed == 0

    def test_clone_collection_copies_metadata_and_items(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        mock_search = _make_mock_search(
            [
                {"id": "item-1", "collection": "source-col", "links": [], "assets": {}},
                {"id": "item-2", "collection": "source-col", "links": [], "assets": {}},
            ],
            total=2,
        )

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "id": "source-col",
            "type": "Collection",
            "description": "Test",
            "links": [],
        }
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "get", return_value=mock_resp),
            patch.object(runner.session, "post", return_value=mock_resp) as mock_post,
            patch("_migrate_catalog.runner.Client") as mock_client,
        ):
            mock_client.open.return_value.search.return_value = mock_search
            copied, skipped, failed = runner.clone_collection("source-col", "target-col")

        assert copied == 2
        assert skipped == 0
        assert failed == 0
        # collection creation + 2 item posts = 3 POSTs
        assert mock_post.call_count == 3
        # items should have collection field updated
        for call in mock_post.call_args_list[1:]:
            assert call.kwargs["json"]["collection"] == "target-col"

    def test_clone_collection_counts_failed_items(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        mock_search = _make_mock_search(
            [{"id": "item-1", "collection": "source-col", "links": [], "assets": {}}],
            total=1,
        )

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "id": "source-col",
            "type": "Collection",
            "description": "Test",
            "links": [],
        }
        mock_resp.raise_for_status = MagicMock()

        # collection POST succeeds, item POST fails
        def post_side_effect(url, **kwargs):
            if "items" in url:
                raise Exception("item post failed")
            return mock_resp

        with (
            patch.object(runner.session, "get", return_value=mock_resp),
            patch.object(runner.session, "post", side_effect=post_side_effect),
            patch("_migrate_catalog.runner.Client") as mock_client,
        ):
            mock_client.open.return_value.search.return_value = mock_search
            copied, skipped, failed = runner.clone_collection("source-col", "target-col")

        assert copied == 0
        assert skipped == 0
        assert failed == 1


class TestFetchExistingIds:
    def test_returns_set_of_item_ids(self):
        runner = STACMigrationRunner("https://api.example.com/stac")

        mock_item_1 = MagicMock()
        mock_item_1.id = "item-1"
        mock_item_2 = MagicMock()
        mock_item_2.id = "item-2"
        mock_page = MagicMock()
        mock_page.items = [mock_item_1, mock_item_2]
        mock_search = MagicMock()
        mock_search.pages.return_value = [mock_page]

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner._fetch_existing_ids("my-col", page_size=100)

        assert result == {"item-1", "item-2"}
        mock_client.open.return_value.search.assert_called_once_with(
            collections=["my-col"], max_items=None, limit=100
        )


class TestCloneResume:
    def _make_collection_resp(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "id": "source-col",
            "type": "Collection",
            "description": "Test",
            "links": [],
        }
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    def test_resume_skips_existing_items_without_posting(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        mock_resp = self._make_collection_resp()
        mock_source_search = _make_mock_search(
            [
                {"id": "item-1", "collection": "source-col", "links": [], "assets": {}},
                {"id": "item-2", "collection": "source-col", "links": [], "assets": {}},
            ],
            total=2,
        )

        with (
            patch.object(runner.session, "get", return_value=mock_resp),
            patch.object(runner.session, "post", return_value=mock_resp) as mock_post,
            patch.object(runner, "_fetch_existing_ids", return_value={"item-1"}),
            patch("_migrate_catalog.runner.Client") as mock_client,
        ):
            mock_client.open.return_value.search.return_value = mock_source_search
            copied, skipped, failed = runner.clone_collection(
                "source-col", "target-col", resume=True
            )

        assert copied == 1
        assert skipped == 1
        assert failed == 0
        # 1 collection POST + 1 item POST (item-2 only; item-1 was skipped)
        assert mock_post.call_count == 2

    def test_resume_empty_target_copies_all(self):
        runner = STACMigrationRunner("https://api.example.com/stac")
        mock_resp = self._make_collection_resp()
        mock_source_search = _make_mock_search(
            [
                {"id": "item-1", "collection": "source-col", "links": [], "assets": {}},
                {"id": "item-2", "collection": "source-col", "links": [], "assets": {}},
            ],
            total=2,
        )

        with (
            patch.object(runner.session, "get", return_value=mock_resp),
            patch.object(runner.session, "post", return_value=mock_resp) as mock_post,
            patch.object(runner, "_fetch_existing_ids", return_value=set()),
            patch("_migrate_catalog.runner.Client") as mock_client,
        ):
            mock_client.open.return_value.search.return_value = mock_source_search
            copied, skipped, failed = runner.clone_collection(
                "source-col", "target-col", resume=True
            )

        assert copied == 2
        assert skipped == 0
        assert failed == 0
        # 1 collection POST + 2 item POSTs
        assert mock_post.call_count == 3


class TestRecoveryFile:
    def test_recovery_file_written_before_delete(self, tmp_path, item_with_wrong_media_type):
        runner = STACMigrationRunner("https://api.example.com/stac", recovery_dir=tmp_path)

        recovery_existed_before_delete = []

        def delete_side_effect(*args, **kwargs):
            files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
            recovery_existed_before_delete.append(bool(files))
            return MagicMock()

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "delete", side_effect=delete_side_effect),
            patch.object(runner.session, "post", return_value=mock_resp),
        ):
            runner._update_item("test-col", "item-1", item_with_wrong_media_type)

        assert recovery_existed_before_delete == [
            True
        ], "Recovery file must exist before delete is called"

        files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
        assert len(files) == 1
        with open(files[0]) as f:
            saved = json.loads(f.readline())
        assert saved["id"] == item_with_wrong_media_type["id"]

    def test_no_recovery_file_without_recovery_dir(self, item_with_wrong_media_type):
        runner = STACMigrationRunner("https://api.example.com/stac")

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "delete", return_value=MagicMock()),
            patch.object(runner.session, "post", return_value=mock_resp),
        ):
            runner._update_item("test-col", "item-1", item_with_wrong_media_type)

        assert runner._recovery_file is None

    def test_multiple_updates_append_to_same_recovery_file(
        self, tmp_path, item_with_wrong_media_type, item_clean
    ):
        runner = STACMigrationRunner("https://api.example.com/stac", recovery_dir=tmp_path)

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "delete", return_value=MagicMock()),
            patch.object(runner.session, "post", return_value=mock_resp),
        ):
            runner._update_item("test-col", "item-1", item_with_wrong_media_type)
            runner._update_item("test-col", "item-2", item_clean)

        files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text().strip().splitlines()
        assert len(lines) == 2


class TestComposeMigrations:
    def test_changed_by_both(self):
        item = {
            "id": "test-both",
            "assets": {
                "data": {
                    "href": "https://example.com/data?scale=0+1",
                    "type": "application/vnd+zarr",
                }
            },
            "links": [],
        }
        composed = compose_migrations([fix_url_encoding, fix_zarr_media_type])
        result = composed(item)
        assert result is not None
        assert "%20" in result["assets"]["data"]["href"]
        assert "+" not in result["assets"]["data"]["href"].split("?")[1]
        assert result["assets"]["data"]["type"] == "application/vnd.zarr; version=3"

    def test_changed_by_one_only(self, item_with_plus_urls):
        # item_with_plus_urls has correct media types — only url fix applies
        composed = compose_migrations([fix_url_encoding, fix_zarr_media_type])
        result = composed(item_with_plus_urls)
        assert result is not None
        query = result["assets"]["thumbnail"]["href"].split("?")[1]
        assert "+" not in query
        assert "%20" in query

    def test_changed_by_none(self, item_clean):
        composed = compose_migrations([fix_url_encoding, fix_zarr_media_type])
        assert composed(item_clean) is None

    def test_does_not_mutate_input(self):
        item = {
            "id": "test-mutate",
            "assets": {"data": {"href": "https://ex.com?q=a+b", "type": "application/vnd+zarr"}},
            "links": [],
        }
        original_href = item["assets"]["data"]["href"]
        original_type = item["assets"]["data"]["type"]
        compose_migrations([fix_url_encoding, fix_zarr_media_type])(item)
        assert item["assets"]["data"]["href"] == original_href
        assert item["assets"]["data"]["type"] == original_type

    def test_composed_name_uses_plus_separator(self):
        from _migrate_catalog.migrations import MIGRATIONS

        migration_name = "+".join(["fix_url_encoding", "fix_zarr_media_type"])
        assert migration_name == "fix_url_encoding+fix_zarr_media_type"
        assert "fix_url_encoding" in MIGRATIONS
        assert "fix_zarr_media_type" in MIGRATIONS


class TestHistoryTracking:
    def test_load_history_returns_empty_for_missing_file(self, tmp_path):
        history = load_history(tmp_path / "history.json")
        assert history == {"runs": []}

    def test_record_run_writes_to_file(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        history = load_history(history_file)
        assert len(history["runs"]) == 1
        run = history["runs"][0]
        assert run["migration_name"] == "fix_zarr_media_type"
        assert run["items_modified"] == 50
        assert run["dry_run"] is False

    def test_record_run_appends_to_existing(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)
        record_run(history_file, migration_result)

        assert len(load_history(history_file)["runs"]) == 2

    def test_was_migration_run_detects_completed_run(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        assert was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_ignores_dry_runs(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        migration_result.dry_run = True
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_false_for_different_collection(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_zarr_media_type", "sentinel-1-slc")

    def test_was_migration_run_false_for_different_migration(self, tmp_path, migration_result):
        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_url_encoding", "sentinel-2-l2a")
