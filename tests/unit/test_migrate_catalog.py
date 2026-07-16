"""Unit tests for the migrate_catalog package."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _migrate_catalog.history import load_history, record_run, was_migration_run
from _migrate_catalog.migrations.add_acquisitions_filter_link import add_acquisitions_filter_link
from _migrate_catalog.migrations.add_eodash_rasterform import add_eodash_rasterform
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


_FILTER_TITLE = "Per-acquisition items (filter by tile grid:code)"


def _acq_item_no_filter() -> dict:
    """An aligned acquisition item carrying only the single parent related link (no filter link)."""
    stac = "https://api.example.com/stac"
    acq_coll = "sentinel-1-grd-rtc-acquisitions-staging"
    return {
        "id": "s1-rtc-31TCG-20260701t175417",
        "collection": acq_coll,
        "properties": {"renders": {"rgb": {"title": "VV, VH, VV/VH composite"}}},
        "links": [
            {
                "rel": "self",
                "href": f"{stac}/collections/{acq_coll}/items/s1-rtc-31TCG-20260701t175417",
            },
            {"rel": "store", "title": "Zarr Store", "href": "https://x/z"},
            {"rel": "viewer", "title": "VV, VH, VV/VH composite", "href": "https://x/v"},
            {"rel": "tilejson", "title": "TileJSON for s1-rtc-...", "href": "https://x/t"},
            {"rel": "xyz", "title": "VV, VH, VV/VH composite", "href": "https://x/xyz"},
            {"rel": "via", "title": "EOPF Explorer", "href": "https://x/e"},
            {
                "rel": "related",
                "type": "application/json",
                "href": f"{stac}/collections/sentinel-1-grd-rtc-staging/items/s1-rtc-31TCG",
                "title": "Parent tile datacube",
            },
        ],
        "assets": {},
    }


class TestAddAcquisitionsFilterLink:
    def test_adds_filter_link_after_parent(self):
        result = add_acquisitions_filter_link(_acq_item_no_filter())
        assert result is not None
        related = [lk for lk in result["links"] if lk["rel"] == "related"]
        assert [lk["title"] for lk in related] == ["Parent tile datacube", _FILTER_TITLE]

    def test_filter_href_targets_the_acquisitions_collection(self):
        result = add_acquisitions_filter_link(_acq_item_no_filter())
        assert result is not None
        filt = next(lk for lk in result["links"] if lk.get("title") == _FILTER_TITLE)
        assert (
            filt["href"]
            == "https://api.example.com/stac/collections/sentinel-1-grd-rtc-acquisitions-staging"
        )
        assert filt["rel"] == "related"
        assert filt["type"] == "application/json"

    def test_now_two_related_links_triggers_grouping(self):
        result = add_acquisitions_filter_link(_acq_item_no_filter())
        assert result is not None
        assert sum(1 for lk in result["links"] if lk["rel"] == "related") == 2

    def test_idempotent(self):
        result1 = add_acquisitions_filter_link(_acq_item_no_filter())
        assert result1 is not None
        assert add_acquisitions_filter_link(result1) is None

    def test_skips_non_acquisition_item(self):
        # No "Parent tile datacube" related link (e.g. a cube or S2 item) → not an acq item → skip.
        item = _canonical_cube_item()
        assert add_acquisitions_filter_link(item) is None

    def test_does_not_mutate_input(self):
        item = _acq_item_no_filter()
        before = len(item["links"])
        add_acquisitions_filter_link(item)
        assert len(item["links"]) == before

    def test_gate_titles_match_registration_source_of_truth(self):
        # The migration gates on these titles; if register_* renames them the migration would
        # silently skip every item. Bind them to the shared constants (drift guard).
        from _migrate_catalog.migrations import add_acquisitions_filter_link as mod
        from stac_link_titles import ACQUISITIONS_FILTER_TITLE, PARENT_DATACUBE_TITLE

        assert mod._PARENT_TITLE == PARENT_DATACUBE_TITLE
        assert mod._FILTER_TITLE == ACQUISITIONS_FILTER_TITLE


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
    # run_migration consumes raw dicts a page at a time (robust to items pystac can't model,
    # while still bounding in-flight writes to one page); clone/fetch still use .pages()/.items.
    mock_search.pages_as_dicts.return_value = [{"features": list(items_dicts)}] if mock_items else []
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
        mock_search.pages_as_dicts.return_value = [{"features": [item]}]

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            runner.run_migration("test-col", add_xyz_link, "add_xyz_link")

        runner._update_item.assert_called_once()
        _, _, body = runner._update_item.call_args[0]
        assert "datetime" in body["properties"]  # key present (None) for the transaction API
        # xyz stays immediately after tilejson through normalization
        rels = [lk["rel"] for lk in body["links"]]
        assert rels.index("xyz") == rels.index("tilejson") + 1

    def test_unmodelable_item_failed_without_delete(self):
        # A live item can carry an asset with no href (e.g. s1-rtc-30TWQ) that pystac's
        # Item.from_dict rejects. It is READ fine (raw dicts, so it can't abort the run) but it
        # cannot be turned into a transaction-valid PUT body — so it must be reported failed and
        # NEVER written. Sending a body the API rejects would fail the item anyway; sending
        # nothing keeps it exactly as it is.
        runner = self._make_runner()  # _update_item (the atomic PUT) is a MagicMock
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
        mock_search.pages_as_dicts.return_value = [{"features": [hrefless]}]
        # Prove the runner does NOT touch the pystac-parsing path.
        mock_search.pages.side_effect = KeyError("href")
        mock_search.items.side_effect = KeyError("href")

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", add_xyz_link, "add_xyz_link")

        assert result.items_processed == 1
        assert result.items_modified == 0
        assert result.items_failed == 1
        runner._update_item.assert_not_called()  # never delete an item we can't re-POST

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


class TestAtomicUpdate:
    """The write must be ONE atomic call.

    The tool used to DELETE then POST, on the belief that pgSTAC had no PUT. That
    left a window in which the item existed nowhere: a timeout, crash or kill
    between the two deleted the item for good, and a re-run could not heal it
    because /search no longer returns it. Only the recovery file could.

    The API in fact advertises the OGC Features transaction extension and exposes
    PUT on the item path. PUT either replaces the item or leaves it untouched, so
    a torn item is not merely unlikely — it is impossible. These tests exist to
    stop anyone reintroducing the window.
    """

    def _runner_with_mock_session(self, **kw):  # noqa: ANN202
        runner = STACMigrationRunner("https://api.example.com/stac", **kw)
        session = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        session.put.return_value = resp
        runner._session = MagicMock(return_value=session)  # type: ignore[method-assign]
        return runner, session

    def test_update_is_a_single_put_and_never_deletes(self, item_with_wrong_media_type) -> None:
        runner, session = self._runner_with_mock_session()

        runner._update_item("test-col", "item-1", item_with_wrong_media_type)

        session.put.assert_called_once()
        session.delete.assert_not_called()  # the window must not exist
        session.post.assert_not_called()
        url = session.put.call_args[0][0]
        assert url == "https://api.example.com/stac/collections/test-col/items/item-1"
        assert session.put.call_args.kwargs["json"] == item_with_wrong_media_type

    def test_a_failing_put_is_tallied_as_failed_not_modified(self) -> None:
        """A failed PUT leaves the item intact — but it must still reach items_failed.

        Driven through run_migration rather than asserting _update_item raises: the
        tallying is the actual contract, and a test that only checks the raise would
        pass even if run_migration swallowed it.
        """
        import requests

        runner = STACMigrationRunner("https://api.example.com/stac")
        session = MagicMock()
        resp = MagicMock()
        resp.raise_for_status.side_effect = requests.HTTPError("500 boom")
        session.put.return_value = resp
        runner._session = MagicMock(return_value=session)  # type: ignore[method-assign]

        search = _make_mock_search([_dirty_item(0)], total=1)
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration("test-col", fix_zarr_media_type, "m")

        assert result.items_failed == 1
        assert result.items_modified == 0
        assert "500 boom" in result.errors[0]["error"]

    def test_put_goes_through_the_thread_local_session(self) -> None:
        """The write must use the per-thread Session (a shared one is not guaranteed
        thread-safe). Patching the real session object rather than _session keeps
        that wiring covered."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        resp = MagicMock()
        resp.raise_for_status = MagicMock()

        with (
            patch.object(runner.session, "put", return_value=resp) as mock_put,
            patch.object(runner.session, "delete") as mock_delete,
        ):
            runner._update_item("test-col", "item-1", {"id": "item-1"})

        # __init__ seeds the constructing thread's local with self.session, so a
        # main-thread write must land on it.
        mock_put.assert_called_once()
        mock_delete.assert_not_called()

    def test_recovery_line_written_before_the_put(self, tmp_path, item_with_wrong_media_type):
        """Still written first — it is now an audit/undo record rather than the only
        way back, but it must describe a write before that write is attempted."""
        runner, session = self._runner_with_mock_session(recovery_dir=tmp_path)
        existed_before_put = []

        def put_side_effect(*args, **kwargs):  # noqa: ANN202
            existed_before_put.append(bool(list(tmp_path.glob(".migration_recovery_*.jsonl"))))
            r = MagicMock()
            r.raise_for_status = MagicMock()
            return r

        session.put.side_effect = put_side_effect
        runner._update_item("test-col", "item-1", item_with_wrong_media_type)

        assert existed_before_put == [True]
        files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
        assert len(files) == 1
        with open(files[0]) as f:
            saved = json.loads(f.readline())
        assert saved["id"] == item_with_wrong_media_type["id"]

    def test_no_recovery_file_without_recovery_dir(self, item_with_wrong_media_type):
        runner, _ = self._runner_with_mock_session()
        runner._update_item("test-col", "item-1", item_with_wrong_media_type)
        assert runner._recovery_file is None

    def test_multiple_updates_append_to_same_recovery_file(
        self, tmp_path, item_with_wrong_media_type, item_clean
    ):
        runner, _ = self._runner_with_mock_session(recovery_dir=tmp_path)

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

    def test_was_migration_run_ignores_bounded_runs(self, tmp_path, migration_result):
        """A --max-writes run applied PART of the migration, so the next chunk must
        not be told it was 'already applied'.

        Chunking a large backfill is the designed workflow (~170k items = ~17 runs).
        The CLI's warning prompts 'Run again?' defaulting to No, so counting a
        bounded run as applied would tell the operator the work is done and make the
        safe-looking default abandon the rest of the backfill.
        """
        history_file = tmp_path / "history.json"
        migration_result.reached_max_writes = True
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_ignores_aborted_runs(self, tmp_path, migration_result):
        """An aborted run stopped because writes were failing wholesale — that is the
        run that most needs re-running, so it must not read as 'already applied'."""
        history_file = tmp_path / "history.json"
        migration_result.aborted = True
        record_run(history_file, migration_result)

        assert not was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_true_once_a_run_completes(self, tmp_path, migration_result):
        """A bounded run followed by a completed one: the completed one counts."""
        history_file = tmp_path / "history.json"
        migration_result.reached_max_writes = True
        record_run(history_file, migration_result)
        migration_result.reached_max_writes = False  # the final, unbounded run
        record_run(history_file, migration_result)

        assert was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")

    def test_was_migration_run_handles_history_predating_these_fields(
        self, tmp_path, migration_result
    ):
        """Entries written before aborted/reached_max_writes existed have neither
        key; they were full runs, so they must still count as applied."""
        import json as _json

        history_file = tmp_path / "history.json"
        record_run(history_file, migration_result)
        data = _json.loads(history_file.read_text())
        data["runs"][0].pop("aborted", None)
        data["runs"][0].pop("reached_max_writes", None)
        history_file.write_text(_json.dumps(data))

        assert was_migration_run(history_file, "fix_zarr_media_type", "sentinel-2-l2a")


# === stamp_expires (coordination#183, Task 4) ===

from datetime import UTC, datetime, timedelta  # noqa: E402

from _migrate_catalog.migrations import MIGRATIONS  # noqa: E402
from _migrate_catalog.migrations.stamp_expires import (  # noqa: E402
    SKIP_HISTOGRAM,
    classify_and_stamp,
    reset_histogram,
    stamp_expires,
)
from s3_item_cleanup import (  # noqa: E402
    DEFAULT_RETENTION_DAYS,
    TIMESTAMPS_EXTENSION,
    format_expires,
    parse_stac_timestamp,
    resolve_exclude_ids,
)


def _stampable_item(
    item_id: str = "S2_pipeline",
    created: str = "2025-01-01T00:00:00Z",
    datetime_str: str = "2024-12-31T00:00:00Z",
    expires: str | None = None,
) -> dict:
    props: dict = {"datetime": datetime_str, "created": created}
    if expires is not None:
        props["expires"] = expires
    return {
        "type": "Feature",
        "id": item_id,
        "collection": "sentinel-2-l2a-staging",
        "properties": props,
        "stac_extensions": [],
        "assets": {},
        # The runner builds its PUT body via real pystac parsing, so a write-path
        # fixture has to be a modelable item, not just the fields stamp_expires reads.
        "stac_version": "1.0.0",
        "geometry": None,
        "bbox": None,
        "links": [],
    }


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


class TestClassifyAndStamp:
    def test_stamps_expires_at_datetime_plus_retention(self) -> None:
        # Retention is measured from acquisition (datetime), not created.
        item = _stampable_item(created="2026-05-05T00:00:00Z", datetime_str="2025-12-31T00:00:00Z")
        result, reason = classify_and_stamp(
            item, retention_days=183, exclude_ids=set(), min_datetime=None
        )
        assert reason == "stamped"
        assert result is not None
        delta = _parse(result["properties"]["expires"]) - _parse("2025-12-31T00:00:00Z")
        assert delta == timedelta(days=183)

    def test_stamped_item_gets_timestamps_extension_once(self) -> None:
        item = _stampable_item()
        result, _ = classify_and_stamp(
            item, retention_days=183, exclude_ids=set(), min_datetime=None
        )
        assert result is not None
        assert result["stac_extensions"].count(TIMESTAMPS_EXTENSION) == 1

    def test_expires_is_utc_z_formatted(self) -> None:
        item = _stampable_item()
        result, _ = classify_and_stamp(
            item, retention_days=183, exclude_ids=set(), min_datetime=None
        )
        assert result is not None
        assert result["properties"]["expires"].endswith("Z")

    def test_already_stamped_item_is_skipped(self) -> None:
        item = _stampable_item(expires="2025-07-01T00:00:00Z")
        result, reason = classify_and_stamp(
            item, retention_days=183, exclude_ids=set(), min_datetime=None
        )
        assert result is None
        assert reason == "already_stamped"

    def test_excluded_item_is_skipped(self) -> None:
        item = _stampable_item(item_id="S2_demo")
        result, reason = classify_and_stamp(
            item,
            retention_days=183,
            exclude_ids={"S2_demo"},
            min_datetime=None,
        )
        assert result is None
        assert reason == "excluded"

    def test_item_without_datetime_is_skipped(self) -> None:
        item = _stampable_item()
        del item["properties"]["datetime"]
        result, reason = classify_and_stamp(
            item, retention_days=183, exclude_ids=set(), min_datetime=None
        )
        assert result is None
        assert reason == "no_datetime"

    def test_item_acquired_before_floor_is_skipped(self) -> None:
        # A 2021 demo scene is skipped (never stamped => never deleted) when the
        # floor is in the pipeline era.
        item = _stampable_item(datetime_str="2021-05-01T00:00:00Z")
        result, reason = classify_and_stamp(
            item,
            retention_days=183,
            exclude_ids=set(),
            min_datetime=_parse("2025-11-01T00:00:00Z"),
        )
        assert result is None
        assert reason == "before_floor"

    def test_item_acquired_on_or_after_floor_is_stamped(self) -> None:
        # The floor is inclusive of its own instant: an item exactly at the floor
        # is stamped, not skipped.
        item = _stampable_item(datetime_str="2025-11-01T00:00:00Z")
        result, reason = classify_and_stamp(
            item,
            retention_days=183,
            exclude_ids=set(),
            min_datetime=_parse("2025-11-01T00:00:00Z"),
        )
        assert reason == "stamped"
        assert result is not None

    def test_excluded_wins_over_floor_eligible(self) -> None:
        # The crown-jewel contract: a demo acquired AFTER the floor (so it would
        # otherwise be stamped) is protected by the exclude list, not the floor.
        # This is the real T33TVF / T36UXA / T27VWL (2026) demo case.
        item = _stampable_item(item_id="demo_after_floor", datetime_str="2026-02-25T00:00:00Z")
        result, reason = classify_and_stamp(
            item,
            retention_days=183,
            exclude_ids={"demo_after_floor"},
            min_datetime=_parse("2025-11-01T00:00:00Z"),
        )
        assert result is None
        assert reason == "excluded"

    def test_same_after_floor_item_is_stamped_without_exclusion(self) -> None:
        # Documents the danger the exclude list guards against: the identical
        # after-floor demo, if NOT excluded, is stamped and becomes deletable.
        item = _stampable_item(item_id="demo_after_floor", datetime_str="2026-02-25T00:00:00Z")
        result, reason = classify_and_stamp(
            item,
            retention_days=183,
            exclude_ids=set(),
            min_datetime=_parse("2025-11-01T00:00:00Z"),
        )
        assert reason == "stamped"
        assert result is not None

    def test_does_not_mutate_input_item(self) -> None:
        item = _stampable_item()
        classify_and_stamp(item, retention_days=183, exclude_ids=set(), min_datetime=None)
        assert "expires" not in item["properties"]


class TestStampExpiresMigration:
    def test_registered_in_migrations(self) -> None:
        assert "stamp_expires" in MIGRATIONS

    def test_default_retention_is_the_shared_constant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        item = _stampable_item()  # datetime default 2024-12-31
        result = stamp_expires(item)
        assert result is not None
        delta = _parse(result["properties"]["expires"]) - _parse("2024-12-31T00:00:00Z")
        assert delta == timedelta(days=DEFAULT_RETENTION_DAYS)

    def test_min_datetime_floor_skips_old_acquisitions(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        monkeypatch.setenv("EXPIRES_MIN_DATETIME", "2025-11-01")  # bare date form
        reset_histogram()
        stamp_expires(_stampable_item(item_id="old", datetime_str="2021-09-17T00:00:00Z"))
        stamp_expires(_stampable_item(item_id="new", datetime_str="2026-02-01T00:00:00Z"))
        assert SKIP_HISTOGRAM["before_floor"] == 1
        assert SKIP_HISTOGRAM["stamped"] == 1

    def test_histogram_counts_outcomes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()
        stamp_expires(_stampable_item(item_id="a"))
        stamp_expires(_stampable_item(item_id="b", expires="2025-07-01T00:00:00Z"))
        assert SKIP_HISTOGRAM["stamped"] == 1
        assert SKIP_HISTOGRAM["already_stamped"] == 1


def test_stamp_expires_uses_shared_format_helper() -> None:
    """The backfill must use the shared expires helpers, not private copies
    (review finding 4 — one load-bearing timestamp format)."""
    from _migrate_catalog.migrations import stamp_expires as se

    assert se.format_expires is format_expires
    assert se.parse_stac_timestamp is parse_stac_timestamp
    assert se.resolve_exclude_ids is resolve_exclude_ids


# === Histogram surfacing + reconciliation (review finding: histogram invisible) ===


def _result(
    *, processed: int, modified: int, skipped: int, failed: int = 0, dry_run: bool = False
) -> MigrationResult:
    return MigrationResult(
        migration_name="stamp_expires",
        collection_id="c",
        started_at="",
        completed_at="",
        items_processed=processed,
        items_modified=modified,
        items_skipped=skipped,
        items_failed=failed,
        dry_run=dry_run,
        errors=[],
    )


class TestHistogramReporting:
    def test_registry_entry_exposes_fn_reporter_and_reset(self) -> None:
        m = MIGRATIONS["stamp_expires"]
        assert m.fn is stamp_expires
        assert m.reporter is not None
        assert m.reset is reset_histogram

    def test_report_lists_reason_counts(self) -> None:
        from _migrate_catalog.migrations.stamp_expires import report

        reset_histogram()
        SKIP_HISTOGRAM["stamped"] = 2
        SKIP_HISTOGRAM["excluded"] = 1
        text = report(_result(processed=3, modified=2, skipped=1))
        assert "stamped" in text
        assert "excluded" in text
        assert "WARNING" not in text  # reconciles

    def test_report_warns_when_histogram_does_not_reconcile(self) -> None:
        from _migrate_catalog.migrations.stamp_expires import report

        reset_histogram()
        SKIP_HISTOGRAM["stamped"] = 5  # classified 5 stampable...
        text = report(_result(processed=2, modified=2, skipped=0))  # ...but run saw 2
        assert "WARNING" in text

    def test_reset_clears_counts_between_runs(self) -> None:
        SKIP_HISTOGRAM["stamped"] = 9
        reset_histogram()
        assert sum(SKIP_HISTOGRAM.values()) == 0

    def test_report_warns_when_exclude_id_matched_nothing(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # The exclude list is the crown-jewel demo protection, so a listed id that
        # matches no item (typo / stale reconverted id) must fail loud.
        from _migrate_catalog.migrations.stamp_expires import report

        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("real_demo\nTYPO_demo_xyz\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()
        stamp_expires(_stampable_item(item_id="real_demo"))  # matches one listed id
        stamp_expires(_stampable_item(item_id="pipeline"))  # a normal item
        text = report(_result(processed=2, modified=1, skipped=1))
        assert "matched no item" in text
        assert "TYPO_demo_xyz" in text
        assert "real_demo" not in text  # the matched id is not flagged

    def test_partial_run_does_not_cry_wolf_about_unmatched_exclude_ids(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A run that stopped early only scanned part of the collection, so an
        exclude id it never reached is NOT evidence of broken demo protection.

        This is the crown-jewel alarm, and --max-writes makes partial scans the
        normal workflow (~17 chunks for the prod backfill). Firing it every time
        would train operators to ignore the one warning that must never be ignored.
        Measured live: a bounded prod run reached 2 of 12 demo ids and warned about
        the other 10, all of which were simply beyond where it stopped.
        """
        from _migrate_catalog.migrations.stamp_expires import report

        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("seen_demo\nnot_yet_reached_demo\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()
        stamp_expires(_stampable_item(item_id="seen_demo"))  # only this one scanned

        bounded = _result(processed=1, modified=0, skipped=1)
        bounded.reached_max_writes = True
        text = report(bounded)
        assert "matched no item" not in text, "a partial scan must not raise the alarm"
        assert "not_yet_reached_demo" in text  # still surfaced, as information
        assert "stopped early" in text.lower()

    def test_aborted_run_also_does_not_cry_wolf(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from _migrate_catalog.migrations.stamp_expires import report

        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("seen_demo\nnot_yet_reached_demo\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()
        stamp_expires(_stampable_item(item_id="seen_demo"))

        aborted = _result(processed=1, modified=0, skipped=1)
        aborted.aborted = True
        assert "matched no item" not in report(aborted)

    def test_complete_run_still_raises_the_alarm(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The suppression must be narrow: on a run that scanned everything, an
        unmatched id IS a stale/typo'd id protecting nothing, and must fail loud."""
        from _migrate_catalog.migrations.stamp_expires import report

        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("real_demo\nTYPO_demo_xyz\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()
        stamp_expires(_stampable_item(item_id="real_demo"))

        complete = _result(processed=1, modified=0, skipped=1)  # neither flag set
        text = report(complete)
        assert "matched no item" in text
        assert "TYPO_demo_xyz" in text

    def test_report_quiet_when_all_exclude_ids_match(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from _migrate_catalog.migrations.stamp_expires import report

        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("real_demo\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()
        stamp_expires(_stampable_item(item_id="real_demo"))
        text = report(_result(processed=1, modified=0, skipped=1))
        assert "matched no item" not in text


class TestRunCommandSurfacesHistogram:
    def test_run_resets_then_prints_histogram(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import importlib

        from click.testing import CliRunner

        # The package shadows its `cli` submodule with the group function, so
        # resolve the real module object from sys.modules to patch it.
        climod = importlib.import_module("_migrate_catalog.cli")

        monkeypatch.delenv("EXPIRES_EXCLUDE_FILE", raising=False)
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        SKIP_HISTOGRAM.clear()
        SKIP_HISTOGRAM["stamped"] = 99  # stale — must be cleared before the run

        seen_at_start = {}

        def fake_run(  # noqa: ANN001
            collection_id,
            fn,
            name,
            dry_run,
            page_size,
            concurrency,
            max_consecutive_failures,
            max_writes,
            ids=None,
        ):
            seen_at_start["total"] = sum(SKIP_HISTOGRAM.values())  # proves reset ran first
            fn(_stampable_item(item_id="a"))
            fn(_stampable_item(item_id="b", expires="2025-07-01T00:00:00Z"))
            return _result(processed=2, modified=1, skipped=1, dry_run=True)

        runner_inst = MagicMock()
        runner_inst.run_migration.side_effect = fake_run
        monkeypatch.setattr(climod, "STACMigrationRunner", lambda *a, **k: runner_inst)

        res = CliRunner().invoke(
            climod.cli, ["run", "coll", "--migration", "stamp_expires", "--dry-run"]
        )

        assert res.exit_code == 0, res.output
        assert seen_at_start["total"] == 0  # histogram was reset before processing
        assert "stamped" in res.output
        assert "already_stamped" in res.output


def test_search_client_is_resilient() -> None:
    """The pagination client must carry a timeout AND retry-with-backoff so a
    stalled socket or a transient reset can't kill a long backfill (runner
    defects seen live: a 4.5h hang, then a ConnectionReset abort at ~20%). The
    migration's idempotent re-run is the final backstop."""
    from _migrate_catalog.runner import _SEARCH_TIMEOUT, _resilient_stac_io

    io = _resilient_stac_io()
    assert io.timeout == _SEARCH_TIMEOUT
    retry = io.session.get_adapter("https://example.com").max_retries
    assert retry.total and retry.total >= 5
    assert retry.backoff_factor and retry.backoff_factor > 0
    assert "POST" in retry.allowed_methods  # /search pagination uses POST


# === Parallel writes (--concurrency) ===

import threading  # noqa: E402
import time  # noqa: E402
from concurrent.futures import ThreadPoolExecutor  # noqa: E402


def _stac_item(item_id: str, assets: dict) -> dict:
    """A transaction-valid item shell.

    The runner turns each raw dict into a PUT body via `_transaction_body` (i.e. real
    pystac parsing), so a write-path fixture must be a modelable item — a bare
    {id, assets, links} dict is reported as unmodelable and never written.
    """
    return {
        "id": item_id,
        "type": "Feature",
        "stac_version": "1.0.0",
        "geometry": None,
        "bbox": None,
        "properties": {"datetime": "2026-01-01T00:00:00Z"},
        "links": [],
        "assets": assets,
        "collection": "test-col",
    }


def _dirty_item(i: int) -> dict:
    """An item fix_zarr_media_type will modify (so it reaches the write path)."""
    return _stac_item(
        f"item-{i}",
        {"data": {"href": "https://ex.com/d.zarr", "type": "application/vnd+zarr"}},
    )


def _clean_item(i: int) -> dict:
    """An item fix_zarr_media_type leaves alone (so it counts as a skip)."""
    return _stac_item(
        f"clean-{i}",
        {"data": {"href": "https://ex.com/d.zarr", "type": "application/vnd.zarr; version=3"}},
    )


class TestRunMigrationConcurrency:
    def _runner_recording_writes(self) -> tuple[STACMigrationRunner, list, list]:
        """Runner whose _update_item records (item_id, thread) instead of doing I/O."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        written: list[str] = []
        write_threads: list[int] = []

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                written.append(item_id)
                write_threads.append(threading.get_ident())

        runner._update_item = record  # type: ignore[method-assign]
        return runner, written, write_threads

    def _run(self, runner, items, concurrency, dry_run=False):  # noqa: ANN001, ANN202
        mock_search = _make_mock_search(items, total=len(items))
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            return runner.run_migration(
                "test-col",
                fix_zarr_media_type,
                "fix_zarr_media_type",
                dry_run=dry_run,
                concurrency=concurrency,
            )

    def test_default_concurrency_is_sequential_and_unchanged(self) -> None:
        """Default (1) must behave exactly like today's sequential path: all writes
        happen on the calling thread, and the tally is unchanged."""
        runner, written, write_threads = self._runner_recording_writes()
        items = [_dirty_item(i) for i in range(5)] + [_clean_item(i) for i in range(3)]

        result = self._run(runner, items, concurrency=1)

        assert result.items_processed == 8
        assert result.items_modified == 5
        assert result.items_skipped == 3
        assert result.items_failed == 0
        assert sorted(written) == sorted(f"item-{i}" for i in range(5))
        assert set(write_threads) == {threading.get_ident()}, "concurrency=1 must not offload I/O"

    def test_parallel_tally_matches_sequential(self) -> None:
        """concurrency=4 must produce the same counters as concurrency=1."""
        items = [_dirty_item(i) for i in range(20)] + [_clean_item(i) for i in range(10)]

        seq_runner, seq_written, _ = self._runner_recording_writes()
        par_runner, par_written, _ = self._runner_recording_writes()

        seq = self._run(seq_runner, items, concurrency=1)
        par = self._run(par_runner, items, concurrency=4)

        assert (par.items_processed, par.items_modified, par.items_skipped, par.items_failed) == (
            seq.items_processed,
            seq.items_modified,
            seq.items_skipped,
            seq.items_failed,
        )
        assert sorted(par_written) == sorted(seq_written)

    def test_parallel_writes_every_item_exactly_once(self) -> None:
        runner, written, _ = self._runner_recording_writes()
        items = [_dirty_item(i) for i in range(50)]

        result = self._run(runner, items, concurrency=8)

        assert result.items_modified == 50
        assert sorted(written) == sorted(f"item-{i}" for i in range(50))
        assert len(written) == len(set(written)), "no item may be written twice"

    def test_parallel_actually_uses_multiple_threads(self) -> None:
        """Guards the point of the change: writes really are dispatched to a pool."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        barrier = threading.Barrier(4, timeout=30)
        lock = threading.Lock()
        threads: set[int] = set()

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            barrier.wait()  # times out unless >=4 writes run concurrently
            with lock:
                threads.add(threading.get_ident())

        runner._update_item = record  # type: ignore[method-assign]
        result = self._run(runner, [_dirty_item(i) for i in range(8)], concurrency=4)

        assert result.items_modified == 8
        assert len(threads) >= 4

    def test_dry_run_never_writes_even_with_concurrency(self) -> None:
        runner, written, _ = self._runner_recording_writes()
        items = [_dirty_item(i) for i in range(10)]

        result = self._run(runner, items, concurrency=4, dry_run=True)

        assert result.items_modified == 10
        assert written == [], "dry run must not reach the write path"

    def test_one_failing_write_does_not_abort_the_batch(self) -> None:
        """A single write raising must be isolated: it lands in items_failed +
        errors, and every other item still gets written."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        written: list[str] = []

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            if item_id == "item-3":
                raise RuntimeError("API error")
            with lock:
                written.append(item_id)

        runner._update_item = record  # type: ignore[method-assign]
        result = self._run(runner, [_dirty_item(i) for i in range(10)], concurrency=4)

        assert result.items_failed == 1
        assert result.items_modified == 9
        assert len(result.errors) == 1
        assert result.errors[0]["item_id"] == "item-3"
        assert "API error" in result.errors[0]["error"]
        assert "item-3" not in written
        assert len(written) == 9

    def test_migration_fn_runs_only_on_the_calling_thread(self) -> None:
        """The core design invariant: migration_fn holds non-thread-safe module
        state (stamp_expires' histogram / exclude sets), so only the WRITES are
        parallelized — classification stays on the main thread."""
        runner, _, _ = self._runner_recording_writes()
        lock = threading.Lock()
        fn_threads: set[int] = set()

        def tracking_fn(item):  # noqa: ANN001, ANN202
            with lock:
                fn_threads.add(threading.get_ident())
            return fix_zarr_media_type(item)

        mock_search = _make_mock_search([_dirty_item(i) for i in range(30)], total=30)
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            runner.run_migration("test-col", tracking_fn, "tracking", concurrency=8)

        assert fn_threads == {threading.get_ident()}


def _multi_page_search(pages_of_items: list[list[dict]], total: int) -> MagicMock:
    """A search yielding several pages — the real shape of a large backfill."""
    pages = []
    for items in pages_of_items:
        mocks = []
        for d in items:
            m = MagicMock()
            m.to_dict.return_value = d
            mocks.append(m)
        page = MagicMock()
        page.items = mocks
        pages.append(page)
    search = MagicMock()
    search.matched.return_value = total
    search.pages.return_value = pages
    search.pages_as_dicts.return_value = [{"features": items} for items in pages_of_items]
    return search


class TestConcurrencyAcrossPages:
    """run_migration classifies and dispatches per PAGE, so multi-page is the real
    production path (a 191k-item backfill is ~1,900 pages) — single-page tests
    would not catch a batch leaking between pages."""

    def test_every_item_across_pages_written_exactly_once(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        written: list[str] = []

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                written.append(item_id)

        runner._update_item = record  # type: ignore[method-assign]
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(5)]
        search = _multi_page_search(pages, total=100)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", concurrency=8
            )

        assert result.items_processed == 100
        assert result.items_modified == 100
        assert result.items_failed == 0
        assert sorted(written) == sorted(f"item-{i}" for i in range(100))
        assert len(written) == len(set(written))


class TestCircuitBreaker:
    """A write is DELETE-then-POST, so if the API refuses writes wholesale, running
    to completion would delete-and-not-restore the entire collection. The run must
    stop instead."""

    def _runner_failing_after(self, ok_count: int) -> STACMigrationRunner:
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        seen = {"n": 0}

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                seen["n"] += 1
                n = seen["n"]
            if n > ok_count:
                raise RuntimeError("503 Service Unavailable")

        runner._update_item = record  # type: ignore[method-assign]
        return runner

    def test_aborts_once_writes_fail_wholesale(self) -> None:
        runner = self._runner_failing_after(0)  # every write fails
        pages = [[_dirty_item(pg * 10 + i) for i in range(10)] for pg in range(20)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col",
                fix_zarr_media_type,
                "fix_zarr_media_type",
                max_consecutive_failures=25,
            )

        assert result.aborted is True
        # Stops early instead of destroying all 200: the breaker is checked per page,
        # so it trips within a page of the threshold.
        assert result.items_processed < 200
        assert result.items_failed >= 25
        assert result.items_failed <= 40

    def test_intermittent_failures_do_not_abort(self) -> None:
        """The counter is CONSECUTIVE: scattered transient errors must not stop a
        run that is otherwise healthy."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        seen = {"n": 0}

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                seen["n"] += 1
                n = seen["n"]
            if n % 10 == 0:  # every 10th write fails, never 25 in a row
                raise RuntimeError("transient")

        runner._update_item = record  # type: ignore[method-assign]
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(10)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col",
                fix_zarr_media_type,
                "fix_zarr_media_type",
                max_consecutive_failures=25,
            )

        assert result.aborted is False
        assert result.items_processed == 200  # ran to completion
        assert result.items_failed == 20

    def test_zero_disables_the_breaker(self) -> None:
        runner = self._runner_failing_after(0)
        pages = [[_dirty_item(pg * 10 + i) for i in range(10)] for pg in range(5)]
        search = _multi_page_search(pages, total=50)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_consecutive_failures=0
            )

        assert result.aborted is False
        assert result.items_processed == 50  # ran to completion despite all failing
        assert result.items_failed == 50

    def test_healthy_run_is_not_marked_aborted(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")
        runner._update_item = MagicMock()  # type: ignore[method-assign]
        search = _multi_page_search([[_dirty_item(i) for i in range(10)]], total=10)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration("test-col", fix_zarr_media_type, "fix_zarr_media_type")

        assert result.aborted is False
        assert result.items_modified == 10

    def test_negative_threshold_rejected(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")
        search = _make_mock_search([_dirty_item(0)], total=1)
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            with pytest.raises(ValueError, match="max_consecutive_failures must be >= 0"):
                runner.run_migration("c", fix_zarr_media_type, "m", max_consecutive_failures=-1)

    def test_negative_concurrency_rejected(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")
        search = _make_mock_search([_dirty_item(0)], total=1)
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            with pytest.raises(ValueError, match="concurrency must be >= 1"):
                runner.run_migration("c", fix_zarr_media_type, "m", concurrency=0)


class TestMaxWrites:
    """A bounded run must be expressible IN the tool.

    Bounding it from outside (watch a file, send a signal) is how a prod run once
    blew past its 10k limit to 20,613 writes: a process backgrounded from a
    non-interactive shell inherits SIGINT=SIG_IGN, so CPython never installs its
    handler and `kill -INT` is a silent no-op. Killing it then tore 3 items between
    their DELETE and their POST. `--max-writes` stops at an item boundary with no
    signal and no kill, so nothing can be left torn.
    """

    def _runner_recording(self) -> tuple[STACMigrationRunner, list]:
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        written: list[str] = []

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                written.append(item_id)

        runner._update_item = record  # type: ignore[method-assign]
        return runner, written

    def test_stops_at_exactly_max_writes(self) -> None:
        runner, written = self._runner_recording()
        # 10 pages x 20 stampable items = 200 candidates, budget 25.
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(10)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col",
                fix_zarr_media_type,
                "fix_zarr_media_type",
                concurrency=8,
                max_writes=25,
            )

        assert len(written) == 25, f"must write EXACTLY 25, wrote {len(written)}"
        assert result.items_modified == 25
        assert result.reached_max_writes is True

    def test_counters_stay_consistent_when_bounded(self) -> None:
        """The budget is checked BEFORE migration_fn runs, so an item that is never
        written is never counted as processed — otherwise the tally (and
        stamp_expires' histogram) would not reconcile."""
        runner, _ = self._runner_recording()
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(10)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_writes=25
            )

        assert result.items_processed == result.items_modified + result.items_skipped + (
            result.items_failed
        )
        assert result.items_processed == 25  # only the classified ones counted

    def test_stamp_expires_histogram_reconciles_when_bounded(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The real proof: report() must not warn on a bounded run."""
        from _migrate_catalog.migrations.stamp_expires import report

        # Pin an empty exclude file: unset, resolve_exclude_ids() falls back to the
        # baked demo list, whose ids match nothing here and would trip an unrelated
        # "matched no item" warning.
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()

        runner, _ = self._runner_recording()
        pages = [[_stampable_item(item_id=f"s-{pg}-{i}") for i in range(20)] for pg in range(10)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", stamp_expires, "stamp_expires", concurrency=4, max_writes=30
            )

        assert result.items_modified == 30
        assert SKIP_HISTOGRAM["stamped"] == 30
        text = report(result)
        assert "does not reconcile" not in text
        assert "WARNING" not in text

    def test_none_means_unbounded(self) -> None:
        runner, written = self._runner_recording()
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(5)]
        search = _multi_page_search(pages, total=100)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_writes=None
            )

        assert len(written) == 100
        assert result.reached_max_writes is False

    def test_budget_larger_than_collection_is_not_flagged(self) -> None:
        runner, written = self._runner_recording()
        search = _multi_page_search([[_dirty_item(i) for i in range(10)]], total=10)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_writes=999
            )

        assert len(written) == 10
        assert result.reached_max_writes is False  # finished naturally, not bounded

    def test_budget_exactly_equal_to_writable_size_is_not_flagged(self) -> None:
        """Budget == what the collection needs: the run finished, it wasn't cut
        short, so reached_max_writes stays False (same as budget > collection)."""
        runner, written = self._runner_recording()
        search = _multi_page_search([[_dirty_item(i) for i in range(10)]], total=10)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_writes=10
            )

        assert len(written) == 10
        assert result.reached_max_writes is False

    def test_failed_writes_consume_budget(self) -> None:
        """max_writes bounds ATTEMPTS, not successes — a failed write may already
        have landed its DELETE, so it has spent real blast radius. Counting only
        successes would keep retrying past N on exactly the failing run you most
        want bounded."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        attempts: list[str] = []

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                attempts.append(item_id)
                n = len(attempts)
            if n % 2 == 0:
                raise RuntimeError("503")

        runner._update_item = record  # type: ignore[method-assign]
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(10)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col",
                fix_zarr_media_type,
                "fix_zarr_media_type",
                max_writes=10,
                max_consecutive_failures=0,
            )

        assert len(attempts) == 10, "budget bounds attempts, not successes"
        assert result.items_modified + result.items_failed == 10
        assert result.items_failed > 0
        assert result.items_modified < 10  # fewer modified than the budget, by design

    def test_stops_fetching_pages_once_budget_is_spent(self) -> None:
        """The page-level break must stop PAGINATION too, not just the writes.

        Without it the item-level check still yields exactly N writes, so the tally
        looks right — but the run keeps fetching every remaining page. At prod scale
        (191k items, page_size 100, budget 10k) that is ~1,810 pointless POST /search
        round-trips against the API whose latency the bounded run exists to watch,
        while the tool appears hung.
        """
        runner, written = self._runner_recording()
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(50)]
        fetched: list[int] = []

        def page_gen():  # noqa: ANN202
            for i, p in enumerate(pages):
                fetched.append(i)
                yield {"features": p}

        search = MagicMock()
        search.matched.return_value = 1000
        search.pages_as_dicts.side_effect = lambda: page_gen()

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_writes=25
            )

        assert len(written) == 25
        # 25 writes needs pages 1-2 (20 + 5); page 2 also trips the flag.
        assert len(fetched) <= 3, f"kept paginating after the budget: {len(fetched)} pages fetched"

    def test_skips_do_not_consume_budget(self) -> None:
        """The prod head is ~21k already-stamped items. A budget must be spent on
        real writes, not burned by skips, or a bounded run does nothing."""
        runner, written = self._runner_recording()
        page = [_clean_item(i) for i in range(50)] + [_dirty_item(i) for i in range(20)]
        search = _multi_page_search([page], total=70)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col", fix_zarr_media_type, "fix_zarr_media_type", max_writes=5
            )

        assert len(written) == 5
        assert result.items_skipped == 50  # all skips seen, none consumed budget

    def test_dry_run_bound_counts_would_be_writes(self) -> None:
        runner, written = self._runner_recording()
        pages = [[_dirty_item(pg * 20 + i) for i in range(20)] for pg in range(10)]
        search = _multi_page_search(pages, total=200)

        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            result = runner.run_migration(
                "test-col",
                fix_zarr_media_type,
                "fix_zarr_media_type",
                dry_run=True,
                max_writes=15,
            )

        assert result.items_modified == 15
        assert written == []
        assert result.reached_max_writes is True

    def test_negative_max_writes_rejected(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")
        search = _make_mock_search([_dirty_item(0)], total=1)
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = search
            with pytest.raises(ValueError, match="max_writes must be >= 1"):
                runner.run_migration("c", fix_zarr_media_type, "m", max_writes=0)


class TestInterruptCancelsQueuedWrites:
    """An operator WILL Ctrl-C this tool (the rollout plan says to stop and restart
    it). Queued writes must be cancelled rather than drained into prod after the
    stop, and in-flight writes must be allowed to finish so no item is left torn
    between its DELETE and its POST.

    The interrupt is injected via the progress bar because that is where the real
    hazard lives: the main thread is then OUTSIDE pool.map's generator, which keeps
    the generator alive through the unwind so its own `finally: future.cancel()`
    never runs. (An interrupt raised inside the generator cancels by itself, so a
    test that injects it there passes with or without the fix and proves nothing.)
    """

    def _run_interrupted(self, n_items: int, concurrency: int) -> tuple[list[str], list[str]]:
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        started: list[str] = []
        finished: list[str] = []

        def record(collection_id, item_id, item_dict):  # noqa: ANN001, ANN202
            with lock:
                started.append(item_id)
            time.sleep(0.005)  # the DELETE -> POST window
            with lock:
                finished.append(item_id)

        runner._update_item = record  # type: ignore[method-assign]

        # Ctrl-C once a few writes have been tallied: main thread is in bar.update,
        # between two yields of the generator, with the rest still queued.
        updates = {"n": 0}
        bar = MagicMock()

        def update(_n: int) -> None:
            updates["n"] += 1
            if updates["n"] == 5:
                raise KeyboardInterrupt

        bar.update.side_effect = update

        search = _make_mock_search([_dirty_item(i) for i in range(n_items)], total=n_items)
        with (
            patch("_migrate_catalog.runner.Client") as mock_client,
            patch("_migrate_catalog.runner.click.progressbar") as mock_pb,
        ):
            mock_client.open.return_value.search.return_value = search
            mock_pb.return_value.__enter__.return_value = bar
            with pytest.raises(KeyboardInterrupt):
                runner.run_migration(
                    "test-col", fix_zarr_media_type, "fix_zarr_media_type", concurrency=concurrency
                )
        return started, finished

    def test_queued_writes_are_cancelled_not_drained(self) -> None:
        n = 400
        started, _ = self._run_interrupted(n_items=n, concurrency=4)
        assert len(started) < n, (
            f"Ctrl-C must cancel queued writes, but {len(started)}/{n} were still "
            f"pushed to the API after the operator asked to stop"
        )

    def test_in_flight_writes_finish_cleanly(self) -> None:
        """No item may be left between its DELETE and its POST."""
        started, finished = self._run_interrupted(n_items=400, concurrency=4)
        torn = set(started) - set(finished)
        assert torn == set(), f"in-flight writes must drain; torn={torn}"


class TestStampExpiresUnderConcurrency:
    def test_histogram_reconciles_under_concurrency(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """End-to-end proof that the real stamp_expires migration keeps a correct
        histogram at concurrency > 1 — i.e. report() does not warn."""
        from _migrate_catalog.migrations.stamp_expires import report

        # Pin an explicit exclude file: unset, resolve_exclude_ids() falls back to
        # the baked demo denylist, whose ids match nothing here and would trip the
        # unrelated "matched no item" warning.
        exclude_file = tmp_path / "exclude.txt"
        exclude_file.write_text("demo_keep\n")
        monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(exclude_file))
        monkeypatch.delenv("EXPIRES_MIN_DATETIME", raising=False)
        reset_histogram()

        items = [_stampable_item(item_id=f"s-{i}") for i in range(20)]
        items += [
            _stampable_item(item_id=f"done-{i}", expires="2025-07-01T00:00:00Z") for i in range(10)
        ]
        items += [_stampable_item(item_id="demo_keep")]

        runner = STACMigrationRunner("https://api.example.com/stac")
        runner._update_item = MagicMock()  # type: ignore[method-assign]
        mock_search = _make_mock_search(items, total=len(items))
        with patch("_migrate_catalog.runner.Client") as mock_client:
            mock_client.open.return_value.search.return_value = mock_search
            result = runner.run_migration("test-col", stamp_expires, "stamp_expires", concurrency=4)

        assert result.items_modified == 20
        assert result.items_skipped == 11  # 10 already_stamped + 1 excluded
        assert SKIP_HISTOGRAM["stamped"] == 20
        assert SKIP_HISTOGRAM["already_stamped"] == 10
        assert SKIP_HISTOGRAM["excluded"] == 1

        text = report(result)
        assert "does not reconcile" not in text
        assert "matched no item" not in text
        assert "WARNING" not in text


class TestRecoveryFileThreadSafety:
    def _mock_out_http(self, runner: STACMigrationRunner) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.post.return_value = mock_resp
        runner._session = MagicMock(return_value=mock_session)  # type: ignore[method-assign]

    def test_concurrent_appends_yield_intact_json_lines(self, tmp_path: Path) -> None:
        """Contract: concurrent writes leave one intact, parseable JSON line per
        item — the restore path reads this file line by line.

        Note this is a regression guard, not proof that the lock works: a buffered
        O_APPEND write of this size turns out to be atomic anyway (verified by
        removing the lock — this still passed). It fails if a future change breaks
        line integrity some other way, e.g. by sharing one open handle across
        threads. test_recovery_append_happens_under_the_lock covers the lock itself.
        """
        runner = STACMigrationRunner("https://api.example.com/stac", recovery_dir=tmp_path)
        self._mock_out_http(runner)

        n = 40
        items = [
            {"id": f"item-{i}", "properties": {"pad": "x" * 60_000}, "assets": {}} for i in range(n)
        ]

        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(lambda it: runner._update_item("test-col", it["id"], it), items))

        files = list(tmp_path.glob(".migration_recovery_*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text().strip().splitlines()
        assert len(lines) == n
        ids = {json.loads(line)["id"] for line in lines}  # raises if any line interleaved
        assert ids == {f"item-{i}" for i in range(n)}

    def test_recovery_append_happens_under_the_lock(self, tmp_path: Path) -> None:
        """The append must happen *inside* _recovery_lock, so line integrity does
        not depend on the platform's append-atomicity behaviour."""
        runner = STACMigrationRunner("https://api.example.com/stac", recovery_dir=tmp_path)
        self._mock_out_http(runner)
        assert runner._recovery_file is not None
        recovery_file = runner._recovery_file

        def size() -> int:
            return recovery_file.stat().st_size if recovery_file.exists() else 0

        sizes: dict[str, int] = {}
        inner = threading.Lock()

        class RecordingLock:
            def __enter__(self) -> None:
                inner.acquire()
                sizes["at_acquire"] = size()

            def __exit__(self, *exc: object) -> None:
                sizes["at_release"] = size()
                inner.release()

        runner._recovery_lock = RecordingLock()  # type: ignore[assignment]
        runner._update_item("test-col", "item-1", {"id": "item-1", "pad": "x" * 1000})

        assert sizes, "the recovery append never acquired the lock"
        assert sizes["at_acquire"] == 0
        assert sizes["at_release"] > 0, "the append must land inside the lock, not after it"


class TestSessionThreadLocality:
    def test_each_thread_gets_its_own_session(self) -> None:
        """requests.Session is not guaranteed thread-safe, so pooled writes must
        not share one."""
        runner = STACMigrationRunner("https://api.example.com/stac")
        lock = threading.Lock()
        sessions: list[int] = []
        ready = threading.Barrier(4, timeout=30)

        def grab(_: int) -> None:
            ready.wait()  # hold all 4 threads alive at once so ids can't be recycled
            session = runner._session()
            with lock:
                sessions.append(id(session))
            ready.wait()

        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(grab, range(4)))

        assert len(set(sessions)) == 4, "each worker thread needs a distinct Session"

    def test_same_thread_reuses_its_session(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")
        assert runner._session() is runner._session()

    def test_session_carries_json_content_type(self) -> None:
        runner = STACMigrationRunner("https://api.example.com/stac")

        def header() -> str:
            return runner._session().headers["Content-Type"]

        with ThreadPoolExecutor(max_workers=1) as ex:
            worker_header = ex.submit(header).result()

        assert header() == "application/json"
        assert worker_header == "application/json"


class TestConcurrencyCliOption:
    def _invoke(self, monkeypatch: pytest.MonkeyPatch, args: list[str]):  # noqa: ANN202
        import importlib

        from click.testing import CliRunner

        climod = importlib.import_module("_migrate_catalog.cli")
        runner_inst = MagicMock()
        runner_inst.run_migration.return_value = _result(processed=0, modified=0, skipped=0)
        monkeypatch.setattr(climod, "STACMigrationRunner", lambda *a, **k: runner_inst)
        res = CliRunner().invoke(climod.cli, args)
        return res, runner_inst

    def test_concurrency_flag_is_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        res, runner_inst = self._invoke(
            monkeypatch,
            ["run", "coll", "--migration", "fix_url_encoding", "--dry-run", "--concurrency", "8"],
        )
        assert res.exit_code == 0, res.output
        assert runner_inst.run_migration.call_args.kwargs["concurrency"] == 8

    def test_concurrency_defaults_to_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default 1 keeps every other migration on today's sequential path."""
        res, runner_inst = self._invoke(
            monkeypatch, ["run", "coll", "--migration", "fix_url_encoding", "--dry-run"]
        )
        assert res.exit_code == 0, res.output
        assert runner_inst.run_migration.call_args.kwargs["concurrency"] == 1

    def test_max_writes_flag_is_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """THE test for the incident. A bound the CLI parses, advertises in --help,
        and then quietly drops is worse than no bound at all: the operator believes
        the run is capped at 10k and it runs to 191k. Pin the wire, not just the
        mechanism."""
        res, runner_inst = self._invoke(
            monkeypatch,
            [
                "run",
                "coll",
                "--migration",
                "fix_url_encoding",
                "--dry-run",
                "--max-writes",
                "10000",
            ],
        )
        assert res.exit_code == 0, res.output
        assert runner_inst.run_migration.call_args.kwargs["max_writes"] == 10000

    def test_max_writes_defaults_to_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        res, runner_inst = self._invoke(
            monkeypatch, ["run", "coll", "--migration", "fix_url_encoding", "--dry-run"]
        )
        assert res.exit_code == 0, res.output
        assert runner_inst.run_migration.call_args.kwargs["max_writes"] is None

    def test_max_consecutive_failures_flag_is_forwarded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        res, runner_inst = self._invoke(
            monkeypatch,
            [
                "run",
                "coll",
                "--migration",
                "fix_url_encoding",
                "--dry-run",
                "--max-consecutive-failures",
                "3",
            ],
        )
        assert res.exit_code == 0, res.output
        assert runner_inst.run_migration.call_args.kwargs["max_consecutive_failures"] == 3

    def test_max_writes_rejects_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        res, _ = self._invoke(
            monkeypatch,
            ["run", "coll", "--migration", "fix_url_encoding", "--dry-run", "--max-writes", "0"],
        )
        assert res.exit_code != 0  # click.IntRange(1, None) rejects it


class TestCliStopPaths:
    def _climod(self):  # noqa: ANN202
        import importlib

        return importlib.import_module("_migrate_catalog.cli")

    def test_keyboard_interrupt_exits_130_and_points_at_max_writes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An operator's wrapper keys on the exit code, and the operator needs to be
        told the tally is gone and that --max-writes is the supported bound."""
        from click.testing import CliRunner

        climod = self._climod()
        runner_inst = MagicMock()
        runner_inst.run_migration.side_effect = KeyboardInterrupt
        monkeypatch.setattr(climod, "STACMigrationRunner", lambda *a, **k: runner_inst)

        res = CliRunner().invoke(
            climod.cli, ["run", "coll", "--migration", "fix_url_encoding", "--yes"]
        )

        assert res.exit_code == 130, f"expected 128+SIGINT, got {res.exit_code}"
        assert "--max-writes" in res.output
        assert "idempotent" in res.output

    def test_bounded_run_is_not_labelled_a_success_when_it_also_aborted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """aborted + reached_max_writes can both be set (budget spent on the same
        page the breaker trips). "(bounded run)" must not appear over a run that the
        API was rejecting wholesale — that reads as "went to plan"."""
        from click.testing import CliRunner

        climod = self._climod()
        runner_inst = MagicMock()
        result = _result(processed=25, modified=0, skipped=0, failed=25)
        result.aborted = True
        result.reached_max_writes = True
        runner_inst.run_migration.return_value = result
        monkeypatch.setattr(climod, "STACMigrationRunner", lambda *a, **k: runner_inst)
        monkeypatch.setattr(climod, "record_run", lambda *a, **k: None)
        monkeypatch.setattr(climod, "was_migration_run", lambda *a, **k: False)

        res = CliRunner().invoke(
            climod.cli,
            ["run", "coll", "--migration", "fix_url_encoding", "--yes", "--max-writes", "25"],
        )

        assert res.exit_code == 1  # the abort dominates
        assert "ABORTED" in res.output
        assert "bounded run" not in res.output, "an aborted run must not read as a success"


# --- add_eodash_rasterform (issue #348) --------------------------------------

_ASC_FORM = (
    "https://raw.githubusercontent.com/EOPF-Explorer/eodash-assets/"
    "refs/heads/main/forms/s1-asc-bandsform.json"
)
_DESC_FORM = (
    "https://raw.githubusercontent.com/EOPF-Explorer/eodash-assets/"
    "refs/heads/main/forms/s1-desc-bandsform.json"
)


def _s1_item(orbit: str | None = "descending", form: str | None = None) -> dict:
    props: dict = {"datetime": "2026-06-05T06:09:07Z"}
    if orbit is not None:
        props["sat:orbit_state"] = orbit
    if form is not None:
        props["eodash:rasterform"] = form
    return {"id": "s1-rtc-31TCH-20260605t060907", "properties": props, "links": [], "assets": {}}


class TestAddEodashRasterform:
    def test_descending_item_gets_desc_form(self):
        result = add_eodash_rasterform(_s1_item(orbit="descending"))
        assert result is not None
        assert result["properties"]["eodash:rasterform"] == _DESC_FORM

    def test_ascending_item_gets_asc_form(self):
        result = add_eodash_rasterform(_s1_item(orbit="ascending"))
        assert result is not None
        assert result["properties"]["eodash:rasterform"] == _ASC_FORM

    def test_item_without_orbit_is_skipped(self):
        """No orbit -> no single render target -> nothing correct to write."""
        assert add_eodash_rasterform(_s1_item(orbit=None)) is None

    def test_unknown_orbit_is_skipped(self):
        assert add_eodash_rasterform(_s1_item(orbit="sideways")) is None

    def test_already_correct_is_skipped(self):
        """Idempotent: a second run must not rewrite (and re-PUT) every item."""
        assert add_eodash_rasterform(_s1_item(orbit="descending", form=_DESC_FORM)) is None

    def test_stale_form_is_corrected(self):
        """The point of comparing by value: an item whose orbit flipped but whose form did not.

        Key-presence idempotence would leave this item permanently wrong.
        """
        result = add_eodash_rasterform(_s1_item(orbit="descending", form=_ASC_FORM))
        assert result is not None
        assert result["properties"]["eodash:rasterform"] == _DESC_FORM

    def test_s2_item_untouched(self):
        """S2 items carry no sat:orbit_state, so the migration is S2-safe by construction."""
        s2 = {
            "id": "S2A_MSIL2A_x",
            "properties": {"datetime": "2026-06-05T06:09:07Z", "eo:cloud_cover": 12},
            "links": [],
            "assets": {},
        }
        assert add_eodash_rasterform(s2) is None

    def test_does_not_mutate_the_input(self):
        """apply_item_transform deep-copies; the caller's dict must be untouched."""
        item = _s1_item(orbit="descending")
        add_eodash_rasterform(item)
        assert "eodash:rasterform" not in item["properties"]

    def test_form_urls_bind_to_the_shared_constants(self):
        """Drift guard: the migration and the emitters must agree byte-for-byte.

        They live in packages that cannot import each other at runtime, so the URLs are
        asserted here against scripts/eodash_rasterform.py (mirrors the stac_link_titles guard).
        """
        from eodash_rasterform import RASTERFORM_BY_ORBIT

        expected = {"ascending": _ASC_FORM, "descending": _DESC_FORM}
        assert expected == RASTERFORM_BY_ORBIT
