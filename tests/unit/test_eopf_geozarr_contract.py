"""Contract tests for the pinned eopf-geozarr OLCI converter.

The pin in pyproject.toml points at an OPEN pull request on a contributor fork
(data-model #212), so the dependency changes shape under us without a version bump —
``importlib.metadata`` reports 0.10.2 for every SHA, so a version assertion is useless.
It has already happened twice:

* bands moved from ``measurements/<band>`` to ``measurements/r0/<band>``, silently
  dead-linking every STAC asset href;
* the grid parameter was renamed ``target_crs`` -> ``output_grid`` and its default flipped
  from ``EPSG:4326`` to ``native``, silently turning the tileable output into a CRS-less
  swath store.

Neither was caught by the existing tests, because ``tests/unit/test_convert_v1_s3.py``
asserts against a ``MagicMock`` that happily accepts any signature. These two classes close
that gap: one watches the call signature, the other converts a tiny synthetic product and
asserts the real output shape that the register/visualization stack depends on.
"""

import json
import sys
import warnings
from importlib.metadata import Distribution
from inspect import signature
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from eopf_geozarr.s3_olci_optimization.olci_band_mapping import OLCI_BANDS
from eopf_geozarr.s3_olci_optimization.olci_converter import convert_olci_optimized

scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import convert_v1_s3  # noqa: E402
import register_v1  # noqa: E402

# Snapshot of the converter's full parameter set, defaults included. Deliberately the WHOLE
# set and not just the arguments we pass: the regression that bit us was an upstream
# parameter we did NOT pass whose default changed underneath. This is a canary, not a
# dependency — we pass every behaviour-affecting parameter explicitly (see
# scripts/convert_v1_s3.py), so a failure here means "read the upstream diff, decide whether
# it affects us, then update this snapshot", never "restore the old upstream behaviour".
_EXPECTED_SIGNATURE = {
    "dt_input": None,
    "output_path": None,
    "enable_sharding": False,
    "spatial_chunk": 1024,
    "compression_level": 3,
    "min_dimension": 256,
    "keep_scale_offset": False,
    # Renamed from `target_crs` and flipped from "EPSG:4326" to "native" at e8236b0d.
    # The pipeline pins EPSG:4326 explicitly and does not inherit this.
    "output_grid": "native",
}

# The kwarg that selects the output grid, named once rather than inline so the rename
# above is a one-line change here.
_GRID_KWARG = "output_grid"


def _installed_sha() -> str:
    """The git SHA actually installed, so failures name the version that broke the contract."""
    try:
        raw = Distribution.from_name("eopf-geozarr").read_text("direct_url.json")
        return json.loads(raw)["vcs_info"]["commit_id"][:12]
    except Exception:  # pragma: no cover - diagnostics only
        return "unknown"


class TestConverterSignature:
    def test_signature_matches_snapshot(self):
        actual = {
            name: (None if param.default is param.empty else param.default)
            for name, param in signature(convert_olci_optimized).parameters.items()
        }
        assert actual == _EXPECTED_SIGNATURE, (
            f"convert_olci_optimized signature changed in eopf-geozarr {_installed_sha()}.\n"
            "Read the upstream diff, decide whether it affects the pipeline, then update "
            "_EXPECTED_SIGNATURE. If a parameter was renamed or its default flipped, check "
            "scripts/convert_v1_s3.py still passes it explicitly."
        )


@pytest.fixture(scope="module")
def converted_store(tmp_path_factory) -> xr.DataTree:
    """Convert a tiny synthetic OLCI product on the same grid the pipeline requests.

    64x64 with min_dimension=16 so the pyramid actually generates overview siblings.
    Runs in well under a second: no network, no S3, no dask.
    """
    rows = cols = 64
    lat = np.linspace(45.0, 44.0, rows)[:, None] + np.zeros((1, cols))
    lon = np.linspace(10.0, 11.0, cols)[None, :] + np.zeros((rows, 1))
    rng = np.random.default_rng(0)
    bands = {
        band: (
            ("rows", "columns"),
            rng.integers(0, 5000, (rows, cols), dtype=np.uint16),
            {"_FillValue": 65535},
        )
        for band, _ in register_v1._S3_OLCI_FALSE_COLOR_BANDS
    }
    measurements = xr.Dataset(
        bands,
        coords={
            "latitude": (("rows", "columns"), lat),
            "longitude": (("rows", "columns"), lon),
        },
    )
    # `orphans` is the one non-level sibling the pipeline knows about; include it so the
    # sibling assertion below is exercised rather than vacuous.
    orphans = xr.Dataset({"removed_pixels": (("rows",), np.zeros(rows, dtype=np.uint16))})
    dt_input = xr.DataTree.from_dict(
        {"/measurements": measurements, "/measurements/orphans": orphans}
    )

    out = tmp_path_factory.mktemp("olci") / "out.zarr"
    return convert_olci_optimized(
        dt_input=dt_input,
        output_path=str(out),
        min_dimension=16,
        **{_GRID_KWARG: "EPSG:4326"},
    )


class TestConverterOutputContract:
    """Each assertion pins a shape some part of the pipeline already depends on."""

    def test_base_level_group_exists(self, converted_store):
        """`_S3_OLCI_BASE_LEVEL` + remap_olci_measurement_paths rewrite hrefs to this group."""
        level = register_v1._S3_OLCI_BASE_LEVEL
        assert level in converted_store["measurements"].children
        band = register_v1._S3_OLCI_FALSE_COLOR_BANDS[0][0]
        assert band in converted_store[f"measurements/{level}"].ds.data_vars

    def test_base_level_is_a_regular_grid_with_crs(self, converted_store):
        """titiler needs an affine grid and a declared CRS; the whole viz stack rests on it."""
        r0 = converted_store[f"measurements/{register_v1._S3_OLCI_BASE_LEVEL}"].ds
        band = register_v1._S3_OLCI_FALSE_COLOR_BANDS[0][0]
        assert r0[band].dims == ("y", "x")
        assert r0[band].attrs.get("grid_mapping") == "spatial_ref"
        assert "crs_wkt" in r0.coords["spatial_ref"].attrs

    def test_per_pixel_geolocation_is_absent_on_the_requested_grid(self, converted_store):
        """The native (swath) output keeps 2-D latitude/longitude; the regridded one must not.

        This is the assertion that goes red if the pipeline ever stops requesting a CRS and
        silently inherits the library's `native` default.
        """
        r0 = converted_store[f"measurements/{register_v1._S3_OLCI_BASE_LEVEL}"].ds
        assert "latitude" not in r0.variables
        assert "longitude" not in r0.variables

    def test_whole_store_opens_as_a_datatree(self, converted_store):
        """Regression guard for `group '/measurements/r2' is not aligned with its parents`."""
        store = converted_store.encoding.get("source") or converted_store["measurements"].encoding
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            reopened = xr.open_datatree(store, engine="zarr", consolidated=False)
        assert register_v1._S3_OLCI_BASE_LEVEL in reopened["measurements"].children

    def test_overview_siblings_are_recognised_as_levels(self, converted_store):
        """`_is_olci_level` decides what the remap skips; it must match real level names."""
        children = set(converted_store["measurements"].children)
        levels = {c for c in children if register_v1._is_olci_level(c)}
        assert len(levels) > 1, f"expected a pyramid, got {sorted(children)}"

    def test_non_level_children_are_all_known_siblings(self, converted_store):
        """An unknown sibling would be rewritten to <level>/<sibling> and dead-link."""
        children = set(converted_store["measurements"].children)
        non_levels = {c for c in children if not register_v1._is_olci_level(c)}
        assert non_levels <= register_v1._S3_OLCI_MEASUREMENT_SIBLINGS, (
            f"unknown non-level children {sorted(non_levels - register_v1._S3_OLCI_MEASUREMENT_SIBLINGS)}; "
            "remap_olci_measurement_paths would rewrite them into the base level and 404"
        )

    def test_false_colour_bands_exist_upstream(self, converted_store):
        """The viz tests only compare our constants to themselves; this checks reality."""
        for band, _ in register_v1._S3_OLCI_FALSE_COLOR_BANDS:
            assert band in OLCI_BANDS

    def test_base_level_constant_agrees_across_scripts(self):
        """convert_v1_s3 duplicates the level name instead of importing register_v1.

        Cheap to duplicate, expensive to let drift: convert would verify the CRS on one
        group while register rewrote asset hrefs to another.
        """
        assert convert_v1_s3.BASE_LEVEL == register_v1._S3_OLCI_BASE_LEVEL
