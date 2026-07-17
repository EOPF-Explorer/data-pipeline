"""S1 GRD RTC quality gate — validate a GeoZarr store/cube; exit 0=PASS / 1=WARN / 2=FAIL.

Wraps the `validate_s1_grd_rtc` notebook's checks as a CLI for the Argo quality-gate step
(plan T1) and as the notebook-validation core (plan T5). The pure check functions are unit-tested;
`main()` opens the store (structural `S1RtcRoot` check + per-level data checks) and exits with the
worst severity found.

Usage:
    uv run python scripts/validate_s1_rtc.py --store <zarr-uri> [--orbit descending] [--res r10m]
exit code = 0 PASS · 1 WARN · 2 FAIL
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import numpy as np
import xarray as xr


class Level(IntEnum):
    PASS = 0
    WARN = 1
    FAIL = 2


@dataclass
class Check:
    level: Level
    label: str
    detail: str


def overall(checks: list[Check]) -> Level:
    """Worst severity across checks (PASS if none)."""
    return max((c.level for c in checks), default=Level.PASS)


# --- pure checks (unit-tested) ----------------------------------------------


def check_finite(
    name: str, arr: np.ndarray, *, fail_below: float = 0.5, warn_below: float = 0.99
) -> Check:
    """Fraction of finite samples: < fail_below → FAIL, < warn_below → WARN, else PASS."""
    a = np.asarray(arr)
    finite = float(np.isfinite(a).mean()) if a.size else 0.0
    detail = f"{finite * 100:.1f}% finite"
    if finite < fail_below:
        return Check(Level.FAIL, f"{name} finite data", detail)
    if finite < warn_below:
        return Check(Level.WARN, f"{name} finite data", detail)
    return Check(Level.PASS, f"{name} finite data", detail)


def check_dtype_dims(
    ds: xr.Dataset, var: str, want_dtype: str, want_dims: tuple[str, ...] = ("time", "y", "x")
) -> Check:
    """Variable present with the expected dtype and dimension order."""
    if var not in ds:
        return Check(Level.FAIL, f"{var} present", "missing")
    got_dtype = str(ds[var].dtype)
    if got_dtype != want_dtype:
        return Check(Level.FAIL, f"{var} dtype", f"{got_dtype} (want {want_dtype})")
    got_dims = tuple(ds[var].dims)
    if got_dims != want_dims:
        return Check(Level.FAIL, f"{var} dims", f"{got_dims} (want {want_dims})")
    return Check(Level.PASS, f"{var} dtype/dims", f"{got_dtype} {got_dims}")


def check_crs(ds: xr.Dataset) -> Check:
    """A resolvable CRS via rioxarray (CF grid_mapping wired) — WARN if absent."""
    import rioxarray  # noqa: F401  -- imported for its side effect: registers the .rio accessor

    try:
        crs = ds["vv"].rio.crs if "vv" in ds else ds.rio.crs
    except Exception:  # noqa: BLE001 -- rioxarray raises various errors when no CRS is resolvable
        crs = None
    if crs is None:
        return Check(Level.WARN, "grid_mapping wired (.rio.crs)", "no CRS — needs CF grid_mapping")
    return Check(Level.PASS, "grid_mapping wired (.rio.crs)", str(crs))


def check_db_range(
    name: str, arr_linear: np.ndarray, *, lo_db: float = -40.0, hi_db: float = 10.0
) -> Check:
    """p2..p98 of the backscatter in dB should sit within plausible bounds (else WARN)."""
    a = np.asarray(arr_linear, dtype="float64")
    a = a[np.isfinite(a) & (a > 0)]
    if a.size == 0:
        return Check(Level.WARN, f"{name} dB range", "no positive finite samples")
    db = 10.0 * np.log10(a)
    p2, p98 = (float(v) for v in np.percentile(db, [2, 98]))
    detail = f"p2..p98 = [{p2:.1f}, {p98:.1f}] dB"
    if p2 < lo_db or p98 > hi_db:
        return Check(Level.WARN, f"{name} dB range", f"{detail} outside [{lo_db}, {hi_db}]")
    return Check(Level.PASS, f"{name} dB range", detail)


def validate_schema(ds: xr.Dataset) -> list[Check]:
    """Cheap (metadata-only) checks: dtype/dims + CRS. Run on the native resolution."""
    return [
        check_dtype_dims(ds, "vv", "float32"),
        check_dtype_dims(ds, "vh", "float32"),
        check_dtype_dims(ds, "border_mask", "uint8"),
        check_crs(ds),
    ]


def validate_data(ds: xr.Dataset) -> list[Check]:
    """Data-sanity checks (finite fraction, dB range) over vv/vh of the given dataset.

    Pass a coarse overview (cheap) and/or a single ``time`` slice (gate the new acquisition).
    """
    checks: list[Check] = []
    for var in ("vv", "vh"):
        if var in ds:
            arr = np.asarray(ds[var].values)
            checks.append(check_finite(var, arr))
            checks.append(check_db_range(var, arr))
    return checks


def validate_dataset(ds: xr.Dataset) -> list[Check]:
    """Schema + data checks over one opened dataset (used by tests / simple single-level runs)."""
    return validate_schema(ds) + validate_data(ds)


# --- store I/O + structural check (integration; exercised in main) ----------


def check_structural(root: Any) -> Check:
    """Validate a store root against the S1RtcRoot model; known x/y/spatial_ref drift → WARN."""
    try:
        from eopf_geozarr.data_api.s1_rtc import S1RtcRoot
        from pydantic import ValidationError
    except Exception as exc:  # noqa: BLE001  -- optional dep; degrade to WARN
        return Check(Level.WARN, "Strict schema (S1RtcRoot)", f"unavailable: {exc}")
    try:
        S1RtcRoot.from_zarr(root)
        return Check(Level.PASS, "Strict schema (S1RtcRoot)", "validates")
    except ValidationError as exc:
        known = ("x", "y", "spatial_ref")
        errs = exc.errors()
        drift = [
            e for e in errs if e["type"] == "extra_forbidden" and e["loc"] and e["loc"][-1] in known
        ]
        real = [e for e in errs if e not in drift]
        if real:
            detail = "; ".join("/".join(map(str, e["loc"])) for e in real[:5])
            return Check(Level.FAIL, "Strict schema (S1RtcRoot)", f"{len(real)} error(s): {detail}")
        return Check(Level.WARN, "Strict schema (S1RtcRoot)", f"known coord drift ({len(drift)})")


def _open_root(store: str) -> Any:
    import zarr

    if "://" in store:
        from zarr.storage import FsspecStore

        return zarr.open_group(FsspecStore.from_url(store), mode="r")
    return zarr.open_group(store, mode="r", zarr_format=3)


def _open_level(store: str, orbit: str, res: str) -> xr.Dataset:
    ds: xr.Dataset = xr.open_zarr(
        store, group=f"{orbit}/{res}", decode_coords="all", consolidated=True
    )
    return ds


def time_index(native: xr.Dataset, when: str) -> int:
    """Nearest `time` index for `when` (ISO) from the native level's time coordinate.

    Overview levels carry the `time` dimension but not always its coordinate/index, so we resolve
    the position on the native level and `isel` it positionally on the overview.
    """
    tvals = np.asarray(native["time"].values).astype("datetime64[ns]")
    target = np.datetime64(when).astype("datetime64[ns]")
    return int(np.argmin(np.abs(tvals - target)))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--store", required=True, help="Zarr store URI (local path or https/s3)")
    ap.add_argument("--orbit", default=None, help="orbit group (default: auto-detect)")
    ap.add_argument("--res", default="r10m", help="native resolution level for schema checks")
    ap.add_argument(
        "--data-res", default="r60m", help="coarse overview for data checks (avoids loading r10m)"
    )
    ap.add_argument(
        "--time",
        default=None,
        help="acquisition datetime to gate (ISO); default validates all times in the cube",
    )
    args = ap.parse_args()

    root = _open_root(args.store)
    members = list(root)
    orbit = args.orbit or next((o for o in ("ascending", "descending") if o in members), None)
    if orbit is None:
        print(f"[FAIL] orbit group — none of ascending/descending in {members}")
        sys.exit(int(Level.FAIL))

    # Schema (cheap) on native; data sanity on a coarse overview, optionally one acquisition slice.
    native = _open_level(args.store, orbit, args.res)
    checks = [check_structural(root), *validate_schema(native)]
    data_ds = _open_level(args.store, orbit, args.data_res)
    if args.time is not None:
        data_ds = data_ds.isel(time=time_index(native, args.time))
    checks += validate_data(data_ds)

    worst = overall(checks)
    for c in checks:
        print(f"[{c.level.name}] {c.label} — {c.detail}")
    scope = f"time={args.time}" if args.time else "all times"
    print(
        f"OVERALL: {worst.name}  (orbit={orbit}, schema={args.res}, data={args.data_res}, {scope})"
    )
    sys.exit(int(worst))


if __name__ == "__main__":
    main()
