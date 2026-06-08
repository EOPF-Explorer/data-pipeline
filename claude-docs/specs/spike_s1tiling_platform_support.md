# Spike: does S1Tiling 1.4.0 support S1C / S1D? (precursor to platform-selection)

**Question**: the S1 GRD RTC pipeline is S1A-only (`platform_list : S1A` in both the local cfg and
the Argo `cfg-base`). The CDSE watcher already *discovers* S1C and S1D scenes but skips them. Before
making `platform_list` selectable, confirm whether the s1tiling 1.4.0 image can actually process
S1C / S1D.

**Method**: inspected the source inside
`registry.orfeo-toolbox.org/s1-tiling/s1tiling:1.4.0-ubuntu-otb9.1.1` (2026-06-08).

## Findings (with evidence)

1. **Platform filter accepts any `S1*`** — `libs/configuration.py:420`:
   `unsupported_platforms = [p for p in platform_list if p and not p.startswith("S1")]`.
   So `platform_list = S1C` (or `S1D`, or empty = all) passes cfg validation; the value is forwarded
   to eodag as `platformSerialIdentifier` (`libs/S1FileManager.py:528,557`). No S1A hardcoding here.

2. **Relative-orbit conversion table omits S1D** — `libs/orbit/_conversions.py:75-79`:
   ```python
   ORBIT_CONVERTERS = {
       "S1A": OrbitConverter(73, 175),
       "S1B": OrbitConverter(27, 175),
       "S1C": OrbitConverter(172, 175),
   }   # <- no S1D
   ```
   RTC needs the relative orbit; computing it for a mission missing from this dict fails (KeyError).

3. **Mission lists elsewhere include S1D but are stale** — `libs/orbit/_file.py:53`
   `ALL_MISSIONS = ("S1A","S1B","S1C","S1D")` with the comment *"Even the yet to be launched S1D is
   listed"*, and the AUX-orbit regex (`_file.py:134`) matches all four. These predate S1D's launch
   (2025) and don't imply orbit-number support — the conversion table (finding 2) is the gate.

## Conclusion

| Mission | s1tiling 1.4.0 | Notes |
|---------|----------------|-------|
| S1A | ✅ supported | current pipeline |
| S1B | ✅ (decommissioned 2022 — moot) | in conversion table |
| **S1C** | **✅ supported** | in conversion table; only a config change to enable |
| **S1D** | **❌ not supported** | absent from `ORBIT_CONVERTERS` → RTC KeyError; needs a newer s1tiling image |

## Recommendation for the platform-selection work

- **Enable S1A + S1C now** (low effort): make `platform_list` selectable in `run_s1tiling.py`'s cfg
  render (and the Argo template's `sed`), defaulting to `S1A S1C` — or set it empty to accept all
  supported missions and let the orbit table reject the rest. The watcher's `query_cdse` already
  surfaces per-product platform; pass it through (or just widen the cfg filter).
- **Defer S1D** until the s1tiling image is bumped to a release whose `ORBIT_CONVERTERS` includes
  S1D. Track as a follow-up; until then the watcher should skip S1D (or it will fail at RTC).
- **Verify S1C end-to-end** before trusting it: run one S1C scene (e.g. 2026-06-06 31TCH) A→B and
  confirm a clean item — finding 2 proves the orbit math exists, but a live run confirms the whole
  RTC chain handles S1C.

## Open question

- Should the watcher **hard-skip S1D** (filter it out in `query_cdse`/`run_watch`) so a daily run
  doesn't repeatedly attempt and fail S1D scenes, until the image supports it? Default: yes, skip
  with a logged note. *Owner: Loïc.*
