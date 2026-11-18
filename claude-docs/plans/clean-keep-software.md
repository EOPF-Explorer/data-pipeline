# Plan: clean `sentinel-2-l2a-new` down to EOPF_GEOZARR >= 0.13.0 (`clean --keep-software-min`)

**Spec**: none, a one-off operator clean. Rule set by Loïc on 2026-11-17: keep the items whose
`processing:software["EOPF_GEOZARR"]` is a version >= 0.13.0, delete every other item, catalogue-only.
**Goal**: the production `sentinel-2-l2a-new` collection holds only items EODC converted with
eopf-geozarr 0.13.0 or later.
**Constraint**: catalogue-only (the Zarr stores are EODC's, on data.eodc.eu); every bound enforced
inside the tool, never by a timeout or a kill; target fixed in the manifest
(`https://api.explorer.eopf.copernicus.eu/stac`, collection `sentinel-2-l2a-new`).

## Current state (2026-11-17)

| Resource | Status |
|---|---|
| `clean --keep-software-min KEY=VERSION` (`operator-tools/manage_collections.py`) | Built, unit-tested offline |
| Re-check (GET) of each candidate just before its DELETE | Built, unit-tested offline |
| Non-zero exit when any item was not deleted (every `clean`) | Built, unit-tested offline |
| `deploy/clean-s2-new-keep-geozarr-workflow.yaml`: floor fixed, real runs on `sha-*` or a digest only, `imagePullPolicy: Always`, no `activeDeadlineSeconds` | Built; tests run its script with a stub `python3` |
| Image containing the flag | Not yet: CI's pull_request build publishes `sha-<merge-sha>` |
| Dry run, canary, batches against production | Not run |
| Register `--update-if-newer` (branch `feat/register-reprocessed-items`) | In review; must not go live before Task 5 is done |
| Recovery path: re-register deleted items from EODC with `register_proxy.py` | Exercised 17 Nov (O3). Since 18 Nov the register refuses into `-new` anything but a final `EOPF_GEOZARR` >= 0.13.0, so only an item EODC serves at such a version can come back |

Census, 2026-11-17 ~13:45Z, read-only `POST /stac/search` over all 34,672 items:

| EOPF_GEOZARR | items | action |
|---|---|---|
| 0.13.1 | 3,943 | keep |
| 0.13.0 | 4,876 | keep |
| 0.12.0 | 2,051 | delete |
| (no key) | 23,802 | delete |

Keep 8,819, delete 25,853. The 0.13.x rows grow as the hourly register cron adds items.

## Dependency graph

```
T1 code + tests ─► T2 image sha-<merge-sha> ─► T3 dry run ─► T4 10-item canary ─► T5 batches ─► T6 register --update-if-newer may go live
                                               (Loïc's go for each real run, same tag throughout)
```

## Tasks

### Task 1 — flag, re-check, exit code, manifest  (in review)
**What**: `--keep-software-min` keeps items at or above the floor, refuses an unreadable value or a
floor that keeps nothing (after printing the per-value counts), re-reads each candidate before its
DELETE, and fails the run after its summary if any item was not deleted. The manifest fixes the
floor, accepts only `sha-*` or `<tag>@sha256:<digest>` for a real run, and has no deadline.
**Verify**:
```bash
uv run --group test pytest tests/unit/test_manage_collections_clean.py tests/unit/test_clean_s2_new_workflow_manifest.py -q
uv run --group test pytest -q
uv run python operator-tools/manage_collections.py clean --help   # lists --keep-software-min
```
**Acceptance criteria**:
- [x] both test files and the full suite pass
- [x] each guard's test fails when its bug is reintroduced (one mutant per guard)
- [ ] PR reviewed and merged

### Task 2 — image  (blocked on Task 1)
**What**: take the `sha-<merge-sha>` tag from CI's pull_request build; check the flag is in it.
**Verify**: the `kubectl run clean-tool-smoke ... clean --help` command in the manifest header.
**Acceptance criteria**:
- [ ] `clean --help` in that image lists `--keep-software-min` and the re-check

### Task 3 — dry run  (blocked on Task 2)
**What**: `argo submit -n devseed deploy/clean-s2-new-keep-geozarr-workflow.yaml -p pipeline_image_version=sha-<merge-sha>`
**Verify**: the per-value block at the top of the pod log, against the census.
**Acceptance criteria**:
- [ ] `delete '0.12.0'` ≈ 2,051 and `delete (missing)` ≈ 23,802 (they can only shrink)
- [ ] `keep '0.13.0'` ≈ 4,876 and `keep '0.13.1'` >= 3,943
- [ ] `unreadable: 0`, and no value outside the census
- [ ] `Would delete 10 STAC items`; the Workflow succeeds

### Task 4 — 10-item real canary  (blocked on Task 3, Loïc's go)
**What**: the same tag, `-p dry_run=false -p max_items=10`.
**Verify**: the run's summary; each deleted id in the log against the API.
**Acceptance criteria**:
- [ ] summary: `STAC items deleted` + re-check skips = 10, no failure, the Workflow succeeds
- [ ] each deleted id answers 404; a kept 0.13.0 item still answers 200
- [ ] a following dry run shows the delete rows down by the number deleted

### Task 5 — batches  (blocked on Task 4, Loïc's go)
**What**: the same tag, `-p dry_run=false -p max_items=2000`, repeated (about 13 runs).
**Verify**: each run's per-value block and summary. A Failed run (an item not deleted) is read, its
cause fixed, and re-run: the clean converges.
**Acceptance criteria**:
- [ ] no run shows a value outside the census or `unreadable` above 0
- [ ] the last run prints `Every item is kept; nothing deleted` and exits 0

### Task 6 — unblock the register update mode  (blocked on Task 5)
**What**: only now may the register `--update-if-newer` flags go live. The re-check already stops
this clean deleting an item upgraded after its walk; finishing first keeps the two from overlapping.
**Acceptance criteria**:
- [ ] Task 5 done before the register flags are deployed

## Risks
- **Stale walk.** A batch deletes for hours from one walk. The register cron creates new items at
  0.13.1 (kept, and not in that walk at all); an in-place upgrade of a walked candidate is caught by
  the re-check (kept on re-check).
- **Transient API errors.** A failed GET or DELETE leaves that item in place and fails the run; the
  next batch retries it. One extra GET per candidate, about 25.9k over the drain.
- **Bound semantics.** `--max-items` counts candidates processed, so a batch with re-check skips
  deletes fewer than N.
- **Tag expiry.** Harbor deletes `sha-*` tags after 7 days. A drain outliving that needs a new dry run
  on `<release tag>@sha256:<digest>`.
- **Memory.** The walk holds every item (about 44 KB each): requests 3Gi, limit 6Gi.
- **Recovery.** Re-register from EODC with `register_proxy.py`, only while EODC serves the item as a final
  `EOPF_GEOZARR` >= 0.13.0 (the register refuses anything else into `-new` since 18 Nov). No cron
  re-registers items acquired more than 14 days ago; the reconcile does, from a final release.

## Open questions
1. Must the recovery path be exercised (re-register one canary-deleted item from EODC) before
   Task 5? Owner: Loïc.
2. The STAC DELETE in `manage_item.delete_item` now has a 30 s client timeout (it had none, and the
   workflow no longer has a deadline). It also applies to `manage_item.py delete` and the staging
   purge. Keep? Owner: Loïc.
3. `delete --clean-first` calls the same clean, so it now stops before deleting the collection when an
   item delete failed (before, it deleted the collection anyway). Keep? Owner: Loïc.

## Done definition
A dry run of the workflow prints only `keep` rows (`total: keep N, delete 0`, `unreadable: 0`) and
exits 0, and only after that do the register `--update-if-newer` flags go live.
