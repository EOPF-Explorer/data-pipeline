# Plan: reconcile `sentinel-2-l2a-new` with EODC re-processing (`scripts/reconcile_versions.py`)

**Spec**: none, a one-off operator run. Design chosen by Loïc on 2026-11-17 after the `/code-review max` of
branch `feat/register-reprocessed-items` (3785add), replacing that branch. The review's verdict and finder
reports are in `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/REVIEWS.md`.
**Goal**: `sentinel-2-l2a-new` holds, for every EODC scene acquired since 2026-09-29 (cloud < 90 %) and not
in `sentinel-2-l2a`, the item built from EODC's newest final `EOPF_GEOZARR >= 0.13.0` version.
**Constraint**: the hourly register is unchanged (create-only, no new flag, `TARGET_RUNS` untouched); every
bound is enforced inside the tool, never by a timeout or a kill; no new dependency beyond the direct
`packaging` pin #519 also adds.

## Current state (2026-11-17)

| Resource | Status |
|---|---|
| `scripts/processing_software.py`: the one `processing:software` parser (object or JSON string) | Built, unit-tested offline |
| `scripts/reconcile_versions.py`: policy table, batched diff, create/replace, bound, exit code | Built, unit-tested offline |
| `deploy/reconcile-s2-new-versions-workflow.yaml`: one-off Workflow, dry run by default | Built; tests run its script with a stub `python3` |
| `scripts/register_proxy.py`, `scripts/query_stac.py` | Unchanged (`git diff origin/main -- scripts/register_proxy.py scripts/query_stac.py` is empty) |
| Image containing the tool | Not yet: CI's pull_request build publishes `sha-<merge-sha>` |
| Dry run, canary, batches against production | Not run |
| s2-new clean (data-pipeline#519) | In review; recommended to run first (see Rollout) |
| `rollback_created_items.py` (branch `feat/rollback-created-items`) | Not on main; reads this tool's created-ids unchanged |
| Restore of a replaced item from its pre-image | Steps below; not exercised |

## Design

```
EODC sentinel-2-l2a-zarr3 ──search(datetime>=2026-09-29, cloud<90, fields=id+datetime+software, limit 1000)──┐
Explorer sentinel-2-l2a-new ──search(fields=..., limit 1000)──────────────────────────────────────────────────┤
                                                                                         classify(id) ◄──────┘
   no key / below floor / unreadable / pre-release ─► skipped_* (counted; ALL of them, or an empty source ─► exit 1)
   >= floor, final, absent ──► ids= search on sentinel-2-l2a, 100 per search ─► in it: skipped_in_archive
                                                                             └─► not:   CREATE
   >= floor, final, newer than target (or target has no key) ─► REPLACE
   equal or older ─► skipped_not_newer;   target unreadable ─► refused_target_unreadable (exit 1)
actions = CREATE + REPLACE, newest acquisition first, then id; the first --max-writes are processed:
   fetch the full source item (register's fetch_source_item), classify it again, build_proxy_item(expires=None)
   CREATE:  register_proxy.create_item (POST, 409 = exists) + record_created, `uncertain` on a 5xx/no answer
   REPLACE: GET (no retries, 30 s, no redirects) ─► 404: gone | not the item / unreadable: refused (failed)
            ─► re-compare ─► append before_put {item_id, run_id, old, new, pre_image} ─► PUT ─► append after_put
summary: one key per source-id fate, adding up to source=N; remaining = actions beyond --max-writes
```

Decisions:
- **Policy from the target.** `POLICIES` in `reconcile_versions.py` holds, for `sentinel-2-l2a-new` only, the
  key (`SOFTWARE_KEY = "EOPF_GEOZARR"`), the floor (0.13.0, the #519 clean's), the acquisition start
  (2026-09-29, the target's earliest acquisition), the exclusion collection (`sentinel-2-l2a`), the cloud
  filter (90, the register template's) and the EODC source. No flag carries any of them; any other
  `--collection` is refused before a network call.
- **Pre-releases, dev builds and local labels are never a source** (`processing_software.is_final`):
  PEP 440 orders `0.14.0rc1` above `0.13.1` and `0.13.0+eodc.1` above `0.13.0`. They are counted as
  `skipped_prerelease`. A post-release (`0.13.1.post1`) is final.
- **A version needs at least `major.minor`**, so a bare all-digit commit sha (`1234567`, a valid PEP 440
  release) is unreadable, not "newer than everything".
- **Never a downgrade.** An equal or older source is skipped. Following an EODC revert (EODC re-publishes
  an older version on purpose) needs a manual decision: restore the pre-images (below) or re-register.
- **The bound processes the first N, it does not refuse.** `remaining=M` is printed; the run exits 0 when
  nothing failed, and is re-run until `remaining=0`, like the clean.
- **Writes never retry.** `open_write_client` keeps requests' default adapter (no retries), unlike
  `stac_auth.open_client` (pystac-client's `Retry(total=5)`). Reads use `stac_auth.open_resilient_client`.
- **Replaced-ids is not rollback input.** Its lines name the item `item_id`, so
  `rollback_created_items.read_created_ids` refuses the file instead of deleting pre-existing items.
- **The register's semaphore.** The Workflow takes `eodc-register-limit`, so no register run creates an id
  while this run may create it, the condition under which a created-ids `uncertain` line names only an item
  this run may have made (the rollback tool's docstring relies on it). Hourly runs queue behind it
  (`concurrencyPolicy: Allow`) and keep their scheduled windows.

## Dependency graph

```
T1 parser ─► T2 tool + tests ─► T3 manifest + tests ─► T4 review, push, PR ─► T5 image sha-<merge-sha>
   ─► T6 dry run ─► T7 10-write canary + recovery exercise ─► T8 batches until remaining=0
                    (Loïc's go for each real run, same tag throughout; #519's clean ideally done first)
```

## Tasks

### Task 1 — one `processing:software` parser  ✅ DONE
**What**: `scripts/processing_software.py`: `read_version(properties, key)` (object or JSON string; None if
absent; ValueError if unreadable) and `is_final(version)`.
**Verify**: `uv run --group test pytest tests/unit/test_processing_software.py -q`
**Acceptance criteria**:
- [x] both wire shapes read; git-describe, bare sha, all-digit sha, empty, non-string, non-JSON refused
- [x] `is_final` false for rc, dev and local; true for a post-release

### Task 2 — the reconcile tool  ✅ DONE
**What**: `scripts/reconcile_versions.py` as in Design.
**Verify**: `uv run --group test pytest tests/unit/test_reconcile_versions.py -q`
**Acceptance criteria**:
- [x] every verdict in one table test; end to end, the summary keys add up to `source`
- [x] 250 sources: 1 source search, 1 target search, 3 `ids=` searches (100/100/10); every search sends
  `limit=1000` and the `fields` include list
- [x] an id in `sentinel-2-l2a` is never created (`skipped_in_archive`)
- [x] all sources unusable (renamed key, below floor, new stamp format, pre-releases) or no source:
  exit 1, nothing fetched or written, the counts printed first
- [x] replace: GET body validation (8 shapes refused, no PUT), GET 404 = gone (no POST), PUT 404 = gone,
  re-compare skips a caught-up item, pre-image on disk before the PUT, outcome after it, a failed PUT
  (timeout, 502, 422) never reaches created-ids and fails the run
- [x] the write session sends a GET and a PUT exactly once on 503 + Retry-After and does not re-send a
  timed-out PUT (real socket); it carries the bearer
- [x] `--max-writes 2` over 5 actions: the 2 newest written, `remaining=3`, re-runs converge to 0 writes
- [x] a real run without `--max-writes`, either list or `--run-id` is refused; `--max-writes` 0/-1/""/x
  refused by argparse; any other collection refused; http URLs refused
- [x] each review finding's test fails when its bug is reintroduced (mutation runs, see Verification)

### Task 3 — the Workflow  ✅ DONE
**What**: `deploy/reconcile-s2-new-versions-workflow.yaml`.
**Verify**: `uv run --group test pytest tests/unit/test_reconcile_s2_new_workflow_manifest.py -q`
**Acceptance criteria**:
- [x] defaults: `--max-writes 10 --dry-run`; only `dry_run=false` writes, and then passes both lists and
  `--run-id {{workflow.name}}`
- [x] a real run refused on `v*`, `pr-*`, `latest`, `main`; accepted on `sha-*` and `*@sha256:*`
- [x] parameters are exactly `pipeline_image_version`, `max_writes`, `dry_run`; a parameter cannot add a flag
- [x] `imagePullPolicy: Always`, no `activeDeadlineSeconds`, the `eodc-register-limit` semaphore, both
  lists as optional archived artifacts, OIDC secrets and no AWS env

### Task 4 — review, push, PR  (NEXT)
**What**: ponytail-review pass, push `feat/reconcile-s2-new-versions`, PR opened by Loïc.
**Acceptance criteria**:
- [x] full suite passes (`uv run --group test pytest -q`)
- [ ] PR reviewed and merged

### Task 5 — image  (blocked on Task 4)
**What**: take the `sha-<merge-sha>` tag from CI's pull_request build (`sha-` + first 7 of
`refs/pull/<N>/merge`); check the tool is in it.
**Verify**: the `kubectl run reconcile-tool-smoke ... --help` command in the manifest header.
**Acceptance criteria**:
- [ ] `reconcile_versions.py --help` runs in that image

### Task 6 — dry run  (blocked on Task 5)
**What**: `argo submit -n devseed deploy/reconcile-s2-new-versions-workflow.yaml -p pipeline_image_version=sha-<merge-sha>`
**Verify**: the `Diff for sentinel-2-l2a-new` block and the summary line in the pod log.
**Acceptance criteria**:
- [ ] `target` matches a fresh count of the collection (34,672 on 17 Nov before the clean; ~8.8k after it)
- [ ] `skipped_unreadable=0`, `skipped_prerelease=0`, `refused_target_unreadable=0`, `failed=0`
- [ ] `skipped_in_archive` is in the expected ~800-1,000 range, or explained
- [ ] the 10 `would create` / `would replace` lines name real ids, with plausible version changes
- [ ] the Workflow succeeds

### Task 7 — 10-write canary and the recovery exercise  (blocked on Task 6, Loïc's go)
**What**: the same tag, `-p dry_run=false -p max_writes=10`; then exercise both recovery paths on one item
each (see Rollback), then let the next run re-apply them.
**Acceptance criteria**:
- [ ] summary: `created + replaced + exists + gone + skipped_* (of the 10) = 10`, `failed=0`, Workflow succeeds
- [ ] the written ids are the ones the dry run listed; each answers 200 with the new `EOPF_GEOZARR`
- [ ] created-ids holds one line per `created`; replaced-ids one `before_put` + one `after_put` per replace
- [ ] one created item rolled back with `rollback_created_items.py`, one replaced item restored from its
  pre-image, both checked, then both re-applied by the next run

### Task 8 — batches  (blocked on Task 7, Loïc's go)
**What**: the same tag, `-p dry_run=false -p max_writes=500`, repeated.
**Acceptance criteria**:
- [ ] no run shows `failed`, `skipped_unreadable` or `refused_target_unreadable` above 0 (or each is explained)
- [ ] the last run prints `remaining=0` with no writes and exits 0

## Rollback

**Created items** (`created-ids` artifact): `scripts/rollback_created_items.py` from branch
`feat/rollback-created-items` (not on main), unchanged:
```bash
uv run scripts/rollback_created_items.py --created-ids created-ids.jsonl \
  --collection sentinel-2-l2a-new --max-items 10 \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac          # dry run; then --apply
```
It re-checks each item's content before its DELETE. Every `sentinel-2-l2a-new` item passes that check, so
the created-ids list itself must be right: this tool writes a line only for an id absent from the target at
diff time, under the register's semaphore.

**Replaced items** (`replaced-ids` artifact). No helper; the steps, one item first, then 10:
1. Hold the reconcile (no new run): it would re-apply the replacement.
2. Candidates: every `before_put` line, minus the ids whose `after_put` outcome is `gone` or `refused`
   (nothing was written):
   ```bash
   jq -r 'select(.phase=="after_put" and (.outcome=="gone" or .outcome=="refused")) | .item_id' \
     replaced-ids.jsonl | sort -u > not-written.txt
   jq -c 'select(.phase=="before_put")' replaced-ids.jsonl | grep -v -F -f not-written.txt > restore.jsonl
   ```
   (`grep -F -f` on an empty `not-written.txt` matches nothing; skip that filter then.)
3. Per line: GET `/stac/collections/sentinel-2-l2a-new/items/<item_id>`. A 404 means nothing to restore. If
   its `processing:software.EOPF_GEOZARR` is not the line's `new_version`, something changed it since: skip
   and report.
4. Body = the line's `pre_image`. pgstac discards the hierarchical links (`self`, `parent`, `root`,
   `collection`) the GET added, but the gateway may also add `auth:*` fields and the authentication
   extension, as it does on collections (`aggregate_items.py`, #439): check one pre-image and drop them.
5. PUT the body to the same URL with a bearer from `stac-auth-oidc`, once, no retry, 30 s timeout; then GET
   and check `EOPF_GEOZARR` equals the line's `old_version`.

## Verification (2026-11-17)

- `uv run --group test pytest tests/unit/test_processing_software.py tests/unit/test_reconcile_versions.py
  tests/unit/test_reconcile_s2_new_workflow_manifest.py -q`: all pass.
- Full suite: `uv run --group test pytest -q`: 2041 passed, 5 skipped.
- Mutation runs: 33 mutants, each reintroducing one review finding's bug (or a manifest guard's) and running
  its tests; all 33 fail their tests on the final test files. The first run missed one (G3, the body's
  `type` check alone), which a new test case now pins. Driver and output in
  `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/mutation-runs/reconcile/`. A caught mutant shows that a
  test catches that bug, nothing about bugs no mutant encodes.

## Risks
- **EODC's page size and `fields`.** EODC's root declares `item-search#fields` (captured 26 Oct 2026);
  whether it honours `limit=1000` or caps it is unknown. A cap only adds pages; ignored `fields` only
  makes pages bigger. The `ids=` exclusion runs on the Explorer's pgstac; an answer with an unasked id
  refuses the run.
- **Snapshot age.** The diff is a snapshot; each write re-reads (source fetch, target GET) before acting,
  and an item the clean deletes between GET and PUT is `gone` and created by the next run.
- **Hourly register delay.** While this run holds the semaphore, register runs queue (their windows are
  fixed by `scheduledTime`, so nothing is lost). At ~1-2 s per write, a 500-write batch is ~10-15 min.
- **Pre-image size.** ~15-45 KB per replace: a 500-replace batch writes ~10-20 MB of replaced-ids.
- **Tag expiry.** Harbor deletes `sha-*` tags after 7 days; past that, dry-run again on
  `<release tag>@sha256:<digest>`.

## Open questions (owner: Loïc)
1. **Exclusion scope.** An id in `sentinel-2-l2a` is never created, but an id already in both collections is
   still replaced in `-new`. Keep?
2. **Acquisition start 2026-09-29.** The target's earliest acquisition is 2026-09-29T08:26Z. A later start
   (after the 30 Sep burst) would shrink `skipped_in_archive` but leave late-September scenes unreconciled.
3. **Unreadable sources.** Only an all-unusable source fails the run; a few unreadable source stamps are
   counted and sampled but exit 0. Fail on any (as the clean refuses any unreadable item)?
4. **Semaphore.** Taking `eodc-register-limit` delays hourly register runs during a batch. Keep, or drop it
   and accept that a created-ids `uncertain` line may name an item a concurrent register run created?
5. **Recovery exercise.** Is exercising the rollback and the restore on one canary item each (Task 7) enough,
   or should the restore be scripted and tested first?
6. **After #519 merges:** switch `manage_collections._software_value` / `_kept` to
   `processing_software.read_version` (it is stricter: an all-digit sha becomes unreadable, which the clean
   refuses).
7. **A recurring reconcile** (CronWorkflow) is out of scope. Wanted after the batches converge?

## Done definition
A real run of the Workflow prints `remaining=0`, writes nothing, `failed=0`, `skipped_unreadable=0` and
exits 0, and the canary's created and replaced items were each recovered once and re-applied.
