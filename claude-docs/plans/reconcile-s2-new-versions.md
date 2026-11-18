# Plan: reconcile `sentinel-2-l2a-new` with EODC re-processing (`scripts/reconcile_versions.py`)

**Spec**: none, a one-off operator run. Design chosen by Loïc on 2026-11-17 after the `/code-review max` of
branch `feat/register-reprocessed-items` (3785add), replacing that branch. The review's verdict and finder
reports are in `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/REVIEWS.md`.
**Goal**: `sentinel-2-l2a-new` holds, for every EODC scene acquired since 2026-09-29 (cloud < 90 %) and not
in `sentinel-2-l2a`, the item built from EODC's newest final `EOPF_GEOZARR >= 0.13.0` version.
**Constraint**: the hourly register is unchanged (create-only, no new flag, `TARGET_RUNS` behaviour
untouched: only its comment now names this tool); every bound is enforced inside the tool, never by a
timeout or a kill; no new dependency beyond the direct `packaging` pin #519 also adds.

## Current state (2026-11-17, after the /code-review high round)

| Resource | Status |
|---|---|
| `scripts/processing_software.py`: the one `processing:software` parser (object or JSON string) | Built, unit-tested offline |
| `scripts/reconcile_versions.py`: fixed policy, batched diff, create/replace, bound, exit code | Built, unit-tested offline; review findings #2, #3, #6, #7, #8, #10 fixed (Task 4) |
| `deploy/reconcile-s2-new-versions-workflow.yaml`: one-off Workflow, dry run by default | Built; tests run its script with a stub `python3`; header fixed (#1, #4, run order, restore limit) |
| `scripts/register_proxy.py` | `TARGET_RUNS` comment names this tool as the one PUT into `-new` (#5); `create_item` takes the target URL and records created-ids itself, for both tools; pipeline behaviour unchanged |
| `scripts/query_stac.py` | Unchanged |
| Image containing the tool | Not yet: CI's pull_request build publishes `sha-<merge-sha>` |
| Dry run, canary, batches against production | Dry run done 17 Nov (Task 6); canary and batches not run |
| s2-new clean (data-pipeline#519) | Merged (289ae5a). Dry run and 10-item canary done 17 Nov; batches ON HOLD: October kept for now, 1-10 Nov cleaned only after this reconcile and after EODC's re-publishing (see Run order) |
| `rollback_created_items.py` (branch `feat/rollback-created-items`, #445) | Not on main; reads this tool's created-ids unchanged |
| Restore of a replaced item from its pre-image | Steps below; not exercised; metadata only (see Rollback) |

## Design

```
EODC sentinel-2-l2a-zarr3 ──search(datetime>=2026-09-29, cloud<90, fields=id+datetime+software+cloud, limit 1000)──┐
Explorer sentinel-2-l2a-new ──search(fields=..., limit 1000)────────────────────────────────────────────────────────┤
                                                                                               classify(id) ◄──────┘
   cloud missing or >= 90 / no key / below floor / unreadable / pre-release
                                    ─► skipped_* (counted; ALL of them, or an empty source ─► exit 1)
   >= floor, final, absent ──► ids= search on sentinel-2-l2a, 100 per search ─► in it: skipped_in_archive
                                                                             └─► not:   CREATE
   >= floor, final, newer than target (or target has no key, or a non-final one) ─► REPLACE
   equal to or older than a final target ─► skipped_not_newer;   target unreadable ─► refused_target_unreadable (counted)
actions = CREATE + REPLACE, newest acquisition first, then id; taken in order until --max-writes POSTs/PUTs are sent
          (or --time-budget seconds have passed since the start, checked between ids):
   CREATE:  fetch the full source item (register's fetch_source_item), classify it again, build_proxy_item(expires=None)
            ─► register_proxy.create_item (POST, 409 = exists) + record_created, `uncertain` on a 5xx/no answer
   REPLACE: GET (no retries, 30 s, no redirects) ─► 404: gone | not the item / unreadable: refused_target_unreadable
            ─► compare with the diff's source version (caught up: skipped_not_newer) ─► fetch, classify against
            the target as just read, build ─► append before_put {item_id, run_id, old, new, pre_image} ─► PUT
            ─► append after_put
   ends before a write (old generation, unbuildable, source changed, gone, not newer, target unreadable): counted,
   does not use the bound
summary: one key per source-id fate, adding up to source=N; remaining = actions not reached
```

Decisions:
- **Policy fixed in code.** Constants in `reconcile_versions.py` hold, for `sentinel-2-l2a-new` only, the
  key (`SOFTWARE_KEY = "EOPF_GEOZARR"`), the floor (0.13.0, the #519 clean's), the acquisition start
  (2026-09-29, the target's earliest acquisition), the exclusion collection (`sentinel-2-l2a`), the cloud
  filter (90, the register template's) and the EODC source. No flag carries any of them; any other
  `--collection` is refused before a network call.
- **Pre-releases, dev builds and local labels are never a source** (`processing_software.is_final`):
  PEP 440 orders `0.14.0rc1` above `0.13.1` and `0.13.0+eodc.1` above `0.13.0`. They are counted as
  `skipped_prerelease`. A post-release (`0.13.1.post1`) is final.
- **A version needs at least `major.minor`**, so a bare all-digit commit sha (`1234567`, a valid PEP 440
  release) is unreadable, not "newer than everything".
- **Never a downgrade of a final release.** A source equal to or older than a final target is skipped.
  Following an EODC revert (EODC re-publishes an older version on purpose) needs a manual decision: restore
  the pre-images (below) or re-register. A target on a pre-release, dev build or local label is replaced by
  any final version >= floor, even one PEP 440 orders lower (`0.13.1` over `0.14.0rc1`, review #7).
- **Cloud cover is checked here too** (review #8): the search's CQL2 filter asks EODC for `< 90`, and
  `classify` re-checks `eo:cloud_cover` (now in `FIELDS`). A missing or non-numeric value is
  `skipped_cloud_cover`, never a create, and counts as unusable for the "nothing usable" refusal.
- **The bound counts writes sent, it does not refuse** (review #2). Actions are taken newest first until
  `--max-writes` POSTs/PUTs have been sent (would be, in a dry run); `remaining=M` counts the actions not
  reached. A write that fails counts (it may have landed). An id that ends before its write does not:
  old generation, unbuildable, source changed since the diff, target GET 404, target already caught up.
  Those end the same way on every run, so counting them made a handful at the head of the order fill every
  run's bound (or, for unbuildable sources, then raised as failures, trip the 10-in-a-row stop), forever.
  No state is carried between runs: they are re-visited each run (one target GET, or one EODC fetch +
  build, each) and never hold up the ids behind them. The alternative, a skip-list file the next run reads,
  needs an input artifact chained between one-off Workflows that an operator can forget or point at the
  wrong run. Other failures stay failures: 5xx, timeouts, and a source GET 404 (an EODC change could turn
  every id into one, which must stop the run, not look like a converged one). They exit 1, and 10 in a row
  stop the run, with the ids not reached reported as `remaining`, not as `failed`.
- **Only failures exit 1** (#522 review 1, Loïc 17 Nov): a write failed or refused, a read an id needed (the
  source fetch, the target GET), the 10-in-a-row stop, or no usable source. An unbuildable source or an
  unreadable target (at diff or at replace time) ends the same way on every run, so failing on it failed the
  run that converges too, forever: it is counted, and named on a `Look:` line after the summary, exit 0.
- **A time budget** (platform-deploy#450 review): `--max-writes` does not bound how long the run holds the
  register's semaphore, since ids that end before their write and 30 s timeouts use none of it.
  `--time-budget SECONDS` (the register's `_seconds` check) starts no new id once the run has lasted that
  long; it logs `time_budget_reached`, the rest is `remaining`, exit 0 unless an id failed. The Workflow
  passes `time_budget_seconds`, 2700 by default, as the register's budget.
- **`--max-writes` is required, dry runs included** (review #6): without it a dry run had a bound of 0 and
  checked nothing, green.
- **Replace reads the target first** (review #10): the GET and the comparison with the diff's source version
  come before the EODC fetch and build, so a gone or caught-up item costs one GET.
- **Writes never retry.** `open_write_client` keeps requests' default adapter (no retries), unlike
  `stac_auth.open_client` (pystac-client's `Retry(total=5)`). Reads use `stac_auth.open_resilient_client`.
  Opening it (the landing-page GET) has the same 30 s timeout (review #3): the run holds the register's
  semaphore, so a hang there would hold up the hourly register.
- **Replaced-ids is not rollback input.** Its lines name the item `item_id`, so
  `rollback_created_items.read_created_ids` refuses the file instead of deleting pre-existing items.
- **The register's semaphore.** The Workflow takes `eodc-register-limit`, so no register run creates an id
  while this run may create it, the condition under which a created-ids `uncertain` line names only an item
  this run may have made (the rollback tool's docstring relies on it). Hourly runs queue behind it
  (`concurrencyPolicy: Allow`) and keep their scheduled windows. It has no deadline (no time-based kill,
  review #1): a reconcile pod stuck Pending (an expired tag in ImagePullBackOff) would hold it forever, so
  the manifest's step 0 is an image smoke check, and its header says a Pending pod has written nothing and
  its Workflow can be deleted to release the semaphore; a Running real run is never deleted.

## Run order (Loïc, 17 Nov 2026)

EODC is re-publishing its 1-10 Nov 2026 acquisitions at 0.13.1 under the same ids and the same store paths
(since 17 Nov 10:07Z; `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/FINDINGS-2026-11-17-eodc-drift.md`).
- **1-10 Nov: this reconcile BEFORE the clean**, and the clean of that window only once EODC's re-publishing
  is over. The reconcile replaces the old items in place and the clean then keeps them; cleaning first would
  delete them and leave a gap until a reconcile re-creates them (the hourly register's 14-day acquisition
  floor will not).
- **October: re-created as EODC re-processes it.** `-new`'s unversioned October items were deleted on
  17 Nov (Loïc); EODC is re-processing the European ones at 0.13.1 from 16 Nov, and the reconcile CREATEs
  each one it re-publishes (until then: `skipped_no_key`).

## Dependency graph

```
T1 parser ─► T2 tool + tests ─► T3 manifest + tests ─► T4 review fixes, push, PR ─► T5 image sha-<merge-sha>
   ─► T6 dry run ─► T7 stops on dry runs, 1-write canary + recovery exercise ─► T8 batches until remaining=0
                    (Loïc's go for each real run, same tag throughout; for 1-10 Nov BEFORE the #519 clean)
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
- [x] a real run without either list or `--run-id` is refused; `--max-writes` missing (any run, since the
  review round) or 0/-1/""/x refused by argparse; any other collection refused; http URLs refused
- [x] each review finding's test fails when its bug is reintroduced (mutation runs, see Verification)
- [x] (review round, 17 Nov) 11 ids at the head of the order that end before their write (unbuildable, old
  generation, source changed, target GET 404) do not stop `--max-writes 1` writing the next id, and two runs
  converge to `remaining=0`; a POST or PUT that fails counts toward the bound
- [x] (review) opening the write client times out (landing page held 1 s, timeout 0.2 s, real socket)
- [x] (review) `--max-writes` missing: argparse exit 2 for a dry run and a real run, nothing searched
- [x] (review) `eo:cloud_cover` 90, missing, a string or NaN: `skipped_cloud_cover`; a catalogue that ignores
  the CQL2 filter gets no write for a cloudy or cloud-less source, and an all-cloudy source exits 1
- [x] (review) a target gone (GET 404) or caught up since the diff costs no source fetch; a fetched source
  no newer than the target as just read is `skipped_source_changed`, no PUT
- [x] (review) a target on `0.14.0rc1`, `0.13.2.dev3` or `0.13.1+eodc.1` is replaced by a final release;
  final targets are still never downgraded; the re-run converges

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

### Task 4 — review fixes, push, PR  (NEXT)
**What**: the `/code-review high` findings of 17 Nov (triage in
`~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/REVIEWS.md`): #2, #3, #6, #7, #8, #10 in the tool, #1, #4
and the run order in the manifest header, #5 in the `TARGET_RUNS` comment, the restore limit in Rollback.
#9 (duplicate write-session setup) is deferred to when `stac_auth` gets timeout/retry parameters. Then a
ponytail-review pass, push `feat/reconcile-s2-new-versions`, PR opened by Loïc.
**Verify**: `uv run --group test pytest -q`
**Acceptance criteria**:
- [x] full suite passes (`uv run --group test pytest -q`)
- [x] each fix's test fails when its bug is reintroduced (see Verification)
- [x] (#522 `/code-review high`, 17 Nov) the 10 findings fixed: only a failed id or no usable source exits 1
  (an unbuildable source and an unreadable target, at diff or replace time, are counted and named on a
  `Look:` line); creates POST under `--stac-api-url`; a boolean `eo:cloud_cover` and an epoch version are
  refused; the clean reads versions with `processing_software.read_version`; a converged run opens no write
  client
- [x] (platform-deploy#450 review) `--time-budget`: with a fake clock (100 s per fetch, budget 250 s) a run
  writes 3 of 5, logs `time_budget_reached`, `remaining=2`, exits 0, and the next run finishes; 0, -1, "",
  nan, inf and 12601 are refused; the Workflow passes `--time-budget 2700` by default
- [ ] PR reviewed and merged

### Task 5 — image  (blocked on Task 4)
**What**: take the `sha-<merge-sha>` tag from CI's pull_request build (`sha-` + first 7 of
`refs/pull/<N>/merge`); check the tool is in it.
**Verify**: the `kubectl run reconcile-tool-smoke ... --help` command in the manifest header (step 0).
**Acceptance criteria**:
- [ ] `reconcile_versions.py --help` runs in that image and lists `--max-writes` as required

### Task 6 — dry run  ✅ DONE 17 Nov
Evidence: `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/dry-run-522_2026-11-17/RESULT.md` (workflow
`reconcile-s2-new-versions-dccqf`, sha-925d429): target 11,819 (after the October clean), replace 1,701,
create 0 (so `skipped_in_archive` 0), all the zero counts at 0, 10 `would replace` on 9 Nov S2C ids
0.13.0 -> 0.13.1, Succeeded. Submitted with `kubectl create`: argo CLI v4 refuses the semaphore field.
**What**: `argo submit -n devseed deploy/reconcile-s2-new-versions-workflow.yaml -p pipeline_image_version=sha-<merge-sha>`
**Verify**: the `Diff for sentinel-2-l2a-new` block and the summary line in the pod log.
**Acceptance criteria**:
- [x] `target` matches a fresh count of the collection (11,819 after the 17 Nov October clean)
- [x] `skipped_unreadable=0`, `skipped_prerelease=0`, `skipped_cloud_cover=0`, `skipped_unbuildable=0`,
  `refused_target_unreadable=0`, `failed=0`
- [x] `skipped_in_archive` is in the expected ~800-1,000 range, or explained (0: no create candidates)
- [x] the 10 `would replace` lines name real ids, with plausible version changes; the 1-10 Nov
  acquisitions EODC has re-published at 0.13.1 appear as `would replace`
- [x] the Workflow succeeds

### Task 7 — the stops on dry runs, a 1-write canary and the recovery exercise  (Loïc's go)
**What**: first two dry runs that exercise the stops: `-p time_budget_seconds=60 -p max_writes=5000`
(logs `time_budget_reached`) and `-p max_writes=5000` stopped with `argo stop` after its `Diff for` line
(logs `stop_requested`). Then, straight after a dry run, the same tag with `-p dry_run=false
-p max_writes=1`: with 0 creates today it is a replace, so restore that item from its pre-image (see
Rollback), check it, and let the next run re-apply it. The created-ids rollback is exercised on the first
create, with data-pipeline#445 merged (#450 xhigh review 6, cross-PR review 3).
**Acceptance criteria**:
- [ ] both dry runs log their stop and exit 0
- [ ] summary: `created + replaced + exists + gone + skipped_* = 1`, `failed=0`, Workflow succeeds
- [ ] the written ids are the ones the dry run listed; each answers 200 with the new `EOPF_GEOZARR`
- [ ] created-ids holds one line per `created`; replaced-ids one `before_put` + one `after_put` per replace
- [ ] the replaced item restored from its pre-image, checked, then re-applied by the next run; the first
  created item rolled back with `rollback_created_items.py` once #445 is merged

### Task 8 — batches  (blocked on Task 7, Loïc's go)
**What**: the same tag, `-p dry_run=false -p max_writes=500`, repeated.
**Acceptance criteria**:
- [ ] no run shows `failed`, `skipped_unreadable`, `skipped_unbuildable`, `skipped_cloud_cover` or
  `refused_target_unreadable` above 0 (or each is explained). Only `failed` fails a run: read the counts
  and the `Look:` lines of each green one too
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

**What a restore cannot undo.** A pre-image restores the item's METADATA. When EODC re-creates a store at the
same path, as it is doing for the 1-10 Nov acquisitions (same ids, same cpm_v300 paths, new bytes with
`EOPF_GEOZARR: "0.13.1"`), the old bytes are gone at EODC. A restored item then describes the old version
but points at the new store, so the replace cannot be undone; only a store EODC has left in place can be
restored to its old state. The Task 7 exercise checks the mechanism, not the data.

## Verification (2026-11-17)

- `uv run --group test pytest tests/unit/test_processing_software.py tests/unit/test_reconcile_versions.py
  tests/unit/test_reconcile_s2_new_workflow_manifest.py -q`: all pass (129 after the review round).
- Full suite: `uv run --group test pytest -q`: 2059 passed, 5 skipped (2041 before the review round).
- Mutation runs, first round: 33 mutants, each reintroducing one review finding's bug (or a manifest guard's)
  and running its tests; all 33 fail their tests on the final test files. The first run missed one (G3, the
  body's `type` check alone), which a new test case now pins. Driver and output in
  `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/mutation-runs/reconcile/`.
- Mutation runs, review round: 15 new mutants (R2a-R10c, R7) reintroduce this round's findings; the 26
  earlier mutants whose code did not move re-run as is, and 7 whose code moved re-run re-anchored. 47 of 48
  fail their tests. The one missed, R3b, drops the `StacApiIO(timeout=...)` while keeping
  `Client.open(timeout=...)`: in pystac-client 0.9.1 `Client.open` overwrites the `stac_io`'s timeout, so
  that argument has no effect and no test can see it go. Driver and output in
  `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/mutation-runs/reconcile-review-fixes/`. A caught mutant
  shows that a test catches that bug, nothing about bugs no mutant encodes.
- #522 `/code-review high` round: the three test files above plus `test_manage_collections_clean.py`
  and `test_clean_s2_new_workflow_manifest.py`, 243 passed (220 before); full suite 2144 passed, 5 skipped.
  11 mutants (M1a-M8b, M11) reintroduce one finding's bug each, M11 the missing time budget (M1c is a guard:
  a failed id exits 1). Each fails its tests except M8a, `run` built under `if actions:`, which is a static
  bug: pytest passes, `mypy --enable-error-code possibly-undefined` flags it. Driver and output (run2) in
  `~/DevDS/EOPF/clean-s2-new-keep-geozarr_2026-11-17/mutation-runs/pr522-review-fixes/`.
- Over-engineering pass (17 Nov): the same five test files, 244 passed; full suite 2145 passed, 5 skipped.
  Every mutant caught before the pass is still caught, as is or re-anchored (`run_ponytail.py` in both
  mutation directories); R3b is still missed, and A1 now fails on a missing import, so its copy A1p stands
  in. The pass found that no test pinned the replace path's refusal of a non-200 GET: the 302 and 500 rows
  send no body, so `.json()` failed first. A 302 that carries the item now fails only through that check.

## Risks
- **EODC's page size and `fields`.** EODC's root declares `item-search#fields` (captured 26 Oct 2026);
  whether it honours `limit=1000` or caps it is unknown. A cap only adds pages; ignored `fields` only
  makes pages bigger. The `ids=` exclusion runs on the Explorer's pgstac; an answer with an unasked id
  refuses the run.
- **Snapshot age.** The diff is a snapshot; each write re-reads (source fetch, target GET) before acting,
  and an item the clean deletes between GET and PUT is `gone` and created by the next run, unless it is in
  `sentinel-2-l2a` (then `skipped_in_archive`: the archive holds it).
- **Hourly register delay.** While this run holds the semaphore, register runs queue (their windows are
  fixed by `scheduledTime`, so nothing is lost). At ~1-2 s per write, a 500-write batch is ~10-15 min.
- **Pre-image size.** ~15-45 KB per replace: a 500-replace batch writes ~10-20 MB of replaced-ids.
- **Tag expiry.** Harbor deletes `sha-*` tags after 7 days; past that, dry-run again on
  `<release tag>@sha256:<digest>`. A tag that has expired leaves the pod Pending with the semaphore held:
  smoke-check the image first (manifest step 0).
- **Schema drift makes a run long.** Unbuildable sources do not use the bound, so if EODC changes its item
  schema and every build fails, a run (a dry run too) fetches every action before it ends, exit 1. At
  ~0.3-1 s per fetch that is minutes to an hour or two for ~12k actions, holding the semaphore. A dry run
  writes nothing and can be deleted at any time; a Running real run is never deleted.
- **EODC removing items mid-run.** A source the diff listed and EODC has since removed answers 404 to the
  fetch: a failure (exit 1; 10 in a row stop the run). The next run's diff no longer lists it.

## Open questions (owner: Loïc)
2. **Acquisition start 2026-09-29.** The target's earliest acquisition is 2026-09-29T08:26Z. A later start
   (after the 30 Sep burst) would shrink `skipped_in_archive` but leave late-September scenes unreconciled.
3. **Unreadable sources.** Only an all-unusable source fails the run; a few unreadable source stamps are
   counted and sampled but exit 0. Fail on any (as the clean refuses any unreadable item)?
5. **Recovery exercise.** Is exercising the rollback and the restore on one canary item each (Task 7) enough,
   or should the restore be scripted and tested first?

### Resolved (Loïc, 17 Nov 2026)
1. **Exclusion scope:** an id already in both `-new` and `sentinel-2-l2a` is replaced in `-new` like any
   other (it adds no duplicate). Only creates are checked against `sentinel-2-l2a`. The manifest header and
   the module docstring now say so (review #4).
4. **Semaphore:** kept. No deadline on it (review #1): the image smoke check and the Pending-pod step in the
   manifest header instead.
- **Run order:** for the 1-10 Nov acquisitions this reconcile runs BEFORE the clean; October comes back as
  EODC re-processes it (see Run order).
6. **The clean's parser:** `manage_collections` reads versions with `processing_software.read_version` and
   its own parser is gone (#522 review 4). An all-digit sha, a major-only version or an epoch is now
   unreadable there too, so the clean refuses (census printed first, nothing deleted).
7. **A recurring reconcile**, the dead-entry delete pass and one-item-per-scene (the 111 scenes with two
   generations at EODC): deferred until EODC answers on the scope of its re-publishing.
8. **Unbuildable source, unreadable target:** counted, named on a `Look:` line, exit 0 (#522 review 1). Only
   failures and an all-unusable source exit 1 (Design).

## Done definition
A real run of the Workflow prints `remaining=0`, writes nothing, `failed=0`, `skipped_unreadable=0`,
`skipped_unbuildable=0` and exits 0, and the canary's created and replaced items were each recovered once
and re-applied.
