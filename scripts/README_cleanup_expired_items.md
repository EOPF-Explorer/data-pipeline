# Expired-item cleanup (`cleanup_expired_items.py`)

Expiry-driven retention for S2 data ([coordination#183](https://github.com/EOPF-Explorer/coordination/issues/183)).
Items carry a STAC `expires` timestamp; this script deletes the ones whose
`expires` is in the past — S3 objects first, then the STAC item.

> ⚠️ **Destructive.** Dry-run is the default. Real deletion requires `--execute`.

## How an item gets an `expires`

`expires` uses the [timestamps extension](https://github.com/stac-extensions/timestamps).
There are two ways it lands on an item:

1. **At registration** — `register_v1.py` stamps `expires = now + EXPIRES_RETENTION_DAYS`.
   - `EXPIRES_RETENTION_DAYS` defaults to **183** (6 months), shared from
     `s3_item_cleanup.DEFAULT_RETENTION_DAYS`. **The default is not the S2
     policy**: the retention actually in force is set per manifest, on the
     workflow templates in platform-deploy
     ([coordination#178](https://github.com/EOPF-Explorer/coordination/issues/178)).
     The code default stays at 183 because other collections (S3 OLCI staging)
     still rely on it — read the manifest, not this constant, to know what a
     collection keeps.
   - **`EXPIRES_RETENTION_DAYS=0` disables stamping** for a whole run — **at
     registration only**. The `stamp_expires` / `restamp_expires` migrations
     refuse a non-positive retention instead (it would compute `expires` at
     acquisition, i.e. past-expiry for every item); to skip a migration, do not
     run it.
   - **`EXPIRES_EXCLUDE_FILE` protects specific ids.** `register_v1` reads the
     **same demo denylist** the cleanup honors, and never stamps `expires` on an
     id in it. So re-registering or **reconverting** a listed demo scene keeps it
     with **no `expires`** (structurally undeletable). See *Demo-scene protection*
     below.
2. **Backfill** — the `stamp_expires` migration stamps existing items
   (`expires = datetime + retention`, keyed off acquisition age; items acquired
   before its `EXPIRES_MIN_DATETIME` floor are left unstamped). See
   [operator-tools/README_MIGRATIONS.md](../operator-tools/README_MIGRATIONS.md).
3. **Policy change** — the `restamp_expires` migration **shortens** an existing
   `expires` when the retention window is reduced. It writes only when the
   recomputed value is earlier (never extends) and never stamps an item that has
   no `expires`. Same doc as above.

## Demo-scene protection (do demo items get an `expires`?)

**No — when they are (re)registered through the demo-reconversion path they get
no `expires` at all.** One list, `scripts/demo_exclude_ids.txt`, is the single
source of truth, honored at both ends:

- **register-time** — `register_v1.add_expires` skips stamping for any id in the
  file, so a reconverted demo scene is registered with no `expires`.
- **cleanup-time** — `cleanup_expired_items.py` skips the same ids even if one
  somehow carries an `expires` (belt-and-suspenders).

**Protection is not automatic — three conditions must all hold at registration:**

1. **Image `>= v1.12.0`** — the release that ships the `register_v1` exclude-list
   logic *and* the baked-in `/app/scripts/demo_exclude_ids.txt`.
2. **`EXPIRES_EXCLUDE_FILE` is set** to that path. In the Argo pipeline this is
   the `expires_exclude_file` parameter on `eopf-explorer-convert-v1-s2`
   (platform-deploy) — **empty by default**, set to
   `/app/scripts/demo_exclude_ids.txt` for demo reconversions.
3. **The id is in the file.**

⚠️ **Caveat:** an item registered with `EXPIRES_EXCLUDE_FILE` unset (the default,
e.g. the regular recent-data pipeline) **does** get `expires = now + retention`.
That is fine in practice — demo scenes are reconverted manually with the param
set, and the regular pipeline only ingests recent acquisition dates, never the
old demo dates — and the cleanup-time skip is the backstop regardless.

## Safety model

- **No `expires` ⇒ never deleted.** The primary protection for demo data.
- **`--exclude-file`** — a newline-delimited item-ID denylist, always skipped
  (same format as the migration's `EXPIRES_EXCLUDE_FILE`; `#` comments allowed).
- **`--allowed-bucket`** — every `s3://` asset URL must live under this bucket
  or the item is skipped (`wrong_bucket`). Default `esa-zarr-sentinel-explorer-fra`.
- **Validate-before-delete** — S3 objects are deleted, then re-counted; the STAC
  item is removed **only** if 0 remain. Otherwise the item is retained with
  status `s3_validation_failed`.
- **Dry-run default** — real deletion needs `--execute`. Dry-run still reports
  the S3 object count that *would* be deleted.
- **`--max-runtime-seconds`** — stop at the next item boundary once the budget
  is spent. Use this, not the pod's `activeDeadlineSeconds`, to bound a run:
  the per-item unit (S3 delete → recount → STAC delete → audit line) is not
  atomic, so an external kill can land between the S3 delete and the STAC
  delete and leave an item pointing at data that is gone. The 2026-09-04
  12:00 run was killed that way at 264 of 300 items; it orphaned nothing only
  because the last DELETE landed inside the final ~800 ms. The budget clock
  starts before the discovery query, so a slow search spends it too.

  ⚠️ **Sizing rule:** the budget stops the run from *starting* another item; it
  does not cut short the one in flight — and it does not cut short a `/search`
  page either. Discovery checks the budget on every item read, so at every page
  boundary, but a page fetch is uninterruptible for its whole retry ladder
  (since 2026-09-18 the search client retries transient 5xx, 500 included). So
  whatever hard deadline sits outside the tool (`activeDeadlineSeconds`, a shell
  `timeout`) must be greater than
  `budget + max(worst-case single item, worst-case search page)`, or the kill
  lands mid-item or mid-page anyway and you are back where you started. The
  worst-case page is `9 × 2 × STAC_HTTP_TIMEOUT + 90 s` (9 attempts, connect
  and read each bounded by the timeout, 90 s of sleeps): **≈1170 s at the 60 s
  default, ≈225 s against the gateway that 500s after its own 15 s upstream
  timeout** (9 × 15 + 90), and raising `STAC_HTTP_TIMEOUT` raises the required
  gap with it. Measured in review (2026-09-18, `run_cleanup` against real
  sockets): a 1 s budget overran to 10.1 s wall clock on a 3-attempt ladder,
  exactly the model. The failure this rule prevents: the last discovery page
  enters the ladder just before the budget expires, the pod is SIGKILLed
  mid-`/search`, and there is **no `cleanup_summary` at all** — so the alert
  pair below never fires.

  S2 sizing as deployed (platform-deploy, 2026-09-18): budget 3000 s against a
  4200 s `activeDeadlineSeconds`, a 1200 s gap. That clears an item whose p90 is
  30 s and the ≈225 s gateway page, but **not** the ≈1170 s stalled-socket page
  at the default timeout. Whether to widen the gap or lower `STAC_HTTP_TIMEOUT`
  is a deploy-time decision; re-check it at the pin bump.

  ⚠️ **Requires image `>= v1.15.0`.** The flag is unreleased at the time of
  writing (`main` is v1.14.0). Adding it to a manifest without bumping
  `pipeline_image_version` gives `unrecognized arguments`, exit 2, every tick.

  ⚠️ **v1.15.0 is also breaking for `--max-items`**, which now refuses `0` and
  anything above 10000. `0` previously ran *unbounded*; any manifest passing it
  will start failing at parse time. Checked 2026-09-07: no manifest in
  platform-deploy passes `0` (all pass `100`), but re-check at the pin bump.
  Pass `--max-runtime-seconds ""` to mean "no budget", so an Argo template can
  splice an empty parameter unconditionally.

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--stac-api-url` | (required) | STAC API base URL |
| `--collection` | (required) | Collection to scan |
| `--s3-endpoint` | `AWS_ENDPOINT_URL` env | S3 endpoint URL |
| `--allowed-bucket` | `esa-zarr-sentinel-explorer-fra` | Assets outside it are skipped |
| `--max-items` | `100` | Cap on items processed per run (1–10000; **`0` used to mean UNLIMITED**, not zero — pystac-client gates pagination on a falsy check) |
| `--page-size` | `100` | Items per `/search` request during discovery (1–10000; `""` means the default). A page, not a cap: `--max-items` still bounds the run. Passed as `limit` so the server's default (10) does not turn a 130-item batch into 13 requests, each racing the gateway's 15 s upstream timeout — a hypothesis, not a measurement: a 100-item page is ~4.5 MB of upstream JSON and could move each request *closer* to that cliff; lower it if ticks keep 500ing. ⚠️ **Requires image `>= v1.17.1`** (the first release after v1.17.0; unreleased at the time of writing, and prod pins v1.16.1, so the bump jumps two releases). Adding it to a manifest without bumping `pipeline_image_version` gives `unrecognized arguments`, exit 2, every tick |
| `--max-runtime-seconds` | off | Stop at the next item boundary after N seconds (1–86400; `""` means off). Size the outer deadline as `budget + max(worst item, worst search page)` — the page is ≈225 s against the 15 s gateway, ≈1170 s at the default `STAC_HTTP_TIMEOUT`; see the sizing rule above |
| `--exclude-file` | `EXPIRES_EXCLUDE_FILE` env | Item-ID denylist |
| `--execute` | off (dry-run) | Actually delete |

## Audit log

One JSON line per item on stdout, then a summary line. Fields per item:

```
ts, event, dry_run, collection, item_id, expires,
s3_objects_deleted, s3_objects_failed, s3_remaining, stac_deleted, status
```

`status` is one of: `dry_run`, `deleted`, `s3_validation_failed`,
`auth_required` (STAC DELETE returned 401/403 — expected once the
stac-auth-proxy enforcement lands; wire the bearer in `_session()`),
`already_gone` (re-fetch got 404 — already deleted, idempotent success),
`refetch_failed` (re-fetch errored — the item is skipped rather than acted on
with stale data), `no_expires`, `not_expired`, `excluded`, `wrong_bucket`,
`stac_delete_error` (the DELETE hit a transport error — item retained, run
continues; see #392), `stac_delete_http_<code>`, `s3_transport_error` (**any**
`BotoCoreError` from the delete, the recount or the dry-run count — that covers
endpoint/timeout failures but also `NoCredentialsError`, `EndpointResolutionError`
and `ParamValidationError`, so a misconfigured pod emits one per item rather than
dying on the first; item retained, run continues), `no_s3_urls` (managed assets
but none resolve to `s3://` — fail closed rather than orphan the data), and
`unconfined_s3_url`.

⚠️ On `s3_transport_error` and on an exception-driven `s3_validation_failed`, the
`s3_objects_deleted` and `s3_remaining` fields are **`null`, not `0`**: the helper
unwinds with its tally, so the counts are genuinely unknown and an item may have
lost objects. Treat `null` as "inspect this prefix", never as "nothing happened".

Exit code is `1` if any item ended in `s3_validation_failed`, `auth_required`,
`refetch_failed`, `stac_delete_error`, `s3_transport_error`, or a
`stac_delete_http_*` status.
`already_gone` is a success.

A spent `--max-runtime-seconds` budget is **not itself** a failure — it does not
raise the exit code. But the two are independent: a run that fails two items and
*then* spends its budget emits `time_budget_reached: true` **and exits `1`**. So
a red cleanup workflow can coincide with a spent budget; read `failures`, not the
flag, to know why.

The inverse needs watching too: `time_budget_reached: true` **with**
`processed: 0` is a run that succeeded, exited `0` and drained nothing — discovery
alone spent the budget. Sustained, that is a permanently stalled cron wearing a
green tick, and no exit code will tell you. **That pair is the condition to alert
on.** Discovery checks the budget between items as it reads (since the search
client started retrying, one page can take minutes), so on such a run
`discovered` may be anything from `0` up to `--max-items`: the items it did read
are **not** processed — the budget is already spent, so the delete loop stops at
its first check — and the log says so (`processing none of them`). A healthy
quiet tick is `time_budget_reached: false, discovered: 0`.

A **configuration** error (a bad `--max-runtime-seconds` value) exits `2` at parse
time, before anything is read or deleted, and writes no summary. That is the one
case where a missing summary line is harmless — the run never started.

The final `cleanup_summary` line carries:

```
ts, event, dry_run, collection, discovered, processed, by_status, failures,
time_budget_reached, discovery_seconds, [aborted]
```

`discovered - processed` is what this run found but did not attempt. It is **not**
the remaining backlog — `discovered` is already capped by `--max-items`.

`discovery_seconds` is the wall clock from opening the STAC client to the end of
the discovery read — everything that goes through the retrying search client,
before the first item is touched. It puts a number on the alert pair:
`time_budget_reached: true, processed: 0` means discovery spent the budget, and
this says how much. *What* spent it has two very different answers — a slow scan
of a large expired backlog, or every page burning its retry ladder on gateway
5xx — and the log tells them apart: the search client writes one WARNING line
per status retry (`Retrying POST …/search after HTTP 500: N retries left, next
sleep S s`; urllib3 itself logs those at DEBUG only, so without it the retry
path is invisible in Loki). Many of those lines with `discovered` at a page or
two is the ladder; none of them is the scan. On an aborted run the field is the
time to the abort, so a page that failed after its whole ladder still shows up.

`time_budget_reached` and `aborted` are both deliberately *not* `by_status` keys:
`by_status` counts per-item outcomes, and a dashboard asserting "every status is
`deleted`" must not trip on either of them.

`aborted: true` appears **only** on a run that died before finishing the batch —
anything raising between opening the STAC client and the end of the item loop.
The key is additive and absent from every healthy run, so consumers keyed on
`event == "cleanup_summary"` are unaffected. On such a run `discovered` is
`null` rather than `0` when discovery itself was what raised: the number is
unknown, and `0` would be a claim. The run exits `1`.

**A missing summary line still means the process died — but it is no longer the
only way to see that.** Two cases still write no summary at all: a
**configuration** error (exit `2` at parse time — see above) and an external
kill (`SIGKILL`, OOM, node loss), which is precisely the failure mode
`--max-runtime-seconds` exists to make unnecessary.

## Notes on the discovery query (verified live 2026-07-10)

- `expires` is **filterable** even though it is not an advertised queryable — the
  collection schema is `additionalProperties: true`, so pgstac filters it via
  JSONB. Because both `register_v1` and `stamp_expires` emit a single fixed
  `%Y-%m-%dT%H:%M:%SZ` format, string ordering equals chronological ordering, so
  `expires < now` selects correctly. **The fixed timestamp format is load-bearing**
  — do not introduce a second format.
- ⚠️ The STAC `POST /search` API requires `sortby` as a **list**
  (`[{"field": "properties.expires", "direction": "asc"}]`); a bare string
  `"+properties.expires"` returns **HTTP 400**. This script goes through
  `pystac_client`, which converts the string form for us — but any **direct
  API / curl caller** (e.g. a future Argo raw-HTTP step) must send the array form.

## Local dry-run

```bash
uv run scripts/cleanup_expired_items.py \
  --stac-api-url https://api.explorer.eopf.copernicus.eu/stac \
  --collection sentinel-2-l2a-staging \
  --max-items 5
```

## Operator-paced backlog drain (production)

The monthly `eopf-explorer-historical-cleanup` CronWorkflow ships **suspended**
and **dry-run**. To drain a backlog by hand, submit one-off runs from the cron
template (this bypasses the schedule without un-suspending it):

```bash
# Dry-run a large batch first and review the JSONL:
argo submit --from cronwf/eopf-explorer-cronwf-historical-cleanup \
  -p dry_run=true -p max_items_per_run=200 -n <namespace>

# Then, once reviewed, the real drain:
argo submit --from cronwf/eopf-explorer-cronwf-historical-cleanup \
  -p dry_run=false -p max_items_per_run=200 -n <namespace>
```

Real deletion in production also requires the tier→STANDARD backlog (Plan 1) to
be complete and documented stakeholder approval on coordination#183.

### Throughput

Deletion is bound by S3's per-object delete rate — measured ~75 objects/sec on
OVH (2026-07-14), single-pod and sequential by design (one coherent audit log).
Batch size (`delete_objects` sends 1000 keys/call) doesn't move this; it's
server-side.

⚠️ **Do not plan against any single measured rate.** Two things move it, and
they are independent:

1. *Object count per item*, a property of the expiring cohort. Regression over
   264 audited prod items (2026-09-04): `sec ≈ 0.0211 × objects − 1.43`, i.e.
   ~47 objects/sec, per-item p90 30 s, plus ~88 s fixed per-run overhead. One
   run draws from only one or two registration cohorts, so `count × mean` does
   not smooth out.
2. *Endpoint throughput on the day.* The 2026-09-07 10:00 run deleted 130 items
   averaging 1,191 objects each at **9.2 s/item — ~118 objects/sec**, 2.5× what
   the regression above predicts for that weight (~24 s/item). The visible
   difference: the 12-hourly S2 staging purge was suspended, where on 09-04 it
   ran across the slow runs. Not proof of causation, but enough that the
   regression is a floor-ish estimate, not a constant.

So the same 1,200-object cohort has been measured at both ~24 s and ~9 s an
item. **Size a run against the slow case** and re-measure before raising a cap.

`--max-runtime-seconds` is what makes that safe to do, but be precise about what
it does: **it can only tune downward.** `--max-items` still hard-caps discovery,
so the budget trims a batch that is running slow and does nothing at all on a
fast day. At the live pin the crossover is `3000 / 130 = 23.1 s/item`, and both
measurements above straddle it — on the fast day all 130 items finish in ~1,200 s
and the budget never fires; on the slow day it trims ~9 items. The point of the
pairing is that it lets you raise `--max-items` (which is what actually speeds a
drain) without the run overrunning its window on a bad day.

⚠️ Raising `--max-items` has its own bound to respect: `stale_items` is fully
materialised in memory before the first delete, so the item cap is also the
memory cap. An OOMKill is a SIGKILL — the tool cannot yield on it, and it lands
wherever it lands, including between the S3 delete and the STAC delete. Raise the
cap in steps and watch the pod's memory. If a large backlog is
ever too slow to drain this way, deletes parallelise ~2× at 4 concurrent workers
(same measurement) — deliberately not implemented, to keep the delete path
simple and auditable. Revisit only if the backlog drain becomes a real pain
point.
