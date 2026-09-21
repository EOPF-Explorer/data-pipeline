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
  page either. Discovery checks the budget after every item read, so at every
  page boundary, but never inside a page and not before the first item: opening
  the client (`GET` on the landing page), a **second `GET` of the same landing
  page**, and the first `/search` page all complete before the first check, and
  each is uninterruptible for its whole retry ladder (since 2026-09-18 the
  search client retries transient 5xx, 500 included). The second `GET` is
  pystac resolving `rel:root` lazily on the first `search()`: `from_file` binds
  the root to the object only when the link's href equals the URL opened byte
  for byte, and stac-fastapi builds `rel:root` from `request.base_url`, which
  ends in `/`, while the manifest's `--stac-api-url` is `.../stac` without one.
  So it is **three round trips unless the root href is byte-identical to
  `--stac-api-url`**, in which case two. Whatever hard deadline sits outside
  the tool (`activeDeadlineSeconds`, a shell `timeout`) must therefore be
  greater than `max(budget + max(worst item, worst page), 3 × worst page)`, or
  the kill lands mid-item or mid-page anyway and you are back where you
  started. The first term is the budget expiring with one unit in flight; the
  second is the budget expiring before the first check, and it only wins when
  the budget is smaller than two pages. The worst-case page is
  `9 × 2 × STAC_HTTP_TIMEOUT + 90 s` (9 attempts, connect and read each bounded
  by the timeout — `requests` applies a scalar timeout to both halves — with
  0 + 2 + 4 + 8 + 16 + 20 + 20 + 20 = 90 s of sleeps between them): **≈1170 s
  at the 60 s default, ≈225 s against the gateway that 500s after its own 15 s
  upstream timeout** (9 × 15 + 90); ≈630 s at the default if only the read half
  stalls. That ceiling holds for a socket that stalls completely: the read
  timeout bounds the gap between bytes, not the request, so a server that
  dribbles a byte every 59 s keeps one attempt alive indefinitely.
  Raising `STAC_HTTP_TIMEOUT` raises the required gap with it;
  the tool refuses values above 300 s (a ≈5490 s page) as a typo fence only —
  see the deployment paragraph for what the fleet can actually absorb.
  Measured 2026-09-18 (`run_cleanup` against a local server whose landing page
  carries `rel:root` with a trailing slash, opened without one — the prod
  shape; `--max-runtime-seconds 1`, S3 and the write session mocked): with
  every response delayed 2.0 s, the first budget check came after exactly
  three round trips, `GET /`, `GET /`, `POST /search` — 6.04 s wall,
  `discovery_seconds: 6.0`; with `/search` answering 500, 500, 200 and no
  delay, 2.01 s wall — the 0 s + 2 s sleeps of a 3-attempt ladder — and
  `discovery_seconds: 2.0`. `test_round_trips_before_the_first_search` pins
  both counts (3 with the prod shape, 2 with a byte-identical root href). That
  server was stac-fastapi-shaped; prod's links also pass through
  `eoapi-stac-auth-proxy`'s link rewriter, which has not been sampled, so prod
  may be 2 — the rule keeps 3 as the conservative count, and at 3000/4200 the
  first term binds either way. The failure this rule prevents: the last discovery page enters the ladder just
  before the budget expires, the pod is SIGKILLed mid-`/search`, and there is
  **no `cleanup_summary` at all** — so the alert pair below never fires.

  S2 sizing as deployed (platform-deploy, 2026-09-18): budget 3000 s against a
  4200 s `activeDeadlineSeconds`. At the default timeout that satisfies the
  rule — 3000 + 1170 = 4170 < 4200, and 3 × 1170 = 3510 is smaller — but with
  30 s to spare, thin enough for whatever the deadline's clock counts before
  the container is running. Solving the first term for the timeout,
  `3000 + 18 × T + 90 < 4200` gives **`STAC_HTTP_TIMEOUT ≤ ~61 s`** as this
  deployment's real tolerance: the tool's 300 s ceiling is ~5× wider than
  that, and anything above ~61 s must come with a larger deadline. Whether the
  30 s margin holds is not answered here; re-check it at the pin bump rather
  than widening a gap the arithmetic does not ask for.

  ⚠️ **Requires image `>= v1.15.0`** (released 2026-09-07; prod pins v1.16.1,
  so the flag is available, and the prod manifest passes it). Adding it to a
  manifest without bumping `pipeline_image_version` gives `unrecognized
  arguments`, exit 2, every tick.

  ⚠️ **v1.15.0 is also breaking for `--max-items`**, which now refuses `0` and
  anything above 10000. `0` previously ran *unbounded*; any manifest passing it
  will start failing at parse time. Re-checked 2026-09-18: no cleanup manifest
  in platform-deploy passes `0` (prod passes `300`, staging `100`), but
  re-check at the pin bump.
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
| `--page-size` | `100` | Items per `/search` request during discovery (1–10000; `""` means the default), clamped to `--max-items`. A page, not a cap: `--max-items` still bounds the run. Passed as `limit` so the server's default (10) does not turn the live 300-item batch into 30 requests, each racing the gateway's 15 s upstream timeout — a hypothesis, not a measurement, and one the tier-down cron argues against (it died the same day already passing `limit=100`): a 100-item page is ~4.5 MB of upstream JSON and could move each request *closer* to that cliff; lower it if ticks keep 500ing. Like `STAC_HTTP_TIMEOUT` it makes one request arbitrarily long; its 10000 ceiling is a memory typo fence (~450 MB of JSON), not a time bound. Logged in the `Cleanup start` line. ⚠️ **Requires image `>= v1.17.1`** (the first release after v1.17.0; unreleased at the time of writing). Prod pins v1.16.1, so its bump jumps two releases; devseed-staging's suspended cron pins v1.13.2, six behind (v1.14.0, v1.15.0, v1.16.0, v1.16.1, v1.17.0, v1.17.1). Adding it to a manifest without bumping `pipeline_image_version` gives `unrecognized arguments`, exit 2, every tick |
| `--max-runtime-seconds` | off | Stop at the next item boundary after N seconds (1–86400; `""` means off). Size the outer deadline as `max(budget + max(worst item, worst page), 3 × worst page)` — the page is ≈225 s against the 15 s gateway, ≈1170 s at the default `STAC_HTTP_TIMEOUT`; see the sizing rule above |
| `STAC_HTTP_TIMEOUT` (env) | `60` | Per-request connect and read timeout, in seconds, for the retrying search client (`(0, 300]`; `""` and unset mean the default). It is the `T` in the sizing rule: a page is uninterruptible for `9 × 2 × T + 90 s`, and at the deployed 3000/4200 anything above ~61 s breaks the rule. `0`, negatives, garbage and values above 300 are rejected — not at parse time but when the read client opens, inside the guard: the run exits `1` with a normal `cleanup_summary` carrying `aborted: true` and `discovered: null`. ⚠️ **Requires image `>= v1.17.1`** (nothing under `scripts/` reads it on `origin/main` or at `v1.17.0`). Unlike a flag, an older image does not reject it — it **ignores it silently**: on prod's v1.16.1 the tick stays green and nothing changes, so set it only together with the pin bump |
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

A **configuration** error on a flag (a bad `--max-runtime-seconds`, `--max-items`
or `--page-size` value) exits `2` at parse time, before anything is read or
deleted, and writes no summary. That is the one case where a missing summary
line is harmless — the run never started. A bad `STAC_HTTP_TIMEOUT` is different
on purpose: it is read when the search client opens, inside the guard, so it
exits `1` with a normal `cleanup_summary` carrying `aborted: true` and
`discovered: null` — the tick stays visible in the audit stream.

The final `cleanup_summary` line carries:

```
ts, event, dry_run, collection, discovered, processed, by_status, failures,
time_budget_reached, discovery_seconds, [aborted]
```

`discovered - processed` is what this run found but did not attempt. It is **not**
the remaining backlog — `discovered` is already capped by `--max-items`.

`discovery_seconds` is the wall clock from opening the STAC read client to the
end of the discovery read — the landing-page `GET`s and every `/search` page,
everything that goes through the retrying search client and nothing else: the
S3 client and the write session are built before the clock starts, so a
credential-chain stall (boto3's IMDS probe in a pod without instance
credentials) does not land in this number, and if that stall is what aborts
the run the field is `null` (the read client never opened). It puts a number on
the alert pair:
`time_budget_reached: true, processed: 0` means discovery spent the budget, and
this says how much. *What* spent it has two very different answers — a slow scan
of a large expired backlog, or every page burning its retry ladder on gateway
5xx — and the log tells them apart: the search client writes one WARNING line
per status retry (`Retrying POST …/search after HTTP 500: N retries left, next
sleep S s`; urllib3 itself logs those at DEBUG only, so without it the retry
path is invisible in Loki). Many of those lines with `discovered` at a page or
two is the ladder; none of them is the scan. On a run aborted *during discovery*
the field is the time to the abort, so a page that failed after its whole ladder
still shows up; a run aborted later, in the delete loop, keeps its real
discovery time.

`time_budget_reached` and `aborted` are both deliberately *not* `by_status` keys:
`by_status` counts per-item outcomes, and a dashboard asserting "every status is
`deleted`" must not trip on either of them.

`aborted: true` appears **only** on a run that died before finishing the batch —
anything raising between building the write session and the end of the item loop
(the S3 client and the STAC read client open after it, so a boto3 stall counts).
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
fast day. At the live manifest (`--max-items 300`, budget 3000 s) the crossover
is `3000 / 300 = 10 s/item`, and both measurements above straddle it — on the
fast day (9.2 s/item) all 300 items finish in ~2,760 s and the budget never
fires, with ~240 s to spare; on the slow day (~24 s/item) a full 300 would need
~7,200 s, so the budget stops the run at ~125 items and trims ~175, every tick,
exit 0, `time_budget_reached: true` with `processed` ≈ 125. That is not the
alert pair (`processed` is not 0) and it is not a stalled cron: 125 × 24 ticks
is ~3,000 items/day against the manifest's own "~7200/day gross" ceiling, and
the backlog only grows if more than that expires per day. But it does mean the
slow-day drain rate is set by the budget, not by `--max-items`, and raising the
cap alone changes nothing on such a day. The point of the pairing is that it
lets you raise `--max-items` (which is what actually speeds a drain on a fast
day) without the run overrunning its window on a bad one.

⚠️ Raising `--max-items` has its own bound to respect: `stale_items` is fully
materialised in memory before the first delete, so the item cap is also the
memory cap. An OOMKill is a SIGKILL — the tool cannot yield on it, and it lands
wherever it lands, including between the S3 delete and the STAC delete. Raise the
cap in steps and watch the pod's memory. If a large backlog is
ever too slow to drain this way, deletes parallelise ~2× at 4 concurrent workers
(same measurement) — deliberately not implemented, to keep the delete path
simple and auditable. Revisit only if the backlog drain becomes a real pain
point.
