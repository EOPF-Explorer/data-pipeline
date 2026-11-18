#!/usr/bin/env python3
"""Keep ``sentinel-2-l2a-new`` in step with EODC re-processing, by eopf-geozarr version.

The hourly register (``register_proxy.py --items-json``) is create-only, so a scene EODC
re-processes with a newer eopf-geozarr (``processing:software["EOPF_GEOZARR"]``) keeps its old
item in the Explorer, and one the clean deleted never comes back. This is the separate,
bounded run that closes both gaps. It diffs EODC's source collection against the target over
a fixed acquisition range, by id and version only, and for each source id:

- ``eo:cloud_cover`` absent or not below ``MAX_CLOUD_COVER`` (the search's CQL2 filter
  asks EODC for the same, and this check does not rely on it being honoured), below the
  floor, without the key, unreadable, or a pre-release: skipped (counted);
- absent from the target and from the exclusion collection: CREATED, by
  ``register_proxy.create_item``, and recorded in ``--created-ids`` exactly as the register
  records it;
- absent from the target but in the exclusion collection (``sentinel-2-l2a``, our converted
  archive): skipped. EODC's 30 Sep 2026 burst holds ~800-1,000 such scenes. Only creates
  are checked against it: an id already in the target is replaced like any other, which
  adds no duplicate;
- in the target with an older version, none, or a pre-release, dev or local-label one:
  REPLACED. A final release replaces a non-final target even where PEP 440 orders it lower
  (``0.13.1`` over ``0.14.0rc1``);
- in the target at an equal or newer final version: skipped. Never a downgrade of a final
  release, so following an EODC revert is a manual decision.

Everything a run may do is fixed in the constants below, never by a flag, and any
``--collection`` but ``sentinel-2-l2a-new`` is refused. Each summary key counts source ids,
and together they add up to all of them.

``--max-writes N`` bounds the run from inside: the creates and replaces are taken newest
acquisition first until N POSTs or PUTs have been sent (would have been, in a dry run), and
the ids not reached are reported as ``remaining``; re-run until it is 0. ``--time-budget
SECONDS`` (optional) also stops it, between ids, once the run has lasted that long: it logs
``time_budget_reached``, and the ids not reached are ``remaining`` too. An id that ends
before its write (an old-generation or unbuildable source, a source changed since the diff,
a target gone, unreadable or already caught up) is counted by reason and does not use
``--max-writes``, so ids that end that way on every run never hold up the ones behind them.
10 failures in a row (an outage) stop the run; skips never count toward them. A SIGTERM
(``argo stop`` or ``terminate``, a pod deletion) lets the id in flight finish, write and line,
then starts no other: ``stop_requested``, and the rest are ``remaining``.
A ``--replaced-ids`` line names its item ``item_id``, not ``id``, so
``rollback_created_items.py`` refuses that file rather than deleting the replaced items,
which existed before the run.

Exit 1, after the summary, if a write failed or was refused, if a read an id needed failed
(the source fetch, the target GET), if the write client cannot open, or if no source item in
the range is usable: a renamed key or a new stamp format must not pass for a converged run.
Exit 1 before any summary if a catalogue read fails; exit 2 if an argument is refused. An
unbuildable source or an unreadable target ends the same way on every run, so failing on it
would fail the run that converges too: it is counted, and named on a ``Look:`` line after the
summary, exit 0.
"""

import argparse
import itertools
import json
import logging
import signal
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlparse

import requests
import stac_auth
from packaging.version import Version
from processing_software import is_final, read_version
from pystac import Item
from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO
from register_proxy import (  # also configures logging, as the register does
    DEFAULT_SOURCE_COLLECTION,
    DEFAULT_SOURCE_STAC_API,
    FLOOR,
    MAX_CONSECUTIVE_FAILURES,
    SOFTWARE_KEY,
    TRANSITION_COLLECTION,
    CreatedIdsError,
    assert_no_s3_urls,
    build_proxy_item,
    check_version,
    create_item,
    fetch_source_item,
    is_old_generation,
    item_url,
    time_budget_arg,
)

logger = logging.getLogger(__name__)

# The policy for TRANSITION_COLLECTION, the one target: no flag sets any of it. Its key and
# floor are register_proxy's, shared with the hourly register.
# The target's earliest acquisition is 2026-09-29T08:26Z (census, 17 Nov 2026).
ACQUIRED_FROM = datetime(2026, 9, 29, tzinfo=UTC)
EXCLUDE_COLLECTION = "sentinel-2-l2a"
# The register template's --max-cloud-cover (the converter's filter, plan rev 2 D4):
# a scene the register would never create is not created here either.
MAX_CLOUD_COVER = 90.0

# The census read 34,672 target items in 35 pages of 1000 with these fields (17 Nov 2026).
PAGE_SIZE = 1000
FIELDS = {
    "include": [
        "id",
        "properties.datetime",
        "properties.processing:software",
        "properties.eo:cloud_cover",
    ]
}
IDS_PER_SEARCH = 100
WRITE_TIMEOUT_S = 30

# Diff verdicts that mean "the source is not one a write may come from".
UNUSABLE = (
    "skipped_cloud_cover",
    "skipped_no_key",
    "skipped_below_floor",
    "skipped_unreadable",
    "skipped_prerelease",
)
SUMMARY_KEYS = (
    "created",
    "replaced",
    "exists",
    "gone",
    "would_create",
    "would_replace",
    *UNUSABLE,
    "skipped_in_archive",
    "skipped_not_newer",
    "skipped_source_changed",
    "skipped_old_generation",
    "skipped_unbuildable",
    "refused_target_unreadable",
    "failed",
    "remaining",
)
SAMPLES = 5
# Ends that recur on every run, so they exit 0, but say EODC's or the target's items changed.
LOOK = ("skipped_unbuildable", "refused_target_unreadable")


class ReplacedIdsError(Exception):
    """The replaced-ids list cannot be written: the run stops, as for ``CreatedIdsError``."""


def classify(source: dict, target: dict | None) -> str:
    """The diff verdict for one source id, from its properties and the target's (or None).

    ``create`` and ``replace`` are candidates; ``create`` still has to pass the exclusion
    collection. A target without the key counts as older than any version: EODC's items from
    before EOPF_GEOZARR was stamped have none. A target on a non-final version counts as
    older than any final one, so it is brought back to a release. A source without a numeric
    ``eo:cloud_cover`` below the maximum is skipped, as the search's filter would have
    dropped it. Properties that are not an object are unreadable, on either side.
    """
    if not isinstance(source, dict):
        return "skipped_unreadable"
    cloud = source.get("eo:cloud_cover")
    if not isinstance(cloud, int | float) or isinstance(cloud, bool):  # a bool is an int
        return "skipped_cloud_cover"
    if not cloud < MAX_CLOUD_COVER:
        return "skipped_cloud_cover"
    version, why = check_version(source)  # the register's check: one rule for both writers
    if why:
        return f"skipped_{why}"
    assert version is not None  # noqa: S101  # nosec B101 -- check_version sets it when why is None
    if target is None:
        return "create"
    if not isinstance(target, dict):
        return "refused_target_unreadable"
    try:
        current = read_version(target, SOFTWARE_KEY)
    except ValueError:
        return "refused_target_unreadable"
    if current is not None and is_final(current) and version <= current:
        return "skipped_not_newer"
    return "replace"


def walk(client: Client, **search: Any) -> dict[str, dict]:
    """Every item the search pages through, as id -> properties (``FIELDS`` only)."""
    return {
        f["id"]: f.get("properties") or {}
        for f in client.search(limit=PAGE_SIZE, fields=FIELDS, **search).items_as_dicts()
    }


def present_in(client: Client, collection: str, ids: list[str]) -> set[str]:
    """Which of ``ids`` ``collection`` holds, ``IDS_PER_SEARCH`` ids per search.

    Refuses (RuntimeError) an answer naming an id it did not ask for: a server ignoring
    ``ids`` would otherwise page its whole collection once per batch.
    """
    present: set[str] = set()
    for batch in itertools.batched(ids, IDS_PER_SEARCH):  # noqa: B911 -- pre-commit's mypy rejects strict=
        search = client.search(collections=[collection], ids=batch, limit=PAGE_SIZE, fields=FIELDS)
        for found in search.items_as_dicts():  # page by page: refused on the first page
            if found["id"] not in batch:
                raise RuntimeError(
                    f"{collection}: an ids= search returned ids it was not asked for"
                )
            present.add(found["id"])
    return present


def not_the_item(body: Any, item_id: str, collection: str, key: str) -> str | None:
    """None if ``body`` is item ``item_id`` of ``collection`` with a readable version, else why."""
    if not isinstance(body, dict) or body.get("type") != "Feature":
        return "the body is not a STAC item"
    if body.get("id") != item_id or body.get("collection") != collection:
        return f"the body is item {body.get('id')!r} of {body.get('collection')!r}"
    if not isinstance(body.get("properties"), dict):
        return "the body has no properties object"
    try:
        read_version(body["properties"], key)
    except ValueError as exc:
        return f"its {key} cannot be read ({exc})"
    return None


def open_write_client(url: str) -> Client:
    """A Client whose session carries a fresh Bearer per request and NO retries.

    ``stac_auth.open_client`` keeps pystac-client's default ``Retry(total=5)``, which re-sends
    a PUT on a read timeout and sleeps a server's Retry-After. Here the session keeps
    requests' default adapter (no retries): every write is sent once, and a failure is
    reported, not repeated. Each call passes its own timeout. Opening it fetches the landing
    page, under the same timeout: the run holds the register's semaphore, so a hang here
    would hold up the hourly register. ``Client.open`` overwrites the ``stac_io``'s timeout
    with its own (None when not given), so the one that applies is ``Client.open``'s; the
    ``StacApiIO`` copy, as in ``stac_auth.open_resilient_client``, only mirrors it.
    """
    client = Client.open(
        url,
        stac_io=StacApiIO(timeout=WRITE_TIMEOUT_S, max_retries=None),
        timeout=WRITE_TIMEOUT_S,
    )
    assert client._stac_io is not None  # noqa: S101  # nosec B101 -- set by Client.open
    client._stac_io.session.auth = stac_auth.bearer_auth
    return client


@dataclass
class Run:
    """One reconcile run's target, its write client and its two lists."""

    collection: str
    stac_api_url: str
    raster_api_url: str
    client: Client
    dry_run: bool
    created_ids: Path | None
    replaced_ids: Path | None
    run_id: str
    # POSTs and PUTs sent (or, in a dry run, that would be): what --max-writes bounds.
    writes: int = 0

    def build(self, item_id: str, target: dict | None = None) -> tuple[Item, Version] | str:
        """Fetch and build one source item as the register would, or a skip verdict.

        The fetched item, not the diff's snapshot, is what gets written, so it is classified
        again, against ``target`` (the target item's properties as just read, None for a
        create): EODC may have changed it since the diff. A source ``build_proxy_item``
        refuses is ``skipped_unbuildable``, not a failure: the same item fails the same way
        on every run, so it must neither hold up the ids behind it nor trip the
        failures-in-a-row stop.
        """
        source = fetch_source_item(DEFAULT_SOURCE_STAC_API, DEFAULT_SOURCE_COLLECTION, item_id)
        try:  # an item EODC changed shape (assets: null, bands as strings) raises any of these
            if is_old_generation(source):
                logger.warning(
                    "   ⚠️  %s: old-generation source (raster:scale on SR_*), skipped", item_id
                )
                return "skipped_old_generation"
            properties = source.get("properties") or {}
            verdict = classify(properties, target)
            if verdict != ("create" if target is None else "replace"):
                logger.warning("   ⚠️  %s: the source is now %s, skipped", item_id, verdict)
                return "skipped_source_changed"
            item = build_proxy_item(
                source, self.collection, self.raster_api_url, self.stac_api_url, expires=None
            )
            assert_no_s3_urls(item)
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            logger.warning("   ⚠️  %s: cannot be built (%s), skipped", item_id, exc)
            return "skipped_unbuildable"
        version = read_version(properties, SOFTWARE_KEY)
        assert version is not None  # noqa: S101  # nosec B101 -- classify said create/replace
        return item, version

    def create(self, item_id: str) -> str:
        """POST one absent id under --stac-api-url, as the replace, recorded in --created-ids
        by the register's ``create_item``."""
        built = self.build(item_id)
        if isinstance(built, str):
            return built
        item, version = built
        self.writes += 1  # before the POST: one that fails may still have landed
        if self.dry_run:
            logger.info("   would create %s (%s %s)", item_id, SOFTWARE_KEY, version)
            return "would_create"
        assert self.created_ids is not None  # noqa: S101  # nosec B101 -- main requires it
        # An uncertain line (a 5xx or no answer) names an item only this run may have made: the
        # id was absent from the target at diff time, and the workflow holds the register's
        # semaphore, so no other run can have created it in between.
        return create_item(self.client, self.collection, item, self.created_ids, self.stac_api_url)

    def record_replace(self, item_id: str, **fields: Any) -> None:
        """Append one line to --replaced-ids; a failed append stops the run."""
        assert self.replaced_ids is not None  # noqa: S101  # nosec B101 -- main requires it
        line = {
            "item_id": item_id,
            "collection": self.collection,
            "run_id": self.run_id,
            "ts": datetime.now(UTC).isoformat(),
            **fields,
        }
        try:
            with self.replaced_ids.open("a") as out:
                out.write(json.dumps(line) + "\n")
        except OSError as exc:
            raise ReplacedIdsError(
                f"cannot record {item_id} in {self.replaced_ids} ({exc})"
            ) from exc

    def replace(self, item_id: str, listed: dict) -> str:
        """Re-read the target item and check it, then build, record the pre-image and PUT.

        ``listed`` is the source's properties as the diff read them. The target comes first,
        compared with that version: an item gone or already caught up costs no source fetch.
        """
        io = self.client._stac_io
        assert io is not None  # noqa: S101  # nosec B101 -- set by Client.open
        session = io.session
        url = item_url(self.stac_api_url, self.collection, item_id)
        got = session.get(url, timeout=WRITE_TIMEOUT_S, allow_redirects=False)
        if got.status_code == 404:
            logger.info("   ⏭️  %s: gone from the target (404), not replaced", item_id)
            return "gone"
        if got.status_code != 200:
            raise RuntimeError(f"refused: the target GET answered {got.status_code}")
        current_item = got.json()
        why = not_the_item(current_item, item_id, self.collection, SOFTWARE_KEY)
        if why:  # as in the diff: the same answer on every run, so no failure
            logger.warning("   ⚠️  %s: refused, %s", item_id, why)
            return "refused_target_unreadable"
        current = read_version(current_item["properties"], SOFTWARE_KEY)
        if classify(listed, current_item["properties"]) != "replace":
            logger.info("   ⏭️  %s: the target is already %s %s", item_id, SOFTWARE_KEY, current)
            return "skipped_not_newer"
        built = self.build(item_id, current_item["properties"])
        if isinstance(built, str):
            return built
        item, version = built
        change = f"{SOFTWARE_KEY} {current} -> {version}"
        self.writes += 1  # before the PUT: one that fails may still have landed
        if self.dry_run:
            logger.info("   would replace %s (%s)", item_id, change)
            return "would_replace"
        self.record_replace(
            item_id,
            phase="before_put",
            old_version=None if current is None else str(current),
            new_version=str(version),
            pre_image=current_item,
        )
        try:
            put = session.put(
                url,
                json=item.to_dict(transform_hrefs=False),
                timeout=WRITE_TIMEOUT_S,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            self.record_replace(item_id, phase="after_put", outcome="uncertain", error=str(exc))
            raise
        status = put.status_code
        # pgstac's update is a delete + create: a 404 means the item was deleted after the GET
        # (the clean), and nothing was written. The next run creates it, unless it is in the
        # exclusion collection: then it is skipped_in_archive, as the archive holds it.
        if 200 <= status < 300:
            outcome = "replaced"
        elif status == 404:
            outcome = "gone"
        else:  # a 5xx may have committed; any other answer is a refusal
            outcome = "uncertain" if status >= 500 else "refused"
        self.record_replace(item_id, phase="after_put", outcome=outcome, status=status)
        if outcome in ("uncertain", "refused"):
            raise RuntimeError(f"the PUT answered {status} ({outcome})")
        if outcome == "gone":
            logger.info("   ⏭️  %s: gone from the target (404 on the PUT), not replaced", item_id)
        else:
            logger.info("🔁 Replaced %s (%s)", item_id, change)
        return outcome


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--collection", required=True, help=f"Must be {TRANSITION_COLLECTION}")
    parser.add_argument("--stac-api-url", required=True, help="Target STAC API")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler base URL for links")
    parser.add_argument(
        "--max-writes",
        type=int,
        required=True,  # a dry run too: without it, it would check nothing and look green
        metavar="N",
        help="POSTs + PUTs this run sends at most (a dry run: would send); the ids not "
        "reached are reported as remaining",
    )
    parser.add_argument(
        "--time-budget",
        type=time_budget_arg,  # the register's check: positive, finite, at most its ceiling
        metavar="SECONDS",
        help="Start no new id after this many seconds since the start; the rest are remaining",
    )
    parser.add_argument("--dry-run", action="store_true", help="Diff and check; write nothing")
    parser.add_argument(
        "--created-ids",
        type=Path,
        metavar="PATH",
        help="JSON lines of the items created (rollback_created_items.py input)",
    )
    parser.add_argument(
        "--replaced-ids",
        type=Path,
        metavar="PATH",
        help="JSON lines of each replaced item's pre-image and outcome",
    )
    parser.add_argument("--run-id", default="", help="Written on each --replaced-ids line")
    args = parser.parse_args(argv)
    if args.max_writes < 1:
        parser.error(f"--max-writes must be >= 1 (got {args.max_writes})")
    return args


def report(
    collection: str, n_source: int, counts: Counter[str], fates: dict[str, list[str]]
) -> int:
    """Log the run's one-line result (fixed keys, each counting source ids); the exit code.

    1 only if an id ``failed``: a write, or a read it needed. A ``LOOK`` end is logged with
    sample ids instead: it recurs on every run, so failing on it would fail the last one too.
    """
    summary = " ".join(f"{key}={counts[key]}" for key in SUMMARY_KEYS)
    logger.info("Summary for %s: source=%d %s", collection, n_source, summary)
    if counts["remaining"]:
        logger.info("remaining: %d id(s) not reached; re-run to continue", counts["remaining"])
    for key in LOOK:
        if counts[key]:
            logger.warning(
                "Look: %s=%d, e.g. %s", key, counts[key], ", ".join(fates[key][:SAMPLES])
            )
    if counts["failed"]:
        logger.error("Failed: %d id(s): a write, or a read it needed, failed", counts["failed"])
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    started = monotonic()  # from here, as the register: the run holds its semaphore from the start
    args = parse_args(argv)
    if args.collection != TRANSITION_COLLECTION:
        logger.error("Refusing --collection %r: only %s", args.collection, TRANSITION_COLLECTION)
        return 1
    for url, name in [
        (args.stac_api_url, "--stac-api-url"),
        (args.raster_api_url, "--raster-api-url"),
    ]:
        if urlparse(url).scheme != "https":
            logger.error("Error: %s must be an HTTPS URL, got: %r", name, url)
            return 1
    if not args.dry_run:
        required = {
            "--created-ids": args.created_ids,
            "--replaced-ids": args.replaced_ids,
            "--run-id": args.run_id,
        }
        missing = [flag for flag, value in required.items() if not value]
        if missing:
            logger.error("A real run needs %s (or pass --dry-run)", ", ".join(missing))
            return 1
        try:  # here, not after the first write: an unwritable list would leave writes unrecorded
            for path in (args.created_ids, args.replaced_ids):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
        except OSError as exc:
            logger.error("Cannot write the run's lists: %s", exc)
            return 1

    # Read everything before the first write; a failure here writes nothing.
    try:
        source_client = stac_auth.open_resilient_client(DEFAULT_SOURCE_STAC_API)
        target_client = stac_auth.open_resilient_client(args.stac_api_url)
        sources = walk(
            source_client,
            collections=[DEFAULT_SOURCE_COLLECTION],
            datetime=(ACQUIRED_FROM, None),
            filter={"op": "<", "args": [{"property": "eo:cloud_cover"}, MAX_CLOUD_COVER]},
            filter_lang="cql2-json",
        )
        targets = walk(target_client, collections=[args.collection])
        verdicts = {i: classify(p, targets.get(i)) for i, p in sources.items()}
        to_create = sorted(i for i, v in verdicts.items() if v == "create")
        for item_id in present_in(target_client, EXCLUDE_COLLECTION, to_create):
            verdicts[item_id] = "skipped_in_archive"
    except Exception as exc:  # noqa: BLE001 - any read failure stops the run before a write
        logger.error("Reading the catalogues failed, nothing written: %s", exc)
        return 1

    counts = Counter(v for v in verdicts.values() if v not in ("create", "replace"))
    logger.info(
        "Diff for %s (%s >= %s, acquired from %s): source=%d target=%d",
        args.collection,
        SOFTWARE_KEY,
        FLOOR,
        ACQUIRED_FROM.date(),
        len(sources),
        len(targets),
    )
    fates: defaultdict[str, list[str]] = defaultdict(list)  # verdict, then outcome -> ids
    for item_id in sorted(verdicts):
        fates[verdicts[item_id]].append(item_id)
    for verdict, ids in sorted(fates.items()):
        logger.info("   %s: %d, e.g. %s", verdict, len(ids), ", ".join(ids[:SAMPLES]))

    # Newest acquisition first, then id: the same order every run, so a canary's ids are the
    # ones its dry run listed.
    actions = sorted(
        ((verdicts[i], i) for i in verdicts if verdicts[i] in ("create", "replace")),
        key=lambda action: (str(sources[action[1]].get("datetime") or ""), action[1]),
        reverse=True,
    )

    usable = len(sources) - sum(counts[key] for key in UNUSABLE)
    if not usable:
        report(args.collection, len(sources), counts, fates)
        logger.error(
            "Refusing: none of the %d source item(s) since %s has a usable %s >= %s. A renamed "
            "key, a new stamp format or an empty source would otherwise pass for a converged run",
            len(sources),
            ACQUIRED_FROM.date(),
            SOFTWARE_KEY,
            FLOOR,
        )
        return 1

    if not actions:  # converged: no write client to open
        return report(args.collection, len(sources), counts, fates)
    try:
        client = open_write_client(args.stac_api_url)
    except Exception as exc:  # noqa: BLE001 - nothing written yet; say so with the counts
        counts["remaining"] = len(actions)
        report(args.collection, len(sources), counts, fates)
        logger.error("Opening the write client failed, nothing written: %s", exc)
        return 1
    run = Run(
        collection=args.collection,
        stac_api_url=args.stac_api_url,
        raster_api_url=args.raster_api_url,
        client=client,
        dry_run=args.dry_run,
        created_ids=args.created_ids,
        replaced_ids=args.replaced_ids,
        run_id=args.run_id,
    )
    # argo stop/terminate and a pod deletion send SIGTERM, then SIGKILL after the pod's grace
    # period: finish the id in flight (its write and its line), start no other.
    stop_requested: list[int] = []
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda signum, _frame: stop_requested.append(signum))
    streak = visited = 0
    try:
        for kind, item_id in actions:
            # Every stop falls between ids, never inside one: a write and its line finish together.
            if stop_requested:
                logger.warning("stop_requested: SIGTERM, no new id started")
                break
            if run.writes == args.max_writes:
                break
            if args.time_budget is not None and monotonic() - started >= args.time_budget:
                logger.warning("time_budget_reached: %g s, no new id started", args.time_budget)
                break
            if streak == MAX_CONSECUTIVE_FAILURES:
                logger.error("Stopping after %d failures in a row", streak)
                break
            visited += 1
            try:
                if kind == "create":
                    outcome = run.create(item_id)
                else:
                    outcome = run.replace(item_id, sources[item_id])
                counts[outcome] += 1
                fates[outcome].append(item_id)
                streak = 0
            except (CreatedIdsError, ReplacedIdsError) as exc:
                logger.error("Stopping: %s", exc)
                counts["failed"] += 1
                break
            except Exception as exc:  # noqa: BLE001 - one bad item must not hide the rest
                counts["failed"] += 1
                streak += 1
                logger.error("   ❌ %s %s: %s", kind, item_id, exc)
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL if previous is None else previous)
    counts["remaining"] = len(actions) - visited
    return report(args.collection, len(sources), counts, fates)


if __name__ == "__main__":
    sys.exit(main())
