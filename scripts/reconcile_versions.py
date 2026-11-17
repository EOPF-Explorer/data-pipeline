#!/usr/bin/env python3
"""Keep ``sentinel-2-l2a-new`` in step with EODC re-processing, by eopf-geozarr version.

The hourly register (``register_proxy.py --items-json``) is create-only, so a scene EODC
re-processes with a newer eopf-geozarr (``processing:software["EOPF_GEOZARR"]``) keeps its old
item in the Explorer, and one the clean deleted never comes back. This is the separate,
bounded run that closes both gaps. It diffs EODC's source collection against the target over
a fixed acquisition range, by id and version only, and for each source id:

- ``eo:cloud_cover`` absent or not below the policy's maximum (the search's CQL2 filter
  asks EODC for the same, and this check does not rely on it being honoured), below the
  floor, without the key, unreadable, or a pre-release: skipped (counted);
- absent from the target and from the exclusion collection: CREATED, by
  ``register_proxy.create_item``, and recorded in ``--created-ids`` exactly as the register
  records it;
- absent from the target but in the exclusion collection (``sentinel-2-l2a``, our converted
  archive): skipped. EODC's 30 Sep 2026 burst holds ~800-1,000 such scenes;
- in the target with an older version (or none): REPLACED;
- in the target at an equal or newer version: skipped. Never a downgrade, so following an
  EODC revert is a manual decision.

Everything a run may do to a target is fixed in ``POLICIES``, never by a flag, and any other
target is refused. Each summary key counts source ids, and together they add up to all of them.

The diff is batched: one paged, ``fields``-limited search of the source and one of the target,
plus one ``ids=`` search per ``IDS_PER_SEARCH`` create candidates against the exclusion
collection. Never one search per item.

``--max-writes N`` bounds the run from inside: the creates and replaces are taken newest
acquisition first until N POSTs or PUTs have been sent (would have been, in a dry run), and
the ids not reached are reported as ``remaining``; re-run until it is 0. An id that ends
before its write (an old-generation or unbuildable source, a source changed since the diff,
a target gone or already caught up) is counted by reason and does not use the bound, so ids
that end that way on every run never hold up the ones behind them. A replace GETs the target
item on a session that never retries and checks that the body is that item with a readable
version. It then re-compares, appends the pre-image to ``--replaced-ids`` and only then PUTs.
A ``--replaced-ids`` line names its item ``item_id``, not ``id``, so
``rollback_created_items.py`` refuses that file rather than deleting the replaced items,
which existed before the run.

Exit 1, after the summary, if any write failed or was refused, if a target item's version
cannot be read, if a source item could not be built, or if no source item in the range is
usable: a renamed key, a new stamp format or a new item schema must not pass for a converged
run.
"""

import argparse
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
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
    MAX_CONSECUTIVE_FAILURES,
    TRANSITION_COLLECTION,
    CreatedIdsError,
    assert_no_s3_urls,
    build_proxy_item,
    create_item,
    fetch_source_item,
    is_old_generation,
    record_created,
    source_item_url,
)

logger = logging.getLogger(__name__)

SOFTWARE_KEY = "EOPF_GEOZARR"


@dataclass(frozen=True)
class Policy:
    """What a reconcile run may do to one target collection."""

    floor: Version
    acquired_from: datetime
    exclude_collection: str
    max_cloud_cover: float
    key: str = SOFTWARE_KEY
    source_stac_api: str = DEFAULT_SOURCE_STAC_API
    source_collection: str = DEFAULT_SOURCE_COLLECTION


POLICIES = {
    TRANSITION_COLLECTION: Policy(
        # Loïc, 2026-11-17: the collection keeps EOPF_GEOZARR >= 0.13.0, the #519 clean's floor.
        floor=Version("0.13.0"),
        # The target's earliest acquisition is 2026-09-29T08:26Z (census, 17 Nov 2026).
        acquired_from=datetime(2026, 9, 29, tzinfo=UTC),
        exclude_collection="sentinel-2-l2a",
        # The register template's --max-cloud-cover (the converter's filter, plan rev 2 D4):
        # a scene the register would never create is not created here either.
        max_cloud_cover=90.0,
    ),
}

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


class ReplacedIdsError(Exception):
    """The replaced-ids list cannot be written: the run stops, as for ``CreatedIdsError``."""


def classify(source: dict, target: dict | None, policy: Policy) -> str:
    """The diff verdict for one source id, from its properties and the target's (or None).

    ``create`` and ``replace`` are candidates; ``create`` still has to pass the exclusion
    collection. A target without the key counts as older than any version: EODC's items from
    before EOPF_GEOZARR was stamped have none. A source without a numeric ``eo:cloud_cover``
    below the maximum is skipped, as the search's filter would have dropped it.
    """
    cloud = source.get("eo:cloud_cover")
    if not isinstance(cloud, int | float) or not cloud < policy.max_cloud_cover:
        return "skipped_cloud_cover"
    try:
        version = read_version(source, policy.key)
    except ValueError:
        return "skipped_unreadable"
    if version is None:
        return "skipped_no_key"
    if version < policy.floor:
        return "skipped_below_floor"
    if not is_final(version):
        return "skipped_prerelease"
    if target is None:
        return "create"
    try:
        current = read_version(target, policy.key)
    except ValueError:
        return "refused_target_unreadable"
    if current is not None and version <= current:
        return "skipped_not_newer"
    return "replace"


def walk(client: Client, **search: Any) -> dict[str, dict]:
    """Every item the search pages through, as id -> properties (``FIELDS`` only)."""
    found: dict[str, dict] = {}
    for page in client.search(limit=PAGE_SIZE, fields=FIELDS, **search).pages_as_dicts():
        for feature in page["features"]:
            found[feature["id"]] = feature.get("properties") or {}
    return found


def present_in(client: Client, collection: str, ids: list[str]) -> set[str]:
    """Which of ``ids`` ``collection`` holds, ``IDS_PER_SEARCH`` ids per search.

    Refuses (RuntimeError) an answer naming an id it did not ask for: a server ignoring
    ``ids`` would otherwise page its whole collection once per batch.
    """
    present: set[str] = set()
    for start in range(0, len(ids), IDS_PER_SEARCH):
        batch = ids[start : start + IDS_PER_SEARCH]
        found = set(walk(client, collections=[collection], ids=batch))
        if found - set(batch):
            raise RuntimeError(f"{collection}: an ids= search returned ids it was not asked for")
        present |= found
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
    would hold up the hourly register. ``Client.open`` resets the ``stac_io``'s timeout
    unless it is also given one (see ``stac_auth.open_resilient_client``), hence both.
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
    policy: Policy
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
        source = fetch_source_item(
            self.policy.source_stac_api, self.policy.source_collection, item_id
        )
        if is_old_generation(source):
            logger.warning(
                "   ⚠️  %s: old-generation source (raster:scale on SR_*), skipped", item_id
            )
            return "skipped_old_generation"
        properties = source.get("properties") or {}
        verdict = classify(properties, target, self.policy)
        if verdict != ("create" if target is None else "replace"):
            logger.warning("   ⚠️  %s: the source is now %s, skipped", item_id, verdict)
            return "skipped_source_changed"
        try:
            item = build_proxy_item(
                source, self.collection, self.raster_api_url, self.stac_api_url, expires=None
            )
            assert_no_s3_urls(item)
        except ValueError as exc:
            logger.warning("   ⚠️  %s: cannot be built (%s), skipped", item_id, exc)
            return "skipped_unbuildable"
        version = read_version(properties, self.policy.key)
        assert version is not None  # noqa: S101  # nosec B101 -- classify said create/replace
        return item, version

    def create(self, item_id: str) -> str:
        """POST one absent id, recorded in --created-ids as ``register_proxy`` records it."""
        built = self.build(item_id)
        if isinstance(built, str):
            return built
        item, version = built
        self.writes += 1  # before the POST: one that fails may still have landed
        if self.dry_run:
            logger.info("   would create %s (%s %s)", item_id, self.policy.key, version)
            return "would_create"
        assert self.created_ids is not None  # noqa: S101  # nosec B101 -- main requires it
        try:
            outcome = create_item(self.client, self.collection, item)
        except requests.RequestException as exc:
            # As in register_proxy.register_one: a 5xx or no answer may have committed. The id
            # was absent from the target at diff time, and the workflow holds the register's
            # semaphore, so no other run can have created it in between.
            if exc.response is None or exc.response.status_code >= 500:
                record_created(self.created_ids, self.collection, item.id, uncertain=True)
            raise
        if outcome == "created":
            record_created(self.created_ids, self.collection, item.id)
        return outcome

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
        url = source_item_url(self.stac_api_url, self.collection, item_id)  # same path shape
        got = session.get(url, timeout=WRITE_TIMEOUT_S, allow_redirects=False)
        if got.status_code == 404:
            logger.info("   ⏭️  %s: gone from the target (404), not replaced", item_id)
            return "gone"
        if got.status_code != 200:
            raise RuntimeError(f"refused: the target GET answered {got.status_code}")
        current_item = got.json()
        why = not_the_item(current_item, item_id, self.collection, self.policy.key)
        if why:
            raise RuntimeError(f"refused: {why}")
        current = read_version(current_item["properties"], self.policy.key)
        if classify(listed, current_item["properties"], self.policy) != "replace":
            logger.info("   ⏭️  %s: the target is already %s %s", item_id, self.policy.key, current)
            return "skipped_not_newer"
        built = self.build(item_id, current_item["properties"])
        if isinstance(built, str):
            return built
        item, version = built
        change = f"{self.policy.key} {current} -> {version}"
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
                headers={"Content-Type": "application/json"},
                timeout=WRITE_TIMEOUT_S,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            self.record_replace(item_id, phase="after_put", outcome="uncertain", error=str(exc))
            raise
        status = put.status_code
        # pgstac's update is a delete + create: a 404 means the item was deleted after the GET
        # (the clean), and nothing was written. The next run creates it.
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


def positive_int(raw: str) -> int:
    """argparse type for --max-writes: a whole number >= 1."""
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"must be a whole number (got {raw!r})") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1 (got {value})")
    return value


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--collection", required=True, help=f"One of {sorted(POLICIES)}")
    parser.add_argument("--stac-api-url", required=True, help="Target STAC API")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler base URL for links")
    parser.add_argument(
        "--max-writes",
        type=positive_int,
        required=True,  # a dry run too: without it, it would check nothing and look green
        metavar="N",
        help="POSTs + PUTs this run sends at most (a dry run: would send); the ids not "
        "reached are reported as remaining",
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
    return parser.parse_args(argv)


def log_summary(collection: str, n_source: int, counts: Counter[str]) -> None:
    """The run's one-line result: fixed keys, each counting source ids."""
    summary = " ".join(f"{key}={counts[key]}" for key in SUMMARY_KEYS)
    logger.info("Summary for %s: source=%d %s", collection, n_source, summary)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    policy = POLICIES.get(args.collection)
    if policy is None:
        logger.error(
            "Refusing --collection %r: only %s has a policy", args.collection, sorted(POLICIES)
        )
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
        source_client = stac_auth.open_resilient_client(policy.source_stac_api)
        target_client = stac_auth.open_resilient_client(args.stac_api_url)
        sources = walk(
            source_client,
            collections=[policy.source_collection],
            datetime=f"{policy.acquired_from.isoformat().replace('+00:00', 'Z')}/..",
            filter={"op": "<", "args": [{"property": "eo:cloud_cover"}, policy.max_cloud_cover]},
            filter_lang="cql2-json",
        )
        targets = walk(target_client, collections=[args.collection])
        verdicts = {i: classify(p, targets.get(i), policy) for i, p in sources.items()}
        to_create = sorted(i for i, v in verdicts.items() if v == "create")
        for item_id in present_in(target_client, policy.exclude_collection, to_create):
            verdicts[item_id] = "skipped_in_archive"
    except Exception as exc:  # noqa: BLE001 - any read failure stops the run before a write
        logger.error("Reading the catalogues failed, nothing written: %s", exc)
        return 1

    counts = Counter(v for v in verdicts.values() if v not in ("create", "replace"))
    logger.info(
        "Diff for %s (%s >= %s, acquired from %s): source=%d target=%d",
        args.collection,
        policy.key,
        policy.floor,
        policy.acquired_from.date(),
        len(sources),
        len(targets),
    )
    for verdict in sorted(set(verdicts.values())):
        ids = sorted(i for i, v in verdicts.items() if v == verdict)
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
        log_summary(args.collection, len(sources), counts)
        logger.error(
            "Refusing: none of the %d source item(s) since %s has a usable %s >= %s. A renamed "
            "key, a new stamp format or an empty source would otherwise pass for a converged run",
            len(sources),
            policy.acquired_from.date(),
            policy.key,
            policy.floor,
        )
        return 1

    if actions:
        run = Run(
            collection=args.collection,
            policy=policy,
            stac_api_url=args.stac_api_url,
            raster_api_url=args.raster_api_url,
            client=open_write_client(args.stac_api_url),
            dry_run=args.dry_run,
            created_ids=args.created_ids,
            replaced_ids=args.replaced_ids,
            run_id=args.run_id,
        )
    streak = visited = 0
    for kind, item_id in actions:
        # Every stop falls between ids, never inside one: a write and its line finish together.
        if run.writes == args.max_writes:
            break
        if streak == MAX_CONSECUTIVE_FAILURES:
            logger.error("Stopping after %d failures in a row", streak)
            break
        visited += 1
        try:
            if kind == "create":
                counts[run.create(item_id)] += 1
            else:
                counts[run.replace(item_id, sources[item_id])] += 1
            streak = 0
        except (CreatedIdsError, ReplacedIdsError) as exc:
            logger.error("Stopping: %s", exc)
            counts["failed"] += 1
            break
        except Exception as exc:  # noqa: BLE001 - one bad item must not hide the rest
            counts["failed"] += 1
            streak += 1
            logger.error("   ❌ %s %s: %s", kind, item_id, exc)
    counts["remaining"] = len(actions) - visited

    log_summary(args.collection, len(sources), counts)
    if counts["remaining"]:
        logger.info("remaining: %d id(s) not reached; re-run to continue", counts["remaining"])
    if counts["failed"] or counts["refused_target_unreadable"] or counts["skipped_unbuildable"]:
        logger.error(
            "Failed: %d write(s) failed or were refused, %d target item(s) have an unreadable "
            "%s, %d source item(s) cannot be built",
            counts["failed"],
            counts["refused_target_unreadable"],
            policy.key,
            counts["skipped_unbuildable"],
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
