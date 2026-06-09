"""Register one S1 GRD RTC Zarr store as a STAC item.

Builds a STAC item from the Zarr store metadata, augments it with
visualization links and alternate S3 assets, then upserts it to the
staging STAC API.

Exit codes:
    0 -- success
    1 -- failure (item build error or API error)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent))

from eopf_geozarr.stac.s1_rtc import build_s1_rtc_stac_item
from pystac_client import Client
from register_v1 import (
    add_alternate_s3_assets,
    add_store_link,
    add_thumbnail_asset,
    add_visualization_links,
    s3_to_https,
    upsert_item,
    warm_thumbnail_cache,
)
from run_ingest_register import check_env_consistency

log = logging.getLogger(__name__)


def register(
    store: str,
    collection: str,
    stac_api_url: str,
    raster_api_url: str,
    s3_endpoint: str,
) -> int:
    """Build and register one S1 RTC STAC item.

    Returns exit code: 0 = success, 1 = failure.
    """
    # Fail fast on a per-env bucket/collection mismatch (the 32TLR footgun): the standalone
    # register path takes a hand-typed --store + --collection, so it needs the same guard as
    # run_ingest_register. Only s3:// stores carry an identifiable bucket in the netloc.
    parsed = urlparse(store)
    if parsed.scheme == "s3":
        check_env_consistency(collection, parsed.netloc)

    try:
        item = build_s1_rtc_stac_item(store, collection)
    except Exception:
        log.exception("Failed to build STAC item from %s", store)
        return 1

    # build_s1_rtc_stac_item returns s3:// hrefs; TiTiler needs https:// via the gateway
    for asset in item.assets.values():
        if asset.href and asset.href.startswith("s3://"):
            asset.href = s3_to_https(asset.href)

    add_store_link(item, store)
    add_alternate_s3_assets(item, s3_endpoint)
    add_visualization_links(item, raster_api_url, collection)
    add_thumbnail_asset(item, raster_api_url, collection)
    warm_thumbnail_cache(item)

    try:
        client = Client.open(stac_api_url)
        upsert_item(client, collection, item)
    except Exception:
        log.exception("Failed to upsert item %s to %s", item.id, stac_api_url)
        return 1

    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True, help="S3 URI of the GeoZarr V3 store")
    parser.add_argument("--collection", required=True, help="Target STAC collection ID")
    parser.add_argument("--stac-api-url", required=True, help="STAC API base URL")
    parser.add_argument("--raster-api-url", required=True, help="TiTiler raster API base URL")
    parser.add_argument("--s3-endpoint", required=True, help="S3 endpoint URL")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = _build_parser().parse_args()
    sys.exit(
        register(
            store=args.store,
            collection=args.collection,
            stac_api_url=args.stac_api_url,
            raster_api_url=args.raster_api_url,
            s3_endpoint=args.s3_endpoint,
        )
    )


if __name__ == "__main__":
    main()
