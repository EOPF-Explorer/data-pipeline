"""End-to-end auth-chain assertions (plan Task INT).

client (scripts/stac_auth) -> stac-auth-proxy (JWKS validation + method/path/scope
gating) -> stac-fastapi-pgstac -> pgstac, with a real Keycloak. Proves — before any
staging change — that a stac-writer client-credentials token is accepted for writes,
rejected when absent, and rejected when valid-but-unauthorized (no stac:write / wrong aud).

Requires Docker. The session fixture in conftest.py manages the stack.
"""

from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import httpx
import pytest
from conftest import DISCOVERY_URL, JWKS_URL, PROXY_URL, TOKEN_URL

scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import stac_auth  # noqa: E402

pytestmark = [pytest.mark.integration, pytest.mark.docker]

COLLECTION_ID = "int-auth-test"

COLLECTION = {
    "type": "Collection",
    "id": COLLECTION_ID,
    "stac_version": "1.0.0",
    "description": "integration auth test",
    "license": "proprietary",
    "extent": {
        "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    },
    "links": [],
}

ITEM = {
    "type": "Feature",
    "stac_version": "1.0.0",
    "id": "int-item",
    "collection": COLLECTION_ID,
    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
    "bbox": [0.0, 0.0, 0.0, 0.0],
    "properties": {"datetime": "2020-01-01T00:00:00Z"},
    "assets": {},
    "links": [],
}

STAC_WRITER = ("stac-writer", "stac-writer-secret")
NOT_A_WRITER = ("not-a-writer", "not-a-writer-secret")
NO_AUDIENCE = ("no-audience-writer", "no-audience-secret")


def _client_credentials_token(client_id: str, client_secret: str) -> str:
    resp = httpx.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _decode_segment(segment: str) -> dict:
    segment += "=" * (-len(segment) % 4)
    return json.loads(base64.urlsafe_b64decode(segment))


def _jwt_header(token: str) -> dict:
    return _decode_segment(token.split(".")[0])


def _jwt_claims(token: str) -> dict:
    return _decode_segment(token.split(".")[1])


@pytest.fixture
def writer_env(monkeypatch):
    """Configure the stac_auth helper as the stac-writer client, cache cleared."""
    monkeypatch.setenv("OIDC_TOKEN_URL", TOKEN_URL)
    monkeypatch.setenv("OIDC_CLIENT_ID", STAC_WRITER[0])
    monkeypatch.setenv("OIDC_CLIENT_SECRET", STAC_WRITER[1])
    stac_auth._cached_token = None
    stac_auth._cached_expiry = 0.0
    yield
    stac_auth._cached_token = None
    stac_auth._cached_expiry = 0.0


# 1 — Keycloak healthy & discovery OK -----------------------------------------


def test_keycloak_discovery_and_jwks():
    disco = httpx.get(DISCOVERY_URL, timeout=10).json()
    assert disco["token_endpoint"]
    assert disco["jwks_uri"]
    assert "client_credentials" in disco["grant_types_supported"]
    keys = httpx.get(JWKS_URL, timeout=10).json()["keys"]
    assert len(keys) >= 1


# 2 — Client config correct (T4 dry-run): claims + kid tie to a JWKS signing key


def test_stac_writer_token_claims():
    token = _client_credentials_token(*STAC_WRITER)
    claims = _jwt_claims(token)
    assert claims["azp"] == "stac-writer"
    aud = claims["aud"]
    assert "stac-api" in (aud if isinstance(aud, list) else [aud])
    assert "stac:write" in claims["scope"].split()
    assert "exp" in claims
    # Signature key provenance: the token's kid must be published in the realm JWKS.
    kid = _jwt_header(token)["kid"]
    jwks_kids = {k["kid"] for k in httpx.get(JWKS_URL, timeout=10).json()["keys"]}
    assert kid in jwks_kids


# 3 — Auth write OK (full prod path: stac_auth -> proxy -> stac) ---------------


def test_authenticated_write_succeeds(writer_env):
    client = stac_auth.open_client(PROXY_URL)
    session = client._stac_io.session  # carries the Bearer header

    # Reset to a clean collection so the item POST is a fresh 201.
    session.delete(f"{PROXY_URL}/collections/{COLLECTION_ID}", timeout=30)
    r_coll = session.post(f"{PROXY_URL}/collections", json=COLLECTION, timeout=30)
    assert r_coll.status_code in (200, 201), r_coll.text
    r_item = session.post(
        f"{PROXY_URL}/collections/{COLLECTION_ID}/items", json=ITEM, timeout=30
    )
    assert r_item.status_code in (200, 201), r_item.text

    r_get = httpx.get(f"{PROXY_URL}/collections/{COLLECTION_ID}/items/int-item", timeout=30)
    assert r_get.status_code == 200


# 4 — Unauthenticated write blocked -------------------------------------------


def test_unauthenticated_write_blocked():
    r = httpx.post(f"{PROXY_URL}/collections/{COLLECTION_ID}/items", json=ITEM, timeout=30)
    assert r.status_code in (401, 403)


# 5 — Public read (no token) --------------------------------------------------


def test_public_read():
    assert httpx.get(f"{PROXY_URL}/collections", timeout=30).status_code == 200
    assert httpx.get(f"{PROXY_URL}/search", timeout=30).status_code == 200


# 6 — Token flow via the helper (fetch + cache + expiry refetch) --------------


def test_helper_token_flow(writer_env):
    first = stac_auth.get_token()
    assert first
    assert stac_auth.get_token() == first  # served from cache
    stac_auth._cached_expiry = 0.0  # force expiry
    refetched = stac_auth.get_token()
    assert refetched  # refetch path works


# 7 — Authenticated != authorized (valid token, no stac:write) → 403 ----------


def test_authenticated_but_unauthorized_blocked():
    token = _client_credentials_token(*NOT_A_WRITER)
    r = httpx.post(
        f"{PROXY_URL}/collections/{COLLECTION_ID}/items",
        json=ITEM,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    assert r.status_code == 403, r.text


# 8 — Audience enforced (has stac:write, wrong/missing aud) → rejected ---------


def test_wrong_audience_rejected():
    token = _client_credentials_token(*NO_AUDIENCE)
    r = httpx.post(
        f"{PROXY_URL}/collections/{COLLECTION_ID}/items",
        json=ITEM,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    assert r.status_code in (401, 403), r.text
