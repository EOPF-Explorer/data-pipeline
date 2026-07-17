# STAC Transactions auth — local integration harness (plan Task INT)

Reproduces the **production auth chain** on localhost with throwaway containers, so the
whole flow is validated *before* any staging/cluster change:

```
scripts/stac_auth (client-credentials)  →  stac-auth-proxy  →  stac-fastapi-pgstac  →  pgstac
                    ▲                     (JWKS validation +
                    │                      method/path/scope gating)
             real Keycloak (realm eoxhub, --import-realm)
```

This is the **pre-staging gate**: platform-deploy T5+ (wiring OIDC env into Argo,
deploying the proxy, repointing `/stac`, NetworkPolicy) must not proceed until this is
green. `realm-eoxhub.json` is the reviewed reference config that platform-deploy **T4**
replicates by hand in the real Keycloak.

## Run

```bash
uv run pytest tests/integration/stac_auth -m docker
```

The session fixture (`conftest.py`) runs `docker compose up -d`, waits for the chain to
answer, runs the 8 assertions, and tears the stack down. Requires a running Docker
daemon. Set `KEEP_STACK=1` to leave the containers up for debugging.

These tests carry the `docker` marker and are **excluded from the default suite**
(`addopts = -m "not docker"`), so `uv run pytest` never spins containers.

## What it asserts

1. **Keycloak healthy & discovery OK** — realm discovery exposes `token_endpoint`,
   `jwks_uri`, `client_credentials` grant; JWKS has ≥1 signing key.
2. **Client config correct (T4 dry-run)** — a `stac-writer` client-credentials token
   carries `azp=stac-writer`, `aud` containing `stac-api`, `scope` containing
   `stac:write`, a short `exp`; its `kid` is published in the realm JWKS.
3. **Auth write OK** — `stac_auth.open_client(proxy)` → create collection + item → `201`;
   `GET` item → `200`.
4. **Unauth write blocked** — no token → `POST …/items` → `401/403`.
5. **Public read** — `GET /collections`, `GET /search` without a token → `200`.
6. **Token flow** — `stac_auth.get_token()` fetches, caches, and refetches after expiry.
7. **Authenticated ≠ authorized** — a valid `not-a-writer` token (no `stac:write`) →
   write → `403`.
8. **Audience enforced** — a `no-audience-writer` token (has `stac:write`, no `stac-api`
   audience) → write → rejected.

## Ports / issuer trick

- Keycloak: host **8066** → container 8080. Proxy: host **8067** → container 8000.
- Keycloak's frontend/issuer is pinned to `http://localhost:8066/auth` (reachable from
  the host-run pytest), while `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true` makes the JWKS/token
  backchannel follow the request host — so the in-network proxy fetches JWKS at
  `http://keycloak:8080/auth` yet the token `iss` still matches the discovery `issuer`.

## Mapping to prod config (platform-deploy T4/T6)

| Harness | Prod equivalent |
|---|---|
| `realm-eoxhub.json` clients/scope/audience mapper | T4: `stac-writer` client + `stac:write` scope + `aud=stac-api` mapper in realm `eoxhub` |
| proxy `ALLOWED_JWT_AUDIENCES=stac-api` | T6 proxy HelmRelease |
| proxy `PRIVATE_ENDPOINTS` scope tuples | T6 proxy HelmRelease |
| proxy `DEFAULT_PUBLIC=true` | T6 proxy HelmRelease |

The `not-a-writer` and `no-audience-writer` clients exist only to prove the negative
cases (7, 8) — they are not part of prod.
