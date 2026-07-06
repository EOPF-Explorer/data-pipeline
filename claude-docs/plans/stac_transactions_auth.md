# Plan: Authenticate the STAC Transactions API (GHSA-9vrc-w855-8hq3 / coordination#265)

> Source of record: `claude-docs/specs/stac_transactions_auth.md`. Seeded from the
> vault plan (2026-06-19, EOEPCA-updated 2026-07-03). This repo covers **T1–T3 + Task
> INT**; T4–T9 land in `platform-deploy` (sibling `../platform-deploy`, separate PRs).

**Goal**: Make `POST/PUT/DELETE` on the production STAC API require a valid OIDC token,
while keeping `GET`/`/search` public — without breaking any in-cluster
ingestion/registration.
**Constraint**: Read path unchanged; token-first / enforce-second rollout; surgical
edits at the 7 write sites; helper is a no-op when OIDC env is absent.

## Architecture / boundaries

- **Application boundary** (`data-pipeline`, this repo): one shared auth helper +
  token injection at the 7 STAC-write sites + tests + local integration harness.
- **Edge / infra boundary** (`platform-deploy`): proxy deployment, `/stac`-path ingress
  repoint, backend NetworkPolicy, OIDC client Secret, OIDC env wired into Argo steps.
- IdP is the **in-cluster Keycloak** `keycloak.core.svc` (realm `eoxhub`, we administer
  it via `core/keycloak` admin secret → no external EOX dependency).

## The 7 write sites (`data-pipeline`)

| File | Write call | Injection point |
|---|---|---|
| `scripts/register_v1.py` (L869) | DELETE+POST items | `Client.open` → `open_client` |
| `scripts/register_v1_s1_rtc.py` (L127) | DELETE+POST items | `Client.open` → `open_client` |
| `scripts/register_per_acquisition.py` (L146/L155) | DELETE+POST items | `Client.open` → `open_client` |
| `scripts/register_v0.py` (L352) | DELETE+POST items | `Client.open` → `open_client` |
| `scripts/update_stac_storage_tier.py` (L387) | DELETE+POST items | `Client.open` → `open_client` |
| `scripts/aggregate_items.py` (httpx PUT L190) | PUT collections | attach `auth_headers()` |
| `operator-tools/manage_collections.py` (session L56) | POST/DELETE items+collections | `session.headers.update(auth_headers())` |
| `operator-tools/manage_item.py` (session L112) † | POST/DELETE items | `session.headers.update(auth_headers())` |

† **Discovered during T2 (not in the original 7):** the operator tool delegates item
writes to `manage_item.STACItemManager`, which owns a separate `requests.Session`. Left
unauthenticated it would break operator item edits under enforcement (T7), contradicting
the Done-definition's "all operator writes succeed token-authenticated". Same one-line
pattern; authenticated alongside `manage_collections.py`.

## Dependency graph

```
T1 stac_auth.py helper ──► T2 wire into 7 write sites ──► T3 unit tests
                                                              │
                                          Task INT: docker-compose harness ──► GATE
                                                              │  (before any platform-deploy change)
                                                              ▼
        ── platform-deploy (separate PRs) ── T4 stac-writer + SealedSecret ──► T5 OIDC env into Argo
        ──► T6 deploy stac-auth-proxy ──► T7 repoint /stac ──► T8 NetworkPolicy ──► T9 e2e verify
```

Rollout invariant: **token-first, enforce-second.**

## Tasks (this repo)

### Task 1 — Shared OIDC auth helper  <status: NEXT>
**What**: New `scripts/stac_auth.py`:
- `get_token()`: client-credentials grant against `OIDC_TOKEN_URL` with
  `OIDC_CLIENT_ID`/`OIDC_CLIENT_SECRET` (env); cache until ~expiry. Returns `None` if env
  unset (no-op for local/dev).
- `auth_headers() -> dict`: `{"Authorization": f"Bearer {token}"}` or `{}`.
- `open_client(url) -> Client`: `Client.open(url, headers=auth_headers() or None)`.
  Verified (pystac-client 0.9.0): headers passed to `Client.open` land on
  `_stac_io.session.headers`, so raw `session.post/delete` inherit `Authorization`.
**Verify**: `uv run pytest tests/unit/test_stac_auth.py`
**Acceptance criteria**:
- [ ] No env set → `get_token()` returns `None`, `auth_headers()` is `{}`,
  `open_client` opens unauthenticated (`headers=None`) — back-compat
- [ ] Env set → token fetched once, cached; header injected; a real
  `StacApiIO(headers=...)` carries `Authorization` on its session
- [ ] Token endpoint failure surfaces a clear error (no silent unauthenticated write)

### Task 2 — Inject the helper at all 7 write sites  <status: blocked by T1>
**What**: Replace `Client.open(...)` with `stac_auth.open_client(...)` at the 5 pystac
sites; attach `auth_headers()` to the httpx PUT (`aggregate_items.py` L190) and the
`requests` session (`manage_collections.py` L56). Surgical — only client/session
construction lines change. `manage_collections.py`'s read-only `Client.open` calls stay
as-is.
**Verify**: `uv run pytest tests/unit/ -k "register or storage_tier or aggregate or manage_collections"`
**Acceptance criteria**:
- [ ] Each of the 7 sites attaches the Bearer header when OIDC env is set (mock-asserted)
- [ ] Existing register/storage-tier/aggregate tests still pass unchanged
- [ ] No behavior change when env is unset

### Task 3 — Tests for auth helper + injection  <status: blocked by T1,T2>
**What**: `tests/unit/test_stac_auth.py` (fetch/cache/no-op/error/expiry) + extend the
7 sites' tests to assert header presence with a mocked token endpoint. Adversarial:
token-endpoint 401, missing secret, expired-token refetch.
**Verify**: `uv run pytest`
**Acceptance criteria**:
- [ ] Helper covered incl. error/expiry paths
- [ ] Each write site has a test proving it authenticates
- [ ] `uv run pytest` green; build+push image (record sha)

### Task INT — Local integration harness (pre-staging gate)  <status: blocked by T3>
**What**: `tests/integration/stac_auth/` — docker-compose (pgstac +
stac-fastapi-pgstac + real stac-auth-proxy + real Keycloak with `--import-realm`
`realm-eoxhub.json` defining `stac-writer` and a `not-a-writer` client) + a pytest
module asserting the 8 checks (Keycloak health/discovery, token claims, auth write OK,
unauth write blocked, public read, token flow, authenticated≠authorized, audience
enforced). See the source plan for the full 8-assertion list.
**Verify**: `docker compose -f tests/integration/stac_auth/docker-compose.yaml up -d && uv run pytest tests/integration/stac_auth -v`
**Acceptance criteria**:
- [ ] Harness reproduces the prod request path with the prod proxy config keys
- [ ] All 8 assertions pass locally; same suite green in CI
- [ ] **Gate:** no staging change (platform-deploy T5+) until this is green

## Tasks (platform-deploy — separate PRs, `../platform-deploy`)

T4 `stac-writer` client + SealedSecret · T5 OIDC env into Argo write steps · T6 deploy
`stac-auth-proxy` HelmRelease · T7 repoint only `/stac` at the proxy · T8 NetworkPolicy
(CNI=Canal, enforced) · T9 end-to-end verification (advisory PoC fails; jobs pass; reads
public). Full detail in the source vault plan / spec.

## Open questions

**None open.** All resolved by live-cluster investigation 2026-06-19 (OIDC client, CNI,
STAC service, proxy config) — see spec.

## Done definition

Task INT green before any staging change; `/stac` served only via stac-auth-proxy (other
paths untouched); unauthenticated `POST/PUT/DELETE` on `/stac` → `401/403` from the
public internet; all converts/crons/operator writes succeed token-authenticated;
`GET`/`/search` unchanged; advisory GHSA-9vrc-w855-8hq3 updated/closed.
