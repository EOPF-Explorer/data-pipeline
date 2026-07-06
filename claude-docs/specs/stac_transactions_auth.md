# Spec: Authenticate the STAC Transactions API (GHSA-9vrc-w855-8hq3 / coordination#265)

## Problem

The production STAC Transactions extension is enabled and reachable from the public
internet with **no authentication** (advisory GHSA-9vrc-w855-8hq3; live PoC in
`claude-docs/security/stac_transactions_unauthenticated_writes.md`). Any anonymous
client can create/overwrite/delete catalog items in any collection via
`POST/PUT/DELETE` on `https://api.explorer.eopf.copernicus.eu/stac`.

Our own pipeline depends on those same endpoints and currently sends no credentials —
so our jobs and an attacker share the same open path.

## Objective

Make `POST/PUT/DELETE` on the production STAC API require a valid OIDC token, while
keeping `GET`/`/search` public — **without breaking any in-cluster ingestion or
registration**.

## Scope

- **In scope (`data-pipeline`, application boundary)**: one shared OIDC auth helper +
  token injection at the 7 STAC-write sites + unit tests + a local integration harness
  (proxy + stac-fastapi-pgstac + Keycloak) that exercises the full auth chain before any
  cluster change.
- **In scope (`platform-deploy`, edge/infra boundary — separate PRs)**: a `stac-writer`
  Keycloak client + SealedSecret, OIDC env wired into Argo write steps,
  `stac-auth-proxy` HelmRelease, `/stac`-path ingress repoint, NetworkPolicy, end-to-end
  verification.
- **Out of scope**: read path behavior, interim mitigations (go straight to the proxy),
  hand-rolled API keys, any change to the render/tiler paths.

## Approach

Deploy `developmentseed/stac-auth-proxy` in front of eoapi (public reads, OIDC-gated
writes), repoint only the `/stac` path of the shared public host at the proxy, make the
backend unreachable except via the proxy, and token-authenticate every write job with a
dedicated `stac-writer` M2M client (client-credentials) in our in-cluster Keycloak
(realm `eoxhub`).

**Write authorization is claims-based and mandatory** (EOEPCA feedback, felix/alukach,
2026-07-03): a write requires `aud=stac-api` (`ALLOWED_JWT_AUDIENCES`) **and** a
`stac:write` scope — a merely-valid realm JWT must not authorize writes.

## Rollout invariant

**Token-first, enforce-second.** Jobs start sending a Bearer header while the backend is
still open (header ignored, nothing breaks); enforcement flips on only after every job is
already authenticating.

## Success criteria

- Unauthenticated public `POST/PUT/DELETE` on `…/items` → `401/403` (advisory PoC fails).
- In-cluster register jobs and the operator tool still succeed (token-authenticated).
- `GET`/`/search` remain public and unchanged.
- A valid realm token lacking `stac:write` (or the `stac-api` audience) → `403`.
- The local integration harness passes before any staging change.

## Back-compat constraint

The `data-pipeline` auth helper is a **no-op when OIDC env is absent** — local/dev and
any unconfigured environment keep working unauthenticated.

## References

- Full plan: `claude-docs/plans/stac_transactions_auth.md`
- Advisory PoC: `claude-docs/security/stac_transactions_unauthenticated_writes.md`
- Upstream: `developmentseed/stac-auth-proxy`
