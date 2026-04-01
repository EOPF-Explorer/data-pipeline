# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest release | Yes |
| Older releases | No |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report vulnerabilities via [GitHub private security advisories](https://github.com/EOPF-Explorer/data-pipeline/security/advisories/new).

We aim to acknowledge reports within **5 business days** and provide a resolution timeline within **15 business days**, depending on severity and complexity.

## Scope

In scope:
- Python scripts in `scripts/`
- Docker image built from `docker/Dockerfile`
- GitHub Actions workflows in `.github/workflows/`

Out of scope:
- Argo Workflows cluster infrastructure
- OVH Cloud S3 / Harbor registry configuration
- Network policies and RBAC (managed at infrastructure level)

## Preferred Languages

English.
