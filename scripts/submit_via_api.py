#!/usr/bin/env python3
"""Submit workflow via Argo API with token authentication.

This ensures workflows are visible in the Argo UI by using service account token auth.

Architecture:
    This script → Argo API (with token) → Workflow (visible in UI)

The sensor does the same thing internally when triggered by AMQP messages,
which is why sensor-created workflows are visible in the UI.

Usage:
    # Direct API submission (for testing/manual runs)
    python scripts/submit_via_api.py \\
        --source-url "https://stac.core.eopf.eodc.eu/collections/sentinel-2-l2a/items/S2B_..." \\
        --item-id "S2B_test" \\
        --collection sentinel-2-l2a

    # Production: Use AMQP (sensor will create workflows via API automatically)
    python examples/submit.py --stac-url "..." --collection sentinel-2-l2a
"""

import json
import os
import sys
from pathlib import Path

import click
import requests  # type: ignore[import-untyped]

TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))


def load_token(token_path: Path) -> str:
    """Load bearer token from file."""
    if not token_path.exists():
        raise FileNotFoundError(f"Token file not found: {token_path}")
    return token_path.read_text().strip()


def submit_workflow(
    api_url: str,
    namespace: str,
    token: str,
    source_url: str,
    item_id: str,
    collection: str,
) -> dict[str, str]:  # Simplified return type
    """Submit workflow via Argo API.

    Args:
        api_url: Argo API base URL (http://localhost:2746)
        namespace: Kubernetes namespace (devseed)
        token: Bearer token for authentication
        source_url: Source STAC item URL
        item_id: Target item ID
        collection: Target collection ID

    Returns:
        API response with workflow metadata
    """
    workflow_spec = {
        "workflow": {
            "metadata": {
                "generateName": "geozarr-",
                "namespace": namespace,
            },
            "spec": {
                "workflowTemplateRef": {"name": "geozarr-pipeline"},
                "arguments": {
                    "parameters": [
                        {"name": "source_url", "value": source_url},
                        {"name": "item_id", "value": item_id},
                        {"name": "register_collection", "value": collection},
                    ]
                },
            },
        }
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    url = f"{api_url}/api/v1/workflows/{namespace}"

    resp = requests.post(url, json=workflow_spec, headers=headers, timeout=TIMEOUT)
    resp.raise_for_status()

    return resp.json()  # type: ignore[no-any-return]


@click.command()
@click.option(
    "--api-url",
    default="http://localhost:2746",
    envvar="ARGO_API_URL",
    help="Argo API base URL",
)
@click.option(
    "--namespace",
    default="devseed",
    envvar="ARGO_NAMESPACE",
    help="Kubernetes namespace",
)
@click.option(
    "--token-path",
    type=click.Path(exists=True, path_type=Path),
    default=".work/argo.token",
    help="Path to bearer token file",
)
@click.option(
    "--source-url",
    required=True,
    help="Source STAC item URL from EODC",
)
@click.option(
    "--item-id",
    required=True,
    help="Target item ID for registration",
)
@click.option(
    "--collection",
    default="sentinel-2-l2a",
    help="Target STAC collection",
)
def main(
    api_url: str,
    namespace: str,
    token_path: Path,
    source_url: str,
    item_id: str,
    collection: str,
) -> None:
    """Submit GeoZarr workflow via Argo API with token authentication."""
    try:
        token = load_token(token_path)
        click.echo(f"📝 Submitting workflow to {namespace}", err=True)

        result = submit_workflow(
            api_url=api_url,
            namespace=namespace,
            token=token,
            source_url=source_url,
            item_id=item_id,
            collection=collection,
        )

        workflow_name = "unknown"
        if isinstance(result, dict):
            metadata = result.get("metadata")
            if isinstance(metadata, dict):
                workflow_name = metadata.get("name", "unknown")
        click.echo(f"✅ Created workflow: {workflow_name}", err=True)
        click.echo(json.dumps(result, indent=2))

    except Exception as e:
        click.echo(f"❌ Failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
