#!/usr/bin/env python3
"""
Patch S1Tiling 1.4.0 for EODAG 4.0.0 compatibility.

EODAG 4.0.0 introduced several breaking changes for S1Tiling:
1. `productType` kwarg to dag.search() was renamed to `collection`.
   Having both causes cop_dataspace to fail silently → falls back to peps.
2. Product properties use STAC names (sat:orbit_state, platform, etc.)
   instead of legacy EODAG names (orbitDirection, platformSerialIdentifier, etc.)
3. cop_dataspace OData v4 rejects `polarizationChannels` and `sensorMode`.
4. cop_dataspace requires UPPERCASE orbit direction ("DESCENDING" not "descending").
5. `relativeOrbitNumber` search param silently returns 0 results on cop_dataspace.
6. KeycloakOIDCPasswordAuth always uses grant_type=password; Sentinel Hub service
   accounts (sh-*) require client_credentials grant instead.

This script patches:
- S1FileManager.py: fixes the search() call (issues 1, 3, 4, 5)
- s1/product.py: adds legacy→STAC property name fallback (issue 2)
- keycloak.py: adds client_credentials branch for sh-* service accounts (issue 6)

Usage (inside the Docker container):
    python3 /patch/s1tiling_eodag4_patch.py
"""

import pathlib
import re

S1T_PKG = pathlib.Path("/opt/S1TilingEnv/lib/python3.10/site-packages/s1tiling/libs")
KEYCLOAK_PKG = pathlib.Path(
    "/opt/S1TilingEnv/lib/python3.10/site-packages/eodag/plugins/authentication"
)


def patch_s1filemanager() -> None:
    """Fix search() call: add collection param, remove unsupported kwargs."""
    fpath = S1T_PKG / "S1FileManager.py"
    src = fpath.read_text()

    # Replace productType with collection (EODAG 4.0.0 rename).
    # Keeping productType alongside collection causes cop_dataspace to fail
    # silently, making EODAG fall back to peps.
    src = src.replace(
        "productType=product_type,",
        "collection=product_type,",
        1,  # only first occurrence
    )

    # Remove polarizationChannels (unsupported by cop_dataspace OData)
    src = re.sub(r"\n\s*# If we have eodag.*\n", "\n", src)
    src = re.sub(r"\n\s*polarizationChannels=dag_polarization_param,", "", src)

    # Remove sensorMode="IW" (unsupported by cop_dataspace OData)
    src = re.sub(r'\n\s*sensorMode="IW",', "", src)

    # Remove relativeOrbitNumber (not supported by cop_dataspace OData —
    # returns 0 results silently). S1Tiling has post-search filtering for
    # relative orbits when len(relative_orbit_list) > 1, and when list
    # has exactly 1 element, orbitNumber=None was passed anyway for most configs.
    src = re.sub(
        r"\n\s*relativeOrbitNumber=dag_orbit_list_param,.*",
        "",
        src,
    )

    # cop_dataspace OData requires UPPERCASE orbit direction values
    # ("DESCENDING" not "descending"). S1Tiling's k_dir_assoc produces lowercase.
    src = src.replace(
        "{ 'ASC': 'ascending', 'DES': 'descending' }",
        "{ 'ASC': 'ASCENDING', 'DES': 'DESCENDING' }",
    )

    fpath.write_text(src)
    print(f"  Patched {fpath.name}")


def patch_product_property() -> None:
    """Add STAC property name fallback to product_property()."""
    fpath = S1T_PKG / "s1" / "product.py"
    src = fpath.read_text()

    old = (
        "def product_property(prod: EOProduct, key: str, default=None):\n"
        '    """\n'
        "    Returns the required (EODAG) product property, "
        "or default in the property isn't found.\n"
        '    """\n'
        "    res = prod.properties.get(key, default)\n"
        "    return res"
    )

    new = (
        "def product_property(prod: EOProduct, key: str, default=None):\n"
        '    """\n'
        "    Returns the required (EODAG) product property, "
        "or default in the property isn't found.\n"
        "    EODAG 4.0.0 uses STAC property names; "
        "fall back to them for legacy keys.\n"
        '    """\n'
        "    _FALLBACK = {\n"
        '        "orbitDirection": "sat:orbit_state",\n'
        '        "platformSerialIdentifier": "platform",\n'
        '        "relativeOrbitNumber": "sat:relative_orbit",\n'
        '        "orbitNumber": "sat:absolute_orbit",\n'
        '        "polarizationChannels": "sar:polarizations",\n'
        '        "startTimeFromAscendingNode": "start_datetime",\n'
        '        "completionTimeFromAscendingNode": "end_datetime",\n'
        "    }\n"
        "    res = prod.properties.get(key, None)\n"
        "    if res is None and key in _FALLBACK:\n"
        "        res = prod.properties.get(_FALLBACK[key], default)\n"
        '        if key == "polarizationChannels" and isinstance(res, list):\n'
        '            res = "+".join(res)\n'
        "    return res if res is not None else default"
    )

    if old not in src:
        print(f"  WARNING: product_property() not found in {fpath.name}, skipping")
        return

    src = src.replace(old, new)
    fpath.write_text(src)
    print(f"  Patched {fpath.name}")


def patch_keycloak_auth() -> None:
    """Use client_credentials grant for Sentinel Hub (sh-*) service accounts.

    KeycloakOIDCPasswordAuth hardcodes grant_type=password, which fails for
    sh-* service accounts that require client_credentials. When the configured
    username starts with 'sh-', the patched _request_new_token sends
    client_credentials with the username as client_id and password as client_secret.
    Non-sh-* accounts keep the original password grant behaviour.
    """
    fpath = KEYCLOAK_PKG / "keycloak.py"
    src = fpath.read_text()

    old = (
        "    def _request_new_token(self) -> dict[str, Any]:\n"
        '        logger.debug("fetching new access token")\n'
        "        req_data = {\n"
        '            "client_id": self.config.client_id,\n'
        '            "client_secret": self.config.client_secret,\n'
        '            "grant_type": self.GRANT_TYPE,\n'
        "        }\n"
        "        credentials = {k: v for k, v in self.config.credentials.items()}\n"
        '        ssl_verify = getattr(self.config, "ssl_verify", True)\n'
        "        try:\n"
        "            response = self.session.post(\n"
        "                self.token_endpoint,\n"
        "                data=dict(req_data, **credentials),\n"
        "                headers=USER_AGENT,\n"
        "                timeout=HTTP_REQ_TIMEOUT,\n"
        "                verify=ssl_verify,\n"
        "            )\n"
        "            response.raise_for_status()\n"
        "        except requests.exceptions.Timeout as exc:\n"
        "            raise TimeOutError(exc, timeout=HTTP_REQ_TIMEOUT) from exc\n"
        "        except requests.RequestException as e:\n"
        "            return self._request_new_token_error(e)\n"
        "        return response.json()"
    )

    new = (
        "    def _request_new_token(self) -> dict[str, Any]:\n"
        '        logger.debug("fetching new access token")\n'
        "        credentials = {k: v for k, v in self.config.credentials.items()}\n"
        '        ssl_verify = getattr(self.config, "ssl_verify", True)\n'
        '        if credentials.get("username", "").startswith("sh-"):\n'
        "            # Sentinel Hub service account: requires client_credentials grant\n"
        "            req_data = {\n"
        '                "grant_type": "client_credentials",\n'
        '                "client_id": credentials["username"],\n'
        '                "client_secret": credentials["password"],\n'
        "            }\n"
        "        else:\n"
        "            req_data = dict(\n"
        "                {\n"
        '                    "client_id": self.config.client_id,\n'
        '                    "client_secret": self.config.client_secret,\n'
        '                    "grant_type": self.GRANT_TYPE,\n'
        "                },\n"
        "                **credentials,\n"
        "            )\n"
        "        try:\n"
        "            response = self.session.post(\n"
        "                self.token_endpoint,\n"
        "                data=req_data,\n"
        "                headers=USER_AGENT,\n"
        "                timeout=HTTP_REQ_TIMEOUT,\n"
        "                verify=ssl_verify,\n"
        "            )\n"
        "            response.raise_for_status()\n"
        "        except requests.exceptions.Timeout as exc:\n"
        "            raise TimeOutError(exc, timeout=HTTP_REQ_TIMEOUT) from exc\n"
        "        except requests.RequestException as e:\n"
        "            return self._request_new_token_error(e)\n"
        "        return response.json()"
    )

    if old not in src:
        print(f"  WARNING: _request_new_token() not found in {fpath.name}, skipping")
        return

    src = src.replace(old, new)
    fpath.write_text(src)
    print(f"  Patched {fpath.name}")


if __name__ == "__main__":
    print("Applying S1Tiling EODAG 4.0.0 compatibility patches...")
    patch_s1filemanager()
    patch_product_property()
    patch_keycloak_auth()
    print("Done.")
