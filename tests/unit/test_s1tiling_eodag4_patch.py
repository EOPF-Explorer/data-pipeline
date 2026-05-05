"""Unit tests for analysis/s1tiling_eodag4_patch.py — keycloak auth patch."""

import importlib.util
from pathlib import Path

PATCH_SCRIPT = Path(__file__).parent.parent.parent / "analysis" / "s1tiling_eodag4_patch.py"

# Exact _request_new_token source as shipped in the s1tiling:1.4.0-ubuntu-otb9.1.1 image.
_ORIGINAL_METHOD = """\
    def _request_new_token(self) -> dict[str, Any]:
        logger.debug("fetching new access token")
        req_data = {
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "grant_type": self.GRANT_TYPE,
        }
        credentials = {k: v for k, v in self.config.credentials.items()}
        ssl_verify = getattr(self.config, "ssl_verify", True)
        try:
            response = self.session.post(
                self.token_endpoint,
                data=dict(req_data, **credentials),
                headers=USER_AGENT,
                timeout=HTTP_REQ_TIMEOUT,
                verify=ssl_verify,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise TimeOutError(exc, timeout=HTTP_REQ_TIMEOUT) from exc
        except requests.RequestException as e:
            return self._request_new_token_error(e)
        return response.json()"""


def _load_patch_module():
    spec = importlib.util.spec_from_file_location("s1tiling_eodag4_patch", PATCH_SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_patch_keycloak_adds_client_credentials_branch(tmp_path):
    """patch_keycloak_auth must rewrite _request_new_token to use client_credentials for sh-* accounts."""
    keycloak_dir = tmp_path / "authentication"
    keycloak_dir.mkdir()
    keycloak_py = keycloak_dir / "keycloak.py"
    keycloak_py.write_text(_ORIGINAL_METHOD)

    mod = _load_patch_module()
    mod.KEYCLOAK_PKG = keycloak_dir
    mod.patch_keycloak_auth()

    patched = keycloak_py.read_text()
    assert "client_credentials" in patched, "client_credentials grant type not added"
    assert 'startswith("sh-")' in patched, "sh- detection not added"
    assert "data=dict(req_data, **credentials)" not in patched, "old merge form still present"


def test_patch_keycloak_skips_when_method_not_found(tmp_path, capsys):
    """patch_keycloak_auth must print WARNING and not crash when original pattern is absent."""
    keycloak_dir = tmp_path / "authentication"
    keycloak_dir.mkdir()
    keycloak_py = keycloak_dir / "keycloak.py"
    keycloak_py.write_text("# already patched or different version\n")

    mod = _load_patch_module()
    mod.KEYCLOAK_PKG = keycloak_dir
    mod.patch_keycloak_auth()  # must not raise

    out = capsys.readouterr().out
    assert "WARNING" in out


def test_patch_keycloak_preserves_password_branch(tmp_path):
    """Non sh-* accounts must still go through the password grant path."""
    keycloak_dir = tmp_path / "authentication"
    keycloak_dir.mkdir()
    keycloak_py = keycloak_dir / "keycloak.py"
    keycloak_py.write_text(_ORIGINAL_METHOD)

    mod = _load_patch_module()
    mod.KEYCLOAK_PKG = keycloak_dir
    mod.patch_keycloak_auth()

    patched = keycloak_py.read_text()
    assert "self.GRANT_TYPE" in patched, "password grant fallback not preserved"
    assert "**credentials" in patched, "credential spread for password grant not preserved"
