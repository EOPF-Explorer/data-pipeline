"""Unit tests for the demo-denylist guard in the operator delete paths.

The demo denylist (``resolve_exclude_ids``: ``$EXPIRES_EXCLUDE_FILE``, else the baked
``scripts/demo_exclude_ids.txt``) already protects these scenes from the expiry
cleanup. The operator CLIs honour it too:

- ``manage_item.py delete`` refuses a listed id before any STAC or S3 call;
- ``manage_collections.py clean`` skips listed ids, reports them on their own line,
  and asks its prompt after that;
- ``manage_collections.py delete`` refuses while the collection still holds a listed
  id, checked before its prompt and ``--clean-first``, and again before the DELETE;
- ``--allow-demo`` lifts the refusal for listed ids only, printing every one it deletes;
- a list that cannot be used, or a lookup that fails or answers malformed, refuses
  with or without ``--allow-demo`` (fails closed);
- the ids given to the three commands must be plain STAC ids, ids are percent-encoded
  in DELETE paths, and ``--api-url`` must not contain ``?`` or ``#``.

No network: HTTP, S3 and the STAC search are mocked.
"""

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
import requests
from click.testing import CliRunner, Result

OPERATOR_TOOLS = Path(__file__).parent.parent.parent / "operator-tools"
if str(OPERATOR_TOOLS) not in sys.path:
    sys.path.insert(0, str(OPERATOR_TOOLS))

import manage_collections  # noqa: E402
import manage_item  # noqa: E402

API = "https://stac.example.com"
COLLECTION = "sentinel-2-l2a-staging"
CONFINE = "s3://bucket/tests-output/sentinel-2-l2a-staging/"
DEMO_A = "S2A_MSIL2A_DEMO_A"
DEMO_B = "S2B_MSIL2A_DEMO_B"


@pytest.fixture(autouse=True)
def demo_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A two-id denylist via $EXPIRES_EXCLUDE_FILE, so no test reads the baked file."""
    path = tmp_path / "demo_exclude_ids.txt"
    path.write_text(f"# demo scenes\n{DEMO_A}\n\n{DEMO_B}\n")
    monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(path))
    return path


@pytest.fixture(params=["missing", "empty", "not-utf8", "not-plain"])
def unusable_list(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A denylist that cannot be used: missing, with no ids, not UTF-8, or with an entry
    that no plain id can equal."""
    path = tmp_path / f"{request.param}.txt"
    if request.param == "empty":
        path.write_text("# nothing here\n")
    elif request.param == "not-utf8":
        path.write_bytes(b"S2A_MSIL2A_\xe9\n")
    elif request.param == "not-plain":
        path.write_text(f"{DEMO_A}  # hero scene\n")
    monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(path))


def _item(item_id: str) -> dict:
    href = f"{CONFINE}{item_id}/data.zarr/"
    return {"id": item_id, "assets": {"data": {"alternate": {"s3": {"href": href}}}}}


def _response(body: object, status: int = 200) -> requests.Response:
    """A real Response, so raise_for_status() and json() fail the way they do live."""
    resp = requests.Response()
    resp.status_code = status
    resp._content = body if isinstance(body, bytes) else json.dumps(body).encode()
    return resp


def _found(*ids: str) -> requests.Response:
    return _response({"features": [{"id": i} for i in ids]})


# === the list ===


@pytest.mark.parametrize(
    "entry",
    [f"﻿{DEMO_A}", f"{DEMO_A} # hero", f"{DEMO_A}​"],
    ids=["bom", "inline-comment", "zero-width"],
)
def test_the_list_refuses_naming_an_entry_that_is_not_plain(demo_list: Path, entry: str) -> None:
    """Such an entry never equals an id, so the scene it means would go unprotected."""
    demo_list.write_text(f"{entry}\n{DEMO_B}\n", encoding="utf-8")

    with pytest.raises(click.ClickException) as exc:
        manage_item.load_demo_ids()
    assert f"not plain STAC ids: {entry!r})" in exc.value.message


def test_a_missing_baked_list_is_called_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baked = tmp_path / "baked" / "demo_exclude_ids.txt"
    monkeypatch.delenv("EXPIRES_EXCLUDE_FILE")
    monkeypatch.setattr(manage_item.s3_item_cleanup, "BAKED_EXCLUDE_FILE", baked)

    with pytest.raises(click.ClickException) as exc:
        manage_item.load_demo_ids()
    assert f"({baked} is missing)" in exc.value.message


# === manage_item.py delete ===


@pytest.fixture
def item_calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, MagicMock]:
    """Every STAC (HTTP) and S3 entry point the item delete can reach."""
    calls = {
        "http": MagicMock(side_effect=AssertionError("unexpected HTTP call")),
        "s3": MagicMock(),
        "get_item": MagicMock(side_effect=lambda collection_id, item_id: _item(item_id)),
        "delete_item": MagicMock(return_value=(True, 3, 0)),
    }
    monkeypatch.setattr(requests.Session, "request", calls["http"])
    monkeypatch.setattr(manage_item.boto3, "client", calls["s3"])
    monkeypatch.setattr(manage_item.STACItemManager, "get_item", calls["get_item"])
    monkeypatch.setattr(manage_item.STACItemManager, "delete_item", calls["delete_item"])
    return calls


def _delete_item(item_id: str, *extra: str, collection: str = COLLECTION) -> Result:
    args = ["--api-url", API, "delete", collection, item_id, "-y", "--clean-s3"]
    return CliRunner().invoke(manage_item.cli, [*args, "--confine-to", CONFINE, *extra])


@pytest.mark.parametrize("extra", [[], ["--dry-run"]], ids=["run", "dry-run"])
def test_item_delete_refuses_a_listed_id_before_any_stac_or_s3_call(
    item_calls: dict[str, MagicMock], extra: list[str]
) -> None:
    result = _delete_item(DEMO_A, *extra)

    assert result.exit_code != 0
    assert f"Refusing to delete {DEMO_A}: it is on the demo denylist" in result.output
    assert [name for name, call in item_calls.items() if call.called] == []


@pytest.mark.parametrize(
    ("extra", "says", "deleted"),
    [([], "deleting", [DEMO_A]), (["--dry-run"], "would delete", [])],
    ids=["run", "dry-run"],
)
def test_item_delete_allow_demo_deletes_a_listed_id_and_prints_it(
    item_calls: dict[str, MagicMock], extra: list[str], says: str, deleted: list[str]
) -> None:
    result = _delete_item(DEMO_A, "--allow-demo", *extra)

    assert result.exit_code == 0, result.output
    assert f"--allow-demo: {says} {DEMO_A}, which is on the demo denylist" in result.output
    assert [c.kwargs["item_id"] for c in item_calls["delete_item"].call_args_list] == deleted


@pytest.mark.parametrize(
    ("collection", "item_id", "extra"),
    [
        (COLLECTION, "S2A%5FMSIL2A_DEMO_A", []),
        (COLLECTION, f"./{DEMO_A}", []),
        (f"{COLLECTION}/items/{DEMO_A}#", "S2A_MSIL2A_NOT_A_DEMO", []),
        (COLLECTION, "..", []),
        (COLLECTION, "S2A%5FMSIL2A_DEMO_A", ["--allow-demo"]),
    ],
    ids=["percent-escape", "dot-segment", "fragment-in-collection", "dot-dot", "allow-demo"],
)
def test_item_delete_refuses_an_id_that_is_not_plain(
    item_calls: dict[str, MagicMock], collection: str, item_id: str, extra: list[str]
) -> None:
    """The URL is normalised on its way to the API: these name DEMO_A, or the collection."""
    result = _delete_item(item_id, *extra, collection=collection)

    assert result.exit_code != 0
    assert "not a plain STAC id" in result.output
    assert [name for name, call in item_calls.items() if call.called] == []


def test_item_delete_of_an_unlisted_id_is_unchanged(item_calls: dict[str, MagicMock]) -> None:
    result = _delete_item("S2A_MSIL2A_NOT_A_DEMO")

    assert result.exit_code == 0, result.output
    assert "demo denylist" not in result.output
    assert item_calls["delete_item"].call_args.kwargs["item_id"] == "S2A_MSIL2A_NOT_A_DEMO"


@pytest.mark.usefixtures("unusable_list")
@pytest.mark.parametrize("flag", [[], ["--allow-demo"]], ids=["default", "allow-demo"])
def test_item_delete_refuses_when_the_list_cannot_be_used(
    item_calls: dict[str, MagicMock], flag: list[str]
) -> None:
    result = _delete_item("S2A_MSIL2A_NOT_A_DEMO", *flag)

    assert result.exit_code != 0
    assert "cannot use the demo denylist" in result.output
    assert [name for name, call in item_calls.items() if call.called] == []


@pytest.mark.parametrize("end", ["?", "#"])
@pytest.mark.parametrize(
    ("cli", "args"),
    [
        (manage_item.cli, ["delete", COLLECTION, "S2A_MSIL2A_NOT_A_DEMO", "-y"]),
        (manage_collections.cli, ["clean", COLLECTION, "-y"]),
        (manage_collections.cli, ["delete", COLLECTION]),
    ],
    ids=["item-delete", "clean", "collection-delete"],
)
def test_an_api_url_with_a_query_or_fragment_is_refused(
    monkeypatch: pytest.MonkeyPatch, cli: click.Group, args: list[str], end: str
) -> None:
    """Request paths are appended to it: after a '?' or '#', the DELETE names DEMO_A."""
    http = MagicMock(side_effect=AssertionError("unexpected HTTP call"))
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", http)

    api_url = f"{API}/collections/{COLLECTION}/items/{DEMO_A}{end}"
    result = CliRunner().invoke(cli, ["--api-url", api_url, *args], input="y\n")

    assert result.exit_code == 2, result.output
    assert "--api-url must not contain" in result.output
    http.assert_not_called()


# === manage_collections.py clean ===


def _manager(items: list[dict]) -> manage_collections.STACCollectionManager:
    mgr = manage_collections.STACCollectionManager(API)
    mgr.get_collection_items = MagicMock(return_value=items)  # type: ignore[method-assign]
    mgr.item_manager = MagicMock()
    mgr.item_manager.delete_item.return_value = (True, 1, 0)
    return mgr


def _deleted_ids(mgr: manage_collections.STACCollectionManager) -> list[str]:
    return [c.kwargs["item_id"] for c in mgr.item_manager.delete_item.call_args_list]


def _clean(mgr: manage_collections.STACCollectionManager, **kwargs: object) -> int:
    deleted, _, _ = mgr.clean_collection(
        COLLECTION,
        clean_s3=True,
        s3_client=MagicMock(),
        confinement=[("bucket", "tests-output/sentinel-2-l2a-staging/")],
        **kwargs,  # type: ignore[arg-type]
    )
    return deleted


def test_clean_skips_listed_ids_reports_them_apart_and_deletes_the_rest(
    capsys: pytest.CaptureFixture[str],
) -> None:
    mgr = _manager([_item("ITEM_0"), _item(DEMO_A), _item("ITEM_1"), _item(DEMO_B)])

    assert _clean(mgr) == 2

    assert _deleted_ids(mgr) == ["ITEM_0", "ITEM_1"]
    out = capsys.readouterr().out
    assert f"Protected demo scenes (demo denylist): 2\n    - {DEMO_A}\n    - {DEMO_B}" in out
    assert "Demo scenes protected: 2 (demo denylist)" in out
    assert "S3 failures" not in out


def test_clean_skips_demos_before_the_bound() -> None:
    """Demos at the head of the list must not use up --max-items."""
    mgr = _manager([_item(DEMO_A), _item(DEMO_B)] + [_item(f"ITEM_{i}") for i in range(5)])

    assert _clean(mgr, max_items=2) == 2
    assert _deleted_ids(mgr) == ["ITEM_0", "ITEM_1"]


def test_clean_with_only_demos_left_deletes_nothing() -> None:
    mgr = _manager([_item(DEMO_A), _item(DEMO_B)])

    assert _clean(mgr) == 0
    mgr.item_manager.delete_item.assert_not_called()


def test_clean_allow_demo_deletes_listed_ids_and_prints_each(
    capsys: pytest.CaptureFixture[str],
) -> None:
    mgr = _manager([_item("ITEM_0"), _item(DEMO_A), _item(DEMO_B)])

    assert _clean(mgr, allow_demo=True) == 3

    assert _deleted_ids(mgr) == ["ITEM_0", DEMO_A, DEMO_B]
    out = capsys.readouterr().out
    assert f"are NOT protected in this run:\n    - {DEMO_A}\n    - {DEMO_B}" in out


def test_clean_refuses_an_id_that_is_not_plain() -> None:
    """Refused before the listing, so `delete --clean-first` refuses before the clean."""
    mgr = _manager([_item("ITEM_0")])

    with pytest.raises(click.ClickException, match="not a plain STAC id"):
        mgr.clean_collection(f"./{COLLECTION}")
    mgr.get_collection_items.assert_not_called()  # type: ignore[attr-defined]
    mgr.item_manager.delete_item.assert_not_called()


@pytest.mark.usefixtures("unusable_list")
@pytest.mark.parametrize("allow_demo", [False, True], ids=["default", "allow-demo"])
def test_clean_refuses_when_the_list_cannot_be_used(allow_demo: bool) -> None:
    mgr = _manager([_item("ITEM_0")])

    with pytest.raises(click.ClickException, match="cannot use the demo denylist"):
        _clean(mgr, allow_demo=allow_demo)
    mgr.get_collection_items.assert_not_called()  # type: ignore[attr-defined]
    mgr.item_manager.delete_item.assert_not_called()


def _transport(monkeypatch: pytest.MonkeyPatch) -> list[requests.PreparedRequest]:
    """Record every request where it leaves for the network; each is answered 204."""
    sent: list[requests.PreparedRequest] = []

    def send(
        adapter: object, request: requests.PreparedRequest, **kwargs: object
    ) -> requests.Response:
        sent.append(request)
        return _response(b"", status=204)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    return sent


@pytest.mark.parametrize(
    ("alias", "sent_as"),
    [
        (f"./{DEMO_A}", f".%2F{DEMO_A}"),
        (f"{DEMO_A}#copy", f"{DEMO_A}%23copy"),
        ("S2A%5FMSIL2A_DEMO_A", "S2A%255FMSIL2A_DEMO_A"),
    ],
    ids=["dot-segment", "fragment", "percent-escape"],
)
def test_clean_deletes_an_item_listed_under_an_alias_by_its_own_id(
    monkeypatch: pytest.MonkeyPatch, alias: str, sent_as: str
) -> None:
    """Ids the server lists are not checked like CLI ids. Put in the URL raw, each of
    these aliases is normalised onto DEMO_A, the item this clean just kept."""
    sent = _transport(monkeypatch)
    mgr = manage_collections.STACCollectionManager(API)  # with its real STACItemManager
    mgr.get_collection_items = MagicMock(return_value=[_item(DEMO_A), _item(alias)])  # type: ignore[method-assign]

    assert mgr.clean_collection(COLLECTION) == (1, 0, 0)
    assert [(r.method, r.url) for r in sent] == [
        ("DELETE", f"{API}/collections/{COLLECTION}/items/{sent_as}")
    ]


@pytest.mark.parametrize("item_id", ["..", ".", ""], ids=["dot-dot", "dot", "empty"])
def test_clean_refuses_an_item_it_cannot_address_before_touching_it(
    monkeypatch: pytest.MonkeyPatch, item_id: str
) -> None:
    """`items/..` is the collection's URL: refused before the item's S3 delete or any request."""
    sent = _transport(monkeypatch)
    s3 = MagicMock()
    mgr = manage_collections.STACCollectionManager(API)
    mgr.get_collection_items = MagicMock(return_value=[{**_item("SCENE"), "id": item_id}])  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="not usable as a URL path segment"):
        mgr.clean_collection(
            COLLECTION,
            clean_s3=True,
            s3_client=s3,
            confinement=[("bucket", "tests-output/sentinel-2-l2a-staging/")],
        )
    assert sent == []
    assert s3.mock_calls == []


@pytest.mark.parametrize("flag", [[], ["--allow-demo"]], ids=["default", "allow-demo"])
def test_cli_clean_passes_allow_demo(monkeypatch: pytest.MonkeyPatch, flag: list[str]) -> None:
    clean = MagicMock(return_value=(0, 0, 0))
    monkeypatch.setattr(manage_collections.STACCollectionManager, "clean_collection", clean)

    result = CliRunner().invoke(
        manage_collections.cli, ["--api-url", API, "clean", COLLECTION, "-y", *flag]
    )

    assert result.exit_code == 0, result.output
    assert clean.call_args.kwargs["allow_demo"] is bool(flag)


def _collections_cli(
    monkeypatch: pytest.MonkeyPatch, items: list[dict], *args: str, answer: str = "y\n"
) -> tuple[Result, MagicMock, MagicMock]:
    """The collections CLI end to end over `items`: (result, item deletes, the walk)."""
    delete_item = MagicMock(return_value=(True, 0, 0))
    walk = MagicMock(return_value=items)
    monkeypatch.setattr(manage_collections.STACItemManager, "delete_item", delete_item)
    monkeypatch.setattr(manage_collections.STACCollectionManager, "get_collection_items", walk)
    result = CliRunner().invoke(manage_collections.cli, ["--api-url", API, *args], input=answer)
    return result, delete_item, walk


@pytest.mark.parametrize(
    ("flag", "listing", "scope"),
    [
        ([], f"demo scenes (demo denylist): 1\n    - {DEMO_A}", "except"),
        (["--allow-demo"], f"NOT protected in this run:\n    - {DEMO_A}", "including"),
    ],
    ids=["default", "allow-demo"],
)
def test_cli_clean_asks_after_listing_the_demo_scenes(
    monkeypatch: pytest.MonkeyPatch, flag: list[str], listing: str, scope: str
) -> None:
    """The listed ids it keeps or deletes are on screen when the prompt asks; "no" deletes nothing."""
    result, delete_item, _ = _collections_cli(
        monkeypatch, [_item("ITEM_0"), _item(DEMO_A)], "clean", COLLECTION, *flag, answer="n\n"
    )

    assert result.exit_code != 0
    assert result.output.index(listing) < result.output.index("Continue?")
    assert f"delete ALL items, {scope} those on the demo denylist, from" in result.output
    assert "Operation failed" not in result.output
    delete_item.assert_not_called()


# === manage_collections.py delete ===


@pytest.fixture
def lookup() -> Iterator[MagicMock]:
    """The denylist lookup: a POST on a requests.Session, autospecced so each call records
    the session it went through. Answers with `.found`, unless a test sets `.side_effect`."""
    with patch.object(requests.Session, "post", autospec=True) as post:
        post.found = []
        post.side_effect = lambda session, url, **kwargs: _found(*post.found)
        yield post


@pytest.fixture
def http_delete(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    delete = MagicMock()
    monkeypatch.setattr(requests.Session, "delete", delete)
    return delete


def test_collection_delete_refuses_while_a_listed_id_remains(
    lookup: MagicMock, http_delete: MagicMock, capsys: pytest.CaptureFixture[str]
) -> None:
    lookup.found = [DEMO_B, "S2A_MSIL2A_NOT_A_DEMO"]  # only listed ids count

    mgr = manage_collections.STACCollectionManager(API)
    with pytest.raises(click.ClickException, match="holds the 1 item"):
        mgr.delete_collection(COLLECTION)

    http_delete.assert_not_called()
    err = capsys.readouterr().err
    assert f"    - {DEMO_B}" in err
    assert "NOT_A_DEMO" not in err
    # One attempt, on the session the DELETE uses and with its timeout, not following a
    # redirect: the listed ids among this collection's items, ids only.
    lookup.assert_called_once_with(
        mgr.session,
        f"{API}/search",
        json={
            "collections": [COLLECTION],
            "ids": [DEMO_A, DEMO_B],
            "limit": 2,
            "fields": {"include": ["id"], "exclude": ["links"]},
        },
        timeout=30,
        allow_redirects=False,
    )


def test_collection_delete_proceeds_when_no_listed_id_remains(
    lookup: MagicMock, http_delete: MagicMock
) -> None:
    assert manage_collections.STACCollectionManager(API).delete_collection(COLLECTION)
    http_delete.assert_called_once_with(f"{API}/collections/{COLLECTION}", timeout=30)


@pytest.mark.parametrize("allow_demo", [False, True], ids=["default", "allow-demo"])
@pytest.mark.parametrize(
    "answer",
    [
        requests.ConnectionError("lookup down"),
        RuntimeError("Failed to fetch OIDC token: 503"),
        _response({"features": []}, status=502),  # parses, but is not an answer
        _response({"features": []}, status=301),  # not followed, and not an answer
        _response(b"<html>maintenance</html>"),
        _response({}),
        _response({"features": None}),
        _response({"features": [{"collection": COLLECTION}]}),
        _response({"features": [], "links": [{"rel": "next", "href": f"{API}/search"}]}),
    ],
    ids=[
        "transport",
        "auth-hook",
        "http-502",
        "redirect",
        "2xx-not-json",
        "no-features",
        "null-features",
        "feature-without-id",
        "next-page",
    ],
)
def test_collection_delete_refuses_when_the_lookup_fails(
    lookup: MagicMock, http_delete: MagicMock, answer: object, allow_demo: bool
) -> None:
    lookup.side_effect = [answer]

    with pytest.raises(click.ClickException, match="could not check it for demo scenes"):
        manage_collections.STACCollectionManager(API).delete_collection(
            COLLECTION, allow_demo=allow_demo
        )
    http_delete.assert_not_called()


@pytest.mark.parametrize("collection_id", ["sentinel%2D2%2Dl2a-staging", f"./{COLLECTION}"])
def test_collection_delete_refuses_an_id_that_is_not_plain(
    lookup: MagicMock, http_delete: MagicMock, collection_id: str
) -> None:
    """The lookup matches ids as given, but the DELETE URL is normalised to COLLECTION."""
    with pytest.raises(click.ClickException, match="not a plain STAC id"):
        manage_collections.STACCollectionManager(API).delete_collection(collection_id)
    lookup.assert_not_called()
    http_delete.assert_not_called()


@pytest.mark.usefixtures("unusable_list")
@pytest.mark.parametrize("allow_demo", [False, True], ids=["default", "allow-demo"])
def test_collection_delete_refuses_when_the_list_cannot_be_used(
    lookup: MagicMock, http_delete: MagicMock, allow_demo: bool
) -> None:
    with pytest.raises(click.ClickException, match="cannot use the demo denylist"):
        manage_collections.STACCollectionManager(API).delete_collection(
            COLLECTION, allow_demo=allow_demo
        )
    lookup.assert_not_called()
    http_delete.assert_not_called()


def test_collection_delete_allow_demo_deletes_and_prints_each_listed_id(
    lookup: MagicMock, http_delete: MagicMock, capsys: pytest.CaptureFixture[str]
) -> None:
    lookup.found = [DEMO_A, DEMO_B]

    mgr = manage_collections.STACCollectionManager(API)
    assert mgr.delete_collection(COLLECTION, allow_demo=True)

    http_delete.assert_called_once()
    assert f"on the demo denylist:\n    - {DEMO_A}\n    - {DEMO_B}" in capsys.readouterr().out


def test_delete_clean_first_refuses_before_the_prompt_and_the_clean(
    monkeypatch: pytest.MonkeyPatch, lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.found = [DEMO_A]
    result, delete_item, walk = _collections_cli(
        monkeypatch, [_item("ITEM_0"), _item(DEMO_A)], "delete", COLLECTION, "--clean-first"
    )

    assert result.exit_code != 0
    assert f"    - {DEMO_A}" in result.output
    assert f"Refusing to delete collection {COLLECTION}: it holds the 1 item" in result.output
    assert "Continue?" not in result.output
    walk.assert_not_called()
    delete_item.assert_not_called()
    http_delete.assert_not_called()


def test_delete_clean_first_still_refuses_a_listed_id_found_after_the_clean(
    monkeypatch: pytest.MonkeyPatch, lookup: MagicMock, http_delete: MagicMock
) -> None:
    """The check before the prompt finds none; the one before the DELETE finds DEMO_A."""
    lookup.side_effect = [_found(), _found(DEMO_A)]
    result, delete_item, _ = _collections_cli(
        monkeypatch, [_item("ITEM_0"), _item(DEMO_A)], "delete", COLLECTION, "--clean-first"
    )

    assert result.exit_code != 0
    assert "remove all items except those on the demo denylist" in result.output  # the prompt
    assert [c.kwargs["item_id"] for c in delete_item.call_args_list] == ["ITEM_0"]
    assert f"Refusing to delete collection {COLLECTION}: it holds the 1 item" in result.output
    http_delete.assert_not_called()


def test_delete_clean_first_allow_demo_lists_the_demo_scenes_then_deletes_everything(
    monkeypatch: pytest.MonkeyPatch, lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.found = [DEMO_A]
    result, delete_item, _ = _collections_cli(
        monkeypatch,
        [_item("ITEM_0"), _item(DEMO_A)],
        "delete",
        COLLECTION,
        "--clean-first",
        "--allow-demo",
    )

    assert result.exit_code == 0, result.output
    assert result.output.index(f"    - {DEMO_A}") < result.output.index("Continue?")
    assert [c.kwargs["item_id"] for c in delete_item.call_args_list] == ["ITEM_0", DEMO_A]
    http_delete.assert_called_once()


def test_cli_delete_allow_demo_reaches_the_collection_delete(
    lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.found = [DEMO_A]

    result = CliRunner().invoke(
        manage_collections.cli,
        ["--api-url", API, "delete", COLLECTION, "--allow-demo"],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    assert result.output.index(f"    - {DEMO_A}") < result.output.index("Continue?")
    http_delete.assert_called_once()
