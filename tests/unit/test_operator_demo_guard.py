"""Unit tests for the demo-denylist guard in the operator delete paths.

The demo denylist (``resolve_exclude_ids``: ``$EXPIRES_EXCLUDE_FILE``, else the baked
``scripts/demo_exclude_ids.txt``) already protects these scenes from the expiry
cleanup. The operator CLIs honour it too:

- ``manage_item.py delete`` refuses a listed id before any STAC or S3 call;
- ``manage_collections.py clean`` skips listed ids and reports them on their own line;
- ``manage_collections.py delete`` refuses while the collection still holds a listed
  id, so ``--clean-first`` cleans everything else and then refuses;
- ``--allow-demo`` overrides each guard and prints every listed id it deletes;
- a list that cannot be read, or a lookup that fails, refuses (fails closed);
- the ids given to the three commands must be plain STAC ids.

No network: HTTP, S3 and the STAC search are mocked.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

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


@pytest.fixture(params=["missing", "empty", "not-utf8"])
def unreadable_list(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A denylist that cannot be resolved: missing, with no ids, or not UTF-8."""
    path = tmp_path / f"{request.param}.txt"
    if request.param == "empty":
        path.write_text("# nothing here\n")
    elif request.param == "not-utf8":
        path.write_bytes(b"S2A_MSIL2A_\xe9\n")
    monkeypatch.setenv("EXPIRES_EXCLUDE_FILE", str(path))


def _item(item_id: str) -> dict:
    href = f"{CONFINE}{item_id}/data.zarr/"
    return {"id": item_id, "assets": {"data": {"alternate": {"s3": {"href": href}}}}}


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


def test_item_delete_allow_demo_deletes_a_listed_id_and_prints_it(
    item_calls: dict[str, MagicMock],
) -> None:
    result = _delete_item(DEMO_A, "--allow-demo")

    assert result.exit_code == 0, result.output
    assert f"--allow-demo: deleting {DEMO_A}" in result.output
    assert item_calls["delete_item"].call_args.kwargs["item_id"] == DEMO_A


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


@pytest.mark.usefixtures("unreadable_list")
def test_item_delete_refuses_when_the_list_cannot_be_resolved(
    item_calls: dict[str, MagicMock],
) -> None:
    result = _delete_item("S2A_MSIL2A_NOT_A_DEMO")

    assert result.exit_code != 0
    assert "demo denylist is unavailable" in result.output
    assert [name for name, call in item_calls.items() if call.called] == []


@pytest.mark.usefixtures("unreadable_list")
def test_item_delete_allow_demo_proceeds_without_a_readable_list(
    item_calls: dict[str, MagicMock],
) -> None:
    result = _delete_item("S2A_MSIL2A_NOT_A_DEMO", "--allow-demo")

    assert result.exit_code == 0, result.output
    assert "Demo denylist unavailable" in result.output
    item_calls["delete_item"].assert_called_once()


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


@pytest.mark.usefixtures("unreadable_list")
def test_clean_refuses_when_the_list_cannot_be_resolved() -> None:
    mgr = _manager([_item("ITEM_0")])

    with pytest.raises(click.ClickException, match="demo denylist is unavailable"):
        _clean(mgr)
    mgr.item_manager.delete_item.assert_not_called()


@pytest.mark.usefixtures("unreadable_list")
def test_clean_allow_demo_proceeds_without_a_readable_list() -> None:
    mgr = _manager([_item("ITEM_0")])

    assert _clean(mgr, allow_demo=True) == 1


@pytest.mark.parametrize("flag", [[], ["--allow-demo"]], ids=["default", "allow-demo"])
def test_cli_clean_passes_allow_demo(monkeypatch: pytest.MonkeyPatch, flag: list[str]) -> None:
    clean = MagicMock(return_value=(0, 0, 0))
    monkeypatch.setattr(manage_collections.STACCollectionManager, "clean_collection", clean)

    result = CliRunner().invoke(
        manage_collections.cli, ["--api-url", API, "clean", COLLECTION, "-y", *flag]
    )

    assert result.exit_code == 0, result.output
    assert clean.call_args.kwargs["allow_demo"] is bool(flag)


# === manage_collections.py delete ===


@pytest.fixture
def lookup(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """The read client behind the denylist lookup; set .found or .search.side_effect."""
    catalog = MagicMock()
    monkeypatch.setattr(
        manage_collections.stac_auth, "open_resilient_client", MagicMock(return_value=catalog)
    )
    catalog.found = []
    catalog.search.return_value.items_as_dicts.side_effect = lambda: iter(
        {"id": i} for i in catalog.found
    )
    return catalog


@pytest.fixture
def http_delete(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    delete = MagicMock()
    monkeypatch.setattr(requests.Session, "delete", delete)
    return delete


def test_collection_delete_refuses_while_a_listed_id_remains(
    lookup: MagicMock, http_delete: MagicMock, capsys: pytest.CaptureFixture[str]
) -> None:
    lookup.found = [DEMO_B]

    with pytest.raises(click.ClickException, match="holds the 1 item"):
        manage_collections.STACCollectionManager(API).delete_collection(COLLECTION)

    http_delete.assert_not_called()
    assert f"    - {DEMO_B}" in capsys.readouterr().err
    # One read for the whole list, scoped to this collection.
    lookup.search.assert_called_once_with(
        collections=[COLLECTION], ids=[DEMO_A, DEMO_B], max_items=2, limit=2
    )


def test_collection_delete_proceeds_when_no_listed_id_remains(
    lookup: MagicMock, http_delete: MagicMock
) -> None:
    assert manage_collections.STACCollectionManager(API).delete_collection(COLLECTION)
    http_delete.assert_called_once_with(f"{API}/collections/{COLLECTION}", timeout=30)


def test_collection_delete_refuses_when_the_lookup_fails(
    lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.search.side_effect = RuntimeError("502 Bad Gateway")

    with pytest.raises(click.ClickException, match="could not check it for demo scenes"):
        manage_collections.STACCollectionManager(API).delete_collection(COLLECTION)
    http_delete.assert_not_called()


@pytest.mark.parametrize("collection_id", ["sentinel%2D2%2Dl2a-staging", f"./{COLLECTION}"])
def test_collection_delete_refuses_an_id_that_is_not_plain(
    lookup: MagicMock, http_delete: MagicMock, collection_id: str
) -> None:
    """The lookup matches ids as given, but the DELETE URL is normalised to COLLECTION."""
    with pytest.raises(click.ClickException, match="not a plain STAC id"):
        manage_collections.STACCollectionManager(API).delete_collection(collection_id)
    lookup.search.assert_not_called()
    http_delete.assert_not_called()


@pytest.mark.usefixtures("unreadable_list")
def test_collection_delete_refuses_when_the_list_cannot_be_resolved(
    lookup: MagicMock, http_delete: MagicMock
) -> None:
    with pytest.raises(click.ClickException, match="demo denylist is unavailable"):
        manage_collections.STACCollectionManager(API).delete_collection(COLLECTION)
    lookup.search.assert_not_called()
    http_delete.assert_not_called()


def test_collection_delete_allow_demo_deletes_and_prints_each_listed_id(
    lookup: MagicMock, http_delete: MagicMock, capsys: pytest.CaptureFixture[str]
) -> None:
    lookup.found = [DEMO_A, DEMO_B]

    mgr = manage_collections.STACCollectionManager(API)
    assert mgr.delete_collection(COLLECTION, allow_demo=True)

    http_delete.assert_called_once()
    assert f"on the demo denylist:\n    - {DEMO_A}\n    - {DEMO_B}" in capsys.readouterr().out


def test_collection_delete_allow_demo_overrides_a_failed_lookup(
    lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.search.side_effect = RuntimeError("502 Bad Gateway")

    assert manage_collections.STACCollectionManager(API).delete_collection(
        COLLECTION, allow_demo=True
    )
    http_delete.assert_called_once()


def _delete_collection_cli(
    monkeypatch: pytest.MonkeyPatch, items: list[dict], *extra: str
) -> tuple[Result, MagicMock]:
    """`delete --clean-first` end to end: the clean, then the guarded DELETE."""
    delete_item = MagicMock(return_value=(True, 0, 0))
    monkeypatch.setattr(manage_collections.STACItemManager, "delete_item", delete_item)
    monkeypatch.setattr(
        manage_collections.STACCollectionManager,
        "get_collection_items",
        MagicMock(return_value=items),
    )
    result = CliRunner().invoke(
        manage_collections.cli,
        ["--api-url", API, "delete", COLLECTION, "--clean-first", *extra],
        input="y\n",
    )
    return result, delete_item


def test_delete_clean_first_cleans_the_rest_then_refuses(
    monkeypatch: pytest.MonkeyPatch, lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.found = [DEMO_A]  # what the clean left behind
    result, delete_item = _delete_collection_cli(monkeypatch, [_item("ITEM_0"), _item(DEMO_A)])

    assert result.exit_code != 0
    assert "remove all items except those on the demo denylist" in result.output  # the prompt
    assert [c.kwargs["item_id"] for c in delete_item.call_args_list] == ["ITEM_0"]
    assert f"Refusing to delete collection {COLLECTION}" in result.output
    http_delete.assert_not_called()


def test_delete_clean_first_allow_demo_deletes_everything(
    monkeypatch: pytest.MonkeyPatch, lookup: MagicMock, http_delete: MagicMock
) -> None:
    lookup.found = []  # the clean removed the demo too
    result, delete_item = _delete_collection_cli(
        monkeypatch, [_item("ITEM_0"), _item(DEMO_A)], "--allow-demo"
    )

    assert result.exit_code == 0, result.output
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
    assert f"    - {DEMO_A}" in result.output
    http_delete.assert_called_once()
