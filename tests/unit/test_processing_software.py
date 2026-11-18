"""The shared ``processing:software`` parser: both wire shapes, and what counts as unreadable."""

import json

import pytest
from packaging.version import Version
from processing_software import is_final, read_version

KEY = "EOPF_GEOZARR"


@pytest.mark.parametrize("encode", [lambda s: s, json.dumps], ids=["object", "json-string"])
def test_reads_the_object_and_the_json_string_the_fields_extension_returns(encode):
    properties = {"processing:software": encode({"EOPF_CPM": "3.0.0", KEY: "0.13.1"})}
    assert read_version(properties, KEY) == Version("0.13.1")


@pytest.mark.parametrize(
    "properties",
    [{}, {"processing:software": None}, {"processing:software": {"EOPF_CPM": "3.0.0"}}],
    ids=["no-software", "null-software", "no-key"],
)
def test_absent_is_none(properties):
    assert read_version(properties, KEY) is None


@pytest.mark.parametrize(
    "software",
    [
        {KEY: "v0.13.1-0-g1a2b3c4"},  # data-model's git-describe stamp
        {KEY: "1a2b3c4"},  # a bare sha
        {KEY: "1234567"},  # an all-digit sha: PEP 440 alone reads it as release 1234567
        {KEY: "1!0.1"},  # an epoch: PEP 440 orders it above every epoch-0 release, and final
        {KEY: ""},
        {KEY: 13},
        "not json",
        ["0.13.1"],
    ],
    ids=["git-describe", "sha", "digit-sha", "epoch", "empty", "number", "not-json", "list"],
)
def test_unreadable_raises(software):
    with pytest.raises(ValueError):
        read_version({"processing:software": software}, KEY)


@pytest.mark.parametrize(
    ("value", "final"),
    [
        ("0.13.1", True),
        ("v0.13.1", True),
        ("0.13.1.post1", True),
        ("0.14.0rc1", False),
        ("0.13.2.dev3", False),
        ("0.13.0+eodc.1", False),
    ],
)
def test_only_releases_are_final(value, final):
    assert is_final(Version(value)) is final
