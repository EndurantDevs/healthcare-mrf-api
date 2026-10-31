# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact lifecycle successor recognition leaves the ordinary guard intact."""

import hashlib
from copy import deepcopy

import pytest

from db import migration_cms_noncurrent_seal as migration
from process import network_fhir_source_custody as reader
from process import provider_directory_source_callable_custody as custody
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS
from tests.test_provider_directory_source_callable_custody import _entry, _roles

_KEY = migration.GUARD + "()"


@pytest.mark.parametrize("schema", ["sample", "alternate_snapshot"])
def test_successor_changes_only_the_final_lifecycle_rejection(schema):
    old, new = migration.lifecycle_guard_bodies(schema)
    assert custody._body_digest(old, schema) == migration.ORIGINAL_SHA256
    assert custody._body_digest(new, schema) == "d26dc42c0034bfeb6020cd481a330d8ba1edd4a386d3e47c2f54a68a3888f3c9"
    compact = migration._migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    subset = compact._terminal()._subset()
    lifecycle = subset._endpoint_dataset_lifecycle_sql(
        subset._SUBSET_IMMUTABLE_COMPARISON_SQL, subset_metadata_is_immutable=True
    )
    replacement = "IF (" + lifecycle + ") AND " + migration._cms_transition_sql() + " IS DISTINCT FROM TRUE THEN"
    assert new.count(replacement) == 1
    assert new.replace(replacement, "IF " + lifecycle + " THEN", 1) == old
    assert old.count("RAISE EXCEPTION") == new.count("RAISE EXCEPTION")
    assert "current_setting('role')='none'" in new
    assert "pg_catalog.to_jsonb(NEW)-ARRAY['status','published_at']" in new
    assert "pg_catalog.to_jsonb(NEW)-'is_current'" in new


@pytest.mark.parametrize("body_index", [0, 1])
def test_both_exact_bodies_are_recognized_without_changing_default_contract(body_index):
    entry = _entry(_KEY)
    entry["definition"]["prosrc"] = migration.lifecycle_guard_bodies("sample")[body_index]
    custody._require_definition(entry, custody._CONTRACTS[_KEY], "sample", 201, custody._TYPE_OIDS, "sample")
    assert custody._CONTRACTS[_KEY][-1] == migration.ORIGINAL_SHA256


@pytest.mark.parametrize(
    "field,value",
    [
        ("pronamespace", "202"),
        ("pronargs", 1),
        ("proargdefaults", "changed"),
        ("prosecdef", False),
        ("proleakproof", True),
        ("proisstrict", True),
        ("proconfig", ["search_path=sample, pg_catalog"]),
        ("prorettype", "25"),
        ("provolatile", "s"),
        ("proparallel", "s"),
        ("prosrc", "BEGIN RETURN NEW; END"),
    ],
)
def test_successor_keeps_every_definition_fence(field, value):
    entry = _entry(_KEY)
    entry["definition"]["prosrc"] = migration.lifecycle_guard_bodies("sample")[1]
    entry["definition"][field] = value
    with pytest.raises(ValueError, match="definition_changed"):
        custody._require_definition(entry, custody._CONTRACTS[_KEY], "sample", 201, custody._TYPE_OIDS, "sample")


@pytest.mark.parametrize("body_index", [0, 1])
def test_even_one_byte_of_unapproved_body_is_refused(body_index):
    entry = _entry(_KEY)
    entry["definition"]["prosrc"] = migration.lifecycle_guard_bodies("sample")[body_index] + "\n"
    with pytest.raises(ValueError, match="definition_changed"):
        custody._require_definition(entry, custody._CONTRACTS[_KEY], "sample", 201, custody._TYPE_OIDS, "sample")


@pytest.mark.parametrize("owner", [101, 103, 104, 105, 106])
def test_successor_does_not_extend_callable_ownership_or_acl(owner):
    entry = _entry(_KEY, owner=owner)
    entry["definition"]["prosrc"] = migration.lifecycle_guard_bodies("sample")[1]
    if owner == 101:
        entry["acl"].append([103, owner, "EXECUTE", False])
        custody._require_acl(entry, _roles(), legacy=False)
        forged = deepcopy(entry)
        forged["acl"].append([102, owner, "EXECUTE", False])
        with pytest.raises(ValueError, match="acl_changed"):
            custody._require_acl(forged, _roles(), legacy=False)
    else:
        with pytest.raises(ValueError, match="owner_changed"):
            custody._require_acl(entry, _roles(), legacy=False)


def test_the_five_metadata_callables_remain_separate_and_pinned():
    assert len(METADATA_FUNCTIONS) == 5
    assert all(not key.startswith(migration.GUARD) for key in METADATA_FUNCTIONS)
    digests_by_name = {
        "provider_directory_endpoint_dataset_admission_metadata_sha256": "57c2890fad111ab97f0ac62e64d0e66ab1685d424259bfe839b823bb1650f40f",
        "provider_directory_subset_canonical_sha256": "b2113ad61fc542dd81ee7afde3afe31eab2a549f0e4e983b4e422a75c5c33cb6",
        "provider_directory_subset_completion_proof_pair_valid": "52d0e8b812dc70ebf7b75bd94dd95b7ee99cbf63ff58f6e9cdeda6dcb1d59c69",
        "provider_directory_subset_completion_proof_shape_valid": "505a38a4011eb99d7f24a8ea344d8433c1514fb3c71e3944b8718b9504ce24f4",
        "provider_directory_subset_content_proof_valid": "6ffdbd5dee49b00851284cc57051d3af0d29cefa59d28300d50b8b7ecb539113",
    }
    assert {
        key.split("(", 1)[0]: spec[-1]
        for key, spec in custody._CONTRACTS.items()
        if key.split("(", 1)[0] in digests_by_name
    } == digests_by_name


def _reader_lifecycle(schema, version, hardened_settings=False):
    expected = reader._REACHABLE_FUNCTIONS[migration.GUARD]
    settings = list(expected[4])
    compact = migration._migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    body = (
        migration.lifecycle_guard_bodies(schema)[version - 1]
        if version
        else compact._function_body(compact._endpoint_guard_sql(schema, compact=True))
    )
    return dict(
        proname=migration.GUARD,
        prosrc=body,
        lanname=expected[1],
        provolatile=expected[2],
        prosecdef=expected[3],
        proconfig=approved_cms_routine_settings(settings)[1] if hardened_settings else settings,
        proowner=100,
    )


def _reader_lifecycle_hook(schema, version, hardened_settings=False):
    hook_name = "tin_npi_connector_endpoint_dataset_guard"
    return dict(
        **_reader_lifecycle(schema, version, hardened_settings),
        tgtype=31,
        tgenabled="O",
        unconditional=True,
        tgdeferrable=False,
        tginitdeferred=False,
        update_columns=reader._UPDATE_COLUMNS[hook_name],
        tgoldtable=None,
        tgnewtable=None,
        tgnargs=0,
        tgargs=b"",
        nspname=schema,
        pronargs=0,
        returns_trigger=True,
    )


@pytest.mark.parametrize("schema", ["snapshot_schema", "Mixed_Catalog23"])
@pytest.mark.parametrize("version", [0, 1, 2])
@pytest.mark.parametrize("hardened_settings", [False, True])
@pytest.mark.parametrize("precomputed", [False, True])
def test_retained_reader_checks_three_exact_lifecycle_variants(schema, version, hardened_settings, precomputed):
    function = _reader_lifecycle(schema, version, hardened_settings)
    if precomputed:
        body = function.pop("prosrc").replace(reader._identifier(schema) + ".", '"__schema__".')
        function["body_sha256"] = hashlib.sha256(reader._normalized(body).encode()).hexdigest()
    before = deepcopy(function)
    reader._require_function(function, schema, 100)
    assert function == before
    hook = _reader_lifecycle_hook(schema, version, hardened_settings)
    before = deepcopy(hook)
    reader._require_guard(hook, schema, 100, "tin_npi_connector_endpoint_dataset_guard", 31)
    assert hook == before


@pytest.mark.parametrize("pin", ["reachable", "hook"])
def test_retained_lifecycle_successor_requires_both_original_body_pins(monkeypatch, pin):
    function = _reader_lifecycle("snapshot_schema", 2)
    hook = _reader_lifecycle_hook("snapshot_schema", 2)
    if pin == "reachable":
        spec = reader._REACHABLE_FUNCTIONS[migration.GUARD]
        monkeypatch.setitem(reader._REACHABLE_FUNCTIONS, migration.GUARD, ("f" * 64, *spec[1:]))
    else:
        monkeypatch.setitem(reader._FUNCTION_HASHES, migration.GUARD, "f" * 64)
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "snapshot_schema", 100)
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_guard(hook, "snapshot_schema", 100, "tin_npi_connector_endpoint_dataset_guard", 31)


@pytest.mark.parametrize("fault", ["extra_sql", "schema"])
def test_retained_lifecycle_refuses_near_miss_successor_bodies(fault):
    function = _reader_lifecycle("snapshot_schema", 2)
    function["prosrc"] = (
        function["prosrc"] + "\nRETURN NULL;"
        if fault == "extra_sql"
        else function["prosrc"].replace('"snapshot_schema".', '"other_catalog".')
    )
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "snapshot_schema", 100)
    hook = _reader_lifecycle_hook("snapshot_schema", 2)
    hook["prosrc"] = function["prosrc"]
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_guard(hook, "snapshot_schema", 100, "tin_npi_connector_endpoint_dataset_guard", 31)


@pytest.mark.parametrize(
    "field,value",
    [
        ("prosrc", "BEGIN RETURN NEW; END"),
        ("proowner", 999),
        ("prosecdef", False),
        ("proconfig", ["search_path=public"]),
        ("lanname", "sql"),
        ("provolatile", "s"),
    ],
)
def test_retained_lifecycle_successor_preserves_other_function_checks(field, value):
    function = _reader_lifecycle("snapshot_schema", 2)
    function[field] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "snapshot_schema", 100)
    hook = _reader_lifecycle_hook("snapshot_schema", 2)
    hook[field] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_guard(hook, "snapshot_schema", 100, "tin_npi_connector_endpoint_dataset_guard", 31)


@pytest.mark.parametrize(
    "field,value",
    [
        ("tgtype", 23),
        ("tgenabled", "D"),
        ("unconditional", False),
        ("tgdeferrable", True),
        ("tginitdeferred", True),
        ("update_columns", ()),
        ("tgoldtable", "other_rows"),
        ("tgnewtable", "other_rows"),
        ("tgnargs", 1),
        ("tgargs", b"other\0"),
        ("nspname", "other_catalog"),
        ("proname", "other_guard"),
        ("pronargs", 1),
        ("returns_trigger", False),
    ],
)
def test_retained_lifecycle_successor_preserves_every_hook_check(field, value):
    hook = _reader_lifecycle_hook("snapshot_schema", 2)
    hook[field] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_guard(hook, "snapshot_schema", 100, "tin_npi_connector_endpoint_dataset_guard", 31)
