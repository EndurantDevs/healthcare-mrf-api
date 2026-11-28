# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical CMS data predicates and legacy recognizer boundaries."""

from copy import deepcopy

import pytest

from db import migration_cms_noncurrent_seal as migration
from process import network_fhir_source_custody as reader
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from tests.test_cms_archive_transaction_migration import body_digest

_KEY = migration.GUARD + "()"


@pytest.mark.parametrize("schema", ["sample", "alternate_snapshot"])
def test_successor_changes_only_the_final_lifecycle_rejection(schema):
    old, new = migration.lifecycle_guard_bodies(schema)
    assert body_digest(old, schema) == migration.ORIGINAL_SHA256
    assert body_digest(new, schema) == "d26dc42c0034bfeb6020cd481a330d8ba1edd4a386d3e47c2f54a68a3888f3c9"
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


def _reader_lifecycle(schema, version, hardened_settings=False):
    expected = reader._REACHABLE_FUNCTIONS[migration.GUARD]
    settings = list(expected[3])
    compact = migration._migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    body = (
        migration.lifecycle_guard_bodies(schema)[version - 1]
        if version
        else compact._function_body(compact._endpoint_guard_sql(schema, compact=True))
    )
    return dict(
        proname=migration.GUARD,
        prosrc=body,
        lanname=expected[0],
        provolatile=expected[1],
        prosecdef=expected[2],
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
@pytest.mark.parametrize("version", [0, 1])
@pytest.mark.parametrize("hardened_settings", [False, True])
def test_historical_reader_lifecycle_variants(schema, version, hardened_settings):
    function = _reader_lifecycle(schema, version, hardened_settings)
    before = deepcopy(function)
    reader._require_function(function, schema)
    assert function == before
    hook = _reader_lifecycle_hook(schema, version, hardened_settings)
    before = deepcopy(hook)
    reader._require_guard(hook, schema, "tin_npi_connector_endpoint_dataset_guard", 31)
    assert hook == before


@pytest.mark.parametrize("fault", ["extra_sql", "schema", "owner"])
def test_retained_lifecycle_reader_no_longer_compares_body_or_owner(fault):
    function = _reader_lifecycle("snapshot_schema", 2)
    hook = _reader_lifecycle_hook("snapshot_schema", 2)
    for record in (function, hook):
        if fault == "owner":
            record["proowner"] = 999
        elif fault == "extra_sql":
            record["prosrc"] += "\nRETURN NULL;"
        else:
            record["prosrc"] = record["prosrc"].replace('"snapshot_schema".', '"other_catalog".')
    reader._require_function(function, "snapshot_schema")
    reader._require_guard(hook, "snapshot_schema", "tin_npi_connector_endpoint_dataset_guard", 31)


@pytest.mark.parametrize(
    "field,value",
    [
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
        reader._require_function(function, "snapshot_schema")
    hook = _reader_lifecycle_hook("snapshot_schema", 2)
    hook[field] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_guard(hook, "snapshot_schema", "tin_npi_connector_endpoint_dataset_guard", 31)


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
        reader._require_guard(hook, "snapshot_schema", "tin_npi_connector_endpoint_dataset_guard", 31)
