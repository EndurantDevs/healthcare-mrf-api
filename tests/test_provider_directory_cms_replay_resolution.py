# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recognize only the exact two-argument repair under unchanged protected pins."""

from copy import deepcopy

import pytest

from db import migration_cms_replay_resolution as migration
from process import network_fhir_source_custody as reader
from tests.test_cms_archive_transaction_migration import body_digest


@pytest.mark.parametrize("schema", ["snapshot_schema", "alternate_catalog"])
def test_only_two_existing_varchar_arguments_change(schema):
    original, hardened = migration.replay_guard_bodies(schema)
    assert hardened.count("completion_proof_sha256::pg_catalog.text") == 2
    assert hardened.replace("completion_proof_sha256::pg_catalog.text", "completion_proof_sha256") == original
    assert body_digest(original, schema) == migration.ORIGINAL_SHA256


def _reader_function(schema, version):
    body = migration.replay_guard_bodies(schema)[version]
    expected = reader._REACHABLE_FUNCTIONS[migration.GUARD]
    return dict(
        proname=migration.GUARD,
        prosrc=body,
        lanname=expected[0],
        provolatile=expected[1],
        prosecdef=expected[2],
        proconfig=list(expected[3]),
        proowner=100,
    )


@pytest.mark.parametrize("version", [0, 1])
def test_retained_reader_accepts_exact_variant_and_preserves_input(version):
    function = _reader_function("snapshot_schema", version)
    before = deepcopy(function)
    reader._require_function(function, "snapshot_schema")
    assert function == before


@pytest.mark.parametrize("fault", ["security", "settings", "volatility", "language"])
def test_typed_reader_variant_keeps_every_other_native_check(fault):
    function = _reader_function("snapshot_schema", 1)
    key, value = {
        "security": ("prosecdef", False),
        "settings": ("proconfig", ["search_path=pg_catalog,public"]),
        "volatility": ("provolatile", "s"),
        "language": ("lanname", "sql"),
    }[fault]
    function[key] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "snapshot_schema")
