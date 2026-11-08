# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recognize only the exact two-argument repair under unchanged protected pins."""

import hashlib
from copy import deepcopy

import pytest

from db import migration_cms_replay_resolution as migration
from process import network_fhir_source_custody as reader
from process import provider_directory_cms_callable_custody as custody
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _original_allocation_pins


@pytest.mark.parametrize("schema", ["snapshot_schema", "alternate_catalog"])
def test_only_two_existing_varchar_arguments_change(schema):
    original, hardened = migration.replay_guard_bodies(schema)
    assert hardened.count("completion_proof_sha256::pg_catalog.text") == 2
    assert hardened.replace("completion_proof_sha256::pg_catalog.text", "completion_proof_sha256") == original
    assert custody._body_digest(original, schema) == migration.ORIGINAL_SHA256


@pytest.mark.parametrize("version", [0, 1])
def test_original_allocation_requires_complete_protected_variant_pin(original_guards, version):
    rows = deepcopy(original_guards)
    guard = next(row for row in rows if row["definition"]["proname"] == migration.GUARD)
    guard["definition"]["prosrc"] = migration.replay_guard_bodies("snapshot_schema")[version]
    pins = _original_allocation_pins(rows)
    assert custody.require_original_cms_allocation_callees(rows, **pins)[migration.GUARD] == guard
    guard["definition"]["prosrc"] += "\n-- unapproved"
    with pytest.raises(ValueError, match="original_allocation_pin_changed"):
        custody.require_original_cms_allocation_callees(rows, **pins)
    pins = _original_allocation_pins(rows)
    with pytest.raises(ValueError, match="definition_changed"):
        custody.require_original_cms_allocation_callees(rows, **pins)


def _reader_function(schema, version):
    body = migration.replay_guard_bodies(schema)[version]
    expected = reader._REACHABLE_FUNCTIONS[migration.GUARD]
    return dict(
        proname=migration.GUARD,
        prosrc=body,
        lanname=expected[1],
        provolatile=expected[2],
        prosecdef=expected[3],
        proconfig=list(expected[4]),
        proowner=100,
    )


@pytest.mark.parametrize("version", [0, 1])
@pytest.mark.parametrize("precomputed", [False, True])
def test_retained_reader_accepts_exact_variant_and_preserves_input(version, precomputed):
    function = _reader_function("snapshot_schema", version)
    if precomputed:
        body = function.pop("prosrc").replace('"snapshot_schema".', '"__schema__".')
        function["body_sha256"] = hashlib.sha256(reader._normalized(body).encode()).hexdigest()
    before = deepcopy(function)
    reader._require_function(function, "snapshot_schema", 100)
    assert function == before


@pytest.mark.parametrize("fault", ["body", "owner", "security", "settings", "volatility", "language"])
def test_typed_reader_variant_keeps_every_other_native_check(fault):
    function = _reader_function("snapshot_schema", 1)
    if fault == "body":
        function["prosrc"] += "\n-- unapproved"
    else:
        key, value = {
            "owner": ("proowner", 999),
            "security": ("prosecdef", False),
            "settings": ("proconfig", ["search_path=pg_catalog,public"]),
            "volatility": ("provolatile", "s"),
            "language": ("lanname", "sql"),
        }[fault]
        function[key] = value
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(function, "snapshot_schema", 100)
