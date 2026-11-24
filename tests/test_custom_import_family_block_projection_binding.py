# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Block projection binding keeps canonical data validation without physical identity."""

from dataclasses import fields
from types import SimpleNamespace

import pytest

from process.custom_import.family_block_binding import (
    FamilyBlockUnavailable,
    check_projection_manifest,
    checked_binding,
    resolve_pinned_family_block_binding,
)
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from process.custom_import.read_identity import GenerationSnapshotBinding
from tests.test_custom_import_query_projection_storage import _definition


def _binding(definition=None):
    definition = definition or _definition()
    return checked_binding(
        23001, definition, {name: slot for slot, name in enumerate(definition.collections_by_name, 1)}
    )


def test_binding_contains_only_logical_layout_and_qualified_names():
    binding = _binding()
    assert {field.name for field in fields(binding)} == {"family_id", "layout", "tables_by_role"}
    assert all(table.schema == "ci_snapshot_23001" for table in binding.tables_by_role.values())
    with pytest.raises(TypeError):
        binding.tables_by_role["family"] = None
    manifest = SimpleNamespace(family_id=23001, definition_sha256=bytes.fromhex(binding.layout.definition.digest))
    check_projection_manifest(binding, manifest)


@pytest.mark.parametrize("value", [True, 0, -1, 2**63, None])
def test_family_identity_is_a_valid_logical_key(value):
    with pytest.raises(FamilyBlockUnavailable):
        checked_binding(value, _binding().layout.definition, _binding().layout.collection_slots)


@pytest.mark.parametrize("fault", ["family", "definition", "missing_definition", "invalid_digest"])
def test_projection_keeps_logical_family_and_canonical_definition_checks(fault):
    binding = _binding()
    manifest = SimpleNamespace(family_id=23001, definition_sha256=bytes.fromhex(binding.layout.definition.digest))
    if fault == "family":
        manifest.family_id = 23002
    elif fault == "definition":
        manifest.definition_sha256 = b"x" * 32
    elif fault == "invalid_digest":
        manifest.definition_sha256 = b"short"
    else:
        del manifest.definition_sha256
    with pytest.raises(QueryProjectionUnavailable):
        check_projection_manifest(binding, manifest)


async def test_pinned_copy_resolves_without_database_or_connection_metadata():
    binding = _binding()
    pin = GenerationSnapshotBinding(23001, False, "copied snapshot")
    for session in (object(), object()):
        actual = await resolve_pinned_family_block_binding(
            session, pin, binding.layout.definition, binding.layout.collection_slots
        )
        assert all(table.schema == "copied snapshot" for table in actual.tables_by_role.values())
        assert actual.layout == binding.layout
        check_projection_manifest(
            actual, SimpleNamespace(family_id=23001, definition_sha256=bytes.fromhex(binding.layout.definition.digest))
        )


@pytest.mark.parametrize(
    "pin",
    [None, GenerationSnapshotBinding(None, False, None), GenerationSnapshotBinding(23001, True, "ci_snapshot_23001")],
)
async def test_legacy_or_missing_pin_cannot_select_block_storage(pin):
    binding = _binding()
    with pytest.raises(FamilyBlockUnavailable):
        await resolve_pinned_family_block_binding(
            object(), pin, binding.layout.definition, binding.layout.collection_slots
        )
