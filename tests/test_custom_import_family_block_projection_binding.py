# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Manifest custody uses observed native signatures, not inferred model hashes."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import query_projection_evidence as reads
from process.custom_import import query_projection_finality as finality
from process.custom_import.family_block_binding import (
    FamilyBlockUnavailable,
    check_projection_manifest,
    checked_binding,
)
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.query_projection_binding import QueryProjectionUnavailable
from tests.test_custom_import_family_block_storage import _catalog
from tests.test_custom_import_query_projection_evidence import _fixture_digest, _generation, _manifest, _seal
from tests.test_custom_import_query_projection_storage import _definition


def _binding(definition=None):
    definition = definition or _definition()
    layout = FamilyBlockLayout(definition, {name: slot for slot, name in enumerate(definition.collections_by_name, 1)})
    return checked_binding(
        23, definition, layout.collection_slots, layout.comment, _catalog(layout), persistence="logged"
    )


def _custody(binding):
    return {
        **{f"{scope}_table_oid": binding.oids_by_role["query_" + scope] for scope in ("root", "child")},
        **{f"{scope}_table_owner": binding.owner_oid for scope in ("root", "child")},
        **{f"{scope}_columns_sha256": binding.columns_sha256_by_role["query_" + scope] for scope in ("root", "child")},
    }


def test_catalog_signatures_are_required_and_copied_immutably():
    binding = _binding()
    catalog = _catalog(binding.layout)
    original = bytearray(b"o" * 32)
    next(native for native in catalog if native["role_name"] == "query_root")["columns_sha256"] = original
    observed = checked_binding(
        23,
        binding.layout.definition,
        binding.layout.collection_slots,
        binding.layout.comment,
        catalog,
        persistence="candidate",
    )
    original[:] = b"x" * 32
    assert observed.columns_sha256_by_role["query_root"] == b"o" * 32
    assert set(observed.columns_sha256_by_role) == set(observed.tables_by_role)
    with pytest.raises(TypeError):
        observed.columns_sha256_by_role["query_root"] = b"y" * 32


@pytest.mark.parametrize("signature", [None, b"short", "x" * 32, 32, []])
def test_catalog_refuses_missing_or_malformed_native_signatures(signature):
    binding = _binding()
    catalog = _catalog(binding.layout)
    catalog[-1]["columns_sha256"] = signature
    with pytest.raises(FamilyBlockUnavailable, match="custody"):
        checked_binding(
            23,
            binding.layout.definition,
            binding.layout.collection_slots,
            binding.layout.comment,
            catalog,
            persistence="logged",
        )
    del catalog[-1]["columns_sha256"]
    with pytest.raises(FamilyBlockUnavailable, match="custody"):
        checked_binding(
            23,
            binding.layout.definition,
            binding.layout.collection_slots,
            binding.layout.comment,
            catalog,
            persistence="logged",
        )


@pytest.mark.parametrize("scope", ["root", "child"])
@pytest.mark.parametrize("suffix", ["table_oid", "table_owner", "columns_sha256"])
@pytest.mark.parametrize("damage", ["missing", "wrong", "invalid"])
def test_each_manifest_custody_cell_must_match_observation(scope, suffix, damage):
    binding = _binding()
    manifest = SimpleNamespace(family_id=binding.family_id, **_custody(binding))
    field = scope + "_" + suffix
    if damage == "missing":
        delattr(manifest, field)
    elif damage == "wrong":
        setattr(manifest, field, b"x" * 32 if suffix == "columns_sha256" else getattr(manifest, field) + 1)
    else:
        setattr(manifest, field, None if suffix == "columns_sha256" else True)
    with pytest.raises(QueryProjectionUnavailable):
        check_projection_manifest(binding, manifest)


@pytest.mark.parametrize("fault", ["signatures", "signature_key", "oids", "oid_key", "owner", "family", "repeated_oid"])
def test_partial_or_default_binding_cannot_qualify_a_manifest(fault):
    binding = _binding()
    manifest = SimpleNamespace(family_id=binding.family_id, **_custody(binding))
    changes_by_fault = {
        "signatures": {"columns_sha256_by_role": None},
        "signature_key": {"columns_sha256_by_role": {}},
        "oids": {"oids_by_role": None},
        "oid_key": {"oids_by_role": {}},
        "owner": {"owner_oid": True},
        "family": {"family_id": True},
        "repeated_oid": {"oids_by_role": dict.fromkeys(("query_root", "query_child"), manifest.root_table_oid)},
    }
    if fault == "repeated_oid":
        manifest.child_table_oid = manifest.root_table_oid
    with pytest.raises(QueryProjectionUnavailable):
        check_projection_manifest(replace(binding, **changes_by_fault[fault]), manifest)


@pytest.mark.parametrize("entry", ["read", "finality"])
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "root_table_oid",
        "child_table_oid",
        "root_table_owner",
        "child_table_owner",
        "root_columns_sha256",
        "child_columns_sha256",
        "missing_native_signatures",
    ],
)
async def test_both_block_dispatches_require_custody_without_extra_queries(monkeypatch, entry, fault):
    binding = _binding()
    definition = binding.layout.definition
    evidence = _fixture_digest(definition).finish()
    generation = _generation()
    manifest = _manifest(definition, evidence, generation, **_custody(binding))
    seal = _seal(evidence, generation)
    if fault == "missing_native_signatures":
        binding = replace(binding, columns_sha256_by_role=None)
    elif fault is not None:
        setattr(manifest, fault, b"x" * 32 if fault.endswith("sha256") else getattr(manifest, fault) + 1)
    native = SimpleNamespace(one_or_none=lambda: (generation, seal, manifest))
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=native),
        get=AsyncMock(return_value=seal),
        scalars=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: manifest)),
    )
    legacy = AsyncMock(side_effect=AssertionError("block custody must use existing native observations"))
    monkeypatch.setattr(reads, "lock_projection_tables", legacy)
    monkeypatch.setattr(finality, "lock_projection_tables", legacy)
    if entry == "read":
        operation = reads.read_projection_tables(
            session, generation, definition, snapshot_family_id=23, family_block_binding=binding
        )
    else:
        operation = finality.load_generation_evidence(session, generation, definition, family_block_binding=binding)
    if fault is None:
        actual = await operation
        assert actual == evidence if entry == "finality" else len(actual) == 2
    else:
        with pytest.raises(QueryProjectionUnavailable):
            await operation
    legacy.assert_not_awaited()
    assert session.execute.await_count == (1 if entry == "read" else 0)
    assert session.get.await_count == session.scalars.await_count == (1 if entry == "finality" else 0)
