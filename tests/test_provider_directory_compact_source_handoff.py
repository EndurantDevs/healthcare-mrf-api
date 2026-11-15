# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact handoffs preserve observations and ordinary semantic validation."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_completion as compact_completion
from process import provider_directory_compact_source_storage as layout
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from tests.test_provider_directory_generic_source_storage import harness as harness
from tests.test_provider_directory_named_source_transition import _named_prepared

pytestmark = pytest.mark.asyncio


def _compact_binding(binding):
    binding.update(contract=storage.NAMED_COMPACT_CONTRACT, payload_format=compact.FORMAT)
    prefix = binding["tables"]["resource_input"]["name"].removesuffix("_ri")
    for root, suffix in {**layout.TABLE_SUFFIXES, "resource_output_aliases": "oa"}.items():
        binding["tables"][root] = {"name": prefix + "_" + suffix}
    return binding


async def test_compact_handoff_retains_inputs_and_skips_heap_operations_on_views(harness, monkeypatch):
    async with _named_prepared(harness, monkeypatch) as (publisher, binding):
        _compact_binding(binding)
        original_tables_by_root = dict(binding["tables"])
        handoff = await completion.prepare_generic_source_handoff(publisher, binding)
        document = handoff.document
        assert document["phase"] == "ready_for_freeze"
        assert document["payload_format"] == compact.FORMAT
        assert document["tables"].items() >= original_tables_by_root.items()
        assert set(document["tables"]) == set(original_tables_by_root) | set(storage.HANDOFF_CONTROL_ROOTS)
        assert all(set(table) == {"name"} for table in document["tables"].values())
        assert not any(statement.startswith("DROP ") for statement in harness.sql)
        for root in ("resource_input", storage.RESOURCE):
            name = document["tables"][root]["name"]
            assert not any(
                statement.startswith("ALTER TABLE") and f'"{name}"' in statement for statement in harness.sql
            )
        for root in ("resource_aliases", "resource_reference_sets", "resource_body_blocks", "resource_output_aliases"):
            name = document["tables"][root]["name"]
            assert any(name in statement and "RESET (autovacuum_enabled)" in statement for statement in harness.sql)
        previous_statements = list(harness.sql)
        replay = await completion.prepare_generic_source_handoff(publisher, document)
        assert replay.document == document and replay.document_sha256 == handoff.document_sha256
        assert harness.sql == previous_statements
        storage._capture.assert_not_awaited()


async def test_compact_frozen_handoff_checks_data_without_physical_identity(harness, monkeypatch):
    async with _named_prepared(harness, monkeypatch) as (publisher, binding):
        _compact_binding(binding)
        handoff = await completion.prepare_generic_source_handoff(publisher, binding)
    driver = SimpleNamespace(
        is_in_transaction=lambda: True, execute=AsyncMock(), fetchval=AsyncMock(return_value=False)
    )
    inventory = AsyncMock(side_effect=AssertionError("named data does not have a physical inventory guard"))
    monkeypatch.setattr(completion, "_frozen_source_inventory", inventory)
    monkeypatch.setattr(completion, "_frozen_handoff_header", AsyncMock(return_value=harness.header))
    stored = SimpleNamespace(
        dataset_hash="a" * 64,
        resource_count=2,
        resource_hashes={"PractitionerRole": "b" * 64},
        resource_counts={"PractitionerRole": 2},
        metadata={"proof_sha256": "c" * 64},
    )
    proof = AsyncMock(return_value=stored)
    monkeypatch.setattr(completion, "_frozen_content_proof", proof)
    for name in ("_relationships", "_every_row", "_frozen_projection_exact"):
        monkeypatch.setattr(completion, name, AsyncMock())
    structural = AsyncMock()
    selected = AsyncMock()
    monkeypatch.setattr(compact_completion, "validate_storage", structural)
    monkeypatch.setattr(compact_completion, "validate_output_structure", selected)
    validation = await completion.validate_frozen_source_handoff(
        driver, harness.directory, handoff, protected_owner_role=None
    )
    assert validation["resource_count"] == 2
    assert structural.await_args.args == selected.await_args.args
    assert structural.await_args.args[2] == handoff.document
    proof.assert_awaited_once()
    inventory.assert_not_awaited()
    selected.side_effect = ValueError("invalid selected observation")
    proof.reset_mock()
    with pytest.raises(ValueError, match="invalid selected observation"):
        await completion.validate_frozen_source_handoff(driver, harness.directory, handoff, protected_owner_role=None)
    proof.assert_not_awaited()


async def test_compact_handoff_without_original_publisher_refuses_before_database():
    with pytest.raises(ValueError, match="original_storage_publisher_required"):
        await completion.prepare_generic_source_handoff(None, {"contract": storage.NAMED_COMPACT_CONTRACT})
