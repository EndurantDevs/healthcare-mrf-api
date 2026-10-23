# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native registration, locking and rollback for generic source bindings."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import func, select

from db.models.custom_import import CustomImportDataset, CustomImportSourceBindingRevision
from process.custom_import import source_binding_store as store
from process.custom_import.definition_store import DefinitionRegistrationError
from tests.custom_import_prepared_source_migration_postgres_cases import _prepared_case
from tests.test_custom_import_snowflake_source_binding import _binding, _definition, _v2_binding
from tests.test_custom_import_source_binding_store import _object_binding


@pytest.mark.asyncio
@pytest.mark.parametrize("connector", ("https", "s3", "upload"))
async def test_native_object_registration_loads_and_replays_exact_immutable_identity(connector):
    definition = _definition()
    binding = _object_binding(definition, connector)
    async with _prepared_case() as case:
        async with case.sessions() as session, session.begin():
            first = await store.register_source_binding(
                session, dataset_key="synthetic_object_binding", definition=definition, binding=binding
            )
        async with case.sessions() as session, session.begin():
            loaded = await store.load_source_binding(
                session,
                definition_revision_id=first.definition_revision_id,
                source_binding_revision_id=first.source_binding_revision_id,
            )
            replay = await store.register_source_binding(
                session, dataset_key="synthetic_object_binding", definition=definition, binding=binding
            )
            assert first.created and replay == replace(first, created=False)
            assert loaded.binding == binding and loaded.definition == definition
            assert loaded.source_binding_sha256 == bytes.fromhex(binding.digest)
            row = await session.get(CustomImportSourceBindingRevision, first.source_binding_revision_id)
            assert row.canonical_binding == binding.canonical
            assert row.connector_kind == connector and row.binding_contract == binding.contract
            assert await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 1


@pytest.mark.asyncio
async def test_native_object_and_legacy_bindings_share_the_existing_revision_sequence():
    definition = _definition()
    bindings = (_binding(definition), _object_binding(definition), _v2_binding(definition))
    async with _prepared_case() as case:
        async with case.sessions() as session, session.begin():
            receipts = [
                await store.register_source_binding(
                    session, dataset_key="synthetic_mixed_binding", definition=definition, binding=binding
                )
                for binding in bindings
            ]
            assert [receipt.revision_number for receipt in receipts] == [1, 2, 3]
            for receipt, binding in zip(receipts, bindings, strict=True):
                loaded = await store.load_source_binding(
                    session,
                    definition_revision_id=receipt.definition_revision_id,
                    source_binding_revision_id=receipt.source_binding_revision_id,
                )
                assert loaded.binding == binding
                replay = await store.register_source_binding(
                    session, dataset_key="synthetic_mixed_binding", definition=definition, binding=binding
                )
                assert replay == replace(receipt, created=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("same_binding", (False, True))
async def test_native_concurrent_object_registration_serializes_append_and_replay(same_binding):
    definition = _definition()
    first_binding = _object_binding(definition)
    second_binding = (
        first_binding
        if same_binding
        else replace(first_binding, source_object={**first_binding.source_object, "version": "approved-v2"})
    )
    first_locked, second_started, release_first = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async with _prepared_case() as case:

        async def register_first():
            async with case.sessions() as session, session.begin():
                receipt = await store.register_source_binding(
                    session, dataset_key="synthetic_concurrent_binding", definition=definition, binding=first_binding
                )
                first_locked.set()
                await release_first.wait()
                return receipt

        async def register_second():
            await first_locked.wait()
            async with case.sessions() as session, session.begin():
                second_started.set()
                return await store.register_source_binding(
                    session, dataset_key="synthetic_concurrent_binding", definition=definition, binding=second_binding
                )

        first_task, second_task = asyncio.create_task(register_first()), asyncio.create_task(register_second())
        try:
            await asyncio.wait_for(second_started.wait(), timeout=5)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(second_task), timeout=0.1)
            release_first.set()
            first, second = await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=10)
        finally:
            release_first.set()
            for task in (first_task, second_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(first_task, second_task, return_exceptions=True)
        assert first.revision_number == 1 and first.created
        assert second.created is not same_binding
        assert second.revision_number == (1 if same_binding else 2)
        async with case.sessions() as session:
            loaded = await store.load_source_binding(
                session,
                definition_revision_id=second.definition_revision_id,
                source_binding_revision_id=second.source_binding_revision_id,
            )
            assert loaded.binding == second_binding
            assert (
                await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision))
                == second.revision_number
            )


@pytest.mark.asyncio
async def test_native_failed_object_readback_rolls_back_definition_and_binding(monkeypatch):
    definition = _definition()
    binding = _object_binding(definition)
    real_loader = store.load_source_binding

    async def changed_readback(*args, **kwargs):
        return replace(await real_loader(*args, **kwargs), dataset_id=999_999)

    async with _prepared_case() as case:
        with monkeypatch.context() as patch:
            patch.setattr(store, "load_source_binding", changed_readback)
            async with case.sessions() as session:
                with pytest.raises(store.ObjectSourceBindingUnavailableError, match="readback"):
                    await _register_rollback_binding(session, definition, binding)
        async with case.sessions() as session, session.begin():
            for model in (CustomImportDataset, CustomImportSourceBindingRevision):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            receipt = await store.register_source_binding(
                session, dataset_key="synthetic_rollback_binding", definition=definition, binding=binding
            )
            assert receipt.created and receipt.revision_number == 1


@pytest.mark.asyncio
async def test_native_object_registration_requires_the_existing_caller_transaction():
    definition = _definition()
    async with _prepared_case() as case:
        async with case.sessions() as session:
            with pytest.raises(DefinitionRegistrationError, match="active caller transaction"):
                await store.register_source_binding(
                    session,
                    dataset_key="synthetic_transaction_binding",
                    definition=definition,
                    binding=_object_binding(definition),
                )


async def _register_rollback_binding(session, definition, binding):
    async with session.begin():
        await store.register_source_binding(
            session, dataset_key="synthetic_rollback_binding", definition=definition, binding=binding
        )
