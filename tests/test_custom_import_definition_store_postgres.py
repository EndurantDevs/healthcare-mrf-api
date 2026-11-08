# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import copy
from pathlib import Path

import pytest
from sqlalchemy import select

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportField,
    CustomImportFieldAlias,
    CustomImportFieldSlot,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
)
from process.custom_import.definition import CustomImportDefinition, load_json_definition
from process.custom_import.definition_store import DefinitionRegistrationError, register_definition
from tests.custom_import_postgres_support import isolated_publication_case, transaction_session

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def _raw_definition() -> dict[str, object]:
    return copy.deepcopy(load_json_definition(_FIXTURE.read_text()))


def _revised_definition(previous: CustomImportDefinition) -> CustomImportDefinition:
    revised_document = _raw_definition()
    revised_document["revision"]["definition"] = 2
    revised_document["aliases"]["providers"]["Provider Identifier"] = "npi"
    return CustomImportDefinition.from_mapping(revised_document, previous=previous)


async def _row_count(session, model) -> int:
    return len((await session.execute(select(model))).scalars().all())


async def _counts(session) -> tuple[int, ...]:
    return tuple(
        [
            await _row_count(session, CustomImportDataset),
            await _row_count(session, CustomImportSchemaRevision),
            await _row_count(session, CustomImportFieldSlot),
            await _row_count(session, CustomImportChildCollection),
            await _row_count(session, CustomImportField),
            await _row_count(session, CustomImportDefinitionRevision),
            await _row_count(session, CustomImportSourceStream),
            await _row_count(session, CustomImportFieldAlias),
            await _row_count(session, CustomImportSelectionProfile),
        ]
    )


@pytest.mark.asyncio
async def test_definition_store_persists_the_immutable_graph_and_replays_exactly():
    first_definition = _definition()
    async with transaction_session() as session, session.begin():
        first = await register_definition(session, "synthetic_store", first_definition)
        replay = await register_definition(session, "synthetic_store", first_definition)

        assert first.created is True
        assert replay == first.__class__(
            dataset_id=first.dataset_id,
            definition_revision_id=first.definition_revision_id,
            schema_revision_id=first.schema_revision_id,
            created=False,
        )
        assert await _counts(session) == (1, 1, 5, 1, 5, 1, 2, 5, 1)

        source_streams = (await session.execute(select(CustomImportSourceStream))).scalars().all()
        assert {(stream.stream_id, stream.collection_slot) for stream in source_streams} == {
            ("providers", None),
            ("rates", 1),
        }

        second = await register_definition(session, "synthetic_store", _revised_definition(first_definition))

        assert second.created is True
        assert second.dataset_id == first.dataset_id
        assert second.schema_revision_id == first.schema_revision_id
        assert second.definition_revision_id != first.definition_revision_id
        assert await _counts(session) == (1, 1, 5, 1, 5, 2, 4, 11, 2)

        expected_counts = await _counts(session)
        content_drift = _raw_definition()
        content_drift["refresh_mode"] = "snapshot"
        with pytest.raises(DefinitionRegistrationError, match="different content"):
            await register_definition(session, "synthetic_store", CustomImportDefinition.from_mapping(content_drift))

        revision_drift = _raw_definition()
        revision_drift["revision"] = {"definition": 3, "schema": 2}
        with pytest.raises(DefinitionRegistrationError, match="content is already bound"):
            await register_definition(session, "synthetic_store", CustomImportDefinition.from_mapping(revision_drift))

        key_drift = _raw_definition()
        key_drift["revision"] = {"definition": 3, "schema": 2}
        key_drift["schema"]["root"]["fields"][1]["id"] = "provider_name"
        key_drift["aliases"]["providers"]["Provider Name"] = "provider_name"
        key_drift["query"]["root_fields"][1] = "provider_name"
        with pytest.raises(DefinitionRegistrationError, match="transition is invalid"):
            await register_definition(session, "synthetic_store", CustomImportDefinition.from_mapping(key_drift))

        assert await _counts(session) == expected_counts


@pytest.mark.asyncio
async def test_definition_store_serializes_concurrent_registration():
    first_definition = _definition()
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            await register_definition(session, "synthetic_store", first_definition)

        definition = _revised_definition(first_definition)
        first_locked = asyncio.Event()
        release_first = asyncio.Event()
        second_started = asyncio.Event()

        async def first_registration():
            async with case.sessions() as session, session.begin():
                registration = await register_definition(session, "synthetic_store", definition)
                first_locked.set()
                await release_first.wait()
                return registration

        async def second_registration():
            await first_locked.wait()
            async with case.sessions() as session, session.begin():
                second_started.set()
                return await register_definition(session, "synthetic_store", definition)

        first_task = asyncio.create_task(first_registration())
        second_task = asyncio.create_task(second_registration())
        try:
            await asyncio.wait_for(second_started.wait(), timeout=2)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(second_task), timeout=0.1)
        except BaseException:
            release_first.set()
            first_task.cancel()
            second_task.cancel()
            await asyncio.gather(first_task, second_task, return_exceptions=True)
            raise

        release_first.set()
        first, second = await asyncio.gather(first_task, second_task)

        assert first.created is True
        assert second == first.__class__(
            dataset_id=first.dataset_id,
            definition_revision_id=first.definition_revision_id,
            schema_revision_id=first.schema_revision_id,
            created=False,
        )


@pytest.mark.asyncio
async def test_definition_store_rejects_drift_in_a_reused_schema_graph():
    first_definition = _definition()
    async with transaction_session() as session, session.begin():
        first = await register_definition(session, "synthetic_store", first_definition)
        original = (
            await session.execute(
                select(CustomImportChildCollection).where(
                    CustomImportChildCollection.schema_revision_id == first.schema_revision_id
                )
            )
        ).scalar_one()
        session.add(
            CustomImportChildCollection(
                schema_revision_id=first.schema_revision_id,
                dataset_id=first.dataset_id,
                collection_slot=2,
                collection_name="unexpected_collection",
                canonical_key_shape=original.canonical_key_shape,
                key_shape_sha256=bytes(original.key_shape_sha256),
            )
        )
        await session.flush()

        with pytest.raises(DefinitionRegistrationError, match="persisted schema graph"):
            await register_definition(session, "synthetic_store", _revised_definition(first_definition))


@pytest.mark.asyncio
async def test_definition_store_replay_rejects_appended_descendant():
    definition = _definition()
    async with transaction_session() as session, session.begin():
        first = await register_definition(session, "synthetic_store", definition)
        session.add(
            CustomImportSourceStream(
                definition_revision_id=first.definition_revision_id,
                dataset_id=first.dataset_id,
                schema_revision_id=first.schema_revision_id,
                stream_slot=3,
                stream_id="unexpected_stream",
                record_kind="root",
                collection_slot=None,
                decoder="csv",
                compression="none",
                snapshot_token_selector="snapshot_token",
                record_path=None,
            )
        )
        await session.flush()

        with pytest.raises(DefinitionRegistrationError, match="persisted definition graph"):
            await register_definition(session, "synthetic_store", definition)
