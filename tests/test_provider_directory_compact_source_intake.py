# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed compact loading bindings reuse existing source admission and controls."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import native_copy_cleanup as copy_cleanup
from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_source_storage as storage
from tests.test_provider_directory_generic_source_policy import _named_factory
from tests.test_provider_directory_generic_source_storage import (
    _bind_named_publisher,
    _named_candidate_context,
    _rows,
)
from tests.test_provider_directory_generic_source_storage import (
    harness as harness,
)


def ordinary_metadata(harness):
    directory = storage.import_module("process.provider_directory_fhir")
    metadata = harness.header["publication_metadata_json"]
    metadata.update(
        source_ids=[directory._stable_source_id(directory.anthem.seed_rows()[0])],
        selected_resources=list(directory.anthem.RESOURCES),
        expected_resources=list(directory.anthem.RESOURCES),
    )
    harness.candidate.selected_resources = directory.anthem.RESOURCES
    return metadata


async def prepare(harness, database, **kwargs):
    return await storage.prepare_generic_source_candidate(database, harness.directory, harness.candidate, **kwargs)


def data_sql(harness):
    return [statement for statement in harness.sql if not statement.startswith(("SET LOCAL ROLE", "RESET ROLE"))]


@pytest.mark.asyncio
async def test_compact_prepare_resumes_only_its_persisted_explicit_format(harness, monkeypatch):
    factory = _named_factory()
    database, completion, _savepoints, capture = _named_candidate_context(harness, monkeypatch, factory, "temporary")
    ordinary_metadata(harness)
    async with _bind_named_publisher(harness, factory, AsyncMock()):
        binding = await prepare(harness, database, payload_format=compact.FORMAT)
        assert binding["contract"] == storage.NAMED_COMPACT_CONTRACT
        assert binding["payload_format"] == compact.FORMAT
        assert set(binding["tables"]) == set(storage._COMPACT_SUFFIXES)
        assert all(
            set(table) == {"schema", "name"} and table["schema"] == binding["storage"]["storage_schema"]
            for table in binding["tables"].values()
        )
        before = data_sql(harness)
        assert await prepare(harness, database) == binding
        assert await prepare(harness, database, payload_format=compact.FORMAT) == binding
        replay_statements = data_sql(harness)
        assert replay_statements[: len(before)] == before
        expected_reads = [
            f'SELECT 1 FROM "{table["schema"]}"."{table["name"]}" LIMIT 0' for table in binding["tables"].values()
        ]
        assert sorted(replay_statements[len(before) :]) == sorted(expected_reads * 2)
        with pytest.raises(ValueError, match="payload_format_changed"):
            await prepare(harness, database, payload_format="different")
        with pytest.raises(ValueError, match="original_storage_publisher_required"):
            await completion.prepare_generic_source_handoff(None, binding)
        assert harness.header["candidate_storage_json"] == binding and data_sql(harness) == replay_statements
    capture.assert_not_awaited()
    assert any(statement.startswith("CREATE VIEW") for statement in harness.sql)
    assert set(binding["storage"]) == {"catalog_schema", "storage_schema"}


@pytest.mark.asyncio
async def test_old_named_format_is_unchanged_and_cannot_hot_switch(harness, monkeypatch):
    factory = _named_factory()
    database, _completion, _savepoints, _capture = _named_candidate_context(harness, monkeypatch, factory, "temporary")
    ordinary_metadata(harness)
    async with _bind_named_publisher(harness, factory, AsyncMock()):
        binding = await prepare(harness, database)
        assert binding["contract"] == storage.NAMED_ISOLATED_CONTRACT
        assert "payload_format" not in binding
        with pytest.raises(ValueError, match="payload_format_changed"):
            await prepare(harness, database, payload_format=compact.FORMAT)
        assert harness.header["candidate_storage_json"] == binding


@pytest.mark.asyncio
@pytest.mark.parametrize("source_id", [storage.VSP_SOURCE_ID, "cms-npd", "different"])
async def test_compact_wrong_source_refuses_before_creation(harness, monkeypatch, source_id):
    factory = _named_factory()
    database, _completion, _savepoints, _capture = _named_candidate_context(harness, monkeypatch, factory, source_id)
    metadata = ordinary_metadata(harness)
    metadata["source_ids"] = [source_id]
    async with _bind_named_publisher(harness, factory, AsyncMock()):
        with pytest.raises(ValueError, match="unsupported|isolated_cms_release_invalid"):
            await prepare(harness, database, payload_format=compact.FORMAT)
    assert data_sql(harness) == [] and harness.header["candidate_storage_json"] is None


@pytest.mark.asyncio
async def test_compact_binding_and_witnesses_are_closed_before_copy(harness, monkeypatch):
    factory = _named_factory()
    database, _completion, _savepoints, _capture = _named_candidate_context(harness, monkeypatch, factory, "temporary")
    ordinary_metadata(harness)
    async with _bind_named_publisher(harness, factory, AsyncMock()):
        binding = await prepare(harness, database, payload_format=compact.FORMAT)
        before = deepcopy(binding)
        writes = data_sql(harness)
        with pytest.raises(ValueError, match="compact_witness_unsupported"):
            await storage.append_generic_source_batch(
                database,
                harness.directory,
                harness.candidate,
                _rows(1),
                [{"dataset_id": "dataset", "resource_type": "PractitionerRole"}],
            )
        assert harness.header["candidate_storage_json"] == before
        assert all(
            statement.startswith("SELECT pg_advisory_xact_lock") for statement in data_sql(harness)[len(writes) :]
        )
        for damage in ("payload_format", "physical", "extra", "missing", "wrong-schema", "unknown-field"):
            malformed = damaged_binding(binding, damage)
            with pytest.raises(ValueError):
                storage._validate_binding(malformed, binding["identity"], phases=("loading",))
    harness.connection.copy_records_to_table.assert_not_awaited()


def damaged_binding(binding, damage):
    malformed = deepcopy(binding)
    if damage == "payload_format":
        malformed["payload_format"] = "different"
    if damage == "physical":
        malformed["tables"]["resource_aliases"]["oid"] = 1
    if damage == "extra":
        malformed["tables"]["unexpected"] = {"name": "extra"}
    if damage == "wrong-schema":
        malformed["tables"]["resource_aliases"]["schema"] = "other_schema"
    if damage == "unknown-field":
        malformed["tables"]["resource_aliases"]["unexpected"] = "value"
    if damage == "missing":
        malformed["tables"].pop("resource_body_blocks")
    return malformed


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [asyncio.CancelledError, TimeoutError])
async def test_compact_copy_uses_existing_cancel_cleanup(monkeypatch, failure):
    events = []
    monkeypatch.setattr(compact, "append_compact_payload_batch", AsyncMock(side_effect=failure))
    monkeypatch.setattr(
        copy_cleanup, "close_interrupted_copy", AsyncMock(side_effect=lambda _c: events.append("close"))
    )
    monkeypatch.setattr(
        copy_cleanup, "invalidate_copy_owner", AsyncMock(side_effect=lambda *_args: events.append("invalidate"))
    )
    monkeypatch.setattr(storage, "source_copy_session", lambda: object())
    from types import SimpleNamespace

    tables_by_root = {
        root: {"name": "pdgs_" + "a" * 32 + "_" + suffix} for root, suffix in storage._COMPACT_SUFFIXES.items()
    }
    binding_by_field = {
        "tables": tables_by_root,
        "identity": {"dataset_id": "dataset"},
        "payload_format": compact.FORMAT,
    }
    with pytest.raises(failure):
        await storage._append_compact_resources(
            object(), SimpleNamespace(schema="source"), binding_by_field, _rows(1), 1
        )
    assert events == ["close", "invalidate"]
