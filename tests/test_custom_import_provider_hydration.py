# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native pages hydrate only the exact query's bounded selected winners."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import read_core
from process.custom_import.read_contracts import (
    CustomImportReadAuthorizationError,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from tests import test_custom_import_provider_query as query_fixture


@pytest.mark.parametrize(
    "entities", [[], ("1000000001",) * 2, ("1",), (True,), ("１" * 10,), tuple(str(1000000000 + n) for n in range(201))]
)
def test_provider_hydration_rejects_unbounded_or_noncanonical_page(entities):
    with pytest.raises(CustomImportReadRequestError):
        read_core._validate_npi_page(entities)


def test_provider_hydration_accepts_full_native_page():
    read_core._validate_npi_page(tuple(str(1000000000 + n) for n in range(200)))
    read_core._validate_npi_page(())


@pytest.mark.asyncio
async def test_provider_hydration_authorizes_before_storage():
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(CustomImportReadAuthorizationError):
        await read_core.CustomImportReadService(authorizer=None).hydrate_npi_page(
            session,
            authorization=ExtensionReadAuthorization("synthetic"),
            pinned_target=query_fixture._target(),
            prepared=None,
            entity_values=(),
        )
    session.execute.assert_not_awaited()


def _install_context(monkeypatch):
    context = query_fixture._context()

    @asynccontextmanager
    async def bounded(session, *, timeout_ms):
        yield

    monkeypatch.setattr(read_core, "_bounded_read_window", bounded)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=context))
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    return context


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["query_fingerprint", "authorization_scope_sha256"])
async def test_provider_hydration_rejects_different_prepared_identity(monkeypatch, changed):
    context = _install_context(monkeypatch)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    session = SimpleNamespace(execute=AsyncMock())
    authorization = ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)
    with pytest.raises(CustomImportReadUnavailableError, match="query identity"):
        await service.hydrate_npi_page(
            session,
            authorization=authorization,
            pinned_target=context.target,
            prepared=replace(prepared, **{changed: "0" * 64}),
            entity_values=(),
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_hydration_rejects_prepared_target_reuse(monkeypatch):
    context = _install_context(monkeypatch)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    session = SimpleNamespace(execute=AsyncMock())
    authorization = ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)
    next_target = replace(context.target, generation_id=context.target.generation_id + 1)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=replace(context, target=next_target)))
    with pytest.raises(CustomImportReadUnavailableError, match="query identity"):
        await service.hydrate_npi_page(
            session, authorization=authorization, pinned_target=next_target, prepared=prepared, entity_values=()
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_hydration_selects_one_winner_per_npi_before_single_batch(monkeypatch):
    context = _install_context(monkeypatch)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    selected_values = (object(), object(), object(), object())
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [(*selected_values, "1000000001")]))
    )
    hydrated = object()
    batch = AsyncMock(return_value=(hydrated,))
    monkeypatch.setattr(read_core, "_hydrate_search_page_items", batch)
    authorization = ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)

    result = await service.hydrate_npi_page(
        session,
        authorization=authorization,
        pinned_target=context.target,
        prepared=prepared,
        entity_values=("1000000001", "1000000000"),
    )

    assert result == {"1000000001": hydrated}
    batch.assert_awaited_once_with(session, context, (selected_values,))
    statement = session.execute.await_args.args[0]
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "DISTINCT ON (" in compiled
    assert "canonical_value IN" in compiled
    assert "ORDER BY" in compiled and "root_record_id ASC" in compiled
    read_core.verify_published_generation.assert_awaited_once_with(session, context.target)
