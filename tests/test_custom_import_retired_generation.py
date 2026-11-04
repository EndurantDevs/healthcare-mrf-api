# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Availability checks distinguish retired storage from unrelated database failures."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from process.custom_import import (
    operator,
    publication,
    query_projection_finality,
    read_identity,
)
from process.custom_import.read_contracts import CustomImportReadUnavailableError
from tests.test_custom_import_provider_query import _target
from tests.test_custom_import_snapshot_reads import _resolver_session


def _database_failure(code, message):
    driver = Exception()
    driver.sqlstate = code
    driver.message = message
    adapter = Exception()
    adapter.__cause__ = driver
    return DBAPIError("synthetic resolver", {}, adapter)


@pytest.mark.parametrize(
    ("code", "message", "retired"),
    (
        ("55000", "custom_import_snapshot_retired", True),
        ("55000", "unrelated prerequisite", False),
        ("42501", "custom_import_snapshot_retired", False),
        ("P0001", "custom_import_snapshot_read_generation_mismatch", False),
    ),
)
async def test_resolver_maps_only_exact_native_retirement(code, message, retired):
    failure = _database_failure(code, message)
    session = _resolver_session(None)
    session.execute.side_effect = failure
    with pytest.raises(CustomImportReadUnavailableError if retired else DBAPIError) as caught:
        await read_identity.resolve_generation_snapshot(session, _target())
    assert caught.value.__cause__ is failure if retired else caught.value is failure


async def test_seal_replay_checks_storage_before_projection_evidence(monkeypatch):
    failure = CustomImportReadUnavailableError("pinned generation storage is retired")
    monkeypatch.setattr(read_identity, "verify_generation_storage", AsyncMock(side_effect=failure))
    projection = AsyncMock()
    monkeypatch.setattr(query_projection_finality, "validate_sealed_projection", projection)
    with pytest.raises(publication.PublicationConflict, match="storage is retired") as caught:
        await publication._validate_sealed_projection(object(), object(), object())
    assert caught.value.__cause__ is failure
    projection.assert_not_awaited()


@pytest.mark.parametrize(
    ("code", "message", "accepted"),
    (
        ("P0001", "custom_import_snapshot_read_generation_mismatch", True),
        ("P0001", "custom_import_snapshot_read_binding_mismatch", False),
        ("42501", "permission denied", False),
    ),
)
async def test_unsealed_history_rolls_back_only_its_expected_probe_failure(code, message, accepted):
    events = []

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise

    identity = SimpleNamespace(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    session = _resolver_session(None)
    session.begin_nested = savepoint
    failure = _database_failure(code, message)
    session.execute.side_effect = failure
    if accepted:
        await operator._pin_generation_storage(session, identity, 4, sealed=False)
    else:
        with pytest.raises(DBAPIError) as caught:
            await operator._pin_generation_storage(session, identity, 4, sealed=False)
        assert caught.value is failure
    assert events == ["savepoint", "rollback"]
