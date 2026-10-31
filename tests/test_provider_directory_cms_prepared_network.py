# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared network batches retain the native encoder and private targets."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from process import provider_directory_cms_prepared_network as network
from process import provider_directory_insurance_network_batch as batch
from process.provider_directory_cms_identity_candidate import CMS_IDENTITY_TABLES
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace


@pytest.fixture
def prepared(monkeypatch):
    workspace = object.__new__(CMSPreparedWorkspace)
    workspace.policy = SimpleNamespace(schema="private_source")
    workspace.source_binding = SimpleNamespace(catalog_schema="accepted_catalog")
    workspace._binding = {"identity": {"metadata": {"source_release": {"vector_sha256": "a" * 64}}}}
    workspace._indexed = False
    workspace._receipts = {root: {"name": "private_map_" + str(i)} for i, root in enumerate(CMS_IDENTITY_TABLES)}
    connection = SimpleNamespace(dialect=postgresql.dialect())
    workspace.connection = object()
    workspace.session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    workspace.verify_current = AsyncMock()
    events = []

    @asynccontextmanager
    async def savepoint(driver):
        assert driver is workspace.connection
        events.append("entered")
        try:
            yield
        except BaseException:
            events.append("rolled_back")
            raise
        else:
            events.append("released")

    encoded = (b"native batch", 1, 1, 0)
    encoded_inputs = []

    def encode(payload):
        encoded_inputs.append(json.loads(payload))
        return encoded

    writer = AsyncMock(return_value={"new_identity_count": 1})
    monkeypatch.setattr(batch, "_encode", encode)
    monkeypatch.setattr(batch, "write_network_batch_native", writer)
    monkeypatch.setattr(network, "_savepoint", savepoint)
    return SimpleNamespace(
        workspace=workspace,
        connection=connection,
        encoded=encoded,
        encoded_inputs=encoded_inputs,
        writer=writer,
        events=events,
    )


@pytest.mark.asyncio
async def test_prepared_network_uses_private_targets_and_original_session(prepared):
    resources = [{"resourceType": "Organization", "id": "network-one", "type": [{"text": "ntwk"}]}]
    assert await network.record_prepared_network_batch(
        prepared.workspace, resource_type="Organization", resources=resources
    ) == {"new_identity_count": 1}
    assert prepared.encoded_inputs == [
        {"source_id": "cms-npd", "release_id": "a" * 64, "resource_type": "Organization", "resources": resources}
    ]
    connection, session, driver, names, encoded, parameters = prepared.writer.await_args.args
    assert connection is prepared.connection and session is prepared.workspace.session
    assert driver is prepared.workspace.connection and encoded is prepared.encoded
    assert parameters == {
        "source_id": "cms-npd",
        "release_id": "a" * 64,
        "resource_type": "Organization",
        "network_resource_id": None,
    }
    assert names["global_network_binding"].startswith('"accepted_catalog".')
    assert all(
        names[key].startswith('"private_source".')
        for key in ("entity_binding", "entity_evidence", "network_binding", "plan_evidence")
    )
    assert prepared.workspace.verify_current.await_count == 3
    assert prepared.events == ["entered", "released"]


@pytest.mark.asyncio
async def test_replaced_workspace_refuses_before_encoding(prepared):
    with pytest.raises(ValueError, match="workspace_required"):
        await network.record_prepared_network_batch(object(), resource_type="Organization", resources=[{}])
    assert not prepared.encoded_inputs and not prepared.events


@pytest.mark.asyncio
async def test_indexed_workspace_refuses_before_encoding_or_sql(prepared):
    prepared.workspace._indexed = True
    with pytest.raises(ValueError, match="already_indexed"):
        await network.record_prepared_network_batch(prepared.workspace, resource_type="Organization", resources=[{}])
    assert not prepared.encoded_inputs and not prepared.events
    prepared.writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_network_rechecks_after_encoding_before_sql(prepared):
    prepared.workspace.verify_current.side_effect = [None, ValueError("output_changed")]
    with pytest.raises(ValueError, match="output_changed"):
        await network.record_prepared_network_batch(prepared.workspace, resource_type="Organization", resources=[{}])
    prepared.writer.assert_not_awaited()
    assert not prepared.events


@pytest.mark.asyncio
async def test_partial_prepared_network_write_failure_propagates_through_rollback(prepared):
    error = ValueError("release_plan_conflict")
    prepared.writer.side_effect = error
    with pytest.raises(ValueError) as rejected:
        await network.record_prepared_network_batch(prepared.workspace, resource_type="InsurancePlan", resources=[{}])
    assert rejected.value is error
    assert prepared.events == ["entered", "rolled_back"]
