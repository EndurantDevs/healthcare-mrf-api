# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped binding drafts publish only with their exact approved network."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest

from process.network_source_binding_store import (
    NetworkSourceBindingBatchCommand,
    apply_network_source_binding_batch,
)
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict
from tests.test_registry_approval_store_postgres import (
    _actor,
    _approve,
    _command,
    _create,
    _draft,
    serving_schema,
)

pytestmark = pytest.mark.asyncio


def _binding(network_id):
    return {
        "binding_id": str(uuid4()),
        "source_system": "ptg",
        "source_id": "synthetic-source",
        "dataset_schema": "synthetic_dataset",
        "dataset_id": "dataset-one",
        "producer_id": "producer-one",
        "edition_id": "edition-one",
        "source_key": "source-network-one",
        "source_scope_json": {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"},
        "network_id": network_id,
        "evidence_id": "synthetic-review",
        "evidence_sha256": "b" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
    }


async def _bind(connection, schema, row, actor):
    command = NetworkSourceBindingBatchCommand(json.dumps([row]).encode(), "Reviewed exact source scope", uuid4().hex)
    async with connection.transaction():
        return await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)


@pytest.mark.parametrize("include_network", [False, True])
async def test_binding_preview_and_approval_require_the_selected_network(serving_schema, include_network):
    connection, schema, engine = serving_schema
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor)
    row = _binding(network["record_id"])
    binding = (await _bind(connection, schema, row, actor))["records"][0]
    selected = (network, binding) if include_network else (binding,)
    command = await _command(connection, schema, *selected)
    async with connection.transaction():
        preview = await preview_registry_approval(connection, command, actor, control_schema=schema)
    assert preview["source_binding_diagnostics"] == {"binding_rows": 1, "unresolved_count": int(not include_network)}
    assert preview["unresolved_count"] == int(not include_network)
    change = next(record for record in preview["records"] if record["record_kind"] == "network_binding")
    assert change["record_id"] == row["binding_id"] and change["after"]["network_id"] == network["record_id"]
    if include_network:
        receipt = await _approve(connection, schema, command, actor)
        assert receipt["selected_count"] == 2
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 2
    else:
        with pytest.raises(RegistryApprovalConflict, match="source_binding_unresolved"):
            await _approve(connection, schema, command, actor)
        assert tuple(
            await connection.fetchrow(
                f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control'
            )
        ) == (2, 0)
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0


async def test_network_archive_needs_the_binding_closure_in_the_same_approved_map(serving_schema):
    connection, schema, engine = serving_schema
    actor, creation = _actor(), _create("network")
    network = await _draft(engine, schema, creation, actor)
    binding_row = _binding(network["record_id"])
    binding = (await _bind(connection, schema, binding_row, actor))["records"][0]
    approved = await _approve(connection, schema, await _command(connection, schema, network, binding), actor)
    archived = await _draft(
        engine,
        schema,
        replace(
            creation,
            record_id=network["record_id"],
            allocation_key=None,
            operation="archive",
            expected_revision=1,
            fields={},
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    with pytest.raises(RegistryApprovalConflict, match="source_binding_unresolved"):
        await _approve(connection, schema, await _command(connection, schema, archived), actor)
    assert (
        await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
        == approved["approved_revision"]
    )
    closed = (
        await _bind(
            connection,
            schema,
            {
                **binding_row,
                "operation": "close",
                "expected_revision": 1,
                "expected_network_id": binding_row["network_id"],
            },
            actor,
        )
    )["records"][0]
    assert closed["archived"] is True
    receipt = await _approve(connection, schema, await _command(connection, schema, archived, closed), actor)
    assert receipt["selected_count"] == 2
    retained = await connection.fetchval(
        f"SELECT record_json->'archived' FROM \"{schema}\".registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network_binding'",
        receipt["approved_revision"],
    )
    assert json.loads(retained) is True
