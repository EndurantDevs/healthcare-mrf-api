# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native approved binding scopes remain separate from mutable review drafts."""

import json
from dataclasses import FrozenInstanceError, replace
from uuid import uuid4

import pytest

from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    ApprovedNetworkSourceBindingError,
    RegistryNetworkSourceCoordinates,
    require_approved_network_source_bindings,
)
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _CountedConnection, _create, _draft

pytestmark = pytest.mark.asyncio
COORDINATES = RegistryNetworkSourceCoordinates(
    "ptg", "synthetic-source", "synthetic_dataset", "dataset-one", "producer-one", "edition-one"
)


def _binding(network_id, index=1, **changes):
    return {
        "binding_id": str(uuid4()),
        "source_system": COORDINATES.source_system,
        "source_id": COORDINATES.source_id,
        "dataset_schema": COORDINATES.dataset_schema,
        "dataset_id": COORDINATES.dataset_id,
        "producer_id": COORDINATES.producer_id,
        "edition_id": COORDINATES.edition_id,
        "source_key": f"explicit-network-{index}",
        "source_scope_json": {
            "cohort_id": "cohort-one",
            "snapshot_id": "snapshot-one",
            "company_key": f"company-{index}",
        },
        "network_id": network_id,
        "evidence_id": "synthetic-review",
        "evidence_sha256": "a" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
        **changes,
    }


async def _apply(connection, schema, actor, bindings):
    command = NetworkSourceBindingBatchCommand(json.dumps(bindings).encode(), "Reviewed exact coordinates", uuid4().hex)
    async with connection.transaction():
        return await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)


async def _pin(connection, schema):
    revision = await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
    async with connection.transaction(isolation="repeatable_read"):
        return await pin_approved_membership_source(connection, approved_revision=revision, control_schema=schema)


async def _count(connection, schema, source, coordinates=COORDINATES):
    async with connection.transaction(isolation="repeatable_read"):
        return await require_approved_network_source_bindings(connection, source, coordinates, control_schema=schema)


async def _setup(fixture, count=1):
    connection, schema, engine = fixture
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor)
    bindings = [_binding(network["record_id"], index) for index in range(1, count + 1)]
    receipt = await _apply(connection, schema, actor, bindings)
    await _approve(connection, schema, await _command(connection, schema, network, *receipt["records"]), actor)
    return actor, network, bindings, await _pin(connection, schema)


async def _network_ids(connection, schema, source):
    fragment = APPROVED_NETWORK_BINDINGS_SQL.format(namespace='"' + schema + '"')
    return await connection.fetchval(
        "WITH " + fragment + " SELECT array_agg(network_id ORDER BY record_key) FROM approved_network_bindings",
        source.approved_revision,
        *COORDINATES.sql_parameters,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"source_system": "unknown"},
        {"source_system": True},
        {"source_id": " source"},
        {"source_id": "source\nname"},
        {"source_id": ""},
        {"source_id": "é" * 65},
        {"dataset_schema": "a.b"},
        {"dataset_schema": "1name"},
        {"dataset_schema": "x" * 64},
        {"dataset_schema": "é"},
        {"dataset_id": None},
        {"producer_id": "x" * 129},
        {"edition_id": "edition "},
        {"source_id": "\ud800"},
    ],
)
async def test_coordinates_reject_invalid_values(changes):
    with pytest.raises(ApprovedNetworkSourceBindingError, match="^registry_source_coordinates_invalid$"):
        replace(COORDINATES, **changes)


async def test_coordinates_are_exact_and_frozen():
    assert COORDINATES.sql_parameters == (
        "ptg",
        "synthetic-source",
        "synthetic_dataset",
        "dataset-one",
        "producer-one",
        "edition-one",
    )
    assert replace(COORDINATES, dataset_schema="Source_One").dataset_schema == "Source_One"
    with pytest.raises(FrozenInstanceError):
        COORDINATES.source_id = "changed"


async def test_real_approval_and_draft_isolation(serving_schema):
    connection, schema, _ = serving_schema
    actor, network, _, source = await _setup(serving_schema)
    assert await _count(connection, schema, source) == 1
    extra = await _apply(connection, schema, actor, [_binding(network["record_id"], 2)])
    assert await _count(connection, schema, source) == 1
    await _approve(connection, schema, await _command(connection, schema, *extra["records"]), actor)
    updated = await _pin(connection, schema)
    assert updated.generation_id != source.generation_id
    assert await _count(connection, schema, updated) == 2
    with pytest.raises(ApprovedNetworkSourceBindingError):
        await _count(connection, schema, source)


@pytest.mark.parametrize(
    "field,changed",
    [
        ("source_system", "aca"),
        ("source_id", "other-source"),
        ("dataset_schema", "other_schema"),
        ("dataset_id", "other-dataset"),
        ("producer_id", "other-producer"),
        ("edition_id", "other-edition"),
    ],
)
async def test_every_coordinate_is_independent(serving_schema, field, changed):
    connection, schema, _ = serving_schema
    _, _, _, source = await _setup(serving_schema)
    assert await _count(connection, schema, source, replace(COORDINATES, **{field: changed})) == 0


async def test_rebind_and_close_need_new_approval(serving_schema):
    connection, schema, engine = serving_schema
    actor, network, bindings, original = await _setup(serving_schema)
    replacement = await _draft(engine, schema, _create("network"), actor)
    rebinding_by_field = {
        **bindings[0],
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": network["record_id"],
        "network_id": replacement["record_id"],
    }
    rebound = await _apply(connection, schema, actor, [rebinding_by_field])
    assert await _count(connection, schema, original) == 1
    assert await _network_ids(connection, schema, original) == [network["record_id"]]
    await _approve(connection, schema, await _command(connection, schema, replacement, *rebound["records"]), actor)
    current = await _pin(connection, schema)
    assert await _network_ids(connection, schema, original) == [network["record_id"]]
    assert await _network_ids(connection, schema, current) == [replacement["record_id"]]
    closed = await _apply(
        connection,
        schema,
        actor,
        [
            {
                **rebinding_by_field,
                "operation": "close",
                "expected_revision": 2,
                "expected_network_id": replacement["record_id"],
            }
        ],
    )
    assert await _count(connection, schema, current) == 1
    await _approve(connection, schema, await _command(connection, schema, *closed["records"]), actor)
    assert await _count(connection, schema, await _pin(connection, schema)) == 0


@pytest.mark.parametrize("damage", ["missing", "archived", "identity"])
async def test_approved_targets_must_be_exact(serving_schema, damage):
    connection, schema, _ = serving_schema
    _, network, _, source = await _setup(serving_schema)
    if damage == "missing":
        await connection.execute(f"DELETE FROM \"{schema}\".registry_approved_record WHERE record_kind='network'")
    else:
        changes = {"archived": True} if damage == "archived" else {"network_id": network["record_id"] + 1}
        await connection.execute(
            f"UPDATE \"{schema}\".registry_approved_record SET record_json=record_json||$1::jsonb WHERE record_kind='network'",
            json.dumps(changes),
        )
    source = await _pin(connection, schema)
    with pytest.raises(ApprovedNetworkSourceBindingError, match="^registry_approved_source_binding_invalid$"):
        await _count(connection, schema, source)


@pytest.mark.parametrize(
    "changes",
    [
        {"binding_id": "00000000-0000-0000-0000-000000000000"},
        {"binding_key": "A" * 64},
        {"source_key": " key"},
        {"source_key": "x" * 513},
        {"source_key": "key\nvalue"},
        {"network_id": True},
        {"network_id": "1"},
        {"network_id": 2147483648},
        {"network_id": 1.5},
        {"revision": True},
        {"archived": "false"},
        {"archived": True, "network_id": True},
        {"source_scope_json": []},
        {"source_scope_json": {}},
        {"source_scope_json": {"unknown": "key"}},
        {"source_scope_json": {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": False}},
    ],
)
async def test_malformed_scope_is_rejected_wholly(serving_schema, changes):
    connection, schema, _ = serving_schema
    await _setup(serving_schema)
    await connection.execute(
        f"UPDATE \"{schema}\".registry_approved_record SET record_json=record_json||$1::jsonb WHERE record_kind='network_binding'",
        json.dumps(changes),
    )
    source = await _pin(connection, schema)
    with pytest.raises(ApprovedNetworkSourceBindingError) as caught:
        await _count(connection, schema, source)
    assert str(caught.value) == "registry_approved_source_binding_invalid"
    assert "synthetic" not in str(caught.value)


@pytest.mark.parametrize("different_key", [False, True])
async def test_duplicate_scope_never_guesses(serving_schema, different_key):
    connection, schema, _ = serving_schema
    await _setup(serving_schema)
    changed_by_field = {"binding_id": str(uuid4()), "binding_key": "b" * 64}
    if different_key:
        changed_by_field["source_key"] = "different-opaque-key"
    await connection.execute(
        f'INSERT INTO "{schema}".registry_approved_record SELECT approved_revision,record_kind,$1,record_revision,custom_revision,record_json||$2::jsonb FROM "{schema}".registry_approved_record WHERE record_kind=\'network_binding\'',
        changed_by_field["binding_id"],
        json.dumps(changed_by_field),
    )
    with pytest.raises(ApprovedNetworkSourceBindingError, match="^registry_approved_source_binding_invalid$"):
        await _count(connection, schema, await _pin(connection, schema))


@pytest.mark.parametrize("count", [1, 100])
async def test_validation_query_count_is_fixed(serving_schema, count):
    connection, schema, _ = serving_schema
    _, _, _, source = await _setup(serving_schema, count)
    counted = _CountedConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        assert (
            await require_approved_network_source_bindings(counted, source, COORDINATES, control_schema=schema) == count
        )
    assert counted.statements == 2


async def test_pin_and_transaction_are_required(serving_schema):
    connection, schema, _ = serving_schema
    _, _, _, source = await _setup(serving_schema)
    for invalid in (None, replace(source, generation_id="a" * 64)):
        with pytest.raises(ApprovedNetworkSourceBindingError):
            await _count(connection, schema, invalid)
    with pytest.raises(ApprovedNetworkSourceBindingError):
        await require_approved_network_source_bindings(connection, source, COORDINATES, control_schema=schema)
    async with connection.transaction():
        with pytest.raises(ApprovedNetworkSourceBindingError):
            await require_approved_network_source_bindings(connection, source, COORDINATES, control_schema=schema)


@pytest.mark.parametrize(
    "source_system,scope",
    [
        (
            "aca",
            {
                "issuer_id": "12345",
                "state": "CA",
                "plan_year": 2100,
                "plan_id": "12345CA0000001",
                "checksum_network": -2147483648,
            },
        ),
        (
            "aca",
            {
                "issuer_id": "12345",
                "state": "CA",
                "plan_year": 2010,
                "plan_id": "12345CA0000001-01",
                "checksum_network": 2147483647,
            },
        ),
        (
            "fhir",
            {
                "organization_id": "network-one",
                "legacy_uuid": "30000000-0000-0000-0000-000000000001",
                "alias_scope": "medical",
            },
        ),
    ],
)
async def test_other_source_scopes_are_explicit(serving_schema, source_system, scope):
    connection, schema, engine = serving_schema
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor)
    binding = _binding(network["record_id"], source_system=source_system, source_scope_json=scope)
    receipt = await _apply(connection, schema, actor, [binding])
    await _approve(connection, schema, await _command(connection, schema, network, *receipt["records"]), actor)
    source = await _pin(connection, schema)
    assert await _count(connection, schema, source, replace(COORDINATES, source_system=source_system)) == 1


@pytest.mark.parametrize(
    "source_system,scope",
    [
        (
            "aca",
            {
                "issuer_id": "00000",
                "state": "CA",
                "plan_year": 2026,
                "plan_id": "00000CA0000001",
                "checksum_network": -17,
            },
        ),
        (
            "aca",
            {
                "issuer_id": "12345",
                "state": "CA",
                "plan_year": True,
                "plan_id": "12345CA0000001",
                "checksum_network": -17,
            },
        ),
        (
            "aca",
            {
                "issuer_id": "12345",
                "state": "CA",
                "plan_year": 2026,
                "plan_id": "54321CA0000001",
                "checksum_network": -17,
            },
        ),
        (
            "aca",
            {
                "issuer_id": "12345",
                "state": "CA",
                "plan_year": 2026,
                "plan_id": "12345CA0000001",
                "checksum_network": True,
            },
        ),
        (
            "fhir",
            {
                "organization_id": "Organization/network-one",
                "legacy_uuid": "30000000-0000-0000-0000-000000000001",
                "alias_scope": "medical",
            },
        ),
        (
            "fhir",
            {
                "organization_id": "network-one",
                "legacy_uuid": "00000000-0000-0000-0000-000000000000",
                "alias_scope": "medical",
            },
        ),
        (
            "fhir",
            {
                "organization_id": "network-one",
                "legacy_uuid": "30000000-0000-0000-0000-000000000001",
                "alias_scope": "medical ",
            },
        ),
    ],
)
async def test_malformed_source_shapes_reject(serving_schema, source_system, scope):
    connection, schema, _ = serving_schema
    await _setup(serving_schema)
    await connection.execute(
        f"UPDATE \"{schema}\".registry_approved_record SET record_json=record_json||$1::jsonb WHERE record_kind='network_binding'",
        json.dumps({"source_system": source_system, "source_scope_json": scope}),
    )
    with pytest.raises(ApprovedNetworkSourceBindingError, match="^registry_approved_source_binding_invalid$"):
        await _count(
            connection, schema, await _pin(connection, schema), replace(COORDINATES, source_system=source_system)
        )
