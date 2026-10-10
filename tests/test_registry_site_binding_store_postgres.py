# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Server-resolved site binding heads use actual retained proof and native checks."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData, insert, select, text
from sqlalchemy.exc import DBAPIError

from db.models.registry_site_binding import RegistrySiteBinding
from process.network_serving_read import NetworkServingReadUnavailable
from process.registry_retained_site_adoption import RetainedSiteAdoptionError
from process.registry_site_binding_store import resolve_site_binding_fields, validated_site_binding_fields
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_retained_site_adoption_postgres import retained_db

pytestmark = pytest.mark.asyncio


def _wire_fields(fixture):
    """Select one exact retained tuple and explicitly name its serving generation."""
    return {"source_generation": fixture.source.generation_id, **fixture.rows[0]}


def _valid_fields():
    """Use a neutral canonical wire example for database-free boundary checks."""
    return {
        "source_generation": 1,
        "provider_system": "npi",
        "provider_id": "1000000004",
        "location_id": "12345678-1234-4234-8234-123456abcdef",
        "location_key": "a" * 64,
        "address_row_sha256": "b" * 64,
    }


@pytest.fixture
async def binding_db(retained_db):
    """Create the actual new model in this test's migrated control namespace."""
    table = RegistrySiteBinding.__table__.to_metadata(MetaData(), schema=retained_db.control_schema)
    async with retained_db.engine.begin() as setup:
        await setup.run_sync(lambda connection: table.create(connection, checkfirst=True))
    return retained_db, table


@pytest.mark.parametrize("retained_db", [(2, "npi"), (2, "provider_directory")], indirect=True)
async def test_fields_resolve_one_exact_office_with_complete_fresh_source_proof(binding_db):
    fixture, table = binding_db
    original = _wire_fields(fixture)
    assert validated_site_binding_fields(original) == original
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        resolved = await resolve_site_binding_fields(
            fixture.connection, original, control_schema=fixture.control_schema
        )
    assert set(resolved) == set(original) | {"source_receipt_json"}
    assert resolved["location_id"] == UUID(original["location_id"])
    proof = resolved["source_receipt_json"]
    assert proof["generation_id"] == original["source_generation"]
    assert proof["candidate_id"] == fixture.source.candidate_id
    assert proof["manifest_sha256"] == fixture.source.manifest_sha256
    assert proof["source_generations"] == fixture.source.source_generations
    assert proof["address_table_oid"] == fixture.source.address_table_oid
    assert all(proof[field] > 0 for field in ("schema_oid", "binding_table_oid", "owner_role_oid"))
    assert proof["records"] == [
        {**fixture.rows[0], "entity_type": original["provider_system"], "entity_id": original["provider_id"]}
    ]
    assert fixture.rows[1]["location_id"] not in json.dumps(proof)
    assert len(json.dumps(proof).encode()) <= 65536
    assert original == _wire_fields(fixture)
    binding_id = uuid4()
    async with fixture.engine.begin() as writer:
        await writer.execute(insert(table).values(binding_id=binding_id, **resolved))
    async with fixture.engine.connect() as reader:
        stored = (await reader.execute(select(table).where(table.c.binding_id == binding_id))).mappings().one()
    assert stored["source_receipt_json"] == proof and stored["location_id"] == resolved["location_id"]
    assert stored["archived"] is False and stored["revision"] == 1 and stored["created_at"].tzinfo is not None
    assert RegistrySiteBinding.__runtime_schema_sync__ is False


@pytest.mark.parametrize(
    "change",
    [
        {"source_generation": 0},
        {"source_generation": -1},
        {"source_generation": True},
        {"source_generation": 9223372036854775808},
        {"source_generation": "1"},
        {"source_generation": 1.0},
        {"provider_system": "manual"},
        {"provider_system": True},
        {"provider_id": 1000000004},
        {"provider_id": "1000000005"},
        {"provider_id": " 1000000004"},
        {"provider_id": "1000000004\n"},
        {"provider_system": "provider_directory", "provider_id": "x" * 129},
        {"provider_system": "provider_directory", "provider_id": ""},
        {"provider_system": "provider_directory", "provider_id": "source\x00provider"},
        {"location_id": str(UUID(int=0))},
        {"location_id": "12345678-1234-4234-8234-123456ABCDEF"},
        {"location_id": UUID("12345678-1234-4234-8234-123456abcdef")},
        {"location_id": "not-a-site"},
        {"location_key": "A" * 64},
        {"address_row_sha256": "b" * 63},
        {"address_row_sha256": False},
    ],
)
async def test_pure_canonical_boundary_rejects_bad_wire_values(change):
    with pytest.raises(ValueError, match="^registry_site_binding_fields_invalid$"):
        validated_site_binding_fields({**_valid_fields(), **change})


@pytest.mark.parametrize(
    "extra_field",
    [
        "source_receipt_json",
        "display_name",
        "network_id",
        "actor_json",
        "entity_type",
        "entity_id",
        "schema_name",
        "client_id",
    ],
)
async def test_clients_cannot_supply_proof_or_unrelated_fields(extra_field):
    with pytest.raises(ValueError, match="^registry_site_binding_fields_invalid$"):
        await resolve_site_binding_fields(None, {**_valid_fields(), extra_field: "untrusted"})
    with pytest.raises(ValueError):
        validated_site_binding_fields({key: value for key, value in _valid_fields().items() if key != "location_key"})
    with pytest.raises(ValueError):
        validated_site_binding_fields(None)


async def test_pure_validation_preserves_exact_opaque_directory_identity():
    original_by_field = {
        **_valid_fields(),
        "provider_system": "provider_directory",
        "provider_id": "source:Organization/provider-1",
    }
    validated = validated_site_binding_fields(original_by_field)
    assert validated == original_by_field and validated is not original_by_field
    validated["provider_id"] = "changed"
    assert original_by_field["provider_id"] == "source:Organization/provider-1"


@pytest.mark.parametrize("damage", ["generation", "namespace", "site", "fingerprint", "ineligible", "acl", "oid"])
async def test_actual_retained_identity_or_catalog_drift_fails_closed(binding_db, damage):
    fixture, table = binding_db
    fields_by_name = _wire_fields(fixture)
    changes_by_damage = {
        "generation": {"source_generation": fixture.source.generation_id + 1},
        "namespace": {"provider_system": "provider_directory"},
        "site": {"location_id": str(uuid4())},
        "fingerprint": {"address_row_sha256": "0" * 64},
    }
    fields_by_name.update(changes_by_damage.get(damage, {}))
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        statement_by_damage = {
            "ineligible": f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false',
            "acl": f'GRANT UPDATE ON "{fixture.source.schema_name}".entity_address_unified TO "{fixture.roles["reader"]}"',
            "oid": f'ALTER TABLE "{fixture.source.schema_name}".provider_location_binding RENAME TO old_binding',
        }
        if damage in statement_by_damage:
            await fixture.connection.execute(statement_by_damage[damage])
        with pytest.raises((RetainedSiteAdoptionError, NetworkServingReadUnavailable)):
            await resolve_site_binding_fields(fixture.connection, fields_by_name, control_schema=fixture.control_schema)
    finally:
        await transaction.rollback()
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".{table.name}') == 0


async def test_real_sqlalchemy_driver_bridge_and_caller_rollback(binding_db):
    fixture, table = binding_db
    binding_id = uuid4()
    async with fixture.engine.connect() as writer:
        transaction = await writer.begin()
        try:
            await writer.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            driver = (await writer.get_raw_connection()).driver_connection
            resolved = await resolve_site_binding_fields(
                driver, _wire_fields(fixture), control_schema=fixture.control_schema
            )
            stored = (
                (await writer.execute(insert(table).values(binding_id=binding_id, **resolved).returning(table)))
                .mappings()
                .one()
            )
            assert stored["source_receipt_json"] == resolved["source_receipt_json"]
        finally:
            await transaction.rollback()
    async with fixture.engine.connect() as reader:
        assert (await reader.execute(select(table).where(table.c.binding_id == binding_id))).first() is None


@pytest.mark.parametrize(
    "change",
    [
        {"binding_id": UUID(int=0)},
        {"source_generation": 0},
        {"location_id": UUID(int=0)},
        {"provider_system": "manual"},
        {"provider_id": ""},
        {"provider_id": "1000000004 "},
        {"provider_id": "x" * 129},
        {"location_key": "A" * 64},
        {"address_row_sha256": "b" * 63},
        {"source_receipt_json": []},
        {"source_receipt_json": {"oversized": "x" * 65536}},
        {"revision": 0},
    ],
)
async def test_actual_model_native_constraints_reject_invalid_heads(binding_db, change):
    fixture, table = binding_db
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        resolved = await resolve_site_binding_fields(
            fixture.connection, _wire_fields(fixture), control_schema=fixture.control_schema
        )
    with pytest.raises(DBAPIError) as rejected:
        async with fixture.engine.begin() as writer:
            await writer.execute(insert(table).values(**{"binding_id": uuid4(), **resolved, **change}))
    assert rejected.value.orig.sqlstate in ("23514", "22001")
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".{table.name}') == 0


async def test_repeatable_snapshot_contract_and_fixed_read_count(binding_db):
    fixture, table = binding_db
    with pytest.raises(RetainedSiteAdoptionError):
        async with fixture.connection.transaction(readonly=True):
            await resolve_site_binding_fields(
                fixture.connection, _wire_fields(fixture), control_schema=fixture.control_schema
            )
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="serializable", readonly=True):
            await asyncio.sleep(0)
            queries.clear()
            await resolve_site_binding_fields(
                fixture.connection, _wire_fields(fixture), control_schema=fixture.control_schema
            )
            await asyncio.sleep(0)
            assert len(queries) == 4
            assert all(query.query.lstrip().split()[0] in ("SELECT", "WITH") for query in queries)
    finally:
        fixture.connection.remove_query_logger(queries.append)
