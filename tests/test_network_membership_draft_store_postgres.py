# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit office drafts through actual native encoding and migrated history."""

import asyncio
import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData, event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models import NPIData
from process import registry_record_store as store
from process.network_membership_copy import MembershipCopyError
from process.registry_record_store import RegistryRecordCommand, RegistryRecordConflict
from tests.test_manual_location_identity_store_postgres import _create as _location
from tests.test_manual_provider_identity_store_postgres import _create as _provider
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_record_store_postgres import _actor, _create

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def membership_db(serving_schema):
    connection, schema, engine = serving_schema
    source_schema = "membership_source_test_" + uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{source_schema}"')
    try:
        source_table = NPIData.__table__.to_metadata(MetaData(), schema=source_schema)
        async with engine.begin() as setup:
            await setup.run_sync(lambda sync: source_table.create(sync))
        yield connection, schema, async_sessionmaker(engine), source_schema, engine
    finally:
        await connection.execute(f'DROP SCHEMA "{source_schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", source_schema) is None


async def _apply(database, command, actor):
    _, schema, sessions, source_schema, _ = database
    async with sessions() as session, session.begin():
        return await store.apply_registry_record_command(
            session, command, actor, schema=schema, source_schema=source_schema
        )


async def _seed(database):
    actor = _actor()
    network = await _apply(database, _create("network"), actor)
    provider = await _apply(database, _provider(), actor)
    first = _location()
    second = _location(
        fields={**first.fields, "address_json": {**first.fields["address_json"], "second_line": "Suite 3"}}
    )
    sites = [await _apply(database, command, actor) for command in (first, second)]
    return actor, network["record_id"], provider["record_id"], [site["record_id"] for site in sites]


def _member(network_id, provider_id, location_id, *, system="manual", evidence="example-review"):
    return {
        "network_id": network_id,
        "provider_system": system,
        "provider_id": provider_id,
        "location_id": location_id,
        "evidence_id": evidence,
    }


def _command(network_id, membership_rows):
    return RegistryRecordCommand(
        "membership",
        network_id,
        "create",
        0,
        {"memberships_json": membership_rows},
        "Explicit office membership",
        uuid4().hex,
    )


async def _binding(database, provider_system, provider_id, location_id, *, archived=False):
    connection, schema, _, _, _ = database
    await connection.execute(
        f'INSERT INTO "{schema}".manual_provider_location_binding '
        "(provider_system,provider_id,location_id,location_key,entity_type,entity_id,archived) VALUES($1,$2,$3,$4,$5,$6,$7)",
        provider_system,
        provider_id,
        UUID(location_id),
        uuid4().hex * 2,
        "provider",
        provider_id[:128],
        archived,
    )


async def test_exact_office_order_clear_and_history(membership_db):
    connection, schema, sessions, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    creation = _command(network_id, [_member(network_id, provider_id, sites[1])])
    first = await _apply(membership_db, creation, actor)
    assert first["record"]["memberships_json"] == creation.fields["memberships_json"]
    assert sites[0] not in json.dumps(first)
    two_offices = [
        _member(network_id, provider_id, site, evidence=f"office-{index}") for index, site in enumerate(reversed(sites))
    ]
    correction = replace(
        creation,
        operation="correct",
        expected_revision=1,
        fields={"memberships_json": two_offices},
        idempotency_key=uuid4().hex,
    )
    second = await _apply(membership_db, correction, actor)
    assert second["record"]["memberships_json"] == two_offices
    clear = replace(correction, expected_revision=2, fields={"memberships_json": []}, idempotency_key=uuid4().hex)
    assert (await _apply(membership_db, clear, actor))["record"]["memberships_json"] == []
    async with sessions() as session:
        assert (await store.get_registry_record(session, "membership", network_id, schema=schema))["revision"] == 3
        assert (
            len(await store.list_registry_records(session, "membership", record_ids=[network_id], schema=schema)) == 1
        )
    assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0
    history = await connection.fetch(
        f"SELECT record_json FROM \"{schema}\".registry_record_history WHERE record_kind='membership' ORDER BY revision"
    )
    assert [json.loads(entry["record_json"])["memberships_json"] for entry in history] == [
        creation.fields["memberships_json"],
        two_offices,
        [],
    ]


async def test_source_npi_and_explicit_directory_binding(membership_db):
    connection, schema, _, source_schema, _ = membership_db
    actor, network_id, _, sites = await _seed(membership_db)
    await connection.execute(f'INSERT INTO "{source_schema}".npi(npi) VALUES(1000000004)')
    directory_id, directory_site = str(uuid4()), str(uuid4())
    await _binding(membership_db, "provider_directory", directory_id, directory_site)
    rows = [
        _member(network_id, "1000000004", sites[0], system="npi"),
        _member(network_id, directory_id, directory_site, system="provider_directory"),
    ]
    assert (await _apply(membership_db, _command(network_id, rows), actor))["record"]["memberships_json"] == rows
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_registry') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{source_schema}".npi') == 1
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(membership_db, replace(_command(network_id, rows), record_id=2147483647), actor)


@pytest.mark.parametrize(
    "changes",
    [
        {"network_id": True},
        {"network_id": "1"},
        {"network_id": -1},
        {"network_id": 1.5},
        {"provider_system": "unknown"},
        {"provider_id": True},
        {"provider_id": ""},
        {"location_id": "00000000-0000-0000-0000-000000000000"},
        {"location_id": True},
        {"evidence_id": ""},
        {"unexpected": "field"},
    ],
)
async def test_native_row_validation_never_writes(membership_db, changes):
    connection, schema, _, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    with pytest.raises(ValueError, match="membership_rows_invalid"):
        await _apply(
            membership_db, _command(network_id, [{**_member(network_id, provider_id, sites[0]), **changes}]), actor
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before


@pytest.mark.parametrize(
    "changes",
    [
        {"network_id": 2147483647},
        {"provider_id": "1000000004", "provider_system": "manual"},
        {"provider_id": "1000000004", "provider_system": "npi"},
        {"provider_id": str(uuid4())},
        {"location_id": str(uuid4())},
        {"provider_system": "provider_directory"},
    ],
)
async def test_unknown_or_wrong_namespace_targets(membership_db, changes):
    connection, schema, _, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(
            membership_db, _command(network_id, [{**_member(network_id, provider_id, sites[0]), **changes}]), actor
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0


async def test_duplicate_pair_and_archived_targets(membership_db):
    connection, schema, _, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    row = _member(network_id, provider_id, sites[0])
    for rows in ([row, row], [row, {**row, "evidence_id": "different-evidence"}]):
        with pytest.raises(ValueError, match="target_invalid"):
            await _apply(membership_db, _command(network_id, rows), actor)
    for table, column, identity in (
        ("manual_provider_registry", "provider_id", UUID(provider_id)),
        ("manual_location_registry", "location_id", UUID(sites[0])),
        ("network_registry_record", "network_id", network_id),
    ):
        await connection.execute(f'UPDATE "{schema}".{table} SET archived=true WHERE {column}=$1', identity)
        with pytest.raises(ValueError, match="target_invalid"):
            await _apply(membership_db, _command(network_id, [row]), actor)
        await connection.execute(f'UPDATE "{schema}".{table} SET archived=false WHERE {column}=$1', identity)
    directory_id, directory_site = str(uuid4()), str(uuid4())
    await _binding(membership_db, "provider_directory", directory_id, directory_site, archived=True)
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(
            membership_db,
            _command(network_id, [_member(network_id, directory_id, directory_site, system="provider_directory")]),
            actor,
        )


async def test_unallocated_network_and_canonical_manual_uuid(membership_db):
    connection, schema, _, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    await connection.execute(
        f"INSERT INTO \"{schema}\".network_registry_record(network_id,display_name) VALUES(2147483647,'Unallocated example')"
    )
    for unknown_network_id in (2147483646, 2147483647):
        with pytest.raises(ValueError, match="target_invalid"):
            await _apply(membership_db, _command(unknown_network_id, []), actor)
    for invalid_provider in (provider_id.upper(), "00000000-0000-0000-0000-000000000000", provider_id.replace("-", "")):
        with pytest.raises(ValueError, match="target_invalid"):
            await _apply(membership_db, _command(network_id, [_member(network_id, invalid_provider, sites[0])]), actor)
    for invalid_identity in (True, 0, -1, 2147483648, str(network_id), uuid4()):
        with pytest.raises(ValueError, match="network_id_invalid"):
            await _apply(membership_db, _command(invalid_identity, []), actor)
    with pytest.raises(ValueError, match="allocation_key_not_supported"):
        await _apply(membership_db, replace(_command(network_id, []), allocation_key=uuid4()), actor)


async def test_binding_is_exact_pair_and_source_npi_required(membership_db):
    connection, schema, _, source_schema, _ = membership_db
    actor, network_id, _, sites = await _seed(membership_db)
    directory_id, bound_site = str(uuid4()), str(uuid4())
    await _binding(membership_db, "provider_directory", directory_id, bound_site)
    for row in (
        _member(network_id, directory_id, sites[0], system="provider_directory"),
        _member(network_id, str(uuid4()), bound_site, system="provider_directory"),
    ):
        with pytest.raises(ValueError, match="target_invalid"):
            await _apply(membership_db, _command(network_id, [row]), actor)
    npi_site = str(uuid4())
    await _binding(membership_db, "npi", "1000000004", npi_site)
    creation = _command(network_id, [_member(network_id, "1000000004", npi_site, system="npi")])
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(membership_db, creation, actor)
    await connection.execute(f'INSERT INTO "{source_schema}".npi(npi) VALUES(1000000004)')
    retained = await _apply(membership_db, creation, actor)
    assert retained["record"]["memberships_json"] == creation.fields["memberships_json"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_location_registry') == 2


async def test_membership_fields_are_closed(membership_db):
    actor, network_id, provider_id, sites = await _seed(membership_db)
    for invalid_fields in (
        {},
        {"memberships_json": None},
        {"memberships_json": {}},
        {"memberships_json": [], "extra": True},
    ):
        with pytest.raises(ValueError):
            await _apply(membership_db, replace(_command(network_id, []), fields=invalid_fields), actor)
    for invalid_rows in ([{"network_id": network_id}], [_member(network_id, "1000000005", sites[0], system="npi")]):
        with pytest.raises(ValueError, match="membership_rows_invalid"):
            await _apply(membership_db, _command(network_id, invalid_rows), actor)
    row = _member(network_id, provider_id, sites[0])
    with pytest.raises(ValueError, match="command_json_invalid"):
        await _apply(membership_db, _command(network_id, [{**row, "evidence_id": float("nan")}]), actor)


async def test_replay_precedes_native_targets_and_lifecycle(membership_db, monkeypatch):
    connection, schema, _, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    creation = _command(network_id, [_member(network_id, provider_id, sites[0])])
    first = await _apply(membership_db, creation, actor)
    correction = replace(
        creation, operation="correct", expected_revision=1, fields={"memberships_json": []}, idempotency_key=uuid4().hex
    )
    await _apply(membership_db, correction, actor)
    await connection.execute(f'UPDATE "{schema}".manual_provider_registry SET archived=true')

    def unavailable(_):
        raise MembershipCopyError("Native membership encoder is unavailable")

    monkeypatch.setattr(store, "_encode", unavailable)
    assert await _apply(membership_db, creation, actor) == first
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(membership_db, replace(creation, reason="Changed retry"), actor)
    with pytest.raises(store.RegistryAddressUnavailable, match="native_unavailable"):
        await _apply(membership_db, replace(correction, expected_revision=2, idempotency_key=uuid4().hex), actor)
    archived = replace(creation, operation="archive", expected_revision=2, fields={}, idempotency_key=uuid4().hex)
    assert (await _apply(membership_db, archived, actor))["record"]["archived"] is True


async def test_restore_revalidates_and_stale_cas(membership_db):
    connection, schema, _, _, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    creation = _command(network_id, [_member(network_id, provider_id, sites[0])])
    await _apply(membership_db, creation, actor)
    archive = replace(creation, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
    retained = await _apply(membership_db, archive, actor)
    assert retained["record"]["memberships_json"] == creation.fields["memberships_json"]
    restore = replace(archive, operation="restore", expected_revision=2, idempotency_key=uuid4().hex)
    await connection.execute(f'UPDATE "{schema}".manual_location_registry SET archived=true')
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(membership_db, restore, actor)
    await connection.execute(f'UPDATE "{schema}".manual_location_registry SET archived=false')
    assert (await _apply(membership_db, restore, actor))["record"]["archived"] is False
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(membership_db, replace(restore, idempotency_key=uuid4().hex), actor)


async def test_whole_caller_rollback_and_savepoint(membership_db):
    connection, schema, sessions, source_schema, _ = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    creation = _command(network_id, [_member(network_id, provider_id, sites[0])])
    before = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    async with sessions() as session:
        transaction = await session.begin()
        await store.apply_registry_record_command(session, creation, actor, schema=schema, source_schema=source_schema)
        await transaction.rollback()
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == before
    async with sessions() as session, session.begin():
        invalid = replace(creation, fields={"memberships_json": [creation.fields["memberships_json"][0]] * 2})
        with pytest.raises(ValueError, match="target_invalid"):
            await store.apply_registry_record_command(
                session, invalid, actor, schema=schema, source_schema=source_schema
            )
        assert await session.scalar(text("SELECT 1")) == 1
        await store.apply_registry_record_command(session, creation, actor, schema=schema, source_schema=source_schema)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_draft') == 1


async def test_native_bounds_and_fixed_query_count(membership_db, monkeypatch):
    connection, schema, _, _, engine = membership_db
    actor, network_id, provider_id, sites = await _seed(membership_db)
    site_ids = [str(uuid4()) for _ in range(5000)]
    await connection.execute(
        f"INSERT INTO \"{schema}\".manual_location_registry(location_id,display_name,address_json,canonical_address_json) SELECT location_id,'Example site','{{}}'::jsonb,'{{}}'::jsonb FROM unnest($1::uuid[]) location_id",
        [UUID(site) for site in site_ids],
    )
    membership_rows = [_member(network_id, provider_id, site) for site in site_ids]
    sql_counts, native_counts = [], []
    original_encoder = store._encode

    def encode_once(input_bytes):
        native_counts.append(len(json.loads(input_bytes)))
        return original_encoder(input_bytes)

    monkeypatch.setattr(store, "_encode", encode_once)
    for row_count, operation, revision in ((1, "create", 0), (5000, "correct", 1)):
        statements = []

        def capture_sql(_, __, statement, ___, ____, _____):
            statements.append(statement)

        event.listen(engine.sync_engine, "before_cursor_execute", capture_sql)
        try:
            command = replace(
                _command(network_id, membership_rows[:row_count]), operation=operation, expected_revision=revision
            )
            assert len((await _apply(membership_db, command, actor))["record"]["memberships_json"]) == row_count
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture_sql)
        sql_counts.append(len(statements))
        assert sum("jsonb_to_recordset" in statement for statement in statements) == 1
    assert sql_counts[0] == sql_counts[1] and native_counts == [1, 5000]
    for invalid_rows in (
        membership_rows + [membership_rows[0]],
        [_member(network_id, provider_id, sites[0], evidence="x" * (8 * 1024 * 1024))],
    ):
        with pytest.raises(ValueError, match="membership_rows_invalid"):
            await _apply(
                membership_db,
                replace(_command(network_id, invalid_rows), operation="correct", expected_revision=2),
                actor,
            )


async def test_concurrent_corrections_have_one_winner(membership_db):
    actor, network_id, provider_id, sites = await _seed(membership_db)
    creation = _command(network_id, [_member(network_id, provider_id, sites[0])])
    await _apply(membership_db, creation, actor)
    correction = replace(creation, operation="correct", expected_revision=1, fields={"memberships_json": []})
    results = await asyncio.gather(
        *[_apply(membership_db, replace(correction, idempotency_key=uuid4().hex), actor) for _ in range(2)],
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, RegistryRecordConflict) for result in results) == 1
