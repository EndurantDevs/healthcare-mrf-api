# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One native set read for mixed exact targets and explicit creation scopes."""

import json
from uuid import uuid4

import pytest
from sqlalchemy import event

from process.network_registry_identity import allocate_network_ids
from process.registry_targets import verify_registry_targets
from tests.test_network_source_binding_store_postgres import _apply, _command, _row, _seed
from tests.test_registry_record_store_postgres import _actor, record_db


def _target(kind, action, key):
    return {"record_kind": kind, "action": action, "target_key": str(key)}


async def _seed_registry(record_db):
    connection, schema, sessions = record_db
    group_id, archived_group = uuid4(), uuid4()
    for identifier, archived in [(group_id, False), (archived_group, True)]:
        await connection.execute(
            f'INSERT INTO "{schema}".company_group_registry(group_id,group_kind,display_name,archived) VALUES ($1,$2,$3,$4)',
            identifier,
            "corporate_parent",
            "Example group",
            archived,
        )
    await connection.execute(
        f'INSERT INTO "{schema}".company_registry(company_id,display_name,roles,archived) VALUES ($1,$2,$3,true)',
        group_id,
        "Example company",
        ["employer"],
    )
    async with sessions() as session, session.begin():
        allocated_ids = list((await allocate_network_ids(session, [uuid4(), uuid4(), uuid4()], schema=schema)).values())
    for identifier, archived in zip(allocated_ids[:2], [False, True]):
        await connection.execute(
            f'INSERT INTO "{schema}".network_registry_record(network_id,display_name,archived) VALUES ($1,$2,$3)',
            identifier,
            "Example network",
            archived,
        )
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) VALUES (2147483647,$1)',
        "Example orphan metadata",
    )
    return group_id, archived_group, allocated_ids


@pytest.mark.asyncio
async def test_mixed_targets_use_one_statement_and_exact_record_namespaces(record_db):
    _, schema, sessions = record_db
    group_id, archived_group, networks = await _seed_registry(record_db)
    requested_targets = [
        _target("group", "edit", group_id),
        _target("group", "restore", archived_group),
        _target("company", "archive", group_id),
        _target("network", "membership", networks[0]),
        _target("network", "restore", networks[1]),
        _target("network", "edit", networks[2]),
        _target("network", "edit", 2147483647),
        _target("group", "edit", uuid4()),
        _target("provider", "edit", group_id),
        _target("location", "link", group_id),
    ]
    statements = []
    engine = sessions.kw["bind"]

    def count_statement(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        async with sessions() as session:
            statuses = await verify_registry_targets(session, requested_targets, schema=schema)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_statement)
    assert len(statements) == 1
    assert statuses[f"group:edit:{group_id}"] == {"known": True, "archived": False}
    assert statuses[f"group:restore:{archived_group}"] == {"known": True, "archived": True}
    assert statuses[f"company:archive:{group_id}"] == {"known": True, "archived": True}
    assert statuses[f"network:membership:{networks[0]}"] == {"known": True, "archived": False}
    assert statuses[f"network:restore:{networks[1]}"] == {"known": True, "archived": True}
    assert sum(status["known"] for status in statuses.values()) == 5
    for request_target in requested_targets[5:]:
        assert statuses[
            f"{request_target['record_kind']}:{request_target['action']}:{request_target['target_key']}"
        ] == {
            "known": False,
            "archived": False,
        }


@pytest.mark.asyncio
async def test_creation_scopes_check_scope_records_and_preserve_archive_state(record_db):
    _, schema, sessions = record_db
    group_id, archived_group, _ = await _seed_registry(record_db)
    targets = [
        _target("network", "create", "root:"),
        _target("provider", "create", f"group:{group_id}"),
        _target("company", "create", f"group:{archived_group}"),
        _target("location", "create", f"company:{group_id}"),
        _target("network", "create", f"company:{uuid4()}"),
    ]
    async with sessions() as session:
        statuses = await verify_registry_targets(session, targets, schema=schema)
    assert statuses["network:create:root:"] == {"known": True, "archived": False}
    assert statuses[f"provider:create:group:{group_id}"] == {"known": True, "archived": False}
    assert statuses[f"company:create:group:{archived_group}"] == {"known": True, "archived": True}
    assert statuses[f"location:create:company:{group_id}"] == {"known": True, "archived": True}
    assert sum(status["known"] for status in statuses.values()) == 4


@pytest.mark.asyncio
async def test_manual_provider_and_site_targets_keep_independent_uuid_namespaces(record_db):
    """A company UUID cannot stand in for a missing provider or location."""
    connection, schema, sessions = record_db
    group_id, _, _ = await _seed_registry(record_db)
    await connection.execute(
        f'INSERT INTO "{schema}".manual_provider_registry(provider_id,display_name,provider_kind) VALUES($1,$2,$3)',
        group_id,
        "Example manual provider",
        "individual",
    )
    location_id = uuid4()
    await connection.execute(
        f'INSERT INTO "{schema}".manual_location_registry(location_id,display_name,address_json,canonical_address_json,archived) '
        "VALUES($1,$2,$3::jsonb,$3::jsonb,true)",
        location_id,
        "Example site",
        json.dumps({"first_line": "100 Example Street"}),
    )
    async with sessions() as session:
        statuses = await verify_registry_targets(
            session,
            [
                _target("provider", "edit", group_id),
                _target("location", "restore", location_id),
                _target("location", "edit", group_id),
                _target("provider", "edit", location_id),
            ],
            schema=schema,
        )
    assert statuses[f"provider:edit:{group_id}"] == {"known": True, "archived": False}
    assert statuses[f"location:restore:{location_id}"] == {"known": True, "archived": True}
    assert (
        statuses[f"location:edit:{group_id}"]
        == statuses[f"provider:edit:{location_id}"]
        == {"known": False, "archived": False}
    )


@pytest.mark.asyncio
async def test_allocated_network_identity_ignores_pending_archive_and_missing_manual_head(record_db):
    _, schema, sessions = record_db
    _, _, network_ids = await _seed_registry(record_db)
    targets = [_target("network", "edit", identifier) for identifier in network_ids]
    async with sessions() as session:
        management_status = await verify_registry_targets(session, targets, schema=schema)
        identity_status = await verify_registry_targets(session, targets, schema=schema, boundary="identity")
        assert management_status[f"network:edit:{network_ids[1]}"] == {"known": True, "archived": True}
        assert not management_status[f"network:edit:{network_ids[2]}"]["known"]
        assert all(flags_by_name == {"known": True, "archived": False} for flags_by_name in identity_status.values())
        for malformed_boundary in (None, [], {}, "unreviewed"):
            with pytest.raises(ValueError, match="boundary_invalid"):
                await verify_registry_targets(session, targets, schema=schema, boundary=malformed_boundary)
        with pytest.raises(ValueError, match="identity_target_invalid"):
            await verify_registry_targets(
                session, [_target("group", "edit", uuid4())], schema=schema, boundary="identity"
            )


@pytest.mark.asyncio
async def test_source_network_binding_publication_has_an_independent_uuid_target(record_db):
    """An organization, network or site grant cannot stand in for a source binding."""
    connection, schema, sessions = record_db
    company_id, _, _ = await _seed_registry(record_db)
    networks = await _seed(connection, schema)
    binding_row = _row(networks[0], binding_id=str(company_id))
    await _apply(connection, schema, _command([binding_row]), _actor())
    requested_targets = [
        _target("company", "publish", company_id),
        _target("network_binding", "publish", company_id),
        _target("site_binding", "publish", company_id),
        _target("network_binding", "publish", uuid4()),
    ]
    statements = []
    engine = sessions.kw["bind"]

    def count_statement(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        async with sessions() as session:
            verified_targets = await verify_registry_targets(session, requested_targets, schema=schema)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_statement)
    assert len(statements) == 1
    assert verified_targets[f"network_binding:publish:{company_id}"] == {"known": True, "archived": False}
    assert verified_targets[f"company:publish:{company_id}"] == {"known": True, "archived": True}
    assert verified_targets[f"site_binding:publish:{company_id}"] == {"known": False, "archived": False}
    assert sum(flags["known"] for flags in verified_targets.values()) == 2
    await _apply(
        connection,
        schema,
        _command([{**binding_row, "operation": "close", "expected_revision": 1, "expected_network_id": networks[0]}]),
        _actor(),
    )
    async with sessions() as session:
        verified_targets = await verify_registry_targets(session, requested_targets, schema=schema)
    assert verified_targets[f"network_binding:publish:{company_id}"] == {"known": True, "archived": True}


@pytest.mark.asyncio
async def test_exact_batch_bound_empty_and_duplicate_targets(record_db):
    _, schema, sessions = record_db
    targets = [_target("network", "edit", number) for number in range(1, 101)]
    async with sessions() as session:
        assert await verify_registry_targets(session, [], schema=schema) == {}
        assert len(await verify_registry_targets(session, targets, schema=schema)) == 100
        assert all(
            not status["known"] for status in (await verify_registry_targets(session, targets, schema=schema)).values()
        )
        assert await verify_registry_targets(session, targets[:1] * 2, schema=schema) == {
            "network:edit:1": {"known": False, "archived": False}
        }
        with pytest.raises(ValueError, match="batch_invalid"):
            await verify_registry_targets(session, targets + targets[:1], schema=schema)


@pytest.mark.parametrize(
    "invalid",
    [
        _target("network", "edit", "0"),
        _target("network", "edit", "-1"),
        _target("network", "edit", "+1"),
        _target("network", "edit", "01"),
        _target("network", "edit", "2147483648"),
        _target("network", "edit", "1.0"),
        _target("network", "edit", "１"),
        _target("network", "edit", "42,43"),
        _target("network", "edit", uuid4()),
        _target("group", "edit", "42"),
        _target("group", "edit", ""),
        _target("group", "edit", uuid4().hex),
        _target("company", "edit", str(uuid4()).upper()),
        _target("network", "create", "root"),
        _target("network", "create", "root:unexpected"),
        _target("network", "create", "network:42"),
        _target("network", "create", "group:not-a-uuid"),
        {"record_kind": "network", "action": "edit", "target_key": True},
        {"record_kind": [], "action": "edit", "target_key": "42"},
        {"record_kind": "network", "action": {}, "target_key": "42"},
        {**_target("network", "edit", "42"), "client_id": "caller-supplied"},
        _target("checksum", "edit", "42"),
        _target("network", "read", "42"),
    ],
)
@pytest.mark.asyncio
async def test_malformed_typed_key_rejects_whole_batch_before_any_sql(record_db, invalid):
    _, schema, sessions = record_db
    statements = []
    engine = sessions.kw["bind"]

    def count_statement(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", count_statement)
    try:
        async with sessions() as session:
            with pytest.raises(ValueError):
                await verify_registry_targets(session, [_target("network", "edit", "42"), invalid], schema=schema)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", count_statement)
    assert statements == []
