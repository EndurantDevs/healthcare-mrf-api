# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in restricted native coordinator; only upstream HTTP authority is synthetic."""

import asyncio
import json
import os
from copy import deepcopy
from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_ptg_office_coordinator as coordinator
from process.ptg_parts.ptg2_candidate_attestation import shared_source_set_metadata
from process.ptg_parts.result_archive_published_identity import validate_published_result_identity
from process.registry_management_permissions import _TABLES, install_registry_management_permissions
from process.registry_ptg_office_custody import RegistryPTGOfficeCustodyProfile
from process.registry_ptg_office_preparation import RegistryPTGOfficePreparationService
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from tests import test_registry_ptg_office_capture_postgres as office_fixtures
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_ptg2_candidate_attestation import _audit_sample, _source_witness
from tests.test_registry_ptg_office_capture_postgres import serving_schema as serving_schema
from tests.test_registry_ptg_office_preparation_postgres import _approved_site
from tests.test_registry_ptg_scope_engine_postgres import (
    _app_authority,
    _approve_published_association,
    _migration,
    _published_lock_provision,
    _published_review_setup,
    _seed_approved_company,
)
from tests.test_result_archive_published_identity import _published_row


def _registered(journal, document):
    """Persist exact fixture cleanup metadata without credentials before mutation."""
    with journal.open("w") as output:
        json.dump(document, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())


def _published_dense_headers(source_snapshot):
    """Seed published headers for the same real packed graph and full source set."""
    header_by_field = _published_row()
    identity, retained = source_snapshot.identity_by_field, source_snapshot.source_fixture
    source_set = shared_source_set_metadata(
        [source_assignment["raw_container_sha256"] for source_assignment in retained.sources]
    )
    header_by_field.update(
        snapshot_id=source_snapshot.specification.snapshot_id,
        import_run_id="ptg2:source-file-import-001",
        run_source_key=retained.binding["source_key"],
        snapshot_key=identity["snapshot_key"],
        raw_container_sha256_values=[
            bytes.fromhex(source_assignment["raw_container_sha256"]) for source_assignment in retained.sources
        ],
        source_assignments=deepcopy(retained.sources),
        layout_mapping_digest=bytes.fromhex(identity["layout_mapping_sha256"]),
        map_digest=bytes.fromhex(identity["map_sha256"]),
        v4_root_map_digest=bytes.fromhex(identity["map_sha256"]),
        finalizer_map_digest=bytes.fromhex(identity["finalizer_map_sha256"]),
        plan_id="native-office-plan",
        plan_scopes=[["native-office-plan", "group"]],
    )
    header_by_field["manifest"]["activation"]["source_key"] = header_by_field["run_source_key"]
    for manifest in (header_by_field["manifest"], header_by_field["layout_manifest"]):
        serving = manifest["serving_index"]
        serving.update(
            shared_snapshot_key=header_by_field["snapshot_key"],
            snapshot_map={"map_digest": identity["map_sha256"]},
            source_witness=_source_witness(source_set),
            audit_sample={**_audit_sample("ab" * 32), "source_count": source_set["source_count"]},
            provider_graph={
                "provider_tax_identity_source": retained.publication.as_dict(),
                "provider_tax_identity": retained.aggregate_by_field,
            },
        )
    header_by_field["manifest"]["serving_index"]["source_set"] = source_set
    header_by_field["layout_manifest"]["serving_index"]["source_count"] = source_set["source_count"]
    validate_published_result_identity(header_by_field)
    return header_by_field


async def _publish_fixture_headers(fixture):
    """Change only this synthetic fixture's headers; validate actual source again later."""
    source_snapshot = fixture.source.source
    headers = _published_dense_headers(source_snapshot)
    schema = source_snapshot.schema
    async with fixture.source.engine.begin() as connection:
        await connection.execute(
            text(f"DELETE FROM {schema}.ptg2_frozen_source_file_binding WHERE internal_run_id=:run"),
            {"run": headers["import_run_id"]},
        )
        await connection.execute(
            text(f"INSERT INTO {schema}.ptg2_import_run VALUES(:run,:month,CAST(:options AS jsonb))"),
            {
                "run": headers["import_run_id"],
                "month": date(2026, 9, 1),
                "options": json.dumps({"source_key": headers["run_source_key"]}),
            },
        )
        await connection.execute(
            text(
                f"UPDATE {schema}.ptg2_snapshot SET status='published',import_month=:month,manifest=CAST(:manifest AS jsonb) WHERE snapshot_id=:snapshot"
            ),
            {
                "snapshot": headers["snapshot_id"],
                "month": date(2026, 9, 1),
                "manifest": json.dumps(headers["manifest"]),
            },
        )
        await connection.execute(
            text(
                f"UPDATE {schema}.ptg2_v3_snapshot_layout SET layout_manifest=CAST(:manifest AS jsonb) WHERE snapshot_key=:key"
            ),
            {"key": headers["snapshot_key"], "manifest": json.dumps(headers["layout_manifest"])},
        )
        await connection.execute(
            text(f"INSERT INTO {schema}.ptg2_v3_snapshot_scope VALUES(:snapshot,:plan,'group',:coverage)"),
            {"snapshot": headers["snapshot_id"], "plan": headers["plan_id"], "coverage": headers["coverage_scope_id"]},
        )
        await connection.execute(
            text(f"INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope VALUES(:snapshot,:plan,'group')"),
            {"snapshot": headers["snapshot_id"], "plan": headers["plan_id"]},
        )
    return headers


@pytest.fixture
async def office_publisher(custom_db, tmp_path):
    """Register the eventual Publisher before sealing its retained reader roster."""
    role = "scope_native_approver_" + uuid4().hex
    password = uuid4().hex
    journal = tmp_path / "office-coordinator-publisher-cleanup.json"
    _registered(journal, {"phase": "registered", "roles": [role], "schemas": []})
    is_created = False
    try:
        await custom_db.connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        is_created = True
        yield SimpleNamespace(role=role, password=password)
    finally:
        if is_created:
            await custom_db.connection.execute(f'DROP OWNED BY "{role}"')
            await custom_db.connection.execute(f'DROP ROLE "{role}"')
        _registered(journal, {"phase": "removed", "roles": [role], "schemas": []})


@pytest.fixture
async def office_db(custom_db, tmp_path, monkeypatch, office_publisher):
    """The real pipeline seals the Publisher as a reader before publication."""
    original_roles = office_fixtures._roles

    def roles_with_publisher(fixture):
        roles = original_roles(fixture)
        return {**roles, "reader_roles": (*roles["reader_roles"], office_publisher.role)}

    monkeypatch.setattr(office_fixtures, "_roles", roles_with_publisher)
    fixture = office_fixtures.complete_office_db.__wrapped__(custom_db, tmp_path, monkeypatch)
    try:
        yield await anext(fixture)
    finally:
        await fixture.aclose()


async def _grant_reads(connection, schema, roles, tables):
    """Grant SELECT on the named existing fixture relations only."""
    for role in roles:
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        for table in tables:
            await connection.execute(f'GRANT SELECT ON "{schema}"."{table}" TO "{role}"')


async def _prepare_control(fixture, custom, roles):
    connection, schema = custom.connection, custom.control_schema
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{custom.roles["owner"]}"')
    async with connection.transaction():
        await install_registry_management_permissions(
            connection, api_role=roles["management"], owner_role=custom.roles["owner"], control_schema=schema
        )
        for statement in _migration("20261009010000_registry_ptg_producer_scope.py")._ddl(schema):
            await connection.execute(statement)
        table = f'"{schema}".registry_ptg_producer_scope'
        await connection.execute(f'ALTER TABLE {table} OWNER TO "{custom.roles["owner"]}"')
        await connection.execute(f'GRANT SELECT,INSERT ON {table} TO "{fixture.source.roles[1]}"')
        await connection.execute(f'GRANT SELECT ON {table} TO "{fixture.source.roles[2]}"')
    await _grant_reads(
        connection, schema, (fixture.source.roles[1], fixture.source.roles[2], roles["publisher"]), sorted(_TABLES)
    )
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles["queue"]}"')
    await connection.execute(f'GRANT SELECT ON "{schema}".registry_publication_request TO "{roles["queue"]}"')
    await connection.execute(
        f'GRANT UPDATE(state,lease_id,lease_expires_at,result_json,updated_at) ON "{schema}".registry_publication_request TO "{roles["queue"]}"'
    )
    for role in (roles["management"], roles["queue"], fixture.source.roles[2]):
        await connection.execute(f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "{role}"')


async def _source_review(fixture, custom, roles, tmp_path):
    document = deepcopy(fixture.source.document)
    view = SimpleNamespace(
        engine=custom.engine,
        control=custom.control_schema,
        document=document,
        roles=(custom.roles["owner"], fixture.source.roles[1], fixture.source.roles[2]),
    )
    await _seed_approved_company(custom.engine, custom.control_schema, document)
    _, binding, _ = await _approved_site(fixture, custom)
    network, revision = await _approve_published_association(view)
    service = replace(
        fixture.source.service,
        store=RegistryPTGProducerScopeStore(custom.roles["owner"], fixture.source.roles[1], custom.control_schema),
        office_custody=RegistryPTGOfficeCustodyProfile(fixture.roles[0], roles["publisher"]),
    )
    view.service = service
    _, _, intent, preview_request = await _published_review_setup(
        view, tmp_path, document["ownership"], network, revision
    )
    intent.update(
        plan_id="native-office-plan",
        file_versions=sorted(
            [
                {
                    "source_file_version_id": file_record.descriptor["engine_source_file_version_id"],
                    "source_identity_sha256": file_record.descriptor["engine_source_identity_hash"],
                    "raw_sha256": file_record.descriptor["raw_sha256"],
                }
                for file_record in fixture.source.source.source_fixture.records
            ],
            key=lambda file_record: file_record["source_file_version_id"],
        ),
    )
    preview_request["command"] = intent
    await custom.connection.execute(
        f'GRANT SELECT ON "{custom.control_schema}".registry_ptg_published_plan_scope TO "{roles["publisher"]}"'
    )
    preview = await service.preview(preview_request)
    approval = await service.approve({name: document[name] for name in ("actor", "session_token_sha256")} | preview)
    return service, {name: document[name] for name in ("actor", "session_token_sha256")} | {
        "source_command": preview["command"],
        "selector": {
            "scope_id": approval["scope_id"],
            "client_id": document["command"]["client_id"],
            "scope_approval_sha256": approval["approval_sha256"],
            "retained_generation_id": fixture.serving.generation_id,
            "reason": "Reviewed exact native office",
            "selections": [
                {
                    "binding_id": binding["record_id"],
                    "binding_revision": binding["revision"],
                    "dense_source_key": 0,
                    "source_record_ordinal": 0,
                }
            ],
        },
        "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 32, "maximum_coordinates": 100},
    }


async def _publisher_rights(fixture, custom, roles):
    connection, source = custom.connection, fixture.source.source.name
    await connection.execute(f'REVOKE "{fixture.roles[0]}" FROM "{fixture.source.roles[2]}"')
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{fixture.source.roles[2]}"')
    await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{roles["publisher"]}","{fixture.roles[0]}"')
    await connection.execute(f'GRANT "{fixture.roles[0]}" TO "{roles["publisher"]}" WITH INHERIT FALSE, SET TRUE')
    tables = await connection.fetch(
        "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=pg_catalog.to_regnamespace($1) AND relkind='r'",
        source,
    )
    await _grant_reads(connection, source, (roles["publisher"],), [item["relname"] for item in tables])
    for table in (
        "ptg2_snapshot",
        "ptg2_import_run",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_layout",
        "ptg2_snapshot_pin",
    ):
        await connection.execute(
            f'GRANT UPDATE(registry_ptg_read_lock) ON "{source}"."{table}" TO "{roles["publisher"]}"'
        )
    await _grant_reads(connection, custom.source_schema, (roles["publisher"],), ("entity_address_unified", "npi"))


async def _open_coordinator(fixture, custom, roles, password, scope, resources):
    """Install actual distinct restricted login factories without execution fallback."""
    for role in (roles["management"], roles["publisher"]):
        resources.engines.append(
            create_async_engine(
                custom.engine.url.set(username=role, password=password),
                pool_size=1,
                max_overflow=0,
                hide_parameters=True,
            )
        )
    resources.pool = await asyncpg.create_pool(
        custom.engine.url.set(
            drivername="postgresql", username=roles["queue"], password=fixture.consumer_password
        ).render_as_string(hide_password=False),
        min_size=1,
        max_size=2,
    )
    physical = RegistryPTGOfficePreparationService(scope, async_sessionmaker(resources.engines[1]))
    service = coordinator.RegistryPTGOfficeIntentCoordinator(
        physical,
        async_sessionmaker(resources.engines[0]),
        resources.pool,
        roles["management"],
        roles["queue"],
        custom.roles["owner"],
    )
    return service


def _observe_handoffs(fixture, custom, roles, journal, monkeypatch):
    """Register capture cleanup, then call the original genuine preparation/read."""
    actual_prepare, actual_read = (
        RegistryPTGOfficePreparationService.prepare,
        RegistryPTGOfficePreparationService.read,
    )
    calls, acknowledgments = [], SimpleNamespace(lose_next=False)

    async def registered_prepare(installed, envelope, *, deadline=None):
        schema = "registry_ptg_office_" + UUID(envelope["selector"]["capture_id"]).hex
        if schema not in fixture.schemas:
            assert await custom.connection.fetchval("SELECT pg_catalog.to_regnamespace($1)", schema) is None
            fixture.schemas.append(schema)
            _registered(journal, {"phase": "registered", "roles": list(roles.values()), "schemas": fixture.schemas})
        record = await custom.connection.fetchrow(
            f"SELECT state FROM \"{custom.control_schema}\".registry_publication_request WHERE command_json->'selector'->>'capture_id'=$1",
            envelope["selector"]["capture_id"],
        )
        assert record["state"] == "running"
        calls.append("genuine_prepare")
        reply = await actual_prepare(installed, envelope, deadline=deadline)
        if acknowledgments.lose_next:
            acknowledgments.lose_next = False
            raise TimeoutError
        return reply

    async def observed_read(installed, envelope, *, deadline=None):
        calls.append("genuine_read")
        return await actual_read(installed, envelope, deadline=deadline)

    monkeypatch.setattr(RegistryPTGOfficePreparationService, "prepare", registered_prepare)
    monkeypatch.setattr(RegistryPTGOfficePreparationService, "read", observed_read)
    return calls, acknowledgments


async def _cleanup_coordinator(fixture, custom, roles, created, resources, journal):
    """Close exact owned connections and revoke only this fixture's added grants."""
    if resources.pool is not None:
        await resources.pool.close()
    for engine in resources.engines:
        await engine.dispose()
    for role in (roles["queue"], fixture.source.roles[2], fixture.source.roles[1]):
        await custom.connection.execute(f'REVOKE ALL ON SCHEMA "{custom.control_schema}" FROM "{role}"')
        await custom.connection.execute(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{custom.control_schema}" FROM "{role}"')
        if role == roles["queue"]:
            await custom.connection.execute(
                f'REVOKE UPDATE(state,lease_id,lease_expires_at,result_json,updated_at) ON "{custom.control_schema}".registry_publication_request FROM "{role}"'
            )
        await custom.connection.execute(f'REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM "{role}"')
    for role in reversed(created):
        await custom.connection.execute(f'DROP OWNED BY "{role}"')
        await custom.connection.execute(f'DROP ROLE "{role}"')
    _registered(
        journal,
        {
            "phase": "extra_roles_removed_parent_owns_schemas",
            "roles": list(roles.values()),
            "schemas": fixture.schemas,
        },
    )


@pytest.fixture
async def coordinator_db(office_db, custom_db, tmp_path, monkeypatch, office_publisher):
    """Register supported exact role families; execution never SETs an admin role."""
    fixture, custom = office_db, custom_db
    roles_by_kind = {
        "management": "scope_native_reader_" + uuid4().hex,
        "publisher": office_publisher.role,
        "queue": fixture.roles[1],
    }
    password = office_publisher.password
    journal = tmp_path / "office-coordinator-cleanup-registration.json"
    _registered(journal, {"phase": "registered", "roles": list(roles_by_kind.values()), "schemas": fixture.schemas})
    created, resources = [], SimpleNamespace(engines=[], pool=None)
    app = _app_authority(monkeypatch)
    try:
        for role in (roles_by_kind["management"],):
            assert not await custom.connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname=$1)", role
            )
            await custom.connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
            created.append(role)
        await _publish_fixture_headers(fixture)
        await _prepare_control(fixture, custom, roles_by_kind)
        async with _published_lock_provision(fixture.source, tmp_path):
            await _publisher_rights(fixture, custom, roles_by_kind)
            scope, request = await _source_review(fixture, custom, roles_by_kind, tmp_path)
            service = await _open_coordinator(fixture, custom, roles_by_kind, password, scope, resources)
            calls, acknowledgments = _observe_handoffs(fixture, custom, roles_by_kind, journal, monkeypatch)
            yield SimpleNamespace(
                service=service,
                request=request,
                fixture=fixture,
                custom=custom,
                roles_by_kind=roles_by_kind,
                app=app,
                calls=calls,
                acknowledgments=acknowledgments,
            )
    finally:
        await _cleanup_coordinator(fixture, custom, roles_by_kind, created, resources, journal)


@pytest.mark.asyncio
async def test_native_coordinator_commits_then_prepares_and_replays_original_audit(coordinator_db):
    native = coordinator_db
    async with native.service.queue_pool.acquire() as connection:
        assert await connection.fetchval(
            "SELECT current_user=session_user AND current_user=$1", native.roles_by_kind["queue"]
        )
        identity = await coordinator._native_identity(connection)
        assert identity == await coordinator._native_identity(native.custom.connection)
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(
                    f'UPDATE "{native.custom.control_schema}".registry_publication_request SET command_json=command_json'
                )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f'SET LOCAL ROLE "{native.custom.roles["owner"]}"')
    reply = await native.service.prepare(native.request)
    assert reply["intent"]["state"] == "completed" and reply["preparation"]["state"] == "prepared"
    namespace = '"' + native.custom.control_schema + '"'
    original_by_field = dict(
        await native.custom.connection.fetchrow(f"SELECT * FROM {namespace}.registry_publication_request")
    )
    renewed = deepcopy(native.request)
    renewed["session_token_sha256"] = "b" * 64
    replay = await native.service.prepare(renewed)
    assert (
        replay["preparation"] == reply["preparation"]
        and replay["intent"]["request_id"] == reply["intent"]["request_id"]
    )
    current_by_field = dict(
        await native.custom.connection.fetchrow(f"SELECT * FROM {namespace}.registry_publication_request")
    )
    assert (
        current_by_field == original_by_field
        and current_by_field["session_token_sha256"] == native.request["session_token_sha256"]
    )
    assert native.calls == ["genuine_prepare", "genuine_read"]
    assert any(authority_request["session_token_sha256"] == "b" * 64 for authority_request in native.app.envelopes)
    assert (
        await native.custom.connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_publication_request") == 1
    )


@pytest.mark.asyncio
async def test_native_competing_exact_claim_and_expired_lease_keep_one_capture(coordinator_db):
    native = coordinator_db
    request, actor, semantic = coordinator._envelope(native.request)
    deadline = coordinator._deadline(None)
    identity = await coordinator._approved_source(native.service, request, deadline)
    receipt = await coordinator._reserve(native.service, request, actor, identity, deadline, {"commit_started": False})
    claims = await asyncio.gather(
        coordinator._claim(native.service, receipt, semantic, identity),
        coordinator._claim(native.service, receipt, semantic, identity),
    )
    assert sum(lease is not None for _, lease, _ in claims) == 1
    assert not native.calls
    pending = await native.service.prepare(request)
    assert pending["preparation"] is None and pending["intent"]["request_id"] == receipt["request_id"]
    async with native.service.queue_pool.acquire() as connection:
        await connection.execute(
            f"""UPDATE "{native.custom.control_schema}".registry_publication_request SET lease_expires_at=pg_catalog.clock_timestamp()-interval '1 second' WHERE request_id=$1""",
            UUID(receipt["request_id"]),
        )
    native.acknowledgments.lose_next = True
    with pytest.raises(coordinator.RegistryPTGScopeDeadlineExpired) as unknown:
        await native.service.prepare(request)
    assert unknown.value.outcome_unknown
    assert (
        await native.custom.connection.fetchval(
            "SELECT pg_catalog.to_regnamespace($1)",
            "registry_ptg_office_" + UUID(receipt["command"]["selector"]["capture_id"]).hex,
        )
        is not None
    )
    renewed = deepcopy(request)
    renewed["session_token_sha256"] = "b" * 64
    assert (await native.service.prepare(renewed))["preparation"] is None
    async with native.service.queue_pool.acquire() as connection:
        await connection.execute(
            f"""UPDATE "{native.custom.control_schema}".registry_publication_request SET lease_expires_at=pg_catalog.clock_timestamp()-interval '1 second' WHERE request_id=$1""",
            UUID(receipt["request_id"]),
        )
    recovered = await native.service.prepare(renewed)
    assert recovered["intent"]["request_id"] == receipt["request_id"]
    assert recovered["preparation"]["capture_id"] == receipt["command"]["selector"]["capture_id"]
    assert recovered["intent"]["state"] == "completed" and native.calls == ["genuine_prepare", "genuine_prepare"]
    assert (
        await native.custom.connection.fetchval(
            f'SELECT count(*) FROM "{native.custom.control_schema}".registry_publication_request'
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe", ["owner_set", "immutable_update"])
async def test_native_queue_profile_refuses_before_intent_insert(coordinator_db, unsafe):
    native = coordinator_db
    queue, owner = native.roles_by_kind["queue"], native.custom.roles["owner"]
    if unsafe == "owner_set":
        await native.custom.connection.execute(f'GRANT "{owner}" TO "{queue}" WITH INHERIT FALSE, SET TRUE')
    else:
        await native.custom.connection.execute(
            f'GRANT UPDATE(command_json) ON "{native.custom.control_schema}".registry_publication_request TO "{queue}"'
        )
    try:
        with pytest.raises(ValueError, match="queue_permissions"):
            await native.service.prepare(native.request)
        assert not native.calls
        assert (
            await native.custom.connection.fetchval(
                f'SELECT count(*) FROM "{native.custom.control_schema}".registry_publication_request'
            )
            == 0
        )
    finally:
        if unsafe == "owner_set":
            await native.custom.connection.execute(f'REVOKE "{owner}" FROM "{queue}"')
        else:
            await native.custom.connection.execute(
                f'REVOKE UPDATE(command_json) ON "{native.custom.control_schema}".registry_publication_request FROM "{queue}"'
            )
