# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native company assertion lifecycle and precise protected draft ACLs."""

import asyncio
import importlib.util
import json
import os
import secrets
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.company_registry_assertion_values import CONTRACT
from process.registry_approval_store import RegistryApprovalConflict, approve_registry_records
from process.registry_management_permissions import (
    install_registry_management_permissions,
    verify_registry_management_permissions,
)
from process.registry_manual_undo import RegistryManualUndoCommand, prepare_registry_manual_undo
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from tests.test_registry_approval_store_postgres import _command as approval_command
from tests.test_result_archive_published_authority_postgres import _is_guarded_test_service

pytestmark = pytest.mark.asyncio
_ACTOR = RegistryActor("platform_admin", UUID(int=100), "example-client")
_MIGRATIONS = (
    "20261007010000_managed_network_registry.py",
    "20261007020000_registry_revision_history.py",
    "20261007030000_network_serving_control.py",
    "20261007040000_registry_source_evidence.py",
    "20261007050000_canonical_address_network_ids.py",
    "20261007060000_registry_approved_selection.py",
    "20261007070000_registry_company_links.py",
    "20261007080000_manual_directory_registry.py",
    "20261007090000_network_membership_drafts.py",
    "20261007100000_registry_publication_requests.py",
    "20261007110000_registry_site_bindings.py",
    "20261007120000_company_network_assertions.py",
    "20261007130000_registry_network_bindings.py",
    "20261007140000_registry_source_recipes.py",
    "20261009020000_company_registry_assertions.py",
    "20261009030000_network_catalog_evidence.py",
)


def _write_journal(path, document):
    path.write_text(json.dumps(document, sort_keys=True, indent=2) + "\n")


async def _inventory(connection):
    schemas_by_name = dict(
        await connection.fetch(
            "SELECT nspname,oid::bigint FROM pg_namespace namespace "
            "WHERE nspname !~ '^pg_(toast_)?temp_[0-9]+$' "
            "OR EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=namespace.oid) ORDER BY nspname"
        )
    )
    roles_by_name = dict(await connection.fetch("SELECT rolname,oid::bigint FROM pg_roles ORDER BY rolname"))
    candidates_by_schema = {}
    for row in await connection.fetch(
        "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE c.relname='network_membership_candidate' AND c.relkind='r' ORDER BY n.nspname"
    ):
        namespace = '"' + row["nspname"].replace('"', '""') + '"'
        digest = await connection.fetchrow(
            f"SELECT count(*) AS count,md5(coalesce(jsonb_agg(to_jsonb(candidate) ORDER BY candidate_id)::text,'')) AS digest "
            f"FROM {namespace}.network_membership_candidate candidate"
        )
        candidates_by_schema[row["nspname"]] = dict(digest)
    return {"schemas": schemas_by_name, "roles": roles_by_name, "candidates": candidates_by_schema}


@dataclass
class AssertionDatabase:
    connection: object
    schema: str
    sessions: object
    roles_by_kind: dict

    @property
    def namespace(self):
        return '"' + self.schema + '"'

    def permission_arguments(self):
        return {
            "api_role": self.roles_by_kind["draft"],
            "owner_role": self.roles_by_kind["owner"],
            "control_schema": self.schema,
        }


async def _install(fixture):
    async with fixture.connection.transaction():
        return await install_registry_management_permissions(fixture.connection, **fixture.permission_arguments())


async def _migrate(engine, monkeypatch, schema):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", schema)
    async with engine.begin() as connection:
        for filename in _MIGRATIONS:
            path = Path(__file__).parents[1] / "alembic/versions" / filename
            spec = importlib.util.spec_from_file_location(path.stem, path)
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)

            def upgrade(sync_connection):
                with Operations.context(MigrationContext.configure(sync_connection)):
                    migration.upgrade()

            await connection.run_sync(upgrade)


def _guarded_url():
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("native PostgreSQL assertion tests require explicit opt-in")
    raw = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not raw or raw != os.getenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN"):
        pytest.fail("native assertion tests require identical explicitly guarded test DSNs")
    url = make_url(raw).set(drivername="postgresql+asyncpg")
    if not _is_guarded_test_service(url):
        pytest.fail("native assertion tests require a guarded PostgreSQL test service")
    return url


def _registration(tmp_path):
    token = uuid4().hex
    directory = Path(os.getenv("COMPANY_REGISTRY_ASSERTION_TEST_RECEIPTS", str(tmp_path)))
    directory.mkdir(parents=True, exist_ok=True)
    journal = directory / (token + ".json")
    document_by_field = {
        "schema": "company_assertion_" + token,
        "roles": {kind: "ca_" + kind + "_" + token for kind in ("owner", "draft", "publisher")},
        "application_name": "company_assertion_" + token,
        "state": "registered",
        "created_roles": [],
    }
    _write_journal(journal, document_by_field)
    return journal, document_by_field


async def _prepare_database(url, monkeypatch, journal, document_by_field, resources_by_kind):
    schema, roles_by_kind = document_by_field["schema"], document_by_field["roles"]
    settings_by_name = {"application_name": document_by_field["application_name"], "statement_timeout": "20s"}
    connection = await asyncpg.connect(
        url.set(drivername="postgresql").render_as_string(hide_password=False), server_settings=settings_by_name
    )
    resources_by_kind["connection"] = connection
    assert 180000 <= int(await connection.fetchval("SHOW server_version_num")) < 190000
    before = await _inventory(connection)
    document_by_field["before"] = before
    _write_journal(journal, document_by_field)
    assert schema not in before["schemas"] and not set(roles_by_kind.values()).intersection(before["roles"])
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    resources_by_kind["is_schema_created"] = True
    engine = create_async_engine(url, pool_size=1, max_overflow=0, connect_args={"server_settings": settings_by_name})
    resources_by_kind["engine"] = engine
    await _migrate(engine, monkeypatch, schema)
    draft_password = secrets.token_hex(32)
    for kind, role in roles_by_kind.items():
        login = f"LOGIN PASSWORD '{draft_password}'" if kind == "draft" else "NOLOGIN"
        await connection.execute(f'CREATE ROLE "{role}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS')
        document_by_field["created_roles"].append(role)
        _write_journal(journal, document_by_field)
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{roles_by_kind["owner"]}"')
    await connection.execute(f'GRANT "{roles_by_kind["owner"]}" TO "{roles_by_kind["publisher"]}"')
    draft_engine = create_async_engine(
        url.set(username=roles_by_kind["draft"], password=draft_password),
        pool_size=2,
        max_overflow=0,
        connect_args={"server_settings": settings_by_name},
    )
    resources_by_kind["draft_engine"] = draft_engine
    fixture = AssertionDatabase(connection, schema, async_sessionmaker(draft_engine), roles_by_kind)
    await _install(fixture)
    document_by_field["state"] = "ready"
    _write_journal(journal, document_by_field)
    return fixture


async def _cleanup_database(journal, document_by_field, resources_by_kind):
    connection = resources_by_kind.get("connection")
    if connection is not None:
        document_by_field["owned_pids"] = list(
            await connection.fetch(
                "SELECT pid FROM pg_stat_activity WHERE application_name=$1", document_by_field["application_name"]
            )
        )
        document_by_field["owned_pids"] = [session_record["pid"] for session_record in document_by_field["owned_pids"]]
    for kind in ("draft_engine", "engine"):
        if kind in resources_by_kind:
            await resources_by_kind[kind].dispose()
    if connection is None:
        return
    try:
        await connection.execute("RESET ROLE")
        if resources_by_kind.get("is_schema_created"):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{document_by_field["schema"]}" CASCADE')
        for role in reversed(document_by_field["created_roles"]):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')
        assert await connection.fetchval("SELECT to_regnamespace($1)", document_by_field["schema"]) is None
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))",
            list(document_by_field["roles"].values()),
        )
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND pid<>pg_backend_pid())",
            document_by_field["application_name"],
        )
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=ANY($1::int[]) AND locktype='advisory')",
            document_by_field["owned_pids"],
        )
        after = await _inventory(connection)
        if "before" in document_by_field:
            assert after == document_by_field["before"]
        document_by_field.update(state="cleanup_verified", after=after)
        _write_journal(journal, document_by_field)
    finally:
        await connection.close()


@pytest.fixture
async def assertion_db(tmp_path, monkeypatch):
    url = _guarded_url()
    journal, document_by_field = _registration(tmp_path)
    resources_by_kind = {}
    try:
        yield await _prepare_database(url, monkeypatch, journal, document_by_field, resources_by_kind)
    finally:
        await _cleanup_database(journal, document_by_field, resources_by_kind)


async def test_inventory_retains_populated_system_temp_schemas(assertion_db):
    connection = assertion_db.connection
    before = await _inventory(connection)
    table = "ca_inventory_" + uuid4().hex
    await connection.execute(f'CREATE TEMP TABLE "{table}" (value integer NOT NULL)')
    try:
        namespace = await connection.fetchval("SELECT nspname FROM pg_namespace WHERE oid=pg_my_temp_schema()")
        assert namespace in (await _inventory(connection))["schemas"]
    finally:
        await connection.execute(f'DROP TABLE "{table}"')
    assert await _inventory(connection) == before


def _role():
    return {
        "assertion_id": str(uuid4()),
        "role": "employer",
        "valid_from": "2025-01-01",
        "valid_to": None,
        "provenance": {
            "kind": "manual_reference",
            "evidence_ref": "reviewed-role-reference",
            "snapshot_id": None,
            "source_record_key": None,
        },
    }


def _identifier(**changes):
    return {
        "assertion_id": str(uuid4()),
        "identifier_system": "naic_company",
        "identifier_scope": "jurisdiction-a",
        "identifier_value": "12345",
        "valid_from": "2025-01-01",
        "valid_to": "2025-06-30",
        "provenance": {
            "kind": "manual_reference",
            "evidence_ref": "reviewed-identifier-reference",
            "snapshot_id": None,
            "source_record_key": None,
        },
    } | changes


def _create(*, identifiers=(), assertions=True):
    company_id = uuid4()
    fields_by_name = {"display_name": "Example Company", "roles": ["employer"], "aliases": []}
    if assertions:
        fields_by_name["assertions"] = {
            "contract": CONTRACT,
            "company_id": str(company_id),
            "expected_revision": 0,
            "role_assertions": [_role()],
            "identifier_assertions": list(identifiers),
        }
    return RegistryRecordCommand(
        "company", company_id, "create", 0, fields_by_name, "Reviewed company draft", uuid4().hex
    )


async def _draft(fixture, command):
    async with fixture.sessions() as session, session.begin():
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["draft"]
        return await apply_registry_record_command(session, command, _ACTOR, schema=fixture.schema)


async def _approve(fixture, *records):
    connection = fixture.connection
    command = await approval_command(connection, fixture.schema, *records)
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["publisher"]}"')
        return await approve_registry_records(connection, command, _ACTOR, control_schema=fixture.schema)


async def _accounting(fixture):
    return await fixture.connection.fetchrow(
        f"SELECT (SELECT count(*) FROM {fixture.namespace}.company_registry) AS heads,"
        f"(SELECT count(*) FROM {fixture.namespace}.company_registry_role_assertion) AS roles,"
        f"(SELECT count(*) FROM {fixture.namespace}.company_registry_identifier_assertion) AS identifiers,"
        f"(SELECT count(*) FROM {fixture.namespace}.registry_record_history) AS history,"
        f"(SELECT draft_revision FROM {fixture.namespace}.registry_revision_control WHERE id=1) AS draft"
    )


async def test_manual_company_assertions_need_no_regulatory_identifiers(assertion_db):
    fixture = assertion_db
    command = _create()
    created = await _draft(fixture, command)
    assert created["record"]["identifier_assertions"] == []
    assert created["record"]["role_assertions"] == command.fields["assertions"]["role_assertions"]
    async with fixture.sessions() as session:
        read = await get_registry_record(session, "company", command.record_id, schema=fixture.schema)
        assert read == created["record"]
        listed = await list_registry_records(session, "company", schema=fixture.schema)
        assert listed == [read]
    approved = await _approve(fixture, created)
    document = json.loads(
        await fixture.connection.fetchval(
            f"SELECT record_json::text FROM {fixture.namespace}.registry_approved_record WHERE approved_revision=$1",
            approved["approved_revision"],
        )
    )
    assert document == created["record"]


@pytest.mark.parametrize(
    "second",
    [
        {"valid_from": "2025-06-30"},
        {"valid_from": "2025-01-01", "valid_to": "2025-01-01"},
        {"valid_from": "2024-01-01", "valid_to": None},
    ],
)
async def test_inclusive_current_draft_identifier_period_conflicts(assertion_db, second):
    await _draft(assertion_db, _create(identifiers=[_identifier()]))
    before = await _accounting(assertion_db)
    with pytest.raises(RegistryRecordConflict, match="assertion_target_conflict"):
        await _draft(assertion_db, _create(identifiers=[_identifier(**second)]))
    assert await _accounting(assertion_db) == before


@pytest.mark.parametrize(
    "second",
    [
        {"valid_from": "2025-07-01", "valid_to": None},
        {"valid_from": "2024-01-01", "valid_to": "2024-12-31"},
        {"identifier_scope": "jurisdiction-b"},
        {"identifier_value": "54321"},
    ],
)
async def test_disjoint_period_scope_and_value_are_distinct(assertion_db, second):
    await _draft(assertion_db, _create(identifiers=[_identifier()]))
    await _draft(assertion_db, _create(identifiers=[_identifier(**second)]))
    assert (await _accounting(assertion_db))["identifiers"] == 2


async def test_retained_approved_map_prevents_claim_after_draft_removal(assertion_db):
    fixture = assertion_db
    original = _create(identifiers=[_identifier()])
    created = await _draft(fixture, original)
    await _approve(fixture, created)
    removal = replace(
        original,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields=original.fields
        | {"assertions": original.fields["assertions"] | {"expected_revision": 1, "identifier_assertions": []}},
    )
    corrected = await _draft(fixture, removal)
    with pytest.raises(RegistryRecordConflict, match="assertion_target_conflict"):
        await _draft(fixture, _create(identifiers=[_identifier()]))
    await _approve(fixture, corrected)
    assert (await _draft(fixture, _create(identifiers=[_identifier()])))["revision"] == 1


async def _source(fixture, *, status="accepted", resolution="unresolved", company=None, present=True):
    snapshot = uuid4()
    await fixture.connection.execute(
        f"INSERT INTO {fixture.namespace}.registry_source_snapshot "
        "(snapshot_id,source_system,source_id,edition_id,source_url,artifact_sha256,input_sha256,parser_version) "
        "VALUES($1,'example','source-a','edition-a','https://example.invalid/source',$2,$2,'parser-a')",
        snapshot,
        "a" * 64,
    )
    if present:
        await fixture.connection.execute(
            f"INSERT INTO {fixture.namespace}.registry_source_observation VALUES($1,'record-a',1,$2,'{{}}'::jsonb,'[]'::jsonb)",
            snapshot,
            status,
        )
        await fixture.connection.execute(
            f"INSERT INTO {fixture.namespace}.registry_identifier_observation "
            "(snapshot_id,source_record_key,identifier_system,identifier_value,entity_kind,raw_value,entity_id,resolution_status) "
            "VALUES($1,'record-a','naic_company','12345','company','12345',$2,$3)",
            snapshot,
            company,
            resolution,
        )
    return {
        "kind": "source_reference",
        "evidence_ref": "source-row-reference",
        "snapshot_id": str(snapshot),
        "source_record_key": "record-a",
    }


@pytest.mark.parametrize("status", ["accepted", "unresolved"])
async def test_exact_source_observation_and_identifier_corroboration(assertion_db, status):
    provenance = await _source(assertion_db, status=status)
    command = _create(identifiers=[_identifier(provenance=provenance)])
    assert (await _draft(assertion_db, command))["record"]["identifier_assertions"][0]["provenance"] == provenance


@pytest.mark.parametrize(
    "case", ["missing_snapshot", "missing_row", "rejected", "conflicting", "other_company", "wrong_value"]
)
async def test_source_reference_or_identifier_conflicts_refuse_whole_command(assertion_db, case):
    fixture = assertion_db
    if case == "missing_snapshot":
        provenance_by_field = {
            "kind": "source_reference",
            "evidence_ref": "source-row-reference",
            "snapshot_id": str(uuid4()),
            "source_record_key": "record-a",
        }
    else:
        provenance_by_field = await _source(
            fixture,
            present=case != "missing_row",
            status="rejected" if case == "rejected" else "accepted",
            resolution="conflicting"
            if case == "conflicting"
            else "resolved"
            if case == "other_company"
            else "unresolved",
            company=uuid4() if case == "other_company" else None,
        )
    command = _create(
        identifiers=[
            _identifier(provenance=provenance_by_field, identifier_value="54321" if case == "wrong_value" else "12345")
        ]
    )
    before = await _accounting(fixture)
    with pytest.raises(RegistryRecordConflict, match="assertion_target_conflict"):
        await _draft(fixture, command)
    assert await _accounting(fixture) == before


async def test_opaque_source_reference_does_not_require_regulatory_identity(assertion_db):
    provenance = await _source(assertion_db)
    command = _create(
        identifiers=[
            _identifier(identifier_system="external_reference", identifier_value="opaque-a", provenance=provenance)
        ]
    )
    result = await _draft(assertion_db, command)
    assert result["record"]["identifier_assertions"][0]["identifier_scope"] == "jurisdiction-a"


async def test_legacy_correction_archive_restore_and_exact_historical_replay(assertion_db):
    fixture = assertion_db
    command = _create(identifiers=[_identifier()])
    initial = await _draft(fixture, command)
    for revision, operation in enumerate(("correct", "archive", "restore"), 1):
        fields = (
            {key: command.fields[key] for key in ("display_name", "roles", "aliases")} if operation == "correct" else {}
        )
        updated = await _draft(
            fixture,
            replace(
                command, operation=operation, expected_revision=revision, fields=fields, idempotency_key=uuid4().hex
            ),
        )
        assert updated["record"]["identifier_assertions"] == initial["record"]["identifier_assertions"]
    assert await _draft(fixture, command) == initial
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(
            fixture,
            replace(
                command,
                operation="correct",
                expected_revision=1,
                fields={key: command.fields[key] for key in ("display_name", "roles", "aliases")},
                idempotency_key=uuid4().hex,
            ),
        )
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _draft(fixture, replace(command, reason="changed command"))
    assert (await _accounting(fixture))["history"] == 4


async def test_outer_rollback_and_post_head_privilege_failure_preserve_state(assertion_db):
    fixture = assertion_db
    before = await _accounting(fixture)
    async with fixture.sessions() as session:
        await session.begin()
        await apply_registry_record_command(
            session, _create(identifiers=[_identifier()]), _ACTOR, schema=fixture.schema
        )
        await session.rollback()
    assert await _accounting(fixture) == before
    columns = ",".join(
        '"' + name + '"'
        for name in (
            "record_kind",
            "record_key",
            "revision",
            "custom_revision",
            "record_json",
            "actor_json",
            "reason",
            "idempotency_key",
            "request_sha256",
        )
    )
    await fixture.connection.execute(
        f'REVOKE INSERT({columns}) ON {fixture.namespace}.registry_record_history FROM "{fixture.roles_by_kind["draft"]}"'
    )
    try:
        with pytest.raises(Exception) as failure:
            await _draft(fixture, _create(identifiers=[_identifier()]))
        assert "permission denied" in str(failure.value).lower()
        assert await _accounting(fixture) == before
    finally:
        await _install(fixture)
    assert await verify_registry_management_permissions(fixture.connection, **fixture.permission_arguments())


async def test_legacy_empty_history_rejects_populated_assertion_rows(assertion_db):
    fixture = assertion_db
    created = await _draft(fixture, _create(assertions=False))
    # Retained historical legacy documents may omit optional fields entirely.
    await fixture.connection.execute(
        f"UPDATE {fixture.namespace}.registry_record_history SET record_json=record_json-'role_assertions'-'identifier_assertions'"
    )
    await _approve(fixture, created)
    role = _role()
    await fixture.connection.execute(
        f"INSERT INTO {fixture.namespace}.company_registry_role_assertion "
        "(assertion_id,company_id,company_revision,role,valid_from,provenance_kind,evidence_ref) "
        "VALUES($1,$2,1,'employer','2025-01-01','manual_reference','reviewed-role-reference')",
        UUID(role["assertion_id"]),
        UUID(created["record_id"]),
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(fixture, created)


async def test_assertion_draft_role_has_insert_only_effective_privileges(assertion_db):
    fixture = assertion_db
    await _draft(fixture, _create(identifiers=[_identifier()]))
    async with fixture.sessions() as session:
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["draft"]
        assert (
            await session.scalar(
                text("SELECT pg_has_role(current_user,:owner,'MEMBER')"), {"owner": fixture.roles_by_kind["owner"]}
            )
            is False
        )
        for table in ("company_registry_role_assertion", "company_registry_identifier_assertion"):
            for statement in (
                f"UPDATE {fixture.namespace}.{table} SET evidence_ref=evidence_ref",
                f"DELETE FROM {fixture.namespace}.{table}",
                f"TRUNCATE {fixture.namespace}.{table}",
                f"INSERT INTO {fixture.namespace}.{table}(created_at) VALUES(now())",
            ):
                await _assert_forbidden(session, statement)


async def test_two_companies_cannot_concurrently_claim_same_scoped_identifier(assertion_db):
    results = await asyncio.wait_for(
        asyncio.gather(
            *[_draft(assertion_db, _create(identifiers=[_identifier()])) for _ in range(2)], return_exceptions=True
        ),
        15,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    assert sum(isinstance(result, RegistryRecordConflict) for result in results) == 1
    assert (await _accounting(assertion_db))["heads"] == 1


async def _assert_forbidden(session, statement):
    with pytest.raises(Exception) as failure:
        async with session.begin_nested():
            await session.execute(text(statement))
    assert "permission denied" in str(failure.value).lower()


async def test_exact_company_approval_replay_returns_original_retained_selection(assertion_db):
    fixture = assertion_db
    created = await _draft(fixture, _create(identifiers=[_identifier()]))
    command = await approval_command(fixture.connection, fixture.schema, created)
    receipts = []
    for _ in range(2):
        async with fixture.connection.transaction():
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["publisher"]}"')
            receipts.append(
                await approve_registry_records(fixture.connection, command, _ACTOR, control_schema=fixture.schema)
            )
    assert receipts[0]["approved_revision"] == receipts[1]["approved_revision"]
    assert receipts[0]["replayed"] is False and receipts[1]["replayed"] is True
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.namespace}.registry_approval_history") == 1


@pytest.mark.parametrize("legacy,collision", [(False, False), (True, False), (False, True)])
async def test_manual_undo_restores_historical_assertions_under_current_checks(assertion_db, legacy, collision):
    fixture = assertion_db
    original = _create(identifiers=[_identifier()], assertions=not legacy)
    first = await _draft(fixture, original)
    later = _create(identifiers=[_identifier(identifier_value="54321")])
    later.fields["assertions"].update(company_id=str(original.record_id), expected_revision=1)
    later.fields["assertions"]["role_assertions"][0].update(role="network_operator")
    later.fields["assertions"]["role_assertions"][0]["provenance"]["evidence_ref"] = "later-role-reference"
    later.fields["assertions"]["identifier_assertions"][0]["provenance"]["evidence_ref"] = "later-identifier-reference"
    corrected = replace(
        later,
        record_id=original.record_id,
        operation="correct",
        expected_revision=1,
        fields=later.fields | {"roles": ["network_operator"]},
    )
    await _draft(fixture, corrected)
    if collision:
        await _draft(fixture, _create(identifiers=[_identifier()]))
    undo = RegistryManualUndoCommand("company", original.record_id, 2, 1, "Reviewed historical assertions", uuid4().hex)
    async with fixture.sessions() as session, session.begin():
        preparation = await prepare_registry_manual_undo(session, undo, _ACTOR, schema=fixture.schema)
    before = await _accounting(fixture)
    if collision:
        with pytest.raises(RegistryRecordConflict, match="assertion_target_conflict"):
            await _draft(fixture, preparation.command)
        assert await _accounting(fixture) == before
        return
    restored = await _draft(fixture, preparation.command)
    for key in ("roles", "role_assertions", "identifier_assertions"):
        assert restored["record"][key] == first["record"][key]
    assert restored["revision"] == 3
    target_record = json.loads(
        await fixture.connection.fetchval(
            f"SELECT record_json::text FROM {fixture.namespace}.registry_record_history WHERE record_kind='company' AND record_key=$1 AND revision=1",
            str(original.record_id),
        )
    )
    assert target_record == first["record"]
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(fixture, replace(preparation.command, idempotency_key=uuid4().hex))
