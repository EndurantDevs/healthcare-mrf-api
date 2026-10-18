# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native company assertion lifecycle and precise protected draft ACLs."""

import asyncio
import csv
import importlib.util
import io
import json
import os
import secrets
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_record_store as record_store
from process.company_registry_assertion_values import CONTRACT
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict, approve_registry_records
from process.registry_management_permissions import (
    install_registry_management_permissions,
    verify_registry_management_permissions,
)
from process.registry_manual_undo import (
    RegistryManualUndoCommand,
    prepare_registry_manual_undo,
    read_registry_manual_history,
)
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from process.registry_source_admission import admit_cms_mlr_edition
from tests.test_registry_approval_store_postgres import _command as approval_command
from tests.test_registry_approval_store_postgres import _create as group_command
from tests.test_registry_source_admission_postgres import _edition as cms_edition
from tests.test_registry_source_admission_postgres import _input as cms_input
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
    "20261017030000_registry_ptg_office_recipe_selection.py",
    "20261007140000_registry_source_recipes.py",
    "20261009010000_registry_ptg_producer_scope.py",
    "20261009020000_company_registry_assertions.py",
    "20261009030000_network_catalog_evidence.py",
    "20261009040000_registry_ptg_published_plan_scope.py",
    "20261010010000_registry_ptg_office_approval.py",
    "20261010020000_registry_ptg_office_retention.py",
    "20261010030000_registry_source_admission_receipt.py",
    "20261011010000_company_registry_group_assertions.py",
    "20261012010000_company_registry_name_origin.py",
    "20261013010000_registry_custom_history_events.py",
    "20261018011000_network_candidate_custody.py",
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


async def _draft(fixture, command, *, undo_provenance=None):
    async with fixture.sessions() as session, session.begin():
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["draft"]
        return await apply_registry_record_command(
            session, command, _ACTOR, schema=fixture.schema, undo_provenance=undo_provenance
        )


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
        f"(SELECT count(*) FROM {fixture.namespace}.company_registry_group_assertion) AS groups,"
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
        for table in (
            "company_registry_role_assertion",
            "company_registry_identifier_assertion",
            "company_registry_group_assertion",
        ):
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


async def test_company_correction_predecessors_survive_approval_and_closed_reads(assertion_db):
    fixture = assertion_db
    original = _create(identifiers=[_identifier()])
    first = await _draft(fixture, original)
    correction = replace(
        original,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields=original.fields | {"assertions": original.fields["assertions"] | {"expected_revision": 1}},
    )
    corrected = await _draft(fixture, correction)
    history_query = f"SELECT record_json::text FROM {fixture.namespace}.registry_record_history WHERE record_kind='company' AND record_key=$1 AND revision=2"
    retained = json.loads(await fixture.connection.fetchval(history_query, str(original.record_id)))
    predecessors = retained["correction_predecessors"]
    assert predecessors == {
        "company_revision": 1,
        "role_assertions": [first["record"]["role_assertions"][0]["assertion_id"]],
        "identifier_assertions": [first["record"]["identifier_assertions"][0]["assertion_id"]],
        "group_assertions": [],
    }
    assert "correction_predecessors" not in corrected["record"]
    command = await approval_command(fixture.connection, fixture.schema, corrected)
    async with fixture.connection.transaction():
        await approve_registry_records(fixture.connection, command, _ACTOR, control_schema=fixture.schema)
    approved = json.loads(
        await fixture.connection.fetchval(
            f"SELECT record_json::text FROM {fixture.namespace}.registry_approved_record WHERE record_kind='company' AND record_key=$1 AND record_revision=2",
            str(original.record_id),
        )
    )
    assert approved == retained
    command = await approval_command(fixture.connection, fixture.schema, corrected)
    async with fixture.connection.transaction():
        preview_by_field = await preview_registry_approval(
            fixture.connection, command, _ACTOR, control_schema=fixture.schema
        )
    assert "correction_predecessors" not in preview_by_field["records"][0]["before"]
    assert "correction_predecessors" not in preview_by_field["records"][0]["after"]
    async with fixture.sessions() as session, session.begin():
        page = await read_registry_manual_history(session, "company", original.record_id, schema=fixture.schema)
    assert all("correction_predecessors" not in history_entry["record"] for history_entry in page["records"])
    assert await _draft(fixture, correction) == corrected
    assert await _draft(fixture, original) == first
    assert json.loads(await fixture.connection.fetchval(history_query, str(original.record_id))) == retained


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
            await _draft(fixture, preparation.command, undo_provenance=preparation.provenance)
        assert await _accounting(fixture) == before
        return
    restored = await _draft(fixture, preparation.command, undo_provenance=preparation.provenance)
    for key in ("roles", "role_assertions", "identifier_assertions"):
        assert restored["record"][key] == first["record"][key]
    assert restored["revision"] == 3
    restored_history = json.loads(
        await fixture.connection.fetchval(
            f"SELECT record_json::text FROM {fixture.namespace}.registry_record_history WHERE record_kind='company' AND record_key=$1 AND revision=3",
            str(original.record_id),
        )
    )
    assert restored_history["correction_predecessors"]["company_revision"] == 2
    assert restored_history["correction_predecessors"]["role_assertions"] == sorted(
        entry["assertion_id"] for entry in later.fields["assertions"]["role_assertions"]
    )
    target_record = json.loads(
        await fixture.connection.fetchval(
            f"SELECT record_json::text FROM {fixture.namespace}.registry_record_history WHERE record_kind='company' AND record_key=$1 AND revision=1",
            str(original.record_id),
        )
    )
    assert target_record == first["record"]
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(
            fixture, replace(preparation.command, idempotency_key=uuid4().hex), undo_provenance=preparation.provenance
        )


def _group_assertion_rows():
    source_by_field = {
        "assertion_id": uuid4(),
        "company_id": uuid4(),
        "company_revision": 1,
        "group_id": None,
        "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation",
        "valid_from": None,
        "valid_to": None,
        "provenance_kind": "source_reference",
        "evidence_ref": "Retained source reference",
        "source_snapshot_id": uuid4(),
        "source_record_key": "record-17",
    }
    manual_by_field = {
        **source_by_field,
        "assertion_id": uuid4(),
        "group_id": uuid4(),
        "group_kind": "naic_group",
        "relationship_kind": "verified_ownership",
        "valid_from": date(2024, 2, 29),
        "valid_to": date(2024, 2, 29),
        "provenance_kind": "manual_reference",
        "source_snapshot_id": None,
        "source_record_key": None,
    }
    return source_by_field, manual_by_field


async def test_migrated_group_assertions_retain_unknown_periods_and_enforce_native_constraints(assertion_db):
    fixture = assertion_db
    statement = text(
        f"INSERT INTO {fixture.namespace}.company_registry_group_assertion "
        "(assertion_id,company_id,company_revision,group_id,group_kind,relationship_kind,valid_from,valid_to,"
        "provenance_kind,evidence_ref,source_snapshot_id,source_record_key) "
        "VALUES(:assertion_id,:company_id,:company_revision,:group_id,:group_kind,:relationship_kind,"
        ":valid_from,:valid_to,:provenance_kind,:evidence_ref,:source_snapshot_id,:source_record_key) "
        "RETURNING assertion_id,group_id,valid_from,valid_to,created_at"
    )
    source_by_field, manual_by_field = _group_assertion_rows()
    async with fixture.sessions() as session, session.begin():
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["draft"]
        retained_source = (await session.execute(statement, source_by_field)).mappings().one()
        assert retained_source["group_id"] is retained_source["valid_from"] is retained_source["valid_to"] is None
        assert retained_source["created_at"] is not None
        retained_manual = (await session.execute(statement, manual_by_field)).mappings().one()
        assert retained_manual["group_id"] == manual_by_field["group_id"]
        assert retained_manual["valid_from"] == retained_manual["valid_to"] == date(2024, 2, 29)
        for changes in (
            {"assertion_id": UUID(int=0)},
            {"company_id": UUID(int=0)},
            {"company_revision": 0},
            {"group_id": UUID(int=0)},
            {"group_kind": "inferred_parent"},
            {"relationship_kind": "automatic_ownership"},
            {"valid_to": date(2024, 2, 28)},
            {"evidence_ref": " leading"},
            {"group_id": None},
            {"source_record_key": "unexpected-manual-source"},
        ):
            with pytest.raises(IntegrityError):
                async with session.begin_nested():
                    await session.execute(statement, {**manual_by_field, "assertion_id": uuid4(), **changes})
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                await session.execute(statement, source_by_field)
        assert (
            await session.scalar(text(f"SELECT count(*) FROM {fixture.namespace}.company_registry_group_assertion"))
            == 2
        )


async def _draft_group_target(fixture, *, kind="corporate_parent", archived=False):
    identity = uuid4()
    command = RegistryRecordCommand(
        "group",
        identity,
        "create",
        0,
        {"group_kind": kind, "display_name": "Example Group", "aliases": []},
        "Reviewed group target",
        uuid4().hex,
    )
    await _draft(fixture, command)
    if archived:
        await _draft(
            fixture, replace(command, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
        )
    return identity


def _declared_group(company, group, **changes):
    return {
        "assertion_id": str(uuid4()),
        "company_id": str(company),
        "group_id": None if group is None else str(group),
        "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation",
        "valid_from": "2025-01-01",
        "valid_to": None,
        "provenance": {
            "kind": "manual_reference",
            "evidence_ref": "Reviewed group reference",
            "snapshot_id": None,
            "source_record_key": None,
        },
    } | changes


def _with_groups(command, rows):
    return replace(
        command, fields=command.fields | {"assertions": command.fields["assertions"] | {"group_assertions": rows}}
    )


async def _admitted_group_edge(fixture, *, unknown=False):
    input_bytes = cms_input()
    if unknown:
        decoded_rows = list(csv.DictReader(io.StringIO(input_bytes.decode(), newline="")))
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=list(decoded_rows[0]), lineterminator="\r\n")
        writer.writeheader()
        writer.writerows([group_assertion | {"naic_group_code": ""} for group_assertion in decoded_rows])
        input_bytes = output.getvalue().encode()
    edition = cms_edition(input_bytes)
    async with fixture.connection.transaction():
        receipt = await admit_cms_mlr_edition(fixture.connection, input_bytes, edition, control_schema=fixture.schema)
    assert receipt["group_assertions"] == 2 and receipt["replayed"] is False
    edge = await fixture.connection.fetchrow(
        f"SELECT * FROM {fixture.namespace}.registry_company_group_assertion WHERE snapshot_id=$1 ORDER BY source_record_key LIMIT 1",
        edition.snapshot_id,
    )
    assert edge["valid_from"] is edge["valid_to"] is None
    command = _create()
    command = replace(command, record_id=edge["company_id"], operation="correct", expected_revision=1)
    command.fields["assertions"].update(company_id=str(edge["company_id"]), expected_revision=1)
    provenance_by_field = {
        "kind": "source_reference",
        "evidence_ref": "Retained group occurrence",
        "snapshot_id": str(edition.snapshot_id),
        "source_record_key": edge["source_record_key"],
    }
    group_assertion = _declared_group(
        edge["company_id"],
        edge["group_id"],
        group_kind="naic_group",
        valid_from=None,
        provenance=provenance_by_field,
    )
    return _with_groups(command, [group_assertion])


async def _current_company_json(fixture, company_id):
    async with fixture.connection.transaction():
        await fixture.connection.execute("SET LOCAL TIME ZONE 'Etc/GMT-2'")
        return json.loads(
            await fixture.connection.fetchval(
                f"SELECT ({record_store._company_record_json_sql(fixture.namespace)})::text FROM {fixture.namespace}.company_registry head WHERE company_id=$1",
                company_id,
            )
        )


async def test_group_target_requires_prospective_approval(assertion_db):
    fixture = assertion_db
    group_creation = group_command()
    group = await _draft(fixture, group_creation)
    creation = _create()
    company = await _draft(
        fixture, _with_groups(creation, [_declared_group(creation.record_id, group_creation.record_id)])
    )
    before = await _accounting(fixture)
    with pytest.raises(RegistryApprovalConflict, match="assertion_target_conflict"):
        await _approve(fixture, company)
    assert await _accounting(fixture) == before
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.namespace}.registry_approval_history") == 0
    approved = await _approve(fixture, group, company)
    assert approved["selected_count"] == 2
    retained = await fixture.connection.fetch(
        f"SELECT record_kind,record_json::text FROM {fixture.namespace}.registry_approved_record "
        "WHERE approved_revision=$1 ORDER BY record_kind",
        approved["approved_revision"],
    )
    assert [row["record_kind"] for row in retained] == ["company", "group"]
    assert json.loads(retained[0]["record_json"])["group_assertions"] == company["record"]["group_assertions"]
    assert json.loads(retained[1]["record_json"]) == group["record"]


@pytest.mark.parametrize("change", ["archive", "kind"])
async def test_group_drafts_preserve_approved_targets(assertion_db, change):
    fixture = assertion_db
    group_creation = group_command()
    group = await _draft(fixture, group_creation)
    creation = _create()
    company = await _draft(
        fixture, _with_groups(creation, [_declared_group(creation.record_id, group_creation.record_id)])
    )
    await _approve(fixture, group, company)
    changed = await _draft(
        fixture,
        replace(
            group_creation,
            operation="archive" if change == "archive" else "correct",
            expected_revision=1,
            fields={} if change == "archive" else group_creation.fields | {"group_kind": "naic_group"},
            idempotency_key=uuid4().hex,
        ),
    )
    unrelated = await _draft(fixture, _create())
    approved = await _approve(fixture, unrelated)
    retained = await fixture.connection.fetchval(
        f"SELECT record_json::text FROM {fixture.namespace}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='group' AND record_key=$2",
        approved["approved_revision"],
        str(group_creation.record_id),
    )
    assert json.loads(retained) == group["record"]
    assert changed["record"]["archived"] == (change == "archive")
    assert changed["record"]["group_kind"] == ("corporate_parent" if change == "archive" else "naic_group")
    with pytest.raises(RegistryApprovalConflict, match="assertion_target_conflict"):
        await _approve(fixture, changed)
    assert (
        await fixture.connection.fetchval(
            f"SELECT approved_revision FROM {fixture.namespace}.registry_revision_control WHERE id=1"
        )
        == approved["approved_revision"]
    )


async def test_company_group_store_roundtrip_omission_clear_and_replay(assertion_db):
    fixture = assertion_db
    group_id = await _draft_group_target(fixture)
    original = _create()
    group_assertion = _declared_group(original.record_id, group_id)
    command = _with_groups(original, [group_assertion])
    created = await _draft(fixture, command)
    assert created["record"]["group_assertions"] == [group_assertion]
    correction = replace(command, operation="correct", expected_revision=1, idempotency_key=uuid4().hex)
    correction = replace(
        correction,
        fields=correction.fields | {"assertions": correction.fields["assertions"] | {"expected_revision": 1}},
    )
    del correction.fields["assertions"]["group_assertions"]
    preserved = await _draft(fixture, correction)
    assert preserved["record"]["group_assertions"] == [group_assertion]
    legacy = replace(
        correction,
        expected_revision=2,
        fields={key: field_value for key, field_value in correction.fields.items() if key != "assertions"},
        idempotency_key=uuid4().hex,
    )
    retained = await _draft(fixture, legacy)
    assert retained["record"]["group_assertions"] == [group_assertion]
    async with fixture.sessions() as session:
        assert (await get_registry_record(session, "company", command.record_id, schema=fixture.schema)) == retained[
            "record"
        ]
        assert (await list_registry_records(session, "company", schema=fixture.schema))[0] == retained["record"]
    actual = await _current_company_json(fixture, command.record_id)
    actual_created = datetime.fromisoformat(actual["created_at"])
    retained_created = datetime.fromisoformat(retained["record"]["created_at"])
    assert actual_created.utcoffset() is not None and retained_created.utcoffset() is not None
    assert actual_created == retained_created and actual["created_at"] != retained["record"]["created_at"]
    assert {key: field_value for key, field_value in actual.items() if key != "created_at"} == {
        key: field_value for key, field_value in retained["record"].items() if key != "created_at"
    }
    assert set(actual["group_assertions"][0]) == set(group_assertion)
    clear = _with_groups(replace(correction, expected_revision=3, idempotency_key=uuid4().hex), [])
    clear.fields["assertions"]["expected_revision"] = 3
    assert (await _draft(fixture, clear))["record"]["group_assertions"] == []
    assert await _draft(fixture, command) == created
    omitted = replace(
        clear,
        fields=clear.fields
        | {
            "assertions": {
                key: field_value for key, field_value in clear.fields["assertions"].items() if key != "group_assertions"
            }
        },
    )
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _draft(fixture, omitted)
    before = await _accounting(fixture)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(fixture, replace(correction, idempotency_key=uuid4().hex))
    assert await _accounting(fixture) == before and before["groups"] == 3


@pytest.mark.parametrize("retained_group", [False, True], ids=["no-typed-group", "retained-typed-group"])
async def test_materialized_company_correction_preserves_omitted_groups(assertion_db, retained_group):
    """Retain the exact materialized baseline and omitted typed groups under the restricted writer."""
    fixture = assertion_db
    command = await _admitted_group_edge(fixture)
    expected_groups = command.fields["assertions"]["group_assertions"] if retained_group else []
    async with fixture.sessions() as session, session.begin():
        initial = await get_registry_record(session, "company", command.record_id, schema=fixture.schema)
        assert initial["revision"] == 1 and initial["group_assertions"] == []
        if retained_group:
            # Retain a typed child through the restricted draft writer; its source edge is genuinely admitted.
            await record_store._retain_company_assertions(
                session,
                initial,
                {"role_assertions": [], "identifier_assertions": [], "group_assertions": expected_groups},
                fixture.schema,
            )
        materialized = await get_registry_record(session, "company", command.record_id, schema=fixture.schema)
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {fixture.namespace}.registry_record_history") == 0
    omitted = replace(
        command,
        fields=command.fields
        | {
            "assertions": {
                key: field_value
                for key, field_value in command.fields["assertions"].items()
                if key != "group_assertions"
            }
        },
        idempotency_key=uuid4().hex,
    )
    await _verify_materialized_rollback(fixture, omitted)
    preserved = await _draft(fixture, omitted)
    await _verify_materialized_predecessor(fixture, materialized, preserved)
    assert preserved["record"]["revision"] == 2
    assert preserved["record"]["group_assertions"] == expected_groups
    assert preserved["record"]["role_assertions"] == omitted.fields["assertions"]["role_assertions"]
    assert preserved["record"]["identifier_assertions"] == omitted.fields["assertions"]["identifier_assertions"]
    before = await _accounting(fixture)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(fixture, replace(omitted, idempotency_key=uuid4().hex))
    assert await _accounting(fixture) == before
    await _verify_materialized_undo(fixture, materialized, omitted.record_id)
    clear = _with_groups(
        replace(
            omitted,
            expected_revision=3,
            fields=omitted.fields | {"assertions": omitted.fields["assertions"] | {"expected_revision": 3}},
            idempotency_key=uuid4().hex,
        ),
        [],
    )
    assert (await _draft(fixture, clear))["record"]["group_assertions"] == []
    assert await _draft(fixture, omitted) == preserved


async def _verify_materialized_undo(fixture, materialized, company_id):
    history_query = (
        f"SELECT revision,custom_revision,record_json::text,actor_json::text,reason,idempotency_key,request_sha256 "
        f"FROM {fixture.namespace}.registry_record_history WHERE record_kind='company' AND record_key=$1 ORDER BY revision"
    )
    retained = await fixture.connection.fetch(history_query, str(company_id))
    command = RegistryManualUndoCommand("company", company_id, 2, 1, "Restore materialized values", uuid4().hex)
    # An unrelated equal-custom-revision row must not gain undo authority.
    await fixture.connection.execute(
        f"UPDATE {fixture.namespace}.registry_record_history SET record_json=record_json-'correction_predecessors' "
        "WHERE record_kind='company' AND record_key=$1 AND revision=2",
        str(company_id),
    )
    try:
        async with fixture.sessions() as session, session.begin():
            with pytest.raises(RegistryRecordConflict, match="undo_history_conflict"):
                await prepare_registry_manual_undo(session, command, _ACTOR, schema=fixture.schema)
    finally:
        await fixture.connection.execute(
            f"UPDATE {fixture.namespace}.registry_record_history SET record_json=$1::jsonb "
            "WHERE record_kind='company' AND record_key=$2 AND revision=2",
            retained[1]["record_json"],
            str(company_id),
        )
    async with fixture.sessions() as session, session.begin():
        preparation = await prepare_registry_manual_undo(session, command, _ACTOR, schema=fixture.schema)
    restored = await _draft(fixture, preparation.command, undo_provenance=preparation.provenance)
    assert restored["revision"] == 3
    for field in ("display_name", "roles", "aliases", "role_assertions", "identifier_assertions", "group_assertions"):
        assert restored["record"][field] == materialized[field]
    history = await fixture.connection.fetch(history_query, str(company_id))
    assert history[:2] == retained
    assert history[2]["actor_json"] == retained[0]["actor_json"]
    assert history[2]["reason"] == preparation.command.reason
    async with fixture.sessions() as session, session.begin():
        replay = await prepare_registry_manual_undo(session, command, _ACTOR, schema=fixture.schema)
    assert replay.command == preparation.command
    assert await _draft(fixture, replay.command, undo_provenance=replay.provenance) == restored
    assert await fixture.connection.fetch(history_query, str(company_id)) == history


async def _verify_materialized_rollback(fixture, command):
    """Real restricted-role history inserts remain inside the caller's savepoint."""
    before = await _accounting(fixture)
    async with fixture.sessions() as session, session.begin():
        with pytest.raises(RuntimeError, match="synthetic-retention-rollback"):
            async with session.begin_nested():
                await apply_registry_record_command(session, command, _ACTOR, schema=fixture.schema)
                assert (
                    await session.scalar(text(f"SELECT count(*) FROM {fixture.namespace}.registry_record_history")) == 2
                )
                raise RuntimeError("synthetic-retention-rollback")
    assert await _accounting(fixture) == before


async def _verify_materialized_predecessor(fixture, materialized, preserved):
    """Compare raw retained history, correction references and real actor attribution."""
    retained = await fixture.connection.fetch(
        f"SELECT revision,custom_revision,record_json::text,actor_json::text,reason,idempotency_key,request_sha256 "
        f"FROM {fixture.namespace}.registry_record_history "
        "WHERE record_kind='company' AND record_key=$1 ORDER BY revision",
        str(preserved["record_id"]),
    )
    assert [history_entry["revision"] for history_entry in retained] == [1, 2]
    assert json.loads(retained[0]["record_json"]) == materialized
    assert retained[0]["custom_revision"] == retained[1]["custom_revision"] == preserved["custom_revision"]
    assert retained[0]["actor_json"] == retained[1]["actor_json"]
    assert retained[0]["reason"] == "Preserved materialized company before manual correction"
    assert retained[0]["idempotency_key"] != retained[1]["idempotency_key"]
    assert retained[0]["request_sha256"] != retained[1]["request_sha256"]
    assert json.loads(retained[1]["record_json"])["correction_predecessors"]["group_assertions"] == sorted(
        assertion["assertion_id"] for assertion in materialized["group_assertions"]
    )


@pytest.mark.parametrize("case", ["missing", "archived", "kind"])
async def test_company_group_store_rejects_incompatible_targets(assertion_db, case):
    fixture = assertion_db
    target = uuid4() if case == "missing" else await _draft_group_target(fixture, archived=case == "archived")
    command = _create()
    row = _declared_group(command.record_id, target, group_kind="naic_group" if case == "kind" else "corporate_parent")
    before = await _accounting(fixture)
    with pytest.raises(RegistryRecordConflict, match="assertion_target_conflict"):
        await _draft(fixture, _with_groups(command, [row]))
    assert await _accounting(fixture) == before


@pytest.mark.parametrize("unknown", [False, True], ids=["known-group", "unknown-group"])
async def test_company_group_store_retains_genuine_source_unknown_periods(assertion_db, unknown):
    command = await _admitted_group_edge(assertion_db, unknown=unknown)
    created = await _draft(assertion_db, command)
    row = command.fields["assertions"]["group_assertions"][0]
    assert created["record"]["group_assertions"] == [row]
    assert (row["group_id"] is None) is unknown and row["valid_from"] is row["valid_to"] is None
    async with assertion_db.sessions() as session:
        read = await get_registry_record(session, "company", command.record_id, schema=assertion_db.schema)
    assert read == created["record"]


@pytest.mark.parametrize(
    "case", ["other_company", "group", "relationship", "start", "end", "occurrence", "snapshot", "null_group_claim"]
)
async def test_company_group_store_requires_exact_native_source_edge(assertion_db, case):
    fixture = assertion_db
    command = await _admitted_group_edge(fixture, unknown=case == "null_group_claim")
    row = command.fields["assertions"]["group_assertions"][0]
    match case:
        case "other_company":
            other = _create()
            row["company_id"] = str(other.record_id)
            command = _with_groups(other, [row])
        case "group":
            row["group_id"] = None
        case "null_group_claim":
            row["group_id"] = str(await _draft_group_target(fixture, kind="naic_group"))
        case "relationship":
            row["relationship_kind"] = "verified_ownership"
        case "start" | "end":
            row["valid_from" if case == "start" else "valid_to"] = "2024-01-01"
        case "occurrence":
            row["provenance"]["source_record_key"] = "missing-occurrence"
        case _:
            row["provenance"]["snapshot_id"] = str(uuid4())
    before = await _accounting(fixture)
    with pytest.raises(RegistryRecordConflict, match="assertion_target_conflict"):
        await _draft(fixture, command)
    assert await _accounting(fixture) == before


async def test_company_group_store_joint_rollback_and_insert_failure(assertion_db):
    fixture = assertion_db
    group_id = await _draft_group_target(fixture)
    command = _create(identifiers=[_identifier()])
    command = _with_groups(command, [_declared_group(command.record_id, group_id)])
    before = await _accounting(fixture)
    async with fixture.sessions() as session:
        await session.begin()
        try:
            await apply_registry_record_command(session, command, _ACTOR, schema=fixture.schema)
        finally:
            await session.rollback()
    assert await _accounting(fixture) == before
    columns = ",".join(
        '"' + name + '"'
        for name in (
            "assertion_id",
            "company_id",
            "company_revision",
            "group_id",
            "group_kind",
            "relationship_kind",
            "valid_from",
            "valid_to",
            "provenance_kind",
            "evidence_ref",
            "source_snapshot_id",
            "source_record_key",
        )
    )
    await fixture.connection.execute(
        f'REVOKE INSERT({columns}) ON {fixture.namespace}.company_registry_group_assertion FROM "{fixture.roles_by_kind["draft"]}"'
    )
    try:
        with pytest.raises(Exception) as failure:
            await _draft(fixture, command)
        assert "permission denied" in str(failure.value).lower()
        assert await _accounting(fixture) == before
    finally:
        await _install(fixture)
    assert (await _draft(fixture, command))["record"]["group_assertions"] == command.fields["assertions"][
        "group_assertions"
    ]


async def test_materialized_group_first_correction_preserves_baseline_and_undo(assertion_db):
    fixture = assertion_db
    company_command = await _admitted_group_edge(fixture)
    group_id = UUID(company_command.fields["assertions"]["group_assertions"][0]["group_id"])
    async with fixture.sessions() as session, session.begin():
        materialized = await get_registry_record(session, "group", group_id, schema=fixture.schema)
    assert materialized["revision"] == 1 and materialized["group_kind"] == "naic_group"
    source_evidence = materialized["source_evidence"]
    assert len(source_evidence) == 1
    assert (
        source_evidence[0]["snapshot_id"]
        == company_command.fields["assertions"]["group_assertions"][0]["provenance"]["snapshot_id"]
    )
    assert (source_evidence[0]["identifier_system"], source_evidence[0]["identifier_value"]) == ("naic_group", "707")
    assert source_evidence[0]["snapshot_present"] is source_evidence[0]["observation_present"] is True
    assert source_evidence[0]["observation_status"] == "accepted"
    materialized_head_by_field = {
        field: field_value for field, field_value in materialized.items() if field != "source_evidence"
    }
    correction = RegistryRecordCommand(
        "group",
        group_id,
        "correct",
        1,
        {"group_kind": "naic_group", "display_name": "Corrected source group", "aliases": ["Corrected"]},
        "Reviewed materialized group correction",
        uuid4().hex,
    )
    await _verify_materialized_rollback(fixture, correction)
    async with fixture.sessions() as session, session.begin():
        assert await get_registry_record(session, "group", group_id, schema=fixture.schema) == materialized
    corrected = await _draft(fixture, correction)
    retained = await fixture.connection.fetch(
        f"SELECT revision,custom_revision,record_json::text,actor_json::text,reason,idempotency_key,request_sha256 "
        f"FROM {fixture.namespace}.registry_record_history WHERE record_kind='group' AND record_key=$1 ORDER BY revision",
        str(group_id),
    )
    assert [history_entry["revision"] for history_entry in retained] == [1, 2]
    assert json.loads(retained[0]["record_json"]) == materialized_head_by_field
    assert all("source_evidence" not in json.loads(entry["record_json"]) for entry in retained)
    assert retained[0]["reason"] == "Preserved materialized group before manual correction"
    assert retained[0]["actor_json"] == retained[1]["actor_json"]
    assert retained[0]["custom_revision"] == retained[1]["custom_revision"] == corrected["custom_revision"]
    assert "correction_predecessors" not in json.loads(retained[1]["record_json"])
    before = await _accounting(fixture)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _draft(fixture, replace(correction, idempotency_key=uuid4().hex))
    assert await _accounting(fixture) == before
    await _verify_materialized_group_undo(fixture, materialized, group_id)
    assert await _draft(fixture, correction) == corrected
    async with fixture.sessions() as session, session.begin():
        assert (await get_registry_record(session, "group", group_id, schema=fixture.schema))[
            "source_evidence"
        ] == source_evidence


async def _verify_materialized_group_undo(fixture, materialized, group_id):
    query = (
        f"SELECT revision,custom_revision,record_json::text,actor_json::text,reason,idempotency_key,request_sha256 "
        f"FROM {fixture.namespace}.registry_record_history WHERE record_kind='group' AND record_key=$1 ORDER BY revision"
    )
    retained = await fixture.connection.fetch(query, str(group_id))
    command = RegistryManualUndoCommand("group", group_id, 2, 1, "Restore materialized group", uuid4().hex)
    await fixture.connection.execute(
        f"UPDATE {fixture.namespace}.registry_record_history SET reason='Unrelated history' "
        "WHERE record_kind='group' AND record_key=$1 AND revision=1",
        str(group_id),
    )
    try:
        async with fixture.sessions() as session, session.begin():
            with pytest.raises(RegistryRecordConflict, match="undo_history_conflict"):
                await prepare_registry_manual_undo(session, command, _ACTOR, schema=fixture.schema)
    finally:
        await fixture.connection.execute(
            f"UPDATE {fixture.namespace}.registry_record_history SET reason=$1 "
            "WHERE record_kind='group' AND record_key=$2 AND revision=1",
            retained[0]["reason"],
            str(group_id),
        )
    async with fixture.sessions() as session, session.begin():
        preparation = await prepare_registry_manual_undo(session, command, _ACTOR, schema=fixture.schema)
    restored = await _draft(fixture, preparation.command, undo_provenance=preparation.provenance)
    assert restored["revision"] == 3
    for field in ("group_kind", "display_name", "aliases", "created_at", "archived"):
        assert restored["record"][field] == materialized[field]
    history = await fixture.connection.fetch(query, str(group_id))
    assert history[:2] == retained
    assert history[2]["actor_json"] == retained[0]["actor_json"]
    assert history[2]["reason"] == preparation.command.reason
    async with fixture.sessions() as session, session.begin():
        replay = await prepare_registry_manual_undo(session, command, _ACTOR, schema=fixture.schema)
    assert replay.command == preparation.command
    assert await _draft(fixture, replay.command, undo_provenance=replay.provenance) == restored
    assert await fixture.connection.fetch(query, str(group_id)) == history
