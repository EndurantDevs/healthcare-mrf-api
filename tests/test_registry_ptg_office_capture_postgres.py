# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native candidate preparation; complete witness and admission are separate.

In PostgreSQL cases only the upstream HTTP decision uses a synthetic fixture;
source, publication, site adoption, COPY, closure and rollback use native paths.
Separate codec-only cases use fixture input without database authority.
No authenticated office-review or publication is installed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import struct
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from process import registry_ptg_office_capture as capture
from process import registry_ptg_scope_engine as scope_engine
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_custom_address_source import NetworkCustomAddressSourceError
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.ptg_parts.result_archive_source_authority import prepare_ptg_result_archive_source_authority
from process.registry_retained_site_adoption import RetainedSiteAdoptionError
from tests.test_network_custom_address_source_postgres import _draft
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as original_serving_schema
from tests.test_registry_approval_store_postgres import _actor, _create
from tests.test_registry_candidate_composition_postgres import _roles
from tests.test_registry_ptg_office_capture import OTHER, _request, _row, _scope
from tests.test_registry_ptg_office_capture import _request as codec_request
from tests.test_registry_ptg_office_capture import _scope as codec_scope
from tests.test_registry_ptg_scope_engine_postgres import _app_authority, _approval, _native_scope, _rows
from tests.test_registry_retained_site_adoption_postgres import _source_addresses
from tests.test_result_archive_published_authority_postgres import _is_guarded_test_service


@pytest.fixture
async def serving_schema():
    """Keep the existing migration fixture behind the same explicit host guard."""
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set guarded native PostgreSQL test variables")
    supplied = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    source_dsn = os.getenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN")
    if not supplied or not source_dsn:
        pytest.skip("set both explicit native test DSNs")
    url = make_url(supplied).set(drivername="postgresql+asyncpg")
    if not _is_guarded_test_service(url) or url != make_url(source_dsn).set(drivername="postgresql+asyncpg"):
        pytest.fail("office tests require one guarded native test database")
    probe = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        assert 180000 <= int(await probe.fetchval("SHOW server_version_num")) < 190000
        await probe.execute("CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public")
    finally:
        await probe.close()
    fixture = original_serving_schema.__wrapped__()
    try:
        yield await anext(fixture)
    finally:
        await fixture.aclose()


async def _seed_retained_membership(fixture, copy_target, source_table, provider_id, network_id):
    connection = fixture.connection
    async with connection.transaction():
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={"unified_address": fixture.base.generation_id, "fhir": "retained-edition-a"},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=2,
            control_schema=fixture.control_schema,
        )
        await connection.execute(f'''CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))''')
        await connection.execute(
            f'''INSERT INTO "{copy_target.schema_name}".provider_location_binding
              SELECT 'npi',$1,md5(location_key)::uuid,location_key,entity_type,entity_id FROM {source_table}''',
            provider_id,
        )
        bindings = await connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".provider_location_binding')
        input_bytes = capture._canonical(
            [
                {
                    "network_id": network_id,
                    "provider_system": binding["provider_system"],
                    "provider_id": binding["provider_id"],
                    "location_id": str(binding["location_id"]),
                    "evidence_id": "e" * 64,
                }
                for binding in bindings
            ]
        )
        await admit_network_membership_batch(
            connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=input_bytes,
            expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(connection, copy_target, control_schema=fixture.control_schema)


async def _publish_sites(fixture, copy_targets, journal, roles):
    """Use the existing full address models and genuine publication pipeline."""
    connection = fixture.connection
    _, source_table = await _source_addresses(fixture, 2, "npi")
    provider_id = "1999999901"
    await connection.execute(f"UPDATE {source_table} SET entity_id=$1,npi=$2", provider_id, int(provider_id))
    await connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=$1', int(provider_id))
    network = await _draft(fixture, _create("network"), _actor())
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    assert await connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None
    copy_targets.append(copy_target)
    journal.write_text(
        json.dumps(
            {
                "phase": "registered",
                "roles": roles,
                "schemas": [],
                "retained_schemas": [candidate.schema_name for candidate in copy_targets],
            }
        )
    )
    await _seed_retained_membership(fixture, copy_target, source_table, provider_id, network["record_id"])
    await prepare_and_publish_network_candidate(
        connection,
        copy_target,
        fixture.base,
        **_roles(fixture),
        control_schema=fixture.control_schema,
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        serving = await resolve_network_serving_manifest(connection, control_schema=fixture.control_schema)
        sites = await connection.fetch(
            f'''SELECT binding.provider_system,binding.provider_id,binding.location_id::text,binding.location_key,
              encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
              FROM "{serving.schema_name}".provider_location_binding binding
              JOIN "{serving.schema_name}".entity_address_unified address USING(location_key)
              ORDER BY binding.location_key'''
        )
    return serving, [dict(site) for site in sites]


async def _publisher_grants(connection, fixture, roles, *, grant):
    owner = roles[0]
    publisher = fixture.roles[2]
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    if grant:
        await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{publisher}","{owner}"')
        await connection.execute(f'GRANT "{owner}" TO "{publisher}" WITH INHERIT FALSE, SET TRUE')
    else:
        await connection.execute(f'REVOKE "{owner}" FROM "{publisher}"')
        await connection.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{publisher}","{owner}"')


async def _registered_request(fixture, rows, **changes):
    identity = uuid4()
    schema = "registry_ptg_office_" + identity.hex
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema) is None
    fixture.schemas.append(schema)
    fixture.journal.write_text(
        json.dumps(
            {
                "phase": "registered",
                "roles": fixture.roles,
                "schemas": fixture.schemas,
                "retained_schemas": [target.schema_name for target in fixture.targets],
            }
        )
    )
    canonical = b"".join(capture._canonical(row) + b"\n" for row in rows)
    return replace(
        capture.RegistryPTGOfficeCaptureRequest(
            identity,
            hashlib.sha256(canonical).hexdigest(),
            len(rows),
            "reviewed_exact_office",
            fixture.serving.generation_id,
            "Reviewed exact synthetic offices",
            uuid4().hex,
        ),
        **changes,
    )


def _office_row(scope, site, group, ordinal, *, source_record_key=None):
    office_by_field = {
        "contract": "registry_ptg_office_assertion.v1",
        "kind": "reviewed_exact_office",
        "assertion_id": "synthetic_assertion",
        "source_record_key": source_record_key or "office:" + str(ordinal),
        "binding_coordinates": scope["coordinates"],
        "binding_source_key": scope["binding_source_key"],
        "source_scope": {name: scope[name] for name in ("company_key", "cohort_id", "snapshot_id")},
        **site,
    }
    witness_by_field = {
        "snapshot_key": scope["evidence"]["graph_identity"]["snapshot_key"],
        "dense_source_key": group["source_key"],
        "source_record_ordinal": group["source_record_ordinal"],
        "provider_group_ref": group["provider_group_ref"],
        "provider_system": site["provider_system"],
        "provider_id": site["provider_id"],
    }
    office_digest, witness_digest = capture._digest(office_by_field), capture._digest(witness_by_field)
    evidence_parts = [
        scope["coordinates"],
        office_by_field["source_scope"],
        scope["binding_source_key"],
        site["provider_system"],
        site["provider_id"],
        site["location_id"],
        office_digest,
        witness_digest,
        site["address_row_sha256"],
    ]
    return {
        "ordinal": ordinal,
        "source_record_key": office_by_field["source_record_key"],
        **{name: scope[name] for name in ("binding_source_key", "company_key", "cohort_id", "snapshot_id")},
        **site,
        "location_hash": "entity_address_unified:" + site["location_key"],
        **{
            name: witness_by_field[name] for name in ("dense_source_key", "source_record_ordinal", "provider_group_ref")
        },
        "provider_witness_sha256": witness_digest,
        "office_evidence_kind": "reviewed_exact_office",
        "office_evidence_json": office_by_field,
        "office_evidence_sha256": office_digest,
        "evidence_id": capture._digest(evidence_parts),
    }


def _rebuilt_row(fixture, row, ordinal, *, source_record_key=None, **site_changes):
    site = {
        name: row[name]
        for name in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
    } | site_changes
    group_by_field = {
        "source_key": row["dense_source_key"],
        "source_record_ordinal": row["source_record_ordinal"],
        "provider_group_ref": row["provider_group_ref"],
    }
    return _office_row(fixture.scope, site, group_by_field, ordinal, source_record_key=source_record_key)


async def _capture_context(scope_fixture, control_schema, roles):
    envelope = await _approval(scope_fixture)
    approval = await scope_fixture.service.approve(envelope)
    scope_by_field = {**(await _rows(scope_fixture))[0], "approval_sha256": approval["approval_sha256"]}
    specification = SimpleNamespace(
        capture_id=scope_by_field["scope_id"],
        ptg_schema_name=scope_fixture.source.name,
        **{name: scope_by_field[name] for name in ("snapshot_id", "binding_source_key", "company_key", "cohort_id")},
    )
    async with scope_fixture.reader() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        frozen = await prepare_ptg_result_archive_source_authority(
            session,
            schema_name=scope_fixture.source.name,
            operation_id=scope_engine._source_specification(specification),
            snapshot_id=scope_by_field["snapshot_id"],
        )
        group = (
            (
                await session.execute(
                    text(
                        f"SELECT source_key,source_record_ordinal,encode(provider_group_global_id_128,'hex') AS provider_group_ref "
                        f"FROM {scope_fixture.source.schema}.ptg2_provider_group_tax_identity_source WHERE source_key=0"
                    )
                )
            )
            .mappings()
            .one()
        )
    context = capture.RegistryPTGOfficeCaptureContext(
        UUID(scope_by_field["scope_id"]),
        scope_by_field["client_id"],
        scope_by_field["approval_sha256"],
        RegistryNetworkSourceCoordinates(**scope_by_field["coordinates"]),
        specification,
        frozen.as_dict(),
        scope_by_field["evidence"]["graph_identity"],
        scope_fixture.service.store,
        control_schema,
        roles[0],
        (roles[1],),
    )
    return context, scope_by_field, group


async def _grant_retained_reader(fixture, scope_fixture):
    publisher, retained_reader = scope_fixture.roles[2], fixture.roles["reader"]
    await fixture.connection.execute(f'GRANT "{retained_reader}" TO "{publisher}"')
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{retained_reader}"')
    await fixture.connection.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{retained_reader}"'
    )


async def _cleanup_office_resources(
    fixture, scope_fixture, roles, schemas, copy_targets, created_roles, is_granted, journal
):
    for schema in schemas + [candidate.schema_name for candidate in copy_targets]:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema) is None
    if is_granted:
        await fixture.connection.execute(f'REVOKE "{fixture.roles["reader"]}" FROM "{scope_fixture.roles[2]}"')
        await _publisher_grants(fixture.connection, scope_fixture, roles, grant=False)
    for role in reversed(created_roles):
        await fixture.connection.execute(f'DROP ROLE "{role}"')
        assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
    journal.write_text(
        json.dumps(
            {
                "phase": "cleanup_verified",
                "roles": roles,
                "schemas": schemas,
                "retained_schemas": [candidate.schema_name for candidate in copy_targets],
            }
        )
    )


@pytest.fixture
async def office_db(custom_db, tmp_path, monkeypatch):
    """Register candidate/role cleanup before provisioning native capabilities."""
    _app_authority(monkeypatch)
    roles = ["office_capture_" + kind + "_" + uuid4().hex for kind in ("owner", "consumer")]
    consumer_password = uuid4().hex
    journal = tmp_path / "office-cleanup-registration.json"
    schemas, copy_targets, created_roles = [], [], []
    journal.write_text(json.dumps({"phase": "registered", "roles": roles, "schemas": schemas}))
    async with _native_scope(tmp_path) as scope_fixture:
        is_granted = False
        try:
            for role in roles:
                assert not await custom_db.connection.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role
                )
                await custom_db.connection.execute(
                    f'CREATE ROLE "{role}" '
                    + ("NOLOGIN" if role == roles[0] else f"LOGIN PASSWORD '{consumer_password}'")
                )
                created_roles.append(role)
            serving, sites = await _publish_sites(custom_db, copy_targets, journal, roles)
            is_granted = True
            await _publisher_grants(custom_db.connection, scope_fixture, roles, grant=True)
            await _grant_retained_reader(custom_db, scope_fixture)
            context, scope_by_field, group = await _capture_context(scope_fixture, custom_db.control_schema, roles)
            office_rows = [_office_row(scope_by_field, site, group, ordinal) for ordinal, site in enumerate(sites, 1)]
            yield SimpleNamespace(
                source=scope_fixture,
                connection=custom_db.connection,
                context=context,
                serving=serving,
                scope=scope_by_field,
                rows=office_rows,
                roles=roles,
                consumer_password=consumer_password,
                schemas=schemas,
                targets=copy_targets,
                journal=journal,
                sessions=scope_fixture.reader,
            )
        finally:
            await _cleanup_office_resources(
                custom_db,
                scope_fixture,
                roles,
                schemas,
                copy_targets,
                created_roles,
                is_granted,
                journal,
            )


async def _stream(rows):
    for row in rows:
        yield capture._canonical([row])


async def _prepare(fixture, request, batches):
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        descriptor = await capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches)
        assert await session.scalar(text("SELECT current_user")) == fixture.source.roles[2]
        return descriptor


def test_compiled_fixture_frames_preserve_whole_input_and_cross_batch_duplicate_grain():
    scope_by_field = codec_scope()
    site_by_field = {
        "provider_system": "npi",
        "provider_id": "1999999901",
        "location_id": "11111111-1111-4111-8111-111111111111",
        "location_key": "e" * 64,
        "address_row_sha256": "f" * 64,
    }
    group_by_field = {"source_key": 0, "source_record_ordinal": 0, "provider_group_ref": "1" * 32}
    first = _office_row(scope_by_field, site_by_field, group_by_field, 1)
    fixture = SimpleNamespace(scope=scope_by_field)
    distinct = _rebuilt_row(fixture, first, 2, location_id="22222222-2222-4222-8222-222222222222")
    duplicate_record = _rebuilt_row(fixture, distinct, 2, source_record_key=first["source_record_key"])
    duplicate_office = _rebuilt_row(fixture, first, 2)
    for second in (distinct, duplicate_record, duplicate_office):
        request = codec_request([first, second])
        context = capture._codec_context(scope_by_field, request)
        canonical_parts = []
        for after, row in enumerate((first, second)):
            encoded, canonical, count, last, parsed = capture._batch(
                capture._encoder(), capture._canonical([row]), context, after
            )
            assert count == 1 and last == after + 1 and parsed == [row]
            assert encoded.startswith(b"PGCOPY\n\xff\r\n\0")
            canonical_parts.append(canonical)
        assert hashlib.sha256(b"".join(canonical_parts)).hexdigest() == request.canonical_input_sha256


@pytest.mark.asyncio
async def test_native_candidate_copy_adopts_exact_sites_and_select_only_consumer(office_db):
    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    document = descriptor.as_dict()
    assert document["manifest"]["state"] == "prepared"
    assert document["manifest"]["accounting"]["input_row_count"] == 2
    assert document["manifest"]["accounting"]["canonical_input_sha256"] == request.canonical_input_sha256
    assert document["manifest"]["command"]["retained_generation_id"] == fixture.serving.generation_id
    assert document["custody"]["owner_role"] == fixture.roles[0] and document["custody"]["heaps"] == 2
    consumer_url = fixture.source.engine.url.set(
        drivername="postgresql", username=fixture.roles[1], password=fixture.consumer_password
    )
    consumer = await asyncpg.connect(consumer_url.render_as_string(hide_password=False))
    try:
        office_rows = await consumer.fetch(
            f'SELECT provider_id,location_id::text FROM "{descriptor.schema_name}".office_assertion'
        )
        assert len(office_rows) == request.input_row_count
        assert {office_row["location_id"] for office_row in office_rows} == {
            office_row["location_id"] for office_row in fixture.rows
        }
        assert {office_row["provider_id"] for office_row in office_rows} == {"1999999901"}
        assert (
            await consumer.fetchval(f'SELECT manifest_sha256 FROM "{descriptor.schema_name}".capture_manifest')
            == descriptor.manifest_sha256
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await consumer.execute(f'DELETE FROM "{descriptor.schema_name}".office_assertion')
        assert not await consumer.fetchval("SELECT pg_has_role(current_user,$1,'SET')", fixture.roles[0])
    finally:
        await consumer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["short", "extra", "malformed", "digest", "site"])
async def test_native_whole_input_failure_rolls_back_savepoint_keeps_outer_transaction(office_db, damage):
    fixture = office_db
    input_rows = fixture.rows[:1] if damage == "short" else fixture.rows
    if damage == "site":
        input_rows = [_rebuilt_row(fixture, fixture.rows[0], 1, address_row_sha256="0" * 64)]
    request = await _registered_request(fixture, input_rows if damage == "site" else fixture.rows)
    if damage == "extra":
        request = replace(request, input_row_count=1)
    if damage == "digest":
        request = replace(request, canonical_input_sha256="0" * 64)

    async def batches():
        if damage == "malformed":
            yield b"["
        else:
            async for raw in _stream(input_rows):
                yield raw

    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        error_type, error_message = {
            "short": (capture.RegistryPTGOfficeCaptureError, "^registry_ptg_office_accounting_invalid$"),
            "extra": (capture.RegistryPTGOfficeCaptureError, "^registry_ptg_office_accounting_invalid$"),
            "digest": (capture.RegistryPTGOfficeCaptureError, "^registry_ptg_office_accounting_invalid$"),
            "malformed": (ValueError, r'^\{"code":"registry_ptg_input_invalid","row_ordinal":null\}$'),
            "site": (RetainedSiteAdoptionError, "^Retained site adoption selection is unresolved$"),
        }[damage]
        with pytest.raises(error_type, match=error_message):
            await capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches())
        driver = await capture.native_driver(session)
        assert (
            await driver.fetchval("SELECT to_regnamespace($1)", "registry_ptg_office_" + request.capture_id.hex) is None
        )
        assert session.in_transaction() and await session.scalar(text("SELECT 1")) == 1
        assert await session.scalar(text("SELECT current_user")) == fixture.source.roles[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate", ["record", "office"])
async def test_native_global_duplicates_across_copy_batches_refuse(office_db, duplicate):
    fixture = office_db
    second = _rebuilt_row(
        fixture,
        fixture.rows[1 if duplicate == "record" else 0],
        2,
        source_record_key=fixture.rows[0]["source_record_key"] if duplicate == "record" else None,
    )
    rows = [fixture.rows[0], second]
    request = await _registered_request(fixture, rows)
    with pytest.raises(asyncpg.UniqueViolationError):
        await _prepare(fixture, request, _stream(rows))
    assert (
        await fixture.connection.fetchval("SELECT to_regnamespace($1)", "registry_ptg_office_" + request.capture_id.hex)
        is None
    )


@pytest.mark.asyncio
async def test_native_cancellation_after_first_copy_preserves_outer_savepoint_owner(office_db):
    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    copied = asyncio.Event()

    async def batches():
        yield capture._canonical([fixture.rows[0]])
        copied.set()
        await asyncio.Event().wait()

    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        task = asyncio.create_task(
            capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches())
        )
        try:
            await asyncio.wait_for(copied.wait(), 3)
            driver = await capture.native_driver(session)
            assert (
                await driver.fetchval(
                    f'SELECT count(*) FROM "registry_ptg_office_{request.capture_id.hex}".office_assertion'
                )
                == 1
            )
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
            assert (
                await driver.fetchval("SELECT to_regnamespace($1)", "registry_ptg_office_" + request.capture_id.hex)
                is None
            )
            assert session.in_transaction() and await session.scalar(text("SELECT 1")) == 1
            assert await session.scalar(text("SELECT current_user")) == fixture.source.roles[2]
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("grants", ["table_column", "unsafe_default"])
async def test_native_grant_closure_revokes_or_refuses_and_rolls_back(office_db, grants):
    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    schema = "registry_ptg_office_" + request.capture_id.hex
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver = await capture.native_driver(session)

        async def batches():
            if grants == "unsafe_default":
                await driver.execute(
                    f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT UPDATE ON TABLES TO "{fixture.roles[1]}"'
                )
            else:
                await driver.execute(f'GRANT UPDATE ON "{schema}".office_assertion TO "{fixture.roles[1]}"')
                await driver.execute(
                    f'GRANT INSERT(provider_id) ON "{schema}".office_assertion TO "{fixture.roles[1]}"'
                )
            async for raw in _stream(fixture.rows):
                yield raw

        if grants == "unsafe_default":
            with pytest.raises(
                NetworkCustomAddressSourceError, match="^Native owner or runtime privileges are not closed$"
            ):
                await capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches())
            assert await driver.fetchval("SELECT to_regnamespace($1)", schema) is None
        else:
            descriptor = await capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches())
            assert descriptor.schema_name == schema
            table_oid = json.loads(descriptor.custody_json)["table_oid"]
            assert not await driver.fetchval(
                "SELECT has_table_privilege($1,$2::oid,'UPDATE')", fixture.roles[1], table_oid
            )
            assert not await driver.fetchval(
                "SELECT has_any_column_privilege($1,$2::oid,'INSERT,UPDATE,REFERENCES')",
                fixture.roles[1],
                table_oid,
            )
        assert await driver.fetchval("SELECT current_user") == fixture.source.roles[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["rls", "view", "extra_heap", "column_acl", "column_default", "owner"])
async def test_native_catalog_substitutions_refuse_and_rollback_restores(office_db, damage):
    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    namespace = '"' + descriptor.schema_name + '"'
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        statement_by_damage = {
            "rls": f"ALTER TABLE {namespace}.office_assertion ENABLE ROW LEVEL SECURITY",
            "view": f"ALTER TABLE {namespace}.office_assertion RENAME TO replaced_heap; CREATE VIEW {namespace}.office_assertion AS SELECT * FROM {namespace}.replaced_heap",
            "extra_heap": f"CREATE TABLE {namespace}.unregistered_fact(id integer)",
            "column_acl": f'GRANT UPDATE(provider_id) ON {namespace}.office_assertion TO "{fixture.roles[1]}"',
            "column_default": f"ALTER TABLE {namespace}.office_assertion ALTER provider_id SET DEFAULT '1999999901'",
            "owner": f"ALTER TABLE {namespace}.office_assertion OWNER TO CURRENT_USER",
        }
        await fixture.connection.execute(statement_by_damage[damage])
        error_type, error_message = (
            (NetworkCustomAddressSourceError, "^Native owner or runtime privileges are not closed$")
            if damage == "column_acl"
            else (capture.RegistryPTGOfficeCaptureError, "^registry_ptg_office_custody_invalid$")
        )
        with pytest.raises(error_type, match=error_message):
            await capture._custody(fixture.connection, descriptor.schema_name, fixture.context)
    finally:
        await transaction.rollback()
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await capture._custody(fixture.connection, descriptor.schema_name, fixture.context) == json.loads(
            descriptor.custody_json
        )


def test_compiled_capture_encoder_has_exact_copy_shape_and_canonical_stream():
    encoded, canonical, count, last, rows = capture._batch(
        capture._encoder(), capture._canonical([_row()]), capture._codec_context(_scope(), _request()), 0
    )
    assert count == last == 1 and canonical == capture._canonical(_row()) + b"\n"
    assert rows == [_row()] and encoded[:11] == b"PGCOPY\n\xff\r\n\0"
    assert struct.unpack_from(">h", encoded, 19)[0] == len(capture.COPY_COLUMNS) == 20
    assert len(capture._COLUMN_TYPES) == 20


@pytest.mark.parametrize("change", ["extra", "ordinal", "witness", "office", "provider", "duplicate"])
def test_compiled_encoder_rejects_invalid_full_office_assertion(change):
    row = _row()
    if change == "extra":
        row["admitted"] = True
    if change == "ordinal":
        row["ordinal"] = 2
    if change == "witness":
        row["provider_witness_sha256"] = "0" * 64
    if change == "office":
        row["office_evidence_json"]["location_id"] = str(OTHER)
    if change == "provider":
        row["provider_id"] = "1234567890"
    rows = [row] if change != "duplicate" else [row, {**row, "ordinal": 2}]
    with pytest.raises(ValueError):
        capture._batch(capture._encoder(), capture._canonical(rows), capture._codec_context(_scope(), _request()), 0)
