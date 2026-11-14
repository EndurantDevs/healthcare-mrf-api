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
from contextlib import AsyncExitStack
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_ptg_office_capture as capture
from process import registry_ptg_scope_engine as scope_engine
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_network_candidate
from process.network_membership_publication import publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.ptg_parts.result_archive_source_authority import prepare_ptg_result_archive_source_authority
from process.registry_ptg_office_custody import RegistryPTGOfficeCustodyProfile, office_preparation_roles
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
        await probe.execute("CREATE EXTENSION IF NOT EXISTS intarray WITH SCHEMA public")
        await probe.execute("CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public")
        await probe.execute("CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public")
    finally:
        await probe.close()
    fixture = original_serving_schema.__wrapped__()
    try:
        yield await anext(fixture)
    finally:
        await fixture.aclose()


async def _seed_retained_membership(fixture, copy_target, source_table, provider_id, network_id):
    connection = fixture.connection
    location_key_sql = "location_key"
    if getattr(fixture, "source_aliases", False):
        location_key_sql = "first_value(location_key) OVER(ORDER BY location_key)"
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
              SELECT 'npi',$1,md5(location_key)::uuid,{location_key_sql},entity_type,entity_id FROM {source_table}''',
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


async def _stamp_address_witness(connection, source_table):
    """Give retained fixture rows genuine canonical keys without changing site UUIDs."""
    import ptg2_address_canon

    addresses = await connection.fetch(
        f"SELECT location_key,first_line,second_line,city_name,state_name,postal_code,country_code FROM {source_table} ORDER BY location_key"
    )
    canonical = ptg2_address_canon.canonicalize_batch([tuple(address_row.values())[1:] for address_row in addresses])
    await connection.execute(
        f"UPDATE {source_table} address SET address_key=item.address_key FROM jsonb_to_recordset($1::jsonb) "
        "AS item(location_key text,address_key uuid) WHERE address.location_key=item.location_key",
        json.dumps(
            [
                {"location_key": address_row["location_key"], "address_key": canonical_address["address_key"]}
                for address_row, canonical_address in zip(addresses, canonical, strict=True)
            ]
        ),
    )


async def _publish_sites(fixture, copy_targets, journal, roles):
    """Use the existing full address models and genuine publication pipeline."""
    connection = fixture.connection
    _, source_table = await _source_addresses(fixture, 2, "npi")
    provider_id = "1999999901"
    await connection.execute(f"UPDATE {source_table} SET entity_id=$1,npi=$2", provider_id, int(provider_id))
    await connection.execute(f'UPDATE "{fixture.source_schema}".npi SET npi=$1', int(provider_id))
    await _stamp_address_witness(connection, source_table)
    network = await _draft(fixture, _create("network"), _actor())
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    assert await connection.fetchval("SELECT to_regnamespace($1)", copy_target.schema_name) is None
    copy_targets.append(copy_target)
    fixture.preparation_ids.append(copy_target.candidate_id)
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
    await prepare_network_candidate(
        connection,
        copy_target,
        fixture.base,
        **_roles(fixture),
        control_schema=fixture.control_schema,
    )
    await fixture.seal_candidate(copy_target)
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        await publish_network_candidate(
            fixture.execution_connection,
            copy_target,
            control_schema=fixture.control_schema,
            preparation_roles=fixture.preparation_roles,
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
    from process.registry_ptg_published_provisioning import install_registry_ptg_office_preparation_permissions

    publisher = fixture.roles[2]
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    if grant:
        async with connection.transaction():
            await install_registry_ptg_office_preparation_permissions(
                connection,
                profile=RegistryPTGOfficeCustodyProfile(roles[0], publisher),
                store=fixture.service.store,
                reader_roles=(roles[1],),
            )
    else:
        await connection.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{publisher}"')
        recipients = ",".join('"' + role + '"' for role in (publisher, fixture.roles[1], roles[1]))
        await connection.execute(f"REVOKE ALL ON SCHEMA hp_snapshot_retention FROM {recipients}")
        await connection.execute(
            "REVOKE ALL ON hp_snapshot_retention.preparation,hp_snapshot_retention.preparation_claim,"
            "hp_snapshot_retention.preparation_relation,hp_snapshot_retention.relation FROM " + recipients
        )
        columns = await connection.fetchval(
            "SELECT string_agg(quote_ident(column_name),',' ORDER BY ordinal_position) FROM information_schema.columns "
            "WHERE table_schema='hp_snapshot_retention' AND table_name='preparation'"
        )
        await connection.execute(f"REVOKE ALL ({columns}) ON hp_snapshot_retention.preparation FROM {recipients}")


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
        tuple(sorted((roles[1], scope_fixture.roles[1]))),
        RegistryPTGOfficeCustodyProfile(roles[0], scope_fixture.roles[2]),
    )
    return context, scope_by_field, group


async def _grant_retained_reader(fixture, scope_fixture):
    publisher, retained_reader = scope_fixture.roles[2], fixture.roles["reader"]
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{retained_reader}"')
    await fixture.connection.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{retained_reader}"'
    )
    for table in (
        "registry_ptg_producer_scope",
        "registry_ptg_published_plan_scope",
        "registry_ptg_office_approval",
        "registry_ptg_office_hold",
    ):
        await fixture.connection.execute(
            f'REVOKE SELECT ON "{fixture.control_schema}"."{table}" FROM "{retained_reader}"'
        )
    await fixture.connection.execute(f'GRANT "{retained_reader}" TO "{publisher}"')


async def _office_reader_grants(fixture, scope_fixture, roles, *, grant):
    reader = '"' + roles[1] + '"'
    verb, preposition = ("GRANT", "TO") if grant else ("REVOKE", "FROM")
    source = scope_fixture.source.schema
    control = '"' + scope_fixture.control + '"'
    for statement in (
        f"{verb} USAGE ON SCHEMA {source} {preposition} {reader}",
        f"{verb} SELECT ON ALL TABLES IN SCHEMA {source} {preposition} {reader}",
        f"{verb} UPDATE ON {source}.ptg2_snapshot {preposition} {reader}",
        f"{verb} USAGE ON SCHEMA {control} {preposition} {reader}",
        f"{verb} SELECT ON {control}.registry_ptg_producer_scope {preposition} {reader}",
        f'{verb} "{fixture.roles["reader"]}" {preposition} {reader}',
    ):
        await fixture.connection.execute(statement)


async def _cleanup_office_preparations(fixture, schemas):
    identities = [UUID(schema.removeprefix("registry_ptg_office_")) for schema in schemas]
    if not identities:
        return
    async with fixture.connection.transaction():
        entries = await fixture.connection.fetch(
            "SELECT preparation_id,node_id,importer_id FROM hp_snapshot_retention.preparation "
            "WHERE preparation_id=ANY($1::uuid[]) FOR UPDATE NOWAIT",
            identities,
        )
        assert all(entry["node_id"] == fixture.control_schema and entry["importer_id"] == "office" for entry in entries)
        for suffix in ("_relation", "_claim", ""):
            await fixture.connection.execute(
                "DELETE FROM hp_snapshot_retention.preparation" + suffix + " WHERE preparation_id=ANY($1::uuid[])",
                identities,
            )


async def _cleanup_office_resources(
    fixture, scope_fixture, roles, schemas, copy_targets, created_roles, is_granted, journal
):
    for name in fixture.office_dependencies:
        await fixture.connection.execute(f'DROP VIEW IF EXISTS "{fixture.control_schema}"."{name}" RESTRICT')
    await _cleanup_office_preparations(fixture, schemas)
    for schema in schemas + [candidate.schema_name for candidate in copy_targets]:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema) is None
    if is_granted:
        await _office_reader_grants(fixture, scope_fixture, roles, grant=False)
        await fixture.connection.execute(f'REVOKE "{fixture.roles["reader"]}" FROM "{scope_fixture.roles[2]}"')
        await _publisher_grants(fixture.connection, scope_fixture, roles, grant=False)
    for role in reversed(created_roles):
        await fixture.connection.execute(f'DROP OWNED BY "{role}"')
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


async def _create_office_roles(connection, roles, created_roles, password):
    for role in roles:
        assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        await connection.execute(
            f'CREATE ROLE "{role}" ' + ("NOLOGIN" if role == roles[0] else f"LOGIN PASSWORD '{password}'")
        )
        created_roles.append(role)


def _office_consumer_engine(engine, role, password):
    return create_async_engine(
        engine.url.set(username=role, password=password),
        pool_size=1,
        max_overflow=0,
        hide_parameters=True,
    )


@pytest.fixture
async def office_db(custom_db, tmp_path, monkeypatch, *, complete_graph=False):
    """Register candidate/role cleanup before provisioning native capabilities."""
    _app_authority(monkeypatch)
    roles = ["office_capture_" + kind + "_" + uuid4().hex for kind in ("owner", "consumer")]
    consumer_password = uuid4().hex
    journal = tmp_path / "office-cleanup-registration.json"
    schemas, copy_targets, created_roles, dependencies = [], [], [], []
    custom_db.office_dependencies = dependencies
    journal.write_text(json.dumps({"phase": "registered", "roles": roles, "schemas": schemas}))
    async with AsyncExitStack() as cleanup, _native_scope(tmp_path, complete_graph=complete_graph) as scope_fixture:
        if not callable(getattr(custom_db, "seal_candidate", None)):
            from tests.test_network_compact_publication_postgres import _named_compact_runtime

            await _named_compact_runtime(custom_db, cleanup, monkeypatch)
        is_granted = False
        reader_engine = None
        try:
            await _create_office_roles(custom_db.connection, roles, created_roles, consumer_password)
            serving, sites = await _publish_sites(custom_db, copy_targets, journal, roles)
            is_granted = True
            await _publisher_grants(custom_db.connection, scope_fixture, roles, grant=True)
            await _grant_retained_reader(custom_db, scope_fixture)
            await _office_reader_grants(custom_db, scope_fixture, roles, grant=True)
            context, scope_by_field, group = await _capture_context(scope_fixture, custom_db.control_schema, roles)
            office_rows = [_office_row(scope_by_field, site, group, ordinal) for ordinal, site in enumerate(sites, 1)]
            reader_engine = _office_consumer_engine(scope_fixture.engine, roles[1], consumer_password)
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
                dependencies=dependencies,
                sessions=scope_fixture.reader,
                consumer_sessions=async_sessionmaker(reader_engine),
                native_sessions=async_sessionmaker(custom_db.engine),
                retained_reader=custom_db.roles["reader"],
            )
        finally:
            if reader_engine is not None:
                await reader_engine.dispose()
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


@pytest.fixture
async def complete_office_db(custom_db, tmp_path, monkeypatch):
    """Select the actual dense bidirectional source graph for office discovery."""
    fixture = office_db.__wrapped__(custom_db, tmp_path, monkeypatch, complete_graph=True)
    try:
        yield await anext(fixture)
    finally:
        await fixture.aclose()


async def _stream(rows):
    for row in rows:
        yield capture._canonical([row])


async def _prepare(fixture, request, batches, *, before_freeze=None):
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        pending = await capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches)
        assert await session.scalar(text("SELECT current_user")) == fixture.source.roles[2]
        assert pending["state"] == "handed_off" and pending["capture_id"] == str(request.capture_id)
    if before_freeze is not None:
        await before_freeze(fixture, request)
    return await _complete_office_preparation(fixture, request)


async def _complete_office_preparation(fixture, request):
    """Use the real administrator freeze and complete reader witness before recording validation."""
    from directory_catalog_runtime import freeze_registry_generation

    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
    from process.registry_ptg_office_approval import _descriptor
    from process.registry_ptg_office_preparation import (
        admit_office_preparation,
        load_named_office_preparation,
        office_validation_report,
    )
    from process.registry_ptg_office_witness import verify_registry_ptg_office_witness

    candidate_id = str(request.capture_id)
    handoff = await load_named_office_preparation(
        fixture.connection,
        candidate_id,
        control_schema=fixture.context.control_schema,
    )
    assert handoff is not None and handoff["kind"] == "office"
    config_by_field = {field: handoff[field] for field in ("kind", "candidate_id", "catalog_schema", "storage_schema")}
    config_by_field["preparation_roles"] = office_preparation_roles(fixture.context)
    async with fixture.native_sessions.kw["bind"].connect() as administrator:
        assert await freeze_registry_generation(administrator, config_by_field, candidate_id) == {
            "preparation_id": candidate_id,
            "state": "frozen",
        }
    async with fixture.consumer_sessions() as reader, reader.begin():
        await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        descriptor = await _descriptor(reader, fixture.context, request)
        witness = await verify_registry_ptg_office_witness(
            reader,
            fixture.context,
            request,
            descriptor,
            read_budget=RegistryPTGGraphReadBudget(64 * 1024 * 1024),
        )
    async with fixture.sessions() as publisher, publisher.begin():
        driver = await capture.native_driver(publisher)
        store, preparation, claim = await admit_office_preparation(driver, request.capture_id, context=fixture.context)
        assert preparation["state"] == "frozen" and claim is not None
        validated = await store.validate_named(
            claim,
            {
                "kind": "office",
                "candidate_id": candidate_id,
                "data": office_validation_report(descriptor, witness),
            },
        )
        assert validated["state"] == "validated"
        await store.yield_named_claim(claim)
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
    descriptor = await _prepare(
        fixture,
        request,
        _stream(fixture.rows),
        before_freeze=_assert_preparation_not_forgeable,
    )
    document = descriptor.as_dict()
    assert document["manifest"]["state"] == "prepared"
    assert document["manifest"]["accounting"]["input_row_count"] == 2
    assert document["manifest"]["accounting"]["canonical_input_sha256"] == request.canonical_input_sha256
    assert document["manifest"]["command"]["retained_generation_id"] == fixture.serving.generation_id
    assert document["storage"] == capture.office_storage(descriptor.schema_name, descriptor.manifest_sha256)
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
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await consumer.execute('SET ROLE "' + fixture.roles[0] + '"')
    finally:
        await consumer.close()
    await _assert_office_readonly(fixture, descriptor)


async def _assert_preparation_not_forgeable(fixture, request):
    """Exercise protected freeze columns and lifecycle CHECKs on the actual handed-off row."""
    relation = "hp_snapshot_retention.preparation"
    url = fixture.sessions.kw["bind"].url.set(drivername="postgresql")
    publisher = await asyncpg.connect(
        url.render_as_string(hide_password=False),
        server_settings={"statement_timeout": "3s", "lock_timeout": "250ms"},
    )
    before = await fixture.connection.fetchrow(
        f"SELECT state,frozen_inventory,frozen_sha256 FROM {relation} WHERE preparation_id=$1",
        request.capture_id,
    )
    assert tuple(before) == ("handed_off", None, None)
    attempts = (
        (
            f"UPDATE {relation} SET frozen_inventory=intake_inventory,frozen_sha256=intake_sha256 WHERE preparation_id=$1",
            (request.capture_id,),
            asyncpg.InsufficientPrivilegeError,
        ),
        (
            f"INSERT INTO {relation} SELECT (jsonb_populate_record(NULL::{relation},to_jsonb(preparation)||"
            "jsonb_build_object('preparation_id',$2::text,'generation_id',$2::text,'run_id',$2::text,"
            "'dataset_key',$2::text,'state','frozen','frozen_inventory',intake_inventory,'frozen_sha256',intake_sha256))).* "
            f"FROM {relation} preparation WHERE preparation_id=$1",
            (request.capture_id, str(uuid4())),
            asyncpg.InsufficientPrivilegeError,
        ),
        (
            f"UPDATE {relation} SET state='frozen' WHERE preparation_id=$1",
            (request.capture_id,),
            (asyncpg.InsufficientPrivilegeError, asyncpg.CheckViolationError),
        ),
    )
    try:
        for statement, arguments, expected_error in attempts:
            transaction = publisher.transaction()
            await transaction.start()
            try:
                with pytest.raises(expected_error):
                    await publisher.execute(statement, *arguments)
            finally:
                await transaction.rollback()
        assert (
            await fixture.connection.fetchrow(
                f"SELECT state,frozen_inventory,frozen_sha256 FROM {relation} WHERE preparation_id=$1",
                request.capture_id,
            )
            == before
        )
    finally:
        await publisher.close()


async def _assert_office_readonly(fixture, descriptor):
    namespace = '"' + descriptor.schema_name + '"'
    statements = (
        f"INSERT INTO {namespace}.office_assertion SELECT * FROM {namespace}.office_assertion",
        f"UPDATE {namespace}.office_assertion SET provider_id=provider_id",
        f"DELETE FROM {namespace}.office_assertion",
        f"TRUNCATE {namespace}.office_assertion",
        f"ALTER TABLE {namespace}.office_assertion ADD COLUMN forbidden integer",
        f"DROP TABLE {namespace}.office_assertion",
        f"CREATE TABLE {namespace}.forbidden(id integer)",
        'SET LOCAL ROLE "' + fixture.roles[0] + '"',
    )
    for sessions in (fixture.sessions, fixture.consumer_sessions, fixture.source.approver):
        url = sessions.kw["bind"].url.set(drivername="postgresql")
        actor = await asyncpg.connect(
            url.render_as_string(hide_password=False),
            server_settings={"statement_timeout": "3s", "lock_timeout": "250ms"},
        )
        try:
            assert (
                await actor.fetchval(f"SELECT count(*) FROM {namespace}.office_assertion")
                == json.loads(descriptor.manifest_json)["accounting"]["input_row_count"]
            )
            for statement in statements:
                await _deny_office_statement(actor, statement)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await actor.copy_records_to_table("office_assertion", schema_name=descriptor.schema_name, records=[])
        finally:
            await actor.close()


async def _deny_office_statement(actor, statement):
    transaction = actor.transaction()
    await transaction.start()
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await actor.execute(statement)
    finally:
        await transaction.rollback()


@pytest.mark.asyncio
async def test_native_frozen_capture_stays_read_only_while_successor_loads(office_db):
    fixture = office_db
    first_request = await _registered_request(fixture, fixture.rows)
    first = await _prepare(fixture, first_request, _stream(fixture.rows))
    before = await fixture.connection.fetchval(
        f'SELECT jsonb_agg(to_jsonb(office) ORDER BY ordinal)::text FROM "{first.schema_name}".office_assertion office'
    )
    successor_request = await _registered_request(fixture, fixture.rows)

    async def successor_rows():
        await _assert_office_readonly(fixture, first)
        async for batch in _stream(fixture.rows):
            yield batch

    successor = await _prepare(fixture, successor_request, successor_rows())
    assert successor.capture_id != first.capture_id and successor.schema_name != first.schema_name
    assert (
        await fixture.connection.fetchval(
            f'SELECT jsonb_agg(to_jsonb(office) ORDER BY ordinal)::text FROM "{first.schema_name}".office_assertion office'
        )
        == before
    )
    await _assert_office_readonly(fixture, first)
    await _assert_office_readonly(fixture, successor)


@pytest.mark.asyncio
async def test_native_unheld_admin_release_rolls_back_dependency_and_replays_exactly(office_db):
    from directory_catalog_runtime import release_registry_generation

    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    candidate_id = str(request.capture_id)
    config_by_field = {
        "kind": "office",
        "candidate_id": candidate_id,
        "catalog_schema": fixture.context.control_schema,
        "storage_schema": descriptor.schema_name,
        "preparation_roles": office_preparation_roles(fixture.context),
    }
    dependency = "office_release_dependency_" + uuid4().hex
    fixture.dependencies.append(dependency)
    fixture.journal.write_text(
        json.dumps({**json.loads(fixture.journal.read_text()), "dependencies": fixture.dependencies})
    )
    await fixture.connection.execute(
        f'CREATE VIEW "{fixture.context.control_schema}"."{dependency}" AS SELECT ordinal FROM "{descriptor.schema_name}".office_assertion'
    )
    before = await fixture.connection.fetchrow(
        "SELECT state,cancellation_requested,claim_token,claim_attempt FROM hp_snapshot_retention.preparation WHERE preparation_id=$1",
        request.capture_id,
    )
    async with fixture.native_sessions.kw["bind"].connect() as administrator:
        with pytest.raises(asyncpg.DependentObjectsStillExistError):
            await release_registry_generation(administrator, config_by_field, candidate_id)
    assert (
        await fixture.connection.fetchrow(
            "SELECT state,cancellation_requested,claim_token,claim_attempt FROM hp_snapshot_retention.preparation WHERE preparation_id=$1",
            request.capture_id,
        )
        == before
    )
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{descriptor.schema_name}".office_assertion') == 2
    await fixture.connection.execute(f'DROP VIEW "{fixture.context.control_schema}"."{dependency}" RESTRICT')
    for _ in range(2):
        async with fixture.native_sessions.kw["bind"].connect() as administrator:
            assert await release_registry_generation(administrator, config_by_field, candidate_id) == {
                "preparation_id": candidate_id,
                "state": "abandoned",
            }
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", descriptor.schema_name) is None
    abandoned = await fixture.connection.fetchrow(
        "SELECT state,cancellation_requested FROM hp_snapshot_retention.preparation WHERE preparation_id=$1",
        request.capture_id,
    )
    assert tuple(abandoned) == ("abandoned", True)


async def _binding_batch(fixture, session, descriptor, request, *, limit=1):
    """Use real page/adoption SQL after the existing native capture preparation."""
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.registry_ptg_office_membership import _VerifiedOfficeSource, read_ptg_office_membership_batch
    from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource

    driver = (await (await session.connection()).get_raw_connection()).driver_connection
    control = fixture.context.control_schema
    revision = await driver.fetchval(f'SELECT approved_revision FROM "{control}".registry_revision_control')
    approved = await pin_approved_membership_source(driver, approved_revision=revision, control_schema=control)
    network = await driver.fetchval(f'SELECT min(network_id) FROM "{fixture.serving.schema_name}".network_membership')
    pin = PinnedPTGOfficeMembershipSource(
        str(request.capture_id),
        fixture.context.client_id,
        "a" * 64,
        "b" * 64,
        descriptor.manifest_sha256,
        fixture.context.coordinates,
        network,
        fixture.serving.generation_id,
    )
    verified = _VerifiedOfficeSource(
        session, driver, session.get_transaction(), pin, fixture.context, request, descriptor, fixture.serving
    )
    batch = await read_ptg_office_membership_batch(driver, verified, approved, None, limit, control)
    return driver, batch


async def _office_candidate(fixture, driver, batch, address_source):
    """Register the exact candidate before creating it under the replay snapshot."""
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    fixture.targets.append(copy_target)
    await create_network_candidate(
        driver,
        copy_target,
        source_generations={
            "unified_address": address_source.generation_id,
            "ptg": batch.generation_id,
            "custom_membership": batch.approved_source.generation_id,
        },
        approved_custom_revision=batch.approved_source.approved_revision,
        expected_head=fixture.serving.generation_id,
        expected_rows=batch.membership_rows,
        control_schema=fixture.context.control_schema,
    )
    return copy_target


async def _copy_selected_offices(fixture, driver, batch, address, pin, copy_target, conflict):
    """Preserve explicit UUIDs and refuse a conflicting tuple atomically."""
    from process.network_initial_source_office_bindings import _copy_bindings
    from process.registry_ptg_office_bindings import copy_ptg_office_bindings

    office = batch.office_records[0]
    if conflict:
        wrong_key = next(
            office_row["location_key"]
            for office_row in fixture.rows
            if office_row["location_key"] != office.location_key
        )
        await _copy_bindings(
            driver,
            copy_target,
            [
                (
                    office.provider_system,
                    office.provider_id,
                    UUID(office.location_id),
                    wrong_key,
                    office.entity_type,
                    office.entity_id,
                )
            ],
        )
        with pytest.raises(ValueError, match="initial_aca_office_binding_conflict"):
            await copy_ptg_office_bindings(
                driver,
                batch,
                copy_target,
                pin,
                runtime_roles=(fixture.retained_reader,),
                control_schema=fixture.context.control_schema,
            )
    else:
        receipt = await copy_ptg_office_bindings(
            driver,
            batch,
            copy_target,
            pin,
            runtime_roles=(fixture.retained_reader,),
            control_schema=fixture.context.control_schema,
        )
        assert receipt == {"office_count": 1}
    copied = await driver.fetch(
        f'SELECT location_id::text,location_key FROM "{copy_target.schema_name}".provider_location_binding'
    )
    assert [dict(office_row) for office_row in copied] == [
        {"location_id": office.location_id, "location_key": wrong_key if conflict else office.location_key}
    ]
    other_ids = {
        office_row["location_id"] for office_row in fixture.rows if office_row["location_id"] != office.location_id
    }
    assert other_ids.isdisjoint(office_row["location_id"] for office_row in copied)


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_explicit_site_uuid_keeps_canonical_address_witness_and_other_office(office_db, conflict):
    from process.network_address_projection import PinnedAddressSource
    from process.registry_ptg_office_bindings import pin_ptg_office_address_source

    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    retained = f'"{fixture.serving.schema_name}"'
    before = await fixture.connection.fetchval(
        f"SELECT jsonb_agg(to_jsonb(address) ORDER BY location_key)::text FROM {retained}.entity_address_unified address"
    )
    async with fixture.native_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver, batch = await _binding_batch(fixture, session, descriptor, request, limit=2 if conflict else 1)
        office = batch.office_records[0]
        key = await driver.fetchval(
            f"SELECT address_key FROM {retained}.entity_address_unified WHERE location_key=$1", office.location_key
        )
        assert key is not None and str(key) != office.location_id
        address = PinnedAddressSource(
            fixture.serving.schema_name, "entity_address_unified", fixture.serving.manifest_sha256
        )
        copy_target = await _office_candidate(fixture, driver, batch, address)
        pin = await pin_ptg_office_address_source(
            driver,
            batch,
            address,
            runtime_roles=(fixture.retained_reader,),
            control_schema=fixture.context.control_schema,
        )
        await _copy_selected_offices(fixture, driver, batch, address, pin, copy_target, conflict)

    assert (
        await fixture.connection.fetchval(
            f"SELECT jsonb_agg(to_jsonb(address) ORDER BY location_key)::text FROM {retained}.entity_address_unified address"
        )
        == before
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "address_key", "source_collision", "provider", "hash"])
async def test_explicit_site_copy_refuses_changed_or_unmatched_exact_witness(office_db, damage):
    from process.network_address_projection import PinnedAddressSource
    from process.registry_ptg_office_bindings import pin_ptg_office_address_source

    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    async with fixture.native_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver, batch = await _binding_batch(fixture, session, descriptor, request)
        office = batch.office_records[0]
        address = PinnedAddressSource(
            fixture.serving.schema_name, "entity_address_unified", fixture.serving.manifest_sha256
        )
        retained = f'"{fixture.serving.schema_name}".entity_address_unified'
        assertions = f'"{descriptor.schema_name}".office_assertion'
        statement_by_damage = {
            "missing": (f"DELETE FROM {retained} WHERE location_key=$1", office.location_key),
            "address_key": (
                f"UPDATE {retained} SET address_key=$1 WHERE location_key='{office.location_key}'",
                uuid4(),
            ),
            "source_collision": (
                f"UPDATE {assertions} SET location_key=$1 WHERE ordinal=1",
                fixture.rows[1]["location_key"],
            ),
            "provider": (f"UPDATE {assertions} SET provider_id=$1 WHERE ordinal=1", "1000000004"),
            "hash": (f"UPDATE {assertions} SET address_row_sha256=$1 WHERE ordinal=1", "0" * 64),
        }
        await driver.execute(*statement_by_damage[damage])
        with pytest.raises(RetainedSiteAdoptionError):
            await pin_ptg_office_address_source(
                driver,
                batch,
                address,
                runtime_roles=(fixture.retained_reader,),
                control_schema=fixture.context.control_schema,
            )
        assert not await driver.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'initial_aca_offices_%')"
        )


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
async def test_native_admin_freeze_closes_existing_column_and_default_grants(office_db, grants):
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

        pending = await capture.prepare_registry_ptg_office_capture(session, fixture.context, request, batches())
        assert pending["state"] == "handed_off"
        assert await driver.fetchval("SELECT current_user") == fixture.source.roles[2]
    descriptor = await _complete_office_preparation(fixture, request)
    assert descriptor.schema_name == schema
    await _assert_office_readonly(fixture, descriptor)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["view", "column_default"])
async def test_native_declared_column_shape_refuses_and_rollback_restores(office_db, damage):
    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    namespace = '"' + descriptor.schema_name + '"'
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        statement_by_damage = {
            "view": f"ALTER TABLE {namespace}.office_assertion RENAME TO replaced_heap; CREATE VIEW {namespace}.office_assertion AS SELECT * FROM {namespace}.replaced_heap",
            "column_default": f"ALTER TABLE {namespace}.office_assertion ALTER provider_id SET DEFAULT '1999999901'",
        }
        await fixture.connection.execute(statement_by_damage[damage])
        with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="layout_invalid"):
            await capture._require_office_layout(fixture.connection, descriptor.schema_name, is_published=False)
    finally:
        await transaction.rollback()
    async with fixture.consumer_sessions() as reader, reader.begin():
        from process.registry_ptg_office_approval import _descriptor

        assert await _descriptor(reader, fixture.context, request) == descriptor


@pytest.mark.asyncio
async def test_native_hidden_office_rows_refuse_and_restored_data_serves(office_db):
    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
    from process.registry_ptg_office_witness import verify_registry_ptg_office_witness

    fixture = office_db
    request = await _registered_request(fixture, fixture.rows)
    descriptor = await _prepare(fixture, request, _stream(fixture.rows))
    relation = '"' + descriptor.schema_name + '".office_assertion'
    await fixture.connection.execute(f"ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY")
    try:
        async with fixture.consumer_sessions() as reader, reader.begin():
            with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="accounting_invalid"):
                await verify_registry_ptg_office_witness(
                    reader,
                    fixture.context,
                    request,
                    descriptor,
                    read_budget=RegistryPTGGraphReadBudget(64 * 1024 * 1024),
                )
    finally:
        await fixture.connection.execute(f"ALTER TABLE {relation} DISABLE ROW LEVEL SECURITY")
    async with fixture.consumer_sessions() as reader, reader.begin():
        evidence = await verify_registry_ptg_office_witness(
            reader,
            fixture.context,
            request,
            descriptor,
            read_budget=RegistryPTGGraphReadBudget(64 * 1024 * 1024),
        )
        assert evidence.as_dict()["accounting"]["input_row_count"] == request.input_row_count


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
