# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native office evidence; candidate preparation never grants admission.

In PostgreSQL cases only the upstream scope-authority HTTP boundary is synthetic.
The source store, graph, publication, COPY and catalog checks remain real.
Separate compiled codec cases use scoped synthetic database evidence.
The office consumer has no capture writes or publisher membership. Its separate
source snapshot UPDATE permission permits the existing native row-lock contract.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_ptg_graph_reader as graph
from process import registry_ptg_office_capture as capture
from process import registry_ptg_office_membership as offices
from process import registry_ptg_office_witness as witness
from process import registry_source_recipe_composition as composition
from process.network_custom_address_source import NetworkCustomAddressSourceError
from process.network_serving_read import NetworkServingReadUnavailable
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthorityError
from process.registry_ptg_cohort_authority import RegistryPTGCohortAuthorityError
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError
from process.registry_retained_site_adoption import RetainedSiteAdoptionError
from tests import test_registry_ptg_graph_reader as graph_fixture
from tests import test_registry_ptg_office_capture_postgres as native_fixture
from tests import test_registry_ptg_office_membership_recipe as recipe_fixture
from tests import test_registry_ptg_office_witness as _codec_fixture
from tests.test_registry_ptg_office_witness import _verify as _codec_verify

serving_schema = native_fixture.serving_schema
custom_db = native_fixture.custom_db
office_db = native_fixture.office_db


def _capability_statements(fixture, retained_reader, *, grant):
    consumer = '"' + fixture.roles[1] + '"'
    source_schema = fixture.source.source.schema
    control_schema = '"' + fixture.source.control + '"'
    verb, preposition = ("GRANT", "TO") if grant else ("REVOKE", "FROM")
    return (
        f"{verb} USAGE ON SCHEMA {source_schema} {preposition} {consumer}",
        f"{verb} SELECT ON ALL TABLES IN SCHEMA {source_schema} {preposition} {consumer}",
        f"{verb} UPDATE ON {source_schema}.ptg2_snapshot {preposition} {consumer}",
        f"{verb} USAGE ON SCHEMA {control_schema} {preposition} {consumer}",
        f"{verb} SELECT ON {control_schema}.registry_ptg_producer_scope {preposition} {consumer}",
        f'{verb} "{retained_reader}" {preposition} {consumer}',
    )


@pytest.fixture
async def witness_db(office_db, custom_db, tmp_path):
    """Register exact additional consumer grants before provisioning or cleanup."""
    retained_reader = custom_db.roles["reader"]
    journal = tmp_path / "office-witness-capability-registration.json"
    grants = _capability_statements(office_db, retained_reader, grant=True)
    revocations = _capability_statements(office_db, retained_reader, grant=False)
    journal.write_text(json.dumps({"phase": "registered", "grants": grants, "revocations": revocations}))
    engine = create_async_engine(
        office_db.source.engine.url.set(username=office_db.roles[1], password=office_db.consumer_password),
        pool_size=1,
        max_overflow=0,
        isolation_level="REPEATABLE READ",
        hide_parameters=True,
    )
    try:
        for statement in grants:
            await office_db.connection.execute(statement)
        yield SimpleNamespace(**vars(office_db), consumer_sessions=async_sessionmaker(engine))
    finally:
        await engine.dispose()
        for statement in reversed(revocations):
            await office_db.connection.execute(statement)
        assert not await office_db.connection.fetchval(
            "SELECT pg_has_role($1,$2,'MEMBER')", office_db.roles[1], retained_reader
        )
        assert not await office_db.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'UPDATE')",
            office_db.roles[1],
            office_db.source.source.schema + ".ptg2_snapshot",
        )
        journal.write_text(json.dumps({"phase": "revoked", "grants": grants, "revocations": revocations}))


async def _candidate(fixture):
    request = await native_fixture._registered_request(fixture, fixture.rows)
    descriptor = await native_fixture._prepare(fixture, request, native_fixture._stream(fixture.rows))
    return request, descriptor


async def _verify(fixture, request, descriptor, budget, *, context=None, sessions=None):
    async with (sessions or fixture.consumer_sessions)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        return await witness.verify_registry_ptg_office_witness(
            session, context or fixture.context, request, descriptor, read_budget=budget
        )


def _budget():
    return graph.RegistryPTGGraphReadBudget(1024 * 1024)


def test_native_codec_and_graph_interfaces_are_required():
    import ptg2_address_canon

    assert capture._encoder() is ptg2_address_canon.encode_registry_ptg_capture_batch
    assert callable(ptg2_address_canon.plan_registry_ptg_graph_locator_pages)
    assert callable(ptg2_address_canon.plan_registry_ptg_graph_member_pages)
    assert callable(ptg2_address_canon.verify_registry_ptg_graph_batch)
    native_fixture.test_compiled_fixture_frames_preserve_whole_input_and_cross_batch_duplicate_grain()


async def test_native_office_source_exhaustion(witness_db):
    request, descriptor = await _candidate(witness_db)
    budget = _budget()
    evidence = (await _verify(witness_db, request, descriptor, budget)).as_dict()
    census = evidence["source_witness_census"]
    assert evidence["contract"] == "registry_ptg_office_witness.v1"
    assert evidence["accounting"] == json.loads(descriptor.manifest_json)["accounting"]
    assert evidence["scope_id"] == str(witness_db.context.scope_id)
    assert evidence["scope_approval_sha256"] == witness_db.context.scope_approval_sha256
    assert evidence["custody"] == json.loads(descriptor.custody_json)
    assert census["nonempty_pages"] == 1 and census["terminal_empty_ordinal"] == 2
    assert census["verified_edge_requests"] == 1
    assert evidence["accounting"]["input_row_count"] == 2
    assert evidence["accounting"]["canonical_input_sha256"] == request.canonical_input_sha256
    assert evidence["graph_budget"] == {
        "read_bytes": budget.read_bytes,
        "read_pages": budget.read_pages,
        "coordinates": budget.coordinates,
    }
    assert budget.read_bytes > 0 and budget.read_pages > 0 and budget.coordinates > 0
    assert not {"authorized", "admitted", "reviewed", "full_source_census"}.intersection(evidence)


async def test_native_consumer_and_publisher_capabilities_are_distinct(witness_db):
    request, descriptor = await _candidate(witness_db)
    consumer, publisher, owner = witness_db.roles[1], witness_db.source.roles[2], witness_db.roles[0]
    table = f'"{descriptor.schema_name}".office_assertion'
    source_table = witness_db.source.source.schema + ".ptg2_snapshot"
    manager = witness_db.connection
    assert await manager.fetchval("SELECT has_table_privilege($1,$2,'SELECT')", consumer, table)
    assert not await manager.fetchval("SELECT has_table_privilege($1,$2,'UPDATE,INSERT,DELETE')", consumer, table)
    assert await manager.fetchval("SELECT has_table_privilege($1,$2,'UPDATE')", consumer, source_table)
    assert not await manager.fetchval("SELECT has_table_privilege($1,$2,'INSERT')", consumer, witness_db.source.table)
    assert not await manager.fetchval("SELECT pg_has_role($1,$2,'SET')", consumer, witness_db.source.roles[1])
    assert not await manager.fetchval("SELECT pg_has_role($1,$2,'SET')", consumer, owner)
    assert await manager.fetchval("SELECT pg_has_role($1,$2,'SET')", publisher, owner)
    assert not await manager.fetchval("SELECT pg_has_role($1,$2,'USAGE')", publisher, owner)
    assert not await manager.fetchval("SELECT has_table_privilege($1,$2,'SELECT')", publisher, table)
    budget = _budget()
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await _verify(witness_db, request, descriptor, budget, sessions=witness_db.sessions)
    assert budget.read_bytes == 0
    await _verify(witness_db, request, descriptor, budget)


async def _recreate_valid_heap(fixture, descriptor):
    namespace = '"' + descriptor.schema_name + '"'
    manager = fixture.connection
    await manager.execute(f"CREATE TABLE {namespace}.replacement (LIKE {namespace}.office_assertion INCLUDING ALL)")
    await manager.execute(f"INSERT INTO {namespace}.replacement SELECT * FROM {namespace}.office_assertion")
    await manager.execute(f"DROP TABLE {namespace}.office_assertion")
    await manager.execute(f"ALTER TABLE {namespace}.replacement RENAME TO office_assertion")
    await manager.execute(f'ALTER TABLE {namespace}.office_assertion OWNER TO "{fixture.roles[0]}"')
    await manager.execute(f'GRANT SELECT ON {namespace}.office_assertion TO "{fixture.roles[1]}"')


async def test_native_valid_heap_replacement_refuses_old_oid(witness_db):
    request, descriptor = await _candidate(witness_db)
    namespace = '"' + descriptor.schema_name + '"'
    old_oid = await witness_db.connection.fetchval("SELECT $1::regclass::oid", namespace + ".office_assertion")
    await _recreate_valid_heap(witness_db, descriptor)
    new_oid = await witness_db.connection.fetchval("SELECT $1::regclass::oid", namespace + ".office_assertion")
    assert old_oid != new_oid
    assert await witness_db.connection.fetchval(f"SELECT count(*) FROM {namespace}.office_assertion") == 2
    current_custody = await capture._custody(witness_db.connection, descriptor.schema_name, witness_db.context)
    assert current_custody["columns_sha256"] == json.loads(descriptor.custody_json)["columns_sha256"]
    assert current_custody["table_oid"] == new_oid
    with pytest.raises(witness.RegistryPTGOfficeWitnessError, match="custody_changed"):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("damage", ["document", "digest"])
async def test_native_manifest_substitution_refuses(witness_db, damage):
    request, descriptor = await _candidate(witness_db)
    namespace = '"' + descriptor.schema_name + '"'
    expression = (
        'manifest_json=manifest_json||\'{"state":"substituted"}\'::jsonb'
        if damage == "document"
        else "manifest_sha256=repeat('f',64)"
    )
    await witness_db.connection.execute(f"UPDATE {namespace}.capture_manifest SET {expression}")
    with pytest.raises(witness.RegistryPTGOfficeWitnessError, match="manifest_changed"):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("damage", ["column", "default", "extra_heap"])
async def test_native_catalog_drift_refuses(witness_db, damage):
    request, descriptor = await _candidate(witness_db)
    namespace = '"' + descriptor.schema_name + '"'
    sql_by_damage = {
        "column": f"ALTER TABLE {namespace}.office_assertion ADD COLUMN substituted text",
        "default": f"ALTER TABLE {namespace}.office_assertion ALTER COLUMN ordinal SET DEFAULT 1",
        "extra_heap": f"CREATE TABLE {namespace}.substituted (id bigint)",
    }
    await witness_db.connection.execute(sql_by_damage[damage])
    with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="custody_invalid"):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("damage", ["default_acl", "column_acl"])
async def test_native_write_privilege_drift_refuses(witness_db, damage):
    request, descriptor = await _candidate(witness_db)
    namespace = '"' + descriptor.schema_name + '"'
    owner, consumer = witness_db.roles
    statement = (
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{owner}" IN SCHEMA {namespace} GRANT INSERT ON TABLES TO "{consumer}"'
        if damage == "default_acl"
        else f'GRANT UPDATE(provider_id) ON {namespace}.office_assertion TO "{consumer}"'
    )
    await witness_db.connection.execute(statement)
    with pytest.raises(NetworkCustomAddressSourceError):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("damage", ["missing", "extra"])
async def test_native_whole_office_count_refuses(witness_db, damage):
    request, descriptor = await _candidate(witness_db)
    namespace = '"' + descriptor.schema_name + '"'
    if damage == "missing":
        await witness_db.connection.execute(f"DELETE FROM {namespace}.office_assertion WHERE ordinal=2")
    else:
        columns = ",".join(capture.COPY_COLUMNS)
        expressions = ",".join(
            {"ordinal": "3", "source_record_key": "'substituted'", "location_id": "gen_random_uuid()"}.get(name, name)
            for name in capture.COPY_COLUMNS
        )
        await witness_db.connection.execute(
            f"INSERT INTO {namespace}.office_assertion ({columns}) SELECT {expressions} "
            f"FROM {namespace}.office_assertion WHERE ordinal=1"
        )
    with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="accounting_invalid"):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("substitution", ["manifest", "root", "file", "frozen"])
async def test_native_source_replacement_refuses(witness_db, substitution):
    request, descriptor = await _candidate(witness_db)
    namespace = witness_db.source.source.schema
    sql_by_substitution = {
        "manifest": f"UPDATE {namespace}.ptg2_snapshot SET manifest=manifest||'{{\"changed\":true}}'::jsonb",
        "root": f"UPDATE {namespace}.ptg2_v4_snapshot_map_root SET map_digest=decode(repeat('f',64),'hex')",
        "file": f"UPDATE {namespace}.ptg2_source_file_version SET raw_sha256=repeat('f',64)",
        "frozen": f"UPDATE {namespace}.ptg2_frozen_source_file_binding SET binding_sha256=repeat('f',64)",
    }
    await witness_db.connection.execute(sql_by_substitution[substitution])
    with pytest.raises(
        (RegistryPTGCohortAuthorityError, RegistryPTGProducerScopeError, PtgResultArchiveSourceAuthorityError),
        match="source_(changed|unavailable)|source authority.*(changed|invalid|match)",
    ):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("damage", ["missing", "duplicate"])
async def test_native_source_witness_cardinality_refuses(witness_db, damage):
    request, descriptor = await _candidate(witness_db)
    namespace = witness_db.source.source.schema
    occurrence = f"{namespace}.ptg2_provider_group_tax_identity_source"
    selected = witness_db.rows[0]
    predicate = "source_key=$1 AND source_record_ordinal=$2"
    statement = (
        f"DELETE FROM {occurrence} WHERE {predicate}"
        if damage == "missing"
        else f"INSERT INTO {occurrence} SELECT * FROM {occurrence} WHERE {predicate}"
    )
    if damage == "duplicate":
        with pytest.raises(
            asyncpg.UniqueViolationError, match="ptg2_provider_group_tax_identity_source_pkey"
        ) as refusal:
            await witness_db.connection.execute(
                statement, selected["dense_source_key"], selected["source_record_ordinal"]
            )
        assert refusal.value.sqlstate == "23505"
        assert (
            await witness_db.connection.fetchval(
                f"SELECT count(*) FROM {occurrence} WHERE {predicate}",
                selected["dense_source_key"],
                selected["source_record_ordinal"],
            )
            == 1
        )
        unchanged = (await _verify(witness_db, request, descriptor, _budget())).as_dict()
        assert unchanged["accounting"]["input_row_count"] == 2
        return
    await witness_db.connection.execute(statement, selected["dense_source_key"], selected["source_record_ordinal"])
    with pytest.raises(RegistryPTGCohortAuthorityError, match="^registry_ptg_source_changed$"):
        await _verify(witness_db, request, descriptor, _budget())


async def test_native_retained_site_substitution_refuses(witness_db):
    request, descriptor = await _candidate(witness_db)
    namespace = '"' + witness_db.serving.schema_name + '"'
    await witness_db.connection.execute(f"UPDATE {namespace}.provider_location_binding SET location_key=repeat('f',64)")
    with pytest.raises(RetainedSiteAdoptionError, match="selection is unresolved"):
        await _verify(witness_db, request, descriptor, _budget())


@pytest.mark.parametrize("field", ["client_id", "scope_approval_sha256"])
async def test_native_scope_context_replacement_refuses(witness_db, field):
    request, descriptor = await _candidate(witness_db)
    context = replace(witness_db.context, **{field: "f" * 64})
    error = capture.RegistryPTGOfficeCaptureError if field == "scope_approval_sha256" else RegistryPTGProducerScopeError
    with pytest.raises(error, match="scope_(unavailable|changed)"):
        await _verify(witness_db, request, descriptor, _budget(), context=context)


async def test_native_retained_generation_context_refuses(witness_db):
    request, descriptor = await _candidate(witness_db)
    altered = replace(request, retained_generation_id=request.retained_generation_id + 1)
    with pytest.raises(NetworkServingReadUnavailable):
        await _verify(witness_db, altered, descriptor, _budget())


async def test_native_cumulative_budget_survives_refused_retry(witness_db):
    request, descriptor = await _candidate(witness_db)
    first = _budget()
    await _verify(witness_db, request, descriptor, first)
    cumulative = graph.RegistryPTGGraphReadBudget(first.read_bytes)
    await _verify(witness_db, request, descriptor, cumulative)
    charged = cumulative.read_bytes, cumulative.read_pages, cumulative.coordinates
    with pytest.raises(graph.RegistryPTGGraphReadError, match="graph_budget"):
        await _verify(witness_db, request, descriptor, cumulative)
    assert cumulative.read_bytes >= charged[0]
    assert cumulative.read_pages >= charged[1]
    assert cumulative.coordinates >= charged[2]
    assert cumulative.read_bytes <= cumulative.maximum_bytes


async def test_native_graph_failure_retains_budget(witness_db):
    request, descriptor = await _candidate(witness_db)
    budget = _budget()
    await _verify(witness_db, request, descriptor, budget)
    before = budget.read_bytes, budget.read_pages, budget.coordinates
    namespace = witness_db.source.source.schema
    block = witness_db.source.source.graph.blocks[0]
    payload = bytes(block.payload)
    changed = payload[:-1] + bytes([payload[-1] ^ 1])
    assert hashlib.sha256(changed).digest() != hashlib.sha256(payload).digest()
    await witness_db.connection.execute(
        f"UPDATE {namespace}.ptg2_v3_block SET payload=$1 WHERE block_hash=$2", changed, block.block_hash
    )
    with pytest.raises(graph.RegistryPTGGraphReadError):
        await _verify(witness_db, request, descriptor, budget)
    assert budget.read_bytes > before[0] and budget.read_pages > before[1]
    assert budget.coordinates >= before[2]


@pytest.fixture
def arrange(monkeypatch):
    """Scope synthetic database evidence to the compiled codec-only cases."""
    return partial(_codec_fixture.arrange.__wrapped__(monkeypatch), use_native_codecs=True)


@pytest.mark.asyncio
async def test_complete_large_office_census_uses_real_native_graph_and_terminal_page(arrange):
    prepared = arrange(4101)
    budget = witness.graph.RegistryPTGGraphReadBudget(1048576)
    evidence = (await _codec_verify(prepared, budget)).as_dict()
    assert prepared.session.witness_calls == [0, 1024, 2048, 3072, 4096, 4101]
    assert evidence["accounting"]["input_row_count"] == 4101
    assert evidence["source_witness_census"]["nonempty_pages"] == 5
    assert evidence["source_witness_census"]["terminal_empty_ordinal"] == 4101
    assert evidence["source_witness_census"]["verified_edge_requests"] == 5
    assert budget.read_bytes == 5 * 288 and budget.read_pages == 5 * 4
    assert evidence["accounting"]["canonical_input_sha256"] == prepared.request.canonical_input_sha256
    assert not {"authorized", "admitted", "reviewed", "published", "current_company"} & evidence.keys()
    evidence["accounting"]["input_row_count"] = 0
    assert (await _codec_verify(prepared)).as_dict()["accounting"]["input_row_count"] == 4101


@pytest.mark.asyncio
async def test_cumulative_graph_budget_is_not_reset_or_refunded(arrange):
    prepared = arrange(1025)
    budget = witness.graph.RegistryPTGGraphReadBudget(400)
    with pytest.raises(witness.graph.RegistryPTGGraphReadError, match="budget"):
        await _codec_verify(prepared, budget)
    assert budget.read_bytes == 288 and budget.read_pages == 4
    with pytest.raises(witness.graph.RegistryPTGGraphReadError, match="budget"):
        await _codec_verify(arrange(1), budget)
    assert budget.read_bytes == 288


@pytest.mark.asyncio
async def test_graph_failure_after_prior_page_retains_shared_budget(arrange):
    prepared = arrange(1025)
    budget = witness.graph.RegistryPTGGraphReadBudget(1048576)
    original_execute = prepared.session.execute

    async def missing_member(query, parameters=None):
        if len(prepared.session.witness_calls) > 1 and "ptg2_v3_block" in str(query):
            return graph_fixture._Result(rows=[])
        return await original_execute(query, parameters)

    prepared.session.execute = missing_member
    with pytest.raises(witness.graph.RegistryPTGGraphReadError):
        await _codec_verify(prepared, budget)
    assert budget.read_bytes == 288 and budget.read_pages == 4
    assert budget.coordinates == 2


async def _assert_single_copy_validation(driver, batch, encoded_inputs):
    async def copy_page(*args, **kwargs):
        kwargs["source"].read()
        return "COPY 1"

    driver.copy_to_table = AsyncMock(side_effect=copy_page)
    await composition._copy_recipe_page(driver, batch, "synthetic_raw")
    assert encoded_inputs == [batch.input_bytes]
    driver.copy_to_table.assert_awaited_once()
    malformed = replace(batch, input_bytes=batch.input_bytes.replace(b'"1234567893"', b'"invalid"'))
    malformed = replace(malformed, input_sha256=hashlib.sha256(malformed.input_bytes).hexdigest())
    driver.copy_to_table.reset_mock()
    with pytest.raises(ValueError):
        await composition._copy_recipe_page(driver, malformed, "synthetic_raw")
    driver.copy_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_retained_site_is_encoded_once_at_copy_without_inheritance(monkeypatch):
    """Read one exact office and encode it only at the validated COPY boundary."""
    from process import registry_retained_site_adoption
    from process.registry_retained_site_adoption import RetainedSiteAdoption

    driver, verified, approved = recipe_fixture._owner()
    verified = replace(verified, request=SimpleNamespace(input_row_count=1))
    location = str(UUID(int=8))
    driver.fetchrow.return_value = {"rows": 1, "bytes": 1024}
    driver.fetch.return_value = [
        {
            "ordinal": 1,
            "provider_system": "npi",
            "provider_id": "1234567893",
            "location_id": UUID(location),
            "location_key": "e" * 64,
            "address_row_sha256": "f" * 64,
            "evidence_id": "1" * 64,
        }
    ]
    adoption = RetainedSiteAdoption("npi", "1234567893", location, "e" * 64, "npi", "1234567893", "f" * 64)
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=approved))
    resolve = AsyncMock(return_value=SimpleNamespace(records=(adoption,)))
    monkeypatch.setattr(registry_retained_site_adoption, "resolve_retained_site_adoptions", resolve)
    import ptg2_address_canon

    native_encoder = ptg2_address_canon.encode_network_membership_batch
    calls = []

    def encode_once(raw):
        calls.append(raw)
        return native_encoder(raw)

    monkeypatch.setattr(ptg2_address_canon, "encode_network_membership_batch", encode_once)
    batch = await offices.read_ptg_office_membership_batch(driver, verified, approved, None, 1, "control_fixture")
    assert calls == []

    await _assert_single_copy_validation(driver, batch, calls)
    assert json.loads(batch.input_bytes) == [
        {
            "network_id": 71,
            "provider_system": "npi",
            "provider_id": "1234567893",
            "location_id": location,
            "evidence_id": "1" * 64,
        }
    ]
    assert batch.office_records == (adoption,) and batch.membership_rows == 1
    assert json.loads(resolve.call_args.args[2])[0]["location_key"] == "e" * 64
