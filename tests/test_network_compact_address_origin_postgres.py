# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""New publications reuse only independently sealed retained address origins."""

import json
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from api.network_address_scope import (
    network_address_read_session_scope,
)
from api.network_compact_address import compact_address_native_sql
from api.network_read_execution import bounded_network_read_rows
from api.network_serving_api_read import api_catalog_read_snapshot
from api.reference_family_reads import pin_claims_reader
from db.connection import Database, current_session
from process import network_indexed_address_artifact as indexed
from process.entity_address_unified import _location_key_expr
from process.network_address_artifact import NetworkAddressArtifactError, verify_network_address_artifact
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.network_membership_candidate_lifecycle import create_network_candidate, seal_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_network_candidate
from process.network_membership_publication import publish_network_candidate
from process.network_published_api_pin import published_api_pin_statement
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_publication_consumer import _publication_address_sources
from process.registry_retained_generation import RegistryRetainedGenerationError
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_address_artifact_postgres import _rotate
from tests.test_network_address_artifact_postgres import address_db as address_db
from tests.test_network_compact_publication_postgres import (
    _bind_offices,
    _candidate,
    _named_compact_runtime,
    _prepare,
    _publish,
    _read,
    _seal,
)
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db
from tests.test_network_custom_address_source_postgres import _bootstrap_prepare, _bootstrap_specification, _seed
from tests.test_network_custom_address_source_postgres import bootstrap_db as bootstrap_db
from tests.test_network_initial_source_office_bindings_postgres import (
    _archive,
    _cleanup_offices,
    _office_fixture,
    _reviewed_batch,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _approved_aca_origin(fixture):
    source = _office_fixture((fixture.connection, fixture.control_schema, fixture.engine), 1)
    try:
        prepared, validation = await _archive(source)
        source.source = PinnedACAMembershipSource(prepared, validation, **source.source_metadata_by_field)
        source.batch = await _reviewed_batch(source)
        await fixture.connection.execute(f'GRANT "{source.reader}" TO "{fixture.executor_role}"')
        canonical = source.canonical[0]
        await fixture.connection.execute(
            f'''UPDATE "{fixture.address.schema}".entity_address_unified SET first_line=$1,second_line=$2,
            city_name=$3,state_name=$4,postal_code=$5,country_code=$6,address_key=$7,premise_key=$8,
            address_precision='street',zip5=$9,state_code=$10,city_norm=$11 WHERE location_key='old-office' ''',
            *source.raw[0],
            source.sites[0],
            UUID(canonical["premise_key"]),
            canonical["zip5"],
            canonical["state_code"],
            canonical["city_norm"],
        )
        await fixture.connection.execute(
            f"DELETE FROM \"{fixture.address.schema}\".entity_address_unified WHERE location_key='other-office'"
        )
        await fixture.connection.execute(
            f'UPDATE "{fixture.address.schema}".entity_address_unified SET location_key='
            + _location_key_expr({"address_role_id": "4", "source_id": "2", "source_record_id": "NULL::text"})
        )
        yield source
    finally:
        await _cleanup_offices(source)


async def _aca_candidate(fixture, aca_source, expected_head, previous=None):
    approved = aca_source.batch.approved_source
    recipes = () if previous else (RegistrySourceMembershipRecipe(aca_source.source, aca_source.coordinates),)
    copy_target, addresses = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=approved.approved_revision,
        expected_head=expected_head,
        address_sources=RegistryCompositionAddressSources(fixture.source, initial_source_recipes=recipes, compact=True),
        writer_roles=fixture.writer_roles,
        source_manifest=previous,
        control_schema=fixture.control_schema,
    )
    assert addresses == fixture.source
    return copy_target


async def test_rotated_aca_successor_preserves_selected_office_and_pinned_reads(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    async with _approved_aca_origin(fixture) as aca_source:
        first = await _aca_candidate(fixture, aca_source, 0)
        await _prepare(fixture, first)
        await _seal(fixture, first)
        await _publish(fixture, first)
        network_id = json.loads(aca_source.batch.input_bytes)[0]["network_id"]
        previous, original_rows = await _read(fixture, network_ids=(network_id,))
        assert len(original_rows) == aca_source.batch.membership_rows == 1
        await _rotate(fixture.address, monkeypatch)
        previous, retained_rows = await _read(fixture, previous.generation_id, network_ids=(network_id,))
        assert retained_rows == original_rows
        second = await _aca_candidate(fixture, aca_source, previous.generation_id, previous)
        await _prepare(fixture, second)
        await _seal(fixture, second)
        await _publish(fixture, second)
        current, actual_rows = await _read(fixture, network_ids=(network_id,))
        assert actual_rows == original_rows
        assert current.compact_layout.base == previous.compact_layout.base
        assert (await _read(fixture, previous.generation_id, network_ids=(network_id,)))[1] == original_rows
        report = json.loads(
            await fixture.connection.fetchval(
                f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(second.candidate_id),
            )
        )
        carrier = report["compact_address_carriers"]
        assert carrier["aca_carry_sha256"] is not None and carrier["selected_address_rows"] == 1


async def test_new_generation_reuses_authenticated_retained_heap_after_rotation(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    await fixture.connection.execute(
        f"DELETE FROM \"{fixture.address.schema}\".entity_address_unified WHERE location_key='other-office'"
    )
    locations = await fixture.connection.fetch(
        f'SELECT location_key,entity_type,entity_id,address_key FROM "{fixture.address.schema}".entity_address_unified'
    )
    first = await _candidate(fixture, (42,), 0)
    await _prepare(fixture, first)
    await _seal(fixture, first)
    await _publish(fixture, first)
    previous, office_rows = await _read(fixture)
    await _rotate(fixture.address, monkeypatch)
    with pytest.raises(NetworkAddressArtifactError, match="source_generation_changed"):
        await _prepare(fixture, await _candidate(fixture, (42,), previous.generation_id))
    second = await _candidate(
        fixture,
        (42,),
        previous.generation_id,
        locations=locations,
        source_manifest=previous,
    )
    await _prepare(fixture, second)
    await _seal(fixture, second)
    await _publish(fixture, second)
    current, actual = await _read(fixture)
    assert actual == office_rows and current.compact_layout.base == previous.compact_layout.base
    assert current.compact_layout.base != CompactAddressRelation(fixture.source.schema_name, fixture.source.table_name)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", second.schema_name + ".entity_address_unified")
        is None
    )


async def test_unknown_manifest_locator_cannot_authorize_address_reuse(compact_publication_db):
    fixture = compact_publication_db
    target = await _candidate(fixture, (42, 88), 0)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET source_generations=source_generations||$2::jsonb WHERE candidate_id=$1",
        UUID(target.candidate_id),
        json.dumps({"retained_network_source": "a" * 64}),
    )
    with pytest.raises(RegistryRetainedGenerationError):
        await _prepare(fixture, target)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", target.schema_name + ".network_slice_ref") is None
    )


async def _populate_parent_addresses(connection, parent, row_count):
    """Populate the exact synthetic source before its protected bootstrap capture."""
    await connection.execute(
        f'UPDATE "{parent.source_schema}".entity_address_unified SET address_key=$1,lat=40,long=-73, '
        "address_precision='street',taxonomy_array='{17}'",
        uuid4(),
    )
    await connection.execute(f'ALTER TABLE "{parent.source_schema}".npi ALTER COLUMN npi DROP DEFAULT')
    if row_count > 1:
        source_table = f'"{parent.source_schema}".entity_address_unified'
        columns = await connection.fetch(
            "SELECT attname,quote_ident(attname) AS sql FROM pg_attribute "
            "WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
            source_table,
        )
        expressions_by_column = {"location_key": "lpad(to_hex(i),64,'0')", "address_key": "md5(i::text)::uuid"}
        selected_columns = ",".join(
            expressions_by_column.get(column["attname"], "a." + column["sql"]) for column in columns
        )
        await connection.execute(
            f"INSERT INTO {source_table} SELECT {selected_columns} "
            f"FROM {source_table} a CROSS JOIN generate_series(1,$1::integer) i",
            row_count - 1,
        )
    assert (
        await connection.fetchval(f'SELECT count(*) FROM "{parent.source_schema}".entity_address_unified') == row_count
    )


async def _open_fixture_actors(fixture, stack, password, monkeypatch):
    """Register and open the existing independent custody and reader fixture actors."""
    connection, control_schema, engine = fixture.connection, fixture.control_schema, fixture.engine
    await _named_compact_runtime(fixture, stack, monkeypatch)
    fixture.runtime_roles = tuple(sorted((fixture.executor_role, fixture.roles["reader"])))
    await connection.execute(f'GRANT USAGE ON SCHEMA "{control_schema}" TO "{fixture.roles["reader"]}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{control_schema}" TO "{fixture.roles["reader"]}"')
    await connection.execute(
        f"GRANT INSERT(candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) "
        f'ON "{control_schema}".network_membership_batch TO "{fixture.executor_role}"'
    )
    await connection.execute(
        f'INSERT INTO "{control_schema}".network_registry_identity(network_id,allocation_key) '
        "OVERRIDING SYSTEM VALUE VALUES(42,$1),(88,$2),(99,$3)",
        uuid4(),
        uuid4(),
        uuid4(),
    )
    fixture.custodian = await asyncpg.connect(
        fixture.custody_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    stack.push_async_callback(fixture.custodian.close)
    fixture.reader_engine = create_async_engine(
        engine.url.set(username=fixture.roles["reader"], password=password), poolclass=NullPool
    )
    stack.push_async_callback(fixture.reader_engine.dispose)


async def _publish_unsealed_legacy(fixture):
    """Publish the flat predecessor through the normal named freeze and validation."""
    connection, control_schema = fixture.connection, fixture.control_schema
    identity = uuid4()
    fixture.legacy = MembershipCopyTarget(
        fixture.dataset_id,
        fixture.schema_id,
        fixture.producer_id,
        str(identity),
        "network_candidate_" + identity.hex,
    )
    async with connection.transaction():
        await create_network_candidate(
            connection,
            fixture.legacy,
            source_generations={"unified_address": fixture.source.generation_id},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=0,
            control_schema=control_schema,
        )
        await _bind_offices(connection, fixture.legacy, [])
        await seal_network_candidate(connection, fixture.legacy, control_schema=control_schema)
    await prepare_network_candidate(
        connection,
        fixture.legacy,
        fixture.source,
        owner_role=fixture.roles["owner"],
        loader_roles=(),
        reader_roles=(fixture.roles["reader"],),
        control_schema=control_schema,
    )
    await _seal(fixture, fixture.legacy)
    async with connection.transaction(isolation="repeatable_read"):
        publication_by_field = await publish_network_candidate(
            connection, fixture.legacy, control_schema=control_schema, preparation_roles=fixture.preparation_roles
        )
    fixture.indexed_source = PinnedAddressSource(
        fixture.legacy.schema_name, "entity_address_unified", publication_by_field["manifest_sha256"]
    )
    return publication_by_field


@pytest.fixture
async def indexed_db(bootstrap_db, serving_schema, request, monkeypatch):
    """Own the actual bootstrap, validated flat predecessor and named compact head."""
    parent = bootstrap_db
    connection, control_schema, engine = serving_schema
    password = uuid4().hex
    await connection.execute(f"ALTER ROLE \"{parent.roles['reader']}\" LOGIN PASSWORD '{password}'")
    await _populate_parent_addresses(connection, parent, getattr(request, "param", 1))
    bootstrap_receipt = await _bootstrap_prepare(
        parent, await _bootstrap_specification(parent), runtime_roles=(parent.roles["reader"],)
    )
    fixture = SimpleNamespace(
        connection=connection,
        engine=engine,
        control_schema=control_schema,
        source_schema=parent.source_schema,
        roles=parent.roles,
        source=bootstrap_receipt.address_source,
        bootstrap=bootstrap_receipt,
        address=SimpleNamespace(schema=bootstrap_receipt.address_source.schema_name),
        dataset_id=str(uuid4()),
        schema_id=str(uuid4()),
        producer_id=str(uuid4()),
    )
    fixture.locations = await connection.fetch(
        f'SELECT location_key,entity_type,entity_id,address_key FROM "{fixture.address.schema}".entity_address_unified '
        "WHERE location_key=$1",
        "a" * 64,
    )
    async with AsyncExitStack() as stack:
        await _open_fixture_actors(fixture, stack, password, monkeypatch)
        publication_by_field = await _publish_unsealed_legacy(fixture)
        fixture.current = await _candidate(
            fixture, (42,), publication_by_field["generation_id"], locations=fixture.locations
        )
        await _prepare(fixture, fixture.current)
        await _seal(fixture, fixture.current)
        await _publish(fixture, fixture.current)
        fixture.previous, fixture.previous_rows = await _read(fixture)
        yield fixture


async def _adopt(fixture):
    from process.registry_composition_addresses import RegistryCompositionAddressSources, _require_source_recipe_phase
    from process.registry_publication_consumer import _publication_address_sources

    previous, sources = await _publication_address_sources(
        fixture.execution_connection,
        fixture.previous.generation_id,
        SimpleNamespace(control_schema=fixture.control_schema),
        RegistryCompositionAddressSources(fixture.indexed_source, compact=True),
    )
    assert previous == fixture.previous and sources.base_source == fixture.indexed_source
    assert sources.npi_source == fixture.bootstrap.npi_source
    fixture.source = sources.base_source
    _require_source_recipe_phase(
        sources,
        previous,
        fixture.previous.generation_id,
    )
    target = await _candidate(
        fixture, (42,), fixture.previous.generation_id, source_manifest=fixture.previous, locations=fixture.locations
    )
    await _prepare(fixture, target)
    return target


async def _address_descriptor(fixture, target):
    report = json.loads(
        await fixture.connection.fetchval(
            f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(target.candidate_id),
        )
    )
    return report["serving_readiness"]["address_artifact"]


async def test_indexed_adoption_ignores_old_arrays_and_preserves_rollback_and_next_successor(
    indexed_db, monkeypatch, record_property
):
    fixture = indexed_db
    proof_calls = []
    prove = indexed.prove_indexed_address_equivalence

    async def measured_proof(*arguments):
        started = time.perf_counter()
        counts = await prove(*arguments)
        proof_calls.append(counts)
        record_property("indexed_content_proof_seconds", time.perf_counter() - started)
        return counts

    monkeypatch.setattr(indexed, "prove_indexed_address_equivalence", measured_proof)
    await fixture.connection.execute(
        f"UPDATE \"{fixture.legacy.schema_name}\".entity_address_unified SET canonical_network_ids='{{99}}'"
    )
    copy_target = await _adopt(fixture)
    await _seal(fixture, copy_target)
    await _publish(fixture, copy_target)
    current, address_rows = await _read(fixture)
    assert current.supports_location_lookup and not fixture.previous.supports_location_lookup
    assert address_rows == fixture.previous_rows
    assert current.compact_layout.base != fixture.previous.compact_layout.base
    assert current.compact_layout.base.schema_name == fixture.legacy.schema_name
    assert current.source_generations["retained_network_source"] == fixture.previous.manifest_sha256
    previous, previous_rows = await _read(fixture, fixture.previous.generation_id)
    assert previous_rows == fixture.previous_rows and not previous.supports_location_lookup
    assert (await _read(fixture, current.generation_id, network_ids=(99,)))[1] == []
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", copy_target.schema_name + ".entity_address_unified")
        is None
    )
    from process.network_compact_retention import (
        NetworkCompactRetentionError,
        require_network_compact_artifacts_unreferenced,
    )

    descriptor = await _address_descriptor(fixture, copy_target)
    async with fixture.connection.transaction(isolation="read_committed"):
        with pytest.raises(NetworkCompactRetentionError, match="artifact_referenced"):
            await require_network_compact_artifacts_unreferenced(
                fixture.connection,
                [CompactAddressRelation(**descriptor["table"])],
                control_schema=fixture.control_schema,
            )

    successor = await _candidate(fixture, (42,), current.generation_id, source_manifest=current)
    await _prepare(fixture, successor)
    await _seal(fixture, successor)
    await _publish(fixture, successor)
    assert (await _read(fixture))[1] == address_rows
    assert (await _read(fixture, current.generation_id))[1] == address_rows
    assert proof_calls == [
        dict(source_rows=1, address_rows=1, missing_locations=0, extra_locations=0, changed_address_rows=0)
    ]
    record_property("indexed_content_proof_rows", 1)


@pytest.mark.parametrize("damage", ["field", "missing", "extra"])
async def test_new_independent_seal_rejects_forged_legacy_content_without_advancing_head(indexed_db, damage):
    fixture = indexed_db
    target = await _adopt(fixture)
    table = f'"{fixture.legacy.schema_name}".entity_address_unified'
    if damage == "field":
        await fixture.connection.execute(f"UPDATE {table} SET first_line='Different office'")
    elif damage == "missing":
        await fixture.connection.execute(f"DELETE FROM {table}")
    else:
        await fixture.connection.execute(
            f"INSERT INTO {table} SELECT (jsonb_populate_record(NULL::{table},to_jsonb(a)||jsonb_build_object('location_key',repeat('b',64)))).* FROM {table} a"
        )
    with pytest.raises(NetworkAddressArtifactError, match="indexed_content_differs"):
        await _seal(fixture, target)
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == fixture.previous.generation_id
    )
    report = json.loads(
        await fixture.connection.fetchval(
            f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(target.candidate_id),
        )
    )
    assert "final_validation" not in report


async def test_worker_selected_indexed_base_requires_exact_retained_generation(indexed_db):
    fixture = indexed_db
    fixture.indexed_source = replace(fixture.indexed_source, generation_id="f" * 64)
    with pytest.raises(ValueError, match="Retained network address source is unavailable"):
        await _adopt(fixture)
    current, rows = await _read(fixture)
    assert current == fixture.previous and rows == fixture.previous_rows


async def test_indexed_dependency_keeps_named_scope_and_snapshot(indexed_db):
    from process.registry_publication_execution import _source_rollback_address_sources

    fixture = indexed_db
    candidate = await _adopt(fixture)
    await _seal(fixture, candidate)
    await _publish(fixture, candidate)
    current, _ = await _read(fixture)
    worker_addresses = await _source_rollback_address_sources(
        fixture.execution_connection, current, fixture.control_schema
    )
    assert worker_addresses.base_source == fixture.indexed_source
    assert worker_addresses.npi_source == fixture.bootstrap.npi_source
    descriptor = await _address_descriptor(fixture, candidate)
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        relation = await verify_network_address_artifact(fixture.execution_connection, descriptor)
        assert relation == CompactAddressRelation(**descriptor["table"])
    with pytest.raises(NetworkAddressArtifactError, match="transaction_required"):
        await verify_network_address_artifact(fixture.execution_connection, descriptor)
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        relation = await verify_network_address_artifact(fixture.execution_connection, descriptor)
        assert relation == CompactAddressRelation(**descriptor["table"])


async def test_indexed_reader_digest_is_independent_of_public_schema_visibility(address_db):
    from process.network_custom_address_source import _source_columns
    from process.network_membership_serving_indexes import (
        _build_serving_indexes,
        _index_digest,
        _serving_index_records,
        _serving_plan,
    )

    fixture = address_db
    plan = _serving_plan(fixture.schema)
    for extension in ("intarray", "btree_gin", "btree_gist"):
        await fixture.admin.execute("CREATE EXTENSION IF NOT EXISTS " + extension)
    await _build_serving_indexes(fixture.admin, fixture.schema, plan)
    await fixture.admin.execute("REVOKE USAGE ON SCHEMA public FROM PUBLIC")
    await fixture.admin.execute(f'GRANT USAGE ON SCHEMA public TO "{fixture.roles["reader"]}"')
    relation = CompactAddressRelation(fixture.schema, "entity_address_unified")
    observations_by_role = {}
    for name, connection in (("publisher", fixture.publisher), ("reader", fixture.reader)):
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            columns = await _source_columns(connection, relation, address=True)
            canonical = await _index_digest(connection, fixture.schema, plan, qualified_definitions=True)
            legacy = await _index_digest(connection, fixture.schema, plan)
            definitions = [
                index_record["index_definition"]
                for index_record in await _serving_index_records(connection, fixture.schema)
            ]
            observations_by_role[name] = columns, canonical, legacy, definitions
    publisher, reader = observations_by_role["publisher"], observations_by_role["reader"]
    assert publisher[:2] == reader[:2]
    assert publisher[1] == publisher[2] != reader[2]
    assert any("public.st_makepoint" in definition for definition in publisher[3])
    assert any("public.gin__int_ops" in definition for definition in publisher[3])
    assert all("public.st_makepoint" not in definition for definition in reader[3])

    await fixture.admin.execute(
        f'ALTER INDEX "{fixture.schema}".entity_address_unified_idx_geo_idx SET (fillfactor=80)'
    )
    async with fixture.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await _source_columns(fixture.reader, relation, address=True) == reader[0]
        assert await _index_digest(fixture.reader, fixture.schema, plan, qualified_definitions=True) != reader[1]
        assert await _index_digest(fixture.reader, fixture.schema, plan) != reader[2]


async def test_changed_index_definition_refuses_retained_data(indexed_db):
    fixture = indexed_db
    target = await _adopt(fixture)
    await _seal(fixture, target)
    await _publish(fixture, target)
    descriptor = await _address_descriptor(fixture, target)
    index = f'"{fixture.legacy.schema_name}".entity_address_unified_idx_geo_idx'
    await fixture.connection.execute("ALTER INDEX " + index + " SET (fillfactor=80)")
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(NetworkAddressArtifactError, match="indexed_artifact_changed"):
            await verify_network_address_artifact(fixture.execution_connection, descriptor)


async def test_indexed_custom_npi_input_keeps_authenticated_bootstrap_identity(indexed_db):
    from process.network_compact_custom_offices import _capture_inputs

    fixture = indexed_db
    approval = await _seed(fixture, system="npi")
    candidate_by_field = {
        "source_generations": {
            "unified_address": fixture.indexed_source.generation_id,
            "retained_network_source": fixture.previous.manifest_sha256,
        }
    }
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        summary, _, npi, relation = await _capture_inputs(
            fixture.execution_connection,
            fixture.indexed_source,
            approval.revision,
            fixture.writer_roles["reader_roles"],
            None,
            f'"{fixture.control_schema}"',
            candidate_by_field,
        )
    assert summary["npi_pairs"] == 1
    assert npi == PinnedAddressSource(
        fixture.bootstrap.address_source.schema_name, "npi", fixture.bootstrap.generation_sha256
    )
    assert relation == CompactAddressRelation(fixture.indexed_source.schema_name, fixture.indexed_source.table_name)


@pytest.mark.parametrize("indexed_db", [100001], indirect=True)
async def test_complete_indexed_content_proof_is_bounded_across_many_ranges(indexed_db, monkeypatch, record_property):
    fixture = indexed_db
    copy_target = await _adopt(fixture)
    prove = indexed.prove_indexed_address_equivalence
    observations = []

    async def measured_proof(*arguments):
        settings = await arguments[0].fetchrow("SELECT current_setting('work_mem'),current_setting('temp_file_limit')")
        assert tuple(settings) == ("16MB", "256MB")
        started = time.perf_counter()
        counts = await prove(*arguments)
        elapsed = time.perf_counter() - started
        observations.append(counts)
        record_property("indexed_content_proof_seconds", elapsed)
        record_property("indexed_content_proof_rows", counts["source_rows"])
        record_property("indexed_content_proof_memory", "/".join(settings))
        return counts

    monkeypatch.setattr(indexed, "prove_indexed_address_equivalence", measured_proof)
    await _seal(fixture, copy_target)
    assert observations == [
        dict(source_rows=100001, address_rows=100001, missing_locations=0, extra_locations=0, changed_address_rows=0)
    ]
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == fixture.previous.generation_id
    )


async def _retained_resolves(fixture):
    from process.network_serving_read import resolve_retained_network_address_source

    async with fixture.connection.transaction(isolation="repeatable_read"):
        return await resolve_retained_network_address_source(
            fixture.connection, fixture.indexed_source, control_schema=fixture.control_schema
        )


async def test_retained_successor_admits_direct_executor_adoption(indexed_db):
    fixture = indexed_db
    assert await fixture.execution_connection.fetchval(
        f'SELECT count(*) FROM "{fixture.legacy.schema_name}".entity_address_unified'
    ) == len(fixture.previous_rows)
    copy_target = await _adopt(fixture)
    await _seal(fixture, copy_target)
    await _publish(fixture, copy_target)
    current, address_rows = await _read(fixture)
    assert address_rows == fixture.previous_rows
    assert current.compact_layout.base.schema_name == fixture.legacy.schema_name
    assert (await _retained_resolves(fixture)).schema_name == fixture.legacy.schema_name


async def _publish_api_candidate(fixture, previous, address_sources):
    target, address_source = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=0,
        expected_head=previous.generation_id,
        address_sources=address_sources,
        writer_roles=fixture.writer_roles,
        source_manifest=previous,
        control_schema=fixture.control_schema,
    )
    assert address_source == fixture.source
    await _prepare(fixture, target)
    await _seal(fixture, target)
    await _publish(fixture, target)
    return await _read(fixture)


@pytest.fixture
async def indexed_api_db(indexed_db, monkeypatch):
    """Publish two indexed generations and bind their genuine request Reader."""
    fixture = indexed_db
    previous, address_sources = await _publication_address_sources(
        fixture.execution_connection,
        fixture.previous.generation_id,
        SimpleNamespace(control_schema=fixture.control_schema),
        RegistryCompositionAddressSources(fixture.indexed_source, compact=True),
    )
    assert previous == fixture.previous and address_sources.base_source == fixture.indexed_source
    fixture.source = address_sources.base_source
    retained, address_rows = await _publish_api_candidate(fixture, previous, address_sources)
    current, current_rows = await _publish_api_candidate(fixture, retained, address_sources)
    assert current_rows == address_rows and len(address_rows) == 1
    assert retained.supports_location_lookup and current.supports_location_lookup
    assert current.compact_layout.base == retained.compact_layout.base
    assert len(current.compact_layout.artifacts) == 1
    assert current.compact_layout.artifacts == retained.compact_layout.artifacts
    assert current.compact_layout.artifacts[0].candidate_id not in (retained.candidate_id, current.candidate_id)

    role, password, url = fixture.roles["reader"], uuid4().hex, fixture.engine.url
    await fixture.connection.execute(f"ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    settings_by_name = {
        "DRIVER": "asyncpg",
        "HOST": url.host,
        "PORT": str(url.port),
        "DATABASE": url.database,
        "READER_USER": role,
        "READER_PASSWORD": password,
        "READER_POOL_MIN_SIZE": "1",
        "READER_POOL_MAX_SIZE": "1",
    }
    for name, setting_value in settings_by_name.items():
        monkeypatch.setenv("HLTHPRT_DB_" + name, setting_value)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    database, requests = Database(), []
    database._database_override = url.database
    native_fetchrow = asyncpg.Connection.fetchrow
    control_schema = fixture.control_schema
    metadata_sql = published_api_pin_statement(control_schema)

    async def observe_metadata(connection, statement, *parameters, **options):
        address_entry = await native_fetchrow(connection, statement, *parameters, **options)
        if statement == metadata_sql:
            requests.append((connection, parameters, address_entry))
        return address_entry

    monkeypatch.setattr(asyncpg.Connection, "fetchrow", observe_metadata)
    try:
        yield SimpleNamespace(
            fixture=fixture,
            database=database,
            current=current,
            retained=retained,
            address_rows=address_rows,
            requests=requests,
        )
    finally:
        await database.disconnect()


async def _api_rows(held, generation_id):
    async with held.database.reader_session() as session:
        assert session.info["api_reader_verified"] is True and current_session() is session
        transaction = session.get_transaction()
        await pin_claims_reader(session, api_read=True)
        before = len(held.requests)
        async with api_catalog_read_snapshot(session, generation_id) as (manifest, approved_relation):
            assert len(held.requests) == before + 1
            _driver, parameters, entry = held.requests[-1]
            assert parameters == (generation_id,)
            assert manifest._api_approved_relation == approved_relation
            assert approved_relation == CompactAddressRelation(held.fixture.control_schema, "registry_approved_record")
            assert set(entry) == {"candidate_json", "manifest_json", "approved_rows"}
            assert manifest.compact_layout.artifacts == held.current.compact_layout.artifacts
            async with network_address_read_session_scope(session, manifest, (42,)) as scope:
                relation, arguments = compact_address_native_sql(
                    scope.manifest.compact_layout, first_parameter=1, network_ids=scope.network_ids
                )
                address_rows = await bounded_network_read_rows(
                    session,
                    f"SELECT location_key,canonical_network_ids FROM {relation} offices ORDER BY location_key",
                    *arguments,
                )
            assert session.get_transaction() is transaction and session.get_nested_transaction() is None
        return manifest, address_rows


async def test_published_api_pin_one_query_and_retained_reader_parity(indexed_api_db):
    held = indexed_api_db
    expected_rows = [
        {field: address_entry[field] for field in ("location_key", "canonical_network_ids")}
        for address_entry in held.address_rows
    ]
    for generation_id, expected_manifest in (
        (None, held.current),
        (held.current.generation_id, held.current),
        (held.retained.generation_id, held.retained),
    ):
        manifest, address_rows = await _api_rows(held, generation_id)
        assert manifest == expected_manifest and address_rows == expected_rows
    assert len(held.requests) == 3 and not held.database.has_reader_session()


async def test_published_request_serves_a_copied_candidate_table_by_name(indexed_api_db):
    """An exact published-table copy serves through the recorded qualified name."""
    held = indexed_api_db
    reader = held.fixture.roles["reader"]
    namespace = '"' + held.current.compact_layout.artifacts[0].schema_name + '"'
    expected = await _api_rows(held, None)
    await held.fixture.connection.execute(
        f"ALTER TABLE {namespace}.network_office_edge RENAME TO original_edge_fixture; "
        f"CREATE TABLE {namespace}.network_office_edge (LIKE {namespace}.original_edge_fixture INCLUDING ALL); "
        f"INSERT INTO {namespace}.network_office_edge SELECT * FROM {namespace}.original_edge_fixture; "
        f'GRANT SELECT ON {namespace}.network_office_edge TO "{reader}"'
    )
    try:
        assert await _api_rows(held, None) == expected
    finally:
        await held.fixture.connection.execute(
            f"DROP TABLE {namespace}.network_office_edge; "
            f"ALTER TABLE {namespace}.original_edge_fixture RENAME TO network_office_edge"
        )
    assert await _api_rows(held, None) == expected


@pytest.mark.parametrize("failure", ["permission", "missing_edge"])
async def test_published_request_reports_native_access_and_missing_table_errors(indexed_api_db, failure):
    held = indexed_api_db
    fixture, base = held.fixture, held.current.compact_layout.base
    if failure == "permission":
        table = f'"{base.schema_name}"."{base.table_name}"'
        role = fixture.roles["reader"]
        change, restore = f'REVOKE SELECT ON {table} FROM "{role}"', f'GRANT SELECT ON {table} TO "{role}"'
        expected = "42501"
    else:
        artifact = held.current.compact_layout.artifacts[0]
        namespace = '"' + artifact.schema_name + '"'
        change = f"ALTER TABLE {namespace}.network_office_edge RENAME TO missing_edge_fixture"
        restore = f"ALTER TABLE {namespace}.missing_edge_fixture RENAME TO network_office_edge"
        expected = "42P01"
    await fixture.connection.execute(change)
    try:
        with pytest.raises(asyncpg.PostgresError) as error:
            await _api_rows(held, None)
        assert error.value.sqlstate == expected
    finally:
        await fixture.connection.execute(restore)
    assert (await _api_rows(held, None))[0] == held.current


@pytest.mark.parametrize("force", [False, True])
async def test_published_reader_cannot_enable_row_level_filtering(indexed_api_db, force):
    held = indexed_api_db
    base = held.current.compact_layout.base
    table = f'"{base.schema_name}"."{base.table_name}"'
    change = "ENABLE ROW LEVEL SECURITY" if not force else "FORCE ROW LEVEL SECURITY"
    expected = await _api_rows(held, None)
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with held.fixture.reader_engine.connect() as connection, connection.begin():
            native = (await connection.get_raw_connection()).driver_connection
            await native.execute(f"ALTER TABLE {table} {change}")
    assert await _api_rows(held, None) == expected
