# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS resource COPY against the actual retained-resource migrations and guards."""

import asyncio
import importlib
import json
import os
from compression import zstd
from concurrent.futures import ProcessPoolExecutor
from contextlib import aclosing, asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import replace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from asyncpg.prepared_stmt import PreparedStatement
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_generic_source_storage as source_storage
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_custody import (
    FHIRSourceCustodyError,
    capture_retained_cms_fhir_source_custody,
    require_retained_cms_fhir_source_custody,
)
from process.provider_directory_cms_resource_batch import persist_prepared_cms_dataset_rows, prepare_cms_dataset_rows
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_completion import complete_generic_source_candidate
from process.provider_directory_generic_source_policy import (
    bind_generic_source_transaction_factory,
    bound_publisher,
    source_transaction,
)
from tests import cms_npd_admission_postgres_support as support
from tests.cms_retained_legacy_postgres_support import (
    assert_legacy_candidate_coverage,
    publish_legacy_retained_source,
    stage_legacy_retained_source,
)

fhir = support.fhir


@asynccontextmanager
async def _cms_source_scope():
    """Keep a strict owned admission URL and the Source template lifetime together."""
    from tests.provider_directory_source_postgres_support import source_component_templates

    configured = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not configured:
        pytest.skip("set NETWORK_REGISTRY_TEST_DSN for the native CMS resource proof")
    source_url = make_url(configured).set(drivername="postgresql+asyncpg")
    assert source_url.host in {"127.0.0.1", "localhost"} and source_url.port is not None
    async with support._owned_database(source_url) as (database_url, _admin), source_component_templates():
        with pytest.MonkeyPatch.context() as settings:
            settings.setenv(support._DSN_ENV, database_url.render_as_string(hide_password=False))
            assert support._database_url() == database_url
            yield


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def cms_source_template():
    """Keep the Source envelope without building an unused ordinary admission database."""
    async with _cms_source_scope():
        yield


@pytest_asyncio.fixture(scope="module", loop_scope="module", autouse=True)
async def cms_resource_template(request):
    """Register exact native database cleanup before migration or test writes."""
    async with _cms_source_scope():
        async with aclosing(support.cms_admission_template.__wrapped__(request)) as baseline:
            await anext(baseline)
            yield


@pytest_asyncio.fixture
async def resource_candidate(monkeypatch, tmp_path):
    """Acquire through real Publisher/Owner roles after the administrator cut."""
    directory, receipt = support.retained_release(tmp_path)
    async with support.protected_admission_database(monkeypatch) as administrator:
        component = administrator._source_component
        database = component.database
        assert database is fhir.db and database.engine is not component.admin_engine
        async with bind_generic_source_transaction_factory(component.factory):
            async with source_transaction(database):
                endpoint_id = await cms._register_source(fhir)
                candidate = await cms._admission_candidate(
                    fhir, endpoint_id, "resource-copy-proof", cms.release_identity(receipt), {}
                )
            yield database, candidate, directory


async def _write(candidate, resources):
    resource_type = resources[0]["resourceType"]
    model = fhir.RESOURCE_MODELS_BY_TYPE[resource_type]
    parsed_rows = [cms._parse_batch_row(fhir, resource, candidate)[1] for resource in resources]
    await cms._persist_source_batch(fhir, model, parsed_rows, resources, candidate, resource_type)


async def _snapshot(database, candidate):
    """Inspect genuine completion, then roll back its DDL and exact loading binding."""
    async with source_transaction(database):
        publisher = bound_publisher()
        connection, policy = publisher.connection, publisher.policy
        identity = source_storage.candidate_identity(fhir, candidate)
        before = await source_storage._locked(connection, policy, identity)
        probe = connection.transaction()
        await probe.start()
        try:
            completed = await complete_generic_source_candidate(database, fhir, candidate)
            resource = storage_relation(policy.schema, completed["tables"][source_storage.RESOURCE]["name"])
            witness = storage_relation(policy.schema, completed["tables"][source_storage.WITNESS]["name"])
            snapshot_rows = await database.all(
                "SELECT r.resource_type,r.resource_id,r.payload_hash,r.payload_json::text,"
                "w.raw_payload_json::text,w.raw_payload_sha256,w.normalized_payload_hash "
                f"FROM {resource} r LEFT JOIN {witness} w "
                "USING(dataset_id,resource_type,resource_id) WHERE r.dataset_id=:dataset_id "
                "ORDER BY r.resource_type,r.resource_id",
                dataset_id=candidate.dataset_id,
            )
            for root in (source_storage.RESOURCE, source_storage.WITNESS):
                assert (
                    await connection.fetchval(
                        f"SELECT count(*) FROM {storage_relation(policy.schema, root)} WHERE dataset_id=$1",
                        candidate.dataset_id,
                    )
                    == 0
                )
            return snapshot_rows
        finally:
            await probe.rollback()
            assert await source_storage._locked(connection, policy, identity) == before
            await source_storage._verify_tables(connection, policy, before)


def _forbid_values(*_args, **_kwargs):
    raise AssertionError("CMS batches must not use VALUES fallback")


@contextmanager
def _native_operation_counters(database, monkeypatch):
    """Observe top-level native SQL/COPY and the original SQLAlchemy metric separately."""
    copied_tables, statements, native_statements = [], [], []
    native_depth = ContextVar("cms_copy_operation_depth", default=0)
    original_copy = asyncpg.Connection.copy_records_to_table

    async def tracked_copy(connection, table, *args, **kwargs):
        copied_tables.append(table)
        token = native_depth.set(native_depth.get() + 1)
        try:
            return await original_copy(connection, table, *args, **kwargs)
        finally:
            native_depth.reset(token)

    def track_statement(_connection, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    def track_native(original, *, prepared=False):
        async def tracked(connection, *args, **kwargs):
            if native_depth.get() == 0:
                native_statements.append(connection.get_query() if prepared else args[0])
            token = native_depth.set(native_depth.get() + 1)
            try:
                return await original(connection, *args, **kwargs)
            finally:
                native_depth.reset(token)

        return tracked

    with monkeypatch.context() as observations:
        observations.setattr(asyncpg.Connection, "copy_records_to_table", tracked_copy)
        for method in ("execute", "executemany", "fetch", "fetchrow", "fetchval"):
            observations.setattr(asyncpg.Connection, method, track_native(getattr(asyncpg.Connection, method)))
        for method in ("executemany", "fetch", "fetchrow", "fetchval"):
            observations.setattr(
                PreparedStatement, method, track_native(getattr(PreparedStatement, method), prepared=True)
            )
        event.listen(database.engine.sync_engine, "before_cursor_execute", track_statement)
        try:
            yield copied_tables, statements, native_statements
        finally:
            event.remove(database.engine.sync_engine, "before_cursor_execute", track_statement)


async def test_one_and_thousand_resources_have_fixed_native_operations(
    resource_candidate, monkeypatch, record_property
):
    database, candidate, _directory = resource_candidate
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT", "false")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT_MIN_ROWS", "2000")
    monkeypatch.setattr(fhir, "_upsert_rows_values", _forbid_values)
    # Custody capture is bounded per interval; verify on every batch so the counts stay size-only.
    monkeypatch.setattr(source_storage, "_VERIFY_INTERVAL_SECONDS", 0.0)
    async with source_transaction(database):
        publisher = bound_publisher()
        binding = await source_storage._locked(
            publisher.connection, publisher.policy, source_storage.candidate_identity(fhir, candidate)
        )
        expected_tables = [binding["tables"][key]["name"] for key in ("resource_input", "witness_input")]
    operation_counts, native_counts = [], []
    with _native_operation_counters(database, monkeypatch) as (copied_tables, statements, native_statements):
        for batch_size in (1, 1_000):
            resources = [
                {
                    "resourceType": "Location",
                    "id": f"office-{batch_size}-{ordinal}",
                    "address": {"line": ["1 Example Street", "Suite 2"], "city": "Example City"},
                    "partOf": {"reference": "Location/parent"},
                }
                for ordinal in range(batch_size)
            ]
            statements.clear()
            copied_tables.clear()
            native_statements.clear()
            await _write(candidate, resources)
            operation_counts.append(len(statements) + len(copied_tables))
            native_counts.append(len(native_statements) + len(copied_tables))
            assert copied_tables == expected_tables
            assert not any(
                "INSERT INTO" in statement and "VALUES" in statement and "dataset_resource" in statement
                for statement in (*statements, *native_statements)
            )
        record_property("sql_and_copy_operation_counts", json.dumps(operation_counts))
        record_property("native_sql_and_copy_operation_counts", json.dumps(native_counts))
        assert operation_counts[0] == operation_counts[1]
        assert operation_counts[0] <= 20
        assert native_counts[0] == native_counts[1]
    snapshot = await _snapshot(database, candidate)
    assert len(snapshot) == 1_001 and all(stored_row[2] == stored_row[6] for stored_row in snapshot)
    assert all(json.loads(stored_row[4])["partOf"] == {"reference": "Location/parent"} for stored_row in snapshot)


@pytest.mark.parametrize("resource_type", cms.RESOURCE_TYPES)
async def test_all_eight_families_preserve_raw_and_normalized_payloads(resource_candidate, resource_type):
    database, candidate, _directory = resource_candidate
    resources = [
        {"resourceType": resource_type, **resource} for resource in support._resource_rows("first")[resource_type]
    ]
    await _write(candidate, resources)
    snapshot = await _snapshot(database, candidate)
    raw_by_id = {resource["id"]: resource for resource in resources}
    model = fhir.RESOURCE_MODELS_BY_TYPE[resource_type]
    expected_by_id = {
        row_by_field["resource_id"]: row_by_field
        for row_by_field in fhir._endpoint_dataset_resource_rows(
            model,
            [cms._parse_batch_row(fhir, resource, candidate)[1] for resource in resources],
            dataset_id=candidate.dataset_id,
            resource_hash_contract=candidate.resource_hash_contract,
        )
    }
    assert len(snapshot) == len(raw_by_id)
    assert all(json.loads(row[4]) == raw_by_id[row[1]] and row[2] == row[6] for row in snapshot)
    assert all(json.loads(row[3]) == expected_by_id[row[1]]["payload_json"] for row in snapshot)
    before = snapshot
    await _write(candidate, resources)
    assert await _snapshot(database, candidate) == before


@pytest.mark.parametrize("failure_phase", ["before_copy", "after_normalized_copy", "after_witness_copy"])
async def test_copy_errors_propagate_and_rollback_without_fallback(resource_candidate, monkeypatch, failure_phase):
    database, candidate, _directory = resource_candidate
    original_copy = asyncpg.Connection.copy_records_to_table
    monkeypatch.setattr(fhir, "_upsert_rows_values", _forbid_values)

    async def failed_copy(connection, table, *args, **kwargs):
        if failure_phase == "before_copy":
            raise RuntimeError("synthetic_copy_failure")
        result = await original_copy(connection, table, *args, **kwargs)
        if (
            failure_phase == "after_normalized_copy"
            and table.endswith("_ri")
            or failure_phase == "after_witness_copy"
            and table.endswith("_wi")
        ):
            raise RuntimeError("synthetic_copy_failure")
        return result

    with monkeypatch.context() as failures:
        failures.setattr(asyncpg.Connection, "copy_records_to_table", failed_copy)
        with pytest.raises(RuntimeError, match="synthetic_copy_failure"):
            await _write(candidate, [{"resourceType": "Location", "id": "office"}])
    assert await _snapshot(database, candidate) == []
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_dataset_proof_shard") == 0


async def test_raw_conflict_and_caller_rollback_preserve_all_tables(resource_candidate):
    database, candidate, _directory = resource_candidate
    resource_by_field = {"resourceType": "Location", "id": "office", "name": "Example", "unmapped": "First"}
    await _write(candidate, [resource_by_field])
    baseline = await _snapshot(database, candidate)
    with pytest.raises(GenericCandidateError, match="witness_replay_conflict"):
        async with source_transaction(database):
            await _write(
                candidate, [{**resource_by_field, "id": "new-office"}, {**resource_by_field, "unmapped": "Changed"}]
            )
            await _snapshot(database, candidate)
    assert await _snapshot(database, candidate) == baseline
    with pytest.raises(RuntimeError, match="synthetic_caller_rollback"):
        async with source_transaction(database):
            await _write(candidate, [{**resource_by_field, "id": "new-office"}])
            raise RuntimeError("synthetic_caller_rollback")
    assert await _snapshot(database, candidate) == baseline


async def test_candidate_hash_and_projection_fences_remain_required(resource_candidate):
    database, candidate, _directory = resource_candidate
    resource_by_field = {"resourceType": "Location", "id": "office"}
    for altered in (
        replace(candidate, resource_hash_contract=fhir.LEGACY_RESOURCE_HASH_CONTRACT),
        replace(candidate, semantic_projection_as_of="2024-01-01"),
    ):
        with pytest.raises(GenericCandidateError, match="acquisition_changed"):
            await _write(altered, [resource_by_field])
    assert await _snapshot(database, candidate) == []


async def test_native_driver_error_is_not_a_values_fallback(resource_candidate, monkeypatch):
    database, candidate, _directory = resource_candidate
    monkeypatch.setattr(fhir, "_upsert_rows_values", _forbid_values)

    async def failed_driver_copy(_connection, _table_name, **_kwargs):
        raise OSError("synthetic_native_copy_failure")

    with monkeypatch.context() as failures:
        failures.setattr(asyncpg.Connection, "copy_records_to_table", failed_driver_copy)
        with pytest.raises(OSError, match="synthetic_native_copy_failure"):
            await _write(candidate, [{"resourceType": "Location", "id": "office"}])
    assert await _snapshot(database, candidate) == []


@pytest.mark.parametrize("unmapped", [{"bad\x00key": "value"}, {"label": "bad\x00value"}])
async def test_late_invalid_raw_evidence_rolls_back_the_complete_batch(resource_candidate, unmapped):
    database, candidate, _directory = resource_candidate
    resources = [
        {"resourceType": "Location", "id": "valid-office"},
        {"resourceType": "Location", "id": "invalid-office", "unmapped": unmapped},
    ]
    with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_resource_invalid"):
        await _write(candidate, resources)
    assert await _snapshot(database, candidate) == []
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_dataset_proof_shard") == 0


async def test_changed_edition_cannot_attach_witnesses_to_another_release(resource_candidate):
    database, candidate, _directory = resource_candidate
    altered_candidate = replace(candidate, source_release={**candidate.source_release, "vector_sha256": "0" * 64})
    with pytest.raises(GenericCandidateError, match="acquisition_changed"):
        await _write(altered_candidate, [{"resourceType": "Location", "id": "office"}])
    assert await _snapshot(database, candidate) == []


async def test_batch_requires_the_same_caller_transaction(resource_candidate):
    database, candidate, _directory = resource_candidate
    model, parsed_by_field = cms._parse_batch_row(fhir, {"resourceType": "Location", "id": "office"}, candidate)
    rows, proof = prepare_cms_dataset_rows(fhir, model, [parsed_by_field], candidate)
    async with source_transaction(database):
        async with database.session_factory() as other_session:
            with pytest.raises(RuntimeError, match="cms_npd_resource_batch_transaction_missing"):
                await persist_prepared_cms_dataset_rows(fhir, other_session, rows, proof, candidate)
    assert await _snapshot(database, candidate) == []


async def test_worker_pool_stages_the_same_rows_witnesses_and_proofs(resource_candidate, monkeypatch, tmp_path):
    """A spawned worker prepares the batches; the publisher process commits the same facts as the inline path."""
    database, candidate, _directory = resource_candidate
    resources = [{"resourceType": "Location", "id": f"office-{ordinal}", "name": "Example"} for ordinal in range(3)]
    path = tmp_path / "locations.zst"
    path.write_bytes(zstd.compress(b"".join(json.dumps(resource).encode() + b"\n" for resource in resources)))
    monkeypatch.setattr(cms, "BATCH_SIZE", 2)
    with ProcessPoolExecutor(max_workers=1) as executor:
        async with source_transaction(database):
            assert await cms._stream_file(fhir, path, candidate, "Location", {}, {}, executor) == 3
    snapshot = await _snapshot(database, candidate)
    model = fhir.RESOURCE_MODELS_BY_TYPE["Location"]
    prepared_rows, _proof = prepare_cms_dataset_rows(
        fhir, model, [cms._parse_batch_row(fhir, resource, candidate)[1] for resource in resources], candidate
    )
    witness_by_id = cms._source_witnesses_by_id(resources, candidate, "Location")
    assert [stored_row[1] for stored_row in snapshot] == [row_by_field["resource_id"] for row_by_field in prepared_rows]
    assert all(
        stored_row[2] == stored_row[6] == expected["payload_hash"]
        and json.loads(stored_row[3]) == json.loads(expected["payload_json"])
        for stored_row, expected in zip(snapshot, prepared_rows)
    )
    assert all(
        stored_row[5] == witness_by_id[stored_row[1]]["raw_payload_sha256"]
        and json.loads(stored_row[4]) == json.loads(witness_by_id[stored_row[1]]["raw_payload_json"])
        for stored_row in snapshot
    )
    assert (
        await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_dataset_proof_shard WHERE dataset_id=:dataset_id",
            dataset_id=candidate.dataset_id,
        )
        == 2
    )


async def test_stream_preappend_bounds_and_cancellation_rollback(resource_candidate, monkeypatch, tmp_path):
    database, candidate, _directory = resource_candidate
    resources = [{"resourceType": "Location", "id": f"office-{ordinal}", "name": "Example"} for ordinal in range(4)]
    lines = [json.dumps(resource).encode() + b"\n" for resource in resources]
    path = tmp_path / "locations.zst"
    path.write_bytes(zstd.compress(b"".join(lines)))
    monkeypatch.setattr(cms, "BATCH_MAX_DECODED_BYTES", len(lines[0]) * 2 - 1)
    original_persist = cms._persist_prepared_batch
    batch_counts = []

    async def tracked_persist(*args):
        batch_counts.append(args[1].input_count)
        return await original_persist(*args)

    async def cancelled(*_args):
        raise asyncio.CancelledError("synthetic_cancelled")

    monkeypatch.setattr(cms, "_persist_prepared_batch", tracked_persist)
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", cancelled)
    with pytest.raises(asyncio.CancelledError, match="synthetic_cancelled"):
        async with source_transaction(database):
            await cms._stream_file(fhir, path, candidate, "Location", {}, {})
    assert batch_counts == [1]
    assert await _snapshot(database, candidate) == []


async def _protect_custody_schema(connection, names):
    """Assign only the fixture schema to distinct native protected and runtime roles."""
    owner, reader, writer = names
    for name in names:
        await connection.execute(f'CREATE ROLE "{name}" NOLOGIN')
    await connection.execute(f'ALTER SCHEMA mrf OWNER TO "{owner}"')
    statements = await connection.fetchval(
        """SELECT string_agg(statement,';') FROM (
      SELECT format('ALTER TABLE mrf.%I OWNER TO %I',relname,$1::text) statement
      FROM pg_class WHERE relnamespace='mrf'::regnamespace AND relkind IN ('r','p')
      UNION ALL SELECT format('ALTER FUNCTION %s OWNER TO %I',p.oid::regprocedure,$1::text)
      FROM pg_proc p WHERE p.pronamespace='mrf'::regnamespace) changes""",
        owner,
    )
    await connection.execute(statements)
    await connection.execute(f'GRANT USAGE ON SCHEMA mrf TO "{reader}","{writer}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA mrf TO "{reader}","{writer}"')
    await connection.execute(f'GRANT INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA mrf TO "{writer}"')


@pytest.mark.parametrize("cms_resource_template", [support.LEGACY_MIGRATION_PREFIXES], indirect=True)
@pytest.mark.parametrize(
    "damage",
    [
        None,
        "disabled",
        "body",
        "acl",
        "owner",
        "generic",
        "receipt",
        "initial_function",
        "initial_columns",
        "initial_arguments",
        "initial_transition",
        "initial_routine",
        "initial_foreign_routine",
    ],
)
async def test_retained_cms_custody_native_closure(monkeypatch, tmp_path, damage):
    """Actual eight-file admission proves closed guards and rejects catalog or receipt drift."""
    names = tuple("custody_" + uuid4().hex for _ in range(3))
    async with support.admission_database(
        monkeypatch, migration_prefixes=support.LEGACY_MIGRATION_PREFIXES
    ) as database:
        candidate, release, digest = await stage_legacy_retained_source(monkeypatch, tmp_path, database)
        await publish_legacy_retained_source(database, candidate, release, digest)
        await assert_legacy_candidate_coverage(database, candidate, release, digest)
        async with database.session() as session:
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            try:
                source_pin = PinnedFHIRMembershipSource(
                    "mrf",
                    "cms-npd",
                    candidate.endpoint_id,
                    candidate.dataset_id,
                    digest,
                    release,
                    await driver.fetchval("SELECT 'mrf.provider_directory_dataset_resource'::regclass::oid"),
                    "cms-npd",
                    "2026-01-01",
                )
                await _protect_custody_schema(driver, names)
                await session.commit()
                async with driver.transaction(isolation="repeatable_read"):
                    await _check_native_custody(driver, source_pin, names, damage)
            finally:
                await _remove_custody_roles(driver, names)


async def _check_native_custody(driver, source_pin, names, damage):
    """Check a real receipt before and after one isolated native capability change."""
    receipt = await capture_retained_cms_fhir_source_custody(
        driver, source_pin, owner_role=names[0], runtime_roles=names[1:]
    )
    assert await require_retained_cms_fhir_source_custody(driver, receipt) == receipt
    if damage and damage.startswith("initial_"):
        await _damage_custody(driver, names, damage)
        with pytest.raises(FHIRSourceCustodyError, match="^fhir_source_custody_unavailable$"):
            await capture_retained_cms_fhir_source_custody(
                driver, source_pin, owner_role=names[0], runtime_roles=names[1:]
            )
        return
    if damage:
        await _damage_custody(driver, names, damage)
        if damage == "generic":
            receipt = replace(receipt, source_pin=replace(source_pin, source_id="example-source"))
        if damage == "receipt":
            receipt = replace(receipt, proof_sha256="0" * 64)
        with pytest.raises(FHIRSourceCustodyError, match="^fhir_source_custody_unavailable$"):
            await require_retained_cms_fhir_source_custody(driver, receipt)
    else:
        await _other_edition_write(driver, names[2])
        assert await require_retained_cms_fhir_source_custody(driver, receipt) == receipt


async def _remove_custody_roles(driver, names):
    """Drop only the exact test roles after their owned fixture objects and grants."""
    owned_roles = []
    for name in names:
        if await driver.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", name):
            owned_roles.append(name)
    if owned_roles:
        role_list = ",".join(f'"{name}"' for name in owned_roles)
        await driver.execute(f"DROP OWNED BY {role_list} CASCADE")
    for name in owned_roles:
        await driver.execute(f'DROP ROLE "{name}"')
        assert not await driver.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", name)


async def _damage_custody(connection, names, damage):
    """Mutate only the isolated fixture's native custody capabilities."""
    sql_by_damage = {
        "disabled": "ALTER TABLE mrf.provider_directory_dataset_resource DISABLE TRIGGER cms_npd_published_resource_immutable",
        "body": "CREATE OR REPLACE FUNCTION mrf.guard_cms_npd_published_resource() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'",
        "acl": f'GRANT TRIGGER ON mrf.provider_directory_dataset_resource TO "{names[2]}"',
        "owner": f'ALTER TABLE mrf.provider_directory_dataset_resource OWNER TO "{names[2]}"',
        "initial_function": """CREATE OR REPLACE TRIGGER cms_npd_published_resource_immutable
          AFTER INSERT ON mrf.provider_directory_dataset_resource REFERENCING NEW TABLE AS changed_new
          FOR EACH STATEMENT EXECUTE FUNCTION mrf.guard_cms_npd_covered_network_witness()""",
        "initial_columns": """CREATE OR REPLACE TRIGGER provider_directory_endpoint_dataset_admission_seal_guard
          BEFORE INSERT OR UPDATE OF status ON mrf.provider_directory_endpoint_dataset
          FOR EACH ROW EXECUTE FUNCTION mrf.guard_provider_directory_endpoint_dataset_admission_seal()""",
        "initial_arguments": """CREATE OR REPLACE TRIGGER provider_directory_endpoint_dataset_admission_raw_guard
          BEFORE UPDATE OF publication_metadata_json ON mrf.provider_directory_endpoint_dataset
          FOR EACH ROW EXECUTE FUNCTION mrf.guard_provider_directory_endpoint_dataset_admission_seal('other')""",
        "initial_transition": """CREATE OR REPLACE TRIGGER cms_npd_published_resource_immutable
          AFTER INSERT ON mrf.provider_directory_dataset_resource REFERENCING NEW TABLE AS other_rows
          FOR EACH STATEMENT EXECUTE FUNCTION mrf.guard_cms_npd_published_resource()""",
        "initial_routine": """CREATE FUNCTION mrf.example_definer() RETURNS void LANGUAGE plpgsql
          IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog AS 'BEGIN RETURN; END'""",
        "initial_foreign_routine": f"""CREATE SCHEMA custody_extra AUTHORIZATION "{names[0]}";
          GRANT USAGE ON SCHEMA custody_extra TO "{names[2]}";
          CREATE FUNCTION custody_extra.example_definer() RETURNS void LANGUAGE plpgsql
          SECURITY DEFINER SET search_path=pg_catalog AS 'BEGIN RETURN; END'""",
    }
    if damage in sql_by_damage:
        await connection.execute(sql_by_damage[damage])


async def _other_edition_write(connection, writer):
    """A guarded writer can add a new release witness without changing retained custody."""
    await connection.execute(f'SET LOCAL ROLE "{writer}"')
    await connection.execute("""INSERT INTO mrf.provider_directory_entity_release_evidence
      SELECT source_id,resource_type,resource_id,'other-edition',payload_sha256,payload_json,observed_at
      FROM mrf.provider_directory_entity_release_evidence LIMIT 1""")
    await connection.execute("RESET ROLE")


async def _assert_epoch_capture(driver, source_pin, capture_options_by_field, damage):
    """Check immutable replay, wrong trusted proof, and task-owned capability drift."""
    from process.network_fhir_membership_source import _require_pinned_source
    from process.network_fhir_source_epoch import (
        FHIRSourceEpochError,
        capture_retained_cms_fhir_source_epoch,
        require_retained_cms_fhir_source_epoch,
        validate_retained_cms_fhir_source_epoch,
    )

    epoch_id = capture_options_by_field["epoch_id"]
    runtime_role = capture_options_by_field["runtime_roles"][0]
    async with driver.transaction(isolation="repeatable_read"):
        if damage in {"expectation", "source_payload"}:
            if damage == "expectation":
                capture_options_by_field["expected_admission_sha256"] = "0" * 64
            with pytest.raises(FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
                await capture_retained_cms_fhir_source_epoch(driver, source_pin, **capture_options_by_field)
            assert await driver.fetchval("SELECT to_regnamespace($1)", "registry_cms_epoch_" + epoch_id.hex) is None
            return
        epoch = await capture_retained_cms_fhir_source_epoch(driver, source_pin, **capture_options_by_field)
        assert validate_retained_cms_fhir_source_epoch(epoch.as_dict()) == epoch
        from process.network_fhir_source_epoch import _content

        assert await _content(driver, source_pin.schema_name) == epoch.content_sha256
        assert await require_retained_cms_fhir_source_epoch(driver, source_pin, epoch) == epoch
        assert await require_retained_cms_fhir_source_epoch(driver, source_pin, epoch, verify_content=False) == epoch
        _assert_epoch_wire(epoch)
        assert not await driver.fetchval(
            "SELECT pg_has_role($1,$2,'SET') OR pg_has_role($1,$2,'MEMBER')", runtime_role, epoch.owner_role
        )
        if damage == "query_plan":
            await _assert_epoch_lookup_plans(driver, source_pin, epoch)
        retained_pin = replace(source_pin, retained_epoch=epoch)
        await _require_pinned_source(driver, retained_pin)
        if damage in {"drop_index", "recreate_index", "wrong_index"}:
            await _assert_epoch_index_damage(driver, source_pin, epoch, damage)
        if damage == "epoch_write":
            await driver.execute(f'GRANT UPDATE ON ALL TABLES IN SCHEMA "{epoch.schema_name}" TO "{runtime_role}"')
            with pytest.raises(FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
                await require_retained_cms_fhir_source_epoch(driver, source_pin, epoch)
        if damage == "origin_change":
            await driver.execute(
                "INSERT INTO mrf.provider_directory_entity_release_evidence SELECT source_id,resource_type,resource_id,'later-source-edition',payload_sha256,payload_json,observed_at FROM mrf.provider_directory_entity_release_evidence LIMIT 1"
            )
            assert await require_retained_cms_fhir_source_epoch(driver, source_pin, epoch) == epoch
            await _require_pinned_source(driver, retained_pin)


async def _assert_epoch_index_damage(driver, source_pin, epoch, damage):
    """Deny missing, differently keyed or identically rebuilt indexes through cheap custody."""
    from process.network_fhir_source_epoch import FHIRSourceEpochError, require_retained_cms_fhir_source_epoch

    await driver.execute(f'DROP INDEX "{epoch.schema_name}".cms_epoch_entity_site')
    if damage != "drop_index":
        column = "site_id" if damage == "recreate_index" else "resource_id"
        await driver.execute(
            f'CREATE INDEX cms_epoch_entity_site ON "{epoch.schema_name}".provider_directory_entity_source_binding ({column})'
        )
    with pytest.raises(FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
        await require_retained_cms_fhir_source_epoch(driver, source_pin, epoch, verify_content=False)


def _enlarge_epoch_source_rows(monkeypatch, size):
    """Acquire the same bounded synthetic raw ledger in source and retained snapshots."""
    original_rows = support._resource_rows

    def bounded_rows(revision):
        """Preserve eight resource families and explicit source references at native admission."""
        rows_by_type = original_rows(revision)
        for kind in ("PractitionerRole", "Location", "InsurancePlan"):
            sample = rows_by_type[kind][0]
            rows_by_type[kind].extend({**sample, "id": kind + "-" + str(index).zfill(4)} for index in range(size))
        sample = rows_by_type["PractitionerRole"][0]
        rows_by_type["PractitionerRole"].extend(
            {**sample, "id": identity} for identity in ("Role-A", "Role-a", "role-A", "role-a")
        )
        return rows_by_type

    monkeypatch.setattr(support, "_resource_rows", bounded_rows)


def _plan_indexes(node):
    """Read native JSON EXPLAIN nodes without changing planner settings."""
    indexes = {node["Index Name"]} if "Index Name" in node else set()
    for child in node.get("Plans", []):
        indexes.update(_plan_indexes(child))
    return indexes


async def _assert_epoch_lookup_plans(driver, source_pin, epoch):
    """Prove default keyset, witness, exact site and plan evidence indexed lookups."""
    namespace = '"' + epoch.schema_name + '"'
    site_id = await driver.fetchval(
        f"SELECT site_id FROM {namespace}.provider_directory_entity_source_binding WHERE resource_type='Location' AND resource_id='Location-0500'"
    )
    cases = (
        (
            "provider_directory_dataset_resource",
            "dataset_id=$1 AND resource_type='PractitionerRole' AND resource_id>$2 ORDER BY resource_type,resource_id LIMIT 50",
            (source_pin.dataset_id, "PractitionerRole-0500"),
            "cms_epoch_pk_1",
        ),
        (
            "provider_directory_cms_npd_resource_witness",
            "dataset_id=$1 AND resource_type='PractitionerRole' AND resource_id=$2",
            (source_pin.dataset_id, "PractitionerRole-0500"),
            "cms_epoch_pk_6",
        ),
        ("provider_directory_entity_source_binding", "site_id=$1", (site_id,), "cms_epoch_entity_site"),
        (
            "provider_directory_insurance_network_plan_evidence",
            "source_id=$1 AND release_id=$2 AND insurance_plan_resource_id=$3",
            ("cms-npd", source_pin.release_id, "InsurancePlan-0500"),
            "cms_epoch_plan_release",
        ),
    )
    for table, predicate, parameters, expected_index in cases:
        plan_json = await driver.fetchval(
            f'EXPLAIN (FORMAT JSON) SELECT * FROM {namespace}."{table}" WHERE ' + predicate, *parameters
        )
        plan = json.loads(plan_json) if isinstance(plan_json, str) else plan_json
        assert expected_index in _plan_indexes(plan[0]["Plan"])
        retained_rows = await driver.fetch(f'SELECT * FROM {namespace}."{table}" WHERE ' + predicate, *parameters)
        source_rows = await driver.fetch(
            f'SELECT * FROM "{source_pin.schema_name}"."{table}" WHERE ' + predicate, *parameters
        )
        assert retained_rows == source_rows and retained_rows


def _assert_epoch_wire(epoch):
    """Reject noncanonical wire arrays and UUID strings before database authority."""
    from process.network_fhir_source_epoch import FHIRSourceEpochError, validate_retained_cms_fhir_source_epoch

    for field, invalid in (
        ("runtime_roles", epoch.runtime_roles[0]),
        ("origin_coordinates", "abc"),
        ("relation_oids", "abc"),
        ("epoch_id", str(epoch.epoch_id).upper()),
    ):
        wire_by_field = epoch.as_dict()
        wire_by_field[field] = invalid
        with pytest.raises(FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
            validate_retained_cms_fhir_source_epoch(wire_by_field)


def _tamper_epoch_export(monkeypatch):
    """Alter a native exported normalized row while retaining its witnessed hashes."""
    from process import network_fhir_source_epoch as epochs

    original_rows = epochs._json_copy_rows

    def tampered_rows(path):
        """Keep the actual COPY transport and change only one payload field."""
        for row_by_field in original_rows(path):
            row_by_field["payload_json"]["unrecognized_epoch_field"] = True
            yield row_by_field

    monkeypatch.setattr(epochs, "_json_copy_rows", tampered_rows)


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "expectation",
        "epoch_write",
        "origin_change",
        "source_payload",
        "drop_index",
        "recreate_index",
        "wrong_index",
        "query_plan",
        "mixed_case",
    ],
)
async def test_native_retained_cms_epoch_closes_independent_custody(monkeypatch, tmp_path, damage):
    """Retain actual eight-file content with trusted admission digests and independent ACLs."""
    names = tuple("epoch_" + uuid4().hex for _ in range(2))
    epoch_id = uuid4()
    async with support.admission_database(monkeypatch) as database:
        if damage in {"query_plan", "mixed_case"}:
            _enlarge_epoch_source_rows(monkeypatch, 1000 if damage == "query_plan" else 1)
        if damage == "mixed_case":
            await database.status(
                "ALTER TABLE mrf.provider_directory_dataset_resource "
                'ALTER COLUMN resource_id TYPE varchar(256) COLLATE "en-x-icu"'
            )
            monkeypatch.setattr(fhir, "ENDPOINT_DATASET_HASH_BATCH_SIZE", 2)
        candidate, release, digest = await stage_legacy_retained_source(monkeypatch, tmp_path, database)
        await assert_legacy_candidate_coverage(database, candidate, release, digest)
        async with database.session() as session:
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            await session.commit()
            try:
                for name in names:
                    await driver.execute(f'CREATE ROLE "{name}" NOLOGIN')
                source_pin = PinnedFHIRMembershipSource(
                    "mrf",
                    "cms-npd",
                    candidate.endpoint_id,
                    candidate.dataset_id,
                    digest,
                    release,
                    await driver.fetchval("SELECT 'mrf.provider_directory_dataset_resource'::regclass::oid"),
                    "cms-npd",
                    "2026-01-01",
                )
                expected = await driver.fetchrow(
                    "SELECT content_proof_admission_sha256,publication_metadata_sha256 FROM mrf.provider_directory_endpoint_dataset WHERE dataset_id=$1",
                    candidate.dataset_id,
                )
                capture_options_by_field = dict(
                    epoch_id=epoch_id,
                    owner_role=names[0],
                    runtime_roles=(names[1],),
                    expected_admission_sha256=expected[0],
                    expected_metadata_sha256=expected[1],
                )
                if damage == "source_payload":
                    _tamper_epoch_export(monkeypatch)
                await _assert_epoch_capture(driver, source_pin, capture_options_by_field, damage)
            finally:
                await driver.execute(f'DROP SCHEMA IF EXISTS "registry_cms_epoch_{epoch_id.hex}" CASCADE')
                await _remove_custody_roles(driver, names)


async def test_native_epoch_full_profile_keeps_live_publication_guard(monkeypatch, tmp_path):
    """Epoch admission does not substitute for the fresh live CMS composite receipt."""
    async with support.admission_database(monkeypatch) as database:
        candidate, release, digest = await stage_legacy_retained_source(monkeypatch, tmp_path, database)
        with pytest.raises(DBAPIError, match="cms_serving_fresh_receipt_required"):
            await publish_legacy_retained_source(database, candidate, release, digest)


@pytest.mark.parametrize("damage", [None, "binding", "seal", "current"])
async def test_registry_admission_handoff_preserves_independent_completion(monkeypatch, tmp_path, damage):
    """Emit actual completed proof inputs, replay exactly, and reject changed proof material."""
    directory, acquired = support.retained_release(tmp_path)
    completed_seals = []
    original_store = fhir._store_validated_endpoint_dataset

    async def observed_store(*args, **kwargs):
        """Capture the freshly minted validator output independently of stored scalar reads."""
        seal = await original_store(*args, **kwargs)
        completed_seals.append(seal)
        return seal

    monkeypatch.setattr(fhir, "_store_validated_endpoint_dataset", observed_store)
    async with support.protected_admission_database(monkeypatch) as database:
        async with bind_generic_source_transaction_factory(database._source_component.factory):
            await _assert_registry_admission_handoff(
                monkeypatch, directory, acquired, database, completed_seals, damage
            )


async def _assert_registry_admission_handoff(monkeypatch, directory, acquired, database, completed_seals, damage):
    """Keep admission and replay on their original protected SOURCE factory."""
    with support.release_probe_client(directory) as client:
        initial = await cms._run_acquired({"context": {}}, {}, "registry-proof", directory, acquired, client)
        receipt = initial["registry_source_admission"]
        seal = completed_seals[-1]
        assert receipt["expected_admission_sha256"] == seal.proof_sha256
        assert receipt["expected_metadata_sha256"] == seal.metadata_sha256
        assert receipt["network_bindings"] == seal.metadata_summary["network_bindings"]
        assert set(receipt) == {
            "version",
            "network_bindings",
            "endpoint_id",
            "dataset_sha256",
            "release_id",
            "resource_table_oid",
            "semantic_projection_as_of",
            "expected_admission_sha256",
            "expected_metadata_sha256",
        }
        assert set(initial["cms_serving_candidate"]) == {
            "version",
            "status",
            "desired_cms_dataset",
            "expected_cms_incumbent",
            "release_id",
            "proof_version",
        }
        assert initial["cms_serving_candidate"]["desired_cms_dataset"]["is_current"] is False
        assert len(json.dumps(receipt).encode()) <= 4096
        await _assert_registry_handoff_retry(monkeypatch, directory, acquired, client, receipt, initial, damage)
        assert len(completed_seals) == 1
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current") == 0


async def _assert_registry_handoff_retry(monkeypatch, directory, acquired, client, receipt, initial, damage):
    """Exercise the actual completed source replay and the current-source branch."""
    if damage == "current":
        state = await fhir._endpoint_dataset_state(initial["dataset_id"])
        assert await cms._current_registry_source_admission(fhir, state) == receipt
    elif damage is None:
        replay = await cms._run_acquired({"context": {}}, {}, "registry-replay", directory, acquired, client)
        assert replay["registry_source_admission"] == receipt
    else:
        _damage_registry_handoff(monkeypatch, damage)
        with pytest.raises(GenericCandidateError, match="admission_replay_changed"):
            await cms._run_acquired({"context": {}}, {}, "registry-damaged", directory, acquired, client)


def _damage_registry_handoff(monkeypatch, damage):
    """Change the genuine sealed SOURCE result at its immutable replay boundary."""
    if damage == "seal":
        original_seal = cms._source_registry_replay_seal

        def changed_seal(candidate, row):
            return original_seal(candidate, {**dict(row), "seal_valid": False})

        monkeypatch.setattr(cms, "_source_registry_replay_seal", changed_seal)
    else:
        from copy import deepcopy

        from process import network_fhir_source_custody as custody

        original_custody = custody._generic_partitioned_custody

        async def changed_binding(*args, **kwargs):
            observed = deepcopy(await original_custody(*args, **kwargs))
            observed["binding"]["identity"]["acquisition_root_run_id"] += "-changed"
            return observed

        monkeypatch.setattr(custody, "_generic_partitioned_custody", changed_binding)


async def test_native_metadata_first_toast_write_and_larger_growth(monkeypatch):
    """Run the shared native regression under the enrolled resource template."""
    from tests import test_provider_directory_cms_complete_publication_regressions as regressions

    await regressions.test_native_metadata_first_toast_write_and_larger_growth(monkeypatch)


async def test_native_receipt_first_write_and_growth(monkeypatch):
    """Run the shared native regression under the enrolled resource template."""
    from tests import test_provider_directory_cms_complete_publication_regressions as regressions

    await regressions.test_native_receipt_first_write_and_growth(monkeypatch)


async def test_native_scope_declared_gin_and_catalog_drift(monkeypatch):
    """Run the shared native regression under the enrolled resource template."""
    from tests import test_provider_directory_cms_complete_publication_regressions as regressions

    await regressions.test_native_scope_declared_gin_and_catalog_drift(monkeypatch)
