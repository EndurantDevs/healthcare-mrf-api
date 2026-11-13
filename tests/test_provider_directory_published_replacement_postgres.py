# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit published replacement fences on real PostgreSQL JSON columns."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from sqlalchemy import event, text

from db.connection import Database
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_published_source_replacement as replacement
from tests import provider_directory_generic_artifact_pages_support as page_copy
from tests.provider_directory_dataset_artifact_pg_support import seal_validated_dataset
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case
from tests.test_provider_directory_dataset_artifact_db import importer
from tests.test_provider_directory_published_alias_reconciliation_db import (
    _install_current_serving_dataset,
    _orphan_source_alias,
)


@asynccontextmanager
async def _dataset_database(monkeypatch):
    """Keep every replacement case inside the existing UUID-owned ordinary boundary."""
    async with ordinary_serving_case(monkeypatch) as case:
        yield case.database, case.schema


def _markers(**changed):
    markers_by_field = dict(
        source_id="source_primary",
        endpoint_id="endpoint_shared",
        dataset_id="dataset_shared",
        dataset_hash="b" * 64,
        parent_run_id="root-shared",
        version=1,
        stage="address_overlay",
        attempt=1,
    )
    markers_by_field.update(changed)
    return {"provider_directory_dataset_followup_" + name: value for name, value in markers_by_field.items()}


async def _prepare(database, schema):
    await _orphan_source_alias(database, schema)
    await _install_current_serving_dataset(database, schema)
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET published_at=timestamp '2026-01-01' "
        "WHERE dataset_id='dataset_serving'",
    )
    metadata = json.dumps(
        dict(
            source_ids=["source_primary", "source_sibling"],
            selected_resources=["Location"],
            expected_resources=["Location"],
            requires_twin_root_verification=False,
        )
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET dataset_hash=:digest, "
        "validated_at=timestamp '2026-01-02',published_at=timestamp '2026-01-02', "
        "publication_metadata_json=CAST(:metadata AS jsonb) WHERE dataset_id='dataset_shared'",
        digest="b" * 64,
        metadata=metadata,
    )
    await seal_validated_dataset(database, schema, "dataset_shared")


async def _alias(database, schema):
    return await database.scalar(
        f"SELECT endpoint_id FROM {schema}.provider_directory_source WHERE source_id='source_primary'"
    )


async def _cut(database, fence):
    async with database.transaction():
        await importer._lock_and_verify_artifact_dataset_fence(fence, database)
        await importer._cutover_provider_directory_artifact_sources(fence)


async def _newer(database, schema, *, endpoint="endpoint_unpublished", qualified=True):
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id,endpoint_id,import_run_id,acquisition_root_run_id,dataset_hash,status,is_current,resource_count,"
        "validated_at,published_at,publication_metadata_json,publication_metadata_summary_json,publication_metadata_sha256,"
        "content_proof_admission_version,content_proof_admission_kind,content_proof_admission_sha256,content_proof_resource_types) "
        "SELECT 'dataset_newer',:endpoint,'run-newer','root-newer',dataset_hash,:status,true,resource_count,"
        "validated_at,timestamp '2026-01-03',publication_metadata_json,publication_metadata_summary_json,publication_metadata_sha256,"
        "content_proof_admission_version,content_proof_admission_kind,content_proof_admission_sha256,content_proof_resource_types "
        f"FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id='dataset_shared'",
        endpoint=endpoint,
        status="published" if qualified else "incomplete",
    )


async def _assert_old_alias_is_ordinary_selection():
    ordinary = await importer._resolve_provider_directory_artifact_datasets(["source_primary"])
    assert ordinary.datasets[0].dataset_id == "dataset_serving"
    with pytest.raises(RuntimeError, match="published_source_alias_fence_changed"):
        await importer.ensure_provider_directory_published_source_alias(
            source_id="source_primary",
            expected_dataset_id="dataset_shared",
            expected_root_run_id="root-shared",
        )


async def _assert_address(address, schema):
    await address._assert_current_provider_directory_dataset(
        schema,
        source_id="source_primary",
        expected_dataset_id="dataset_shared",
        expected_root_run_id="root-shared",
    )


async def test_native_old_valid_alias_staged_reads_atomic_rollback_and_replay(monkeypatch):
    address = importlib.import_module("process.entity_address_unified")
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        monkeypatch.setattr(address, "db", database)
        await _assert_old_alias_is_ordinary_selection()
        async with replacement.preparation_scope(importer, _markers()) as fence:
            artifacts.require_overlay_followup_fence(importer, fence, _markers())
            assert replacement.selected(importer, fence).incumbent[0] == "dataset_serving"
            assert (
                replacement.selected(importer, fence).incumbent[4] is None
            )  # Legacy incumbents have no invented hash.
            async with importer._provider_directory_artifact_dataset_scope(
                run_id="staged-run",
                source_ids=["source_primary"],
                fence=fence,
                resource_fence=fence,
                metrics={},
                resource_types=frozenset({"Location"}),
            ):
                table = importer._qt(schema, importer.ProviderDirectoryLocation.__tablename__)
                assert await database.scalar(f"SELECT count(*) FROM {table}") > 0
                assert await _alias(database, schema) == "endpoint_repoint"
            with pytest.raises(RuntimeError, match="address_alias_not_committed"):
                await _assert_address(address, schema)
            with pytest.raises(RuntimeError, match="rollback probe"):
                async with database.transaction():
                    await importer._lock_and_verify_artifact_dataset_fence(fence, database)
                    await importer._cutover_provider_directory_artifact_sources(fence)
                    await _assert_address(address, schema)
                    raise RuntimeError("rollback probe")
            assert await _alias(database, schema) == "endpoint_repoint"
            await _cut(database, fence)
            await _assert_address(address, schema)
        assert (
            await database.scalar(
                f"SELECT endpoint_id FROM {schema}.provider_directory_source WHERE source_id='source_sibling'"
            )
            == "endpoint_shared"
        )
        assert (
            await database.scalar(
                f"SELECT is_current FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id='dataset_serving'"
            )
            is True
        )
        async with replacement.preparation_scope(importer, _markers()) as replay:
            assert replay.datasets[0].reconcile_source_alias_on_cutover is False
            await _cut(database, replay)
        assert await _alias(database, schema) == "endpoint_shared"


@pytest.mark.parametrize(
    "changed", [dict(dataset_id="stale"), dict(dataset_hash="c" * 64), dict(parent_run_id="stale-root")]
)
async def test_native_stale_descriptor_is_never_retargeted(monkeypatch, changed):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        with pytest.raises(RuntimeError, match="descriptor_stale"):
            async with replacement.preparation_scope(importer, _markers(**changed)):
                pytest.fail("stale descriptor entered preparation")
        assert await _alias(database, schema) == "endpoint_repoint"


async def _assert_drift_refused(database, fence, damage):
    expected_error = {
        "configured": "provider_directory_source_verification_contract_changed",
        "incumbent": "provider_directory_published_replacement_incumbent_changed",
        "incumbent_metadata": "provider_directory_published_replacement_incumbent_changed",
        "newer": "provider_directory_published_replacement_newer_publication",
        "same_endpoint": "provider_directory_endpoint_dataset_current_changed",
        "snapshot": "provider_directory_published_replacement_fresh_snapshot_required",
    }[damage]
    with pytest.raises(RuntimeError, match="^" + expected_error + "$"):
        async with database.transaction():
            if damage == "snapshot":
                await database.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            await importer._lock_and_verify_artifact_dataset_fence(fence, database)
            await importer._cutover_provider_directory_artifact_sources(fence)


async def _apply_drift(database, schema, damage):
    match damage:
        case "configured":
            await database.status(
                f"UPDATE {schema}.provider_directory_source SET metadata_json="
                "jsonb_set(metadata_json::jsonb,ARRAY[:key],to_jsonb('endpoint_unpublished'::text)) "
                "WHERE source_id='source_primary'",
                key=importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY,
            )
        case "incumbent":
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET acquisition_root_run_id='changed' WHERE dataset_id='dataset_serving'"
            )
        case "incumbent_metadata":
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET publication_metadata_sha256=repeat('d',64) "
                "WHERE dataset_id='dataset_serving'"
            )
        case "newer":
            await _newer(database, schema)
        case "same_endpoint":
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='superseded',is_current=false,superseded_at=now() WHERE dataset_id='dataset_shared'"
            )
            await _newer(database, schema, endpoint="endpoint_shared")


@pytest.mark.parametrize(
    "damage", ["configured", "incumbent", "incumbent_metadata", "newer", "same_endpoint", "snapshot"]
)
async def test_native_final_fence_refuses_concurrent_drift(monkeypatch, damage):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        async with replacement.preparation_scope(importer, _markers()) as fence:
            await _apply_drift(database, schema, damage)
            await _assert_drift_refused(database, fence, damage)
        assert await _alias(database, schema) == "endpoint_repoint"


async def test_native_newer_incomplete_header_is_not_qualified(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        await _newer(database, schema, qualified=False)
        async with replacement.preparation_scope(importer, _markers()) as fence:
            await _cut(database, fence)
        assert await _alias(database, schema) == "endpoint_shared"


async def test_native_pinned_reader_keeps_old_alias_through_commit(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        reader = Database()
        await reader.connect()
        try:
            async with reader.transaction():
                await reader.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                assert await _alias(reader, schema) == "endpoint_repoint"
                async with replacement.preparation_scope(importer, _markers()) as fence:
                    await _cut(database, fence)
                assert await _alias(reader, schema) == "endpoint_repoint"
            assert await _alias(reader, schema) == "endpoint_shared"
        finally:
            await reader.disconnect()


async def _assert_publisher_blocks_cut(database, fence):
    with pytest.raises(Exception, match="lock|Lock"):
        await _cut(database, fence)


async def test_native_other_publisher_share_lock_prevents_cut(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        publisher = Database()
        await publisher.connect()
        try:
            async with replacement.preparation_scope(importer, _markers()) as fence:
                async with publisher.transaction():
                    await publisher.all(
                        f"SELECT source_id FROM {schema}.provider_directory_source WHERE source_id='source_primary' FOR SHARE"
                    )
                    await _assert_publisher_blocks_cut(database, fence)
                    assert await _alias(database, schema) == "endpoint_repoint"
                await _cut(database, fence)
        finally:
            await publisher.disconnect()


async def test_preparation_cannot_be_borrowed_by_another_task(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        async with replacement.preparation_scope(importer, _markers()) as fence:

            async def borrowed():
                replacement.selected(importer, fence)

            with pytest.raises(RuntimeError, match="scope_changed"):
                await asyncio.create_task(borrowed())


async def _swap_and_abort(database, schema, stage, fence, retain):
    from process import provider_directory_serving_recovery_receipts as receipts

    target_relation = stage.target_relation
    old_oid = await database.scalar(f"SELECT '{schema}.{target_relation}'::regclass::oid::bigint")
    async with database.transaction():
        await importer._lock_and_verify_artifact_dataset_fence(fence, database)
        await database.status(
            f"LOCK TABLE {schema}.{target_relation},{schema}.prepared_overlay IN ACCESS EXCLUSIVE MODE"
        )
        if retain:
            native = await receipts.relation(database, schema, target_relation, content=False)
            await receipts.retain(database, schema, native)
        await importer._install_provider_directory_prepared_stage(stage)
        await importer._finish_provider_directory_prepared_stage(stage)
        await importer._cutover_provider_directory_artifact_sources(fence)
        assert await database.scalar(f"SELECT marker FROM {schema}.{target_relation}") == "new"
        assert await database.scalar(f"SELECT marker FROM {schema}.{receipts.retained_name(old_oid)}") == "old"
        raise RuntimeError("rollback probe")


@pytest.mark.parametrize("retain", [False, True])
async def test_native_stage_cut_requires_retention_and_rolls_back_with_alias(monkeypatch, retain):
    async with ordinary_serving_case(monkeypatch) as case:
        database, schema = case.database, case.schema
        await _prepare(database, schema)
        target_relation = "provider_directory_address_overlay"
        await database.status(f"CREATE TABLE {schema}.prepared_overlay (LIKE {schema}.{target_relation} INCLUDING ALL)")
        await database.status(f"INSERT INTO {schema}.prepared_overlay VALUES ('new')")
        old_oid = await database.scalar(f"SELECT '{schema}.{target_relation}'::regclass::oid::bigint")

        async def rename_indexes(_schema, _stage):
            await database.status(
                f"ALTER INDEX {_schema}.{_stage}_pkey RENAME TO provider_directory_address_overlay_pkey"
            )

        stage = importer.ProviderDirectoryPreparedArtifactStage(
            schema, "prepared_overlay", target_relation, rename_indexes
        )
        async with replacement.preparation_scope(importer, _markers()) as fence:
            expected_error = (
                "rollback probe" if retain else "provider_directory_published_replacement_retained_output_required"
            )
            with pytest.raises(RuntimeError, match="^" + expected_error + "$"):
                await _swap_and_abort(database, schema, stage, fence, retain)
            assert await _alias(database, schema) == "endpoint_repoint"
            assert await database.scalar(f"SELECT marker FROM {schema}.{target_relation}") == "old"
            assert await database.scalar(f"SELECT '{schema}.{target_relation}'::regclass::oid::bigint") == old_oid
            assert await database.scalar(f"SELECT marker FROM {schema}.prepared_overlay") == "new"


@pytest.mark.parametrize("damage", ["root", "count"])
async def test_native_missing_incumbent_provenance_refuses_preparation(monkeypatch, damage):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        column, value = ("acquisition_root_run_id", "NULL") if damage == "root" else ("resource_count", "-1")
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET {column}={value} WHERE dataset_id='dataset_serving'"
        )
        with pytest.raises(RuntimeError, match="header_identity_invalid"):
            async with replacement.preparation_scope(importer, _markers()):
                pytest.fail("missing incumbent provenance entered preparation")


@pytest.mark.parametrize("entered", [False, True])
@pytest.mark.parametrize("copied", [False, True])
async def test_native_replacement_fence_cannot_publish_outside_its_scope(monkeypatch, entered, copied):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        if entered:
            async with replacement.preparation_scope(importer, _markers()) as fence:
                assert fence.datasets[0].dataset_id == "dataset_shared"
        else:
            fence = await importer._resolve_provider_directory_artifact_datasets(
                ["source_primary"], allow_published_replacement=True
            )
        if copied:
            fence = replace(fence)
        with pytest.raises(RuntimeError, match="scope_required"):
            await _cut(database, fence)
        assert await _alias(database, schema) == "endpoint_repoint"


async def test_native_prepared_admission_cannot_cross_incumbent_scopes(monkeypatch):
    from types import SimpleNamespace

    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare(database, schema)
        request = SimpleNamespace(
            metrics=_markers(), publish_artifacts_targets={"address_overlay"}, publish_corroboration=False
        )
        async with replacement.preparation_scope(importer, _markers()) as fence:
            admission = artifacts._publication_admission(importer, fence, request, frozenset({"Location"}))
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET import_run_id='new-incumbent-run' WHERE dataset_id='dataset_serving'"
        )
        async with replacement.preparation_scope(importer, _markers()):
            token = artifacts._ACTIVE.set(admission)
            try:
                with pytest.raises(RuntimeError, match="rest_artifact_admission_changed"):
                    artifacts.current(importer)
            finally:
                artifacts._ACTIVE.reset(token)


@pytest.mark.asyncio
async def test_database_insert_keeps_exact_origin_type_order_and_repaired_tuple_parity(monkeypatch):
    """One database insert preserves origin, resource type, NPI repair and complete tuples."""
    async with page_copy.native_case(monkeypatch) as case:
        prepared = page_copy.enrichment()
        statement, expected, params, kind, _counts = page_copy.copy_inputs(case, prepared)
        async with case.connection.begin():
            await artifacts._insert_projection(
                case.directory, page_copy.MODEL, "new_stage", statement, expected, params, kind, prepared
            )
            await artifacts._verify_set_parity(case.connection, expected, '"mrf"."new_stage"', params)
            rows = (
                await case.connection.execute(
                    text("SELECT source_id,resource_id,npi,names FROM mrf.new_stage ORDER BY source_id,resource_id")
                )
            ).all()
            assert [(row.source_id, row.resource_id, row.npi) for row in rows] == [
                ("source_a", str(1000000000 + row_number), 1000000000 + row_number) for row_number in range(1, 1031)
            ]
            assert all(row.names == [{"family": "dataset_a"}] for row in rows)
            assert prepared["metrics"]["resource_id_npis_backfilled"]["Practitioner"] == 1030


@pytest.mark.asyncio
async def test_database_insert_stores_explicit_json_null_as_sql_null(monkeypatch):
    """Readers expand JSON columns as arrays, so a JSON null scalar must not reach them."""
    async with page_copy.native_case(monkeypatch, rows=2) as case:
        prepared = page_copy.enrichment()
        statement, expected, params, kind, _counts = page_copy.copy_inputs(case, prepared)
        async with case.connection.begin():
            await case.connection.execute(
                text(
                    "UPDATE mrf.provider_directory_dataset_resource "
                    "SET payload_json=(payload_json::jsonb || jsonb_build_object('telecom', NULL))::json "
                    "WHERE dataset_id='dataset_a' AND resource_type='Practitioner' AND resource_id='1000000001'"
                )
            )
            await artifacts._insert_projection(
                case.directory, page_copy.MODEL, "new_stage", statement, expected, params, kind, prepared
            )
            stored = await case.connection.execute(
                text(
                    "SELECT count(*) FILTER (WHERE telecom IS NULL), count(*) FILTER (WHERE names IS NULL), "
                    "count(*) FILTER (WHERE jsonb_typeof(telecom::jsonb) = 'null') FROM mrf.new_stage"
                )
            )
            assert tuple(stored.one()) == (2, 0, 0)


@pytest.mark.asyncio
async def test_failed_database_insert_leaves_no_candidate_rows(monkeypatch):
    """A failure after the insert leaves no candidate rows committed."""
    async with page_copy.native_case(monkeypatch) as case:

        async def fail_repair_count(_statement, **_params):
            raise RuntimeError("fixture_count_interrupted")

        case.directory.db.scalar = fail_repair_count
        prepared = page_copy.enrichment()
        statement, expected, params, kind, _counts = page_copy.copy_inputs(case, prepared)
        with pytest.raises(RuntimeError, match="fixture_count_interrupted"):
            async with case.connection.begin():
                await artifacts._insert_projection(
                    case.directory, page_copy.MODEL, "new_stage", statement, expected, params, kind, prepared
                )
        assert (await case.connection.execute(text("SELECT count(*) FROM mrf.new_stage"))).scalar_one() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_hashagg", ["on", "off"])
@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "DELETE FROM mrf.parity_actual WHERE ctid IN (SELECT ctid FROM mrf.parity_actual LIMIT 1)",
        "INSERT INTO mrf.parity_actual SELECT * FROM mrf.parity_actual LIMIT 1",
        "UPDATE mrf.parity_actual SET body=jsonb_build_object('changed',true) WHERE id=2",
    ],
    ids=["equal", "missing_duplicate", "extra_duplicate", "changed_json"],
)
async def test_native_parity_sorts_and_restores(monkeypatch, initial_hashagg, mutation):
    """Keep exhaustive multiset checks on spillable sorts without leaking planner settings."""
    parity_queries = []

    def capture_parity(connection, _cursor, statement, _parameters, _context, _executemany):
        if statement.startswith("WITH expected AS MATERIALIZED"):
            assert connection.exec_driver_sql("SHOW enable_hashagg").scalar_one() == "off"
            parity_queries.append(statement)

    async with page_copy.native_case(monkeypatch, rows=0) as case, case.connection.begin():
        for statement in (
            "CREATE TABLE mrf.parity_expected AS SELECT id,jsonb_build_object('nested',"
            "jsonb_build_array(id,NULL,repeat('x',128))) AS body FROM (VALUES(1),(1),(2)) fixture(id)",
            "CREATE TABLE mrf.parity_actual AS TABLE mrf.parity_expected",
        ):
            await case.connection.execute(text(statement))
        if mutation:
            await case.connection.execute(text(mutation))
        await case.connection.execute(text("SET LOCAL enable_hashagg=" + initial_hashagg))
        event.listen(case.connection.sync_connection, "before_cursor_execute", capture_parity)
        try:
            if mutation:
                with pytest.raises(RuntimeError, match="scratch_tuple_parity_failed"):
                    await artifacts._verify_set_parity(
                        case.connection, "SELECT * FROM mrf.parity_expected", "mrf.parity_actual", {}
                    )
            else:
                await artifacts._verify_set_parity(
                    case.connection, "SELECT * FROM mrf.parity_expected", "mrf.parity_actual", {}
                )
        finally:
            event.remove(case.connection.sync_connection, "before_cursor_execute", capture_parity)
        assert (await case.connection.execute(text("SHOW enable_hashagg"))).scalar_one() == initial_hashagg
        assert len(parity_queries) == 1
        await case.connection.execute(text("SET LOCAL enable_hashagg=off"))
        plan = (await case.connection.execute(text("EXPLAIN(VERBOSE,FORMAT JSON) " + parity_queries[0]))).scalar_one()
        nodes = [plan[0]["Plan"]]
        for node in nodes:
            nodes.extend(node.get("Plans", ()))
        operations = [node for node in nodes if node["Node Type"] == "Aggregate"]
        assert len(operations) == 1 and operations[0]["Strategy"] == "Sorted"
        assert len(operations[0]["Group Key"]) == 1
        assert "sum" in operations[0]["Filter"]
        assert all(node.get("Strategy") != "Hashed" for node in nodes)
        sorts = [node for node in nodes if node["Node Type"] == "Sort"]
        assert len(sorts) == 1 and len(sorts[0]["Output"]) == 2
        assert "AS row_value, 1 AS delta" in parity_queries[0]
        assert "AS row_value, -1 AS delta" in parity_queries[0]
