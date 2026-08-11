# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed contracts for generic numeric-grid address aliases."""

from __future__ import annotations

import contextlib
import importlib
import importlib.util
import inspect
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from process.address_numeric_grid_alias import (
    _mark_failed,
    _normalize_scope,
    _reviewed_digest,
    _reviewer,
    _statement_timeout,
)
from process.address_numeric_grid_alias_support import NumericGridAliasResult
from process.address_numeric_grid_alias_revoke import _reason, _uuid
from process.address_strict_source_backfill import (
    StrictSourceBackfillResult,
    _target_limit,
)
from process import address_numeric_grid_alias_worker
from process.ext import (
    address_alias_sql,
    address_canon,
    address_strict_source_backfill_sql,
)


provider_directory = importlib.import_module("process.provider_directory_fhir")
entity_address = importlib.import_module("process.entity_address_unified")


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260811100000_address_numeric_grid_alias.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "address_numeric_grid_alias_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _Recorder:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))


def test_numeric_grid_parser_is_generic_and_structural():
    incomplete = address_canon.numeric_grid_parts_v1(
        "1548 E 4500",
        "Suite 202",
    )
    complete = address_canon.numeric_grid_parts_v1(
        "1548 E 4500 S",
        "STE 202",
    )

    assert incomplete == address_canon.NumericGridParts("1548", "e", "4500", "")
    assert complete == address_canon.NumericGridParts("1548", "e", "4500", "s")
    assert address_canon.numeric_grid_parts_v1("1548 East Main Street", None) is None
    assert address_canon.numeric_grid_parts_v1("1548 E 4500 S Extra", None) is None
    assert address_canon.numeric_grid_parts_v1("123-125 N", None) is None
    assert address_canon.numeric_grid_parts_v1("123/125 N", None) is None


def test_candidate_sql_requires_exact_unit_and_geography_and_counts_ambiguity_first():
    sql = address_alias_sql.numeric_grid_candidate_insert_sql(
        schema="mrf",
        archive='"mrf"."address_archive_v2"',
    )

    assert "target.unit_norm = source.unit_norm" in sql
    assert "target.state_code = source.state_code" in sql
    assert "target.zip5 = source.zip5" in sql
    assert "target.country_code = source.country_code" in sql
    assert "count(DISTINCT target_address_key)" in sql
    assert "WHEN counts.candidate_count <> 1 THEN 'ambiguous'" in sql
    assert "target_strict_source_count < 2" in sql
    assert "retried.shadow_run_id = CAST(:retry_shadow_run_id AS uuid)" in sql
    assert "similarity(" not in sql.lower()
    assert "levenshtein" not in sql.lower()


@pytest.mark.parametrize("mode", ["off", "shadow", "apply", " SHADOW "])
def test_alias_mode_accepts_only_controlled_values(mode):
    assert address_alias_sql.numeric_grid_alias_mode(mode) == mode.strip().lower()


def test_alias_scope_and_apply_inputs_fail_closed():
    assert _normalize_scope("tx", "787") == ("TX", "787")
    assert _statement_timeout("15s") == "15s"
    with pytest.raises(ValueError, match="state_code"):
        _normalize_scope("Texas", None)
    with pytest.raises(ValueError, match="zip_prefix"):
        _normalize_scope(None, "84A")
    with pytest.raises(ValueError, match="SHA-256"):
        _reviewed_digest("not-reviewed")
    with pytest.raises(ValueError, match="reviewed_by"):
        _reviewer(" ")
    assert _target_limit(256) == 256
    with pytest.raises(ValueError, match="max_targets"):
        _target_limit(0)
    with pytest.raises(ValueError, match="max_targets"):
        _target_limit(10_001)
    assert _uuid("00000000-0000-0000-0000-000000000001", name="source") == (
        "00000000-0000-0000-0000-000000000001"
    )
    assert _reason(" reviewed rollback ") == "reviewed rollback"
    with pytest.raises(ValueError, match="valid UUID"):
        _uuid("not-a-key", name="source")
    with pytest.raises(ValueError, match="non-empty reason"):
        _reason(" ")


def test_strict_source_registry_uses_exact_source_rows_only():
    projections = address_strict_source_backfill_sql.SOURCE_PROJECTIONS
    tables = {projection.table for projection in projections}

    assert "mrf_address_evidence" in tables
    assert "mrf_address" not in tables
    assert "facility_anchor" not in tables
    assert "entity_address_unified" not in tables
    assert "openaddresses_geocode" not in tables
    assert {projection.source_bit for projection in projections} == {
        1,
        2,
        4,
        16,
        32,
        128,
    }
    assert "partd_pharmacy_activity_v2" not in tables
    assert 128 == next(
        projection.source_bit
        for projection in projections
        if projection.table == "provider_directory_address_overlay"
    )


def test_strict_source_evidence_recomputes_key_and_identity_after_index_prefilter():
    projection = next(
        projection
        for projection in address_strict_source_backfill_sql.SOURCE_PROJECTIONS
        if projection.table == "provider_directory_address_overlay"
    )
    sql = address_strict_source_backfill_sql.evidence_insert_sql(
        schema="mrf",
        projection=projection,
    )

    assert "source.address_key = target.address_key" in sql
    assert '"mrf".addr_key_v1(' in sql
    assert '"mrf".addr_identity_key_v1(' in sql
    assert "= target.identity_key" in sql
    assert "source_bits" not in sql
    assert "similarity(" not in sql.lower()
    assert "levenshtein" not in sql.lower()


def test_migration_creates_durable_review_and_generation_contract(monkeypatch):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "alias_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()
    normalized = " ".join(" ".join(sql.split()) for sql in recorder.statements)
    alias_statements = migration._split_sql_statements(
        migration._alias_schema_sql("alias_contract")
    )

    assert migration.revision == "20260811100000_address_numeric_grid_alias"
    assert migration.down_revision == (
        "20260811030000_fhir_formulary_source_acquisition_lease"
    )
    assert "addr_numeric_grid_parts_v1" in normalized
    assert "ADD COLUMN strict_source_bits integer NOT NULL DEFAULT 0" in normalized
    assert "CREATE TABLE \"alias_contract\".\"address_alias_state_v1\"" in normalized
    assert "CREATE TABLE \"alias_contract\".\"address_alias_artifact_state_v1\"" in normalized
    assert "CREATE TABLE \"alias_contract\".\"address_alias_run_v1\"" in normalized
    assert "CREATE TABLE \"alias_contract\".\"address_alias_candidate_v1\"" in normalized
    assert "CREATE TABLE \"alias_contract\".\"address_alias_v1\"" in normalized
    assert "mode IN ('shadow', 'backfill', 'apply', 'revoke')" in normalized
    assert "'backfilled'" in normalized
    assert "'revoked'" in normalized
    assert "evidence_digest varchar(64)" in normalized
    assert "address_observed_in_source boolean" in normalized
    assert "revoke_run_id uuid" in normalized
    assert "WHERE revoked_at IS NULL" in normalized
    assert "target_strict_source_count >= 2" in normalized
    assert "address_alias_v1_generation_insert_trg" in normalized
    assert "address_alias_v1_generation_update_trg" in normalized
    assert "address_alias_v1_generation_delete_trg" in normalized
    assert "address_alias_v1_immutable_trg" in normalized
    assert "address_alias_candidate_v1_guard_trg" in normalized
    assert "address_alias_run_v1_status_guard_trg" in normalized
    assert "candidates may only be inserted into a running run" in normalized
    assert "sealed address alias candidate evidence is immutable" in normalized
    assert "terminal address alias run evidence is immutable" in normalized
    assert "address alias run audit rows are immutable" in normalized
    assert "pg_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'))" in normalized
    assert len(alias_statements) > 10
    assert recorder.statements[1:] == list(alias_statements)
    assert all(statement.rstrip().endswith(";") for statement in alias_statements)


def test_request_path_never_joins_the_offline_alias_table():
    request_module = (
        Path(__file__).resolve().parents[1] / "api" / "endpoint" / "npi.py"
    ).read_text(encoding="utf-8")

    assert "address_alias_v1" not in request_module


@pytest.mark.asyncio
async def test_failure_marker_preserves_a_terminal_run(monkeypatch):
    first = AsyncMock(return_value=Mock(status="applied"))
    monkeypatch.setattr(
        importlib.import_module("process.address_numeric_grid_alias").db,
        "first",
        first,
    )

    status = await _mark_failed(
        "mrf",
        "00000000-0000-0000-0000-000000000001",
        RuntimeError("ambiguous client result"),
    )

    assert status == "applied"
    assert "AND status = 'running'" in first.await_args.args[0]


def test_alias_generation_lock_precedes_every_materialization_fence():
    single_cutover = inspect.getsource(
        provider_directory._promote_provider_directory_artifact_stage_transaction
    )
    bundled_cutover = inspect.getsource(
        provider_directory._promote_provider_directory_artifact_bundle_transaction
    )
    resolver = inspect.getsource(address_canon._apply_persisted_numeric_grid_aliases)
    entity_cutover = inspect.getsource(entity_address._run_entity_address_cutover)

    assert single_cutover.index("alias_advisory_xact_lock_sql") < single_cutover.index(
        "_assert_provider_directory_artifact_build_fence"
    )
    assert bundled_cutover.index("alias_advisory_xact_lock_sql") < bundled_cutover.index(
        "_apply_locked_provider_directory_artifact_bundle"
    )
    assert resolver.index("alias_advisory_xact_lock_sql") < resolver.index(
        "active_alias_generation_sql"
    )
    assert entity_cutover.index("alias_advisory_xact_lock_sql") < entity_cutover.index(
        "_assert_provider_directory_overlay_alias_fence"
    ) < entity_cutover.index("_acquire_cutover_locks")


def test_apply_waits_for_archive_writers_before_reading_reviewed_evidence():
    source = inspect.getsource(
        importlib.import_module(
            "process.address_numeric_grid_alias"
        ).run_numeric_grid_alias
    )

    assert 'isolation = "READ COMMITTED" if operation == "apply"' in source
    assert source.index("_archive_lock_key") < source.index(
        "alias_advisory_xact_lock_sql"
    ) < source.index("LOCK TABLE") < source.index("reviewed shadow changed before apply")


def test_shadow_review_attribution_is_written_only_on_new_approvals():
    source = inspect.getsource(
        importlib.import_module(
            "process.address_numeric_grid_alias"
        )._approve_shadow_candidates
    )

    assert "WITH approved AS" in source
    assert "reviewed_by IS NULL" in source
    assert "EXISTS (SELECT 1 FROM approved)" in source


@pytest.mark.asyncio
async def test_unified_overlay_generation_and_relation_fence_fail_closed(monkeypatch):
    selected_overlay = [
        "SELECT * FROM mrf.provider_directory_address_overlay AS overlay"
    ]
    read_fence = AsyncMock(return_value=(1, 11))
    monkeypatch.setattr(
        entity_address,
        "_provider_directory_overlay_alias_fence",
        read_fence,
    )
    context = {"address_alias_generation": 2}

    with pytest.raises(RuntimeError, match="stale address alias generation"):
        await entity_address._capture_provider_directory_overlay_alias_fence(
            "mrf",
            selected_overlay,
            context,
        )

    read_fence.return_value = (2, 11)
    await entity_address._capture_provider_directory_overlay_alias_fence(
        "mrf",
        selected_overlay,
        context,
    )
    assert context["provider_directory_overlay_relation_oid"] == 11

    read_fence.return_value = (2, 12)
    with pytest.raises(RuntimeError, match="overlay changed"):
        await entity_address._assert_provider_directory_overlay_alias_fence(
            "mrf",
            context,
        )


@pytest.mark.asyncio
async def test_unified_without_selected_overlay_does_not_require_receipt(monkeypatch):
    read_fence = AsyncMock()
    monkeypatch.setattr(
        entity_address,
        "_provider_directory_overlay_alias_fence",
        read_fence,
    )
    context = {"address_alias_generation": 2}

    await entity_address._capture_provider_directory_overlay_alias_fence(
        "mrf",
        ["SELECT * FROM mrf.npi_address"],
        context,
    )

    read_fence.assert_not_awaited()
    assert "provider_directory_overlay_relation_oid" not in context


@pytest.mark.asyncio
async def test_overlay_requires_full_rebuild_after_alias_generation_change(monkeypatch):
    @contextlib.asynccontextmanager
    async def build_guard(*_args, **_kwargs):
        yield provider_directory.ProviderDirectoryArtifactBuildFence(target_oid=11)

    build = AsyncMock()
    monkeypatch.setattr(
        provider_directory,
        "_address_overlay_missing_requirement",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        provider_directory,
        "_ensure_provider_directory_address_overlay_table",
        AsyncMock(),
    )
    monkeypatch.setattr(
        provider_directory,
        "_address_overlay_scope_sources",
        AsyncMock(return_value=["synthetic-source"]),
    )
    monkeypatch.setattr(
        provider_directory,
        "_provider_directory_artifact_build_guard",
        build_guard,
    )
    monkeypatch.setattr(
        provider_directory,
        "_address_alias_generation",
        AsyncMock(return_value=2),
    )
    monkeypatch.setattr(
        provider_directory,
        "_address_alias_artifact_generation",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory,
        "_build_provider_directory_address_overlay_stage",
        build,
    )

    with pytest.raises(RuntimeError, match="full Provider Directory"):
        await provider_directory.publish_provider_directory_address_overlay(
            "mrf",
            run_id="synthetic-run",
            source_ids=["synthetic-source"],
        )
    build.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlay_full_rebuild_can_advance_alias_generation_receipt(monkeypatch):
    @contextlib.asynccontextmanager
    async def build_guard(*_args, **_kwargs):
        yield provider_directory.ProviderDirectoryArtifactBuildFence(target_oid=11)

    build = AsyncMock(return_value={"published": True})
    monkeypatch.setattr(
        provider_directory,
        "_address_overlay_missing_requirement",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        provider_directory,
        "_ensure_provider_directory_address_overlay_table",
        AsyncMock(),
    )
    monkeypatch.setattr(
        provider_directory,
        "_address_overlay_scope_sources",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        provider_directory,
        "_provider_directory_artifact_build_guard",
        build_guard,
    )
    monkeypatch.setattr(
        provider_directory,
        "_address_alias_generation",
        AsyncMock(return_value=2),
    )
    monkeypatch.setattr(
        provider_directory,
        "_address_alias_artifact_generation",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        provider_directory,
        "_build_provider_directory_address_overlay_stage",
        build,
    )

    result = await provider_directory.publish_provider_directory_address_overlay("mrf")

    assert result == {"published": True}
    assert build.await_args.args[5].alias_generation == 2


@pytest.mark.asyncio
async def test_control_worker_forwards_reviewed_alias_inputs_and_progress(monkeypatch):
    result = NumericGridAliasResult(
        run_id="00000000-0000-0000-0000-000000000001",
        mode="shadow",
        status="sealed",
        candidate_digest="a" * 64,
        source_count=3,
        candidate_sources=2,
        candidate_rows=2,
        no_candidate=1,
        active_skipped=0,
        eligible=1,
        ambiguous=1,
        insufficient_provenance=0,
        promoted=0,
        generation=7,
        sample_rows=[],
    )
    run = AsyncMock(return_value=result)
    cancel = AsyncMock()
    progress = Mock()
    monkeypatch.setattr(address_numeric_grid_alias_worker, "run_numeric_grid_alias", run)
    monkeypatch.setattr(address_numeric_grid_alias_worker, "raise_if_cancelled", cancel)
    monkeypatch.setattr(address_numeric_grid_alias_worker, "enqueue_live_progress", progress)

    payload = await address_numeric_grid_alias_worker.process_data(
        {"worker": "synthetic"},
        {
            "mode": "shadow",
            "state_code": "UT",
            "zip_prefix": "84",
            "sample_limit": 5,
            "timeout": "30s",
        },
    )

    assert payload == result.__dict__
    assert run.await_args.kwargs["state_code"] == "UT"
    assert run.await_args.kwargs["zip_prefix"] == "84"
    assert run.await_args.kwargs["sample_limit"] == 5
    assert run.await_args.kwargs["timeout"] == "30s"
    await run.await_args.kwargs["cancel_check"]()
    cancel.assert_awaited_once_with(
        {"worker": "synthetic"},
        {
            "mode": "shadow",
            "state_code": "UT",
            "zip_prefix": "84",
            "sample_limit": 5,
            "timeout": "30s",
        },
    )
    assert progress.call_args.kwargs["run_id"] == result.run_id
    assert progress.call_args.kwargs["status"] == "succeeded"
    assert progress.call_args.kwargs["source_count"] == 3


@pytest.mark.asyncio
async def test_control_worker_forwards_strict_source_backfill_inputs(monkeypatch):
    result = StrictSourceBackfillResult(
        run_id="00000000-0000-0000-0000-000000000002",
        status="backfilled",
        reviewed_shadow_run_id="00000000-0000-0000-0000-000000000001",
        reviewed_candidate_digest="b" * 64,
        evidence_digest="c" * 64,
        target_count=2,
        evidence_target_count=1,
        evidence_pair_count=2,
        updated_target_count=1,
        source_target_counts={"nppes": 1, "provider_directory_overlay": 1},
        missing_relations=[],
        generation=4,
    )
    run = AsyncMock(return_value=result)
    progress = Mock()
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "run_strict_source_backfill",
        run,
    )
    monkeypatch.setattr(address_numeric_grid_alias_worker, "enqueue_live_progress", progress)

    payload = await address_numeric_grid_alias_worker.process_address_strict_source_backfill(
        {},
        {
            "alias_run_id": result.reviewed_shadow_run_id,
            "expected_candidate_sha256": result.reviewed_candidate_digest,
            "reviewed_by": "synthetic-reviewer",
            "max_targets": 12,
            "timeout": "45s",
        },
    )

    assert payload == result.__dict__
    assert run.await_args.kwargs["alias_run_id"] == result.reviewed_shadow_run_id
    assert run.await_args.kwargs["max_targets"] == 12
    assert run.await_args.kwargs["timeout"] == "45s"
    assert "schema" not in run.await_args.kwargs
    assert progress.call_args.kwargs["source_target_counts"] == {
        "nppes": 1,
        "provider_directory_overlay": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("max_targets", (0, 10_001))
async def test_strict_backfill_worker_rejects_out_of_range_mutation_cap(
    monkeypatch,
    max_targets,
):
    run = AsyncMock()
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "run_strict_source_backfill",
        run,
    )

    with pytest.raises(ValueError, match="max_targets"):
        await address_numeric_grid_alias_worker.process_address_strict_source_backfill(
            {},
            {
                "alias_run_id": "00000000-0000-0000-0000-000000000001",
                "expected_candidate_sha256": "a" * 64,
                "reviewed_by": "synthetic-reviewer",
                "max_targets": max_targets,
            },
        )

    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_enqueued_strict_backfill_validates_cap_before_queueing(monkeypatch):
    create_pool = AsyncMock()
    monkeypatch.setattr(address_numeric_grid_alias_worker, "create_pool", create_pool)

    with pytest.raises(ValueError, match="max_targets"):
        await address_numeric_grid_alias_worker.run_address_strict_source_backfill_command(
            alias_run_id="00000000-0000-0000-0000-000000000001",
            expected_candidate_sha256="a" * 64,
            reviewed_by="synthetic-reviewer",
            max_targets=0,
            enqueue=True,
        )

    create_pool.assert_not_awaited()
