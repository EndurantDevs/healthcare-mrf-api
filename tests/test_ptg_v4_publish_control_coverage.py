from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publication
from process.ptg_parts import ptg2_v4_stale_metadata_fence as fence
from process.ptg_parts import ptg2_v4_stale_metadata_plan as stale_plan
from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as reconcile
from process.ptg_parts import source_pointers
from process.ptg_parts.ptg2_shared_reuse import SharedLogicalPlanScope
from process.ptg_parts.ptg2_v4_attempt_registry import AttemptAttachment
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2V4StaleMetadataConflict,
)
from tests.ptg_v4_publish_control_support import (
    QueryResult,
    ScriptedSession,
    SourcePublicationSession,
    TransactionDatabase,
    active_stale_context,
    assignment,
    prepared_layout_arguments,
    source_row,
)


def _patch_publication_pipeline(monkeypatch, *, publish_result: object) -> object:
    prepared = object()
    database = TransactionDatabase(object())
    monkeypatch.setattr(publication, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(publication.db, "transaction", database.transaction)
    monkeypatch.setattr(publication, "touch_shared_layout_build", AsyncMock())
    monkeypatch.setattr(publication, "touch_v4_shared_layout_build", AsyncMock())
    monkeypatch.setattr(
        publication,
        "_prepare_price_with_early_finalizer",
        AsyncMock(return_value=(prepared, 0.25, None, None)),
    )
    monkeypatch.setattr(
        publication,
        "_publish_prepared_shared_layout",
        AsyncMock(return_value=publish_result),
    )
    monkeypatch.setattr(
        publication,
        "cleanup_prepared_shared_price_artifacts",
        AsyncMock(),
    )
    return prepared


async def _publish_layout(**overrides):
    layout_arguments_by_name = {
        "schema_name": "mrf",
        "manifest_stage_table": "manifest_stage",
        "reserved_snapshot_key": 7,
        "build_token": "token",
        "expected_coverage_scope_id": b"c" * 32,
        "logical_snapshot_id": "snapshot",
        "expected_source_identities": (),
        "serving_run_entries": (),
        "code_dictionary_entries": (),
        "provider_set_metadata_entries": (),
        "source_audit_witness_entries": (),
        "expected_raw_source_sha256": (),
        "graph_artifact_entries": (),
        "provider_identifier_quarantine": {},
    }
    layout_arguments_by_name.update(overrides)
    return await publication.publish_strict_shared_v3_layout(
        **layout_arguments_by_name
    )


@pytest.mark.asyncio
async def test_publication_normalizes_v3_and_v4_evidence(monkeypatch) -> None:
    """Forward authenticated V4 byte evidence while keeping V3 evidence optional."""

    _patch_publication_pipeline(monkeypatch, publish_result="published")
    assert await _publish_layout() == "published"
    raw_hash = "a" * 64
    assert await _publish_layout(
        provider_graph_v4=True,
        expected_raw_source_sha256=(raw_hash,),
        compressed_acquisition_entries=(
            {"raw_sha256": raw_hash, "byte_count": "41"},
            {"raw_sha256": raw_hash, "byte_count": 41},
        ),
        empty_npi_tin_only_normalization_count=0,
    ) == "published"
    forwarded = publication._publish_prepared_shared_layout.await_args.kwargs
    assert forwarded["compressed_acquisition_bytes"] == 41
    assert forwarded["empty_npi_tin_only_normalization_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entries", "expected_hashes", "normalization", "message"),
    (
        ((), (), None, "normalization evidence"),
        (({"raw_sha256": "a" * 64, "byte_count": "bad"},), ("a" * 64,), 0, "entry"),
        (({"raw_sha256": "z" * 64, "byte_count": 1},), ("z" * 64,), 0, "entry"),
        (
            (
                {"raw_sha256": "a" * 64, "byte_count": 1},
                {"raw_sha256": "a" * 64, "byte_count": 2},
            ),
            ("a" * 64,),
            0,
            "entry",
        ),
        (({"raw_sha256": "a" * 64, "byte_count": 1},), ("b" * 64,), 0, "source hashes"),
    ),
)
async def test_publication_rejects_malformed_resource_evidence(
    monkeypatch,
    entries,
    expected_hashes,
    normalization,
    message,
) -> None:
    """Fail before preparation when acquisition proof is incomplete or ambiguous."""

    _patch_publication_pipeline(monkeypatch, publish_result="unused")
    with pytest.raises(RuntimeError, match=message):
        await _publish_layout(
            provider_graph_v4=True,
            expected_raw_source_sha256=expected_hashes,
            compressed_acquisition_entries=entries,
            empty_npi_tin_only_normalization_count=normalization,
        )
    publication._prepare_price_with_early_finalizer.assert_not_awaited()


@pytest.mark.asyncio
async def test_publication_cleans_prepared_price_on_failure(monkeypatch) -> None:
    """Clean prepared files after a downstream publication failure."""

    prepared = _patch_publication_pipeline(monkeypatch, publish_result=None)
    publication._publish_prepared_shared_layout.side_effect = RuntimeError("audit failed")
    with pytest.raises(RuntimeError, match="audit failed"):
        await _publish_layout()
    cleanup = publication.cleanup_prepared_shared_price_artifacts
    cleanup.assert_awaited_once_with(prepared)


@pytest.mark.asyncio
async def test_prepared_layout_guards_schema_and_v4_resource_proof(monkeypatch) -> None:
    """Reject wrong schemas and unauthenticated V4 inputs before touching the build."""

    monkeypatch.setattr(publication, "resolve_ptg2_schema", lambda: "mrf")
    with pytest.raises(RuntimeError, match="resource evidence"):
        await publication._publish_prepared_shared_layout(
            **prepared_layout_arguments(provider_graph_v4=True)
        )
    with pytest.raises(RuntimeError, match="configured PostgreSQL schema"):
        await publication._publish_prepared_shared_layout(
            **prepared_layout_arguments(schema_name="other")
        )


def _source_session(snapshot_id: str, source_assignment, *, plans=None):
    expected_plans = plans or [{"plan_id": "plan", "plan_market_type": "group"}]
    return SourcePublicationSession(
        scope={
            "plan_id": "plan",
            "plan_market_type": "group",
            "coverage_scope_id": b"c" * 32,
        },
        plans=expected_plans,
        sources=[source_row(snapshot_id, source_assignment)],
    )


@pytest.mark.asyncio
async def test_source_publication_locks_and_preserves_deferred_evidence(
    monkeypatch,
) -> None:
    """Lock the attempt and persist deferred logical-hash evidence unchanged."""

    deferred_assignment = assignment(logical_hash=None, deferred=True)
    session = _source_session("snapshot", deferred_assignment)
    database = TransactionDatabase(session)
    lifecycle_lock = AsyncMock()
    writable_lock = AsyncMock()
    monkeypatch.setattr(publication.db, "transaction", database.transaction)
    monkeypatch.setattr(publication, "acquire_ptg2_lifecycle_lock", lifecycle_lock)
    monkeypatch.setattr(publication, "lock_writable_snapshot", writable_lock)
    rows = await publication.publish_shared_v3_snapshot_sources(
        schema_name="mrf",
        snapshot_id="snapshot",
        plan_scopes=(
            SharedLogicalPlanScope("plan", "EIN", "GROUP"),
            SharedLogicalPlanScope("plan", "ein", "group"),
        ),
        coverage_scope_id=b"c" * 32,
        assignments=(deferred_assignment,),
    )
    assert rows[0]["logical_json_sha256"] is None
    assert rows[0]["logical_hash_deferred"] is True
    lifecycle_lock.assert_awaited_once_with(session)
    assert writable_lock.await_args.kwargs["snapshot_id"] == "snapshot"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("empty_plans", "requires logical plans"),
        ("scope", "immutable source scope"),
        ("plans", "stale logical plan mappings"),
        ("sources", "conflicting source-key mapping"),
        ("source_set", "conflicting logical source-set seal"),
    ),
)
async def test_source_publication_fails_closed_on_conflicting_state(
    monkeypatch,
    mutation,
    message,
) -> None:
    """Reject every immutable source publication conflict without repair."""

    source_assignment = assignment()
    session = _source_session("snapshot", source_assignment)
    plan_scopes = [SharedLogicalPlanScope("plan", "ein", "group")]
    if mutation == "empty_plans":
        plan_scopes = []
    elif mutation == "scope":
        session.scope["plan_id"] = "other"
    elif mutation == "plans":
        session.plans = [{"plan_id": "other", "plan_market_type": "group"}]
    elif mutation == "sources":
        session.sources[0]["source_trace_set_hash"] = "e" * 64
    else:
        session.persisted_source_set = {
            "contract": "other",
            "source_count": 1,
            "raw_container_sha256_digest": "f" * 64,
        }
    database = TransactionDatabase(session)
    monkeypatch.setattr(publication.db, "transaction", database.transaction)
    monkeypatch.setattr(publication, "acquire_ptg2_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(publication, "lock_writable_snapshot", AsyncMock())
    with pytest.raises((ValueError, RuntimeError), match=message):
        await publication.publish_shared_v3_snapshot_sources(
            schema_name="mrf",
            snapshot_id="snapshot",
            plan_scopes=plan_scopes,
            coverage_scope_id=b"c" * 32,
            assignments=(source_assignment,),
        )


@pytest.mark.asyncio
async def test_source_pointer_cas_covers_noop_success_and_conflict() -> None:
    """Accept driver no-result and returned rows but reject an empty CAS result."""

    cas_arguments_by_name = {
        "schema_name": "mrf",
        "source_key": "source",
        "snapshot_id": "new",
        "previous_snapshot_id": "old",
        "import_month": dt.date(2026, 7, 1),
        "updated_at": dt.datetime(2026, 7, 24),
    }
    for session in (
        ScriptedSession(QueryResult(rows=())),
        ScriptedSession(QueryResult(rows=({"snapshot_id": "new"},))),
    ):
        if session.results[0].rows:
            await source_pointers._compare_and_swap_source_pointer(
                session,
                **cas_arguments_by_name,
            )
        else:
            with pytest.raises(source_pointers.PTG2SourcePointerConflict):
                await source_pointers._compare_and_swap_source_pointer(
                    session,
                    **cas_arguments_by_name,
                )
    assert source_pointers._has_result_row(None) is True


@pytest.mark.asyncio
async def test_source_pointer_wrappers_lock_before_activation(monkeypatch) -> None:
    """Resolve the configured schema and fence the snapshot before activation."""

    session = object()
    database = TransactionDatabase(session)
    writable_lock = AsyncMock()
    activation = AsyncMock(return_value={"status": "promoted"})
    monkeypatch.setattr(source_pointers, "resolve_ptg2_schema", lambda: "tenant")
    monkeypatch.setattr(source_pointers.db, "transaction", database.transaction)
    monkeypatch.setattr(source_pointers, "_acquire_source_pointer_gc_lock", AsyncMock())
    monkeypatch.setattr(source_pointers, "lock_writable_snapshot", writable_lock)
    monkeypatch.setattr(
        source_pointers,
        "_activate_ptg2_source_candidate_in_transaction",
        activation,
    )
    result = await source_pointers.activate_ptg2_source_candidate(
        source_key=" Source ",
        snapshot_id=" Snapshot ",
    )
    assert result == {"status": "promoted"}
    assert writable_lock.await_args.kwargs == {
        "schema_name": "tenant",
        "snapshot_id": "Snapshot",
    }
    assert activation.await_args.kwargs["source_key"] == "source"


@pytest.mark.asyncio
async def test_attempt_fence_translates_guards_and_supports_status_sessions() -> None:
    """Use either session API and translate only durable fence failures."""

    status_session = SimpleNamespace(status=AsyncMock())
    await fence.lock_writable_snapshot(
        status_session,
        None,
        schema_name="mrf",
        snapshot_id="snapshot",
        internal_run_id="run",
        allow_reconciled=True,
    )
    assert status_session.status.await_args.kwargs["allow_reconciled"] is True
    rejected = SimpleNamespace(
        execute=AsyncMock(side_effect=RuntimeError("PTG2_ATTEMPT_FENCE_RECONCILED"))
    )
    with pytest.raises(fence.StaleMetadataFenceError):
        await fence.lock_writable_snapshot(
            rejected,
            None,
            schema_name="mrf",
            snapshot_id="snapshot",
        )
    with pytest.raises(TypeError, match="cannot execute"):
        await fence._execute_statement(object(), "SELECT 1", {})


@pytest.mark.asyncio
async def test_attempt_row_guard_deduplicates_coordinates(monkeypatch) -> None:
    """Fence each distinct snapshot/run coordinate and reject missing coordinates."""

    attachment = AttemptAttachment(
        "joined",
        "joined_table",
        ("snapshot_id", "previous_snapshot_id"),
        ("run_id",),
    )
    monkeypatch.setattr(
        fence,
        "attempt_attachment_for_table",
        lambda _table_name: attachment,
    )
    lock = AsyncMock()
    monkeypatch.setattr(fence, "lock_writable_snapshot", lock)
    await fence.guard_attempt_rows(
        object(),
        None,
        schema_name="mrf",
        table_name="joined_table",
        attempt_rows=(
            {"snapshot_id": "b", "previous_snapshot_id": "a", "run_id": "run"},
            {"snapshot_id": "a", "run_id": "run"},
        ),
    )
    assert [call.kwargs["snapshot_id"] for call in lock.await_args_list] == ["a", "b"]
    with pytest.raises(ValueError, match="omitted"):
        await fence.guard_attempt_rows(
            object(),
            None,
            schema_name="mrf",
            table_name="joined_table",
            attempt_rows=({},),
        )


def test_stale_plan_collects_all_fail_closed_reasons() -> None:
    """Report missing rows and every unsafe state on a present attempt."""

    missing = active_stale_context(
        snapshot_by_field=None,
        internal_run_by_field=None,
    )
    assert stale_plan._reason_codes(
        missing,
        internal_run_id="run",
        stale_after_seconds=300,
        target_digest="a" * 64,
    ) == ("snapshot_not_found", "internal_run_not_found")
    unsafe = active_stale_context(attachment_count_by_name={"artifact": 1})
    snapshot_by_field = dict(unsafe.snapshot_by_field)
    internal_run_by_field = dict(unsafe.internal_run_by_field)
    snapshot_by_field.update(
        import_run_id="other",
        status="published",
        validated_at=unsafe.observed_at,
        published_at=unsafe.observed_at,
        manifest=[],
    )
    internal_run_by_field.update(
        status="finished",
        finished_at=unsafe.observed_at,
        error="failed",
        options={},
        report={"physical_layout": True},
    )
    context = active_stale_context(
        snapshot_by_field=snapshot_by_field,
        internal_run_by_field=internal_run_by_field,
        attempt_fence_by_field={"state": "reconciled"},
        attachment_count_by_name={"artifact": 1},
    )
    reasons = stale_plan._reason_codes(
        context,
        internal_run_id="run",
        stale_after_seconds=300,
        target_digest="a" * 64,
    )
    assert reasons == (
        "snapshot_run_pair_mismatch",
        "snapshot_not_building",
        "snapshot_validation_evidence_present",
        "snapshot_publication_evidence_present",
        "internal_run_not_active",
        "internal_run_finished_at_present",
        "internal_run_error_present",
        "internal_run_not_v4",
        "attempt_fence_not_active",
        "snapshot_manifest_not_object",
        "physical_layout_or_recovery_report_present",
        "attached_state_present",
    )


def test_reconcile_validates_policy_and_reviewed_state(monkeypatch) -> None:
    """Normalize control inputs and reject stale, ineligible, or changed reviews."""

    assert reconcile._normalized_identifier(" snap ", field_name="snapshot_id") == "snap"
    with pytest.raises(ValueError, match="PTG identifier"):
        reconcile._normalized_identifier("/", field_name="snapshot_id")
    monkeypatch.delenv(reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV, raising=False)
    assert reconcile._server_stale_after_seconds() > 300
    monkeypatch.setenv(reconcile.PTG2_V4_STALE_METADATA_SECONDS_ENV, "bad")
    with pytest.raises(RuntimeError, match="integer"):
        reconcile._server_stale_after_seconds()
    request = SimpleNamespace(expected_plan_digest="a" * 64, target_digest="b" * 64)
    with pytest.raises(PTG2V4StaleMetadataConflict, match="not eligible"):
        reconcile._reviewed_marker(
            active_stale_context(),
            request,
            {"status": "ineligible", "reason_codes": ["unsafe"]},
        )
    with pytest.raises(PTG2V4StaleMetadataConflict, match="state changed"):
        reconcile._reviewed_marker(
            active_stale_context(),
            request,
            {"status": "ready", "plan_digest": "c" * 64, "reason_codes": []},
        )
