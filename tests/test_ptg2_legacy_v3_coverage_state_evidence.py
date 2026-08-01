"""Edge contracts for reconciled state, attachment evidence, and fences."""

from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    LEGACY_V3_RECONCILE_CONTRACT,
)
from process.ptg_parts import ptg2_legacy_v3_metadata_evidence as evidence
from process.ptg_parts import ptg2_legacy_v3_reconciled_state as state
from process.ptg_parts import ptg2_v4_stale_metadata_fence as fence
from process.ptg_parts.ptg2_v4_attempt_registry import AttemptAttachment
from process.ptg_parts.ptg2_v4_stale_metadata_types import (
    PTG2_V4_STALE_METADATA_MARKER,
)
from process.ptg_parts.ptg_source_attempt_guard import canonical_digest


_SNAPSHOT_ID = "ptg2:202607:state-evidence"
_INTERNAL_RUN_ID = "ptg2:state-evidence-run"
_OUTER_RUN_ID = "run_state_evidence"
_SOURCE_IMPORT_ID = "source-import-state-evidence"


class _QueryResult:
    def __init__(
        self,
        *,
        scalar=None,
        rows=(),
        mapping=None,
    ) -> None:
        self._scalar = scalar
        self._rows = list(rows)
        self._mapping = mapping

    def scalar_one(self):
        return self._scalar

    def all(self):
        return self._rows

    def mappings(self):
        return self

    def one_or_none(self):
        return self._mapping


class _Session:
    def __init__(self, *responses: _QueryResult) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, object]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        return self.responses.pop(0)


def _audit_marker() -> dict[str, object]:
    return {
        "plan_digest": "a" * 64,
        "attachment_digest": "b" * 64,
        "catalog_digest": "c" * 64,
        "event_high_water_mark": 7,
        "retained_state_digest": "d" * 64,
        "preserved_row_digest": "e" * 64,
    }


def _audit_payload(marker_by_name: dict[str, object]) -> dict[str, object]:
    return {
        "contract": LEGACY_V3_RECONCILE_CONTRACT,
        "snapshot_id": _SNAPSHOT_ID,
        "internal_run_id": _INTERNAL_RUN_ID,
        "outer_run_id": _OUTER_RUN_ID,
        "target_digest": "f" * 64,
        "source_file_import_id": _SOURCE_IMPORT_ID,
        "plan_digest": marker_by_name["plan_digest"],
        "reconciliation_id": canonical_digest(marker_by_name),
        "attachment_digest": marker_by_name["attachment_digest"],
        "catalog_digest": marker_by_name["catalog_digest"],
        "event_high_water_mark": 7,
        "marker": marker_by_name,
    }


def _terminal_observation(
    marker_by_name: dict[str, object],
    audit_payload_by_field: dict[str, object],
) -> dict[str, object]:
    return {
        "audit": {"payload": audit_payload_by_field},
        "snapshot": {
            "payload": {
                "snapshot_id": _SNAPSHOT_ID,
                "import_run_id": _INTERNAL_RUN_ID,
                "status": "failed",
                "validated_at": None,
                "published_at": None,
                "manifest": {},
            }
        },
        "internal_run": {
            "payload": {
                "import_run_id": _INTERNAL_RUN_ID,
                "status": "failed",
                "finished_at": "2026-08-01T00:00:00Z",
                "options": {
                    "storage_generation": "shared_blocks_v3",
                    "snapshot_arch": "postgres_binary_v3",
                    "source_file_import_id": _SOURCE_IMPORT_ID,
                },
            }
        },
        "run_snapshots": [
            {
                "snapshot_id": _SNAPSHOT_ID,
                "import_run_id": _INTERNAL_RUN_ID,
            }
        ],
        "attachment_digest": marker_by_name["attachment_digest"],
        "catalog_digest": marker_by_name["catalog_digest"],
        "event_high_water_mark": 7,
    }


def _valid_review() -> state.ReconciledStateReview:
    marker_by_name = _audit_marker()
    observation_by_field = _terminal_observation(
        marker_by_name,
        _audit_payload(marker_by_name),
    )
    return state.ReconciledStateReview(
        observation=observation_by_field,
        operational_evidence={"exact_external_absence": True},
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
        target_digest="f" * 64,
        capabilities_ready=True,
        retained_state_digest=marker_by_name["retained_state_digest"],
        preserved_row_digest=marker_by_name["preserved_row_digest"],
        lineage_reasons=(),
        attachment_reasons=(),
        source_pair_reasons=(),
    )


def test_reconciled_state_helper_type_edges() -> None:
    assert state._payload(None) == {}
    assert state._payload({"payload": []}) == {}
    assert state._payload({"payload": {"status": "failed"}}) == {
        "status": "failed"
    }
    assert state._source_file_import_id({"options": []}) == ""
    assert state._source_file_import_id(
        {"options": {"source_file_import_id": 7}}
    ) == ""
    assert state._source_file_import_id(
        {"options": {"source_file_import_id": _SOURCE_IMPORT_ID}}
    ) == _SOURCE_IMPORT_ID
    assert state._integer_or_default(None, -1) == -1
    assert state._integer_or_default("7", -1) == 7


def test_reconciled_state_reports_operational_guards() -> None:
    review = _valid_review()
    assert state.reconciled_state_reasons(review) == []
    blocked_review = replace(
        review,
        capabilities_ready=False,
        operational_evidence={"exact_external_absence": False},
    )
    assert state.reconciled_state_reasons(blocked_review) == [
        "external_attempt_identity_present",
        "shared_attempt_guard_capability_missing",
    ]


def test_reconciled_state_reports_each_terminal_view_change() -> None:
    review = _valid_review()
    snapshot_observation = copy.deepcopy(review.observation)
    snapshot_observation["snapshot"]["payload"]["status"] = "building"
    assert state.reconciled_state_reasons(
        replace(review, observation=snapshot_observation)
    ) == ["reconciled_snapshot_changed"]

    run_observation = copy.deepcopy(review.observation)
    run_observation["internal_run"]["payload"]["status"] = "running"
    assert state.reconciled_state_reasons(
        replace(review, observation=run_observation)
    ) == ["reconciled_internal_run_changed"]

    pair_observation = copy.deepcopy(review.observation)
    pair_observation["run_snapshots"] = []
    assert state.reconciled_state_reasons(
        replace(review, observation=pair_observation)
    ) == ["reconciled_pair_cardinality_changed"]


def test_attachment_where_requires_registered_coordinates() -> None:
    snapshot_attachment = AttemptAttachment(
        "snapshot",
        "snapshot_table",
        ("snapshot_id",),
    )
    run_attachment = AttemptAttachment(
        "run",
        "run_table",
        (),
        ("import_run_id",),
    )
    assert evidence._attachment_where(snapshot_attachment) == (
        '"snapshot_id" = :snapshot_id'
    )
    assert evidence._attachment_where(run_attachment) == (
        '"import_run_id" = :internal_run_id'
    )
    with pytest.raises(RuntimeError, match="has no attempt coordinate"):
        evidence._attachment_where(AttemptAttachment("empty", "empty_table"))


@pytest.mark.asyncio
@pytest.mark.parametrize("relation_present", (False, True))
async def test_attachment_relation_probe_returns_scalar_boolean(
    relation_present: bool,
) -> None:
    session = _Session(_QueryResult(scalar=relation_present))
    assert await evidence.has_relation(
        session,
        "mrf",
        "ptg2_snapshot",
    ) is relation_present
    assert session.calls[0][1] == {
        "qualified_name": "mrf.ptg2_snapshot"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("optional_relation", "expected_count"),
    ((False, -1), (True, 0)),
)
async def test_missing_attachment_relation_has_explicit_count(
    monkeypatch,
    optional_relation: bool,
    expected_count: int,
) -> None:
    attachment = AttemptAttachment(
        "missing",
        "missing_table",
        ("snapshot_id",),
        optional_relation=optional_relation,
    )
    monkeypatch.setattr(evidence, "has_relation", AsyncMock(return_value=False))
    assert await evidence._one_attachment_evidence(
        object(),
        schema_name="mrf",
        attachment=attachment,
        parameters_by_name={"snapshot_id": _SNAPSHOT_ID},
    ) == (expected_count, None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attachment_name", "expects_retained_rows"),
    (("snapshot_scope", True), ("layout_bindings", False)),
)
async def test_present_attachment_retains_only_allowed_rows(
    monkeypatch,
    attachment_name: str,
    expects_retained_rows: bool,
) -> None:
    attachment = AttemptAttachment(
        attachment_name,
        f"{attachment_name}_table",
        ("snapshot_id",),
    )
    retained_rows = [{"row": {"synthetic": True}, "xmin": "1"}]
    retained_loader = AsyncMock(return_value=retained_rows)
    monkeypatch.setattr(evidence, "has_relation", AsyncMock(return_value=True))
    monkeypatch.setattr(evidence, "_retained_rows", retained_loader)
    monkeypatch.setattr(
        evidence,
        "_catalog_row",
        AsyncMock(return_value={"attachment": attachment_name}),
    )
    count, returned_rows, catalog_row = await evidence._one_attachment_evidence(
        _Session(_QueryResult(scalar=1)),
        schema_name="mrf",
        attachment=attachment,
        parameters_by_name={"snapshot_id": _SNAPSHOT_ID},
    )
    assert count == 1
    assert returned_rows == (retained_rows if expects_retained_rows else None)
    assert catalog_row == {"attachment": attachment_name}
    assert retained_loader.await_count == int(expects_retained_rows)


@pytest.mark.asyncio
async def test_catalog_row_and_retained_row_adapters() -> None:
    attachment = AttemptAttachment(
        "snapshot_scope",
        "snapshot_scope_table",
        ("snapshot_id",),
    )
    absent_session = _Session(_QueryResult(mapping=None))
    assert await evidence._catalog_row(
        absent_session,
        schema_name="mrf",
        attachment=attachment,
    ) is None
    present_session = _Session(
        _QueryResult(mapping={"relation_oid": 7, "columns": []}),
        _QueryResult(
            rows=(SimpleNamespace(row_payload={"id": 1}, row_xmin="8"),)
        ),
    )
    catalog_row = await evidence._catalog_row(
        present_session,
        schema_name="mrf",
        attachment=attachment,
    )
    retained_rows = await evidence._retained_rows(
        present_session,
        table_name='"mrf"."snapshot_scope_table"',
        where_clause='"snapshot_id" = :snapshot_id',
        parameters_by_name={"snapshot_id": _SNAPSHOT_ID},
    )
    assert catalog_row["attachment"] == "snapshot_scope"
    assert retained_rows == [{"row": {"id": 1}, "xmin": "8"}]


def test_stale_metadata_fence_recognizes_direct_and_marker_errors() -> None:
    assert fence.has_stale_metadata_marker(
        {PTG2_V4_STALE_METADATA_MARKER: True}
    )
    assert not fence.has_stale_metadata_marker([])
    assert fence.is_stale_metadata_fence_error(
        fence.StaleMetadataFenceError("direct")
    )
    assert fence.is_stale_metadata_fence_error(
        RuntimeError("PTG2_LEGACY_V3_ATTEMPT_RECONCILED")
    )
    assert not fence.is_stale_metadata_fence_error(RuntimeError("ordinary"))


@pytest.mark.asyncio
async def test_snapshot_lock_preserves_unrelated_execution_errors() -> None:
    ordinary_error = RuntimeError("ordinary database error")
    session = SimpleNamespace(execute=AsyncMock(side_effect=ordinary_error))
    with pytest.raises(RuntimeError, match="ordinary database error") as raised:
        await fence.lock_writable_snapshot(
            session,
            None,
            schema_name="mrf",
            snapshot_id=_SNAPSHOT_ID,
        )
    assert raised.value is ordinary_error


@pytest.mark.asyncio
async def test_stage_table_names_reject_invalid_and_skip_empty_work() -> None:
    assert fence._validated_table_names(["stage_b", "stage_a", "stage_a"]) == (
        "stage_a",
        "stage_b",
    )
    with pytest.raises(ValueError, match="table name is invalid"):
        fence._validated_table_names(["invalid-stage"])
    await fence.register_attempt_stage_tables(
        object(),
        schema_name="mrf",
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        table_names=[],
    )
    await fence.drop_attempt_stage_tables(
        object(),
        schema_name="mrf",
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        table_names=[],
    )
