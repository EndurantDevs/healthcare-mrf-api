# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed margin for rollback mutation and candidate publication."""

from __future__ import annotations

import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publisher
from process.ptg_parts import source_snapshot_rollback as rollback
from process.ptg_parts import source_snapshot_rollback_relations as rollback_relations
from process.ptg_parts import source_snapshot_rollback_store as rollback_store
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
    RollbackDecision,
)
from tests.test_ptg_source_snapshot_rollback import (
    CURRENT_SNAPSHOT,
    ROLLBACK_OWNER,
    SOURCE_KEY,
    TARGET_SNAPSHOT,
)


class _EmptyResult:
    def one_or_none(self) -> None:
        return None


class _ScalarResult:
    def __init__(self, value: object) -> None:
        self.value = value

    def scalar_one(self) -> object:
        return self.value


class _RecordingSession:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, object]] = []

    async def execute(self, statement: object, params: object = None) -> object:
        self.calls.append((statement, params))
        return self.result


def test_rollback_coordinates_and_report_are_explicit():
    with pytest.raises(ValueError, match="are required"):
        rollback._normalized_coordinates(
            source_key="",
            snapshot_id=TARGET_SNAPSHOT,
            expected_current_snapshot_id=CURRENT_SNAPSHOT,
            rollback_owner_id=ROLLBACK_OWNER,
        )
    with pytest.raises(ValueError, match="must differ"):
        rollback._normalized_coordinates(
            source_key=SOURCE_KEY,
            snapshot_id=TARGET_SNAPSHOT,
            expected_current_snapshot_id=TARGET_SNAPSHOT,
            rollback_owner_id=ROLLBACK_OWNER,
        )
    report = rollback._rollback_report(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
        decision=RollbackDecision(
            is_already_rolled_back=False,
            plan_pointer_entries=(),
            should_reverse_global_pointer=False,
            allowed_action="delete",
        ),
        global_pointer_status="deferred",
    )
    assert report["rollback_owner_id"] == ROLLBACK_OWNER
    assert report["allowed_amount_pointer"]["status"] == "removed"
    assert report["global_pointer"] == "deferred"


def test_rollback_cache_clear_and_row_normalizers(monkeypatch):
    from api import ptg2_snapshot

    snapshot_cache_by_key = {"synthetic": object()}
    monkeypatch.setattr(
        ptg2_snapshot,
        "_PTG2_SNAPSHOT_RESOLVE_CACHE",
        snapshot_cache_by_key,
        raising=False,
    )
    rollback._clear_ptg2_snapshot_cache()
    assert not snapshot_cache_by_key
    assert rollback_relations._row_mapping({"value": 1}) == {"value": 1}
    assert rollback_store._row_mapping({"value": 1}) == {"value": 1}


@pytest.mark.asyncio
async def test_rollback_timestamp_and_compare_and_set_fail_closed():
    session = _RecordingSession(_ScalarResult("not-a-timestamp"))
    with pytest.raises(RuntimeError, match="did not return"):
        await rollback_relations.database_utc_timestamp(session)

    changed_session = _RecordingSession(_EmptyResult())
    with pytest.raises(PTG2SourceSnapshotRollbackConflict, match="changed"):
        await rollback_store._require_changed_row(
            changed_session,
            "UPDATE synthetic",
            {},
            failure_message="pointer changed",
        )


def test_publication_progress_rejects_bad_deltas_and_names():
    progress = publisher._MeasuredPublicationProgress(
        "publication",
        None,
        interval_seconds=0,
        clock=lambda: 0,
    )
    progress.add("rows", 0)
    with pytest.raises(ValueError, match="metric must be non-empty"):
        progress.add("", 1)


@pytest.mark.asyncio
async def test_completed_price_probe_does_not_mask_task_failure():
    async def fail() -> tuple[object, float]:
        raise RuntimeError("synthetic preparation failure")

    task = asyncio.create_task(fail())
    await asyncio.gather(task, return_exceptions=True)
    assert publisher._completed_prepared_price(task) is None


def test_publication_summary_rejects_duplicate_lane_kinds():
    publication = SimpleNamespace(
        object_kinds=("duplicate", "duplicate"),
        mapping_count=1,
        unique_block_count=1,
        logical_byte_count=1,
    )
    summary = SimpleNamespace(
        object_kinds=("duplicate",),
        mapping_count=1,
        unique_block_count=1,
        logical_byte_count=1,
    )
    with pytest.raises(RuntimeError, match="invalid object kinds"):
        publisher._validate_authoritative_mapping_summary(summary, publication)


@pytest.mark.asyncio
async def test_price_preparation_without_early_key_can_publish_after_completion(
    monkeypatch,
):
    prepared = SimpleNamespace(price_set_count=1)

    async def prepare_price_artifacts(**_kwargs):
        return prepared

    publish_prepared = AsyncMock(return_value="published")
    monkeypatch.setattr(
        publisher,
        "prepare_shared_price_artifacts",
        prepare_price_artifacts,
    )

    publication_result = await publisher._prepare_price_with_early_finalizer(
        schema_name="mrf",
        manifest_stage_table="stage",
        price_set_summary_source_count=1,
        finalizer_inputs=publisher._EarlyFinalizerInputs(
            raw_work_directory=".",
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
        ),
        publish_prepared_price=publish_prepared,
    )

    assert publication_result[0] is prepared
    assert publication_result[2] is None
    assert publication_result[3].publication == "published"


def test_publication_scalar_and_file_guards(tmp_path):
    for value in (True, object()):
        with pytest.raises(RuntimeError, match="invalid counter"):
            publisher._integer(value, "counter")
    with pytest.raises(RuntimeError, match="negative counter"):
        publisher._integer(-1, "counter")

    with pytest.raises(RuntimeError, match="escapes"):
        publisher._output_file(
            {"output_directory": str(tmp_path)},
            {"path": "../outside"},
        )
    with pytest.raises(RuntimeError, match="missing or empty"):
        publisher._output_file(
            {"output_directory": str(tmp_path)},
            {"path": "missing"},
        )


def test_publication_source_ordinals_fail_closed(monkeypatch):
    identity = SimpleNamespace(
        as_dict=lambda: {
            "source_type": "rate_file",
            "identity_kind": "raw_sha256",
            "identity_sha256": "11" * 32,
        }
    )
    assignment = SimpleNamespace(
        source_key=1,
        identity=identity,
        raw_container_sha256="11" * 32,
        logical_json_sha256="11" * 32,
        logical_hash_deferred=False,
        source_trace_set_hash="22" * 32,
    )
    with pytest.raises(ValueError, match="valid snapshot_id"):
        publisher._snapshot_source_rows(snapshot_id="", assignments=(assignment,))
    with pytest.raises(ValueError, match="complete and dense"):
        publisher._snapshot_source_rows(
            snapshot_id="snapshot",
            assignments=(assignment,),
        )

    assignment.source_key = 0
    monkeypatch.setattr(
        publisher,
        "deterministic_source_key_assignments",
        lambda _records: ((1, identity),),
    )
    with pytest.raises(ValueError, match="physical artifact ordinals"):
        publisher._snapshot_source_rows(
            snapshot_id="snapshot",
            assignments=(assignment,),
        )


@pytest.mark.parametrize(
    "payload",
    (
        b"x" * (64 * 1024 + 1),
        b"not-json\n",
        json.dumps([]).encode() + b"\n",
        json.dumps({"codec": "none"}).encode() + b"\n",
        json.dumps(
            {
                "codec": "none",
                "object_kind": "bad",
                "block_key": 0,
                "fragment_no": 0,
                "entry_count": 0,
                "raw_byte_count": 1,
                "stored_byte_count": 1,
                "hash": hashlib.sha256(b"x").hexdigest(),
            }
        ).encode()
        + b"\n",
    ),
)
def test_v4_reference_manifest_rejects_unbounded_or_changed_records(
    tmp_path,
    payload,
):
    manifest_path = tmp_path / "references.jsonl"
    manifest_path.write_bytes(payload)
    with pytest.raises(RuntimeError, match="PTG V4 graph reference"):
        tuple(publisher._iter_v4_block_references(manifest_path))


@pytest.mark.asyncio
async def test_empty_v4_reference_manifest_queues_nothing(tmp_path):
    manifest_path = tmp_path / "references.jsonl"
    manifest_path.write_bytes(b"")
    await publisher._queue_failed_v4_graph_blocks(
        schema_name="mrf",
        reference_manifest_path=manifest_path,
    )


def test_v4_compiler_artifact_requires_one_exact_output():
    compilation = SimpleNamespace(output_artifacts=())
    with pytest.raises(RuntimeError, match="compiler output is missing"):
        publisher._v4_compiler_artifact(compilation, "provider_tax_identities")
