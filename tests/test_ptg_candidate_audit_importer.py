# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports, control_workers
from process import PTGCandidateAudit
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_evidence,
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import source_set_digest


ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")
process_ptg = importlib.import_module("process.ptg")


RAW_DIGEST = "ab" * 32
SOURCE_SET_DIGEST = source_set_digest((RAW_DIGEST,))
SOURCE_WITNESS_DIGEST = "cd" * 32
SOURCE_WITNESS_SAMPLE_DIGEST = "de" * 32
AUDIT_SAMPLE_DIGEST = "ef" * 32
EMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload({})
NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload(
    {123456789: 2}
)


def _source_witness_by_field() -> dict[str, object]:
    return {
        "contract": "ptg2_v3_source_witness_payload_v5",
        "format_version": 5,
        "selection_method": "bottom_k_independent_occurrence_provider_cohorts_v3",
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
        "source_count": 1,
        "source_set_digest": SOURCE_SET_DIGEST,
        "occurrence_target": 10_000,
        "total_target": 11_000,
        "provider_quota": 1_000,
        "queryable_occurrence_population_count": 50_000,
        "provider_population_count": 5_000,
        "emitted_rate_row_count": 1_000,
        "unqueryable_rate_row_count": 0,
        "occurrence_witness_count": 10_000,
        "provider_witness_count": 1_000,
        "record_count": 11_000,
        "evidence_dictionary_count": 1_000,
        "evidence_dictionary_raw_bytes": 10_000,
        "evidence_dictionary_stored_bytes": 5_000,
        "sample_digest": SOURCE_WITNESS_SAMPLE_DIGEST,
        "payload_sha256": SOURCE_WITNESS_DIGEST,
        "payload_bytes": 123_456,
        "compression": "per_record_zlib_shared_evidence_dictionary_v1",
    }


def _audit_sample_by_field() -> dict[str, object]:
    return {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 2_560,
        "maximum_rows": 2_560,
        "sample_digest": AUDIT_SAMPLE_DIGEST,
        "source_count": 1,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "complete_population": False,
        "serving_multiplicity_semantics": "source_multiset_v1",
    }


def _candidate_serving_index(
    storage_generation: str,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> dict[str, object]:
    serving_index_by_field = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": storage_generation,
        "source_set": {
            "contract": "sorted_raw_container_sha256_bytes_v1",
            "source_count": 1,
            "raw_container_sha256_digest": SOURCE_SET_DIGEST,
        },
        "source_witness": _source_witness_by_field(),
        "audit_sample": _audit_sample_by_field(),
        "provider_identifier_quarantine": provider_identifier_quarantine,
    }
    if storage_generation == "shared_blocks_v4":
        serving_index_by_field.update(
            {
                "type": "ptg2_shared_blocks_v4",
                "provider_scope_strategy": "postgres_packed_graph_v4",
                "shared_block_layout": "packed_snapshot_maps_v4",
                "shared_snapshot_key": 17,
                "snapshot_map": {
                    "contract": "ptg_v4_packed_snapshot_map_v1",
                    "map_digest": ("ab" * 32),
                },
            }
        )
    return serving_index_by_field


def _candidate_row(
    *,
    activated: bool = False,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    storage_generation: str = "shared_blocks_v3",
) -> dict[str, object]:
    """Build one sealed candidate database row fixture."""

    snapshot_id = "candidate-snapshot"
    activation_map = {
        "contract": "ptg2_candidate_activation_v1",
        "state": "activated" if activated else "validated",
        "source_key": "derived-source",
        "expected_previous_snapshot_id": "previous-snapshot",
    }
    if activated:
        activation_map["mode"] = "audited_control"
    serving_index_by_field = _candidate_serving_index(
        storage_generation,
        provider_identifier_quarantine,
    )
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": "ptg2:derived-import",
        "status": "published" if activated else "validated",
        "previous_snapshot_id": "previous-snapshot",
        "manifest": {
            "activation": activation_map,
            "serving_index": serving_index_by_field,
        },
        "snapshot_key": 17,
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "layout_state": "sealed",
        "layout_generation": storage_generation,
        "layout_mapping_digest": (
            bytes.fromhex("ab" * 32)
            if storage_generation == "shared_blocks_v4"
            else None
        ),
        "layout_manifest": {"serving_index": serving_index_by_field},
        "v4_root_state": (
            "complete" if storage_generation == "shared_blocks_v4" else None
        ),
        "v4_root_map_digest": (
            bytes.fromhex("ab" * 32)
            if storage_generation == "shared_blocks_v4"
            else None
        ),
        "current_snapshot_id": snapshot_id if activated else "previous-snapshot",
        "audit_report_digest": bytes.fromhex("cd" * 32) if activated else None,
        "audit_report": (
            _passing_report(
                provider_identifier_quarantine=provider_identifier_quarantine
            )
            if activated
            else None
        ),
        "audit_activated_at": "2026-07-13T12:00:00+00:00" if activated else None,
    }


def _candidate_row_with_equivalent_current() -> dict[str, object]:
    candidate_row = _candidate_row()
    current_row = _candidate_row(activated=True)
    candidate_row.update(
        {
            "current_snapshot_id": "active-snapshot",
            "current_import_run_id": "ptg2:active-import",
            "current_status": current_row["status"],
            "current_previous_snapshot_id": current_row["previous_snapshot_id"],
            "current_manifest": current_row["manifest"],
            "current_snapshot_key": current_row["snapshot_key"],
            "current_plan_id": current_row["plan_id"],
            "current_plan_market_type": current_row["plan_market_type"],
            "current_layout_state": current_row["layout_state"],
            "current_layout_generation": current_row["layout_generation"],
            "current_layout_manifest": current_row["layout_manifest"],
            "current_audit_report_digest": current_row["audit_report_digest"],
            "current_audit_report": current_row["audit_report"],
            "current_audit_activated_at": current_row["audit_activated_at"],
        }
    )
    return candidate_row


def _passing_report(
    *,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> dict[str, object]:
    return {
        "status": "pass",
        "release_gate_eligible": True,
        "duration_seconds": 12.5,
        "checks": {
            "source_witnesses": 11_000,
            "api_witnesses_matched": 10_000,
            "api_challenges_executed": 10_000,
            "provider_witnesses_validated": 1_000,
            "api_audit_occurrences_validated": 1,
        },
        "http": {"standard_api_actual_http_requests": 10_001},
        "latency": {
            "request_p50_ms": 18.2,
            "request_p95_ms": 31.2,
            "request_max_ms": 35.4,
        },
        "source": {
            "private_detail": "must-not-leak",
            "source_set_digest": SOURCE_SET_DIGEST,
            "source_witness_payload_sha256": SOURCE_WITNESS_DIGEST,
            "provider_identifier_quarantine": provider_identifier_quarantine,
        },
        "failures": {"examples": [{"body": "must-not-leak"}]},
    }


def _passing_batch_report(
    *,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> dict[str, object]:
    return {
        "status": "pass",
        "release_gate_eligible": True,
        "duration_seconds": 12.5,
        "checks": {
            "source_witnesses": 11_000,
            "source_occurrence_witnesses_matched": 10_000,
            "unique_source_conditions_executed": 9_000,
            "provider_witnesses_validated": 1_000,
            "persisted_audit_occurrences_validated": 2_560,
            "batch_requests_executed": 1,
        },
        "http": {
            "batch_api_planned_http_requests": 1,
            "batch_api_actual_http_requests": 1,
            "batch_api_completed_http_requests": 1,
            "batch_api_failed_http_requests": 0,
            "retry_count": 0,
            "max_concurrency": 1,
        },
        "batch": {"endpoint_duration_ms": 12_000.0},
        "source": {
            "provider_identifier_quarantine": (
                provider_identifier_quarantine_evidence(
                    provider_identifier_quarantine
                )
            ),
        },
    }


def _target(
    *,
    activated: bool = False,
    provider_identifier_quarantine=EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
) -> ptg_candidate_audit.CandidateAuditTarget:
    return ptg_candidate_audit.CandidateAuditTarget(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        snapshot_status="published" if activated else "validated",
        snapshot_key=17,
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
        expected_current_snapshot_id="previous-snapshot",
        current_snapshot_id="candidate-snapshot" if activated else "previous-snapshot",
        raw_container_sha256=(RAW_DIGEST,),
        provider_identifier_quarantine=provider_identifier_quarantine,
        source_witness=_candidate_row(
            provider_identifier_quarantine=provider_identifier_quarantine
        )["manifest"]["serving_index"]["source_witness"],
        audit_sample=_candidate_row(
            provider_identifier_quarantine=provider_identifier_quarantine
        )["manifest"]["serving_index"]["audit_sample"],
        activated=activated,
        audit_report=(
            _passing_report(
                provider_identifier_quarantine=provider_identifier_quarantine
            )
            if activated
            else None
        ),
        audit_report_digest="cd" * 32 if activated else None,
    )


def test_registry_and_dedicated_worker_contract():
    importers_map = {item["name"]: item for item in control_imports.importer_registry()}
    workers_map = {item["queue"]: item for item in control_workers.worker_registry()}
    adapter = control_imports._SINGLE_JOB_ADAPTERS["ptg-candidate-audit"]

    item = importers_map["ptg-candidate-audit"]
    assert item["family"] == "mrf"
    assert item["enqueue_adapter"] == "arq_single_job"
    assert item["cancelable"] is True
    assert item["queue"] == "arq:PTGCandidateAudit"
    assert [param["name"] for param in item["params_schema"]] == [
        "candidate_run_id",
        "snapshot_id",
        "import_id",
    ]
    assert item["params_schema"][0]["required"] is True
    assert adapter["target_module"] == "process.ptg_candidate_audit"
    assert adapter["target_function"] == "main"
    assert adapter["job_prefix"] == "ptg_candidate_audit"
    assert workers_map["arq:PTGCandidateAudit"]["worker_class"] == "process.PTGCandidateAudit"
    assert PTGCandidateAudit.max_jobs == 1
    assert PTGCandidateAudit.queue_read_limit == 1


@pytest.mark.asyncio
async def test_generic_enqueue_uses_dedicated_queue_and_stable_job_id(monkeypatch):
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class Job:
        job_id = "ptg_candidate_audit_control-run"

    class Redis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return Job()

    async def create_pool(*_args, **_kwargs):
        return Redis()

    monkeypatch.setattr(control_imports, "create_pool", create_pool)
    enqueue_response = await control_imports._enqueue_import_start(
        {
            "run_id": "control-run",
            "importer": "ptg-candidate-audit",
            "family": "mrf",
            "params": {
                "candidate_run_id": "ptg2:derived-import",
                "snapshot_id": "candidate-snapshot",
            },
        }
    )

    assert enqueue_response["status"] == "queued"
    assert enqueue_response["metrics"]["queue"] == "arq:PTGCandidateAudit"
    args, kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["task"]["candidate_run_id"] == "ptg2:derived-import"
    assert kwargs == {
        "_queue_name": "arq:PTGCandidateAudit",
        "_job_id": "ptg_candidate_audit_control-run",
    }


@pytest.mark.asyncio
async def test_candidate_scope_is_derived_and_corroboration_cannot_spoof(monkeypatch):
    candidate_rows = AsyncMock(return_value=[_candidate_row()])
    raw_sources = AsyncMock(return_value=(RAW_DIGEST,))
    monkeypatch.setattr(ptg_candidate_audit, "_candidate_rows", candidate_rows)
    monkeypatch.setattr(ptg_candidate_audit, "_candidate_raw_sources", raw_sources)

    target = await ptg_candidate_audit.load_candidate_audit_target(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
    )

    assert target.plan_id == "12-3456789"
    assert target.plan_market_type == "group"
    assert target.source_key == "derived-source"
    assert target.raw_container_sha256 == (RAW_DIGEST,)
    candidate_rows.assert_awaited_once_with("ptg2:derived-import")
    raw_sources.assert_awaited_once_with("candidate-snapshot")

    with pytest.raises(ValueError, match="snapshot_id does not corroborate"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
            snapshot_id="caller-spoofed-snapshot",
        )
    with pytest.raises(ValueError, match="import_id does not corroborate"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
            import_id="caller-spoofed-import",
        )


@pytest.mark.asyncio
async def test_candidate_scope_accepts_complete_v4_packed_layout(monkeypatch):
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_rows",
        AsyncMock(
            return_value=[
                _candidate_row(storage_generation="shared_blocks_v4")
            ]
        ),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        AsyncMock(return_value=(RAW_DIGEST,)),
    )

    target = await ptg_candidate_audit.load_candidate_audit_target(
        candidate_run_id="ptg2:derived-import",
    )

    assert target.storage_generation == "shared_blocks_v4"
    assert target.snapshot_key == 17


@pytest.mark.asyncio
async def test_controlled_rebuild_run_identity_keeps_audit_targets_unambiguous(
    monkeypatch,
):
    """Resolve the legacy and rebuilt snapshots through distinct audit runs."""

    legacy_run_id = process_ptg._ptg2_import_run_id("test-import")
    rebuild_run_id = process_ptg._ptg2_import_run_id(
        "test-import",
        full_rebuild_scope_digest="1" * 64,
    )
    candidate_by_run_id = {}
    for candidate_run_id, snapshot_id in (
        (legacy_run_id, "legacy-snapshot"),
        (rebuild_run_id, "rebuild-snapshot"),
    ):
        candidate_by_run_id[candidate_run_id] = {
            **_candidate_row(),
            "snapshot_id": snapshot_id,
            "import_run_id": candidate_run_id,
        }

    async def candidate_rows(candidate_run_id):
        return [candidate_by_run_id[candidate_run_id]]

    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_rows",
        candidate_rows,
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        AsyncMock(return_value=(RAW_DIGEST,)),
    )

    legacy_target = await ptg_candidate_audit.load_candidate_audit_target(
        candidate_run_id=legacy_run_id,
        import_id="test-import",
    )
    rebuild_target = await ptg_candidate_audit.load_candidate_audit_target(
        candidate_run_id=rebuild_run_id,
        import_id="test-import",
    )

    assert legacy_target.snapshot_id == "legacy-snapshot"
    assert rebuild_target.snapshot_id == "rebuild-snapshot"
    assert legacy_target.candidate_run_id != rebuild_target.candidate_run_id


def test_candidate_import_id_only_strips_exact_rebuild_suffix():
    assert ptg_candidate_audit._candidate_import_id(
        "ptg2:test:rebuild-label"
    ) == "test:rebuild-label"
    assert ptg_candidate_audit._candidate_import_id(
        "ptg2:test:rebuild-" + "a" * 23
    ) == "test:rebuild-" + "a" * 23
    assert ptg_candidate_audit._candidate_import_id(
        "ptg2:test:rebuild-" + "a" * 24
    ) == "test"


@pytest.mark.asyncio
async def test_candidate_target_query_ignores_unsupported_attestations(monkeypatch):
    database_read = AsyncMock(return_value=[])
    monkeypatch.setattr(ptg_candidate_audit.db, "all", database_read)

    assert await ptg_candidate_audit._candidate_rows("ptg2:derived-import") == []

    query = database_read.await_args.args[0]
    query_parameters = database_read.await_args.kwargs
    assert " attestation.contract = ANY(" in query
    assert "current_attestation.contract = ANY(" in query
    assert query.count(":supported_contracts") == 2
    assert "ptg2_v4_snapshot_map_root AS v4_root" in query
    assert "ptg2_v4_snapshot_map_root AS current_v4_root" in query
    assert query_parameters["supported_contracts"] == list(
        ptg_candidate_audit.PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
    )


@pytest.mark.asyncio
async def test_equivalent_current_snapshot_reuses_existing_attestation(monkeypatch):
    candidate_row = _candidate_row_with_equivalent_current()
    raw_sources = AsyncMock(side_effect=[(RAW_DIGEST,), (RAW_DIGEST,)])
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_rows",
        AsyncMock(return_value=[candidate_row]),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        raw_sources,
    )

    target = await ptg_candidate_audit.load_candidate_audit_target(
        candidate_run_id="ptg2:derived-import",
    )

    assert target.snapshot_id == "candidate-snapshot"
    assert target.equivalent_current_snapshot_id == "active-snapshot"
    assert target.equivalent_current_import_run_id == "ptg2:active-import"
    assert target.equivalent_audit_report == _passing_report()
    assert target.equivalent_audit_report_digest == "cd" * 32
    assert raw_sources.await_args_list[0].args == ("candidate-snapshot",)
    assert raw_sources.await_args_list[1].args == ("active-snapshot",)


@pytest.mark.asyncio
async def test_non_equivalent_current_snapshot_fails_closed(monkeypatch):
    candidate_row = _candidate_row_with_equivalent_current()
    candidate_row["current_snapshot_key"] = 18
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_rows",
        AsyncMock(return_value=[candidate_row]),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        AsyncMock(side_effect=[(RAW_DIGEST,), (RAW_DIGEST,)]),
    )

    with pytest.raises(ValueError, match="non-equivalent snapshot"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
        )


@pytest.mark.asyncio
async def test_candidate_scope_rejects_snapshot_layout_quarantine_mismatch(monkeypatch):
    candidate_row = _candidate_row()
    candidate_row["layout_manifest"] = {
        "serving_index": {
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
            "provider_identifier_quarantine": provider_identifier_quarantine_payload(
                {123456789: 1}
            ),
        }
    }
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_rows",
        AsyncMock(return_value=[candidate_row]),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        AsyncMock(return_value=(RAW_DIGEST,)),
    )

    with pytest.raises(ValueError, match="changed after layout sealing"):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
        )


@pytest.mark.parametrize(
    ("mapping_value", "expected_mapping"),
    (
        ({"value": 1}, {"value": 1}),
        ('{"value": 2}', {"value": 2}),
        ("[]", {}),
        ("{", {}),
        (None, {}),
    ),
)
def test_candidate_mapping_edges(mapping_value, expected_mapping):
    assert ptg_candidate_audit._mapping(mapping_value) == expected_mapping


def test_candidate_row_mapping_edges():
    driver_row = Mock()
    driver_row._mapping = {"value": 2}

    assert ptg_candidate_audit._row_mapping({"value": 1}) == {"value": 1}
    assert ptg_candidate_audit._row_mapping(driver_row) == {"value": 2}


@pytest.mark.parametrize(
    ("database_rows", "message"),
    (
        (
            [{"source_key": "bad", "raw_container_sha256": RAW_DIGEST}],
            "invalid ordinal",
        ),
        (
            [{"source_key": 1, "raw_container_sha256": RAW_DIGEST}],
            "not dense",
        ),
        ([], "no public raw source"),
        (
            [
                {"source_key": 0, "raw_container_sha256": RAW_DIGEST},
                {"source_key": 1, "raw_container_sha256": RAW_DIGEST},
            ],
            "ambiguous",
        ),
    ),
)
@pytest.mark.asyncio
async def test_candidate_raw_source_edges(monkeypatch, database_rows, message):
    monkeypatch.setattr(
        ptg_candidate_audit.db,
        "all",
        AsyncMock(return_value=database_rows),
    )

    with pytest.raises(ValueError, match=message):
        await ptg_candidate_audit._candidate_raw_sources("candidate-snapshot")


@pytest.mark.asyncio
async def test_candidate_raw_sources_return_dense_validated_digests(monkeypatch):
    monkeypatch.setattr(
        ptg_candidate_audit.db,
        "all",
        AsyncMock(
            return_value=[
                {"source_key": 0, "raw_container_sha256": bytes.fromhex(RAW_DIGEST)}
            ]
        ),
    )

    assert await ptg_candidate_audit._candidate_raw_sources(
        "candidate-snapshot"
    ) == (RAW_DIGEST,)


def test_candidate_digest_and_current_pointer_edges():
    with pytest.raises(ValueError, match="candidate digest is invalid"):
        ptg_candidate_audit._normalized_digest("bad", field="digest")
    assert ptg_candidate_audit._current_snapshot_row({}) is None


@pytest.mark.asyncio
async def test_equivalent_current_snapshot_requires_valid_attested_target(
    monkeypatch,
):
    with pytest.raises(ValueError, match="deferred activation"):
        await ptg_candidate_audit._reuse_equivalent_current_target({}, _target())

    with pytest.raises(ValueError, match="superseded by an invalid snapshot"):
        await ptg_candidate_audit._reuse_equivalent_current_target(
            {"current_snapshot_id": "active-snapshot"},
            _target(),
        )

    current_target = replace(
        _target(activated=True),
        audit_report=None,
        audit_report_digest=None,
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_raw_sources",
        AsyncMock(return_value=(RAW_DIGEST,)),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_candidate_target_from_row",
        Mock(return_value=current_target),
    )
    with pytest.raises(ValueError, match="no audit attestation"):
        await ptg_candidate_audit._reuse_equivalent_current_target(
            _candidate_row_with_equivalent_current(),
            _target(),
        )


@pytest.mark.parametrize(
    ("candidate_run_id", "candidate_rows", "message"),
    (
        ("", [], "candidate_run_id is required"),
        ("ptg2:missing", [], "did not resolve a candidate"),
        (
            "ptg2:ambiguous",
            [_candidate_row(), _candidate_row()],
            "does not resolve exactly one candidate",
        ),
    ),
)
@pytest.mark.asyncio
async def test_candidate_resolution_requires_one_exact_run(
    monkeypatch,
    candidate_run_id,
    candidate_rows,
    message,
):
    rows_loader = AsyncMock(return_value=candidate_rows)
    monkeypatch.setattr(ptg_candidate_audit, "_candidate_rows", rows_loader)

    with pytest.raises(ValueError, match=message):
        await ptg_candidate_audit.load_candidate_audit_target(
            candidate_run_id=candidate_run_id
        )

    if candidate_run_id:
        rows_loader.assert_awaited_once_with(candidate_run_id)
    else:
        rows_loader.assert_not_awaited()


@pytest.mark.parametrize(
    ("activated", "candidate_mutator", "message"),
    (
        (
            False,
            lambda candidate_row: candidate_row.update(import_run_id="wrong"),
            "run binding",
        ),
        (
            False,
            lambda candidate_row: candidate_row["manifest"]["serving_index"].update(
                provider_identifier_quarantine={}
            ),
            "quarantine is invalid",
        ),
        (
            False,
            lambda candidate_row: candidate_row["layout_manifest"].update(
                serving_index={
                    **candidate_row["layout_manifest"]["serving_index"],
                    "source_witness": {},
                }
            ),
            "source witness changed",
        ),
        (
            False,
            lambda candidate_row: candidate_row.update(snapshot_id=""),
            "exact strict",
        ),
        (
            False,
            lambda candidate_row: candidate_row["manifest"]["activation"].update(
                source_key=""
            ),
            "source scope is incomplete",
        ),
        (
            False,
            lambda candidate_row: candidate_row.update(
                previous_snapshot_id="other"
            ),
            "predecessor binding",
        ),
        (
            True,
            lambda candidate_row: candidate_row["manifest"]["activation"].update(
                mode="wrong"
            ),
            "cannot be corroborated",
        ),
        (
            False,
            lambda candidate_row: candidate_row.update(status="draft"),
            "not validated",
        ),
        (
            False,
            lambda candidate_row: candidate_row.update(
                current_snapshot_id="candidate-snapshot"
            ),
            "not validated",
        ),
        (
            False,
            lambda candidate_row: candidate_row.update(
                current_snapshot_id="new-current"
            ),
            "not validated",
        ),
        (
            True,
            lambda candidate_row: candidate_row.update(audit_report=None),
            "no corroborating audit report",
        ),
    ),
)
def test_candidate_target_binding_edges(
    activated,
    candidate_mutator,
    message,
):
    candidate_row_by_field = _candidate_row(activated=activated)
    candidate_mutator(candidate_row_by_field)

    with pytest.raises(ValueError, match=message):
        ptg_candidate_audit._candidate_target_from_row(
            candidate_row_by_field,
            candidate_run_id="ptg2:derived-import",
            raw_container_sha256=(RAW_DIGEST,),
        )


@pytest.mark.parametrize(
    "configuration_case",
    ("url", "token", "header", "trusted"),
)
def test_audit_configuration_security_edges(monkeypatch, configuration_case):
    message_by_case = {
        "url": ptg_candidate_audit.API_BASE_URL_ENV,
        "token": "HLTHPRT_CONTROL_API_TOKEN",
        "header": "auth header",
        "trusted": ptg_candidate_audit.TRUSTED_CLUSTER_HTTP_ENV,
    }
    monkeypatch.delenv("PTG_AUDIT_API_BASE_URL", raising=False)
    monkeypatch.delenv(ptg_candidate_audit.AUTH_SCHEME_ENV, raising=False)
    monkeypatch.setenv(
        ptg_candidate_audit.API_BASE_URL_ENV,
        "https://candidate.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "token")
    monkeypatch.setenv(ptg_candidate_audit.AUTH_HEADER_ENV, "Authorization")
    monkeypatch.setenv(ptg_candidate_audit.TRUSTED_CLUSTER_HTTP_ENV, "false")

    if configuration_case == "url":
        monkeypatch.delenv(ptg_candidate_audit.API_BASE_URL_ENV)
    elif configuration_case == "token":
        monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    elif configuration_case == "header":
        monkeypatch.setenv(ptg_candidate_audit.AUTH_HEADER_ENV, "bad\nheader")
    else:
        monkeypatch.setenv(
            ptg_candidate_audit.TRUSTED_CLUSTER_HTTP_ENV,
            "maybe",
        )

    with pytest.raises(ValueError, match=message_by_case[configuration_case]):
        ptg_candidate_audit._audit_configuration("candidate-snapshot")


def test_audit_summary_timing_edges():
    empty_summary_by_field = ptg_candidate_audit._audit_summary({}, "d" * 64)
    assert empty_summary_by_field["audit_timings"] == {}

    batch_summary_by_field = ptg_candidate_audit._audit_summary(
        {
            "duration_seconds": True,
            "latency": {"request_p50_ms": True},
            "batch": {"endpoint_duration_ms": 1.0},
        },
        "d" * 64,
    )
    assert batch_summary_by_field["audit_timings"] == {
        "endpoint_duration_ms": 1.0
    }


def test_audit_summary_projects_partition_request_accounting():
    summary_by_field = ptg_candidate_audit._audit_summary(
        _passing_batch_report(),
        "d" * 64,
    )

    assert summary_by_field["audit_counts"] == {
        "source_witnesses": 11_000,
        "source_occurrence_witnesses_matched": 10_000,
        "unique_source_conditions_executed": 9_000,
        "provider_witnesses_validated": 1_000,
        "persisted_audit_occurrences_validated": 2_560,
        "batch_requests_executed": 1,
        "batch_api_planned_http_requests": 1,
        "batch_api_actual_http_requests": 1,
        "batch_api_completed_http_requests": 1,
        "batch_api_failed_http_requests": 0,
        "retry_count": 0,
        "max_concurrency": 1,
    }
    assert summary_by_field["audit_timings"] == {
        "duration_seconds": 12.5,
        "endpoint_duration_ms": 12_000.0,
    }


def test_candidate_audit_defaults_follow_current_attestation_writer():
    assert not hasattr(ptg_candidate_audit, "resolve_retained_raw_files")
    assert ptg_candidate_audit.PTG2_BATCH_AUDIT_WRITER_ENABLED is True
    assert (
        ptg_candidate_audit.PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT
        == ptg_candidate_audit.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
    )


@pytest.mark.asyncio
async def test_legacy_v3_release_audit_remains_explicitly_callable(
    monkeypatch,
):
    expected_report = _passing_report()
    runner = AsyncMock(return_value=expected_report)
    witness = Mock(occurrence_records=())
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "http://candidate-api.default.svc.cluster.local:8080",
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP",
        "true",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(ptg_candidate_audit, "run_fast_candidate_audit", runner)

    report = await ptg_candidate_audit.run_release_audit(_target(), witness)

    assert report is expected_report
    kwargs = runner.await_args.kwargs
    assert kwargs["witness"] is witness
    assert kwargs["audit_target"].source_set_digest == SOURCE_SET_DIGEST
    assert kwargs["http"].concurrency == 32
    assert kwargs["http"].headers["User-Agent"] == (
        "ptg2-v3-fast-candidate-audit/1.0"
    )


def test_release_gate_and_quarantine_evidence_fail_closed():
    with pytest.raises(
        ptg_candidate_audit.CandidateAuditReleaseGateError,
        match="did not pass",
    ):
        ptg_candidate_audit._require_passing_audit_report(
            {"status": "fail", "release_gate_eligible": False}
        )

    for quarantine in ({}, NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE):
        with pytest.raises(
            ptg_candidate_audit.CandidateAuditReleaseGateError,
            match="quarantine",
        ):
            ptg_candidate_audit._require_v3_quarantine_match(
                {"source": {"provider_identifier_quarantine": quarantine}},
                _target(),
            )

    for quarantine in (
        {},
        provider_identifier_quarantine_evidence(
            NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
        ),
    ):
        with pytest.raises(
            ptg_candidate_audit.CandidateAuditReleaseGateError,
            match="quarantine",
        ):
            ptg_candidate_audit._require_v4_quarantine_match(
                {"source": {"provider_identifier_quarantine": quarantine}},
                _target(),
            )


@pytest.mark.asyncio
async def test_legacy_release_audit_translates_scanner_failure(monkeypatch):
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_fast_candidate_audit",
        AsyncMock(
            side_effect=ptg_candidate_audit.FastCandidateAuditError(
                "api_contract_mismatch"
            )
        ),
    )

    with pytest.raises(
        ptg_candidate_audit.CandidateAuditReleaseGateError,
        match="api_contract_mismatch",
    ):
        await ptg_candidate_audit.run_release_audit(
            _target(),
            object(),
            http_config=object(),
        )


@pytest.mark.asyncio
async def test_release_audit_loads_once_and_uses_partitioned_http_configuration(
    monkeypatch,
):
    expected_report = _passing_batch_report()
    runner = AsyncMock(return_value=expected_report)
    witness = object()
    persisted_sample = object()
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "http://candidate-api.default.svc.cluster.local:8080",
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_TRUSTED_CLUSTER_HTTP",
        "true",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    witness_loader = AsyncMock(return_value=witness)
    sample_loader = AsyncMock(return_value=persisted_sample)
    monkeypatch.setattr(ptg_candidate_audit, "load_shared_source_witness", witness_loader)
    monkeypatch.setattr(ptg_candidate_audit, "load_persisted_audit_sample", sample_loader)
    monkeypatch.setattr(ptg_candidate_audit, "run_partitioned_candidate_audit", runner)

    progress_callback = AsyncMock()
    candidate_target = replace(
        _target(),
        storage_generation="shared_blocks_v4",
    )
    report = await ptg_candidate_audit.run_batch_release_audit(
        candidate_target,
        progress_callback=progress_callback,
    )

    assert report is expected_report
    kwargs = runner.await_args.kwargs
    assert kwargs["audit_target"].snapshot_id == "candidate-snapshot"
    assert kwargs["audit_target"].raw_container_sha256 == (RAW_DIGEST,)
    assert kwargs["audit_target"].storage_generation == "shared_blocks_v4"
    assert kwargs["audit_target"].source_witness["payload_sha256"] == (
        SOURCE_WITNESS_DIGEST
    )
    assert kwargs["witness"] is witness
    assert kwargs["persisted_sample"] is persisted_sample
    assert kwargs["progress_callback"] is progress_callback
    assert kwargs["http_config"].concurrency == 2
    assert kwargs["http_config"].deadline_seconds == 55.0
    assert kwargs["http_config"].verify_tls is False
    assert kwargs["http_config"].require_uvloop is True
    assert kwargs["http_config"].headers["Authorization"] == (
        "Bearer public-control-token"
    )
    assert kwargs["http_config"].headers["User-Agent"] == (
        "ptg2-v3-partitioned-candidate-audit/4.1"
    )
    witness_loader.assert_awaited_once()
    sample_loader.assert_awaited_once()


@pytest.mark.asyncio
async def test_release_audit_accepts_exact_nonempty_provider_quarantine(monkeypatch):
    expected_report = _passing_batch_report(
        provider_identifier_quarantine=NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )
    runner = AsyncMock(return_value=expected_report)
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_persisted_audit_sample",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(ptg_candidate_audit, "run_partitioned_candidate_audit", runner)

    report = await ptg_candidate_audit.run_batch_release_audit(
        _target(
            provider_identifier_quarantine=(
                NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
            )
        )
    )

    assert report is expected_report
    assert (
        runner.await_args.kwargs["audit_target"].provider_identifier_quarantine
        == NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )


@pytest.mark.asyncio
async def test_release_audit_failure_is_deterministic_and_not_retryable(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_persisted_audit_sample",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_partitioned_candidate_audit",
        AsyncMock(
            side_effect=ptg_candidate_audit.BatchCandidateAuditContractError(
                "source_witness_missing_from_api"
            )
        ),
    )

    with pytest.raises(ptg_candidate_audit.CandidateAuditReleaseGateError) as exc_info:
        await ptg_candidate_audit.run_batch_release_audit(_target())

    assert str(exc_info.value).endswith("source_witness_missing_from_api")
    assert exc_info.value.control_error_code == "ptg_candidate_audit_release_gate_failed"
    assert exc_info.value.retryable is False


@pytest.mark.asyncio
async def test_release_audit_transport_failure_requires_explicit_retry(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "https://public-api.internal.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_persisted_audit_sample",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_partitioned_candidate_audit",
        AsyncMock(
            side_effect=ptg_candidate_audit.BatchCandidateAuditTransportError(
                "batch_endpoint_transport_failed"
            )
        ),
    )

    with pytest.raises(ptg_candidate_audit.CandidateAuditTransportError) as exc_info:
        await ptg_candidate_audit.run_batch_release_audit(_target())

    assert exc_info.value.control_error_code == "ptg_candidate_audit_transport_failed"
    assert exc_info.value.retryable is False


def test_release_audit_rejects_untrusted_plain_http_configuration(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PTG2_CANDIDATE_AUDIT_API_BASE_URL",
        "http://public-api.example",
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "public-control-token")

    with pytest.raises(ValueError, match="verified HTTPS or explicit cluster HTTP"):
        ptg_candidate_audit._audit_configuration("candidate-snapshot")


@pytest.mark.asyncio
async def test_progress_without_control_run_is_a_no_op(monkeypatch):
    mark_control_run = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "mark_control_run", mark_control_run)

    await ptg_candidate_audit._progress(
        None,
        snapshot_id="candidate-snapshot",
        phase="candidate release audit",
        message="ignored",
        pct=20,
    )

    mark_control_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_progress_reports_phase_for_control_run(monkeypatch):
    mark_control_run = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "mark_control_run", mark_control_run)

    await ptg_candidate_audit._progress(
        "control-run",
        snapshot_id="candidate-snapshot",
        phase="candidate release audit",
        message="submitting two requests per second",
        pct=20,
    )

    assert mark_control_run.await_args.kwargs["progress"] == {
        "unit": "phase",
        "done": 20,
        "total": 100,
        "pct": 20,
        "message": "submitting two requests per second",
        "phase": "candidate release audit",
    }


@pytest.mark.asyncio
async def test_partition_progress_reports_exact_counters_within_audit_band(
    monkeypatch,
):
    mark_control_run = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "mark_control_run", mark_control_run)

    await ptg_candidate_audit._partition_progress(
        "control-run",
        snapshot_id="candidate-snapshot",
        completed=3,
        total=4,
    )

    assert mark_control_run.await_args.kwargs["progress"] == {
        "unit": "partition",
        "done": 3,
        "total": 4,
        "pct": 68,
        "message": "completed 3 of 4 audit partitions",
        "phase": "candidate release audit",
    }


@pytest.mark.asyncio
async def test_partition_progress_without_control_run_is_a_noop(monkeypatch):
    mark_control_run = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "mark_control_run", mark_control_run)

    await ptg_candidate_audit._partition_progress(
        None,
        snapshot_id="candidate-snapshot",
        completed=0,
        total=0,
    )

    mark_control_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_rolling_v3_writer_path_loads_witness_once(monkeypatch):
    witness = Mock(occurrence_records=(object(), object()))
    report = _passing_report()
    http_config = object()
    witness_loader = AsyncMock(return_value=witness)
    audit = AsyncMock(return_value=report)
    progress = AsyncMock()
    monkeypatch.setattr(
        ptg_candidate_audit,
        "PTG2_BATCH_AUDIT_WRITER_ENABLED",
        False,
    )
    monkeypatch.setattr(ptg_candidate_audit, "_progress", progress)
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        witness_loader,
    )
    monkeypatch.setattr(ptg_candidate_audit, "run_release_audit", audit)

    audit_report = await ptg_candidate_audit._execute_release_audit(
        _target(),
        control_run_id="control-run",
        http_config=http_config,
    )

    assert audit_report is report
    witness_loader.assert_awaited_once()
    audit.assert_awaited_once_with(_target(), witness, http_config=http_config)
    assert progress.await_count == 2


@pytest.mark.asyncio
async def test_default_v4_audit_attests_then_activates(
    monkeypatch,
):
    events: list[str] = []
    report = _passing_batch_report(
        provider_identifier_quarantine=NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
    )
    witness_loader = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        witness_loader,
    )

    async def audit(target, *, http_config, progress_callback):
        events.append("audit")
        assert target.snapshot_key == 17
        assert http_config is None
        assert progress_callback is not None
        return report

    monkeypatch.setattr(ptg_candidate_audit, "run_batch_release_audit", audit)

    async def attest(**kwargs):
        events.append("attest")
        assert kwargs["plan_id"] == "12-3456789"
        assert kwargs["source_key"] == "derived-source"
        assert kwargs["storage_generation"] == "shared_blocks_v3"
        assert kwargs["report"] is report
        return {"report_digest": "ef" * 32}

    async def promote(**kwargs):
        events.append("promote")
        assert kwargs["expected_current_snapshot_id"] == "previous-snapshot"
        return {"status": "promoted"}

    monkeypatch.setattr(ptg_candidate_audit, "record_candidate_audit_attestation", attest)
    monkeypatch.setattr(ptg_candidate_audit, "promote_ptg2_source_snapshot", promote)

    activation_response = await ptg_candidate_audit._audit_and_activate(
        _target(
            provider_identifier_quarantine=(
                NONEMPTY_PROVIDER_IDENTIFIER_QUARANTINE
            )
        ),
        control_run_id="control-run",
    )

    assert events == ["audit", "attest", "promote"]
    assert activation_response["arch_version"] == "postgres_binary_v3"
    assert activation_response["snapshot_status"] == "published"
    assert activation_response["activation_status"] == "activated"
    assert activation_response["audit_report_digest"] == "ef" * 32
    assert activation_response["audit_counts"][
        "batch_api_actual_http_requests"
    ] == 1
    assert activation_response["metrics"]["candidate_run_id"] == "ptg2:derived-import"
    witness_loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_v4_writer_path_avoids_local_witness_load(monkeypatch):
    expected_report = _passing_batch_report()
    batch_audit = AsyncMock(return_value=expected_report)
    partition_progress = AsyncMock()
    witness_loader = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_partition_progress",
        partition_progress,
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_batch_release_audit",
        batch_audit,
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_shared_source_witness",
        witness_loader,
    )

    report = await ptg_candidate_audit._execute_release_audit(
        _target(),
        control_run_id="control-run",
        http_config=None,
    )

    assert report is expected_report
    batch_audit.assert_awaited_once()
    progress_callback = batch_audit.await_args.kwargs["progress_callback"]
    await progress_callback(2, 4)
    partition_progress.assert_awaited_once_with(
        "control-run",
        snapshot_id="candidate-snapshot",
        completed=2,
        total=4,
    )
    witness_loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_failing_audit_never_attests_or_activates(monkeypatch):
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    witness_loader = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "load_shared_source_witness", witness_loader)
    monkeypatch.setattr(
        ptg_candidate_audit,
        "run_batch_release_audit",
        AsyncMock(
            side_effect=RuntimeError(
                "candidate release audit did not pass the release gate"
            )
        ),
    )
    attest = AsyncMock()
    promote = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "record_candidate_audit_attestation", attest)
    monkeypatch.setattr(ptg_candidate_audit, "promote_ptg2_source_snapshot", promote)

    with pytest.raises(RuntimeError, match="did not pass"):
        await ptg_candidate_audit._audit_and_activate(
            _target(),
            control_run_id="control-run",
        )

    attest.assert_not_awaited()
    promote.assert_not_awaited()
    witness_loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_activation_requires_completed_promotion(monkeypatch):
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_execute_release_audit",
        AsyncMock(return_value=_passing_batch_report()),
    )
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "record_candidate_audit_attestation",
        AsyncMock(return_value={"report_digest": "ef" * 32}),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "promote_ptg2_source_snapshot",
        AsyncMock(return_value={"status": "unchanged"}),
    )

    with pytest.raises(RuntimeError, match="promotion did not complete"):
        await ptg_candidate_audit._audit_and_activate(
            _target(),
            control_run_id="control-run",
        )


@pytest.mark.asyncio
async def test_main_requires_candidate_run_id():
    with pytest.raises(ValueError, match="candidate_run_id is required"):
        await ptg_candidate_audit.main(candidate_run_id="")


@pytest.mark.asyncio
async def test_main_runs_new_candidate_with_partitioned_configuration(monkeypatch):
    @asynccontextmanager
    async def guard(candidate_run_id):
        assert candidate_run_id == "ptg2:derived-import"
        yield

    candidate_target = _target()
    http_config = object()
    expected_activation_result_by_field = {"status": "activated"}
    audit = AsyncMock(return_value=expected_activation_result_by_field)
    configuration = Mock(return_value=http_config)
    monkeypatch.setattr(ptg_candidate_audit, "candidate_audit_guard", guard)
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_candidate_audit_target",
        AsyncMock(return_value=candidate_target),
    )
    monkeypatch.setattr(ptg_candidate_audit, "_audit_configuration", configuration)
    monkeypatch.setattr(ptg_candidate_audit, "_audit_and_activate", audit)

    activation_result = await ptg_candidate_audit.main(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
        run_id="control-run",
    )

    assert activation_result is expected_activation_result_by_field
    configuration.assert_called_once_with(
        "candidate-snapshot",
        batch_writer=True,
    )
    audit.assert_awaited_once_with(
        candidate_target,
        control_run_id="control-run",
        http_config=http_config,
    )


@pytest.mark.asyncio
async def test_already_active_redelivery_returns_corroborated_success(monkeypatch):
    @asynccontextmanager
    async def guard(_candidate_run_id):
        yield

    audit_configuration = Mock(side_effect=AssertionError("redelivery must not require API configuration"))
    monkeypatch.setattr(ptg_candidate_audit, "_audit_configuration", audit_configuration)
    monkeypatch.setattr(ptg_candidate_audit, "candidate_audit_guard", guard)
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_candidate_audit_target",
        AsyncMock(return_value=_target(activated=True)),
    )
    audit = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "_audit_and_activate", audit)
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())

    redelivery_response = await ptg_candidate_audit.main(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
        run_id="control-run",
    )

    assert redelivery_response["activation_status"] == "activated"
    assert redelivery_response["idempotent"] is True
    assert redelivery_response["audit_report_digest"] == "cd" * 32
    audit.assert_not_awaited()
    audit_configuration.assert_not_called()


@pytest.mark.asyncio
async def test_equivalent_current_redelivery_returns_reused_success(monkeypatch):
    @asynccontextmanager
    async def guard(_candidate_run_id):
        yield

    equivalent_target = replace(
        _target(),
        current_snapshot_id="active-snapshot",
        equivalent_current_snapshot_id="active-snapshot",
        equivalent_current_import_run_id="ptg2:active-import",
        equivalent_audit_report=_passing_report(),
        equivalent_audit_report_digest="cd" * 32,
    )
    audit_configuration = Mock(
        side_effect=AssertionError("equivalent reuse must not call the API")
    )
    audit = AsyncMock()
    monkeypatch.setattr(ptg_candidate_audit, "candidate_audit_guard", guard)
    monkeypatch.setattr(
        ptg_candidate_audit,
        "load_candidate_audit_target",
        AsyncMock(return_value=equivalent_target),
    )
    monkeypatch.setattr(
        ptg_candidate_audit,
        "_audit_configuration",
        audit_configuration,
    )
    monkeypatch.setattr(ptg_candidate_audit, "_audit_and_activate", audit)
    monkeypatch.setattr(ptg_candidate_audit, "_progress", AsyncMock())

    response = await ptg_candidate_audit.main(
        candidate_run_id="ptg2:derived-import",
        snapshot_id="candidate-snapshot",
        import_id="derived-import",
        run_id="control-run",
    )

    assert response["activation_status"] == "activated"
    assert response["snapshot_id"] == "active-snapshot"
    assert response["candidate_snapshot_id"] == "candidate-snapshot"
    assert response["candidate_run_id"] == "ptg2:derived-import"
    assert response["activated_import_run_id"] == "ptg2:active-import"
    assert response["activation_mode"] == "equivalent_current_layout"
    assert response["equivalent_reuse"] is True
    assert response["idempotent"] is True
    audit.assert_not_awaited()
    audit_configuration.assert_not_called()


def test_scheduler_result_is_redacted():
    scheduler_result_map = ptg_candidate_audit._success_result(
        _target(),
        report=_passing_report(),
        report_digest="ef" * 32,
        idempotent=False,
    )
    encoded = json.dumps(scheduler_result_map, sort_keys=True)

    assert scheduler_result_map["storage_generation"] == "shared_blocks_v3"
    assert "must-not-leak" not in encoded
    assert "derived-source" not in encoded
    assert "12-3456789" not in encoded
    assert "source_path" not in encoded
    assert "Authorization" not in encoded
    assert "response" not in encoded
    assert set(scheduler_result_map) == {
        "arch_version",
        "storage_generation",
        "snapshot_status",
        "activation_status",
        "snapshot_id",
        "candidate_snapshot_id",
        "import_run_id",
        "candidate_run_id",
        "activated_import_run_id",
        "activation_mode",
        "equivalent_reuse",
        "idempotent",
        "audit_report_digest",
        "audit_counts",
        "audit_timings",
        "metrics",
    }


class _AuditAutocommit:
    def __init__(self, acquired):
        self.acquired = acquired
        self.calls = []

    async def scalar(self, query, params):
        self.calls.append((query, params))
        return self.acquired if len(self.calls) == 1 else None


class _AuditConnection:
    def __init__(self, autocommit, awaitable):
        self.autocommit = autocommit
        self.awaitable = awaitable

    def execution_options(self, **_kwargs):
        if not self.awaitable:
            return self.autocommit

        async def resolved():
            return self.autocommit

        return resolved()


class _AuditConnectionContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _AuditEngine:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return _AuditConnectionContext(self.connection)


@pytest.mark.asyncio
async def test_candidate_audit_guard_covers_connection_protocols_and_rejection(
    monkeypatch,
):
    """Cover sync and async connection setup plus lock rejection."""

    good_autocommit = _AuditAutocommit(True)
    good_engine = _AuditEngine(_AuditConnection(good_autocommit, awaitable=True))
    monkeypatch.setattr(ptg_candidate_audit.db, "engine", None)

    async def connect():
        ptg_candidate_audit.db.engine = good_engine

    monkeypatch.setattr(ptg_candidate_audit.db, "connect", connect)
    async with ptg_candidate_audit.candidate_audit_guard("run-good"):
        assert ptg_candidate_audit.db.engine is good_engine
    assert len(good_autocommit.calls) == 2

    bad_autocommit = _AuditAutocommit(False)
    ptg_candidate_audit.db.engine = _AuditEngine(
        _AuditConnection(bad_autocommit, awaitable=False)
    )
    with pytest.raises(RuntimeError, match="was not acquired"):
        async with ptg_candidate_audit.candidate_audit_guard("run-bad"):
            pytest.fail("guard accepted a connection without an advisory lock")
    assert len(bad_autocommit.calls) == 1
