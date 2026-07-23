from __future__ import annotations

import datetime
from copy import deepcopy

import pytest

from process.ptg_parts import ptg2_partitioned_candidate_audit as audit
from process.ptg_parts import ptg2_partitioned_candidate_audit_contract as contract
from process.ptg_parts import (
    ptg2_batch_candidate_audit_report_sections as report_sections,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_report import (
    PartitionedAuditHttpMetrics,
    PartitionedAuditReportInput,
    build_partitioned_audit_report,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_report_validation import (
    validate_partitioned_http_metrics,
)
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportTarget,
    validate_batch_candidate_release_audit_report,
)
from process.ptg_parts.ptg2_candidate_audit_contract import FastAuditHttpConfig
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import source_set_digest


_RAW_DIGEST = "ab" * 32


def _source_witness():
    return {
        "contract": "ptg2_v3_source_witness_payload_v5",
        "format_version": 5,
        "selection_method": "bottom_k_independent_occurrence_provider_cohorts_v3",
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
        "source_count": 1,
        "source_set_digest": source_set_digest((_RAW_DIGEST,)),
        "occurrence_target": 10_000,
        "total_target": 11_000,
        "provider_quota": 1_000,
        "queryable_occurrence_population_count": 201,
        "provider_population_count": 1,
        "emitted_rate_row_count": 201,
        "unqueryable_rate_row_count": 0,
        "occurrence_witness_count": 201,
        "provider_witness_count": 1,
        "record_count": 202,
        "evidence_dictionary_count": 2,
        "evidence_dictionary_raw_bytes": 10_000,
        "evidence_dictionary_stored_bytes": 5_000,
        "sample_digest": "b" * 64,
        "payload_sha256": "c" * 64,
        "payload_bytes": 123_456,
        "compression": "per_record_zlib_shared_evidence_dictionary_v1",
    }


def _audit_sample():
    return {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 2,
        "maximum_rows": 2_560,
        "sample_digest": "a" * 64,
        "source_count": 1,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "complete_population": False,
        "serving_multiplicity_semantics": "source_multiset_v1",
    }


def _target():
    return BatchAuditReportTarget(
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
        raw_container_sha256=(_RAW_DIGEST,),
        source_witness=_source_witness(),
        audit_sample=_audit_sample(),
        provider_identifier_quarantine=provider_identifier_quarantine_payload(
            {}
        ),
    )


def _plan():
    source_challenges = tuple(
        contract.PartitionedSourceChallenge(
            ordinal=0,
            code_system="CPT",
            code="99213",
            npi=1_000_000_000 + index,
            source_artifact_key=0,
            tuple_digest=f"{index:064x}",
            network_name_digests=(),
            multiplicity=1,
        )
        for index in range(201)
    )
    persisted_occurrences = tuple(
        contract.PartitionedPersistedOccurrence(
            ordinal=0,
            occurrence_id=index.to_bytes(32, "big"),
            code_system="CPT",
            code="99213",
            code_key=7,
            provider_set_key=index,
            price_key=index,
            source_artifact_key=0,
            npi=2_000_000_000 + index,
            atom_ordinal=0,
            atom_key=index,
        )
        for index in (1, 2)
    )
    return contract.build_partitioned_candidate_audit_plan(
        binding=contract.PartitionedCandidateAuditBinding(
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
            audit_sample_digest="a" * 64,
            source_witness_sample_digest="b" * 64,
            source_witness_payload_sha256="c" * 64,
            ordered_source_ordinal_digest="d" * 64,
            source_occurrence_count=201,
            persisted_occurrence_count=2,
        ),
        source_challenges=source_challenges,
        persisted_occurrences=persisted_occurrences,
    )


def _block_io():
    return {
        "logical_block_deliveries": 1,
        "physical_mapping_references": 1,
        "physical_mapping_aliases": 0,
        "unique_physical_blocks": 1,
        "physical_block_reads": 1,
        "physical_block_decodes": 1,
        "physical_payload_preparations": 1,
        "expected_logical_payload_processes": 1,
        "logical_payload_processes": 1,
        "logical_payload_fragment_references": 1,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": 1024,
    }


def _candidate_io(request):
    count = len(request.source_challenges)
    return {
        "candidate_occurrence_deliveries": count,
        "unique_candidate_projections": count,
        "candidate_projection_builds": count,
        "candidate_projection_reuse_deliveries": 0,
        "repeated_candidate_projection_builds": 0,
        "availability_condition_count": count,
        "duplicate_availability_deliveries": 0,
    }


def _report():
    plan = _plan()
    partition_results = tuple(
        contract.build_partitioned_candidate_audit_result(
            request=request,
            matched_source_occurrence_count=request.source_occurrence_count,
            validated_persisted_occurrence_count=len(
                request.persisted_occurrences
            ),
            duration_ms=100,
            block_io=_block_io(),
            candidate_processing_io=_candidate_io(request),
        )
        for request in plan.requests
    )
    aggregate = contract.validate_partitioned_candidate_audit_results(
        plan,
        partition_results,
    )
    metrics = audit.PartitionedAuditHttpMetrics(
        planned_request_count=5,
        started_request_count=5,
        completed_request_count=5,
        peak_in_flight=2,
        start_times=[0.0, 0.5, 1.0, 1.5, 2.0],
    )
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    return build_partitioned_audit_report(
        PartitionedAuditReportInput(
            audit_target=_target(),
            plan=plan,
            aggregate=aggregate,
            metrics=metrics,
            http_config=FastAuditHttpConfig(
                api_base_url="https://candidate-api.internal.example",
                headers={},
                verify_tls=True,
                transport_contract="verified_https_v1",
            ),
            witness_io={
                "payload_reads": 1,
                "payload_decodes": 1,
                "record_decodes": 202,
                "unique_evidence_entries": 2,
                "evidence_decompressions": 2,
                "evidence_sha256_hashes": 2,
                "evidence_json_parses": 2,
                "evidence_reuse_deliveries": 200,
                "repeated_evidence_decompressions": 0,
                "repeated_evidence_sha256_hashes": 0,
                "repeated_evidence_json_parses": 0,
            },
            event_loop_contract="uvloop",
            started_at=completed_at - datetime.timedelta(seconds=2),
            completed_at=completed_at,
        )
    )


def test_partitioned_report_validates_dynamic_request_count_and_wall_time():
    report = _report()

    evidence = validate_batch_candidate_release_audit_report(
        report,
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    assert report["checks"]["batch_requests_executed"] == 5
    assert report["http"]["batch_api_actual_http_requests"] == 5
    assert report["duration_seconds"] == 2
    assert evidence["batch_api_actual_http_requests"] == 5


def test_partitioned_report_validates_truthful_slow_start_span():
    report = deepcopy(_report())
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    report["started_at"] = (
        completed_at - datetime.timedelta(seconds=106)
    ).isoformat()
    report["completed_at"] = completed_at.isoformat()
    report["duration_seconds"] = 106
    report["http"]["request_start_span_seconds"] = 90
    report["http"]["request_start_rate_actual_per_second"] = round(
        (report["checks"]["batch_requests_executed"] - 1) / 90,
        6,
    )

    evidence = validate_batch_candidate_release_audit_report(
        report,
        snapshot_id="candidate-snapshot",
        source_key="test-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    assert evidence["batch_api_actual_http_requests"] == 5


def _partitioned_timing_report(
    *,
    duration_seconds=916,
    request_start_span_seconds=900,
):
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    return {
        "started_at": (
            completed_at - datetime.timedelta(seconds=duration_seconds)
        ).isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": duration_seconds,
        "batch": {
            "request_contract": (
                report_sections
                .PTG2_PARTITIONED_CANDIDATE_AUDIT_REQUEST_CONTRACT
            ),
        },
        "checks": {"batch_requests_executed": 243},
        "http": {
            "request_start_span_seconds": request_start_span_seconds,
        },
    }, completed_at


def test_partitioned_report_timing_uses_measured_request_start_span():
    report_by_field, completed_at = _partitioned_timing_report()

    assert report_sections.validate_report_timing(
        report_by_field,
        evaluated_at=completed_at,
    ) == completed_at


@pytest.mark.parametrize(
    ("duration_seconds", "request_start_span_seconds"),
    (
        (961, 900),
        (100, 102),
        (20_000, 19_950),
        (100, True),
        (100, -1),
        (100, float("inf")),
        (100, float("nan")),
    ),
)
def test_partitioned_report_timing_rejects_invalid_duration_envelope(
    duration_seconds,
    request_start_span_seconds,
):
    report_by_field, completed_at = _partitioned_timing_report(
        duration_seconds=duration_seconds,
        request_start_span_seconds=request_start_span_seconds,
    )

    with pytest.raises(ValueError, match="timing is invalid"):
        report_sections.validate_report_timing(
            report_by_field,
            evaluated_at=completed_at,
        )


def test_partitioned_report_rejects_forged_request_count():
    report = deepcopy(_report())
    report["http"]["batch_api_actual_http_requests"] = 2

    with pytest.raises(ValueError, match="HTTP accounting"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_partitioned_report_rejects_invalid_response_binding():
    report = deepcopy(_report())
    report["batch"]["response_contract"] = "invalid"

    with pytest.raises(ValueError, match="response binding"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_http_metrics_report_zero_rate_until_two_distinct_starts_exist():
    metrics = PartitionedAuditHttpMetrics(planned_request_count=1)
    assert metrics.start_span_seconds == 0
    assert metrics.actual_start_rate_per_second == 0

    metrics.start_times[:] = [1.0, 1.0]
    assert metrics.actual_start_rate_per_second == 0


def test_single_request_partitioned_http_metrics_require_zero_start_span():
    report = deepcopy(_report())
    for field_name in (
        "batch_api_planned_http_requests",
        "batch_api_actual_http_requests",
        "batch_api_completed_http_requests",
    ):
        report["http"][field_name] = 1
    report["http"]["max_concurrency"] = 1
    report["http"]["request_start_rate_actual_per_second"] = 0
    report["http"]["request_start_span_seconds"] = 0.5

    with pytest.raises(ValueError, match="HTTP accounting"):
        validate_partitioned_http_metrics(
            report["http"],
            expected_request_count=1,
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("max_concurrency", 0),
        ("request_start_rate_actual_per_second", 2.2),
        ("request_start_rate_actual_per_second", 1.5),
        ("request_start_rate_actual_per_second", True),
        ("request_start_span_seconds", 0),
        ("request_start_span_seconds", 0.1),
    ],
)
def test_partitioned_report_rejects_invalid_concurrency_or_start_pacing(
    field_name,
    invalid_value,
):
    report = deepcopy(_report())
    report["http"][field_name] = invalid_value

    with pytest.raises(ValueError, match="HTTP accounting"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="test-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )
