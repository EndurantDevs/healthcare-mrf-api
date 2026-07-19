# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import json
from copy import deepcopy

import pytest
from aiohttp import web

from process.ptg_parts import ptg2_batch_candidate_audit as batch_audit
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    BatchAuditReportInput,
    BatchAuditReportTarget,
    PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT,
    build_batch_audit_report,
    validate_batch_candidate_release_audit_report,
)
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchWitnessBinding,
    build_audit_batch_request,
    matched_audit_batch_digest,
    parse_audit_batch_response,
)
from process.ptg_parts.ptg2_candidate_audit_contract import FastAuditHttpConfig
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import source_set_digest


_RAW_DIGEST = "ab" * 32
_AUDIT_SAMPLE_DIGEST = "cd" * 32
_WITNESS_SAMPLE_DIGEST = "de" * 32
_WITNESS_PAYLOAD_DIGEST = "ef" * 32


def _source_witness() -> dict[str, object]:
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
        "queryable_occurrence_population_count": 2,
        "provider_population_count": 1,
        "emitted_rate_row_count": 2,
        "unqueryable_rate_row_count": 0,
        "occurrence_witness_count": 2,
        "provider_witness_count": 1,
        "record_count": 3,
        "evidence_dictionary_count": 2,
        "evidence_dictionary_raw_bytes": 10_000,
        "evidence_dictionary_stored_bytes": 5_000,
        "sample_digest": _WITNESS_SAMPLE_DIGEST,
        "payload_sha256": _WITNESS_PAYLOAD_DIGEST,
        "payload_bytes": 123_456,
        "compression": "per_record_zlib_shared_evidence_dictionary_v1",
    }


def _audit_sample() -> dict[str, object]:
    return {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 2,
        "maximum_rows": 2_560,
        "sample_digest": _AUDIT_SAMPLE_DIGEST,
        "source_count": 1,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "complete_population": False,
        "serving_multiplicity_semantics": "source_multiset_v1",
    }


def _target(
    *,
    source_witness: dict[str, object] | None = None,
    provider_identifier_quarantine: dict[str, object] | None = None,
) -> BatchAuditReportTarget:
    return BatchAuditReportTarget(
        snapshot_id="candidate-snapshot",
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
        raw_container_sha256=(_RAW_DIGEST,),
        source_witness=source_witness or _source_witness(),
        audit_sample=_audit_sample(),
        provider_identifier_quarantine=(
            provider_identifier_quarantine
            if provider_identifier_quarantine is not None
            else provider_identifier_quarantine_payload({})
        ),
    )


def _http_config(api_base_url: str) -> FastAuditHttpConfig:
    return FastAuditHttpConfig(
        api_base_url=api_base_url,
        headers={
            "Authorization": "Bearer secret",
            "X-HealthPorta-PTG-Candidate-Audit": "candidate-snapshot",
        },
        verify_tls=False,
        transport_contract="authenticated_cluster_service_v1",
        require_uvloop=False,
    )


def _block_io() -> dict[str, int]:
    return {
        "logical_block_deliveries": 7,
        "physical_mapping_references": 7,
        "physical_mapping_aliases": 2,
        "unique_physical_blocks": 5,
        "physical_block_reads": 5,
        "physical_block_decodes": 5,
        "physical_payload_preparations": 5,
        "expected_logical_payload_processes": 5,
        "logical_payload_processes": 5,
        "logical_payload_fragment_references": 5,
        "logical_payload_fragment_aliases": 0,
        "repeated_physical_reads": 0,
        "repeated_physical_decodes": 0,
        "repeated_physical_preparations": 0,
        "repeated_logical_payload_processes": 0,
        "peak_raw_bytes": 8_192,
    }


def _witness_io() -> dict[str, int]:
    return {
        "payload_reads": 1,
        "payload_decodes": 1,
        "record_decodes": 3,
        "unique_evidence_entries": 2,
        "evidence_decompressions": 2,
        "evidence_sha256_hashes": 2,
        "evidence_json_parses": 2,
        "evidence_reuse_deliveries": 1,
        "repeated_evidence_decompressions": 0,
        "repeated_evidence_sha256_hashes": 0,
        "repeated_evidence_json_parses": 0,
    }


def _candidate_processing_io() -> dict[str, int]:
    return {
        "candidate_occurrence_deliveries": 3,
        "unique_candidate_projections": 2,
        "candidate_projection_builds": 2,
        "candidate_projection_reuse_deliveries": 1,
        "repeated_candidate_projection_builds": 0,
        "availability_condition_count": 2,
        "duplicate_availability_deliveries": 0,
    }


def _response_payload(request_digest: str) -> dict[str, object]:
    return {
        "contract": "ptg2_v3_source_witness_batch_response_v1",
        "request_digest": request_digest,
        "challenge_count": 2,
        "unique_challenge_count": 2,
        "matched_challenge_count": 2,
        "persisted_audit_occurrence_count": 2,
        "validated_persisted_audit_occurrence_count": 2,
        "matched_challenge_digest": matched_audit_batch_digest(
            request_digest,
            2,
        ),
        "duration_ms": 12.5,
        "block_io": _block_io(),
        "witness_io": _witness_io(),
        "candidate_processing_io": _candidate_processing_io(),
    }


def _request(target: BatchAuditReportTarget | None = None):
    target = target or _target()
    witness = target.source_witness
    return build_audit_batch_request(
        snapshot_id=target.snapshot_id,
        source_key=target.source_key,
        plan_id=target.plan_id,
        plan_market_type=target.plan_market_type,
        witness_binding=AuditBatchWitnessBinding(
            audit_sample_digest=_AUDIT_SAMPLE_DIGEST,
            source_witness_sample_digest=_WITNESS_SAMPLE_DIGEST,
            source_witness_payload_sha256=_WITNESS_PAYLOAD_DIGEST,
            raw_container_sha256=(_RAW_DIGEST,),
            source_witness_occurrence_count=int(
                witness["occurrence_witness_count"]
            ),
        ),
    )


@pytest.mark.asyncio
async def test_batch_client_executes_exactly_one_authenticated_post(
    unused_tcp_port,
):
    observed_requests: list[dict[str, object]] = []

    async def handler(request: web.Request) -> web.Response:
        request_payload = await request.json()
        observed_requests.append(request_payload)
        assert request.method == "POST"
        assert request.headers["Authorization"] == "Bearer secret"
        assert request.headers["X-HealthPorta-PTG-Candidate-Audit"] == (
            "candidate-snapshot"
        )
        return web.json_response(_response_payload(request_payload["request_digest"]))

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        report = await batch_audit.run_batch_candidate_audit(
            audit_target=_target(),
            http_config=_http_config(f"http://127.0.0.1:{unused_tcp_port}"),
        )
    finally:
        await runner.cleanup()

    assert len(observed_requests) == 1
    assert report["http"]["batch_api_actual_http_requests"] == 1
    assert report["http"]["retry_count"] == 0
    assert report["checks"]["batch_requests_executed"] == 1
    assert _RAW_DIGEST not in json.dumps(report, sort_keys=True)


@pytest.mark.asyncio
async def test_batch_client_does_not_retry_transient_response(unused_tcp_port):
    observed_requests: list[web.Request] = []

    async def handler(request: web.Request) -> web.Response:
        observed_requests.append(request)
        return web.Response(status=503)

    application = web.Application()
    application.router.add_post(
        "/api/v1/pricing/providers/audit-source-witness-batch",
        handler,
    )
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", unused_tcp_port)
    await site.start()
    try:
        with pytest.raises(
            batch_audit.BatchCandidateAuditTransportError,
            match="temporarily_unavailable",
        ):
            await batch_audit.run_batch_candidate_audit(
                audit_target=_target(),
                http_config=_http_config(
                    f"http://127.0.0.1:{unused_tcp_port}"
                ),
            )
    finally:
        await runner.cleanup()

    assert len(observed_requests) == 1


def _v4_report(
    target: BatchAuditReportTarget | None = None,
) -> dict[str, object]:
    target = target or _target()
    request = _request(target)
    response = parse_audit_batch_response(
        _response_payload(request.request_digest),
        request=request,
        expected_source_witness=target.source_witness,
        expected_audit_sample=target.audit_sample,
    )
    completed_at = datetime.datetime.now(datetime.timezone.utc)
    return build_batch_audit_report(
        BatchAuditReportInput(
            target=target,
            request=request,
            response=response,
            http_config=FastAuditHttpConfig(
                api_base_url="https://candidate-api.internal.example",
                headers={},
                verify_tls=True,
                transport_contract="verified_https_v1",
            ),
            event_loop_contract="uvloop",
            started_at=completed_at - datetime.timedelta(seconds=1),
            completed_at=completed_at,
        )
    )


def test_v4_report_validates_one_request_and_once_only_ledgers():
    report = _v4_report()

    evidence = validate_batch_candidate_release_audit_report(
        report,
        snapshot_id="candidate-snapshot",
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    assert evidence["contract"] == PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT
    assert evidence["batch_api_actual_http_requests"] == 1
    assert evidence["checks"]["batch_requests_executed"] == 1
    assert evidence["source_witness_manifest"] == _source_witness()
    assert evidence["audit_sample_public"] == _audit_sample()
    assert report["batch"]["ordered_source_ordinal_digest"] == (
        _request().ordered_source_ordinal_digest
    )
    assert _RAW_DIGEST not in json.dumps(report, sort_keys=True)


def test_v4_report_rejects_forged_request_and_matched_digests():
    report = deepcopy(_v4_report())
    forged_request_digest = "0" * 64
    report["batch"]["request_digest"] = forged_request_digest
    report["batch"]["matched_challenge_digest"] = matched_audit_batch_digest(
        forged_request_digest,
        report["batch"]["matched_challenge_count"],
    )

    with pytest.raises(ValueError, match="request binding"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="derived-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_v4_report_rejects_changed_source_ordinal_digest():
    report = deepcopy(_v4_report())
    report["batch"]["ordered_source_ordinal_digest"] = "0" * 64

    with pytest.raises(ValueError, match="request binding"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="derived-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_v4_report_rejects_private_audit_sample_fields():
    report = deepcopy(_v4_report())
    report["api_audit_sample"]["private_source_identity"] = _RAW_DIGEST

    with pytest.raises(ValueError, match="api_audit_sample"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="derived-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_v4_report_excludes_source_witness_extensions():
    source_witness = _source_witness()
    source_witness["source_path"] = "/private/client/raw-rates.json.gz"

    report = _v4_report(_target(source_witness=source_witness))

    assert "source_path" not in report["source"]["witness"]
    assert "/private/client" not in json.dumps(report, sort_keys=True)
    validate_batch_candidate_release_audit_report(
        report,
        snapshot_id="candidate-snapshot",
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
    )


@pytest.mark.parametrize(
    "target_mapping",
    (
        ("source", "witness"),
        ("target",),
        ("redaction",),
    ),
)
def test_v4_report_rejects_sensitive_extension_fields(target_mapping):
    report = deepcopy(_v4_report())
    extension_target = report
    for field_name in target_mapping:
        extension_target = extension_target[field_name]
    extension_target["source_path"] = "/private/client/raw-rates.json.gz"

    with pytest.raises(ValueError, match="invalid"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="derived-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_v4_report_redacts_quarantined_provider_values():
    quarantine = provider_identifier_quarantine_payload({123456789: 2})

    report = _v4_report(
        _target(provider_identifier_quarantine=quarantine)
    )

    quarantine_evidence = report["source"]["provider_identifier_quarantine"]
    assert set(quarantine_evidence) == {
        "contract",
        "distinct_value_count",
        "occurrence_count",
        "sha256",
    }
    assert "123456789" not in json.dumps(report, sort_keys=True)


@pytest.mark.parametrize(
    ("section", "field_name", "invalid_value", "message"),
    (
        ("http", "batch_api_actual_http_requests", 2, "HTTP accounting"),
        ("batch", "matched_challenge_count", 1, "response binding"),
        ("block_io", "repeated_physical_reads", 1, "block_io_repeated"),
        ("block_io", "unique_physical_blocks", 0, "block_io_repeated"),
        ("witness_io", "record_decodes", 2, "witness I/O"),
    ),
)
def test_v4_report_rejects_false_request_or_repeat_claims(
    section,
    field_name,
    invalid_value,
    message,
):
    report = deepcopy(_v4_report())
    if section in {"block_io", "witness_io"}:
        target_mapping = report["io"][section]
    else:
        target_mapping = report[section]
    target_mapping[field_name] = invalid_value

    with pytest.raises(ValueError, match=message):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="derived-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_v4_report_binds_coverage_condition_count_to_batch_response():
    report = deepcopy(_v4_report())
    report["coverage"]["unique_source_condition_count"] = 1
    report["checks"]["unique_source_conditions_executed"] = 1

    with pytest.raises(ValueError, match="response binding"):
        validate_batch_candidate_release_audit_report(
            report,
            snapshot_id="candidate-snapshot",
            source_key="derived-source",
            plan_id="12-3456789",
            plan_market_type="group",
        )
