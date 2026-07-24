# See LICENSE.

from __future__ import annotations

import asyncio
import datetime
import hashlib
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_candidate_attestation, source_pointers
from process.ptg_parts.ptg2_candidate_audit_contract import (
    PTG2_FAST_AUDIT_CONTRACT,
    PTG2_FAST_AUDIT_TOOL_VERSION,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
)
from tests.ptg2_attestation_compat_test_support import (
    writer_evidence_by_field,
    writer_identity_by_field,
    writer_report_by_field,
)


EMPTY_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload({})
MALFORMED_PROVIDER_IDENTIFIER_QUARANTINE = provider_identifier_quarantine_payload(
    {123456789: 1}
)


@pytest.fixture(autouse=True)
def _isolate_attempt_fence(monkeypatch):
    """Keep candidate-contract tests focused on their existing boundary."""

    monkeypatch.setattr(
        source_pointers,
        "lock_writable_snapshot",
        AsyncMock(),
    )


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _source_witness(source_set):
    return {
        "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
        "format_version": 5,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
        "source_count": source_set["source_count"],
        "source_set_digest": source_set["raw_container_sha256_digest"],
        "occurrence_target": PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
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
        "sample_digest": "cd" * 32,
        "payload_sha256": (b"w" * 32).hex(),
        "payload_bytes": 1024,
        "compression": "per_record_zlib_shared_evidence_dictionary_v1",
    }


def _audit_sample(sample_digest: str) -> dict[str, object]:
    return {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 1,
        "maximum_rows": 2_560,
        "sample_digest": sample_digest,
        "source_count": 1,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "complete_population": False,
        "serving_multiplicity_semantics": "source_multiset_v1",
    }


def _release_report(**target_overrides):
    """Support the release report test fixture."""
    completed_at = datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0
    )
    started_at = completed_at - datetime.timedelta(seconds=30)
    target_map = {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": "shared_blocks_v3",
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_path_sha256": _sha256(
            "/api/v1/pricing/providers/audit-search-by-procedure"
        ),
        "api_audit_path_sha256": _sha256(
            "/api/v1/pricing/providers/audit-occurrences"
        ),
        "endpoint_contract": "pricing.providers.search_by_procedure",
        "audit_endpoint_contract": "persisted_served_occurrence_sample_v2",
        "snapshot_id_sha256": _sha256("snap_new"),
        "source_key_sha256": _sha256("source_a"),
        "plan_id_sha256": _sha256("12-3456789"),
        "market_type_sha256": _sha256("group"),
        "tls_verified": True,
        "transport_contract": "verified_https_v1",
    }
    target_map.update(target_overrides)
    return {
        "schema_version": 3,
        "harness": {
            "name": "ptg2_v3_fast_source_witness_audit",
            "version": PTG2_FAST_AUDIT_TOOL_VERSION,
            "contract": PTG2_FAST_AUDIT_CONTRACT,
        },
        "runtime": {"http_client": "aiohttp", "event_loop": "uvloop"},
        "status": "pass",
        "profile": "release",
        "release_profile_enforced": True,
        "release_gate_eligible": True,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": 30.0,
        "target": target_map,
        "reproducibility": {},
        "source": {
            "source_count": 1,
            "source_set_digest": (b"s" * 32).hex(),
            "witness": _source_witness(
                {
                    "source_count": 1,
                    "raw_container_sha256_digest": (b"s" * 32).hex(),
                }
            ),
            "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
        },
        "coverage": {
            "failures": [],
            "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
            "queryable_occurrence_population_count": 50_000,
            "emitted_rate_row_count": 1_000,
            "unqueryable_rate_row_count": 0,
            "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
            "occurrence_sample_count": 10_000,
            "provider_sample_count": 1_000,
        },
        "checks": {
            "source_witnesses": 11_000,
            "api_witnesses_matched": 10_000,
            "api_challenges_executed": 10_000,
            "provider_witnesses_validated": 1_000,
            "api_audit_occurrences_validated": 1,
        },
        "http": {
            "standard_api_actual_http_requests": 10_001,
            "retry_count": 0,
            "max_concurrency": 32,
        },
        "random_api_requests": {"requested": 10_000, "executed": 10_000},
        "latency": {
            "request_p50_ms": 100.0,
            "request_p95_ms": 250.0,
            "request_max_ms": 300.0,
            "request_p95_ceiling_ms": 250.0,
            "request_p95_within_ceiling": True,
        },
        "api_audit_sample": {
            "sample_digest": "ab" * 32,
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "failures": {"counts": {}, "examples": []},
        "limitations": [],
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": [
                "source_paths",
                "source_file_names",
                "raw_source_hashes",
                "source_trace_URLs",
                "plan_and_snapshot_values",
                "auth_values",
                "HTTP_bodies",
                "network_names",
                "arbitrary_source_and_API_strings",
            ],
        },
    }


def test_release_report_validation_is_exact_and_deterministic():
    report = _release_report()
    first = ptg2_candidate_attestation.validate_candidate_release_audit_report(
        report,
        snapshot_id="snap_new",
        source_key="source_a",
        plan_id="12-3456789",
        plan_market_type="group",
    )
    second = ptg2_candidate_attestation.validate_candidate_release_audit_report(
        dict(reversed(list(report.items()))),
        snapshot_id="snap_new",
        source_key="source_a",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    assert len(first["report_digest"]) == 32
    assert first["report_digest"] == second["report_digest"]
    assert first["checks"]["source_witnesses"] == 11_000
    assert first["standard_api_actual_http_requests"] == 10_001


def test_release_report_rejects_a_missing_top_level_section():
    report = _release_report()
    report.pop("failures")

    with pytest.raises(ValueError, match="missing=failures"):
        ptg2_candidate_attestation._v3_report_sections(report)


def test_release_report_rejects_non_uvloop_or_non_aiohttp_runtime():
    report = _release_report()
    report["runtime"] = {"http_client": "httpx", "event_loop": "asyncio"}

    with pytest.raises(ValueError, match="async runtime"):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            report,
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
        )


def test_release_report_accepts_explicit_authenticated_cluster_transport():
    report = _release_report(
        tls_verified=False,
        transport_contract="authenticated_cluster_service_v1",
    )

    result = ptg2_candidate_attestation.validate_candidate_release_audit_report(
        report,
        snapshot_id="snap_new",
        source_key="source_a",
        plan_id="12-3456789",
        plan_market_type="group",
    )

    assert len(result["report_digest"]) == 32


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda report: report["target"].update(expected_snapshot_lifecycle="published"), "target"),
        (lambda report: report["checks"].update(source_witnesses=10_999), "below"),
        (lambda report: report["failures"]["counts"].update(altered=1), "release gate"),
        (lambda report: report["api_audit_sample"].update(source_set_validated=False), "coverage"),
        (lambda report: report["random_api_requests"].update(executed=9_999), "coverage"),
        (lambda report: report["checks"].update(source_witnesses="11000"), "invalid"),
        (lambda report: report["api_audit_sample"].update(sample_digest="bad"), "sample_digest"),
        (lambda report: report["redaction"]["excluded"].pop(), "redaction"),
        (lambda report: report["target"].update(transport_contract=None), "transport"),
        (lambda report: report["target"].update(tls_verified=False), "transport"),
        (lambda report: report["latency"].update(request_p95_ms=250.001), "latency"),
        (lambda report: report["latency"].update(request_p95_ceiling_ms=251.0), "latency"),
        (lambda report: report["latency"].update(request_p95_within_ceiling=False), "latency"),
        (lambda report: report.update(unexpected_sensitive_value="no"), "fields"),
    ],
)
def test_release_report_validation_fails_closed(mutation, message):
    report = _release_report()
    mutation(report)
    with pytest.raises(ValueError, match=message):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            report,
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
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
def test_attestation_mapping_edges(mapping_value, expected_mapping):
    assert ptg2_candidate_attestation._mapping(mapping_value) == expected_mapping


def test_attestation_row_mapping_edges():
    driver_row = Mock()
    driver_row._mapping = {"value": 3}

    assert ptg2_candidate_attestation._row_mapping(None) == {}
    assert ptg2_candidate_attestation._row_mapping({"value": 1}) == {"value": 1}
    assert ptg2_candidate_attestation._row_mapping(driver_row) == {"value": 3}
    assert ptg2_candidate_attestation._row_mapping((("value", 4),)) == {
        "value": 4
    }


@pytest.mark.parametrize(
    ("timestamp_value", "message"),
    (
        (None, "is invalid"),
        ("not-a-timestamp", "is invalid"),
        ("2026-07-19T12:00:00", "must include a timezone"),
    ),
)
def test_attestation_timestamp_edges(timestamp_value, message):
    with pytest.raises(ValueError, match=message):
        ptg2_candidate_attestation._report_timestamp(
            timestamp_value,
            field="time",
        )


def test_v3_report_time_normalizes_clock():
    report_by_field = _release_report()
    completed_at = datetime.datetime.fromisoformat(report_by_field["completed_at"])

    assert ptg2_candidate_attestation._validated_v3_report_time(
        report_by_field,
        completed_at.replace(tzinfo=None),
    ) == completed_at


@pytest.mark.parametrize(
    ("report_mutator", "evaluation_delta", "message"),
    (
        (
            lambda report_by_field: report_by_field.update(duration_seconds=True),
            datetime.timedelta(),
            "timing is invalid",
        ),
        (
            lambda _report_by_field: None,
            datetime.timedelta(
                seconds=(
                    ptg2_candidate_attestation
                    .PTG2_CANDIDATE_AUDIT_REPORT_FUTURE_SKEW_SECONDS
                    + 1
                )
            ),
            "future",
        ),
    ),
)
def test_v3_report_time_edges(report_mutator, evaluation_delta, message):
    report_by_field = _release_report()
    report_mutator(report_by_field)
    completed_at = datetime.datetime.fromisoformat(report_by_field["completed_at"])

    with pytest.raises(ValueError, match=message):
        ptg2_candidate_attestation._validated_v3_report_time(
            report_by_field,
            completed_at - evaluation_delta,
        )


@pytest.mark.parametrize(
    ("section_name", "field_name", "invalid_value"),
    (
        (None, "schema_version", 4),
        ("harness", "name", "wrong"),
        ("harness", "contract", "wrong"),
        ("harness", "version", "wrong"),
    ),
)
def test_v3_tool_identity_edges(section_name, field_name, invalid_value):
    report_by_field = _release_report()
    mutation_mapping = (
        report_by_field
        if section_name is None
        else report_by_field[section_name]
    )
    mutation_mapping[field_name] = invalid_value

    with pytest.raises(ValueError):
        ptg2_candidate_attestation._validated_v3_tool_version(
            ptg2_candidate_attestation._v3_report_sections(report_by_field)
        )


@pytest.mark.asyncio
async def test_database_timestamp_type_edge():
    timestamp_result = Mock()
    timestamp_result.scalar_one.return_value = "not-a-timestamp"
    session = Mock()
    session.execute = AsyncMock(return_value=timestamp_result)

    with pytest.raises(RuntimeError, match="did not return"):
        await ptg2_candidate_attestation._database_timestamp(session)


@pytest.mark.parametrize(
    "database_timestamp",
    (
        datetime.datetime(2026, 7, 19, 12),
        datetime.datetime(
            2026,
            7,
            19,
            12,
            tzinfo=datetime.timezone.utc,
        ),
    ),
)
@pytest.mark.asyncio
async def test_database_timestamp_timezone_edges(database_timestamp):
    timestamp_result = Mock()
    timestamp_result.scalar_one.return_value = database_timestamp
    session = Mock()
    session.execute = AsyncMock(return_value=timestamp_result)
    expected_timestamp = database_timestamp.replace(tzinfo=datetime.timezone.utc)

    assert (
        await ptg2_candidate_attestation._database_timestamp(session)
        == expected_timestamp
    )


def test_release_report_cannot_refresh_attestation_after_freshness_window():
    report = _release_report()
    completed_at = datetime.datetime.fromisoformat(report["completed_at"])

    with pytest.raises(ValueError, match="too old"):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            report,
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
            evaluated_at=completed_at + datetime.timedelta(minutes=121),
        )


def test_record_candidate_attestation_rechecks_freshness_after_lock(monkeypatch):
    report = _release_report()
    completed_at = datetime.datetime.fromisoformat(report["completed_at"])
    session = _Session()
    locked_identity = AsyncMock()
    monkeypatch.setenv(
        ptg2_candidate_attestation.PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_ENV,
        "30",
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT",
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=completed_at + datetime.timedelta(minutes=31)),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        locked_identity,
    )

    with pytest.raises(ValueError, match="too old"):
        asyncio.run(
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                snapshot_id="snap_new",
                source_key="source_a",
                plan_id="12-3456789",
                plan_market_type="group",
                report=report,
            )
        )

    locked_identity.assert_not_awaited()
    assert session.calls == []


def test_candidate_identity_binds_postgres_bytea_source_and_sealed_sample():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    audit_sample_digest = "ab" * 32
    coverage_scope_id = b"c" * 32
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": _audit_sample(audit_sample_digest),
        "source_witness": _source_witness(source_set),
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }
    layout_serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_count": 1,
        "audit_sample": _audit_sample(audit_sample_digest),
        "source_witness": _source_witness(source_set),
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }

    identity = ptg2_candidate_attestation._candidate_identity(
        {
            "status": "validated",
            "manifest": {
                "activation": {
                    "contract": "ptg2_candidate_activation_v1",
                    "state": "validated",
                    "source_key": "source_a",
                },
                "serving_index": serving_index,
            },
            "layout_manifest": {"serving_index": layout_serving_index},
            "snapshot_key": 17,
            "layout_state": "sealed",
            "layout_generation": "shared_blocks_v3",
            "plan_id": "12-3456789",
            "plan_market_type": "group",
            "coverage_scope_id": coverage_scope_id,
            "raw_container_sha256_values": [raw_container_digest],
        }
    )

    assert identity["source_set_digest"] == bytes.fromhex(
        source_set["raw_container_sha256_digest"]
    )
    assert identity["audit_sample_digest"] == bytes.fromhex(audit_sample_digest)
    assert identity["ordered_source_ordinal_digest"] == (
        ptg2_candidate_attestation.ordered_source_ordinal_digest(
            [raw_container_digest.hex()]
        )
    )


def test_candidate_identity_binds_complete_v4_packed_root():
    raw_container_digest = b"x" * 32
    map_digest = b"m" * 32
    coverage_scope_id = b"c" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    common_index = {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": "shared_blocks_v4",
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "shared_snapshot_key": 17,
        "snapshot_map": {"map_digest": map_digest.hex()},
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_witness": _source_witness(source_set),
        "audit_sample": _audit_sample("ab" * 32),
        "provider_identifier_quarantine": (
            EMPTY_PROVIDER_IDENTIFIER_QUARANTINE
        ),
    }
    identity = ptg2_candidate_attestation._candidate_identity(
        {
            "status": "validated",
            "manifest": {
                "activation": {
                    "contract": "ptg2_candidate_activation_v1",
                    "state": "validated",
                    "source_key": "source_a",
                },
                "serving_index": {
                    **common_index,
                    "source_set": source_set,
                },
            },
            "layout_manifest": {
                "serving_index": {
                    **common_index,
                    "source_count": 1,
                }
            },
            "snapshot_key": 17,
            "layout_state": "sealed",
            "layout_generation": "shared_blocks_v4",
            "layout_mapping_digest": map_digest,
            "v4_root_state": "complete",
            "v4_root_map_digest": map_digest,
            "plan_id": "12-3456789",
            "plan_market_type": "group",
            "coverage_scope_id": coverage_scope_id,
            "raw_container_sha256_values": [raw_container_digest],
        }
    )

    assert identity["storage_generation"] == "shared_blocks_v4"
    assert identity["snapshot_key"] == 17


def test_candidate_identity_rejects_snapshot_layout_sample_mismatch():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    coverage_scope_id = b"c" * 32
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": _audit_sample("ab" * 32),
        "source_witness": _source_witness(source_set),
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }

    with pytest.raises(ValueError, match="sample changed"):
        ptg2_candidate_attestation._candidate_identity(
            {
                "status": "validated",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                    },
                    "serving_index": serving_index,
                },
                "layout_manifest": {
                    "serving_index": {
                        "arch_version": "postgres_binary_v3",
                        "storage_generation": "shared_blocks_v3",
                        "coverage_scope_id": coverage_scope_id.hex(),
                        "source_count": 1,
                        "audit_sample": _audit_sample("cd" * 32),
                        "source_witness": _source_witness(source_set),
                        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
                    }
                },
                "snapshot_key": 17,
                "layout_state": "sealed",
                "layout_generation": "shared_blocks_v3",
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": coverage_scope_id,
                "raw_container_sha256_values": [raw_container_digest],
            }
        )


def test_candidate_identity_rejects_snapshot_layout_quarantine_mismatch():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    coverage_scope_id = b"c" * 32
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": _audit_sample("ab" * 32),
        "source_witness": _source_witness(source_set),
        "provider_identifier_quarantine": MALFORMED_PROVIDER_IDENTIFIER_QUARANTINE,
    }

    with pytest.raises(ValueError, match="quarantine changed"):
        ptg2_candidate_attestation._candidate_identity(
            {
                "status": "validated",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                    },
                    "serving_index": serving_index,
                },
                "layout_manifest": {
                    "serving_index": {
                        "arch_version": "postgres_binary_v3",
                        "storage_generation": "shared_blocks_v3",
                        "coverage_scope_id": coverage_scope_id.hex(),
                        "source_count": 1,
                        "audit_sample": _audit_sample("ab" * 32),
                        "source_witness": _source_witness(source_set),
                        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
                    }
                },
                "snapshot_key": 17,
                "layout_state": "sealed",
                "layout_generation": "shared_blocks_v3",
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": coverage_scope_id,
                "raw_container_sha256_values": [raw_container_digest],
            }
        )


def test_candidate_identity_rejects_snapshot_layout_physical_scope_mismatch():
    raw_container_digest = b"x" * 32
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        [raw_container_digest.hex()]
    )
    coverage_scope_id = b"c" * 32
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "coverage_scope_id": coverage_scope_id.hex(),
        "source_set": source_set,
        "audit_sample": _audit_sample("ab" * 32),
        "source_witness": _source_witness(source_set),
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }

    with pytest.raises(ValueError, match="physical scope"):
        ptg2_candidate_attestation._candidate_identity(
            {
                "status": "validated",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                    },
                    "serving_index": serving_index,
                },
                "layout_manifest": {
                    "serving_index": {
                        "arch_version": "postgres_binary_v3",
                        "storage_generation": "shared_blocks_v3",
                        "coverage_scope_id": (b"d" * 32).hex(),
                        "source_count": 1,
                        "audit_sample": _audit_sample("ab" * 32),
                        "source_witness": _source_witness(source_set),
                        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
                    }
                },
                "snapshot_key": 17,
                "layout_state": "sealed",
                "layout_generation": "shared_blocks_v3",
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": coverage_scope_id,
                "raw_container_sha256_values": [raw_container_digest],
            }
        )


class _Result:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row

    def one_or_none(self):
        return self._row


class _RowsResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _Session:
    def __init__(self, result=None):
        self.calls = []
        self.result = result or _Result((b"r" * 32,))

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        return self.result


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _candidate_plan_pointer_entries():
    return [
        {
            "plan_source_key": "plan_source_a",
            "plan_id": "12-3456789",
            "plan_market_type": "group",
            "import_month": datetime.date(2026, 7, 1),
            "source_key": "source_a",
            "snapshot_id": "snap_new",
            "previous_snapshot_id": "snap_old",
            "updated_at": datetime.datetime(2026, 7, 13, 9, 0, 0),
        }
    ]


def test_record_candidate_attestation_binds_database_identity(monkeypatch):
    """Keep the rollback writer path covered without making it the default."""
    session = _Session()
    identity_map = {
        "snapshot_key": 17,
        "storage_generation": "shared_blocks_v3",
        "source_key": "source_a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "audit_sample_digest": bytes.fromhex("ab" * 32),
        "source_witness_digest": b"w" * 32,
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity_map),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT",
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=datetime.datetime.now(datetime.timezone.utc)),
    )

    attestation_result = asyncio.run(
        ptg2_candidate_attestation.record_candidate_audit_attestation(
            snapshot_id="snap_new",
            source_key="SOURCE_A",
            plan_id="12-3456789",
            plan_market_type="GROUP",
            report=_release_report(),
        )
    )

    assert attestation_result["status"] == "attested"
    assert (
        attestation_result["contract"]
        == ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
    )
    assert len(attestation_result["report_digest"]) == 64
    sql, params = session.calls[0]
    assert "ptg2_v3_candidate_audit_attestation" in sql
    assert params["snapshot_key"] == 17
    assert params["source_key"] == "source_a"
    assert params["coverage_scope_id"] == b"c" * 32
    assert params["source_set_digest"] == b"s" * 32
    assert params["audit_sample_digest"] == bytes.fromhex("ab" * 32)
    assert params["source_witness_digest"] == b"w" * 32
    assert (
        params["contract"]
        == ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
    )
    assert params["expires_at"] > params["attested_at"]
    assert "contract = EXCLUDED.contract" in sql
    assert "tool_name = EXCLUDED.tool_name" in sql
    assert "attestation.contract = :v3_contract" in sql
    assert "EXCLUDED.contract = :v4_contract" in sql
    assert params["v3_contract"] == (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
    )
    assert params["v4_contract"] == (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
    )


def test_record_candidate_attestation_rejects_report_quarantine_mismatch(monkeypatch):
    session = _Session()
    identity_map = {
        "snapshot_key": 17,
        "storage_generation": "shared_blocks_v3",
        "source_key": "source_a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "audit_sample_digest": bytes.fromhex("ab" * 32),
        "source_witness_digest": b"w" * 32,
        "provider_identifier_quarantine": MALFORMED_PROVIDER_IDENTIFIER_QUARANTINE,
    }
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity_map),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT",
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=datetime.datetime.now(datetime.timezone.utc)),
    )

    with pytest.raises(ValueError, match="does not match the sealed candidate"):
        asyncio.run(
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                snapshot_id="snap_new",
                source_key="source_a",
                plan_id="12-3456789",
                plan_market_type="group",
                report=_release_report(),
            )
        )

    assert session.calls == []


def test_attestation_expiry_is_capped_by_report_freshness(monkeypatch):
    """Keep attestation expiry within the already-running report freshness window."""

    report = _release_report()
    completed_at = datetime.datetime.fromisoformat(report["completed_at"])
    database_now = completed_at + datetime.timedelta(minutes=10)
    session = _Session()
    identity_map = {
        "snapshot_key": 17,
        "storage_generation": "shared_blocks_v3",
        "source_key": "source_a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "audit_sample_digest": bytes.fromhex("ab" * 32),
        "source_witness_digest": b"w" * 32,
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }
    monkeypatch.setenv(
        ptg2_candidate_attestation.PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_ENV,
        "30",
    )
    monkeypatch.setenv(
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_ENV,
        "24",
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT",
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity_map),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=database_now),
    )

    asyncio.run(
        ptg2_candidate_attestation.record_candidate_audit_attestation(
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
            report=report,
        )
    )

    sql, params = session.calls[0]
    assert params["attested_at"] == database_now
    assert params["expires_at"] == completed_at + datetime.timedelta(minutes=30)
    assert "attestation.source_key = EXCLUDED.source_key" in sql
    assert "attestation.plan_id = EXCLUDED.plan_id" in sql


def test_activation_rechecks_attestation_expiry_against_wall_clock(monkeypatch):
    report = _release_report()
    expected_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report)
    ).digest()
    session = _Session(_Result((expected_digest, report)))
    identity_map = {
        "snapshot_key": 17,
        "source_key": "source_a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
        "source_set_digest": b"s" * 32,
        "audit_sample_digest": bytes.fromhex("ab" * 32),
        "source_witness_digest": b"w" * 32,
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity_map),
    )

    digest = asyncio.run(
        ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
            session,
            schema_name="mrf",
            snapshot_id="snap_new",
            snapshot_key=17,
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
            coverage_scope_id=b"c" * 32,
        )
    )

    assert digest == expected_digest
    sql, params = session.calls[0]
    assert "expires_at > clock_timestamp()" in sql
    assert "expires_at > now()" not in sql
    assert "source_set_digest = :source_set_digest" in sql
    assert "contract = ANY(CAST(:supported_contracts AS text[]))" in sql
    assert params["supported_contracts"] == list(
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
    )
    assert params["audit_sample_digest"] == bytes.fromhex("ab" * 32)
    assert params["source_witness_digest"] == b"w" * 32


def test_writer_cutover_writes_v4_and_keeps_v3_reader_compatibility():
    assert (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT
        == ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
    )
    assert (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT
        == ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
    )
    assert ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS == (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4,
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
    )


def test_writer_cutover_rejects_new_v3_attestation_writes(monkeypatch):
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "validate_candidate_release_audit_report",
        lambda *_args, **_kwargs: {
            "contract": (
                ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3
            )
        },
    )
    with pytest.raises(
        ptg2_candidate_attestation.CandidateAttestationWriterContractError,
        match="not enabled for writes",
    ) as exc_info:
        asyncio.run(
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                snapshot_id="snap_new",
                source_key="source_a",
                plan_id="12-3456789",
                plan_market_type="group",
                report={"schema_version": 3},
            )
        )
    assert exc_info.value.retryable is False


def test_writer_cutover_rechecks_contract_under_lock(monkeypatch):
    session = _Session()
    evidence_contracts = iter(
        (
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4,
            ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
        )
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "validate_candidate_release_audit_report",
        lambda *_args, **_kwargs: {"contract": next(evidence_contracts)},
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=datetime.datetime.now(datetime.timezone.utc)),
    )
    locked_identity = AsyncMock()
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        locked_identity,
    )

    with pytest.raises(
        ptg2_candidate_attestation.CandidateAttestationWriterContractError,
        match="not enabled for writes",
    ):
        asyncio.run(
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                snapshot_id="snap_new",
                source_key="source_a",
                plan_id="12-3456789",
                plan_market_type="group",
                report={"schema_version": 4},
            )
        )

    locked_identity.assert_not_awaited()
    assert session.calls == []


def test_activation_rejects_report_quarantine_changed_after_attestation(monkeypatch):
    report = _release_report()
    report["source"]["provider_identifier_quarantine"] = (
        MALFORMED_PROVIDER_IDENTIFIER_QUARANTINE
    )
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report)
    ).digest()
    session = _Session(_Result((report_digest, report)))
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(
            return_value={
                "snapshot_key": 17,
                "source_key": "source_a",
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": b"c" * 32,
                "source_set_digest": b"s" * 32,
                "audit_sample_digest": bytes.fromhex("ab" * 32),
                "source_witness_digest": b"w" * 32,
                "provider_identifier_quarantine": (
                    EMPTY_PROVIDER_IDENTIFIER_QUARANTINE
                ),
            }
        ),
    )

    with pytest.raises(ValueError, match="changed after its release audit"):
        asyncio.run(
            ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
                session,
                schema_name="mrf",
                snapshot_id="snap_new",
                snapshot_key=17,
                source_key="source_a",
                plan_id="12-3456789",
                plan_market_type="group",
                coverage_scope_id=b"c" * 32,
            )
        )


def test_activation_rejects_stored_report_changed_after_attestation(monkeypatch):
    report = _release_report()
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report)
    ).digest()
    report["duration_seconds"] = 601.0
    session = _Session(_Result((report_digest, report)))
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(
            return_value={
                "snapshot_key": 17,
                "source_key": "source_a",
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": b"c" * 32,
                "source_set_digest": b"s" * 32,
                "audit_sample_digest": bytes.fromhex("ab" * 32),
                "source_witness_digest": b"w" * 32,
                "provider_identifier_quarantine": (
                    EMPTY_PROVIDER_IDENTIFIER_QUARANTINE
                ),
            }
        ),
    )

    with pytest.raises(ValueError, match="report changed after validation"):
        asyncio.run(
            ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
                session,
                schema_name="mrf",
                snapshot_id="snap_new",
                snapshot_key=17,
                source_key="source_a",
                plan_id="12-3456789",
                plan_market_type="group",
                coverage_scope_id=b"c" * 32,
            )
        )

    assert len(session.calls) == 1


def test_strict_candidate_publication_preserves_locked_manifest():
    session = _Session()
    asyncio.run(
        source_pointers._publish_snapshot_in_pointer_transaction(
            session,
            schema_name="mrf",
            snapshot_attributes={
                "snapshot_id": "snap_new",
                "status": "published",
                "published_at": datetime.datetime(2026, 7, 13, 9, 0, 0),
                "previous_snapshot_id": "snap_old",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "activated",
                        "mode": "audited_control",
                    },
                    "serving_index": {"sealed": "must-not-be-rewritten"},
                },
            },
        )
    )

    sql, params = session.calls[0]
    assert "jsonb_set" in sql
    assert "manifest->'activation'->>'state' = 'validated'" in sql
    assert "import_run_id =" not in sql
    assert "manifest_json" not in params
    assert "must-not-be-rewritten" not in params["activation_json"]


def test_candidate_staging_never_rewrites_an_existing_validated_row():
    session = _Session(_Result(("validated",)))
    attributes_map = {
        "snapshot_id": "snap_new",
        "import_run_id": "run_1",
        "import_month": datetime.date(2026, 7, 1),
        "status": "validated",
        "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
        "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
        "previous_snapshot_id": "snap_old",
        "manifest": {"activation": {"state": "validated"}},
    }

    asyncio.run(
        source_pointers._stage_snapshot_in_pointer_transaction(
            session,
            schema_name="mrf",
            snapshot_attributes=attributes_map,
        )
    )

    sql, _params = session.calls[0]
    assert "AND status = 'building'" in sql
    assert "status IN ('building', 'validated')" not in sql
    assert "existing.manifest::jsonb = CAST(:manifest_json AS jsonb)" in sql


def test_non_candidate_publication_cannot_rewrite_a_strict_candidate():
    session = _Session(_Result(("published",)))
    asyncio.run(
        source_pointers._publish_snapshot_in_pointer_transaction(
            session,
            schema_name="mrf",
            snapshot_attributes={
                "snapshot_id": "snap_old",
                "import_run_id": "run_1",
                "import_month": datetime.date(2026, 7, 1),
                "status": "published",
                "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
                "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
                "published_at": datetime.datetime(2026, 7, 13, 9, 0, 0),
                "previous_snapshot_id": None,
                "manifest": {},
            },
        )
    )

    sql, params = session.calls[0]
    assert sql.count("<> :candidate_activation_contract") == 2
    assert params["candidate_activation_contract"] == (
        "ptg2_candidate_activation_v1"
    )


def test_generic_publish_uses_locked_database_candidate_not_caller_attributes(monkeypatch):
    """Verify generic publish uses locked database candidate not caller attributes."""
    session = object()
    activate = AsyncMock(return_value={"status": "promoted"})
    source_plan_rows = AsyncMock(side_effect=AssertionError("legacy path was selected"))
    monkeypatch.setattr(
        source_pointers.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        AsyncMock(),
    )
    locked_snapshot = AsyncMock(
        return_value={
            "snapshot_id": "snap_new",
            "status": "validated",
            "manifest": {
                "activation": {
                    "contract": "ptg2_candidate_activation_v1",
                    "state": "validated",
                }
            },
        }
    )
    monkeypatch.setattr(
        source_pointers,
        "_locked_snapshot_publication_row",
        locked_snapshot,
    )
    monkeypatch.setattr(
        source_pointers,
        "_activate_ptg2_source_candidate_in_transaction",
        activate,
    )
    monkeypatch.setattr(
        source_pointers,
        "_source_plan_rows",
        source_plan_rows,
    )

    publication_result = asyncio.run(
        source_pointers._publish_ptg2_source_pointers(
            source_key="source_a",
            snapshot_id="snap_new",
            previous_snapshot_id="snap_old",
            import_month=datetime.date(2026, 7, 1),
            updated_at=datetime.datetime(2026, 7, 13, 9, 0, 0),
            snapshot_attributes={
                "snapshot_id": "snap_new",
                "status": "published",
                "manifest": {},
            },
        )
    )

    assert publication_result == {"status": "promoted"}
    locked_snapshot.assert_awaited_once_with(
        session,
        schema_name="mrf",
        snapshot_id="snap_new",
    )
    activate.assert_awaited_once_with(
        session,
        schema_name="mrf",
        source_key="source_a",
        snapshot_id="snap_new",
        expected_current_snapshot_id="snap_old",
    )
    source_plan_rows.assert_not_awaited()


def test_candidate_metadata_is_reread_from_postgres_under_row_lock():
    candidate_map = {
        "snapshot_id": "snap_new",
        "import_run_id": "run_1",
        "import_month": datetime.date(2026, 7, 1),
        "status": "validated",
        "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
        "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
        "published_at": None,
        "previous_snapshot_id": "snap_old",
        "manifest": {"activation": {"state": "validated"}},
        "snapshot_key": 17,
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
    }
    session = _Session(_Result(candidate_map))

    observed = asyncio.run(
        source_pointers._locked_candidate_activation_row(
            session,
            schema_name="mrf",
            snapshot_id="snap_new",
        )
    )

    assert observed == candidate_map
    sql, params = session.calls[0]
    assert "FOR UPDATE OF snapshot" in sql
    assert "snapshot.import_run_id" in sql
    assert "snapshot.import_month" in sql
    assert "snapshot.previous_snapshot_id" in sql
    assert "snapshot.manifest" in sql
    assert "binding.snapshot_key" in sql
    assert "scope.coverage_scope_id" in sql
    assert params == {"snapshot_id": "snap_new"}


def test_candidate_plan_pointer_entries_load_all_logical_plan_mappings():
    session = _Session(
        _RowsResult(
            [
                {"plan_id": "12-3456789", "plan_market_type": "group"},
                {"plan_id": "98-7654321", "plan_market_type": "group"},
            ]
        )
    )
    activated_at = datetime.datetime(2026, 7, 13, 9, 0, 0)

    entries = asyncio.run(
        source_pointers._candidate_plan_pointer_entries(
            session,
            schema_name="mrf",
            source_key="source_a",
            snapshot_id="snap_new",
            previous_snapshot_id="snap_old",
            import_month=datetime.date(2026, 7, 1),
            activated_at=activated_at,
        )
    )

    assert [entry["plan_id"] for entry in entries] == [
        "12-3456789",
        "98-7654321",
    ]
    assert {entry["snapshot_id"] for entry in entries} == {"snap_new"}
    assert {entry["previous_snapshot_id"] for entry in entries} == {"snap_old"}
    assert {entry["updated_at"] for entry in entries} == {activated_at}
    sql, params = session.calls[0]
    assert '"mrf".ptg2_v3_snapshot_plan_scope' in sql
    assert "ORDER BY plan_id, plan_market_type" in sql
    assert params == {"snapshot_id": "snap_new"}


def test_activation_cas_does_not_accept_candidate_already_current():
    session = _Session(_Result(None))

    with pytest.raises(source_pointers.PTG2SourcePointerConflict):
        asyncio.run(
            source_pointers._compare_and_swap_source_pointer(
                session,
                schema_name="mrf",
                source_key="source_a",
                snapshot_id="snap_new",
                previous_snapshot_id="snap_old",
                import_month=datetime.date(2026, 7, 1),
                updated_at=datetime.datetime(2026, 7, 13, 9, 0, 0),
                allow_already_current=False,
            )
        )

    sql, params = session.calls[0]
    assert "current_pointer.snapshot_id IS NOT DISTINCT FROM :previous_snapshot_id" in sql
    assert "current_pointer.snapshot_id = :snapshot_id" in sql
    assert params["allow_already_current"] is False


def test_strict_candidate_activation_verifies_and_consumes_attestation_atomically(monkeypatch):
    """Verify strict candidate activation verifies and consumes attestation atomically."""
    events = []
    cas_calls = []
    session = object()
    activation_time = datetime.datetime(2026, 7, 13, 9, 0, 0)
    monkeypatch.setattr(
        source_pointers.db,
        "transaction",
        lambda: _Transaction(session),
    )

    async def record(name, result=None, **_kwargs):
        events.append(name)
        return result

    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        lambda _session: record("lock"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        lambda _session, **_kwargs: record(
            "candidate",
            {
                "snapshot_id": "snap_new",
                "import_run_id": "run_1",
                "import_month": datetime.date(2026, 7, 1),
                "status": "validated",
                "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
                "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
                "published_at": None,
                "previous_snapshot_id": "snap_old",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                        "expected_previous_snapshot_id": "snap_old",
                    }
                },
                "snapshot_key": 17,
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": b"c" * 32,
            },
        ),
    )
    monkeypatch.setattr(
        source_pointers,
        "_database_utc_timestamp",
        lambda _session: record("clock", activation_time),
    )
    monkeypatch.setattr(
        source_pointers,
        "_candidate_plan_pointer_entries",
        AsyncMock(return_value=_candidate_plan_pointer_entries()),
    )
    monkeypatch.setattr(
        source_pointers,
        "verify_candidate_audit_attestation_in_transaction",
        lambda _session, **_kwargs: record("verify", b"r" * 32),
    )
    async def compare_and_swap(_session, **kwargs):
        events.append("source_cas")
        cas_calls.append(kwargs)

    monkeypatch.setattr(
        source_pointers,
        "_compare_and_swap_source_pointer",
        compare_and_swap,
    )
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        lambda _session, **_kwargs: record("publish"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_reconcile_global_snapshot_pointer",
        lambda _session, **_kwargs: record("global"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_replace_source_plan_pointers",
        lambda _session, **_kwargs: record("plan_pointers"),
    )
    monkeypatch.setattr(
        source_pointers,
        "consume_candidate_audit_attestation_in_transaction",
        lambda _session, **_kwargs: record("consume"),
    )

    activation_result = asyncio.run(
        source_pointers.activate_ptg2_source_candidate(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
        )
    )

    assert activation_result["status"] == "promoted"
    assert activation_result["plan_source_count"] == 1
    assert events == [
        "lock",
        "candidate",
        "clock",
        "verify",
        "source_cas",
        "publish",
        "global",
        "plan_pointers",
        "consume",
    ]
    assert cas_calls[0]["allow_already_current"] is False


def _mixed_candidate_activation_row():
    return {
        "snapshot_id": "snap_new",
        "import_run_id": "run_1",
        "import_month": datetime.date(2026, 7, 1),
        "status": "validated",
        "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
        "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
        "published_at": None,
        "previous_snapshot_id": "snap_old",
        "manifest": {
            "activation": {
                "contract": "ptg2_candidate_activation_v1",
                "state": "validated",
                "source_key": "source_a",
                "expected_previous_snapshot_id": "snap_old",
            },
            "allowed_amount_index": {
                "contract": "ptg2_allowed_amounts_v1",
                "arch_version": "postgres_binary_v3",
                "storage": "postgresql",
                "snapshot_scoped": True,
                "data_domain": "allowed_amounts",
                "source_key": "source_a",
                "current_source_key": "source_a_allowed_amounts",
                "previous_snapshot_id": "allowed_old",
                "allowed_amount_payments": 4,
                "allowed_amount_evidence": True,
            },
        },
        "snapshot_key": 17,
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
    }


def _mixed_candidate_activation_recorders(event_names, cas_calls):
    """Create ordered event recorders for mixed activation."""

    async def record_event(name, result=None, **_kwargs):
        event_names.append(name)
        return result

    async def compare_and_swap(_session, **kwargs):
        event_names.append(f"cas:{kwargs['source_key']}")
        cas_calls.append(kwargs)

    return record_event, compare_and_swap


def _install_mixed_candidate_activation_readers(
    monkeypatch,
    record_event,
    transaction_session,
    activation_time,
):
    """Install transaction, candidate, clock, and attestation readers."""

    monkeypatch.setattr(
        source_pointers.db,
        "transaction",
        lambda: _Transaction(transaction_session),
    )
    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        lambda _session: record_event("lock"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        lambda _session, **_kwargs: record_event(
            "candidate",
            _mixed_candidate_activation_row(),
        ),
    )
    monkeypatch.setattr(
        source_pointers,
        "_database_utc_timestamp",
        lambda _session: record_event("clock", activation_time),
    )
    monkeypatch.setattr(
        source_pointers,
        "_candidate_plan_pointer_entries",
        AsyncMock(return_value=_candidate_plan_pointer_entries()),
    )
    monkeypatch.setattr(
        source_pointers,
        "verify_candidate_audit_attestation_in_transaction",
        lambda _session, **_kwargs: record_event("verify", b"r" * 32),
    )


def _install_mixed_candidate_activation_writers(
    monkeypatch,
    record_event,
    compare_and_swap,
):
    """Install pointer, snapshot, plan, and attestation writers."""

    monkeypatch.setattr(
        source_pointers,
        "_compare_and_swap_source_pointer",
        compare_and_swap,
    )
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        lambda _session, **_kwargs: record_event("publish"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_reconcile_global_snapshot_pointer",
        lambda _session, **_kwargs: record_event("global"),
    )
    monkeypatch.setattr(
        source_pointers,
        "_replace_source_plan_pointers",
        lambda _session, **_kwargs: record_event("plan_pointers"),
    )
    monkeypatch.setattr(
        source_pointers,
        "consume_candidate_audit_attestation_in_transaction",
        lambda _session, **_kwargs: record_event("consume"),
    )


def _install_mixed_candidate_activation_collaborators(
    monkeypatch,
    event_names,
    cas_calls,
):
    """Install the audited mixed-candidate activation collaborators."""

    transaction_session = object()
    activation_time = datetime.datetime(2026, 7, 13, 9, 0, 0)
    record_event, compare_and_swap = _mixed_candidate_activation_recorders(
        event_names,
        cas_calls,
    )
    _install_mixed_candidate_activation_readers(
        monkeypatch,
        record_event,
        transaction_session,
        activation_time,
    )
    _install_mixed_candidate_activation_writers(
        monkeypatch,
        record_event,
        compare_and_swap,
    )


def test_audited_mixed_candidate_activates_allowed_pointer_in_same_transaction(
    monkeypatch,
):
    """Advance negotiated and allowed pointers during audited activation."""

    event_names = []
    cas_calls = []
    _install_mixed_candidate_activation_collaborators(
        monkeypatch,
        event_names,
        cas_calls,
    )
    activation_result = asyncio.run(
        source_pointers.activate_ptg2_source_candidate(
            source_key="source_a",
            snapshot_id="snap_new",
            expected_current_snapshot_id="snap_old",
        )
    )

    assert event_names == [
        "lock",
        "candidate",
        "clock",
        "verify",
        "cas:source_a",
        "cas:source_a_allowed_amounts",
        "publish",
        "global",
        "plan_pointers",
        "consume",
    ]
    assert [call["previous_snapshot_id"] for call in cas_calls] == [
        "snap_old",
        "allowed_old",
    ]
    assert all(call["allow_already_current"] is False for call in cas_calls)
    assert activation_result["plan_source_count"] == 1
    assert activation_result["allowed_amount_pointer"] == {
        "status": "promoted",
        "source_key": "source_a_allowed_amounts",
        "snapshot_id": "snap_new",
        "previous_snapshot_id": "allowed_old",
    }


def test_attestation_consumption_failure_rolls_back_all_activation_state(monkeypatch):
    """Verify attestation consumption failure rolls back all activation state."""
    session = object()
    state_map = {
        "source_pointer": "snap_old",
        "snapshot_status": "validated",
        "global_pointer": "snap_old",
        "plan_pointer": "snap_old",
        "attestation_consumed": False,
    }
    original_state_map = dict(state_map)

    class RollbackTransaction:
        exit_type = None

        async def __aenter__(self):
            return session

        async def __aexit__(self, exc_type, exc, tb):
            self.exit_type = exc_type
            if exc_type is not None:
                state_map.clear()
                state_map.update(original_state_map)
            return False

    transaction = RollbackTransaction()
    monkeypatch.setattr(source_pointers.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        AsyncMock(
            return_value={
                "snapshot_id": "snap_new",
                "import_run_id": "run_1",
                "import_month": datetime.date(2026, 7, 1),
                "status": "validated",
                "created_at": datetime.datetime(2026, 7, 13, 8, 0, 0),
                "validated_at": datetime.datetime(2026, 7, 13, 8, 30, 0),
                "published_at": None,
                "previous_snapshot_id": "snap_old",
                "manifest": {
                    "activation": {
                        "contract": "ptg2_candidate_activation_v1",
                        "state": "validated",
                        "source_key": "source_a",
                        "expected_previous_snapshot_id": "snap_old",
                    }
                },
                "snapshot_key": 17,
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": b"c" * 32,
            }
        ),
    )
    monkeypatch.setattr(
        source_pointers,
        "_database_utc_timestamp",
        AsyncMock(return_value=datetime.datetime(2026, 7, 13, 9, 0, 0)),
    )
    monkeypatch.setattr(
        source_pointers,
        "_candidate_plan_pointer_entries",
        AsyncMock(return_value=_candidate_plan_pointer_entries()),
    )
    monkeypatch.setattr(
        source_pointers,
        "verify_candidate_audit_attestation_in_transaction",
        AsyncMock(return_value=b"r" * 32),
    )

    async def source_cas(*_args, **_kwargs):
        state_map["source_pointer"] = "snap_new"

    async def publish(*_args, **_kwargs):
        state_map["snapshot_status"] = "published"

    async def global_pointer(*_args, **_kwargs):
        state_map["global_pointer"] = "snap_new"

    async def plan_pointer(*_args, **_kwargs):
        state_map["plan_pointer"] = "snap_new"

    async def consume(*_args, **_kwargs):
        state_map["attestation_consumed"] = True
        raise RuntimeError("attestation changed during activation")

    monkeypatch.setattr(source_pointers, "_compare_and_swap_source_pointer", source_cas)
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        publish,
    )
    monkeypatch.setattr(
        source_pointers,
        "_reconcile_global_snapshot_pointer",
        global_pointer,
    )
    monkeypatch.setattr(
        source_pointers,
        "_replace_source_plan_pointers",
        plan_pointer,
    )
    monkeypatch.setattr(
        source_pointers,
        "consume_candidate_audit_attestation_in_transaction",
        consume,
    )

    with pytest.raises(RuntimeError, match="attestation changed"):
        asyncio.run(
            source_pointers.activate_ptg2_source_candidate(
                source_key="source_a",
                snapshot_id="snap_new",
                expected_current_snapshot_id="snap_old",
            )
        )

    assert transaction.exit_type is RuntimeError
    assert state_map == original_state_map


def test_candidate_attestation_low_level_validation_edges(monkeypatch):
    """Cover canonical serialization and configuration fallbacks."""
    with pytest.raises(ValueError, match="canonical JSON"):
        ptg2_candidate_attestation._canonical_report_bytes({"bad": object()})
    assert (
        ptg2_candidate_attestation._sha256_hex("ab" * 32, field="digest")
        == "ab" * 32
    )

    monkeypatch.setenv(
        ptg2_candidate_attestation.PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_ENV,
        "invalid",
    )
    assert ptg2_candidate_attestation._audit_report_max_age() == datetime.timedelta(
        minutes=(
            ptg2_candidate_attestation.PTG2_CANDIDATE_AUDIT_REPORT_MAX_AGE_MINUTES_DEFAULT
        )
    )
    monkeypatch.setenv(
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_ENV,
        "invalid",
    )
    assert ptg2_candidate_attestation._attestation_ttl_hours() == (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_TTL_HOURS_DEFAULT
    )


def test_candidate_attestation_report_shape_validation_edges():
    """Reject missing mappings and incomplete batch reports."""
    with pytest.raises(ValueError, match="field missing"):
        ptg2_candidate_attestation._required_report_mapping({}, "missing")
    with pytest.raises(ValueError):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            {
                "schema_version": (
                    ptg2_candidate_attestation.PTG2_BATCH_AUDIT_REPORT_SCHEMA_VERSION
                )
            },
            snapshot_id="s",
            source_key="k",
            plan_id="p",
            plan_market_type="group",
        )
    with pytest.raises(ValueError, match="legacy candidate audit reports require"):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            _release_report(),
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
            storage_generation="shared_blocks_v4",
        )


def test_candidate_attestation_physical_identity_validation_edges():
    """Reject incomplete or inconsistent immutable physical identity."""
    with pytest.raises(ValueError, match="immutable audit identity"):
        ptg2_candidate_attestation._validated_candidate_physical_identity(
            {
                "raw_container_sha256_values": ["aa" * 32],
                "coverage_scope_id": b"",
            },
            {"source_set": {}, "coverage_scope_id": ""},
            {},
        )
    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        ("aa" * 32,)
    )
    with pytest.raises(ValueError, match="PostgreSQL bindings"):
        ptg2_candidate_attestation._validated_candidate_physical_identity(
            {
                "raw_container_sha256_values": ["aa" * 32],
                "coverage_scope_id": b"d" * 32,
            },
            {
                "source_set": source_set,
                "coverage_scope_id": (b"c" * 32).hex(),
            },
            {
                "coverage_scope_id": (b"c" * 32).hex(),
                "source_count": 1,
            },
        )


def test_candidate_attestation_source_witness_validation_edges():
    """Reject changed, incompatible, and mismatched source witnesses."""
    with pytest.raises(ValueError, match="changed after layout sealing"):
        ptg2_candidate_attestation._validated_candidate_source_witness(
            {},
            {"changed": True},
            source_count=1,
            source_set_digest_hex="00" * 32,
        )
    with pytest.raises(ValueError, match="incompatible"):
        ptg2_candidate_attestation._validated_candidate_source_witness(
            {}, {}, source_count=1, source_set_digest_hex="00" * 32
        )
    witness = _source_witness(
        {
            "source_count": 1,
            "raw_container_sha256_digest": "11" * 32,
        }
    )
    with pytest.raises(ValueError, match="changed after layout sealing"):
        ptg2_candidate_attestation._validated_candidate_source_witness(
            witness,
            witness,
            source_count=1,
            source_set_digest_hex="22" * 32,
        )


def test_candidate_attestation_v3_check_validation_edges():
    """Reject invalid V3 status, check counts, and witness sections."""
    with pytest.raises(ValueError, match="strict validated candidate"):
        ptg2_candidate_attestation._candidate_identity({"status": "invalid"})
    check_counts_by_name = {
        "source_witnesses": 3,
        "api_witnesses_matched": 1,
        "api_challenges_executed": 1,
        "provider_witnesses_validated": 1,
        "api_audit_occurrences_validated": 2,
    }
    with pytest.raises(ValueError, match="coverage is invalid"):
        ptg2_candidate_attestation._validated_v3_checks(
            check_counts_by_name,
            {
                "record_count": 3,
                "occurrence_witness_count": 1,
                "provider_witness_count": 1,
            },
        )

    invalid_sections = Mock(
        source_by_field={"source_count": 1, "witness": {}},
        checks_by_name={},
    )
    with pytest.raises(ValueError, match="coverage is invalid"):
        ptg2_candidate_attestation._validated_v3_witness(invalid_sections)
    witness = _source_witness(
        {
            "source_count": 1,
            "raw_container_sha256_digest": "11" * 32,
        }
    )
    mismatched_sections = Mock(
        source_by_field={
            "source_count": 1,
            "witness": witness,
            "source_set_digest": "22" * 32,
        },
        checks_by_name={},
    )
    with pytest.raises(ValueError, match="source set is invalid"):
        ptg2_candidate_attestation._validated_v3_witness(mismatched_sections)


def test_candidate_attestation_digest_and_sample_validation_edges():
    """Reject syntactically invalid digests and incompatible sample manifests."""

    for invalid_digest in ("g" * 64, "00 " * 20 + "0000"):
        with pytest.raises(ValueError, match="field digest is invalid"):
            ptg2_candidate_attestation._sha256_digest(
                invalid_digest,
                field="digest",
            )

    source_set = ptg2_candidate_attestation.shared_source_set_metadata(
        ("aa" * 32,)
    )
    with pytest.raises(ValueError, match="audit identity is malformed"):
        ptg2_candidate_attestation._validated_candidate_physical_identity(
            {
                "raw_container_sha256_values": ["aa" * 32],
                "coverage_scope_id": b"c" * 32,
            },
            {
                "source_set": source_set,
                "coverage_scope_id": "g" * 64,
            },
            {"coverage_scope_id": "g" * 64, "source_count": 1},
        )
    with pytest.raises(ValueError, match="audit sample is incompatible"):
        ptg2_candidate_attestation._validated_candidate_audit_sample({}, {}, 1)


def test_locked_candidate_identity_loads_and_validates_database_row(monkeypatch):
    database_row_by_field = {"status": "validated", "snapshot_key": 17}
    session = _Session(_Result(database_row_by_field))
    candidate_identity = Mock(return_value={"snapshot_key": 17})
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_candidate_identity",
        candidate_identity,
    )

    identity = asyncio.run(
        ptg2_candidate_attestation._locked_candidate_identity(
            session,
            schema_name="mrf",
            snapshot_id="snap-new",
        )
    )

    assert identity == {"snapshot_key": 17}
    candidate_identity.assert_called_once_with(database_row_by_field)
    assert "FOR UPDATE OF snapshot" in session.calls[0][0]
    assert "ptg2_v4_snapshot_map_root v4_root" in session.calls[0][0]
    assert session.calls[0][1]["v3_generation"] == "shared_blocks_v3"
    assert session.calls[0][1]["v4_generation"] == "shared_blocks_v4"


def test_locked_candidate_identity_rejects_missing_candidate():
    session = _Session(_Result(None))

    with pytest.raises(ValueError, match="validated candidate is unavailable"):
        asyncio.run(
            ptg2_candidate_attestation._locked_candidate_identity(
                session,
                schema_name="mrf",
                snapshot_id="snap-new",
            )
        )


@pytest.mark.parametrize(
    "missing_field",
    ("snapshot_id", "source_key", "plan_id", "plan_market_type"),
)
def test_record_candidate_attestation_requires_complete_target(missing_field):
    target_by_field = {
        "snapshot_id": "snap-new",
        "source_key": "source-a",
        "plan_id": "12-3456789",
        "plan_market_type": "group",
    }
    target_by_field[missing_field] = ""

    with pytest.raises(ValueError, match="snapshot, source, plan, and market"):
        asyncio.run(
            ptg2_candidate_attestation.record_candidate_audit_attestation(
                **target_by_field,
                report={},
            )
        )


def _install_v4_attestation_writer(
    monkeypatch,
    *,
    session,
    identity,
    evidence,
    database_now,
):
    report_validator = Mock(return_value=evidence)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "validate_candidate_release_audit_report",
        report_validator,
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_database_timestamp",
        AsyncMock(return_value=database_now),
    )
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity),
    )
    return report_validator


def _record_v4_attestation(*, storage_generation="shared_blocks_v3"):
    return ptg2_candidate_attestation.record_candidate_audit_attestation(
        snapshot_id="snap-new",
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
        report=writer_report_by_field(4),
        storage_generation=storage_generation,
    )


@pytest.mark.parametrize(
    ("mismatch", "message"),
    (
        ("target", "target does not match"),
        ("audit_sample_digest", "sealed candidate sample"),
        ("source_set_digest", "sealed candidate sources"),
        ("ordered_source_ordinal_digest", "source ordinals"),
        ("source_witness_manifest", "metadata does not match"),
        ("audit_sample_public", "metadata does not match"),
        ("source_witness_digest", "sealed source witnesses"),
        ("provider_identifier_quarantine_evidence", "quarantine does not match"),
    ),
)
def test_v4_attestation_writer_rejects_post_lock_identity_changes(
    monkeypatch,
    mismatch,
    message,
):
    database_now = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.timezone.utc)
    report = writer_report_by_field(4)
    evidence = writer_evidence_by_field(report, completed_at=database_now)
    identity = writer_identity_by_field()
    if mismatch == "target":
        identity["source_key"] = "different-source"
    elif mismatch in {"audit_sample_digest", "source_set_digest", "source_witness_digest"}:
        evidence[mismatch] = b"x" * 32
    elif mismatch == "ordered_source_ordinal_digest":
        evidence[mismatch] = "x" * 64
    elif mismatch in {"source_witness_manifest", "audit_sample_public"}:
        evidence[mismatch] = {"changed": True}
    else:
        evidence[mismatch] = {"changed": True}
    session = _Session()
    _install_v4_attestation_writer(
        monkeypatch,
        session=session,
        identity=identity,
        evidence=evidence,
        database_now=database_now,
    )

    with pytest.raises(ValueError, match=message):
        asyncio.run(_record_v4_attestation())

    assert session.calls == []


def test_v4_attestation_writer_persists_request_accounting(monkeypatch):
    database_now = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.timezone.utc)
    report = writer_report_by_field(4)
    evidence = writer_evidence_by_field(report, completed_at=database_now)
    session = _Session()
    _install_v4_attestation_writer(
        monkeypatch,
        session=session,
        identity=writer_identity_by_field(),
        evidence=evidence,
        database_now=database_now,
    )

    result = asyncio.run(_record_v4_attestation())

    assert result["status"] == "attested"
    assert result["batch_api_actual_http_requests"] == 1
    assert session.calls[0][1]["contract"] == (
        ptg2_candidate_attestation.PTG2_CANDIDATE_ATTESTATION_CONTRACT_V4
    )


def test_v4_attestation_writer_binds_packed_storage_generation(monkeypatch):
    database_now = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.timezone.utc)
    report = writer_report_by_field(4)
    evidence = writer_evidence_by_field(report, completed_at=database_now)
    evidence["storage_generation"] = "shared_blocks_v4"
    identity = writer_identity_by_field()
    identity["storage_generation"] = "shared_blocks_v4"
    session = _Session()
    report_validator = _install_v4_attestation_writer(
        monkeypatch,
        session=session,
        identity=identity,
        evidence=evidence,
        database_now=database_now,
    )

    result = asyncio.run(
        _record_v4_attestation(storage_generation="shared_blocks_v4")
    )

    assert result["status"] == "attested"
    assert report_validator.call_count == 2
    assert all(
        call.kwargs["storage_generation"] == "shared_blocks_v4"
        for call in report_validator.call_args_list
    )
    assert session.calls[0][1]["snapshot_key"] == 17


def test_v4_attestation_writer_rejects_conflicting_existing_evidence(monkeypatch):
    database_now = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.timezone.utc)
    report = writer_report_by_field(4)
    _install_v4_attestation_writer(
        monkeypatch,
        session=_Session(_Result(None)),
        identity=writer_identity_by_field(),
        evidence=writer_evidence_by_field(report, completed_at=database_now),
        database_now=database_now,
    )

    with pytest.raises(ValueError, match="conflicts with existing evidence"):
        asyncio.run(_record_v4_attestation())


def test_v4_attestation_writer_rejects_expired_evidence_after_lock(monkeypatch):
    database_now = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.timezone.utc)
    report = writer_report_by_field(4)
    completed_at = database_now - ptg2_candidate_attestation._audit_report_max_age()
    evidence = writer_evidence_by_field(report, completed_at=completed_at)
    _install_v4_attestation_writer(
        monkeypatch,
        session=_Session(),
        identity=writer_identity_by_field(),
        evidence=evidence,
        database_now=database_now,
    )

    with pytest.raises(ValueError, match="too old for candidate activation"):
        asyncio.run(_record_v4_attestation())


def _verify_v4_attestation(monkeypatch, *, identity_by_field, query_result):
    session = _Session(query_result)
    monkeypatch.setattr(
        ptg2_candidate_attestation,
        "_locked_candidate_identity",
        AsyncMock(return_value=identity_by_field),
    )
    return session, ptg2_candidate_attestation.verify_candidate_audit_attestation_in_transaction(
        session,
        schema_name="mrf",
        snapshot_id="snap-new",
        snapshot_key=17,
        source_key="source-a",
        plan_id="12-3456789",
        plan_market_type="group",
        coverage_scope_id=b"c" * 32,
    )


def test_v4_attestation_verifier_accepts_unchanged_evidence(monkeypatch):
    identity_by_field = writer_identity_by_field()
    report_by_field = writer_report_by_field(4)
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report_by_field)
    ).digest()
    _session, verification = _verify_v4_attestation(
        monkeypatch,
        identity_by_field=identity_by_field,
        query_result=_Result((report_digest, report_by_field)),
    )

    assert asyncio.run(verification) == report_digest


@pytest.mark.parametrize(
    ("identity_source_key", "stored_row_kind", "message"),
    (
        ("different-source", "valid", "identity changed"),
        ("source-a", "missing", "no current passing"),
        ("source-a", "invalid_digest", "digest is invalid"),
    ),
)
def test_v4_attestation_verifier_rejects_identity_or_row_drift(
    monkeypatch,
    identity_source_key,
    stored_row_kind,
    message,
):
    """Reject changed identity, missing evidence, and malformed row digests."""

    identity_by_field = writer_identity_by_field()
    identity_by_field["source_key"] = identity_source_key
    report_by_field = writer_report_by_field(4)
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report_by_field)
    ).digest()
    stored_rows_by_kind = {
        "valid": (report_digest, report_by_field),
        "missing": None,
        "invalid_digest": (b"x", report_by_field),
    }
    _session, verification = _verify_v4_attestation(
        monkeypatch,
        identity_by_field=identity_by_field,
        query_result=_Result(stored_rows_by_kind[stored_row_kind]),
    )

    with pytest.raises(ValueError, match=message):
        asyncio.run(verification)


@pytest.mark.parametrize(
    ("identity_field", "changed_value", "message"),
    (
        (
            "provider_identifier_quarantine_evidence",
            {"changed": True},
            "quarantine changed",
        ),
        ("source_witness_digest", b"x" * 32, "source witness changed"),
        ("ordered_source_ordinal_digest", "x" * 64, "source ordinals changed"),
    ),
)
def test_v4_attestation_verifier_rejects_digest_bound_identity_drift(
    monkeypatch,
    identity_field,
    changed_value,
    message,
):
    """Reject changed quarantine, witness, and source-order evidence."""

    identity_by_field = writer_identity_by_field()
    identity_by_field[identity_field] = changed_value
    report_by_field = writer_report_by_field(4)
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report_by_field)
    ).digest()
    _session, verification = _verify_v4_attestation(
        monkeypatch,
        identity_by_field=identity_by_field,
        query_result=_Result((report_digest, report_by_field)),
    )

    with pytest.raises(ValueError, match=message):
        asyncio.run(verification)


@pytest.mark.parametrize(
    ("manifest_field", "changed_field", "changed_value"),
    (
        ("source_witness_manifest", "payload_bytes", 1),
        ("audit_sample_public", "maximum_rows", 1),
    ),
)
def test_v4_attestation_verifier_rejects_sealed_metadata_drift(
    monkeypatch,
    manifest_field,
    changed_field,
    changed_value,
):
    """Reject witness or audit-sample metadata changed after attestation."""

    identity_by_field = writer_identity_by_field()
    identity_by_field[manifest_field] = {
        **identity_by_field[manifest_field],
        changed_field: changed_value,
    }
    report_by_field = writer_report_by_field(4)
    report_digest = hashlib.sha256(
        ptg2_candidate_attestation._canonical_report_bytes(report_by_field)
    ).digest()
    _session, verification = _verify_v4_attestation(
        monkeypatch,
        identity_by_field=identity_by_field,
        query_result=_Result((report_digest, report_by_field)),
    )

    with pytest.raises(ValueError, match="audit metadata changed"):
        asyncio.run(verification)


@pytest.mark.parametrize("aware_timestamp", (False, True))
def test_candidate_attestation_consumption_normalizes_time_and_detects_conflict(
    aware_timestamp,
):
    activated_at = datetime.datetime(2026, 7, 20, 12)
    if aware_timestamp:
        activated_at = activated_at.replace(tzinfo=datetime.timezone.utc)
    session = _Session(_Result(None if aware_timestamp else ("snap-new",)))
    consumption = ptg2_candidate_attestation.consume_candidate_audit_attestation_in_transaction(
        session,
        schema_name="mrf",
        snapshot_id="snap-new",
        report_digest=b"r" * 32,
        activated_at=activated_at,
    )

    if aware_timestamp:
        with pytest.raises(RuntimeError, match="changed during activation"):
            asyncio.run(consumption)
    else:
        asyncio.run(consumption)
        assert session.calls[0][1]["activated_at"].tzinfo is datetime.timezone.utc
