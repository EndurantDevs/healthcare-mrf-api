# See LICENSE.

from __future__ import annotations

import collections
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from api import ptg2_capacity_evidence as capacity
from process.ptg_parts import ptg2_candidate_attestation
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from scripts.validation import ptg2_v3_source_api_audit as audit


PUBLIC_KEY = bytes(range(32))
TRUST = audit.CapacityEvidenceTrust(
    api_evidence_key_id="capacity-test-key",
    api_evidence_public_key=PUBLIC_KEY,
    release_digest="11" * 32,
    environment_id="22" * 32,
)
RUN_NONCE = "a1" * 32
CONTENTION_RUN_ID = "77" * 32

from tests.ptg2_v3_source_api_capacity_request_support import (
    _IntentFetcher,
    _capacity_config,
    _capacity_query,
    _contract,
    _fetcher,
    _observation,
    _options,
    _page_payload,
)

def _capacity_gate_cohort_reports(cohort_count_map, distinct_random):
    return {
        cohort: {
            "planned": count,
            "physical": count,
            "verified": count,
            "eligible": count,
            "rejected": 0,
            "distinct_semantic_queries": (
                distinct_random
                if cohort == audit.CAPACITY_RANDOM_COHORT
                else count
            ),
        }
        for cohort, count in cohort_count_map.items()
    }


def _capacity_gate_reconciliation(eligible):
    return {
        "contract": "planned_first_attempt_outcomes_v1",
        "accounted": True,
        "release_clean": True,
        "planned_first_physical_attempts": eligible,
        "plans_with_physical_attempt": eligible,
        "planned_without_physical_attempt": 0,
        "initial_physical_attempts": eligible,
        "retry_physical_attempts": 0,
        "invalid_attempt_index_records": 0,
        "eligible_physical_attempts": eligible,
        "rejected_physical_attempts": 0,
        "unclassified_physical_attempts": 0,
        "verified_rejected_physical_attempts": 0,
        "semantic_outcomes_without_physical_attempt": 0,
        "structural_failures": {},
    }


def _capacity_gate_counters(eligible, cohort_count_map, distinct_random):
    return {
        "planned": eligible,
        "physical": eligible,
        "verified": eligible,
        "eligible": eligible,
        "rejected": 0,
        "retries": 0,
        "timeouts": 0,
        "transport_errors": 0,
        "http_errors": 0,
        "verification_errors": 0,
        "redirects": 0,
        "status": {"200": eligible},
        "rejections": {},
        "cohorts": _capacity_gate_cohort_reports(
            cohort_count_map, distinct_random
        ),
        "reconciliation": _capacity_gate_reconciliation(eligible),
    }


def _capacity_gate_latency(eligible, cohort_count_map):
    return {
        "gate_source": audit.CAPACITY_CLIENT_LATENCY_SOURCE,
        "eligible_verified_client_http_duration_ms": {
            "count": eligible,
            "p95_ms": 12.0,
        },
        "eligible_verified_client_http_duration_ms_by_cohort": {
            cohort: {
                "count": count,
                "p95_ms": 12.0,
                "gate_p95_ms": 12.0,
            }
            for cohort, count in cohort_count_map.items()
        },
    }


def _capacity_gate_report(
    *,
    eligible=3_000,
    matched_positive_eligible=100,
    negative_eligible=400,
    random_eligible=2_500,
    distinct_random=2_500,
):
    """Build a clean release-gate report with configurable cohort totals."""

    cohort_count_map = {
        audit.CAPACITY_POSITIVE_COHORT: matched_positive_eligible,
        audit.CAPACITY_NEGATIVE_COHORT: negative_eligible,
        audit.CAPACITY_RANDOM_COHORT: random_eligible,
    }
    return {
        "enabled": True,
        "contract": "signed_isolated_standard_http_capacity_v3",
        "trust": TRUST.report_value,
        "selection": audit.sampling_seed_evidence(
            "release",
            audit.authoritative_release_seed(TRUST),
            TRUST,
        ),
        "counters": _capacity_gate_counters(
            eligible, cohort_count_map, distinct_random
        ),
        "page_contract": {
            "latency_gate": audit.CAPACITY_CLIENT_LATENCY_SOURCE,
        },
        "latency": _capacity_gate_latency(eligible, cohort_count_map),
        "complete_request_accounting": {
            "contract": "all_standard_http_attempts_v1",
            "actual_http_requests": eligible,
            "retries": 0,
            "release_clean": True,
        },
        "commitments": {
            "run_digest": "33" * 32,
            "run_digest_count": 1,
        },
    }

def _qualified_client_latency_report(monkeypatch):
    signed_duration_ns_by_class = {
        "matched_positive": 41_000_000,
        "negative": 10_000_000,
        "random": 10_000_000,
    }
    client_latency_ms_by_cohort = {
        audit.CAPACITY_POSITIVE_COHORT: 1.0,
        audit.CAPACITY_NEGATIVE_COHORT: 60.0,
        audit.CAPACITY_RANDOM_COHORT: 60.0,
    }

    def collect(_headers, **kwargs):
        observation = _observation(kwargs["query_parameters"])
        observation["server_duration_ns"] = signed_duration_ns_by_class[
            kwargs["expected_semantic_class"]
        ]
        return observation

    monkeypatch.setattr(
        audit.capacity_evidence,
        "collect_capacity_http_observation",
        collect,
    )
    collector = audit.CapacityEvidenceCollector(
        TRUST,
        contention_run_id=CONTENTION_RUN_ID,
        run_nonce=RUN_NONCE,
    )
    for ordinal, cohort in enumerate(
        (
            audit.CAPACITY_POSITIVE_COHORT,
            audit.CAPACITY_NEGATIVE_COHORT,
            audit.CAPACITY_RANDOM_COHORT,
        )
    ):
        plan = collector.plan(audit.CapacityObservationIntent(cohort, True))
        observation_record = collector.collect_response(
            plan,
            challenge=collector.fresh_challenge(),
            query_parameters=_capacity_query(code=f"9921{ordinal}"),
            response_headers={},
            response_status_code=200,
            response_body=b"{}",
            response_redirect_count=0,
            collector_received_at=dt.datetime.now(dt.timezone.utc),
            attempt_index=0,
            latency_ms=client_latency_ms_by_cohort[cohort],
        )
        collector.mark_semantic_outcome((observation_record,), successful=True)
    return collector.report()

def _sha256(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _candidate_attestation_target():
    return {
        "expected_architecture": "postgres_binary_v3",
        "expected_storage_generation": "shared_blocks_v3",
        "expected_database_backend": "postgresql",
        "expected_snapshot_lifecycle": "validated",
        "architecture_assertion": "required_postgresql_session_evidence",
        "api_path_sha256": _sha256("/api/v1/pricing/providers/audit-search-by-procedure"),
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


def _candidate_attestation_fixture_report():
    """Build the release-audit fixture with capacity evidence for attestation."""

    completed_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    started_at = completed_at - dt.timedelta(minutes=10)
    candidate_fixture_map = {
        "schema_version": 2,
        "harness": {"name": "ptg2_v3_source_api_audit", "version": "2.15.0"},
        "status": "pass",
        "profile": "release",
        "release_profile_enforced": True,
        "release_gate_eligible": True,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": 600.0,
        "target": _candidate_attestation_target(),
        "reproducibility": {},
        "source": {
            "provider_identifier_quarantine": (
                provider_identifier_quarantine_payload({})
            )
        },
        "coverage": {"failures": []},
        "checks": {
            "source_occurrence_ids": 2_500,
            "api_occurrence_ids": 2_500,
            "negative_queries": 250,
            "random_api_requests_executed": 2_500,
        },
        "http": {
            "standard_api_actual_http_requests": 3_000,
            "capacity_evidence": _capacity_gate_report(eligible=3_000),
        },
        "random_api_requests": {},
        "latency": {},
        "api_audit_sample": {
            "sample_digest": "ab" * 32,
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "failures": {"counts": {}, "examples": []},
        "limitations": [],
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": list(audit.REDACTION_EXCLUDED_FIELDS),
        },
    }

    return candidate_fixture_map
