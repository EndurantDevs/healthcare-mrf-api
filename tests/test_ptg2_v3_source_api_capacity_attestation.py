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
from tests.ptg2_v3_source_api_capacity_gate_support import (
    _candidate_attestation_fixture_report,
    _candidate_attestation_target,
    _capacity_gate_cohort_reports,
    _capacity_gate_counters,
    _capacity_gate_latency,
    _capacity_gate_reconciliation,
    _capacity_gate_report,
    _qualified_client_latency_report,
    _sha256,
)

def test_top_level_latency_gate_uses_verified_client_http_timing(
    monkeypatch,
):
    """Gate end-to-end HTTP time after API evidence qualifies each response."""

    capacity_report = _qualified_client_latency_report(monkeypatch)
    assert capacity_report["latency"][
        "client_physical_attempt_ms"
    ]["p95_ms"] == 60.0
    assert capacity_report["latency"][
        "eligible_verified_client_http_duration_ms_by_cohort"
    ][audit.CAPACITY_POSITIVE_COHORT]["gate_p95_ms"] == 1.0
    assert capacity_report["latency"][
        "eligible_signed_server_duration_ms_by_cohort"
    ][audit.CAPACITY_POSITIVE_COHORT]["gate_p95_ms"] == 41.0

    runner = object.__new__(audit.AuditRunner)
    runner.config = _capacity_config()
    progress = audit._AuditProgress(
        first_page_first_observation_ms=[1.0],
        negative_first_page_ms=[60.0],
        random_cold_first_page_ms=[60.0],
        cold_page_ms=[1.0, 60.0, 60.0],
        positive_cold_query_ms=[2_000.0],
        negative_cold_query_ms=[2_000.0],
        random_cold_latency_ms=[2_000.0],
    )
    latency_reports = runner._build_latency_reports(progress, capacity_report)
    top_level_latency = audit.AuditRunner._latency_report(
        progress,
        latency_reports,
    )

    assert top_level_latency["gate_source"] == audit.CAPACITY_CLIENT_LATENCY_SOURCE
    assert top_level_latency["cold"]["first_page_first_observation"][
        "p95_ms"
    ] == 1.0
    assert top_level_latency["cold"]["first_page_first_observation"][
        "client_diagnostics"
    ]["by_class"]["matched_positive"]["p95_ms"] == 1.0
    assert top_level_latency["cold"]["first_page_first_observation"][
        "client_diagnostics"
    ]["by_class"]["negative"]["p95_ms"] == 60.0
    assert progress.failure_counts == {
        "negative_first_page_latency": 1,
        "random_first_page_latency": 1,
    }
    assert top_level_latency["cold"]["logical_query"]["diagnostic_only"] is True


def test_signed_latency_gate_uses_capacity_nearest_rank_p95():
    report = audit._latency_gate_summary([10.0] * 19 + [50.0])

    assert report["gate_p95_ms"] == 10.0

def test_candidate_attestation_rejects_legacy_full_source_report():
    """Only the bounded witness report may authorize candidate activation."""

    with pytest.raises(ValueError, match="missing=runtime"):
        ptg2_candidate_attestation.validate_candidate_release_audit_report(
            _candidate_attestation_fixture_report(),
            snapshot_id="snap_new",
            source_key="source_a",
            plan_id="12-3456789",
            plan_market_type="group",
        )
