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


def _capacity_query(**overrides):
    query_parameter_map = {
        "plan_id": "plan-secret",
        "snapshot_id": "snapshot-secret",
        "mode": "exact_source",
        "code_system": "CPT",
        "code": "99213",
        "npi": "1234567890",
        "include_providers": "true",
        "include_details": "true",
        "include_sources": "true",
        "include_allowed_amounts": "false",
        "include_unverified_addresses": "true",
        "order_by": "npi",
        "order": "asc",
        "limit": "100",
        "offset": "0",
    }
    query_parameter_map.update(overrides)
    return query_parameter_map


def _observation(query_parameters, *, ordinal=0):
    semantic = hashlib.sha256(
        audit.canonical_json(
            {
                "snapshot_id": query_parameters["snapshot_id"],
                "code_system": query_parameters["code_system"],
                "code": query_parameters["code"],
                "npi": query_parameters["npi"],
            }
        ).encode("utf-8")
    ).hexdigest()
    return {
        "evidence_version": capacity.CAPACITY_EVIDENCE_VERSION,
        "run_digest": "33" * 32,
        "semantic_query_digest": semantic,
        "challenge_digest": f"{ordinal + 1:064x}",
        "scope_digest": "44" * 32,
        "process_instance_digest": "55" * 32,
        "response_body_sha256": "66" * 32,
        "api_evidence_signature": "signed-redacted-value",
        "collector_received_at": "2026-07-14T12:00:00Z",
        "server_duration_ns": 12_000_000,
        "observation_ordinal": ordinal,
        "cold": ordinal == 0,
        "first_observation": ordinal == 0,
    }


def _options(*, retries=0):
    return audit.HttpApiFetcherOptions(
        base_url="https://api.example.invalid",
        api_path=audit.DEFAULT_API_PATH,
        headers={"Authorization": "Bearer private-token"},
        page_size=100,
        max_pages=5,
        timeout_seconds=5.0,
        retries=retries,
        retry_backoff_seconds=0.0,
        max_response_bytes=2 * 1024 * 1024,
        verify_tls=True,
    )


def _fetcher(handler, collector, *, retries=0):
    fetcher = audit.HttpApiFetcher(
        _options(retries=retries),
        capacity_collector=collector,
    )
    fetcher.client.close()
    fetcher.client = httpx.Client(
        transport=httpx.MockTransport(handler),
        headers=dict(_options(retries=retries).headers),
        follow_redirects=False,
    )
    return fetcher


def _page_payload(query_parameter_map, *, provider_items, total, offset):
    return {
        "result_state": "matched" if total else "no_matching_rates",
        "pricing_scope": audit.EXPECTED_PRICING_SCOPE,
        "resolved_snapshot_id": query_parameter_map["snapshot_id"],
        "query": {
            "snapshot_id": query_parameter_map["snapshot_id"],
            "plan_id": query_parameter_map["plan_id"],
            "mode": query_parameter_map["mode"],
        },
        "provenance": {
            "arch_version": audit.EXPECTED_ARCHITECTURE,
            "storage_generation": audit.EXPECTED_STORAGE_GENERATION,
            "database_backend": audit.EXPECTED_DATABASE_BACKEND,
            "database_evidence": {
                "contract": audit.DATABASE_EVIDENCE_CONTRACT,
                "server_version_num": 180000,
                "database_selected": True,
                "backend_session_active": True,
                "transaction_snapshot_observed": True,
            },
            "plan_id": query_parameter_map["plan_id"],
            "snapshot_id": query_parameter_map["snapshot_id"],
            "mode": query_parameter_map["mode"],
            "pricing_scope": audit.EXPECTED_PRICING_SCOPE,
        },
        "items": provider_items,
        "pagination": {
            "offset": offset,
            "limit": 100,
            "total": total,
            "has_more": offset + len(provider_items) < total,
        },
    }


def _contract(config, *, matched):
    return audit.PageContract(
        result_state="matched" if matched else "no_matching_rates",
        pricing_scope=audit.EXPECTED_PRICING_SCOPE,
        resolved_snapshot_id=config.snapshot_id,
        query_snapshot_id=config.snapshot_id,
        query_plan_id=config.plan_id,
        query_mode=audit.EXPECTED_QUERY_MODE,
        provenance_arch_version=audit.EXPECTED_ARCHITECTURE,
        provenance_storage_generation=audit.EXPECTED_STORAGE_GENERATION,
        provenance_database_backend=audit.EXPECTED_DATABASE_BACKEND,
        provenance_database_evidence_contract=audit.DATABASE_EVIDENCE_CONTRACT,
        provenance_postgres_server_version_num=180000,
        provenance_database_selected=True,
        provenance_backend_session_active=True,
        provenance_transaction_snapshot_observed=True,
        provenance_plan_id=config.plan_id,
        provenance_snapshot_id=config.snapshot_id,
        provenance_mode=audit.EXPECTED_QUERY_MODE,
        provenance_pricing_scope=audit.EXPECTED_PRICING_SCOPE,
    )


def _capacity_config():
    return audit.AuditConfig(
        profile="diagnostic",
        api_base_url="https://api.example.invalid",
        api_path=audit.DEFAULT_API_PATH,
        api_audit_path=audit.DEFAULT_API_AUDIT_PATH,
        plan_id="plan-secret",
        snapshot_id="snapshot-secret",
        plan_market_type=None,
        source_key=None,
        seed="capacity-test",
        source_occurrence_samples=3,
        api_occurrence_samples=3,
        negative_samples=2,
        random_api_calls=2_500,
        random_api_max_limit=100,
        min_source_occurrence_checks=1,
        min_api_occurrence_checks=1,
        min_negative_checks=1,
        min_random_api_calls=1,
        min_resolved_rate_fraction=1.0,
        max_unresolved_provider_references=0,
        max_invalid_prices=0,
        max_invalid_npis=0,
        max_invalid_field_types=0,
        page_size=100,
        max_pages=5,
        api_audit_page_size=100,
        api_audit_max_pages=5,
        warm_repeats=1,
        concurrency=1,
        failure_example_limit=10,
        verify_tls=True,
        capacity_evidence_trust=TRUST,
        capacity_contention_run_id=CONTENTION_RUN_ID,
    )

class _IntentFetcher:
    def __init__(self, config):
        self.config = config
        self.calls = []
        self.request_count = 0

    def fetch_all(
        self,
        params,
        *,
        phase,
        page_size=None,
        capacity_intent=None,
    ):
        self.calls.append((phase, page_size, capacity_intent))
        self.request_count += 1
        has_matching_code = str(params["code"]) != "00000"
        return audit.FetchResult(
            items=(),
            contracts=(_contract(self.config, matched=has_matching_code),),
            page_latencies_ms=(1.0,),
            total_latency_ms=1.0,
            pages=1,
            retries=0,
            response_fingerprint="77" * 32,
        )
