# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

from scripts.ptg_v4_dev_canary_reference import (
    build_v3_reference_evidence,
    compare_v4_document_to_reference,
    evaluate_database_reference_evidence,
    validate_reference_evidence,
)
from scripts.ptg_v4_dev_canary_kubernetes import FROZEN_V3_CANDIDATE_IMAGE
from scripts.ptg_v4_dev_canary_support import BoundedApiSpec, evaluate_api_probe


def _spec(snapshot_id: str) -> BoundedApiSpec:
    return BoundedApiSpec(
        snapshot_id=snapshot_id,
        npi=None,
        code_system="CPT",
        code="70553",
        limit=25,
        expected_item_count=25,
        expected_result_state="matched",
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        minimum_cold_process_samples=20,
    )


def _document(snapshot_id: str, generation: str) -> dict[str, object]:
    return {
        "result_state": "matched",
        "items": [
            {
                "plan_id": "example-plan",
                "plan_market_type": "group",
                "provider_count": 1_648,
                "provider_set_count": 1,
                "procedure_code": "70553",
                "reported_code_system": "CPT",
                "reported_code": "70553",
                "negotiation_arrangement": "FFS",
                "procedure_name": "MRI",
                "procedure_description": "MRI",
                "prices": [
                    {
                        "negotiated_rate": item_index,
                        "negotiated_type": "negotiated",
                        "expiration_date": "9999-12-31",
                        "service_code": ["00"],
                        "billing_class": "professional",
                        "setting": "both",
                        "billing_code_modifier": [],
                        "additional_information": None,
                    }
                ],
                "price_summary": [
                    {
                        "component": "global",
                        "modifier": [],
                        "rate": item_index,
                        "negotiated_type": "negotiated",
                        "billing_class": "professional",
                        "setting": "both",
                        "service_code": ["00"],
                        "raw_price_count": 1,
                    }
                ],
                "npi": 1_000_000_000 + item_index,
                "price_set_count": 1,
                "rate_pack_count": 1,
                # Mutable provider-directory enrichment must not enter the oracle.
                "provider_name": f"Provider {item_index}",
                "address": {"city": "Old city"},
            }
            for item_index in range(25)
        ],
        "provenance": {
            "snapshot_id": snapshot_id,
            "storage_generation": generation,
        },
    }


def _reference() -> dict[str, object]:
    return build_v3_reference_evidence(
        _document("v3-snapshot", "shared_blocks_v3"),
        _spec("v3-snapshot"),
        runtime_evidence={
            "contract": "ptg_v3_frozen_candidate_runtime_v1",
            "namespace": "healthporta-dev",
            "deployment": "healthcare-mrf-api-v3-candidate",
            "service": "healthcare-mrf-api-v3-candidate",
            "deployment_uid": "deployment-uid",
            "deployment_generation": 1,
            "pod_uid": "pod-uid",
            "image": FROZEN_V3_CANDIDATE_IMAGE,
            "image_digest": "a" * 64,
        },
    )


def test_reference_is_derived_without_persisting_provider_payload() -> None:
    reference = _reference()

    assert validate_reference_evidence(reference) == []
    assert reference["item_count"] == 25
    assert reference["semantic_key_count"] == 25
    assert "items" not in reference
    assert "provider_name" not in str(reference)


def test_v4_exact_page_accepts_numeric_encoding_and_directory_changes() -> None:
    v4_document = _document("v4-snapshot", "shared_blocks_v4")
    first_item = v4_document["items"][0]
    first_item["prices"][0]["negotiated_rate"] = 0.0
    first_item["price_summary"][0]["rate"] = 0.0
    first_item["provider_name"] = "Current provider display"
    first_item["address"] = {"city": "Current city"}

    failures, _page = compare_v4_document_to_reference(
        v4_document,
        _spec("v4-snapshot"),
        _reference(),
    )

    assert failures == []


@pytest.mark.parametrize("mutation", ("npi", "rate", "order", "duplicate"))
def test_v4_page_rejects_semantic_drift(mutation: str) -> None:
    v4_document = _document("v4-snapshot", "shared_blocks_v4")
    items = v4_document["items"]
    if mutation == "npi":
        items[0]["npi"] = 1_999_999_999
    elif mutation == "rate":
        items[0]["prices"][0]["negotiated_rate"] = 0.5
    elif mutation == "order":
        items[0], items[1] = items[1], items[0]
    else:
        items[-1] = deepcopy(items[0])

    failures, _page = compare_v4_document_to_reference(
        v4_document,
        _spec("v4-snapshot"),
        _reference(),
    )

    assert failures
    assert any("frozen V3" in failure or "duplicate" in failure for failure in failures)


def test_reference_rejects_operator_rewritten_query_or_digest() -> None:
    reference = _reference()
    reference["query"]["code"] = "99213"
    reference["page_digest"] = "0" * 64

    failures = validate_reference_evidence(reference)

    assert "public V3 reference does not use the fixed CPT 70553 query" in failures
    assert "public V3 reference page digest is not self-authenticating" in failures


def test_reference_rejects_recomputed_page_with_duplicate_expansion_keys() -> None:
    reference = _reference()
    duplicate_digest = reference["item_digests"][0]
    reference["item_digests"] = [duplicate_digest] * 25
    reference["semantic_key_count"] = 1
    # Use a valid page digest copied from an independently built duplicate page.
    duplicate_document = _document("v3-snapshot", "shared_blocks_v3")
    duplicate_document["items"] = [deepcopy(duplicate_document["items"][0])] * 25
    duplicate_evidence = build_v3_reference_evidence
    from scripts.ptg_v4_dev_canary_reference import semantic_page

    reference["page_digest"] = semantic_page(duplicate_document)["page_digest"]

    failures = validate_reference_evidence(reference)

    assert "public V3 reference item digests are invalid" in failures


def test_acceptance_rejects_cold_sample_from_another_reference_snapshot() -> None:
    reference = _reference()
    cold_samples = [
            {
                "process_identity": f"process-{index}",
                "process_started_at": f"2026-07-23T00:00:{index:02d}Z",
                "image_identity": "image-v4",
                "snapshot_id": "v4-snapshot",
                "latency_ms": 10,
                "first_v4_request": True,
                "document_valid": True,
                "graph_read_evidence": {
                    "passed": True,
                    "runtime_identity": {
                        "process_identity": f"process-{index}",
                        "process_started_at": (
                            f"2026-07-23T00:00:{index:02d}Z"
                        ),
                        "image_identity": "image-v4",
                    },
                },
                "semantic_page_digest": reference["page_digest"],
                "reference_snapshot_id": reference["reference_snapshot_id"],
            }
        for index in range(20)
    ]
    cold_samples[-1]["reference_snapshot_id"] = "another-v3-snapshot"

    report = evaluate_api_probe(
        spec=_spec("v4-snapshot"),
        cold_sample_evidence=cold_samples,
        warm_samples_ms=[10] * 20,
        response_documents=[_document("v4-snapshot", "shared_blocks_v4")],
        graph_read_evidence={
            "passed": True,
            "failures": [],
            "runtime_identity": {"image_identity": "image-v4"},
        },
        semantic_reference=reference,
    )

    assert report["passed"] is False
    assert (
        "persisted cold response uses a different V3 reference snapshot"
        in report["failures"]
    )


def _database_evidence_by_field() -> dict[str, object]:
    source_set_by_field = {
        "contract": "sorted_raw_container_sha256_bytes_v1",
        "source_count": 1,
        "raw_container_sha256_digest": "1" * 64,
    }
    return {
        "v4_snapshot_id": "v4-snapshot",
        "reference_snapshot_id": "v3-snapshot",
        "rollback_owner_id": "fragmented-case-391",
        "same_raw_sources": True,
        "same_source_trace_sets": True,
        "v4_source_set": source_set_by_field,
        "reference_source_set": dict(source_set_by_field),
        "reference_snapshot": {
            "snapshot_status": "published",
            "layout_state": "sealed",
            "layout_generation": "shared_blocks_v3",
            "snapshot_block_count": 100,
            "snapshot_block_foreign_key_validated": True,
        },
        "rollback_pin": {
            "owner_type": "ptg_v4_rollback",
            "owner_id": "fragmented-case-391",
            "snapshot_id": "v3-snapshot",
            "reason": "retain V3 rollback",
        },
    }


def test_database_reference_requires_exact_source_set_and_rollback_pin() -> None:
    assert (
        evaluate_database_reference_evidence(_database_evidence_by_field())[
            "passed"
        ]
        is True
    )

    evidence = _database_evidence_by_field()
    evidence["same_raw_sources"] = False
    evidence["rollback_pin"]["owner_id"] = "another-owner"
    evidence["reference_snapshot"]["snapshot_block_foreign_key_validated"] = False

    report = evaluate_database_reference_evidence(evidence)

    assert report["passed"] is False
    assert "V3 reference and V4 snapshot use different raw source sets" in report[
        "failures"
    ]
    assert "V3 reference does not have the exact rollback pin" in report["failures"]
    assert "V3 block-locator foreign key is not validated" in report["failures"]
