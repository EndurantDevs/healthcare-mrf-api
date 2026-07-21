# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from process.provider_directory_retained_artifact_contract import (
    FHIR_BUNDLE_PAGE,
    ORDERED_STREAMS,
    PAYLOAD,
    RetainedCampaignItem,
    RetainedCampaignPlan,
    endpoint_request_fence_digest,
)


ROOT = Path(__file__).resolve().parents[1]
GENERIC_CORE = tuple(
    sorted((ROOT / "process").glob("provider_directory_retained_*.py"))
) + (
    ROOT
    / "alembic"
    / "versions"
    / "20260721160000_provider_directory_retained_artifact_acquisition.py",
)
FORBIDDEN_ADAPTER_TOKENS = (
    "uhc",
    "optum",
    "aetna",
    "molina",
    "humana",
    "scan_health",
    "devoted",
    "simpra",
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _bundle_plan(source_case_by_field: dict[str, Any]) -> RetainedCampaignPlan:
    endpoint_id = _digest(f"endpoint:{source_case_by_field['endpoint_id']}")
    stream_identity = _digest(f"stream:{endpoint_id}")
    campaign_item = RetainedCampaignItem(
        source_item_id=_digest(f"item:{source_case_by_field['endpoint_id']}"),
        source_entry_sha256=_digest(f"entry:{source_case_by_field['endpoint_id']}"),
        artifact_kind=FHIR_BUNDLE_PAGE,
        family="PractitionerRole",
        collection_kind="fhir_resource",
        partition_metadata={"resource_type": "PractitionerRole", "page": 0},
        stream_identity_sha256=stream_identity,
        sequence_ordinal=0,
        item_role=PAYLOAD,
        source_locator=source_case_by_field["source_locator"],
        declared_byte_count=source_case_by_field["declared_byte_count"],
    )
    return RetainedCampaignPlan(
        adapter_id=source_case_by_field["adapter_id"],
        endpoint_id=endpoint_id,
        request_fence_id=endpoint_request_fence_digest(endpoint_id),
        credential_descriptor_sha256=_digest(
            f"credential:{source_case_by_field['adapter_id']}:fixture"
        ),
        source_census_sha256=_digest(f"census:{source_case_by_field['endpoint_id']}"),
        census_mode=ORDERED_STREAMS,
        items=(campaign_item,),
        per_item_byte_budget=4 * 1024 * 1024,
        aggregate_byte_budget=64 * 1024 * 1024,
        expected_stream_identities=(stream_identity,),
    ).validate()


@pytest.fixture(
    params=(
        {
            "adapter_id": "iowa_fhir_r4_v1",
            "endpoint_id": "iowa_medicaid_directory",
            "source_locator": "fixture://iowa/PractitionerRole?page=0",
            "declared_byte_count": 8192,
        },
        {
            "adapter_id": "uhc_fhir_r4_v1",
            "endpoint_id": "uhc_provider_directory",
            "source_locator": "fixture://uhc/PractitionerRole?page=0",
            "declared_byte_count": 16384,
        },
    ),
    ids=("non-uhc-bundle", "uhc-bundle"),
)
def source_bundle_plan(request: pytest.FixtureRequest) -> RetainedCampaignPlan:
    """Build both source fixtures through the same generic contract."""

    return _bundle_plan(request.param)


def test_bundle_sources_share_generic_contract(
    source_bundle_plan: RetainedCampaignPlan,
) -> None:
    assert source_bundle_plan.census_mode == ORDERED_STREAMS
    assert source_bundle_plan.items[0].artifact_kind == FHIR_BUNDLE_PAGE
    assert source_bundle_plan.items[0].family == "PractitionerRole"
    assert len(source_bundle_plan.campaign_id) == 64


def test_generic_core_has_no_adapter_dependency_or_vendor_tokens() -> None:
    for source_path in GENERIC_CORE:
        source_text = source_path.read_text(encoding="utf-8").lower()
        assert "import process.legacy_dataset_retained" not in source_text
        assert "from process.legacy_dataset_retained" not in source_text
        for forbidden_token in FORBIDDEN_ADAPTER_TOKENS:
            assert (
                forbidden_token not in source_text
            ), f"{source_path.name} contains adapter token {forbidden_token}"
