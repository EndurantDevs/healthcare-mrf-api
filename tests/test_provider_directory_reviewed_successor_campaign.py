"""Exact successor campaign contract for reviewed Provider Directory roots."""

from __future__ import annotations

import json

from process import provider_directory_fhir_manual_catalog as manual_catalog
from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    ReviewedRootPolicy,
)
from process.provider_directory_fhir_subset_profiles import (
    SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V5_CAMPAIGN_ID,
)


SUCCESSOR_CAMPAIGN_ID = (
    "provider-directory-reviewed-subset-2026-08-11-v5-r2"
)


def _reviewed_entry() -> dict:
    manifest = json.loads(
        manual_catalog.DEFAULT_MANUAL_SOURCE_MANIFEST.read_text(
            encoding="utf-8"
        )
    )
    reviewed_entries = [
        entry
        for entry in manifest["entries"]
        if entry.get("classification")
        == manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION
    ]
    assert len(reviewed_entries) == 1
    return reviewed_entries[0]


def test_successor_campaign_retains_v5_semantics_and_requires_two_roots():
    """Bind a new two-root profile without inventing a new algorithm."""

    entry = _reviewed_entry()
    contract = entry["manual_current_version_census"]
    assert contract["verification_campaign_id"] == SUCCESSOR_CAMPAIGN_ID
    assert contract["verification_campaign_id"] != DIRECT_V5_CAMPAIGN_ID
    assert contract["strategy_version"] == SERVER_ISSUED_SUBSET_STRATEGY_VERSION
    assert contract["completion_scopes"] == list(
        SERVER_ISSUED_SUBSET_COMPLETION_SCOPES
    )

    seed_row = manual_catalog.reviewed_manual_census_seed_rows(
        entry["source_ids"][0],
        root_policy=ReviewedRootPolicy(2),
    )[0]
    metadata = seed_row["metadata_json"]
    assert metadata["provider_directory_verification_campaign_id"] == (
        SUCCESSOR_CAMPAIGN_ID
    )
    assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY][
        "required_root_count"
    ] == 2
