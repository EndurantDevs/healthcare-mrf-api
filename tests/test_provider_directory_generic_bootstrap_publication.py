# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Catalog-bound first-publication policy tests."""

import pytest

from api import provider_directory_source_catalog_outcomes as catalog_outcomes
from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_validated_publication_catalog as publication_catalog
from process.provider_directory_publication_catalog_authority import (
    bootstrap_catalog_authority,
)
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
    AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY,
    ValidatedPublicationCandidate,
)
from tests.test_provider_directory_generic_admission_publication import (
    CATALOG_DIGEST,
    CATALOG_ENTRY_ID,
    _generic_candidate_map,
    _generic_catalog_fixture,
    _generic_publication_fence,
    _locked_candidate_row_map,
    importer,
)
from tests.test_provider_directory_source_outcomes import _MappingResult
from tests.test_provider_directory_validated_publication_contract import (
    CANDIDATE_ID,
    CURRENT_DATASET_ID,
    ENDPOINT_ID,
    SOURCE_ID,
)


def test_bootstrap_policy_is_closed_and_forbids_an_incumbent():
    bootstrap_candidate_map = _generic_candidate_map(first_publication=True)
    assert (
        ValidatedPublicationCandidate.from_payload(
            bootstrap_candidate_map
        ).to_payload()
        == bootstrap_candidate_map
    )
    replacement_without_current_map = {
        field_name: field_value
        for field_name, field_value in {
            **bootstrap_candidate_map,
            "automatic_publication_policy": (
                AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY
            ),
        }.items()
        if not field_name.startswith("source_catalog_")
    }
    with pytest.raises(ValueError, match="expected_current_required"):
        ValidatedPublicationCandidate.from_payload(replacement_without_current_map)
    replacement_candidate_map = _generic_candidate_map()
    with pytest.raises(ValueError, match="expected_current_forbidden"):
        ValidatedPublicationCandidate.from_payload(
            {
                **replacement_candidate_map,
                "automatic_publication_policy": (
                    AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
                ),
                "source_catalog_entry_id": CATALOG_ENTRY_ID,
                "source_catalog_digest_sha256": CATALOG_DIGEST,
            }
        )
    for field_name, invalid_field_value in (
        ("source_catalog_entry_id", " example-directory"),
        ("source_catalog_digest_sha256", "D" * 64),
    ):
        with pytest.raises(ValueError, match="identity_invalid"):
            ValidatedPublicationCandidate.from_payload(
                {**bootstrap_candidate_map, field_name: invalid_field_value}
            )


@pytest.mark.parametrize("classification", ("acquisition", "bulk_acquisition"))
def test_bootstrap_authority_uses_the_current_profile_catalog(classification):
    catalog = provider_directory_source_catalog()
    entry = next(
        catalog_entry
        for catalog_entry in catalog["items"]
        if catalog_entry["classification"] == classification
        and catalog_entry["runnable"] is True
        and catalog_entry["profile_enabled"] is True
        and len(catalog_entry["source_ids"]) == 1
    )

    assert bootstrap_catalog_authority(entry["source_ids"][0]) == (
        entry["entry_id"],
        catalog["catalog_digest"],
    )
    assert bootstrap_catalog_authority("source-not-configured") is None


@pytest.mark.asyncio
async def test_catalog_bootstrap_candidate_rejects_authority_drift(monkeypatch):
    catalog_map, canonical_dataset, dataset_rows, execute, _ = (
        _generic_catalog_fixture(monkeypatch)
    )
    canonical_dataset.expected_incumbent_dataset_id = None
    dataset_rows[1]["previous_dataset_id"] = None
    execute.return_value = _MappingResult([dataset_rows[1]])

    enriched_catalog = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog_map
    )
    assert enriched_catalog["items"][0]["validated_publication_candidate"] == (
        _generic_candidate_map(first_publication=True)
    )

    monkeypatch.setattr(
        publication_catalog,
        "bootstrap_catalog_authority",
        lambda _source_id: None,
    )
    drifted_catalog = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog_map
    )
    assert "validated_publication_candidate" not in drifted_catalog["items"][0]


def test_locked_bootstrap_requires_catalog_authority_and_no_current(monkeypatch):
    monkeypatch.setattr(
        importer,
        "bootstrap_catalog_authority",
        lambda source_id: (
            (CATALOG_ENTRY_ID, CATALOG_DIGEST)
            if source_id == SOURCE_ID
            else None
        ),
    )
    fence = _generic_publication_fence(first_publication=True)
    candidate_row_map = _locked_candidate_row_map()
    candidate_row_map.update(
        previous_dataset_id=None,
        locked_current_dataset_ids=[],
    )
    importer._assert_locked_artifact_fence_datasets(
        fence,
        [candidate_row_map],
        {ENDPOINT_ID: [CANDIDATE_ID]},
    )

    monkeypatch.setattr(
        importer,
        "bootstrap_catalog_authority",
        lambda _source_id: ("other-entry", CATALOG_DIGEST),
    )
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._assert_locked_artifact_fence_datasets(
            fence,
            [candidate_row_map],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )
    monkeypatch.setattr(
        importer,
        "bootstrap_catalog_authority",
        lambda _source_id: (CATALOG_ENTRY_ID, CATALOG_DIGEST),
    )
    candidate_row_map["locked_current_dataset_ids"] = [CURRENT_DATASET_ID]
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._assert_locked_artifact_fence_datasets(
            fence,
            [candidate_row_map],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )
