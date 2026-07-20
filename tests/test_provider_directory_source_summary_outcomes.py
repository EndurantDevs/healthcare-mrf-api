from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_outcomes as outcomes
from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryError,
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
    source_summary_sha256,
    validate_fhir_source_summary,
    validate_semantic_source_summary,
)
from tests.test_provider_directory_source_outcomes import (
    DATASET_HASH,
    DATASET_ID,
    ENDPOINT_ID,
    RESOURCE_COUNTS,
    ROOT_RUN_ID,
    SELECTED_RESOURCES,
    SOURCE_IDS,
    _catalog,
    _dataset_row,
    _expected_outcome_summary,
    _identity_proof,
    _MappingResult,
    _metadata,
    _relation_metadata,
)
from tests.test_provider_directory_uhc_source_summary import _uhc_summary


def _sealed_source_summary(**count_overrides):
    count_by_field = {
        "addressed_locations": 2,
        "distinct_npis": 4,
        "organization_resources": 3,
        "geocoded_locations": 1,
        "individual_practitioners": 4,
        "network_plan_links": 5,
        "address_records": 6,
        "organization_affiliation_links": 1,
        "practitioner_role_resources": 7,
        **count_overrides,
    }
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            dataset_id=DATASET_ID,
            endpoint_id=ENDPOINT_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            dataset_hash=DATASET_HASH,
        ),
        source_ids=SOURCE_IDS,
        selected_resources=SELECTED_RESOURCES,
        count_by_resource=RESOURCE_COUNTS,
        hash_by_resource={
            resource_type: str(index) * 64
            for index, resource_type in enumerate(
                sorted(RESOURCE_COUNTS),
                start=1,
            )
        },
        count_by_field=count_by_field,
        count_by_category={
            "intentional_drop_counts": {},
            "unknown_field_counts": {},
        },
        identity_by_field={
            "semantic_contract_id": (
                "healthporta.provider-directory.fhir-normalized-resource.v1"
            )
        },
    )


def test_sealed_uhc_summary_dispatches_all_retained_fact_counts():
    source_summary_map = _uhc_summary()
    resource_count_by_type = source_summary_map["resource_counts"]
    dataset = SimpleNamespace(
        acquisition_root_run_id="uhc-root",
        dataset_id="uhc-dataset",
        endpoint_id="uhc-endpoint",
        dataset_hash="d" * 64,
        source_ids=("provider-directory-uhc",),
        resource_count=sum(resource_count_by_type.values()),
        publication_metadata={"source_summary_v1": source_summary_map},
    )

    outcome_map = outcomes.source_summary_outcome_counts(
        dataset,
        tuple(resource_count_by_type),
        resource_count_by_type,
        {},
    )

    assert outcome_map["semantic_contract_id"] == (
        "healthporta.uhc.semantic-facts.v1"
    )
    assert outcome_map["raw_provider_records"] == 18_696
    assert outcome_map["raw_address_rows"] == 46_768
    assert outcome_map["conflict_counts"] == {"name": 5}
    assert outcome_map["unknown_field_counts"] == {}
    assert "individual_practitioners" not in outcome_map


def test_fhir_validator_accepts_only_the_fhir_semantic_contract():
    fhir_summary_map = _sealed_source_summary()

    assert validate_fhir_source_summary(
        fhir_summary_map,
        expected_by_field={},
    ) == fhir_summary_map
    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_semantic_contract_invalid",
    ):
        validate_fhir_source_summary(
            _uhc_summary(),
            expected_by_field={},
        )


@pytest.mark.parametrize(
    ("field_name", "impossible_count"),
    (
        ("individual_practitioners", 5),
        ("organization_resources", 4),
        ("practitioner_role_resources", 8),
        ("addressed_locations", 3),
        ("geocoded_locations", 3),
        ("distinct_npis", 20),
    ),
)
def test_rehashed_inconsistent_fhir_metrics_fail_closed(
    field_name,
    impossible_count,
):
    source_summary_map = _sealed_source_summary()
    source_summary_map[field_name] = impossible_count
    source_summary_map["summary_sha256"] = source_summary_sha256(
        source_summary_map
    )

    with pytest.raises(
        ProviderDirectorySourceSummaryError,
        match="provider_directory_source_summary_fhir_metrics_inconsistent",
    ):
        validate_semantic_source_summary(
            source_summary_map,
            expected_by_field={},
        )


@pytest.mark.asyncio
async def test_sealed_source_summary_adds_fast_provider_metrics(monkeypatch):
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        source_summary_v1=_sealed_source_summary(),
        **_relation_metadata(),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary_map == {
        **_expected_outcome_summary(),
        "semantic_contract_id": (
            "healthporta.provider-directory.fhir-normalized-resource.v1"
        ),
        "addressed_locations": 2,
        "distinct_npis": 4,
        "organization_resources": 3,
        "geocoded_locations": 1,
        "individual_practitioners": 4,
        "address_records": 6,
        "practitioner_role_resources": 7,
    }


@pytest.mark.asyncio
async def test_tampered_or_stale_source_summary_is_not_exposed(monkeypatch):
    tampered_summary_map = _sealed_source_summary()
    tampered_summary_map["distinct_npis"] = 999
    stale_summary_map = _sealed_source_summary()
    stale_summary_map["dataset_id"] = "stale-dataset"
    foreign_summary_map = _sealed_source_summary()
    foreign_summary_map["semantic_contract_id"] = "foreign.semantic.v1"
    foreign_summary_map["summary_sha256"] = source_summary_sha256(
        foreign_summary_map
    )
    for invalid_summary_map in (
        tampered_summary_map,
        stale_summary_map,
        foreign_summary_map,
    ):
        publication_metadata = _metadata(
            outcome_resource_counts_v1=_identity_proof(),
            source_summary_v1=invalid_summary_map,
            **_relation_metadata(),
        )
        monkeypatch.setattr(
            outcomes.db,
            "execute",
            AsyncMock(
                return_value=_MappingResult(
                    [_dataset_row(publication_metadata=publication_metadata)]
                )
            ),
        )

        summary_map = (
            await outcomes.enrich_provider_directory_source_catalog(_catalog())
        )["items"][0]["outcome_summary"]

        assert "distinct_npis" not in summary_map
        assert summary_map["network_plan_links"] == 5


@pytest.mark.asyncio
async def test_source_summary_relation_mismatch_rejects_summary(monkeypatch):
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        source_summary_v1=_sealed_source_summary(network_plan_links=4),
        **_relation_metadata(),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert "distinct_npis" not in summary_map
    assert summary_map["network_plan_links"] == 5


@pytest.mark.asyncio
async def test_source_summary_requires_complete_fhir_metric_set(monkeypatch):
    incomplete_summary_map = _sealed_source_summary()
    incomplete_summary_map.pop("organization_resources")
    incomplete_summary_map["summary_sha256"] = source_summary_sha256(
        incomplete_summary_map
    )
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        source_summary_v1=incomplete_summary_map,
        **_relation_metadata(),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert "distinct_npis" not in summary_map
    assert summary_map["network_plan_links"] == 5


@pytest.mark.asyncio
async def test_source_summary_missing_relation_proof_rejects_summary(monkeypatch):
    relation_metadata_by_name = _relation_metadata()
    relation_metadata_by_name.pop("dataset_network_plan")
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        source_summary_v1=_sealed_source_summary(),
        **relation_metadata_by_name,
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert "distinct_npis" not in summary_map
    assert "network_plan_links" not in summary_map
    assert summary_map["organization_affiliation_links"] == 1


@pytest.mark.asyncio
async def test_source_summary_does_not_claim_unbound_profile_counts(monkeypatch):
    source_summary_map = _sealed_source_summary()
    source_summary_map["profile_rows"] = 12
    source_summary_map["evidence_rows"] = 34
    source_summary_map["summary_sha256"] = source_summary_sha256(
        source_summary_map
    )
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        source_summary_v1=source_summary_map,
        **_relation_metadata(),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [_dataset_row(publication_metadata=publication_metadata)]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert "profile_rows" not in summary_map
    assert "evidence_rows" not in summary_map


@pytest.mark.asyncio
async def test_source_summary_requires_nonblank_acquisition_root(monkeypatch):
    identity_proof_map = _identity_proof()
    identity_proof_map["acquisition_root_run_id"] = None
    relation_metadata_by_name = _relation_metadata()
    for relation_proof_map in relation_metadata_by_name.values():
        relation_proof_map["acquisition_root_run_id"] = None
    publication_metadata = _metadata(
        outcome_resource_counts_v1=identity_proof_map,
        source_summary_v1=_sealed_source_summary(),
        **relation_metadata_by_name,
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        acquisition_root_run_id=None,
                        publication_metadata=publication_metadata,
                    )
                ]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert "distinct_npis" not in summary_map
    assert summary_map["network_plan_links"] == 5


@pytest.mark.asyncio
async def test_source_summary_requires_dataset_hash(monkeypatch):
    publication_metadata = _metadata(
        source_summary_v1=_sealed_source_summary(),
        resource_diagnostics={
            resource_type: {
                "complete": True,
                "bounded": False,
                "error": None,
                "next_url_remaining": False,
                "rows_fetched": resource_count,
            }
            for resource_type, resource_count in RESOURCE_COUNTS.items()
        },
        **_relation_metadata(),
    )
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        dataset_hash=None,
                        publication_metadata=publication_metadata,
                    )
                ]
            )
        ),
    )

    summary_map = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary_map["resource_counts"] == dict(sorted(RESOURCE_COUNTS.items()))
    assert "distinct_npis" not in summary_map
