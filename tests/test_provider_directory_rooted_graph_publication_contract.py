# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import fields, replace
from datetime import UTC, datetime
import json
from types import SimpleNamespace

import pytest

import process.provider_directory_rooted_graph_publication as publication
import process.provider_directory_rooted_graph_publication_store as publication_store
from process.provider_directory_dataset_scoped_publication import (
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_rooted_graph_publication import (
    build_provider_directory_rooted_graph_dataset_identity,
    canonical_json,
    provider_directory_rooted_graph_publication_metadata,
    ProviderDirectoryRootedGraphDatasetIdentity,
)
from process.provider_directory_rooted_graph_publication_contract import (
    ProviderDirectoryRootedGraphDatasetReadiness,
    ProviderDirectoryRootedGraphPublicationError,
    ProviderDirectoryRootedGraphPublicationResult,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
)
from process.provider_directory_rooted_graph_twin_contract import (
    build_provider_directory_rooted_graph_twin_admission,
    build_provider_directory_rooted_graph_twin_attempt,
    ProviderDirectoryRootedGraphSealedRoot,
    ProviderDirectoryRootedGraphTwinAdmission,
    ProviderDirectoryRootedGraphTwinAttempt,
    ProviderDirectoryRootedGraphTwinError,
)
from tests.provider_directory_rooted_graph_publication_test_support import (
    dataset_identity,
    exact_current,
    readiness,
    resource_counts,
    sealed_roots,
    twin_admission,
)


def _forge(instance, **changes):
    forged = object.__new__(type(instance))
    for field in fields(instance):
        object.__setattr__(
            forged,
            field.name,
            changes.get(field.name, getattr(instance, field.name)),
        )
    return forged


@pytest.mark.parametrize(
    "variant",
    (LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT),
)
def test_twin_attempt_is_role_and_request_order_neutral(variant: str) -> None:
    baseline, candidate = sealed_roots(variant=variant)
    timestamp = datetime(2026, 8, 10, 12, tzinfo=UTC)

    forward = build_provider_directory_rooted_graph_twin_attempt(
        baseline,
        candidate,
        attempted_at=timestamp,
    )
    reverse = build_provider_directory_rooted_graph_twin_attempt(
        candidate,
        baseline,
        attempted_at=timestamp,
    )

    assert forward == reverse
    assert forward.matched is True
    assert forward.first_acquisition_id < forward.second_acquisition_id
    admitted = build_provider_directory_rooted_graph_twin_admission(
        forward,
        candidate,
        admitted_at=timestamp,
    )
    assert admitted.publication_acquisition_id == candidate.acquisition_id
    assert admitted.comparison_acquisition_id == baseline.acquisition_id
    assert admitted.publication_authority is True


def test_twin_mismatch_attempt_is_durable_but_not_admissible() -> None:
    baseline, candidate = sealed_roots()
    mismatched_candidate = _forge(candidate, resource_set_sha256="0" * 64)
    timestamp = datetime(2026, 8, 10, 12, tzinfo=UTC)
    attempt = build_provider_directory_rooted_graph_twin_attempt(
        baseline,
        mismatched_candidate,
        attempted_at=timestamp,
    )

    assert attempt.matched is False
    with pytest.raises(ValueError, match="twin_authority_invalid"):
        build_provider_directory_rooted_graph_twin_admission(
            attempt,
            mismatched_candidate,
            admitted_at=timestamp,
        )


def test_readiness_accepts_exact_partial_without_claiming_complete() -> None:
    partial = readiness(retry_exhausted_count=8)
    assert partial.cohort_complete is False
    assert partial.retry_exhausted_count == 8
    with pytest.raises(ValueError, match="dataset_readiness_invalid"):
        replace(partial, retry_exhausted_count=0)


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("pending_count", 1),
        ("leased_count", 1),
        ("completed_count", 0),
        ("error_count", 1),
        ("used_work_items", 2),
        ("used_resource_rows", 1),
        ("used_edge_rows", 0),
        ("used_payload_bytes", 1_000_001),
        ("insurance_plan_page_count", 0),
        ("root_resource_count", 10),
        ("root_dataset_hash", "not-a-hash"),
        ("acquisition_role", "observer"),
        ("run_id", "bad"),
    ),
)
def test_sealed_root_rejects_incomplete_or_unbound_proof(
    field_name: str,
    value: object,
) -> None:
    baseline, _ = sealed_roots()
    with pytest.raises(ValueError, match="sealed_root_invalid"):
        replace(baseline, **{field_name: value})


def test_twin_attempt_rejects_wrong_pair_shape() -> None:
    baseline, candidate = sealed_roots()
    timestamp = datetime(2026, 8, 10, 12, tzinfo=UTC)
    with pytest.raises(ValueError, match="twin_lineage_invalid"):
        build_provider_directory_rooted_graph_twin_attempt(
            baseline,
            baseline,
            attempted_at=timestamp,
        )
    with pytest.raises(ValueError, match="twin_lineage_invalid"):
        build_provider_directory_rooted_graph_twin_attempt(
            baseline,
            replace(candidate, acquisition_role="baseline"),
            attempted_at=timestamp,
        )
    with pytest.raises(ValueError, match="twin_lineage_invalid"):
        build_provider_directory_rooted_graph_twin_attempt(
            baseline,
            replace(candidate, run_id=baseline.run_id),
            attempted_at=timestamp,
        )
    drifted = _forge(candidate, root_dataset_hash="0" * 64)
    with pytest.raises(ValueError, match="twin_lineage_invalid"):
        build_provider_directory_rooted_graph_twin_attempt(
            baseline,
            drifted,
            attempted_at=timestamp,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("attempt_contract_id", "wrong"),
        ("attempt_id", "pdrgat_" + "0" * 48),
        ("first_acquisition_id", "bad"),
        ("second_acquisition_id", "pdrga_" + "0" * 48),
        ("matched", False),
        ("source_authority_id", ""),
        ("root_dataset_variant", "generic"),
        ("attempted_at", datetime(2026, 8, 10, 12)),
    ),
)
def test_twin_attempt_public_type_revalidates_fields(
    field_name: str,
    value: object,
) -> None:
    baseline, candidate = sealed_roots()
    attempt = build_provider_directory_rooted_graph_twin_attempt(
        baseline,
        candidate,
        attempted_at=datetime(2026, 8, 10, 12, tzinfo=UTC),
    )
    arguments_by_field = {
        field.name: (
            value if field.name == field_name else getattr(attempt, field.name)
        )
        for field in fields(attempt)
    }
    with pytest.raises(ValueError, match="twin_attempt_invalid"):
        ProviderDirectoryRootedGraphTwinAttempt(**arguments_by_field)


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("admission_contract_id", "wrong"),
        ("admission_id", "pdrgad_" + "0" * 48),
        ("publication_acquisition_id", "bad"),
        ("comparison_acquisition_id", "pdrga_" + "2" * 48),
        ("publication_authority", False),
        ("used_work_items", 2),
        ("used_payload_bytes", 1_000_001),
        ("admitted_at", datetime(2026, 8, 10, 12)),
    ),
)
def test_twin_admission_public_type_revalidates_fields(
    field_name: str,
    value: object,
) -> None:
    admission = twin_admission()
    arguments_by_field = {
        field.name: (
            value if field.name == field_name else getattr(admission, field.name)
        )
        for field in fields(admission)
    }
    with pytest.raises(ValueError, match="twin_admission_invalid"):
        ProviderDirectoryRootedGraphTwinAdmission(**arguments_by_field)


def test_twin_error_is_bounded() -> None:
    assert ProviderDirectoryRootedGraphTwinError("missing").code == "missing"
    assert ProviderDirectoryRootedGraphTwinError("unknown").code == "state"


@pytest.mark.parametrize(
    "variant",
    (LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT),
)
def test_dataset_identity_binds_exact_lineage_for_both_generations(
    variant: str,
) -> None:
    identity = dataset_identity(variant=variant)
    rebuilt = ProviderDirectoryRootedGraphDatasetIdentity(
        **{field.name: getattr(identity, field.name) for field in fields(identity)}
    )
    assert rebuilt == identity
    assert identity.root_dataset_variant == variant


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("source_id", "pdfhir_forged"),
        ("endpoint_id", "0" * 64),
        ("source_authority_id", "forged"),
        ("root_source_id", "pdfhir_forged"),
        ("root_endpoint_id", "0" * 64),
        ("practitioner_origin_source_id", "pdfhir_forged"),
        ("practitioner_origin_endpoint_id", "0" * 64),
        ("root_dataset_id", "generic-current"),
        ("root_practitioner_resource_count", 0),
        ("semantic_projection_as_of", "not-a-date"),
        ("operation_key", "not-a-hash"),
    ),
)
def test_dataset_identity_rejects_public_constructor_forgery(
    field_name: str,
    value: object,
) -> None:
    identity = dataset_identity()
    with pytest.raises(ValueError, match="dataset_identity_invalid"):
        replace(identity, **{field_name: value})


@pytest.mark.parametrize(
    "field_name",
    (
        "root_source_id",
        "root_endpoint_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "source_authority_id",
        "endpoint_signature_sha256",
        "root_dataset_id",
        "root_dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "root_resource_count",
    ),
)
def test_identity_builder_rejects_forged_admission_lineage(field_name: str) -> None:
    admission = twin_admission()
    value = 2 if field_name == "root_resource_count" else "0" * 64
    forged = _forge(admission, **{field_name: value})
    with pytest.raises(ValueError, match="dataset_admission_invalid"):
        build_provider_directory_rooted_graph_dataset_identity(
            forged,
            exact_current(),
        )


def test_identity_builder_rejects_nominal_wrong_types() -> None:
    with pytest.raises(ValueError, match="dataset_admission_invalid"):
        build_provider_directory_rooted_graph_dataset_identity(
            SimpleNamespace(),
            exact_current(),
        )
    with pytest.raises(ValueError, match="dataset_admission_invalid"):
        build_provider_directory_rooted_graph_dataset_identity(
            twin_admission(),
            SimpleNamespace(),
        )


def test_metadata_is_canonical_across_reordered_resource_counts() -> None:
    identity = dataset_identity()
    admission = twin_admission()
    reversed_count_by_type = dict(reversed(tuple(resource_counts().items())))
    metadata = provider_directory_rooted_graph_publication_metadata(
        identity,
        admission,
        previous_dataset_id=identity.root_dataset_id,
        resource_counts=reversed_count_by_type,
    )

    assert tuple(metadata["resource_counts"]) == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
    )
    encoded = canonical_json(metadata)
    assert canonical_json(json.loads(encoded)) == encoded
    assert json.loads(encoded)["resource_counts"] == resource_counts()
    assert "provider_directory_reviewed_root_policy_v1" not in metadata
    assert "acquisition_operation_key" not in metadata


@pytest.mark.parametrize(
    "counts,previous",
    (
        ({}, "root"),
        ({**resource_counts(), "foreign": 0}, "root"),
        ({**resource_counts(), "Practitioner": -1}, "root"),
        ({**resource_counts(), "Practitioner": 2}, "root"),
        (resource_counts(), None),
    ),
)
def test_metadata_rejects_nonexact_content(counts, previous) -> None:
    identity = dataset_identity()
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as error:
        provider_directory_rooted_graph_publication_metadata(
            identity,
            twin_admission(),
            previous_dataset_id=(
                identity.root_dataset_id if previous == "root" else previous
            ),
            resource_counts=counts,
        )
    assert error.value.code == "content"


def test_metadata_rejects_forged_nominal_admission() -> None:
    identity = dataset_identity()
    admission = twin_admission()
    forged = _forge(admission, rooted_graph_sha256="0" * 64)
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError):
        provider_directory_rooted_graph_publication_metadata(
            identity,
            forged,
            previous_dataset_id=identity.root_dataset_id,
            resource_counts=resource_counts(),
        )


def test_canonical_json_maps_failures_to_content() -> None:
    with pytest.raises(ProviderDirectoryRootedGraphPublicationError) as error:
        canonical_json({"invalid": float("nan")})
    assert error.value.code == "content"


@pytest.mark.parametrize(
    "variant",
    (LEGACY_PRACTITIONER_VARIANT, ROOTED_COMBINED_VARIANT),
)
def test_readiness_and_result_revalidate_public_boundaries(variant: str) -> None:
    ready = readiness(variant=variant)
    assert (
        ProviderDirectoryRootedGraphPublicationResult(
            readiness=ready,
            replayed=False,
        ).readiness
        == ready
    )


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("dataset_id", "generic"),
        ("previous_dataset_id", None),
        ("source_id", "pdfhir_forged"),
        ("endpoint_id", "0" * 64),
        ("source_authority_id", "forged"),
        ("root_dataset_hash", "bad"),
        ("practitioner_resource_count", 0),
        ("semantic_projection_as_of", "bad"),
        ("operation_key", "bad"),
        ("resource_count", 0),
        ("resource_counts", {}),
        ("publication_kind", "generic"),
        ("cohort_complete", False),
        ("rooted_graph_complete", False),
        ("endpoint_collection_complete", True),
        ("endpoint_complete", True),
    ),
)
def test_readiness_rejects_forged_fields(field_name: str, value: object) -> None:
    with pytest.raises(ValueError, match="dataset_readiness_invalid"):
        replace(readiness(), **{field_name: value})


def test_publication_result_rejects_nominal_values() -> None:
    with pytest.raises(ValueError, match="publication_result_invalid"):
        ProviderDirectoryRootedGraphPublicationResult(
            readiness=SimpleNamespace(),
            replayed=False,
        )
    with pytest.raises(ValueError, match="publication_result_invalid"):
        ProviderDirectoryRootedGraphPublicationResult(
            readiness=readiness(),
            replayed=1,
        )


@pytest.mark.asyncio
async def test_public_facades_forward_safe_batch_and_database(monkeypatch) -> None:
    sentinel_database = object()
    expected_readiness = readiness()
    expected_result = ProviderDirectoryRootedGraphPublicationResult(
        readiness=expected_readiness,
        replayed=True,
    )

    async def fake_load(dataset_id, *, database):
        assert dataset_id == expected_readiness.dataset_id
        assert database is sentinel_database
        return expected_readiness

    async def fake_publish(acquisition_id, *, database, batch_size):
        assert acquisition_id == "pdrga_" + "2" * 48
        assert database is sentinel_database
        assert batch_size == 4096
        return expected_result

    monkeypatch.setattr(publication_store, "load_dataset_readiness", fake_load)
    monkeypatch.setattr(
        publication_store,
        "publish_admitted_rooted_graph_dataset",
        fake_publish,
    )

    assert (
        await publication.load_provider_directory_rooted_graph_dataset_readiness(
            expected_readiness.dataset_id,
            database=sentinel_database,
        )
        == expected_readiness
    )
    assert (
        await publication.publish_provider_directory_rooted_graph_dataset(
            "pdrga_" + "2" * 48,
            database=sentinel_database,
        )
        == expected_result
    )
