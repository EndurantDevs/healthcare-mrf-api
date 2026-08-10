# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure tests for rooted graph scope and query identities."""

from dataclasses import replace

import pytest

from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_QUERY_PATTERN,
    ROOTED_GRAPH_SCOPE_PATTERN,
    build_provider_directory_rooted_graph_scope,
    canonical_fhir_resource_id,
    provider_directory_rooted_graph_query_id,
    provider_directory_rooted_graph_scope_id,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)


def _scope():
    return build_provider_directory_rooted_graph_scope(
        root_dataset_variant="uhc_flex_practitioner",
        root_publication_contract_id=(
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        root_source_id="synthetic-root-source",
        root_endpoint_id="f" * 64,
        acquisition_source_id="synthetic-acquisition-source",
        acquisition_endpoint_id="e" * 64,
        source_authority_id="synthetic-reviewed-authority",
        root_dataset_id="dataset-synthetic-a",
        root_dataset_hash="d" * 64,
        root_content_proof_sha256="c" * 64,
        root_resource_count=3,
    )


def test_scope_identity_binds_endpoint_and_exact_published_root_lineage():
    scope = _scope()

    assert ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(scope.scope_id)
    assert scope.scope_id == provider_directory_rooted_graph_scope_id(
        root_dataset_variant=scope.root_dataset_variant,
        root_publication_contract_id=scope.root_publication_contract_id,
        root_source_id="synthetic-root-source",
        root_endpoint_id="f" * 64,
        acquisition_source_id="synthetic-acquisition-source",
        acquisition_endpoint_id="e" * 64,
        source_authority_id="synthetic-reviewed-authority",
        root_dataset_id="dataset-synthetic-a",
        root_dataset_hash="d" * 64,
        root_content_proof_sha256="c" * 64,
        root_resource_count=3,
        max_work_items=scope.max_work_items,
        max_resource_rows=scope.max_resource_rows,
        max_edge_rows=scope.max_edge_rows,
        max_payload_bytes=scope.max_payload_bytes,
    )
    changed = provider_directory_rooted_graph_scope_id(
        root_dataset_variant=scope.root_dataset_variant,
        root_publication_contract_id=scope.root_publication_contract_id,
        root_source_id="synthetic-root-source",
        root_endpoint_id="f" * 64,
        acquisition_source_id="synthetic-acquisition-source",
        acquisition_endpoint_id="e" * 64,
        source_authority_id="synthetic-reviewed-authority",
        root_dataset_id="dataset-synthetic-a",
        root_dataset_hash="d" * 64,
        root_content_proof_sha256="c" * 64,
        root_resource_count=4,
        max_work_items=scope.max_work_items,
        max_resource_rows=scope.max_resource_rows,
        max_edge_rows=scope.max_edge_rows,
        max_payload_bytes=scope.max_payload_bytes,
    )
    assert changed != scope.scope_id


def test_scope_accepts_only_exact_legacy_or_rooted_current_owner_shapes() -> None:
    rooted = build_provider_directory_rooted_graph_scope(
        root_dataset_variant="rooted_combined",
        root_publication_contract_id=(
            "healthporta.provider-directory.rooted-graph-publication.v1"
        ),
        root_source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        root_endpoint_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        acquisition_source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        acquisition_endpoint_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        source_authority_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
        root_dataset_id="dataset-synthetic-rooted-a",
        root_dataset_hash="a" * 64,
        root_content_proof_sha256="b" * 64,
        root_resource_count=3,
    )

    assert rooted.root_source_id == rooted.acquisition_source_id
    assert rooted.root_endpoint_id == rooted.acquisition_endpoint_id
    for fields in (
        {
            "root_dataset_variant": "rooted_combined",
            "root_publication_contract_id": (
                "healthporta.provider-directory.rooted-graph-publication.v1"
            ),
            "root_source_id": "synthetic-other-source",
        },
        {
            "root_dataset_variant": "arbitrary_combined",
            "root_publication_contract_id": (
                "healthporta.provider-directory.rooted-graph-publication.v1"
            ),
        },
    ):
        scope_by_field = {
            "root_dataset_variant": rooted.root_dataset_variant,
            "root_publication_contract_id": rooted.root_publication_contract_id,
            "root_source_id": rooted.root_source_id,
            "root_endpoint_id": rooted.root_endpoint_id,
            "acquisition_source_id": rooted.acquisition_source_id,
            "acquisition_endpoint_id": rooted.acquisition_endpoint_id,
            "source_authority_id": rooted.source_authority_id,
            "root_dataset_id": rooted.root_dataset_id,
            "root_dataset_hash": rooted.root_dataset_hash,
            "root_content_proof_sha256": rooted.root_content_proof_sha256,
            "root_resource_count": rooted.root_resource_count,
        }
        scope_by_field.update(fields)
        with pytest.raises(ValueError, match="scope_identity_invalid"):
            build_provider_directory_rooted_graph_scope(**scope_by_field)


def test_scope_repr_does_not_expose_lineage_identifiers():
    scope = _scope()

    representation = repr(scope)
    assert representation == (
        "<provider-directory-rooted-graph-scope root_resources=3>"
    )
    assert scope.scope_id not in representation
    assert scope.root_dataset_id not in representation
    assert scope.acquisition_endpoint_id not in representation


@pytest.mark.parametrize(
    "change",
    [
        {"scope_id": "pdrgs_" + "0" * 48},
        {"connector_id": "pdrgc_" + "0" * 48},
        {"root_resource_type": "Organization"},
        {"root_resource_count": 2},
    ],
)
def test_scope_rejects_later_identity_drift(change):
    with pytest.raises(ValueError, match="rooted_graph_scope_inconsistent"):
        replace(_scope(), **change)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"acquisition_endpoint_id": "not-a-hash"},
        {"root_dataset_id": ""},
        {"root_dataset_id": " padded "},
        {"root_dataset_id": "x" * 97},
        {"root_dataset_hash": "D" * 64},
        {"root_content_proof_sha256": "short"},
        {"root_resource_count": True},
        {"root_resource_count": 0},
    ],
)
def test_scope_builder_rejects_noncanonical_lineage(kwargs):
    fields_by_name = {
        "root_dataset_variant": "uhc_flex_practitioner",
        "root_publication_contract_id": (
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        "root_source_id": "synthetic-root-source",
        "root_endpoint_id": "f" * 64,
        "acquisition_source_id": "synthetic-acquisition-source",
        "acquisition_endpoint_id": "e" * 64,
        "source_authority_id": "synthetic-reviewed-authority",
        "root_dataset_id": "dataset-synthetic-a",
        "root_dataset_hash": "d" * 64,
        "root_content_proof_sha256": "c" * 64,
        "root_resource_count": 3,
    }
    fields_by_name.update(kwargs)

    with pytest.raises(ValueError, match="provider_directory_rooted_graph"):
        build_provider_directory_rooted_graph_scope(**fields_by_name)


def test_query_identity_is_key_order_independent_and_scope_bound():
    scope_id = _scope().scope_id
    first = provider_directory_rooted_graph_query_id(
        scope_id,
        {"resource_type": "Organization", "kind": "direct_read"},
    )
    second = provider_directory_rooted_graph_query_id(
        scope_id,
        {"kind": "direct_read", "resource_type": "Organization"},
    )
    other = provider_directory_rooted_graph_query_id(
        scope_id,
        {"kind": "direct_read", "resource_type": "Location"},
    )

    assert ROOTED_GRAPH_QUERY_PATTERN.fullmatch(first)
    assert first == second
    assert other != first


@pytest.mark.parametrize(
    ("scope_id", "query_identity"),
    [
        ("bad", {"kind": "direct_read"}),
        ("pdrgs_" + "0" * 48, {}),
        ("pdrgs_" + "0" * 48, {"value": float("nan")}),
        ("pdrgs_" + "0" * 48, {"value": object()}),
        ("pdrgs_" + "0" * 48, {"value": "x" * 9000}),
    ],
)
def test_query_identity_rejects_invalid_inputs(scope_id, query_identity):
    with pytest.raises(ValueError, match="rooted_graph"):
        provider_directory_rooted_graph_query_id(scope_id, query_identity)


@pytest.mark.parametrize(
    "candidate",
    ["", "has/slash", " padded", "under_score", "a" * 65, None, True],
)
def test_resource_id_validation_rejects_non_fhir_ids(candidate):
    with pytest.raises(ValueError, match="resource_id_invalid"):
        canonical_fhir_resource_id(candidate)


def test_resource_id_validation_accepts_the_fhir_id_alphabet():
    assert canonical_fhir_resource_id("alpha-1.2") == "alpha-1.2"
