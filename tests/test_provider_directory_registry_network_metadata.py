# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Registry source declarations survive the existing immutable publication seal."""

import hashlib
import importlib
import json
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_npd as cms
from process.provider_directory_admission_seal import _bounded_metadata_summary
from process.provider_directory_registry_network_metadata import (
    configured_fhir_network_metadata,
    fhir_network_binding_metadata,
)

directory = importlib.import_module("process.provider_directory_fhir")


def _candidate():
    declaration = fhir_network_binding_metadata(
        source_id="synthetic-source",
        dataset_schema=directory._schema(),
        dataset_id="dataset-one",
        producer_id="declared-directory-publisher",
        alias_scope="medical",
    )
    return directory.EndpointDatasetCandidate(
        endpoint_id="endpoint-one",
        dataset_id="dataset-one",
        acquisition_root_run_id="run-one",
        source_ids=("synthetic-source",),
        selected_resources=("Organization",),
        expected_resources=("Organization",),
        import_run_id="run-one",
        previous_dataset_id=None,
        registry_network_binding_metadata=declaration,
    )


def test_network_declaration_is_sealed_without_assigning_a_canonical_network():
    candidate = _candidate()
    metadata = directory._endpoint_dataset_publication_metadata(candidate, {})
    descriptor = metadata["network_bindings"]
    assert descriptor["edition_id"] == descriptor["dataset_id"] == candidate.dataset_id
    assert descriptor["source_key_kind"] == "organization_resource_id"
    assert "network_id" not in descriptor and "company_id" not in descriptor
    assert descriptor is not candidate.registry_network_binding_metadata
    summary = _bounded_metadata_summary(metadata)
    assert summary["network_bindings"] == descriptor
    digest = hashlib.sha256(json.dumps(summary, sort_keys=True).encode()).hexdigest()
    changed_metadata_by_field = {**summary, "network_bindings": {**descriptor, "producer_id": "different-publisher"}}
    assert hashlib.sha256(json.dumps(changed_metadata_by_field, sort_keys=True).encode()).hexdigest() != digest


@pytest.mark.parametrize(
    "change",
    [
        {"source_system": "aca"},
        {"source_id": "other-source"},
        {"dataset_schema": "other_schema"},
        {"dataset_id": "other-dataset"},
        {"edition_id": "reporting-year"},
        {"source_key_kind": "network_label"},
        {"network_id": 42},
        {"producer_id": " padded"},
        {"alias_scope": "\ud800"},
    ],
)
def test_candidate_rejects_changed_or_inferred_source_coordinates(change):
    candidate = _candidate()
    changed = replace(
        candidate, registry_network_binding_metadata={**candidate.registry_network_binding_metadata, **change}
    )
    with pytest.raises(ValueError):
        directory._endpoint_dataset_candidate_metadata(changed)


def test_terminal_replay_cannot_add_remove_or_change_a_sealed_declaration():
    candidate = _candidate()
    metadata = directory._endpoint_dataset_publication_metadata(candidate, {})
    terminal = replace(candidate, already_published=True, published_metadata=metadata)
    directory._assert_finalized_endpoint_dataset_replay(terminal)
    without_declaration = replace(terminal, registry_network_binding_metadata=None)
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        directory._assert_finalized_endpoint_dataset_replay(without_declaration)
    changed = replace(
        terminal,
        registry_network_binding_metadata={
            **candidate.registry_network_binding_metadata,
            "producer_id": "another-declared-publisher",
        },
    )
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        directory._assert_finalized_endpoint_dataset_replay(changed)
    with pytest.raises(ValueError, match="registry_network_metadata_invalid"):
        directory._endpoint_dataset_publication_metadata(candidate, {}, network_bindings=metadata["network_bindings"])


def test_legacy_candidate_and_multi_source_boundaries_remain_explicit():
    candidate = _candidate()
    legacy = replace(candidate, registry_network_binding_metadata=None)
    metadata = directory._endpoint_dataset_publication_metadata(legacy, {})
    assert "network_bindings" not in metadata
    directory._assert_finalized_endpoint_dataset_replay(
        replace(legacy, already_published=True, published_metadata=metadata)
    )
    with pytest.raises(ValueError, match="registry_network_metadata_invalid"):
        directory._endpoint_dataset_candidate_metadata(
            replace(candidate, source_ids=("synthetic-source", "other-source"))
        )


def test_source_configuration_supplies_the_declared_publisher_namespace():
    candidate = _candidate()
    source_records = [
        {
            "metadata_json": {
                "registry_network_source": {
                    "producer_id": "declared-directory-publisher",
                    "alias_scope": "medical",
                }
            }
        }
    ]
    declaration = configured_fhir_network_metadata(
        source_records,
        candidate.source_ids,
        directory._schema(),
        candidate.dataset_id,
        directory._source_metadata,
    )
    assert declaration == candidate.registry_network_binding_metadata
    assert (
        configured_fhir_network_metadata(
            [{"metadata_json": {}}],
            candidate.source_ids,
            directory._schema(),
            candidate.dataset_id,
            directory._source_metadata,
        )
        is None
    )
    with pytest.raises(ValueError, match="registry_network_metadata_invalid"):
        configured_fhir_network_metadata(
            source_records + [{"metadata_json": {}}],
            ("synthetic-source", "other-source"),
            directory._schema(),
            candidate.dataset_id,
            directory._source_metadata,
        )


def test_generic_candidate_builder_seals_the_configured_source_namespace():
    """Exercise the actual builder before its ordinary candidate initialization."""
    source_records = [
        {
            "source_id": "synthetic-source",
            "metadata_json": {
                "registry_network_source": {
                    "producer_id": "declared-directory-publisher",
                    "alias_scope": "medical",
                }
            },
        }
    ]
    selection = directory.EndpointDatasetCandidateSelection(
        dataset_id="dataset-one",
        acquisition_root_run_id="run-one",
        previous_dataset_id=None,
        reused_from_checkpoint=False,
        resource_hash_contract=directory.LEGACY_RESOURCE_HASH_CONTRACT,
    )
    candidate = directory._build_endpoint_dataset_candidate(
        source_records,
        "endpoint-one",
        ("Organization",),
        ("Organization",),
        selection,
        "run-one",
        None,
        directory._endpoint_dataset_verification_profile(source_records, None),
    )
    assert candidate.registry_network_binding_metadata == _candidate().registry_network_binding_metadata
    metadata = directory._endpoint_dataset_candidate_metadata(candidate)
    assert metadata["network_bindings"] == candidate.registry_network_binding_metadata


@pytest.mark.parametrize(
    "configuration",
    [
        True,
        [],
        {},
        {"producer_id": "publisher"},
        {
            "producer_id": "publisher",
            "alias_scope": "medical",
            "network_id": 42,
        },
    ],
)
def test_source_configuration_rejects_incomplete_or_extra_authority(configuration):
    with pytest.raises(ValueError, match="registry_network_metadata_invalid"):
        configured_fhir_network_metadata(
            [{"metadata_json": {"registry_network_source": configuration}}],
            ("synthetic-source",),
            directory._schema(),
            "dataset-one",
            directory._source_metadata,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", ["new", "legacy", "declared"])
async def test_cms_candidate_seals_new_namespace_and_preserves_retained_editions(monkeypatch, retained):
    identity_by_field = {"vector_sha256": "a" * 64, "generated_at": "2026-09-24"}
    declaration = fhir_network_binding_metadata(
        source_id=cms.SOURCE_ID,
        dataset_schema=directory._schema(),
        dataset_id="dataset-one",
        producer_id=cms.ADAPTER_CONTRACT,
        alias_scope=cms.SOURCE_ID,
    )
    state_by_field = (
        {}
        if retained == "new"
        else {
            "endpoint_id": "endpoint-one",
            "status": directory.ENDPOINT_DATASET_VALIDATED,
            "acquisition_root_run_id": "original-run",
            "previous_dataset_id": None,
            "publication_metadata_json": {
                "source_release": identity_by_field,
                **({"network_bindings": declaration} if retained == "declared" else {}),
            },
        }
    )
    monkeypatch.setattr(directory, "_endpoint_dataset_state", AsyncMock(return_value=state_by_field))
    monkeypatch.setattr(directory, "_current_endpoint_dataset_id", AsyncMock(return_value=None))
    monkeypatch.setattr(
        directory, "_initialize_endpoint_dataset_candidate", AsyncMock(side_effect=lambda candidate, _: candidate)
    )
    monkeypatch.setattr(
        directory, "_dataset_resource_hash_contract", lambda _: directory.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    monkeypatch.setattr(directory, "_dataset_semantic_projection_as_of", lambda *_: identity_by_field["generated_at"])
    monkeypatch.setattr(cms.recovery, "is_disposed", AsyncMock(return_value=False))
    candidate = await cms._candidate(
        directory,
        "endpoint-one",
        "retry-run",
        identity_by_field,
        existing_dataset_id="dataset-one",
    )
    metadata = directory._endpoint_dataset_candidate_metadata(candidate)
    if retained == "legacy":
        assert candidate.registry_network_binding_metadata is None and "network_bindings" not in metadata
    else:
        assert metadata["network_bindings"] == declaration
    assert candidate.acquisition_root_run_id == ("retry-run" if retained == "new" else "original-run")
