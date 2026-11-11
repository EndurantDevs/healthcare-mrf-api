# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rooted witnesses preserve their native proof and explicit coverage provenance."""

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_source_witness_contract as contract
from process import network_fhir_source_witness_publication as publication
from process.provider_directory_rooted_graph_publication_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from tests.test_network_fhir_source_witness_contract import legacy_origin_fields
from tests.test_network_fhir_source_witness_publication import _publication_header


def _rooted_metadata(fields, count_by_resource):
    """Describe the original native cohort and explicitly bounded coverage."""
    return {
        "publication_contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
        "publication_kind": PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "source_ids": [PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID],
        "endpoint_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        "dataset_id": fields["dataset_id"],
        "acquisition_root_run_id": fields["root_run_id"],
        "root_dataset_id": "synthetic-practitioner-edition",
        "root_dataset_hash": "b" * 64,
        "root_content_proof_sha256": "c" * 64,
        "root_cohort_id": "synthetic-cohort",
        "rooted_graph_sha256": "d" * 64,
        "semantic_projection_as_of": "2026-01-01",
        "resource_counts": count_by_resource,
        "cohort_complete": True,
        "rooted_graph_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }


def _rooted_publication(*, partial=False):
    """Pair actual rooted lineage fields with the matching native proof projection."""
    fields = legacy_origin_fields()
    count_by_resource = {
        kind: int(kind == "Organization") for kind in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
    }
    hashes = dict.fromkeys(count_by_resource, "a" * 64)
    metadata = _rooted_metadata(fields, count_by_resource)
    if partial:
        from process.fhir_request_failure_policy import (
            FHIR_REQUEST_FAILURE_POLICY_ID,
            FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE,
        )

        metadata.update(
            publication_contract_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID,
            cohort_complete=False,
            request_failure_coverage={
                "policy_id": FHIR_REQUEST_FAILURE_POLICY_ID,
                "resource_coverage": FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE,
                "total_requests": 100,
                "failed_requests": 1,
                "rooted_total_requests": 50,
                "rooted_failed_requests": 0,
            },
        )
    encoded = json.dumps(metadata)
    fields.update(
        source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        source_aliases=(PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,),
        endpoint_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
        resource_counts=tuple(sorted(count_by_resource.items())),
        resource_hashes=tuple(sorted(hashes.items())),
        admission_profile=contract.OBSERVED_ROOTED_PROFILE,
        proof_sha256=metadata["rooted_graph_sha256"],
        semantic_projection_as_of=metadata["semantic_projection_as_of"],
        metadata_bytes=len(encoded.encode()),
        metadata_text_sha256=hashlib.sha256(encoded.encode()).hexdigest(),
    )
    header = _publication_header(SimpleNamespace(**fields), None)
    header.update(
        hash_contract="semantic_content_v3",
        hash_contract_type="string",
        rooted_metadata=encoded,
        proof_sha256=fields["proof_sha256"],
        semantic_as_of_absent=False,
        semantic_as_of=fields["semantic_projection_as_of"],
    )
    summary = publication.rooted_observation_summary(header, hashes)
    fields["summary_sha256"] = summary["summary_sha256"]
    origin = contract.source_origin(fields)
    return _native_reader(metadata, origin, count_by_resource), origin, header


def _native_reader(metadata, origin, count_by_resource):
    """Project the exact independently authenticated native header for this fixture."""
    native_by_field = {
        **metadata,
        "dataset_hash": origin.dataset_sha256,
        "status": "published",
        "published_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "resource_count": 1,
    }
    for kind, field in publication.ROOTED_RESOURCE_COUNT_COLUMNS:
        native_by_field[field] = count_by_resource[kind]
    reader = SimpleNamespace(fetchrow=AsyncMock(return_value={"document": native_by_field, "intrinsic_valid": True}))
    return reader


@pytest.mark.asyncio
@pytest.mark.parametrize("partial", [False, True])
async def test_rooted_witness_requires_actual_native_proof_without_generic_admission(partial):
    reader, origin, header = _rooted_publication(partial=partial)
    await publication.verify_publication(reader, origin, header)
    assert publication.publication_profile(header) == contract.OBSERVED_ROOTED_PROFILE
    assert origin.admission_metadata_sha256 is None and origin.descriptor_counts is None
    assert header["summary"] is None
    assert json.loads(header["rooted_metadata"])["cohort_complete"] is (not partial)
    assert "provider_directory_rooted_graph_dataset_intrinsic_valid" in reader.fetchrow.call_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("section", "field", "replacement"),
    [
        ("proof", "intrinsic_valid", False),
        ("document", "root_cohort_id", "different-cohort"),
        ("document", "dataset_hash", "0" * 64),
        ("document", "organization_resource_count", 0),
        ("document", "request_failure_coverage", {}),
        ("header", "rooted_metadata", "{}"),
        ("header", "proof_absent", False),
    ],
)
async def test_rooted_witness_refuses_native_or_provenance_drift(section, field, replacement):
    reader, origin, header = _rooted_publication()
    proof = reader.fetchrow.return_value
    by_section = {"proof": proof, "document": proof["document"], "header": header}
    by_section[section][field] = replacement
    with pytest.raises(ValueError):
        await publication.verify_publication(reader, origin, header)
