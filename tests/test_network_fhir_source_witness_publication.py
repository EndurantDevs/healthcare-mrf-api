# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Proof-free original publications still authenticate raw bytes and complete summaries."""

import hashlib
import json
import struct
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_source_witness_contract as contract
from process import network_fhir_source_witness_publication as publication
from process import provider_directory_admission_validation as admission
from process.network_fhir_source_witness_contract import source_origin
from process.network_fhir_source_witness_publication import verify_publication
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID,
    SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS,
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
)
from tests.test_network_fhir_source_witness_contract import legacy_origin_fields, origin_fields


def _publication_header(origin, summary):
    """Return the original proof-free header bound to the supplied metadata bytes."""
    return {
        "hash_contract": None,
        "hash_contract_type": None,
        "proof_absent": True,
        "uhc_proof_absent": True,
        "completion_proof_required_version": None,
        "completion_absent": True,
        "candidate_storage_json": None,
        "admission_absent": True,
        "proof_sha256": None,
        "semantic_as_of_absent": True,
        "semantic_as_of": None,
        "dataset_id": origin.dataset_id,
        "endpoint_id": origin.endpoint_id,
        "acquisition_root_run_id": origin.root_run_id,
        "dataset_hash": origin.dataset_sha256,
        "resource_count": sum(count for _, count in origin.resource_counts),
        "metadata_sha256": origin.metadata_text_sha256,
        "metadata_bytes": origin.metadata_bytes,
        "publication_metadata_sha256": None,
        "status": "published",
        "published_at": datetime(2026, 1, 1, tzinfo=UTC),
        "summary": summary,
    }


def _original_publication(proof_key=None):
    """Pair original bytes with a header and rollback-only binary COPY reader."""
    fields = legacy_origin_fields()
    resource_count_by_type = dict(fields["resource_counts"])
    summary = build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            fields["dataset_id"], fields["endpoint_id"], fields["root_run_id"], fields["dataset_sha256"]
        ),
        source_ids=[fields["source_id"]],
        selected_resources=sorted(resource_count_by_type),
        count_by_resource=resource_count_by_type,
        hash_by_resource=dict(fields["resource_hashes"]),
        count_by_field={**dict.fromkeys(SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS, 0), "organization_resources": 1},
        count_by_category={"unknown_field_counts": {}, "intentional_drop_counts": {}},
        identity_by_field={"semantic_contract_id": SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID},
    )
    metadata = {"source_ids": [fields["source_id"]], "source_summary_v1": summary}
    if proof_key is not None:
        metadata[proof_key] = {"contract_id": "unexpected-proof"}
    encoded = json.dumps(metadata).encode()
    fields.update(
        metadata_bytes=len(encoded),
        metadata_text_sha256=hashlib.sha256(encoded).hexdigest(),
        summary_sha256=summary["summary_sha256"],
    )
    origin = source_origin(fields)
    header_by_name = _publication_header(origin, summary)
    start, rollback = AsyncMock(), AsyncMock()

    async def copy(query, dataset_id, *, output, **options):
        assert dataset_id == origin.dataset_id and options == {"format": "binary"}
        assert query.startswith("SELECT publication_metadata_json::text")
        assert f'FROM "{origin.header_relation["schema"]}"."{origin.header_relation["name"]}"' in query
        await output(
            b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, len(encoded)) + encoded + struct.pack("!h", -1)
        )
        return "COPY 1"

    reader = SimpleNamespace(transaction=lambda: SimpleNamespace(start=start, rollback=rollback), copy_from_query=copy)
    return reader, origin, header_by_name, start, rollback


@pytest.mark.asyncio
async def test_observed_legacy_publication_validates_original_copy_without_admission():
    reader, origin, header, start, rollback = _original_publication()
    await verify_publication(reader, origin, header)
    assert origin.proof_sha256 is None and origin.admission_metadata_sha256 is None
    start.assert_awaited_once()
    rollback.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("proof_key", ["provider_directory_content_proof_v1", "uhc_canonical_content_proof_v1"])
async def test_observed_legacy_publication_refuses_proof_hidden_from_header(proof_key):
    reader, origin, header, _, _ = _original_publication(proof_key)
    with pytest.raises(ValueError):
        await verify_publication(reader, origin, header)


def _completion_header():
    """Describe the admitted original completion profile without granting publication."""
    return _publication_header(source_origin(legacy_origin_fields()), {}) | {
        "hash_contract": "transport_neutral_v2",
        "hash_contract_type": "string",
        "proof_absent": False,
        "proof_sha256": "a" * 64,
        "completion_proof_required_version": 3,
        "completion_absent": False,
        "completion_proof_sha256": "b" * 64,
        "admission_absent": False,
        "content_proof_admission_kind": "generic",
        "content_proof_admission_version": 1,
        "publication_metadata_sha256": "c" * 64,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"completion_proof_required_version": True},
        {"completion_proof_required_version": 2},
        {"completion_absent": True},
        {"completion_proof_sha256": None},
        {"hash_contract": "transport_bound_v1"},
        {"proof_absent": True},
        {"uhc_proof_absent": False},
        {"admission_absent": True},
    ],
)
def test_completion_profile_refuses_incomplete_proof_instead_of_legacy_fallback(change):
    header = _completion_header()
    assert publication.publication_profile(header) == contract.ADMITTED_TRANSPORT_PROFILE
    with pytest.raises(contract.SourceWitnessContractError, match="source_fhir_witness_contract_invalid"):
        publication.publication_profile(header | change)


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"proof_absent": False, "proof_sha256": "a" * 64}, contract.OBSERVED_PROOF_PROFILE),
        (
            {
                "hash_contract": "semantic_content_v3",
                "hash_contract_type": "string",
                "semantic_as_of_absent": False,
                "semantic_as_of": "2026-01-01",
            },
            contract.OBSERVED_SEMANTIC_PROFILE,
        ),
    ],
)
def test_observed_profiles_require_the_original_proof_family(changes, expected):
    header = _publication_header(source_origin(legacy_origin_fields()), {}) | changes
    assert publication.publication_profile(header) == expected
    with pytest.raises(contract.SourceWitnessContractError, match="source_fhir_witness_contract_invalid"):
        publication.publication_profile(header | {"uhc_proof_absent": False})


def _historical_copy(tmp_path, shard_count, damage):
    """Seal real legacy descriptors whose aggregate predates deduplicated totals."""
    from tests.test_provider_directory_dataset_selection_bounded_db import (
        _large_metadata_by_field,
        _replace_with_legacy_contract,
    )

    fields = origin_fields()
    metadata = _large_metadata_by_field(
        shard_count,
        dataset_id=fields["dataset_id"],
        endpoint_id=fields["endpoint_id"],
        root_run_id=fields["root_run_id"],
        source_id_list=[fields["source_id"]],
    )
    proof = metadata["provider_directory_content_proof_v1"]
    _replace_with_legacy_contract(proof)
    proof.update(resource_count=1, resource_counts={"Location": 1})
    proof.pop("proof_sha256")
    proof["proof_sha256"] = contract.digest(proof)
    metadata["resource_hash_contract"] = "transport_bound_v1"
    metadata.pop("semantic_projection_as_of")
    metadata.pop("proof_resource_scope")
    if damage == "parent":
        metadata["acquisition_root_run_id"] = "foreign-root"
    elif damage == "lineage":
        metadata["source_ids"] = ["foreign-source"]
    fields.update(
        admission_profile=contract.OBSERVED_PROOF_PROFILE,
        admission_metadata_sha256=None,
        semantic_projection_as_of=None,
        dataset_sha256=proof["dataset_hash"],
        proof_sha256=proof["proof_sha256"],
        resource_counts=(("Location", 1),),
        resource_hashes=(("Location", proof["resource_hashes"]["Location"]),),
        descriptor_counts=(("Location", 2),),
    )
    origin = source_origin(fields)
    encoded = json.dumps(metadata).encode()
    path = tmp_path / "publication.copy"
    path.write_bytes(
        b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, len(encoded)) + encoded + struct.pack("!h", -1)
    )
    expected = admission._AdmissionCopyExpectation(
        origin.dataset_id, origin.endpoint_id, origin.root_run_id, origin.dataset_sha256, 1
    )
    return path, expected, origin


@pytest.mark.parametrize("case", ["known_refusal", "normal_admission", "parent", "lineage"])
def test_historical_profile_accepts_only_its_original_refusal_and_rechecks_lineage(tmp_path, case):
    path, expected, origin = _historical_copy(tmp_path, 1 if case == "normal_admission" else 2, case)
    with publication._metadata_stream(path) as stream:
        if case == "known_refusal":
            with pytest.raises(admission.AdmissionSealError, match="proof shard resource total changed"):
                admission._validate_finished_stream(stream, expected)
            publication._historical_proof(stream, expected, origin)
        else:
            with pytest.raises(contract.SourceWitnessContractError, match="source_fhir_witness_contract_invalid"):
                publication._historical_proof(stream, expected, origin)


@pytest.mark.parametrize("unresolved", [1, False])
def test_legacy_plan_summary_preserves_exact_optional_count(unresolved):
    origin = source_origin(legacy_origin_fields())
    plan_by_field = {"version": 1, "complete": True, "dataset_id": origin.dataset_id}
    publication._plan_summary({"dataset_network_plan": plan_by_field}, origin)
    publication._plan_summary({"dataset_network_plan": plan_by_field | {"unresolved_reference_count": 0}}, origin)
    with pytest.raises(ValueError, match="^retained_fhir_capture_unavailable$"):
        publication._plan_summary(
            {"dataset_network_plan": plan_by_field | {"unresolved_reference_count": unresolved}}, origin
        )


def test_admitted_profile_keeps_canonical_proof_distinct_from_generic_completion():
    header = _completion_header() | {
        "proof_absent": True,
        "uhc_proof_absent": False,
        "content_proof_admission_kind": "uhc_canonical",
        "completion_proof_required_version": None,
        "completion_absent": True,
        "completion_proof_sha256": None,
    }
    assert publication.publication_profile(header) == contract.ADMITTED_UHC_PROFILE
    with pytest.raises(contract.SourceWitnessContractError, match="source_fhir_witness_contract_invalid"):
        publication.publication_profile(header | {"hash_contract": "transport_bound_v1"})


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["marker", "dataset", "missing"])
async def test_completion_publication_revalidates_original_proof_before_copy(damage):
    from process.provider_directory_generic_candidate import GenericCandidateError
    from process.provider_directory_generic_source_storage import validate_handoff_completion
    from tests.provider_directory_fhir_subset_completion_support import build_proof_pair

    proof, digest, _ = build_proof_pair()
    reader, original, header, start, rollback = _original_publication()
    fields = asdict(original) | {
        "admission_profile": contract.ADMITTED_TRANSPORT_PROFILE,
        "proof_sha256": "a" * 64,
        "admission_metadata_sha256": "c" * 64,
        "descriptor_counts": original.resource_counts,
        "completion_required_version": 3,
        "completion_sha256": digest,
    }
    origin = source_origin(fields)
    header.update(
        {
            field: header_value
            for field, header_value in _completion_header().items()
            if field
            in {
                "hash_contract",
                "hash_contract_type",
                "proof_absent",
                "proof_sha256",
                "completion_proof_required_version",
                "completion_absent",
                "admission_absent",
                "content_proof_admission_kind",
                "content_proof_admission_version",
                "publication_metadata_sha256",
            }
        }
    )
    header.update(
        completion_proof_sha256=digest,
        completion_proof_json=proof,
        previous_dataset_id=None,
        content_proof_admission_sha256="a" * 64,
    )
    validate_handoff_completion(
        header | {"dataset_hash": proof["dataset"]["hash"], "resource_count": proof["dataset"]["count"]}
    )
    if damage == "marker":
        header["completion_proof_sha256"] = "f" * 64
    elif damage == "missing":
        header["completion_proof_json"] = None
    error = contract.SourceWitnessContractError if damage == "marker" else GenericCandidateError
    message = {
        "marker": "source_fhir_witness_contract_invalid",
        "dataset": "completion_dataset_changed",
        "missing": "completion_proof_invalid",
    }[damage]
    with pytest.raises(error, match=message):
        await verify_publication(reader, origin, header)
    start.assert_not_awaited()
    rollback.assert_not_awaited()
