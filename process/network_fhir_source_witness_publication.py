# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate explicitly supported original publications without creating admission."""

import json
import struct
from contextlib import contextmanager

from ijson.backends import python as ijson_python

from process import network_fhir_retained_capture as retained
from process import provider_directory_admission_validation as admission
from process import provider_directory_proof_store as proofs
from process.network_fhir_retained_capture_contract import REFUSAL, SEMANTIC_PROFILE, is_sha256
from process.network_fhir_source_witness_contract import (
    ADMITTED_LEGACY_PROFILE,
    ADMITTED_TRANSPORT_PROFILE,
    ADMITTED_UHC_PROFILE,
    HASH_CONTRACTS,
    OBSERVED_LEGACY_PROFILE,
    OBSERVED_PROOF_PROFILE,
    OBSERVED_SEMANTIC_PROFILE,
    SourceWitnessCompletionOrigin,
    require,
    source_origin,
)
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
)
from process.provider_directory_admission_stream import _copy_field_reader, _GenericProofStream
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_generic_source_storage import validate_handoff_completion
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID,
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
    validate_semantic_source_summary,
)
from process.uhc_canonical_proof import UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY


def publication_profile(header):
    """Select by original fields, never by whether another profile's proof fails."""
    contract, kind = header["hash_contract"], header.get("content_proof_admission_kind")
    require(
        (contract is None and header["hash_contract_type"] is None)
        or (type(contract) is str and header["hash_contract_type"] == "string")
    )
    has_generic_proof = header["proof_absent"] is False
    has_uhc_proof = header.get("uhc_proof_absent", True) is False
    require(not (has_generic_proof and has_uhc_proof))
    completion = header["completion_proof_required_version"]
    if completion is not None:
        require(type(completion) is int and completion == 3 and header["completion_absent"] is False)
        require(contract == "transport_neutral_v2" and kind == "generic" and has_generic_proof and not has_uhc_proof)
        require(is_sha256(header.get("completion_proof_sha256")))
    else:
        require(header["completion_absent"] is True)
    if contract == "semantic_content_v4":
        require(completion is None)
        return SEMANTIC_PROFILE
    require(header["candidate_storage_json"] is None)
    has_no_admission = header["admission_absent"] is True
    if has_no_admission and contract == "semantic_content_v3":
        require(not has_generic_proof and not has_uhc_proof and header["proof_sha256"] is None)
        require(header["semantic_as_of_absent"] is False and type(header["semantic_as_of"]) is str)
        return OBSERVED_SEMANTIC_PROFILE
    require(header["semantic_as_of_absent"] is True and header["semantic_as_of"] is None)
    if has_no_admission:
        require(completion is None and not has_uhc_proof and contract in {None, "transport_bound_v1"})
        if has_generic_proof:
            require(is_sha256(header["proof_sha256"]))
            return OBSERVED_PROOF_PROFILE
        require(header["proof_absent"] is True and header["proof_sha256"] is None)
        return OBSERVED_LEGACY_PROFILE
    require(type(header["content_proof_admission_version"]) is int and header["content_proof_admission_version"] == 1)
    require(is_sha256(header["publication_metadata_sha256"]) and is_sha256(header["proof_sha256"]))
    if kind == "uhc_canonical":
        require(has_uhc_proof and not has_generic_proof and contract == "transport_neutral_v2" and completion is None)
        return ADMITTED_UHC_PROFILE
    require(kind == "generic" and has_generic_proof and not has_uhc_proof)
    require(contract in {None, "transport_bound_v1", "transport_neutral_v2"})
    return ADMITTED_TRANSPORT_PROFILE if contract == "transport_neutral_v2" else ADMITTED_LEGACY_PROFILE


def _summary(summary, origin):
    contract = (
        SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID
        if origin.admission_profile == ADMITTED_UHC_PROFILE
        else SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID
    )
    summary = validate_semantic_source_summary(
        summary,
        expected_by_field={
            "source_ids": [origin.source_id],
            "dataset_id": origin.dataset_id,
            "endpoint_id": origin.endpoint_id,
            "acquisition_root_run_id": origin.root_run_id,
            "dataset_hash": origin.dataset_sha256,
        },
        expected_semantic_contract_id=contract,
    )
    require(
        summary["summary_sha256"] == origin.summary_sha256
        and summary["resource_counts"] == dict(origin.resource_counts)
        and summary["resource_hashes"] == dict(origin.resource_hashes)
    )


@contextmanager
def _metadata_stream(path):
    """Reuse the bounded original parser, including duplicate-key and COPY-trailer guards."""
    stream = _GenericProofStream(path.parent)
    try:
        source, field = _copy_field_reader(path)
        with source:
            for event in ijson_python.parse(field):
                stream.event(*event)
            require(field.remaining == 0 and source.read(2) == struct.pack("!h", -1) and not source.read(1))
        require(stream.complete and stream.mode == "root")
        stream.descriptor_file.close()
        yield stream
    finally:
        stream.close()


def _historical_proof(stream, expected, origin):
    """Preserve the one known unadmitted legacy refusal while verifying every other proof guard."""
    require(stream.proof_header.get("contract_id") == proofs.PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID)
    try:
        admission._validate_finished_stream(stream, expected)
    except AdmissionSealError as error:
        require(str(error) == REFUSAL)
    else:
        require(False)
    lineage, legacy, scope = admission._validated_lineage_context(stream, expected)
    require(legacy and scope is None)
    admission._require_unchanged_lineage(stream, lineage, legacy, scope)
    admission._require_shard_scopes(stream, expected, lineage, set(dict(origin.resource_counts)))
    proofs._assert_expected_proof_contract(stream.proof_header, "transport_bound_v1", None, None)


def _validate_metadata(stream, origin, header):
    metadata = stream.metadata
    require(metadata.get("resource_hash_contract", "transport_bound_v1") == HASH_CONTRACTS[origin.admission_profile])
    require(metadata.get("semantic_projection_as_of") == origin.semantic_projection_as_of)
    require(metadata.get("source_ids") == [origin.source_id])
    _summary(metadata.get("source_summary_v1"), origin)
    _plan_summary(metadata, origin)
    if origin.admission_profile in {OBSERVED_LEGACY_PROFILE, OBSERVED_SEMANTIC_PROFILE}:
        require(not stream.proof_header and UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY not in metadata)
        return
    expected = admission._AdmissionCopyExpectation(
        origin.dataset_id,
        origin.endpoint_id,
        origin.root_run_id,
        origin.dataset_sha256,
        sum(dict(origin.resource_counts).values()),
        dict(origin.resource_hashes),
        dict(origin.resource_counts),
    )
    if origin.admission_profile == OBSERVED_PROOF_PROFILE:
        _historical_proof(stream, expected, origin)
    else:
        seal = admission._validate_finished_stream(stream, expected)
        kind = "uhc_canonical" if origin.admission_profile == ADMITTED_UHC_PROFILE else "generic"
        require(
            (seal.admission_version, seal.admission_kind, seal.proof_sha256, seal.resource_types)
            == (1, kind, origin.proof_sha256, tuple(dict(origin.resource_counts)))
        )
        _admitted_header(header, origin, kind)
        require(
            (
                header["content_proof_admission_version"],
                header["content_proof_admission_kind"],
                header["content_proof_admission_sha256"],
                header["content_proof_resource_types"],
            )
            == (1, kind, origin.proof_sha256, list(dict(origin.resource_counts)))
        )
    proof = metadata.get(UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY, stream.proof_header)
    require(proof["proof_sha256"] == origin.proof_sha256)
    counts = dict.fromkeys(dict(origin.resource_counts), 0)
    if origin.admission_profile == ADMITTED_UHC_PROFILE:
        for shard in proof["shards"]:
            for kind, count in shard["resource_counts"].items():
                require(kind in counts and type(count) is int and count > 0)
                counts[kind] += count
    else:
        require(set(stream.resource_counts) <= counts.keys())
        counts.update(stream.resource_counts)
    require(tuple(sorted(counts.items())) == origin.descriptor_counts)


def _admitted_header(header, origin, kind):
    """Authenticate the current seal while preserving allowed projection bookkeeping changes."""
    summary = json.loads(header["admitted_summary"])
    require(type(summary) is dict)
    require(summary.get("resource_hash_contract", "transport_bound_v1") == HASH_CONTRACTS[origin.admission_profile])
    require(summary.get("semantic_projection_as_of") == origin.semantic_projection_as_of)
    _summary(summary.get("source_summary_v1"), origin)
    _plan_summary(summary, origin)
    if kind == "generic":
        require(
            summary.get(ADMISSION_GENERIC_PROOF_SUMMARY_KEY)
            == {
                "dataset_hash": origin.dataset_sha256,
                "resource_count": sum(dict(origin.resource_counts).values()),
                "resource_counts": dict(origin.resource_counts),
                "resource_hashes": dict(origin.resource_hashes),
            }
        )
    seal = ProviderDirectoryAdmissionSeal(
        summary, origin.admission_metadata_sha256, 1, kind, origin.proof_sha256, tuple(dict(origin.resource_counts))
    )
    require(canonical_payload_sha256(seal.digest_envelope()) == origin.admission_metadata_sha256)


def _plan_summary(metadata, origin):
    """Older original summaries may omit misses; the Source snapshot measures them independently."""
    if "dataset_network_plan" not in metadata:
        return
    plan = metadata["dataset_network_plan"]
    require(
        type(plan) is dict
        and plan.get("version") == 1
        and plan.get("complete") is True
        and plan.get("dataset_id") == origin.dataset_id
    )
    if "unresolved_reference_count" in plan:
        retained._require_plan_summary(metadata, origin)


async def verify_publication(reader, declaration, header):
    """Revalidate original publication bytes; resource EOF and fresh replay remain independent."""
    origin = source_origin(declaration)
    require(publication_profile(header) == origin.admission_profile)
    require(header["candidate_storage_json"] is None)
    require(
        (
            header["oid"],
            header["dataset_id"],
            header["endpoint_id"],
            header["acquisition_root_run_id"],
            header["dataset_hash"],
            header["resource_count"],
            header["metadata_sha256"],
            header["metadata_bytes"],
            header["proof_sha256"],
            header["publication_metadata_sha256"],
        )
        == (
            origin.header_oid,
            origin.dataset_id,
            origin.endpoint_id,
            origin.root_run_id,
            origin.dataset_sha256,
            sum(dict(origin.resource_counts).values()),
            origin.metadata_text_sha256,
            origin.metadata_bytes,
            origin.proof_sha256,
            origin.admission_metadata_sha256,
        )
    )
    require(header["status"] in {"published", "superseded"} and header["published_at"] is not None)
    summary = json.loads(header["summary"]) if type(header["summary"]) is str else header["summary"]
    _summary(summary, origin)
    if type(origin) is SourceWitnessCompletionOrigin:
        require(
            (header["completion_proof_required_version"], header["completion_proof_sha256"])
            == (origin.completion_required_version, origin.completion_sha256)
        )
        validate_handoff_completion(header)
    else:
        require(header["completion_proof_required_version"] is None and header["completion_absent"] is True)
    async with retained._publication_metadata_copy(reader, origin) as path:
        with _metadata_stream(path) as stream:
            _validate_metadata(stream, origin, header)
