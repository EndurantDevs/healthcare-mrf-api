# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate explicitly supported original publications without creating admission."""

import hashlib
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
    OBSERVED_ROOTED_PROFILE,
    OBSERVED_SEMANTIC_PROFILE,
    SourceWitnessCompletionOrigin,
    require,
    source_origin,
)
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
)
from process.provider_directory_admission_stream import _Capture, _copy_field_reader, _GenericProofStream
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_generic_source_storage import validate_handoff_completion
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID,
    SOURCE_SUMMARY_UHC_SEMANTIC_CONTRACT_ID,
    validate_semantic_source_summary,
)
from process.uhc_canonical_proof import UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY

ROOTED_RESOURCE_COUNT_COLUMNS = (
    ("Endpoint", "endpoint_resource_count"),
    ("HealthcareService", "healthcare_service_resource_count"),
    ("InsurancePlan", "insurance_plan_resource_count"),
    ("Location", "location_resource_count"),
    ("Organization", "organization_resource_count"),
    ("OrganizationAffiliation", "organization_affiliation_resource_count"),
    ("Practitioner", "practitioner_resource_count"),
    ("PractitionerRole", "practitioner_role_resource_count"),
)


class _SkippedMetadataCapture(_Capture):
    """Validate unused values without retaining their aggregate object graph."""

    def __init__(self):
        self.allow_payload_numbers = True
        self.depth = 0
        self.event_count = 0
        self.estimated_bytes = 0

    def _is_complete_after_event(self, event_name, event_value):
        normalized = self._normalized_event_value(event_name, event_value)
        # Keep the original scalar and event bounds; no aggregate is constructed.
        self.estimated_bytes = 0
        self._record_event_size(event_name, normalized)
        if event_name in {"start_map", "start_array"}:
            self.depth += 1
        elif event_name in {"end_map", "end_array"}:
            self.depth -= 1
        return self.depth == 0


class _AdmittedGenericWitnessStream(_GenericProofStream):
    """Reconstruct proof inputs while the original raw hash and seal stay authoritative."""

    def _start_capture(self, capture_scope, key, event_name, event_value, *, allow_payload_numbers=False):
        if capture_scope == "root" and key in {
            "completion_proof_v1",
            "resource_diagnostics",
            "server_issued_subset_replay_evidence",
        }:
            self.capture = _SkippedMetadataCapture()
            self.capture_target = (capture_scope, key)
            if self.capture._is_complete_after_event(event_name, event_value):
                self._store_capture()
            return
        super()._start_capture(capture_scope, key, event_name, event_value, allow_payload_numbers=allow_payload_numbers)

    def _store_capture(self):
        if isinstance(self.capture, _SkippedMetadataCapture):
            self.capture = self.capture_target = None
        else:
            super()._store_capture()


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
    if header.get("rooted_metadata") is not None:
        require(contract == "semantic_content_v3" and completion is None)
        require(not has_generic_proof and not has_uhc_proof and header["admission_absent"] is True)
        rooted_publication_metadata(header)
        return OBSERVED_ROOTED_PROFILE
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


def rooted_publication_metadata(header):
    """Read genuine rooted lineage without manufacturing a generic admission summary."""
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

    raw = header.get("rooted_metadata")
    require(type(raw) is str and 0 < len(raw.encode()) <= ADMISSION_METADATA_SUMMARY_MAX_BYTES)
    metadata = json.loads(raw)
    require(type(metadata) is dict)
    require(
        metadata.get("publication_contract_id")
        in {
            PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_CONTRACT_ID,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID,
        }
    )
    require(metadata.get("publication_kind") == PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND)
    require(metadata.get("source_ids") == [PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID])
    require(metadata.get("source_id") == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID)
    require(metadata.get("endpoint_id") == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID)
    require(metadata.get("endpoint_complete") is False and metadata.get("endpoint_collection_complete") is False)
    require(type(metadata.get("cohort_complete")) is bool and type(metadata.get("rooted_graph_complete")) is bool)
    counts = metadata.get("resource_counts")
    require(type(counts) is dict and set(counts) == set(PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES))
    require(all(type(count) is int and count >= 0 for count in counts.values()))
    require(is_sha256(metadata.get("rooted_graph_sha256")))
    return metadata


def rooted_observation_summary(header, hashes):
    """Commit observed family roots with the existing canonical summary encoding."""
    from process.provider_directory_source_summary import ProviderDirectorySourceSummaryBinding, build_source_summary

    metadata = rooted_publication_metadata(header)
    return build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            header["dataset_id"], header["endpoint_id"], header["acquisition_root_run_id"], header["dataset_hash"]
        ),
        source_ids=metadata["source_ids"],
        selected_resources=sorted(metadata["resource_counts"]),
        count_by_resource=metadata["resource_counts"],
        hash_by_resource=hashes,
    )


async def verify_rooted_publication(reader, origin, header):
    """Authenticate the actual rooted native proof and its complete immutable header."""
    from db.plan_release_storage import storage_relation
    from process.provider_directory_dataset_scoped_publication import ROOTED_INTRINSIC_VALID_FUNCTION

    metadata = rooted_publication_metadata(header)
    encoded = header["rooted_metadata"].encode()
    require(
        len(encoded) == origin.metadata_bytes and hashlib.sha256(encoded).hexdigest() == origin.metadata_text_sha256
    )
    require(
        (metadata["source_id"], metadata["endpoint_id"], metadata["acquisition_root_run_id"])
        == (origin.source_id, origin.endpoint_id, origin.root_run_id)
    )
    native = await reader.fetchrow(
        "SELECT to_jsonb(rooted) AS document,"
        + storage_relation(origin.schema_name, ROOTED_INTRINSIC_VALID_FUNCTION)
        + "(rooted.dataset_id) AS intrinsic_valid FROM "
        + storage_relation(origin.schema_name, "provider_directory_rooted_graph_dataset")
        + " rooted WHERE rooted.dataset_id=$1",
        origin.dataset_id,
    )
    require(native is not None and native["intrinsic_valid"] is True)
    actual = native["document"]
    actual = json.loads(actual) if type(actual) is str else actual
    require(type(actual) is dict)
    for field in (
        "dataset_id",
        "source_id",
        "endpoint_id",
        "acquisition_root_run_id",
        "publication_contract_id",
        "publication_kind",
        "root_dataset_id",
        "root_dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "rooted_graph_sha256",
        "cohort_complete",
        "rooted_graph_complete",
        "request_failure_coverage",
    ):
        require(actual.get(field) == metadata.get(field))
    require(actual.get("dataset_hash") == origin.dataset_sha256)
    require(actual.get("status") in {"published", "superseded"} and actual.get("published_at") is not None)
    require(actual.get("resource_count") == sum(dict(origin.resource_counts).values()))
    for kind, field in ROOTED_RESOURCE_COUNT_COLUMNS:
        require(actual.get(field) == dict(origin.resource_counts)[kind])
    require(metadata["rooted_graph_sha256"] == origin.proof_sha256)
    require(metadata["semantic_projection_as_of"] == origin.semantic_projection_as_of)
    summary = rooted_observation_summary(header, dict(origin.resource_hashes))
    require(summary["summary_sha256"] == origin.summary_sha256)


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
def _metadata_stream(path, *, admission_profile=None):
    """Reuse the bounded original parser, including duplicate-key and COPY-trailer guards."""
    stream_type = (
        _AdmittedGenericWitnessStream
        if admission_profile in {ADMITTED_LEGACY_PROFILE, ADMITTED_TRANSPORT_PROFILE}
        else _GenericProofStream
    )
    stream = stream_type(path.parent)
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
    require(header["candidate_storage_json"] is None or origin.admission_profile == OBSERVED_ROOTED_PROFILE)
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
    if origin.admission_profile == OBSERVED_ROOTED_PROFILE:
        await verify_rooted_publication(reader, origin, header)
        return
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
        with _metadata_stream(path, admission_profile=origin.admission_profile) as stream:
            _validate_metadata(stream, origin, header)
