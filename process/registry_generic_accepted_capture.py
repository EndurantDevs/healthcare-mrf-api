# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve the native current semantic FHIR publication and its exact capture."""

import json
from dataclasses import dataclass
from datetime import date
from uuid import NAMESPACE_URL, uuid5

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_retained_capture_contract import (
    PRODUCER,
    SEMANTIC_PROFILE,
    RetainedFHIRInput,
    canonical,
    require,
)
from process.network_generic_source_published_snapshot import published_generic_binding
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    ProviderDirectoryAdmissionSeal,
)
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.provider_directory_resource_hash import SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
from process.provider_directory_source_summary import validate_fhir_source_summary

_IDENTITY_FIELDS = (
    "schema_name",
    "source_id",
    "endpoint_id",
    "dataset_id",
    "dataset_sha256",
    "root_run_id",
    "admission_metadata_sha256",
    "proof_sha256",
)
_CAPTURE_DOMAIN = uuid5(NAMESPACE_URL, "urn:managed-network-registry:accepted-fhir-capture:v1")


@dataclass(frozen=True)
class AcceptedGenericFHIRHead:
    """Native publication identity; this value does not issue a retained read capability."""

    schema_name: str
    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_sha256: str
    root_run_id: str
    admission_metadata_sha256: str
    proof_sha256: str
    semantic_projection_as_of: str


def generic_fhir_capture_id(publication):
    """Allocate one capture per native publication, independent of reader and physical routing."""
    require(type(publication) in {AcceptedGenericFHIRHead, RetainedFHIRInput})
    if type(publication) is RetainedFHIRInput:
        require(publication.admission_profile == SEMANTIC_PROFILE)
    return uuid5(_CAPTURE_DOMAIN, canonical([PRODUCER, *[getattr(publication, key) for key in _IDENTITY_FIELDS]]))


def require_generic_capture_head(capture, head):
    """Match an authenticated capture to the exact accepted native admission."""
    require(type(head) is AcceptedGenericFHIRHead)
    origin = capture.origin
    require(
        origin.admission_profile == SEMANTIC_PROFILE
        and all(getattr(origin, key) == getattr(head, key) for key in _IDENTITY_FIELDS)
        and origin.semantic_projection_as_of == head.semantic_projection_as_of
        and capture.capture_id == str(generic_fhir_capture_id(head))
    )
    return capture


async def _current_header(driver, schema_name, source_id, endpoint_id):
    require(driver.is_in_transaction())
    namespace = _identifier(schema_name)
    rows = await driver.fetch(
        f"""SELECT d.dataset_id,d.endpoint_id,d.acquisition_root_run_id,d.dataset_hash,d.resource_count,
        d.tableoid::oid::bigint AS header_oid,d.candidate_storage_json,d.completion_proof_required_version,
        d.publication_metadata_sha256,d.content_proof_admission_version,d.content_proof_admission_kind,
        d.content_proof_admission_sha256,d.content_proof_resource_types,
        CASE WHEN octet_length(d.publication_metadata_summary_json::text)<=$3
          THEN d.publication_metadata_summary_json::text END AS admitted_summary
        FROM {namespace}.provider_directory_endpoint_dataset d
        JOIN {namespace}.provider_directory_source s ON s.endpoint_id=d.endpoint_id
        WHERE s.source_id=$1 AND s.endpoint_id=$2 AND d.is_current AND d.status='published'
          AND d.published_at IS NOT NULL LIMIT 2""",
        source_id,
        endpoint_id,
        ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    )
    require(len(rows) == 1)
    header_by_field = dict(rows[0])
    published_generic_binding(
        header_by_field,
        dataset_id=header_by_field["dataset_id"],
        endpoint_id=endpoint_id,
        root_run_id=header_by_field["acquisition_root_run_id"],
        source_id=source_id,
    )
    return header_by_field


def _admitted_summary(header, schema_name, source_id):
    """Authenticate the admitted resource summary and reconstruct its exact committed head."""
    summary = json.loads(header["admitted_summary"])
    require(
        type(summary) is dict
        and summary.get("source_ids") == [source_id]
        and summary.get("resource_hash_contract") == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        and header["content_proof_admission_version"] == 1
        and header["content_proof_admission_kind"] == "generic"
    )
    source_summary = validate_fhir_source_summary(
        summary.get("source_summary_v1"),
        expected_by_field={
            "source_ids": [source_id],
            "dataset_id": header["dataset_id"],
            "endpoint_id": header["endpoint_id"],
            "acquisition_root_run_id": header["acquisition_root_run_id"],
            "dataset_hash": header["dataset_hash"],
        },
    )
    require(header["resource_count"] == source_summary["total_resources"])
    require(
        summary.get(ADMISSION_GENERIC_PROOF_SUMMARY_KEY)
        == {
            "dataset_hash": header["dataset_hash"],
            "resource_count": header["resource_count"],
            "resource_counts": source_summary["resource_counts"],
            "resource_hashes": source_summary["resource_hashes"],
        }
    )
    _require_admitted_plan(summary, header)
    seal = ProviderDirectoryAdmissionSeal(
        summary,
        header["publication_metadata_sha256"],
        1,
        "generic",
        header["content_proof_admission_sha256"],
        tuple(sorted(source_summary["resource_counts"])),
    )
    require(
        header["content_proof_resource_types"] == list(seal.resource_types)
        and canonical_payload_sha256(seal.digest_envelope()) == header["publication_metadata_sha256"]
    )
    projection_date = summary["semantic_projection_as_of"]
    require(type(projection_date) is str and date.fromisoformat(projection_date).isoformat() == projection_date)
    head = AcceptedGenericFHIRHead(
        schema_name,
        source_id,
        header["endpoint_id"],
        header["dataset_id"],
        header["dataset_hash"],
        header["acquisition_root_run_id"],
        header["publication_metadata_sha256"],
        header["content_proof_admission_sha256"],
        projection_date,
    )
    return head, summary, source_summary


def _require_admitted_plan(summary, header):
    plan = summary.get("dataset_network_plan")
    require(
        type(plan) is dict
        and plan.get("version") == 1
        and plan.get("complete") is True
        and plan.get("dataset_id") == header["dataset_id"]
        and type(plan.get("unresolved_reference_count")) is int
        and plan["unresolved_reference_count"] >= 0
    )


async def read_current_generic_fhir_head(driver, *, schema_name, source_id, endpoint_id):
    """Observe only native current-header metadata, without reading source resource payloads."""
    header = await _current_header(driver, schema_name, source_id, endpoint_id)
    return _admitted_summary(header, schema_name, source_id)[0]


async def is_original_generic_capture_current(driver, capture):
    """Allow an exact original capture without inventing a legacy successor admission."""
    origin = capture.origin
    namespace = _identifier(origin.schema_name)
    current_headers = await driver.fetch(
        f"""SELECT d.dataset_id,d.endpoint_id,d.dataset_hash,d.acquisition_root_run_id,d.publication_metadata_sha256,
        d.content_proof_admission_sha256,d.candidate_storage_json,d.completion_proof_required_version
        FROM {namespace}.provider_directory_endpoint_dataset d
        JOIN {namespace}.provider_directory_source s ON s.endpoint_id=d.endpoint_id
        WHERE s.source_id=$1 AND s.endpoint_id=$2 AND d.is_current AND d.status='published'
          AND d.published_at IS NOT NULL LIMIT 2""",
        origin.source_id,
        origin.endpoint_id,
    )
    require(len(current_headers) == 1)
    current_header = current_headers[0]
    binding = published_generic_binding(
        current_header,
        dataset_id=current_header["dataset_id"],
        endpoint_id=origin.endpoint_id,
        root_run_id=current_header["acquisition_root_run_id"],
        source_id=origin.source_id,
    )
    if binding is not None and origin.admission_profile != SEMANTIC_PROFILE:
        return False
    return (
        current_header["dataset_id"],
        current_header["dataset_hash"],
        current_header["acquisition_root_run_id"],
        current_header["publication_metadata_sha256"],
        current_header["content_proof_admission_sha256"],
    ) == (
        origin.dataset_id,
        origin.dataset_sha256,
        origin.root_run_id,
        origin.admission_metadata_sha256,
        origin.proof_sha256 if origin.admission_profile == SEMANTIC_PROFILE else None,
    )


async def read_accepted_generic_capture_metadata(driver, head, read_budget):
    """Authenticate the exact deterministic capture; returned metadata is not a read capability."""
    from process.registry_generic_refresh_anchors import read_original_generic_capture_metadata

    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        head.source_id,
        head.schema_name,
        head.dataset_id,
        PRODUCER,
        str(generic_fhir_capture_id(head)),
    )
    capture = await read_original_generic_capture_metadata(driver, coordinates, read_budget)
    return require_generic_capture_head(capture, head)


async def read_current_generic_fhir_input(
    driver, *, schema_name, source_id, endpoint_id, preparation_schema="hp_snapshot_retention"
):
    """Derive complete capture input only for the genuine post-commit Reader."""
    header = await _current_header(driver, schema_name, source_id, endpoint_id)
    head, summary, source_summary = _admitted_summary(header, schema_name, source_id)
    namespace = _identifier(schema_name)
    physical = await driver.fetchrow(
        f"""SELECT session_user::text AS reader_role,
        octet_length(publication_metadata_json::text)::bigint AS metadata_bytes,
        encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS metadata_sha256,
        to_regclass($2)::oid::bigint AS parent_oid,
        (SELECT tableoid::oid::bigint FROM {namespace}.provider_directory_dataset_resource
          WHERE dataset_id=$1 ORDER BY resource_type COLLATE "C",resource_id COLLATE "C" LIMIT 1) AS leaf_oid
        FROM {namespace}.provider_directory_endpoint_dataset WHERE dataset_id=$1""",
        head.dataset_id,
        f"{namespace}.provider_directory_dataset_resource",
    )
    counts = tuple(sorted(source_summary["resource_counts"].items()))
    return RetainedFHIRInput(
        head.schema_name,
        head.source_id,
        head.endpoint_id,
        head.dataset_id,
        head.dataset_sha256,
        head.root_run_id,
        header["header_oid"],
        physical["parent_oid"],
        physical["leaf_oid"],
        physical["reader_role"],
        physical["metadata_bytes"],
        physical["metadata_sha256"],
        source_summary["summary_sha256"],
        head.proof_sha256,
        counts,
        tuple(sorted(source_summary["resource_hashes"].items())),
        counts,
        None,
        preparation_schema=preparation_schema,
        admission_profile=SEMANTIC_PROFILE,
        admission_metadata_sha256=head.admission_metadata_sha256,
        semantic_projection_as_of=head.semantic_projection_as_of,
        unresolved_plan_network_count=summary["dataset_network_plan"]["unresolved_reference_count"],
    )
