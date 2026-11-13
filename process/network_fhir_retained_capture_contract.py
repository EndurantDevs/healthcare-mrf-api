# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed registry captures of retained normalized FHIR, not ingestion receipts."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from uuid import NAMESPACE_URL, UUID, uuid5

from process.network_address_projection import _identifier
from process.provider_directory_admission_seal import ADMISSION_RAW_METADATA_MAX_BYTES

TABLES = (
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_plan_evidence",
)
PRODUCER = "registry-retained-normalized-fhir-v1"
LEGACY_PROFILE = "legacy-descriptor-refusal-v1"
SEMANTIC_PROFILE = "admitted-semantic-v4-v1"
REFUSAL = "provider_directory_admission_shard_validation_invalid:provider directory proof shard resource total changed"
_SITE_DOMAIN = uuid5(NAMESPACE_URL, "urn:managed-network-registry:retained-fhir-location:v1")


GENERIC_UNVERIFIED_NETWORK = "generic_unverified_unapproved_network"


class RetainedFHIRCaptureError(ValueError):
    """A retained normalized capture or its native custody is unavailable."""


def require(condition):
    """Keep failures bounded and independent of source payloads."""
    if not condition:
        raise RetainedFHIRCaptureError("retained_fhir_capture_unavailable")


def canonical(value):
    """Use an unambiguous JSON encoding for registry-only identities and receipts."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value):
    """Bind a complete closed registry document, without claiming ingestion authority."""
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def is_sha256(value):
    """Recognize exact lowercase SHA-256 values without coercion."""
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def text(value, bound):
    """Require bounded exact source coordinates."""
    require(type(value) is str and 0 < len(value.encode()) <= bound and value.strip() == value and value.isprintable())


def registry_location_id(source_id, resource_id):
    """Allocate only a registry-domain identity for an exact source Location key."""
    text(source_id, 64)
    require(type(resource_id) is str and re.fullmatch(r"[A-Za-z0-9.-]{1,64}", resource_id) is not None)
    return uuid5(_SITE_DOMAIN, canonical([source_id, "Location", resource_id]))


def retained_metadata_max_bytes(profile):
    """Keep admitted semantic metadata within its existing raw admission budget."""
    require(type(profile) is str and profile in {LEGACY_PROFILE, SEMANTIC_PROFILE})
    return ADMISSION_RAW_METADATA_MAX_BYTES if profile == SEMANTIC_PROFILE else 64 * 1024**2 - 32768 - 1


@dataclass(frozen=True)
class RetainedFHIRInput:
    """Reviewed original publication, with one explicit supported admission profile."""

    schema_name: str
    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_sha256: str
    root_run_id: str
    header_relation: dict
    resource_relation: dict
    metadata_bytes: int
    metadata_text_sha256: str
    summary_sha256: str
    proof_sha256: str
    resource_counts: tuple[tuple[str, int], ...]
    resource_hashes: tuple[tuple[str, str], ...]
    descriptor_counts: tuple[tuple[str, int], ...]
    original_admission_refusal: str | None
    preparation_schema: str = "hp_snapshot_retention"
    admission_profile: str = LEGACY_PROFILE
    admission_metadata_sha256: str | None = None
    semantic_projection_as_of: str | None = None
    unresolved_plan_network_count: int = 0

    def __post_init__(self):
        """Validate the exact profile, metadata budget and retained publication coordinates."""
        _identifier(self.schema_name)
        _identifier(self.preparation_schema)
        require(self.source_id != "cms-npd")
        for name, bound in (("source_id", 64), ("endpoint_id", 64), ("dataset_id", 96), ("root_run_id", 64)):
            text(getattr(self, name), bound)
        for relation in (self.header_relation, self.resource_relation):
            require(type(relation) is dict and set(relation) == {"schema", "name"})
            _identifier(relation["schema"])
            _identifier(relation["name"])
        require(self.resource_relation["schema"] == self.schema_name)
        for name in ("dataset_sha256", "metadata_text_sha256", "summary_sha256", "proof_sha256"):
            require(is_sha256(getattr(self, name)))
        require(
            type(self.metadata_bytes) is int
            and 0 < self.metadata_bytes <= retained_metadata_max_bytes(self.admission_profile)
        )
        require(type(self.unresolved_plan_network_count) is int and self.unresolved_plan_network_count >= 0)
        if self.admission_profile == LEGACY_PROFILE:
            require(
                self.original_admission_refusal == REFUSAL
                and self.admission_metadata_sha256 is None
                and self.semantic_projection_as_of is None
                and self.unresolved_plan_network_count == 0
            )
        else:
            require(self.original_admission_refusal is None and is_sha256(self.admission_metadata_sha256))
            require(
                type(self.semantic_projection_as_of) is str
                and date.fromisoformat(self.semantic_projection_as_of).isoformat() == self.semantic_projection_as_of
            )
        _validate_retained_resource_census(self)


def _validate_retained_resource_census(origin):
    """Require complete, ordered family counts and content hashes for the admission profile."""
    require(type(origin.resource_counts) is tuple and 0 < len(origin.resource_counts) <= 64)
    require(tuple(sorted(origin.resource_counts)) == origin.resource_counts)
    require(
        all(type(name) is str and name and type(count) is int and count >= 0 for name, count in origin.resource_counts)
    )
    require(
        len(dict(origin.resource_counts)) == len(origin.resource_counts)
        and sum(dict(origin.resource_counts).values()) > 0
    )
    require(
        type(origin.resource_hashes) is tuple
        and tuple(name for name, _ in origin.resource_hashes) == tuple(dict(origin.resource_counts))
    )
    require(all(is_sha256(family_hash) for _, family_hash in origin.resource_hashes))
    require(
        type(origin.descriptor_counts) is tuple
        and tuple(name for name, _ in origin.descriptor_counts) == tuple(dict(origin.resource_counts))
    )
    require(
        all(
            type(count) is int and count >= dict(origin.resource_counts)[name]
            for name, count in origin.descriptor_counts
        )
    )
    if origin.admission_profile == LEGACY_PROFILE:
        require(sum(count for _, count in origin.descriptor_counts) > sum(dict(origin.resource_counts).values()))
    else:
        require(origin.descriptor_counts == origin.resource_counts)


@dataclass(frozen=True)
class RetainedFHIRCapture:
    """Named captured edition with logical provenance and complete content accounting."""

    origin: RetainedFHIRInput
    capture_id: str
    schema_name: str
    tables: dict
    content_sha256: str
    capture_sha256: str
    contract: str = "retained-normalized-fhir-v2"

    @property
    def release_id(self):
        """Return the logical release token used by this captured edition."""
        return "registry-capture:" + self.capture_id

    def as_dict(self):
        """Serialize the complete named capture using its canonical wire encoding."""
        return json.loads(canonical(asdict(self)))


def retained_origin(value):
    """Decode strict named provenance and the one supported historical layout."""
    if type(value) is RetainedFHIRInput:
        value.__post_init__()
        return value
    require(type(value) is dict)
    fields = set(RetainedFHIRInput.__dataclass_fields__)
    retired_fields = {"header_oid", "resource_parent_oid", "resource_leaf_oid", "reader_role"}
    origin_by_field = dict(value)
    if set(value) == fields - {"header_relation", "resource_relation"} | retired_fields:
        origin_by_field = {key: field for key, field in value.items() if key not in retired_fields}
        origin_by_field.update(
            header_relation={"schema": origin_by_field["schema_name"], "name": TABLES[0]},
            resource_relation={"schema": origin_by_field["schema_name"], "name": TABLES[1]},
        )
    require(set(origin_by_field) == fields)
    for name in ("resource_counts", "resource_hashes", "descriptor_counts"):
        require(
            type(origin_by_field[name]) in {list, tuple}
            and all(type(pair) in {list, tuple} and len(pair) == 2 for pair in origin_by_field[name])
        )
        origin_by_field[name] = tuple(tuple(pair) for pair in origin_by_field[name])
    return RetainedFHIRInput(**origin_by_field)


def capture_digest(capture):
    """Bind every capture field except its own digest."""
    document = capture.as_dict()
    document.pop("capture_sha256")
    return digest(document)


def validate_capture(capture):
    """Validate named content, retaining historical logical capture identifiers."""
    if type(capture) is dict:
        capture_by_field = dict(capture)
        legacy_fields = {
            "origin",
            "capture_id",
            "schema_name",
            "schema_oid",
            "relation_oids",
            "owner_role",
            "runtime_roles",
            "catalog_sha256",
            "content_sha256",
            "capture_sha256",
        }
        if set(capture_by_field) == legacy_fields:
            capture_by_field = {
                key: field_value
                for key, field_value in capture_by_field.items()
                if key in {"origin", "capture_id", "schema_name", "content_sha256", "capture_sha256"}
            }
            capture_by_field.update(
                tables={name: {"schema": capture_by_field["schema_name"], "name": name} for name in TABLES},
                contract="retained-normalized-fhir-v1",
            )
        require(set(capture_by_field) == set(RetainedFHIRCapture.__dataclass_fields__))
        capture_by_field["origin"] = retained_origin(capture_by_field["origin"])
        capture = RetainedFHIRCapture(**capture_by_field)
    require(type(capture) is RetainedFHIRCapture)
    retained_origin(capture.origin)
    require(
        type(capture.capture_id) is str
        and str(UUID(capture.capture_id)) == capture.capture_id
        and UUID(capture.capture_id).int
    )
    require(capture.schema_name == "registry_fhir_capture_" + UUID(capture.capture_id).hex)
    require(capture.tables == {name: {"schema": capture.schema_name, "name": name} for name in TABLES})
    require(all(is_sha256(getattr(capture, name)) for name in ("content_sha256", "capture_sha256")))
    require(capture.contract in {"retained-normalized-fhir-v1", "retained-normalized-fhir-v2"})
    require(len(canonical(capture.as_dict()).encode()) <= 65536)
    if capture.contract == "retained-normalized-fhir-v2":
        require(capture.capture_sha256 == capture_digest(capture))
    return capture
