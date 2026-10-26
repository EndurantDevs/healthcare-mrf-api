# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed registry captures of retained normalized FHIR, not ingestion receipts."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from uuid import NAMESPACE_URL, UUID, uuid5

from process.network_address_projection import _identifier

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


@dataclass(frozen=True)
class RetainedFHIRInput:
    """Reviewed original publication, with one explicit supported admission profile."""

    schema_name: str
    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_sha256: str
    root_run_id: str
    header_oid: int
    resource_parent_oid: int
    resource_leaf_oid: int
    reader_role: str
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
        _identifier(self.schema_name)
        _identifier(self.reader_role)
        _identifier(self.preparation_schema)
        require(self.source_id != "cms-npd")
        for name, bound in (("source_id", 64), ("endpoint_id", 64), ("dataset_id", 96), ("root_run_id", 64)):
            text(getattr(self, name), bound)
        for name in ("header_oid", "resource_parent_oid", "resource_leaf_oid"):
            require(type(getattr(self, name)) is int and 0 < getattr(self, name) < 2**32)
        for name in ("dataset_sha256", "metadata_text_sha256", "summary_sha256", "proof_sha256"):
            require(is_sha256(getattr(self, name)))
        require(type(self.metadata_bytes) is int and 0 < self.metadata_bytes < 64 * 1024**2 - 32768)
        require(type(self.admission_profile) is str and self.admission_profile in {LEGACY_PROFILE, SEMANTIC_PROFILE})
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
        require(type(self.resource_counts) is tuple and 0 < len(self.resource_counts) <= 64)
        require(tuple(sorted(self.resource_counts)) == self.resource_counts)
        require(
            all(
                type(name) is str and name and type(count) is int and count >= 0 for name, count in self.resource_counts
            )
        )
        require(
            len(dict(self.resource_counts)) == len(self.resource_counts)
            and sum(dict(self.resource_counts).values()) > 0
        )
        require(
            type(self.resource_hashes) is tuple
            and tuple(name for name, _ in self.resource_hashes) == tuple(dict(self.resource_counts))
        )
        require(all(is_sha256(family_hash) for _, family_hash in self.resource_hashes))
        require(
            type(self.descriptor_counts) is tuple
            and tuple(name for name, _ in self.descriptor_counts) == tuple(dict(self.resource_counts))
        )
        require(
            all(
                type(count) is int and count >= dict(self.resource_counts)[name]
                for name, count in self.descriptor_counts
            )
        )
        if self.admission_profile == LEGACY_PROFILE:
            require(sum(count for _, count in self.descriptor_counts) > sum(dict(self.resource_counts).values()))
        else:
            require(self.descriptor_counts == self.resource_counts)


@dataclass(frozen=True)
class RetainedFHIRCapture:
    """Registry-only captured edition, complete content and native read-only custody."""

    origin: RetainedFHIRInput
    capture_id: str
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    owner_role: str
    runtime_roles: tuple[str, ...]
    catalog_sha256: str
    content_sha256: str
    capture_sha256: str

    @property
    def release_id(self):
        """Name new capture evidence without inventing an original source release."""
        return "registry-capture:" + self.capture_id

    def as_dict(self):
        """Return the exact bounded wire object."""
        return json.loads(canonical(asdict(self)))


def capture_digest(capture):
    """Bind every capture field except its own digest."""
    document = capture.as_dict()
    document.pop("capture_sha256")
    return digest(document)


def validate_capture(capture):
    """Reject open, mixed or noncanonical capture coordinates before native reads."""
    if type(capture) is dict:
        require(set(capture) == set(RetainedFHIRCapture.__dataclass_fields__))
        capture_by_field = dict(capture)
        origin_by_field = capture_by_field["origin"]
        require(type(origin_by_field) is dict and set(origin_by_field) == set(RetainedFHIRInput.__dataclass_fields__))
        origin_by_field = dict(origin_by_field)
        for name in ("resource_counts", "resource_hashes", "descriptor_counts"):
            require(
                type(origin_by_field[name]) is list
                and all(type(pair) is list and len(pair) == 2 for pair in origin_by_field[name])
            )
            origin_by_field[name] = tuple(tuple(pair) for pair in origin_by_field[name])
        capture_by_field["origin"] = RetainedFHIRInput(**origin_by_field)
        require(
            type(capture_by_field["relation_oids"]) is list
            and all(type(pair) is list and len(pair) == 2 for pair in capture_by_field["relation_oids"])
        )
        capture_by_field["relation_oids"] = tuple(tuple(pair) for pair in capture_by_field["relation_oids"])
        require(type(capture_by_field["runtime_roles"]) is list)
        capture_by_field["runtime_roles"] = tuple(capture_by_field["runtime_roles"])
        capture = RetainedFHIRCapture(**capture_by_field)
    require(type(capture) is RetainedFHIRCapture and type(capture.origin) is RetainedFHIRInput)
    require(
        type(capture.capture_id) is str
        and str(UUID(capture.capture_id)) == capture.capture_id
        and UUID(capture.capture_id).int
    )
    require(capture.schema_name == "registry_fhir_capture_" + UUID(capture.capture_id).hex)
    require(type(capture.schema_oid) is int and 0 < capture.schema_oid < 2**32)
    _identifier(capture.owner_role)
    require(type(capture.runtime_roles) is tuple and 0 < len(capture.runtime_roles) <= 32)
    require(
        tuple(sorted(set(capture.runtime_roles))) == capture.runtime_roles
        and capture.owner_role not in capture.runtime_roles
    )
    for role in capture.runtime_roles:
        _identifier(role)
    require(
        type(capture.relation_oids) is tuple
        and tuple(name for name, _ in capture.relation_oids) == tuple(sorted(TABLES))
    )
    require(all(type(oid) is int and 0 < oid < 2**32 for _, oid in capture.relation_oids))
    require(len(set(oid for _, oid in capture.relation_oids)) == len(TABLES))
    require(all(is_sha256(getattr(capture, name)) for name in ("catalog_sha256", "content_sha256", "capture_sha256")))
    require(capture.capture_sha256 == capture_digest(capture) and len(canonical(capture.as_dict()).encode()) <= 65536)
    return capture
