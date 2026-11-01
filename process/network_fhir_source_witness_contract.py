# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed Source witness declarations; native export and capture custody confer authority."""

import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_fhir_retained_capture_contract import (
    SEMANTIC_PROFILE,
    canonical,
    digest,
    is_sha256,
    text,
)
from process.network_fhir_retained_capture_contract import TABLES as RETAINED_TABLES
from process.registry_fhir_catalog_extraction import fhir_source_uri

CONTRACT = "fhir-source-membership-witness-v1"
CAPTURE_CONTRACT = "registry-source-fhir-witness-capture-v1"
PRODUCER = "registry-source-fhir-witness-v1"
SCHEMA_PREFIX = "registry_fhir_witness_"
COMMENT_PREFIX = "registry-source-fhir-witness-v1:"
TABLES = RETAINED_TABLES[1:]


class SourceWitnessContractError(ValueError):
    """A Source witness declaration or its closed capture differs."""


def require(condition):
    """Keep refusal independent of source payloads and coordinates."""
    if not condition:
        raise SourceWitnessContractError("source_fhir_witness_contract_invalid")


def _oid(value):
    require(type(value) is int and 0 < value < 2**32)


def _system_identifier(value):
    require(
        type(value) is str
        and value.isascii()
        and 0 < len(value) <= 20
        and value.isdecimal()
        and str(int(value)) == value
        and 0 < int(value) < 2**64
    )


def _pairs(value):
    require(type(value) in {list, tuple})
    require(all(type(pair) in {list, tuple} and len(pair) == 2 for pair in value))
    return tuple(tuple(pair) for pair in value)


def canonical_timestamp(value):
    """Encode an aware instant once; persisted strings must already be canonical UTC."""
    if type(value) is datetime:
        require(value.tzinfo is not None and value.utcoffset() is not None)
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    require(type(value) is str and len(value) == 32)
    try:
        instant = datetime.fromisoformat(value)
    except ValueError:
        raise SourceWitnessContractError("source_fhir_witness_contract_invalid") from None
    require(instant.tzinfo is not None and instant.utcoffset() is not None)
    require(instant.astimezone(UTC).isoformat(timespec="microseconds") == value)
    return value


@dataclass(frozen=True)
class SourceWitnessOrigin:
    """The exact 25-field Source declaration, without any Reader authority fields."""

    schema_name: str
    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_sha256: str
    root_run_id: str
    header_oid: int
    resource_parent_oid: int
    resource_leaf_oid: int
    metadata_bytes: int
    metadata_text_sha256: str
    summary_sha256: str
    proof_sha256: str
    resource_counts: tuple[tuple[str, int], ...]
    resource_hashes: tuple[tuple[str, str], ...]
    descriptor_counts: tuple[tuple[str, int], ...]
    admission_profile: str
    admission_metadata_sha256: str | None
    semantic_projection_as_of: str | None
    unresolved_plan_network_count: int
    source_uri: str
    source_aliases: tuple[str, ...]
    native_catalog_sha256: str
    database_oid: int
    database_system_identifier: str

    def __post_init__(self):
        _identifier(self.schema_name)
        for name, bound in (("source_id", 64), ("endpoint_id", 64), ("dataset_id", 96), ("root_run_id", 64)):
            text(getattr(self, name), bound)
        for name in ("header_oid", "resource_parent_oid", "resource_leaf_oid", "database_oid"):
            _oid(getattr(self, name))
        for name in (
            "dataset_sha256",
            "metadata_text_sha256",
            "summary_sha256",
            "proof_sha256",
            "native_catalog_sha256",
        ):
            require(is_sha256(getattr(self, name)))
        require(type(self.metadata_bytes) is int and 0 < self.metadata_bytes < 64 * 1024**2 - 32768)
        _system_identifier(self.database_system_identifier)
        require(
            type(self.source_aliases) is tuple
            and 0 < len(self.source_aliases) <= 1000
            and all(type(alias) is str for alias in self.source_aliases)
            and tuple(sorted(set(self.source_aliases))) == self.source_aliases
            and self.source_id in self.source_aliases
        )
        for alias in self.source_aliases:
            text(alias, 64)
        require(type(self.source_uri) is str and fhir_source_uri(self.source_uri) is not None)
        _origin_families(self)
        require(self.source_id != "cms-npd" and self.admission_profile == SEMANTIC_PROFILE)
        require(type(self.unresolved_plan_network_count) is int and self.unresolved_plan_network_count >= 0)
        require(is_sha256(self.admission_metadata_sha256) and self.descriptor_counts == self.resource_counts)
        require(
            type(self.semantic_projection_as_of) is str
            and date.fromisoformat(self.semantic_projection_as_of).isoformat() == self.semantic_projection_as_of
        )


def _origin_families(origin):
    for name in ("resource_counts", "resource_hashes", "descriptor_counts"):
        pairs = getattr(origin, name)
        require(type(pairs) is tuple and all(type(pair) is tuple and len(pair) == 2 for pair in pairs))
    families = tuple(name for name, _ in origin.resource_counts)
    require(
        0 < len(families) <= 64
        and all(type(name) is str and name for name in families)
        and tuple(sorted(set(families))) == families
        and all(type(count) is int and count >= 0 for _, count in origin.resource_counts)
        and sum(count for _, count in origin.resource_counts) > 0
    )
    require(tuple(name for name, _ in origin.resource_hashes) == families)
    require(tuple(name for name, _ in origin.descriptor_counts) == families)
    require(all(is_sha256(root) for _, root in origin.resource_hashes))
    require(all(type(count) is int and count >= 0 for _, count in origin.descriptor_counts))


def source_origin(value):
    """Decode a declaration only; the original Source factory authenticates it."""
    if type(value) is SourceWitnessOrigin:
        value.__post_init__()
        return value
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    require(type(value) is dict and set(value) == set(SourceWitnessOrigin.__dataclass_fields__))
    origin_by_field = dict(value)
    for name in ("resource_counts", "resource_hashes", "descriptor_counts"):
        origin_by_field[name] = _pairs(origin_by_field[name])
    require(type(origin_by_field["source_aliases"]) in {list, tuple})
    origin_by_field["source_aliases"] = tuple(origin_by_field["source_aliases"])
    return SourceWitnessOrigin(**origin_by_field)


@dataclass(frozen=True)
class SourceWitnessDatabaseIdentity:
    """Source database identity, distinct from the Core target's native OIDs."""

    database_name: str
    database_oid: int
    system_identifier: str
    postmaster_started: str
    recovering: bool

    def __post_init__(self):
        text(self.database_name, 63)
        _oid(self.database_oid)
        _system_identifier(self.system_identifier)
        require(type(self.postmaster_started) is str)
        canonical_timestamp(self.postmaster_started)
        require(self.recovering is False)


def database_identity(value):
    """Freeze the actual five-field export, normalizing an aware timestamp to UTC."""
    if type(value) is SourceWitnessDatabaseIdentity:
        value.__post_init__()
        return value
    require(type(value) is dict and set(value) == set(SourceWitnessDatabaseIdentity.__dataclass_fields__))
    identity_by_field = dict(value)
    identity_by_field["postmaster_started"] = canonical_timestamp(identity_by_field["postmaster_started"])
    return SourceWitnessDatabaseIdentity(**identity_by_field)


@dataclass(frozen=True)
class SourceWitnessCapture:
    """Registry capture receipt; serialized fields alone confer no native read capability."""

    origin: SourceWitnessOrigin
    source_database: SourceWitnessDatabaseIdentity
    capture_id: str
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]
    owner_role: str
    runtime_roles: tuple[str, ...]
    observed_at: str
    catalog_sha256: str
    content_sha256: str
    capture_sha256: str
    contract: str = CAPTURE_CONTRACT

    @property
    def release_id(self):
        """Name only this registry capture, never an original Source release."""
        return "registry-source-witness:" + self.capture_id

    def as_dict(self):
        """Return the bounded canonical JSON representation."""
        return json.loads(canonical(asdict(self)))


@dataclass(frozen=True)
class SourceWitnessCaptureAnchor:
    """Independent caller-supplied Core coordinates; Source does not attest observation time."""

    capture_id: str
    release_id: str
    observed_at: str

    def __post_init__(self):
        require(
            type(self.capture_id) is str and str(UUID(self.capture_id)) == self.capture_id and UUID(self.capture_id).int
        )
        require(type(self.release_id) is str and self.release_id == "registry-source-witness:" + self.capture_id)
        require(type(self.observed_at) is str)
        canonical_timestamp(self.observed_at)


def require_capture_anchor(capture, expected_anchor):
    """Compare a capture with separate Core coordinates, never a self-derived default."""
    require(type(expected_anchor) is SourceWitnessCaptureAnchor)
    expected_anchor.__post_init__()
    capture = validate_capture(capture)
    require(
        (capture.capture_id, capture.release_id, capture.observed_at)
        == (expected_anchor.capture_id, expected_anchor.release_id, expected_anchor.observed_at)
    )
    return capture


def capture_digest(capture):
    """Bind all explicit provenance and custody fields except the digest itself."""
    require(type(capture) is SourceWitnessCapture)
    document = capture.as_dict()
    document.pop("capture_sha256")
    return digest(document)


def _decode_capture(document):
    require(set(document) == set(SourceWitnessCapture.__dataclass_fields__))
    capture_by_field = dict(document)
    origin = capture_by_field["origin"]
    source_database = capture_by_field["source_database"]
    require(type(origin) is dict and type(source_database) is dict)
    require(type(origin.get("source_aliases")) is list)
    for name in ("resource_counts", "resource_hashes", "descriptor_counts"):
        require(type(origin.get(name)) is list and all(type(pair) is list for pair in origin[name]))
    require(type(source_database.get("postmaster_started")) is str)
    capture_by_field["origin"] = source_origin(origin)
    capture_by_field["source_database"] = database_identity(source_database)
    require(type(capture_by_field["relation_oids"]) is list and type(capture_by_field["runtime_roles"]) is list)
    require(all(type(pair) is list for pair in capture_by_field["relation_oids"]))
    capture_by_field["relation_oids"] = _pairs(capture_by_field["relation_oids"])
    capture_by_field["runtime_roles"] = tuple(capture_by_field["runtime_roles"])
    return SourceWitnessCapture(**capture_by_field)


def validate_capture(capture):
    """Reject mixed provenance and malformed capture declarations before native reads."""
    if type(capture) is dict:
        capture = _decode_capture(capture)
    require(
        type(capture) is SourceWitnessCapture and type(capture.contract) is str and capture.contract == CAPTURE_CONTRACT
    )
    require(
        type(capture.origin) is SourceWitnessOrigin and type(capture.source_database) is SourceWitnessDatabaseIdentity
    )
    source_origin(capture.origin)
    database_identity(capture.source_database)
    require(
        (capture.origin.database_oid, capture.origin.database_system_identifier)
        == (capture.source_database.database_oid, capture.source_database.system_identifier)
    )
    require(
        type(capture.capture_id) is str
        and str(UUID(capture.capture_id)) == capture.capture_id
        and UUID(capture.capture_id).int
    )
    require(type(capture.schema_name) is str and capture.schema_name == SCHEMA_PREFIX + UUID(capture.capture_id).hex)
    _oid(capture.schema_oid)
    _identifier(capture.owner_role)
    require(type(capture.runtime_roles) is tuple and 0 < len(capture.runtime_roles) <= 32)
    require(
        all(type(role) is str for role in capture.runtime_roles)
        and tuple(sorted(set(capture.runtime_roles))) == capture.runtime_roles
        and capture.owner_role not in capture.runtime_roles
    )
    for role in capture.runtime_roles:
        _identifier(role)
    require(type(capture.observed_at) is str)
    canonical_timestamp(capture.observed_at)
    require(type(capture.relation_oids) is tuple and _pairs(capture.relation_oids) == capture.relation_oids)
    require(tuple(name for name, _ in capture.relation_oids) == tuple(sorted(TABLES)))
    for _, oid in capture.relation_oids:
        _oid(oid)
    require(len({oid for _, oid in capture.relation_oids}) == len(TABLES))
    require(all(is_sha256(getattr(capture, name)) for name in ("catalog_sha256", "content_sha256", "capture_sha256")))
    require(len(canonical(capture.as_dict()).encode()) <= 65536 and capture.capture_sha256 == capture_digest(capture))
    return capture
