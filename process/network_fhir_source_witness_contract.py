# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical Source provenance and bounded content declarations."""

import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import UTC, date, datetime
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_fhir_retained_capture_contract import (
    LEGACY_PROFILE,
    SEMANTIC_PROFILE,
    canonical,
    digest,
    is_sha256,
    retained_metadata_max_bytes,
    text,
)
from process.network_fhir_retained_capture_contract import TABLES as RETAINED_TABLES
from process.provider_directory_admission_seal import ADMISSION_RAW_METADATA_MAX_BYTES
from process.registry_fhir_catalog_extraction import fhir_source_uri

CONTRACT = "fhir-source-membership-witness-v1"
COMPLETION_CONTRACT = "fhir-source-membership-witness-v2"
OBSERVED_LEGACY_PROFILE = "observed-legacy-normalized-v1"
OBSERVED_PROOF_PROFILE = "observed-generic-legacy-proof-v1"
OBSERVED_SEMANTIC_PROFILE = "observed-semantic-v3-v1"
OBSERVED_ROOTED_PROFILE = "observed-rooted-graph-v1"
ADMITTED_LEGACY_PROFILE = "admitted-generic-legacy-v1"
ADMITTED_TRANSPORT_PROFILE = "admitted-generic-transport-v2-v1"
ADMITTED_UHC_PROFILE = "admitted-uhc-transport-v2-v1"
HASH_CONTRACTS = {
    OBSERVED_LEGACY_PROFILE: "transport_bound_v1",
    OBSERVED_PROOF_PROFILE: "transport_bound_v1",
    OBSERVED_SEMANTIC_PROFILE: "semantic_content_v3",
    OBSERVED_ROOTED_PROFILE: "semantic_content_v3",
    ADMITTED_LEGACY_PROFILE: "transport_bound_v1",
    ADMITTED_TRANSPORT_PROFILE: "transport_neutral_v2",
    ADMITTED_UHC_PROFILE: "transport_neutral_v2",
    SEMANTIC_PROFILE: "semantic_content_v4",
}
LEGACY_CAPTURE_CONTRACT = "registry-source-fhir-witness-capture-v1"
CAPTURE_CONTRACT = "registry-source-fhir-capture-v2"
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


def _pairs(value):
    require(type(value) in {list, tuple})
    require(all(type(pair) in {list, tuple} and len(pair) == 2 for pair in value))
    return tuple(tuple(pair) for pair in value)


def source_metadata_max_bytes(profile):
    """Bound original metadata independently of projected declarations and native batches."""
    require(profile in HASH_CONTRACTS)
    if profile in {OBSERVED_LEGACY_PROFILE, OBSERVED_ROOTED_PROFILE}:
        return retained_metadata_max_bytes(LEGACY_PROFILE)
    if profile == SEMANTIC_PROFILE:
        return retained_metadata_max_bytes(profile)
    return ADMISSION_RAW_METADATA_MAX_BYTES


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
    """Logical source, named relations and complete content observations."""

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
    proof_sha256: str | None
    resource_counts: tuple[tuple[str, int], ...]
    resource_hashes: tuple[tuple[str, str], ...]
    descriptor_counts: tuple[tuple[str, int], ...] | None
    admission_profile: str
    admission_metadata_sha256: str | None
    semantic_projection_as_of: str | None
    unresolved_plan_network_count: int
    source_uri: str
    source_aliases: tuple[str, ...]

    def __post_init__(self):
        _identifier(self.schema_name)
        for name, bound in (("source_id", 64), ("endpoint_id", 64), ("dataset_id", 96), ("root_run_id", 64)):
            text(getattr(self, name), bound)
        for relation in (self.header_relation, self.resource_relation):
            require(type(relation) is dict and set(relation) == {"schema", "name"})
            _identifier(relation["schema"])
            _identifier(relation["name"])
        require(self.resource_relation["schema"] == self.schema_name)
        for name in (
            "dataset_sha256",
            "metadata_text_sha256",
            "summary_sha256",
        ):
            require(is_sha256(getattr(self, name)))
        require(
            type(self.metadata_bytes) is int
            and 0 < self.metadata_bytes <= source_metadata_max_bytes(self.admission_profile)
        )
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
        require(self.source_id != "cms-npd")
        require(type(self.unresolved_plan_network_count) is int and self.unresolved_plan_network_count >= 0)
        require(self.admission_profile in HASH_CONTRACTS)
        if self.admission_profile == OBSERVED_ROOTED_PROFILE:
            require(is_sha256(self.proof_sha256))
            require(self.admission_metadata_sha256 is None and self.descriptor_counts is None)
        elif self.admission_profile in {OBSERVED_LEGACY_PROFILE, OBSERVED_SEMANTIC_PROFILE}:
            require(
                self.proof_sha256 is None and self.admission_metadata_sha256 is None and self.descriptor_counts is None
            )
        else:
            require(is_sha256(self.proof_sha256) and self.descriptor_counts is not None)
            require(all(count >= dict(self.resource_counts)[kind] for kind, count in self.descriptor_counts))
            if self.admission_profile == OBSERVED_PROOF_PROFILE:
                require(self.admission_metadata_sha256 is None)
                require(self.descriptor_counts != self.resource_counts)
            else:
                require(is_sha256(self.admission_metadata_sha256))
                if self.admission_profile != SEMANTIC_PROFILE:
                    require(self.descriptor_counts == self.resource_counts)
        if self.admission_profile in {SEMANTIC_PROFILE, OBSERVED_SEMANTIC_PROFILE, OBSERVED_ROOTED_PROFILE}:
            require(
                type(self.semantic_projection_as_of) is str
                and date.fromisoformat(self.semantic_projection_as_of).isoformat() == self.semantic_projection_as_of
            )
        else:
            require(self.semantic_projection_as_of is None)


@dataclass(frozen=True)
class SourceWitnessCompletionOrigin(SourceWitnessOrigin):
    """Version two additionally pins the independently persisted completion proof."""

    completion_required_version: int
    completion_sha256: str

    def __post_init__(self):
        super().__post_init__()
        require(self.admission_profile == ADMITTED_TRANSPORT_PROFILE)
        require(type(self.completion_required_version) is int and self.completion_required_version == 3)
        require(is_sha256(self.completion_sha256))


def is_completion_v3_origin(origin):
    """Select only the existing completion declaration authenticated by publication intake."""
    if not is_dataclass(origin) or isinstance(origin, type) or "completion_sha256" not in origin.__dataclass_fields__:
        return False
    return type(source_origin(origin)) is SourceWitnessCompletionOrigin


def source_resource_marker_valid(value, *, completion_v3):
    """Completion rows require acquired digests; every older profile remains null-only."""
    return is_sha256(value) if completion_v3 else value is None


def _origin_families(origin):
    for name in ("resource_counts", "resource_hashes"):
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
    require(all(is_sha256(root) for _, root in origin.resource_hashes))
    if origin.descriptor_counts is not None:
        pairs = origin.descriptor_counts
        require(type(pairs) is tuple and all(type(pair) is tuple and len(pair) == 2 for pair in pairs))
        require(tuple(name for name, _ in pairs) == families)
        require(all(type(count) is int and count >= 0 for _, count in pairs))


def _named_source_origin_fields(origin_by_field, origin_type):
    """Project the supported historical wire layout onto strict named provenance fields."""
    expected_fields = set(origin_type.__dataclass_fields__)
    legacy_physical_fields = {
        "header_oid",
        "resource_parent_oid",
        "resource_leaf_oid",
        "native_catalog_sha256",
        "database_oid",
        "database_system_identifier",
    }
    legacy_fields = expected_fields - {"header_relation", "resource_relation"} | legacy_physical_fields
    if set(origin_by_field) == legacy_fields:
        schema = origin_by_field["schema_name"]
        origin_by_field = {key: field for key, field in origin_by_field.items() if key not in legacy_physical_fields}
        origin_by_field.update(
            header_relation={"schema": schema, "name": "provider_directory_endpoint_dataset"},
            resource_relation={"schema": schema, "name": "provider_directory_dataset_resource"},
        )
    require(set(origin_by_field) == expected_fields)
    return dict(origin_by_field)


def source_origin(value):
    """Decode named source provenance and validate its logical content fields."""
    if type(value) in {SourceWitnessOrigin, SourceWitnessCompletionOrigin}:
        value.__post_init__()
        return value
    origin_document = asdict(value) if is_dataclass(value) and not isinstance(value, type) else value
    require(type(origin_document) is dict)
    origin_type = SourceWitnessCompletionOrigin if "completion_sha256" in origin_document else SourceWitnessOrigin
    origin_by_field = _named_source_origin_fields(origin_document, origin_type)
    for name in ("resource_counts", "resource_hashes", "descriptor_counts"):
        if name != "descriptor_counts" or origin_by_field[name] is not None:
            origin_by_field[name] = _pairs(origin_by_field[name])
    require(type(origin_by_field["source_aliases"]) in {list, tuple})
    origin_by_field["source_aliases"] = tuple(origin_by_field["source_aliases"])
    return origin_type(**origin_by_field)


@dataclass(frozen=True)
class SourceWitnessCapture:
    """Named Source capture with logical provenance and complete content accounting."""

    origin: SourceWitnessOrigin
    capture_id: str
    schema_name: str
    tables: dict
    observed_at: str
    content_sha256: str
    capture_sha256: str
    contract: str = CAPTURE_CONTRACT

    @property
    def release_id(self):
        """Select this logical registry operation independently of Source's run."""
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


def source_witness_membership_date(capture, expected_anchor):
    """Bind legacy evaluation to the independent Core observation, preserving absent Source dates."""
    capture = require_capture_anchor(capture, expected_anchor)
    if capture.origin.semantic_projection_as_of is None:
        return datetime.fromisoformat(capture.observed_at).date().isoformat()
    return capture.origin.semantic_projection_as_of


def capture_digest(capture):
    """Bind the named edition and its data fields."""
    require(type(capture) is SourceWitnessCapture)
    document = capture.as_dict()
    document.pop("capture_sha256")
    return digest(document)


def _decode_capture(document):
    capture_by_field = dict(document)
    fields = set(SourceWitnessCapture.__dataclass_fields__)
    legacy_fields = {
        "origin",
        "source_database",
        "capture_id",
        "schema_name",
        "schema_oid",
        "relation_oids",
        "owner_role",
        "runtime_roles",
        "observed_at",
        "catalog_sha256",
        "content_sha256",
        "capture_sha256",
        "contract",
    }
    if set(document) == legacy_fields and document["contract"] == LEGACY_CAPTURE_CONTRACT:
        capture_by_field = {key: document[key] for key in fields - {"tables"}}
        capture_by_field["tables"] = {name: {"schema": document["schema_name"], "name": name} for name in TABLES}
    require(set(capture_by_field) == fields)
    origin = capture_by_field["origin"]
    require(type(origin) is dict)
    for field in ("resource_counts", "resource_hashes", "descriptor_counts"):
        if origin.get(field) is not None:
            require(type(origin[field]) is list and all(type(pair) is list for pair in origin[field]))
    capture_by_field["origin"] = source_origin(origin)
    return SourceWitnessCapture(**capture_by_field)


def validate_capture(capture):
    """Validate logical scope and content declarations before named reads."""
    if type(capture) is dict:
        capture = _decode_capture(capture)
    require(type(capture) is SourceWitnessCapture and capture.contract in {CAPTURE_CONTRACT, LEGACY_CAPTURE_CONTRACT})
    source_origin(capture.origin)
    require(
        type(capture.capture_id) is str
        and str(UUID(capture.capture_id)) == capture.capture_id
        and UUID(capture.capture_id).int
    )
    require(capture.schema_name == SCHEMA_PREFIX + UUID(capture.capture_id).hex)
    require(capture.tables == {name: {"schema": capture.schema_name, "name": name} for name in TABLES})
    require(type(capture.observed_at) is str)
    canonical_timestamp(capture.observed_at)
    require(all(is_sha256(getattr(capture, name)) for name in ("content_sha256", "capture_sha256")))
    require(len(canonical(capture.as_dict()).encode()) <= 65536)
    if capture.contract == CAPTURE_CONTRACT:
        require(capture.capture_sha256 == capture_digest(capture))
    return capture
