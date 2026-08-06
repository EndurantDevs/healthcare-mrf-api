# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure primitives for typed public-evidence source-record inventories."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from types import MappingProxyType
from typing import Literal, NamedTuple

from public_evidence.evidence_record_primitives import EvidenceSourceRecordReference
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
)

PUBLIC_EVIDENCE_SOURCE_RECORD_INVENTORY_CONTRACT = (
    "healthporta.public-evidence-source-record-inventory.v1"
)
PUBLIC_EVIDENCE_SOURCE_RECORD_INCLUSION_CONTRACT = (
    "healthporta.public-evidence-source-record-inclusion.v1"
)
SOURCE_RECORD_INVENTORY_TREE_CONTRACT = (
    "healthporta_public_evidence_rfc6962_shape_sha256_v1"
)
SOURCE_RECORD_INVENTORY_ORDERING_CONTRACT = "declared_member_ordinal_not_verified_v1"
REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT = (
    "source_record_ref_ascii_ascending_unique_v1"
)
SOURCE_RECORD_INVENTORY_REF_PREFIX = "peinv1_"
SOURCE_RECORD_INCLUSION_REF_PREFIX = "peinc1_"
MAX_SOURCE_RECORD_INVENTORY_MEMBERS = 2**53 - 1
MAX_SOURCE_RECORD_AUDIT_PATH = 53

SOURCE_RECORD_KINDS_BY_SOURCE = MappingProxyType(
    {
        "tic": frozenset({"tic_provider_group_occurrence"}),
        "public_provider_directory_fhir": frozenset(
            {
                "fhir_insurance_plan",
                "fhir_location",
                "fhir_network",
                "fhir_npi_resource",
                "fhir_organization",
                "fhir_practitioner_role",
            }
        ),
        "nppes_entity_address": frozenset({"nppes_registry_record"}),
        "public_hpt": frozenset({"hpt_hospital_record"}),
    }
)

_INVALID = "public_evidence_source_record_inclusion_invalid"
_REFERENCE_DOMAIN = b"HEALTHPORTA_SOURCE_RECORD_INCLUSION_REFERENCE_V1\x00"
_DIGEST_DOMAIN = b"HEALTHPORTA_SOURCE_RECORD_INCLUSION_DIGEST_V1\x00"
_LEAF_DOMAIN = b"HEALTHPORTA_SOURCE_RECORD_INVENTORY_LEAF_V1\x00"
_NODE_DOMAIN = b"HEALTHPORTA_SOURCE_RECORD_INVENTORY_NODE_V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_PROTOCOL_RE = re.compile(r"[a-z][a-z0-9_.:-]{1,94}_v[1-9][0-9]*", re.ASCII)
_KIND_RE = re.compile(r"[a-z][a-z0-9_]{1,63}", flags=re.ASCII)
_OPAQUE_REF_RE = re.compile(r"[A-Za-z0-9_-]{43}", flags=re.ASCII)


class PublicEvidenceSourceRecordInclusionError(RuntimeError):
    """One deliberately uniform inventory or inclusion validation failure."""


def _fail() -> PublicEvidenceSourceRecordInclusionError:
    return PublicEvidenceSourceRecordInclusionError(_INVALID)


class SourceRecordInventoryAuthorityState(NamedTuple):
    """Fixed non-authoritative state for a declared typed inventory."""

    lifecycle_state: Literal["declared_inventory_descriptor_only"]
    authenticated_replay_state: Literal["required_not_executed"]
    source_bytes_authenticated: Literal[False]
    complete_inventory_scan_verified: Literal[False]
    member_ordering_verified: Literal[False]
    duplicate_rejection_verified: Literal[False]
    source_authenticity_claimed: Literal[False]
    whole_source_complete: Literal[False]
    adapter_execution_authority: Literal["none"]
    database_io_enabled: Literal[False]
    serving_authority: Literal["none"]
    current_pointer_authority: Literal["none"]
    publication_enabled: Literal[False]
    replacement_enabled: Literal[False]
    deletion_enabled: Literal[False]
    retirement_enabled: Literal[False]
    supersession_enabled: Literal[False]


class PublicEvidenceSourceRecordInventoryDescriptor(NamedTuple):
    """Release-bound typed inventory root without source-authentication claims."""

    contract: str
    foundation_scope: str
    tree_contract_id: str
    ordering_contract_id: str
    release: PublicEvidenceSourceReleaseDescriptor
    source_kind: str
    record_kind: str
    record_identity_contract_id: str
    payload_canonicalization_contract_id: str
    member_count: int
    member_root_sha256: str
    source_binding_fingerprint_sha256: str | None
    inventory_policy_descriptor_sha256: str
    inventory_ref: str
    contract_sha256: str
    authority_state: SourceRecordInventoryAuthorityState

    def __repr__(self) -> str:
        return "<public-evidence-source-record-inventory>"


class PublicEvidenceSourceRecordInclusionWitness(NamedTuple):
    """One exact member path against a declared typed inventory root."""

    contract: str
    tree_contract_id: str
    inventory: PublicEvidenceSourceRecordInventoryDescriptor
    source_record: EvidenceSourceRecordReference
    member_ordinal: int
    leaf_sha256: str
    audit_path_sha256s: tuple[str, ...]
    inclusion_ref: str
    contract_sha256: str
    membership_state: Literal["verified_against_declared_inventory"]
    authenticated_replay_state: Literal["required_not_executed"]
    source_bytes_authenticated: Literal[False]
    complete_inventory_scan_verified: Literal[False]
    payload_derivation_verified: Literal[False]
    source_authenticity_claimed: Literal[False]

    def __repr__(self) -> str:
        return "<public-evidence-source-record-inclusion>"


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_protocol(value: object) -> str:
    if type(value) is not str or _PROTOCOL_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_kind(value: object) -> str:
    if type(value) is not str or _KIND_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_positive_count(value: object) -> int:
    if type(value) is not int or not 1 <= value <= MAX_SOURCE_RECORD_INVENTORY_MEMBERS:
        raise _fail()
    return value


def _strict_ordinal(value: object, member_count: int) -> int:
    if type(value) is not int or not 0 <= value < member_count:
        raise _fail()
    return value


def _exact_dict(value: object, fields: frozenset[str]) -> dict[str, object]:
    if type(value) is not dict or len(value) != len(fields):
        raise _fail()
    if any(type(key) is not str for key in value) or frozenset(value) != fields:
        raise _fail()
    return value


def _canonical_json(payload: object) -> bytes:
    try:
        return json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, UnicodeEncodeError, ValueError):
        raise _fail() from None


def _framed_digest(domain: bytes, purpose: str, payload: object) -> bytes:
    purpose_bytes = purpose.encode("ascii")
    encoded = _canonical_json(payload)
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(purpose_bytes).to_bytes(2, "big"))
    digest.update(purpose_bytes)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.digest()


def _canonical_sha256(purpose: str, payload: object) -> str:
    return _framed_digest(_DIGEST_DOMAIN, purpose, payload).hex()


def _source_binding_fingerprint(
    release: PublicEvidenceSourceReleaseDescriptor,
) -> str | None:
    binding = release.source_binding
    if binding is None:
        return None
    return _canonical_sha256(
        "source_binding_fingerprint",
        {
            "contract_id": binding.contract_id,
            "source_artifact_source_type": binding.source_artifact_source_type,
            "source_artifact_identity_kind": binding.source_artifact_identity_kind,
            "source_artifact_sha256": binding.source_artifact_sha256,
            "source_binding_sha256": binding.source_binding_sha256,
            "shadow_bundle_binding_sha256": binding.shadow_bundle_binding_sha256,
        },
    )


def _derived_ref(prefix: str, purpose: str, payload: object) -> str:
    token = base64.urlsafe_b64encode(
        _framed_digest(_REFERENCE_DOMAIN, purpose, payload)
    ).rstrip(b"=")
    return f"{prefix}{token.decode('ascii')}"


def _validate_derived_ref(value: object, prefix: str, expected: str) -> str:
    if (
        type(value) is not str
        or not value.startswith(prefix)
        or _OPAQUE_REF_RE.fullmatch(value[len(prefix) :]) is None
        or not hmac.compare_digest(value, expected)
    ):
        raise _fail()
    return value


def _leaf_sha256(payload: object) -> str:
    encoded = _canonical_json(payload)
    digest = hashlib.sha256()
    digest.update(_LEAF_DOMAIN)
    digest.update(b"\x00")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def derive_inventory_node_sha256(left_sha256: object, right_sha256: object) -> str:
    """Derive an RFC6962-shaped node using HealthPorta-specific framing."""
    left = bytes.fromhex(_strict_sha256(left_sha256))
    right = bytes.fromhex(_strict_sha256(right_sha256))
    digest = hashlib.sha256()
    digest.update(_NODE_DOMAIN)
    digest.update(b"\x01")
    digest.update(left)
    digest.update(right)
    return digest.hexdigest()


def _bounded_audit_path(value: object) -> tuple[str, ...]:
    if type(value) is not tuple or len(value) > MAX_SOURCE_RECORD_AUDIT_PATH:
        raise _fail()
    return tuple(_strict_sha256(item) for item in value)


def _verify_audit_path(
    leaf_sha256: str,
    member_ordinal: int,
    member_count: int,
    audit_path: tuple[str, ...],
    expected_root_sha256: str,
) -> None:
    node_index = member_ordinal
    last_index = member_count - 1
    calculated = leaf_sha256
    for sibling in audit_path:
        if node_index & 1 or node_index == last_index:
            calculated = derive_inventory_node_sha256(sibling, calculated)
            while node_index and not node_index & 1:
                node_index >>= 1
                last_index >>= 1
        else:
            calculated = derive_inventory_node_sha256(calculated, sibling)
        node_index >>= 1
        last_index >>= 1
    if last_index != 0 or not hmac.compare_digest(calculated, expected_root_sha256):
        raise _fail()
