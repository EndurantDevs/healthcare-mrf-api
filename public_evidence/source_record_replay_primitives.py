# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure values for capability-free retained-row replay results."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import struct
from typing import Literal, Mapping, NamedTuple

from public_evidence.source_record_inclusion_primitives import (
    REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT,
    PublicEvidenceSourceRecordInventoryDescriptor,
)
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
)

PUBLIC_EVIDENCE_FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_CONTRACT = (
    "healthporta.public-evidence-fhir-organization-retained-row-replay.v1"
)
FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_ID = (
    "healthporta_public_evidence_fhir_organization_retained_row_replay_v1"
)
FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID = "healthporta_fhir_source_record_hmac_v1"
FHIR_ORGANIZATION_PAYLOAD_CANONICALIZATION_CONTRACT_ID = (
    "provider_directory_normalized_payload_sha256_v1"
)
CONNECTOR_FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID = (
    "healthporta.tin-npi.fhir-source-record-hmac.v1"
)
CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID = (
    "healthporta.tin-npi.fhir-organization-scan.v2"
)
FHIR_ORGANIZATION_REPLAY_REF_PREFIX = "perp1_"

_INVALID = "public_evidence_fhir_organization_replay_invalid"
_REFERENCE_DOMAIN = b"HEALTHPORTA_FHIR_ORGANIZATION_REPLAY_REFERENCE_V1\x00"
_DIGEST_DOMAIN = b"HEALTHPORTA_FHIR_ORGANIZATION_REPLAY_DIGEST_V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_TOKEN_POLICY_RE = re.compile(
    r"ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})",
    flags=re.ASCII,
)
_TOKEN_POLICY_DESCRIPTOR_DOMAIN = b"PTG2V4TINPOLICY\x01"
_TOKEN_POLICY_DESCRIPTOR_FIELDS = (
    "ein_ascii_digits_or_2_7_hyphen_v1",
    "hmac_sha256_ptg_tin_v1",
    "tin_id_128=first_16_bytes(tin_hmac_sha256)",
    "tin_hmac_sha256_full_32_bytes_authoritative",
)


class PublicEvidenceFhirOrganizationReplayError(RuntimeError):
    """One deliberately uniform retained-row replay validation failure."""


def replay_validation_error() -> PublicEvidenceFhirOrganizationReplayError:
    """Return the public, redacted replay error used across both layers."""

    return PublicEvidenceFhirOrganizationReplayError(_INVALID)


class RecordKindReplayAuthorityState(NamedTuple):
    """Exact claims and non-authority established for one supplied row vector."""

    lifecycle_state: Literal[
        "verified_provided_fhir_organization_retained_row_vector_only"
    ]
    retained_payload_hashes_recomputed: Literal[True]
    record_identity_hmacs_rederived: Literal[True]
    provided_row_count_matched_dataset_fence: Literal[True]
    provided_row_identity_digest_matched_dataset_fence: Literal[True]
    provided_source_record_vector_matched_inventory: Literal[True]
    canonical_member_ordering_reconstructed: Literal[True]
    duplicate_source_record_refs_rejected: Literal[True]
    declared_inventory_root_recomputed: Literal[True]
    source_bytes_authenticated: Literal[False]
    source_authenticity_claimed: Literal[False]
    whole_source_complete: Literal[False]
    release_content_binding_verified: Literal[False]
    durable_relation_replay_verified: Literal[False]
    payload_derivation_verified: Literal[False]
    adapter_execution_authority: Literal["none"]
    database_io_enabled: Literal[False]
    serving_authority: Literal["none"]
    current_pointer_authority: Literal["none"]
    publication_enabled: Literal[False]
    replacement_enabled: Literal[False]
    deletion_enabled: Literal[False]
    retirement_enabled: Literal[False]
    supersession_enabled: Literal[False]


class PublicEvidenceFhirOrganizationReplayResult(NamedTuple):
    """Opaque result for checking one caller-supplied retained-row vector."""

    contract: str
    foundation_scope: str
    replay_policy_id: str
    replay_policy_descriptor_sha256: str
    release: PublicEvidenceSourceReleaseDescriptor
    inventory: PublicEvidenceSourceRecordInventoryDescriptor
    source_vector_sha256: str
    dataset_fence_sha256: str
    token_policy_id: str
    token_policy_descriptor_sha256: str
    record_kind: str
    record_identity_contract_id: str
    record_identity_descriptor_sha256: str
    payload_canonicalization_contract_id: str
    member_count: int
    member_root_sha256: str
    source_record_vector_sha256: str
    scan_contract_id: str
    scan_proof_sha256: str
    replay_ref: str
    contract_sha256: str
    authority_state: RecordKindReplayAuthorityState

    def __repr__(self) -> str:
        return "<public-evidence-fhir-organization-retained-row-replay>"

    __str__ = __repr__


def _canonical_json(payload: object) -> bytes:
    try:
        return json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, UnicodeEncodeError, ValueError):
        raise replay_validation_error() from None


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


def canonical_replay_sha256(purpose: str, payload: object) -> str:
    """Hash one JSON-safe replay payload under the replay digest domain."""

    if type(purpose) is not str or not purpose or not purpose.isascii():
        raise replay_validation_error()
    return _framed_digest(_DIGEST_DOMAIN, purpose, payload).hex()


def derived_replay_ref(purpose: str, payload: object) -> str:
    """Derive one opaque result reference from its complete public payload."""

    if type(purpose) is not str or not purpose or not purpose.isascii():
        raise replay_validation_error()
    token = base64.urlsafe_b64encode(
        _framed_digest(_REFERENCE_DOMAIN, purpose, payload)
    ).rstrip(b"=")
    return f"{FHIR_ORGANIZATION_REPLAY_REF_PREFIX}{token.decode('ascii')}"


def strict_replay_sha256(value: object) -> str:
    """Return a lowercase SHA-256 digest or fail with the replay error."""

    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise replay_validation_error()
    return value


def strict_replay_token_policy_id(value: object) -> str:
    """Return one frozen PTG token policy ID without accepting subclasses."""

    if type(value) is not str or _TOKEN_POLICY_RE.fullmatch(value) is None:
        raise replay_validation_error()
    return value


def connector_token_policy_descriptor_sha256(token_policy_id: object) -> str:
    """Rebuild the connector's nonsecret Release-1 policy descriptor."""

    policy_id = strict_replay_token_policy_id(token_policy_id)
    fields = (policy_id, *_TOKEN_POLICY_DESCRIPTOR_FIELDS)
    digest = hashlib.sha256(_TOKEN_POLICY_DESCRIPTOR_DOMAIN)
    for field in fields:
        encoded = field.encode("ascii")
        digest.update(struct.pack(">I", len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def fixed_replay_authority() -> RecordKindReplayAuthorityState:
    """Return the immutable claim boundary of a retained-row replay result."""

    return RecordKindReplayAuthorityState(
        lifecycle_state=(
            "verified_provided_fhir_organization_retained_row_vector_only"
        ),
        retained_payload_hashes_recomputed=True,
        record_identity_hmacs_rederived=True,
        provided_row_count_matched_dataset_fence=True,
        provided_row_identity_digest_matched_dataset_fence=True,
        provided_source_record_vector_matched_inventory=True,
        canonical_member_ordering_reconstructed=True,
        duplicate_source_record_refs_rejected=True,
        declared_inventory_root_recomputed=True,
        source_bytes_authenticated=False,
        source_authenticity_claimed=False,
        whole_source_complete=False,
        release_content_binding_verified=False,
        durable_relation_replay_verified=False,
        payload_derivation_verified=False,
        adapter_execution_authority="none",
        database_io_enabled=False,
        serving_authority="none",
        current_pointer_authority="none",
        publication_enabled=False,
        replacement_enabled=False,
        deletion_enabled=False,
        retirement_enabled=False,
        supersession_enabled=False,
    )


_RECORD_IDENTITY_DESCRIPTOR_PAYLOAD = {
    "contract_id": FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
    "underlying_message_format_id": (
        CONNECTOR_FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID
    ),
    "message_fields": [
        "token_policy_id",
        "source_id",
        "source_endpoint_id",
        "source_dataset_id",
        "resource_id",
    ],
    "digest_algorithm": "hmac_sha256",
    "secret_material_exposed": False,
}
FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256 = canonical_replay_sha256(
    "record_identity_descriptor", _RECORD_IDENTITY_DESCRIPTOR_PAYLOAD
)

_REPLAY_POLICY_PAYLOAD = {
    "replay_policy_id": FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_ID,
    "record_kind": "fhir_organization",
    "record_identity_contract_id": FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
    "record_identity_descriptor_sha256": (
        FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256
    ),
    "payload_canonicalization_contract_id": (
        FHIR_ORGANIZATION_PAYLOAD_CANONICALIZATION_CONTRACT_ID
    ),
    "scan_contract_id": CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
    "inventory_ordering_contract_id": (REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT),
    "authority_state": fixed_replay_authority()._asdict(),
}
FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_DESCRIPTOR_SHA256 = (
    canonical_replay_sha256("replay_policy_descriptor", _REPLAY_POLICY_PAYLOAD)
)


def canonical_replay_binding_sha256(purpose: str, payload: Mapping[str, object]) -> str:
    """Hash one already validated source-vector or dataset-fence payload."""

    if type(payload) is not dict:
        raise replay_validation_error()
    return canonical_replay_sha256(purpose, payload)


def canonical_source_record_vector_sha256(source_record_refs: tuple[str, ...]) -> str:
    """Hash the strict public-evidence member ordering without exposing HMACs."""

    if (
        type(source_record_refs) is not tuple
        or not source_record_refs
        or any(type(value) is not str for value in source_record_refs)
        or source_record_refs != tuple(sorted(set(source_record_refs)))
    ):
        raise replay_validation_error()
    return canonical_replay_sha256(
        "source_record_ref_vector",
        {
            "ordering_contract_id": REQUIRED_AUTHENTICATED_REPLAY_ORDERING_CONTRACT,
            "source_record_refs": list(source_record_refs),
        },
    )
