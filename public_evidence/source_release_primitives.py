# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict primitives for publication-disabled public evidence releases."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import json
import re
from typing import Any, Literal, Mapping


TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT = (
    "ptg2_tax_identity_shadow_source_binding_v1"
)
PUBLIC_EVIDENCE_IDENTITY_REF_PREFIX = "peid1_"
PUBLIC_EVIDENCE_IMPORT_RUN_REF_PREFIX = "perun1_"
PUBLIC_EVIDENCE_RELEASE_REF_PREFIX = "perel1_"

_REFERENCE_DIGEST_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_REFERENCE_V1\x00"
_INVALID = "public_evidence_source_release_invalid"
_MAX_JSON_SAFE_INTEGER = 2**53 - 1
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_PROTOCOL_ID_RE = re.compile(r"[a-z][a-z0-9_]{1,94}_v[1-9][0-9]*", flags=re.ASCII)
_COUNT_UNIT_RE = re.compile(r"[a-z][a-z0-9_]{1,95}", flags=re.ASCII)
_PHYSICAL_SOURCE_TYPE_RE = re.compile(
    r"[a-z0-9][a-z0-9._-]{0,63}",
    flags=re.ASCII,
)
_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)
_ATTESTATION_MODES = (
    "declared_complete_artifact",
    "declared_complete_dataset",
    "positive_evidence_only",
)


class PublicEvidenceSourceReleaseError(RuntimeError):
    """One deliberately uniform source-release validation failure."""


def _fail() -> PublicEvidenceSourceReleaseError:
    return PublicEvidenceSourceReleaseError(_INVALID)


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_protocol_id(value: object) -> str:
    if type(value) is not str or _PROTOCOL_ID_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _derived_opaque_ref(
    prefix: str,
    purpose: str,
    payload: Mapping[str, Any],
) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    purpose_bytes = purpose.encode("ascii")
    digest = hashlib.sha256()
    digest.update(_REFERENCE_DIGEST_DOMAIN)
    digest.update(len(purpose_bytes).to_bytes(2, "big"))
    digest.update(purpose_bytes)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    token = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")
    return f"{prefix}{token}"


def _strict_derived_ref(value: object, expected: str) -> str:
    if type(value) is not str or not hmac.compare_digest(value, expected):
        raise _fail()
    return value


def _canonical_utc(value: object) -> tuple[str, datetime]:
    if type(value) is not str or _UTC_RE.fullmatch(value) is None:
        raise _fail()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise _fail() from None
    return value, parsed


def derive_public_evidence_identity_ref(
    identity_kind: object,
    content_identity_kind: object,
    content_sha256: object,
) -> str:
    """Derive a non-reversible identity reference from typed content evidence."""
    if type(identity_kind) is not str or identity_kind not in {
        "immutable_artifact",
        "immutable_dataset",
    }:
        raise _fail()
    identity_contract = _strict_protocol_id(content_identity_kind)
    content_digest = _strict_sha256(content_sha256)
    return _derived_opaque_ref(
        PUBLIC_EVIDENCE_IDENTITY_REF_PREFIX,
        "immutable_source_identity",
        {
            "identity_kind": identity_kind,
            "content_identity_kind": identity_contract,
            "content_sha256": content_digest,
        },
    )


@dataclass(frozen=True, slots=True, repr=False)
class ImmutablePublicSourceIdentity:
    identity_kind: Literal["immutable_artifact", "immutable_dataset"]
    content_identity_kind: str
    identity_ref: str
    content_sha256: str

    def __post_init__(self) -> None:
        expected_ref = derive_public_evidence_identity_ref(
            self.identity_kind,
            self.content_identity_kind,
            self.content_sha256,
        )
        _strict_derived_ref(self.identity_ref, expected_ref)


@dataclass(frozen=True, slots=True, repr=False)
class PublicEvidenceCompletenessAttestation:
    mode: str
    evidence_contract_id: str
    count_unit: str
    subject_sha256: str
    expected_record_count: int | None
    observed_record_count: int
    evidence_root_sha256: str

    def __post_init__(self) -> None:
        expected = self.expected_record_count
        observed = self.observed_record_count
        if (
            type(self.mode) is not str
            or self.mode not in _ATTESTATION_MODES
            or type(self.evidence_contract_id) is not str
            or _PROTOCOL_ID_RE.fullmatch(self.evidence_contract_id) is None
            or type(self.count_unit) is not str
            or _COUNT_UNIT_RE.fullmatch(self.count_unit) is None
            or type(observed) is not int
            or not 0 <= observed <= _MAX_JSON_SAFE_INTEGER
            or (expected is not None and type(expected) is not int)
            or (type(expected) is int and not 0 <= expected <= _MAX_JSON_SAFE_INTEGER)
        ):
            raise _fail()
        if self.mode == "positive_evidence_only" and expected is not None:
            raise _fail()
        if self.mode != "positive_evidence_only" and expected != observed:
            raise _fail()
        _strict_sha256(self.subject_sha256)
        _strict_sha256(self.evidence_root_sha256)


@dataclass(frozen=True, slots=True, repr=False)
class CanonicalUtcInterval:
    start_at: str
    end_at: str | None

    def __post_init__(self) -> None:
        _, start = _canonical_utc(self.start_at)
        if self.end_at is None:
            return
        _, end = _canonical_utc(self.end_at)
        if end < start:
            raise _fail()


@dataclass(frozen=True, slots=True, repr=False)
class OpaqueSourceBindingReference:
    """Map the upstream physical-identity triple and both binding digests."""

    contract_id: str
    source_artifact_source_type: str
    source_artifact_identity_kind: str
    source_artifact_sha256: str
    source_binding_sha256: str
    shadow_bundle_binding_sha256: str

    def __post_init__(self) -> None:
        if type(self.contract_id) is not str or self.contract_id != (
            TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT
        ):
            raise _fail()
        if (
            type(self.source_artifact_source_type) is not str
            or _PHYSICAL_SOURCE_TYPE_RE.fullmatch(
                self.source_artifact_source_type
            )
            is None
        ):
            raise _fail()
        _strict_protocol_id(self.source_artifact_identity_kind)
        source_artifact = _strict_sha256(self.source_artifact_sha256)
        source_binding = _strict_sha256(self.source_binding_sha256)
        shadow_bundle_binding = _strict_sha256(self.shadow_bundle_binding_sha256)
        if len({source_artifact, source_binding, shadow_bundle_binding}) != 3:
            raise _fail()
