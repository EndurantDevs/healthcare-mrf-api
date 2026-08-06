# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict values for publication-disabled public evidence records."""

from __future__ import annotations

import base64
from datetime import datetime
import hashlib
import hmac
import json
import re
from typing import Any, Mapping, NamedTuple
import uuid

from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    validate_public_evidence_source_release,
)
from public_evidence.source_release_primitives import CanonicalUtcInterval

PUBLIC_EVIDENCE_RECORD_CONTRACT = "healthporta.public-evidence-record.v1"
PUBLIC_EVIDENCE_TAX_IDENTITY_REF_PREFIX = "petax1_"
PUBLIC_EVIDENCE_SOURCE_RECORD_REF_PREFIX = "pesr1_"
PUBLIC_EVIDENCE_SOURCE_ENTITY_REF_PREFIX = "peent1_"
PUBLIC_EVIDENCE_PROVIDER_GROUP_REF_PREFIX = "pegrp1_"
PUBLIC_EVIDENCE_RECORD_REF_PREFIX = "peev1_"
MAX_PUBLIC_EVIDENCE_SOURCE_RECORDS = 16

_INVALID = "public_evidence_record_invalid"
_REFERENCE_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_REFERENCE_V1\x00"
_SHA256_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_DIGEST_V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_PROTOCOL_RE = re.compile(r"[a-z][a-z0-9_.:-]{1,94}_v[1-9][0-9]*", flags=re.ASCII)
_KIND_RE = re.compile(r"[a-z][a-z0-9_]{1,63}", flags=re.ASCII)
_OPAQUE_REF_BODY_RE = re.compile(r"[A-Za-z0-9_-]{43}", flags=re.ASCII)
_NPI_RE = re.compile(r"[0-9]{10}", flags=re.ASCII)
_ZIP5_RE = re.compile(r"[0-9]{5}", flags=re.ASCII)
_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)
_ADDRESS_PURPOSES = frozenset(
    {
        "hpt_hospital_location_candidate",
        "nppes_mailing",
        "nppes_practice_location",
        "provider_directory_location",
    }
)
_GEO_QUALITIES = frozenset(
    {"parcel", "rooftop", "street", "unavailable", "zip5_centroid"}
)
_FRESHNESS_STATES = frozenset({"current", "stale", "unknown"})


class PublicEvidenceRecordError(RuntimeError):
    """One deliberately uniform normalized-record validation failure."""


def _fail() -> PublicEvidenceRecordError:
    return PublicEvidenceRecordError(_INVALID)


class OpaqueTaxIdentityReference(NamedTuple):
    tin_type: str
    token_policy_contract_id: str
    token_policy_id: str
    token_policy_descriptor_sha256: str
    locator_128: str
    full_hmac_sha256: str
    normalization_contract_id: str
    tax_identity_ref: str

    def __repr__(self) -> str:
        return f"<opaque-tax-identity-reference type={self.tin_type!r}>"


class EvidenceSourceRecordReference(NamedTuple):
    source_release_ref: str
    record_kind: str
    identity_contract_id: str
    record_hmac_sha256: str
    payload_sha256: str
    source_record_ref: str

    def __repr__(self) -> str:
        return "<evidence-source-record-reference>"


class OpaqueSourceEntityReference(NamedTuple):
    source_release_ref: str
    entity_kind: str
    identity_contract_id: str
    identity_sha256: str
    source_entity_ref: str

    def __repr__(self) -> str:
        return f"<opaque-source-entity-reference kind={self.entity_kind!r}>"


class ProviderGroupReference(NamedTuple):
    source_release_ref: str
    identity_contract_id: str
    identity_sha256: str
    provider_group_ref: str

    def __repr__(self) -> str:
        return "<provider-group-reference>"


class CanonicalAddressEvidence(NamedTuple):
    address_key: str
    address_site_key: str | None
    canonicalization_contract_id: str
    purpose: str
    zip5: str | None
    geo_derivation_contract_id: str
    geo_quality: str
    freshness_state: str
    freshness_rule_version: str
    freshness_as_of: str
    selection_rule_version: str
    selection_eligible: bool

    def __repr__(self) -> str:
        return "<canonical-address-evidence>"


class ProviderDirectoryNetworkContext(NamedTuple):
    npi_source_record: EvidenceSourceRecordReference
    practitioner_role_source_record: EvidenceSourceRecordReference
    location_source_record: EvidenceSourceRecordReference
    network_source_record: EvidenceSourceRecordReference
    insurance_plan_source_record: EvidenceSourceRecordReference
    role_active: bool
    pricing_bridge_state: str

    def __repr__(self) -> str:
        return "<provider-directory-network-context>"


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


def _exact_dict(value: object, fields: frozenset[str]) -> dict[str, object]:
    if type(value) is not dict or len(value) != len(fields):
        raise _fail()
    if any(type(key) is not str for key in value):
        raise _fail()
    if frozenset(value) != fields:
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


def _derived_ref(prefix: str, purpose: str, payload: object) -> str:
    purpose_bytes = purpose.encode("ascii")
    encoded = _canonical_json(payload)
    digest = hashlib.sha256()
    digest.update(_REFERENCE_DOMAIN)
    digest.update(len(purpose_bytes).to_bytes(2, "big"))
    digest.update(purpose_bytes)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    token = base64.urlsafe_b64encode(digest.digest()).rstrip(b"=").decode("ascii")
    return f"{prefix}{token}"


def _validate_derived_ref(value: object, prefix: str, expected: str) -> str:
    if (
        type(value) is not str
        or not value.startswith(prefix)
        or _OPAQUE_REF_BODY_RE.fullmatch(value[len(prefix) :]) is None
        or not hmac.compare_digest(value, expected)
    ):
        raise _fail()
    return value


def _canonical_sha256(purpose: str, payload: object) -> str:
    purpose_bytes = purpose.encode("ascii")
    encoded = _canonical_json(payload)
    digest = hashlib.sha256()
    digest.update(_SHA256_DOMAIN)
    digest.update(len(purpose_bytes).to_bytes(2, "big"))
    digest.update(purpose_bytes)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def _validated_release(value: object) -> PublicEvidenceSourceReleaseDescriptor:
    try:
        return validate_public_evidence_source_release(value)
    except Exception:
        raise _fail() from None


def _strict_utc(value: object) -> str:
    if type(value) is not str or _UTC_RE.fullmatch(value) is None:
        raise _fail()
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise _fail() from None
    return value


def _strict_uuid(value: object) -> str:
    if type(value) is not str:
        raise _fail()
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise _fail() from None
    if str(parsed) != value:
        raise _fail()
    return value


def _strict_npi(value: object) -> str:
    if type(value) is not str or _NPI_RE.fullmatch(value) is None:
        raise _fail()
    if not 1_000_000_000 <= int(value) <= 2_999_999_999:
        raise _fail()
    digits = [int(character) for character in "80840" + value]
    for offset in range(1, len(digits), 2):
        doubled = digits[-1 - offset] * 2
        digits[-1 - offset] = doubled // 10 + doubled % 10
    if sum(digits) % 10:
        raise _fail()
    return value


def _build_source_record_reference(
    release: PublicEvidenceSourceReleaseDescriptor, raw: object
) -> EvidenceSourceRecordReference:
    fields = frozenset(
        "record_kind identity_contract_id record_hmac_sha256 payload_sha256".split()
    )
    values = _exact_dict(raw, fields)
    payload = {
        "source_release_ref": release.source_release_ref,
        "record_kind": _strict_kind(values["record_kind"]),
        "identity_contract_id": _strict_protocol(values["identity_contract_id"]),
        "record_hmac_sha256": _strict_sha256(values["record_hmac_sha256"]),
        "payload_sha256": _strict_sha256(values["payload_sha256"]),
    }
    return EvidenceSourceRecordReference(
        **payload,
        source_record_ref=_derived_ref(
            PUBLIC_EVIDENCE_SOURCE_RECORD_REF_PREFIX, "source_record", payload
        ),
    )


def build_evidence_source_record_reference(
    release: PublicEvidenceSourceReleaseDescriptor, raw: Mapping[str, object]
) -> EvidenceSourceRecordReference:
    """Build an opaque record witness bound to one source release."""
    return _build_source_record_reference(_validated_release(release), raw)


def validate_evidence_source_record_reference(
    release: PublicEvidenceSourceReleaseDescriptor, value: object
) -> EvidenceSourceRecordReference:
    """Rebuild and validate an exact source-record witness."""
    if type(value) is not EvidenceSourceRecordReference:
        raise _fail()
    rebuilt = build_evidence_source_record_reference(
        release,
        {
            "record_kind": value.record_kind,
            "identity_contract_id": value.identity_contract_id,
            "record_hmac_sha256": value.record_hmac_sha256,
            "payload_sha256": value.payload_sha256,
        },
    )
    if (
        type(value.source_release_ref) is not str
        or value.source_release_ref != rebuilt.source_release_ref
    ):
        raise _fail()
    _validate_derived_ref(
        value.source_record_ref,
        PUBLIC_EVIDENCE_SOURCE_RECORD_REF_PREFIX,
        rebuilt.source_record_ref,
    )
    return rebuilt


def build_canonical_address_evidence(
    raw: Mapping[str, object],
) -> CanonicalAddressEvidence:
    """Validate address keys and metadata without selecting a location."""
    fields = frozenset(CanonicalAddressEvidence._fields)
    address_fields = _exact_dict(raw, fields)
    site_key = address_fields["address_site_key"]
    zip5 = address_fields["zip5"]
    if site_key is not None:
        site_key = _strict_uuid(site_key)
    if zip5 is not None and (type(zip5) is not str or _ZIP5_RE.fullmatch(zip5) is None):
        raise _fail()
    purpose = address_fields["purpose"]
    geo_quality = address_fields["geo_quality"]
    freshness = address_fields["freshness_state"]
    eligible = address_fields["selection_eligible"]
    if (
        type(purpose) is not str
        or purpose not in _ADDRESS_PURPOSES
        or type(geo_quality) is not str
        or geo_quality not in _GEO_QUALITIES
        or type(freshness) is not str
        or freshness not in _FRESHNESS_STATES
        or type(eligible) is not bool
        or (geo_quality == "zip5_centroid" and zip5 is None)
    ):
        raise _fail()
    return CanonicalAddressEvidence(
        address_key=_strict_uuid(address_fields["address_key"]),
        address_site_key=site_key,
        canonicalization_contract_id=_strict_protocol(
            address_fields["canonicalization_contract_id"]
        ),
        purpose=purpose,
        zip5=zip5,
        geo_derivation_contract_id=_strict_protocol(
            address_fields["geo_derivation_contract_id"]
        ),
        geo_quality=geo_quality,
        freshness_state=freshness,
        freshness_rule_version=_strict_protocol(
            address_fields["freshness_rule_version"]
        ),
        freshness_as_of=_strict_utc(address_fields["freshness_as_of"]),
        selection_rule_version=_strict_protocol(
            address_fields["selection_rule_version"]
        ),
        selection_eligible=eligible,
    )


def validate_canonical_address_evidence(value: object) -> CanonicalAddressEvidence:
    """Rebuild and validate an exact canonical address descriptor."""
    if type(value) is not CanonicalAddressEvidence:
        raise _fail()
    return build_canonical_address_evidence(
        {field_name: getattr(value, field_name) for field_name in value._fields}
    )


def _validate_address_freshness(value: object, observed_at: str) -> None:
    address = validate_canonical_address_evidence(value)
    if address.freshness_as_of > observed_at:
        raise _fail()


def _normalized_source_records(
    release: PublicEvidenceSourceReleaseDescriptor, value: object
) -> tuple[EvidenceSourceRecordReference, ...]:
    if (
        type(value) is not tuple
        or not 1 <= len(value) <= MAX_PUBLIC_EVIDENCE_SOURCE_RECORDS
    ):
        raise _fail()
    normalized_records = tuple(
        validate_evidence_source_record_reference(release, item) for item in value
    )
    ordered_records = tuple(
        sorted(normalized_records, key=lambda item: item.source_record_ref)
    )
    if len({item.source_record_ref for item in ordered_records}) != len(
        ordered_records
    ):
        raise _fail()
    return ordered_records


def _validated_temporal_scope(
    release: PublicEvidenceSourceReleaseDescriptor,
    observed_at: object,
    effective_interval: object,
) -> tuple[str, CanonicalUtcInterval]:
    observed = _strict_utc(observed_at)
    if type(effective_interval) is not CanonicalUtcInterval:
        raise _fail()
    try:
        effective = CanonicalUtcInterval(
            effective_interval.start_at, effective_interval.end_at
        )
    except Exception:
        raise _fail() from None
    if (
        not release.observed_interval.start_at
        <= observed
        <= release.observed_interval.end_at
    ):
        raise _fail()
    release_effective = release.effective_interval
    if effective.start_at < release_effective.start_at:
        raise _fail()
    if release_effective.end_at is not None and (
        effective.end_at is None or effective.end_at > release_effective.end_at
    ):
        raise _fail()
    return observed, effective
