# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication-disabled contracts for phase-one public evidence releases."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
import hashlib
import hmac
import json
import re
from typing import Any, Literal, Mapping


_CONTRACT_ID = "healthporta.public-evidence-source-release.v1"
PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT = _CONTRACT_ID
PUBLIC_EVIDENCE_FOUNDATION_SCOPE = "phase_1_public_source_neutral_foundation"
TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT = (
    "ptg2_tax_identity_shadow_source_binding_v1"
)

_CONTRACT_DIGEST_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_SOURCE_RELEASE_V1\x00"
_INVALID = "public_evidence_source_release_invalid"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_PUBLIC_ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,159}", flags=re.ASCII)
_PRIVATE_ID_SHAPE_RE = re.compile(
    r"(?<![0-9])(?:[0-9][._-]*){8,}[0-9](?![._-]*[0-9])|"
    r"\b(?:sk-[a-z0-9_-]{20,}|"
    r"(?:(?:ghp|gho|ghu|ghs|ghr)_|github_pat_)[a-z0-9_]{20,}|"
    r"(?:sk|rk)_live_[a-z0-9_]{20,})\b",
    flags=re.ASCII,
)
_UTC_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
    flags=re.ASCII,
)
_SENSITIVE_ID_SEGMENTS = frozenset(
    "apikey authorization bearer credential ein key npi password path private "
    "raw secret tin token uri url".split()
)
_SENSITIVE_PREFIXES = tuple(
    "apikey authorization bearer credential password private raw secret token".split()
)
_SENSITIVE_SUFFIXES = tuple("apikey credential password private secret token".split())
_SENSITIVE_COMPOUNDS = frozenset(
    "artifactpath artifacturi artifacturl datasetpath dataseturi dataseturl "
    "endpointpath endpointuri endpointurl filepath fileuri fileurl privatepath "
    "privateuri privateurl rawpath rawuri rawurl sourcepath sourceuri sourceurl".split()
)
_COMPLETE_MODES = ("complete_artifact", "complete_dataset", "positive_evidence_only")
_REQUIRED_TRUE_FIELDS = tuple(
    "artifact_bytes_verified public_access_verified "
    "processing_retention_rights_verified semantic_limits_verified "
    "completeness_verified".split()
)
_REQUIRED_FALSE_FIELDS = tuple(
    "legal_ownership_claimed exact_rate_site_claimed redistribution_enabled "
    "export_enabled publication_enabled replacement_enabled".split()
)
_SOURCE_FIELDS = frozenset(
    "artifact_bytes_verified artifact_identity authority_classification "
    "completeness_proof completeness_verified effective_interval "
    "exact_rate_site_claimed export_enabled import_run_id legal_ownership_claimed "
    "observed_interval processing_retention_rights_verified public_access_verified "
    "publication_enabled redistribution_enabled replacement_enabled "
    "rights_classification rights_proof_sha256 semantic_limits "
    "semantic_limits_verified source_binding source_kind source_release_id "
    "trust_classification".split()
)
_INIT_FIELDS = tuple(sorted(_SOURCE_FIELDS.difference(_REQUIRED_FALSE_FIELDS)))


class PublicEvidenceSourceReleaseError(RuntimeError):
    pass


def _fail() -> PublicEvidenceSourceReleaseError:
    return PublicEvidenceSourceReleaseError(_INVALID)


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_public_id(value: object) -> str:
    if type(value) is not str or _PUBLIC_ID_RE.fullmatch(value) is None:
        raise _fail()
    segments = frozenset(re.split(r"[._-]+", value))
    has_sensitive_affix = any(
        segment.startswith(_SENSITIVE_PREFIXES)
        or segment.endswith(_SENSITIVE_SUFFIXES)
        or segment in _SENSITIVE_COMPOUNDS
        for segment in segments
    )
    if (
        segments.intersection(_SENSITIVE_ID_SEGMENTS)
        or has_sensitive_affix
        or _PRIVATE_ID_SHAPE_RE.search(value)
    ):
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


@dataclass(frozen=True, slots=True, repr=False)
class ImmutablePublicSourceIdentity:
    identity_kind: Literal["immutable_artifact", "immutable_dataset"]
    identity_id: str
    content_sha256: str

    def __post_init__(self) -> None:
        if type(self.identity_kind) is not str or self.identity_kind not in {
            "immutable_artifact",
            "immutable_dataset",
        }:
            raise _fail()
        _strict_public_id(self.identity_id)
        _strict_sha256(self.content_sha256)


@dataclass(frozen=True, slots=True, repr=False)
class PublicEvidenceCompletenessProof:
    mode: str
    expected_record_count: int | None
    observed_record_count: int
    proof_sha256: str

    def __post_init__(self) -> None:
        observed = self.observed_record_count
        expected = self.expected_record_count
        if (
            type(self.mode) is not str
            or self.mode not in _COMPLETE_MODES
            or type(observed) is not int
            or not 0 <= observed <= 2**63 - 1
            or (expected is not None and type(expected) is not int)
            or (type(expected) is int and not 0 <= expected <= 2**63 - 1)
        ):
            raise _fail()
        if self.mode == "positive_evidence_only" and expected is not None:
            raise _fail()
        if self.mode != "positive_evidence_only" and expected != observed:
            raise _fail()
        _strict_sha256(self.proof_sha256)


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
    contract_id: str
    binding_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.contract_id) is not str
            or self.contract_id != TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT
        ):
            raise _fail()
        _strict_sha256(self.binding_sha256)


@dataclass(frozen=True, slots=True)
class _SourcePolicy:
    identity_kind: str
    authority: str
    trust: str
    rights: str
    completeness_mode: str
    semantic_limits: tuple[str, ...]
    source_binding_required: bool


_SOURCE_POLICIES = {
    "tic": _SourcePolicy(
        *"immutable_artifact payer_transparency_in_coverage "
        "authoritative_tic_rate_group_association "
        "tic_public_access_processing_retention_reviewed complete_artifact".split(),
        (
            "provider_group_membership_not_legal_ownership",
            "tic_rate_not_bound_to_exact_provider_site",
        ),
        True,
    ),
    "public_provider_directory_fhir": _SourcePolicy(
        *"immutable_dataset public_payer_provider_directory_fhir "
        "public_provider_directory_source_evidence "
        "provider_directory_public_access_processing_retention_reviewed "
        "complete_dataset".split(),
        (
            "directory_relationship_not_legal_ownership",
            "directory_location_not_exact_rate_site",
            "location_corroboration_requires_exact_npi_active_role_location_plan_network_bridge",
        ),
        False,
    ),
    "nppes_entity_address": _SourcePolicy(
        *"immutable_dataset cms_nppes_npi_registry "
        "authoritative_npi_enumeration_and_registry_record_status "
        "nppes_public_access_processing_retention_reviewed complete_dataset".split(),
        (
            "non_system_fields_provider_or_authorized_official_reported",
            "nppes_not_payer_confirmed",
            "nppes_has_no_plan_network_binding",
            "nppes_not_tin_address_proof",
            "nppes_not_affiliation_or_ownership_proof",
            "nppes_not_credentialing_proof",
            "nppes_not_current_service_site_proof",
            "nppes_not_universal_ein_npi_crosswalk",
            "registry_address_not_exact_rate_site",
        ),
        False,
    ),
    "public_hpt": _SourcePolicy(
        *"immutable_artifact hospital_published_hpt_machine_readable_artifact "
        "public_hospital_entity_location_candidate "
        "hpt_public_access_processing_retention_reviewed positive_evidence_only".split(),
        (
            "cms_hpt_rule_schema_is_regulatory_context_not_artifact_authorship",
            "hospital_evidence_not_universal_ein_npi_crosswalk",
            "hospital_location_not_exact_rate_site",
        ),
        False,
    ),
}


def _detached_typed(value: object, expected_type: type[Any]) -> Any:
    if type(value) is not expected_type:
        raise _fail()
    try:
        return expected_type(
            **{field_name: getattr(value, field_name) for field_name in value.__slots__}
        )
    except PublicEvidenceSourceReleaseError:
        raise
    except Exception:
        raise _fail() from None


def _validate_policy_fields(
    release_by_field: Mapping[str, object], policy: _SourcePolicy
) -> None:
    semantic_limits = release_by_field.get("semantic_limits")
    if (
        type(release_by_field.get("authority_classification")) is not str
        or release_by_field.get("authority_classification") != policy.authority
        or type(release_by_field.get("trust_classification")) is not str
        or release_by_field.get("trust_classification") != policy.trust
        or type(semantic_limits) is not tuple
        or any(type(limit) is not str for limit in semantic_limits)
        or semantic_limits != policy.semantic_limits
        or type(release_by_field.get("rights_classification")) is not str
        or release_by_field.get("rights_classification") != policy.rights
    ):
        raise _fail()
    if any(
        release_by_field.get(field_name) is not True
        for field_name in _REQUIRED_TRUE_FIELDS
    ) or any(
        release_by_field.get(field_name) is not False
        for field_name in _REQUIRED_FALSE_FIELDS
    ):
        raise _fail()


def _normalized_release(raw: object) -> dict[str, Any]:
    if type(raw) is not dict or set(raw) != _SOURCE_FIELDS:
        raise _fail()
    source_kind = raw.get("source_kind")
    if type(source_kind) is not str or source_kind not in _SOURCE_POLICIES:
        raise _fail()
    policy = _SOURCE_POLICIES[source_kind]
    _validate_policy_fields(raw, policy)
    artifact = _detached_typed(
        raw.get("artifact_identity"), ImmutablePublicSourceIdentity
    )
    completeness = _detached_typed(
        raw.get("completeness_proof"), PublicEvidenceCompletenessProof
    )
    raw_binding = raw.get("source_binding")
    binding = (
        None
        if raw_binding is None
        else _detached_typed(raw_binding, OpaqueSourceBindingReference)
    )
    observed = _detached_typed(raw.get("observed_interval"), CanonicalUtcInterval)
    effective = _detached_typed(raw.get("effective_interval"), CanonicalUtcInterval)
    if (
        artifact.identity_kind != policy.identity_kind
        or completeness.mode != policy.completeness_mode
        or (binding is not None) != policy.source_binding_required
        or observed.end_at is None
    ):
        raise _fail()
    return {
        **raw,
        "source_kind": source_kind,
        "artifact_identity": artifact,
        "completeness_proof": completeness,
        "observed_interval": observed,
        "effective_interval": effective,
        "source_binding": binding,
        "rights_proof_sha256": _strict_sha256(raw.get("rights_proof_sha256")),
        "import_run_id": _strict_public_id(raw.get("import_run_id")),
        "source_release_id": _strict_public_id(raw.get("source_release_id")),
    }


def _release_payload(release_by_field: Mapping[str, Any]) -> dict[str, Any]:
    artifact = release_by_field["artifact_identity"]
    completeness = release_by_field["completeness_proof"]
    observed = release_by_field["observed_interval"]
    effective = release_by_field["effective_interval"]
    binding = release_by_field["source_binding"]
    return {
        "contract": PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "source_kind": release_by_field["source_kind"],
        "authority_classification": release_by_field["authority_classification"],
        "trust_classification": release_by_field["trust_classification"],
        "semantic_limits": list(release_by_field["semantic_limits"]),
        "artifact_identity": dict(
            identity_kind=artifact.identity_kind,
            identity_id=artifact.identity_id,
            content_sha256=artifact.content_sha256,
        ),
        "completeness_proof": {
            "mode": completeness.mode,
            "expected_record_count": completeness.expected_record_count,
            "observed_record_count": completeness.observed_record_count,
            "proof_sha256": completeness.proof_sha256,
        },
        "rights": dict(
            classification=release_by_field["rights_classification"],
            proof_sha256=release_by_field["rights_proof_sha256"],
        ),
        "source_binding": (
            None
            if binding is None
            else {
                "contract_id": binding.contract_id,
                "binding_sha256": binding.binding_sha256,
            }
        ),
        "observed_interval": dict(start_at=observed.start_at, end_at=observed.end_at),
        "effective_interval": dict(
            start_at=effective.start_at, end_at=effective.end_at
        ),
        "import_run_id": release_by_field["import_run_id"],
        "source_release_id": release_by_field["source_release_id"],
        "verification": {field_name: True for field_name in _REQUIRED_TRUE_FIELDS},
        "claims": dict(legal_ownership_claimed=False, exact_rate_site_claimed=False),
        "serving_authority": "none",
        "redistribution_enabled": False,
        "export_enabled": False,
        "publication_enabled": False,
        "replacement_enabled": False,
    }


def _release_sha256(values: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _release_payload(values),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(_CONTRACT_DIGEST_DOMAIN)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class PublicEvidenceSourceReleaseDescriptor:
    source_kind: str
    authority_classification: str
    trust_classification: str
    semantic_limits: tuple[str, ...]
    artifact_identity: ImmutablePublicSourceIdentity
    completeness_proof: PublicEvidenceCompletenessProof
    rights_classification: str
    rights_proof_sha256: str
    source_binding: OpaqueSourceBindingReference | None
    observed_interval: CanonicalUtcInterval
    effective_interval: CanonicalUtcInterval
    import_run_id: str
    source_release_id: str
    artifact_bytes_verified: Literal[True]
    public_access_verified: Literal[True]
    processing_retention_rights_verified: Literal[True]
    semantic_limits_verified: Literal[True]
    completeness_verified: Literal[True]
    contract_sha256: str
    contract: str = field(default=_CONTRACT_ID, init=False)
    foundation_scope: str = field(default=PUBLIC_EVIDENCE_FOUNDATION_SCOPE, init=False)
    serving_authority: Literal["none"] = field(default="none", init=False)
    legal_ownership_claimed: Literal[False] = field(default=False, init=False)
    exact_rate_site_claimed: Literal[False] = field(default=False, init=False)
    redistribution_enabled: Literal[False] = field(default=False, init=False)
    export_enabled: Literal[False] = field(default=False, init=False)
    publication_enabled: Literal[False] = field(default=False, init=False)
    replacement_enabled: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        normalized = _normalized_release(_descriptor_input(self))
        supplied_digest = _strict_sha256(self.contract_sha256)
        if not hmac.compare_digest(supplied_digest, _release_sha256(normalized)):
            raise _fail()
        for field_name in (
            "artifact_identity",
            "completeness_proof",
            "source_binding",
            "observed_interval",
            "effective_interval",
        ):
            object.__setattr__(self, field_name, normalized[field_name])


def _descriptor_input(
    descriptor: PublicEvidenceSourceReleaseDescriptor,
) -> dict[str, object]:
    return {
        field_name: getattr(descriptor, field_name) for field_name in _SOURCE_FIELDS
    }


def _descriptor_from_normalized(
    normalized: Mapping[str, Any],
) -> PublicEvidenceSourceReleaseDescriptor:
    return PublicEvidenceSourceReleaseDescriptor(
        **{field_name: normalized[field_name] for field_name in _INIT_FIELDS},
        contract_sha256=_release_sha256(normalized),
    )


def build_public_evidence_source_release(
    raw: Mapping[str, object],
) -> PublicEvidenceSourceReleaseDescriptor:
    """Validate and freeze one capability-free public evidence release."""
    try:
        return _descriptor_from_normalized(_normalized_release(raw))
    except PublicEvidenceSourceReleaseError:
        raise
    except Exception:
        raise _fail() from None


def validate_public_evidence_source_release(
    descriptor: object,
) -> PublicEvidenceSourceReleaseDescriptor:
    """Rebuild one exact descriptor and reject forged or mutated instances."""
    if type(descriptor) is not PublicEvidenceSourceReleaseDescriptor:
        raise _fail()
    try:
        fixed_state = (
            descriptor.contract,
            descriptor.foundation_scope,
            descriptor.serving_authority,
        )
        if fixed_state != (
            PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT,
            PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
            "none",
        ):
            raise _fail()
        _normalized_release(_descriptor_input(descriptor))
        return replace(descriptor)
    except PublicEvidenceSourceReleaseError:
        raise
    except Exception:
        raise _fail() from None
