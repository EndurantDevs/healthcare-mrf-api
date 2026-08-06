# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication-disabled contracts for phase-one public evidence releases."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import hmac
import json
from typing import Any, Literal, Mapping

from public_evidence.source_release_policies import SOURCE_POLICIES, SourcePolicy
from public_evidence.source_release_primitives import (
    PUBLIC_EVIDENCE_IDENTITY_REF_PREFIX,
    PUBLIC_EVIDENCE_IMPORT_RUN_REF_PREFIX,
    PUBLIC_EVIDENCE_RELEASE_REF_PREFIX,
    TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
    CanonicalUtcInterval,
    ImmutablePublicSourceIdentity,
    OpaqueSourceBindingReference,
    PublicEvidenceCompletenessAttestation,
    PublicEvidenceSourceReleaseError,
    _derived_opaque_ref,
    _fail,
    _strict_derived_ref,
    _strict_sha256,
    derive_public_evidence_identity_ref,
)

_CONTRACT_ID = "healthporta.public-evidence-source-release.v1"
PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT = _CONTRACT_ID
PUBLIC_EVIDENCE_FOUNDATION_SCOPE = "phase_1_public_source_neutral_foundation"
_CONTRACT_DIGEST_DOMAIN = b"HEALTHPORTA_PUBLIC_EVIDENCE_SOURCE_RELEASE_V1\x00"
_REQUIRED_TRUE_FIELDS = tuple(
    "artifact_bytes_verified public_access_verified "
    "processing_retention_rights_verified semantic_limits_verified "
    "completeness_attestation_verified".split()
)
_REQUIRED_FALSE_FIELDS = tuple(
    "legal_ownership_claimed exact_rate_site_claimed whole_source_complete "
    "redistribution_enabled export_enabled publication_enabled replacement_enabled "
    "deletion_enabled retirement_enabled supersession_enabled".split()
)
_SOURCE_FIELDS = frozenset(
    "artifact_bytes_verified artifact_identity authority_classification "
    "completeness_attestation completeness_attestation_verified deletion_enabled "
    "effective_interval exact_rate_site_claimed export_enabled import_run_ref "
    "legal_ownership_claimed observed_interval processing_retention_rights_verified "
    "public_access_verified publication_enabled redistribution_enabled "
    "replacement_enabled retirement_enabled rights_classification rights_proof_sha256 "
    "semantic_limits semantic_limits_verified source_binding source_kind "
    "source_release_ref supersession_enabled trust_classification "
    "whole_source_complete".split()
)
_BUILD_FIELDS = _SOURCE_FIELDS.difference({"import_run_ref", "source_release_ref"})
_INIT_FIELDS = tuple(sorted(_SOURCE_FIELDS.difference(_REQUIRED_FALSE_FIELDS)))


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
    release_by_field: Mapping[str, object], policy: SourcePolicy
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


def _release_components(
    raw: Mapping[str, object],
) -> tuple[
    ImmutablePublicSourceIdentity,
    PublicEvidenceCompletenessAttestation,
    OpaqueSourceBindingReference | None,
    CanonicalUtcInterval,
    CanonicalUtcInterval,
]:
    """Detach and revalidate every nested release component."""
    artifact = _detached_typed(
        raw.get("artifact_identity"), ImmutablePublicSourceIdentity
    )
    attestation = _detached_typed(
        raw.get("completeness_attestation"), PublicEvidenceCompletenessAttestation
    )
    raw_binding = raw.get("source_binding")
    binding = (
        None
        if raw_binding is None
        else _detached_typed(raw_binding, OpaqueSourceBindingReference)
    )
    observed = _detached_typed(raw.get("observed_interval"), CanonicalUtcInterval)
    effective = _detached_typed(raw.get("effective_interval"), CanonicalUtcInterval)
    return artifact, attestation, binding, observed, effective


def _validate_release_relationships(
    policy: SourcePolicy,
    artifact: ImmutablePublicSourceIdentity,
    attestation: PublicEvidenceCompletenessAttestation,
    binding: OpaqueSourceBindingReference | None,
    observed: CanonicalUtcInterval,
) -> None:
    """Require one policy-consistent, same-subject evidence relationship."""
    if (
        artifact.identity_kind != policy.identity_kind
        or artifact.content_identity_kind not in policy.content_identity_kinds
        or attestation.mode != policy.attestation_mode
        or attestation.evidence_contract_id != policy.evidence_contract_id
        or attestation.count_unit != policy.count_unit
        or not hmac.compare_digest(attestation.subject_sha256, artifact.content_sha256)
        or (binding is not None) != policy.source_binding_required
        or (
            binding is not None
            and (
                binding.source_artifact_source_type
                not in policy.source_binding_source_types
                or binding.source_artifact_identity_kind
                != artifact.content_identity_kind
                or not hmac.compare_digest(
                    binding.source_artifact_sha256,
                    artifact.content_sha256,
                )
            )
        )
        or observed.end_at is None
    ):
        raise _fail()


def _normalized_release(raw: object) -> dict[str, Any]:
    """Normalize either build input or one full descriptor reconstruction."""
    if type(raw) is not dict:
        raise _fail()
    raw_fields = frozenset(raw)
    if raw_fields not in (_BUILD_FIELDS, _SOURCE_FIELDS):
        raise _fail()
    source_kind = raw.get("source_kind")
    if type(source_kind) is not str or source_kind not in SOURCE_POLICIES:
        raise _fail()
    policy = SOURCE_POLICIES[source_kind]
    _validate_policy_fields(raw, policy)
    artifact, attestation, binding, observed, effective = _release_components(raw)
    _validate_release_relationships(policy, artifact, attestation, binding, observed)
    release_by_field = {
        **raw,
        "source_kind": source_kind,
        "artifact_identity": artifact,
        "completeness_attestation": attestation,
        "source_binding": binding,
        "observed_interval": observed,
        "effective_interval": effective,
        "rights_proof_sha256": _strict_sha256(raw.get("rights_proof_sha256")),
    }
    import_run_ref = _derive_import_run_ref(release_by_field)
    release_by_field["import_run_ref"] = import_run_ref
    source_release_ref = _derive_source_release_ref(release_by_field)
    release_by_field["source_release_ref"] = source_release_ref
    if raw_fields == _SOURCE_FIELDS:
        _strict_derived_ref(raw.get("import_run_ref"), import_run_ref)
        _strict_derived_ref(raw.get("source_release_ref"), source_release_ref)
    return release_by_field


def _artifact_payload(artifact: ImmutablePublicSourceIdentity) -> dict[str, str]:
    return {
        "identity_kind": artifact.identity_kind,
        "content_identity_kind": artifact.content_identity_kind,
        "identity_ref": artifact.identity_ref,
        "content_sha256": artifact.content_sha256,
    }


def _attestation_payload(
    attestation: PublicEvidenceCompletenessAttestation,
) -> dict[str, object]:
    return {
        "mode": attestation.mode,
        "evidence_contract_id": attestation.evidence_contract_id,
        "count_unit": attestation.count_unit,
        "subject_sha256": attestation.subject_sha256,
        "expected_record_count": attestation.expected_record_count,
        "observed_record_count": attestation.observed_record_count,
        "evidence_root_sha256": attestation.evidence_root_sha256,
    }


def _binding_payload(
    binding: OpaqueSourceBindingReference | None,
) -> dict[str, str] | None:
    if binding is None:
        return None
    return {
        "contract_id": binding.contract_id,
        "source_artifact_source_type": binding.source_artifact_source_type,
        "source_artifact_identity_kind": binding.source_artifact_identity_kind,
        "source_artifact_sha256": binding.source_artifact_sha256,
        "source_binding_sha256": binding.source_binding_sha256,
        "shadow_bundle_binding_sha256": binding.shadow_bundle_binding_sha256,
    }


def _lifecycle_payload() -> dict[str, object]:
    return {
        "state": "verified_disabled",
        "serving_authority": "none",
        "current_pointer_authority": "none",
        "redistribution_enabled": False,
        "export_enabled": False,
        "publication_enabled": False,
        "replacement_enabled": False,
        "deletion_enabled": False,
        "retirement_enabled": False,
        "supersession_enabled": False,
    }


def _release_payload(release_by_field: Mapping[str, Any]) -> dict[str, Any]:
    observed = release_by_field["observed_interval"]
    effective = release_by_field["effective_interval"]
    return {
        "contract": PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "source_kind": release_by_field["source_kind"],
        "authority_classification": release_by_field["authority_classification"],
        "trust_classification": release_by_field["trust_classification"],
        "semantic_limits": list(release_by_field["semantic_limits"]),
        "artifact_identity": _artifact_payload(release_by_field["artifact_identity"]),
        "completeness_attestation": _attestation_payload(
            release_by_field["completeness_attestation"]
        ),
        "rights": {
            "classification": release_by_field["rights_classification"],
            "proof_sha256": release_by_field["rights_proof_sha256"],
        },
        "source_binding": _binding_payload(release_by_field["source_binding"]),
        "observed_interval": {"start_at": observed.start_at, "end_at": observed.end_at},
        "effective_interval": {
            "start_at": effective.start_at,
            "end_at": effective.end_at,
        },
        "import_run_ref": release_by_field["import_run_ref"],
        "source_release_ref": release_by_field["source_release_ref"],
        "verification": {field_name: True for field_name in _REQUIRED_TRUE_FIELDS},
        "claims": {
            "legal_ownership_claimed": False,
            "exact_rate_site_claimed": False,
            "whole_source_complete": False,
        },
        "lifecycle": _lifecycle_payload(),
    }


def _derive_import_run_ref(values: Mapping[str, Any]) -> str:
    observed = values["observed_interval"]
    effective = values["effective_interval"]
    return _derived_opaque_ref(
        PUBLIC_EVIDENCE_IMPORT_RUN_REF_PREFIX,
        "import_run",
        {
            "source_kind": values["source_kind"],
            "artifact_identity": _artifact_payload(values["artifact_identity"]),
            "completeness_attestation": _attestation_payload(
                values["completeness_attestation"]
            ),
            "source_binding": _binding_payload(values["source_binding"]),
            "rights_proof_sha256": values["rights_proof_sha256"],
            "observed_interval": {
                "start_at": observed.start_at,
                "end_at": observed.end_at,
            },
            "effective_interval": {
                "start_at": effective.start_at,
                "end_at": effective.end_at,
            },
        },
    )


def _derive_source_release_ref(values: Mapping[str, Any]) -> str:
    payload = _release_payload({**values, "source_release_ref": None})
    payload.pop("source_release_ref")
    return _derived_opaque_ref(
        PUBLIC_EVIDENCE_RELEASE_REF_PREFIX,
        "source_release",
        payload,
    )


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
    completeness_attestation: PublicEvidenceCompletenessAttestation
    rights_classification: str
    rights_proof_sha256: str
    source_binding: OpaqueSourceBindingReference | None
    observed_interval: CanonicalUtcInterval
    effective_interval: CanonicalUtcInterval
    import_run_ref: str
    source_release_ref: str
    artifact_bytes_verified: Literal[True]
    public_access_verified: Literal[True]
    processing_retention_rights_verified: Literal[True]
    semantic_limits_verified: Literal[True]
    completeness_attestation_verified: Literal[True]
    contract_sha256: str
    contract: str = field(default=_CONTRACT_ID, init=False)
    foundation_scope: str = field(default=PUBLIC_EVIDENCE_FOUNDATION_SCOPE, init=False)
    lifecycle_state: Literal["verified_disabled"] = field(
        default="verified_disabled", init=False
    )
    serving_authority: Literal["none"] = field(default="none", init=False)
    current_pointer_authority: Literal["none"] = field(default="none", init=False)
    legal_ownership_claimed: Literal[False] = field(default=False, init=False)
    exact_rate_site_claimed: Literal[False] = field(default=False, init=False)
    whole_source_complete: Literal[False] = field(default=False, init=False)
    redistribution_enabled: Literal[False] = field(default=False, init=False)
    export_enabled: Literal[False] = field(default=False, init=False)
    publication_enabled: Literal[False] = field(default=False, init=False)
    replacement_enabled: Literal[False] = field(default=False, init=False)
    deletion_enabled: Literal[False] = field(default=False, init=False)
    retirement_enabled: Literal[False] = field(default=False, init=False)
    supersession_enabled: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        normalized = _normalized_release(_descriptor_input(self))
        supplied_digest = _strict_sha256(self.contract_sha256)
        if not hmac.compare_digest(supplied_digest, _release_sha256(normalized)):
            raise _fail()
        for field_name in (
            "artifact_identity",
            "completeness_attestation",
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
    fixed_string_by_field = {
        "contract": PUBLIC_EVIDENCE_SOURCE_RELEASE_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "lifecycle_state": "verified_disabled",
        "serving_authority": "none",
        "current_pointer_authority": "none",
    }
    try:
        if any(
            type(getattr(descriptor, field_name)) is not str
            or getattr(descriptor, field_name) != expected
            for field_name, expected in fixed_string_by_field.items()
        ) or any(
            getattr(descriptor, field_name)
            is not (field_name in _REQUIRED_TRUE_FIELDS)
            for field_name in _REQUIRED_TRUE_FIELDS + _REQUIRED_FALSE_FIELDS
        ):
            raise _fail()
        _normalized_release(_descriptor_input(descriptor))
        return replace(descriptor)
    except PublicEvidenceSourceReleaseError:
        raise
    except Exception:
        raise _fail() from None
