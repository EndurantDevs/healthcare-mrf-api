# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated TiC tax-identity row receipts for public evidence records."""

from __future__ import annotations

from contextlib import ExitStack
import hmac
from pathlib import Path
import re
from typing import Literal

from process.evidence_source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
    validate_public_evidence_source_release,
)
from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from process.ptg_parts import _ptg2_tax_identity_shadow_files as shadow_files
from process.tin_npi_connector_security import canonical_token_policy_id
from process.evidence_tic_binding_proof import (
    TIC_PROVIDER_GROUP_REFERENCE_CONTRACT,
    TIC_TAX_IDENTITY_POLICY_VERSION,
    TicTaxIdentityBindingError,
    _AuthenticatedShadowRow,
    _TicTaxIdentityBindingReceipt,
    _fail,
    _issue_tic_tax_identity_binding_receipt,
)

_HEX_64_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_RECORD_BYTES = 65
_PLATFORM_PATH_TYPE = type(Path())
_MATCHED_STATE_BY_TYPE = {"ein": 1, "npi": 5}
_UNAVAILABLE_STATES = frozenset({2, 3, 4})
_INVALID = "public_evidence_tic_tax_identity_binding_invalid"
_NOT_FOUND = "public_evidence_tic_tax_identity_row_not_found"
_UNAVAILABLE = "public_evidence_tic_tax_identity_row_unavailable"


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _HEX_64_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _validated_state_counts(
    value: object,
) -> admission.TaxIdentityShadowStateCounts:
    if type(value) is not admission.TaxIdentityShadowStateCounts:
        raise _fail()
    try:
        state_counts = (
            value.matched_ein,
            value.matched_npi,
            value.missing,
            value.malformed,
            value.unsupported_type,
        )
        if any(type(count) is not int for count in state_counts) or any(
            not 0 <= count <= admission.TAX_IDENTITY_SHADOW_MAX_ROWS
            for count in state_counts
        ):
            raise _fail()
        if sum(state_counts) > admission.TAX_IDENTITY_SHADOW_MAX_ROWS:
            raise _fail()
        return admission.TaxIdentityShadowStateCounts(*state_counts)
    except TicTaxIdentityBindingError:
        raise
    except Exception:
        raise _fail() from None


def _validate_exact_artifact_field_types(
    artifact: admission.TaxIdentityShadowArtifactDescriptor,
    version: Literal[1, 2],
) -> None:
    """Reject non-builtins before path methods, parsing, or arithmetic."""
    artifact_string_fields = (
        artifact.artifact_format,
        artifact.token_policy_id,
        artifact.sha256,
        artifact.normalization_contract,
        artifact.hmac_contract,
    )
    version_specific_fields = (
        artifact.token_message_contract,
        artifact.tin_id_128_contract,
        artifact.full_hmac_authority_contract,
    )
    if (
        type(artifact.path) is not _PLATFORM_PATH_TYPE
        or any(
            type(artifact_field) is not str for artifact_field in artifact_string_fields
        )
        or any(
            type(artifact_field) is not int
            for artifact_field in (
                artifact.sidecar_version,
                artifact.byte_count,
                artifact.row_count,
                artifact.provider_group_count,
                artifact.record_bytes,
            )
        )
        or not 0
        <= artifact.byte_count
        <= admission.TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES
        or not 0 <= artifact.row_count <= admission.TAX_IDENTITY_SHADOW_MAX_ROWS
        or not 0
        <= artifact.provider_group_count
        <= admission.TAX_IDENTITY_SHADOW_MAX_ROWS
        or (
            version == 1
            and any(
                artifact_field is not None for artifact_field in version_specific_fields
            )
        )
        or (
            version == 2
            and any(
                type(artifact_field) is not str
                for artifact_field in version_specific_fields
            )
        )
    ):
        raise _fail()


def _artifact_contract_fields(
    artifact: admission.TaxIdentityShadowArtifactDescriptor,
    version: Literal[1, 2],
) -> tuple[admission.TaxIdentityShadowStateCounts, str, str, str]:
    """Validate exact descriptor fields before reconstructing the artifact."""
    _validate_exact_artifact_field_types(artifact, version)
    counts = _validated_state_counts(artifact.state_counts)
    try:
        policy_id = canonical_token_policy_id(artifact.token_policy_id)
    except Exception:
        raise _fail() from None
    expected_format = admission._V1_FORMAT if version == 1 else admission._V2_FORMAT
    expected_normalization = (
        admission._V1_NORMALIZATION_CONTRACT
        if version == 1
        else admission._V2_NORMALIZATION_CONTRACT
    )
    expected_bytes = (
        admission._HEADER_BYTES
        + len(policy_id)
        + artifact.row_count * admission._RECORD_BYTES
    )
    expected_v2_fields = (
        admission._V2_TOKEN_MESSAGE_CONTRACT,
        admission._V2_TIN_ID_CONTRACT,
        admission._V2_HMAC_AUTHORITY_CONTRACT,
    )
    if (
        not artifact.path.is_absolute()
        or artifact.sidecar_version != version
        or artifact.artifact_format != expected_format
        or artifact.byte_count != expected_bytes
        or artifact.provider_group_count != artifact.row_count
        or artifact.record_bytes != admission._RECORD_BYTES
        or counts.total != artifact.row_count
        or (version == 1 and counts.matched_npi != 0)
        or artifact.normalization_contract != expected_normalization
        or artifact.hmac_contract != admission._HMAC_CONTRACT
        or (
            artifact.token_message_contract,
            artifact.tin_id_128_contract,
            artifact.full_hmac_authority_contract,
        )
        != (expected_v2_fields if version == 2 else (None, None, None))
    ):
        raise _fail()
    return counts, policy_id, expected_format, expected_normalization


def _validated_artifact(
    artifact: object, version: Literal[1, 2]
) -> admission.TaxIdentityShadowArtifactDescriptor:
    if type(artifact) is not admission.TaxIdentityShadowArtifactDescriptor:
        raise _fail()
    try:
        counts, policy_id, artifact_format, normalization = _artifact_contract_fields(
            artifact, version
        )
        v2_fields = (
            admission._V2_TOKEN_MESSAGE_CONTRACT,
            admission._V2_TIN_ID_CONTRACT,
            admission._V2_HMAC_AUTHORITY_CONTRACT,
        )
        return admission.TaxIdentityShadowArtifactDescriptor(
            version,
            artifact.path,
            artifact_format,
            artifact.byte_count,
            artifact.row_count,
            artifact.provider_group_count,
            admission._RECORD_BYTES,
            policy_id,
            _strict_sha256(artifact.sha256),
            counts,
            normalization,
            admission._HMAC_CONTRACT,
            *(v2_fields if version == 2 else (None, None, None)),
        )
    except TicTaxIdentityBindingError:
        raise
    except Exception:
        raise _fail() from None


def validate_tax_identity_shadow_bundle_descriptor(
    bundle_candidate: object,
) -> admission.TaxIdentityShadowBundleDescriptor:
    """Detach and revalidate one admitted, publication-disabled bundle receipt."""

    if type(bundle_candidate) is not admission.TaxIdentityShadowBundleDescriptor:
        raise _fail()
    try:
        if (
            type(bundle_candidate.contract) is not str
            or bundle_candidate.contract
            != admission.TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT
            or type(bundle_candidate.shadow_state) is not str
            or bundle_candidate.shadow_state != "SHADOW"
            or type(bundle_candidate.projection_authority) is not str
            or bundle_candidate.projection_authority
            != admission.TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY
            or bundle_candidate.publication_enabled is not False
        ):
            raise _fail()
        v1 = _validated_artifact(bundle_candidate.v1, 1)
        v2 = _validated_artifact(bundle_candidate.v2, 2)
        expected_binding = admission._shadow_binding_sha256(v1, v2)
        if not admission._is_pair_consistent(v1, v2) or not hmac.compare_digest(
            _strict_sha256(bundle_candidate.binding_sha256), expected_binding
        ):
            raise _fail()
        return admission.TaxIdentityShadowBundleDescriptor(v1, v2, expected_binding)
    except TicTaxIdentityBindingError:
        raise
    except Exception:
        raise _fail() from None


def _read_exact_record(stream: object) -> bytes:
    try:
        encoded = stream.read(_RECORD_BYTES)
    except Exception:
        raise _fail() from None
    if type(encoded) is not bytes or len(encoded) != _RECORD_BYTES:
        raise _fail()
    return encoded


def _validated_row(encoded: bytes) -> tuple[bytes, int, bytes, bytes]:
    if type(encoded) is not bytes or len(encoded) != _RECORD_BYTES:
        raise _fail()
    group_id = encoded[:16]
    state = encoded[16]
    locator = encoded[17:33]
    full_hmac = encoded[33:]
    is_matched = state in _MATCHED_STATE_BY_TYPE.values()
    if (
        (
            is_matched
            and (
                full_hmac == bytes(32)
                or not hmac.compare_digest(locator, full_hmac[:16])
            )
        )
        or (
            state in _UNAVAILABLE_STATES
            and (locator != bytes(16) or full_hmac != bytes(32))
        )
        or (not is_matched and state not in _UNAVAILABLE_STATES)
    ):
        raise _fail()
    return group_id, state, locator, full_hmac


def _identity_type_for_state(state: int) -> Literal["ein", "npi"]:
    identity_type = next(
        (
            name
            for name, expected_state in _MATCHED_STATE_BY_TYPE.items()
            if state == expected_state
        ),
        None,
    )
    if identity_type is None:
        raise _fail(_UNAVAILABLE)
    return identity_type


def _scan_v2_rows(
    held_v2: object,
    bundle: admission.TaxIdentityShadowBundleDescriptor,
    target_group_id: bytes,
) -> _AuthenticatedShadowRow | None:
    held_v2.stream.seek(admission._HEADER_BYTES + len(bundle.v2.token_policy_id))
    previous_group: bytes | None = None
    matched_row: _AuthenticatedShadowRow | None = None
    for _ in range(bundle.v2.row_count):
        group_id, state, locator, full_hmac = _validated_row(
            _read_exact_record(held_v2.stream)
        )
        if previous_group is not None and previous_group >= group_id:
            raise _fail()
        previous_group = group_id
        if group_id == target_group_id:
            matched_row = _AuthenticatedShadowRow(
                group_id, _identity_type_for_state(state), locator, full_hmac
            )
    if held_v2.stream.read(1):
        raise _fail()
    return matched_row


def _read_authenticated_row(
    bundle: admission.TaxIdentityShadowBundleDescriptor,
    scratch_root: str | Path,
    target_group_id: bytes,
) -> _AuthenticatedShadowRow:
    if (
        type(scratch_root) not in {str, _PLATFORM_PATH_TYPE}
        or type(target_group_id) is not bytes
        or len(target_group_id) != 16
    ):
        raise _fail()
    try:
        with shadow_files._open_scratch_root(scratch_root) as root:
            first = shadow_files._preflight_artifact(
                root, bundle.v1, admission.TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES
            )
            second = shadow_files._preflight_artifact(
                root, bundle.v2, admission.TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES
            )
            shadow_files._recheck_root(root)
            with ExitStack() as stack:
                v1 = stack.enter_context(shadow_files._open_artifact(root, first))
                v2 = stack.enter_context(shadow_files._open_artifact(root, second))
                if not shadow_files._is_held_artifact_pair_distinct(v1, v2):
                    raise _fail()
                shadow_files._authenticate_held_artifact(v1)
                shadow_files._authenticate_held_artifact(v2)
                matched_row = _scan_v2_rows(v2, bundle, target_group_id)
                shadow_files._recheck_artifact(root, v1)
                shadow_files._recheck_artifact(root, v2)
                shadow_files._recheck_root(root)
        if matched_row is None:
            raise _fail(_NOT_FOUND)
        return matched_row
    except TicTaxIdentityBindingError:
        raise
    except Exception:
        raise _fail() from None


def _resolve_tic_tax_identity_binding(
    release: PublicEvidenceSourceReleaseDescriptor,
    bundle: admission.TaxIdentityShadowBundleDescriptor,
    *,
    scratch_root: str | Path,
    provider_group_global_id_128: bytes,
) -> _TicTaxIdentityBindingReceipt:
    """Authenticate one exact held-file row and return an internal proof."""

    try:
        fixed_release = validate_public_evidence_source_release(release)
        fixed_bundle = validate_tax_identity_shadow_bundle_descriptor(bundle)
        binding = fixed_release.source_binding
        if (
            type(fixed_release.source_kind) is not str
            or fixed_release.source_kind != "tic"
            or binding is None
            or type(binding.contract_id) is not str
            or binding.contract_id != TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT
            or not hmac.compare_digest(
                binding.binding_sha256, fixed_bundle.binding_sha256
            )
        ):
            raise _fail()
        policy_id = canonical_token_policy_id(fixed_bundle.v2.token_policy_id)
        authenticated_row = _read_authenticated_row(
            fixed_bundle, scratch_root, provider_group_global_id_128
        )
        return _issue_tic_tax_identity_binding_receipt(
            fixed_release, fixed_bundle, policy_id, authenticated_row
        )
    except TicTaxIdentityBindingError:
        raise
    except Exception:
        raise _fail() from None


__all__ = [
    "TicTaxIdentityBindingError",
    "TIC_PROVIDER_GROUP_REFERENCE_CONTRACT",
    "TIC_TAX_IDENTITY_POLICY_VERSION",
    "validate_tax_identity_shadow_bundle_descriptor",
]
