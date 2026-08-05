# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed validation-only admission for paired tax-identity sidecars."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping

from process.ptg_parts._ptg2_tax_identity_shadow_files import (
    TaxIdentityShadowAdmissionError,
    authenticate_shadow_artifact_pair,
)
from process.tin_npi_connector_security import canonical_token_policy_id
from process.tin_npi_connector_support import TinNpiConnectorError


TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT = "ptg2_tax_identity_shadow_bundle_v1"
TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY = "v1_only"
TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES = 8 * 1024 * 1024 * 1024
TAX_IDENTITY_SHADOW_MAX_ROWS = 100_000_000

_BINDING_DIGEST_DOMAIN = b"PTG2_TAX_IDENTITY_SHADOW_BUNDLE_V1\x00"
_HEADER_BYTES = 13
_RECORD_BYTES = 65
_V1_FORMAT = "ptg2_provider_group_tax_identity_v1"
_V2_FORMAT = "ptg2_provider_group_tax_identity_v2"
_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_V1_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_V2_NORMALIZATION_CONTRACT = (
    "ein_ascii_digits_or_2_7_hyphen_and_npi_10_ascii_digits_cms_80840_luhn_v2"
)
_V2_TOKEN_MESSAGE_CONTRACT = (
    "healthporta_ptg_tin_v1_nul_u16be_type_length_type_u16be_value_length_value"
)
_V2_TIN_ID_CONTRACT = "first_16_bytes(tin_hmac_sha256)"
_V2_HMAC_AUTHORITY_CONTRACT = "tin_hmac_sha256_full_32_bytes_authoritative"
_V1_FIELDS = frozenset(
    "path bytes row_count provider_group_count matched_ein_count missing_count "
    "malformed_count unsupported_type_count format version record_bytes "
    "token_policy_id normalization_contract hmac_contract sha256 final".split()
)
_V2_FIELDS = _V1_FIELDS | frozenset(
    "matched_npi_count token_message_contract tin_id_128_contract "
    "full_hmac_authority_contract".split()
)

_DESCRIPTOR_INVALID = "ptg2_tax_identity_shadow_descriptor_invalid"
_CEILING_INVALID = "ptg2_tax_identity_shadow_ceiling_invalid"
_CEILING_EXCEEDED = "ptg2_tax_identity_shadow_ceiling_exceeded"
_PAIR_INVALID = "ptg2_tax_identity_shadow_pair_invalid"


@dataclass(frozen=True, slots=True)
class TaxIdentityShadowStateCounts:
    """Normalized five-state counts for one scanner artifact."""

    matched_ein: int
    matched_npi: int
    missing: int
    malformed: int
    unsupported_type: int

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value < 0
            for value in (
                self.matched_ein,
                self.matched_npi,
                self.missing,
                self.malformed,
                self.unsupported_type,
            )
        ):
            raise TaxIdentityShadowAdmissionError(_DESCRIPTOR_INVALID)

    @property
    def total(self) -> int:
        """Return the total rows represented by all five scanner states."""

        return (
            self.matched_ein
            + self.matched_npi
            + self.missing
            + self.malformed
            + self.unsupported_type
        )


@dataclass(frozen=True, slots=True, repr=False)
class TaxIdentityShadowArtifactDescriptor:
    """Immutable metadata authenticated while one scanner artifact was held."""

    sidecar_version: Literal[1, 2]
    path: Path
    artifact_format: str
    byte_count: int
    row_count: int
    provider_group_count: int
    record_bytes: int
    token_policy_id: str
    sha256: str
    state_counts: TaxIdentityShadowStateCounts
    normalization_contract: str
    hmac_contract: str
    token_message_contract: str | None
    tin_id_128_contract: str | None
    full_hmac_authority_contract: str | None

    def __repr__(self) -> str:
        return (
            "<TaxIdentityShadowArtifactDescriptor "
            f"version={self.sidecar_version} rows={self.row_count} "
            "path=<redacted> policy=<redacted> sha256=<redacted>>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class TaxIdentityShadowBundleDescriptor:
    """Validated SHADOW pair that cannot authorize publication.

    Admission closes its held file descriptors before returning. Every later
    byte consumer must securely reopen the artifacts and reauthenticate their
    exact SHA-256 digests; this descriptor alone is never byte-use authority.
    """

    v1: TaxIdentityShadowArtifactDescriptor
    v2: TaxIdentityShadowArtifactDescriptor
    binding_sha256: str
    contract: str = field(default=TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT, init=False)
    shadow_state: Literal["SHADOW"] = field(default="SHADOW", init=False)
    projection_authority: Literal["v1_only"] = field(
        default=TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY,
        init=False,
    )
    publication_enabled: Literal[False] = field(default=False, init=False)

    def __repr__(self) -> str:
        return (
            "<TaxIdentityShadowBundleDescriptor state=SHADOW "
            f"rows={self.v1.row_count} projection_authority=v1_only "
            "publication_enabled=False binding=<redacted>>"
        )


@dataclass(frozen=True, slots=True)
class _DescriptorCore:
    path: Path
    policy_id: str
    row_count: int
    provider_group_count: int
    byte_count: int
    state_counts: TaxIdentityShadowStateCounts


def _fail(code: str) -> TaxIdentityShadowAdmissionError:
    return TaxIdentityShadowAdmissionError(code)


def _strict_count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise _fail(_DESCRIPTOR_INVALID)
    return value


def _strict_sha256(value: object) -> str:
    if type(value) is not str or len(value) != 64 or value.lower() != value:
        raise _fail(_DESCRIPTOR_INVALID)
    try:
        bytes.fromhex(value)
    except ValueError:
        raise _fail(_DESCRIPTOR_INVALID) from None
    return value


def _strict_policy_id(value: object) -> str:
    try:
        return canonical_token_policy_id(value)
    except TinNpiConnectorError:
        raise _fail(_DESCRIPTOR_INVALID) from None


def _validated_ceiling(value: object, hard_maximum: int) -> int:
    if type(value) is not int or value <= 0 or value > hard_maximum:
        raise _fail(_CEILING_INVALID)
    return value


def _descriptor_counts(raw: Mapping[str, Any], version: int) -> TaxIdentityShadowStateCounts:
    return TaxIdentityShadowStateCounts(
        matched_ein=_strict_count(raw.get("matched_ein_count")),
        matched_npi=(
            _strict_count(raw.get("matched_npi_count")) if version == 2 else 0
        ),
        missing=_strict_count(raw.get("missing_count")),
        malformed=_strict_count(raw.get("malformed_count")),
        unsupported_type=_strict_count(raw.get("unsupported_type_count")),
    )


def _parse_descriptor_core(
    raw: Mapping[str, Any],
    version: Literal[1, 2],
) -> _DescriptorCore:
    expected_fields = _V1_FIELDS if version == 1 else _V2_FIELDS
    if not isinstance(raw, Mapping) or set(raw) != expected_fields:
        raise _fail(_DESCRIPTOR_INVALID)
    raw_path = raw.get("path")
    if type(raw_path) is not str or not raw_path or not Path(raw_path).is_absolute():
        raise _fail(_DESCRIPTOR_INVALID)
    return _DescriptorCore(
        path=Path(raw_path),
        policy_id=_strict_policy_id(raw.get("token_policy_id")),
        row_count=_strict_count(raw.get("row_count")),
        provider_group_count=_strict_count(raw.get("provider_group_count")),
        byte_count=_strict_count(raw.get("bytes")),
        state_counts=_descriptor_counts(raw, version),
    )


def _validate_descriptor_contract(
    raw: Mapping[str, Any],
    core: _DescriptorCore,
    version: Literal[1, 2],
    max_artifact_bytes: int,
    max_row_count: int,
) -> None:
    artifact_format = _V1_FORMAT if version == 1 else _V2_FORMAT
    normalization = (
        _V1_NORMALIZATION_CONTRACT if version == 1 else _V2_NORMALIZATION_CONTRACT
    )
    expected_bytes = _HEADER_BYTES + len(core.policy_id) + core.row_count * _RECORD_BYTES
    if core.row_count > max_row_count or core.byte_count > max_artifact_bytes:
        raise _fail(_CEILING_EXCEEDED)
    is_invalid = (
        core.provider_group_count != core.row_count
        or core.state_counts.total != core.row_count
        or core.byte_count != expected_bytes
        or raw.get("format") != artifact_format
        or type(raw.get("version")) is not int
        or raw.get("version") != version
        or type(raw.get("record_bytes")) is not int
        or raw.get("record_bytes") != _RECORD_BYTES
        or raw.get("normalization_contract") != normalization
        or raw.get("hmac_contract") != _HMAC_CONTRACT
        or raw.get("final") is not True
    )
    if is_invalid:
        raise _fail(_DESCRIPTOR_INVALID)
    if version == 2 and (
        raw.get("token_message_contract") != _V2_TOKEN_MESSAGE_CONTRACT
        or raw.get("tin_id_128_contract") != _V2_TIN_ID_CONTRACT
        or raw.get("full_hmac_authority_contract") != _V2_HMAC_AUTHORITY_CONTRACT
    ):
        raise _fail(_DESCRIPTOR_INVALID)


def _normalize_descriptor(
    raw: Mapping[str, Any],
    *,
    version: Literal[1, 2],
    max_artifact_bytes: int,
    max_row_count: int,
) -> TaxIdentityShadowArtifactDescriptor:
    """Freeze one exact scanner contract after strict aggregate validation."""

    core = _parse_descriptor_core(raw, version)
    _validate_descriptor_contract(
        raw,
        core,
        version,
        max_artifact_bytes,
        max_row_count,
    )
    artifact_format = _V1_FORMAT if version == 1 else _V2_FORMAT
    normalization_contract = (
        _V1_NORMALIZATION_CONTRACT if version == 1 else _V2_NORMALIZATION_CONTRACT
    )
    return TaxIdentityShadowArtifactDescriptor(
        sidecar_version=version,
        path=core.path,
        artifact_format=artifact_format,
        byte_count=core.byte_count,
        row_count=core.row_count,
        provider_group_count=core.provider_group_count,
        record_bytes=_RECORD_BYTES,
        token_policy_id=core.policy_id,
        sha256=_strict_sha256(raw.get("sha256")),
        state_counts=core.state_counts,
        normalization_contract=normalization_contract,
        hmac_contract=_HMAC_CONTRACT,
        token_message_contract=(_V2_TOKEN_MESSAGE_CONTRACT if version == 2 else None),
        tin_id_128_contract=(_V2_TIN_ID_CONTRACT if version == 2 else None),
        full_hmac_authority_contract=(
            _V2_HMAC_AUTHORITY_CONTRACT if version == 2 else None
        ),
    )


def is_aggregate_transition_feasible(
    v1: TaxIdentityShadowStateCounts,
    v2: TaxIdentityShadowStateCounts,
) -> bool:
    """Return necessary aggregate feasibility only; this never proves row parity."""

    npi_derived_malformed = v2.malformed - v1.malformed
    return (
        v1.matched_npi == 0
        and v1.matched_ein == v2.matched_ein
        and v1.missing == v2.missing
        and npi_derived_malformed >= 0
        and v1.unsupported_type
        == v2.matched_npi + npi_derived_malformed + v2.unsupported_type
    )


def _is_pair_consistent(
    v1: TaxIdentityShadowArtifactDescriptor,
    v2: TaxIdentityShadowArtifactDescriptor,
) -> bool:
    return (
        v1.path != v2.path
        and v1.row_count == v2.row_count
        and v1.provider_group_count == v2.provider_group_count
        and hmac.compare_digest(v1.token_policy_id, v2.token_policy_id)
        and is_aggregate_transition_feasible(v1.state_counts, v2.state_counts)
    )


def _binding_artifact_fields(artifact: TaxIdentityShadowArtifactDescriptor) -> dict[str, Any]:
    counts = artifact.state_counts
    return {
        "version": artifact.sidecar_version,
        "format": artifact.artifact_format,
        "bytes": artifact.byte_count,
        "rows": artifact.row_count,
        "groups": artifact.provider_group_count,
        "record_bytes": artifact.record_bytes,
        "token_policy_id": artifact.token_policy_id,
        "sha256": artifact.sha256,
        "state_counts": {
            "matched_ein": counts.matched_ein,
            "matched_npi": counts.matched_npi,
            "missing": counts.missing,
            "malformed": counts.malformed,
            "unsupported_type": counts.unsupported_type,
        },
        "normalization_contract": artifact.normalization_contract,
        "hmac_contract": artifact.hmac_contract,
        "token_message_contract": artifact.token_message_contract,
        "tin_id_128_contract": artifact.tin_id_128_contract,
        "full_hmac_authority_contract": artifact.full_hmac_authority_contract,
    }


def _shadow_binding_sha256(
    v1: TaxIdentityShadowArtifactDescriptor,
    v2: TaxIdentityShadowArtifactDescriptor,
) -> str:
    normalized_payload_dict = {
        "contract": TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT,
        "shadow_state": "SHADOW",
        "projection_authority": TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY,
        "publication_enabled": False,
        "v1": _binding_artifact_fields(v1),
        "v2": _binding_artifact_fields(v2),
    }
    encoded = json.dumps(
        normalized_payload_dict,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(_BINDING_DIGEST_DOMAIN)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def admit_tax_identity_shadow_bundle(
    *,
    scratch_root: str | Path,
    v1_scanner_descriptor: Mapping[str, Any],
    v2_scanner_descriptor: Mapping[str, Any],
    max_artifact_bytes: int = TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES,
    max_row_count: int = TAX_IDENTITY_SHADOW_MAX_ROWS,
) -> TaxIdentityShadowBundleDescriptor:
    """Authenticate a publication-disabled scanner pair and close held FDs.

    Later byte consumers must securely reopen both artifacts and reauthenticate
    their exact SHA-256 digests. This returned descriptor is metadata authority,
    never authority to consume path contents without another held-FD check.
    """

    byte_ceiling = _validated_ceiling(
        max_artifact_bytes,
        TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES,
    )
    row_ceiling = _validated_ceiling(max_row_count, TAX_IDENTITY_SHADOW_MAX_ROWS)
    v1 = _normalize_descriptor(
        v1_scanner_descriptor,
        version=1,
        max_artifact_bytes=byte_ceiling,
        max_row_count=row_ceiling,
    )
    v2 = _normalize_descriptor(
        v2_scanner_descriptor,
        version=2,
        max_artifact_bytes=byte_ceiling,
        max_row_count=row_ceiling,
    )
    if not _is_pair_consistent(v1, v2):
        raise _fail(_PAIR_INVALID)
    authenticate_shadow_artifact_pair(
        scratch_root=scratch_root,
        v1=v1,
        v2=v2,
        max_artifact_bytes=byte_ceiling,
    )
    return TaxIdentityShadowBundleDescriptor(
        v1=v1,
        v2=v2,
        binding_sha256=_shadow_binding_sha256(v1, v2),
    )


aggregate_transition_feasible = is_aggregate_transition_feasible


__all__ = [
    "TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT",
    "TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES",
    "TAX_IDENTITY_SHADOW_MAX_ROWS",
    "TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY",
    "TaxIdentityShadowAdmissionError",
    "TaxIdentityShadowArtifactDescriptor",
    "TaxIdentityShadowBundleDescriptor",
    "TaxIdentityShadowStateCounts",
    "admit_tax_identity_shadow_bundle",
    "aggregate_transition_feasible",
    "is_aggregate_transition_feasible",
]
