# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure primitives for replaying one retained NPPES registry archive."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import json
import re
from typing import Literal
from urllib.parse import urlsplit

from public_evidence.nppes_registry_error import (
    NppesRegistryReplayError,
    replay_error,
)
from public_evidence.nppes_registry_row_projection import (
    DEACTIVATION_DATE_HEADER as _DEACTIVATION_DATE_HEADER,
    ENTITY_TYPE_HEADER as _ENTITY_TYPE_HEADER,
    ENUMERATION_DATE_HEADER as _ENUMERATION_DATE_HEADER,
    LAST_UPDATE_DATE_HEADER as _LAST_UPDATE_DATE_HEADER,
    NPI_HEADER as _NPI_HEADER,
    PROJECTION_HEADERS as _PROJECTION_HEADERS,
    REACTIVATION_DATE_HEADER as _REACTIVATION_DATE_HEADER,
    REQUIRED_HEADERS,
    _project_nppes_registry_row,
)


NPPES_REGISTRY_PAYLOAD_CONTRACT = (
    "healthporta_nppes_registry_csv_row_payload_sha256_v1"
)
NPPES_REGISTRY_IDENTITY_CONTRACT = (
    "healthporta_nppes_public_artifact_row_hmac_sha256_v1"
)
NPPES_REGISTRY_TREE_CONTRACT = (
    "healthporta_nppes_registry_source_order_rfc6962_shape_sha256_v1"
)
NPPES_REGISTRY_MANIFEST_CONTRACT = "healthporta.nppes-registry-manifest.v1"

_HEADER_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_HEADER_V1\x00"
_PAYLOAD_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_PAYLOAD_V1\x00"
_IDENTITY_KEY_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_PUBLIC_HMAC_KEY_V1\x00"
_IDENTITY_MESSAGE_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_ROW_IDENTITY_V1\x00"
_LEAF_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_LEAF_V1\x00"
_MANIFEST_DOMAIN = b"HEALTHPORTA_NPPES_REGISTRY_MANIFEST_V1\x00"
_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_ARCHIVE_RE = re.compile(
    r"NPPES_Data_Dissemination_(?:"
    r"(?:January|February|March|April|May|June|July|August|September|October|"
    r"November|December)_[0-9]{4}|[0-9]{6}_[0-9]{6}_Weekly"
    r")_V2\.zip",
    flags=re.ASCII,
)
_PRIMARY_MEMBER_RE = re.compile(
    r"npidata_pfile_([0-9]{8})-([0-9]{8})\.csv", flags=re.ASCII
)
_MAX_HEADER_FIELDS = 1024
_MAX_HEADER_FIELD_BYTES = 1024
_MAX_PROJECTED_VALUE_BYTES = 1024

NPI_HEADER = _NPI_HEADER
ENTITY_TYPE_HEADER = _ENTITY_TYPE_HEADER
ENUMERATION_DATE_HEADER = _ENUMERATION_DATE_HEADER
LAST_UPDATE_DATE_HEADER = _LAST_UPDATE_DATE_HEADER
DEACTIVATION_DATE_HEADER = _DEACTIVATION_DATE_HEADER
REACTIVATION_DATE_HEADER = _REACTIVATION_DATE_HEADER

@dataclass(frozen=True, slots=True, repr=False)
class NppesRegistryHeader:
    """Precompiled positions for the bounded six-field NPPES projection."""

    fields: tuple[str, ...]
    projection_positions: tuple[int, ...]
    sha256: str

    def __repr__(self) -> str:
        return "<nppes-registry-header>"


def _canonical_json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, UnicodeError, ValueError):
        raise replay_error() from None


def _framed_sha256(domain: bytes, value: object) -> str:
    encoded = _canonical_json(value)
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise replay_error()
    return value


def _safe_archive_name(value: object) -> str:
    if type(value) is not str or _ARCHIVE_RE.fullmatch(value) is None:
        raise replay_error()
    return value


def _snapshot_at(member_name: object) -> str:
    if type(member_name) is not str or "/" in member_name or "\\" in member_name:
        raise replay_error()
    matched = _PRIMARY_MEMBER_RE.fullmatch(member_name)
    if matched is None:
        raise replay_error()
    try:
        first = datetime.strptime(matched.group(1), "%Y%m%d")
        last = datetime.strptime(matched.group(2), "%Y%m%d")
    except ValueError:
        raise replay_error() from None
    if last < first:
        raise replay_error()
    return last.strftime("%Y-%m-%dT00:00:00Z")


@dataclass(frozen=True, slots=True, repr=False)
class NppesArchiveIdentity:
    """Immutable byte and replay identity for one official NPPES ZIP."""

    source_url: str
    archive_name: str
    primary_member_name: str
    artifact_sha256: str
    artifact_byte_count: int
    snapshot_at: str
    rights_proof_sha256: str
    record_identity_contract_id: str

    def __repr__(self) -> str:
        return "<nppes-archive-identity>"


@dataclass(frozen=True, slots=True, repr=False)
class NppesRegistryRowObservation:
    """One exact source-row witness and its bounded v1 projection state."""

    source_row_ordinal: int
    npi: str
    entity_type_code: str | None
    provider_enumeration_date: str | None
    last_update_date: str | None
    npi_deactivation_date: str | None
    npi_reactivation_date: str | None
    payload_sha256: str
    record_hmac_sha256: str
    leaf_sha256: str
    npi_entity_type: str | None
    enumeration_state: Literal["active", "deactivated"]
    effective_start_at: str | None
    effective_end_at: str
    exclusion_reason: str | None

    def __repr__(self) -> str:
        return "<nppes-registry-row-observation>"


def build_nppes_archive_identity(
    *,
    source_url: object,
    archive_name: object,
    primary_member_name: object,
    artifact_sha256: object,
    artifact_byte_count: object,
    rights_proof_sha256: object,
    record_identity_contract_id: object = NPPES_REGISTRY_IDENTITY_CONTRACT,
) -> NppesArchiveIdentity:
    """Validate one allowlisted CMS artifact identity."""

    try:
        canonical_archive_name = _safe_archive_name(archive_name)
        if type(source_url) is not str:
            raise replay_error()
        expected_url = f"https://download.cms.gov/nppes/{canonical_archive_name}"
        parsed = urlsplit(source_url)
        if (
            source_url != expected_url
            or parsed.scheme != "https"
            or parsed.hostname != "download.cms.gov"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in (None, 443)
            or parsed.query
            or parsed.fragment
            or parsed.path != f"/nppes/{canonical_archive_name}"
        ):
            raise replay_error()
        if type(primary_member_name) is not str:
            raise replay_error()
        canonical_member = primary_member_name
        snapshot = _snapshot_at(canonical_member)
        if type(artifact_byte_count) is not int or not 1 <= artifact_byte_count < 2**63:
            raise replay_error()
        if (
            type(record_identity_contract_id) is not str
            or record_identity_contract_id != NPPES_REGISTRY_IDENTITY_CONTRACT
        ):
            raise replay_error()
        validated_identity = NppesArchiveIdentity(
            source_url=source_url,
            archive_name=canonical_archive_name,
            primary_member_name=canonical_member,
            artifact_sha256=_strict_sha256(artifact_sha256),
            artifact_byte_count=artifact_byte_count,
            snapshot_at=snapshot,
            rights_proof_sha256=_strict_sha256(rights_proof_sha256),
            record_identity_contract_id=NPPES_REGISTRY_IDENTITY_CONTRACT,
        )
    except Exception:
        normalized_error = replay_error()
    else:
        return validated_identity
    raise normalized_error


def validate_nppes_archive_identity(candidate: object) -> NppesArchiveIdentity:
    """Rebuild one exact archive identity and reject direct forged instances."""

    try:
        if type(candidate) is not NppesArchiveIdentity:
            raise replay_error()
        rebuilt = build_nppes_archive_identity(
            source_url=candidate.source_url,
            archive_name=candidate.archive_name,
            primary_member_name=candidate.primary_member_name,
            artifact_sha256=candidate.artifact_sha256,
            artifact_byte_count=candidate.artifact_byte_count,
            rights_proof_sha256=candidate.rights_proof_sha256,
            record_identity_contract_id=candidate.record_identity_contract_id,
        )
        if candidate != rebuilt:
            raise replay_error()
    except Exception:
        normalized_error = replay_error()
    else:
        return rebuilt
    raise normalized_error


def _validated_header(header: object) -> tuple[str, ...]:
    if type(header) not in {tuple, list} or not 1 <= len(header) <= _MAX_HEADER_FIELDS:
        raise replay_error()
    fields: list[str] = []
    for field_name in header:
        if type(field_name) is not str or not field_name:
            raise replay_error()
        try:
            encoded = field_name.encode("utf-8")
        except UnicodeError:
            raise replay_error() from None
        if len(encoded) > _MAX_HEADER_FIELD_BYTES:
            raise replay_error()
        fields.append(field_name)
    if len(set(fields)) != len(fields):
        raise replay_error()
    fixed_fields = tuple(fields)
    if not REQUIRED_HEADERS.issubset(fixed_fields):
        raise replay_error()
    return fixed_fields


def validate_nppes_header(header: object) -> tuple[str, ...]:
    """Return one exact, unique, bounded primary-file header vector."""

    try:
        fixed = _validated_header(header)
    except Exception:
        normalized_error = replay_error()
    else:
        return fixed
    raise normalized_error


def _header_sha256(fixed_header: tuple[str, ...]) -> str:
    return _framed_sha256(_HEADER_DOMAIN, list(fixed_header))


def compile_nppes_registry_header(header: object) -> NppesRegistryHeader:
    """Compile one exact header once for multi-million-row replay."""

    try:
        fixed = _validated_header(header)
        positions = tuple(fixed.index(field_name) for field_name in _PROJECTION_HEADERS)
        compiled = NppesRegistryHeader(
            fields=fixed,
            projection_positions=positions,
            sha256=_header_sha256(fixed),
        )
    except Exception:
        normalized_error = replay_error()
    else:
        return compiled
    raise normalized_error


def nppes_header_sha256(header: object) -> str:
    """Hash one exact primary-file header vector."""

    if type(header) is NppesRegistryHeader:
        return header.sha256
    return compile_nppes_registry_header(header).sha256


def _projected_row_values(
    header: NppesRegistryHeader, row_values: object
) -> tuple[str, ...]:
    if type(row_values) not in {tuple, list} or len(row_values) != len(header.fields):
        raise replay_error()
    selected_values: list[str] = []
    for position in header.projection_positions:
        field_value = row_values[position]
        if type(field_value) is not str:
            raise replay_error()
        try:
            encoded = field_value.encode("utf-8")
        except UnicodeError:
            raise replay_error() from None
        if len(encoded) > _MAX_PROJECTED_VALUE_BYTES:
            raise replay_error()
        selected_values.append(field_value)
    return tuple(selected_values)


def _record_hmac(
    identity: NppesArchiveIdentity,
    source_row_ordinal: int,
) -> str:
    artifact = bytes.fromhex(identity.artifact_sha256)
    public_key = hashlib.sha256(_IDENTITY_KEY_DOMAIN + artifact).digest()
    policy = identity.record_identity_contract_id.encode("ascii")
    member = identity.primary_member_name.encode("utf-8")
    message = bytearray(_IDENTITY_MESSAGE_DOMAIN)
    message.extend(len(policy).to_bytes(2, "big"))
    message.extend(policy)
    message.extend(artifact)
    message.extend(len(member).to_bytes(2, "big"))
    message.extend(member)
    message.extend(source_row_ordinal.to_bytes(8, "big"))
    return hmac.new(public_key, bytes(message), hashlib.sha256).hexdigest()


def _scan_compiled_nppes_registry_row(
    identity: NppesArchiveIdentity,
    header: NppesRegistryHeader,
    row_values: object,
    source_row_ordinal: object,
) -> NppesRegistryRowObservation:
    fixed_values = _projected_row_values(header, row_values)
    if (
        type(source_row_ordinal) is not int
        or not 1 <= source_row_ordinal <= 2**53 - 1
    ):
        raise replay_error()
    projection = _project_nppes_registry_row(
        identity.snapshot_at,
        fixed_values,
        NPPES_REGISTRY_PAYLOAD_CONTRACT,
    )
    payload_sha256 = _framed_sha256(
        _PAYLOAD_DOMAIN,
        dict(projection.payload_fields),
    )
    record_hmac = _record_hmac(identity, source_row_ordinal)
    leaf_sha256 = _framed_sha256(
        _LEAF_DOMAIN,
        {
            "identity_contract_id": identity.record_identity_contract_id,
            "payload_sha256": payload_sha256,
            "record_hmac_sha256": record_hmac,
            "record_kind": "nppes_registry_record",
            "source_row_ordinal": source_row_ordinal,
        },
    )
    return NppesRegistryRowObservation(
        source_row_ordinal=source_row_ordinal,
        npi=projection.npi,
        entity_type_code=projection.entity_type_code,
        provider_enumeration_date=projection.provider_enumeration_date,
        last_update_date=projection.last_update_date,
        npi_deactivation_date=projection.npi_deactivation_date,
        npi_reactivation_date=projection.npi_reactivation_date,
        payload_sha256=payload_sha256,
        record_hmac_sha256=record_hmac,
        leaf_sha256=leaf_sha256,
        npi_entity_type=projection.npi_entity_type,
        enumeration_state=projection.enumeration_state,
        effective_start_at=projection.effective_start_at,
        effective_end_at=identity.snapshot_at,
        exclusion_reason=projection.exclusion_reason,
    )


def scan_nppes_registry_row(
    identity: object,
    header: object,
    row_values: object,
    source_row_ordinal: object,
) -> NppesRegistryRowObservation:
    """Hash and classify one exact NPPES primary CSV data row."""

    try:
        fixed_identity = validate_nppes_archive_identity(identity)
        compiled_header = compile_nppes_registry_header(header)
        observation = _scan_compiled_nppes_registry_row(
            fixed_identity,
            compiled_header,
            row_values,
            source_row_ordinal,
        )
    except Exception:
        normalized_error = replay_error()
    else:
        return observation
    raise normalized_error


def nppes_manifest_sha256(payload: object) -> str:
    """Hash one already-normalized archive-manifest payload."""

    return _framed_sha256(_MANIFEST_DOMAIN, payload)
