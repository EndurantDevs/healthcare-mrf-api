# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pathless source binding for publication-disabled tax-identity evidence."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from process.ptg_parts import ptg2_tax_identity_shadow_admission as _admission
from process.ptg_parts.ptg2_shared_reuse import SharedPhysicalArtifactIdentity
from process.ptg_parts.ptg2_tax_identity_shadow_admission import (
    TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT,
    TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES,
    TAX_IDENTITY_SHADOW_MAX_ROWS,
    TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY,
    TaxIdentityShadowArtifactDescriptor,
    TaxIdentityShadowBundleDescriptor,
    TaxIdentityShadowStateCounts,
)


TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT = (
    "ptg2_tax_identity_shadow_source_binding_v1"
)

_BINDING_DIGEST_DOMAIN = b"PTG2_TAX_IDENTITY_SHADOW_SOURCE_BINDING_V1\x00"
_BINDING_INVALID = "ptg2_tax_identity_shadow_source_binding_invalid"
_HEX_RE = re.compile(r"[0-9a-f]+", flags=re.ASCII)
_SOURCE_TYPE_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}", flags=re.ASCII)
_SOURCE_SHARD_RE = re.compile(
    r"(?:file:[0-9]+|manifest:(?:[0-9]+|[0-9a-f]{16}|[0-9a-f]{32}|[0-9a-f]{64}))",
    flags=re.ASCII,
)
_PHYSICAL_IDENTITY_KINDS = frozenset(
    {"logical_json_sha256_v1", "raw_container_sha256_v1"}
)
_PATH_TYPE = type(Path())


class TaxIdentityShadowSourceBindingError(RuntimeError):
    """One redacted source-binding contract failure."""


def _fail() -> TaxIdentityShadowSourceBindingError:
    return TaxIdentityShadowSourceBindingError(_BINDING_INVALID)


def _strict_lower_hex(value: object, *, lengths: frozenset[int]) -> str:
    if (
        type(value) is not str
        or len(value) not in lengths
        or _HEX_RE.fullmatch(value) is None
    ):
        raise _fail()
    return value


def _strict_sha256(value: object) -> str:
    return _strict_lower_hex(value, lengths=frozenset({64}))


def _strict_run_coordinate(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _fail()
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise _fail() from None
    if (
        len(encoded) > 96
        or any(unicodedata.category(character) == "Cc" for character in value)
    ):
        raise _fail()
    return value


def _strict_shadow_state_counts(
    counts: object,
) -> TaxIdentityShadowStateCounts:
    if type(counts) is not TaxIdentityShadowStateCounts:
        raise _fail()
    try:
        detached = TaxIdentityShadowStateCounts(
            matched_ein=counts.matched_ein,
            matched_npi=counts.matched_npi,
            missing=counts.missing,
            malformed=counts.malformed,
            unsupported_type=counts.unsupported_type,
        )
    except Exception:
        raise _fail() from None
    return detached


def _strict_artifact_primitives(
    artifact: object,
) -> tuple[TaxIdentityShadowArtifactDescriptor, TaxIdentityShadowStateCounts]:
    if type(artifact) is not TaxIdentityShadowArtifactDescriptor:
        raise _fail()
    try:
        counts = _strict_shadow_state_counts(artifact.state_counts)
        typed_scalars = (
            (artifact.sidecar_version, int),
            (artifact.artifact_format, str),
            (artifact.byte_count, int),
            (artifact.row_count, int),
            (artifact.provider_group_count, int),
            (artifact.record_bytes, int),
            (artifact.token_policy_id, str),
            (artifact.sha256, str),
            (artifact.normalization_contract, str),
            (artifact.hmac_contract, str),
        )
        optional_contracts = (
            artifact.token_message_contract,
            artifact.tin_id_128_contract,
            artifact.full_hmac_authority_contract,
        )
        if (
            type(artifact.path) is not _PATH_TYPE
            or not artifact.path.is_absolute()
            or any(type(raw) is not expected for raw, expected in typed_scalars)
            or any(type(raw) not in (str, type(None)) for raw in optional_contracts)
        ):
            raise _fail()
    except Exception:
        raise _fail() from None
    return artifact, counts


def _artifact_scanner_mapping(
    artifact: object,
    *,
    version: Literal[1, 2],
) -> dict[str, object]:
    validated, counts = _strict_artifact_primitives(artifact)
    descriptor_by_field: dict[str, object] = {
        "path": str(validated.path),
        "bytes": validated.byte_count,
        "row_count": validated.row_count,
        "provider_group_count": validated.provider_group_count,
        "matched_ein_count": counts.matched_ein,
        "missing_count": counts.missing,
        "malformed_count": counts.malformed,
        "unsupported_type_count": counts.unsupported_type,
        "format": validated.artifact_format,
        "version": validated.sidecar_version,
        "record_bytes": validated.record_bytes,
        "token_policy_id": validated.token_policy_id,
        "normalization_contract": validated.normalization_contract,
        "hmac_contract": validated.hmac_contract,
        "sha256": validated.sha256,
        "final": True,
    }
    if version == 2:
        descriptor_by_field.update(
            matched_npi_count=counts.matched_npi,
            token_message_contract=validated.token_message_contract,
            tin_id_128_contract=validated.tin_id_128_contract,
            full_hmac_authority_contract=validated.full_hmac_authority_contract,
        )
    return descriptor_by_field


def _normalized_artifact(
    artifact: object,
    *,
    version: Literal[1, 2],
) -> TaxIdentityShadowArtifactDescriptor:
    """Reapply canonical admission metadata without file use.

    The private admission helpers called by this module remain the sole owners of
    descriptor normalization, pair feasibility, and bundle digest semantics.
    """

    try:
        normalized = _admission._normalize_descriptor(
            _artifact_scanner_mapping(artifact, version=version),
            version=version,
            max_artifact_bytes=TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES,
            max_row_count=TAX_IDENTITY_SHADOW_MAX_ROWS,
        )
    except Exception:
        raise _fail() from None
    if normalized != artifact:
        raise _fail()
    return normalized


def _validated_shadow_bundle_binding(
    shadow_bundle: object,
) -> str:
    if type(shadow_bundle) is not TaxIdentityShadowBundleDescriptor:
        raise _fail()
    try:
        is_metadata_valid = (
            type(shadow_bundle.contract) is str
            and shadow_bundle.contract == TAX_IDENTITY_SHADOW_BUNDLE_CONTRACT
            and type(shadow_bundle.shadow_state) is str
            and shadow_bundle.shadow_state == "SHADOW"
            and type(shadow_bundle.projection_authority) is str
            and shadow_bundle.projection_authority
            == TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY
            and shadow_bundle.publication_enabled is False
        )
        supplied_binding = _strict_sha256(shadow_bundle.binding_sha256)
        v1 = _normalized_artifact(shadow_bundle.v1, version=1)
        v2 = _normalized_artifact(shadow_bundle.v2, version=2)
        if not is_metadata_valid or not _admission._is_pair_consistent(v1, v2):
            raise _fail()
        canonical_binding = _admission._shadow_binding_sha256(v1, v2)
    except Exception:
        raise _fail() from None
    if not hmac.compare_digest(supplied_binding, canonical_binding):
        raise _fail()
    return canonical_binding


@dataclass(frozen=True, slots=True, repr=False)
class TaxIdentityShadowSourceCoordinates:
    """Pathless immutable coordinates for one admitted scanner artifact pair."""

    source_type: str
    physical_identity_kind: str
    physical_identity_sha256: str
    source_identity_hash: str
    source_file_version_id: str
    raw_container_sha256: str
    logical_json_sha256: str | None
    logical_hash_deferred: bool
    source_shard_id: str
    source_run_contract_sha256: str
    import_run_id: str
    snapshot_id: str

    def __post_init__(self) -> None:
        if (
            type(self.source_type) is not str
            or _SOURCE_TYPE_RE.fullmatch(self.source_type) is None
            or type(self.physical_identity_kind) is not str
            or self.physical_identity_kind not in _PHYSICAL_IDENTITY_KINDS
            or type(self.logical_hash_deferred) is not bool
            or type(self.source_shard_id) is not str
            or _SOURCE_SHARD_RE.fullmatch(self.source_shard_id) is None
            or len(self.source_shard_id.encode("ascii")) > 96
        ):
            raise _fail()
        physical_sha256 = _strict_sha256(self.physical_identity_sha256)
        _strict_lower_hex(
            self.source_identity_hash,
            lengths=frozenset({16, 32, 64}),
        )
        _strict_lower_hex(
            self.source_file_version_id,
            lengths=frozenset({16, 32}),
        )
        raw_sha256 = _strict_sha256(self.raw_container_sha256)
        _strict_sha256(self.source_run_contract_sha256)
        _strict_run_coordinate(self.import_run_id)
        _strict_run_coordinate(self.snapshot_id)
        if self.logical_hash_deferred:
            if (
                self.logical_json_sha256 is not None
                or self.physical_identity_kind != "raw_container_sha256_v1"
                or not hmac.compare_digest(physical_sha256, raw_sha256)
            ):
                raise _fail()
            return
        logical_sha256 = _strict_sha256(self.logical_json_sha256)
        if (
            self.physical_identity_kind != "logical_json_sha256_v1"
            or not hmac.compare_digest(physical_sha256, logical_sha256)
        ):
            raise _fail()

    def __repr__(self) -> str:
        return (
            "<TaxIdentityShadowSourceCoordinates source=<redacted> "
            "shard=<redacted> run=<redacted>>"
        )


def _detached_source_coordinates(
    coordinates: object,
) -> TaxIdentityShadowSourceCoordinates:
    if type(coordinates) is not TaxIdentityShadowSourceCoordinates:
        raise _fail()
    try:
        return TaxIdentityShadowSourceCoordinates(
            source_type=coordinates.source_type,
            physical_identity_kind=coordinates.physical_identity_kind,
            physical_identity_sha256=coordinates.physical_identity_sha256,
            source_identity_hash=coordinates.source_identity_hash,
            source_file_version_id=coordinates.source_file_version_id,
            raw_container_sha256=coordinates.raw_container_sha256,
            logical_json_sha256=coordinates.logical_json_sha256,
            logical_hash_deferred=coordinates.logical_hash_deferred,
            source_shard_id=coordinates.source_shard_id,
            source_run_contract_sha256=coordinates.source_run_contract_sha256,
            import_run_id=coordinates.import_run_id,
            snapshot_id=coordinates.snapshot_id,
        )
    except Exception:
        raise _fail() from None


@dataclass(frozen=True, slots=True, repr=False)
class TaxIdentityShadowSourceBindingInput:
    """Explicit source evidence used to derive one pathless coordinate set."""

    physical_identity: SharedPhysicalArtifactIdentity
    source_identity_hash: str
    source_file_version_id: str
    raw_container_sha256: str
    logical_json_sha256: str | None
    logical_hash_deferred: bool
    source_shard_id: str
    source_run_contract_sha256: str
    import_run_id: str
    snapshot_id: str

    def __post_init__(self) -> None:
        self.coordinates

    @property
    def coordinates(self) -> TaxIdentityShadowSourceCoordinates:
        """Return freshly validated, flattened, pathless source coordinates."""

        try:
            if type(self.physical_identity) is not SharedPhysicalArtifactIdentity:
                raise _fail()
            return TaxIdentityShadowSourceCoordinates(
                source_type=self.physical_identity.source_type,
                physical_identity_kind=self.physical_identity.identity_kind,
                physical_identity_sha256=self.physical_identity.identity_sha256,
                source_identity_hash=self.source_identity_hash,
                source_file_version_id=self.source_file_version_id,
                raw_container_sha256=self.raw_container_sha256,
                logical_json_sha256=self.logical_json_sha256,
                logical_hash_deferred=self.logical_hash_deferred,
                source_shard_id=self.source_shard_id,
                source_run_contract_sha256=self.source_run_contract_sha256,
                import_run_id=self.import_run_id,
                snapshot_id=self.snapshot_id,
            )
        except Exception:
            raise _fail() from None

    def __repr__(self) -> str:
        return (
            "<TaxIdentityShadowSourceBindingInput source=<redacted> "
            "shard=<redacted> run=<redacted>>"
        )


def _binding_payload(
    shadow_bundle_binding_sha256: str,
    coordinates: TaxIdentityShadowSourceCoordinates,
) -> dict[str, object]:
    return {
        "contract": TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "shadow_state": "SHADOW",
        "projection_authority": TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY,
        "publication_enabled": False,
        "shadow_bundle_binding_sha256": shadow_bundle_binding_sha256,
        "source_artifact": {
            "source_type": coordinates.source_type,
            "identity_kind": coordinates.physical_identity_kind,
            "identity_sha256": coordinates.physical_identity_sha256,
        },
        "retained_source": {
            "source_identity_hash": coordinates.source_identity_hash,
            "source_file_version_id": coordinates.source_file_version_id,
            "raw_container_sha256": coordinates.raw_container_sha256,
            "logical_json_sha256": coordinates.logical_json_sha256,
            "logical_hash_deferred": coordinates.logical_hash_deferred,
        },
        "shard": {
            "source_shard_id": coordinates.source_shard_id,
            "source_run_contract_sha256": coordinates.source_run_contract_sha256,
        },
        "run": {
            "import_run_id": coordinates.import_run_id,
            "snapshot_id": coordinates.snapshot_id,
        },
    }


def _source_binding_sha256(
    shadow_bundle_binding_sha256: str,
    coordinates: TaxIdentityShadowSourceCoordinates,
) -> str:
    encoded = json.dumps(
        _binding_payload(shadow_bundle_binding_sha256, coordinates),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    digest = hashlib.sha256()
    digest.update(_BINDING_DIGEST_DOMAIN)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class BoundTaxIdentityShadowBundleDescriptor:
    """Pathless SHADOW metadata binding that cannot authorize publication.

    The source binding performs no file authentication. A later byte consumer
    must securely reopen and reauthenticate the original admitted artifacts,
    compare both binding digests, and verify the bound run, snapshot, and source
    against the authoritative source catalog in one pinned generation.
    """

    shadow_bundle_binding_sha256: str
    coordinates: TaxIdentityShadowSourceCoordinates
    binding_sha256: str
    contract: str = field(
        default=TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        init=False,
    )
    shadow_state: Literal["SHADOW"] = field(default="SHADOW", init=False)
    projection_authority: Literal["v1_only"] = field(
        default=TAX_IDENTITY_SHADOW_PROJECTION_AUTHORITY,
        init=False,
    )
    publication_enabled: Literal[False] = field(default=False, init=False)

    def __post_init__(self) -> None:
        bundle_binding = _strict_sha256(self.shadow_bundle_binding_sha256)
        coordinates = _detached_source_coordinates(self.coordinates)
        binding = _strict_sha256(self.binding_sha256)
        expected_binding = _source_binding_sha256(bundle_binding, coordinates)
        if not hmac.compare_digest(binding, expected_binding):
            raise _fail()
        object.__setattr__(self, "coordinates", coordinates)

    def __repr__(self) -> str:
        return (
            "<BoundTaxIdentityShadowBundleDescriptor state=SHADOW "
            "projection_authority=v1_only publication_enabled=False "
            "binding=<redacted>>"
        )


def bind_tax_identity_shadow_source(
    *,
    shadow_bundle: TaxIdentityShadowBundleDescriptor,
    source: TaxIdentityShadowSourceBindingInput,
) -> BoundTaxIdentityShadowBundleDescriptor:
    """Bind admitted metadata to source coordinates without byte or catalog use.

    This result is not source-catalog authority. A later consumer must verify
    its coordinates against the source catalog in the same pinned generation.
    """

    if type(source) is not TaxIdentityShadowSourceBindingInput:
        raise _fail()
    bundle_binding = _validated_shadow_bundle_binding(shadow_bundle)
    coordinates = source.coordinates
    return BoundTaxIdentityShadowBundleDescriptor(
        shadow_bundle_binding_sha256=bundle_binding,
        coordinates=coordinates,
        binding_sha256=_source_binding_sha256(bundle_binding, coordinates),
    )


__all__ = [
    "TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT",
    "BoundTaxIdentityShadowBundleDescriptor",
    "TaxIdentityShadowSourceBindingInput",
    "TaxIdentityShadowSourceBindingError",
    "TaxIdentityShadowSourceCoordinates",
    "bind_tax_identity_shadow_source",
]
