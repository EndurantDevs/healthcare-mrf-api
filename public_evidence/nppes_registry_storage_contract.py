# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable member and release-admission rows for one NPPES replay."""

from __future__ import annotations

from dataclasses import dataclass
import hmac
import re
from typing import Literal, NamedTuple

from public_evidence.evidence_record_primitives import _canonical_sha256, _derived_ref
from public_evidence.nppes_registry_candidate_encoder import (
    NppesRegistryCandidateEncoder,
    NppesRegistryPersistenceRows,
    validate_nppes_registry_persistence_rows,
)
from public_evidence.nppes_registry_primitives import (
    NPPES_REGISTRY_IDENTITY_CONTRACT,
    NPPES_REGISTRY_MANIFEST_CONTRACT,
    NPPES_REGISTRY_PAYLOAD_CONTRACT,
    NPPES_REGISTRY_TREE_CONTRACT,
    replay_error,
)
from public_evidence.nppes_registry_replay_contract import (
    NppesRegistryArchiveManifest,
    validate_nppes_registry_manifest,
)


NPPES_REGISTRY_MEMBER_CONTRACT = "healthporta.nppes-registry-member.v1"
NPPES_REGISTRY_ADMISSION_CONTRACT = "healthporta.nppes-registry-admission.v1"
NPPES_REGISTRY_ADMISSION_REF_PREFIX = "penpa1_"

_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_EXCLUSION_REASONS = (
    "effective_start_not_disclosed",
    "entity_type_not_disclosed",
)


@dataclass(frozen=True, slots=True, repr=False)
class NppesRegistryArchiveObservation:
    """Acquisition and ZIP-layout evidence that does not alter release identity."""

    listing_sha256: str
    zip_member_count: int
    zip_member_census_sha256: str

    def __repr__(self) -> str:
        return "<nppes-registry-archive-observation>"


class NppesRegistryMemberRow(NamedTuple):
    """One complete primary-member source occurrence and v1 disposition."""

    contract: str
    admission_ref: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    source_row_ordinal: int
    npi: str
    entity_type_code: str | None
    provider_enumeration_date: str | None
    last_update_date: str | None
    npi_deactivation_date: str | None
    npi_reactivation_date: str | None
    source_record_ref: str
    record_kind: str
    identity_contract_id: str
    record_hmac_sha256: str
    payload_sha256: str
    leaf_sha256: str
    projection_state: Literal["projected_v1", "excluded_v1"]
    exclusion_reason: str | None
    evidence_ref: str | None
    row_sha256: str

    def __repr__(self) -> str:
        return "<nppes-registry-member-row>"

    __str__ = __repr__


class NppesRegistryAdmissionRow(NamedTuple):
    """One terminal, publication-disabled release-admission receipt."""

    admission_ref: str
    contract: str
    contract_sha256: str
    source_release_ref: str
    source_release_contract_sha256: str
    source_kind: str
    source_url: str
    archive_name: str
    primary_member_name: str
    artifact_sha256: str
    artifact_byte_count: int
    zip_member_count: int
    zip_member_census_sha256: str
    header_sha256: str
    payload_contract_id: str
    record_identity_contract_id: str
    tree_contract_id: str
    manifest_contract: str
    manifest_sha256: str
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    effective_start_not_disclosed_count: int
    entity_type_not_disclosed_count: int
    evidence_root_sha256: str
    minimum_effective_start_at: str
    snapshot_at: str
    rights_proof_sha256: str
    admission_state: Literal["verified_complete_disabled"]
    serving_authority: Literal["none"]
    publication_enabled: Literal[False]

    def __repr__(self) -> str:
        return "<nppes-registry-admission-row>"

    __str__ = __repr__


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise replay_error()
    return value


def _archive_observation(value: object) -> NppesRegistryArchiveObservation:
    if type(value) is not NppesRegistryArchiveObservation:
        raise replay_error()
    if type(value.zip_member_count) is not int or not 1 <= value.zip_member_count <= 4096:
        raise replay_error()
    return NppesRegistryArchiveObservation(
        listing_sha256=_strict_sha256(value.listing_sha256),
        zip_member_count=value.zip_member_count,
        zip_member_census_sha256=_strict_sha256(value.zip_member_census_sha256),
    )


def _member_payload(row: NppesRegistryMemberRow) -> dict[str, object]:
    payload = dict(row._asdict())
    payload.pop("row_sha256")
    return payload


def _build_member_row(
    fixed_manifest: NppesRegistryArchiveManifest,
    admission_ref: str,
    persistence_rows: object,
) -> NppesRegistryMemberRow:
    """Bind one sealed persistence family to its exact source member row."""

    try:
        (
            persistence_rows,
            observation,
            source_record,
            is_projected,
            evidence_ref,
        ) = _validated_member_components(
            fixed_manifest,
            persistence_rows
        )
        release = fixed_manifest.release
        projection_state = "projected_v1" if is_projected else "excluded_v1"
        member_row = NppesRegistryMemberRow(
            contract=NPPES_REGISTRY_MEMBER_CONTRACT,
            admission_ref=admission_ref,
            source_release_ref=release.source_release_ref,
            source_release_contract_sha256=release.contract_sha256,
            source_kind=release.source_kind,
            source_row_ordinal=observation.source_row_ordinal,
            npi=observation.npi,
            entity_type_code=observation.entity_type_code,
            provider_enumeration_date=observation.provider_enumeration_date,
            last_update_date=observation.last_update_date,
            npi_deactivation_date=observation.npi_deactivation_date,
            npi_reactivation_date=observation.npi_reactivation_date,
            source_record_ref=source_record.source_record_ref,
            record_kind=source_record.record_kind,
            identity_contract_id=source_record.identity_contract_id,
            record_hmac_sha256=source_record.record_hmac_sha256,
            payload_sha256=source_record.payload_sha256,
            leaf_sha256=observation.leaf_sha256,
            projection_state=projection_state,
            exclusion_reason=persistence_rows.exclusion_reason,
            evidence_ref=evidence_ref,
            row_sha256="",
        )
        finished_member = member_row._replace(
            row_sha256=_canonical_sha256(
                "nppes_registry_member_row",
                _member_payload(member_row),
            )
        )
    except Exception:
        normalized_error = replay_error()
    else:
        return finished_member
    raise normalized_error


def _validated_member_components(
    fixed_manifest: NppesRegistryArchiveManifest,
    persistence_rows: object,
) -> tuple[NppesRegistryPersistenceRows, object, object, bool, str | None]:
    persistence_rows = validate_nppes_registry_persistence_rows(persistence_rows)
    observation = persistence_rows.observation
    source_record = persistence_rows.source_record
    release = fixed_manifest.release
    is_projected = persistence_rows.exclusion_reason is None
    typed_components = (
        persistence_rows.common_row,
        persistence_rows.source_link_row,
        persistence_rows.typed_row,
    )
    evidence_ref = (
        persistence_rows.common_row.evidence_ref if is_projected else None
    )
    if (
        source_record.source_release_ref != release.source_release_ref
        or source_record.record_kind != "nppes_registry_record"
        or source_record.identity_contract_id
        != fixed_manifest.identity.record_identity_contract_id
        or not hmac.compare_digest(
            source_record.record_hmac_sha256,
            observation.record_hmac_sha256,
        )
        or not hmac.compare_digest(
            source_record.payload_sha256,
            observation.payload_sha256,
        )
        or persistence_rows.exclusion_reason != observation.exclusion_reason
        or (
            is_projected
            and any(component is None for component in typed_components)
        )
        or (
            not is_projected
            and any(component is not None for component in typed_components)
        )
        or (
            not is_projected
            and persistence_rows.exclusion_reason not in _EXCLUSION_REASONS
        )
    ):
        raise replay_error()
    return persistence_rows, observation, source_record, is_projected, evidence_ref


class NppesRegistryMemberEncoder:
    """Replay raw rows and emit paired persistence/member rows together."""

    __slots__ = ("_admission_ref", "_encoder", "_finished", "_manifest")

    def __init__(
        self,
        manifest: object,
        header: object,
        archive_observation: object,
    ) -> None:
        try:
            fixed_manifest = validate_nppes_registry_manifest(manifest)
            encoder = NppesRegistryCandidateEncoder(fixed_manifest, header)
            admission = build_nppes_registry_admission_row(
                fixed_manifest,
                archive_observation,
            )
        except Exception:
            normalized_error = replay_error()
        else:
            self._manifest = fixed_manifest
            self._encoder = encoder
            self._admission_ref = admission.admission_ref
            self._finished = False
            return
        raise normalized_error

    def encode(
        self,
        row_values: object,
    ) -> tuple[NppesRegistryPersistenceRows, NppesRegistryMemberRow]:
        """Replay one raw row once and derive both inseparable storage shapes."""

        if self._finished:
            raise replay_error()
        try:
            persistence_rows = self._encoder.add(row_values)
            member = _build_member_row(
                self._manifest,
                self._admission_ref,
                persistence_rows,
            )
        except Exception:
            normalized_error = replay_error()
        else:
            return persistence_rows, member
        raise normalized_error

    def finish(self) -> NppesRegistryArchiveManifest:
        """Require the paired encoder to reproduce the complete manifest."""

        if self._finished:
            raise replay_error()
        self._finished = True
        return self._encoder.finish()


def _exclusion_count(
    manifest: NppesRegistryArchiveManifest,
    reason: str,
) -> int:
    return dict(manifest.exclusion_counts).get(reason, 0)


def _admission_payload(row: NppesRegistryAdmissionRow) -> dict[str, object]:
    payload = dict(row._asdict())
    payload.pop("admission_ref")
    payload.pop("contract_sha256")
    return payload


def _initial_admission_row(
    fixed_manifest: NppesRegistryArchiveManifest,
    observed: NppesRegistryArchiveObservation,
) -> NppesRegistryAdmissionRow:
    identity = fixed_manifest.identity
    release = fixed_manifest.release
    return NppesRegistryAdmissionRow(
        admission_ref="",
        contract=NPPES_REGISTRY_ADMISSION_CONTRACT,
        contract_sha256="",
        source_release_ref=release.source_release_ref,
        source_release_contract_sha256=release.contract_sha256,
        source_kind=release.source_kind,
        source_url=identity.source_url,
        archive_name=identity.archive_name,
        primary_member_name=identity.primary_member_name,
        artifact_sha256=identity.artifact_sha256,
        artifact_byte_count=identity.artifact_byte_count,
        zip_member_count=observed.zip_member_count,
        zip_member_census_sha256=observed.zip_member_census_sha256,
        header_sha256=fixed_manifest.header_sha256,
        payload_contract_id=NPPES_REGISTRY_PAYLOAD_CONTRACT,
        record_identity_contract_id=NPPES_REGISTRY_IDENTITY_CONTRACT,
        tree_contract_id=NPPES_REGISTRY_TREE_CONTRACT,
        manifest_contract=NPPES_REGISTRY_MANIFEST_CONTRACT,
        manifest_sha256=fixed_manifest.manifest_sha256,
        source_record_count=fixed_manifest.source_record_count,
        projected_record_count=fixed_manifest.projected_record_count,
        excluded_record_count=fixed_manifest.excluded_record_count,
        effective_start_not_disclosed_count=_exclusion_count(
            fixed_manifest,
            "effective_start_not_disclosed",
        ),
        entity_type_not_disclosed_count=_exclusion_count(
            fixed_manifest,
            "entity_type_not_disclosed",
        ),
        evidence_root_sha256=fixed_manifest.evidence_root_sha256,
        minimum_effective_start_at=fixed_manifest.minimum_effective_start_at,
        snapshot_at=identity.snapshot_at,
        rights_proof_sha256=identity.rights_proof_sha256,
        admission_state="verified_complete_disabled",
        serving_authority="none",
        publication_enabled=False,
    )


def build_nppes_registry_admission_row(
    manifest: object,
    archive_observation: object,
) -> NppesRegistryAdmissionRow:
    """Build the sole complete, disabled admission claim for one release."""

    try:
        fixed_manifest = validate_nppes_registry_manifest(manifest)
        observed = _archive_observation(archive_observation)
        initial = _initial_admission_row(fixed_manifest, observed)
        admission_payload = _admission_payload(initial)
        finished = initial._replace(
            admission_ref=_derived_ref(
                NPPES_REGISTRY_ADMISSION_REF_PREFIX,
                "nppes_registry_admission",
                admission_payload,
            ),
            contract_sha256=_canonical_sha256(
                "nppes_registry_admission_contract",
                admission_payload,
            ),
        )
    except Exception:
        normalized_error = replay_error()
    else:
        return finished
    raise normalized_error


__all__ = (
    "NPPES_REGISTRY_ADMISSION_CONTRACT",
    "NPPES_REGISTRY_MEMBER_CONTRACT",
    "NppesRegistryAdmissionRow",
    "NppesRegistryArchiveObservation",
    "NppesRegistryMemberRow",
    "NppesRegistryMemberEncoder",
    "build_nppes_registry_admission_row",
)
