# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fast, exact NPI-enumeration row encoding for a verified NPPES replay."""

from __future__ import annotations

from dataclasses import dataclass
import hmac

from public_evidence.evidence_record_policies import _fixed_authority_state
from public_evidence.evidence_record_primitives import (
    PUBLIC_EVIDENCE_RECORD_CONTRACT,
    PUBLIC_EVIDENCE_RECORD_REF_PREFIX,
    PUBLIC_EVIDENCE_SOURCE_RECORD_REF_PREFIX,
    EvidenceSourceRecordReference,
    _canonical_sha256,
    _derived_ref,
)
from public_evidence.nppes_registry_primitives import (
    NppesRegistryRowObservation,
    nppes_header_sha256,
    replay_error,
    validate_nppes_header,
)
from public_evidence.nppes_registry_replay_contract import (
    NppesRegistryArchiveManifest,
    NppesRegistryArchiveScanner,
    validate_nppes_registry_manifest,
)
from public_evidence.record_persistence_candidate_primitives import (
    SOURCE_LINK_ORDERING_CONTRACT,
    NpiEnumerationRow,
    PublicEvidenceRecordCommonRow,
    PublicEvidenceRecordSourceLinkRow,
)
from public_evidence.source_release_contract import PUBLIC_EVIDENCE_FOUNDATION_SCOPE


_RECORD_TYPE = "npi_enumeration"
_RELATIONSHIP_CLASS = "nppes_npi_enumeration"
_RECORD_KIND = "nppes_registry_record"
_PERSISTENCE_ROWS_SEAL = object()


@dataclass(frozen=True, slots=True, repr=False, init=False)
class NppesRegistryPersistenceRows:
    """One source witness and its optional exact frozen-v1 row family."""

    observation: NppesRegistryRowObservation
    source_record: EvidenceSourceRecordReference
    common_row: PublicEvidenceRecordCommonRow | None
    source_link_row: PublicEvidenceRecordSourceLinkRow | None
    typed_row: NpiEnumerationRow | None
    exclusion_reason: str | None
    _seal: object

    def __repr__(self) -> str:
        return "<nppes-registry-persistence-rows>"


def _new_persistence_rows(
    *,
    observation: NppesRegistryRowObservation,
    source_record: EvidenceSourceRecordReference,
    common_row: PublicEvidenceRecordCommonRow | None,
    source_link_row: PublicEvidenceRecordSourceLinkRow | None,
    typed_row: NpiEnumerationRow | None,
    exclusion_reason: str | None,
) -> NppesRegistryPersistenceRows:
    encoded = object.__new__(NppesRegistryPersistenceRows)
    for field_name, field_value in (
        ("observation", observation),
        ("source_record", source_record),
        ("common_row", common_row),
        ("source_link_row", source_link_row),
        ("typed_row", typed_row),
        ("exclusion_reason", exclusion_reason),
        ("_seal", _PERSISTENCE_ROWS_SEAL),
    ):
        object.__setattr__(encoded, field_name, field_value)
    return encoded


def validate_nppes_registry_persistence_rows(
    candidate: object,
) -> NppesRegistryPersistenceRows:
    """Require the exact row family emitted by this module's sealed encoder."""

    if (
        type(candidate) is not NppesRegistryPersistenceRows
        or candidate._seal is not _PERSISTENCE_ROWS_SEAL
    ):
        raise replay_error()
    return candidate


def _source_record(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
) -> EvidenceSourceRecordReference:
    payload = {
        "source_release_ref": manifest.release.source_release_ref,
        "record_kind": _RECORD_KIND,
        "identity_contract_id": manifest.identity.record_identity_contract_id,
        "record_hmac_sha256": observation.record_hmac_sha256,
        "payload_sha256": observation.payload_sha256,
    }
    return EvidenceSourceRecordReference(
        **payload,
        source_record_ref=_derived_ref(
            PUBLIC_EVIDENCE_SOURCE_RECORD_REF_PREFIX,
            "source_record",
            payload,
        ),
    )


def _record_payload(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
    source_record: EvidenceSourceRecordReference,
) -> dict[str, object]:
    return {
        "contract": PUBLIC_EVIDENCE_RECORD_CONTRACT,
        "foundation_scope": PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        "source_kind": manifest.release.source_kind,
        "source_release_ref": manifest.release.source_release_ref,
        "source_release_contract_sha256": manifest.release.contract_sha256,
        "source_records": [dict(source_record._asdict())],
        "observed_at": manifest.identity.snapshot_at,
        "effective_interval": {
            "start_at": observation.effective_start_at,
            "end_at": observation.effective_end_at,
        },
        "record_type": _RECORD_TYPE,
        "evidence": {
            "relationship_class": _RELATIONSHIP_CLASS,
            "npi": observation.npi,
            "npi_entity_type": observation.npi_entity_type,
            "enumeration_state": observation.enumeration_state,
        },
        "authority_state": dict(_fixed_authority_state()._asdict()),
    }


def _finished_row(row: object, purpose: str) -> object:
    payload = dict(row._asdict())
    payload.pop("row_sha256")
    return row._replace(
        row_sha256=_canonical_sha256(
            f"persistence_candidate_{purpose}",
            payload,
        )
    )


def _typed_row(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
    evidence_ref: str,
) -> NpiEnumerationRow:
    row = NpiEnumerationRow(
        evidence_ref=evidence_ref,
        source_release_ref=manifest.release.source_release_ref,
        source_release_contract_sha256=manifest.release.contract_sha256,
        source_kind=manifest.release.source_kind,
        record_type=_RECORD_TYPE,
        relationship_class=_RELATIONSHIP_CLASS,
        npi=observation.npi,
        npi_entity_type=observation.npi_entity_type,
        enumeration_state=observation.enumeration_state,
        row_sha256="",
    )
    return _finished_row(row, "typed_row")


def _source_link_row(
    manifest: NppesRegistryArchiveManifest,
    source_record: EvidenceSourceRecordReference,
    evidence_ref: str,
) -> PublicEvidenceRecordSourceLinkRow:
    row = PublicEvidenceRecordSourceLinkRow(
        evidence_ref=evidence_ref,
        source_release_ref=manifest.release.source_release_ref,
        source_release_contract_sha256=manifest.release.contract_sha256,
        source_kind=manifest.release.source_kind,
        source_record_ordinal=0,
        source_record_ref=source_record.source_record_ref,
        record_kind=_RECORD_KIND,
        row_sha256="",
    )
    return _finished_row(row, "source_link_row")


def _source_link_vector_sha256(
    source_link: PublicEvidenceRecordSourceLinkRow,
) -> str:
    return _canonical_sha256(
        "persistence_candidate_source_link_vector",
        {
            "ordering_contract_id": SOURCE_LINK_ORDERING_CONTRACT,
            "source_record_count": 1,
            "links": [
                {
                    "source_record_ordinal": 0,
                    "source_record_ref": source_link.source_record_ref,
                    "row_sha256": source_link.row_sha256,
                }
            ],
        },
    )


def _common_row(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
    evidence_ref: str,
    contract_sha256: str,
    source_link: PublicEvidenceRecordSourceLinkRow,
    typed_row: NpiEnumerationRow,
) -> PublicEvidenceRecordCommonRow:
    authority = _fixed_authority_state()
    common_row = PublicEvidenceRecordCommonRow(
        evidence_ref=evidence_ref,
        record_contract=PUBLIC_EVIDENCE_RECORD_CONTRACT,
        record_contract_sha256=contract_sha256,
        foundation_scope=PUBLIC_EVIDENCE_FOUNDATION_SCOPE,
        source_release_ref=manifest.release.source_release_ref,
        source_release_contract_sha256=manifest.release.contract_sha256,
        source_kind=manifest.release.source_kind,
        observed_at=manifest.identity.snapshot_at,
        effective_start_at=observation.effective_start_at,
        effective_end_at=observation.effective_end_at,
        record_type=_RECORD_TYPE,
        relationship_class=_RELATIONSHIP_CLASS,
        source_record_count=1,
        source_link_ordering_contract_id=SOURCE_LINK_ORDERING_CONTRACT,
        source_link_vector_sha256=_source_link_vector_sha256(source_link),
        typed_row_sha256=typed_row.row_sha256,
        authority_state_sha256=_canonical_sha256(
            "persistence_candidate_record_authority_state",
            dict(authority._asdict()),
        ),
        lifecycle_state=authority.lifecycle_state,
        positive_evidence_only=authority.positive_evidence_only,
        serving_authority=authority.serving_authority,
        current_pointer_authority=authority.current_pointer_authority,
        database_io_authority=authority.database_io_authority,
        publication_enabled=authority.publication_enabled,
        row_sha256="",
    )
    return _finished_row(common_row, "common_row")


def _projected_rows(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
    source_record: EvidenceSourceRecordReference,
) -> NppesRegistryPersistenceRows:
    record_payload = _record_payload(manifest, observation, source_record)
    evidence_ref = _derived_ref(
        PUBLIC_EVIDENCE_RECORD_REF_PREFIX,
        "evidence_record",
        record_payload,
    )
    contract_sha256 = _canonical_sha256(
        "evidence_record_contract",
        record_payload,
    )
    typed_row = _typed_row(manifest, observation, evidence_ref)
    source_link = _source_link_row(manifest, source_record, evidence_ref)
    common_row = _common_row(
        manifest,
        observation,
        evidence_ref,
        contract_sha256,
        source_link,
        typed_row,
    )
    return _new_persistence_rows(
        observation=observation,
        source_record=source_record,
        common_row=common_row,
        source_link_row=source_link,
        typed_row=typed_row,
        exclusion_reason=None,
    )


def _persistence_rows(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
) -> NppesRegistryPersistenceRows:
    source_record = _source_record(manifest, observation)
    if observation.exclusion_reason is None:
        return _projected_rows(manifest, observation, source_record)
    return _new_persistence_rows(
        observation=observation,
        source_record=source_record,
        common_row=None,
        source_link_row=None,
        typed_row=None,
        exclusion_reason=observation.exclusion_reason,
    )


class NppesRegistryCandidateEncoder:
    """Replay raw rows and emit parity-proved storage rows without deep rebuilds."""

    __slots__ = ("_finished", "_manifest", "_scanner")

    def __init__(self, manifest: object, header: object) -> None:
        try:
            fixed_manifest = validate_nppes_registry_manifest(manifest)
            fixed_header = validate_nppes_header(header)
            if not hmac.compare_digest(
                nppes_header_sha256(fixed_header),
                fixed_manifest.header_sha256,
            ):
                raise replay_error()
            scanner = NppesRegistryArchiveScanner(fixed_manifest.identity, fixed_header)
        except Exception:
            normalized_error = replay_error()
        else:
            self._manifest = fixed_manifest
            self._scanner = scanner
            self._finished = False
            return
        raise normalized_error

    def add(self, row_values: object) -> NppesRegistryPersistenceRows:
        """Replay and encode the next exact primary CSV row."""

        if self._finished:
            raise replay_error()
        try:
            encoded = _persistence_rows(self._manifest, self._scanner.add(row_values))
        except Exception:
            normalized_error = replay_error()
        else:
            return encoded
        raise normalized_error

    def finish(self) -> NppesRegistryArchiveManifest:
        """Seal the replay after proving the expected complete manifest."""

        if self._finished:
            raise replay_error()
        self._finished = True
        try:
            replayed = self._scanner.finish()
            if not hmac.compare_digest(
                replayed.manifest_sha256,
                self._manifest.manifest_sha256,
            ):
                raise replay_error()
        except Exception:
            normalized_error = replay_error()
        else:
            return replayed
        raise normalized_error


__all__ = (
    "NppesRegistryCandidateEncoder",
    "NppesRegistryPersistenceRows",
    "validate_nppes_registry_persistence_rows",
)
