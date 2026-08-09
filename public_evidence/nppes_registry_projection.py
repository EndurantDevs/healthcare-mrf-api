# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete second-pass projection for one verified NPPES registry archive."""

from __future__ import annotations

from dataclasses import dataclass
import hmac

from public_evidence.evidence_record_contract import build_public_evidence_record
from public_evidence.evidence_record_primitives import (
    EvidenceSourceRecordReference,
    build_evidence_source_record_reference,
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
from public_evidence.record_persistence_candidate_contract import (
    build_public_evidence_record_persistence_candidate,
    validate_public_evidence_record_persistence_candidate,
)
from public_evidence.record_persistence_candidate_primitives import (
    PublicEvidenceRecordPersistenceCandidate,
)
from public_evidence.source_release_primitives import CanonicalUtcInterval


@dataclass(frozen=True, slots=True, repr=False)
class NppesRegistryPersistenceProjection:
    """One source record plus its optional frozen v1 enumeration candidate."""

    source_record: EvidenceSourceRecordReference
    candidate: PublicEvidenceRecordPersistenceCandidate | None
    exclusion_reason: str | None

    def __repr__(self) -> str:
        return "<nppes-registry-persistence-projection>"


def _build_projection(
    manifest: NppesRegistryArchiveManifest,
    observation: NppesRegistryRowObservation,
) -> NppesRegistryPersistenceProjection:
    source_record = build_evidence_source_record_reference(
        manifest.release,
        {
            "record_kind": "nppes_registry_record",
            "identity_contract_id": manifest.identity.record_identity_contract_id,
            "record_hmac_sha256": observation.record_hmac_sha256,
            "payload_sha256": observation.payload_sha256,
        },
    )
    persistence_candidate = None
    if observation.exclusion_reason is None:
        normalized_record = build_public_evidence_record(
            manifest.release,
            {
                "record_type": "npi_enumeration",
                "source_records": (source_record,),
                "observed_at": manifest.identity.snapshot_at,
                "effective_interval": CanonicalUtcInterval(
                    observation.effective_start_at,
                    observation.effective_end_at,
                ),
                "relationship_class": "nppes_npi_enumeration",
                "npi": observation.npi,
                "npi_entity_type": observation.npi_entity_type,
                "enumeration_state": observation.enumeration_state,
            },
        )
        persistence_candidate = validate_public_evidence_record_persistence_candidate(
            build_public_evidence_record_persistence_candidate(normalized_record)
        )
    return NppesRegistryPersistenceProjection(
        source_record=source_record,
        candidate=persistence_candidate,
        exclusion_reason=observation.exclusion_reason,
    )


class NppesRegistryPersistenceProjector:
    """Re-run every raw row before exposing persistence projections."""

    __slots__ = ("_finished", "_manifest", "_scanner")

    def __init__(self, manifest: object, header: object) -> None:
        try:
            fixed_manifest = validate_nppes_registry_manifest(manifest)
            fixed_header = validate_nppes_header(header)
            if not hmac.compare_digest(
                nppes_header_sha256(fixed_header), fixed_manifest.header_sha256
            ):
                raise replay_error()
            scanner = NppesRegistryArchiveScanner(
                fixed_manifest.identity,
                fixed_header,
            )
        except Exception:
            normalized_error = replay_error()
        else:
            self._manifest = fixed_manifest
            self._scanner = scanner
            self._finished = False
            return
        raise normalized_error

    def add(self, row_values: object) -> NppesRegistryPersistenceProjection:
        """Rebuild one row under the archive identity and advance the replay."""

        if self._finished:
            raise replay_error()
        try:
            observation = self._scanner.add(row_values)
            projection = _build_projection(self._manifest, observation)
        except Exception:
            normalized_error = replay_error()
        else:
            return projection
        raise normalized_error

    def finish(self) -> NppesRegistryArchiveManifest:
        """Require the complete second pass to reproduce the exact manifest."""

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
    "NppesRegistryPersistenceProjection",
    "NppesRegistryPersistenceProjector",
)
