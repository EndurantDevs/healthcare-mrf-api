# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic replay contract for retained NPPES registry archives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hmac
import re
from public_evidence.nppes_registry_primitives import (
    NPPES_REGISTRY_MANIFEST_CONTRACT,
    NppesArchiveIdentity,
    NppesRegistryReplayError,
    NppesRegistryRowObservation,
    _scan_compiled_nppes_registry_row,
    compile_nppes_registry_header,
    nppes_manifest_sha256,
    replay_error,
    validate_nppes_archive_identity,
)
from public_evidence.nppes_registry_merkle import NppesEvidenceRootAccumulator
from public_evidence.source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    build_public_evidence_source_release,
    validate_public_evidence_source_release,
)
from public_evidence.source_release_policies import SOURCE_POLICIES
from public_evidence.source_release_primitives import (
    CanonicalUtcInterval,
    ImmutablePublicSourceIdentity,
    PublicEvidenceCompletenessAttestation,
    derive_public_evidence_identity_ref,
)


@dataclass(frozen=True, slots=True, repr=False)
class NppesRegistryArchiveManifest:
    """One exact replay result for every primary row in one retained ZIP."""

    contract: str
    identity: NppesArchiveIdentity
    header_sha256: str
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    exclusion_counts: tuple[tuple[str, int], ...]
    evidence_root_sha256: str
    minimum_effective_start_at: str
    release: PublicEvidenceSourceReleaseDescriptor
    manifest_sha256: str

    def __repr__(self) -> str:
        return "<nppes-registry-archive-manifest>"


@dataclass(frozen=True, slots=True)
class _ManifestClaims:
    header_sha256: str
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    exclusion_counts: tuple[tuple[str, int], ...]
    evidence_root_sha256: str
    minimum_effective_start_at: str


def _release_input(
    identity: NppesArchiveIdentity,
    *,
    source_record_count: int,
    evidence_root_sha256: str,
    minimum_effective_start_at: str,
) -> dict[str, object]:
    policy = SOURCE_POLICIES["nppes_entity_address"]
    artifact = ImmutablePublicSourceIdentity(
        identity_kind=policy.identity_kind,
        content_identity_kind="raw_container_sha256_v1",
        identity_ref=derive_public_evidence_identity_ref(
            policy.identity_kind,
            "raw_container_sha256_v1",
            identity.artifact_sha256,
        ),
        content_sha256=identity.artifact_sha256,
    )
    attestation = PublicEvidenceCompletenessAttestation(
        mode=policy.attestation_mode,
        evidence_contract_id=policy.evidence_contract_id,
        count_unit=policy.count_unit,
        subject_sha256=identity.artifact_sha256,
        expected_record_count=source_record_count,
        observed_record_count=source_record_count,
        evidence_root_sha256=evidence_root_sha256,
    )
    return {
        "source_kind": "nppes_entity_address",
        "authority_classification": policy.authority,
        "trust_classification": policy.trust,
        "semantic_limits": policy.semantic_limits,
        "artifact_identity": artifact,
        "completeness_attestation": attestation,
        "rights_classification": policy.rights,
        "rights_proof_sha256": identity.rights_proof_sha256,
        "source_binding": None,
        "observed_interval": CanonicalUtcInterval(
            identity.snapshot_at, identity.snapshot_at
        ),
        "effective_interval": CanonicalUtcInterval(
            minimum_effective_start_at, identity.snapshot_at
        ),
        "artifact_bytes_verified": True,
        "public_access_verified": True,
        "processing_retention_rights_verified": True,
        "semantic_limits_verified": True,
        "completeness_attestation_verified": True,
        "legal_ownership_claimed": False,
        "exact_rate_site_claimed": False,
        "whole_source_complete": False,
        "redistribution_enabled": False,
        "export_enabled": False,
        "publication_enabled": False,
        "replacement_enabled": False,
        "deletion_enabled": False,
        "retirement_enabled": False,
        "supersession_enabled": False,
    }


def _manifest_payload(
    identity: NppesArchiveIdentity,
    claims: _ManifestClaims,
    release: PublicEvidenceSourceReleaseDescriptor,
) -> dict[str, object]:
    return {
        "contract": NPPES_REGISTRY_MANIFEST_CONTRACT,
        "source_url": identity.source_url,
        "archive_name": identity.archive_name,
        "primary_member_name": identity.primary_member_name,
        "artifact_sha256": identity.artifact_sha256,
        "artifact_byte_count": identity.artifact_byte_count,
        "snapshot_at": identity.snapshot_at,
        "rights_proof_sha256": identity.rights_proof_sha256,
        "record_identity_contract_id": identity.record_identity_contract_id,
        "header_sha256": claims.header_sha256,
        "source_record_count": claims.source_record_count,
        "projected_record_count": claims.projected_record_count,
        "excluded_record_count": claims.excluded_record_count,
        "exclusion_counts": [
            {"reason": reason, "record_count": record_count}
            for reason, record_count in claims.exclusion_counts
        ],
        "evidence_root_sha256": claims.evidence_root_sha256,
        "minimum_effective_start_at": claims.minimum_effective_start_at,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": release.contract_sha256,
    }


class NppesRegistryArchiveScanner:
    """Perform one bounded, sequential, complete primary-row scan."""

    __slots__ = (
        "_excluded",
        "_finished",
        "_header",
        "_identity",
        "_minimum_start",
        "_projected",
        "_root",
        "_seen_npis",
    )

    def __init__(self, identity: object, header: object) -> None:
        self._identity = validate_nppes_archive_identity(identity)
        self._header = compile_nppes_registry_header(header)
        self._root = NppesEvidenceRootAccumulator()
        self._projected = 0
        self._excluded: dict[str, int] = {}
        self._minimum_start: str | None = None
        self._seen_npis: set[int] = set()
        self._finished = False

    @property
    def count(self) -> int:
        """Return the number of source rows accepted so far."""

        return self._root.count

    def add(self, row_values: object) -> NppesRegistryRowObservation:
        """Replay and accumulate the next exact source-order row."""

        if self._finished:
            raise replay_error()
        observation = _scan_compiled_nppes_registry_row(
            self._identity,
            self._header,
            row_values,
            self._root.count + 1,
        )
        npi_number = int(observation.npi)
        if npi_number in self._seen_npis:
            raise replay_error()
        self._seen_npis.add(npi_number)
        self._root.add(observation.leaf_sha256)
        if observation.exclusion_reason is None:
            self._projected += 1
        else:
            reason = observation.exclusion_reason
            self._excluded[reason] = self._excluded.get(reason, 0) + 1
        if observation.effective_start_at is not None and (
            self._minimum_start is None
            or observation.effective_start_at < self._minimum_start
        ):
            self._minimum_start = observation.effective_start_at
        return observation

    def finish(self) -> NppesRegistryArchiveManifest:
        """Seal the complete nonempty row census and source release."""

        if self._finished:
            raise replay_error()
        self._finished = True
        try:
            source_count = self._root.count
            excluded_count = sum(self._excluded.values())
            if (
                source_count == 0
                or source_count != self._projected + excluded_count
                or self._minimum_start is None
            ):
                raise replay_error()
            evidence_root = self._root.finish()
            exclusion_census = tuple(sorted(self._excluded.items()))
            release = build_public_evidence_source_release(
                _release_input(
                    self._identity,
                    source_record_count=source_count,
                    evidence_root_sha256=evidence_root,
                    minimum_effective_start_at=self._minimum_start,
                )
            )
            claims = _ManifestClaims(
                header_sha256=self._header.sha256,
                source_record_count=source_count,
                projected_record_count=self._projected,
                excluded_record_count=excluded_count,
                exclusion_counts=exclusion_census,
                evidence_root_sha256=evidence_root,
                minimum_effective_start_at=self._minimum_start,
            )
            manifest_payload = _manifest_payload(
                self._identity,
                claims,
                release,
            )
            manifest = NppesRegistryArchiveManifest(
                contract=NPPES_REGISTRY_MANIFEST_CONTRACT,
                identity=self._identity,
                header_sha256=self._header.sha256,
                source_record_count=source_count,
                projected_record_count=self._projected,
                excluded_record_count=excluded_count,
                exclusion_counts=exclusion_census,
                evidence_root_sha256=evidence_root,
                minimum_effective_start_at=self._minimum_start,
                release=release,
                manifest_sha256=nppes_manifest_sha256(manifest_payload),
            )
        except Exception:
            normalized_error = replay_error()
        else:
            return manifest
        raise normalized_error


def _validate_exclusion_counts(
    census: object, expected_count: int
) -> tuple[tuple[str, int], ...]:
    if type(census) is not tuple:
        raise replay_error()
    normalized_counts: list[tuple[str, int]] = []
    for exclusion_count in census:
        if (
            type(exclusion_count) is not tuple
            or len(exclusion_count) != 2
            or type(exclusion_count[0]) is not str
            or exclusion_count[0]
            not in {
                "effective_start_not_disclosed",
                "entity_type_not_disclosed",
            }
            or type(exclusion_count[1]) is not int
            or exclusion_count[1] <= 0
        ):
            raise replay_error()
        normalized_counts.append(exclusion_count)
    fixed_counts = tuple(sorted(normalized_counts))
    if fixed_counts != census or len(
        {exclusion_count[0] for exclusion_count in fixed_counts}
    ) != len(fixed_counts):
        raise replay_error()
    if sum(exclusion_count[1] for exclusion_count in fixed_counts) != expected_count:
        raise replay_error()
    return fixed_counts


def _validated_manifest_claims(
    candidate: NppesRegistryArchiveManifest,
    identity: NppesArchiveIdentity,
) -> _ManifestClaims:
    if (
        type(candidate.contract) is not str
        or candidate.contract != NPPES_REGISTRY_MANIFEST_CONTRACT
        or type(candidate.identity) is not NppesArchiveIdentity
        or type(candidate.source_record_count) is not int
        or type(candidate.projected_record_count) is not int
        or type(candidate.excluded_record_count) is not int
        or candidate.source_record_count <= 0
        or candidate.projected_record_count < 0
        or candidate.excluded_record_count < 0
        or candidate.source_record_count
        != candidate.projected_record_count + candidate.excluded_record_count
        or type(candidate.header_sha256) is not str
        or type(candidate.evidence_root_sha256) is not str
        or type(candidate.manifest_sha256) is not str
        or type(candidate.minimum_effective_start_at) is not str
        or re.fullmatch(r"[0-9a-f]{64}", candidate.header_sha256) is None
        or re.fullmatch(r"[0-9a-f]{64}", candidate.evidence_root_sha256) is None
        or re.fullmatch(r"[0-9a-f]{64}", candidate.manifest_sha256) is None
    ):
        raise replay_error()
    try:
        minimum_start = datetime.strptime(
            candidate.minimum_effective_start_at,
            "%Y-%m-%dT%H:%M:%SZ",
        )
        snapshot = datetime.strptime(identity.snapshot_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        raise replay_error() from None
    if minimum_start > snapshot:
        raise replay_error()
    exclusion_counts = _validate_exclusion_counts(
        candidate.exclusion_counts,
        candidate.excluded_record_count,
    )
    return _ManifestClaims(
        header_sha256=candidate.header_sha256,
        source_record_count=candidate.source_record_count,
        projected_record_count=candidate.projected_record_count,
        excluded_record_count=candidate.excluded_record_count,
        exclusion_counts=exclusion_counts,
        evidence_root_sha256=candidate.evidence_root_sha256,
        minimum_effective_start_at=candidate.minimum_effective_start_at,
    )


def _rebuilt_manifest(
    candidate: NppesRegistryArchiveManifest,
    identity: NppesArchiveIdentity,
    claims: _ManifestClaims,
) -> NppesRegistryArchiveManifest:
    release = validate_public_evidence_source_release(candidate.release)
    expected_release = build_public_evidence_source_release(
        _release_input(
            identity,
            source_record_count=claims.source_record_count,
            evidence_root_sha256=claims.evidence_root_sha256,
            minimum_effective_start_at=claims.minimum_effective_start_at,
        )
    )
    if release != expected_release:
        raise replay_error()
    manifest_payload = _manifest_payload(identity, claims, expected_release)
    if not hmac.compare_digest(
        candidate.manifest_sha256,
        nppes_manifest_sha256(manifest_payload),
    ):
        raise replay_error()
    return NppesRegistryArchiveManifest(
        contract=candidate.contract,
        identity=identity,
        header_sha256=claims.header_sha256,
        source_record_count=claims.source_record_count,
        projected_record_count=claims.projected_record_count,
        excluded_record_count=claims.excluded_record_count,
        exclusion_counts=claims.exclusion_counts,
        evidence_root_sha256=claims.evidence_root_sha256,
        minimum_effective_start_at=claims.minimum_effective_start_at,
        release=expected_release,
        manifest_sha256=candidate.manifest_sha256,
    )


def validate_nppes_registry_manifest(
    candidate: object,
) -> NppesRegistryArchiveManifest:
    """Validate one exact manifest shape and all release-bound claims."""

    try:
        if type(candidate) is not NppesRegistryArchiveManifest:
            raise replay_error()
        identity = validate_nppes_archive_identity(candidate.identity)
        claims = _validated_manifest_claims(candidate, identity)
        validated_manifest = _rebuilt_manifest(candidate, identity, claims)
    except Exception:
        normalized_error = replay_error()
    else:
        return validated_manifest
    raise normalized_error


def build_nppes_manifest_from_rows(
    identity: NppesArchiveIdentity,
    header: tuple[str, ...],
    rows: tuple[tuple[str, ...], ...],
) -> NppesRegistryArchiveManifest:
    """Build a bounded in-memory manifest for tests and small verification runs."""

    try:
        if type(rows) is not tuple:
            raise replay_error()
        scanner = NppesRegistryArchiveScanner(identity, header)
        for row_values in rows:
            scanner.add(row_values)
        manifest = scanner.finish()
    except Exception:
        normalized_error = replay_error()
    else:
        return manifest
    raise normalized_error


__all__ = (
    "NppesRegistryArchiveManifest",
    "NppesRegistryArchiveScanner",
    "NppesRegistryReplayError",
    "build_nppes_manifest_from_rows",
    "validate_nppes_registry_manifest",
)
