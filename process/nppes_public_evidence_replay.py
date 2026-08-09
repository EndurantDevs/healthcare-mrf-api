# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete first-pass replay preparation for one retained NPPES archive."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
from typing import Awaitable, Callable

from public_evidence.evidence_record_primitives import _canonical_sha256
from public_evidence.nppes_registry_metrics import nppes_manifest_metrics
from public_evidence.nppes_registry_primitives import build_nppes_archive_identity
from public_evidence.nppes_registry_primitives import nppes_header_sha256
from public_evidence.nppes_registry_replay_contract import (
    NppesRegistryArchiveManifest,
    NppesRegistryArchiveScanner,
    validate_nppes_registry_manifest,
)
from public_evidence.nppes_registry_storage_contract import (
    NppesRegistryAdmissionRow,
    NppesRegistryArchiveObservation,
    build_nppes_registry_admission_row,
)
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import (
    PreparedNppesArchive,
    archive_error,
    validate_prepared_nppes_archive,
)
from process.nppes_public_evidence_import import (
    NppesEvidenceRuntimeConfig,
    validate_nppes_evidence_runtime_config,
)
from process.nppes_public_evidence_members import NppesPrimaryCsvRows


CancelCheck = Callable[[], Awaitable[None] | None]
ProgressCallback = Callable[[int], Awaitable[None] | None]
_DEFAULT_CHECK_INTERVAL = 10_000


@dataclass(frozen=True, slots=True, repr=False)
class PreparedNppesRegistryReplay:
    """One retained archive with a complete source-row manifest and receipt."""

    archive: PreparedNppesArchive
    header: tuple[str, ...]
    manifest: NppesRegistryArchiveManifest
    archive_observation: NppesRegistryArchiveObservation
    admission_row: NppesRegistryAdmissionRow

    def __repr__(self) -> str:
        return "<prepared-nppes-registry-replay>"


async def _invoke(callback: Callable[[], object] | None) -> None:
    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _progress(callback: ProgressCallback | None, count: int) -> None:
    if callback is None:
        return
    result = callback(count)
    if inspect.isawaitable(result):
        await result


def _layout_sha256(prepared: PreparedNppesArchive) -> str:
    return _canonical_sha256(
        "nppes_registry_zip_member_census",
        {
            "members": [
                {
                    "ordinal": member.ordinal,
                    "name": member.name,
                    "crc32": member.crc32,
                    "compressed_size": member.compressed_size,
                    "uncompressed_size": member.uncompressed_size,
                }
                for member in prepared.layout.members
            ],
            "primary_member_name": prepared.layout.primary_member_name,
        },
    )


def _archive_identity(
    prepared: PreparedNppesArchive,
    config: NppesEvidenceRuntimeConfig,
):
    config = validate_nppes_evidence_runtime_config(config)
    prepared = validate_prepared_nppes_archive(prepared)
    retained = prepared.retained
    rights_digest = config.rights_proof_sha256
    return build_nppes_archive_identity(
        source_url=retained.candidate.source_url,
        archive_name=retained.candidate.archive_name,
        primary_member_name=prepared.layout.primary_member_name,
        artifact_sha256=retained.artifact_sha256,
        artifact_byte_count=retained.artifact_byte_count,
        rights_proof_sha256=rights_digest,
    )


def validate_prepared_nppes_registry_replay(
    candidate: object,
    config: object,
) -> PreparedNppesRegistryReplay:
    """Rebuild every retained identity and deterministic receipt relationship."""

    try:
        if (
            type(candidate) is not PreparedNppesRegistryReplay
        ):
            raise archive_error()
        config = validate_nppes_evidence_runtime_config(config)
        if not config.required:
            raise archive_error()
        archive = validate_prepared_nppes_archive(candidate.archive)
        identity = _archive_identity(archive, config)
        manifest = validate_nppes_registry_manifest(candidate.manifest)
        if (
            manifest.identity != identity
            or type(candidate.header) is not tuple
            or nppes_header_sha256(candidate.header) != manifest.header_sha256
        ):
            raise archive_error()
        observation = NppesRegistryArchiveObservation(
            listing_sha256=archive.retained.listing_sha256,
            zip_member_count=len(archive.layout.members),
            zip_member_census_sha256=_layout_sha256(archive),
        )
        admission = build_nppes_registry_admission_row(manifest, observation)
        if (
            candidate.archive_observation != observation
            or candidate.admission_row != admission
        ):
            raise archive_error()
        rebuilt = PreparedNppesRegistryReplay(
            archive=archive,
            header=candidate.header,
            manifest=manifest,
            archive_observation=observation,
            admission_row=admission,
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt
    raise normalized_error


async def prepare_nppes_registry_replay(
    prepared: object,
    config: object,
    *,
    cancel_check: CancelCheck | None = None,
    progress: ProgressCallback | None = None,
    check_interval: int = _DEFAULT_CHECK_INTERVAL,
) -> PreparedNppesRegistryReplay:
    """Read every primary row and freeze the exact manifest before any DB write."""

    try:
        if (
            type(check_interval) is not int
            or check_interval <= 0
        ):
            raise archive_error()
        prepared = validate_prepared_nppes_archive(prepared)
        config = validate_nppes_evidence_runtime_config(config)
        if not config.required:
            raise archive_error()
        await _invoke(cancel_check)
        identity = _archive_identity(prepared, config)
        with NppesPrimaryCsvRows(prepared) as primary_rows:
            header = primary_rows.header
            scanner = NppesRegistryArchiveScanner(identity, header)
            for row_count, row_values in enumerate(primary_rows, start=1):
                scanner.add(row_values)
                if row_count % check_interval == 0:
                    await _invoke(cancel_check)
                    await _progress(progress, row_count)
                    await asyncio.sleep(0)
            manifest = scanner.finish()
        observation = NppesRegistryArchiveObservation(
            listing_sha256=prepared.retained.listing_sha256,
            zip_member_count=len(prepared.layout.members),
            zip_member_census_sha256=_layout_sha256(prepared),
        )
        admission = build_nppes_registry_admission_row(manifest, observation)
        await _invoke(cancel_check)
        await _progress(progress, manifest.source_record_count)
        prepared_replay = PreparedNppesRegistryReplay(
            archive=prepared,
            header=header,
            manifest=manifest,
            archive_observation=observation,
            admission_row=admission,
        )
        nppes_manifest_metrics(prepared_replay.manifest)
    except (asyncio.CancelledError, ImportCancelledError, KeyboardInterrupt):
        raise
    except Exception:
        normalized_error = archive_error()
    else:
        return prepared_replay
    raise normalized_error


__all__ = (
    "PreparedNppesRegistryReplay",
    "prepare_nppes_registry_replay",
    "validate_prepared_nppes_registry_replay",
)
