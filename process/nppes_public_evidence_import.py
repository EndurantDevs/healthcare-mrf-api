# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Required-mode orchestration for retained NPPES evidence archives."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from pathlib import Path

from db.models import db
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_acquisition import (
    acquire_nppes_archive,
    acquire_nppes_listing,
    nppes_artifact_store,
)
from process.nppes_public_evidence_archive import (
    PreparedNppesArchive,
    archive_error,
    prepare_nppes_archive,
    select_nppes_release_chain,
    validate_prepared_nppes_archive,
)
from process.nppes_public_evidence_chain import (
    NppesPublicEvidenceArchiveReceipt,
    NppesPublicEvidenceChainReceipt,
    _finished_chain_receipt,
    validate_nppes_public_evidence_chain_receipt,
)
from process.nppes_public_evidence_members import (
    NppesLegacyLayout,
    materialize_nppes_legacy_members,
    open_verified_nppes_legacy_text,
)
from process.nppes_public_evidence_prepared_chain import (
    PreparedNppesReleaseChain,
    build_prepared_nppes_release_chain,
    validate_prepared_nppes_release_chain,
)
from process.nppes_public_evidence_rights import (
    NPPES_RIGHTS_PROOF_SHA256,
    verified_nppes_rights_proof_sha256 as _verified_rights_proof_sha256,
)


@dataclass(frozen=True, slots=True, repr=False)
class NppesEvidenceRuntimeConfig:
    """Fail-closed source-evidence runtime configuration."""

    mode: str
    rights_proof_sha256: str | None

    @property
    def is_required(self) -> bool:
        """Return whether authenticated NPPES evidence is mandatory."""

        return self.mode == "required"

    required = is_required

    def __repr__(self) -> str:
        return "<nppes-evidence-runtime-config>"


def validate_nppes_evidence_runtime_config(
    candidate: object,
) -> NppesEvidenceRuntimeConfig:
    """Rebuild the sole off or reviewed-rights required configuration."""

    try:
        if type(candidate) is not NppesEvidenceRuntimeConfig:
            raise archive_error()
        if (
            type(candidate.mode) is not str
            or (
                candidate.rights_proof_sha256 is not None
                and type(candidate.rights_proof_sha256) is not str
            )
        ):
            raise archive_error()
        if candidate.mode == "required":
            if candidate.rights_proof_sha256 != _verified_rights_proof_sha256():
                raise archive_error()
        elif candidate.mode == "off":
            if candidate.rights_proof_sha256 is not None and (
                candidate.rights_proof_sha256 != _verified_rights_proof_sha256()
            ):
                raise archive_error()
        else:
            raise archive_error()
        rebuilt = NppesEvidenceRuntimeConfig(
            mode=candidate.mode,
            rights_proof_sha256=candidate.rights_proof_sha256,
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt
    raise normalized_error


def resolve_nppes_evidence_runtime_config() -> NppesEvidenceRuntimeConfig:
    """Resolve off/required mode without silently weakening required mode."""

    try:
        mode = os.getenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", "off").strip().lower()
        if mode not in {"off", "required"}:
            raise archive_error()
        raw_rights = os.getenv("HLTHPRT_NPPES_RIGHTS_PROOF_SHA256")
        rights_digest = raw_rights.strip() if raw_rights else None
        expected_rights_digest = (
            _verified_rights_proof_sha256()
            if rights_digest is not None or mode == "required"
            else NPPES_RIGHTS_PROOF_SHA256
        )
        if mode == "required" and (
            rights_digest is None
            or rights_digest != expected_rights_digest
        ):
            raise archive_error()
        if mode == "off" and rights_digest is not None:
            if rights_digest != expected_rights_digest:
                raise archive_error()
        config = NppesEvidenceRuntimeConfig(
            mode=mode,
            rights_proof_sha256=rights_digest,
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return config
    raise normalized_error


async def _acquire_prepared_archive(
    store: object,
    archive_candidate: object,
    listing_sha256: str,
    cancel_check,
) -> PreparedNppesArchive:
    retained_archive = await acquire_nppes_archive(
        store,
        archive_candidate,
        listing_sha256,
        cancel_check=cancel_check,
    )
    return await asyncio.to_thread(prepare_nppes_archive, retained_archive)


async def prepare_nppes_release_chain(
    config: object,
    *,
    cancel_check=None,
) -> PreparedNppesReleaseChain:
    """Acquire, rehash, inspect, and deterministically order the release chain."""

    try:
        config = validate_nppes_evidence_runtime_config(config)
        if not config.required:
            raise archive_error()
        store = nppes_artifact_store()
        listing = await acquire_nppes_listing(
            store,
            cancel_check=cancel_check,
        )
        monthlies = tuple(
            candidate
            for candidate in listing.candidates
            if candidate.archive_kind == "monthly"
        )
        if not monthlies:
            raise archive_error()
        latest_monthly = max(
            monthlies,
            key=lambda archive_candidate: archive_candidate.period_start,
        )
        monthly_prepared = await _acquire_prepared_archive(
            store,
            latest_monthly,
            listing.listing_sha256,
            cancel_check,
        )
        chain = select_nppes_release_chain(
            listing.candidates,
            monthly_prepared.layout.primary_snapshot_date,
        )
        prepared_archives: list[PreparedNppesArchive] = [monthly_prepared]
        for candidate in chain[1:]:
            prepared_archives.append(
                await _acquire_prepared_archive(
                    store,
                    candidate,
                    listing.listing_sha256,
                    cancel_check,
                )
            )
        prepared_chain = build_prepared_nppes_release_chain(
            listing,
            tuple(prepared_archives),
        )
    except (asyncio.CancelledError, ImportCancelledError):
        raise
    except Exception:
        normalized_error = archive_error()
    else:
        return prepared_chain
    raise normalized_error


async def materialize_prepared_nppes_archive(
    prepared: object,
    destination: object,
) -> NppesLegacyLayout:
    """Materialize one prepared archive without blocking the event loop."""

    try:
        prepared = validate_prepared_nppes_archive(prepared)
        if not isinstance(destination, Path):
            raise archive_error()
        layout = await asyncio.to_thread(
            materialize_nppes_legacy_members,
            prepared,
            destination,
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return layout
    raise normalized_error


def _archive_receipt(
    prepared_replay: object,
    writer_receipt: object,
) -> NppesPublicEvidenceArchiveReceipt:
    from process.nppes_public_evidence_replay import (
        PreparedNppesRegistryReplay,
        validate_prepared_nppes_registry_replay,
    )
    from process.nppes_public_evidence_writer import NppesRegistryAdmissionReceipt

    if (
        type(prepared_replay) is not PreparedNppesRegistryReplay
        or type(writer_receipt) is not NppesRegistryAdmissionReceipt
    ):
        raise archive_error()
    replay = validate_prepared_nppes_registry_replay(
        prepared_replay,
        NppesEvidenceRuntimeConfig("required", NPPES_RIGHTS_PROOF_SHA256),
    )
    admission = replay.admission_row
    if (
        writer_receipt.admission_ref != admission.admission_ref
        or writer_receipt.source_release_ref != admission.source_release_ref
        or writer_receipt.artifact_sha256 != admission.artifact_sha256
        or writer_receipt.manifest_sha256 != admission.manifest_sha256
        or writer_receipt.source_record_count != admission.source_record_count
        or writer_receipt.projected_record_count != admission.projected_record_count
        or writer_receipt.excluded_record_count != admission.excluded_record_count
        or writer_receipt.write_state not in {"inserted", "already_present"}
    ):
        raise archive_error()
    return NppesPublicEvidenceArchiveReceipt(
        archive_name=replay.archive.archive_name,
        snapshot_at=replay.manifest.identity.snapshot_at,
        admission_ref=writer_receipt.admission_ref,
        source_release_ref=writer_receipt.source_release_ref,
        artifact_sha256=writer_receipt.artifact_sha256,
        manifest_sha256=writer_receipt.manifest_sha256,
        source_record_count=writer_receipt.source_record_count,
        projected_record_count=writer_receipt.projected_record_count,
        excluded_record_count=writer_receipt.excluded_record_count,
        write_state=writer_receipt.write_state,
    )


async def _admit_prepared_archive(
    prepared_archive: object,
    config: object,
    schema: str,
    database: object,
    expected_source_record_count: int,
    cancel_check,
    progress,
) -> NppesPublicEvidenceArchiveReceipt:
    from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
    from process.nppes_public_evidence_writer import admit_nppes_registry_archive

    prepared_replay = await prepare_nppes_registry_replay(
        prepared_archive,
        config,
        cancel_check=cancel_check,
        progress=progress,
    )
    if prepared_replay.manifest.source_record_count != expected_source_record_count:
        raise archive_error()
    admitted_receipt = await admit_nppes_registry_archive(
        prepared_replay,
        config,
        schema=schema,
        database=database,
        cancel_check=cancel_check,
        progress=progress,
    )
    return _archive_receipt(prepared_replay, admitted_receipt)


def _expected_source_counts(
    prepared_chain: PreparedNppesReleaseChain,
    expected_source_record_counts: object,
) -> tuple[int, ...]:
    if (
        type(expected_source_record_counts) is not tuple
        or len(expected_source_record_counts) != len(prepared_chain.archives)
    ):
        raise archive_error()
    expected_names = tuple(
        prepared_archive.archive_name
        for prepared_archive in prepared_chain.archives
    )
    names: list[str] = []
    counts: list[int] = []
    for entry in expected_source_record_counts:
        if (
            type(entry) is not tuple
            or len(entry) != 2
            or type(entry[0]) is not str
            or type(entry[1]) is not int
            or entry[1] <= 0
        ):
            raise archive_error()
        names.append(entry[0])
        counts.append(entry[1])
    if tuple(names) != expected_names or len(set(names)) != len(names):
        raise archive_error()
    return tuple(counts)


async def _admitted_archive_receipts(
    prepared_chain: PreparedNppesReleaseChain,
    config: NppesEvidenceRuntimeConfig,
    source_record_counts: tuple[int, ...],
    schema: str,
    database: object,
    cancel_check,
    progress,
) -> tuple[NppesPublicEvidenceArchiveReceipt, ...]:
    receipts: list[NppesPublicEvidenceArchiveReceipt] = []
    for archive, expected_count in zip(
        prepared_chain.archives,
        source_record_counts,
        strict=True,
    ):
        receipts.append(
            await _admit_prepared_archive(
                archive,
                config,
                schema,
                database,
                expected_count,
                cancel_check,
                progress,
            )
        )
    return tuple(receipts)


async def _admit_finished_chain(
    prepared_chain: PreparedNppesReleaseChain,
    receipts: tuple[NppesPublicEvidenceArchiveReceipt, ...],
    schema: str,
    database: object,
    cancel_check,
) -> NppesPublicEvidenceChainReceipt:
    from process.nppes_public_evidence_chain_writer import (
        admit_nppes_public_evidence_chain,
    )

    listing = prepared_chain.listing
    chain_receipt = _finished_chain_receipt(
        listing.listing_sha256,
        listing.byte_count,
        tuple(
            archive_candidate.archive_name
            for archive_candidate in listing.candidates
        ),
        receipts,
    )
    return await admit_nppes_public_evidence_chain(
        chain_receipt,
        schema=schema,
        database=database,
        cancel_check=cancel_check,
    )


async def import_nppes_public_evidence_chain(
    prepared_chain: object,
    config: object,
    *,
    expected_source_record_counts: object,
    schema: str = "mrf",
    database: object = db,
    cancel_check=None,
    progress=None,
) -> NppesPublicEvidenceChainReceipt:
    """Replay and atomically admit each archive in deterministic chain order."""

    try:
        prepared_chain = validate_prepared_nppes_release_chain(prepared_chain)
        fixed_config = validate_nppes_evidence_runtime_config(config)
        if not fixed_config.required:
            raise archive_error()
        source_record_counts = _expected_source_counts(
            prepared_chain,
            expected_source_record_counts,
        )
        receipts = await _admitted_archive_receipts(
            prepared_chain,
            fixed_config,
            source_record_counts,
            schema,
            database,
            cancel_check,
            progress,
        )
        admitted_chain_receipt = await _admit_finished_chain(
            prepared_chain,
            receipts,
            schema,
            database,
            cancel_check,
        )
    except (asyncio.CancelledError, ImportCancelledError):
        raise
    except Exception:
        normalized_error = archive_error()
    else:
        return admitted_chain_receipt
    raise normalized_error


__all__ = (
    "NppesEvidenceRuntimeConfig",
    "NPPES_RIGHTS_PROOF_SHA256",
    "NppesPublicEvidenceArchiveReceipt",
    "NppesPublicEvidenceChainReceipt",
    "PreparedNppesArchive",
    "PreparedNppesReleaseChain",
    "build_prepared_nppes_release_chain",
    "import_nppes_public_evidence_chain",
    "materialize_prepared_nppes_archive",
    "open_verified_nppes_legacy_text",
    "prepare_nppes_release_chain",
    "resolve_nppes_evidence_runtime_config",
    "validate_prepared_nppes_archive",
    "validate_prepared_nppes_release_chain",
    "validate_nppes_public_evidence_chain_receipt",
    "validate_nppes_evidence_runtime_config",
)
