# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native-only admission of retained UHC provider and plan JSON files."""

from __future__ import annotations

import asyncio
import os
import stat
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Sequence

import asyncpg

from process.uhc_retained_locks import retained_source_lock
from process.uhc_retained_native import (
    consume_native_attestation,
    is_native_verified_source,
    retain_source_native,
)
from process.uhc_retained_range_manifest import (
    RANGE_CONTRACT_VERSION,
    range_manifest_path,
    range_set_digest,
    retained_raw_path,
)
from process.uhc_retained_registry_contract import (
    SourceBinding,
    UHCSourceBindingMismatch,
    expected_catalog_file_hash_pair as _expected_catalog_file_hash_pair,
    require_digest as _require_digest,
    safe_file_name as _safe_file_name,
)
from process.uhc_retained_registry_store import (
    persist_source_proofs as _persist_source_proofs,
)
from process.uhc_retained_types import (
    RawRangeProof,
    RetainedRawArtifactProof,
    UHCRetainedAdmissionError,
    VerifiedRetainedSource,
    _validate_range_count,
    _verify_artifact,
)


FileIdentity = tuple[str, int, int, int, int, int, int, int]


def _source_file_identity_snapshot(
    raw_artifact: RetainedRawArtifactProof,
) -> tuple[FileIdentity, FileIdentity]:
    """Capture exact raw and manifest path identities after byte verification."""

    identities = []
    for path in (raw_artifact.path, raw_artifact.manifest_path):
        absolute_path = Path(os.path.abspath(path))
        try:
            path_stat = os.stat(absolute_path, follow_symlinks=False)
        except OSError as error:
            raise UHCRetainedAdmissionError(
                "retained source changed during database admission"
            ) from error
        if (
            not stat.S_ISREG(path_stat.st_mode)
            or path_stat.st_nlink != 1
            or path_stat.st_mode & 0o022
        ):
            raise UHCRetainedAdmissionError(
                "retained source changed during database admission"
            )
        identities.append(
            (
                str(absolute_path),
                path_stat.st_dev,
                path_stat.st_ino,
                path_stat.st_size,
                path_stat.st_mtime_ns,
                path_stat.st_ctime_ns,
                path_stat.st_mode,
                path_stat.st_nlink,
            )
        )
    return identities[0], identities[1]


def _assert_source_file_identities_unchanged(
    expected_identities: tuple[FileIdentity, FileIdentity],
    raw_artifact: RetainedRawArtifactProof,
) -> None:
    """Fail before commit when retained paths changed after native verification."""

    if _source_file_identity_snapshot(raw_artifact) != expected_identities:
        raise UHCRetainedAdmissionError(
            "retained source changed during database admission"
        )


async def _release_after_pending_acquire(acquire_task, lock) -> None:
    """Finish a shielded acquire despite repeated cancellation, then release."""

    while not acquire_task.done():
        try:
            await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            continue
        except BaseException:
            return
    if not acquire_task.cancelled() and acquire_task.exception() is None:
        lock.release()


@asynccontextmanager
async def _locked_retained_source(lock):
    """Hold the native/registry filesystem lock without blocking the event loop."""

    acquire_task = asyncio.create_task(asyncio.to_thread(lock.acquire))
    try:
        await asyncio.shield(acquire_task)
    except BaseException:
        await _release_after_pending_acquire(acquire_task, lock)
        raise
    try:
        yield
    finally:
        lock.release()


def uhc_retained_artifact_root() -> Path:
    """Resolve the mandatory non-temporary root for retained UHC data files."""

    configured_root = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT")
    if not configured_root:
        raise UHCRetainedAdmissionError(
            "UHC retained admission requires the provider-directory artifact root"
        )
    try:
        base_root = Path(configured_root).resolve()
        temporary_root = Path(tempfile.gettempdir()).resolve()
        if base_root == Path("/") or base_root.is_relative_to(temporary_root):
            raise UHCRetainedAdmissionError(
                "UHC retained admission cannot use temporary storage"
            )
        retained_entry = base_root / "uhc-provider-files"
        retained_entry.mkdir(parents=True, exist_ok=True)
        if retained_entry.is_symlink():
            raise UHCRetainedAdmissionError(
                "UHC retained artifact root cannot be a symbolic link"
            )
        retained_root = retained_entry.resolve()
        if (
            not retained_root.is_relative_to(base_root)
            or retained_root.is_relative_to(temporary_root)
        ):
            raise UHCRetainedAdmissionError(
                "UHC retained artifact root escapes durable storage"
            )
    except OSError as error:
        raise UHCRetainedAdmissionError(
            "UHC retained artifact storage is unavailable"
        ) from error
    return retained_root


def _require_durable_retained_root(output_root: Path) -> None:
    """Require the one canonical content-addressed provider-directory root."""

    required_root = uhc_retained_artifact_root().resolve()
    if output_root.resolve() != required_root:
        raise UHCRetainedAdmissionError(
            "retained UHC source is not in the exact configured artifact root"
        )


def _validate_binding_identity(
    binding: SourceBinding,
    raw_artifact: RetainedRawArtifactProof,
) -> None:
    """Require exact catalog and raw-artifact identities."""

    _require_digest(binding.source_file_id, "source_file_id")
    _require_digest(binding.catalog_set_sha256, "catalog_set_sha256")
    _require_digest(binding.catalog_entry_sha256, "catalog_entry_sha256")
    _require_digest(binding.artifact_sha256, "artifact_sha256")
    if binding.artifact_sha256 != raw_artifact.sha256:
        raise UHCSourceBindingMismatch("source binding and raw artifact hashes differ")
    _require_digest(raw_artifact.sha256, "raw_artifact.sha256")
    _require_digest(raw_artifact.range_set_sha256, "range_set_sha256")
    _require_digest(raw_artifact.manifest_sha256, "manifest_sha256")
    if (
        raw_artifact.byte_count <= 0
        or raw_artifact.record_count <= 0
        or raw_artifact.canonical_byte_count <= 0
        or raw_artifact.manifest_byte_count <= 0
    ):
        raise UHCRetainedAdmissionError("raw artifact proof is empty")
    if binding.family not in {"cs", "ifp"}:
        raise UHCRetainedAdmissionError("source family is unsupported")
    if binding.collection_kind not in {"provider_membership", "plan_reference"}:
        raise UHCRetainedAdmissionError("collection kind is unsupported")
    _safe_file_name(binding.file_name)
    expected_entry_sha256, expected_file_id = _expected_catalog_file_hash_pair(
        family=binding.family,
        collection_kind=binding.collection_kind,
        file_name=binding.file_name,
        source_url=binding.source_url,
        catalog_modified_at=binding.catalog_modified_at,
        size_bytes=binding.size_bytes,
    )
    if (
        binding.catalog_entry_sha256 != expected_entry_sha256
        or binding.source_file_id != expected_file_id
    ):
        raise UHCSourceBindingMismatch("catalog file identity is inconsistent")


def _validate_range_proofs(
    raw_artifact: RetainedRawArtifactProof,
    ranges: Sequence[RawRangeProof],
) -> None:
    """Require one complete, ordered, content-bound logical range layout."""

    _validate_range_count(len(ranges))
    if (
        raw_artifact.contract_version != RANGE_CONTRACT_VERSION
        or raw_artifact.range_count != len(ranges)
    ):
        raise UHCRetainedAdmissionError("retained range contract is incompatible")
    expected_record_start = 0
    previous_raw_end = 0
    for range_ordinal, raw_range in enumerate(ranges):
        _require_digest(raw_range.raw_sha256, "raw_range.raw_sha256")
        _require_digest(raw_range.canonical_sha256, "canonical_sha256")
        if (
            raw_range.artifact_sha256 != raw_artifact.sha256
            or raw_range.contract_version != raw_artifact.contract_version
            or raw_range.range_count != len(ranges)
            or raw_range.range_ordinal != range_ordinal
            or raw_range.raw_byte_start < previous_raw_end
            or raw_range.raw_byte_end <= raw_range.raw_byte_start
            or raw_range.raw_byte_end > raw_artifact.byte_count
            or raw_range.raw_byte_count
            != raw_range.raw_byte_end - raw_range.raw_byte_start
            or raw_range.record_start != expected_record_start
            or raw_range.record_end - raw_range.record_start
            != raw_range.record_count
            or raw_range.record_count <= 0
            or raw_range.canonical_byte_count <= 0
            or Path(raw_range.path) != Path(raw_artifact.path)
        ):
            raise UHCRetainedAdmissionError("raw range proof is not contiguous")
        expected_record_start = raw_range.record_end
        previous_raw_end = raw_range.raw_byte_end
    if expected_record_start != raw_artifact.record_count:
        raise UHCRetainedAdmissionError(
            "raw range proof does not cover every retained record"
        )
    if sum(raw_range.canonical_byte_count for raw_range in ranges) != (
        raw_artifact.canonical_byte_count
    ):
        raise UHCRetainedAdmissionError("raw range canonical byte proof differs")
    if range_set_digest(
        raw_artifact.sha256,
        raw_artifact.byte_count,
        raw_artifact.record_count,
        tuple(ranges),
    ) != raw_artifact.range_set_sha256:
        raise UHCRetainedAdmissionError("raw range-set proof differs")


def _validate_source_proofs(
    binding: SourceBinding,
    source: VerifiedRetainedSource,
    expected_identities: tuple[FileIdentity, FileIdentity],
) -> None:
    """Rehash exact native-attested files and validate all immutable fields."""

    raw_artifact = source.raw_artifact
    _validate_binding_identity(binding, raw_artifact)
    _validate_range_proofs(raw_artifact, source.ranges)
    output_root = Path(raw_artifact.path).parent
    if Path(raw_artifact.path) != retained_raw_path(output_root, raw_artifact.sha256):
        raise UHCRetainedAdmissionError("retained raw path is not canonical")
    expected_manifest_path = range_manifest_path(
        output_root,
        raw_artifact.sha256,
        raw_artifact.range_count,
        contract_version=raw_artifact.contract_version,
    )
    if Path(raw_artifact.manifest_path) != expected_manifest_path:
        raise UHCRetainedAdmissionError("retained manifest path is not canonical")
    _assert_source_file_identities_unchanged(expected_identities, raw_artifact)
    _verify_artifact(
        Path(raw_artifact.path),
        raw_artifact.sha256,
        raw_artifact.byte_count,
    )
    _verify_artifact(
        Path(raw_artifact.manifest_path),
        raw_artifact.manifest_sha256,
        raw_artifact.manifest_byte_count,
    )
    _assert_source_file_identities_unchanged(expected_identities, raw_artifact)


async def _register_source_transaction(
    connection: asyncpg.Connection,
    binding: SourceBinding,
    source: VerifiedRetainedSource,
    expected_identities: tuple[FileIdentity, FileIdentity],
) -> None:
    """Validate and persist one source within the caller's transaction."""

    raw_artifact = source.raw_artifact
    await asyncio.to_thread(
        _validate_source_proofs,
        binding,
        source,
        expected_identities,
    )
    await _persist_source_proofs(
        connection,
        binding=binding,
        raw_artifact=raw_artifact,
        ranges=source.ranges,
    )
    await asyncio.to_thread(
        _assert_source_file_identities_unchanged,
        expected_identities,
        raw_artifact,
    )


async def _register_attested_source_under_lock(
    connection: asyncpg.Connection,
    *,
    binding: SourceBinding,
    source: VerifiedRetainedSource,
) -> None:
    if not is_native_verified_source(source):
        raise UHCRetainedAdmissionError(
            "retained UHC registration requires a native-verified source"
        )
    expected_identities = consume_native_attestation(source)
    _assert_source_file_identities_unchanged(
        expected_identities,
        source.raw_artifact,
    )
    async with connection.transaction():
        await _register_source_transaction(
            connection,
            binding,
            source,
            expected_identities,
        )


async def register_verified_source(
    connection: asyncpg.Connection,
    *,
    binding: SourceBinding,
    source: VerifiedRetainedSource,
) -> None:
    """Persist one fresh private native proof; it may be consumed only once."""

    if not is_native_verified_source(source):
        raise UHCRetainedAdmissionError(
            "retained UHC registration requires a native-verified source"
        )
    raw_artifact = source.raw_artifact
    output_root = Path(raw_artifact.path).parent
    _require_durable_retained_root(output_root)
    source_lock = retained_source_lock(
        output_root,
        raw_artifact.sha256,
    )
    async with _locked_retained_source(source_lock):
        await _register_attested_source_under_lock(
            connection,
            binding=binding,
            source=source,
        )


async def admit_retained_source(
    connection: asyncpg.Connection,
    *,
    binding: SourceBinding,
    source_path: str | Path,
    expected_sha256: str,
    expected_byte_count: int,
    range_count: int,
) -> VerifiedRetainedSource:
    """Run native retention and commit its one-use proof under one lock."""

    output_root = uhc_retained_artifact_root()
    source_lock = retained_source_lock(
        output_root,
        expected_sha256,
    )
    async with _locked_retained_source(source_lock):
        source = await retain_source_native(
            source_path=source_path,
            output_root=output_root,
            expected_sha256=expected_sha256,
            expected_byte_count=expected_byte_count,
            range_count=range_count,
        )
        if binding.artifact_sha256 != source.raw_artifact.sha256:
            raise UHCSourceBindingMismatch(
                "source binding and native artifact hashes differ"
            )
        await _register_attested_source_under_lock(
            connection,
            binding=binding,
            source=source,
        )
        return source
