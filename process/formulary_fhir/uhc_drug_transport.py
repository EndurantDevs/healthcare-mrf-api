# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded transport and staging for UHC formulary source artifacts."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import os
from pathlib import Path
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from typing import Any, AsyncContextManager

import aiohttp
from process.formulary_fhir.async_safety import cancellable_to_thread
from process.formulary_fhir.async_safety import drain_operation
from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.formulary_fhir.source_artifacts import bind_verified_source_artifact
from process.formulary_fhir.uhc_drug_payload import UHCDrugPayloadError
from process.formulary_fhir.uhc_drug_payload import (
    uhc_drug_object_array_item_count,
)
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from process.provider_directory_retained_blob_staging import (
    prepare_retained_artifact_staging_directory,
)
from process.uhc_provider_file_catalog_contract import UHCFileCatalogError
from process.uhc_provider_file_catalog_contract import trusted_public_https_url


DEFAULT_MAX_FILE_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30 * 60
DEFAULT_CONCURRENCY = 4
MAX_CONCURRENCY = 8
DEFAULT_MAX_TOTAL_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MIN_FREE_BYTES = 5 * 1024 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
USER_AGENT = "HealthPorta-UHC-Formulary-Artifacts/1.0"

CancelCheck = Callable[[], Awaitable[None] | None]
ProgressCallback = Callable[[int, int, str, str], Awaitable[None] | None]
SessionFactory = Callable[[aiohttp.ClientTimeout], AsyncContextManager[Any]]


class UHCDrugArtifactAcquisitionError(RuntimeError):
    """Report one bounded artifact-acquisition failure without source values."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        self.retryable = retryable is True
        self.is_retryable = self.retryable
        super().__init__(message)


def _positive_environment_integer(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value in (None, ""):
        return default
    try:
        configured_value = int(raw_value)
    except ValueError:
        raise UHCDrugArtifactAcquisitionError(
            f"{name} must be a positive integer"
        ) from None
    if not 0 < configured_value <= 2**63 - 1:
        raise UHCDrugArtifactAcquisitionError(
            f"{name} must be a positive integer"
        )
    return configured_value


def uhc_drug_download_concurrency() -> int:
    """Return the bounded number of simultaneous drug-file downloads."""

    configured_concurrency = _positive_environment_integer(
        "HLTHPRT_UHC_FORMULARY_DOWNLOAD_CONCURRENCY",
        DEFAULT_CONCURRENCY,
    )
    if configured_concurrency > MAX_CONCURRENCY:
        raise UHCDrugArtifactAcquisitionError(
            "HLTHPRT_UHC_FORMULARY_DOWNLOAD_CONCURRENCY exceeds its bound"
        )
    return configured_concurrency


def _download_directory() -> Path:
    try:
        return prepare_retained_artifact_staging_directory("uhc-formulary")
    except RetainedArtifactError:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact staging storage is unavailable"
        ) from None


async def _invoke(callback: Callable[..., Any] | None, *args: Any) -> None:
    if callback is None:
        return
    callback_result = callback(*args)
    if inspect.isawaitable(callback_result):
        await callback_result


async def _shielded_to_thread(operation: Any, *args: Any) -> Any:
    return await cancellable_to_thread(operation, *args)


def default_uhc_drug_session_factory(
    timeout: aiohttp.ClientTimeout,
) -> AsyncContextManager[aiohttp.ClientSession]:
    """Build the identity-encoding session used by the exact source lane."""

    return aiohttp.ClientSession(
        timeout=timeout,
        auto_decompress=False,
        headers={"User-Agent": USER_AGENT},
    )


def _validated_source_url(identity: SourceArtifactIdentity) -> str:
    try:
        source_url = trusted_public_https_url(identity.source_url)
    except UHCFileCatalogError:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact URL is invalid"
        ) from None
    if source_url != identity.source_url:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact URL is not canonical"
        )
    return source_url


def _declared_response_length(
    response: Any,
    identity: SourceArtifactIdentity,
    *,
    source_url: str,
    max_bytes: int,
) -> int | None:
    if response.status in {408, 425, 429} or 500 <= response.status <= 599:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact transport is temporarily unavailable",
            retryable=True,
        )
    if response.status != 200 or str(response.url) != source_url:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact did not return its exact reviewed URL"
        )
    content_encoding = response.headers.get("Content-Encoding", "").strip().lower()
    if content_encoding not in {"", "identity"}:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact uses unsupported content encoding"
        )
    declared_length = response.content_length
    if declared_length is not None and not 0 < declared_length <= max_bytes:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact declared byte count is invalid"
        )
    if (
        declared_length is not None
        and identity.expected_byte_count is not None
        and declared_length != identity.expected_byte_count
    ):
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact declared byte count changed"
        )
    return declared_length


async def _download_response_body(
    response: Any,
    output_file: Any,
    *,
    max_bytes: int,
    cancel_check: CancelCheck | None,
) -> tuple[str, int]:
    content_digest = hashlib.sha256()
    downloaded_byte_count = 0
    async for response_chunk in response.content.iter_chunked(DOWNLOAD_CHUNK_BYTES):
        await _invoke(cancel_check)
        if not response_chunk:
            continue
        downloaded_byte_count += len(response_chunk)
        if downloaded_byte_count > max_bytes:
            raise UHCDrugArtifactAcquisitionError(
                "UHC drug artifact exceeded its byte limit"
            )
        content_digest.update(response_chunk)
        output_file.write(response_chunk)
    return content_digest.hexdigest(), downloaded_byte_count


def _require_complete_response(
    identity: SourceArtifactIdentity,
    *,
    declared_length: int | None,
    downloaded_byte_count: int,
) -> None:
    if downloaded_byte_count <= 0 or (
        declared_length is not None and downloaded_byte_count != declared_length
    ):
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact response was truncated or empty",
            retryable=True,
        )
    expected_byte_count = identity.expected_byte_count
    if expected_byte_count is None or downloaded_byte_count == expected_byte_count:
        return
    if downloaded_byte_count < expected_byte_count:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact response was truncated or empty",
            retryable=True,
        )
    raise UHCDrugArtifactAcquisitionError(
        "UHC drug artifact byte count is inconsistent"
    )


async def stream_uhc_drug_response(
    session: Any,
    identity: SourceArtifactIdentity,
    output_file: Any,
    *,
    max_bytes: int,
    cancel_check: CancelCheck | None,
) -> tuple[str, int]:
    """Stream one exact identity-encoded reviewed response into a stage file."""

    source_url = _validated_source_url(identity)
    async with session.get(
        source_url,
        allow_redirects=False,
        headers={"Accept-Encoding": "identity"},
    ) as response:
        declared_length = _declared_response_length(
            response,
            identity,
            source_url=source_url,
            max_bytes=max_bytes,
        )
        content_digest, downloaded_byte_count = await _download_response_body(
            response,
            output_file,
            max_bytes=max_bytes,
            cancel_check=cancel_check,
        )
    _require_complete_response(
        identity,
        declared_length=declared_length,
        downloaded_byte_count=downloaded_byte_count,
    )
    return content_digest, downloaded_byte_count


def validate_uhc_drug_object_array(
    source_path: Path,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> int:
    """Require one complete nonempty top-level array of JSON objects."""

    try:
        return uhc_drug_object_array_item_count(
            source_path,
            cancel_check=cancel_check,
        )
    except UHCDrugPayloadError:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact JSON structure is invalid"
        ) from None


def _flush_and_sync(output_file: Any) -> None:
    output_file.flush()
    os.fsync(output_file.fileno())


async def _download_to_stage(
    session: Any,
    identity: SourceArtifactIdentity,
    *,
    max_bytes: int,
    cancel_check: CancelCheck | None,
) -> tuple[Path, str, int]:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="uhc-formulary-",
            suffix=".part",
            dir=_download_directory(),
            delete=False,
        ) as output_file:
            temporary_path = Path(output_file.name)
            os.chmod(temporary_path, 0o600)
            artifact_sha256, artifact_byte_count = await stream_uhc_drug_response(
                session,
                identity,
                output_file,
                max_bytes=max_bytes,
                cancel_check=cancel_check,
            )
            await drain_operation(
                asyncio.to_thread(_flush_and_sync, output_file),
                preserve_cancellation=True,
            )
        await _shielded_to_thread(validate_uhc_drug_object_array, temporary_path)
        return temporary_path, artifact_sha256, artifact_byte_count
    except (aiohttp.ClientError, asyncio.TimeoutError):
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact transport is unavailable",
            retryable=True,
        ) from None
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


async def _acquire_identity(
    session: Any,
    identity: SourceArtifactIdentity,
    semaphore: asyncio.Semaphore,
    *,
    database: Any,
    cancel_check: CancelCheck | None,
    max_bytes: int,
) -> tuple[int, str, str]:
    async with semaphore:
        await _invoke(cancel_check)
        staged_path, artifact_sha256, artifact_byte_count = await _download_to_stage(
            session,
            identity,
            max_bytes=max_bytes,
            cancel_check=cancel_check,
        )
        try:
            await _invoke(cancel_check)
            await bind_verified_source_artifact(
                identity,
                source_path=staged_path,
                artifact_sha256=artifact_sha256,
                artifact_byte_count=artifact_byte_count,
                database=database,
            )
        finally:
            staged_path.unlink(missing_ok=True)
        return artifact_byte_count, identity.family, identity.file_name


def _preflight_pending_artifacts(
    pending_artifacts: tuple[SourceArtifactIdentity, ...],
    *,
    max_file_bytes: int,
    concurrency: int,
) -> None:
    if not pending_artifacts:
        return
    effective_sizes = tuple(
        artifact.expected_byte_count or max_file_bytes
        for artifact in pending_artifacts
    )
    if any(byte_count > max_file_bytes for byte_count in effective_sizes):
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact exceeds its configured byte limit"
        )
    retained_peak = sum(effective_sizes)
    stage_peak = sum(sorted(effective_sizes, reverse=True)[:concurrency])
    if retained_peak > _positive_environment_integer(
        "HLTHPRT_UHC_FORMULARY_TOTAL_MAX_BYTES",
        DEFAULT_MAX_TOTAL_BYTES,
    ):
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact set exceeds its aggregate byte limit"
        )
    minimum_free_bytes = _positive_environment_integer(
        "HLTHPRT_UHC_FORMULARY_MIN_FREE_BYTES",
        DEFAULT_MIN_FREE_BYTES,
    )
    available_bytes = shutil.disk_usage(_download_directory()).free
    if available_bytes < retained_peak + stage_peak + minimum_free_bytes:
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact storage capacity is insufficient"
        )


async def _complete_pending_tasks(
    tasks: tuple[asyncio.Task[tuple[int, str, str]], ...],
    *,
    progress_callback: ProgressCallback | None,
) -> int:
    downloaded_byte_count = 0
    try:
        for completed_count, completed_task in enumerate(
            asyncio.as_completed(tasks),
            start=1,
        ):
            artifact_bytes, family, file_name = await completed_task
            downloaded_byte_count += artifact_bytes
            await _invoke(
                progress_callback,
                completed_count,
                len(tasks),
                family,
                file_name,
            )
    except BaseException:
        for pending_task in tasks:
            pending_task.cancel()
        await drain_operation(
            _join_cancelled_tasks(tasks),
            preserve_cancellation=False,
        )
        raise
    return downloaded_byte_count


async def _join_cancelled_tasks(
    tasks: tuple[asyncio.Task[tuple[int, str, str]], ...],
) -> None:
    await asyncio.gather(*tasks, return_exceptions=True)


async def acquire_pending_uhc_drug_artifacts(
    pending_artifacts: tuple[SourceArtifactIdentity, ...],
    *,
    database: Any,
    session_factory: SessionFactory,
    cancel_check: CancelCheck | None,
    progress_callback: ProgressCallback | None,
) -> int:
    """Acquire, validate, and bind only unresolved exact source identities."""

    if not pending_artifacts:
        return 0
    concurrency = uhc_drug_download_concurrency()
    max_file_bytes = _positive_environment_integer(
        "HLTHPRT_UHC_FORMULARY_FILE_MAX_BYTES",
        DEFAULT_MAX_FILE_BYTES,
    )
    _preflight_pending_artifacts(
        pending_artifacts,
        max_file_bytes=max_file_bytes,
        concurrency=concurrency,
    )
    timeout_seconds = _positive_environment_integer(
        "HLTHPRT_UHC_FORMULARY_DOWNLOAD_TIMEOUT_SECONDS",
        DEFAULT_TIMEOUT_SECONDS,
    )
    timeout = aiohttp.ClientTimeout(
        total=timeout_seconds,
        connect=min(60, timeout_seconds),
        sock_read=timeout_seconds,
    )
    semaphore = asyncio.Semaphore(concurrency)
    async with session_factory(timeout) as session:
        tasks = tuple(
            asyncio.create_task(
                _acquire_identity(
                    session,
                    artifact_identity,
                    semaphore,
                    database=database,
                    cancel_check=cancel_check,
                    max_bytes=max_file_bytes,
                )
            )
            for artifact_identity in pending_artifacts
        )
        return await _complete_pending_tasks(
            tasks,
            progress_callback=progress_callback,
        )


__all__ = (
    "CancelCheck",
    "ProgressCallback",
    "SessionFactory",
    "UHCDrugArtifactAcquisitionError",
    "acquire_pending_uhc_drug_artifacts",
    "default_uhc_drug_session_factory",
    "stream_uhc_drug_response",
    "uhc_drug_download_concurrency",
    "validate_uhc_drug_object_array",
)
