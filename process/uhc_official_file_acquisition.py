# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Restart-safe acquisition for UHC's reviewed official provider-file catalog."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import stat
import tempfile
from typing import Any, AsyncContextManager
import urllib.parse

import aiohttp
import asyncpg

from process.uhc_provider_file_catalog_store import (
    CATALOG_FILE_TABLE,
    CATALOG_SET_TABLE,
)
from process.uhc_provider_file_identity import (
    UHCSourceFileDescriptor,
    logical_scopes_for_current_census,
)
from process.uhc_retained_registry_contract import (
    SourceBinding,
    expected_catalog_file_hash_pair,
)
from process.uhc_retained_registry_store_names import table_name
from process.uhc_retained_source_registry import (
    admit_retained_source,
    uhc_retained_artifact_root,
)
from process.uhc_retained_types import UHCRetainedAdmissionError


DEFAULT_FILE_MAX_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_DOWNLOAD_TIMEOUT_SECONDS = 30 * 60
DEFAULT_DOWNLOAD_CONCURRENCY = 4
MAX_DOWNLOAD_CONCURRENCY = 8
DEFAULT_ADMISSION_CONCURRENCY = 2
MAX_ADMISSION_CONCURRENCY = 4
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
RETAINED_RANGE_COUNT = 4

ProgressCallback = Callable[[int, int, str, str], Awaitable[None]]
CancelCheck = Callable[[], Awaitable[None]]


class UHCOfficialFileAcquisitionError(RuntimeError):
    """Fail closed when one reviewed catalog file cannot be retained exactly."""


@dataclass(frozen=True)
class UHCOfficialFileAcquisitionResult:
    catalog_set_sha256: str
    file_count: int
    downloaded_file_count: int
    reused_file_count: int
    downloaded_byte_count: int


@dataclass(frozen=True)
class _CatalogAcquisitionContext:
    pipeline_semaphore: asyncio.Semaphore
    admission_semaphore: asyncio.Semaphore
    shared_connection_lock: asyncio.Lock
    connection: asyncpg.Connection
    connection_factory: (
        Callable[[], AsyncContextManager[asyncpg.Connection]] | None
    )
    active_session: aiohttp.ClientSession
    catalog_set_sha256: str
    staged_paths: set[Path]


def _positive_environment_integer(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value in (None, ""):
        return default
    try:
        value = int(raw_value)
    except ValueError as error:
        raise UHCOfficialFileAcquisitionError(
            f"{name} must be a positive integer"
        ) from error
    if value <= 0 or value > 2**63 - 1:
        raise UHCOfficialFileAcquisitionError(
            f"{name} must be a positive integer"
        )
    return value


def _file_max_bytes() -> int:
    return _positive_environment_integer(
        "HLTHPRT_UHC_PROVIDER_FILE_MAX_BYTES",
        DEFAULT_FILE_MAX_BYTES,
    )


def _download_timeout_seconds() -> int:
    return _positive_environment_integer(
        "HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_TIMEOUT_SECONDS",
        DEFAULT_DOWNLOAD_TIMEOUT_SECONDS,
    )


def uhc_provider_file_download_concurrency() -> int:
    """Return the bounded number of concurrent official-file downloads."""

    concurrency = _positive_environment_integer(
        "HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_CONCURRENCY",
        DEFAULT_DOWNLOAD_CONCURRENCY,
    )
    if concurrency > MAX_DOWNLOAD_CONCURRENCY:
        raise UHCOfficialFileAcquisitionError(
            "HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_CONCURRENCY "
            f"must not exceed {MAX_DOWNLOAD_CONCURRENCY}"
        )
    return concurrency


def uhc_provider_file_admission_concurrency() -> int:
    """Return the bounded number of simultaneous native admissions."""

    concurrency = _positive_environment_integer(
        "HLTHPRT_UHC_PROVIDER_FILE_ADMISSION_CONCURRENCY",
        DEFAULT_ADMISSION_CONCURRENCY,
    )
    if concurrency > MAX_ADMISSION_CONCURRENCY:
        raise UHCOfficialFileAcquisitionError(
            "HLTHPRT_UHC_PROVIDER_FILE_ADMISSION_CONCURRENCY "
            f"must not exceed {MAX_ADMISSION_CONCURRENCY}"
        )
    return concurrency


def _row_mapping(database_record: Any) -> dict[str, Any]:
    if database_record is None:
        return {}
    mapping = (
        database_record._mapping
        if hasattr(database_record, "_mapping")
        else database_record
    )
    return dict(mapping)


def _catalog_file_binding_fields(
    catalog_set_sha256: str,
    catalog_file: Mapping[str, Any],
) -> dict[str, Any]:
    binding_field_map = {
        "source_file_id": str(catalog_file["file_id"]),
        "family": str(catalog_file["family"]),
        "collection_kind": str(catalog_file["collection_kind"]),
        "file_name": str(catalog_file["file_name"]),
        "source_url": str(catalog_file["source_url"]),
        "catalog_modified_at": str(catalog_file["catalog_modified_at"]),
        "size_bytes": catalog_file["size_bytes"],
        "catalog_set_sha256": catalog_set_sha256,
        "catalog_entry_sha256": str(catalog_file["catalog_entry_sha256"]),
    }
    expected_entry_hash, expected_file_id = expected_catalog_file_hash_pair(
        family=binding_field_map["family"],
        collection_kind=binding_field_map["collection_kind"],
        file_name=binding_field_map["file_name"],
        source_url=binding_field_map["source_url"],
        catalog_modified_at=binding_field_map["catalog_modified_at"],
        size_bytes=binding_field_map["size_bytes"],
    )
    if (
        binding_field_map["catalog_entry_sha256"] != expected_entry_hash
        or binding_field_map["source_file_id"] != expected_file_id
        or catalog_file.get("availability") != "published"
        or catalog_file.get("catalog_support") != "cataloged"
    ):
        raise UHCOfficialFileAcquisitionError(
            "UHC catalog file identity or availability is invalid"
        )
    return binding_field_map


async def _selected_catalog_files(
    connection: asyncpg.Connection,
    catalog_set_sha256: str,
) -> tuple[dict[str, Any], ...]:
    """Load and validate every file in the selected immutable catalog set."""
    catalog_row = await connection.fetchrow(
        f"""
        SELECT file_count, provider_file_count, plan_reference_file_count
          FROM {table_name(CATALOG_SET_TABLE)}
         WHERE catalog_set_sha256=$1
        """,
        catalog_set_sha256,
    )
    if catalog_row is None:
        raise UHCOfficialFileAcquisitionError(
            "selected UHC catalog set was not found"
        )
    catalog_records = await connection.fetch(
        f"""
        SELECT file_id, family, collection_kind, file_name, source_url,
               catalog_modified_at, catalog_entry_sha256, size_bytes,
               availability, catalog_support
          FROM {table_name(CATALOG_FILE_TABLE)}
         WHERE catalog_set_sha256=$1
         ORDER BY family, collection_kind, file_name, file_id
        """,
        catalog_set_sha256,
    )
    _validate_selected_catalog_counts(catalog_row, catalog_records)
    validated_records = []
    for catalog_record in catalog_records:
        catalog_record_map = _row_mapping(catalog_record)
        _catalog_file_binding_fields(
            catalog_set_sha256,
            catalog_record_map,
        )
        validated_records.append(catalog_record_map)
    logical_scopes_for_current_census(
        UHCSourceFileDescriptor(
            family=str(catalog_record["family"]),
            collection_kind=str(catalog_record["collection_kind"]),
            file_name=str(catalog_record["file_name"]),
        )
        for catalog_record in validated_records
    )
    return tuple(validated_records)


def _validate_selected_catalog_counts(
    catalog_record: Mapping[str, Any],
    catalog_file_records: Sequence[Mapping[str, Any]],
) -> None:
    """Require the selected rows to match their immutable set census."""
    expected_count = int(catalog_record["file_count"])
    provider_count = sum(
        catalog_file["collection_kind"] == "provider_membership"
        for catalog_file in catalog_file_records
    )
    plan_count = sum(
        catalog_file["collection_kind"] == "plan_reference"
        for catalog_file in catalog_file_records
    )
    if (
        len(catalog_file_records) != expected_count
        or provider_count != int(catalog_record["provider_file_count"])
        or plan_count != int(catalog_record["plan_reference_file_count"])
        or expected_count <= 0
    ):
        raise UHCOfficialFileAcquisitionError(
            "selected UHC catalog set is incomplete"
        )


def _is_retained_file_usable(
    storage_uri: Any,
    expected_bytes: Any = None,
) -> bool:
    if not isinstance(storage_uri, str):
        return False
    parsed = urllib.parse.urlsplit(storage_uri)
    if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
        return False
    try:
        path = Path(urllib.parse.unquote(parsed.path))
        path_stat = os.stat(path, follow_symlinks=False)
    except (OSError, ValueError):
        return False
    return bool(
        stat.S_ISREG(path_stat.st_mode)
        and path_stat.st_nlink == 1
        and not path_stat.st_mode & 0o022
        and (
            expected_bytes is None
            or (
                isinstance(expected_bytes, int)
                and path_stat.st_size == expected_bytes
            )
        )
    )


async def _has_reusable_binding(
    connection: asyncpg.Connection,
    catalog_set_sha256: str,
    source_file_id: str,
) -> bool:
    binding_record = await connection.fetchrow(
        f"""
        SELECT binding.artifact_sha256, raw.byte_count,
               raw.storage_uri AS raw_storage_uri,
               layout.manifest_storage_uri
          FROM {table_name("provider_directory_uhc_source_binding")} AS binding
          JOIN {table_name("provider_directory_uhc_raw_artifact")} AS raw
            ON raw.artifact_sha256=binding.artifact_sha256
          JOIN {table_name("provider_directory_uhc_raw_layout")} AS layout
            ON layout.artifact_sha256=binding.artifact_sha256
         WHERE binding.catalog_set_sha256=$1
           AND binding.source_file_id=$2
           AND binding.released_at IS NULL
           AND raw.status='verified'
           AND layout.status='verified'
         ORDER BY layout.contract_version DESC, layout.range_count DESC
         LIMIT 1
        """,
        catalog_set_sha256,
        source_file_id,
    )
    return bool(
        binding_record is not None
        and _is_retained_file_usable(
            binding_record["raw_storage_uri"],
            binding_record["byte_count"],
        )
        and _is_retained_file_usable(
            binding_record["manifest_storage_uri"]
        )
    )


def _download_directory() -> Path:
    retained_root = uhc_retained_artifact_root()
    download_root = retained_root / "downloads"
    try:
        download_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if download_root.is_symlink():
            raise OSError("symbolic link")
        os.chmod(download_root, 0o700)
    except OSError as error:
        raise UHCOfficialFileAcquisitionError(
            "UHC download staging storage is unavailable"
        ) from error
    return download_root


async def _download_file(
    session: aiohttp.ClientSession,
    catalog_file: Mapping[str, Any],
    *,
    max_bytes: int,
) -> tuple[Path, str, int]:
    """Stream one exact reviewed URL into a private fsynced staging file."""

    download_root = _download_directory()
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix="official-provider-file-",
            suffix=".part",
            dir=download_root,
            delete=False,
        ) as output:
            temporary_path = Path(output.name)
            os.chmod(temporary_path, 0o600)
            digest, byte_count, declared_length = (
                await _stream_download_response(
                    session,
                    catalog_file,
                    output,
                    max_bytes=max_bytes,
                )
            )
            output.flush()
            os.fsync(output.fileno())
        _validate_download_byte_count(
            catalog_file,
            byte_count=byte_count,
            declared_length=declared_length,
        )
        return temporary_path, digest.hexdigest(), byte_count
    except (aiohttp.ClientError, asyncio.TimeoutError) as error:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise UHCOfficialFileAcquisitionError(
            "UHC provider-file transport is unavailable"
        ) from error
    except BaseException:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


async def _stream_download_response(
    session: aiohttp.ClientSession,
    catalog_file: Mapping[str, Any],
    output: Any,
    *,
    max_bytes: int,
) -> tuple[Any, int, int | None]:
    """Hash and persist one identity-encoded response within its size bound."""

    digest = hashlib.sha256()
    byte_count = 0
    source_url = str(catalog_file["source_url"])
    async with session.get(source_url, allow_redirects=False) as response:
        if response.status != 200 or str(response.url) != source_url:
            raise UHCOfficialFileAcquisitionError(
                "UHC provider-file download did not return the exact reviewed URL"
            )
        content_encoding = response.headers.get(
            "Content-Encoding", ""
        ).strip().lower()
        if content_encoding not in ("", "identity"):
            raise UHCOfficialFileAcquisitionError(
                "UHC provider-file response uses unsupported content encoding"
            )
        declared_length = response.content_length
        if declared_length is not None and not 0 < declared_length <= max_bytes:
            raise UHCOfficialFileAcquisitionError(
                "UHC provider-file response size is invalid"
            )
        async for response_chunk in response.content.iter_chunked(
            DOWNLOAD_CHUNK_BYTES
        ):
            if not response_chunk:
                continue
            byte_count += len(response_chunk)
            if byte_count > max_bytes:
                raise UHCOfficialFileAcquisitionError(
                    "UHC provider-file exceeded the configured size limit"
                )
            digest.update(response_chunk)
            output.write(response_chunk)
    return digest, byte_count, declared_length


def _validate_download_byte_count(
    catalog_file: Mapping[str, Any],
    *,
    byte_count: int,
    declared_length: int | None,
) -> None:
    """Reject empty, truncated, or catalog-inconsistent downloads."""

    if byte_count <= 0 or (
        declared_length is not None and byte_count != declared_length
    ):
        raise UHCOfficialFileAcquisitionError(
            "UHC provider-file response was truncated or empty"
        )
    catalog_size = catalog_file.get("size_bytes")
    if catalog_size is not None and byte_count != catalog_size:
        raise UHCOfficialFileAcquisitionError(
            "UHC provider-file byte count differs from its catalog identity"
        )


async def _admit_downloaded_catalog_file(
    connection: asyncpg.Connection,
    catalog_set_sha256: str,
    catalog_file: Mapping[str, Any],
    temporary_path: Path,
    artifact_sha256: str,
    byte_count: int,
) -> None:
    """Atomically admit one concurrently downloaded provider file."""

    try:
        binding_field_map = _catalog_file_binding_fields(
            catalog_set_sha256,
            catalog_file,
        )
        await admit_retained_source(
            connection,
            binding=SourceBinding(
                **binding_field_map,
                artifact_sha256=artifact_sha256,
            ),
            source_path=temporary_path,
            expected_sha256=artifact_sha256,
            expected_byte_count=byte_count,
            range_count=RETAINED_RANGE_COUNT,
        )
    except UHCRetainedAdmissionError as error:
        raise UHCOfficialFileAcquisitionError(
            "UHC provider-file retained admission failed"
        ) from error
    finally:
        temporary_path.unlink(missing_ok=True)


async def _acquire_missing_catalog_file(
    acquisition_context: _CatalogAcquisitionContext,
    catalog_file: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Path, str, int]:
    """Download and admit one file under bounded pipeline concurrency."""

    async with acquisition_context.pipeline_semaphore:
        temporary_path, artifact_sha256, byte_count = await _download_file(
            acquisition_context.active_session,
            catalog_file,
            max_bytes=_file_max_bytes(),
        )
        acquisition_context.staged_paths.add(temporary_path)
        async with acquisition_context.admission_semaphore:
            if acquisition_context.connection_factory is None:
                async with acquisition_context.shared_connection_lock:
                    await _admit_downloaded_catalog_file(
                        acquisition_context.connection,
                        acquisition_context.catalog_set_sha256,
                        catalog_file,
                        temporary_path,
                        artifact_sha256,
                        byte_count,
                    )
            else:
                async with (
                    acquisition_context.connection_factory()
                ) as worker_connection:
                    await _admit_downloaded_catalog_file(
                        worker_connection,
                        acquisition_context.catalog_set_sha256,
                        catalog_file,
                        temporary_path,
                        artifact_sha256,
                        byte_count,
                    )
    return catalog_file, temporary_path, artifact_sha256, byte_count


async def _catalog_files_requiring_download(
    connection: asyncpg.Connection,
    catalog_set_sha256: str,
    catalog_files: Sequence[Mapping[str, Any]],
    cancel_check: CancelCheck | None,
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    """Partition reusable and missing bindings before concurrent transfer."""

    reusable_files: list[Mapping[str, Any]] = []
    missing_files: list[Mapping[str, Any]] = []
    for catalog_file in catalog_files:
        if cancel_check is not None:
            await cancel_check()
        is_reusable = await _has_reusable_binding(
            connection,
            catalog_set_sha256,
            str(catalog_file["file_id"]),
        )
        (reusable_files if is_reusable else missing_files).append(
            catalog_file
        )
    return reusable_files, missing_files


async def _report_catalog_file_progress(
    progress_callback: ProgressCallback | None,
    *,
    completed_count: int,
    total_count: int,
    catalog_file: Mapping[str, Any],
    disposition: str,
) -> None:
    if progress_callback is None:
        return
    await progress_callback(
        completed_count,
        total_count,
        str(catalog_file["file_name"]),
        disposition,
    )


async def _report_reused_catalog_files(
    reusable_files: Sequence[Mapping[str, Any]],
    catalog_file_count: int,
    progress_callback: ProgressCallback | None,
) -> int:
    completed_count = 0
    for catalog_file in reusable_files:
        completed_count += 1
        await _report_catalog_file_progress(
            progress_callback,
            completed_count=completed_count,
            total_count=catalog_file_count,
            catalog_file=catalog_file,
            disposition="reused",
        )
    return completed_count


def _catalog_acquisition_context(
    connection: asyncpg.Connection,
    connection_factory: (
        Callable[[], AsyncContextManager[asyncpg.Connection]] | None
    ),
    active_session: aiohttp.ClientSession,
    catalog_set_sha256: str,
    staged_paths: set[Path],
) -> _CatalogAcquisitionContext:
    return _CatalogAcquisitionContext(
        pipeline_semaphore=asyncio.Semaphore(
            uhc_provider_file_download_concurrency()
        ),
        admission_semaphore=asyncio.Semaphore(
            uhc_provider_file_admission_concurrency()
        ),
        shared_connection_lock=asyncio.Lock(),
        connection=connection,
        connection_factory=connection_factory,
        active_session=active_session,
        catalog_set_sha256=catalog_set_sha256,
        staged_paths=staged_paths,
    )


async def _cleanup_catalog_acquisition_tasks(
    tasks: Sequence[asyncio.Task],
    staged_paths: set[Path],
) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for staged_path in staged_paths:
        staged_path.unlink(missing_ok=True)


async def _acquire_catalog_files(
    connection: asyncpg.Connection,
    active_session: aiohttp.ClientSession,
    catalog_set_sha256: str,
    catalog_files: Sequence[Mapping[str, Any]],
    progress_callback: ProgressCallback | None,
    cancel_check: CancelCheck | None,
    connection_factory: (
        Callable[[], AsyncContextManager[asyncpg.Connection]] | None
    ) = None,
) -> tuple[int, int, int]:
    """Acquire files with concurrent transfer and serialized admission."""

    reusable_files, missing_files = await _catalog_files_requiring_download(
        connection,
        catalog_set_sha256,
        catalog_files,
        cancel_check,
    )
    reused_count = len(reusable_files)
    downloaded_bytes = 0
    completed_count = await _report_reused_catalog_files(
        reusable_files,
        len(catalog_files),
        progress_callback,
    )
    staged_paths: set[Path] = set()
    acquisition_context = _catalog_acquisition_context(
        connection,
        connection_factory,
        active_session,
        catalog_set_sha256,
        staged_paths,
    )
    tasks = [
        asyncio.create_task(
            _acquire_missing_catalog_file(acquisition_context, catalog_file)
        )
        for catalog_file in missing_files
    ]
    downloaded_count = 0
    try:
        for completed_task in asyncio.as_completed(tasks):
            catalog_file, temporary_path, artifact_sha256, byte_count = (
                await completed_task
            )
            downloaded_count += 1
            downloaded_bytes += byte_count
            completed_count += 1
            await _report_catalog_file_progress(
                progress_callback,
                completed_count=completed_count,
                total_count=len(catalog_files),
                catalog_file=catalog_file,
                disposition="downloaded",
            )
    finally:
        await _cleanup_catalog_acquisition_tasks(tasks, staged_paths)
    return downloaded_count, reused_count, downloaded_bytes


async def acquire_complete_uhc_catalog_set(
    connection: asyncpg.Connection,
    catalog_set_sha256: str,
    *,
    progress_callback: ProgressCallback | None = None,
    cancel_check: CancelCheck | None = None,
    session: aiohttp.ClientSession | None = None,
    connection_factory: (
        Callable[[], AsyncContextManager[asyncpg.Connection]] | None
    ) = None,
) -> UHCOfficialFileAcquisitionResult:
    """Download and admit every missing file in one immutable catalog set."""

    catalog_files = await _selected_catalog_files(
        connection,
        catalog_set_sha256,
    )
    timeout = aiohttp.ClientTimeout(
        total=_download_timeout_seconds(),
        connect=30,
        sock_read=5 * 60,
    )
    should_close_session = session is None
    active_session = session or aiohttp.ClientSession(
        timeout=timeout,
        auto_decompress=False,
        headers={"User-Agent": "HealthPorta-Official-Provider-Files/1.0"},
    )
    try:
        downloaded_count, reused_count, downloaded_bytes = (
            await _acquire_catalog_files(
                connection,
                active_session,
                catalog_set_sha256,
                catalog_files,
                progress_callback,
                cancel_check,
                connection_factory,
            )
        )
    finally:
        if should_close_session:
            await active_session.close()
    if downloaded_count + reused_count != len(catalog_files):
        raise UHCOfficialFileAcquisitionError(
            "UHC provider-file acquisition did not complete the selected catalog"
        )
    return UHCOfficialFileAcquisitionResult(
        catalog_set_sha256=catalog_set_sha256,
        file_count=len(catalog_files),
        downloaded_file_count=downloaded_count,
        reused_file_count=reused_count,
        downloaded_byte_count=downloaded_bytes,
    )


__all__ = [
    "UHCOfficialFileAcquisitionError",
    "UHCOfficialFileAcquisitionResult",
    "acquire_complete_uhc_catalog_set",
    "uhc_provider_file_admission_concurrency",
    "uhc_provider_file_download_concurrency",
]
