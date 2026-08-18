# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Rust-first COPY bridge for national evidence-alias shadow derivation."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any, BinaryIO

from process.address_evidence_alias_process import run_native_process
from process.ext.address_alias_sql import (
    ADDRESS_ALIAS_CANDIDATE_TABLE,
    ADDRESS_ALIAS_TABLE,
    _relation,
)
from process.ext.address_canon import _is_canon_version_match
from process.ptg_parts.rust_scanner import (
    _ptg2_rust_scanner_binary,
)


logger = logging.getLogger(__name__)
ADDRESS_EVIDENCE_ALIAS_NATIVE_ENV = "HLTHPRT_ADDRESS_EVIDENCE_ALIAS_NATIVE"
ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT = "address_evidence_alias_native_v1"
ADDRESS_EVIDENCE_ALIAS_NATIVE_THREADS_ENV = (
    "HLTHPRT_ADDRESS_EVIDENCE_ALIAS_NATIVE_THREADS"
)
ADDRESS_EVIDENCE_ALIAS_SCRATCH_DIR_ENV = (
    "HLTHPRT_ADDRESS_EVIDENCE_ALIAS_SCRATCH_DIR"
)
_CANDIDATE_COLUMNS = (
    "run_id",
    "source_address_key",
    "source_identity_key",
    "target_address_key",
    "target_identity_key",
    "candidate_count",
    "target_strict_source_bits",
    "target_strict_source_count",
    "decision",
    "review_status",
    "match_rule",
    "match_classification",
    "evidence_npi",
    "evidence_npi_count",
)


def _is_native_enabled() -> bool:
    return os.getenv(ADDRESS_EVIDENCE_ALIAS_NATIVE_ENV, "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _native_threads() -> int:
    raw_value = os.getenv(ADDRESS_EVIDENCE_ALIAS_NATIVE_THREADS_ENV, "8")
    try:
        thread_count = int(raw_value)
    except ValueError as error:
        raise RuntimeError("native address evidence thread count is invalid") from error
    if not 1 <= thread_count <= 8:
        raise RuntimeError("native address evidence thread count is invalid")
    return thread_count


async def _run_native_process(
    binary: Path,
    arguments: tuple[str, ...],
    label: str,
    *,
    cleanup_deadline_monotonic: float | None = None,
    pass_fds: tuple[int, ...] = (),
) -> bytes:
    return await run_native_process(
        binary,
        arguments,
        label,
        thread_count=_native_threads(),
        cleanup_deadline_monotonic=cleanup_deadline_monotonic,
        pass_fds=pass_fds,
    )


async def _is_native_version_current(
    binary: Path,
    *,
    cleanup_deadline_monotonic: float | None = None,
) -> bool:
    try:
        canonical_payload = json.loads(
            (
                await _run_native_process(
                    binary,
                    ("--canon-version",),
                    "Rust address canonicalizer version check",
                    cleanup_deadline_monotonic=cleanup_deadline_monotonic,
                )
            ).decode("utf-8", errors="strict")
        )
        evidence_contract = (
            (
                await _run_native_process(
                    binary,
                    ("--address-evidence-alias-version",),
                    "Rust address evidence version check",
                    cleanup_deadline_monotonic=cleanup_deadline_monotonic,
                )
            )
            .decode("utf-8", errors="strict")
            .strip()
        )
    except TimeoutError:
        raise
    except Exception as error:
        logger.warning("native address evidence version check failed: %s", error)
        return False
    return _is_canon_version_match(canonical_payload) and (
        evidence_contract == ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT
    )


def _archive_copy_sql(archive: str) -> str:
    return f"""
        SELECT address_key::text,
               identity_key,
               precision,
               first_line,
               second_line,
               city_name,
               state_name,
               postal_code,
               COALESCE(country_code, 'US'),
               COALESCE(strict_source_bits, 0)::text,
               merged_into::text,
               state_code,
               zip5
        FROM {archive}
    """


def _membership_copy_sql(schema: str) -> str:
    unified = _relation(schema, "entity_address_unified")
    return f"""
        SELECT COALESCE(npi, inferred_npi)::text,
               address_key::text
        FROM {unified}
        WHERE type IN ('primary', 'secondary', 'practice', 'site')
          AND address_key IS NOT NULL
    """


def _alias_copy_sql(schema: str) -> str:
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    return f"""
        SELECT source_address_key::text,
               target_address_key::text,
               shadow_run_id::text
        FROM {aliases}
        WHERE revoked_at IS NULL
    """


async def _driver(session: Any) -> Any | None:
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    driver = getattr(raw_connection, "driver_connection", raw_connection)
    if (
        getattr(driver, "copy_from_query", None) is None
        or getattr(driver, "copy_to_table", None) is None
    ):
        return None
    return driver


async def _run_scanner(
    binary: Path,
    paths: tuple[Path, ...],
    file_descriptors: tuple[int, ...],
    *,
    cleanup_deadline_monotonic: float | None = None,
) -> None:
    await _run_native_process(
        binary,
        ("--address-evidence-alias-copy", *(str(path) for path in paths)),
        "Rust address evidence matcher",
        cleanup_deadline_monotonic=cleanup_deadline_monotonic,
        pass_fds=file_descriptors,
    )


async def _export_copy(driver: Any, query: str, output: BinaryIO) -> None:
    await driver.copy_from_query(
        query,
        output=output,
        format="text",
        delimiter="\t",
        null="\\N",
    )


def _validated_summary_file(source: BinaryIO) -> dict[str, Any]:
    source.seek(0)
    summary = json.loads(source.read().decode("utf-8", errors="strict"))
    if summary.get("contract") != ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT:
        raise RuntimeError("Rust address evidence summary contract is invalid")
    for field in (
        "archive_rows",
        "membership_rows",
        "visible_memberships",
        "source_count",
        "active_skipped",
        "pair_count",
        "pair_match_count",
        "global_pair_count",
        "candidate_rows",
        "elapsed_ms",
    ):
        if type(summary.get(field)) is not int or summary[field] < 0:
            raise RuntimeError(
                f"Rust address evidence summary field is invalid: {field}"
            )
    digest = summary.get("output_sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        raise RuntimeError("Rust address evidence output digest is invalid")
    try:
        bytes.fromhex(digest)
    except ValueError as error:
        raise RuntimeError("Rust address evidence output digest is invalid") from error
    return summary


def _sha256_file(source: BinaryIO) -> str:
    source.seek(0)
    digest = hashlib.file_digest(source, "sha256").hexdigest()
    source.seek(0)
    return digest


def _native_scratch_directory() -> Path | None:
    raw_path = os.getenv(ADDRESS_EVIDENCE_ALIAS_SCRATCH_DIR_ENV) or os.getenv(
        "HLTHPRT_WORKER_STATE_DIR"
    )
    if not raw_path:
        return None
    scratch_directory = Path(raw_path)
    return scratch_directory if scratch_directory.is_dir() else None


def _native_descriptor_root() -> Path | None:
    descriptor_root = Path("/proc/self/fd")
    return descriptor_root if descriptor_root.is_dir() else None


def _warn_native_fallback(reason: str) -> None:
    logger.warning("%s; using PostgreSQL address evidence oracle", reason)


def _copied_count(status: Any) -> int | None:
    if not isinstance(status, str):
        return None
    try:
        return int(status.rsplit(" ", 1)[-1])
    except ValueError:
        return None


async def _export_native_shadow_files(
    driver: Any,
    *,
    archive: str,
    schema: str,
    run_id: str,
    state_code: str | None,
    zip_prefix: str | None,
    retry_shadow_run_id: str | None,
    native_files: tuple[BinaryIO, ...],
) -> None:
    (
        archive_file,
        membership_file,
        aliases_file,
        config_file,
        _,
        _,
    ) = native_files
    await _export_copy(driver, _archive_copy_sql(archive), archive_file)
    await _export_copy(driver, _membership_copy_sql(schema), membership_file)
    await _export_copy(driver, _alias_copy_sql(schema), aliases_file)
    config_file.write(
        json.dumps(
            {
                "run_id": run_id,
                "state_code": state_code,
                "zip_prefix": zip_prefix,
                "retry_shadow_run_id": retry_shadow_run_id,
            },
            sort_keys=True,
        ).encode("utf-8")
    )


async def _copy_native_shadow_candidates(
    driver: Any,
    binary: Path,
    schema: str,
    native_paths: tuple[Path, ...],
    native_files: tuple[BinaryIO, ...],
    *,
    cleanup_deadline_monotonic: float | None = None,
) -> dict[str, Any]:
    (
        archive_path,
        membership_path,
        aliases_path,
        config_path,
        candidates_path,
        summary_path,
    ) = native_paths
    candidate_file, summary_file = native_files[-2:]
    await _run_scanner(
        binary,
        (
            archive_path,
            membership_path,
            aliases_path,
            config_path,
            candidates_path,
            summary_path,
        ),
        tuple(stream.fileno() for stream in native_files),
        cleanup_deadline_monotonic=cleanup_deadline_monotonic,
    )
    summary_payload = _validated_summary_file(summary_file)
    if _sha256_file(candidate_file) != summary_payload["output_sha256"]:
        raise RuntimeError("Rust address evidence output digest differs from summary")
    candidate_file.seek(0)
    status = await driver.copy_to_table(
        ADDRESS_ALIAS_CANDIDATE_TABLE,
        schema_name=schema,
        source=candidate_file,
        columns=_CANDIDATE_COLUMNS,
        format="text",
        delimiter="\t",
        null="\\N",
    )
    copied_count = _copied_count(status)
    if copied_count != summary_payload["candidate_rows"]:
        raise RuntimeError("Rust address evidence candidate COPY count differs from summary")
    return summary_payload


async def _run_anonymous_native_shadow(
    driver: Any,
    binary: Path,
    schema: str,
    scratch_directory: Path,
    descriptor_root: Path,
    scope_by_field: dict[str, str | None],
    cleanup_deadline_monotonic: float | None,
) -> dict[str, Any]:
    """Run the scanner against anonymous files retained through candidate COPY."""
    with ExitStack() as native_stack:
        native_files = tuple(
            native_stack.enter_context(
                tempfile.TemporaryFile(mode="w+b", dir=scratch_directory)
            )
            for _ in range(6)
        )
        native_paths = tuple(
            descriptor_root / str(stream.fileno()) for stream in native_files
        )
        await _export_native_shadow_files(
            driver,
            archive=str(scope_by_field["archive"]),
            schema=schema,
            run_id=str(scope_by_field["run_id"]),
            state_code=scope_by_field["state_code"],
            zip_prefix=scope_by_field["zip_prefix"],
            retry_shadow_run_id=scope_by_field["retry_shadow_run_id"],
            native_files=native_files,
        )
        for stream in native_files:
            stream.flush()
            stream.seek(0)
        return await _copy_native_shadow_candidates(
            driver,
            binary,
            schema,
            native_paths,
            native_files,
            cleanup_deadline_monotonic=cleanup_deadline_monotonic,
        )


async def try_native_evidence_shadow(
    session: Any,
    *,
    schema: str,
    archive: str,
    run_id: str,
    state_code: str | None,
    zip_prefix: str | None,
    retry_shadow_run_id: str | None,
    cleanup_deadline_monotonic: float | None = None,
) -> dict[str, Any] | None:
    """Derive and COPY one exact candidate set, or return None when unavailable."""

    if not _is_native_enabled():
        return None
    started = time.monotonic()
    binary = _ptg2_rust_scanner_binary()
    if binary is None or not await _is_native_version_current(
        binary,
        cleanup_deadline_monotonic=cleanup_deadline_monotonic,
    ):
        _warn_native_fallback("native address evidence matcher unavailable")
        return None
    driver = await _driver(session)
    if driver is None:
        _warn_native_fallback("database driver lacks COPY support")
        return None
    scratch_directory = _native_scratch_directory()
    if scratch_directory is None:
        _warn_native_fallback("native address evidence scratch volume unavailable")
        return None
    descriptor_root = _native_descriptor_root()
    if descriptor_root is None:
        _warn_native_fallback("native file-descriptor transport unavailable")
        return None
    summary_payload = await _run_anonymous_native_shadow(
        driver,
        binary,
        schema,
        scratch_directory,
        descriptor_root,
        {
            "archive": archive,
            "run_id": run_id,
            "state_code": state_code,
            "zip_prefix": zip_prefix,
            "retry_shadow_run_id": retry_shadow_run_id,
        },
        cleanup_deadline_monotonic,
    )
    summary_payload["wall_elapsed_ms"] = round((time.monotonic() - started) * 1000)
    logger.info("native address evidence shadow receipt=%s", summary_payload)
    return summary_payload


__all__ = ["try_native_evidence_shadow"]
