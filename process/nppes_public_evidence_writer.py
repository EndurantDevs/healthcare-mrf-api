# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic PostgreSQL admission for one completely replayed NPPES archive."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import inspect
import os
import re
from typing import Awaitable, Callable

from db.models import db
from public_evidence.nppes_registry_storage_contract import NppesRegistryMemberEncoder
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_import import NppesEvidenceRuntimeConfig
from process.nppes_public_evidence_catalog import assert_nppes_admission_catalog
from process.nppes_public_evidence_members import NppesPrimaryCsvRows
from process.nppes_public_evidence_replay import (
    PreparedNppesRegistryReplay,
    validate_prepared_nppes_registry_replay,
)
from process.nppes_public_evidence_rows import (
    ADMISSION_COLUMNS,
    COMMON_COLUMNS,
    MEMBER_COLUMNS,
    NPI_ENUMERATION_COLUMNS,
    SOURCE_IDENTITY_COLUMNS,
    SOURCE_LINK_COLUMNS,
    SOURCE_RECORD_COLUMNS,
    SOURCE_RELEASE_COLUMNS,
    NppesRegistryDatabaseRowEncoder,
    admission_values,
    source_identity_values,
    source_release_values,
)
from process.nppes_public_evidence_writer_contract import (
    NppesPublicEvidenceWriterError,
    NppesRegistryAdmissionReceipt,
    writer_error,
)


_SCHEMA_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)
_DEFAULT_BATCH_SIZE = 20_000
_TABLE_BY_STAGE = {
    "nppes_stage_source_record": "public_evidence_source_record",
    "nppes_stage_member": "public_evidence_nppes_registry_member",
    "nppes_stage_common": "public_evidence_record",
    "nppes_stage_source_link": "public_evidence_record_source_link",
    "nppes_stage_typed": "public_evidence_npi_enumeration",
}
_COLUMNS_BY_STAGE = {
    "nppes_stage_source_record": SOURCE_RECORD_COLUMNS,
    "nppes_stage_member": MEMBER_COLUMNS,
    "nppes_stage_common": COMMON_COLUMNS,
    "nppes_stage_source_link": SOURCE_LINK_COLUMNS,
    "nppes_stage_typed": NPI_ENUMERATION_COLUMNS,
}
CancelCheck = Callable[[], Awaitable[None] | None]
ProgressCallback = Callable[[int], Awaitable[None] | None]


@dataclass(slots=True)
class _StageBuffers:
    source_records: list[tuple[object, ...]] = field(default_factory=list)
    members: list[tuple[object, ...]] = field(default_factory=list)
    common_rows: list[tuple[object, ...]] = field(default_factory=list)
    source_links: list[tuple[object, ...]] = field(default_factory=list)
    typed_rows: list[tuple[object, ...]] = field(default_factory=list)

    def clear(self) -> None:
        """Discard every row after a successful batch COPY."""

        self.source_records.clear()
        self.members.clear()
        self.common_rows.clear()
        self.source_links.clear()
        self.typed_rows.clear()


def _schema_name(value: object) -> str:
    if type(value) is not str or _SCHEMA_RE.fullmatch(value) is None:
        raise writer_error()
    return value


def _quoted(identifier: str) -> str:
    return f'"{identifier}"'


def _qualified(schema: str, table: str) -> str:
    return f"{_quoted(schema)}.{_quoted(table)}"


async def _invoke(callback: Callable[..., object] | None, *args: object) -> None:
    if callback is None:
        return
    result = callback(*args)
    if inspect.isawaitable(result):
        await result


def _batch_size() -> int:
    raw = os.getenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_BATCH_SIZE", "")
    if not raw:
        return _DEFAULT_BATCH_SIZE
    if not raw.isascii() or not raw.isdigit():
        raise writer_error()
    value = int(raw)
    if not 100 <= value <= 100_000:
        raise writer_error()
    return value


def _receipt(prepared: PreparedNppesRegistryReplay, write_state: str) -> NppesRegistryAdmissionReceipt:
    admission = prepared.admission_row
    return NppesRegistryAdmissionReceipt(
        admission_ref=admission.admission_ref,
        source_release_ref=admission.source_release_ref,
        artifact_sha256=admission.artifact_sha256,
        manifest_sha256=admission.manifest_sha256,
        source_record_count=admission.source_record_count,
        projected_record_count=admission.projected_record_count,
        excluded_record_count=admission.excluded_record_count,
        write_state=write_state,
    )


async def _assert_catalog(connection: object, schema: str) -> None:
    await assert_nppes_admission_catalog(connection, schema)


async def _drop_stages(connection: object) -> None:
    await connection.execute(
        "DROP TABLE IF EXISTS "
        + ", ".join(f"pg_temp.{_quoted(stage)}" for stage in _TABLE_BY_STAGE)
    )


async def _drain_stage_cleanup(connection: object) -> None:
    cleanup_task = asyncio.create_task(_drop_stages(connection))
    current_task = asyncio.current_task()
    intercepted_cancellations = 0
    cleanup_failure = None
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            intercepted_cancellations += 1
            if current_task is not None:
                current_task.uncancel()
        except BaseException as failure:
            cleanup_failure = failure
            break
    if cleanup_failure is None:
        try:
            cleanup_task.result()
        except BaseException as failure:
            cleanup_failure = failure
    outstanding_cancellations = current_task.cancelling() if current_task else 0
    if current_task is not None:
        for _ in range(outstanding_cancellations):
            current_task.uncancel()
        for _ in range(intercepted_cancellations + outstanding_cancellations):
            current_task.cancel()
    if intercepted_cancellations or outstanding_cancellations:
        raise asyncio.CancelledError
    if cleanup_failure is not None:
        raise cleanup_failure


async def _create_stages(connection: object, schema: str) -> None:
    await _drop_stages(connection)
    for stage_name, table_name in _TABLE_BY_STAGE.items():
        await connection.execute(
            f"CREATE TEMP TABLE {_quoted(stage_name)} "
            f"(LIKE {_qualified(schema, table_name)} INCLUDING DEFAULTS "
            "INCLUDING CONSTRAINTS) ON COMMIT PRESERVE ROWS"
        )
    index_statements = (
        "CREATE UNIQUE INDEX ON nppes_stage_source_record (source_record_ref)",
        "CREATE UNIQUE INDEX ON nppes_stage_member (source_release_ref, source_row_ordinal)",
        "CREATE UNIQUE INDEX ON nppes_stage_member (source_release_ref, npi)",
        "CREATE UNIQUE INDEX ON nppes_stage_member (source_release_ref, source_record_ref)",
        "CREATE UNIQUE INDEX ON nppes_stage_common (evidence_ref)",
        "CREATE UNIQUE INDEX ON nppes_stage_source_link (evidence_ref)",
        "CREATE UNIQUE INDEX ON nppes_stage_typed (evidence_ref)",
    )
    for statement in index_statements:
        await connection.execute(statement)


async def _copy_stage(
    connection: object,
    stage_name: str,
    records: list[tuple[object, ...]],
) -> None:
    if not records:
        return
    status = await connection.copy_records_to_table(
        stage_name,
        columns=_COLUMNS_BY_STAGE[stage_name],
        records=records,
    )
    if (
        type(status) is not str
        or re.fullmatch(r"COPY [0-9]+", status) is None
        or int(status.removeprefix("COPY ")) != len(records)
    ):
        raise writer_error()


async def _flush_buffers(connection: object, buffers: _StageBuffers) -> None:
    await _copy_stage(connection, "nppes_stage_source_record", buffers.source_records)
    await _copy_stage(connection, "nppes_stage_member", buffers.members)
    await _copy_stage(connection, "nppes_stage_common", buffers.common_rows)
    await _copy_stage(connection, "nppes_stage_source_link", buffers.source_links)
    await _copy_stage(connection, "nppes_stage_typed", buffers.typed_rows)
    buffers.clear()


async def _stage_complete_replay(
    connection: object,
    prepared: PreparedNppesRegistryReplay,
    *,
    cancel_check: CancelCheck | None,
    progress: ProgressCallback | None,
) -> None:
    member_encoder = NppesRegistryMemberEncoder(
        prepared.manifest,
        prepared.header,
        prepared.archive_observation,
    )
    database_rows = NppesRegistryDatabaseRowEncoder(
        prepared.manifest.release,
        prepared.admission_row,
    )
    stage_buffers = _StageBuffers()
    batch_size = _batch_size()
    with NppesPrimaryCsvRows(prepared.archive) as primary_rows:
        if primary_rows.header != prepared.header:
            raise writer_error()
        for row_count, row_values in enumerate(primary_rows, start=1):
            encoded, member = member_encoder.encode(row_values)
            stage_buffers.source_records.append(
                database_rows.source_record(encoded.source_record)
            )
            stage_buffers.members.append(database_rows.member(member))
            projected_rows = database_rows.projected(encoded)
            if projected_rows is not None:
                common_row, source_link, typed_row = projected_rows
                stage_buffers.common_rows.append(common_row)
                stage_buffers.source_links.append(source_link)
                stage_buffers.typed_rows.append(typed_row)
            if row_count % batch_size == 0:
                await _flush_buffers(connection, stage_buffers)
                await _invoke(cancel_check)
                await _invoke(progress, row_count)
                await asyncio.sleep(0)
        await _flush_buffers(connection, stage_buffers)
    replayed = member_encoder.finish()
    if replayed.manifest_sha256 != prepared.manifest.manifest_sha256:
        raise writer_error()


async def _assert_stage_counts(
    connection: object,
    prepared: PreparedNppesRegistryReplay,
) -> None:
    expected_count_by_stage = {
        "nppes_stage_source_record": prepared.manifest.source_record_count,
        "nppes_stage_member": prepared.manifest.source_record_count,
        "nppes_stage_common": prepared.manifest.projected_record_count,
        "nppes_stage_source_link": prepared.manifest.projected_record_count,
        "nppes_stage_typed": prepared.manifest.projected_record_count,
    }
    for stage_name, expected_count in expected_count_by_stage.items():
        actual_count = await connection.fetchval(
            f"SELECT count(*) FROM {_quoted(stage_name)}"
        )
        if actual_count != expected_count:
            raise writer_error()


async def _has_existing_admission(
    connection: object,
    schema: str,
    prepared: PreparedNppesRegistryReplay,
) -> bool:
    column_sql = ", ".join(_quoted(column) for column in ADMISSION_COLUMNS)
    row = await connection.fetchrow(
        f"SELECT {column_sql} FROM "
        f"{_qualified(schema, 'public_evidence_nppes_registry_admission')} "
        "WHERE source_release_ref=$1",
        prepared.manifest.release.source_release_ref,
    )
    if row is None:
        return False
    if tuple(row[column] for column in ADMISSION_COLUMNS) != admission_values(
        prepared.admission_row
    ):
        raise writer_error()
    sealed = await connection.fetchval(
        f"SELECT EXISTS (SELECT 1 FROM "
        f"{_qualified(schema, 'public_evidence_nppes_registry_admission_seal')} "
        "WHERE admission_ref=$1)",
        prepared.admission_row.admission_ref,
    )
    if sealed is not True:
        raise writer_error()
    return True


async def _assert_no_partial_release(
    connection: object,
    schema: str,
    prepared: PreparedNppesRegistryReplay,
) -> None:
    exists = await connection.fetchval(
        f"SELECT EXISTS (SELECT 1 FROM "
        f"{_qualified(schema, 'public_evidence_source_release')} "
        "WHERE source_release_ref=$1)",
        prepared.manifest.release.source_release_ref,
    )
    if exists:
        raise writer_error()


async def _insert_single(
    connection: object,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    values: tuple[object, ...],
    *,
    allow_identical: bool = False,
) -> None:
    column_sql = ", ".join(_quoted(column) for column in columns)
    placeholders = ", ".join(f"${index}" for index in range(1, len(values) + 1))
    conflict = " ON CONFLICT DO NOTHING" if allow_identical else ""
    await connection.execute(
        f"INSERT INTO {_qualified(schema, table)} ({column_sql}) "
        f"VALUES ({placeholders}){conflict}",
        *values,
    )
    if allow_identical:
        identity_ref = values[0]
        stored = await connection.fetchrow(
            f"SELECT {column_sql} FROM {_qualified(schema, table)} "
            f"WHERE {_quoted(columns[0])}=$1",
            identity_ref,
        )
        if stored is None or tuple(stored[column] for column in columns) != values:
            raise writer_error()


async def _insert_stage(
    connection: object,
    schema: str,
    stage_name: str,
) -> None:
    columns = _COLUMNS_BY_STAGE[stage_name]
    column_sql = ", ".join(_quoted(column) for column in columns)
    table = _TABLE_BY_STAGE[stage_name]
    await connection.execute(
        f"INSERT INTO {_qualified(schema, table)} ({column_sql}) "
        f"SELECT {column_sql} FROM {_quoted(stage_name)}"
    )


async def _analyze_admission_tables(connection: object, schema: str) -> None:
    """Refresh bulk-load statistics before planning the deferred set validator."""

    await connection.execute(
        "ANALYZE "
        + ", ".join(
            _qualified(schema, table_name)
            for table_name in _TABLE_BY_STAGE.values()
        )
    )


async def _finalize(
    connection: object,
    schema: str,
    prepared: PreparedNppesRegistryReplay,
) -> str:
    release = prepared.manifest.release
    async with connection.transaction():
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended("
            "'healthporta.public-evidence-nppes-admission:' || $1, 0))",
            release.source_release_ref,
        )
        if await _has_existing_admission(connection, schema, prepared):
            return "already_present"
        await _assert_no_partial_release(connection, schema, prepared)
        await _insert_single(
            connection,
            schema,
            "public_evidence_source_identity",
            SOURCE_IDENTITY_COLUMNS,
            source_identity_values(release),
            allow_identical=True,
        )
        await _insert_single(
            connection,
            schema,
            "public_evidence_source_release",
            SOURCE_RELEASE_COLUMNS,
            source_release_values(release),
        )
        await _insert_single(
            connection,
            schema,
            "public_evidence_nppes_registry_admission",
            ADMISSION_COLUMNS,
            admission_values(prepared.admission_row),
        )
        for stage_name in (
            "nppes_stage_source_record",
            "nppes_stage_common",
            "nppes_stage_source_link",
            "nppes_stage_typed",
            "nppes_stage_member",
        ):
            await _insert_stage(connection, schema, stage_name)
        await _analyze_admission_tables(connection, schema)
        await connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    return "inserted"


async def _admit(
    prepared: PreparedNppesRegistryReplay,
    config: NppesEvidenceRuntimeConfig,
    schema: str,
    database: object,
    cancel_check: CancelCheck | None,
    progress: ProgressCallback | None,
) -> NppesRegistryAdmissionReceipt:
    async with database.acquire_driver() as connection:
        await _assert_catalog(connection, schema)
        if await _has_existing_admission(connection, schema, prepared):
            return _receipt(prepared, "already_present")
        await _assert_no_partial_release(connection, schema, prepared)
        try:
            await _create_stages(connection, schema)
            await _stage_complete_replay(
                connection,
                prepared,
                cancel_check=cancel_check,
                progress=progress,
            )
            await _assert_stage_counts(connection, prepared)
            await _invoke(cancel_check)
            state = await _finalize(connection, schema, prepared)
        finally:
            await _drain_stage_cleanup(connection)
    return _receipt(prepared, state)


async def admit_nppes_registry_archive(
    prepared: object,
    config: object,
    *,
    schema: str = "mrf",
    database: object = db,
    cancel_check: CancelCheck | None = None,
    progress: ProgressCallback | None = None,
) -> NppesRegistryAdmissionReceipt:
    """Stage a complete replay, then publish all immutable rows atomically."""

    try:
        if type(config) is not NppesEvidenceRuntimeConfig:
            raise writer_error()
        fixed = validate_prepared_nppes_registry_replay(prepared, config)
        admission_receipt = await _admit(
            fixed,
            config,
            _schema_name(schema),
            database,
            cancel_check,
            progress,
        )
    except (asyncio.CancelledError, ImportCancelledError):
        raise
    except Exception:
        normalized_error = writer_error()
    else:
        return admission_receipt
    raise normalized_error


__all__ = ("NppesPublicEvidenceWriterError", "NppesRegistryAdmissionReceipt", "admit_nppes_registry_archive")
