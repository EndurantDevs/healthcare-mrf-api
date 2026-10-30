# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic PostgreSQL admission for one completely replayed NPPES archive."""

from __future__ import annotations

import asyncio
import inspect
import os
import re
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from db.models import db
from process import nppes_public_evidence_storage as storage
from process import provider_directory_generic_source_policy as source_policy
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_catalog import assert_nppes_candidate_catalog
from process.nppes_public_evidence_import import NppesEvidenceRuntimeConfig
from process.nppes_public_evidence_members import NppesPrimaryCsvRows
from process.nppes_public_evidence_prepared_chunks import PreparedNppesRow, prepare_nppes_row
from process.nppes_public_evidence_replay import (
    PreparedNppesRegistryReplay,
    validate_prepared_nppes_registry_replay,
)
from process.nppes_public_evidence_rows import (
    NppesRegistryDatabaseRowEncoder,
)
from process.nppes_public_evidence_writer_contract import (
    NppesPublicEvidenceWriterError,
    NppesRegistryAdmissionReceipt,
    writer_error,
)
from public_evidence.nppes_registry_storage_contract import NppesRegistryMemberEncoder

_SCHEMA_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)
_DEFAULT_BATCH_SIZE = 20_000
CancelCheck = Callable[[], Awaitable[None] | None]
ProgressCallback = Callable[[int], Awaitable[None] | None]


@dataclass(slots=True)
class _StageBuffers:
    source_records: list[PreparedNppesRow] = field(default_factory=list)
    members: list[PreparedNppesRow] = field(default_factory=list)
    common_rows: list[PreparedNppesRow] = field(default_factory=list)
    source_links: list[PreparedNppesRow] = field(default_factory=list)
    typed_rows: list[PreparedNppesRow] = field(default_factory=list)

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
        f"INSERT INTO {_qualified(schema, table)} ({column_sql}) VALUES ({placeholders}){conflict}",
        *values,
    )
    if allow_identical:
        identity_ref = values[0]
        stored = await connection.fetchrow(
            f"SELECT {column_sql} FROM {_qualified(schema, table)} WHERE {_quoted(columns[0])}=$1",
            identity_ref,
        )
        if stored is None or tuple(stored[column] for column in columns) != values:
            raise writer_error()


async def _candidate_catalog(database, schema):
    async with source_policy.source_transaction(database):
        publisher = source_policy.bound_publisher()
        if schema != publisher.policy.schema:
            raise writer_error()
        await assert_nppes_candidate_catalog(publisher.connection, schema, publisher.policy)


async def _admit(
    prepared: PreparedNppesRegistryReplay,
    config: NppesEvidenceRuntimeConfig,
    schema: str,
    database: object,
    cancel_check: CancelCheck | None,
    progress: ProgressCallback | None,
) -> NppesRegistryAdmissionReceipt:
    await _invoke(cancel_check)
    producer_identity = source_policy.source_producer_identity()
    await _candidate_catalog(database, schema)
    candidate = await storage.prepare_nppes_evidence_candidate(database, prepared, producer_identity=producer_identity)
    expected_by_root = {
        root: prepared.manifest.source_record_count
        if root in {"source_record", "member"}
        else prepared.manifest.projected_record_count
        for root in storage.ROOTS
    }
    if (
        type(candidate) is not storage.NppesEvidenceCandidate
        or candidate.phase not in {"loading", "prepared", "published"}
        or type(candidate.ordinal) is not int
        or not 0 <= candidate.ordinal <= prepared.manifest.source_record_count
        or type(candidate.counts) is not dict
        or set(candidate.counts) != set(expected_by_root)
        or any(
            type(identity_value) is not int or not 0 <= identity_value <= expected_by_root[root]
            for root, identity_value in candidate.counts.items()
        )
        or candidate.counts["source_record"] != candidate.ordinal
        or candidate.counts["member"] != candidate.ordinal
        or not candidate.counts["common"]
        == candidate.counts["source_link"]
        == candidate.counts["typed"]
        <= candidate.ordinal
    ):
        raise writer_error()
    if candidate.phase == "published":
        if candidate.counts != expected_by_root:
            raise writer_error()
        await _invoke(cancel_check)
        await _candidate_catalog(database, schema)
        return _receipt(prepared, "already_present")
    if candidate.counts != expected_by_root:
        if candidate.ordinal == prepared.manifest.source_record_count:
            raise writer_error()
        candidate = await _load_candidate(database, candidate, prepared, cancel_check=cancel_check, progress=progress)
    await _invoke(cancel_check)
    candidate = await storage.prepare_nppes_evidence_publication(
        database, candidate, prepared, cancel_check=cancel_check
    )
    state = await _publish_candidate(database, schema, candidate, prepared, cancel_check)
    if state not in {"inserted", "already_present"}:
        raise writer_error()
    return _receipt(prepared, state)


async def _publish_candidate(database, schema, candidate, prepared, cancel_check):
    from process.nppes_isolated_catalog import issued_isolated_nppes_catalog

    if issued_isolated_nppes_catalog() is not None:
        await _candidate_catalog(database, schema)
        state = await storage.publish_nppes_evidence_candidate(database, candidate, prepared, cancel_check=cancel_check)
    else:
        async with source_policy.source_transaction(database):
            await _candidate_catalog(database, schema)
            state = await storage.publish_nppes_evidence_candidate(
                database, candidate, prepared, cancel_check=cancel_check
            )
    return state


async def _flush_candidate(database, candidate, buffers, *, cancel_check, progress):
    if not buffers.members:
        return candidate
    expected_by_root = {
        root: candidate.counts[root] + len(getattr(buffers, fields[2])) for root, fields in storage.ROOTS.items()
    }
    updated = await storage.append_nppes_evidence_batch(database, candidate, buffers, cancel_check=cancel_check)
    if updated.ordinal != expected_by_root["member"] or updated.counts != expected_by_root:
        raise writer_error()
    buffers.clear()
    await _invoke(progress, updated.ordinal)
    await _invoke(cancel_check)
    await asyncio.sleep(0)
    return updated


async def _load_candidate(database, candidate, prepared, *, cancel_check, progress):
    encoder = NppesRegistryMemberEncoder(prepared.manifest, prepared.header, prepared.archive_observation)
    database_rows = NppesRegistryDatabaseRowEncoder(prepared.manifest.release, prepared.admission_row)
    prefix = candidate.ordinal
    prefix_counts = dict.fromkeys(storage.ROOTS, 0)
    buffers = _StageBuffers()
    batch_size = min(_batch_size(), storage.COPY_MAX_ROWS)
    batch_bytes = storage.COPY_FRAME_BYTES + 2 * len(storage.ROOTS)
    with NppesPrimaryCsvRows(prepared.archive) as csv_source:
        if csv_source.header != prepared.header:
            raise writer_error()
        for ordinal, raw_row in enumerate(csv_source, start=1):
            encoded, member = encoder.encode(raw_row)
            if ordinal <= prefix:
                prefix_counts["source_record"] += 1
                prefix_counts["member"] += 1
                if encoded.common_row is not None:
                    prefix_counts["common"] += 1
                    prefix_counts["source_link"] += 1
                    prefix_counts["typed"] += 1
                if ordinal == prefix and prefix_counts != candidate.counts:
                    raise writer_error()
                if ordinal == prefix or ordinal % batch_size == 0:
                    await _invoke(progress, ordinal)
                    await _invoke(cancel_check)
                    await asyncio.sleep(0)
                continue
            rows_by_root = {
                "source_record": database_rows.source_record(encoded.source_record),
                "member": database_rows.member(member),
            }
            projected = database_rows.projected(encoded)
            if projected is not None:
                rows_by_root.update(zip(("common", "source_link", "typed"), projected))
            prepared_by_root = {
                root: prepare_nppes_row(root, prepared_row) for root, prepared_row in rows_by_root.items()
            }
            size = sum(
                max(storage._record_bytes(prepared_row.values), len(prepared_row.payload) + 1)
                for prepared_row in prepared_by_root.values()
            )
            if size + storage.COPY_FRAME_BYTES + 2 * len(storage.ROOTS) > storage.COPY_MAX_BYTES:
                raise writer_error()
            if buffers.members and (len(buffers.members) >= batch_size or batch_bytes + size > storage.COPY_MAX_BYTES):
                candidate = await _flush_candidate(
                    database, candidate, buffers, cancel_check=cancel_check, progress=progress
                )
                batch_bytes = storage.COPY_FRAME_BYTES + 2 * len(storage.ROOTS)
            for root, prepared_row in prepared_by_root.items():
                getattr(buffers, storage.ROOTS[root][2]).append(prepared_row)
            batch_bytes += size
    if encoder.finish() != prepared.manifest:
        raise writer_error()
    return await _flush_candidate(database, candidate, buffers, cancel_check=cancel_check, progress=progress)


async def admit_nppes_registry_archive(
    prepared: object,
    config: object,
    *,
    schema: str = "mrf",
    database: object = db,
    cancel_check: CancelCheck | None = None,
    progress: ProgressCallback | None = None,
) -> NppesRegistryAdmissionReceipt:
    """Resume a protected candidate, then atomically publish its verified family."""

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
    except asyncio.CancelledError, ImportCancelledError:
        raise
    except Exception:
        normalized_error = writer_error()
    else:
        return admission_receipt
    raise normalized_error


__all__ = ("NppesPublicEvidenceWriterError", "NppesRegistryAdmissionReceipt", "admit_nppes_registry_archive")
