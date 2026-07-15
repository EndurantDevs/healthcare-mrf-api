# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bulk publication primitives for strict shared_blocks_v3 PTG snapshots."""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedBlock,
    SharedBlockReference,
    lock_shared_layout_for_dense_write,
)
from process.ptg_parts.ptg2_shared_finalize import validate_v3_finalizer_summary
from process.ptg_parts.ptg2_shared_graph import SharedGraphConversionResult
from process.ptg_parts.ptg2_shared_graph import (
    MembershipArtifact,
    SharedGraphShardBundle,
)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHARED_BLOCK_STAGE_COLUMNS = (
    "block_hash",
    "format_version",
    "object_kind",
    "block_key",
    "fragment_no",
    "entry_count",
    "codec",
    "raw_byte_count",
    "stored_byte_count",
    "payload",
)


@dataclass(frozen=True)
class SharedBlockStagePublication:
    references: tuple[SharedBlockReference, ...]
    mapping_count: int
    unique_block_count: int
    logical_byte_count: int
    stored_byte_count: int


@dataclass(frozen=True)
class SharedGraphPublication:
    references: tuple[SharedBlockReference, ...]
    block_count: int
    owner_count: int
    provider_group_count: int
    npi_count: int
    support_digest: bytes
    logical_byte_count: int
    stored_byte_count: int


@dataclass(frozen=True)
class SharedDictionaryPublication:
    code_count: int
    provider_set_count: int
    serving_rate_count: int
    support_digest: bytes


def shared_graph_bundles_from_artifacts(
    artifact_entries: Iterable[dict[str, Any]],
) -> tuple[SharedGraphShardBundle, ...]:
    """Group four validated source directions into deterministic graph shards."""

    field_by_name = {
        "provider_group_npi": "group_npi",
        "provider_npi_group": "npi_group",
        "provider_inverted": "group_provider_set",
        "provider_forward": "provider_set_group",
    }
    artifact_by_shard: dict[str, dict[str, MembershipArtifact]] = {}
    for raw_entry in artifact_entries:
        if not isinstance(raw_entry, dict):
            continue
        name = str(raw_entry.get("name") or raw_entry.get("kind") or "").strip()
        field_name = field_by_name.get(name)
        if field_name is None:
            continue
        shard_id = str(
            raw_entry.get("source_shard_id") or raw_entry.get("shard_id") or ""
        ).strip()
        path = str(raw_entry.get("path") or "").strip()
        if not shard_id or not path:
            raise RuntimeError(f"strict V3 graph artifact {name!r} lacks shard/path metadata")
        shard = artifact_by_shard.setdefault(shard_id, {})
        if field_name in shard:
            raise RuntimeError(
                f"strict V3 graph shard {shard_id!r} repeats direction {name!r}"
            )
        shard[field_name] = MembershipArtifact(Path(path), dict(raw_entry))

    required_fields = (
        "group_npi",
        "npi_group",
        "group_provider_set",
        "provider_set_group",
    )
    bundles: list[SharedGraphShardBundle] = []
    for shard_id, shard in sorted(artifact_by_shard.items()):
        missing = [field_name for field_name in required_fields if field_name not in shard]
        if missing:
            raise RuntimeError(
                f"strict V3 graph shard {shard_id!r} is incomplete: missing {', '.join(missing)}"
            )
        bundles.append(
            SharedGraphShardBundle(
                shard_id=shard_id,
                group_npi=shard["group_npi"],
                npi_group=shard["npi_group"],
                group_provider_set=shard["group_provider_set"],
                provider_set_group=shard["provider_set_group"],
            )
        )
    if not bundles:
        raise RuntimeError("strict V3 publish is missing provider membership graph artifacts")
    return tuple(bundles)


def _safe_identifier(value: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"unsafe PostgreSQL identifier: {value!r}")
    return normalized


def _validated_coverage_scope_id(value: Any) -> bytes:
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ValueError("strict V3 expected coverage scope id must contain exactly 32 bytes")
    normalized = bytes(value)
    if len(normalized) != 32:
        raise ValueError("strict V3 expected coverage scope id must contain exactly 32 bytes")
    return normalized


def shared_block_stage_name(build_token: str) -> str:
    """Return a bounded stage-table name from a sanitized token or random fallback."""

    token = re.sub(r"[^a-z0-9]+", "", str(build_token or "").lower())[:20]
    if not token:
        token = uuid.uuid4().hex[:20]
    return f"ptg2_v3_block_stage_{token}"


async def create_shared_block_stage(*, schema_name: str, stage_table: str) -> None:
    """Drop and recreate the unlogged shared-block COPY staging table."""

    schema = _quote_ident(_safe_identifier(schema_name))
    table = _quote_ident(_safe_identifier(stage_table))
    await db.status(f"DROP TABLE IF EXISTS {schema}.{table};")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {schema}.{table} (
            block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
            format_version smallint NOT NULL
                CHECK (format_version = {PTG2_V3_SHARED_FORMAT_VERSION}),
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL CHECK (block_key >= 0),
            fragment_no integer NOT NULL CHECK (fragment_no >= 0),
            entry_count bigint NOT NULL CHECK (entry_count >= 0),
            codec varchar(16) NOT NULL CHECK (codec IN ('none', 'zlib')),
            raw_byte_count bigint NOT NULL CHECK (raw_byte_count >= 0),
            stored_byte_count bigint NOT NULL CHECK (stored_byte_count >= 0),
            payload bytea NOT NULL CHECK (octet_length(payload) = stored_byte_count)
        );
        """
    )


async def copy_shared_block_binary_file(
    copy_path: str | Path,
    *,
    schema_name: str,
    stage_table: str,
) -> None:
    """Validate and binary-COPY a shared-block file into the staging table."""

    path = Path(copy_path)
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(f"strict V3 shared-block COPY is missing or empty: {path}")
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("active database driver does not expose binary COPY")
        with path.open("rb") as source:
            await copy_to_table(
                _safe_identifier(stage_table),
                source=source,
                schema_name=_safe_identifier(schema_name),
                columns=list(_SHARED_BLOCK_STAGE_COLUMNS),
                format="binary",
            )


def _required_summary_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"strict V3 finalizer summary is missing {name}")
    return value


def _required_summary_integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}") from exc
    if normalized < 0:
        raise RuntimeError(f"strict V3 finalizer summary has negative {name}")
    return normalized


def _finalizer_output_file(output_directory: Path, raw_path: Any, name: str) -> Path:
    relative_path = Path(str(raw_path or ""))
    if not str(relative_path) or relative_path.is_absolute():
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name} path")
    output_root = output_directory.resolve()
    path = (output_root / relative_path).resolve()
    try:
        path.relative_to(output_root)
    except ValueError as exc:
        raise RuntimeError(f"strict V3 finalizer {name} path escapes its output directory") from exc
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(f"strict V3 finalizer {name} output is missing or empty")
    return path


async def _copy_binary_file_to_stage(
    path: Path,
    *,
    schema_name: str,
    stage_table: str,
    columns: Sequence[str],
) -> None:
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("active database driver does not expose binary COPY")
        with path.open("rb") as source:
            await copy_to_table(
                _safe_identifier(stage_table),
                source=source,
                schema_name=_safe_identifier(schema_name),
                columns=list(columns),
                format="binary",
            )


async def _copy_text_file_to_stage(
    path: Path,
    *,
    schema_name: str,
    stage_table: str,
    columns: Sequence[str],
) -> None:
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("active database driver does not expose text COPY")
        with path.open("rb") as source:
            await copy_to_table(
                _safe_identifier(stage_table),
                source=source,
                schema_name=_safe_identifier(schema_name),
                columns=list(columns),
                format="text",
            )


def _provider_set_metadata_files(
    entries: Iterable[Mapping[str, Any]],
    *,
    required: bool,
) -> tuple[tuple[Path, int], ...]:
    files: list[tuple[Path, int]] = []
    seen_paths: set[Path] = set()
    for raw_entry in entries:
        if not isinstance(raw_entry, Mapping):
            raise RuntimeError("strict V3 provider-set metadata entry must be an object")
        path = Path(str(raw_entry.get("path") or "")).resolve()
        if path in seen_paths or not path.is_file() or path.stat().st_size <= 0:
            raise RuntimeError("strict V3 provider-set metadata file is missing or repeated")
        raw_row_count = raw_entry.get("row_count")
        if isinstance(raw_row_count, bool):
            raise RuntimeError("strict V3 provider-set metadata row count is invalid")
        try:
            row_count = int(raw_row_count)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "strict V3 provider-set metadata row count is invalid"
            ) from exc
        if row_count <= 0:
            raise RuntimeError("strict V3 provider-set metadata file must contain rows")
        seen_paths.add(path)
        files.append((path, row_count))
    if required and not files:
        raise RuntimeError("strict V3 provider-set metadata is required")
    return tuple(files)


async def publish_shared_finalizer_dictionaries(
    finalizer_summary: dict[str, Any],
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    expected_coverage_scope_id: bytes,
    provider_set_metadata_entries: Iterable[dict[str, Any]],
) -> SharedDictionaryPublication:
    """Load small fixed dictionaries while keeping the large projections block-only."""

    coverage_scope_id = _validated_coverage_scope_id(expected_coverage_scope_id)
    finalizer_summary = validate_v3_finalizer_summary(finalizer_summary)
    output_directory = Path(str(finalizer_summary.get("output_directory") or ""))
    dictionaries = _required_summary_mapping(
        finalizer_summary.get("dictionaries"),
        "dictionaries",
    )
    code_summary = _required_summary_mapping(dictionaries.get("code"), "code dictionary")
    provider_summary = _required_summary_mapping(
        dictionaries.get("provider_set"),
        "provider-set dictionary",
    )
    preservation = _required_summary_mapping(
        finalizer_summary.get("preservation"),
        "preservation",
    )
    code_count = _required_summary_integer(code_summary.get("row_count"), "code row_count")
    provider_count = _required_summary_integer(
        provider_summary.get("row_count"),
        "provider-set row_count",
    )
    serving_rate_count = _required_summary_integer(
        preservation.get("encoded_records"),
        "encoded_records",
    )
    try:
        support_digest = bytes.fromhex(str(dictionaries.get("support_digest") or ""))
    except ValueError as exc:
        raise RuntimeError("strict V3 finalizer support digest is invalid") from exc
    if len(support_digest) != 32:
        raise RuntimeError("strict V3 finalizer support digest must contain 32 bytes")
    code_path = _finalizer_output_file(output_directory, code_summary.get("path"), "code")
    provider_path = _finalizer_output_file(
        output_directory,
        provider_summary.get("path"),
        "provider-set",
    )
    provider_metadata_files = _provider_set_metadata_files(
        provider_set_metadata_entries,
        required=provider_count > 0,
    )

    token = uuid.uuid4().hex[:20]
    code_stage = f"ptg2_v3_code_stage_{token}"
    provider_stage = f"ptg2_v3_provider_stage_{token}"
    provider_metadata_stage = f"ptg2_v3_provider_metadata_stage_{token}"
    schema = _quote_ident(_safe_identifier(schema_name))
    quoted_code_stage = _quote_ident(code_stage)
    quoted_provider_stage = _quote_ident(provider_stage)
    quoted_provider_metadata_stage = _quote_ident(provider_metadata_stage)
    try:
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {schema}.{quoted_code_stage} (
                code_key integer NOT NULL,
                code_global_id_128 bytea NOT NULL CHECK (octet_length(code_global_id_128) = 16),
                coverage_scope_id bytea NOT NULL
                    CHECK (octet_length(coverage_scope_id) = 32),
                reported_code_system text,
                reported_code text,
                negotiation_arrangement text,
                billing_code_type_version text,
                source_name text,
                source_description text,
                rate_count bigint NOT NULL CHECK (rate_count >= 0)
            );
            """
        )
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {schema}.{quoted_provider_stage} (
                provider_set_key integer NOT NULL,
                provider_set_global_id_128 bytea NOT NULL
                    CHECK (octet_length(provider_set_global_id_128) = 16),
                provider_count bigint NOT NULL CHECK (provider_count >= 0),
                network_names text[]
            );
            """
        )
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {schema}.{quoted_provider_metadata_stage} (
                provider_set_global_id_128 text NOT NULL,
                network_names text[] NOT NULL
            );
            """
        )
        await _copy_binary_file_to_stage(
            code_path,
            schema_name=schema_name,
            stage_table=code_stage,
            columns=(
                "code_key",
                "code_global_id_128",
                "coverage_scope_id",
                "reported_code_system",
                "reported_code",
                "negotiation_arrangement",
                "billing_code_type_version",
                "source_name",
                "source_description",
                "rate_count",
            ),
        )
        await _copy_binary_file_to_stage(
            provider_path,
            schema_name=schema_name,
            stage_table=provider_stage,
            columns=(
                "provider_set_key",
                "provider_set_global_id_128",
                "provider_count",
            ),
        )
        for metadata_path, _row_count in provider_metadata_files:
            await _copy_text_file_to_stage(
                metadata_path,
                schema_name=schema_name,
                stage_table=provider_metadata_stage,
                columns=("provider_set_global_id_128", "network_names"),
            )
        if provider_count > 0:
            await db.status(
                f"""
                CREATE UNIQUE INDEX ON {schema}.{quoted_provider_stage}
                    (provider_set_global_id_128);
                CREATE INDEX ON {schema}.{quoted_provider_metadata_stage}
                    ((decode(provider_set_global_id_128, 'hex')));
                ANALYZE {schema}.{quoted_provider_stage};
                ANALYZE {schema}.{quoted_provider_metadata_stage};
                """
            )
        async with db.transaction() as session:
            await lock_shared_layout_for_dense_write(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
            )
            observed_code = await session.execute(
                db.text(
                    f"""
                    SELECT COUNT(*), COALESCE(SUM(rate_count), 0),
                           COUNT(DISTINCT coverage_scope_id),
                           COUNT(*) FILTER (
                               WHERE coverage_scope_id = :expected_coverage_scope_id
                           )
                      FROM {schema}.{quoted_code_stage}
                    """
                ),
                {"expected_coverage_scope_id": coverage_scope_id},
            )
            (
                observed_code_count,
                observed_rate_count,
                observed_scope_count,
                matching_scope_count,
            ) = observed_code.one()
            observed_provider_count = await session.scalar(
                db.text(f"SELECT COUNT(*) FROM {schema}.{quoted_provider_stage}")
            )
            expected_metadata_rows = sum(
                row_count for _path, row_count in provider_metadata_files
            )
            observed_metadata_rows = 0
            conflicting_metadata = False
            if provider_count > 0:
                observed_metadata_rows = await session.scalar(
                    db.text(
                        f"SELECT COUNT(*) FROM {schema}.{quoted_provider_metadata_stage}"
                    )
                )
                conflicting_metadata = await session.scalar(
                    db.text(
                        f"""
                        SELECT EXISTS (
                            SELECT 1
                              FROM {schema}.{quoted_provider_metadata_stage}
                             GROUP BY provider_set_global_id_128
                            HAVING COUNT(DISTINCT network_names) <> 1
                        )
                        """
                    )
                )
            if int(observed_code_count) != code_count:
                raise RuntimeError("strict V3 code dictionary row count changed during COPY")
            if int(observed_rate_count) != serving_rate_count:
                raise RuntimeError("strict V3 code rate counts do not preserve encoded rows")
            if int(observed_provider_count or 0) != provider_count:
                raise RuntimeError("strict V3 provider dictionary row count changed during COPY")
            if int(observed_metadata_rows or 0) != expected_metadata_rows:
                raise RuntimeError("strict V3 provider-set metadata row count changed during COPY")
            if bool(conflicting_metadata):
                raise RuntimeError("strict V3 provider-set metadata has conflicting network names")
            if int(observed_code_count) > 0 and (
                int(observed_scope_count) != 1
                or int(matching_scope_count) != int(observed_code_count)
            ):
                raise RuntimeError(
                    "strict V3 code dictionary coverage scope does not match expected scope"
                )
            if int(observed_code_count) == 0 and (
                int(observed_scope_count) != 0 or int(matching_scope_count) != 0
            ):
                raise RuntimeError("strict V3 empty code dictionary has scope rows")
            if provider_count > 0:
                await session.execute(
                    db.text(
                        f"""
                        UPDATE {schema}.{quoted_provider_stage} provider_stage
                           SET network_names = metadata.network_names
                          FROM (
                                SELECT DISTINCT provider_set_global_id_128, network_names
                                  FROM {schema}.{quoted_provider_metadata_stage}
                               ) metadata
                         WHERE provider_stage.provider_set_global_id_128 =
                               decode(metadata.provider_set_global_id_128, 'hex')
                        """
                    )
                )
                unmatched_provider_metadata = await session.scalar(
                    db.text(
                        f"""
                        SELECT EXISTS (
                            SELECT 1
                              FROM {schema}.{quoted_provider_stage}
                             WHERE network_names IS NULL
                        ) OR EXISTS (
                            SELECT 1
                              FROM {schema}.{quoted_provider_metadata_stage} metadata
                             WHERE NOT EXISTS (
                                   SELECT 1
                                     FROM {schema}.{quoted_provider_stage} provider_stage
                                    WHERE provider_stage.provider_set_global_id_128 =
                                          decode(metadata.provider_set_global_id_128, 'hex')
                               )
                        )
                        """
                    )
                )
                if bool(unmatched_provider_metadata):
                    raise RuntimeError(
                        "strict V3 provider-set metadata does not exactly cover the provider dictionary"
                    )
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_code
                        (snapshot_key, code_key, code_global_id_128, coverage_scope_id,
                         reported_code_system, reported_code,
                         negotiation_arrangement, billing_code_type_version,
                         source_name, source_description, rate_count)
                    SELECT :snapshot_key, code_key, code_global_id_128, coverage_scope_id,
                           reported_code_system, reported_code,
                           negotiation_arrangement, billing_code_type_version,
                           source_name, source_description, rate_count
                      FROM {schema}.{quoted_code_stage}
                     ORDER BY code_key
                    """
                ),
                {"snapshot_key": int(snapshot_key)},
            )
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_provider_set
                        (snapshot_key, provider_set_key,
                         provider_set_global_id_128, provider_count,
                         network_names)
                    SELECT :snapshot_key, provider_set_key,
                           provider_set_global_id_128, provider_count,
                           network_names
                      FROM {schema}.{quoted_provider_stage}
                     ORDER BY provider_set_key
                    """
                ),
                {"snapshot_key": int(snapshot_key)},
            )
        return SharedDictionaryPublication(
            code_count=code_count,
            provider_set_count=provider_count,
            serving_rate_count=serving_rate_count,
            support_digest=support_digest,
        )
    finally:
        await db.status(
            f"DROP TABLE IF EXISTS {schema}.{quoted_code_stage}, "
            f"{schema}.{quoted_provider_stage}, "
            f"{schema}.{quoted_provider_metadata_stage};"
        )


async def _validate_shared_block_stage(
    session: Any,
    *,
    schema_name: str,
    stage_table: str,
) -> None:
    schema = _quote_ident(_safe_identifier(schema_name))
    stage = _quote_ident(_safe_identifier(stage_table))
    duplicate_mapping = await session.scalar(
        db.text(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {schema}.{stage}
                 GROUP BY object_kind, block_key, fragment_no
                HAVING COUNT(DISTINCT block_hash) > 1
                    OR COUNT(DISTINCT entry_count) > 1
            )
            """
        )
    )
    if duplicate_mapping:
        raise RuntimeError("strict V3 shared-block stage contains conflicting mapping rows")
    incompatible_version = await session.scalar(
        db.text(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {schema}.{stage}
                 WHERE format_version <> :format_version
            )
            """
        ),
        {"format_version": PTG2_V3_SHARED_FORMAT_VERSION},
    )
    if incompatible_version:
        raise RuntimeError("strict V3 shared-block stage uses an incompatible format version")
    duplicate_hash = await session.scalar(
        db.text(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {schema}.{stage}
                 GROUP BY block_hash
                HAVING COUNT(DISTINCT format_version) > 1
                    OR COUNT(DISTINCT object_kind) > 1
                    OR COUNT(DISTINCT codec) > 1
                    OR COUNT(DISTINCT entry_count) > 1
                    OR COUNT(DISTINCT raw_byte_count) > 1
                    OR COUNT(DISTINCT stored_byte_count) > 1
                    OR COUNT(DISTINCT payload) > 1
            )
            """
        )
    )
    if duplicate_hash:
        raise RuntimeError("strict V3 shared-block stage contains conflicting content hashes")


async def publish_shared_block_stage(
    *,
    schema_name: str,
    stage_table: str,
    snapshot_key: int,
    build_token: str,
) -> SharedBlockStagePublication:
    """Publish a compact Rust-generated block stage without reading payloads in Python."""

    schema = _quote_ident(_safe_identifier(schema_name))
    stage = _quote_ident(_safe_identifier(stage_table))
    try:
        async with db.transaction() as session:
            await lock_shared_layout_for_dense_write(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
            )
            await _validate_shared_block_stage(
                session,
                schema_name=schema_name,
                stage_table=stage_table,
            )
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_block
                        (block_hash, format_version, object_kind, codec, entry_count,
                         raw_byte_count, stored_byte_count, payload, created_at)
                    SELECT DISTINCT ON (block_hash)
                           block_hash, format_version, object_kind, codec, entry_count,
                           raw_byte_count, stored_byte_count, payload, now()
                      FROM {schema}.{stage}
                     ORDER BY block_hash
                    ON CONFLICT (block_hash) DO NOTHING
                    """
                )
            )
            mismatch = await session.scalar(
                db.text(
                    f"""
                    SELECT EXISTS (
                        SELECT 1
                          FROM {schema}.{stage} staged
                          JOIN {schema}.ptg2_v3_block stored
                            ON stored.block_hash = staged.block_hash
                         WHERE stored.format_version <> staged.format_version
                            OR stored.object_kind <> staged.object_kind
                            OR stored.codec <> staged.codec
                            OR stored.entry_count <> staged.entry_count
                            OR stored.raw_byte_count <> staged.raw_byte_count
                            OR stored.stored_byte_count <> staged.stored_byte_count
                            OR stored.payload <> staged.payload
                    )
                    """
                )
            )
            if mismatch:
                raise RuntimeError("strict V3 shared block conflicts with stored content metadata")
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_snapshot_block
                        (snapshot_key, object_kind, block_key, fragment_no,
                         entry_count, block_hash)
                    SELECT DISTINCT ON (object_kind, block_key, fragment_no)
                           :snapshot_key, object_kind, block_key, fragment_no,
                           entry_count, block_hash
                      FROM {schema}.{stage}
                     ORDER BY object_kind, block_key, fragment_no, block_hash
                    ON CONFLICT (snapshot_key, object_kind, block_key, fragment_no)
                    DO NOTHING
                    """
                ),
                {"snapshot_key": int(snapshot_key)},
            )
            mapping_mismatch = await session.scalar(
                db.text(
                    f"""
                    SELECT EXISTS (
                        SELECT 1
                          FROM {schema}.{stage} staged
                          JOIN {schema}.ptg2_v3_snapshot_block mapping
                            ON mapping.snapshot_key = :snapshot_key
                           AND mapping.object_kind = staged.object_kind
                           AND mapping.block_key = staged.block_key
                           AND mapping.fragment_no = staged.fragment_no
                         WHERE mapping.entry_count <> staged.entry_count
                            OR mapping.block_hash <> staged.block_hash
                    )
                    """
                ),
                {"snapshot_key": int(snapshot_key)},
            )
            if mapping_mismatch:
                raise RuntimeError("strict V3 shared layout mapping conflicts with staged output")
            result = await session.execute(
                db.text(
                    f"""
                    SELECT object_kind, block_key, fragment_no, entry_count,
                           block_hash, raw_byte_count, stored_byte_count
                      FROM {schema}.{stage}
                     GROUP BY object_kind, block_key, fragment_no, entry_count,
                              block_hash, raw_byte_count, stored_byte_count
                     ORDER BY object_kind, block_key, fragment_no
                    """
                )
            )
            rows = [dict(getattr(row, "_mapping", row)) for row in result]
        references = tuple(
            SharedBlockReference(
                object_kind=str(row["object_kind"]),
                block_key=int(row["block_key"]),
                fragment_no=int(row["fragment_no"]),
                entry_count=int(row["entry_count"]),
                block_hash=bytes(row["block_hash"]),
                raw_byte_count=int(row["raw_byte_count"]),
            )
            for row in rows
        )
        return SharedBlockStagePublication(
            references=references,
            mapping_count=len(references),
            unique_block_count=len({reference.block_hash for reference in references}),
            logical_byte_count=sum(reference.raw_byte_count for reference in references),
            stored_byte_count=sum(int(row["stored_byte_count"]) for row in rows),
        )
    finally:
        await db.status(f"DROP TABLE IF EXISTS {schema}.{stage};")


async def _publish_graph_block_stage(
    conversion: SharedGraphConversionResult,
    *,
    schema_name: str,
    stage_table: str,
    snapshot_key: int,
    build_token: str,
) -> tuple[SharedBlockReference, ...]:
    """Publish graph blocks with SQL-side validation and no Python row dictionaries."""

    schema = _quote_ident(_safe_identifier(schema_name))
    stage = _quote_ident(_safe_identifier(stage_table))
    async with db.transaction() as session:
        await lock_shared_layout_for_dense_write(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
        )
        await _validate_shared_block_stage(
            session,
            schema_name=schema_name,
            stage_table=stage_table,
        )
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_block
                    (block_hash, format_version, object_kind, codec, entry_count,
                     raw_byte_count, stored_byte_count, payload, created_at)
                SELECT DISTINCT ON (block_hash)
                       block_hash, format_version, object_kind, codec, entry_count,
                       raw_byte_count, stored_byte_count, payload, now()
                  FROM {schema}.{stage}
                 ORDER BY block_hash
                ON CONFLICT (block_hash) DO NOTHING
                """
            )
        )
        content_mismatch = await session.scalar(
            db.text(
                f"""
                SELECT EXISTS (
                    SELECT 1
                      FROM {schema}.{stage} staged
                      JOIN {schema}.ptg2_v3_block stored
                        ON stored.block_hash = staged.block_hash
                     WHERE stored.format_version <> staged.format_version
                        OR stored.object_kind <> staged.object_kind
                        OR stored.codec <> staged.codec
                        OR stored.entry_count <> staged.entry_count
                        OR stored.raw_byte_count <> staged.raw_byte_count
                        OR stored.stored_byte_count <> staged.stored_byte_count
                        OR stored.payload <> staged.payload
                )
                """
            )
        )
        if content_mismatch:
            raise RuntimeError("strict V3 graph block conflicts with stored content")
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_block
                    (snapshot_key, object_kind, block_key, fragment_no,
                     entry_count, block_hash)
                SELECT :snapshot_key, object_kind, block_key, fragment_no,
                       entry_count, block_hash
                  FROM {schema}.{stage}
                 ORDER BY object_kind, block_key, fragment_no
                ON CONFLICT (snapshot_key, object_kind, block_key, fragment_no)
                DO NOTHING
                """
            ),
            {"snapshot_key": int(snapshot_key)},
        )
        mapping_mismatch = await session.scalar(
            db.text(
                f"""
                SELECT EXISTS (
                    SELECT 1
                      FROM {schema}.{stage} staged
                      JOIN {schema}.ptg2_v3_snapshot_block mapping
                        ON mapping.snapshot_key = :snapshot_key
                       AND mapping.object_kind = staged.object_kind
                       AND mapping.block_key = staged.block_key
                       AND mapping.fragment_no = staged.fragment_no
                     WHERE mapping.entry_count <> staged.entry_count
                        OR mapping.block_hash <> staged.block_hash
                )
                """
            ),
            {"snapshot_key": int(snapshot_key)},
        )
        if mapping_mismatch:
            raise RuntimeError("strict V3 graph block mapping conflicts with staged output")
        counts = await session.execute(
            db.text(
                f"""
                SELECT COUNT(*), COALESCE(SUM(raw_byte_count), 0),
                       COALESCE(SUM(stored_byte_count), 0)
                  FROM {schema}.{stage}
                """
            )
        )
        block_count, raw_byte_count, stored_byte_count = (
            int(value) for value in counts.one()
        )
        if (
            block_count != int(conversion.block_count)
            or raw_byte_count != int(conversion.raw_block_byte_count)
            or stored_byte_count != int(conversion.stored_block_byte_count)
        ):
            raise RuntimeError("strict V3 graph block counts changed during binary COPY")

    references = tuple(conversion.iter_references())
    if len(references) != int(conversion.block_count):
        raise RuntimeError("strict V3 graph reference count mismatch")
    return references


def _expected_graph_row_counts(
    conversion: SharedGraphConversionResult,
) -> tuple[int, int, int]:
    return (
        int(conversion.owner_count),
        int(conversion.provider_group_count),
        int(conversion.npi_count),
    )


async def publish_shared_graph(
    conversion: SharedGraphConversionResult,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    block_batch_rows: int = 32,
    row_batch_rows: int = 10_000,
) -> SharedGraphPublication:
    """Publish graph payloads and support rows entirely through binary COPY."""

    del block_batch_rows, row_batch_rows
    schema = _quote_ident(_safe_identifier(schema_name))
    token = uuid.uuid4().hex[:20]
    block_stage = shared_block_stage_name(f"graph-{snapshot_key}-{token}")
    owner_stage = f"ptg2_v3_graph_owner_stage_{token}"
    group_stage = f"ptg2_v3_graph_group_stage_{token}"
    npi_stage = f"ptg2_v3_graph_npi_stage_{token}"
    await create_shared_block_stage(schema_name=schema_name, stage_table=block_stage)
    try:
        await copy_shared_block_binary_file(
            conversion.block_copy_path,
            schema_name=schema_name,
            stage_table=block_stage,
        )
        references = await _publish_graph_block_stage(
            conversion,
            schema_name=schema_name,
            stage_table=block_stage,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
        )
        for ddl in (
            f"""
            CREATE UNLOGGED TABLE {schema}.{_quote_ident(owner_stage)} (
                direction smallint NOT NULL,
                owner_key bigint NOT NULL,
                first_chunk integer NOT NULL,
                member_offset integer NOT NULL,
                member_count bigint NOT NULL
            )
            """,
            f"""
            CREATE UNLOGGED TABLE {schema}.{_quote_ident(group_stage)} (
                provider_group_key integer NOT NULL,
                provider_group_global_id_128 bytea NOT NULL
            )
            """,
            f"""
            CREATE UNLOGGED TABLE {schema}.{_quote_ident(npi_stage)} (
                npi bigint NOT NULL
            )
            """,
        ):
            await db.status(ddl)
        await _copy_binary_file_to_stage(
            conversion.owner_copy_path,
            schema_name=schema_name,
            stage_table=owner_stage,
            columns=("direction", "owner_key", "first_chunk", "member_offset", "member_count"),
        )
        await _copy_binary_file_to_stage(
            conversion.group_copy_path,
            schema_name=schema_name,
            stage_table=group_stage,
            columns=("provider_group_key", "provider_group_global_id_128"),
        )
        await _copy_binary_file_to_stage(
            conversion.npi_copy_path,
            schema_name=schema_name,
            stage_table=npi_stage,
            columns=("npi",),
        )
        async with db.transaction() as session:
            await lock_shared_layout_for_dense_write(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
            )
            observed = await session.execute(
                db.text(
                    f"""
                    SELECT
                        (SELECT COUNT(*) FROM {schema}.{_quote_ident(owner_stage)}),
                        (SELECT COUNT(*) FROM {schema}.{_quote_ident(group_stage)}),
                        (SELECT COUNT(*) FROM {schema}.{_quote_ident(npi_stage)}),
                        (SELECT COUNT(DISTINCT provider_group_key)
                           FROM {schema}.{_quote_ident(group_stage)}),
                        (SELECT COUNT(DISTINCT provider_group_global_id_128)
                           FROM {schema}.{_quote_ident(group_stage)})
                    """
                )
            )
            owner_count, group_count, npi_count, group_key_count, group_id_count = (
                int(value) for value in observed.one()
            )
            if (owner_count, group_count, npi_count) != _expected_graph_row_counts(
                conversion
            ):
                raise RuntimeError("strict V3 graph row count changed during binary COPY")
            if group_key_count != group_count or group_id_count != group_count:
                raise RuntimeError("strict V3 provider-group dictionary changed during binary COPY")
            snapshot_params = {"snapshot_key": int(snapshot_key)}
            for statement in (
                f"""
                    INSERT INTO {schema}.ptg2_v3_graph_owner
                        (snapshot_key, direction, owner_key, first_chunk,
                         member_offset, member_count)
                    SELECT :snapshot_key, direction, owner_key, first_chunk,
                           member_offset, member_count
                      FROM {schema}.{_quote_ident(owner_stage)}
                     ORDER BY direction, owner_key
                """,
                f"""
                    INSERT INTO {schema}.ptg2_v3_provider_group
                        (snapshot_key, provider_group_key, provider_group_global_id_128)
                    SELECT :snapshot_key, provider_group_key, provider_group_global_id_128
                      FROM {schema}.{_quote_ident(group_stage)}
                     ORDER BY provider_group_key
                """,
                f"""
                    INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
                    SELECT :snapshot_key, npi
                      FROM {schema}.{_quote_ident(npi_stage)}
                     ORDER BY npi
                """,
            ):
                await session.execute(db.text(statement), snapshot_params)
            published = await session.execute(
                db.text(
                    f"""
                    SELECT
                        (SELECT COUNT(*) FROM {schema}.ptg2_v3_graph_owner
                          WHERE snapshot_key = :snapshot_key),
                        (SELECT COUNT(*) FROM {schema}.ptg2_v3_provider_group
                          WHERE snapshot_key = :snapshot_key),
                        (SELECT COUNT(*) FROM {schema}.ptg2_v3_npi_scope
                          WHERE snapshot_key = :snapshot_key)
                    """
                ),
                snapshot_params,
            )
            if tuple(
                int(value) for value in published.one()
            ) != _expected_graph_row_counts(conversion):
                raise RuntimeError("strict V3 graph published row count mismatch")
    finally:
        await db.status(
            f"DROP TABLE IF EXISTS {schema}.{_quote_ident(owner_stage)}, "
            f"{schema}.{_quote_ident(group_stage)}, {schema}.{_quote_ident(npi_stage)}, "
            f"{schema}.{_quote_ident(block_stage)};"
        )
    return SharedGraphPublication(
        references=references,
        block_count=int(conversion.block_count),
        owner_count=int(conversion.owner_count),
        provider_group_count=int(conversion.provider_group_count),
        npi_count=int(conversion.npi_count),
        support_digest=bytes(conversion.support_digest),
        logical_byte_count=conversion.raw_block_byte_count,
        stored_byte_count=conversion.stored_block_byte_count,
    )


__all__ = [
    "SharedBlockStagePublication",
    "SharedDictionaryPublication",
    "SharedGraphPublication",
    "copy_shared_block_binary_file",
    "create_shared_block_stage",
    "publish_shared_block_stage",
    "publish_shared_finalizer_dictionaries",
    "publish_shared_graph",
    "shared_graph_bundles_from_artifacts",
    "shared_block_stage_name",
]
