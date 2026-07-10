# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL-owned PTG2 binary artifact storage.

The serving layer still uses mmap-backed binary readers for latency.  This
module makes PostgreSQL the durable owner of those bytes and treats files as
rebuildable local cache artifacts.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import zlib
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import text

from db.connection import db
from process.ptg_parts.artifacts import resolve_ptg2_artifact_dir, sha256_file
from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.db_tables import _quote_ident

PTG2_ARTIFACT_DB_URI_PREFIX = "db://ptg2_artifact/"
PTG2_ARTIFACT_DB_STORE_ENV = "HLTHPRT_PTG2_ARTIFACT_DB_STORE"
PTG2_ARTIFACT_DB_RETAIN_LOCAL_CACHE_ENV = "HLTHPRT_PTG2_ARTIFACT_DB_RETAIN_LOCAL_CACHE"
PTG2_ARTIFACT_DB_CHUNK_BYTES_ENV = "HLTHPRT_PTG2_ARTIFACT_DB_CHUNK_BYTES"
PTG2_ARTIFACT_DB_COMPRESSION_LEVEL_ENV = "HLTHPRT_PTG2_ARTIFACT_DB_COMPRESSION_LEVEL"
PTG2_ARTIFACT_DB_CACHE_DIR_ENV = "HLTHPRT_PTG2_ARTIFACT_DB_CACHE_DIR"
_DEFAULT_CHUNK_BYTES = 8 * 1024 * 1024


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return str(raw_value).strip().lower() not in {"0", "false", "no", "off"}


def ptg2_artifact_db_store_enabled() -> bool:
    return _env_bool(PTG2_ARTIFACT_DB_STORE_ENV, True)


def ptg2_artifact_db_retain_local_cache() -> bool:
    return _env_bool(PTG2_ARTIFACT_DB_RETAIN_LOCAL_CACHE_ENV, False)


def _artifact_db_chunk_bytes() -> int:
    try:
        return max(int(os.getenv(PTG2_ARTIFACT_DB_CHUNK_BYTES_ENV, str(_DEFAULT_CHUNK_BYTES))), 1024 * 1024)
    except ValueError:
        return _DEFAULT_CHUNK_BYTES


def _artifact_db_compression_level() -> int:
    try:
        return min(max(int(os.getenv(PTG2_ARTIFACT_DB_COMPRESSION_LEVEL_ENV, "6")), 0), 9)
    except ValueError:
        return 6


def ptg2_db_artifact_uri(artifact_id: str) -> str:
    return f"{PTG2_ARTIFACT_DB_URI_PREFIX}{artifact_id}"


def ptg2_artifact_id_from_db_uri(uri: str) -> str | None:
    text_value = str(uri or "").strip()
    if not text_value.startswith(PTG2_ARTIFACT_DB_URI_PREFIX):
        return None
    artifact_id = text_value[len(PTG2_ARTIFACT_DB_URI_PREFIX) :].strip()
    if not artifact_id or "/" in artifact_id or ".." in artifact_id:
        return None
    return artifact_id


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


async def ensure_ptg2_artifact_blob_table(schema_name: str | None = None) -> None:
    schema = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    qualified_table = f"{_quote_ident(schema)}.ptg2_artifact_blob_chunk"
    await db.status(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            artifact_id varchar(96) NOT NULL,
            chunk_no integer NOT NULL,
            compression varchar(32),
            payload bytea NOT NULL,
            raw_byte_count integer NOT NULL,
            byte_count integer NOT NULL,
            created_at timestamp,
            PRIMARY KEY (artifact_id, chunk_no)
        );
        """
    )
    await db.status(
        f"""
        CREATE INDEX IF NOT EXISTS ptg2_artifact_blob_artifact_idx
        ON {qualified_table} (artifact_id);
        """
    )


async def delete_ptg2_artifacts_for_snapshot(
    snapshot_id: str,
    *,
    schema_name: str | None = None,
) -> None:
    """Delete PostgreSQL-owned artifacts for one unpublished snapshot."""

    snapshot_id = str(snapshot_id or "").strip()
    if not snapshot_id:
        return
    schema = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    qualified_chunks = f"{_quote_ident(schema)}.ptg2_artifact_blob_chunk"
    qualified_manifest = f"{_quote_ident(schema)}.ptg2_artifact_manifest"
    await ensure_ptg2_artifact_blob_table(schema)
    async with db.transaction() as session:
        await session.execute(
            text(
                f"""
                DELETE FROM {qualified_chunks}
                 WHERE artifact_id IN (
                    SELECT artifact_id
                      FROM {qualified_manifest}
                     WHERE snapshot_id = :snapshot_id
                 )
                """
            ),
            {"snapshot_id": snapshot_id},
        )
        await session.execute(
            text(f"DELETE FROM {qualified_manifest} WHERE snapshot_id = :snapshot_id"),
            {"snapshot_id": snapshot_id},
        )


def _artifact_id_for(
    *,
    snapshot_id: str | None,
    artifact_kind: str,
    name: str,
    sha256: str,
    byte_count: int,
) -> str:
    return semantic_hash(
        {
            "kind": artifact_kind,
            "name": name,
            "snapshot_id": snapshot_id,
            "sha256": sha256,
            "byte_count": int(byte_count),
        },
        domain="ptg2_artifact_blob",
    )[:32]


def _json_param(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), sort_keys=True, default=str, separators=(",", ":"))


async def store_ptg2_artifact_file_in_db(
    path: str | Path,
    *,
    snapshot_id: str | None,
    artifact_kind: str,
    name: str | None = None,
    import_run_id: str | None = None,
    schema_name: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    retain_local_cache: bool | None = None,
) -> dict[str, Any]:
    """Store an artifact file in PostgreSQL chunks and return manifest metadata."""

    artifact_path = Path(path)
    entry = dict(metadata or {})
    entry.setdefault("name", name or artifact_kind)
    entry.setdefault("path", str(artifact_path))
    if not artifact_path.exists() or artifact_path.stat().st_size <= 0:
        return entry

    artifact_sha, byte_count = sha256_file(artifact_path)
    expected_sha = str(entry.get("sha256") or artifact_sha)
    expected_byte_count = int(entry.get("byte_count") or byte_count)
    if expected_sha != artifact_sha:
        raise ValueError(f"artifact checksum changed before PostgreSQL upload: {artifact_path}")
    if expected_byte_count != byte_count:
        raise ValueError(f"artifact byte_count changed before PostgreSQL upload: {artifact_path}")

    schema = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    artifact_name = str(entry.get("name") or name or artifact_kind)
    artifact_id = _artifact_id_for(
        snapshot_id=snapshot_id,
        artifact_kind=artifact_kind,
        name=artifact_name,
        sha256=artifact_sha,
        byte_count=byte_count,
    )
    chunk_size = _artifact_db_chunk_bytes()
    compression_level = _artifact_db_compression_level()
    compression = "zlib" if compression_level > 0 else "none"
    storage_uri = ptg2_db_artifact_uri(artifact_id)
    qualified_chunks = f"{_quote_ident(schema)}.ptg2_artifact_blob_chunk"
    qualified_manifest = f"{_quote_ident(schema)}.ptg2_artifact_manifest"
    payload_metadata = {
        **entry,
        "storage": "postgresql_chunks_v1",
        "storage_uri": storage_uri,
        "chunk_bytes": chunk_size,
        "compression": compression,
        "compression_level": compression_level,
    }

    await ensure_ptg2_artifact_blob_table(schema)
    async with db.transaction() as session:
        await session.execute(
            text(f"DELETE FROM {qualified_chunks} WHERE artifact_id = :artifact_id"),
            {"artifact_id": artifact_id},
        )
        chunk_no = 0
        with artifact_path.open("rb") as fp:
            for raw_chunk in iter(lambda: fp.read(chunk_size), b""):
                payload = zlib.compress(raw_chunk, compression_level) if compression == "zlib" else raw_chunk
                await session.execute(
                    text(
                        f"""
                        INSERT INTO {qualified_chunks}
                            (artifact_id, chunk_no, compression, payload, raw_byte_count, byte_count, created_at)
                        VALUES
                            (:artifact_id, :chunk_no, :compression, :payload, :raw_byte_count, :byte_count, :created_at)
                        """
                    ),
                    {
                        "artifact_id": artifact_id,
                        "chunk_no": chunk_no,
                        "compression": compression,
                        "payload": payload,
                        "raw_byte_count": len(raw_chunk),
                        "byte_count": len(payload),
                        "created_at": _utcnow(),
                    },
                )
                chunk_no += 1
        await session.execute(
            text(
                f"""
                INSERT INTO {qualified_manifest}
                    (artifact_id, snapshot_id, import_run_id, artifact_kind, storage_uri, sha256, byte_count, payload, created_at)
                VALUES
                    (:artifact_id, :snapshot_id, :import_run_id, :artifact_kind, :storage_uri, :sha256, :byte_count,
                     CAST(:payload AS json), :created_at)
                ON CONFLICT (artifact_id) DO UPDATE SET
                    snapshot_id = EXCLUDED.snapshot_id,
                    import_run_id = EXCLUDED.import_run_id,
                    artifact_kind = EXCLUDED.artifact_kind,
                    storage_uri = EXCLUDED.storage_uri,
                    sha256 = EXCLUDED.sha256,
                    byte_count = EXCLUDED.byte_count,
                    payload = EXCLUDED.payload,
                    created_at = EXCLUDED.created_at
                """
            ),
            {
                "artifact_id": artifact_id,
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "artifact_kind": artifact_kind,
                "storage_uri": storage_uri,
                "sha256": artifact_sha,
                "byte_count": byte_count,
                "payload": _json_param(payload_metadata),
                "created_at": _utcnow(),
            },
        )

    entry.update(
        {
            "artifact_id": artifact_id,
            "storage": "postgresql_chunks_v1",
            "storage_uri": storage_uri,
            "sha256": artifact_sha,
            "byte_count": byte_count,
            "chunk_bytes": chunk_size,
            "compression": compression,
        }
    )
    if retain_local_cache is None:
        retain_local_cache = ptg2_artifact_db_retain_local_cache()
    if not retain_local_cache:
        artifact_path.unlink(missing_ok=True)
    return entry


def _artifact_cache_root() -> Path:
    configured = os.getenv(PTG2_ARTIFACT_DB_CACHE_DIR_ENV)
    root = Path(configured) if configured else resolve_ptg2_artifact_dir() / "db-cache"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _artifact_cache_path(artifact_id: str, metadata: Mapping[str, Any] | None) -> Path:
    entry = metadata or {}
    raw_path = str(entry.get("path") or "").strip()
    file_name = Path(raw_path).name if raw_path else ""
    if not file_name:
        name = str(entry.get("name") or "artifact").strip() or "artifact"
        suffix = str(entry.get("suffix") or ".bin").strip() or ".bin"
        if not suffix.startswith("."):
            suffix = f".{suffix}"
        file_name = f"{name}_{artifact_id}{suffix}"
    return _artifact_cache_root() / artifact_id[:2] / artifact_id[2:4] / file_name


def _cached_file_valid(path: Path, metadata: Mapping[str, Any] | None) -> bool:
    if not path.exists() or not path.is_file():
        return False
    expected_byte_count = (metadata or {}).get("byte_count")
    expected_sha = str((metadata or {}).get("sha256") or "").strip()
    try:
        if isinstance(expected_byte_count, int) and path.stat().st_size != expected_byte_count:
            return False
        if expected_sha:
            actual_sha, _byte_count = sha256_file(path)
            return actual_sha == expected_sha
        return True
    except OSError:
        return False


async def materialize_ptg2_artifact_from_db(
    session,
    storage_uri: str,
    *,
    schema_name: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> Path:
    """Hydrate a PostgreSQL-owned artifact into a verified local cache file."""

    artifact_id = ptg2_artifact_id_from_db_uri(storage_uri)
    if not artifact_id:
        raise ValueError(f"unsupported PTG2 artifact storage uri: {storage_uri!r}")
    cache_path = _artifact_cache_path(artifact_id, metadata)
    if _cached_file_valid(cache_path, metadata):
        return cache_path

    schema = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    qualified_chunks = f"{_quote_ident(schema)}.ptg2_artifact_blob_chunk"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_name(f"{cache_path.name}.tmp.{os.getpid()}")
    digest = hashlib.sha256()
    total_raw_bytes = 0
    chunk_count = 0
    try:
        with tmp_path.open("wb") as out:
            result = await session.stream(
                text(
                    f"""
                    SELECT chunk_no, compression, payload, raw_byte_count
                      FROM {qualified_chunks}
                     WHERE artifact_id = :artifact_id
                     ORDER BY chunk_no
                    """
                ),
                {"artifact_id": artifact_id},
            )
            async for row in result:
                data = _row_mapping(row)
                compression = str(data.get("compression") or "none")
                payload = bytes(data.get("payload") or b"")
                raw_chunk = zlib.decompress(payload) if compression == "zlib" else payload
                expected_raw = data.get("raw_byte_count")
                if expected_raw is not None and len(raw_chunk) != int(expected_raw):
                    raise ValueError(f"artifact chunk raw byte_count mismatch for {artifact_id}:{chunk_count}")
                out.write(raw_chunk)
                digest.update(raw_chunk)
                total_raw_bytes += len(raw_chunk)
                chunk_count += 1
        if chunk_count <= 0:
            raise FileNotFoundError(f"PTG2 artifact has no PostgreSQL chunks: {artifact_id}")
        expected_byte_count = (metadata or {}).get("byte_count")
        if isinstance(expected_byte_count, int) and total_raw_bytes != expected_byte_count:
            raise ValueError(
                f"artifact byte_count mismatch for {artifact_id}: expected {expected_byte_count}, got {total_raw_bytes}"
            )
        expected_sha = str((metadata or {}).get("sha256") or "").strip()
        actual_sha = digest.hexdigest()
        if expected_sha and actual_sha != expected_sha:
            raise ValueError(f"artifact checksum mismatch for {artifact_id}")
        os.replace(tmp_path, cache_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return cache_path


async def hydrate_ptg2_artifact_entry_from_db(
    session,
    entry: Mapping[str, Any],
    *,
    schema_name: str | None = None,
) -> dict[str, Any]:
    hydrated = dict(entry)
    storage_uri = str(hydrated.get("storage_uri") or "").strip()
    if not ptg2_artifact_id_from_db_uri(storage_uri):
        return hydrated
    cache_path = await materialize_ptg2_artifact_from_db(
        session,
        storage_uri,
        schema_name=schema_name,
        metadata=hydrated,
    )
    hydrated["path"] = str(cache_path)
    hydrated["cache_path"] = str(cache_path)
    return hydrated
