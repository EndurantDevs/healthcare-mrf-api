# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL proof for uncached PTG2 artifact hydration."""

from __future__ import annotations

import hashlib
import uuid
import zlib

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.ptg_parts import ptg2_artifact_blobs as artifact_blobs
from tests.test_ptg_wave_recovery_storage_postgres import _dsn


@pytest.mark.asyncio
async def test_uncached_artifact_hydration_verifies_expected_sha(
    monkeypatch,
    tmp_path,
) -> None:
    """Stream real PostgreSQL chunks and verify their authenticated bytes."""

    raw_bytes = b"uncached PostgreSQL artifact hydration"
    artifact_id = f"hydration-{uuid.uuid4().hex}"
    expected_sha = hashlib.sha256(raw_bytes).hexdigest()
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=1,
        max_overflow=0,
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    monkeypatch.setenv(
        artifact_blobs.PTG2_ARTIFACT_DB_CACHE_DIR_ENV,
        str(tmp_path),
    )
    try:
        async with sessions() as session:
            transaction = await session.begin()
            try:
                await session.execute(
                    text(
                        "INSERT INTO mrf.ptg2_artifact_blob_chunk "
                        "(artifact_id, chunk_no, compression, payload, "
                        "raw_byte_count, byte_count, created_at) "
                        "VALUES (:artifact_id, 0, 'zlib', :payload, "
                        ":raw_byte_count, :byte_count, now())"
                    ),
                    {
                        "artifact_id": artifact_id,
                        "payload": zlib.compress(raw_bytes),
                        "raw_byte_count": len(raw_bytes),
                        "byte_count": len(zlib.compress(raw_bytes)),
                    },
                )
                cache_path = await artifact_blobs.materialize_ptg2_artifact_from_db(
                    session,
                    artifact_blobs.ptg2_db_artifact_uri(artifact_id),
                    schema_name="mrf",
                    metadata={
                        "name": "uncached-hydration",
                        "byte_count": len(raw_bytes),
                        "sha256": expected_sha,
                    },
                )
                assert cache_path.read_bytes() == raw_bytes
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
