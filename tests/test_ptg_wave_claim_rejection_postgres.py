"""PostgreSQL identity-map proof for exact-wave claim rejection state."""

from __future__ import annotations

import datetime as dt
import os
import re
import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import PTGImportWave
from process.ptg_wave_claims import _advance_released_wave_for_rejection


_POSTGRES_DSN_ENV = "HLTHPRT_PTG_WAVE_CLAIM_POSTGRES_DSN"
_TEST_DATABASE_RE = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


def _database_url():
    raw_dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {_POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    if (
        not database_url.drivername.startswith("postgresql")
        or not database_url.database
        or not _TEST_DATABASE_RE.search(str(database_url.database))
    ):
        pytest.fail(f"{_POSTGRES_DSN_ENV} must target an explicit PostgreSQL test database")
    return database_url.set(drivername="postgresql+asyncpg")


def _wave() -> PTGImportWave:
    digest = "1" * 64
    wave_digest = "2" * 64
    return PTGImportWave(
        wave_id="wave-sync-unit",
        idempotency_key="wave-sync-unit",
        request_digest=digest,
        cohort_attestation={"schema_version": 1},
        cohort_attestation_digest=digest,
        cohort_signature_digest=digest,
        physical_coordinate_count=1,
        physical_coordinate_digest=digest,
        imported_coordinate_count=1,
        imported_coordinate_digest=digest,
        reused_coordinate_count=0,
        reused_coordinate_digest=digest,
        partition_digest=digest,
        intent_count=1,
        jobs_digest=digest,
        manifest_digest=digest,
        wave_digest=wave_digest,
        queue="arq:PTGSmall",
        release_queue=f"arq:PTGSmall:wave:{wave_digest}",
        worker_class="process.PTGSmall",
        resource_class="small",
        worker_limit=12,
        protocol_identity="healthporta.ptg-small.exact-wave.v1",
        serializer_identity="arq-0.28.process-msgpack.v1",
        enqueue_time_ms=1,
        state_version=4,
        state="released",
        created_at=dt.datetime.now(dt.UTC).replace(tzinfo=None),
    )


@pytest.mark.asyncio
async def test_released_rejection_moves_database_and_identity_map_exactly_one_version():
    database_url = _database_url()
    schema_name = f"ptg_wave_claim_sync_{uuid.uuid4().hex}"
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    model_schema = PTGImportWave.__table__.schema
    engine = create_async_engine(
        database_url,
        execution_options={"schema_translate_map": {model_schema: schema_name}},
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {quoted_schema}"))
            await connection.run_sync(PTGImportWave.__table__.create)
        async with session_factory() as session:
            async with session.begin():
                wave = _wave()
                session.add(wave)
                await session.flush()

                await _advance_released_wave_for_rejection(session, wave)
                assert wave.state == "executing"
                assert wave.state_version == 5
                await session.flush()

                persisted = (await session.execute(text(
                    f"SELECT state, state_version FROM {quoted_schema}.ptg_import_wave "
                    "WHERE wave_id = 'wave-sync-unit'"
                ))).one()
                assert tuple(persisted) == ("executing", 5)

        async with session_factory() as session:
            reloaded = (await session.execute(
                select(PTGImportWave).where(PTGImportWave.wave_id == "wave-sync-unit")
            )).scalar_one()
            assert reloaded.state == "executing"
            assert reloaded.state_version == 5
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
        await engine.dispose()
