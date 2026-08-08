"""SQLAlchemy/asyncpg proof for exact-wave supersession persistence."""

from __future__ import annotations

import datetime as dt
import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import control_import_wave_supersession as supersession
from db.models import PTGImportWaveSupersession
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _evidence,
    _install_migration,
    _quote,
    asyncpg,
)


def _successor_insert(schema: str):
    """Build the successor insert executed in the ORM transaction."""

    quoted = _quote(schema)
    return text(
        f"""
        INSERT INTO {quoted}.ptg_import_wave (
            wave_id, state, intent_count, wave_digest, manifest_digest,
            jobs_digest, release_queue, queue, worker_class, resource_class,
            worker_limit, cohort_attestation
        ) VALUES (
            :wave_id, 'admitted', 1, :digest, :digest, :digest,
            'arq:PTGSmall:wave:' || :digest, 'arq:PTGSmall',
            'process.PTGSmall', 'small', 12, CAST(:cohort AS jsonb)
        )
        """
    )


def _supersession_witness(evidence: dict[str, object]) -> SimpleNamespace:
    """Build the attested evidence interface consumed by persistence."""

    unsigned_evidence_map = deepcopy(evidence)
    unsigned_evidence_map.pop("proof_digest")
    return SimpleNamespace(
        as_mapping=lambda: deepcopy(evidence),
        evidence_mapping=lambda: deepcopy(unsigned_evidence_map),
        proof_digest=evidence["proof_digest"],
    )


async def _persist_with_orm(
    session_factory,
    schema: str,
    wave_id: str,
    evidence: dict[str, object],
    naive_utc: dt.datetime,
) -> None:
    """Insert successor and supersession through one ORM transaction."""

    cohort_map = {
        "schema_version": "healthporta.ptg-import-wave-attestation.v3",
        "wave_id": wave_id,
        "supersession": evidence,
    }
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                _successor_insert(schema),
                {
                    "wave_id": wave_id,
                    "digest": "b" * 64,
                    "cohort": json.dumps(cohort_map),
                },
            )
            await supersession.persist_admission_supersession(
                session,
                {"wave_id": wave_id, "supersession": evidence},
                now=naive_utc,
            )


@pytest.mark.asyncio
async def test_orm_supersession_binds_naive_admission_clock_as_utc(monkeypatch):
    """Persist the legacy naive UTC clock through the real timestamptz adapter."""

    dsn = _dsn()
    schema = "wave_recovery_orm_timestamp"
    quoted = _quote(schema)
    connection = await asyncpg.connect(dsn)
    model_schema = PTGImportWaveSupersession.__table__.schema
    engine = create_async_engine(
        make_url(dsn).set(drivername="postgresql+asyncpg"),
        execution_options={"schema_translate_map": {model_schema: schema}},
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await _install_migration(connection, monkeypatch, schema)
        wave_id = "orm-successor"
        evidence, _canonical = _evidence(wave_id)
        witness = _supersession_witness(evidence)

        async def attest(*_args, **_kwargs):
            return witness

        monkeypatch.setattr(
            supersession,
            "attest_locked_logical_preclaim_supersession",
            attest,
        )
        naive_utc = dt.datetime(2026, 8, 8, 1, 2, 3)
        await _persist_with_orm(
            session_factory,
            schema,
            wave_id,
            evidence,
            naive_utc,
        )

        created_at = await connection.fetchval(
            f"SELECT created_at FROM {quoted}.ptg_import_wave_supersession "
            "WHERE successor_wave_id = $1",
            wave_id,
        )
        assert created_at == naive_utc.replace(tzinfo=dt.UTC)
    finally:
        await engine.dispose()
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        await connection.close()
