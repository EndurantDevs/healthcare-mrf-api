# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The three code-set sources must become visible in one transaction."""

from __future__ import annotations

import importlib
import os
import re
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

from db.models import CodeCatalog

code_sets = importlib.import_module("process.code_sets")

POS_HTML = "<table><tr><td>23</td><td>Emergency</td><td>Emergency care</td></tr></table>"
RC_HTML = "<table><tr><td>0450</td><td>Emergency revenue</td></tr></table>"


def _dsn() -> str:
    raw = os.getenv("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN", "")
    if not raw:
        pytest.skip("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN is not set")
    url = make_url(raw)
    if (
        url.drivername != "postgresql"
        or url.username != "postgres"
        or url.host not in {"127.0.0.1", "localhost"}
        or url.port != 5440
        or re.fullmatch(r"hc_scoped_reference_[0-9a-f]{32}", url.database or "") is None
    ):
        pytest.fail("code-set publication test requires its dedicated local database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _seed_catalog(engine):
    metadata = MetaData(schema="mrf")
    CodeCatalog.__table__.to_metadata(metadata, schema="mrf")
    async with engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA IF EXISTS mrf CASCADE"))
        await connection.execute(text("CREATE SCHEMA mrf"))
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            text(
                "CREATE TABLE mrf.code_sets_result_generation (id smallint primary key, "
                "local_lineage_id uuid not null, local_generation bigint not null, "
                "origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, "
                "code_catalog_oid bigint, row_count bigint, row_sha256 text)"
            )
        )
        await connection.execute(
            text("INSERT INTO mrf.code_sets_result_generation VALUES (1,:lineage,0,NULL,NULL,NULL,NULL,NULL,NULL)"),
            {"lineage": str(uuid4())},
        )
        await connection.execute(
            text(
                "INSERT INTO mrf.code_catalog (code_system, code, source) VALUES ('OTHER', '1', 'synthetic-unrelated')"
            )
        )
        await connection.execute(
            text("INSERT INTO mrf.code_catalog (code_system, code, source) VALUES ('POS', '99', :source)"),
            {"source": code_sets.SOURCE_POS},
        )


def _stub_feeds(monkeypatch):
    monkeypatch.setattr(code_sets, "ensure_database", AsyncMock())
    monkeypatch.setattr(code_sets, "_ensure_code_catalog", AsyncMock())
    monkeypatch.setattr(
        code_sets,
        "modifier_code_rows",
        lambda: [code_sets.CodeSetRow("MODIFIER", "26", "Professional", source=code_sets.SOURCE_MODIFIER)],
    )
    monkeypatch.setattr(
        code_sets,
        "_download_text",
        lambda url: POS_HTML if url == code_sets.DEFAULT_POS_URL else RC_HTML,
    )


async def _assert_late_failure_rolls_back(sessions, monkeypatch):
    original_upsert = code_sets._upsert_code_rows

    async def fail_after_revenue(schema, code_rows):
        count = await original_upsert(schema, code_rows)
        if code_rows and code_rows[0].source == code_sets.SOURCE_RC:
            raise RuntimeError("synthetic late failure")
        return count

    monkeypatch.setattr(code_sets, "_upsert_code_rows", fail_after_revenue)
    async with sessions() as session, session.begin():
        async with code_sets.db.bind_existing_session(session):
            with pytest.raises(RuntimeError, match="synthetic late failure"):
                await code_sets.import_code_sets()
            catalog_sources = (
                (await session.execute(text("SELECT source FROM mrf.code_catalog ORDER BY source"))).scalars().all()
            )
            assert catalog_sources == [code_sets.SOURCE_POS, "synthetic-unrelated"]
            assert await session.scalar(text("SELECT local_generation FROM mrf.code_sets_result_generation")) == 0

    monkeypatch.setattr(code_sets, "_upsert_code_rows", original_upsert)


async def _assert_foreign_conflict_rolls_back(sessions):
    async with sessions() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO mrf.code_catalog (code_system, code, display_name, source) "
                "VALUES ('RC', '0450', 'Foreign revenue', 'synthetic-foreign')"
            )
        )
        async with code_sets.db.bind_existing_session(session):
            with pytest.raises(RuntimeError, match="owned by another source"):
                await code_sets.import_code_sets()
            catalog_entries = (
                await session.execute(
                    text("SELECT code_system, code, display_name, source FROM mrf.code_catalog ORDER BY code_system")
                )
            ).all()
            assert [tuple(entry) for entry in catalog_entries] == [
                ("OTHER", "1", None, "synthetic-unrelated"),
                ("POS", "99", None, code_sets.SOURCE_POS),
                ("RC", "0450", "Foreign revenue", "synthetic-foreign"),
            ]
            assert await session.scalar(text("SELECT local_generation FROM mrf.code_sets_result_generation")) == 0
        await session.execute(text("DELETE FROM mrf.code_catalog WHERE source='synthetic-foreign'"))


async def _assert_partial_feed_retains_prior_code(sessions):
    async with sessions() as session, session.begin():
        async with code_sets.db.bind_existing_session(session):
            import_counts = await code_sets.import_code_sets()
            assert (import_counts["pos_rows"], import_counts["rc_rows"], import_counts["modifier_rows"]) == (1, 1, 1)
    async with sessions() as session:
        catalog_sources = (
            (await session.execute(text("SELECT source FROM mrf.code_catalog ORDER BY source"))).scalars().all()
        )
        assert catalog_sources == [
            code_sets.SOURCE_RC,
            code_sets.SOURCE_MODIFIER,
            code_sets.SOURCE_POS,
            code_sets.SOURCE_POS,
            "synthetic-unrelated",
        ]
        # A partial but nonempty POS response must not delete a previously valid code.
        assert await session.scalar(text("SELECT count(*) FROM mrf.code_catalog WHERE code='99'")) == 1
        generation = (
            await session.execute(
                text(
                    "SELECT local_generation,origin_generation,row_count,row_sha256 FROM mrf.code_sets_result_generation"
                )
            )
        ).one()
        assert generation[0:3] == (1, 1, 4)
        assert len(generation[3]) == 64


@pytest.mark.asyncio
async def test_code_sets_rolls_back_late_failure_and_foreign_key_conflict(monkeypatch):
    """A failed import leaves no partial rows; a partial feed retains prior codes."""
    dsn = _dsn()
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", make_url(dsn).database)
    engine = create_async_engine(dsn)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await _seed_catalog(engine)
        _stub_feeds(monkeypatch)
        await _assert_late_failure_rolls_back(sessions, monkeypatch)
        await _assert_foreign_conflict_rolls_back(sessions)
        await _assert_partial_feed_retains_prior_code(sessions)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA IF EXISTS mrf CASCADE"))
        await engine.dispose()
