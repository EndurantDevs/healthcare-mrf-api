# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The three code-set sources must become visible in one transaction."""

from __future__ import annotations

import importlib
import json
import os
import re
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import MetaData

from db.connection import Database
from db.models import CodeCatalog, ImportRun
from process import reference_family_archive as native
from process import scoped_catalog_handoff as handoff
from process.scoped_catalog_retention import cleanup_retained_catalog
from tests.scoped_catalog_native_fixture import catalog_actors, seal_catalog

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
        or url.port not in {5432, 5440}
        or re.fullmatch(r"hc_scoped_reference_[0-9a-f]{32}", url.database or "") is None
    ):
        pytest.fail("code-set publication test requires its dedicated local database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _seed_catalog(engine):
    metadata = MetaData(schema="mrf")
    CodeCatalog.__table__.to_metadata(metadata, schema="mrf")
    ImportRun.__table__.to_metadata(metadata, schema="mrf")
    async with engine.begin() as connection:
        assert await connection.scalar(text("SELECT to_regnamespace('mrf')")) is None
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


def _copy():
    return native.ReferenceFamilySourceCopy(native.native_copy_projection, 1024**2, 30)


async def _ordinary_preparation(actors):
    context_by_field = {
        "control_run_id": uuid4().hex,
        "_control_attempt_id": uuid4().hex,
        "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
    }
    async with actors.builder.begin() as session:
        await session.execute(
            text(
                "INSERT INTO mrf.import_run (run_id,engine,importer,node_id,status,progress,params) "
                "VALUES (:run_id,'healthcare-mrf-api','code-sets','synthetic-node','running',CAST(:progress AS json),'{}')"
            ),
            {
                "run_id": context_by_field["control_run_id"],
                "progress": json.dumps(
                    {
                        "attempt_id": context_by_field["_control_attempt_id"],
                        "attempt_started_at": context_by_field["_control_attempt_started_at"],
                    }
                ),
            },
        )
    result = await code_sets.import_code_sets(_control_context={"context": context_by_field})
    assert result["status"] == "finalizing"
    assert context_by_field["control_run_handoff_committed"] is True
    return result


async def _cancel_preparation(actors, candidate):
    async with actors.builder.begin() as session:
        current = (
            (
                await session.execute(
                    text("SELECT * FROM mrf.import_run WHERE run_id=:run_id"), {"run_id": candidate["run_id"]}
                )
            )
            .mappings()
            .one()
        )
        await handoff.request_catalog_cancel(session, current)
    async with actors.publisher.begin() as session:
        receipt = await handoff.cancel_catalog_handoff(session, candidate)
        assert receipt == {"handoff": candidate, "status": "canceled"}
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) == receipt


async def _assert_late_failure_rolls_back(actors):
    result = await _ordinary_preparation(actors)
    candidate = result[handoff.METRIC]
    with pytest.raises(RuntimeError, match="synthetic late failure"):
        async with actors.publisher.begin() as session:
            await handoff.publish_catalog_handoff(session, candidate, source_copy=_copy())
            raise RuntimeError("synthetic late failure")
    async with actors.builder.begin() as session:
        catalog_sources = (
            (await session.execute(text("SELECT source FROM mrf.code_catalog ORDER BY source"))).scalars().all()
        )
        assert catalog_sources == [code_sets.SOURCE_POS, "synthetic-unrelated"]
        assert await session.scalar(text("SELECT local_generation FROM mrf.code_sets_result_generation")) == 0
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) is None
    await _cancel_preparation(actors, candidate)


async def _assert_foreign_conflict_rolls_back(actors):
    async with actors.publisher.begin() as session:
        await session.execute(
            text(
                "INSERT INTO mrf.code_catalog (code_system, code, display_name, source) "
                "VALUES ('RC', '0450', 'Foreign revenue', 'synthetic-foreign')"
            )
        )
    candidate = (await _ordinary_preparation(actors))[handoff.METRIC]
    with pytest.raises(RuntimeError, match="foreign key ownership"):
        async with actors.publisher.begin() as session:
            await handoff.publish_catalog_handoff(session, candidate, source_copy=_copy())
    async with actors.publisher.begin() as session:
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
    await _cancel_preparation(actors, candidate)


async def _assert_partial_feed_retains_prior_code(actors):
    import_counts = await _ordinary_preparation(actors)
    assert (import_counts["pos_rows"], import_counts["rc_rows"], import_counts["modifier_rows"]) == (1, 1, 1)
    candidate = import_counts[handoff.METRIC]
    async with actors.builder.begin() as reader:
        await reader.execute(text("LOCK TABLE mrf.code_catalog IN ACCESS SHARE MODE"))
        before_oid = await reader.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid"))
        with pytest.raises(DBAPIError, match="lock"):
            async with actors.publisher.begin() as publisher:
                await handoff.publish_catalog_handoff(publisher, candidate, source_copy=_copy())
        assert await reader.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid")) == before_oid
        assert await reader.scalar(text("SELECT count(*) FROM mrf.code_catalog")) == 2
    async with actors.publisher.begin() as session:
        receipt = await handoff.publish_catalog_handoff(session, candidate, source_copy=_copy())
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) == receipt
    async with actors.builder.begin() as session:
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
        assert await session.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid")) != before_oid
    async with actors.publisher.begin() as session:
        await cleanup_retained_catalog(session, receipt["retained"])


@pytest.mark.asyncio
async def test_code_sets_rolls_back_late_failure_and_foreign_key_conflict(monkeypatch):
    """A failed import leaves no partial rows; a partial feed retains prior codes."""
    dsn = _dsn()
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", make_url(dsn).database)
    engine = create_async_engine(dsn)
    try:
        async with catalog_actors(engine, ["mrf"]) as actors:
            await _seed_catalog(engine)
            await seal_catalog(actors, "mrf", (CodeCatalog,), "code_sets_result_generation")
            async with engine.begin() as connection:
                await connection.execute(text(f'ALTER TABLE mrf.import_run OWNER TO "{actors.roles["builder"]}"'))
            monkeypatch.setattr(
                code_sets, "db", Database(engine=actors.builder.kw["bind"], session_factory=actors.builder)
            )
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            _stub_feeds(monkeypatch)
            await _assert_late_failure_rolls_back(actors)
            await _assert_foreign_conflict_rolls_back(actors)
            await _assert_partial_feed_retains_prior_code(actors)
    finally:
        await engine.dispose()
