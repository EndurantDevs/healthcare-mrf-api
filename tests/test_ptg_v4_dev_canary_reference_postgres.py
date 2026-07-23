# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-PostgreSQL proof for the V3 rollback/source-equivalence canary query."""

from __future__ import annotations

import os
import re
import uuid

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from scripts.ptg_v4_dev_canary_db_reference import (
    ReferenceEquivalenceScope,
    collect_reference_equivalence,
)
from scripts.ptg_v4_dev_canary_reference import (
    evaluate_database_reference_evidence,
)
from tests.ptg_v4_dev_canary_reference_postgres_support import (
    create_reference_fixture,
    quoted,
)


POSTGRES_DSN_ENV = "HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN"
_TEST_DATABASE_RE = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)
_TEST_SCHEMA_RE = re.compile(r"^ptg2_v4_reference_test_[0-9a-f]{32}$")


def _database_url() -> str:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL reference proof")
    database_url = make_url(raw_dsn)
    if (
        not database_url.drivername.startswith("postgresql")
        or not _TEST_DATABASE_RE.search(str(database_url.database or ""))
    ):
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    return database_url.set(drivername="postgresql").render_as_string(
        hide_password=False
    )


@pytest.mark.asyncio
async def test_reference_query_proves_pin_fk_and_exact_raw_source_set() -> None:
    schema_name = f"ptg2_v4_reference_test_{uuid.uuid4().hex}"
    assert _TEST_SCHEMA_RE.fullmatch(schema_name)
    connection = await asyncpg.connect(_database_url(), statement_cache_size=0)
    try:
        await create_reference_fixture(connection, schema_name)
        evidence = await collect_reference_equivalence(
            connection,
            schema_name,
            ReferenceEquivalenceScope(
                v4_snapshot_id="v4-snapshot",
                reference_snapshot_id="v3-snapshot",
                rollback_owner_id="aetna-391",
            ),
        )

        assert evaluate_database_reference_evidence(evidence)["passed"] is True

        await connection.execute(
            f"""
            UPDATE {quoted(schema_name)}.ptg2_v3_snapshot_source
               SET raw_container_sha256 = repeat('33', 32)
             WHERE snapshot_id = 'v4-snapshot'
            """
        )
        drifted_evidence = await collect_reference_equivalence(
            connection,
            schema_name,
            ReferenceEquivalenceScope(
                v4_snapshot_id="v4-snapshot",
                reference_snapshot_id="v3-snapshot",
                rollback_owner_id="aetna-391",
            ),
        )
        assert (
            evaluate_database_reference_evidence(drifted_evidence)["passed"]
            is False
        )
    finally:
        if not _TEST_SCHEMA_RE.fullmatch(schema_name):
            raise RuntimeError("refusing to drop a non-disposable schema")
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {quoted(schema_name)} CASCADE"
        )
        await connection.close()
