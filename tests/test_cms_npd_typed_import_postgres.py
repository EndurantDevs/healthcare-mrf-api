# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage, finish, check and publish synthetic CMS NPD releases through the real encoder and PostgreSQL."""

import json
import os
from uuid import uuid4

import asyncpg
import pytest

from process.cms_npd_typed_import import LOCK_KEY, run
from process.cms_npd_typed_schema import release_table_statements
from process.cms_npd_typed_stage import CmsNpdTypedImportError
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary
from tests.test_cms_npd_typed_import import LocalSource, write_release

DSN_ENV = "HLTHPRT_CMS_NPD_TYPED_IMPORT_TEST_DSN"
SCANNER = _ptg2_rust_scanner_binary()
pytestmark = [
    pytest.mark.skipif(not os.getenv(DSN_ENV), reason=f"{DSN_ENV} names a disposable PostgreSQL database"),
    pytest.mark.skipif(
        SCANNER is None,
        reason="build support/ptg2_scanner first (cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml)",
    ),
]


async def _connect():
    return await asyncpg.connect(os.environ[DSN_ENV])


@pytest.fixture
async def control(monkeypatch):
    """A fresh control schema with an empty release table; drops every schema it recorded."""
    schema = "cms_npd_import_ctl_" + uuid4().hex[:12]
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("HLTHPRT_DB_READER_USER", raising=False)
    admin = await _connect()
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    for statement in release_table_statements(schema):
        await admin.execute(statement)
    try:
        yield admin, schema
    finally:
        for row in await admin.fetch(f'SELECT schema_name FROM "{schema}".cms_npd_release'):
            await admin.execute(f'DROP SCHEMA IF EXISTS "{row["schema_name"]}" CASCADE')
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def _import(source):
    return await run({}, {}, "", source=source, connect=_connect, scanner=SCANNER)


async def _statuses(admin, control_schema):
    rows = await admin.fetch(f'SELECT release_id, status FROM "{control_schema}".cms_npd_release')
    return {row["release_id"]: row["status"] for row in rows}


async def _schema_exists(admin, schema):
    return await admin.fetchval("SELECT to_regnamespace($1) IS NOT NULL", schema)


async def test_releases_publish_roll_and_retire(control, tmp_path):
    admin, control_schema = control
    first = LocalSource(tmp_path / "r1", write_release(tmp_path / "r1", 2, "2026-01-01"))
    published = await _import(first)
    first_id, first_schema = published["release_id"], published["schema"]
    assert published["status"] == "published" and published["previous_release_id"] is None
    assert published["row_counts"] == {
        "practitioner": 2,
        "practitioner_address": 2,
        "practitioner_role": 2,
        "organization": 3,
        "organization_address": 2,
        "location": 2,
        "endpoint": 2,
        "organization_affiliation": 2,
        "healthcare_service": 2,
        "insurance_plan": 2,
        "network": 1,
    }
    assert published["unresolved_counts"]["endpoint.organization"] == 2
    assert sum(published["unresolved_counts"].values()) == 2
    assert set(published["seconds"]) == {"observe", "download", "stage", "finish", "check", "publish"}
    assert await admin.fetchval(f'SELECT count(*) FROM "{first_schema}".practitioner_role') == 2
    assert await admin.fetchval(f'SELECT count(*) FROM "{first_schema}".network') == 1
    typed_references = f"""SELECT
        (SELECT count(*) FROM "{first_schema}".organization WHERE other_type_codes = '{{ein}}'),
        (SELECT count(*) FROM "{first_schema}".location WHERE part_of = id),
        (SELECT count(*) FROM "{first_schema}".insurance_plan WHERE administered_by = 'net-1'
            AND cardinality(coverage_area_ids) = 1 AND jsonb_array_length(nested_references) = 3),
        (SELECT count(*) FROM "{first_schema}".practitioner_role WHERE network_ids = '{{net-1}}'
            AND cardinality(insurance_plan_ids) = 1),
        (SELECT count(*) FROM "{first_schema}".practitioner WHERE npi_explicit)"""
    assert tuple(await admin.fetchrow(typed_references)) == (2, 2, 2, 2, 0)
    recorded = await admin.fetchrow(f'SELECT * FROM "{control_schema}".cms_npd_release')
    assert (recorded["release_id"], recorded["status"], recorded["generated_on"].isoformat()) == (
        first_id,
        "current",
        "2026-01-01",
    )
    assert json.loads(recorded["files"])["06-Practitioner.ndjson"]["etag"] == '"etag-06-Practitioner.ndjson"'
    assert json.loads(recorded["row_counts"]) == published["row_counts"]
    assert first.kept == {first_id}

    unchanged = await _import(first)
    assert (unchanged["status"], unchanged["release_id"]) == ("unchanged", first_id)

    second = await _import(LocalSource(tmp_path / "r2", write_release(tmp_path / "r2", 3, "2026-02-01")))
    assert second["previous_release_id"] == first_id and second["dropped_release_id"] is None
    assert await _statuses(admin, control_schema) == {first_id: "previous", second["release_id"]: "current"}
    assert await _schema_exists(admin, first_schema)

    third_source = LocalSource(tmp_path / "r3", write_release(tmp_path / "r3", 4, "2026-03-01"))
    third = await _import(third_source)
    assert third["dropped_release_id"] == first_id
    assert await _statuses(admin, control_schema) == {second["release_id"]: "previous", third["release_id"]: "current"}
    assert not await _schema_exists(admin, first_schema)
    assert third_source.kept == {second["release_id"], third["release_id"]}
    assert await admin.fetchval(f'SELECT count(*) FROM "{third["schema"]}".practitioner') == 4


async def _assert_failed_run(admin, control_schema, source, code, current_id):
    with pytest.raises(CmsNpdTypedImportError) as error:
        await _import(source)
    assert error.value.control_error_code == code
    assert await _statuses(admin, control_schema) == {current_id: "current"}
    candidate = "cms_npd_" + source.observed.vector_sha256[:16]
    assert not await _schema_exists(admin, candidate)


async def test_failed_runs_drop_their_candidate_and_keep_the_current_release(control, tmp_path):
    admin, control_schema = control
    current = await _import(LocalSource(tmp_path / "good", write_release(tmp_path / "good", 5)))
    current_id = current["release_id"]

    bad = LocalSource(tmp_path / "bad", write_release(tmp_path / "bad", 5, "2026-02-01", bad_type="Location"))
    await _assert_failed_run(admin, control_schema, bad, "cms_npd_typed_scanner_failed", current_id)

    small = LocalSource(tmp_path / "small", write_release(tmp_path / "small", 1, "2026-03-01"))
    await _assert_failed_run(admin, control_schema, small, "cms_npd_typed_table_shrunk", current_id)

    holder = await _connect()
    try:
        assert await holder.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_KEY)
        busy = LocalSource(tmp_path / "busy", write_release(tmp_path / "busy", 6, "2026-04-01"))
        await _assert_failed_run(admin, control_schema, busy, "cms_npd_typed_import_in_progress", current_id)
    finally:
        await holder.close()
