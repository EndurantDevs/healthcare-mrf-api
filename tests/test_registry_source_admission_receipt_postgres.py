# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native completion, immutable retained evidence and caller rollback boundaries."""

import asyncio
import hashlib
from contextlib import suppress
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from process import registry_source_admission as admission
from process import registry_source_admission_receipt as witness
from process.cms_planfinder_workbook_input import HEADERS
from tests.test_cms_planfinder_workbook_input import _workbook
from tests.test_registry_management_permissions_postgres import (
    _install,
)
from tests.test_registry_management_permissions_postgres import (
    permissions_db as permissions_db,
)
from tests.test_registry_management_permissions_postgres import (
    provider_db as provider_db,
)
from tests.test_registry_management_permissions_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_planfinder_admission_postgres import _edition as _planfinder_edition
from tests.test_registry_source_admission_postgres import _HEADERS, _edition, _input


async def _retained_counts(fixture, edition):
    columns = ",".join(
        f'(SELECT pg_catalog.count(*) FROM "{fixture.schema}"."{table}" WHERE snapshot_id=$1) AS "{table}"'
        for table in witness._TABLES
    )
    return await fixture.connection.fetchrow("SELECT " + columns, edition.snapshot_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind", ["commercial-mlr", "plan-finder"])
async def test_native_completion_lookup_retains_original_receipt_without_encoder(
    permissions_db, tmp_path, monkeypatch, source_kind
):
    fixture = permissions_db
    await _install(fixture)
    if source_kind == "commercial-mlr":
        prepared = _input()
        edition = _edition(prepared)
        admit = admission.admit_cms_mlr_edition
        header = admission._source_header_sha256(_HEADERS)
    else:
        prepared, digest = _workbook(tmp_path, count=3)
        edition = _planfinder_edition(digest)
        admit = admission.admit_cms_planfinder_edition
        header = admission._source_header_sha256(HEADERS)
    retrieved_at = datetime(2024, 9, 12, 8, 30, 45, 123456, timezone(timedelta(hours=2)))
    edition = replace(edition, retrieved_at=retrieved_at)
    baseline_path = await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
    original_path = "public, pg_temp, pg_catalog"
    async with fixture.connection.transaction():
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        receipt = await admit(fixture.connection, prepared, edition, control_schema=fixture.schema)
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert fixture.connection.is_in_transaction()
    assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == baseline_path
    timestamp_sql = f'SELECT retrieved_at FROM "{fixture.schema}".registry_source_snapshot WHERE snapshot_id=$1'
    assert await fixture.connection.fetchval(timestamp_sql, edition.snapshot_id) == retrieved_at
    refetched = replace(edition, retrieved_at=retrieved_at + timedelta(days=1))
    monkeypatch.setattr(
        admission, "_encode_edition", lambda *_: pytest.fail("completed lookup invoked native encoding")
    )
    async with fixture.connection.transaction():
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        observed = await admission.read_registry_source_admission_receipt(
            fixture.connection, refetched, header_sha256=header, control_schema=fixture.schema
        )
        assert observed == receipt
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert fixture.connection.is_in_transaction()
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection, edition, header_sha256="f" * 64, control_schema=fixture.schema
            )
            is None
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                replace(edition, parser_version="changed-parser"),
                header_sha256=header,
                control_schema=fixture.schema,
            )
            is None
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
    assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == baseline_path


@pytest.mark.asyncio
async def test_native_refetch_replay_preserves_first_timestamp_and_sealed_manifest(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    input_bytes = _input()
    retrieved_at = datetime(2024, 9, 12, 8, 30, 45, 123456, timezone(timedelta(hours=2)))
    edition = replace(_edition(input_bytes), retrieved_at=retrieved_at)
    async with fixture.connection.transaction():
        await admission.admit_cms_mlr_edition(fixture.connection, input_bytes, edition, control_schema=fixture.schema)
    manifest_sql = (
        f'SELECT manifest_json,manifest_sha256 FROM "{fixture.schema}".registry_source_admission_receipt '
        "WHERE snapshot_id=$1"
    )
    timestamp_sql = f'SELECT retrieved_at FROM "{fixture.schema}".registry_source_snapshot WHERE snapshot_id=$1'
    original_manifest = await fixture.connection.fetchrow(manifest_sql, edition.snapshot_id)
    assert original_manifest is not None
    assert await fixture.connection.fetchval(timestamp_sql, edition.snapshot_id) == retrieved_at
    async with fixture.connection.transaction():
        replay = await admission.admit_cms_mlr_edition(
            fixture.connection,
            input_bytes,
            replace(edition, retrieved_at=retrieved_at + timedelta(days=1)),
            control_schema=fixture.schema,
        )
    assert replay["replayed"] is True
    assert await fixture.connection.fetchval(timestamp_sql, edition.snapshot_id) == retrieved_at
    assert await fixture.connection.fetchrow(manifest_sql, edition.snapshot_id) == original_manifest


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,assignment",
    [
        ("registry_source_observation", "observation_json=observation_json||'{\"tampered\":true}'::jsonb"),
        ("registry_identifier_observation", "raw_value=raw_value||'-tampered'"),
        ("registry_issuer_company_assertion", "valid_from='2024-01-01'::date"),
        ("registry_company_group_assertion", "valid_from='2024-01-01'::date"),
    ],
)
async def test_changed_complete_native_content_never_reuses_or_reseals_receipt(permissions_db, table, assignment):
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    async with fixture.connection.transaction():
        await admission.admit_cms_mlr_edition(fixture.connection, prepared, edition, control_schema=fixture.schema)
    await fixture.connection.execute(
        f'UPDATE "{fixture.schema}".{table} SET {assignment} WHERE snapshot_id=$1', edition.snapshot_id
    )
    async with fixture.connection.transaction():
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            is None
        )
        with pytest.raises(ValueError, match="differs|immutable"):
            await admission.admit_cms_mlr_edition(fixture.connection, prepared, edition, control_schema=fixture.schema)


@pytest.mark.asyncio
async def test_native_admission_rollback_commits_no_completion_witness(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    baseline_path = await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
    original_path = "public, pg_temp, pg_catalog"
    transaction = fixture.connection.transaction()
    await transaction.start()
    try:
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        receipt = await admission.admit_cms_mlr_edition(
            fixture.connection, prepared, edition, control_schema=fixture.schema
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            == receipt
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert all(row_count > 0 for row_count in (await _retained_counts(fixture, edition)).values())
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.schema}".registry_source_admission_receipt'
            )
            == 1
        )
    finally:
        await transaction.rollback()
    assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == baseline_path
    assert all(row_count == 0 for row_count in (await _retained_counts(fixture, edition)).values())
    async with fixture.connection.transaction():
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            is None
        )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_source_observation') == 0
    )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_source_admission_receipt')
        == 0
    )


@pytest.mark.asyncio
async def test_historical_replay_does_not_manufacture_missing_completion(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    async with fixture.connection.transaction():
        await admission.admit_cms_mlr_edition(fixture.connection, prepared, edition, control_schema=fixture.schema)
    await fixture.connection.execute(
        f'DELETE FROM "{fixture.schema}".registry_source_admission_receipt WHERE snapshot_id=$1', edition.snapshot_id
    )
    async with fixture.connection.transaction():
        receipt = await admission.admit_cms_mlr_edition(
            fixture.connection, prepared, edition, control_schema=fixture.schema
        )
        assert receipt["replayed"] is True
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            is None
        )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_source_admission_receipt')
        == 0
    )


@pytest.mark.asyncio
async def test_column_write_privilege_cannot_authorize_completed_lookup(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    async with fixture.connection.transaction():
        await admission.admit_cms_mlr_edition(fixture.connection, prepared, edition, control_schema=fixture.schema)
    await fixture.connection.execute(
        f'GRANT UPDATE(observation_json) ON "{fixture.schema}".registry_source_observation TO "{fixture.roles_by_kind["api"]}"'
    )
    async with fixture.connection.transaction():
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            is None
        )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_source_observation') == 2
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind", ["commercial-mlr", "plan-finder"])
async def test_native_admission_and_lookup_resist_temporary_catalog_and_type_shadows(
    permissions_db, tmp_path, source_kind
):
    fixture = permissions_db
    await _install(fixture)
    if source_kind == "commercial-mlr":
        prepared = _input()
        edition = _edition(prepared)
        admit = admission.admit_cms_mlr_edition
        header = admission._source_header_sha256(_HEADERS)
    else:
        prepared, digest = _workbook(tmp_path, count=3)
        edition = _planfinder_edition(digest)
        admit = admission.admit_cms_planfinder_edition
        header = admission._source_header_sha256(HEADERS)
    baseline_path = await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
    original_path = "pg_temp, public, pg_catalog"
    transaction = fixture.connection.transaction()
    await transaction.start()
    try:
        await fixture.connection.execute("CREATE TEMP TABLE pg_class (shadowed pg_catalog.bool) ON COMMIT DROP")
        await fixture.connection.execute("CREATE DOMAIN pg_temp.text AS pg_catalog.text CHECK (false)")
        await fixture.connection.execute("CREATE DOMAIN pg_temp.jsonb AS pg_catalog.jsonb CHECK (false)")
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        assert await fixture.connection.fetchval(
            "SELECT pg_catalog.to_regclass('pg_class')=pg_catalog.to_regclass('pg_temp.pg_class')"
        )
        assert await fixture.connection.fetchval(
            "SELECT 'text'::pg_catalog.regtype::pg_catalog.oid="
            "(SELECT oid FROM pg_catalog.pg_type WHERE typnamespace=pg_catalog.pg_my_temp_schema() AND typname='text')"
        )
        for statement in ("SELECT 'shadow'::text", "SELECT '{}'::jsonb"):
            with pytest.raises(asyncpg.CheckViolationError):
                async with fixture.connection.transaction():
                    await fixture.connection.fetchval(statement)
        receipt = await admit(fixture.connection, prepared, edition, control_schema=fixture.schema)
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection, edition, header_sha256=header, control_schema=fixture.schema
            )
            == receipt
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        counts = await _retained_counts(fixture, edition)
        assert counts["registry_source_snapshot"] == counts["registry_source_admission_receipt"] == 1
        assert counts["registry_source_observation"] == receipt["observations"]
        assert counts["registry_identifier_observation"] > 0
        assert counts["registry_issuer_company_assertion"] == receipt["issuer_assertions"]
        assert counts["registry_company_group_assertion"] == receipt["group_assertions"]
    finally:
        await transaction.rollback()
    assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == baseline_path
    assert await fixture.connection.fetchval("SELECT pg_catalog.to_regclass('pg_temp.pg_class')") is None
    assert await fixture.connection.fetchval("SELECT pg_catalog.to_regtype('pg_temp.text')") is None
    assert await fixture.connection.fetchval("SELECT pg_catalog.to_regtype('pg_temp.jsonb')") is None
    assert all(row_count == 0 for row_count in (await _retained_counts(fixture, edition)).values())


@pytest.mark.asyncio
async def test_native_lookup_sql_error_restores_path_and_keeps_outer_work(permissions_db, monkeypatch):
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    async with fixture.connection.transaction():
        receipt = await admission.admit_cms_mlr_edition(
            fixture.connection, prepared, edition, control_schema=fixture.schema
        )
    read_completed = witness.read_completed_registry_source_admission
    original_errors = []

    async def read_then_fail(*args):
        assert await read_completed(*args) == receipt
        assert (
            await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
            == "pg_catalog,pg_temp"
        )
        try:
            await fixture.connection.execute("SELECT 1/0")
        except asyncpg.DivisionByZeroError as error:
            original_errors.append(error)
            raise

    monkeypatch.setattr(witness, "read_completed_registry_source_admission", read_then_fail)
    original_path = "public, pg_temp, pg_catalog"
    async with fixture.connection.transaction():
        await fixture.connection.execute("CREATE TEMP TABLE receipt_outer_work (value pg_catalog.int4) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO pg_temp.receipt_outer_work VALUES(7)")
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        with pytest.raises(asyncpg.DivisionByZeroError) as caught:
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
        assert caught.value is original_errors[0] and caught.value.sqlstate == "22012"
        assert fixture.connection.is_in_transaction()
        assert await fixture.connection.fetchval("SELECT 1") == 1
        assert await fixture.connection.fetchval("SELECT value FROM pg_temp.receipt_outer_work") == 7
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        monkeypatch.setattr(witness, "read_completed_registry_source_admission", read_completed)
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            == receipt
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path


async def _cancel_suspended_lookup(task, ready):
    """Cancel only after the native lookup enters its savepoint, then await cleanup."""
    try:
        await asyncio.wait_for(ready.wait(), timeout=10)
        task.cancel("cancel completed native lookup before savepoint release")
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_native_lookup_task_cancellation_restores_path_and_keeps_outer_work(permissions_db, monkeypatch):
    """Real task cancellation rolls back its savepoint and preserves the caller transaction."""
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    async with fixture.connection.transaction():
        receipt = await admission.admit_cms_mlr_edition(
            fixture.connection, prepared, edition, control_schema=fixture.schema
        )
    read_completed = witness.read_completed_registry_source_admission
    ready = asyncio.Event()
    suspended = asyncio.Event()

    async def read_then_suspend(*args):
        assert await read_completed(*args) == receipt
        assert (
            await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
            == "pg_catalog,pg_temp"
        )
        ready.set()
        await suspended.wait()

    monkeypatch.setattr(witness, "read_completed_registry_source_admission", read_then_suspend)
    original_path = "public, pg_temp, pg_catalog"
    async with fixture.connection.transaction():
        await fixture.connection.execute("CREATE TEMP TABLE receipt_outer_work (value pg_catalog.int4) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO pg_temp.receipt_outer_work VALUES(7)")
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        task = asyncio.create_task(
            admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
        )
        await _cancel_suspended_lookup(task, ready)
        assert fixture.connection.is_in_transaction()
        assert await fixture.connection.fetchval("SELECT 1") == 1
        assert await fixture.connection.fetchval("SELECT value FROM pg_temp.receipt_outer_work") == 7
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert all(row_count > 0 for row_count in (await _retained_counts(fixture, edition)).values())
        monkeypatch.setattr(witness, "read_completed_registry_source_admission", read_completed)
        assert (
            await admission.read_registry_source_admission_receipt(
                fixture.connection,
                edition,
                header_sha256=admission._source_header_sha256(_HEADERS),
                control_schema=fixture.schema,
            )
            == receipt
        )
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path


@pytest.mark.asyncio
async def test_native_seal_sql_error_rolls_back_content_and_witness_together(permissions_db, monkeypatch):
    fixture = permissions_db
    await _install(fixture)
    prepared = _input()
    edition = _edition(prepared)
    seal_completed = witness.seal_registry_source_admission
    original_errors = []

    async def seal_then_fail(*args):
        await seal_completed(*args)
        assert all(row_count > 0 for row_count in (await _retained_counts(fixture, edition)).values())
        try:
            await fixture.connection.execute("SELECT 1/0")
        except asyncpg.DivisionByZeroError as error:
            original_errors.append(error)
            raise

    monkeypatch.setattr(witness, "seal_registry_source_admission", seal_then_fail)
    original_path = "public, pg_temp, pg_catalog"
    async with fixture.connection.transaction():
        await fixture.connection.execute("CREATE TEMP TABLE receipt_outer_work (value pg_catalog.int4) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO pg_temp.receipt_outer_work VALUES(7)")
        await fixture.connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        with pytest.raises(asyncpg.DivisionByZeroError) as caught:
            await admission.admit_cms_mlr_edition(fixture.connection, prepared, edition, control_schema=fixture.schema)
        assert caught.value is original_errors[0] and caught.value.sqlstate == "22012"
        assert fixture.connection.is_in_transaction()
        assert await fixture.connection.fetchval("SELECT 1") == 1
        assert await fixture.connection.fetchval("SELECT value FROM pg_temp.receipt_outer_work") == 7
        assert await fixture.connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == original_path
        assert all(row_count == 0 for row_count in (await _retained_counts(fixture, edition)).values())
