# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""OpenAddresses native batching parity and failure-safety proofs."""
from __future__ import annotations

import importlib
import json
import os
import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import make_url

from db.connection import Database
from db.models import OpenAddressesGeocode, OpenAddressesZipRecovery
from process.ext import address_canon, address_fast
from process.ext import utils as ext_utils
openaddresses = importlib.import_module("process.openaddresses")
POSTGRES_DSN_ENV = "HLTHPRT_OPENADDRESSES_BATCH_POSTGRES_DSN"


def _feature(row_number: int, *, zip5: str | None = "78701") -> dict:
    properties_by_name = {
        "number": str(row_number),
        "street": "Main Street",
        "city": "Austin",
        "region": "TX",
        "id": f"synthetic-{row_number}",
    }
    if zip5 is not None:
        properties_by_name["postcode"] = zip5
    return {
        "type": "Feature",
        "properties": properties_by_name,
        "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
    }


def _canonicalized(rows) -> list[dict[str, str]]:
    canonical_rows = []
    for address_fields in rows:
        identity_key = address_canon.identity_key_v1(*address_fields)
        canonical_rows.append(
            {
                "identity_key": identity_key,
                "address_key": str(address_canon.key_from_identity(identity_key)),
            }
        )
    return canonical_rows


@pytest.mark.parametrize(
    ("row_count", "expected_batch_sizes"),
    [(1, [1]), (4999, [4999]), (5000, [5000]), (5001, [5000, 1])],
)
def test_native_batch_boundaries_have_exact_scalar_parity(
    monkeypatch,
    row_count,
    expected_batch_sizes,
):
    feature = _feature(123)
    expected = openaddresses._record_from_feature(
        feature,
        source_name="synthetic/source",
        data_id=10,
        job_id=20,
        updated=None,
    )
    seen_batch_sizes = []

    def canonicalize_batch(rows):
        rows = list(rows)
        seen_batch_sizes.append(len(rows))
        return _canonicalized(rows)

    monkeypatch.setattr(
        openaddresses,
        "_iter_geojson_features",
        lambda _path: (feature for _ in range(row_count)),
    )
    monkeypatch.setattr(openaddresses, "canonicalize_address_batch", canonicalize_batch)
    batches = list(
        openaddresses._iter_record_batches(
            None,
            batch_size=5000,
            source_name="synthetic/source",
            data_id=10,
            job_id=20,
        )
    )
    actual_rows = [
        {**address_record, "imported_at": None}
        for record_batch in batches
        for address_record in record_batch.rows
    ]

    assert seen_batch_sizes == expected_batch_sizes
    assert actual_rows == [{**expected, "imported_at": None}] * row_count


def test_native_batch_preserves_mixed_outcomes(monkeypatch):
    rejected = _feature(3)
    rejected["geometry"] = {"type": "LineString", "coordinates": []}
    features = [_feature(1), _feature(2, zip5=None), rejected]
    monkeypatch.setattr(openaddresses, "_iter_geojson_features", lambda _path: iter(features))
    monkeypatch.setattr(openaddresses, "canonicalize_address_batch", _canonicalized)

    record_batch = next(openaddresses._iter_record_batches(None, batch_size=5000))

    assert record_batch.processed == 3
    assert len(record_batch.rows) == 1
    assert len(record_batch.zip_recovery_rows) == 1
    assert record_batch.rejection_counts == {"missing_zip5": 1, "not_point": 1}
    assert record_batch.rows[0]["identity_key"]
    assert record_batch.rows[0]["address_key"] == address_canon.key_from_identity(
        record_batch.rows[0]["identity_key"]
    )


@pytest.mark.parametrize(
    "canonical_rows",
    [
        [],
        [None, None],
        [
            {"identity_key": "valid", "address_key": str(uuid.uuid4())},
            {"identity_key": " ", "address_key": str(uuid.uuid4())},
        ],
        [
            {"identity_key": "valid", "address_key": str(uuid.uuid4())},
            {"identity_key": "valid", "address_key": "bad"},
        ],
        [
            {
                "identity_key": "valid",
                "address_key": str(address_canon.key_from_identity("valid")),
            },
            {
                "identity_key": "other",
                "address_key": str(address_canon.key_from_identity("different")),
            },
        ],
    ],
)
def test_native_batch_validation_is_atomic(monkeypatch, canonical_rows):
    rows = [
        openaddresses._record_from_feature(
            _feature(index),
            source_name="synthetic/source",
            data_id=10,
            job_id=20,
            updated=None,
            defer_canonical=True,
        )
        for index in (1, 2)
    ]
    monkeypatch.setattr(openaddresses, "canonicalize_address_batch", lambda _rows: canonical_rows)

    with pytest.raises(RuntimeError):
        openaddresses._attach_canonical_keys(rows)

    assert all(row["identity_key"] is None and row["address_key"] is None for row in rows)


def test_openaddresses_inherits_all_optional_native_fallbacks(monkeypatch):
    current_version = address_canon.current_canon_version()

    def missing_module(_name):
        raise ImportError("synthetic missing module")

    def failed_batch(_rows):
        raise RuntimeError("synthetic native failure")

    importers = [
        missing_module,
        lambda _name: SimpleNamespace(
            canon_version=lambda: {
                **current_version,
                "ruleset_version": current_version["ruleset_version"] - 1,
            }
        ),
        lambda _name: SimpleNamespace(
            canon_version=lambda: current_version,
            canonicalize_batch=failed_batch,
        ),
    ]
    expected = openaddresses._record_from_feature(
        _feature(1),
        source_name="synthetic/source",
        data_id=10,
        job_id=20,
        updated=None,
    )
    deferred_by_field = {**expected, "identity_key": None, "address_key": None}
    try:
        for importer in importers:
            monkeypatch.setattr(address_fast.importlib, "import_module", importer)
            address_fast._fast_module.cache_clear()
            [actual_row] = openaddresses._attach_canonical_keys([deferred_by_field])
            assert actual_row["identity_key"] == expected["identity_key"]
            assert actual_row["address_key"] == expected["address_key"]
    finally:
        address_fast._fast_module.cache_clear()


def _postgres_url():
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for disposable PostgreSQL proof")
    url = make_url(raw_dsn)
    if "test" not in str(url.database or "").lower():
        pytest.fail(f"{POSTGRES_DSN_ENV} must identify a disposable test database")
    return url


def _configure_database(monkeypatch, url) -> None:
    database_env_by_name = {
        "HLTHPRT_DB_DRIVER": "asyncpg",
        "HLTHPRT_DB_HOST": url.host or "127.0.0.1",
        "HLTHPRT_DB_PORT": str(url.port or 5432),
        "HLTHPRT_DB_USER": url.username or "postgres",
        "HLTHPRT_DB_PASSWORD": url.password or "",
        "HLTHPRT_DB_DATABASE": url.database,
    }
    for name, value in database_env_by_name.items():
        monkeypatch.setenv(name, str(value))


async def _relation_oid(database, schema: str, table: str) -> int | None:
    return await database.scalar(
        "SELECT CAST(to_regclass(:relation) AS oid);",
        relation=f'"{schema}"."{table}"',
    )


def _isolated_database(monkeypatch, schema):
    database = Database()
    real_make_class = ext_utils.make_class

    def isolated_class(model, suffix):
        return real_make_class(model, suffix, schema_override=schema)

    async def ensure_database(_test_mode):
        await database.connect()

    monkeypatch.setattr(openaddresses, "db", database)
    monkeypatch.setattr(ext_utils, "db", database)
    monkeypatch.setattr(openaddresses, "make_class", isolated_class)
    monkeypatch.setattr(openaddresses, "ensure_database", ensure_database)
    return database, isolated_class


async def _prepare_postgres_case(monkeypatch, tmp_path, persisted_batches):
    url = _postgres_url()
    _configure_database(monkeypatch, url)
    schema = f"oa_native_batch_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database, isolated_class = _isolated_database(monkeypatch, schema)
    await database.connect()
    await database.status(f'CREATE SCHEMA "{schema}";')
    live_table = OpenAddressesGeocode.__table__.to_metadata(MetaData(), schema=schema)
    await database.create_table(live_table)
    seed_row = openaddresses._record_from_feature(
        _feature(999), source_name="synthetic/prior", data_id=1, job_id=1, updated=None
    )
    await database.insert(live_table).values(seed_row).status()
    source_path = tmp_path / "source.geojson"
    source_path.write_text(
        "\n".join(json.dumps(_feature(index)) for index in range(1, 9)),
        encoding="utf-8",
    )
    import_id = f"failure{persisted_batches}{uuid.uuid4().hex[:8]}"
    task_by_name = {
        "batch_size": 2, "import_id": import_id, "local_files": [str(source_path)],
        "min_rows": 1, "source_concurrency": 1, "test_mode": True,
        "zip_restore_concurrency": 1, "zip_restore_shards": 1,
    }
    suffix = openaddresses._normalize_import_id(import_id)
    return SimpleNamespace(
        database=database, schema=schema, task_by_name=task_by_name,
        context_by_name={"context": {"run": 0}, "import_date": "bootstrap"},
        original_flush=openaddresses._flush_rows,
        stage_table=isolated_class(OpenAddressesGeocode, suffix).__tablename__,
        recovery_table=isolated_class(OpenAddressesZipRecovery, suffix).__tablename__,
        published_recovery_table=openaddresses._published_zip_recovery_table_name(isolated_class(OpenAddressesZipRecovery, suffix).__tablename__),
        live_oid=await _relation_oid(database, schema, OpenAddressesGeocode.__main_table__),
    )


async def _fail_then_retry(case, monkeypatch, persisted_batches):
    flushed_batch_sizes = []

    async def injected_failure(rows, stage_cls):
        if len(flushed_batch_sizes) == persisted_batches:
            raise RuntimeError("synthetic batch failure")
        result = await case.original_flush(rows, stage_cls)
        flushed_batch_sizes.append(len(rows))
        return result

    monkeypatch.setattr(openaddresses, "_flush_rows", injected_failure)
    with pytest.raises(BaseExceptionGroup) as failure:
        await openaddresses.process_data(case.context_by_name, case.task_by_name)
    assert "synthetic batch failure" in repr(failure.value)
    assert len(flushed_batch_sizes) == persisted_batches
    assert case.context_by_name["context"].get("run", 0) == 0
    partial_oid = await _relation_oid(case.database, case.schema, case.stage_table)
    partial_count = await case.database.scalar(
        f'SELECT count(*) FROM "{case.schema}"."{case.stage_table}";'
    )
    assert partial_count == persisted_batches * 2
    await openaddresses.shutdown(case.context_by_name)
    assert await _relation_oid(
        case.database, case.schema, OpenAddressesGeocode.__main_table__
    ) == case.live_oid
    monkeypatch.setattr(openaddresses, "_flush_rows", case.original_flush)
    await openaddresses.process_data(case.context_by_name, case.task_by_name)
    retry_oid = await _relation_oid(case.database, case.schema, case.stage_table)
    assert retry_oid != partial_oid
    assert await case.database.scalar(
        f'SELECT count(*) FROM "{case.schema}"."{case.stage_table}";'
    ) == 8
    return retry_oid


async def _publish_and_assert(case, monkeypatch, retry_oid):
    async def no_zip_restore(**_kwargs):
        return openaddresses.OpenAddressesZipRestoreStats()

    async def no_backfill(**_kwargs):
        return openaddresses.OpenAddressesBackfillStats(0, 0, 0)

    monkeypatch.setattr(openaddresses, "restore_openaddresses_zips", no_zip_restore)
    monkeypatch.setattr(
        openaddresses, "refresh_archive_geocodes_from_openaddresses_sharded", no_backfill
    )
    monkeypatch.setattr(openaddresses, "print_time_info", lambda _started_at: None)
    await openaddresses.shutdown(case.context_by_name)
    live_table = OpenAddressesGeocode.__main_table__
    archived_table = openaddresses._archived_identifier(live_table)
    assert case.context_by_name["context"]["openaddresses_stage_published"] is True
    assert await _relation_oid(case.database, case.schema, live_table) == retry_oid
    assert await _relation_oid(case.database, case.schema, case.stage_table) is None
    assert await _relation_oid(case.database, case.schema, case.recovery_table) is None
    assert await _relation_oid(case.database, case.schema, case.published_recovery_table) is None
    assert await _relation_oid(case.database, case.schema, archived_table) == case.live_oid
    assert await case.database.scalar(
        f'SELECT count(*) FROM "{case.schema}"."{archived_table}";'
    ) == 1
    assert await case.database.scalar(
        f'SELECT count(*) FROM "{case.schema}"."{live_table}" '
        "WHERE identity_key IS NOT NULL AND address_key IS NOT NULL;"
    ) == 8
    assert await case.database.scalar(
        f'SELECT count(DISTINCT row_hash) FROM "{case.schema}"."{live_table}";'
    ) == 8


async def _loaded_postgres_case(monkeypatch, tmp_path):
    case = await _prepare_postgres_case(monkeypatch, tmp_path, 0)
    await openaddresses.process_data(case.context_by_name, case.task_by_name)
    stage_oid = await _relation_oid(case.database, case.schema, case.stage_table)
    case.zip_restore_calls = 0

    async def no_zip_restore(**_kwargs):
        case.zip_restore_calls += 1
        return openaddresses.OpenAddressesZipRestoreStats()

    monkeypatch.setattr(openaddresses, "restore_openaddresses_zips", no_zip_restore)
    monkeypatch.setattr(openaddresses, "print_time_info", lambda _started_at: None)
    return case, stage_oid


@pytest.mark.parametrize("failure_phase", ["stage_rename", "marker_rename"])
@pytest.mark.asyncio
async def test_real_postgres_swap_failure_rolls_back_and_retries(
    monkeypatch, tmp_path, failure_phase
):
    case, stage_oid = await _loaded_postgres_case(monkeypatch, tmp_path)
    original_status = case.database.status
    failure_tracker = SimpleNamespace(has_failed=False)

    async def fail_swap_once(statement, **params):
        target_by_phase = {
            "stage_rename": f'ALTER TABLE "{case.schema}"."{case.stage_table}" RENAME TO "openaddresses_geocode";',
            "marker_rename": (
                f'ALTER TABLE "{case.schema}"."{case.recovery_table}" '
                f'RENAME TO "{case.published_recovery_table}";'
            ),
        }
        target = target_by_phase[failure_phase]
        if not failure_tracker.has_failed and statement == target:
            failure_tracker.has_failed = True
            raise RuntimeError("synthetic swap failure")
        return await original_status(statement, **params)

    async def no_backfill(**_kwargs):
        return openaddresses.OpenAddressesBackfillStats(0, 0, 0)

    monkeypatch.setattr(case.database, "status", fail_swap_once)
    monkeypatch.setattr(openaddresses, "refresh_archive_geocodes_from_openaddresses_sharded", no_backfill)
    try:
        with pytest.raises(RuntimeError, match="synthetic swap failure"):
            await openaddresses.shutdown(case.context_by_name)
        assert await _relation_oid(case.database, case.schema, "openaddresses_geocode") == case.live_oid
        assert await _relation_oid(case.database, case.schema, case.stage_table) == stage_oid
        assert await _relation_oid(case.database, case.schema, "openaddresses_geocode_old") is None
        assert await _relation_oid(case.database, case.schema, case.recovery_table)
        assert await _relation_oid(case.database, case.schema, case.published_recovery_table) is None
        assert not case.context_by_name["context"].get("openaddresses_stage_published")
        await _publish_and_assert(case, monkeypatch, stage_oid)
    finally:
        await original_status(f'DROP SCHEMA IF EXISTS "{case.schema}" CASCADE;')
        await case.database.disconnect()


def _restart_context_by_name(case):
    return {
        "context": {"run": 0, "test_mode": True, "import_date": case.context_by_name["context"]["import_date"]},
        "import_date": case.context_by_name["import_date"],
    }


async def _assert_postpublish_restart(case, failure_tracker, live_oid, archived_oid):
    restart_context_by_name = _restart_context_by_name(case)
    await openaddresses.process_data(restart_context_by_name, case.task_by_name)
    assert restart_context_by_name["context"]["run"] == 1
    await openaddresses.shutdown(restart_context_by_name)
    assert failure_tracker.backfill_call_count == 2
    assert await _relation_oid(case.database, case.schema, "openaddresses_geocode") == live_oid
    assert await _relation_oid(case.database, case.schema, "openaddresses_geocode_old") == archived_oid
    assert await _relation_oid(case.database, case.schema, case.recovery_table) is None
    assert await _relation_oid(case.database, case.schema, case.published_recovery_table) is None
    assert case.zip_restore_calls == 1
    await openaddresses.shutdown(case.context_by_name)
    await openaddresses.shutdown(restart_context_by_name)
    assert failure_tracker.backfill_call_count == 2
    missing_context_by_name = _restart_context_by_name(case)
    missing_context_by_name["context"].update({"run": 1, "import_date": "missing20260814"})
    with pytest.raises(RuntimeError, match="staging table .* is missing"):
        await openaddresses.shutdown(missing_context_by_name)


@pytest.mark.parametrize("failure_phase", ["backfill", "recovery_drop"])
@pytest.mark.asyncio
async def test_real_postgres_postpublish_failure_resumes_without_second_swap(
    monkeypatch, tmp_path, failure_phase
):
    case, stage_oid = await _loaded_postgres_case(monkeypatch, tmp_path)
    original_status = case.database.status
    failure_tracker = SimpleNamespace(backfill_call_count=0, has_cleanup_failed=False)

    async def backfill(**_kwargs):
        failure_tracker.backfill_call_count += 1
        if failure_phase == "backfill" and failure_tracker.backfill_call_count == 1:
            raise RuntimeError("synthetic backfill failure")
        return openaddresses.OpenAddressesBackfillStats(0, 0, 0)

    async def fail_cleanup_once(statement, **params):
        target = f'DROP TABLE "{case.schema}"."{case.published_recovery_table}";'
        if (
            failure_phase == "recovery_drop"
            and not failure_tracker.has_cleanup_failed
            and statement == target
        ):
            failure_tracker.has_cleanup_failed = True
            raise RuntimeError("synthetic recovery drop failure")
        return await original_status(statement, **params)

    monkeypatch.setattr(openaddresses, "refresh_archive_geocodes_from_openaddresses_sharded", backfill)
    monkeypatch.setattr(case.database, "status", fail_cleanup_once)
    try:
        with pytest.raises(RuntimeError, match=f"synthetic {failure_phase.replace('_', ' ')} failure"):
            await openaddresses.shutdown(case.context_by_name)
        live_oid = await _relation_oid(case.database, case.schema, "openaddresses_geocode")
        archived_oid = await _relation_oid(case.database, case.schema, "openaddresses_geocode_old")
        assert live_oid == stage_oid
        assert archived_oid == case.live_oid
        assert await _relation_oid(case.database, case.schema, case.recovery_table) is None
        assert await _relation_oid(case.database, case.schema, case.published_recovery_table)
        assert case.context_by_name["context"]["openaddresses_stage_published"] is True
        await _assert_postpublish_restart(case, failure_tracker, live_oid, archived_oid)
    finally:
        await original_status(f'DROP SCHEMA IF EXISTS "{case.schema}" CASCADE;')
        await case.database.disconnect()


@pytest.mark.parametrize("persisted_batches", [0, 1, 3])
@pytest.mark.asyncio
async def test_real_postgres_failure_never_publishes_and_retry_resets(
    monkeypatch, tmp_path, persisted_batches
):
    case = await _prepare_postgres_case(monkeypatch, tmp_path, persisted_batches)
    try:
        retry_oid = await _fail_then_retry(case, monkeypatch, persisted_batches)
        await _publish_and_assert(case, monkeypatch, retry_oid)
    finally:
        await case.database.status(f'DROP SCHEMA IF EXISTS "{case.schema}" CASCADE;')
        await case.database.disconnect()
