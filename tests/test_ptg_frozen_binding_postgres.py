# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for immutable frozen multipart bindings."""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
import re
import asyncio
from typing import Any

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.connection import db
from db.migration_ptg2_frozen_source_file_binding import (
    install_frozen_source_file_binding,
    uninstall_frozen_source_file_binding,
)
from process.ptg_parts.frozen_rate_binding import (
    FrozenRateFileBindingMismatchError,
    frozen_rate_binding_from_params,
    normalize_protected_frozen_rate_params,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
    measure_frozen_binding_storage,
    recheck_frozen_binding,
)
from process.ptg_parts.frozen_rate_candidate import (
    validate_frozen_candidate_evidence,
)
from tests.ptg_frozen_test_support import (
    frozen_candidate_evidence,
    protected_control_payload,
)


POSTGRES_DSN_ENV = "HLTHPRT_PTG_FROZEN_BINDING_POSTGRES_DSN"
_DISPOSABLE_DATABASE_RE = re.compile(
    r"^ptg_frozen_binding_test_[a-z0-9][a-z0-9_]{7,}$"
)


class _MigrationOperations:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _configure_database(
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
) -> None:
    database_url = make_url(dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not database_url.host
        or not database_url.username
        or not _DISPOSABLE_DATABASE_RE.fullmatch(database_name)
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must target an explicit disposable "
            "PostgreSQL test database"
        )
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(database_url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(database_url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(database_url.username))
    monkeypatch.setenv(
        "HLTHPRT_DB_PASSWORD",
        str(database_url.password or ""),
    )
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)


async def _install_binding_schema(dsn: str) -> None:
    operations = _MigrationOperations()
    install_frozen_source_file_binding(operations, "mrf")
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute('CREATE SCHEMA IF NOT EXISTS "mrf"')
        for statement in operations.statements:
            await connection.execute(statement)
    finally:
        await connection.close()


async def _exercise_binding_cas(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    async with db.acquire() as connection:
        first_binding = await insert_or_compare_frozen_binding(
            connection,
            params_by_name,
        )
    async with db.acquire() as connection:
        replayed_binding = await insert_or_compare_frozen_binding(
            connection,
            params_by_name,
        )
    assert first_binding == replayed_binding
    assert await recheck_frozen_binding(params_by_name) == first_binding

    drifted_params = copy.deepcopy(params_by_name)
    drifted_params["source_key"] = "source-b"
    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="binding changed",
    ):
        async with db.acquire() as connection:
            await insert_or_compare_frozen_binding(
                connection,
                drifted_params,
            )
    assert first_binding is not None
    return first_binding


async def _exercise_date_input_binding(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    date_params_by_name = copy.deepcopy(params_by_name)
    date_params_by_name["source_file_import_id"] = "source-file-import-002"
    date_params_by_name["import_id"] = "source-file-import-002"
    date_params_by_name["import_month"] = dt.date(2026, 7, 1)
    async with db.acquire() as connection:
        inserted_binding = await insert_or_compare_frozen_binding(
            connection,
            date_params_by_name,
        )
    assert inserted_binding is not None
    assert inserted_binding["import_month"] == "2026-07-01"
    assert await recheck_frozen_binding(date_params_by_name) == inserted_binding
    return inserted_binding


async def _assert_database_guards(
    dsn: str,
) -> dict[str, dict[str, Any]]:
    """Verify immutable DDL, trigger shape, and stored payloads."""

    connection = await asyncpg.connect(dsn)
    try:
        stored_bindings = await _stored_bindings(connection)
        await _assert_immutable_binding_writes(connection)
        await _assert_binding_trigger_catalog(connection)
        row_count = await connection.fetchval(
            'SELECT count(*) FROM "mrf".'
            '"ptg2_frozen_source_file_binding"'
        )
        assert row_count == 4
    finally:
        await connection.close()
    return stored_bindings


async def _stored_bindings(connection) -> dict[str, dict[str, Any]]:
    binding_records = await connection.fetch(
        'SELECT source_file_import_id, import_month, binding_payload '
        'FROM "mrf"."ptg2_frozen_source_file_binding" '
        "ORDER BY source_file_import_id"
    )
    assert len(binding_records) == 4
    bindings_by_import_id: dict[str, dict[str, Any]] = {}
    for binding_record in binding_records:
        binding_payload = binding_record["binding_payload"]
        parsed_payload = (
            json.loads(binding_payload)
            if isinstance(binding_payload, str)
            else dict(binding_payload)
        )
        assert binding_record["import_month"] == dt.date(2026, 7, 1)
        assert parsed_payload["import_month"] == "2026-07-01"
        bindings_by_import_id[
            binding_record["source_file_import_id"]
        ] = parsed_payload
    return bindings_by_import_id


async def _assert_immutable_binding_writes(connection) -> None:
    statements = (
        'UPDATE "mrf"."ptg2_frozen_source_file_binding" '
        "SET source_key = 'source-b'",
        'DELETE FROM "mrf"."ptg2_frozen_source_file_binding"',
        'TRUNCATE "mrf"."ptg2_frozen_source_file_binding"',
    )
    for statement in statements:
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG2_FROZEN_SOURCE_FILE_BINDING_IMMUTABLE",
        ):
            await connection.execute(statement)
    try:
        await connection.execute("SET session_replication_role = replica")
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG2_FROZEN_SOURCE_FILE_BINDING_IMMUTABLE",
        ):
            await connection.execute(
                'UPDATE "mrf"."ptg2_frozen_source_file_binding" '
                "SET source_key = 'replica-bypass'"
            )
    finally:
        await connection.execute("SET session_replication_role = origin")


async def _assert_binding_trigger_catalog(connection) -> None:
    trigger_records = await connection.fetch(
        """
        SELECT trigger_catalog.tgname,
               trigger_catalog.tgenabled,
               pg_get_triggerdef(trigger_catalog.oid) AS definition,
               function_catalog.proname
          FROM pg_trigger AS trigger_catalog
          JOIN pg_class AS relation_catalog
            ON relation_catalog.oid = trigger_catalog.tgrelid
          JOIN pg_namespace AS namespace_catalog
            ON namespace_catalog.oid = relation_catalog.relnamespace
          JOIN pg_proc AS function_catalog
            ON function_catalog.oid = trigger_catalog.tgfoid
         WHERE namespace_catalog.nspname = 'mrf'
           AND relation_catalog.relname =
               'ptg2_frozen_source_file_binding'
           AND NOT trigger_catalog.tgisinternal
         ORDER BY trigger_catalog.tgname
        """
    )
    trigger_shapes = [
        (
            trigger_record["tgname"],
            _trigger_enable_mode(trigger_record["tgenabled"]),
            trigger_record["proname"],
        )
        for trigger_record in trigger_records
    ]
    assert trigger_shapes == [
        (
            "ptg2_frozen_source_file_binding_row_guard",
            "A",
            "guard_ptg2_frozen_source_file_binding",
        ),
        (
            "ptg2_frozen_source_file_binding_truncate_guard",
            "A",
            "guard_ptg2_frozen_source_file_binding",
        ),
    ]
    assert "BEFORE" in trigger_records[0]["definition"]
    assert "UPDATE" in trigger_records[0]["definition"]
    assert "DELETE" in trigger_records[0]["definition"]
    assert "BEFORE TRUNCATE" in trigger_records[1]["definition"]


def _trigger_enable_mode(raw_mode: str | bytes) -> str:
    return (
        raw_mode.decode("ascii")
        if isinstance(raw_mode, bytes)
        else raw_mode
    )


async def _exercise_concurrent_binding_cas(
    params_by_name: dict[str, Any],
) -> None:
    """Prove exact replay converges and a conflicting race cannot overwrite."""

    await _assert_exact_binding_race(params_by_name)
    successful_binding = await _conflicting_binding_race(params_by_name)
    stored_binding = await _stored_conflicting_binding()
    assert stored_binding == successful_binding


async def _assert_exact_binding_race(
    params_by_name: dict[str, Any],
) -> None:
    exact_params_by_name = copy.deepcopy(params_by_name)
    exact_params_by_name["source_file_import_id"] = "source-file-import-003"
    exact_params_by_name["import_id"] = "source-file-import-003"

    async def insert_exact():
        async with db.acquire() as connection:
            return await insert_or_compare_frozen_binding(
                connection,
                exact_params_by_name,
            )

    exact_results = await asyncio.gather(
        *(insert_exact() for _ in range(8))
    )
    assert exact_results == [exact_results[0]] * 8


async def _conflicting_binding_race(
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    left_params_by_name = copy.deepcopy(params_by_name)
    left_params_by_name["source_file_import_id"] = "source-file-import-004"
    left_params_by_name["import_id"] = "source-file-import-004"
    right_params_by_name = copy.deepcopy(left_params_by_name)
    right_params_by_name["source_key"] = "source-conflicting"

    async def insert_variant(variant_by_name):
        async with db.acquire() as connection:
            return await insert_or_compare_frozen_binding(
                connection,
                variant_by_name,
            )

    race_results = await asyncio.gather(
        insert_variant(left_params_by_name),
        insert_variant(right_params_by_name),
        return_exceptions=True,
    )
    successful_bindings = [
        race_outcome
        for race_outcome in race_results
        if isinstance(race_outcome, dict)
    ]
    rejected_bindings = [
        race_outcome
        for race_outcome in race_results
        if isinstance(
            race_outcome,
            FrozenRateFileBindingMismatchError,
        )
    ]
    assert len(successful_bindings) == 1
    assert len(rejected_bindings) == 1
    return successful_bindings[0]


async def _stored_conflicting_binding() -> dict[str, Any]:
    async with db.acquire() as connection:
        stored_records = await connection.all(
            db.text(
                'SELECT binding_payload FROM "mrf".'
                '"ptg2_frozen_source_file_binding" '
                "WHERE source_file_import_id = :source_file_import_id"
            ),
            source_file_import_id="source-file-import-004",
        )
    assert len(stored_records) == 1
    stored_payload = stored_records[0]._mapping["binding_payload"]
    if isinstance(stored_payload, str):
        stored_payload = json.loads(stored_payload)
    return dict(stored_payload)


async def _assert_downgrade_refusal(dsn: str) -> None:
    operations = _MigrationOperations()
    uninstall_frozen_source_file_binding(operations, "mrf")
    connection = await asyncpg.connect(dsn)
    try:
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG2_FROZEN_SOURCE_FILE_BINDING_DOWNGRADE_REFUSED",
        ):
            async with connection.transaction():
                for statement in operations.statements:
                    await connection.execute(statement)
        table_oid = await connection.fetchval(
            "SELECT to_regclass("
            "'mrf.ptg2_frozen_source_file_binding')::oid"
        )
        assert isinstance(table_oid, int)
    finally:
        await connection.close()


async def _assert_binding_storage() -> None:
    async with db.acquire() as connection:
        storage_measurement = await measure_frozen_binding_storage(
            connection
        )
    assert storage_measurement["retained_metadata"]["binding_rows"] == 4
    assert (
        storage_measurement["retained_metadata"][
            "binding_relation_total_bytes"
        ]
        > 0
    )
    assert set(storage_measurement["owned_payload_bytes"].values()) == {0}


def _assert_candidate_binding_proof(
    params_by_name: dict[str, Any],
    stored_binding: dict[str, Any],
) -> None:
    manifest, database_sources = frozen_candidate_evidence(
        params_by_name,
        stored_binding,
    )
    candidate_identity = validate_frozen_candidate_evidence(
        manifest,
        candidate_run_id="ptg2:source-file-import-001",
        database_binding=stored_binding,
        database_sources=database_sources,
    )
    assert "ptg_frozen_candidate_identity_v1" in str(candidate_identity)


@pytest.mark.asyncio
async def test_frozen_binding_migration_cas_and_candidate_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise binding CAS, immutable DDL, storage, and candidate proof."""

    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    _configure_database(monkeypatch, dsn)
    await _install_binding_schema(dsn)
    await db.disconnect()
    await db.connect()
    try:
        raw_params = protected_control_payload()["params"]
        assert isinstance(raw_params, dict)
        params_by_name = normalize_protected_frozen_rate_params(
            raw_params
        )
        assert params_by_name["import_month"] == "2026-07"
        expected_binding = await _exercise_binding_cas(params_by_name)
        date_input_binding = await _exercise_date_input_binding(
            params_by_name
        )
        await _exercise_concurrent_binding_cas(params_by_name)
        stored_binding_by_import_id = await _assert_database_guards(dsn)
        await _assert_binding_storage()
        stored_binding = stored_binding_by_import_id[
            "source-file-import-001"
        ]
        assert stored_binding == expected_binding
        assert (
            stored_binding_by_import_id["source-file-import-002"]
            == date_input_binding
        )
        _assert_candidate_binding_proof(
            params_by_name,
            stored_binding,
        )
        await _assert_downgrade_refusal(dsn)
    finally:
        await db.disconnect()
