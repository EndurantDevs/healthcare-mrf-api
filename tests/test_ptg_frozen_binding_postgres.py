# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for immutable frozen multipart bindings."""

from __future__ import annotations

import copy
import datetime as dt
import json
import os
import re
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
    FROZEN_RATE_FILE_BINDING_OPTION,
    FrozenRateFileBindingMismatchError,
    frozen_rate_binding_from_params,
    normalize_protected_frozen_rate_params,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
    recheck_frozen_binding,
)
from process.ptg_parts.frozen_rate_candidate import (
    validate_frozen_candidate_evidence,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_PROOF_CONTRACT,
    frozen_rate_file_proof_sha256,
)
from tests.ptg_frozen_test_support import protected_control_payload


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


def _candidate_evidence(
    params_by_name: dict[str, Any],
    binding_by_name: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    descriptors = params_by_name["frozen_rate_files"]
    proof_rows = [
        {
            "contract": FROZEN_RATE_FILE_PROOF_CONTRACT,
            **descriptor,
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in descriptors
    ]
    manifest_by_name = {
        "source_file_import_id": params_by_name[
            "source_file_import_id"
        ],
        "frozen_rate_file_set_contract": params_by_name[
            "frozen_rate_file_set_contract"
        ],
        "frozen_rate_files": descriptors,
        "frozen_rate_file_set_sha256": params_by_name[
            "frozen_rate_file_set_sha256"
        ],
        "frozen_rate_file_count": params_by_name[
            "frozen_rate_file_count"
        ],
        "frozen_rate_file_proof": proof_rows,
        "frozen_rate_file_proof_sha256": (
            frozen_rate_file_proof_sha256(proof_rows)
        ),
        "source_file_versions": [
            {
                **descriptor,
                "url": descriptor["canonical_url"],
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": "downloaded",
            }
            for descriptor in descriptors
        ],
        FROZEN_RATE_FILE_BINDING_OPTION: binding_by_name,
    }
    database_sources = [
        {
            "source_key": ordinal,
            "raw_container_sha256": descriptor["raw_sha256"],
            "source_file_version_count": 1,
            "source_file_version_id": descriptor[
                "engine_source_file_version_id"
            ],
            "version_raw_sha256": descriptor["raw_sha256"],
        }
        for ordinal, descriptor in enumerate(descriptors)
    ]
    return manifest_by_name, database_sources


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
    connection = await asyncpg.connect(dsn)
    try:
        binding_rows = await connection.fetch(
            'SELECT source_file_import_id, import_month, binding_payload '
            'FROM "mrf"."ptg2_frozen_source_file_binding" '
            "ORDER BY source_file_import_id"
        )
        assert len(binding_rows) == 2
        stored_binding_by_import_id: dict[str, dict[str, Any]] = {}
        for binding_row in binding_rows:
            binding_payload = binding_row["binding_payload"]
            parsed_payload = (
                json.loads(binding_payload)
                if isinstance(binding_payload, str)
                else dict(binding_payload)
            )
            assert binding_row["import_month"] == dt.date(2026, 7, 1)
            assert parsed_payload["import_month"] == "2026-07-01"
            stored_binding_by_import_id[
                binding_row["source_file_import_id"]
            ] = (
                parsed_payload
            )
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG2_FROZEN_SOURCE_FILE_BINDING_IMMUTABLE",
        ):
            await connection.execute(
                'UPDATE "mrf"."ptg2_frozen_source_file_binding" '
                "SET source_key = 'source-b'"
            )
        row_count = await connection.fetchval(
            'SELECT count(*) FROM "mrf".'
            '"ptg2_frozen_source_file_binding"'
        )
        assert row_count == 2
    finally:
        await connection.close()
    return stored_binding_by_import_id


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


@pytest.mark.asyncio
async def test_frozen_binding_migration_cas_and_candidate_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        stored_binding_by_import_id = await _assert_database_guards(dsn)
        stored_binding = stored_binding_by_import_id[
            "source-file-import-001"
        ]
        assert stored_binding == expected_binding
        assert (
            stored_binding_by_import_id["source-file-import-002"]
            == date_input_binding
        )
        manifest, database_sources = _candidate_evidence(
            params_by_name,
            stored_binding,
        )
        candidate_identity = validate_frozen_candidate_evidence(
            manifest,
            candidate_run_id="ptg2:source-file-import-001",
            database_binding=stored_binding,
            database_sources=database_sources,
        )
        assert "ptg_frozen_candidate_identity_v1" in str(
            candidate_identity
        )
        await _assert_downgrade_refusal(dsn)
    finally:
        await db.disconnect()
