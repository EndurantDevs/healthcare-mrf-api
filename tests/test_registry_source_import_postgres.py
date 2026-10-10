# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the receipt CLI through its configured pool and native CMS admission."""

import asyncio
import json
import re
import sys
from dataclasses import asdict, replace
from uuid import uuid4

import pytest

from db.connection import Database
from process import registry_source_import as source_import
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_source_admission_postgres import _edition, _input

pytest.importorskip("ptg2_address_canon")


def _arguments(input_file, edition):
    arguments = ["--input-file", str(input_file)]
    for field, value in asdict(edition).items():
        if field == "published_at":
            value = value.isoformat() if value is not None else "unknown"
        arguments.extend(("--" + field.replace("_", "-"), str(value)))
    return arguments


def _configured_pool(monkeypatch, engine, schema):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    connection_url = engine.url
    for field, value in (
        ("DRIVER", "postgresql+asyncpg"),
        ("USER", connection_url.username),
        ("PASSWORD", connection_url.password or ""),
        ("HOST", connection_url.host),
        ("PORT", connection_url.port),
        ("DATABASE_OVERRIDE", connection_url.database),
        ("POOL_MIN_SIZE", 1),
        ("POOL_MAX_SIZE", 1),
        ("ECHO", "true"),
    ):
        monkeypatch.setenv("HLTHPRT_DB_" + field, str(value))
    databases = []

    def database_factory():
        """Observe real owned pools while retaining the production driver bridge."""
        database = Database()
        databases.append(database)
        return database

    monkeypatch.setattr(source_import, "Database", database_factory)
    return databases


async def _invoke(arguments, capsys):
    status = await asyncio.to_thread(source_import.run_command, arguments)
    captured = capsys.readouterr()
    if status == 0:
        assert captured.err == "" and captured.out.count("\n") == 1
        return status, json.loads(captured.out)
    assert captured.out == "" and captured.err.count("\n") == 1
    return status, json.loads(captured.err)


async def _source_totals(connection, schema):
    tables = (
        "registry_source_snapshot",
        "registry_source_observation",
        "registry_identifier_observation",
        "registry_issuer_company_assertion",
        "registry_company_group_assertion",
        "company_registry",
        "company_group_registry",
        "registry_identifier_binding",
    )
    projections = ",".join(f'(SELECT count(*) FROM "{schema}".{table}) AS {table}' for table in tables)
    return dict(await connection.fetchrow("SELECT " + projections))


@pytest.mark.asyncio
async def test_cli_native_admission_replay_and_explicit_later_edition(serving_schema, monkeypatch, tmp_path, capsys):
    connection, schema, engine = serving_schema
    databases = _configured_pool(monkeypatch, engine, schema)
    input_bytes = _input()
    input_file = tmp_path / "source.csv"
    input_file.write_bytes(input_bytes)
    earlier = _edition(input_bytes)
    status, receipt_by_name = await _invoke(_arguments(input_file, earlier), capsys)
    assert status == 0 and receipt_by_name["snapshot_id"] == str(earlier.snapshot_id)
    assert receipt_by_name["observations"] == 2 and receipt_by_name["resolved_issuers"] == 2
    assert receipt_by_name["identity_materialization"]["companies_created"] == 1
    assert receipt_by_name["native_counts"]["input_rows"] == 2 and receipt_by_name["copy_bytes"] > 21
    before_totals_by_table = await _source_totals(connection, schema)
    status, replay_by_name = await _invoke(_arguments(input_file, earlier), capsys)
    assert status == 0 and replay_by_name["replayed"] is True
    assert replay_by_name["copy_sha256"] == receipt_by_name["copy_sha256"]
    assert replay_by_name["identity_materialization"]["companies_created"] == 0
    assert await _source_totals(connection, schema) == before_totals_by_table
    later = replace(earlier, snapshot_id=uuid4(), edition_id="2025", reporting_year=2025, published_at=None)
    status, later_by_name = await _invoke(_arguments(input_file, later), capsys)
    assert status == 0 and later_by_name["replayed"] is False
    assert later_by_name["identity_materialization"]["companies_created"] == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 2
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 4
    assert tuple(
        await connection.fetchrow(
            f'SELECT reporting_year,published_at FROM "{schema}".registry_source_snapshot WHERE snapshot_id=$1',
            later.snapshot_id,
        )
    ) == (2025, None)
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)
    assert len(databases) == 3 and all(database.engine is None for database in databases)
    serialized = json.dumps(receipt_by_name)
    assert len(serialized.encode()) <= 8192
    assert all(
        excluded_value not in serialized
        for excluded_value in (str(input_file), earlier.source_url, "Synthetic Company", "Synthetic Group")
    )
    assert set(receipt_by_name) == set(source_import._COUNTERS) | {
        "status",
        "snapshot_id",
        "input_sha256",
        "artifact_sha256",
        "copy_sha256",
        "copy_bytes",
        "replayed",
        "identity_materialization",
        "native_counts",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("digest_mismatch", "input_limit", "input_unavailable"))
async def test_input_failures_precede_database_creation(serving_schema, monkeypatch, tmp_path, capsys, failure):
    connection, schema, engine = serving_schema
    databases = _configured_pool(monkeypatch, engine, schema)
    input_bytes = b"a" * (source_import.MAX_INPUT_BYTES + 1) if failure == "input_limit" else _input()
    input_file = tmp_path / "source.csv"
    if failure != "input_unavailable":
        input_file.write_bytes(input_bytes)
    edition = _edition(input_bytes)
    if failure == "digest_mismatch":
        edition = replace(edition, input_sha256="b" * 64)
    status, error_by_name = await _invoke(_arguments(input_file, edition), capsys)
    assert status == 1 and error_by_name == {"status": "error", "code": failure}
    assert databases == []
    assert not any((await _source_totals(connection, schema)).values())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("native_csv", "durable_assertion"))
async def test_whole_edition_failure_rolls_back_and_closes_pool(serving_schema, monkeypatch, tmp_path, capsys, failure):
    connection, schema, engine = serving_schema
    databases = _configured_pool(monkeypatch, engine, schema)
    input_bytes = _input("Synthetic\0Company") if failure == "native_csv" else _input()
    if failure == "durable_assertion":
        await connection.execute(
            f'ALTER TABLE "{schema}".registry_issuer_company_assertion '
            "ADD CONSTRAINT cli_reject_company CHECK(company_id IS NULL)"
        )
    input_file = tmp_path / "source.csv"
    input_file.write_bytes(input_bytes)
    status, error_by_name = await _invoke(_arguments(input_file, _edition(input_bytes)), capsys)
    assert status == 1 and error_by_name == {"status": "error", "code": "admission_failed"}
    assert len(databases) == 1 and databases[0].engine is None
    assert not any((await _source_totals(connection, schema)).values())


@pytest.mark.asyncio
async def test_changed_snapshot_metadata_is_rejected_without_partial_changes(
    serving_schema, monkeypatch, tmp_path, capsys
):
    connection, schema, engine = serving_schema
    databases = _configured_pool(monkeypatch, engine, schema)
    input_bytes = _input()
    input_file = tmp_path / "source.csv"
    input_file.write_bytes(input_bytes)
    edition = _edition(input_bytes)
    assert (await _invoke(_arguments(input_file, edition), capsys))[0] == 0
    before_totals_by_table = await _source_totals(connection, schema)
    changed = replace(edition, reporting_year=2026)
    status, error_by_name = await _invoke(_arguments(input_file, changed), capsys)
    assert status == 1 and error_by_name == {"status": "error", "code": "admission_failed"}
    assert await _source_totals(connection, schema) == before_totals_by_table
    assert await connection.fetchval(f'SELECT reporting_year FROM "{schema}".registry_source_snapshot') == 2024
    assert len(databases) == 2 and all(database.engine is None for database in databases)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "primary_error,expected_status,expected_code",
    (
        (KeyboardInterrupt, 130, "canceled"),
        (asyncio.CancelledError, 130, "canceled"),
        (None, 1, "admission_failed"),
    ),
    ids=("keyboard-and-disconnect", "cancel-and-disconnect", "disconnect-after-replay"),
)
async def test_cancellation_and_disconnect_failures_close_configured_pool(
    serving_schema, monkeypatch, tmp_path, capsys, primary_error, expected_status, expected_code
):
    connection, schema, engine = serving_schema
    databases = _configured_pool(monkeypatch, engine, schema)
    input_bytes = _input()
    input_file = tmp_path / "source.csv"
    input_file.write_bytes(input_bytes)
    edition = _edition(input_bytes)
    assert (await _invoke(_arguments(input_file, edition), capsys))[0] == 0
    before_totals_by_table = await _source_totals(connection, schema)
    assert len(before_totals_by_table) == 8
    assert before_totals_by_table["registry_source_snapshot"] == 1
    assert before_totals_by_table["registry_source_observation"] == 2
    original_disconnect = Database.disconnect
    released_databases = []

    async def disconnect_then_fail(database):
        assert database.engine.pool.checkedout() == 0
        await original_disconnect(database)
        assert database.engine is None
        released_databases.append(database)
        raise RuntimeError("Synthetic cleanup diagnostic")

    async def cancel_before_write(driver, source_bytes, source_edition):
        assert driver.is_in_transaction()
        assert source_bytes == input_bytes and source_edition == edition
        raise primary_error("Synthetic admission diagnostic")

    monkeypatch.setattr(Database, "disconnect", disconnect_then_fail)
    if primary_error is not None:
        monkeypatch.setattr(source_import, "admit_cms_mlr_edition", cancel_before_write)
    status, error_by_name = await _invoke(_arguments(input_file, edition), capsys)
    assert status == expected_status and error_by_name == {"status": "error", "code": expected_code}
    assert await _source_totals(connection, schema) == before_totals_by_table
    assert len(databases) == 2 and all(database.engine is None for database in databases)
    assert released_databases == [databases[1]]


@pytest.mark.parametrize(
    "argument,value",
    (
        ("--snapshot-id", "invalid-private-snapshot"),
        ("--source-system", "unsupported-private-source"),
        ("--reporting-year", "private-not-a-year"),
        ("--published-at", "private-not-a-date"),
        ("--published-at", "2025-09-12T00:00:00"),
    ),
)
def test_parser_errors_redact_values(argument, value, tmp_path, capsys, monkeypatch):
    edition = _edition(_input())
    arguments = _arguments(tmp_path / "missing.csv", edition)
    arguments[arguments.index(argument) + 1] = value
    monkeypatch.setattr(source_import, "Database", lambda: pytest.fail("Unexpected database access"))
    with pytest.raises(SystemExit) as exit_status:
        source_import.run_command(arguments)
    captured = capsys.readouterr()
    assert exit_status.value.code == 2 and captured.out == ""
    assert json.loads(captured.err) == {"status": "error", "code": "invalid_arguments"}


@pytest.mark.parametrize(
    "argument,value",
    (
        ("--input-sha256", "a" * 63),
        ("--artifact-sha256", "A" * 64),
        ("--reporting-year", "2101"),
        ("--snapshot-id", "00000000-0000-0000-0000-000000000000"),
    ),
)
def test_edition_metadata_is_validated_before_file_or_pool(argument, value, tmp_path, capsys, monkeypatch):
    arguments = _arguments(tmp_path / "missing.csv", _edition(_input()))
    arguments[arguments.index(argument) + 1] = value
    monkeypatch.setattr(source_import, "Database", lambda: pytest.fail("Unexpected database access"))
    assert source_import.run_command(arguments) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and json.loads(captured.err) == {"status": "error", "code": "invalid_arguments"}


@pytest.mark.parametrize("argument", ("--dsn", "--input-sha"))
def test_no_connection_arguments_or_abbreviations(tmp_path, capsys, argument):
    arguments = _arguments(tmp_path / "missing.csv", _edition(_input()))
    with pytest.raises(SystemExit) as exit_status:
        source_import.run_command(arguments + [argument, "private-connection-value"])
    assert exit_status.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == "" and json.loads(captured.err) == {"status": "error", "code": "invalid_arguments"}


@pytest.mark.asyncio
async def test_module_entrypoint_admits_through_actual_configured_database(serving_schema, monkeypatch, tmp_path):
    connection, schema, engine = serving_schema
    _configured_pool(monkeypatch, engine, schema)
    input_bytes = _input()
    input_file = tmp_path / "source.csv"
    input_file.write_bytes(input_bytes)
    edition = _edition(input_bytes)
    cli_process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "process.registry_source_import",
        *_arguments(input_file, edition),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(cli_process.communicate(), timeout=20)
    finally:
        if cli_process.returncode is None:
            cli_process.kill()
            await cli_process.wait()
    assert cli_process.returncode == 0 and stdout.count(b"\n") == 1
    # Python 3.14 can report the dependency's syntax warnings before the CLI starts.
    assert (
        stderr == b""
        or re.fullmatch(
            rb"(?:[^\r\n]+/sanic/server/websockets/impl\.py:[0-9]+: SyntaxWarning: "
            rb"'return' in a 'finally' block\r?\n[ \t]+return\r?\n)+",
            stderr,
        )
        is not None
    )
    receipt_by_name = json.loads(stdout)
    assert receipt_by_name["snapshot_id"] == str(edition.snapshot_id) and receipt_by_name["observations"] == 2
    assert receipt_by_name["identity_materialization"]["companies_created"] == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 2
    assert b"Synthetic Company" not in stdout and str(input_file).encode() not in stdout
