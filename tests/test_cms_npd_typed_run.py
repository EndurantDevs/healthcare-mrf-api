# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The typed CMS NPD import run and its release SQL against recording asyncpg-style connections."""

import json

import pytest
from sqlalchemy.engine import URL

from process import cms_npd_typed_import as typed_import
from process import cms_npd_typed_release as release_sql
from process.cms_npd_typed_schema import TABLE_STATEMENTS, release_schema_name
from process.cms_npd_typed_stage import CmsNpdTypedImportError
from tests.test_cms_npd_typed_import import LocalSource, write_release

OLD_ID, OLDER_ID = "c" * 64, "d" * 64


class FakeConnection:
    """Records normalized statements; answers the few reads the importer makes."""

    def __init__(self, *, locked=True, releases=(), dropped=None):
        self.locked, self.releases, self.dropped = locked, list(releases), dropped
        self.statements, self.closed = [], False

    def _record(self, sql, args=()):
        self.statements.append(" ".join(sql.split()))
        self.last_args = args

    def transaction(self, **options):
        connection = self

        class _Transaction:
            async def __aenter__(self):
                connection.statements.append(f"BEGIN {options}" if options else "BEGIN")

            async def __aexit__(self, error_type, *_):
                connection.statements.append("COMMIT" if error_type is None else "ROLLBACK")

        return _Transaction()

    async def execute(self, sql, *args):
        self._record(sql, args)

    async def fetch(self, sql):
        self._record(sql)
        return self.releases

    async def fetchrow(self, sql, *args):
        self._record(sql, args)
        return self.dropped

    async def fetchval(self, sql, *args):
        self._record(sql, args)
        if "pg_try_advisory_lock" in sql:
            return self.locked
        return 7 if "FROM network" in sql else 0

    async def close(self):
        self.closed = True


def recorded(release_id, status, row_counts=None):
    return {
        "release_id": release_id,
        "schema_name": release_schema_name(release_id),
        "status": status,
        "row_counts": json.dumps(row_counts or {}),
    }


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """A local release, the progress calls, and a stage stand-in that reports every file."""
    observed = write_release(tmp_path / "release", 1)
    harness_by_field = {"observed": observed, "source": LocalSource(tmp_path / "release", observed)}
    harness_by_field["progress"] = progress_calls = []
    harness_by_field["staged"] = staged_calls = []
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_READER_USER", "api_reader")

    async def fake_progress(run_id, phase, *, unit, done, total, message):
        progress_calls.append((run_id, phase, unit, done, total, message))

    async def fake_stage(plans, schema, connect, scanner, *, concurrency, on_file_done):
        staged_calls.append((schema, scanner, concurrency))
        for done, plan in enumerate(plans, 1):
            await on_file_done(plan, done)
        return {table: 5 for plan in plans for table in plan.tables}

    monkeypatch.setattr(typed_import, "write_phase_progress", fake_progress)
    monkeypatch.setattr(typed_import, "stage_files", fake_stage)
    return harness_by_field


def connector(*connections):
    pending_connections = list(connections)

    async def connect():
        return pending_connections.pop(0)

    return connect


async def run_import(harness, *connections):
    return await typed_import.run(
        {}, {}, "run-1", source=harness["source"], connect=connector(*connections), scanner="scanner-bin"
    )


async def test_new_release_is_built_checked_granted_and_published(harness):
    current = recorded(OLD_ID, "current", {"practitioner": 5})
    older_by_field = {"release_id": OLDER_ID, "schema_name": release_schema_name(OLDER_ID)}
    lock = FakeConnection(releases=[current, recorded(OLDER_ID, "previous")], dropped=older_by_field)
    summary = await run_import(harness, lock)
    release_id = harness["observed"].vector_sha256
    schema = release_schema_name(release_id)
    assert summary["status"] == "published" and summary["schema"] == schema
    assert (summary["previous_release_id"], summary["dropped_release_id"]) == (OLD_ID, OLDER_ID)
    assert summary["row_counts"]["network"] == 7 and summary["row_counts"]["practitioner"] == 5
    assert summary["rows"] == sum(summary["row_counts"].values())
    assert set(summary["unresolved_counts"]) == set(release_sql.UNRESOLVED_QUERIES)
    assert set(summary["seconds"]) == {"observe", "download", "stage", "finish", "check", "publish"}
    assert harness["staged"] == [(schema, "scanner-bin", typed_import.STAGE_CONCURRENCY)]
    assert harness["source"].kept == {release_id, OLD_ID}

    statements = lock.statements
    assert statements[0] == "SELECT pg_try_advisory_lock($1)"
    assert statements[1] == 'SELECT release_id, schema_name, status, row_counts FROM "mrf".cms_npd_release'
    ordered_statements = [
        f'CREATE SCHEMA "{schema}"',
        f"SET LOCAL maintenance_work_mem = '{release_sql.MAINTENANCE_WORK_MEM}'",
        "SELECT count(*) FROM network",
        f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "api_reader"',
        "DELETE FROM \"mrf\".cms_npd_release WHERE status = 'previous' RETURNING release_id, schema_name",
        f'DROP SCHEMA IF EXISTS "{release_schema_name(OLDER_ID)}" CASCADE',
    ]
    assert [statements.index(statement) for statement in ordered_statements] == sorted(
        statements.index(statement) for statement in ordered_statements
    )
    assert lock.closed

    phases = [phase for _, phase, unit, *_ in harness["progress"] if unit == "phase"]
    assert phases == [
        f"cms-npd-typed-{name}" for name in ("observe", "download", "stage", "finish", "check", "publish")
    ]
    file_progress = [(done, total) for _, phase, unit, done, total, _ in harness["progress"] if unit == "files"]
    assert file_progress == [(done, 8) for done in range(1, 9)]


async def test_first_release_keeps_only_itself(harness, monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_READER_USER")
    lock = FakeConnection()
    summary = await run_import(harness, lock)
    assert (summary["previous_release_id"], summary["dropped_release_id"]) == (None, None)
    assert harness["source"].kept == {harness["observed"].vector_sha256}
    schema = release_schema_name(harness["observed"].vector_sha256)
    assert not any(statement.startswith("GRANT") for statement in lock.statements)
    drops = [statement for statement in lock.statements if statement.startswith("DROP SCHEMA")]
    assert drops == [f'DROP SCHEMA IF EXISTS "{schema}" CASCADE']  # only the pre-COPY reset of its own candidate
    insert = next(statement for statement in lock.statements if statement.startswith("INSERT"))
    assert "VALUES ($1, $2, 'current', $3, now(), $4, $5::jsonb, $6::jsonb, $7::jsonb)" in insert


async def test_busy_lock_refuses_and_closes(harness):
    lock = FakeConnection(locked=False)
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await run_import(harness, lock)
    assert caught.value.control_error_code == "cms_npd_typed_import_in_progress"
    assert lock.closed and lock.statements == ["SELECT pg_try_advisory_lock($1)"]


async def test_current_release_is_unchanged_without_download(harness):
    lock = FakeConnection(releases=[recorded(harness["observed"].vector_sha256, "current")])
    summary = await run_import(harness, lock)
    assert summary["status"] == "unchanged" and summary["schema"] == release_schema_name(summary["release_id"])
    assert set(summary["seconds"]) == {"observe"} and harness["staged"] == [] and harness["source"].kept is None


@pytest.mark.parametrize(
    ("recorded_release", "code"),
    [
        (lambda release_id: recorded(release_id, "previous"), "cms_npd_typed_release_is_previous"),
        (
            lambda release_id: {**recorded(OLD_ID, "current"), "schema_name": release_schema_name(release_id)},
            "cms_npd_typed_schema_collision",
        ),
    ],
)
async def test_previous_or_colliding_release_is_refused(harness, recorded_release, code):
    lock = FakeConnection(releases=[recorded_release(harness["observed"].vector_sha256)])
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await run_import(harness, lock)
    assert caught.value.control_error_code == code and harness["staged"] == [] and lock.closed


@pytest.mark.parametrize("failure", ["stage", "empty_table"])
async def test_failed_candidate_is_dropped_on_a_fresh_connection(harness, monkeypatch, failure):
    async def failing_stage(plans, *_args, **_kwargs):
        if failure == "stage":
            raise CmsNpdTypedImportError("cms_npd_typed_scanner_failed", "synthetic")
        return {table: 0 for plan in plans for table in plan.tables}

    monkeypatch.setattr(typed_import, "stage_files", failing_stage)
    lock, cleanup = FakeConnection(), FakeConnection()
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await run_import(harness, lock, cleanup)
    expected = "cms_npd_typed_scanner_failed" if failure == "stage" else "cms_npd_typed_table_empty"
    assert caught.value.control_error_code == expected
    schema = release_schema_name(harness["observed"].vector_sha256)
    assert cleanup.statements == [f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'] and cleanup.closed
    assert lock.closed and not any(statement.startswith("GRANT") for statement in lock.statements)
    assert not any(statement.startswith("INSERT") for statement in lock.statements)


async def test_schema_lifecycle_statements_run_in_their_transactions():
    connection = FakeConnection()
    await release_sql.create_release_schema(connection, "cms_npd_x")
    assert connection.statements[:4] == [
        "BEGIN",
        'DROP SCHEMA IF EXISTS "cms_npd_x" CASCADE',
        'CREATE SCHEMA "cms_npd_x"',
        'SET LOCAL search_path TO "cms_npd_x"',
    ]
    assert connection.statements[-1] == "COMMIT" and len(connection.statements) == 5 + len(TABLE_STATEMENTS)

    connection = FakeConnection()
    counts_by_name, network_rows = await release_sql.unresolved_counts(connection, "cms_npd_x")
    assert connection.statements[:2] == ["BEGIN {'readonly': True}", 'SET LOCAL search_path TO "cms_npd_x"']
    assert counts_by_name == dict.fromkeys(release_sql.UNRESOLVED_QUERIES, 0) and network_rows == 7


async def test_publish_without_previous_returns_nothing_and_identifiers_are_validated(monkeypatch):
    release_by_field = {
        "release_id": "a" * 64,
        "schema": "cms_npd_a",
        "generated_at": "2026-01-02",
        "run_id": "",
        "row_counts": {},
        "unresolved_counts": {},
        "files": {"f": {"etag": '"e"'}},
    }
    assert await release_sql.publish(FakeConnection(), "mrf", release_by_field) is None
    for identifier in ('bad"name', "1abc", "x" * 64):
        with pytest.raises(CmsNpdTypedImportError) as caught:
            release_sql.quoted(identifier)
        assert caught.value.control_error_code == "cms_npd_typed_identifier_invalid"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "control_1")
    assert release_sql.control_schema() == "control_1"
    connection = FakeConnection(releases=[recorded(OLD_ID, "current", {"practitioner": 3})])
    assert (await release_sql.release_rows(connection, "control_1"))["current"]["row_counts"] == {"practitioner": 3}


def test_artifact_root_uses_the_configured_or_temporary_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path / "durable"))
    assert typed_import._artifact_root() == tmp_path / "durable" / "cms-npd-typed"
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT")
    monkeypatch.setattr(typed_import.tempfile, "gettempdir", lambda: str(tmp_path / "scratch"))
    assert typed_import._artifact_root().is_dir()
    assert typed_import._artifact_root() == tmp_path / "scratch" / "cms-npd-typed"


def test_scanner_binary_is_required(monkeypatch):
    monkeypatch.setattr(typed_import, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(CmsNpdTypedImportError) as caught:
        typed_import._scanner_binary()
    assert caught.value.control_error_code == "cms_npd_typed_scanner_missing"
    monkeypatch.setattr(typed_import, "_ptg2_rust_scanner_binary", lambda: "/opt/scanner")
    assert typed_import._scanner_binary() == "/opt/scanner"


async def test_default_connection_uses_the_application_database_url(monkeypatch):
    import db.models

    application_url = URL.create(
        "postgresql+asyncpg", username="app", password="example", host="db.example.test", database="app"
    )

    class FakeDatabase:
        engine = None

        async def connect(self):
            self.engine = type("Engine", (), {"url": application_url})

    connected_dsns = []

    async def fake_connect(*, dsn):
        connected_dsns.append(dsn)
        return "connection"

    monkeypatch.setattr(db.models, "db", FakeDatabase())
    monkeypatch.setattr(typed_import.asyncpg, "connect", fake_connect)
    assert await typed_import.connect_default() == "connection"
    db.models.db.connect = None  # an already connected engine is reused
    assert await typed_import.connect_default() == "connection"
    expected = application_url.set(drivername="postgresql").render_as_string(hide_password=False)
    assert connected_dsns == [expected] * 2


def test_http_source_observes_and_downloads_through_the_source_module(tmp_path, monkeypatch):
    calls = []

    def fake_observe(*, client, base_url):
        calls.append(("observe", base_url, client.timeout.connect))
        return "observed"

    def fake_download(root, observed, *, base_url, parallel):
        calls.append(("download", root, observed, base_url, parallel))
        return root / "release"

    monkeypatch.setattr(typed_import.source_module, "observe_release", fake_observe)
    monkeypatch.setattr(typed_import.source_module, "download_release", fake_download)
    source = typed_import.HttpSource(tmp_path, base_url="https://downloads.example.test")
    assert source.observe() == "observed"
    assert source.fetch("observed") == tmp_path / "release"
    assert calls == [
        ("observe", "https://downloads.example.test", 30.0),
        ("download", tmp_path, "observed", "https://downloads.example.test", typed_import.DOWNLOAD_PARALLEL),
    ]
    typed_import.HttpSource(tmp_path / "empty").prune(set())  # no releases directory yet: nothing to do
