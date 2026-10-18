# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import csv
import datetime
import importlib
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

os.environ.setdefault("HLTHPRT_NUCC_DOWNLOAD_URL_DIR", "https://nucc.org")
os.environ.setdefault("HLTHPRT_NUCC_DOWNLOAD_URL_FILE", "/feed.html")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")


@pytest.fixture
def nucc_module():
    return importlib.import_module("process.nucc")


@pytest.fixture(autouse=True)
def legacy_native_boundary(monkeypatch, nucc_module):
    """Host parser tests do not establish database custody or open native connections."""

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace()

    monkeypatch.setattr(nucc_module.db, "transaction", transaction)
    monkeypatch.setattr(nucc_module.protected_source, "is_protected_source", AsyncMock(return_value=False))
    monkeypatch.setattr(nucc_module.native, "bind_nucc_native_attempt", AsyncMock(return_value=None))
    monkeypatch.setattr(nucc_module.native, "is_nucc_native_handoff_required", AsyncMock(return_value=False))
    monkeypatch.setattr(nucc_module.native, "require_nucc_unretained_rotation", AsyncMock())
    monkeypatch.setattr(nucc_module, "_prepare_plain_stage", AsyncMock())


@pytest.mark.asyncio
async def test_prepare_nucc_import_requires_import_date(nucc_module):
    with pytest.raises(KeyError, match="import_date"):
        await nucc_module._prepare_nucc_import({}, {})


def test_nucc_taxonomy_row_preserves_nullable_columns(nucc_module):
    taxonomy_by_field = {"Code": "1234", "Classification": None}
    csv_map = {"Code": "code", "Classification": "classification"}

    normalized_row = nucc_module._nucc_taxonomy_row(taxonomy_by_field, csv_map)

    assert normalized_row["code"] == "1234"
    assert normalized_row["classification"] is None
    assert "int_code" in normalized_row


def test_report_nucc_source_progress_preserves_lifecycle(monkeypatch, nucc_module):
    progress_event_list = []
    monkeypatch.setattr(
        nucc_module,
        "enqueue_live_progress",
        lambda **event_by_name: progress_event_list.append(event_by_name),
    )

    for completed in (False, True):
        nucc_module._report_nucc_source_progress(
            "run_123",
            "nucc.csv",
            file_index=0,
            file_count=1,
            completed=completed,
        )

    assert [event["phase"] for event in progress_event_list] == [
        "nucc downloading source",
        "nucc source processed",
    ]
    assert [event["done"] for event in progress_event_list] == [0, 1]
    assert [event["message"] for event in progress_event_list] == [
        "downloading file 1/1",
        "processed file 1/1",
    ]


@pytest.mark.asyncio
async def test_process_data_extracts_records(monkeypatch, nucc_module, tmp_path):
    html = '<a href="/images/stories/CSV/nucc_taxonomy_001.csv">download</a>'

    async def fake_download(path):
        return html

    async def fake_download_and_save(url, filepath, **kwargs):
        content = "Code,Classification,Grouping\n1234,Sample Classification,Group" + "\n"
        Path(filepath).write_text(content)

    push_calls = []

    async def fake_push(_ctx, cls, objects):
        push_calls.append((cls.__tablename__, False, list(objects)))

    def fake_make_class(base_cls, suffix):
        table = SimpleNamespace(name=f"{base_cls.__tablename__}_{suffix}", schema="mrf")
        return SimpleNamespace(
            __main_table__=base_cls.__tablename__,
            __tablename__=table.name,
            __table__=table,
            __my_index_elements__=getattr(base_cls, "__my_index_elements__", []),
        )

    monkeypatch.setattr(nucc_module, "download_it", fake_download)
    monkeypatch.setattr(nucc_module, "download_it_and_save", fake_download_and_save)
    monkeypatch.setattr(nucc_module, "_write_nucc_batch", fake_push)
    monkeypatch.setattr(nucc_module, "make_class", fake_make_class)
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())

    import_context_map = {"import_date": "20260101", "context": {"_control_attempt_id": "synthetic"}}
    await nucc_module.process_data(import_context_map)

    assert push_calls
    table, rewrite, taxonomy_rows = push_calls[0]
    assert table == "nucc_taxonomy_20260101"
    assert rewrite is False
    assert taxonomy_rows[0]["code"] == "1234"


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
async def test_nucc_csv_preserves_text_across_read_chunks(monkeypatch, nucc_module, tmp_path, encoding):
    definition_text_list = [
        f'{row_number}: A quoted "term", a newline\r\n and UTF-8 café. ' * 16 for row_number in range(1000)
    ]
    source_csv_path = tmp_path / "nucc.csv"
    with source_csv_path.open("w", encoding=encoding, newline="") as csv_stream:
        csv_writer = csv.writer(csv_stream)
        csv_writer.writerow(["Code", "Definition"])
        for row_number, definition_text in enumerate(definition_text_list):
            csv_writer.writerow([f"{row_number:010d}", definition_text])
    taxonomy_row_list = []

    async def capture_rows(_ctx, _model, taxonomy_row_batch):
        taxonomy_row_list.extend(taxonomy_row_batch)

    monkeypatch.setattr(nucc_module, "_write_nucc_batch", capture_rows)
    csv_map = await nucc_module._read_nucc_csv_map(str(source_csv_path))
    row_count = await nucc_module._stage_nucc_taxonomy_rows(
        {"context": {}},
        {},
        str(source_csv_path),
        csv_map,
        object(),
        test_mode=False,
        run_id="",
        source_file="nucc.csv",
    )

    assert row_count == len(definition_text_list)
    for row_number, taxonomy_row in enumerate(taxonomy_row_list):
        code = f"{row_number:010d}"
        assert taxonomy_row == {
            "code": code,
            "definition": definition_text_list[row_number],
            "int_code": nucc_module.return_checksum([code], crc=32),
        }


@pytest.mark.asyncio
async def test_startup_sets_context_without_shared_payload_tables(monkeypatch, nucc_module):
    create_calls = []
    status_calls = []

    monkeypatch.setattr(
        nucc_module,
        "make_class",
        lambda cls, suffix: SimpleNamespace(
            __main_table__=cls.__tablename__,
            __tablename__=f"{cls.__tablename__}_{suffix}",
            __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
            __my_index_elements__=["code"],
        ),
    )

    monkeypatch.setattr(nucc_module, "init_db", AsyncMock())
    monkeypatch.setattr(
        nucc_module.db, "create_table", AsyncMock(side_effect=lambda table, **kw: create_calls.append(table.name))
    )
    monkeypatch.setattr(nucc_module.db, "status", AsyncMock(side_effect=lambda stmt: status_calls.append(stmt)))
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())

    startup_context_map: dict[str, object] = {}
    await nucc_module.startup(startup_context_map)

    assert startup_context_map["context"]["run"] == 0
    assert (datetime.datetime.utcnow() - startup_context_map["context"]["start"]).total_seconds() < 2
    assert not create_calls
    assert not status_calls


@pytest.fixture
def nucc_publication_driver(monkeypatch, nucc_module):
    """Use the publication session's driver and reject a separate connection."""
    driver = object()
    session = SimpleNamespace(
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        )
    )

    @asynccontextmanager
    async def transaction():
        yield session
        session.committed = True

    def separate_connection():
        raise AssertionError("publication must keep its original transaction")

    monkeypatch.setattr(nucc_module.db, "transaction", transaction)
    monkeypatch.setattr(nucc_module.db, "acquire_driver", separate_connection)
    stage_by_field = {
        "schema": "mrf",
        "name": "nucc_taxonomy_s_synthetic",
        "oid": 100,
        "owner_oid": 7,
        "previous_oid": 99,
    }
    events = []

    async def validate(actual_session, schema, name):
        assert actual_session is session and (schema, name) == ("mrf", stage_by_field["name"])
        events.append("validate")
        return {"row_count": 7}

    return SimpleNamespace(
        driver=driver, session=session, stage_by_field=stage_by_field, events=events, validate=validate
    )


@pytest.mark.asyncio
async def test_shutdown_finishes_indexes_then_rotates_on_the_original_transaction(
    monkeypatch, nucc_module, nucc_publication_driver
):
    """Build indexes before atomic rotation and preserve the original native transaction."""
    events = nucc_publication_driver.events
    stage_by_field = nucc_publication_driver.stage_by_field
    original_session = nucc_publication_driver.session

    async def execute(statement):
        sql = str(statement)
        if "ALTER TABLE" in sql or "DROP TABLE" in sql:
            preserved_reads.assert_awaited_once()
            assert events.index("indexes") < events.index("validate")
        events.append(sql)

    async def finish(session, *_args, **options):
        assert session is original_session and options["create_constraints"] is True
        assert options["physical_names"] == {"nucc_taxonomy": stage_by_field["name"]}
        events.append("indexes")

    async def require_rotation(*_args, **_options):
        assert events.index("indexes") < events.index("validate")
        events.append("retention")

    monkeypatch.setattr(nucc_module.native, "require_nucc_unretained_rotation", require_rotation)
    preserved_reads = AsyncMock()
    commit_run = AsyncMock(return_value={"heartbeat_at": "actual-heartbeat", "finished_at": "actual-finished"})
    original_session.execute = AsyncMock(side_effect=execute)
    monkeypatch.setattr(nucc_module, "prepare_local_native_input_publication", preserved_reads)
    monkeypatch.setattr(nucc_module, "_plain_run", AsyncMock())
    monkeypatch.setattr(nucc_module, "_require_plain_stage", AsyncMock())
    monkeypatch.setattr(nucc_module.native, "_relation_oid", AsyncMock(return_value=99))
    monkeypatch.setattr(nucc_module.native, "_create_model_indexes", finish)
    monkeypatch.setattr(nucc_module.native, "_validate_nucc_table_set", nucc_publication_driver.validate)
    monkeypatch.setattr(nucc_module, "_commit_plain_run", commit_run)
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())
    captured_time_by_name = {}
    monkeypatch.setattr(nucc_module, "print_time_info", lambda start: captured_time_by_name.setdefault("start", start))
    context_by_field = {
        "import_date": "s_synthetic",
        "context": {
            "run": 1,
            "rows": 7,
            "control_run_id": "run-nucc",
            "nucc_plain_stage": stage_by_field,
            "start": datetime.datetime.utcnow() - datetime.timedelta(seconds=5),
        },
    }
    result_by_field = await nucc_module.shutdown(context_by_field)
    preserved_reads.assert_awaited_once_with(
        original_session, ledger_schema="mrf", relations=(("nucc_taxonomy", stage_by_field["name"]),)
    )
    assert original_session.committed is True and captured_time_by_name["start"]
    assert result_by_field["rows"] == 7 and result_by_field["terminal_progress"]["phase"] == "nucc published"
    assert commit_run.await_args.args == (original_session, context_by_field, result_by_field)
    assert context_by_field["context"]["control_run_terminal_committed"] is True
    assert context_by_field["context"]["_control_committed_finished_at"] == "actual-finished"
    event_count = len(events)
    await nucc_module.shutdown(context_by_field)
    assert len(events) == event_count


@pytest.mark.asyncio
async def test_protected_startup_defers_all_payload_ddl(monkeypatch, nucc_module):
    monkeypatch.setattr(nucc_module, "init_db", AsyncMock())
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(nucc_module.native, "is_nucc_native_handoff_required", AsyncMock(return_value=True))
    status, create = AsyncMock(), AsyncMock()
    monkeypatch.setattr(nucc_module.db, "status", status)
    monkeypatch.setattr(nucc_module.db, "create_table", create)
    await nucc_module.startup({})
    status.assert_not_awaited()
    create.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_prepare_preserves_the_actual_control_attempt(monkeypatch, nucc_module):
    attempt_by_field = {
        "_control_attempt_id": "run:" + "a" * 32,
        "_control_attempt_started_at": "2026-10-01T00:00:00+00:00",
    }
    context_by_field = {"import_date": "20260101", "control_run_id": "run", "context": attempt_by_field.copy()}

    async def bind(_session, received, *, schema_name):
        assert received is context_by_field and received["context"] | attempt_by_field == received["context"]
        assert schema_name == "mrf"
        received["import_date"] = "native_suffix"
        received["context"]["nucc_native_stage"] = {"original": "legacy_native"}

    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(nucc_module.native, "bind_nucc_native_attempt", bind)
    assert await nucc_module._prepare_nucc_import(context_by_field, {}) == ("native_suffix", "run", False)
    assert all(context_by_field["context"][key] == value for key, value in attempt_by_field.items())


@pytest.mark.asyncio
async def test_native_csv_uses_bounded_copy_without_legacy_inserts(monkeypatch, nucc_module, tmp_path):
    source = tmp_path / "taxonomy.csv"
    source.write_text("Code,Definition\n" + "1234567890,synthetic\n" * 5001)
    copied_batch_sizes = []

    async def copy(_session, stage, model, row_count):
        assert stage == {"synthetic": "custody"} and model == "model"
        copied_batch_sizes.append(len(row_count))

    legacy = AsyncMock(side_effect=AssertionError("native path cannot allocate a plain stage"))
    monkeypatch.setattr(nucc_module.native, "copy_nucc_native_batch", copy)
    monkeypatch.setattr(nucc_module, "_prepare_plain_stage", legacy)
    row_count = await nucc_module._stage_nucc_taxonomy_rows(
        {"context": {"nucc_native_stage": {"synthetic": "custody"}}},
        {},
        str(source),
        {"Code": "code", "Definition": "definition"},
        "model",
        test_mode=False,
        run_id="",
        source_file="taxonomy.csv",
    )
    assert row_count == 5001 and copied_batch_sizes == [5000, 1]
    legacy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_error", [None, RuntimeError("commit transport lost"), asyncio.CancelledError()])
@pytest.mark.parametrize("readback", ["committed", "absent", "unavailable"])
async def test_native_handoff_resolves_commit_before_terminal_mark(monkeypatch, nucc_module, commit_error, readback):
    handoff_by_field = {"row_count": 3}

    @asynccontextmanager
    async def transaction():
        yield "session"
        if commit_error is not None:
            raise commit_error

    context_by_field = {"import_date": "suffix", "context": {"run": 1, "nucc_native_stage": {"custody": "original"}}}
    monkeypatch.setattr(nucc_module.db, "transaction", transaction)
    monkeypatch.setattr(nucc_module, "make_class", lambda *_: "model")
    prepare = AsyncMock(return_value={"row_count": 3})
    record_handoff = AsyncMock(return_value=handoff_by_field)
    resolve = AsyncMock(return_value=handoff_by_field if readback == "committed" else None)
    if readback == "unavailable":
        resolve.side_effect = RuntimeError("readback unavailable")
    monkeypatch.setattr(nucc_module.native, "prepare_nucc_native_publication_stage", prepare)
    monkeypatch.setattr(nucc_module.native, "record_nucc_native_handoff", record_handoff)
    monkeypatch.setattr(nucc_module.native, "reconcile_nucc_native_handoff", resolve)
    if commit_error is not None and readback != "committed":
        with pytest.raises(RuntimeError if readback == "unavailable" else type(commit_error)):
            await nucc_module._handoff_nucc_generation(context_by_field)
        assert "control_run_handoff_committed" not in context_by_field["context"]
        assert context_by_field["context"].get("nucc_native_commit_unknown", False) is (readback == "unavailable")
    else:
        result_by_field = await nucc_module._handoff_nucc_generation(context_by_field)
        assert result_by_field == {"rows": 3, "nucc_handoff": handoff_by_field}
        assert context_by_field["context"]["control_run_handoff_committed"] is True
    assert record_handoff.await_args.args == ("session", context_by_field)
    assert prepare.await_args.kwargs["native_stage"] is context_by_field["context"]["nucc_native_stage"]
    assert resolve.await_count == int(commit_error is not None)


@pytest.mark.asyncio
async def test_failed_copy_leaves_custody_and_does_not_handoff(monkeypatch, nucc_module):
    custody_by_field = {"original": "stage"}
    context_by_field = {"context": {"nucc_native_stage": custody_by_field, "run": 0}}
    monkeypatch.setattr(
        nucc_module.native, "copy_nucc_native_batch", AsyncMock(side_effect=RuntimeError("COPY failed"))
    )
    record = AsyncMock()
    monkeypatch.setattr(nucc_module.native, "record_nucc_native_handoff", record)
    with pytest.raises(RuntimeError, match="COPY failed"):
        await nucc_module._write_nucc_batch(context_by_field, "model", [{"code": "1234567890"}])
    assert context_by_field["context"]["nucc_native_stage"] is custody_by_field
    assert await nucc_module.shutdown(context_by_field) is None
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_index_validation_never_records_or_classifies_a_commit(monkeypatch, nucc_module):
    context_by_field = {"import_date": "suffix", "context": {"run": 1, "nucc_native_stage": {"custody": "original"}}}
    monkeypatch.setattr(nucc_module, "make_class", lambda *_: "model")
    monkeypatch.setattr(
        nucc_module.native, "prepare_nucc_native_publication_stage", AsyncMock(side_effect=RuntimeError("invalid set"))
    )
    record_handoff = AsyncMock()
    resolve_commit = AsyncMock()
    monkeypatch.setattr(nucc_module.native, "record_nucc_native_handoff", record_handoff)
    monkeypatch.setattr(nucc_module.native, "reconcile_nucc_native_handoff", resolve_commit)
    with pytest.raises(RuntimeError, match="invalid set"):
        await nucc_module._handoff_nucc_generation(context_by_field)
    record_handoff.assert_not_awaited()
    resolve_commit.assert_not_awaited()
    assert "control_run_handoff_committed" not in context_by_field["context"]
    assert "nucc_native_commit_unknown" not in context_by_field["context"]


@pytest.mark.asyncio
@pytest.mark.parametrize("has_native_stage", [False, True])
async def test_missing_sources_cannot_strand_native_custody_as_success(monkeypatch, nucc_module, has_native_stage):
    context_by_field = {"context": {"run": 0}}
    if has_native_stage:
        context_by_field["context"]["nucc_native_stage"] = {"custody": "original"}
    monkeypatch.setattr(nucc_module, "_prepare_nucc_import", AsyncMock(return_value=("suffix", "", False)))
    monkeypatch.setattr(nucc_module, "_discover_nucc_source_files", AsyncMock(return_value=[]))
    with pytest.raises(nucc_module.native.ReferenceFamilyArchiveError, match="source files are unavailable"):
        await nucc_module._process_admitted_nucc_data(context_by_field, {}, publish_here=False)
    if has_native_stage:
        assert context_by_field["context"]["nucc_native_stage"] == {"custody": "original"}


@pytest.mark.asyncio
async def test_main_creates_a_genuine_managed_run(monkeypatch, nucc_module):
    actual_by_field = {"run_id": "synthetic", "status": "queued"}
    managed = AsyncMock(return_value=actual_by_field)
    raw_pool = AsyncMock(side_effect=AssertionError("normal CLI must use managed admission"))
    monkeypatch.setattr(nucc_module.protected_source, "create_managed_source_run", managed)
    monkeypatch.setattr(nucc_module, "create_pool", raw_pool)
    assert await nucc_module.main() is actual_by_field
    managed.assert_awaited_once_with(test_mode=False)
    raw_pool.assert_not_awaited()


@pytest.fixture
def plain_committed_times():
    context_by_field = {
        "control_run_id": "run-nucc",
        "context": {
            "_control_attempt_id": "run-nucc:" + "a" * 32,
            "_control_attempt_started_at": "2001-02-03T04:00:00+00:00",
        },
    }
    times_by_field = {
        "heartbeat_at": datetime.datetime(2001, 2, 3, 4, 5, 6, 123456),
        "finished_at": datetime.datetime(2001, 2, 3, 4, 5, 6, 123457),
    }
    return SimpleNamespace(context_by_field=context_by_field, times_by_field=times_by_field)


def _plain_timestamp_result(row):
    return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))


def _plain_timestamp_readback(plain_committed_times, nucc_module):
    context = plain_committed_times.context_by_field["context"]
    return {
        "status": "succeeded",
        "error": None,
        "progress": {
            "attempt_id": context["_control_attempt_id"],
            "attempt_started_at": context["_control_attempt_started_at"],
            "phase": "nucc published",
        },
        "metrics": {"rows": 7},
        **plain_committed_times.times_by_field,
    }


@pytest.mark.asyncio
async def test_plain_commit_stores_utc_independently_of_session_timezone(nucc_module, plain_committed_times):
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_plain_timestamp_result(plain_committed_times.times_by_field))
    )
    await nucc_module._commit_plain_run(session, plain_committed_times.context_by_field, nucc_module._plain_result(7))
    statement, parameters = session.execute.await_args.args
    assert "heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
    assert "finished_at=clock_timestamp() AT TIME ZONE 'UTC'" in str(statement)
    assert parameters["attempt"] == plain_committed_times.context_by_field["context"]["_control_attempt_id"]
    assert "metrics->>'nucc_handoff' IS NULL" in str(statement)


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["commit", "readback"])
@pytest.mark.parametrize("non_utc_value", [False, True])
async def test_plain_native_timestamps_project_exact_canonical_utc(
    monkeypatch, nucc_module, nucc_publication_driver, plain_committed_times, entrypoint, non_utc_value
):
    from process.control_lifecycle import _committed_control_timestamp

    expected_by_field = dict(plain_committed_times.times_by_field)
    if non_utc_value:
        non_utc_offset = datetime.timezone(datetime.timedelta(hours=2))
        plain_committed_times.times_by_field = {
            key: database_time.replace(tzinfo=datetime.UTC).astimezone(non_utc_offset)
            for key, database_time in expected_by_field.items()
        }
    session = nucc_publication_driver.session
    if entrypoint == "commit":
        session.execute = AsyncMock(return_value=_plain_timestamp_result(plain_committed_times.times_by_field))
        actual = await nucc_module._commit_plain_run(
            session, plain_committed_times.context_by_field, nucc_module._plain_result(7)
        )
    else:
        session.execute = AsyncMock(
            return_value=_plain_timestamp_result(_plain_timestamp_readback(plain_committed_times, nucc_module))
        )
        monkeypatch.setattr(nucc_module.native, "_relation_oid", AsyncMock(return_value=100))
        monkeypatch.setattr(nucc_module, "_require_plain_stage", AsyncMock())
        monkeypatch.setattr(nucc_module.native, "_validate_nucc_table_set", AsyncMock(return_value={"row_count": 7}))
        actual = await nucc_module._read_plain_commit(
            plain_committed_times.context_by_field, nucc_publication_driver.stage_by_field, nucc_module._plain_result(7)
        )
    for key, native_value in expected_by_field.items():
        assert actual[key] == native_value.replace(tzinfo=datetime.UTC).isoformat(timespec="microseconds")
        assert _committed_control_timestamp(actual[key]) == native_value.replace(tzinfo=datetime.UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["commit", "readback"])
@pytest.mark.parametrize("invalid", [None, "2001-02-03T04:05:06.123456+00:00", 17, datetime.date(2001, 2, 3)])
async def test_plain_commit_refuses_missing_or_non_native_database_time(
    monkeypatch, nucc_module, nucc_publication_driver, plain_committed_times, entrypoint, invalid
):
    plain_committed_times.times_by_field["heartbeat_at"] = invalid
    row = (
        plain_committed_times.times_by_field
        if entrypoint == "commit"
        else _plain_timestamp_readback(plain_committed_times, nucc_module)
    )
    session = nucc_publication_driver.session
    session.execute = AsyncMock(return_value=_plain_timestamp_result(row))
    monkeypatch.setattr(nucc_module.native, "_relation_oid", AsyncMock(return_value=100))
    monkeypatch.setattr(nucc_module, "_require_plain_stage", AsyncMock())
    monkeypatch.setattr(nucc_module.native, "_validate_nucc_table_set", AsyncMock(return_value={"row_count": 7}))
    with pytest.raises(nucc_module.native.ReferenceFamilyArchiveError):
        if entrypoint == "commit":
            await nucc_module._commit_plain_run(
                session, plain_committed_times.context_by_field, nucc_module._plain_result(7)
            )
        else:
            await nucc_module._read_plain_commit(
                plain_committed_times.context_by_field,
                nucc_publication_driver.stage_by_field,
                nucc_module._plain_result(7),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_commit_ack", [False, True])
async def test_plain_publication_keeps_exact_utc_times_after_commit_or_readback(
    monkeypatch, nucc_module, nucc_publication_driver, plain_committed_times, lost_commit_ack
):
    from process.control_lifecycle import _committed_control_timestamp

    session = nucc_publication_driver.session
    transaction_state = SimpleNamespace(count=0)

    @asynccontextmanager
    async def transaction():
        transaction_state.count += 1
        current = transaction_state.count
        yield session
        if lost_commit_ack and current == 1:
            raise RuntimeError("synthetic lost commit acknowledgement")

    async def execute(statement, parameters=None):
        if str(statement).startswith("UPDATE"):
            return _plain_timestamp_result(plain_committed_times.times_by_field)
        if str(statement).startswith("SELECT status"):
            return _plain_timestamp_result(_plain_timestamp_readback(plain_committed_times, nucc_module))

    context_by_field = plain_committed_times.context_by_field
    context_by_field["context"].update(
        nucc_plain_stage=nucc_publication_driver.stage_by_field,
        run=1,
        rows=7,
        start=datetime.datetime.now(datetime.UTC),
    )
    session.execute = AsyncMock(side_effect=execute)
    monkeypatch.setattr(nucc_module.db, "transaction", transaction)
    monkeypatch.setattr(nucc_module, "prepare_local_native_input_publication", AsyncMock())
    monkeypatch.setattr(nucc_module, "_plain_run", AsyncMock())
    monkeypatch.setattr(nucc_module, "_finish_plain_stage", AsyncMock(return_value=7))
    monkeypatch.setattr(nucc_module.native, "_relation_oid", AsyncMock(return_value=100))
    monkeypatch.setattr(nucc_module, "_require_plain_stage", AsyncMock())
    monkeypatch.setattr(nucc_module.native, "_validate_nucc_table_set", AsyncMock(return_value={"row_count": 7}))
    monkeypatch.setattr(nucc_module, "print_time_info", lambda _start: None)
    assert await nucc_module._publish_plain_generation(context_by_field) == nucc_module._plain_result(7)
    committed = context_by_field["context"]
    assert committed["control_run_terminal_committed"] is True
    assert "nucc_native_commit_unknown" not in committed
    assert transaction_state.count == (2 if lost_commit_ack else 1)
    for key, actual_db_value in plain_committed_times.times_by_field.items():
        assert _committed_control_timestamp(committed["_control_committed_" + key]) == actual_db_value.replace(
            tzinfo=datetime.UTC
        )


@pytest.mark.asyncio
async def test_main_enqueues_job_test_mode(monkeypatch, nucc_module):
    monkeypatch.setenv("HLTHPRT_TEST_DATABASE_SUFFIX", "_isolated")
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(nucc_module, "create_pool", AsyncMock(return_value=fake_pool))

    monkeypatch.setattr(nucc_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await nucc_module.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with(
        "process_data",
        {"test_mode": True},
        _queue_name="arq:NUCC",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["indexes", "validation", "census", "cancel"])
async def test_plain_preparation_failure_never_locks_the_incumbent(
    monkeypatch, nucc_module, nucc_publication_driver, failure
):
    async def indexes(*_args, **_options):
        if failure == "indexes":
            raise ValueError("index preparation failed")
        if failure == "cancel":
            raise asyncio.CancelledError()

    async def validate(*_args):
        if failure == "validation":
            raise ValueError("candidate validation failed")
        return {"row_count": 6}

    retention = AsyncMock()
    nucc_publication_driver.session.execute = AsyncMock()
    monkeypatch.setattr(nucc_module, "_require_plain_stage", AsyncMock())
    monkeypatch.setattr(nucc_module.native, "_create_model_indexes", indexes)
    monkeypatch.setattr(nucc_module.native, "_validate_nucc_table_set", validate)
    monkeypatch.setattr(nucc_module.native, "require_nucc_unretained_rotation", retention)
    error = (
        asyncio.CancelledError
        if failure == "cancel"
        else nucc_module.native.ReferenceFamilyArchiveError
        if failure == "census"
        else ValueError
    )
    with pytest.raises(error):
        await nucc_module._finish_plain_stage(
            nucc_publication_driver.session, nucc_publication_driver.stage_by_field, 7
        )
    retention.assert_not_awaited()
    statements = [str(call.args[0]) for call in nucc_publication_driver.session.execute.await_args_list]
    assert len(statements) == 1 and '"nucc_taxonomy_s_synthetic"' in statements[0]
