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
    monkeypatch.setattr(nucc_module.native, "bind_nucc_native_attempt", AsyncMock(return_value=None))
    monkeypatch.setattr(nucc_module.native, "is_nucc_native_handoff_required", AsyncMock(return_value=False))
    monkeypatch.setattr(nucc_module.native, "require_nucc_unretained_rotation", AsyncMock())


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

    async def fake_push(objects, cls, rewrite=False):
        push_calls.append((cls.__tablename__, rewrite, objects))

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
    monkeypatch.setattr(nucc_module, "push_objects", fake_push)
    monkeypatch.setattr(nucc_module, "make_class", fake_make_class)
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())

    import_context_map = {"import_date": "20260101"}
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

    async def capture_rows(taxonomy_row_batch, _model):
        taxonomy_row_list.extend(taxonomy_row_batch)

    monkeypatch.setattr(nucc_module, "push_objects", capture_rows)
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
async def test_startup_sets_context_and_creates_tables(monkeypatch, nucc_module):
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
    assert create_calls
    assert any("DROP TABLE" in stmt for stmt in status_calls)


@pytest.mark.asyncio
async def test_shutdown_rotates_tables(monkeypatch, nucc_module):
    monkeypatch.setattr(
        nucc_module,
        "make_class",
        lambda cls, suffix: SimpleNamespace(
            __main_table__=cls.__tablename__,
            __tablename__=f"{cls.__tablename__}_{suffix}",
            __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
        ),
    )

    status_calls = []
    monkeypatch.setattr(nucc_module.db, "status", AsyncMock(side_effect=lambda stmt: status_calls.append(stmt)))
    monkeypatch.setattr(nucc_module.db, "scalar", AsyncMock(return_value=7))
    monkeypatch.setattr(nucc_module.db, "execute_ddl", AsyncMock())
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(nucc_module, "mark_control_run", AsyncMock())

    @asynccontextmanager
    async def fake_tx():
        yield SimpleNamespace()

    monkeypatch.setattr(nucc_module.db, "transaction", lambda: fake_tx())

    captured_time_by_name = {}
    monkeypatch.setattr(
        nucc_module,
        "print_time_info",
        lambda start: captured_time_by_name.setdefault("start", start),
    )

    shutdown_context_map = {
        "import_date": "20260102",
        "context": {
            "run": 1,
            "control_run_id": "run-nucc",
            "start": datetime.datetime.utcnow() - datetime.timedelta(seconds=5),
        },
    }

    terminal_result = await nucc_module.shutdown(shutdown_context_map)

    assert status_calls
    assert captured_time_by_name["start"]
    assert terminal_result["rows"] == 7
    assert terminal_result["terminal_progress"]["phase"] == "nucc published"
    assert nucc_module.mark_control_run.await_args.kwargs["metrics"] == {"rows": 7}
    status_count = len(status_calls)
    shutdown_context_map["context"]["run"] = 0
    await nucc_module.shutdown(shutdown_context_map)
    assert len(status_calls) == status_count


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

    legacy = AsyncMock(side_effect=AssertionError("native path cannot use INSERT"))
    monkeypatch.setattr(nucc_module.native, "copy_nucc_native_batch", copy)
    monkeypatch.setattr(nucc_module, "push_objects", legacy)
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
    if has_native_stage:
        with pytest.raises(nucc_module.native.ReferenceFamilyArchiveError, match="source files are unavailable"):
            await nucc_module.process_nucc_data(context_by_field)
        assert context_by_field["context"]["nucc_native_stage"] == {"custody": "original"}
    else:
        assert await nucc_module.process_nucc_data(context_by_field) is None


@pytest.mark.asyncio
async def test_main_enqueues_job(monkeypatch, nucc_module):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(nucc_module, "create_pool", AsyncMock(return_value=fake_pool))

    monkeypatch.setattr(nucc_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await nucc_module.main()

    fake_pool.enqueue_job.assert_awaited_once_with(
        "process_data",
        {"test_mode": False},
        _queue_name="arq:NUCC",
    )


@pytest.mark.asyncio
async def test_main_enqueues_job_test_mode(monkeypatch, nucc_module):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(nucc_module, "create_pool", AsyncMock(return_value=fake_pool))

    monkeypatch.setattr(nucc_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await nucc_module.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with(
        "process_data",
        {"test_mode": True},
        _queue_name="arq:NUCC",
    )
