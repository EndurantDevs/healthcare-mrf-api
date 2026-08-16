# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import os
import datetime
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
async def test_startup_sets_context_and_creates_tables(monkeypatch, nucc_module):
    create_calls = []
    status_calls = []

    monkeypatch.setattr(nucc_module, "make_class", lambda cls, suffix: SimpleNamespace(
        __main_table__=cls.__tablename__,
        __tablename__=f"{cls.__tablename__}_{suffix}",
        __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
        __my_index_elements__=["code"],
    ))

    monkeypatch.setattr(nucc_module, "init_db", AsyncMock())
    monkeypatch.setattr(nucc_module.db, "create_table", AsyncMock(side_effect=lambda table, **kw: create_calls.append(table.name)))
    monkeypatch.setattr(nucc_module.db, "status", AsyncMock(side_effect=lambda stmt: status_calls.append(stmt)))
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())

    startup_context_map: dict[str, object] = {}
    await nucc_module.startup(startup_context_map)

    assert startup_context_map["context"]["run"] == 0
    assert (
        datetime.datetime.utcnow() - startup_context_map["context"]["start"]
    ).total_seconds() < 2
    assert create_calls
    assert any("DROP TABLE" in stmt for stmt in status_calls)


@pytest.mark.asyncio
async def test_shutdown_rotates_tables(monkeypatch, nucc_module):
    monkeypatch.setattr(nucc_module, "make_class", lambda cls, suffix: SimpleNamespace(
        __main_table__=cls.__tablename__,
        __tablename__=f"{cls.__tablename__}_{suffix}",
        __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
    ))

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
            "start": datetime.datetime.utcnow() - datetime.timedelta(seconds=5)
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
