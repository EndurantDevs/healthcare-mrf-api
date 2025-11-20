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

    ctx = {"import_date": "20260101"}
    await nucc_module.process_data(ctx)

    assert push_calls
    table, rewrite, rows = push_calls[0]
    assert table == "nucc_taxonomy_20260101"
    assert rewrite is False
    assert rows[0]["code"] == "1234"


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

    ctx: dict[str, object] = {}
    await nucc_module.startup(ctx)

    assert ctx["context"]["run"] == 0
    assert (datetime.datetime.utcnow() - ctx["context"]["start"]).total_seconds() < 2
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
    monkeypatch.setattr(nucc_module.db, "execute_ddl", AsyncMock())
    monkeypatch.setattr(nucc_module, "ensure_database", AsyncMock())

    @asynccontextmanager
    async def fake_tx():
        yield SimpleNamespace()

    monkeypatch.setattr(nucc_module.db, "transaction", lambda: fake_tx())

    captured = {}
    monkeypatch.setattr(nucc_module, "print_time_info", lambda start: captured.setdefault("start", start))

    ctx = {
        "import_date": "20260102",
        "context": {
            "start": datetime.datetime.utcnow() - datetime.timedelta(seconds=5)
        },
    }

    await nucc_module.shutdown(ctx)

    assert status_calls
    assert captured["start"]


@pytest.mark.asyncio
async def test_main_enqueues_job(monkeypatch, nucc_module):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(nucc_module, "create_pool", AsyncMock(return_value=fake_pool))

    monkeypatch.setattr(nucc_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await nucc_module.main()

    fake_pool.enqueue_job.assert_awaited_once_with("process_data", {"test_mode": False})


@pytest.mark.asyncio
async def test_main_enqueues_job_test_mode(monkeypatch, nucc_module):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(nucc_module, "create_pool", AsyncMock(return_value=fake_pool))

    monkeypatch.setattr(nucc_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await nucc_module.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with("process_data", {"test_mode": True})
