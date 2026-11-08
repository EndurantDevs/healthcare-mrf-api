# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated parser buffers remain bounded across every legacy CSV member."""

import asyncio
import csv
import io
import zipfile
from collections import Counter
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import npi_isolated_batches as batches
from process.control_cancel import ImportCancelledError
from tests.test_process_npi_import import _build_nppes_zip, _install_test_mode_dependencies
from tests.test_process_npi_unit import _build_minimal_row
from tests.test_process_npi_unit import npi_module as npi_module


def _expanded_row(index):
    row = _build_minimal_row(str(1000000000 + index))
    for number in range(1, 16):
        row[f"Healthcare Provider Taxonomy Code_{number}"] = f"T{number}"
        row[f"Provider License Number_{number}"] = f"L{number}"
        row[f"Healthcare Provider Taxonomy Group_{number}"] = f"G{number}"
    for number in range(1, 51):
        row[f"Other Provider Identifier_{number}"] = f"ID{number}"
    return row


def test_raw_batches_enforce_rows_resident_and_conversion_bytes():
    assert list(batches.raw_batches(iter(()))) == []
    rows = [{"name": "x"} for _ in range(batches.RAW_ROWS * 2 + 1)]
    chunks = list(batches.raw_batches(rows))
    assert [len(chunk) for chunk in chunks] == [batches.RAW_ROWS, batches.RAW_ROWS, 1]
    assert [row for chunk in chunks for row in chunk] == rows
    byte_rows = [{"name": "é" * 200_000} for _ in range(12)]
    byte_chunks = list(batches.raw_batches(byte_rows))
    assert len(byte_chunks) > 1
    assert all(sum(batches.memory_bytes(row) + 16 for row in chunk) <= batches.RAW_BYTES for chunk in byte_chunks)
    with pytest.raises(batches.NpiBatchError, match="parser byte"):
        list(batches.raw_batches([{"name": "é" * (2 * 1024**2)}]))


@pytest.mark.asyncio
async def test_maximum_primary_expansion_is_split_without_losing_any_family(monkeypatch, npi_module):
    monkeypatch.setattr(npi_module, "source_enabled", lambda _: True)
    monkeypatch.setattr(npi_module, "_prepare_npi_address_rows", lambda rows, **_: list(rows))
    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: object())
    observed = Counter()
    lengths = []

    class Writer:
        async def add(self, payload):
            for kind, rows in payload.items():
                observed[kind] += len(rows)

        finish_window = AsyncMock()

    writer = Writer()

    @asynccontextmanager
    async def window(_ctx):
        yield writer

    original = npi_module._process_npi_chunk

    async def process(ctx, task):
        lengths.append(len(task["row_list"]))
        assert sum(batches.memory_bytes(row) + 16 for row in task["row_list"]) <= batches.RAW_BYTES
        await original(ctx, task)

    monkeypatch.setattr(npi_module, "process_npi_chunk", npi_module._bound_isolated_parser(process))
    monkeypatch.setattr(npi_module, "_isolated_write_window", window)
    monkeypatch.setattr(batches, "current", lambda: writer)
    count = 257
    await npi_module.process_npi_chunk(
        {"redis": None},
        {
            "row_list": (_expanded_row(index) for index in range(count)),
            "npi_csv_map": {"NPI": "npi"},
            "npi_csv_map_reverse": {"npi": "NPI"},
            "direct": True,
        },
    )
    assert max(lengths) <= batches.RAW_ROWS and sum(lengths) == count
    assert observed == Counter(
        npi_obj_list=count,
        npi_taxonomy_list=15 * count,
        npi_other_id_list=50 * count,
        npi_taxonomy_group_list=15 * count,
        npi_address_list=2 * count,
    )
    assert sum(observed.values()) == 83 * count
    writer.finish_window.assert_awaited_once()


@pytest.mark.asyncio
async def test_isolated_parser_cannot_send_its_payload_to_an_unbound_queue(monkeypatch, npi_module):
    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: object())
    redis = SimpleNamespace(enqueue_job=AsyncMock())
    with pytest.raises(batches.NpiBatchError, match="direct bounded"):
        await npi_module.process_npi_chunk({"redis": redis}, {"row_list": []})
    redis.enqueue_job.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("nested_context", [True, False])
@pytest.mark.parametrize("cancel_after", [0, 1])
async def test_original_control_run_stops_parser_before_next_raw_batch(
    monkeypatch, npi_module, nested_context, cancel_after
):
    """A chunk without its own run ID still checks the original control signal."""
    flags = {"cancel:original-run": b"1"} if cancel_after == 0 else {}
    reads, parsed = [], []

    async def get(key):
        reads.append(key)
        return flags.get(key)

    writer = SimpleNamespace(finish_window=AsyncMock())

    @asynccontextmanager
    async def window(_ctx):
        yield writer

    async def parse(_ctx, task):
        assert task["run_id"] == "original-run"
        parsed.append(task["row_list"])
        flags["cancel:original-run"] = b"1"

    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: object())
    monkeypatch.setattr(npi_module, "_isolated_write_window", window)
    context_by_name = {
        "redis": SimpleNamespace(get=get),
        "control_run_id": "foreign-run" if nested_context else " original-run ",
        "context": {"control_run_id": " original-run "} if nested_context else {},
    }
    with pytest.raises(ImportCancelledError, match="original-run"):
        await npi_module._bound_isolated_parser(parse)(
            context_by_name, {"direct": True, "row_list": [{"NPI": "1000000001"}] * (batches.RAW_ROWS + 1)}
        )
    assert len(parsed) == cancel_after
    assert reads and set(reads) == {"cancel:original-run"}
    writer.finish_window.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("nested_context", [True, False])
async def test_attempt_without_task_run_id_checks_original_cancel_before_parsing(
    monkeypatch, npi_module, nested_context
):
    """The outer task used by every member flush retains the control identity."""
    redis = SimpleNamespace(get=AsyncMock(return_value=b"1"))
    context_by_name = {
        "redis": redis,
        "control_run_id": "foreign-run" if nested_context else " original-run ",
        "context": {"control_run_id": " original-run "} if nested_context else {},
    }

    @asynccontextmanager
    async def window(_ctx):
        yield None

    monkeypatch.setattr(npi_module, "_isolated_write_window", window)
    monkeypatch.setattr(npi_module.isolated_npi, "verify_npi_isolated_execution", AsyncMock(return_value=object()))
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", AsyncMock())
    prerequisite = AsyncMock()
    monkeypatch.setattr(npi_module, "ensure_database", prerequisite)
    with pytest.raises(ImportCancelledError, match="original-run"):
        await npi_module.execute_npi_import_attempt(context_by_name, {})
    redis.get.assert_awaited_once_with("cancel:original-run")
    prerequisite.assert_not_awaited()


def _repeat_members(path, count):
    with zipfile.ZipFile(path) as source:
        members = [(name, source.read(name).decode()) for name in source.namelist()]
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in members:
            reader = csv.DictReader(io.StringIO(content))
            row = next(reader)
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
            writer.writeheader()
            for index in range(count):
                writer.writerow({**row, "NPI": str(1000000000 + index)})
            archive.writestr(name, output.getvalue())


class _ParserWriter:
    def __init__(self, failure):
        self.failure = failure
        self.totals, self.owners = Counter(), set()
        self.writes = 0
        self.finish_window = AsyncMock()

    async def add(self, payload):
        self.writes += 1
        self.owners.add(asyncio.current_task())
        if self.writes == 2 and self.failure is not None:
            if self.failure == "cancel":
                raise asyncio.CancelledError
            raise RuntimeError("synthetic write failed")
        for kind, rows in payload.items():
            assert len(rows) <= 2 * batches.RAW_ROWS
            assert sum(batches.memory_bytes(row) for row in rows) < batches.RAW_BYTES
            self.totals[kind] += len(rows)
        await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "error", "cancel"])
async def test_execute_bounds_primary_other_name_and_practice_location_buffers(
    monkeypatch, tmp_path, npi_module, failure
):
    """Drain all ZIP members in one owner task and retain terminal failure behavior."""
    archive = _build_nppes_zip(tmp_path)
    count = 300
    _repeat_members(archive, count)
    _install_test_mode_dependencies(monkeypatch, npi_module, archive, [])
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.test/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "index.html")
    monkeypatch.setenv("HLTHPRT_NPI_MAX_PENDING_SAVE_TASKS", "99")
    scope = SimpleNamespace(output_schema="npi_example", verify=AsyncMock())
    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: scope)
    monkeypatch.setattr(npi_module.isolated_npi, "verify_npi_isolated_execution", AsyncMock(return_value=scope))
    monkeypatch.setattr(npi_module, "source_enabled", lambda _: True)
    monkeypatch.setattr(npi_module, "_prepare_npi_address_rows", lambda rows, **_: list(rows))
    writer = _ParserWriter(failure)

    @asynccontextmanager
    async def window(_ctx):
        yield writer

    async def save(_ctx, payload):
        await writer.add(payload)
        await writer.finish_window()

    monkeypatch.setattr(npi_module, "_isolated_write_window", window)
    monkeypatch.setattr(batches, "current", lambda: writer)
    monkeypatch.setattr(npi_module, "save_npi_data", save)
    context_by_name = {"context": {}, "redis": None, "import_date": "20260331"}
    if failure is None:
        await npi_module.process_data(context_by_name, {"test_mode": True})
        assert writer.totals == Counter(
            npi_obj_list=count,
            npi_taxonomy_list=count,
            npi_other_id_list=2 * count,
            npi_taxonomy_group_list=count,
            npi_address_list=3 * count,
        )
        assert context_by_name["context"]["run"] == 1
    else:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await npi_module.process_data(context_by_name, {"test_mode": True})
        assert context_by_name["context"]["run"] == 0
    assert writer.owners == {asyncio.current_task()}
    assert not context_by_name["context"].get(npi_module._NPI_PENDING_SAVE_TASKS_KEY)


@pytest.mark.asyncio
async def test_raw_flushes_preserve_original_logical_window_boundary(monkeypatch, tmp_path, npi_module):
    """A bounded raw flush must not reset the original 300001-row window counter."""
    archive = _build_nppes_zip(tmp_path)
    _install_test_mode_dependencies(monkeypatch, npi_module, archive, [])
    monkeypatch.setattr(npi_module, "source_enabled", lambda _: True)
    monkeypatch.setattr(npi_module, "TEST_NPI_ROWS", 300002)
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.test/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "index.html")
    scope = SimpleNamespace(output_schema="npi_example", verify=AsyncMock())
    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: scope)
    monkeypatch.setattr(npi_module.isolated_npi, "verify_npi_isolated_execution", AsyncMock(return_value=scope))
    windows, lengths = [], []
    state_by_name = {"rows": 0}

    async def add(payload):
        state_by_name["rows"] += len(payload["npi_obj_list"])

    async def finish_window():
        if state_by_name["rows"]:
            windows.append(state_by_name["rows"])
            state_by_name["rows"] = 0

    writer = SimpleNamespace(add=add, finish_window=finish_window)

    @asynccontextmanager
    async def window(_ctx):
        yield writer

    @asynccontextmanager
    async def source(_layout, kind, _path):
        yield kind

    async def reader(kind, **_options):
        for _ in range(300002 if kind == "primary" else 0):
            yield {"NPI": "1000000001"}

    async def normalize(_ctx, task):
        lengths.append(len(task["row_list"]))
        await writer.add({"npi_obj_list": task["row_list"]})

    monkeypatch.setattr(npi_module, "_isolated_write_window", window)
    monkeypatch.setattr(batches, "current", lambda: writer)
    monkeypatch.setattr(npi_module, "_open_nppes_source", source)
    monkeypatch.setattr(npi_module, "AsyncDictReader", reader)
    monkeypatch.setattr(npi_module, "_process_npi_chunk", normalize)
    monkeypatch.setattr(npi_module, "process_npi_chunk", npi_module._bound_isolated_parser(normalize))
    await npi_module.process_data({"context": {}, "redis": None, "import_date": "20260331"}, {"test_mode": True})
    assert max(lengths) <= batches.RAW_ROWS and sum(lengths) == 300002
    assert windows == [300001, 1]


@pytest.mark.asyncio
async def test_normalized_row_byte_refusal_happens_before_copy(monkeypatch):
    writer = batches.NativeWriteWindow(None, None, AsyncMock())
    monkeypatch.setattr(writer, "_open", AsyncMock())
    monkeypatch.setattr(writer, "_family", AsyncMock(return_value=(None, ("npi", "name"))))
    copy = AsyncMock()
    monkeypatch.setattr(writer, "_copy", copy)
    with pytest.raises(batches.NpiBatchError, match="COPY byte"):
        await writer.add({"npi_obj_list": [{"npi": 1, "name": "x" * batches.COPY_BYTES}]})
    copy.assert_not_awaited()


class _Driver:
    def __init__(self, borrowed=False):
        self.borrowed = borrowed
        self.execute = AsyncMock()
        self.native_transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
        self.copies = []

    def is_in_transaction(self):
        return self.borrowed

    def transaction(self):
        return self.native_transaction

    async def fetchval(self, query, *args):
        return 42 if "to_regclass" in query else 16_384

    async def copy_records_to_table(self, _table, *, records, **options):
        values = [record async for record in records] if hasattr(records, "__aiter__") else list(records)
        self.copies.append(values)


def _database(driver, *, borrowed=False):
    closed_events = []
    sql_connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
    )
    session = SimpleNamespace(connection=AsyncMock(return_value=sql_connection))
    proxy = SimpleNamespace(raw_connection=None if borrowed else driver, _connection=session, scalar=AsyncMock())

    @asynccontextmanager
    async def acquire():
        try:
            yield proxy
        finally:
            closed_events.append(True)

    return SimpleNamespace(acquire=acquire, proxy=proxy), closed_events


def _models(_kind):
    return SimpleNamespace(
        __tablename__="npi",
        __table__=SimpleNamespace(
            schema="npi_example", columns=[SimpleNamespace(name="npi"), SimpleNamespace(name="name")]
        ),
        __my_index_elements__=("npi",),
    ), True


@pytest.mark.asyncio
async def test_owned_transaction_precedes_temporary_tables_and_bounds_copy_buffers():
    driver = _Driver()
    database, closed = _database(driver)
    rows = [{"npi": index, "name": "é" * 4096} for index in range(257)]
    async with batches.bind(database, _models, AsyncMock()) as writer:
        await writer.add({"npi_obj_list": rows})
        driver.native_transaction.start.assert_awaited_once()
        assert len(driver.copies) > 1
        assert all(
            len(values) <= batches.COPY_ROWS and batches.memory_bytes(values) <= batches.COPY_BYTES
            for values in driver.copies
        )
    driver.native_transaction.commit.assert_awaited_once()
    assert closed == [True] and batches.current() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_borrowed_driver_uses_same_session_and_preserves_outer_transaction(failure):
    driver = _Driver(borrowed=True)
    database, closed = _database(driver, borrowed=True)
    writer = batches.NativeWriteWindow(database, _models, AsyncMock())
    await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}]})
    assert writer.connection is driver
    if failure:
        await writer.close(RuntimeError("synthetic refusal"))
    else:
        await writer.finish_window()
    commands = [call.args[0] for call in driver.execute.await_args_list]
    assert commands[0].startswith("SAVEPOINT")
    assert any(command.startswith("RELEASE SAVEPOINT") for command in commands)
    assert any(command.startswith("ROLLBACK TO SAVEPOINT") for command in commands) is failure
    assert any(command.startswith("DROP TABLE pg_temp.") for command in commands) is not failure
    driver.native_transaction.start.assert_not_awaited()
    driver.native_transaction.commit.assert_not_awaited()
    driver.native_transaction.rollback.assert_not_awaited()
    assert closed == [True]


@pytest.mark.asyncio
async def test_disk_admission_failure_rolls_back_before_copy(monkeypatch):
    driver = _Driver()
    database, closed = _database(driver)
    monkeypatch.setattr(batches, "WINDOW_BYTES", 1)
    with pytest.raises(batches.NpiBatchError, match="temporary disk"):
        async with batches.bind(database, _models, AsyncMock()) as writer:
            await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}]})
    assert not driver.copies
    driver.native_transaction.rollback.assert_awaited_once()
    assert closed == [True] and batches.current() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["copy_unavailable", "inactive_borrowed"])
async def test_missing_native_capability_releases_connection_without_output(failure):
    driver = _Driver()
    if failure == "copy_unavailable":
        driver.copy_records_to_table = None
    database, closed = _database(driver, borrowed=failure == "inactive_borrowed")
    with pytest.raises(batches.NpiBatchError, match="requires native COPY|borrowed transaction is not active"):
        async with batches.bind(database, _models, AsyncMock()) as writer:
            await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}]})
    driver.execute.assert_not_awaited()
    driver.native_transaction.start.assert_not_awaited()
    assert not driver.copies and closed == [True] and batches.current() is None
    if failure == "inactive_borrowed":
        database.proxy.scalar.assert_awaited_once_with("SELECT 1")


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", [None, 0, True])
async def test_missing_or_untyped_temporary_oid_rolls_back_before_copy(oid):
    driver = _Driver()
    driver.fetchval = AsyncMock(return_value=oid)
    database, closed = _database(driver)
    with pytest.raises(batches.NpiBatchError, match="temporary relation identity differs"):
        async with batches.bind(database, _models, AsyncMock()) as writer:
            await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}]})
    driver.native_transaction.rollback.assert_awaited_once()
    assert not driver.copies and closed == [True] and batches.current() is None


@pytest.mark.asyncio
async def test_unexpected_normalized_column_rolls_back_before_native_copy():
    driver = _Driver()
    database, closed = _database(driver)
    with pytest.raises(batches.NpiBatchError, match="normalized columns differ"):
        async with batches.bind(database, _models, AsyncMock()) as writer:
            await writer.add({"npi_obj_list": [{"npi": 1, "unexpected": "value"}]})
    driver.native_transaction.rollback.assert_awaited_once()
    assert not driver.copies and closed == [True] and batches.current() is None


@pytest.mark.asyncio
async def test_invalid_model_identifier_refuses_before_temporary_ddl():
    driver = _Driver()
    database, closed = _database(driver)
    model, rewrite = _models(None)
    model.__table__.schema = 'unexpected"schema'
    with pytest.raises(batches.NpiBatchError, match="native identifier differs"):
        async with batches.bind(database, lambda _kind: (model, rewrite), AsyncMock()) as writer:
            await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}]})
    driver.execute.assert_not_awaited()
    driver.native_transaction.rollback.assert_awaited_once()
    assert not driver.copies and closed == [True] and batches.current() is None


@pytest.mark.asyncio
async def test_weekly_cleanup_cannot_overlap_an_unpublished_logical_window():
    driver = _Driver()
    database, closed = _database(driver)
    read_source = AsyncMock()

    async def source_rows():
        await read_source()
        yield {"NPI": "1000000001"}

    with pytest.raises(batches.NpiBatchError, match="overlaps a logical write window"):
        async with batches.bind(database, _models, AsyncMock()) as writer:
            await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}]})
            await writer.delete_selected(source_rows(), [(_models(None)[0], ())])
    read_source.assert_not_awaited()
    assert not any('DELETE FROM "npi_example"' in call.args[0] for call in driver.execute.await_args_list)
    driver.native_transaction.rollback.assert_awaited_once()
    assert closed == [True] and batches.current() is None


@pytest.mark.asyncio
async def test_oversized_weekly_row_rolls_back_key_tables_before_output_delete(monkeypatch):
    driver = _Driver()
    database, closed = _database(driver)
    monkeypatch.setattr(batches, "RAW_BYTES", 64)

    async def source_rows():
        yield {"NPI": "1000000001", "extra": "x" * 64}

    with pytest.raises(batches.NpiBatchError, match="weekly CSV row exceeds"):
        async with batches.bind(database, _models, AsyncMock()) as writer:
            await writer.delete_selected(source_rows(), [(_models(None)[0], ())])
    assert not driver.copies
    assert not any(command.args[0].startswith("DELETE") for command in driver.execute.await_args_list)
    driver.native_transaction.rollback.assert_awaited_once()
    assert closed == [True] and batches.current() is None


@pytest.mark.asyncio
async def test_empty_families_preserve_cancel_check_without_opening_unneeded_output():
    check = AsyncMock()
    writer = batches.NativeWriteWindow(None, None, check)
    await writer.add({"npi_obj_list": []})
    check.assert_awaited_once()
    assert writer.connection is None
    driver = _Driver()
    database, closed = _database(driver)
    async with batches.bind(database, _models, check) as writer:
        await writer.add({"npi_obj_list": [{"npi": 1, "name": "example"}], "npi_other_id_list": []})
        assert set(writer.families) == {"npi_obj_list"}
    assert closed == [True] and len(driver.copies) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_failure", [False, True])
async def test_normalized_save_uses_the_bound_native_window_without_legacy_fallback(
    monkeypatch, npi_module, copy_failure
):
    driver = _Driver()
    if copy_failure:
        driver.copy_records_to_table = AsyncMock(side_effect=RuntimeError("native COPY refused"))
    database, closed = _database(driver)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: object())
    legacy = AsyncMock(side_effect=AssertionError("legacy output writer executed"))
    monkeypatch.setattr(npi_module, "push_objects", legacy)
    payload_by_kind = {"npi_obj_list": [{"npi": 1, "name": "example"}]}
    context_by_name = {"import_date": "20260331", "context": {}, "redis": None}
    if copy_failure:
        with pytest.raises(RuntimeError, match="native COPY refused"):
            async with batches.bind(database, _models, AsyncMock()):
                await npi_module.save_npi_data(context_by_name, payload_by_kind)
        driver.native_transaction.rollback.assert_awaited_once()
        driver.native_transaction.commit.assert_not_awaited()
    else:
        async with batches.bind(database, _models, AsyncMock()):
            await npi_module.save_npi_data(context_by_name, payload_by_kind)
        driver.native_transaction.commit.assert_awaited_once()
        assert driver.copies == [[(1, "example", 1)]]
    legacy.assert_not_awaited()
    assert closed == [True] and batches.current() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [batches.RAW_ROWS, batches.RAW_ROWS + 1])
async def test_weekly_key_buffers_close_at_the_exact_row_boundary(count):
    driver = _Driver()
    database, closed = _database(driver)

    async def source_rows():
        yield {"NPI": ""}
        for index in range(count):
            yield {"NPI": str(1000000000 + index)}

    async with batches.bind(database, _models, AsyncMock()) as writer:
        await writer.delete_selected(source_rows(), [(_models(None)[0], ())])
    assert [len(values) for values in driver.copies] == (
        [batches.RAW_ROWS] if count == batches.RAW_ROWS else [batches.RAW_ROWS, 1]
    )
    copied_keys = [key[0] for values in driver.copies for key in values]
    assert sorted(copied_keys) == list(range(1000000000, 1000000000 + count))
    driver.native_transaction.commit.assert_awaited_once()
    driver.native_transaction.rollback.assert_not_awaited()
    assert closed == [True] and batches.current() is None


@pytest.mark.asyncio
async def test_repeated_cancel_drains_the_original_native_cleanup():
    driver = _Driver()
    database, closed = _database(driver)
    writer = batches.NativeWriteWindow(database, _models, AsyncMock())
    await writer._open()
    started, release = asyncio.Event(), asyncio.Event()

    async def rollback():
        started.set()
        await release.wait()

    driver.native_transaction.rollback.side_effect = rollback
    task = asyncio.create_task(writer.close(asyncio.CancelledError()))
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    driver.native_transaction.rollback.assert_awaited_once()
    assert closed == [True] and writer.connection is None


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["copy", "merge"])
@pytest.mark.parametrize("nested_context", [True, False])
async def test_original_control_run_signal_stops_isolated_copy_or_merge(monkeypatch, npi_module, phase, nested_context):
    """Use the real cancel lookup after admission and before native output mutation."""
    driver = _Driver()
    database, closed = _database(driver)
    flags, reads = {}, []

    async def get(key):
        reads.append(key)
        return flags.get(key)

    context_by_name = {
        "import_date": "20260331",
        "redis": SimpleNamespace(get=get),
        "control_run_id": "foreign-run" if nested_context else " original-run ",
        "context": {"control_run_id": " original-run "} if nested_context else {},
    }
    monkeypatch.setattr(npi_module, "db", database)
    monkeypatch.setattr(npi_module, "_npi_staging_model", lambda *_args: _models(None)[0])
    monkeypatch.setattr(npi_module.isolated_npi, "current_npi_isolated_execution", lambda: object())
    with pytest.raises(ImportCancelledError, match="original-run"):
        async with npi_module._isolated_write_window(context_by_name) as writer:
            original_family = writer._family

            async def family(kind):
                result = await original_family(kind)
                if phase == "copy":
                    flags["cancel:original-run"] = b"1"
                return result

            monkeypatch.setattr(writer, "_family", family)
            await writer.add({"npi_obj_list": [{"npi": 1000000001, "name": "example"}]})
            flags["cancel:original-run"] = b"1"
            await writer.finish_window()
    assert reads and set(reads) == {"cancel:original-run"}
    assert len(driver.copies) == (0 if phase == "copy" else 1)
    assert not any(
        call.args[0].startswith(('DELETE FROM "npi_example".', 'INSERT INTO "npi_example".'))
        for call in driver.execute.await_args_list
    )
    driver.native_transaction.rollback.assert_awaited_once()
    assert closed == [True] and batches.current() is None
