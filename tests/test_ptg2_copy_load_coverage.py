from __future__ import annotations

import datetime
import io
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from process.ptg_parts import copy_load
from tests.ptg2_copy_load_coverage_support import (
    CopyConnection,
    CopyDatabase,
    install_capture,
    model,
)


def test_copy_value_helpers_coerce_json_defaults_and_postgres_nuls():
    """Normalize nested NULs and JSON values before PostgreSQL COPY."""

    nested_values_by_name = {
        "nul\x00key": ["a\x00", ("b\x00",)],
        "scalar": 7,
    }
    assert copy_load._strip_postgres_nuls(nested_values_by_name) == {
        "nulkey": ["a", ("b",)],
        "scalar": 7,
    }
    assert copy_load._copy_record_values(("x\x00", nested_values_by_name))[0] == "x"
    assert copy_load._json_default(datetime.date(2026, 7, 24)) == "2026-07-24"
    assert copy_load._json_default(
        datetime.datetime(2026, 7, 24, 10, 30)
    ) == "2026-07-24T10:30:00"
    assert copy_load._json_default(Decimal("10.50")) == "10.5"
    assert copy_load._json_default(SimpleNamespace(name="fallback")).startswith(
        "namespace"
    )

    copy_model = model("id", "payload", json_columns=("payload",))
    json_columns = copy_load._ptg2_json_columns(copy_model)
    record = copy_load._ptg2_copy_record(
        {"id": "a\x00", "payload": {"value": Decimal("4.20")}},
        ["id", "payload", "missing"],
        json_columns,
    )
    assert record == ("a", '{"value": "4.2"}', None)


def test_conflict_target_resolution_obeys_model_metadata():
    """Prefer declared unique indexes, then legacy indexes, then primary keys."""

    indexed_model = model(
        "id",
        initial_indexes=[{}, {"index_elements": ("tenant_id", "id")}],
    )
    legacy_model = model("id", conflict_targets=("legacy_id",))
    fallthrough_model = model(
        "id",
        conflict_targets=("legacy_id",),
        initial_indexes=[{}],
    )
    primary_model = model("record_id", primary_key=("record_id",))

    assert copy_load._ptg2_conflict_targets(indexed_model) == [
        "tenant_id",
        "id",
    ]
    assert copy_load._ptg2_conflict_targets(legacy_model) == ["legacy_id"]
    assert copy_load._ptg2_conflict_targets(fallthrough_model) == ["legacy_id"]
    assert copy_load._ptg2_conflict_targets(primary_model) == ["record_id"]
    assert copy_load._primary_key_column_names(primary_model) == ["record_id"]


@pytest.mark.asyncio
async def test_copy_upsert_short_circuits_and_falls_back_without_conflict(
    monkeypatch,
):
    """Avoid COPY for empty shapes and use row fallback without a conflict key."""

    await copy_load._copy_upsert_ptg2_objects([], model("id"))
    await copy_load._copy_upsert_ptg2_objects(
        [{"unknown": "value"}],
        model("id"),
    )
    push_objects = AsyncMock()
    monkeypatch.setattr(copy_load, "push_objects", push_objects)
    no_key_model = model("value", primary_key=())

    rows = [{"value": "one"}, {"value": "two"}]
    await copy_load._copy_upsert_ptg2_objects(rows, no_key_model)

    push_objects.assert_awaited_once_with(
        rows,
        no_key_model,
        rewrite=True,
        use_copy=False,
    )


@pytest.mark.asyncio
async def test_copy_upsert_dedupes_and_emits_update_sql(monkeypatch):
    """COPY only the last conflict-key row and update its non-key columns."""

    capture = install_capture(monkeypatch, copy_load)
    monkeypatch.setattr(copy_load.os, "getpid", lambda: 41)
    monkeypatch.setattr(copy_load.time, "time_ns", lambda: 99)
    rows = [
        {"id": "same", "payload": {"old": True}, "note": "old"},
        {"id": "same", "payload": {"date": datetime.date(2026, 7, 24)}, "note": "n\x00ew"},
    ]
    copy_model = model("id", "payload", "note", json_columns=("payload",))

    await copy_load._copy_upsert_ptg2_objects(rows, copy_model)

    record_call = capture.driver.record_calls[0]
    assert record_call["table_name"] == "ptg2_stage_sample_41_99"
    assert record_call["records"] == [
        ("same", '{"date": "2026-07-24"}', "new")
    ]
    assert capture.guard_calls[0]["attempt_rows"] == [rows[-1]]
    emitted_sql = "\n".join(capture.connection.status_calls)
    assert 'ON CONFLICT ("id") DO UPDATE SET' in emitted_sql
    assert '"payload" = EXCLUDED."payload"' in emitted_sql
    assert '"note" = EXCLUDED."note"' in emitted_sql


@pytest.mark.asyncio
async def test_copy_upsert_emits_do_nothing_for_key_only_rows(monkeypatch):
    """Use DO NOTHING when a row has no mutable columns."""

    capture = install_capture(monkeypatch, copy_load)

    await copy_load._copy_upsert_ptg2_objects(
        [{"id": "one"}],
        model("id"),
    )

    emitted_sql = "\n".join(capture.connection.status_calls)
    assert 'ON CONFLICT ("id") DO NOTHING' in emitted_sql
    assert capture.driver.record_calls[0]["records"] == [("one",)]


@pytest.mark.asyncio
async def test_copy_insert_uses_raw_driver_and_guards_attempt(monkeypatch):
    """Support direct raw drivers while preserving guard and COPY data."""

    capture = install_capture(monkeypatch, copy_load, wrapped_driver=False)
    rows = [{"id": "a\x00", "payload": {"label": "v\x00"}}]

    await copy_load._copy_insert_ptg2_objects(
        rows,
        model("id", "payload", json_columns=("payload",), schema=None),
    )
    await copy_load._copy_insert_ptg2_objects([], model("id"))

    assert capture.driver.record_calls == [
        {
            "table_name": "sample",
            "schema_name": "resolved_schema",
            "columns": ["id", "payload"],
            "records": [("a", '{"label": "v"}')],
        }
    ]
    assert capture.guard_calls[0]["table_name"] == "sample"
    assert capture.guard_calls[0]["attempt_rows"] == rows


@pytest.mark.asyncio
async def test_copy_ignore_delegates_without_conflict_metadata(monkeypatch):
    """Use direct insert behavior when the model has no ignore key."""

    insert_copy = AsyncMock()
    monkeypatch.setattr(copy_load, "_copy_insert_ptg2_objects", insert_copy)
    rows = [{"id": "one"}]

    await copy_load._copy_ignore_ptg2_objects([], model("id"))
    await copy_load._copy_ignore_ptg2_objects(
        rows,
        model("id", conflict_targets=()),
    )

    insert_copy.assert_awaited_once_with(rows, ANY)


@pytest.mark.asyncio
async def test_copy_ignore_emits_conflict_safe_stage_insert(monkeypatch):
    """Stage COPY records and ignore duplicates at the declared key."""

    capture = install_capture(monkeypatch, copy_load)
    rows = [{"id": "one", "payload": {"text": "a\x00"}}]
    copy_model = model(
        "id",
        "payload",
        json_columns=("payload",),
        conflict_targets=("id",),
    )

    await copy_load._copy_ignore_ptg2_objects(rows, copy_model)

    assert capture.driver.record_calls[0]["records"] == [
        ("one", '{"text": "a"}')
    ]
    assert capture.guard_calls[0]["attempt_rows"] == rows
    emitted_sql = "\n".join(capture.connection.status_calls)
    assert 'ON CONFLICT ("id") DO NOTHING' in emitted_sql


@pytest.mark.asyncio
async def test_copy_price_stage_rows_preserves_schema_and_cleans_nuls(monkeypatch):
    """Build price-stage records with the fenced snapshot identity."""

    capture = install_capture(monkeypatch, copy_load)
    rows = [{"price_set_hash": "hash\x00", "created_at": "now\x00"}]

    await copy_load._copy_stage_price_set_rows([], "snapshot-empty")
    await copy_load._copy_stage_price_set_rows(rows, "snapshot-1")

    record_call = capture.driver.record_calls[0]
    assert record_call["table_name"] == "ptg2_price_set_stage"
    assert record_call["records"] == [("snapshot-1", "hash", "now")]
    assert capture.guard_calls[0]["attempt_rows"] == [
        {"snapshot_id": "snapshot-1"}
    ]


@pytest.mark.asyncio
async def test_copy_serving_stage_rows_coerces_defaults_and_json(monkeypatch):
    """Serialize optional serving payloads and remove embedded NUL bytes."""

    capture = install_capture(monkeypatch, copy_load)
    serving_rate_rows = [
        {
            "serving_rate_id": "rate-1",
            "plan_name": "plan\x00",
            "prices": {"amount": Decimal("12.40")},
            "confidence": {"label": "g\x00ood"},
            "network_names": ["n\x00et"],
        },
        {
            "serving_rate_id": "rate-2",
            "provider_set_hashes": ["provider"],
            "source_trace": {"date": datetime.date(2026, 7, 24)},
        },
    ]

    await copy_load._copy_stage_serving_rate_rows([], "empty")
    await copy_load._copy_stage_serving_rate_rows(serving_rate_rows, "snapshot-2")

    priced_record, traced_record = capture.driver.record_calls[0]["records"]
    assert priced_record[1] == traced_record[1] == "snapshot-2"
    assert priced_record[3] == "plan"
    assert priced_record[18] == []
    assert priced_record[23] == ["net"]
    assert priced_record[25] == '{"amount": "12.4"}'
    assert priced_record[26] is None
    assert priced_record[27] == '{"label": "g\\u0000ood"}'
    assert traced_record[18] == ["provider"]
    assert traced_record[23] == []
    assert traced_record[25] is None
    assert traced_record[26] == '{"date": "2026-07-24"}'
    assert traced_record[27] is None


@pytest.mark.asyncio
async def test_copy_compact_rows_applies_network_defaults(monkeypatch):
    """COPY compact rows with snapshot fencing and normalized arrays."""

    capture = install_capture(monkeypatch, copy_load)
    rows = [
        {"serving_rate_id": "rate-1", "network_names": None},
        {"serving_rate_id": "rate-2", "network_names": ["n\x00et"]},
    ]

    await copy_load._copy_compact_serving_rate_rows([], "empty")
    await copy_load._copy_compact_serving_rate_rows(rows, "snapshot-3")

    records = capture.driver.record_calls[0]["records"]
    assert records[0][1] == records[1][1] == "snapshot-3"
    assert records[0][11] == []
    assert records[1][11] == ["net"]
    assert capture.guard_calls[0]["table_name"] == "ptg2_serving_rate_compact"


@pytest.mark.asyncio
async def test_compact_source_requires_snapshot_and_driver(monkeypatch):
    """Reject an unfenced durable target and a driver without stream COPY."""

    with pytest.raises(ValueError, match="requires snapshot_id"):
        await copy_load._copy_compact_serving_rate_source(io.BytesIO(b"row"))

    source = io.BytesIO(b"row")
    connection = CopyConnection(object())
    monkeypatch.setattr(copy_load, "db", CopyDatabase(connection))
    monkeypatch.setattr(copy_load, "resolve_ptg2_schema", lambda: "mrf")
    with pytest.raises(NotImplementedError, match="does not expose copy_to_table"):
        await copy_load._copy_compact_serving_rate_source(
            source,
            target_table="temporary_compact",
        )
    source.close()


@pytest.mark.asyncio
async def test_compact_source_guards_copies_and_closes(monkeypatch):
    """Fence snapshot COPY and close its stream after successful consumption."""

    capture = install_capture(monkeypatch, copy_load)
    source = io.BytesIO(b"compact\trow\n")

    await copy_load._copy_compact_serving_rate_source(
        source,
        snapshot_id="snapshot-4",
    )

    assert source.closed
    assert capture.driver.source_calls[0]["payload"] == b"compact\trow\n"
    assert capture.driver.source_calls[0]["format"] == "text"
    assert capture.guard_calls[0]["attempt_rows"] == [
        {"snapshot_id": "snapshot-4"}
    ]


@pytest.mark.asyncio
async def test_compact_custom_target_skips_guard_and_handles_no_close(monkeypatch):
    """Allow disposable compact targets without a snapshot fence."""

    capture = install_capture(monkeypatch, copy_load)

    class ReadOnlySource:
        def read(self):
            return b"custom\trow\n"

    await copy_load._copy_compact_serving_rate_source(
        ReadOnlySource(),
        target_table="temporary_compact",
    )

    assert capture.guard_calls == []
    assert capture.driver.source_calls[0]["table_name"] == "temporary_compact"


@pytest.mark.asyncio
async def test_compact_file_skips_absent_empty_and_forwards_nonempty(
    tmp_path,
    monkeypatch,
):
    """Only open non-empty compact files and forward target metadata."""

    empty_path = tmp_path / "empty.copy"
    empty_path.write_bytes(b"")
    await copy_load._copy_compact_serving_rate_file(tmp_path / "missing.copy")
    await copy_load._copy_compact_serving_rate_file(empty_path)
    source_copy = AsyncMock()
    monkeypatch.setattr(copy_load, "_copy_compact_serving_rate_source", source_copy)
    full_path = tmp_path / "full.copy"
    full_path.write_bytes(b"payload")

    await copy_load._copy_compact_serving_rate_file(
        full_path,
        snapshot_id="snapshot-5",
        target_table="compact_stage",
    )

    source = source_copy.await_args.args[0]
    assert source.read() == b"payload"
    assert source_copy.await_args.kwargs == {
        "snapshot_id": "snapshot-5",
        "target_table": "compact_stage",
    }
    source.close()


@pytest.mark.asyncio
async def test_dictionary_copy_rejects_missing_empty_and_unsupported(tmp_path):
    """Ignore absent shards but reject a non-empty unknown dictionary kind."""

    empty_path = tmp_path / "empty.copy"
    empty_path.write_bytes(b"")
    await copy_load._copy_ptg2_dictionary_file(
        tmp_path / "missing.copy",
        "procedure",
    )
    await copy_load._copy_ptg2_dictionary_file(empty_path, "procedure")
    unknown_path = tmp_path / "unknown.copy"
    unknown_path.write_bytes(b"unknown")

    with pytest.raises(ValueError, match="Unsupported PTG2 dictionary"):
        await copy_load._copy_ptg2_dictionary_file(unknown_path, "unknown")


@pytest.mark.asyncio
async def test_dictionary_target_uses_direct_copy_without_dedupe(
    tmp_path,
    monkeypatch,
):
    """COPY a stage-compatible dictionary directly into its target."""

    capture = install_capture(monkeypatch, copy_load)
    monkeypatch.setattr(
        copy_load,
        "_uses_ptg2_stage_copy_dedupe",
        lambda _kind: False,
    )
    copy_path = tmp_path / "procedures.copy"
    copy_path.write_bytes(b"hash\tcode\n")

    await copy_load._copy_ptg2_dictionary_file(
        copy_path,
        "procedure",
        target_table="procedure_stage",
    )

    source_call = capture.driver.source_calls[0]
    assert source_call["table_name"] == "procedure_stage"
    assert source_call["payload"] == b"hash\tcode\n"
    assert source_call["schema_name"] == "resolved_schema"
    assert capture.connection.status_calls == []


@pytest.mark.asyncio
async def test_dictionary_target_dedupes_through_temporary_table(
    tmp_path,
    monkeypatch,
):
    """Deduplicate dictionary shards before inserting into a stage target."""

    capture = install_capture(monkeypatch, copy_load)
    monkeypatch.setattr(
        copy_load,
        "_uses_ptg2_stage_copy_dedupe",
        lambda _kind: True,
    )
    monkeypatch.setattr(
        copy_load,
        "_ptg2_dictionary_select_columns",
        lambda _kind, columns: ", ".join(f"normalize({column})" for column in columns),
    )
    copy_path = tmp_path / "price-sets.copy"
    copy_path.write_bytes(b"set\tcodes\n")

    await copy_load._copy_ptg2_dictionary_file(
        copy_path,
        "price_code_set",
        target_table="price_code_set_stage",
    )

    assert capture.driver.source_calls[0]["table_name"].startswith(
        "ptg2_stage_ptg2_price_code_set_"
    )
    emitted_sql = "\n".join(capture.connection.status_calls)
    assert 'LIKE "resolved_schema"."price_code_set_stage"' in emitted_sql
    assert 'ON CONFLICT ("code_set_hash") DO NOTHING' in emitted_sql
    assert "normalize(code_set_hash)" in emitted_sql


@pytest.mark.asyncio
async def test_dictionary_durable_target_stages_then_inserts(tmp_path, monkeypatch):
    """Use conflict-safe staging for the durable dictionary table."""

    capture = install_capture(monkeypatch, copy_load, wrapped_driver=False)
    copy_path = tmp_path / "members.copy"
    copy_path.write_bytes(b"group\t123\n")

    await copy_load._copy_ptg2_dictionary_file(
        copy_path,
        "provider_group_member",
    )

    source_call = capture.driver.source_calls[0]
    assert source_call["table_name"].startswith(
        "ptg2_stage_ptg2_provider_group_member_"
    )
    assert "schema_name" not in source_call
    emitted_sql = "\n".join(capture.connection.status_calls)
    assert 'INSERT INTO "resolved_schema"."ptg2_provider_group_member"' in emitted_sql
    assert 'ON CONFLICT ("provider_group_hash", "npi") DO NOTHING' in emitted_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("target_table", ("procedure_stage", None))
async def test_dictionary_copy_requires_stream_driver(tmp_path, monkeypatch, target_table):
    """Reject direct and durable dictionary COPY without stream support."""
    copy_path = tmp_path / "procedure.copy"
    copy_path.write_bytes(b"procedure")
    monkeypatch.setattr(copy_load, "db", CopyDatabase(CopyConnection(object())))
    monkeypatch.setattr(copy_load, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(
        copy_load,
        "_uses_ptg2_stage_copy_dedupe",
        lambda _kind: False,
    )
    with pytest.raises(NotImplementedError, match="does not expose copy_to_table"):
        await copy_load._copy_ptg2_dictionary_file(
            copy_path,
            "procedure",
            target_table=target_table,
        )
