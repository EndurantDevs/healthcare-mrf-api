# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical epoch hashing preserves the original task and failure boundary."""

import asyncio
import hashlib
import json
import struct
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_fhir_source_epoch as epoch
from tests.network_fhir_source_epoch_content_postgres_support import content_database as content_database

_COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + bytes(8)
_COPY_TRAILER = struct.pack("!h", -1)


def _copy_field(payload):
    return struct.pack("!hi", 1, len(payload)) + payload


@pytest.mark.parametrize(
    "damage",
    ["header", "extra_data", "missing_trailer", "short_length", "negative_length", "oversize", "short_field"],
)
def test_resource_copy_requires_complete_bounded_binary_frames(tmp_path, damage):
    path = tmp_path / "resources.copy"
    field = _copy_field(b'{"retained":true}')
    good = _COPY_HEADER + field + _COPY_TRAILER
    frame_by_damage = {
        "header": b"!" + good[1:],
        "extra_data": good + b"!",
        "missing_trailer": good[:-2],
        "short_length": _COPY_HEADER + field[:5],
        "negative_length": _COPY_HEADER + struct.pack("!hi", 1, -1),
        "oversize": _COPY_HEADER + struct.pack("!hi", 1, epoch.MAX_ROW_BYTES + 1),
        "short_field": _COPY_HEADER + field[:-1],
    }
    path.write_bytes(frame_by_damage[damage])
    with pytest.raises(epoch.FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
        list(epoch._json_copy_rows(path))
    path.write_bytes(good)
    assert list(epoch._json_copy_rows(path)) == [{"retained": True}]


def _json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@pytest.fixture
def resource_copy_rows():
    """Build two independently normalized Location rows with their original FHIR JSON."""
    fhir = import_module("process.provider_directory_fhir")
    metadata = {
        "resource_hash_contract": fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-09-01",
    }
    rows = []
    for resource_id, name in [("location-1", "North Office"), ("location-2", "Centre de Santé")]:
        raw_by_field = {"resourceType": "Location", "id": resource_id, "name": name}
        row = fhir.materialize_provider_directory_dataset_fhir_resource(
            source_id="cms-npd", dataset_id="synthetic", resource=raw_by_field, run_id="root", **metadata
        )
        rows.append(
            dict(
                row,
                raw_payload_json=raw_by_field,
                raw_payload_sha256=hashlib.sha256(_json_bytes(raw_by_field)).hexdigest(),
            )
        )
    identities = [_json_bytes([row["resource_type"], row["resource_id"], row["payload_hash"]]) for row in rows]
    pin = SimpleNamespace(dataset_id="synthetic", dataset_sha256=hashlib.sha256(b"\n".join(identities)).hexdigest())
    return rows, pin, metadata


@pytest.mark.parametrize("damage", ["duplicate", "raw_identity", "raw_hash", "raw_projection", "missing_row"])
def test_resource_copy_rejects_inconsistent_raw_or_incomplete_normalized_rows(tmp_path, resource_copy_rows, damage):
    rows, pin, metadata = resource_copy_rows
    first, second = rows
    misidentified_raw_by_field = dict(second["raw_payload_json"], id="different-location")
    changed_raw_by_field = dict(second["raw_payload_json"], name="Different Office")
    different_projection_by_field = dict(
        second,
        raw_payload_json=changed_raw_by_field,
        raw_payload_sha256=hashlib.sha256(_json_bytes(changed_raw_by_field)).hexdigest(),
    )
    rows_by_damage = {
        "duplicate": (first, first, second),
        "raw_identity": (first, dict(second, raw_payload_json=misidentified_raw_by_field)),
        "raw_hash": (first, dict(second, raw_payload_sha256=first["raw_payload_sha256"])),
        "raw_projection": (first, different_projection_by_field),
        "missing_row": (first,),
    }
    path = tmp_path / "resources.copy"
    path.write_bytes(
        _COPY_HEADER + b"".join(_copy_field(_json_bytes(row)) for row in rows_by_damage[damage]) + _COPY_TRAILER
    )
    with pytest.raises(epoch.FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
        epoch._verify_resource_copy(path, pin, metadata, "root")
    path.write_bytes(_COPY_HEADER + b"".join(_copy_field(_json_bytes(row)) for row in rows) + _COPY_TRAILER)
    assert epoch._verify_resource_copy(path, pin, metadata, "root") == 2


@pytest.mark.asyncio
async def test_native_resource_copy_rejects_incomplete_export_and_accepts_full_replay(
    content_database, tmp_path, resource_copy_rows
):
    rows, pin, metadata = resource_copy_rows
    connection = content_database.connection
    relation = f'"{content_database.schema}"."resource_copy_input"'
    await connection.execute(f"CREATE TABLE {relation} (position integer PRIMARY KEY, payload text NOT NULL)")
    await connection.executemany(
        f"INSERT INTO {relation} (position, payload) VALUES ($1, $2)",
        [(index, _json_bytes(row).decode()) for index, row in enumerate(rows)],
    )
    query = f"SELECT payload FROM {relation} WHERE position < $1 ORDER BY position"
    path = tmp_path / "resources.copy"
    await epoch._copy_json_rows(connection, query, (1,), path)
    with pytest.raises(epoch.FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
        epoch._verify_resource_copy(path, pin, metadata, "root")
    await epoch._copy_json_rows(connection, query, (2,), path)
    assert list(epoch._json_copy_rows(path)) == rows
    assert epoch._verify_resource_copy(path, pin, metadata, "root") == 2


@pytest.mark.asyncio
async def test_native_epoch_copy_splits_wide_batches_without_replaying_a_row(content_database):
    connection, schema = content_database.connection, content_database.schema
    target_schema, table = schema + "_copy", epoch._TABLES[1]
    source_relation = f'"{schema}"."{table}"'
    candidate_relation = f'"{target_schema}"."{table}"'
    source_pin = SimpleNamespace(schema_name=schema, dataset_id="selected")
    expected_rows = [("selected", "Location", key, "wide-value" * 64) for key in ("a", "b", "c")]

    async with connection.transaction():
        await connection.execute(f"DROP TABLE {source_relation}")
        await connection.execute(
            f"CREATE TABLE {source_relation} (dataset_id text, resource_type text, resource_id text, payload text,"
            " PRIMARY KEY (dataset_id, resource_type, resource_id))"
        )
        await connection.executemany(
            f"INSERT INTO {source_relation} VALUES ($1,$2,$3,$4)",
            [*expected_rows, ("other", "Location", "d", "unselected")],
        )
        await connection.execute(f'CREATE SCHEMA "{target_schema}"')
        await connection.execute(f"CREATE TABLE {candidate_relation} (LIKE {source_relation} INCLUDING ALL)")
        row_bytes = await connection.fetchval(
            f"SELECT max(octet_length(record_send(row_data))) FROM {source_relation} row_data WHERE dataset_id=$1",
            source_pin.dataset_id,
        )
        admission = AsyncMock(copy_batch_rows=3, copy_batch_bytes=row_bytes + 20)
        with pytest.raises(epoch.FHIRSourceEpochError, match="^fhir_source_epoch_unavailable$"):
            await epoch._copy_epoch_rows(connection, source_pin, target_schema, table, None, admission)
        admission.assert_not_awaited()
        assert await connection.fetchval(f"SELECT count(*) FROM {candidate_relation}") == 0

        admission.copy_batch_bytes = row_bytes + 21
        await epoch._copy_epoch_rows(connection, source_pin, target_schema, table, None, admission)
        copied_rows = await connection.fetch(f"SELECT * FROM {candidate_relation} ORDER BY resource_id")
        assert [tuple(copied_row) for copied_row in copied_rows] == expected_rows
        assert [call.args for call in admission.await_args_list] == [
            (phase, target_schema, table) for _ in expected_rows for phase in ("before_copy_batch", "after_copy_batch")
        ]
        await connection.execute(f'DROP SCHEMA "{target_schema}" CASCADE')


@pytest.mark.asyncio
async def test_canonical_content_keeps_source_capabilities_on_original_task(monkeypatch):
    """Already canonical producers keep the existing body without a savepoint."""
    owner = asyncio.current_task()
    source = object()

    async def chunks(connection, schema, source_input):
        assert asyncio.current_task() is owner and source_input is source
        assert schema == "synthetic"
        return "a" * 64

    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=[value for _, value in epoch._CONTENT_SETTINGS]),
        transaction=Mock(side_effect=AssertionError("canonical producer needs no savepoint")),
    )
    monkeypatch.setattr(epoch, "_content_chunks", chunks)
    assert await epoch._content(connection, "synthetic", source) == "a" * 64
    connection.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("body_fails", (False, True))
async def test_failed_content_cleanup_preserves_original_exception_and_discards_connection(monkeypatch, body_fails):
    """Retire rollback failures after either body outcome, preserving a failed body's exception."""
    body_error = RuntimeError("original content failure")
    rollback_error = ConnectionError("rollback failed")
    transaction = SimpleNamespace(start=AsyncMock(), rollback=AsyncMock(side_effect=rollback_error), commit=AsyncMock())
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=["alternate"] * 5),
        fetch=AsyncMock(),
        transaction=lambda: transaction,
        terminate=Mock(),
    )
    monkeypatch.setattr(
        epoch, "_content_chunks", AsyncMock(side_effect=body_error if body_fails else None, return_value="a" * 64)
    )
    expected_error = body_error if body_fails else rollback_error
    with pytest.raises(type(expected_error)) as observed:
        await epoch._content(connection, "synthetic")
    assert observed.value is expected_error
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()
    connection.terminate.assert_called_once()
