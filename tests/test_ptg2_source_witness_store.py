# See LICENSE.

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_source_witness_store as witness_store


def _use_test_payload_bounds(monkeypatch) -> None:
    monkeypatch.setattr(witness_store, "PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES", 8)
    monkeypatch.setattr(witness_store, "PTG2_V3_SOURCE_WITNESS_MAX_PART_COUNT", 4)
    monkeypatch.setattr(witness_store, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", 32)


def test_segmented_source_witness_payload_round_trips_with_authenticated_parts(
    monkeypatch,
):
    witness_payload = b"01234567abcdefghIJKLMNOP"
    _use_test_payload_bounds(monkeypatch)

    payload_parts = witness_store.split_source_witness_payload(witness_payload)
    payload_part_rows = [
        {"part_number": 0, "payload": payload_parts[0]},
        *[
            {
                "part_number": part_number,
                "payload": payload_part,
                "part_sha256": hashlib.sha256(payload_part).digest(),
            }
            for part_number, payload_part in enumerate(payload_parts[1:], start=1)
        ],
    ]

    assert payload_parts == (b"01234567", b"abcdefgh", b"IJKLMNOP")
    assert witness_store.assemble_source_witness_payload(
        payload_part_rows,
        expected_payload_sha256=hashlib.sha256(witness_payload).hexdigest(),
    ) == witness_payload


def test_source_witness_store_rejects_invalid_payload_shapes(monkeypatch):
    _use_test_payload_bounds(monkeypatch)
    with pytest.raises(RuntimeError, match="payload size"):
        witness_store.split_source_witness_payload(b"")
    monkeypatch.setattr(witness_store, "PTG2_V3_SOURCE_WITNESS_MAX_PART_COUNT", 1)
    with pytest.raises(RuntimeError, match="part count"):
        witness_store.split_source_witness_payload(b"123456789")
    with pytest.raises(RuntimeError, match="part count"):
        witness_store.assemble_source_witness_payload(
            [], expected_payload_sha256=b"x" * 32
        )


def test_source_witness_store_rejects_invalid_part_content(monkeypatch):
    _use_test_payload_bounds(monkeypatch)
    invalid_part_rows = [
        {"part_number": 0, "payload": b"123456789"},
        {
            "part_number": 1,
            "payload": b"tail",
            "part_sha256": hashlib.sha256(b"tail").digest(),
        },
    ]
    with pytest.raises(RuntimeError, match="part exceeds"):
        witness_store.assemble_source_witness_payload(
            invalid_part_rows,
            expected_payload_sha256=b"x" * 32,
        )
    with pytest.raises(RuntimeError, match="part is empty"):
        witness_store.assemble_source_witness_payload(
            [{"part_number": 0, "payload": b""}],
            expected_payload_sha256=b"x" * 32,
        )


def test_source_witness_store_rejects_aggregate_and_digest(monkeypatch):
    _use_test_payload_bounds(monkeypatch)
    monkeypatch.setattr(witness_store, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", 7)
    with pytest.raises(RuntimeError, match="payload exceeds"):
        witness_store.assemble_source_witness_payload(
            [{"part_number": 0, "payload": b"12345678"}],
            expected_payload_sha256=hashlib.sha256(b"12345678").digest(),
        )
    monkeypatch.setattr(witness_store, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", 32)
    with pytest.raises(RuntimeError, match="payload digest"):
        witness_store.assemble_source_witness_payload(
            [{"part_number": 0, "payload": b"payload"}],
            expected_payload_sha256=b"short",
        )


@pytest.mark.parametrize("mutation", ["gap", "digest", "short_middle"])
def test_segmented_source_witness_payload_rejects_incomplete_or_corrupt_parts(
    monkeypatch,
    mutation,
):
    witness_payload = b"01234567abcdefghIJKLMNOP"
    _use_test_payload_bounds(monkeypatch)
    payload_part_rows = [
        {"part_number": 0, "payload": b"01234567"},
        {
            "part_number": 1,
            "payload": b"abcdefgh",
            "part_sha256": hashlib.sha256(b"abcdefgh").digest(),
        },
        {
            "part_number": 2,
            "payload": b"IJKLMNOP",
            "part_sha256": hashlib.sha256(b"IJKLMNOP").digest(),
        },
    ]
    if mutation == "gap":
        payload_part_rows[1]["part_number"] = 2
    elif mutation == "digest":
        payload_part_rows[2]["part_sha256"] = b"x" * 32
    else:
        payload_part_rows[1]["payload"] = b"short"
        payload_part_rows[1]["part_sha256"] = hashlib.sha256(b"short").digest()

    with pytest.raises(RuntimeError):
        witness_store.assemble_source_witness_payload(
            payload_part_rows,
            expected_payload_sha256=hashlib.sha256(witness_payload).digest(),
        )


@pytest.mark.asyncio
async def test_source_witness_row_replacement_persists_all_parts_in_one_session(
    monkeypatch,
):
    class RecordingSession:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, params):
            self.calls.append((str(statement), params))

    witness_payload = b"01234567abcdefghIJKLMNOP"
    witness_metadata_by_field = {
        "source_set_digest": "11" * 32,
        "sample_digest": "22" * 32,
        "queryable_occurrence_population_count": 1,
        "provider_population_count": 0,
        "occurrence_witness_count": 1,
        "provider_witness_count": 0,
        "payload_sha256": hashlib.sha256(witness_payload).hexdigest(),
    }
    _use_test_payload_bounds(monkeypatch)
    session = RecordingSession()

    await witness_store._replace_source_witness_row(
        schema='"mrf"',
        snapshot_key=7,
        witness_payload=witness_payload,
        witness_metadata=witness_metadata_by_field,
        session=session,
    )

    assert len(session.calls) == 4
    assert "DELETE FROM" in session.calls[0][0]
    assert session.calls[1][1]["payload"] == b"01234567"
    assert [call[1]["part_number"] for call in session.calls[2:]] == [1, 2]
    assert [call[1]["payload"] for call in session.calls[2:]] == [
        b"abcdefgh",
        b"IJKLMNOP",
    ]
    assert all(
        call[1]["part_sha256"] == hashlib.sha256(call[1]["payload"]).digest()
        for call in session.calls[2:]
    )


@pytest.mark.asyncio
async def test_source_witness_load_rejects_missing_and_inconsistent_rows(monkeypatch):
    monkeypatch.setattr(witness_store.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="no persisted source witness"):
        await witness_store.load_shared_source_witness(
            schema_name="mrf",
            snapshot_key=7,
            expected_raw_source_sha256=[],
            expected_metadata={},
        )

    database_row_by_field = {
        "contract": "contract",
        "selection_method": "selection",
        "source_set_digest": b"s" * 32,
        "sample_digest": b"a" * 32,
        "queryable_occurrence_population_count": 1,
        "provider_population_count": 0,
        "occurrence_witness_count": 1,
        "provider_witness_count": 0,
        "payload_sha256": b"p" * 32,
        "payload": b"payload",
    }
    monkeypatch.setattr(
        witness_store.db,
        "first",
        AsyncMock(return_value=database_row_by_field),
    )
    monkeypatch.setattr(witness_store.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(witness_store, "assemble_source_witness_payload", lambda *_args, **_kwargs: b"payload")
    monkeypatch.setattr(
        witness_store,
        "decode_persisted_source_witness",
        lambda *_args, **_kwargs: SimpleNamespace(metadata={}),
    )
    with pytest.raises(RuntimeError, match="row is inconsistent"):
        await witness_store.load_shared_source_witness(
            schema_name="mrf",
            snapshot_key=7,
            expected_raw_source_sha256=[],
            expected_metadata={},
        )
