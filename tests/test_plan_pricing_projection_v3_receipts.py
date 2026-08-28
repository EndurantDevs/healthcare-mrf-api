# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import plan_pricing_projection_v3 as projection
from api.plan_pricing_aggregate_pack import AggregateZipRecord


PROJECTION_ID = "a" * 64


class _Session:
    def __init__(
        self, *, execute_result=None, stream_rows=()
    ) -> None:
        self.calls: list[tuple[str, object]] = []
        self.execute_result = execute_result
        self.stream_rows = tuple(stream_rows)
        self.stream_calls: list[tuple[object, object]] = []

    async def execute(self, statement, parameters=None):
        statement_text = str(statement)
        self.calls.append((statement_text, parameters))
        return self.execute_result or SimpleNamespace()

    async def stream(self, statement, parameters=None):
        self.stream_calls.append((statement, parameters))
        stream_rows = self.stream_rows

        class _MappingStream:
            def mappings(self):
                return self

            async def __aiter__(self):
                for stream_row in stream_rows:
                    yield stream_row

        return _MappingStream()


def _aggregate(zip5: str, provider_count: int = 3) -> AggregateZipRecord:
    return AggregateZipRecord(
        zip5,
        provider_count,
        4,
        Decimal("1"),
        Decimal("2.5"),
        Decimal("4"),
    )


def _stored_pack_row(parameter_by_field):
    encoded_pack = parameter_by_field["payload"]
    payload_sha256 = hashlib.sha256(encoded_pack).digest()
    return {
        **parameter_by_field,
        "payload_sha256": payload_sha256,
        "computed_sha256": payload_sha256,
    }


async def _stored_rows_and_counts(aggregate_rows):
    build_session = _Session()
    state = projection._BuildState(hashlib.sha256())
    await projection._store_aggregate_packs(
        build_session,
        PROJECTION_ID,
        ("CPT", "27447"),
        tuple(aggregate_rows),
        state,
    )
    stored_rows = [
        _stored_pack_row(parameter_by_field)
        for parameter_by_field in build_session.calls[0][1]
    ]
    return stored_rows, projection.ProjectionV3Counts(
        0,
        0,
        0,
        state.aggregate_entry_count,
        state.aggregate_pack_count,
        state.aggregate_raw_byte_count,
        state.aggregate_stored_byte_count,
        0,
    )


@pytest.mark.asyncio
async def test_stored_pack_readback_streams_and_validates_builder_totals() -> None:
    stored_rows, counts = await _stored_rows_and_counts(
        (_aggregate("10001"), _aggregate("11001", 5))
    )
    readback_session = _Session(stream_rows=stored_rows)

    await projection.validate_stored_aggregate_packs(
        readback_session, PROJECTION_ID, counts
    )

    statement, parameters = readback_session.stream_calls[0]
    assert statement.get_execution_options()["yield_per"] == 1
    assert "pg_catalog.sha256(payload) AS computed_sha256" in str(statement)
    assert parameters == {"projection_id": PROJECTION_ID}


@pytest.mark.asyncio
async def test_stored_pack_readback_rejects_corrupt_receipts() -> None:
    stored_rows, counts = await _stored_rows_and_counts((_aggregate("10001"),))
    valid_row = stored_rows[0]
    corrupt_payload = bytes([0]) + valid_row["payload"][1:]
    corrupt_sha256 = hashlib.sha256(corrupt_payload).digest()
    corrupt_rows = (
        {**valid_row, "stored_byte_count": valid_row["stored_byte_count"] + 1},
        {**valid_row, "raw_byte_count": valid_row["raw_byte_count"] + 1},
        {**valid_row, "payload_sha256": b"x" * 32},
        {**valid_row, "entry_count": valid_row["entry_count"] + 1},
        {**valid_row, "logical_digest": "f" * 64},
        {
            **valid_row,
            "payload": corrupt_payload,
            "payload_sha256": corrupt_sha256,
            "computed_sha256": corrupt_sha256,
        },
    )
    for corrupt_row_by_field in corrupt_rows:
        with pytest.raises(ValueError):
            await projection.validate_stored_aggregate_packs(
                _Session(stream_rows=(corrupt_row_by_field,)),
                PROJECTION_ID,
                counts,
            )


@pytest.mark.asyncio
async def test_prewarm_rows_extend_digest_in_final_rank_order() -> None:
    state = projection._BuildState(hashlib.sha256())
    projection._retain_prewarm_shape(
        state.prewarm_heap, ("CPT", "27447"), _aggregate("10001", 3)
    )
    projection._retain_prewarm_shape(
        state.prewarm_heap, ("HCPCS", "G0439"), _aggregate("10002", 8)
    )
    session = _Session()
    assert await projection._store_prewarm_shapes(
        session, PROJECTION_ID, state
    ) == 2

    shape_rows = session.calls[0][1]
    assert [shape_row["provider_count"] for shape_row in shape_rows] == [8, 3]
    expected_digest = hashlib.sha256()
    for shape_row_by_field in shape_rows:
        projection.digest_row(
            expected_digest,
            "prewarm-shape",
            (
                shape_row_by_field["shape_rank"],
                shape_row_by_field["code_system"],
                shape_row_by_field["code"],
                shape_row_by_field["geo_cell"],
                shape_row_by_field["provider_count"],
            ),
            b"",
        )
    assert state.content_digest.digest() == expected_digest.digest()


@pytest.mark.asyncio
async def test_aggregate_reads_only_preflighted_member_and_set_cell_stages() -> None:
    aggregate_result = SimpleNamespace(
        mappings=lambda: [
            {
                "geo_cell": "10001",
                "provider_count": 1,
                "rate_count": 1,
                "minimum_negotiated_rate": "1",
                "median_lower": "1",
                "median_upper": "1",
                "maximum_negotiated_rate": "1",
            }
        ]
    )
    ruled_session = _Session(execute_result=aggregate_result)
    await projection._aggregate_records(
        ruled_session, PROJECTION_ID, ("CPT", "27447")
    )
    aggregate_sql, parameters = ruled_session.calls[0]
    assert "plan_pricing_eligible_member_cell_stage" in aggregate_sql
    assert "plan_pricing_set_cell_stage" in aggregate_sql
    assert "plan_pricing_provider_member_stage" not in aggregate_sql
    assert parameters is None
