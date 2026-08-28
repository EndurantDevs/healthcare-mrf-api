# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import plan_pricing_projection_v3 as projection
from api import ptg2_serving as serving
from api.plan_pricing_aggregate_pack import AggregateZipRecord


PROJECTION_ID = "a" * 64


class _Session:
    def __init__(
        self, *, execute_result=None, stream_rows=(), work_rows: int = 1
    ) -> None:
        self.calls: list[tuple[str, object]] = []
        self.execute_result = execute_result
        self.stream_rows = tuple(stream_rows)
        self.work_rows = work_rows
        self.stream_calls: list[tuple[object, object]] = []

    async def execute(self, statement, parameters=None):
        statement_text = str(statement)
        self.calls.append((statement_text, parameters))
        if "SUM(price.count)" in statement_text:
            return SimpleNamespace(scalar_one=lambda: self.work_rows)
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
async def test_aggregate_taxonomy_rule_is_normalized_and_optional(
    monkeypatch,
) -> None:
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
    monkeypatch.setattr(
        serving,
        "_inferred_provider_taxonomy_rule",
        lambda _code: SimpleNamespace(
            taxonomy_codes=(" 207x00000x ", "207X00000X", " ")
        ),
    )
    ruled_session = _Session(execute_result=aggregate_result)
    await projection._aggregate_records(
        ruled_session, PROJECTION_ID, ("CPT", "27447")
    )
    ruled_work_sql, _ = ruled_session.calls[0]
    ruled_sql, ruled_parameters = ruled_session.calls[1]
    assert "upper(btrim(taxonomy_code))" in ruled_work_sql
    assert "upper(btrim(taxonomy_code))" in ruled_sql
    assert "= ANY(CAST(:taxonomy_codes AS varchar[]))" in ruled_sql
    assert ruled_parameters["taxonomy_codes"] == ["207X00000X"]

    monkeypatch.setattr(
        serving, "_inferred_provider_taxonomy_rule", lambda _code: None
    )
    unruled_session = _Session(execute_result=aggregate_result)
    await projection._aggregate_records(
        unruled_session, PROJECTION_ID, ("HCPCS", "G0439")
    )
    unruled_work_sql, _ = unruled_session.calls[0]
    unruled_sql, unruled_parameters = unruled_session.calls[1]
    assert "unnest(provider.taxonomy_codes)" not in unruled_work_sql
    assert "unnest(provider.taxonomy_codes)" not in unruled_sql
    assert unruled_parameters["taxonomy_codes"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("work_rows", "fails"), ((1, False), (2, True)))
async def test_aggregate_work_cap_is_inclusive(
    monkeypatch, work_rows: int, fails: bool
) -> None:
    from api import plan_pricing_projection_v3_aggregate as aggregate_build

    monkeypatch.setattr(serving, "_inferred_provider_taxonomy_rule", lambda _code: None)
    monkeypatch.setattr(aggregate_build, "MAX_CODE_AGGREGATE_WORK_ROWS", 1)
    aggregate_result = SimpleNamespace(mappings=lambda: [])
    session = _Session(execute_result=aggregate_result, work_rows=work_rows)
    state = projection._BuildState(hashlib.sha256())

    if not fails:
        await aggregate_build._aggregate_records(
            session,
            PROJECTION_ID,
            ("HCPCS", "G0439"),
            state,
        )
        assert state.aggregate_work_rows == 1
        assert len(session.calls) == 2
        return

    with pytest.raises(ValueError, match="aggregate work bound exceeded"):
        await aggregate_build._aggregate_records(
            session,
            PROJECTION_ID,
            ("HCPCS", "G0439"),
            state,
        )
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_aggregate_release_work_cap_is_cumulative(monkeypatch) -> None:
    from api import plan_pricing_projection_v3_aggregate as aggregate_build

    monkeypatch.setattr(serving, "_inferred_provider_taxonomy_rule", lambda _code: None)
    monkeypatch.setattr(
        aggregate_build, "MAX_PROJECTION_AGGREGATE_WORK_ROWS", 1
    )
    state = projection._BuildState(hashlib.sha256())
    state.aggregate_work_rows = 1
    session = _Session(work_rows=1)

    with pytest.raises(ValueError, match="aggregate work bound exceeded"):
        await aggregate_build._aggregate_records(
            session, PROJECTION_ID, ("HCPCS", "G0439"), state
        )

    assert len(session.calls) == 1
