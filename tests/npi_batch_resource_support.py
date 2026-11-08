# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Maximum parser expansion through the native bounded COPY adapter."""

import importlib
import json
from collections import Counter

import pytest
from sqlalchemy import text

from process import npi_isolated_batches as batches
from tests.npi_batch_native_support import _assert_temp_removed, _nothing, _prepared
from tests.test_npi_isolated_batches import _expanded_row


def _expanded_raw_rows():
    raw_rows = []
    for index in range(257):
        source_row = _expanded_row(index)
        for number in range(1, 51):
            source_row[f"Other Provider Identifier Issuer_{number}"] = "issuer-" + "x" * 192
        raw_rows.append(source_row)
    return raw_rows


async def _assert_complete_normalized_fields(session, schema, models_by_kind, expected_by_kind):
    observed_counts = Counter()
    for kind, expected_rows in expected_by_kind.items():
        model, _rewrite = models_by_kind[kind]
        actual_rows = (
            (await session.execute(text(f'SELECT to_jsonb(t) FROM "{schema}"."{model.__tablename__}" t')))
            .scalars()
            .all()
        )
        observed_counts[kind] = len(actual_rows)
        expected_rows = json.loads(json.dumps(expected_rows, default=str))
        keys = model.__my_index_elements__
        actual_by_key = {tuple(row[key] for key in keys): row for row in actual_rows}
        for expected in expected_rows:
            actual = actual_by_key[tuple(expected[key] for key in keys)]
            assert {field: actual[field] for field in expected} == expected
    return observed_counts


@pytest.mark.asyncio
async def test_native_maximum_parser_expansion_matches_complete_legacy_rows(
    native_runtime, monkeypatch, record_property
):
    """Compare every normalized field after three raw buffers and bounded native COPY."""
    npi = importlib.import_module("process.npi")
    schema, database, models_by_kind = await _prepared(native_runtime)
    expected_by_kind = {}
    raw_rows = _expanded_raw_rows()
    task_by_name = {
        "row_list": raw_rows,
        "npi_csv_map": {"NPI": "npi", "Entity Type Code": "entity_type_code"},
        "npi_csv_map_reverse": {"npi": "NPI"},
        "direct": True,
    }

    async def capture(_ctx, payload):
        expected_by_kind.update(payload)

    async def check_cancel(*_args):
        return None

    monkeypatch.setattr(npi, "source_enabled", lambda _: True)
    monkeypatch.setattr(npi, "save_npi_data", capture)
    monkeypatch.setattr(npi.isolated_npi, "current_npi_isolated_execution", lambda: None)
    await npi._process_npi_chunk({"redis": None}, task_by_name)
    expected_counts = Counter({kind: len(normalized_rows) for kind, normalized_rows in expected_by_kind.items()})
    assert sum(expected_counts.values()) == 257 * 83
    monkeypatch.setattr(npi.isolated_npi, "current_npi_isolated_execution", lambda: object())
    monkeypatch.setattr(npi, "raise_if_cancelled", check_cancel)
    async with native_runtime.builders() as session, session.begin(), database.bind_existing_session(session):
        async with batches.bind(database, models_by_kind.__getitem__, _nothing) as writer:
            await npi.process_npi_chunk({"redis": None, "import_date": "20260331"}, task_by_name)
        observed_counts = await _assert_complete_normalized_fields(session, schema, models_by_kind, expected_by_kind)
        assert observed_counts == expected_counts
        assert 0 < writer.peak_temporary_bytes < batches.WINDOW_BYTES // 3
        await _assert_temp_removed(session, writer.prefix)
    record_property("raw_rows", len(raw_rows))
    record_property("normalized_rows", sum(observed_counts.values()))
    record_property("maximum_raw_rows", batches.RAW_ROWS)
    record_property("maximum_copy_bytes", batches.COPY_BYTES)
    record_property("peak_temporary_relation_bytes", writer.peak_temporary_bytes)
