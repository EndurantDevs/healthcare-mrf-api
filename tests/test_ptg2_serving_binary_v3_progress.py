# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_serving_binary as serving_binary


def _publish_options(progress_callback):
    return serving_binary._V3PublishOptions(
        schema_name="mrf",
        source_table="ptg2_serving_source",
        target_table="ptg2_serving_binary_target",
        expected_row_count=4,
        price_set_atom_table="ptg2_price_set_atom",
        price_atom_table="ptg2_price_atom",
        source_layout=serving_binary.PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
        code_count_table=None,
        provider_set_dictionary_table=None,
        price_atom_table_layout="lean_dict_v2",
        price_atom_constant_keys={"setting_key": 0},
        progress_callback=progress_callback,
    )


@pytest.mark.asyncio
async def test_v3_dense_map_progress_is_reported(monkeypatch):
    progress_events = []
    dense_stats_by_name = {
        "row_count": 2,
        "distinct_id_count": 2,
        "distinct_key_count": 2,
        "minimum_key": 0,
        "maximum_key": 1,
    }
    monkeypatch.setattr(serving_binary, "_drop_v3_stage_tables", AsyncMock())
    monkeypatch.setattr(
        serving_binary,
        "_create_v3_stage_maps",
        AsyncMock(return_value=(dense_stats_by_name, dense_stats_by_name)),
    )
    monkeypatch.setattr(serving_binary, "create_ptg2_serving_binary_table", AsyncMock())
    monkeypatch.setattr(
        serving_binary,
        "_finish_v3_serving_binary",
        AsyncMock(return_value={"arch_version": "postgres_binary_v3"}),
    )

    await serving_binary._write_v3_serving_binary(
        _publish_options(lambda step, **details: progress_events.append((step, details)))
    )

    assert [step for step, _ in progress_events] == [
        "serving binary dense maps",
        "serving binary dense maps complete",
    ]
    assert [details["done"] for _, details in progress_events] == [0, 1]
    assert {details["total"] for _, details in progress_events} == {6}


@pytest.mark.asyncio
async def test_v3_artifact_stream_progress_is_reported(monkeypatch):
    progress_events = []
    stream_kinds = (
        serving_binary.PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND,
        serving_binary.PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND,
        serving_binary.PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
        serving_binary.PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
    )

    async def fake_stream(**kwargs):
        return {"artifact_kind": kwargs["kind"]}

    monkeypatch.setattr(serving_binary, "_serving_binary_stream_tasks", lambda: 4)
    monkeypatch.setattr(serving_binary, "_stream_serving_binary_copy", fake_stream)

    await serving_binary._run_v3_streams(
        sql_by_kind={kind: f"SELECT '{kind}'" for kind in stream_kinds},
        schema_name="mrf",
        target_table="binary",
        target_copy_format="binary",
        atom_count=10,
        atom_key_bits=24,
        progress_callback=lambda step, **details: progress_events.append((step, details)),
    )

    assert [details["done"] for _, details in progress_events] == [2, 3, 4, 5]
    assert {details["total"] for _, details in progress_events} == {6}
