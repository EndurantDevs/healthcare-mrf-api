# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic sampling and closed compact completion semantics."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_completion as compact_completion
from process import provider_directory_compact_source_storage as layout
from process import provider_directory_generic_candidate as candidate_rows
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from tests.test_provider_directory_generic_candidate import _merge, _resource


async def _stream(resources):
    for resource in resources:
        yield resource


@pytest.mark.asyncio
async def test_changed_outputs_reuse_the_complete_key_reducer(monkeypatch):
    resource_rows = [_resource("same", values=("one",)), _resource("same", values=("two",)), _resource("unchanged")]
    monkeypatch.setattr(compact_completion, "_conflicting_rows", lambda *_args: _stream(resource_rows))
    monkeypatch.setattr(completion, "_resource_callbacks", lambda *_args: (dict, _merge))
    actual_rows = [
        resource
        async for resource in compact_completion._changed_resources(
            None, None, {}, SimpleNamespace(dataset_id="dataset"), None, "keys"
        )
    ]
    assert len(actual_rows) == 1 and actual_rows[0]["resource_id"] == "same"
    assert actual_rows[0]["payload_json"]["values"] == ["one", "two"]


@pytest.mark.asyncio
async def test_reduction_chunks_use_the_existing_copy_byte_bound(monkeypatch):
    resource_rows = [
        {**_resource("same"), "payload_json": {"resource_id": "same", "padding": "x" * 700, "last": index}}
        for index in range(6)
    ]
    monkeypatch.setattr(completion, "COPY_MAX_BYTES", 1200)
    prepared_rows = []

    def prepare(resource):
        prepared_rows.append(resource)
        return resource

    actual_rows = [
        resource
        async for resource in completion._reduced_rows(
            _stream(resource_rows), dataset_id="dataset", prepare=prepare, merge=lambda _previous, incoming: incoming
        )
    ]
    assert actual_rows == [resource_rows[-1]]
    assert len(prepared_rows) > len(resource_rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["rows", "bytes"])
@pytest.mark.parametrize("fail_read", [False, True])
async def test_locator_pages_preserve_order_and_propagate_reconstruction_failure(monkeypatch, boundary, fail_read):
    keys = [{"batch_ordinal": 7, "row_ordinal": index, "read_bytes": 100} for index in range(5)]
    resources = [_resource(str(index)) for index in range(5)]
    monkeypatch.setattr(candidate_rows, "COPY_MAX_ROWS", 2 if boundary == "rows" else 100)
    monkeypatch.setattr(candidate_rows, "COPY_MAX_BYTES", 10000 if boundary == "rows" else 221)
    pages = []

    async def reconstruct(connection, tables, dataset_id, selected):
        assert connection is cursor_connection and tables is payload_tables and dataset_id == "dataset"
        pages.append(list(selected))
        if fail_read and len(pages) == 2:
            raise TimeoutError("payload read interrupted")
        return [resources[key["row_ordinal"]] for key in selected]

    cursor = lambda *_args, **_kwargs: _stream(keys)
    cursor_connection = SimpleNamespace(cursor=cursor)
    payload_tables = object()
    monkeypatch.setattr(compact, "read_compact_payload_keys", reconstruct)
    actual_rows = []

    async def consume():
        async for row in compact_completion._bounded_resources(
            cursor_connection, payload_tables, "dataset", "selected locator query", "argument"
        ):
            actual_rows.append(row)

    if fail_read:
        with pytest.raises(TimeoutError, match="payload read interrupted"):
            await consume()
        assert actual_rows == resources[:2] and pages == [keys[:2], keys[2:4]]
    else:
        await consume()
        assert actual_rows == resources and pages == [keys[:2], keys[2:4], keys[4:]]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["rows", "bytes"])
@pytest.mark.parametrize("fail_write", [False, True])
async def test_changed_output_batches_bound_copy_and_stop_after_write_failure(monkeypatch, boundary, fail_write):
    resources = [_resource(str(index)) for index in range(5)]
    row_bytes = candidate_rows.copy_record_bytes(
        tuple(
            storage._encoded(resources[0][name]) if name == "payload_json" else resources[0][name]
            for name in candidate_rows.RESOURCE_COLUMNS
        )
    )
    monkeypatch.setattr(candidate_rows, "COPY_MAX_ROWS", 2 if boundary == "rows" else 100)
    monkeypatch.setattr(
        candidate_rows,
        "COPY_MAX_BYTES",
        10000 if boundary == "rows" else candidate_rows.COPY_FRAME_BYTES + 2 * row_bytes,
    )
    visited, batches = [], []
    connection, policy, binding, owner = object(), object(), object(), object()

    async def incoming():
        for row in resources:
            visited.append(row)
            yield row

    async def write(actual_connection, actual_policy, actual_binding, rows, *, session):
        assert session is owner
        assert (actual_connection, actual_policy, actual_binding) == (connection, policy, binding)
        batches.append(list(rows))
        if fail_write:
            raise TimeoutError("output write interrupted")

    monkeypatch.setattr(compact_completion, "_write_overrides", write)
    if fail_write:
        with pytest.raises(TimeoutError, match="output write interrupted"):
            await compact_completion._store_changed_resources(connection, policy, binding, incoming(), session=owner)
        assert batches == [resources[:2]] and visited == resources[:3]
    else:
        await compact_completion._store_changed_resources(connection, policy, binding, incoming(), session=owner)
        assert batches == [resources[:2], resources[2:4], resources[4:]] and visited == resources


@pytest.mark.asyncio
@pytest.mark.parametrize("reused", [1, 2])
async def test_exact_observation_reuse_only_appends_missing_output_payloads(monkeypatch, reused):
    resources = [_resource("first"), _resource("second")]
    aliases, _, _ = compact.prepare_compact_payload_batch(
        resources, dataset_id="dataset", batch_ordinal=5, format_id=compact.FORMAT
    )
    matched_rows = [(*alias, None) for alias in aliases[:reused]]
    binding_by_field = {
        "identity": {"dataset_id": "dataset"},
        "ordinal": 3,
        "tables": {
            root: {"name": "pdgs_" + "a" * 32 + "_" + suffix}
            for root, suffix in {**layout.TABLE_SUFFIXES, "resource_output_aliases": "oa"}.items()
        },
    }
    previous_by_field = deepcopy(binding_by_field)
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=8))
    policy = SimpleNamespace(schema="source")
    copy, append = AsyncMock(), AsyncMock()
    monkeypatch.setattr(compact_completion, "_reusable_aliases", AsyncMock(return_value=matched_rows))
    monkeypatch.setattr(compact, "_copy", copy)
    monkeypatch.setattr(storage, "_append_compact_resources", append)
    await compact_completion._write_overrides(connection, policy, binding_by_field, resources, session=object())
    copy.assert_awaited_once_with(
        connection,
        compact_completion._output_tables(policy, binding_by_field),
        "aliases",
        (*compact._ALIAS_COLUMNS, "npi_text"),
        matched_rows,
    )
    assert connection.execute.await_args.args[1:] == ("dataset", ["Organization", "Organization"], ["first", "second"])
    if reused == len(resources):
        connection.fetchval.assert_not_awaited()
        append.assert_not_awaited()
    else:
        connection.fetchval.assert_awaited_once()
        append.assert_awaited_once_with(
            connection,
            policy,
            {
                **binding_by_field,
                "tables": {
                    **binding_by_field["tables"],
                    "resource_aliases": binding_by_field["tables"]["resource_output_aliases"],
                },
            },
            resources[reused:],
            8,
        )
    assert binding_by_field == previous_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [asyncio.CancelledError, TimeoutError])
async def test_reused_alias_copy_interruption_closes_driver_and_invalidates_original_session(monkeypatch, interruption):
    resources = [_resource("first")]
    aliases, _, _ = compact.prepare_compact_payload_batch(
        resources, dataset_id="dataset", batch_ordinal=5, format_id=compact.FORMAT
    )
    binding_by_field = {
        "identity": {"dataset_id": "dataset"},
        "ordinal": 3,
        "tables": {
            root: {"name": "pdgs_" + "a" * 32 + "_" + suffix}
            for root, suffix in {**layout.TABLE_SUFFIXES, "resource_output_aliases": "oa"}.items()
        },
    }
    before = deepcopy(binding_by_field)
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(), close=AsyncMock(), invalidate=AsyncMock())
    owner = SimpleNamespace(invalidate=AsyncMock())
    failure = interruption()
    append = AsyncMock()
    monkeypatch.setattr(compact_completion, "_reusable_aliases", AsyncMock(return_value=[(*aliases[0], None)]))
    monkeypatch.setattr(compact, "_copy", AsyncMock(side_effect=failure))
    monkeypatch.setattr(storage, "_append_compact_resources", append)
    with pytest.raises(interruption) as raised:
        await compact_completion._write_overrides(
            connection, SimpleNamespace(schema="source"), binding_by_field, resources, session=owner
        )
    assert raised.value is failure and binding_by_field == before
    connection.close.assert_awaited_once_with(timeout=5)
    owner.invalidate.assert_awaited_once_with()
    connection.invalidate.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    append.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinal", [True, 3, 2**63 - 1])
async def test_output_payload_refuses_invalid_or_reused_sequence_ordinal(monkeypatch, ordinal):
    binding_by_field = {
        "identity": {"dataset_id": "dataset"},
        "ordinal": 3,
        "tables": {
            root: {"name": "pdgs_" + "a" * 32 + "_" + suffix}
            for root, suffix in {**layout.TABLE_SUFFIXES, "resource_output_aliases": "oa"}.items()
        },
    }
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=ordinal))
    append = AsyncMock()
    monkeypatch.setattr(compact_completion, "_reusable_aliases", AsyncMock(return_value=[]))
    monkeypatch.setattr(compact, "_copy", AsyncMock())
    monkeypatch.setattr(storage, "_append_compact_resources", append)
    with pytest.raises(ValueError, match="ordinal_exhausted"):
        await compact_completion._write_overrides(
            connection, SimpleNamespace(schema="source"), binding_by_field, [_resource()], session=object()
        )
    append.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("population", [1, 10001])
async def test_sample_is_deterministic_and_accounts_for_coverage(monkeypatch, caplog, population):
    binding_by_field = {
        "identity": {"dataset_id": "dataset", "endpoint_id": "endpoint", "acquisition_root_run_id": "root"},
        "tables": {
            root: {"name": "pdgs_" + "a" * 32 + "_" + suffix}
            for root, suffix in {**layout.TABLE_SUFFIXES, "resource_output_aliases": "oa"}.items()
        },
    }
    sample_rows = [_resource(str(index)) for index in range(min(population, 2))]
    queries = []

    async def selected(_connection, _tables, _dataset, query, *args):
        queries.append((query, args))
        async for resource in _stream(sample_rows):
            yield resource

    checked_rows = []
    monkeypatch.setattr(compact_completion, "_validate_output_keys", AsyncMock())
    monkeypatch.setattr(compact_completion, "_validate_selected_aliases", AsyncMock())
    monkeypatch.setattr(compact_completion, "_bounded_resources", selected)
    monkeypatch.setattr(completion, "_RECHECK_ROWS", 2)
    monkeypatch.setattr(completion, "_resource_callbacks", lambda *_args: (checked_rows.append, None))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=population))
    with caplog.at_level("INFO", logger=compact_completion.__name__):
        for _ in range(2):
            await compact_completion.validate_output(
                connection,
                SimpleNamespace(schema="source"),
                binding_by_field,
                SimpleNamespace(dataset_id="dataset"),
                None,
            )
    assert queries[0] == queries[1] and "TABLESAMPLE" not in queries[0][0]
    assert len(queries[0][1][0]) == 64 and len(checked_rows) == 2 * len(sample_rows)
    assert f"population={population} sampled={len(sample_rows)} seed=" in caplog.text


@pytest.mark.parametrize("damage", [None, "extra", "physical", "suffix"])
def test_prepared_compact_inventory_is_closed_by_name(damage):
    suffixes_by_root = {
        **layout.TABLE_SUFFIXES,
        "resource_output_aliases": "oa",
        **{root: storage._SUFFIXES[root] for root in storage.RELATIONS},
    }
    binding_by_field = {
        "phase": "prepared",
        "auxiliary": None,
        "relationship_sha256": None,
        "tables": {root: {"name": "pdgs_" + "a" * 32 + "_" + suffix} for root, suffix in suffixes_by_root.items()},
    }
    if damage == "extra":
        binding_by_field["tables"]["unexpected"] = {"name": "extra"}
    elif damage == "physical":
        binding_by_field["tables"]["resource_output_aliases"]["oid"] = 1
    elif damage == "suffix":
        binding_by_field["tables"][storage.RESOURCE]["name"] = "pdgs_" + "a" * 32 + "_ri"
    if damage is None:
        layout.validate_tables(binding_by_field)
        return
    with pytest.raises(ValueError):
        layout.validate_tables(binding_by_field)
