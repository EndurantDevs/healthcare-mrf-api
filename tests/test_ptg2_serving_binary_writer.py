# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_serving_binary as serving_binary_writer


def test_natural_lean_stream_sql_joins_dictionary_tables():
    by_code_sql = serving_binary_writer._serving_binary_stream_sql(
        qualified_source='"mrf"."ptg2_manifest_stage_serving_token"',
        kind="by_code",
        source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN,
        qualified_code_count_table='"mrf"."ptg2_code_count_token"',
        qualified_provider_set_dictionary_table='"mrf"."ptg2_provider_set_dict_token"',
    )
    reverse_sql = serving_binary_writer._serving_binary_stream_sql(
        qualified_source='"mrf"."ptg2_manifest_stage_serving_token"',
        kind="by_provider_set",
        source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN,
        qualified_code_count_table='"mrf"."ptg2_code_count_token"',
        qualified_provider_set_dictionary_table='"mrf"."ptg2_provider_set_dict_token"',
    )

    assert 'FROM "mrf"."ptg2_manifest_stage_serving_token" serving' in by_code_sql
    assert 'JOIN "mrf"."ptg2_code_count_token" code_count' in by_code_sql
    assert 'JOIN "mrf"."ptg2_provider_set_dict_token" provider_set_dictionary' in by_code_sql
    assert "code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system" in by_code_sql
    assert "ORDER BY code_count.code_key, provider_set_dictionary.provider_set_key" in by_code_sql
    assert "ORDER BY provider_set_dictionary.provider_set_key, code_count.code_key" in reverse_sql


def test_natural_lean_stream_sql_requires_dictionary_tables():
    with pytest.raises(ValueError, match="dictionary tables"):
        serving_binary_writer._serving_binary_stream_sql(
            qualified_source='"mrf"."ptg2_manifest_stage_serving_token"',
            kind="by_code",
            source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN,
        )


def test_price_set_atom_stream_sql_uses_authoritative_order():
    sql = serving_binary_writer._price_set_atom_stream_sql(
        qualified_source='"mrf"."ptg2_price_set_atom_token"'
    )

    assert "SELECT price_set_global_id_128, price_atom_global_id_128" in sql
    assert 'FROM "mrf"."ptg2_price_set_atom_token"' in sql
    assert "ORDER BY price_set_global_id_128, price_atom_global_id_128" in sql


def test_default_stream_configs_respect_db_pool_capacity(monkeypatch):
    monkeypatch.delenv(serving_binary_writer.PTG2_SERVING_BINARY_STREAM_TASKS_ENV, raising=False)
    monkeypatch.setenv(serving_binary_writer.DB_POOL_MAX_SIZE_ENV, "5")
    stage_configs = serving_binary_writer._serving_binary_stream_stage_configs(include_price_set_atoms=True)

    assert serving_binary_writer._configured_serving_binary_stream_tasks() == 3
    assert serving_binary_writer._serving_binary_stream_tasks() == 2
    assert [stage.kind for stage in stage_configs] == [
        "by_code",
        "price_set_atoms_by_id_v2",
        "by_provider_set",
    ]


def test_three_streams_require_sufficient_db_pool_capacity(monkeypatch):
    monkeypatch.setenv(serving_binary_writer.DB_POOL_MAX_SIZE_ENV, "32")

    assert serving_binary_writer._serving_binary_stream_tasks() == 3

    monkeypatch.setenv(serving_binary_writer.DB_POOL_MAX_SIZE_ENV, "1")
    with pytest.raises(RuntimeError, match="at least two database connections"):
        serving_binary_writer._serving_binary_stream_tasks()


@pytest.mark.asyncio
async def test_serving_binary_table_is_created_logged(monkeypatch):
    statements = []
    queries = []

    async def fake_status(statement):
        statements.append(statement)

    async def fake_first(statement, **_params):
        queries.append(statement)
        return {"relation_persistence": "logged"}

    monkeypatch.setattr(serving_binary_writer.db, "status", fake_status)
    monkeypatch.setattr(serving_binary_writer.db, "first", fake_first)

    await serving_binary_writer.create_ptg2_serving_binary_table(
        schema_name="mrf",
        target_table="ptg2_serving_binary_token",
    )

    create_statement = next(statement for statement in statements if "CREATE TABLE" in statement)
    assert "CREATE UNLOGGED TABLE" not in create_statement
    assert 'CREATE TABLE "mrf"."ptg2_serving_binary_token"' in create_statement
    storage_summary = await serving_binary_writer.finalize_ptg2_serving_binary_table(
        schema_name="mrf",
        target_table="ptg2_serving_binary_token",
    )
    assert storage_summary["relation_persistence"] == "logged"
    assert "relation.relpersistence" in queries[0]


@pytest.mark.asyncio
async def test_price_set_atom_v2_writer_groups_sorted_ids_by_prefix(monkeypatch):
    copied_records = []

    async def fake_copy_records(*, schema_name, table_name, records):
        copied_records.extend(records)

    monkeypatch.setattr(serving_binary_writer, "_copy_serving_binary_records", fake_copy_records)
    writer = serving_binary_writer._PriceSetAtomPrefixBlockWriter(
        schema_name="mrf",
        table_name="ptg2_serving_binary_test",
        max_payload_bytes=1024 * 1024,
        batch_size=100,
    )
    first_atom_id = bytes.fromhex("000000000000000000000000000000c1")

    await writer.add_price_set(bytes.fromhex("000100000000000000000000000000a1"), (first_atom_id,))
    await writer.add_price_set(bytes.fromhex("000100000000000000000000000000a2"), ())
    await writer.add_price_set(bytes.fromhex("000200000000000000000000000000a3"), ())
    await writer.finish()

    v2_blocks = [
        (block_key, block_no, entry_count)
        for artifact_kind, block_key, block_no, entry_count, *_ in copied_records
        if artifact_kind == serving_binary_writer.PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND
    ]
    assert v2_blocks == [(1, 0, 2), (2, 0, 1)]
    assert writer.summary() == {
        "format": "price_set_atoms_by_id_v2",
        "artifact_kind": "price_set_atoms_by_id_v2",
        "id_prefix_bytes": 2,
        "id_bucket_count": 65536,
        "id_block_count": 2,
        "price_set_count": 3,
        "atom_ref_count": 1,
        "copied_records": 2,
    }


@pytest.mark.asyncio
async def test_price_set_atom_v2_writer_rejects_oversized_membership(monkeypatch):
    async def fake_copy_records(**_kwargs):
        raise AssertionError("oversized membership must fail before COPY")

    monkeypatch.setattr(serving_binary_writer, "_copy_serving_binary_records", fake_copy_records)
    writer = serving_binary_writer._PriceSetAtomPrefixBlockWriter(
        schema_name="mrf",
        table_name="ptg2_serving_binary_test",
        max_payload_bytes=32,
        batch_size=100,
    )

    with pytest.raises(RuntimeError, match="maximum single-membership size of 32 bytes"):
        await writer.add_price_set(
            bytes.fromhex("000100000000000000000000000000a1"),
            (bytes.fromhex("000000000000000000000000000000c1"),),
        )


@pytest.mark.asyncio
async def test_price_set_atom_table_is_authoritative_when_empty(monkeypatch):
    async def fake_table_writer(**_kwargs):
        return {"source": "price_set_atom_table", "price_set_count": 0}

    def fail_sidecar_lookup(_artifact):
        raise AssertionError("filesystem sidecar must not be consulted when a DB table exists")

    monkeypatch.setattr(serving_binary_writer, "_write_atom_map_blocks_from_table", fake_table_writer)
    monkeypatch.setattr(serving_binary_writer, "_price_forward_artifact_path", fail_sidecar_lookup)

    summary = await serving_binary_writer._write_price_set_atom_blocks(
        schema_name="mrf",
        table_name="ptg2_serving_binary_test",
        price_set_atom_table="ptg2_price_set_atom_token",
        price_forward_artifact={"path": "/tmp/legacy-price-forward"},
        max_payload_bytes=1024,
    )

    assert summary == {"source": "price_set_atom_table", "price_set_count": 0}


@pytest.mark.asyncio
async def test_three_stream_stages_start_concurrently(monkeypatch):
    started_kinds = set()
    all_started = asyncio.Event()

    async def fake_stream_stage(**kwargs):
        started_kinds.add(kwargs["config"].kind)
        if len(started_kinds) == 3:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=1)
        return {"kind": kwargs["config"].kind}

    monkeypatch.setattr(serving_binary_writer, "_stream_serving_binary_stage", fake_stream_stage)
    stage_configs = serving_binary_writer._serving_binary_stream_stage_configs(include_price_set_atoms=True)

    stage_result_by_kind = await serving_binary_writer._run_serving_binary_stream_stages(
        stage_configs=stage_configs,
        stream_tasks=3,
        progress_callback=None,
        expected_row_count=10,
        qualified_source='"mrf"."serving"',
        source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
        qualified_code_count_table=None,
        qualified_provider_set_dictionary_table=None,
        qualified_price_set_atom_table='"mrf"."price_set_atom"',
        schema_name="mrf",
        target_table="binary",
        source_copy_format="binary",
        target_copy_format="binary",
        timing_by_stage={},
    )

    assert set(stage_result_by_kind) == {"by_code", "by_provider_set", "price_set_atoms_by_id_v2"}
    assert started_kinds == set(stage_result_by_kind)


@pytest.mark.asyncio
async def test_failed_stream_stage_cancels_concurrent_peers(monkeypatch):
    started_kinds = set()
    cancelled_kinds = set()
    all_started = asyncio.Event()

    async def fake_stream_stage(**kwargs):
        kind = kwargs["config"].kind
        started_kinds.add(kind)
        if len(started_kinds) == 3:
            all_started.set()
        await all_started.wait()
        if kind == "price_set_atoms_by_id_v2":
            raise RuntimeError("atom stream failed")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_kinds.add(kind)
            raise

    monkeypatch.setattr(serving_binary_writer, "_stream_serving_binary_stage", fake_stream_stage)
    stage_configs = serving_binary_writer._serving_binary_stream_stage_configs(include_price_set_atoms=True)

    with pytest.raises(RuntimeError, match="atom stream failed"):
        await serving_binary_writer._run_serving_binary_stream_stages(
            stage_configs=stage_configs,
            stream_tasks=3,
            progress_callback=None,
            expected_row_count=10,
            qualified_source='"mrf"."serving"',
            source_layout=serving_binary_writer.PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
            qualified_code_count_table=None,
            qualified_provider_set_dictionary_table=None,
            qualified_price_set_atom_table='"mrf"."price_set_atom"',
            schema_name="mrf",
            target_table="binary",
            source_copy_format="binary",
            target_copy_format="binary",
            timing_by_stage={},
        )

    assert cancelled_kinds == {"by_code", "by_provider_set"}


@pytest.mark.asyncio
async def test_rust_stream_preserves_atom_summary_timing_and_bytes(monkeypatch):
    observed_stage_kinds = []

    async def fake_noop(**_kwargs):
        return None

    async def fake_run_stages(**kwargs):
        observed_stage_kinds.extend(config.kind for config in kwargs["stage_configs"])
        kwargs["timing_by_stage"]["price_set_atoms_stream_seconds"] = 0.25
        return {
            "by_code": {"row_count": 2},
            "by_provider_set": {"row_count": 2},
            "price_set_atoms_by_id_v2": {
                "source": "price_set_atom_table",
                "price_set_count": 1,
                "atom_ref_count": 2,
                "source_copy_bytes": 96,
                "target_copy_bytes": 80,
                "pipeline_seconds": 0.2,
            },
        }

    async def fake_finalize(**_kwargs):
        return {"record_count": 3}

    async def fail_python_atom_stage(**_kwargs):
        raise AssertionError("Rust atom stream must replace the Python atom stage")

    monkeypatch.setattr(serving_binary_writer, "create_ptg2_serving_binary_table", fake_noop)
    monkeypatch.setattr(serving_binary_writer, "_run_serving_binary_stream_stages", fake_run_stages)
    monkeypatch.setattr(serving_binary_writer, "_finalize_stream_binary_table", fake_finalize)
    monkeypatch.setattr(serving_binary_writer, "_write_atom_map_stage", fail_python_atom_stage)
    monkeypatch.setattr(serving_binary_writer, "_use_serving_binary_combined_stream", lambda: False)
    monkeypatch.setattr(serving_binary_writer, "_serving_binary_stream_tasks", lambda: 3)

    summary = await serving_binary_writer._write_serving_binary_rust_stream(
        schema_name="mrf",
        source_table="serving",
        target_table="binary",
        expected_row_count=2,
        price_set_atom_table="price_set_atom",
    )

    assert observed_stage_kinds == ["by_code", "price_set_atoms_by_id_v2", "by_provider_set"]
    assert summary["stream_tasks"] == 3
    assert summary["price_set_atoms"]["stage_table"] == "mrf.price_set_atom"
    assert summary["price_set_atoms"]["source_copy_bytes"] == 96
    assert summary["price_set_atoms"]["target_copy_bytes"] == 80
    assert summary["timing"]["price_set_atoms_stream_seconds"] == 0.25


@pytest.mark.asyncio
async def test_combined_stream_serializes_atom_stage_when_pool_allows_one_pipeline(monkeypatch):
    stage_events = []

    async def fake_noop(**_kwargs):
        return None

    async def fake_combined(**_kwargs):
        stage_events.extend(["combined_start", "combined_done"])
        return {
            "pipeline_seconds": 0.1,
            "by_code": {"row_count": 2},
            "by_provider_set": {"row_count": 2},
        }

    async def fake_atom_stage(**_kwargs):
        stage_events.append("atoms")
        return {"price_set_count": 1, "atom_ref_count": 2}

    async def fake_finalize(**_kwargs):
        return {"record_count": 3}

    monkeypatch.setattr(serving_binary_writer, "create_ptg2_serving_binary_table", fake_noop)
    monkeypatch.setattr(serving_binary_writer, "_use_serving_binary_combined_stream", lambda: True)
    monkeypatch.setattr(serving_binary_writer, "_serving_binary_stream_tasks", lambda: 1)
    monkeypatch.setattr(serving_binary_writer, "_stream_serving_binary_combined_copy", fake_combined)
    monkeypatch.setattr(serving_binary_writer, "_stream_serving_binary_stage", fake_atom_stage)
    monkeypatch.setattr(serving_binary_writer, "_finalize_stream_binary_table", fake_finalize)

    summary = await serving_binary_writer._write_serving_binary_rust_stream(
        schema_name="mrf",
        source_table="serving",
        target_table="binary",
        expected_row_count=2,
        price_set_atom_table="price_set_atom",
    )

    assert stage_events == ["combined_start", "combined_done", "atoms"]
    assert summary["stream_tasks"] == 1
    assert summary["price_set_atoms"]["stage_table"] == "mrf.price_set_atom"


@pytest.mark.asyncio
async def test_python_writer_remains_final_fallback(monkeypatch):
    async def fail_rust_writer(**_kwargs):
        raise RuntimeError("Rust writer failed")

    async def fake_python_writer(**_kwargs):
        return {"writer": "python_stream"}

    monkeypatch.setattr(serving_binary_writer, "_ptg2_rust_scanner_binary", lambda: Path("scanner"))
    monkeypatch.setattr(serving_binary_writer, "_serving_binary_rust_enabled", lambda: True)
    monkeypatch.setattr(serving_binary_writer, "_use_serving_binary_stream", lambda: True)
    monkeypatch.setattr(serving_binary_writer, "_write_serving_binary_rust_stream", fail_rust_writer)
    monkeypatch.setattr(serving_binary_writer, "_write_ptg2_serving_binary_table_rust", fail_rust_writer)
    monkeypatch.setattr(serving_binary_writer, "_write_ptg2_serving_binary_table_python", fake_python_writer)

    summary = await serving_binary_writer.write_ptg2_serving_binary_table(
        schema_name="mrf",
        source_table="serving",
        target_table="binary",
        price_set_atom_table="price_set_atom",
    )

    assert summary == {"writer": "python_stream"}


@pytest.mark.asyncio
async def test_cancelled_stream_writer_drops_partial_target(monkeypatch):
    dropped_targets = []

    async def cancel_stream_writer(**_kwargs):
        raise asyncio.CancelledError

    async def fake_drop_target(**kwargs):
        dropped_targets.append((kwargs["schema_name"], kwargs["target_table"]))

    async def fail_fallback(**_kwargs):
        raise AssertionError("cancellation must not start a fallback writer")

    monkeypatch.setattr(serving_binary_writer, "_ptg2_rust_scanner_binary", lambda: Path("scanner"))
    monkeypatch.setattr(serving_binary_writer, "_serving_binary_rust_enabled", lambda: True)
    monkeypatch.setattr(serving_binary_writer, "_use_serving_binary_stream", lambda: True)
    monkeypatch.setattr(serving_binary_writer, "_write_serving_binary_rust_stream", cancel_stream_writer)
    monkeypatch.setattr(serving_binary_writer, "_write_ptg2_serving_binary_table_rust", fail_fallback)
    monkeypatch.setattr(serving_binary_writer, "_drop_ptg2_serving_binary_table", fake_drop_target)

    with pytest.raises(asyncio.CancelledError):
        await serving_binary_writer.write_ptg2_serving_binary_table(
            schema_name="mrf",
            source_table="serving",
            target_table="binary",
            price_set_atom_table="price_set_atom",
        )

    assert dropped_targets == [("mrf", "binary")]
