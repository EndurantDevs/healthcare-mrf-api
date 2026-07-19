from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_manifest_publish as manifest_publish
from process.ptg_parts import ptg2_shared_price as shared_price
from process.ptg_parts.ptg2_serving_binary_v3 import (
    decode_price_memberships,
    encode_price_memberships,
)

_CONSTANT_PRICE_ATOM_ROWS = (
    ("negotiated_type", 0, "negotiated", None),
    ("expiration_date", 0, "2028-12-31", None),
    ("service_code", 0, None, ["11"]),
    ("billing_class", 0, "professional", None),
    ("setting", 0, "inpatient", None),
    ("billing_code_modifier", 0, None, ["AA"]),
    ("additional_information", 0, "constant note", None),
)


@pytest.mark.asyncio
async def test_price_key_map_export_is_dense_key_ordered(monkeypatch, tmp_path):
    observed_queries = []

    class DriverConnection:
        async def copy_from_query(self, query, *, output, **copy_options):
            observed_queries.append(query)
            assert copy_options == {"format": "binary"}
            output.write(b"postgres-binary-copy")

    @asynccontextmanager
    async def acquire_driver():
        yield DriverConnection()

    monkeypatch.setattr(shared_price.db, "acquire_driver", acquire_driver)
    output_path = tmp_path / "price-key-map.copy"

    exported_path = await shared_price.export_shared_price_key_map(
        shared_price.PreparedSharedPriceKeyMap(
            schema_name="mrf",
            price_key_map="prepared_price_keys",
            price_set_count=2,
        ),
        output_path,
    )

    assert exported_path == output_path
    assert output_path.read_bytes() == b"postgres-binary-copy"
    assert len(observed_queries) == 1
    normalized_query = " ".join(observed_queries[0].split())
    assert "SELECT price_set_global_id_128, price_key" in normalized_query
    assert 'FROM "mrf"."prepared_price_keys"' in normalized_query
    assert normalized_query.endswith("ORDER BY price_key")


@pytest.mark.asyncio
async def test_strict_price_stage_deduplicates_identical_cross_file_atoms(monkeypatch):
    scalar = AsyncMock(side_effect=[3, 2, False])
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "scalar", scalar)
    monkeypatch.setattr(shared_price.db, "status", status)

    metrics = await shared_price._normalize_strict_price_atom_stage(
        schema_name="mrf",
        price_atom_table="price_atoms",
    )

    assert metrics == {
        "rows_before": 3,
        "rows_after": 2,
        "duplicate_rows_removed": 1,
        "conflicting_ids": 0,
    }
    scalar_statements = [call.args[0] for call in scalar.await_args_list]
    assert "IS DISTINCT FROM" in scalar_statements[2]
    status_statements = [call.args[0] for call in status.await_args_list]
    assert any("SELECT DISTINCT ON (price_atom_global_id_128)" in sql for sql in status_statements)
    assert any("CREATE UNIQUE INDEX" in sql for sql in status_statements)
    assert any("RENAME TO \"price_atoms\"" in sql for sql in status_statements)


@pytest.mark.asyncio
async def test_strict_price_stage_rejects_same_id_with_different_payload(monkeypatch):
    scalar = AsyncMock(side_effect=[2, 1, True])
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "scalar", scalar)
    monkeypatch.setattr(shared_price.db, "status", status)

    with pytest.raises(RuntimeError, match="conflicting source payloads"):
        await shared_price._normalize_strict_price_atom_stage(
            schema_name="mrf",
            price_atom_table="price_atoms",
        )

    assert not any(
        "RENAME TO \"price_atoms\"" in call.args[0]
        for call in status.await_args_list
    )


@pytest.mark.asyncio
async def test_strict_price_stage_rejects_empty_dictionary(monkeypatch):
    monkeypatch.setattr(shared_price.db, "scalar", AsyncMock(side_effect=[0, 0]))
    monkeypatch.setattr(shared_price.db, "status", AsyncMock())

    with pytest.raises(RuntimeError, match="empty price atom dictionary"):
        await shared_price._normalize_strict_price_atom_stage(
            schema_name="mrf",
            price_atom_table="price_atoms",
        )


def test_strict_price_memberships_preserve_duplicate_source_occurrences():
    sql = shared_price._v3_price_membership_sql(
        qualified_price_set_atom_table="mrf.price_set_atoms",
        qualified_price_key_map="mrf.price_keys",
        qualified_atom_key_map="mrf.atom_keys",
    )
    assert "SELECT DISTINCT" not in sql

    payload = encode_price_memberships(((3, (1, 1, 257)),), 24)
    assert decode_price_memberships(payload) == {3: (1, 1, 257)}


@pytest.mark.asyncio
async def test_price_keys_use_exact_minimum_rate_order(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_validate_v3_dense_map",
        AsyncMock(return_value={"row_count": 2, "minimum_key": 0, "maximum_key": 1}),
    )

    await shared_price._create_v3_price_key_stage(
        schema_name="mrf",
        price_set_summary_table="price_set_summaries",
        price_set_summary_source_count=1,
        stage_table="price_keys",
    )

    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert 'FROM "mrf"."price_set_summaries"' in sql
    assert "MIN(" not in sql
    assert "GROUP BY" not in sql
    assert " JOIN " not in sql
    assert "ORDER BY minimum_negotiated_rate ASC NULLS LAST" in sql
    assert "price_set_global_id_128" in sql
    assert "CREATE UNIQUE INDEX" in sql
    assert "(price_key)" in sql


@pytest.mark.asyncio
async def test_omitted_summary_source_count_uses_safe_canonicalization(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(shared_price.db, "scalar", AsyncMock(return_value=False))
    monkeypatch.setattr(
        shared_price,
        "_validate_v3_dense_map",
        AsyncMock(return_value={"row_count": 2, "minimum_key": 0, "maximum_key": 1}),
    )

    await shared_price._create_v3_price_key_stage(
        schema_name="mrf",
        price_set_summary_table="price_set_summaries",
        stage_table="price_keys",
    )

    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "MIN(minimum_negotiated_rate)" in sql
    assert "MAX(minimum_negotiated_rate)" in sql
    assert "GROUP BY price_set_global_id_128" in sql


@pytest.mark.asyncio
async def test_cross_file_price_summaries_reject_conflicting_minimum(monkeypatch):
    status = AsyncMock()
    scalar = AsyncMock(return_value=True)
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(shared_price.db, "scalar", scalar)

    with pytest.raises(RuntimeError, match="conflicting minimum rates"):
        await shared_price._create_v3_price_key_stage(
            schema_name="mrf",
            price_set_summary_table="price_set_summaries",
            price_set_summary_source_count=2,
            stage_table="price_keys",
        )

    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "MIN(minimum_negotiated_rate)" in sql
    assert "MAX(minimum_negotiated_rate)" in sql
    assert "GROUP BY price_set_global_id_128" in sql
    assert "payload_conflict" in scalar.await_args.args[0]


@pytest.mark.asyncio
async def test_dense_map_uses_exact_ctas_count_and_index_bounds(monkeypatch):
    first = AsyncMock(return_value={"minimum_key": 0, "maximum_key": 4})
    monkeypatch.setattr(shared_price.db, "first", first)

    dense_stats = await shared_price._validate_v3_dense_map(
        schema_name="mrf",
        table_name="price_keys",
        id_column="price_set_global_id_128",
        key_column="price_key",
        expected_row_count=5,
    )

    assert dense_stats == {
        "row_count": 5,
        "distinct_id_count": 5,
        "distinct_key_count": 5,
        "minimum_key": 0,
        "maximum_key": 4,
    }
    validation_sql = first.await_args.args[0]
    assert "COUNT(" not in validation_sql
    assert "ORDER BY \"price_key\" ASC" in validation_sql
    assert "ORDER BY \"price_key\" DESC" in validation_sql


@pytest.mark.asyncio
async def test_lean_price_atom_skips_missing_stage(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(manifest_publish.db, "status", status)
    monkeypatch.setattr(
        manifest_publish,
        "_table_exists",
        AsyncMock(return_value=False),
    )

    manifest = await manifest_publish._rewrite_price_atom_lean_dictionary(
        schema_name="mrf",
        price_atom_table="missing_price_atoms",
        price_atom_dictionary_table="price_attributes",
    )

    assert manifest is None
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_lean_price_atom_does_not_repeat_numeric_rate_conversion(monkeypatch):
    monkeypatch.delenv(manifest_publish.PTG2_UNLOGGED_STAGE_ENV, raising=False)
    status = AsyncMock()
    monkeypatch.setattr(manifest_publish.db, "status", status)
    monkeypatch.setattr(manifest_publish.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(manifest_publish.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        manifest_publish,
        "_table_exists",
        AsyncMock(return_value=True),
    )

    await manifest_publish._rewrite_price_atom_lean_dictionary(
        schema_name="mrf",
        price_atom_table="price_atoms",
        price_atom_dictionary_table="price_attributes",
    )

    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "NULLIF(BTRIM(price_atom.negotiated_rate::text), '')::numeric" not in sql
    assert "negotiated_rate_numeric" not in sql
    assert "price_atom.negotiated_rate::text AS negotiated_rate" in sql
    assert 'ANALYZE "mrf"."price_atoms"' in sql
    assert 'CREATE UNLOGGED TABLE "mrf"."price_attributes" AS' in sql
    assert 'CREATE UNLOGGED TABLE "mrf"."price_atoms_lean_' in sql
    assert "md5(" not in sql
    assert "hashtextextended" in sql
    assert "hash_array_extended" in sql
    assert "IS NOT DISTINCT FROM" in sql
    dictionary_sql = next(
        call.args[0]
        for call in status.await_args_list
        if "WITH dictionary_source AS" in call.args[0]
    )
    assert dictionary_sql.count('FROM "mrf"."price_atoms"') == 1
    assert "UNION ALL" not in dictionary_sql
    assert "GROUP BY GROUPING SETS" in dictionary_sql
    for attribute in (
        "negotiated_type",
        "expiration_date",
        "service_code",
        "billing_class",
        "setting",
        "billing_code_modifier",
        "additional_information",
    ):
        assert f"GROUPING({attribute}) = 0" in dictionary_sql
        assert f"({attribute})" in dictionary_sql


@pytest.mark.asyncio
async def test_lean_price_atom_respects_logged_stage_override(monkeypatch):
    monkeypatch.setenv(manifest_publish.PTG2_UNLOGGED_STAGE_ENV, "false")
    status = AsyncMock()
    monkeypatch.setattr(manifest_publish.db, "status", status)
    monkeypatch.setattr(manifest_publish.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(manifest_publish.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        manifest_publish,
        "_table_exists",
        AsyncMock(return_value=True),
    )

    await manifest_publish._rewrite_price_atom_lean_dictionary(
        schema_name="mrf",
        price_atom_table="price_atoms",
        price_atom_dictionary_table="price_attributes",
    )

    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert 'CREATE TABLE "mrf"."price_attributes" AS' in sql
    assert 'CREATE TABLE "mrf"."price_atoms_lean_' in sql
    assert "CREATE UNLOGGED TABLE" not in sql


@pytest.mark.asyncio
async def test_lean_price_atom_rejects_dictionary_hash_collisions(monkeypatch):
    status = AsyncMock()
    all_rows = AsyncMock()
    monkeypatch.setattr(manifest_publish.db, "status", status)
    monkeypatch.setattr(manifest_publish.db, "scalar", AsyncMock(return_value=1))
    monkeypatch.setattr(manifest_publish.db, "all", all_rows)
    monkeypatch.setattr(
        manifest_publish,
        "_table_exists",
        AsyncMock(return_value=True),
    )

    with pytest.raises(
        RuntimeError,
        match=r"dictionary lookup key collision in mrf\.price_attributes",
    ):
        await manifest_publish._rewrite_price_atom_lean_dictionary(
            schema_name="mrf",
            price_atom_table="price_atoms",
            price_atom_dictionary_table="price_attributes",
        )

    all_rows.assert_not_awaited()
    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert 'CREATE UNLOGGED TABLE "mrf"."price_attributes" AS' in sql
    assert "CREATE UNIQUE INDEX" not in sql
    assert 'DROP TABLE "mrf"."price_atoms"' not in sql


@pytest.mark.asyncio
async def test_lean_price_atom_v1_keeps_and_indexes_dictionary(monkeypatch):
    status = AsyncMock()
    all_rows = AsyncMock()
    monkeypatch.setattr(manifest_publish.db, "status", status)
    monkeypatch.setattr(manifest_publish.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(manifest_publish.db, "all", all_rows)
    monkeypatch.setattr(
        manifest_publish,
        "_table_exists",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        manifest_publish,
        "_ptg2_manifest_price_atom_layout",
        lambda: "lean_dict_v1",
    )

    manifest = await manifest_publish._rewrite_price_atom_lean_dictionary(
        schema_name="mrf",
        price_atom_table="price_atoms",
        price_atom_dictionary_table="price_attributes",
    )

    all_rows.assert_not_awaited()
    assert manifest["price_atom_table_layout"] == "lean_dict_v1"
    assert manifest["price_atom_dictionary_table"] == "mrf.price_attributes"
    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert 'CREATE UNIQUE INDEX "price_attributes_key_idx_' in sql
    assert '(attr_kind, attr_key)' in sql
    assert status.await_args_list[-1].args[0] == 'ANALYZE "mrf"."price_attributes";'


@pytest.mark.asyncio
async def test_lean_price_atom_v2_drops_all_constant_dictionary(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(manifest_publish.db, "status", status)
    monkeypatch.setattr(manifest_publish.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(
        manifest_publish.db,
        "all",
        AsyncMock(return_value=_CONSTANT_PRICE_ATOM_ROWS),
    )
    monkeypatch.setattr(
        manifest_publish,
        "_table_exists",
        AsyncMock(return_value=True),
    )

    manifest = await manifest_publish._rewrite_price_atom_lean_dictionary(
        schema_name="mrf",
        price_atom_table="price_atoms",
        price_atom_dictionary_table="price_attributes",
    )

    assert manifest["price_atom_dictionary_table"] is None
    assert manifest["price_atom_constant_keys"] == {
        "negotiated_type_key": 0,
        "expiration_date_key": 0,
        "service_code_key": 0,
        "billing_class_key": 0,
        "setting_key": 0,
        "billing_code_modifier_key": 0,
        "additional_information_key": 0,
    }
    assert manifest["price_atom_constant_values"]["service_code"] == ["11"]
    lean_sql = next(
        call.args[0]
        for call in status.await_args_list
        if "price_atom.negotiated_rate::text AS negotiated_rate" in call.args[0]
    )
    assert " JOIN " not in lean_sql
    assert status.await_args_list[-1].args[0] == (
        'DROP TABLE IF EXISTS "mrf"."price_attributes" CASCADE;'
    )


@pytest.mark.asyncio
async def test_prepared_price_artifacts_rank_summary_in_parallel(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        AsyncMock(return_value={"rows_after": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_rewrite_price_atom_lean_dictionary",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_atom_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    prepared = await shared_price.prepare_shared_price_artifacts(
        schema_name="mrf",
        manifest_stage_table="manifest_stage",
        price_set_summary_source_count=1,
    )

    shared_price._normalize_strict_price_atom_stage.assert_not_awaited()
    assert prepared.stage_metrics["price_atom_source_mode"] == (
        "single_scanner_unique_provenance"
    )
    assert prepared.stage_metrics["normalization_seconds"] == 0.0
    assert prepared.stage_metrics["rows_before"] == 2
    assert prepared.stage_metrics["rows_after"] == 2
    assert prepared.stage_metrics["duplicate_rows_removed"] == 0
    assert prepared.stage_metrics["conflicting_ids"] == 0
    assert not any(
        "negotiated_rate_numeric" in call.args[0] for call in status.await_args_list
    )
    price_stage_call = shared_price._create_v3_price_key_stage.await_args
    assert price_stage_call.kwargs["price_set_summary_table"].startswith(
        "ptg2_manifest_stage_price_set_summary_"
    )
    assert price_stage_call.kwargs["price_set_summary_source_count"] == 1


@pytest.mark.asyncio
async def test_prepared_price_artifacts_normalize_cross_source_atoms(monkeypatch):
    normalizer = AsyncMock(
        return_value={
            "rows_before": 3,
            "rows_after": 2,
            "duplicate_rows_removed": 1,
            "conflicting_ids": 0,
        }
    )
    monkeypatch.setattr(shared_price.db, "status", AsyncMock())
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        normalizer,
    )
    monkeypatch.setattr(
        shared_price,
        "_rewrite_price_atom_lean_dictionary",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_atom_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    prepared = await shared_price.prepare_shared_price_artifacts(
        schema_name="mrf",
        manifest_stage_table="manifest_stage",
        price_set_summary_source_count=2,
    )

    normalizer.assert_awaited_once()
    assert prepared.stage_metrics["price_atom_source_mode"] == (
        "cross_file_canonicalize"
    )
    assert prepared.stage_metrics["rows_before"] == 3
    assert prepared.stage_metrics["rows_after"] == 2
    assert prepared.stage_metrics["duplicate_rows_removed"] == 1


@pytest.mark.asyncio
async def test_price_key_ready_fires_while_atom_preparation_is_still_running(
    monkeypatch,
):
    atom_release = asyncio.Event()
    ready = asyncio.Event()
    observed_keys = []

    async def rewrite_atom_stage(**_kwargs):
        await atom_release.wait()
        return {}

    def price_key_ready(prepared_key):
        observed_keys.append(prepared_key)
        ready.set()

    monkeypatch.setattr(shared_price.db, "status", AsyncMock())
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_price,
        "_rewrite_price_atom_lean_dictionary",
        rewrite_atom_stage,
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_atom_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    prepare_task = asyncio.create_task(
        shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            price_key_ready=price_key_ready,
        )
    )
    await asyncio.wait_for(ready.wait(), timeout=0.5)
    assert not prepare_task.done()
    assert len(observed_keys) == 1
    observed_key = observed_keys[0]
    assert observed_key.schema_name == "mrf"
    assert observed_key.price_set_count == 2
    assert observed_key.price_key_map.startswith("ptg2_manifest_stage_v3_price_key_")

    atom_release.set()
    prepared = await prepare_task
    shared_price._normalize_strict_price_atom_stage.assert_not_awaited()
    assert prepared.price_key_map == observed_key.price_key_map
    assert prepared.stage_metrics["price_key_build_seconds"] >= 0


@pytest.mark.asyncio
async def test_price_prepare_failure_removes_partial_key_stages(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        AsyncMock(side_effect=RuntimeError("broken stage")),
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        AsyncMock(return_value={"row_count": 2}),
    )

    with pytest.raises(RuntimeError, match="broken stage"):
        await shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=2,
        )

    cleanup_sql = status.await_args_list[-1].args[0]
    assert "v3_price_key" in cleanup_sql
    assert "v3_atom_key" in cleanup_sql
    assert "v3_price_attr" in cleanup_sql


@pytest.mark.asyncio
async def test_price_prepare_repeated_cancellation_finishes_drain_and_cleanup(
    monkeypatch,
):
    """Drain child and stage cleanup despite repeated task cancellation."""

    child_cleanup_started = asyncio.Event()
    release_child_cleanup = asyncio.Event()
    child_cleanup_finished = asyncio.Event()
    stage_cleanup_started = asyncio.Event()
    release_stage_cleanup = asyncio.Event()
    stage_cleanup_finished = asyncio.Event()

    async def delayed_price_stage(**_kwargs):
        try:
            await asyncio.Future()
        finally:
            child_cleanup_started.set()
            await release_child_cleanup.wait()
            child_cleanup_finished.set()

    async def fail_atom_stage(**_kwargs):
        raise RuntimeError("broken atom stage")

    async def status(sql):
        if "," in sql:
            stage_cleanup_started.set()
            await release_stage_cleanup.wait()
            stage_cleanup_finished.set()

    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_price_atom_stage",
        fail_atom_stage,
    )
    monkeypatch.setattr(
        shared_price,
        "_create_v3_price_key_stage",
        delayed_price_stage,
    )

    prepare_task = asyncio.create_task(
        shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=2,
        )
    )
    await child_cleanup_started.wait()
    prepare_task.cancel()
    await asyncio.sleep(0)
    prepare_task.cancel()
    await asyncio.sleep(0)
    assert not prepare_task.done()

    release_child_cleanup.set()
    await stage_cleanup_started.wait()
    assert child_cleanup_finished.is_set()
    prepare_task.cancel()
    await asyncio.sleep(0)
    prepare_task.cancel()
    await asyncio.sleep(0)
    assert not prepare_task.done()

    release_stage_cleanup.set()
    with pytest.raises(RuntimeError, match="broken atom stage"):
        await prepare_task
    assert stage_cleanup_finished.is_set()
