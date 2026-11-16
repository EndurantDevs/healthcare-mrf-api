# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_batch as batch


def test_cache_joins_match_exact_aggregate_inputs():
    token = batch._TABLE.set('"data"."address_cache"')
    try:
        joined = batch.join_sql("aggregated")
        assert "SELECT lookup_key,native_fields" in joined
        assert all(f"(aggregated.{field})::text" in joined for field in batch.stage.ADDRESS_FIELDS)
        label = batch.formatted_sql("data")
        assert "native_fields IS NOT NULL" in label
        assert "ELSE data.addr_formatted_address_v2(" in label
        assert "COALESCE" not in label
        assert "FROM aggregated" in batch.direct_materialize_select_sql("data")
        assert joined in batch.raw_materialize_select_address_sql("data", "")
    finally:
        batch._TABLE.reset(token)


def test_missing_cache_and_unavailable_keys_preserve_sql():
    assert batch.join_sql("raw") == ""
    assert batch.field_sql("address_key", "original") == "original"
    assert batch.identity_sql("data", "raw").startswith("data.addr_identity_key_v1(")
    token = batch._TABLE.set('"data"."address_cache"')
    try:
        assert batch.key_sql("original", "raw.address_source", False) == "original"
        key = batch.key_sql("original", "raw.address_source", True)
        assert key.startswith("CASE WHEN raw.address_source='ptg' THEN NULL::uuid")
        assert "native_fields->>'address_key'" in key
        assert "native_fields->>'identity_key'" in batch.identity_sql("data", "raw")
    finally:
        batch._TABLE.reset(token)


@pytest.mark.asyncio
async def test_scope_retires_cache_and_resets_context(monkeypatch):
    events = []

    @asynccontextmanager
    async def staged(database, schema, name, source, **options):
        assert options == {"committed": True}
        events.append((schema, name, source))
        try:
            yield '"data"."raw_batch"'
        finally:
            events.append("retired")

    monkeypatch.setattr(batch.stage, "staged_address_fields", staged)
    monkeypatch.setattr(batch, "_can_stage", lambda: True)
    sql = SimpleNamespace(
        db=object(),
        row_preparation=SimpleNamespace(native_preparation_module=lambda: object()),
        _archived_identifier=lambda name, suffix: name + suffix,
    )

    @batch.scope
    async def operation():
        await batch.prepare_raw(sql, "data", "raw")
        assert batch._TABLE.get() == '"data"."raw_batch"'
        raise RuntimeError("consumer failure")

    with pytest.raises(RuntimeError, match="consumer failure"):
        await operation()
    assert events[0][1].startswith("raw_address_batch_")
    assert events[-1] == "retired"
    assert batch._TABLE.get() is None and batch._SCOPE.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,allowed", [(False, True), (True, False)])
async def test_unsupported_preparation_keeps_original_scope(monkeypatch, enabled, allowed):
    monkeypatch.setattr(batch, "_can_stage", lambda: allowed)
    staged = AsyncMock()
    monkeypatch.setattr(batch.stage, "staged_address_fields", staged)
    async with batch.prepared_source(None, "data", "SELECT inputs", enabled=enabled):
        assert batch._TABLE.get() is None
    staged.assert_not_called()


@pytest.mark.asyncio
async def test_direct_cache_uses_prepared_six_field_projection(monkeypatch):
    @asynccontextmanager
    async def staged(database, schema, name, source, **options):
        assert options == {"temporary": True}
        assert name.startswith("address_rows_input_")
        assert "FROM (SELECT prepared_inputs) b" in source
        assert all(f"(b.{field})::text AS {field}" in source for field in batch.stage.ADDRESS_FIELDS)
        yield '"pg_temp"."cache"'

    monkeypatch.setattr(batch, "_can_stage", lambda: True)
    monkeypatch.setattr(batch.stage, "staged_address_fields", staged)
    async with batch.prepared_source(SimpleNamespace(db=object()), "data", "SELECT prepared_inputs", enabled=True):
        assert batch._TABLE.get() == '"pg_temp"."cache"'
    assert batch._TABLE.get() is None


@pytest.mark.asyncio
async def test_failed_shard_drains_siblings_before_return():
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def fail():
        await started.wait()
        raise RuntimeError("shard failure")

    async def sibling():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            stopped.set()

    token = batch._TABLE.set('"data"."cache"')
    try:
        with pytest.raises(RuntimeError, match="shard failure"):
            await batch.gather(fail(), sibling())
        assert stopped.is_set()
    finally:
        batch._TABLE.reset(token)


@pytest.mark.asyncio
async def test_without_cache_existing_gather_contract_is_used(monkeypatch):
    gather = AsyncMock(return_value=["complete"])
    monkeypatch.setattr(batch.admission, "gather", gather)
    marker = object()
    assert await batch.gather(marker) == ["complete"]
    gather.assert_awaited_once_with(marker, return_exceptions=False)


@pytest.mark.asyncio
async def test_admitted_or_source_profile_does_not_add_scratch():
    token = batch.admission._ADMISSION.set(object())
    try:
        assert not batch._can_stage()
    finally:
        batch.admission._ADMISSION.reset(token)
    token = batch.source._FACTORY.set(object())
    try:
        assert not batch._can_stage()
    finally:
        batch.source._FACTORY.reset(token)


@pytest.mark.asyncio
async def test_raw_preparation_without_scope_is_a_noop():
    await batch.prepare_raw(None, "data", "raw")


@pytest.mark.asyncio
async def test_incompatible_raw_case_disables_previous_cache_until_scope_cleanup(monkeypatch):
    """A later source collation must not reuse an earlier native cache."""
    events = []

    @asynccontextmanager
    async def staged(*args, **options):
        events.append("created")
        try:
            yield '"data"."first_cache"'
        finally:
            events.append("retired")

    probe = AsyncMock(return_value=False)
    monkeypatch.setattr(batch, "_can_stage", lambda: True)
    monkeypatch.setattr(batch.stage, "staged_address_fields", staged)
    monkeypatch.setattr(batch.stage, "_has_supported_case", probe)
    sql = SimpleNamespace(
        db=object(),
        row_preparation=SimpleNamespace(native_preparation_module=lambda: object()),
        _archived_identifier=lambda name, suffix: name + suffix,
    )

    @batch.scope
    async def prepare():
        await batch.prepare_inputs(sql, "data", "raw", "SELECT first_inputs")
        assert batch._TABLE.get() == '"data"."first_cache"'
        await batch.prepare_inputs(sql, "data", "raw", "SELECT incompatible_inputs")
        probe.assert_awaited_once_with(sql.db, "SELECT incompatible_inputs", {})
        assert batch._TABLE.get() is None
        assert batch.join_sql("raw") == ""
        assert batch.field_sql("address_key", "original_key") == "original_key"
        assert events == ["created"]

    await prepare()
    assert events == ["created", "retired"]
    assert batch._SCOPE.get() is None and batch._TABLE.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped", [False, True])
async def test_partial_without_ordinary_scope_leaves_projection_unopened(monkeypatch, scoped):
    """A missing scope or admitted profile keeps its existing preparation path."""
    projection = AsyncMock()
    monkeypatch.setattr(batch.projection, "scoped_projection", projection)
    monkeypatch.setattr(batch, "_can_stage", lambda: False)

    async def prepare():
        await batch.prepare_partial(None, "data", "partial", {}, source_ids=["selected"], run_id="run")

    await (batch.scope(prepare)() if scoped else prepare())
    projection.assert_not_called()
    assert batch._TABLE.get() is None and batch._LIVE.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("unsupported", ["live", "current"])
async def test_partial_incompatible_case_falls_back_and_retires_inputs(monkeypatch, unsupported):
    """Either input's collation can decline native keys without leaking live scratch."""
    events = []

    @asynccontextmanager
    async def transaction():
        yield

    async def status(statement):
        events.append(statement)

    @asynccontextmanager
    async def projected(*args, **options):
        try:
            yield '"data"."projection"'
        finally:
            events.append("projection retired")

    probe = AsyncMock(side_effect=[False] if unsupported == "live" else [True, False])
    staged = AsyncMock()
    monkeypatch.setattr(batch, "_can_stage", lambda: True)
    monkeypatch.setattr(batch.projection, "scoped_projection", projected)
    monkeypatch.setattr(batch.projection, "group_input_sql", lambda *args: "SELECT current_inputs")
    monkeypatch.setattr(batch.stage, "_has_supported_case", probe)
    monkeypatch.setattr(batch.stage, "staged_address_fields", staged)
    sql = SimpleNamespace(
        db=SimpleNamespace(transaction=transaction, status=status),
        row_preparation=SimpleNamespace(native_preparation_module=lambda: object()),
        EntityAddressUnified=SimpleNamespace(__main_table__="entity_address_unified"),
        _archived_identifier=lambda name, suffix: name + suffix,
        _provider_directory_live_source_filter_sql=lambda *args, **options: "AND live.entity_id='selected'",
    )

    @batch.scope
    async def prepare():
        await batch.prepare_partial(sql, "data", "partial", {}, source_ids=["selected"], run_id="run")
        assert batch._TABLE.get() is None and batch._LIVE.get() is not None
        assert batch.field_sql("address_key", "sql_key") == "sql_key"
        assert probe.await_count == (1 if unsupported == "live" else 2)
        assert batch._LIVE.get() in probe.await_args_list[0].args[1]
        if unsupported == "current":
            assert probe.await_args_list[1].args[1] == "SELECT current_inputs"
        raise RuntimeError("consumer failure after fallback")

    with pytest.raises(RuntimeError, match="consumer failure after fallback"):
        await prepare()
    staged.assert_not_called()
    created = next(statement.split()[3] for statement in events if statement.startswith("CREATE UNLOGGED"))
    assert events[-2:] == [f"DROP TABLE IF EXISTS {created}", "projection retired"]
    assert batch._TABLE.get() is None and batch._LIVE.get() is None and batch._SCOPE.get() is None


@pytest.mark.parametrize("sources", [[], ["SELECT unrelated_inputs"]])
def test_partial_groups_require_an_available_provider_directory_source(sources):
    """Do not construct affected groups from unrelated sources alone."""
    native = importlib.import_module("process.entity_address_unified")
    with pytest.raises(RuntimeError, match="requires available Provider Directory source tables"):
        native._prepare_partial_affected_groups_sql("data", "affected", sources, source_ids=["selected"])


def test_current_group_wrapper_preserves_entity_and_address_admission():
    """The ordinary wrapper keeps SQL fallback and filters incomplete source rows."""
    native = importlib.import_module("process.entity_address_unified")
    selected = native._provider_directory_current_group_select_sql("SELECT provider_inputs")
    assert "FROM (SELECT provider_inputs) src" in selected
    assert "src.entity_type IS NOT NULL AND src.entity_id IS NOT NULL" in selected
    assert "src.address_key IS NOT NULL OR NULLIF(TRIM(src.first_line),'') IS NOT NULL" in selected
    assert "src.entity_id ~ '^[0-9]+$'" in selected
    assert "native_fields" not in selected


@pytest.mark.asyncio
@pytest.mark.parametrize("active", ["projection", "live"])
async def test_projection_only_gather_preserves_exception_results(active):
    """Retain alias/support exception collection without requiring a native table."""
    token = (batch.projection._ACTIVE if active == "projection" else batch._LIVE).set(object())

    async def fail():
        """Provide an ordinary shard exception for explicit outcome collection."""
        raise ValueError("shard failure")

    async def succeed():
        """Keep the successful sibling result in its original position."""
        return "complete"

    try:
        results = await batch.gather(fail(), succeed(), return_exceptions=True)
        assert isinstance(results[0], ValueError) and results[1] == "complete"
    finally:
        (batch.projection._ACTIVE if active == "projection" else batch._LIVE).reset(token)
