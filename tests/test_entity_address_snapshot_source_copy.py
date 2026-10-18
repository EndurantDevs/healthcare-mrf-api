# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded source cloning preserves native legacy shape and one pinned lifecycle."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import entity_address_snapshot_source as source
from process import network_cms_registry_source_pair as pair

DATASET = UUID("a2bc9d01-7c83-4a63-b0ab-78d2fcc9fdce")
MODELS = (source.entity_address_unified.EntityAddressUnified, *source.entity_address_unified.SUPPORT_TABLE_MODELS)


def _source_capture():
    return source.EntityAddressArchiveSourceCapture(
        source.LEGACY_CONTRACT, "mrf", source.entity_address_archive_relations(), "1-2-3"
    )


def _index(**changes):
    index_by_field = dict(
        index_name="entity_pk",
        source_prefix="CREATE UNIQUE INDEX entity_pk ON mrf.entity_address_unified",
        definition="CREATE UNIQUE INDEX entity_pk ON mrf.entity_address_unified USING btree (entity_type, entity_id, location_key)",
        indisunique=True,
        constraint_name="entity_pk",
        constraint_type="p",
        condeferrable=True,
        condeferred=True,
    )
    index_by_field.update(changes)
    return index_by_field


def test_native_index_relocation_preserves_key_expression_options_and_constraint_timing():
    index = _index()
    assert source._legacy_index_statements(index, schema="owned", table="entity_address_unified") == [
        'CREATE UNIQUE INDEX "entity_pk" ON "owned"."entity_address_unified" USING btree (entity_type, entity_id, location_key)',
        'ALTER TABLE "owned"."entity_address_unified" ADD CONSTRAINT "entity_pk" PRIMARY KEY USING INDEX "entity_pk" DEFERRABLE INITIALLY DEFERRED',
    ]
    expression = " USING gin (taxonomy_array public.gin__int_ops) WITH (fastupdate=off) WHERE (type = 'primary'::text)"
    index = _index(
        index_name="source_taxonomy",
        indisunique=False,
        constraint_name=None,
        source_prefix="CREATE INDEX source_taxonomy ON mrf.entity_address_unified",
    )
    index["definition"] = index["source_prefix"] + expression
    assert source._legacy_index_statements(index, schema="owned", table="entity_address_unified") == [
        'CREATE INDEX "source_taxonomy" ON "owned"."entity_address_unified"' + expression
    ]


@pytest.mark.parametrize(
    "predicate", [predicate for names, predicate in source._SOURCE_INDEX_PREDICATES.items() for _name in names]
)
def test_live_predicates_use_shared_native_restoration(predicate):
    """Promoted index names retain exact source predicates without changing captured recipes."""
    prefix = "CREATE INDEX promoted_partial_index ON mrf.entity_address_unified"
    index_by_field = _index(
        index_name="promoted_partial_index",
        source_prefix=prefix,
        definition=prefix + " USING btree (npi) WHERE " + predicate,
        indisunique=False,
        constraint_name=None,
    )
    original = index_by_field.copy()
    raw = source._legacy_index_statements(index_by_field, schema="owned", table="entity_address_unified")
    restored = source._source_index_statements(index_by_field, schema="owned", table="entity_address_unified")
    assert restored == [raw[0].removesuffix(predicate) + predicate.replace("])::text[]", "])")]
    assert index_by_field == original


@pytest.mark.parametrize(
    "changes",
    [
        {"index_name": 'foreign"; DROP TABLE x'},
        {"definition": "CREATE INDEX other ON foreign.table USING btree (x)"},
        {"definition": "CREATE UNIQUE INDEX entity_pk ON mrf.entity_address_unifiedx USING btree (x)"},
        {"constraint_name": "unsafe name"},
        {"constraint_type": "x"},
        {"condeferrable": False},
        {"definition": "x" * 131073},
    ],
)
def test_index_header_or_constraint_mismatch_never_generates_ddl(changes):
    with pytest.raises(RuntimeError, match="definition|constraint"):
        source._legacy_index_statements(_index(**changes), schema="owned", table="entity_address_unified")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_bytes": 0, "timeout": 10},
        {"max_bytes": True, "timeout": 10},
        {"max_bytes": 1024, "timeout": float("inf")},
        {"max_bytes": 1024, "timeout": 0},
    ],
)
def test_native_copy_factory_rejects_missing_or_unbounded_policy(kwargs):
    with pytest.raises(ValueError):
        source.native_entity_address_source_copy(**kwargs)


def test_native_copy_factory_uses_existing_transaction_owned_native_protocol():
    copy = source.native_entity_address_source_copy(max_bytes=2048, timeout=12)
    assert copy.copy_rows is source.family_archive.native_copy_projection
    assert (copy.max_bytes, copy.timeout) == (2048, 12)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["copy", "custody", "deadline", "expired", "family"])
async def test_clone_refuses_incomplete_capability_before_any_ddl(missing):
    capture = source.EntityAddressArchiveSourceCapture(
        source.LEGACY_CONTRACT, "mrf", source.entity_address_archive_relations(), "00000003-0000001B-1"
    )
    options_by_name = dict(
        source_capture=capture,
        stage_schema="owned",
        source_copy=source.native_entity_address_source_copy(max_bytes=1024, timeout=10),
        copy_deadline=asyncio.get_running_loop().time() + 10,
        on_precreated=AsyncMock(),
    )
    if missing == "copy":
        options_by_name["source_copy"] = None
    if missing == "custody":
        options_by_name["on_precreated"] = None
    if missing == "deadline":
        options_by_name["copy_deadline"] = float("nan")
    if missing == "expired":
        options_by_name["copy_deadline"] = asyncio.get_running_loop().time() - 1
    if missing == "family":
        options_by_name["source_capture"] = source.EntityAddressArchiveSourceCapture(
            source.LEGACY_CONTRACT, "mrf", (), capture.postgres_snapshot
        )
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises((ValueError, TimeoutError)):
        await source._clone_entity_address_archive_source(session, **options_by_name)
    session.execute.assert_not_awaited()


async def _source_indexes(_session, entry):
    name = entry["model"].__tablename__
    return [
        _index(
            index_name=name + "_pkey",
            source_prefix=f"CREATE UNIQUE INDEX {name}_pkey ON mrf.{name}",
            definition=f"CREATE UNIQUE INDEX {name}_pkey ON mrf.{name} USING btree (location_key)",
            constraint_name=name + "_pkey",
            condeferrable=False,
            condeferred=False,
        )
    ]


def _break_last_index(monkeypatch):
    original = source._legacy_index_statements

    def broken(index, **kwargs):
        if index["index_name"] == MODELS[-1].__tablename__ + "_pkey":
            raise RuntimeError("synthetic late index failure")
        return original(index, **kwargs)

    monkeypatch.setattr(source, "_legacy_index_statements", broken)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "overflow", "different", "index"])
async def test_all_heaps_and_custody_precede_copy_all_indexes_precede_set_validation(monkeypatch, failure):
    """Observe the complete clone order and refuse late copy/index/set failures."""
    events = []

    async def execute(statement):
        events.append(("sql", str(statement)))

    session = SimpleNamespace(execute=execute)
    source_relations = [{"model": model, "source_ref": f'"mrf"."{model.__tablename__}"'} for model in MODELS]
    monkeypatch.setattr(source, "_legacy_source_catalog", AsyncMock(return_value=source_relations))
    monkeypatch.setattr(source, "_require_legacy_target_custody", AsyncMock())

    monkeypatch.setattr(source, "_legacy_source_indexes", _source_indexes)

    async def custody(_session):
        assert len([event for event in events if event[0] == "sql" and event[1].startswith("CREATE TABLE")]) == 7
        events.append(("custody", None))

    budgets = []

    async def copy(_session, query, **options):
        assert events.index(("custody", None)) < len(events)
        assert query.startswith("SELECT ") and "mrf." in query
        assert tuple(options["columns"]) == tuple(MODELS[len(budgets)].__table__.columns.keys())
        assert 0 < options["timeout"] <= 5
        budgets.append(options["max_bytes"])
        events.append(("copy", options["table_name"]))
        if failure == "overflow" and len(budgets) == 7:
            return 999
        return 3

    async def is_equal(_session, model, **kwargs):
        assert len(budgets) == 7
        assert len([event for event in events if event[0] == "sql" and event[1].startswith("CREATE UNIQUE INDEX")]) == 7
        events.append(("compare", model.__tablename__))
        return not (failure == "different" and model is MODELS[-1])

    monkeypatch.setattr(source.family_archive, "_is_model_table_equal", is_equal)
    if failure == "index":
        _break_last_index(monkeypatch)
    capture = _source_capture()
    work = source._clone_entity_address_archive_source(
        session,
        source_capture=capture,
        stage_schema="owned",
        source_copy=source.EntityAddressSourceCopy(copy, 64, 5),
        copy_deadline=asyncio.get_running_loop().time() + 5,
        on_precreated=custody,
    )
    if failure:
        with pytest.raises(RuntimeError):
            await work
    else:
        await work
        assert len([event for event in events if event[0] == "compare"]) == 7
    assert budgets == [64, 61, 58, 55, 52, 49, 46]
    assert not any(event[0] == "sql" and "INSERT INTO" in event[1] for event in events)
    if failure in ("overflow", "index"):
        assert not any(event[0] == "compare" for event in events)


@pytest.mark.asyncio
async def test_source_deadline_covers_capture_before_any_candidate_or_dump(monkeypatch):
    started = asyncio.Event()

    async def capture(*_args, **_kwargs):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(source, "capture_entity_address_archive_source", capture)

    @asynccontextmanager
    async def scope():
        yield SimpleNamespace(begin=scope)

    clone, dump = AsyncMock(), AsyncMock()
    monkeypatch.setattr(source, "_clone_entity_address_archive_source", clone)
    with pytest.raises(TimeoutError):
        await source.export_entity_address_archive_stage(
            scope,
            schema_name="mrf",
            dataset_id=DATASET,
            archive_copy=dump,
            stage_created=AsyncMock(),
            source_copy=source.native_entity_address_source_copy(max_bytes=1024, timeout=0.01),
        )
    assert started.is_set()
    clone.assert_not_awaited()
    dump.assert_not_awaited()


@pytest.mark.asyncio
async def test_receipt_wrapper_forwards_copy_and_drains_exact_owned_cleanup_on_cancel(monkeypatch):
    native_copy = source.native_entity_address_source_copy(max_bytes=1024, timeout=5)
    owner = object()
    monkeypatch.setattr(
        source.importlib,
        "import_module",
        lambda _: SimpleNamespace(capture_created_entity_address_archive_stage=AsyncMock(return_value=owner)),
    )

    async def stage(_sessions, **options):
        assert options["source_copy"] is native_copy
        await options["stage_created"](object())
        raise asyncio.CancelledError

    monkeypatch.setattr(source, "export_entity_address_archive_stage", stage)
    cleanup = AsyncMock()
    monkeypatch.setattr(source, "_cleanup_owned_archive_stage", cleanup)
    with pytest.raises(asyncio.CancelledError):
        await source.export_entity_address_archive_with_receipt(
            object(),
            schema_name="mrf",
            dataset_id=DATASET,
            queued_serving_capture={},
            archive_copy=AsyncMock(),
            source_copy=native_copy,
        )
    assert cleanup.await_args.args[2] is owner


@pytest.mark.asyncio
async def test_cms_public_boundary_refuses_missing_capability_before_session_access():
    factory = Mock()
    with pytest.raises(ValueError, match="bounded native COPY"):
        await pair.capture_registry_cms_source_pair(
            factory, None, capture_id=DATASET, owner_role="owner", runtime_roles=("reader",), source_copy=None
        )
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_cms_pair_passes_explicit_capability_and_retains_original_receipt_callbacks(monkeypatch):
    """Only orchestration is faked here; native source/pair authority stays a separate proof."""

    @asynccontextmanager
    async def session_factory():
        yield SimpleNamespace(begin=session_factory, execute=AsyncMock())

    proof = SimpleNamespace(source_pin=SimpleNamespace(schema_name="mrf"))
    owner, captured, frozen_pair = object(), object(), object()
    copy = source.native_entity_address_source_copy(max_bytes=4096, timeout=10)
    monkeypatch.setattr(pair, "_retained_pair", AsyncMock(return_value=None))
    monkeypatch.setattr(pair, "_publication", AsyncMock(return_value={"address": "exact"}))
    monkeypatch.setattr(pair.serving, "capture_native_dependencies", AsyncMock(return_value={"address": "exact"}))
    monkeypatch.setattr(pair, "_address_oid_vector", Mock(return_value=(1,)))
    monkeypatch.setattr(pair, "capture_registry_cms_address_copy_source", AsyncMock(return_value=captured))
    monkeypatch.setattr(pair, "capture_created_entity_address_archive_stage", AsyncMock(return_value=owner))
    stamp, freeze = AsyncMock(), AsyncMock(return_value=frozen_pair)
    monkeypatch.setattr(pair, "_stamp_stage", stamp)
    monkeypatch.setattr(pair, "_freeze_pair", freeze)

    source_capture = object()
    capture = AsyncMock(return_value=source_capture)
    monkeypatch.setattr(pair, "capture_entity_address_archive_source", capture)

    async def clone(session, **options):
        assert options["source_copy"] is copy
        assert options["source_capture"] is source_capture
        assert options["stage_schema"] == pair.entity_address_archive_stage_schema(DATASET)
        assert options["copy_deadline"] > asyncio.get_running_loop().time()
        await options["on_precreated"](session)

    monkeypatch.setattr(pair, "_clone_entity_address_archive_source", clone)
    assert await pair._capture_pair(session_factory, proof, DATASET, "owner", ("reader",), copy) is frozen_pair
    capture.assert_awaited_once_with(capture.await_args.args[0], schema_name="mrf")
    assert stamp.await_args.args[-1] is owner
    assert freeze.await_args.args[-3:] == (owner, captured, {"address": "exact"})


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["foreign_owner", "foreign_schema_owner", "column_writer", "schema_writer"])
async def test_target_custody_refuses_foreign_writer_before_payload(monkeypatch, damage):
    relation_by_field = {"owner_oid": 10, "schema_owner_oid": 10, "schema_oid": 20}
    if damage == "foreign_owner":
        relation_by_field["owner_oid"] = 11
    if damage == "foreign_schema_owner":
        relation_by_field["schema_owner_oid"] = 11
    monkeypatch.setattr(source, "_legacy_source_catalog", AsyncMock(return_value=[relation_by_field]))
    guard = AsyncMock(side_effect=RuntimeError("column writer") if damage == "column_writer" else None)
    monkeypatch.setattr(source.family_archive, "_require_selected_mrf_catalog", guard)
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[10, damage != "schema_writer"]))
    capture = _source_capture()
    with pytest.raises(RuntimeError, match="owner|writer"):
        await source._require_legacy_target_custody(session, capture, "owned")
    if damage.startswith("foreign"):
        guard.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("close_error", [None, TimeoutError("bounded close failure")])
async def test_interrupted_native_copy_drains_cancel_before_transport_fallback(close_error):
    events = []

    async def interrupted(*_args, **_kwargs):
        raise asyncio.CancelledError("original cancel")

    async def close(*, timeout):
        assert timeout == 5
        events.append("cancel acknowledged")
        if close_error:
            raise close_error

    driver = SimpleNamespace(
        copy_from_query=interrupted,
        copy_to_table=AsyncMock(),
        close=close,
        terminate=lambda: events.append("fallback terminate"),
    )
    with pytest.raises(asyncio.CancelledError, match="original cancel"):
        await source.family_archive._copy_native_projection(driver, "SELECT 1", "owned", "rows", ("id",), 10, 1)
    assert events == (["cancel acknowledged"] if close_error is None else ["cancel acknowledged", "fallback terminate"])
    driver.copy_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_cancellation_does_not_abandon_native_cancel_handshake():
    copying, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def copy(*_args, **_kwargs):
        copying.set()
        await asyncio.Future()

    async def close(*, timeout):
        closing.set()
        await release.wait()

    driver = SimpleNamespace(copy_from_query=copy, close=close, terminate=Mock())
    task = asyncio.create_task(
        source.family_archive._copy_native_projection(driver, "SELECT 1", "owned", "rows", ("id",), 10, 30)
    )
    await copying.wait()
    task.cancel()
    await closing.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    driver.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_native_interruption_invalidates_sqlalchemy_owner_after_cancel_handshake():
    events = []

    async def interrupt(*_args, **_kwargs):
        raise asyncio.CancelledError("original cancel")

    async def close(*, timeout):
        events.append("closed")

    async def invalidate():
        events.append("owner invalidated")

    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        copy_from_query=interrupt,
        copy_to_table=AsyncMock(),
        close=close,
        terminate=Mock(),
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        invalidate=invalidate,
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        ),
    )
    with pytest.raises(asyncio.CancelledError, match="original cancel"):
        await source.family_archive.native_copy_projection(
            session, "SELECT 1", schema_name="owned", table_name="rows", columns=("id",), max_bytes=10, timeout=1
        )
    assert events == ["closed", "owner invalidated"]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["unknown", "ambiguous", "callback", "bundle"])
async def test_stage_hook_compatibility_is_closed_before_io(invalid):
    factory = Mock()
    callback_by_name = dict(archive_copy=AsyncMock(), stage_created=AsyncMock())
    if invalid == "unknown":
        callback_by_name["other_callback"] = AsyncMock()
    if invalid == "ambiguous":
        callback_by_name["hooks"] = source.EntityAddressArchiveStageHooks(AsyncMock(), AsyncMock())
    if invalid == "callback":
        callback_by_name["source_captured"] = True
    if invalid == "bundle":
        callback_by_name = {"hooks": object()}
    with pytest.raises((ValueError, TypeError)):
        await source.export_entity_address_archive_stage(
            factory,
            schema_name="mrf",
            dataset_id=DATASET,
            source_copy=source.native_entity_address_source_copy(max_bytes=1024, timeout=5),
            **callback_by_name,
        )
    factory.assert_not_called()
