# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host checks for fixed-scope model COPY and restore index completion."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.models import CodeCatalog
from process import code_sets_result_archive as codes
from process import ms_drg_result_archive as drg
from process import ms_drg_result_generation as generation
from process import reference_family_archive as native


@pytest.fixture(autouse=True)
def catalog_shape_boundary(monkeypatch):
    monkeypatch.setattr(codes, "match_catalog_text_columns", AsyncMock())
    monkeypatch.setattr(drg, "match_catalog_text_columns", AsyncMock())


def _session(events):
    async def execute(statement, _parameters=None):
        events.append(str(statement))
        return SimpleNamespace(all=lambda: [])

    return SimpleNamespace(in_transaction=lambda: True, execute=execute, scalar=AsyncMock(return_value=11))


def _manifest():
    tables = [
        dict(table=name, sources=list(sources), row_count=1, row_sha256="a" * 64, schema_sha256="b" * 64)
        for name, sources in drg._SCOPES
    ]
    return dict(
        contract=drg.CONTRACT,
        origin_lineage_id=str(uuid4()),
        origin_generation=1,
        published_at="2026-01-01T00:00:00+00:00",
        include_relationships=False,
        tables=tables,
        content_sha256=drg._digest(tables),
    )


def _source():
    return codes.CodeSetsSourceGeneration(
        uuid4(),
        1,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        len(codes.SOURCES),
        "a" * 64,
        codes._schema_digest(("shape",)),
    )


def _stage():
    source = _source()
    dataset_id = uuid4()
    return codes.CodeSetsStage(
        dataset_id, codes.stage_schema(dataset_id), 11, 12, source, source.row_count, source.row_sha256
    )


def _assert_load_then_indexes(events, table_names):
    heaps = [event for event in events if event.lstrip().startswith("CREATE TABLE")]
    assert len(heaps) == len(table_names)
    assert all("PRIMARY KEY" not in heap and "LIKE " not in heap for heap in heaps)
    assert not any("INSERT INTO" in event for event in events)
    first_index = next(
        index for index, event in enumerate(events) if "CREATE INDEX" in event or "ADD PRIMARY KEY" in event
    )
    assert max(index for index, event in enumerate(events) if event.startswith("COPY ")) < first_index
    assert all(any(f'."{name}"' in event or f".{name}" in event for event in heaps) for name in table_names)


@pytest.mark.asyncio
@pytest.mark.parametrize("target_name", [CodeCatalog.__tablename__, codes.PREDECESSOR_TABLE])
async def test_code_set_clone_copies_complete_model_scope_before_all_indexes(target_name):
    events, copies = [], []
    session = _session(events)

    async def copy_rows(observed_session, query, **options):
        assert observed_session is session
        copies.append((query, options))
        events.append("COPY " + options["table_name"])
        return 7

    capability = native.ReferenceFamilySourceCopy(copy_rows, 13, 30)
    await codes._clone_slice(session, "source", "candidate", target_table=target_name, source_copy=capability)
    _assert_load_then_indexes(events, (target_name,))
    query, options = copies[0]
    assert options["columns"] == tuple(CodeCatalog.__table__.columns.keys())
    assert options["max_bytes"] == 13 and 0 < options["timeout"] <= 30
    assert query.endswith(
        "WHERE source=ANY(ARRAY[" + ",".join("'" + name + "'" for name, _ in codes.SOURCES) + "]::text[])"
    )
    assert query.startswith('SELECT "code_system","code",') and 'FROM "source"."code_catalog"' in query
    assert len([event for event in events if "CREATE INDEX" in event]) == len(CodeCatalog.__my_additional_indexes__)


@pytest.mark.asyncio
@pytest.mark.parametrize("predecessors", [False, True])
async def test_ms_drg_copies_entire_family_with_one_budget_and_deadline(monkeypatch, predecessors):
    events, copies, deadlines = [], [], []
    session = _session(events)
    manifest = _manifest()
    monkeypatch.setattr(drg, "_strict_stage", AsyncMock())
    monkeypatch.setattr(drg, "source_manifest", lambda _generation: manifest)
    monkeypatch.setattr(drg, "read_current_generation", AsyncMock(return_value={}))
    monkeypatch.setattr(drg, "verify_stage", AsyncMock())
    monkeypatch.setattr(
        drg, "_current", AsyncMock(return_value=({"local_generation": 0}, {"tables": manifest["tables"]}))
    )
    monkeypatch.setattr(drg, "_predecessor_content", AsyncMock(return_value=manifest["tables"]))

    async def copy_rows(observed_session, query, **options):
        assert observed_session is session
        copies.append((query, options))
        deadlines.append(asyncio.get_running_loop().time() + options["timeout"])
        events.append("COPY " + options["table_name"])
        return 3

    capability = native.ReferenceFamilySourceCopy(copy_rows, 9, 30)
    if predecessors:
        stage = await drg._create_stage(session, "source", uuid4(), predecessors=True)
        session.scalar.return_value = False
        await drg.prepare_predecessor(session, "source", stage, manifest, source_copy=capability)
        names = drg.PREDECESSORS
    else:
        stage, observed_manifest = await drg.prepare_source(session, "source", uuid4(), source_copy=capability)
        assert observed_manifest == manifest
        names = drg.TABLES
    _assert_load_then_indexes(events, drg.TABLES + names if predecessors else names)
    assert [options["max_bytes"] for _query, options in copies] == [9, 6, 3]
    assert max(deadlines) - min(deadlines) < 0.01
    for (query, options), model, (_, scope_sources), name in zip(copies, drg.MODELS, drg._SCOPES, names, strict=True):
        assert options["columns"] == tuple(model.__table__.columns.keys()) and options["table_name"] == name
        assert options["schema_name"] == stage["schema_name"]
        assert query.endswith(
            "WHERE source=ANY(ARRAY["
            + ",".join("'" + source_name + "'" for source_name in scope_sources)
            + "]::text[])"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("copied", [True, -1, 14])
async def test_bad_copy_accounting_never_completes_keys_or_indexes(copied):
    events = []
    with pytest.raises(native.ReferenceFamilyArchiveError, match="accounting is invalid"):
        await codes._clone_slice(
            _session(events),
            "source",
            "candidate",
            source_copy=native.ReferenceFamilySourceCopy(AsyncMock(return_value=copied), 13, 30),
        )
    assert not any("ADD PRIMARY KEY" in event or "CREATE INDEX" in event for event in events)


@pytest.mark.asyncio
async def test_incomplete_ms_drg_family_never_indexes_or_verifies(monkeypatch):
    events = []
    manifest = _manifest()
    monkeypatch.setattr(drg, "source_manifest", lambda _generation: manifest)
    monkeypatch.setattr(drg, "read_current_generation", AsyncMock(return_value={}))
    verify = AsyncMock()
    monkeypatch.setattr(drg, "_strict_stage", verify)
    copy = AsyncMock(side_effect=[3, OSError("synthetic copy failure")])
    with pytest.raises(OSError, match="synthetic copy failure"):
        await drg.prepare_source(
            _session(events), "source", uuid4(), source_copy=native.ReferenceFamilySourceCopy(copy, 9, 30)
        )
    assert copy.await_count == 2
    assert not any("ADD PRIMARY KEY" in event or "CREATE INDEX" in event for event in events)
    verify.assert_not_awaited()


@pytest.mark.asyncio
async def test_code_set_model_clone_refuses_source_column_shape_drift(monkeypatch):
    monkeypatch.setattr("process.scoped_catalog_binding.pin_catalog_source", AsyncMock(return_value=False))
    source = _source()
    authority = codes.CodeSetsGeneration(
        uuid4(), 1, source.origin_lineage_id, 1, source.published_at, 9, source.row_count, source.row_sha256
    )
    monkeypatch.setattr(codes, "read_generation", AsyncMock(return_value=authority))
    monkeypatch.setattr(
        codes,
        "scope_receipt",
        AsyncMock(
            side_effect=[
                (source.row_count, source.row_sha256, 9),
                (source.row_count, source.row_sha256, 12),
            ]
        ),
    )
    monkeypatch.setattr(codes, "_clone_slice", AsyncMock(return_value=(11, 12)))
    monkeypatch.setattr(codes, "_verify_clone", AsyncMock())
    monkeypatch.setattr(codes, "_column_signature", AsyncMock(side_effect=[("model",), ("source",)]))
    with pytest.raises(codes.CodeSetsArchiveError, match="source table shape differs"):
        await codes.prepare_source(
            _session([]), "source", uuid4(), source_copy=native.ReferenceFamilySourceCopy(AsyncMock(), 13, 30)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("archive", [codes, drg])
async def test_missing_copy_capability_fails_before_any_database_command(archive):
    events = []
    with pytest.raises(
        archive.CodeSetsArchiveError if archive is codes else archive.MsDrgArchiveError, match="COPY capability"
    ):
        await archive.prepare_source(_session(events), "source", uuid4())
    assert events == []


@pytest.mark.asyncio
async def test_ms_drg_copy_refuses_targets_outside_its_closed_model_scope():
    copy = AsyncMock()
    with pytest.raises(drg.MsDrgArchiveError, match="source scope differs"):
        await drg._copy_slice(
            _session([]),
            "source",
            "candidate",
            "unrelated",
            CodeCatalog,
            source_copy=native.ReferenceFamilySourceCopy(copy, 13, 30),
            remaining=13,
            deadline=asyncio.get_running_loop().time() + 30,
        )
    copy.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_candidate_indexes_complete_without_replacing_registered_oids(monkeypatch):
    events = []
    session = _session(events)
    monkeypatch.setattr(codes, "_verify_clone", AsyncMock())
    signature = AsyncMock(return_value=("shape",))
    monkeypatch.setattr(codes, "_column_signature", signature)
    stage = _stage()
    verify = AsyncMock()
    monkeypatch.setattr(codes, "verify_stage", verify)
    await codes.complete_restore(session, stage, predecessor_oid=13)
    assert not any("CREATE TABLE" in event or "DROP " in event for event in events)
    assert any("ADD PRIMARY KEY" in event for event in events)
    assert all(codes.PREDECESSOR_TABLE not in event for event in events)
    signature.assert_awaited_once_with(session, stage.catalog_oid, pending_primary=True)
    verify.assert_awaited_once_with(session, stage, predecessor_oid=13)


@pytest.mark.asyncio
async def test_ms_drg_restore_precreates_only_heaps_and_finishes_candidate_family(monkeypatch):
    events = []
    session = _session(events)
    manifest = _manifest()
    monkeypatch.setattr(drg, "_verify_namespace", AsyncMock())
    shape = AsyncMock(return_value="b" * 64)
    monkeypatch.setattr(drg, "_table_shape", shape)
    verify = AsyncMock()
    monkeypatch.setattr(drg, "verify_stage", verify)
    stage = await drg.precreate_restore(session, "source", uuid4(), manifest)
    assert len([event for event in events if event.lstrip().startswith("CREATE TABLE")]) == 6
    assert not any("PRIMARY KEY" in event or "CREATE INDEX" in event or "LIKE " in event for event in events)
    events.clear()
    await drg.complete_restore(session, stage, manifest)
    assert not any("CREATE TABLE" in event or "DROP " in event for event in events)
    assert len([event for event in events if "ADD PRIMARY KEY" in event]) == 3
    assert all("_predecessor" not in event for event in events)
    assert all(call.kwargs == {"pending_primary": True} for call in shape.await_args_list)
    verify.assert_awaited_once_with(session, stage, manifest, receiving=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("archive", [codes, drg])
async def test_restore_rejects_changed_registered_oid_before_any_index_work(monkeypatch, archive):
    events = []
    if archive is codes:
        monkeypatch.setattr(
            codes, "_verify_clone", AsyncMock(side_effect=codes.CodeSetsArchiveError("ownership changed"))
        )
        with pytest.raises(codes.CodeSetsArchiveError, match="ownership changed"):
            await codes.complete_restore(_session(events), _stage(), predecessor_oid=13)
    else:
        dataset_id = uuid4()
        stage_by_field = dict(
            dataset_id=str(dataset_id), schema_name=drg.stage_schema(dataset_id), schema_oid=11, relation_oids=(12,) * 6
        )
        with pytest.raises(drg.MsDrgArchiveError, match="identity changed"):
            await drg.complete_restore(_session(events), stage_by_field, _manifest())
    assert not any("CREATE " in event or "ALTER " in event for event in events)


@pytest.mark.asyncio
@pytest.mark.parametrize("archive", [codes, generation])
async def test_pending_primary_shape_does_not_weaken_ready_receipts(archive):
    columns = [(name,) for name in CodeCatalog.__table__.columns.keys()]

    def result(rows):
        return SimpleNamespace(all=lambda: rows, scalars=lambda: SimpleNamespace(all=lambda: rows))

    session = SimpleNamespace(execute=AsyncMock(side_effect=[result(columns), result([])]))
    reader = codes._column_signature if archive is codes else generation._table_shape
    options = {} if archive is codes else {"model": CodeCatalog}
    pending = await reader(session, 11, **options, pending_primary=True)
    session.execute.side_effect = [result(columns), result(["code_system", "code"])]
    assert await reader(session, 11, **options) == pending
    session.execute.side_effect = [result(columns), result([])]
    with pytest.raises(RuntimeError, match="key differs"):
        await reader(session, 11, **options)
    session.execute.side_effect = [result(columns), result(["code_system", "code"])]
    with pytest.raises(RuntimeError, match="key differs"):
        await reader(session, 11, **options, pending_primary=True)
