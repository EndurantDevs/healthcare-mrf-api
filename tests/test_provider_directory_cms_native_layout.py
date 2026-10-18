# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native dispatch and exact observed index-state rejection without projection changes."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_bootstrap_sources as bootstrap
from process import network_fhir_source_epoch as epoch
from process import network_registry_cms_prepared_pair as prepared_pair
from process import provider_directory_cms_native_layout as layout
from process.network_fhir_source_epoch import _LOOKUP_INDEXES, _PRIMARY_KEYS, _TABLES
from process.network_registry_cms_prepared_pair import RegistryCMSRawEpochAdmission
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation, RetainedRawRelation
from tests.test_provider_directory_cms_nonprofile_capacity import _producer


@pytest.mark.asyncio
async def test_only_declared_native_stages_use_native_validator(monkeypatch):
    """The same aggregate check keeps FHIR relations on the unchanged Profile validator."""
    producer = _producer()
    native = OwnedRelation("synthetic", "entity_address_unified_cms" + "a" * 20 + "_raw", 11, 100, "u")
    artifact = OwnedRelation("synthetic", "artifact_stage", 12, 100, "u")
    native_capture = AsyncMock(return_value=layout.NativeRelationLayout(11, (42,), "a" * 64))
    monkeypatch.setattr(layout, "capture_native_layout", native_capture)
    profile_capture = AsyncMock(return_value=SimpleNamespace(relation_oid=12, effective_tablespace_oids=(42,)))
    producer.fhir._provider_directory_profile_relation_storage_fingerprint = profile_capture
    producer._current_wal_bytes = AsyncMock(return_value=0)
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, (native, artifact))
    await producer._assert_physical(request, {"data_tablespace_oid": 42})
    native_capture.assert_awaited_once_with(producer.fhir, native, producer.plan.native_address_targets)
    profile_capture.assert_awaited_once_with(12, expected_persistence="u")


@pytest.mark.parametrize("field", ["indisvalid", "indisready", "indislive", "indimmediate"])
def test_nonready_native_index_is_rejected(field):
    """A declaration cannot authorize an unfinished or invalid physical index."""
    name = "entity_address_unified_cms" + "a" * 20
    index_by_field = {"indisvalid": True, "indisready": True, "indislive": True, "indimmediate": True, field: False}
    relation = OwnedRelation("synthetic", name, 11, 100, "u")
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        layout._assert_indexes([index_by_field], [], relation, layout.ENTITY_ADDRESS_RESULT_MODELS[0], None)


def _raw_adapter():
    return RegistryCMSRawEpochAdmission(None, None, "registry_cms_epoch_" + "a" * 32, None, 0, "0/0", "{}")


@pytest.mark.parametrize(
    "phases",
    [("before_insert",), ("after_insert",), ("before_index",), ("created", "created"), ("created", "after_insert")],
)
def test_raw_copy_refuses_missing_or_repeated_phases(phases):
    """A raw namespace cannot authorize growth outside the creator's exact sequence."""
    adapter = _raw_adapter()
    with pytest.raises(ValueError, match="copy_phase_invalid"):
        for phase in phases:
            adapter._phase_indexes(phase, _TABLES[0])


def test_raw_index_growth_follows_all_completed_copies():
    """Every index confirms its exact prefix after all eleven source copies finish."""
    adapter = _raw_adapter()
    for table in _TABLES:
        phases = ("created", "before_insert")
        if table != _TABLES[0]:
            phases += ("before_copy_batch", "after_copy_batch") * 3
        for phase in (*phases, "after_insert"):
            assert adapter._phase_indexes(phase, table) == ()
    with pytest.raises(ValueError, match="copy_phase_invalid"):
        adapter._phase_indexes("after_index", _TABLES[0])
    index_steps = tuple(("cms_epoch_pk_" + str(index), table) for index, (table, _) in enumerate(_PRIMARY_KEYS))
    index_steps += tuple((name, table) for name, table, _ in _LOOKUP_INDEXES)
    for name, table in index_steps:
        previous_names = adapter._phase_indexes("before_index", table)
        assert adapter._phase_indexes("after_index", table) == (*previous_names, name)
    assert adapter._indexes_by_table[_TABLES[2]] == (
        "cms_epoch_pk_2",
        "cms_epoch_entity_site",
        "cms_epoch_entity_organization",
    )
    with pytest.raises(ValueError, match="copy_phase_invalid"):
        adapter._phase_indexes("before_index", _TABLES[0])


@pytest.mark.parametrize(
    "phases",
    [
        ("after_copy_batch",),
        ("before_copy_batch", "before_copy_batch"),
        ("before_copy_batch", "after_insert"),
        ("before_copy_batch", "after_copy_batch", "after_copy_batch"),
        ("before_copy_batch", "after_copy_batch", "before_index"),
    ],
)
def test_raw_copy_batch_refuses_unpaired_or_premature_events(phases):
    """Only paired imports may close a table or advance to the next batch."""
    adapter = _raw_adapter()
    for phase in ("created", "before_insert"):
        adapter._phase_indexes(phase, _TABLES[0])
    with pytest.raises(ValueError, match="copy_phase_invalid"):
        for phase in phases:
            adapter._phase_indexes(phase, _TABLES[0])


def test_raw_copy_batch_cannot_switch_tables_mid_import():
    """An unfinished original heap cannot lend its phase authority to another."""
    adapter = _raw_adapter()
    for phase in ("created", "before_insert", "before_copy_batch"):
        adapter._phase_indexes(phase, _TABLES[0])
    with pytest.raises(ValueError, match="copy_phase_invalid"):
        adapter._phase_indexes("after_copy_batch", _TABLES[1])


async def _completed_raw_phase(monkeypatch, phase):
    from tests.test_provider_directory_cms_preparation import _admission, _database, _inputs

    admission = _admission()
    execution, fence, projection = _inputs()
    await admission.before_scratch(
        execution, fence, projection, set(admission.plan.publish_targets), frozenset(admission.plan.resource_types)
    )
    database = _database()
    fhir = SimpleNamespace(
        db=database,
        _unscoped_qt=lambda schema, name: schema + "." + name,
        _pagination_checkpoint_row_mapping=lambda row: row,
    )
    adapter, logical = _raw_adapter(), _TABLES[0]
    adapter.prepared = SimpleNamespace(fhir=fhir, nonprofile_admission=admission)
    coordinate = (adapter.schema_name, logical)
    database.relations[".".join(coordinate)] = {"oid": 42, "total_bytes": 100, "persistence": "p"}
    await admission.register_external_relation(fhir, *coordinate, 42)
    admission._logged_relations.add(coordinate)
    if phase == "after_index":
        adapter._next_table, adapter._is_index_pending = len(_TABLES), True
    else:
        adapter._copy_phase_by_table[logical] = (
            "before_copy_batch" if phase == "after_copy_batch" else "after_copy_batch"
        )
    admission.measure = AsyncMock(wraps=admission.measure)
    admission.check_phase = AsyncMock(wraps=admission.check_phase)
    bounds = AsyncMock()
    monkeypatch.setattr(prepared_pair, "_assert_retained_bounds", bounds)
    return adapter, admission, database, coordinate, bounds


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["after_copy_batch", "after_insert", "after_index"])
async def test_completed_raw_phase_checks_fresh_readiness_once(monkeypatch, phase):
    adapter, admission, _database, coordinate, bounds = await _completed_raw_phase(monkeypatch, phase)
    await epoch._admit_epoch_copy(adapter, phase, *coordinate, 42)
    admission.measure.assert_awaited_once()
    admission.check_phase.assert_awaited_once()
    request = admission.check_phase.await_args.args[0]
    assert request.phase == "readiness" and request.relations == (OwnedRelation(*coordinate, 42, 100, "p"),)
    assert len(request.raw_relations) == 1 and request.raw_relations[0].oid == 42
    bounds.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["oid", "unlogged", "custody", "extra_budget", "cancel"])
async def test_completed_raw_phase_preserves_failure_and_cancel_boundary(monkeypatch, failure):
    adapter, admission, database, coordinate, bounds = await _completed_raw_phase(monkeypatch, "after_copy_batch")
    if failure == "oid":
        database.relations[".".join(coordinate)]["oid"] = 43
    elif failure == "unlogged":
        database.relations[".".join(coordinate)]["persistence"] = "u"
    elif failure == "extra_budget":
        bounds.side_effect = RuntimeError("synthetic extra budget")
    else:
        admission.check_phase.side_effect = (
            asyncio.CancelledError() if failure == "cancel" else RuntimeError("synthetic custody")
        )
    error = asyncio.CancelledError if failure == "cancel" else epoch.FHIRSourceEpochError
    with pytest.raises(error):
        await epoch._admit_epoch_copy(adapter, "after_copy_batch", *coordinate, 42)
    admission.measure.assert_awaited_once()
    if failure == "extra_budget":
        bounds.assert_awaited_once()
    else:
        bounds.assert_not_awaited()


@pytest.mark.parametrize("rows", [0, -1, True, 1.0, None, epoch.MAX_COPY_BATCH_ROWS + 1])
def test_epoch_copy_rejects_invalid_row_limits(rows):
    """Invalid callback caps fail before any candidate creation."""
    with pytest.raises(epoch.FHIRSourceEpochError):
        epoch._epoch_copy_limits(SimpleNamespace(copy_batch_rows=rows))


@pytest.mark.parametrize("byte_limit", [0, -1, True, 1.0, None, epoch.MAX_COPY_BATCH_BYTES + 1])
def test_epoch_copy_rejects_invalid_byte_limits(byte_limit):
    """Binary spool bounds are exact positive integers with an independent hard cap."""
    with pytest.raises(epoch.FHIRSourceEpochError):
        epoch._epoch_copy_limits(SimpleNamespace(copy_batch_bytes=byte_limit))


@pytest.mark.parametrize("rows", [1, 1000, 10000])
def test_prepared_raw_batches_respect_signed_geometry(rows):
    """The adapter caps native input without changing the signed physical budgets."""
    adapter = _raw_adapter()
    adapter.prepared = SimpleNamespace(nonprofile_admission=SimpleNamespace(plan=SimpleNamespace(batch_size=rows)))
    assert epoch._epoch_copy_limits(adapter) == (min(rows, 4096), 64 * 1024**2)
    assert epoch._epoch_copy_limits(None) == (4096, 64 * 1024**2)
    assert epoch._epoch_copy_limits(lambda *_arguments: None) == (4096, 64 * 1024**2)


@pytest.mark.parametrize("rows", [0, -1, True, 1.0, None])
def test_prepared_raw_batches_refuse_invalid_signed_geometry(rows):
    """The adapter cannot coerce invalid signed row geometry into a valid hard cap."""
    adapter = _raw_adapter()
    adapter.prepared = SimpleNamespace(nonprofile_admission=SimpleNamespace(plan=SimpleNamespace(batch_size=rows)))
    with pytest.raises(epoch.FHIRSourceEpochError):
        epoch._epoch_copy_limits(adapter)


@pytest.mark.parametrize("table", _TABLES)
def test_epoch_keyset_preserves_complete_source_closure(table):
    """Every compound range retains the original edition/release and resource predicate."""
    pin = SimpleNamespace(schema_name="synthetic", dataset_id="dataset", release_id="release")
    keys = epoch._COPY_KEYS[table]
    lower_keys = tuple("lower" for _key in keys)
    upper_keys = tuple("upper" for _key in keys)
    predicate, parameters = epoch._epoch_copy_range(pin, table, lower_keys, upper_keys)
    assert "(" + epoch._scope(table, '"synthetic"') + ")" in predicate
    scope_parameters = ("dataset",)
    if table in {_TABLES[3], _TABLES[5]}:
        scope_parameters += ("release",)
    assert parameters == (*scope_parameters, *lower_keys, *upper_keys)
    assert "ROW(" in predicate and ")>ROW(" in predicate and ")<=ROW(" in predicate


@pytest.mark.asyncio
async def test_overwide_epoch_rows_shrink_then_refuse_without_copy(monkeypatch):
    """An overwide single row never reaches the target import or a row callback."""
    pin = SimpleNamespace(schema_name="synthetic", dataset_id="dataset", release_id="release")
    admission = SimpleNamespace(copy_batch_rows=4, copy_batch_bytes=64)
    bounds = AsyncMock(
        side_effect=[
            {"row_count": 2, "native_bytes": 128, "copy_key_0": "upper"},
            {"row_count": 1, "native_bytes": 128, "copy_key_0": "upper"},
        ]
    )
    copy_batch = AsyncMock(side_effect=AssertionError("overwide target COPY"))
    monkeypatch.setattr(epoch, "_epoch_batch_bounds", bounds)
    monkeypatch.setattr(epoch, "_copy_epoch_batch", copy_batch)
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch._copy_epoch_rows(None, pin, "target", _TABLES[0], 11, admission)
    assert [call.args[-1] for call in bounds.await_args_list] == [4, 1]
    copy_batch.assert_not_awaited()


@pytest.mark.asyncio
async def test_epoch_export_overflow_retries_smaller_range_then_refuses(monkeypatch):
    """Transport overflow may shrink a range, but cannot silently omit its last row."""
    pin = SimpleNamespace(schema_name="synthetic", dataset_id="dataset", release_id="release")
    admission = SimpleNamespace(copy_batch_rows=4, copy_batch_bytes=64)
    bounds = AsyncMock(
        side_effect=[
            {"row_count": 2, "native_bytes": 20, "copy_key_0": "upper"},
            {"row_count": 1, "native_bytes": 20, "copy_key_0": "upper"},
        ]
    )
    copy_batch = AsyncMock(side_effect=bootstrap._CopyBatchTooLarge)
    monkeypatch.setattr(epoch, "_epoch_batch_bounds", bounds)
    monkeypatch.setattr(epoch, "_copy_epoch_batch", copy_batch)
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch._copy_epoch_rows(None, pin, "target", _TABLES[0], 11, admission)
    assert [call.args[-1] for call in bounds.await_args_list] == [4, 1]
    assert [call.args[4][2] for call in copy_batch.await_args_list] == [2, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [RuntimeError, asyncio.CancelledError])
async def test_copy_admission_refusal_closes_spool_without_import(monkeypatch, interruption):
    """Rejected import admission closes exported spools without a target write."""
    temporary_file = bootstrap.tempfile.TemporaryFile
    spools = []

    def tracked_spool(*arguments, **options):
        copy_file = temporary_file(*arguments, **options)
        spools.append(copy_file)
        return copy_file

    @asynccontextmanager
    async def savepoint():
        """Model only the transport context; no native success is asserted."""
        yield

    async def export(_query, *parameters, **options):
        """Supply export accounting solely to reach the rejecting admission gate."""
        await options["output"](b"synthetic exported bytes")
        return "COPY 1"

    async def refuse_import(phase):
        assert phase == "before_import"
        raise interruption("synthetic import admission refusal")

    importer = AsyncMock(side_effect=AssertionError("refused target COPY"))
    driver = SimpleNamespace(transaction=savepoint, copy_from_query=export, copy_to_table=importer)
    monkeypatch.setattr(bootstrap.tempfile, "TemporaryFile", tracked_spool)
    with pytest.raises(interruption):
        await bootstrap._copy_native_batch(
            driver, "synthetic query", (), "synthetic", "heap", 1, import_admission=refuse_import
        )
    importer.assert_not_awaited()
    assert spools and all(copy_file.closed for copy_file in spools)


@pytest.mark.asyncio
async def test_raw_annotation_requires_signed_retention_policy():
    """An annotation cannot turn an existing lease without a raw policy into authority."""
    producer = _producer()
    producer._current_wal_bytes = AsyncMock(return_value=0)
    relation = OwnedRelation("registry_cms_epoch_" + "a" * 32, _TABLES[2], 11, 0, "p")
    raw_relation = RetainedRawRelation(relation.schema, relation.relation, relation.oid, "{}", ())
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, (relation,), (), (raw_relation,))
    with pytest.raises(RuntimeError, match="raw_policy_required"):
        await producer._assert_physical(request, {"data_tablespace_oid": 42})


@pytest.mark.asyncio
async def test_issued_scratch_native_capture_rechecks_proof_after_catalog(monkeypatch):
    from process import provider_directory_artifact_scratch_inputs as scratch

    fhir, relation = object(), OwnedRelation("synthetic", "pdout_" + "a" * 32, 11, 100, "p")
    proof = SimpleNamespace(target="provider_directory_location")
    capture = AsyncMock(side_effect=[proof, proof])
    monkeypatch.setattr(scratch, "scratch_relation_proof", capture)
    catalog_by_field = dict(indexes=[], attributes=[])
    observe = AsyncMock(return_value=(catalog_by_field, None))
    monkeypatch.setattr(layout, "_observe_catalog", observe)
    expected = layout.NativeRelationLayout(11, (42,), "a" * 64)
    monkeypatch.setattr(layout, "_observed_layout", lambda *_args: expected)
    assert await layout.capture_scratch_layout(fhir, relation) == expected
    assert capture.await_count == 2
    capture.side_effect = [proof, None]
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_scratch_layout(fhir, relation)
    capture.side_effect = [None]
    assert await layout.capture_scratch_layout(fhir, relation) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True])
async def test_issued_scratch_main_index_retains_native_physical_refusal(monkeypatch, batch):
    from process import provider_directory_artifact_scratch_inputs as scratch

    relation = OwnedRelation("synthetic", "pdout_" + "a" * 32, 11, 100, "p")
    index_by_field = dict(
        relation_oid=11,
        indisvalid=True,
        indisready=True,
        indislive=True,
        indimmediate=True,
        indisexclusion=False,
        indisreplident=True,
        relfilenode=1,
        index_persistence="p",
        index_schema="synthetic",
    )
    monkeypatch.setattr(
        scratch, "scratch_relation_proof", AsyncMock(return_value=SimpleNamespace(target="provider_directory_location"))
    )
    monkeypatch.setattr(
        layout, "_observe_catalog", AsyncMock(return_value=(dict(indexes=[index_by_field], attributes=[]), None))
    )

    @asynccontextmanager
    async def proofs(*_args, **_kwargs):
        yield {(relation.schema, relation.relation): (SimpleNamespace(target="provider_directory_location"), None)}

    monkeypatch.setattr(scratch, "scratch_relation_batch", proofs)
    monkeypatch.setattr(
        layout,
        "observe_native_catalog_batch",
        AsyncMock(return_value={(relation.schema, relation.relation, relation.oid): ({}, None)}),
    )
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        if batch:
            async with layout.scratch_layout_batch(SimpleNamespace(db=object()), (relation,)):
                pytest.fail("Invalid native index cannot enter the physical pass")
        else:
            await layout.capture_scratch_layout(object(), relation)


@pytest.mark.asyncio
async def test_scratch_layout_batch_fences_native_and_ordinary_work_and_clears_map(monkeypatch):
    from process import provider_directory_artifact_scratch_inputs as scratch

    relations = tuple(
        OwnedRelation("synthetic", name, oid, 100, "p") for oid, name in enumerate(("first", "second"), 1)
    )
    events, databases = [], (object(), object())

    @asynccontextmanager
    async def proofs(_fhir, actual, **_kwargs):
        assert actual == relations
        events.append("before")
        yield {
            (item.schema, item.relation): (SimpleNamespace(target="provider_directory_location"), database)
            for item, database in zip(relations, databases, strict=True)
        }
        events.append("after")

    async def capture(_fhir, relation, _proof, *, database, observed_catalog):
        assert database is databases[relation.oid - 1]
        assert observed_catalog == (dict(relation_oid=relation.oid), None)
        events.append(relation.relation)
        return layout.NativeRelationLayout(relation.oid, (42,), "a" * 64)

    monkeypatch.setattr(scratch, "scratch_relation_batch", proofs)
    monkeypatch.setattr(layout, "_capture_scratch_proof_layout", capture)

    async def census(_fhir, members, *, database):
        assert database is databases[members[0].oid - 1]
        return {(item.schema, item.relation, item.oid): (dict(relation_oid=item.oid), None) for item in members}

    monkeypatch.setattr(layout, "observe_native_catalog_batch", census)
    async with layout.scratch_layout_batch(object(), relations) as layouts:
        assert len(layouts) == 2
        events.append("ordinary")
    assert events == ["before", "first", "second", "ordinary", "after"]
    assert layouts == {}


@pytest.mark.asyncio
async def test_cms_catalog_capture_uses_one_complete_native_query(monkeypatch):
    from process.provider_directory_cms_nonprofile_observation import catalog_observation

    directory = importlib.import_module("process.provider_directory_fhir")
    relation = OwnedRelation("synthetic", "native_heap", 11, 0, "p")
    sections_by_name = dict(attributes=[], indexes=[], constraints=[], triggers=[], database_oid=7)
    database = SimpleNamespace(
        first=AsyncMock(return_value=sections_by_name), all=AsyncMock(return_value=[]), scalar=AsyncMock(return_value=7)
    )
    monkeypatch.setattr(
        directory,
        "_profile_capacity_relation_row",
        AsyncMock(
            return_value=(
                dict(relation_oid=11, schema_name=relation.schema, relation_name=relation.relation, relpersistence="p"),
                None,
            )
        ),
    )
    observed, toast = await layout._observe_catalog(catalog_observation(directory, database), relation)
    assert toast is None and observed["attributes"] == observed["indexes"] == []
    database.all.assert_not_awaited()
    database.scalar.assert_not_awaited()
    assert observed["database_oid"] == 7
    database.first.assert_awaited_once()
    assert database.first.await_args.kwargs == {"relation_oids": [11]}
    query = database.first.await_args.args[0]
    for kind in ("ATTRIBUTE", "INDEX", "CONSTRAINT", "TRIGGER"):
        assert getattr(directory, "_PROFILE_CAPACITY_" + kind + "_SQL").strip().removesuffix(";") in query


@pytest.mark.asyncio
@pytest.mark.parametrize("relation_oids", [[], [11, 12]])
async def test_catalog_bundle_keeps_empty_sections_and_native_values(relation_oids):
    rows_by_name = dict(
        attributes=[dict(attnum=1, attnotnull=True, default_expression="")],
        indexes=[],
        constraints=[],
        triggers=[],
        database_oid=7,
    )
    database = SimpleNamespace(first=AsyncMock(return_value=rows_by_name))
    backend = SimpleNamespace(db=database, _pagination_checkpoint_row_mapping=lambda row: row)
    result = await layout._native_catalog_rows(backend, relation_oids)
    assert result == (rows_by_name["attributes"], [], [], [], 7)
    assert type(result[0][0]["attnum"]) is int and type(result[0][0]["attnotnull"]) is bool
    database.first.assert_awaited_once()
    assert database.first.await_args.kwargs == {"relation_oids": relation_oids}


def test_catalog_statement_orders_fixed_deparsing_and_consumes_restoration():
    directory = importlib.import_module("process.provider_directory_fhir")
    statement = layout._native_catalog_statement(directory)
    boundaries = [
        "native_previous_path AS MATERIALIZED",
        "native_catalog_path AS MATERIALIZED",
        "native_indexes AS MATERIALIZED",
        "native_catalog AS MATERIALIZED",
        "native_restored_path AS MATERIALIZED",
        "SELECT native_catalog.* FROM native_catalog CROSS JOIN native_restored_path",
    ]
    assert [statement.index(boundary) for boundary in boundaries] == sorted(
        statement.index(boundary) for boundary in boundaries
    )
    assert "pg_catalog.current_setting('search_path') AS previous_path" in statement
    assert "pg_catalog.set_config('search_path','pg_catalog',true)" in statement
    assert "pg_catalog.set_config('search_path',previous_path,true)" in statement
    assert "FROM native_catalog CROSS JOIN native_previous_path" in statement
    assert statement.count("native_path.configured_path='pg_catalog' OFFSET 0") == 6
    assert statement.count("FROM LATERAL (") == 4
    assert "FROM native_catalog_path native_path CROSS JOIN LATERAL (" in statement


def test_sequence_statement_materializes_empty_results_before_restoring_path():
    statement = layout._evidence_sequence_statement()
    assert "native_previous_path AS MATERIALIZED" in statement
    assert "pg_catalog.current_setting('search_path') AS previous_path" in statement
    assert "pg_catalog.set_config('search_path','pg_catalog',true)" in statement
    assert "COALESCE(jsonb_agg(to_jsonb(catalog_row)), '[]'::jsonb) AS evidence_sequences" in statement
    assert "FROM native_catalog_path native_path CROSS JOIN LATERAL (" in statement
    assert "native_path.configured_path='pg_catalog' OFFSET 0" in statement
    assert "pg_catalog.set_config('search_path',previous_path,true)" in statement
    assert "FROM native_catalog CROSS JOIN native_previous_path" in statement
    assert statement.endswith("SELECT native_catalog.* FROM native_catalog CROSS JOIN native_restored_path")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [
        None,
        {},
        dict(attributes=[], indexes=[], constraints=[], database_oid=7),
        dict(attributes=[], indexes="[]", constraints=[], triggers=[], database_oid=7),
        dict(attributes=[None], indexes=[], constraints=[], triggers=[], database_oid=7),
    ],
)
async def test_catalog_bundle_refuses_changed_native_transport_shape(changed):
    database = SimpleNamespace(first=AsyncMock(return_value=changed))
    backend = SimpleNamespace(db=database, _pagination_checkpoint_row_mapping=lambda row: row)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout._native_catalog_rows(backend, [11])


@pytest.mark.asyncio
@pytest.mark.parametrize("database_oid", [None, 0, True, -1, 2**32, "7"])
async def test_catalog_bundle_refuses_malformed_database_identity(database_oid):
    database = SimpleNamespace(
        first=AsyncMock(
            return_value=dict(attributes=[], indexes=[], constraints=[], triggers=[], database_oid=database_oid)
        )
    )
    backend = SimpleNamespace(db=database, _pagination_checkpoint_row_mapping=lambda row: row)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout._native_catalog_rows(backend, [11])


@pytest.mark.asyncio
@pytest.mark.parametrize("physical_present", [None, False, 1, "true"])
async def test_catalog_bundle_preserves_missing_physical_metadata_refusal(physical_present):
    rows_by_name = dict(
        attributes=[],
        indexes=[dict(index_oid=13, physical_present=physical_present)],
        constraints=[],
        triggers=[],
        database_oid=7,
    )
    backend = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value=rows_by_name)),
        _pagination_checkpoint_row_mapping=lambda row: row,
    )
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout._native_catalog_rows(backend, [11])


@pytest.mark.asyncio
async def test_catalog_bundle_preserves_physical_fields_and_strips_transport_sentinel():
    index_by_field = dict(
        index_oid=13, index_schema="synthetic", index_persistence="p", opclass_names=["int8_ops", "text_ops"]
    )
    rows_by_name = dict(
        attributes=[],
        indexes=[dict(index_by_field, physical_present=True)],
        constraints=[],
        triggers=[],
        database_oid=7,
    )
    database = SimpleNamespace(first=AsyncMock(return_value=rows_by_name))
    backend = SimpleNamespace(db=database, _pagination_checkpoint_row_mapping=lambda row: row)
    observed = await layout._native_catalog_rows(backend, [11])
    assert observed[1] == [index_by_field] and observed[4] == 7
    assert rows_by_name["indexes"][0]["physical_present"] is True
    query = database.first.await_args.args[0]
    assert "LEFT JOIN pg_class relation ON relation.oid=catalog_index.index_oid" in query
    assert "LEFT JOIN pg_index index_row ON index_row.indexrelid=relation.oid" in query
    assert "WITH ORDINALITY" in query and "ORDER BY item.n" in query


@pytest.mark.asyncio
async def test_relation_safety_refusal_precedes_native_catalog_query():
    directory = importlib.import_module("process.provider_directory_fhir")
    database = SimpleNamespace(first=AsyncMock(return_value=dict(relkind="v", toast_oid=0)))
    from process.provider_directory_cms_nonprofile_observation import catalog_observation

    relation = OwnedRelation("synthetic", "unsafe", 11, 0, "p")
    with pytest.raises(directory.ProviderDirectoryArtifactBuildStale, match="storage_shape_unsupported"):
        await layout._observe_catalog(catalog_observation(directory, database), relation)
    database.first.assert_awaited_once_with(directory._PROFILE_CAPACITY_RELATION_SQL, relation_oid=11)
