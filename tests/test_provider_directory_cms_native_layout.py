# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native dispatch and exact observed index-state rejection without projection changes."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_bootstrap_sources as bootstrap
from process import network_fhir_source_epoch as epoch
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
