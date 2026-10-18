# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Receipt locks and issued bridge state; no mocked native authority claim."""

import asyncio
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from weakref import ref

import pytest

from process import entity_address_snapshot_receipt as receipts
from process import network_registry_cms_prepared_address as address
from process import network_registry_cms_prepared_address_pin as pins
from process import provider_directory_cms_native_layout as layouts
from process.provider_directory_cms_preparation import NonprofileAdmission, RetainedNativeRelation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "readonly,isolation,mode",
    [
        ("off", "read committed", "SHARE"),
        ("on", "repeatable read", "ACCESS SHARE"),
        ("on", "serializable", "ACCESS SHARE"),
    ],
)
async def test_receipt_lock_derives_actual_transaction(readonly, isolation, mode):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[readonly, isolation]), execute=AsyncMock())
    await receipts._lock_relation_family(session, "sample", ("b", "a"))
    assert str(session.execute.call_args.args[0]) == f'LOCK TABLE "sample"."a", "sample"."b" IN {mode} MODE'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "readonly,isolation", [("on", "read committed"), ("on", "read uncommitted"), (None, "repeatable read")]
)
async def test_unstable_or_unknown_reader_refuses_before_locks(readonly, isolation):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[readonly, isolation]), execute=AsyncMock())
    with pytest.raises(receipts.EntityAddressArchiveReceiptError):
        await receipts._lock_relation_family(session, "sample", ("a",))
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_unissued_input_cannot_open_source_scope_or_target():
    with pytest.raises(ValueError, match="pin_invalid"):
        async with pins.pinned_prepared_registry_address(object(), object()):
            pytest.fail("unissued source reached a context")
    input_pin = pins.PinnedRegistryCMSAddressInput(object(), object(), object(), (), (), "", "{}", "{}")
    target = SimpleNamespace(fetchval=AsyncMock())
    with pytest.raises(ValueError, match="pin_invalid"):
        await input_pin.require_target(target, "retained_owner")
    target.fetchval.assert_not_awaited()


def _issued(monkeypatch):
    source = SimpleNamespace(require_active=Mock(), verify_transaction=AsyncMock())
    admission = SimpleNamespace(_assert_lease=Mock())
    stage = ("entity_address_unified", "stage", 11)
    prepared = SimpleNamespace(
        address=SimpleNamespace(committed=False, stage_oids=(stage,)), nonprofile_admission=admission
    )
    pin = pins.PinnedRegistryCMSAddressInput(prepared, source, object(), (stage,), (), "{}", "{}", "{}")
    state_by_field = dict(
        task=ref(asyncio.current_task()),
        prepared=lambda: prepared,
        source_input=source,
        admission=lambda: admission,
        active=True,
        complete=False,
        target=None,
        relations={},
    )
    monkeypatch.setitem(pins._INPUTS, pin, state_by_field)
    return pin, state_by_field


@pytest.mark.asyncio
async def test_expired_cross_task_and_changed_prepared_source_refuse(monkeypatch):
    pin, state_by_field = _issued(monkeypatch)
    pin.require_active()

    async def other_task():
        with pytest.raises(ValueError, match="pin_invalid"):
            pin.require_active()

    await asyncio.create_task(other_task())
    pin.prepared.address.stage_oids = (("entity_address_unified", "replacement", 12),)
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_active()
    state_by_field["active"] = False
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_active()


@pytest.mark.asyncio
async def test_equal_constructed_annotation_cannot_replace_registered_proof(monkeypatch):
    pin, state_by_field = _issued(monkeypatch)
    annotation = RetainedNativeRelation(
        "retained", "entity_address_unified", 21, "{}", object(), phase="loading", source_input=pin
    )
    state_by_field["relations"][(annotation.schema, annotation.relation)] = ref(annotation)
    pin.require_annotation(annotation)
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_annotation(replace(annotation))
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_annotation(annotation, final=True)
    state_by_field["active"] = False
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_annotation(annotation)


@pytest.mark.parametrize("phase", ["loading", "indexing"])
def test_caught_partial_index_work_cannot_pass_final_admission(phase):
    annotation = RetainedNativeRelation("retained", "entity_address_unified", 21, "{}", object(), phase=phase)
    admission = SimpleNamespace(_native_relations={(annotation.schema, annotation.relation): annotation})
    with pytest.raises(RuntimeError, match="native_incomplete"):
        NonprofileAdmission._require_finished_native_relations(admission)


def _structure():
    return dict(
        columns=["unchanged"],
        indexes=[dict(relation_kind="main", indisprimary=True, indisunique=True, keys="1")],
        constraints=[dict(constraint_type="n", column="id"), dict(constraint_type="p", keys="1")],
    )


@pytest.mark.parametrize("phase", ["loading", "indexing"])
def test_phase_keeps_exact_native_heap_and_bounded_index_multisets(monkeypatch, phase):
    monkeypatch.setattr(layouts, "_structural_catalog", deepcopy)
    original = _structure()
    actual = deepcopy(original)
    actual["indexes"], actual["constraints"] = [], actual["constraints"][:1]
    layouts._retained_phase_structure(original, actual, phase)
    actual["columns"] = ["changed"]
    with pytest.raises(RuntimeError, match="shape_unsupported"):
        layouts._retained_phase_structure(original, actual, phase)


def test_indexing_accepts_pending_pk_but_denies_extra_duplicates(monkeypatch):
    monkeypatch.setattr(layouts, "_structural_catalog", deepcopy)
    original, actual = _structure(), _structure()
    actual["indexes"][0]["indisprimary"] = False
    actual["constraints"] = actual["constraints"][:1]
    layouts._retained_phase_structure(original, actual, "indexing")
    with pytest.raises(RuntimeError, match="shape_unsupported"):
        layouts._retained_phase_structure(original, actual, "loading")
    actual["indexes"].append(deepcopy(actual["indexes"][0]))
    with pytest.raises(RuntimeError, match="shape_unsupported"):
        layouts._retained_phase_structure(original, actual, "indexing")
    original["indexes"].append(deepcopy(original["indexes"][0]))
    layouts._retained_phase_structure(original, actual, "indexing")


def test_source_heap_has_native_columns_but_no_source_nextval_or_indexes():
    columns = [
        dict(
            attname="evidence_id",
            type="bigint",
            attnotnull=True,
            attgenerated="",
            attidentity="",
            collation_name=None,
            collation_schema=None,
            default_expression="nextval('source.seq'::regclass)",
        )
    ]
    statement = address._source_heap_sql("retained", "entity_address_evidence", columns)
    assert statement == 'CREATE TABLE "retained"."entity_address_evidence" ("evidence_id" bigint NOT NULL)'
    assert "LIKE" not in statement and "INDEX" not in statement and "source" not in statement


def test_native_column_recipe_preserves_storage_and_compression():
    native = (None, json.dumps({"logical_catalog": {"target": {"columns": [{"attname": "value"}]}}}))
    layout = SimpleNamespace(
        oid=7,
        catalog_json=json.dumps(
            {"attributes": [{"relation_oid": 7, "attname": "value", "attstorage": "m", "attcompression": "p"}]}
        ),
    )
    columns = pins._column_recipes(native, {"target": layout})["target"]
    assert columns == [{"attname": "value", "storage": "m", "compression": "p"}]
    assert list(address._source_column_options("retained", "target", columns)) == [
        'ALTER TABLE "retained"."target" ALTER COLUMN "value" SET STORAGE MAIN',
        'ALTER TABLE "retained"."target" ALTER COLUMN "value" SET COMPRESSION pglz',
    ]


@pytest.mark.asyncio
async def test_source_bounds_and_export_never_use_retained_driver(monkeypatch):
    from tests.test_network_registry_cms_prepared_address_copy import BoundsConnection, _prepared

    reader = BoundsConnection([("a",), ("b",)])
    target = SimpleNamespace(fetchrow=AsyncMock(), fetchval=AsyncMock())
    input_pin = SimpleNamespace(connection=reader, verify_transaction=AsyncMock(), require_active=Mock())
    input_pin.copy_connections = lambda driver: pins._CopyConnections(input_pin, driver)

    async def copy(transport, _query, _params, _schema, _logical, count, **options):
        assert transport.source is input_pin and transport.target is target
        assert count == 2
        await options["import_admission"]("before_import")
        await options["import_admission"]("after_import")

    monkeypatch.setattr(address, "_copy_native_batch", copy)
    await address._copy_prepared_rows(
        target, _prepared(), "retained", ("entity_address_unified", "stage", 42), 2, (4, 1024), input_pin
    )
    target.fetchrow.assert_not_awaited()
    target.fetchval.assert_not_awaited()
    assert len(reader.bounds) == 1 and input_pin.verify_transaction.await_count == 2
