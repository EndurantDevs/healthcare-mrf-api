# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Receipt locks and issued bridge state; no mocked native authority claim."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

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
async def test_unissued_input_cannot_open_source_scope_or_copy():
    with pytest.raises(ValueError, match="pin_invalid"):
        async with pins.pinned_prepared_registry_address(object(), object()):
            pytest.fail("unissued source reached a context")
    input_pin = pins.PinnedRegistryCMSAddressInput(object(), object(), object(), (), (), "", "{}", "{}")
    with pytest.raises(ValueError, match="pin_invalid"):
        input_pin.require_active()


def _issued(monkeypatch, *, source_layouts=()):
    source = SimpleNamespace(require_active=Mock(), verify_transaction=AsyncMock())
    admission = SimpleNamespace(_assert_lease=Mock())
    stage = ("entity_address_unified", "stage")
    prepared = SimpleNamespace(
        address=SimpleNamespace(committed=False, stage_relations=(stage,)), nonprofile_admission=admission
    )
    pin = pins.PinnedRegistryCMSAddressInput(prepared, source, object(), (stage,), source_layouts, "{}", "{}", "{}")
    state_by_field = dict(active=True, complete=False, relations={})
    monkeypatch.setitem(pins._INPUTS, pin, state_by_field)
    return pin, state_by_field


@pytest.mark.asyncio
async def test_lease_lifetime_and_changed_named_selection_refuse(monkeypatch):
    pin, state_by_field = _issued(monkeypatch)
    pin.require_active()

    async def other_task():
        pin.require_active()

    await asyncio.create_task(other_task())
    pin.prepared.address.stage_relations = (("entity_address_unified", "replacement"),)
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_active()
    state_by_field["active"] = False
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_active()


@pytest.mark.asyncio
async def test_equal_logical_annotations_preserve_registered_phase(monkeypatch):
    pin, state_by_field = _issued(monkeypatch)
    annotation = SimpleNamespace(
        schema="retained",
        relation="entity_address_unified",
        policy_json="{}",
        source_layout="logical layout",
        phase="loading",
        source_input=pin,
    )
    state_by_field["relations"][(annotation.schema, annotation.relation)] = annotation
    pin.require_annotation(annotation)
    pin.require_annotation(SimpleNamespace(**vars(annotation)))
    changed = SimpleNamespace(**{**vars(annotation), "source_layout": "changed layout"})
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_annotation(changed)
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_annotation(annotation, final=True)
    state_by_field["active"] = False
    with pytest.raises(ValueError, match="pin_invalid"):
        pin.require_annotation(annotation)


@pytest.mark.parametrize("phase", ["loading", "indexing"])
def test_caught_partial_index_work_cannot_pass_final_admission(phase):
    annotation = SimpleNamespace(schema="retained", relation="entity_address_unified", phase=phase)
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
        catalog_json=json.dumps(
            {"attributes": [{"relation_kind": "main", "attname": "value", "attstorage": "m", "attcompression": "p"}]}
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
        target, _prepared(), "retained", ("entity_address_unified", "stage"), 2, (4, 1024), input_pin
    )
    target.fetchrow.assert_not_awaited()
    target.fetchval.assert_not_awaited()
    assert len(reader.bounds) == 1 and input_pin.verify_transaction.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", ["active", "closed", "missing"])
async def test_first_annotation_uses_native_transaction_without_owner_ceremony(monkeypatch, transaction):
    source_layout = layouts.RetainedNativeSourceLayout("EntityAddressUnified", "sample", "stage", "{}")
    pin, progress = _issued(monkeypatch, source_layouts=(("entity_address_unified", source_layout),))
    connection = SimpleNamespace(
        is_in_transaction=lambda: transaction == "active",
        fetch=AsyncMock(side_effect=AssertionError("unexpected actor catalog")),
        fetchrow=AsyncMock(side_effect=AssertionError("unexpected role catalog")),
    )
    binding = None if transaction == "missing" else SimpleNamespace(session=object())
    pin.prepared.fhir = SimpleNamespace(db=SimpleNamespace(_transaction_binding=lambda: binding))
    monkeypatch.setattr(pins, "native_driver", AsyncMock(return_value=connection))
    annotation = RetainedNativeRelation("retained", "entity_address_unified", "{}", source_layout, "loading", pin)
    if transaction == "active":
        await pin.register_relation(annotation, pin.prepared.nonprofile_admission)
        assert progress["relations"] == {("retained", "entity_address_unified"): annotation}
    else:
        with pytest.raises(ValueError, match="pin_invalid"):
            await pin.register_relation(annotation, pin.prepared.nonprofile_admission)
        assert progress["relations"] == {}
    assert "target" not in progress
    connection.fetch.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_name", "crossed_schema"])
async def test_copy_completion_requires_one_complete_named_target(monkeypatch, damage):
    names = sorted(model.__tablename__ for model in receipts._models())
    stages = tuple((name, name + "_prepared") for name in names)
    prepared = SimpleNamespace(
        address=SimpleNamespace(committed=False, stage_relations=stages),
        nonprofile_admission=SimpleNamespace(_assert_lease=Mock()),
    )
    source = SimpleNamespace(require_active=Mock(), verify_transaction=AsyncMock())
    pin = pins.PinnedRegistryCMSAddressInput(prepared, source, object(), stages, (), "{}", "{}", "{}")
    annotation_by_name = {name: SimpleNamespace(schema="retained", relation=name, phase="finished") for name in names}
    if damage == "missing_name":
        annotation_by_name.pop(names[0])
    else:
        annotation_by_name[names[0]].schema = "other_retained_schema"
    monkeypatch.setitem(pins._INPUTS, pin, {"active": True, "complete": False, "relations": annotation_by_name})
    monkeypatch.setattr(pins.PinnedRegistryCMSAddressInput, "_require_target_transaction", AsyncMock())
    capture = AsyncMock()
    monkeypatch.setattr(pins, "validate_registry_cms_address_copy", capture)
    with pytest.raises(ValueError, match="pin_invalid"):
        await pin.finish_copy(object(), SimpleNamespace(schema_name="retained", relation_names=tuple(names)), object())
    capture.assert_not_awaited()
