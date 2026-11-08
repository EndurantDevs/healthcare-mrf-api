# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact clone transformations never authenticate captured metadata by themselves."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.migration_provider_directory_dataset_candidates import TABLES
from process import provider_directory_cms_allocation_semantics as semantics


def _columns():
    """Keep every represented field except the exact native allocation identities."""
    parent_by_field = {
        "oid": 101,
        "native": {
            "columns": [
                {
                    "attribute": dict(
                        attrelid="101",
                        attislocal=True,
                        attinhcount=0,
                        attacl=None,
                        attname="payload",
                        attnum=1,
                        attidentity="",
                        attgenerated="",
                        attisdropped=False,
                        atthasmissing=False,
                        attmissingval=None,
                        attstorage="x",
                        attcompression="",
                        attnotnull=True,
                    ),
                    "default": dict(oid="301", adrelid="101", adnum=1, adbin="fixed-native-expression"),
                    "type": {"oid": "25"},
                    "collation": {"oid": "100"},
                }
            ],
        },
    }
    child = deepcopy(parent_by_field)
    child["oid"] = 202
    column = child["native"]["columns"][0]
    column["attribute"].update(attrelid="202", attislocal=False, attinhcount=1)
    column["default"].update(oid="302", adrelid="202")
    return parent_by_field, child


def test_exact_column_clone_does_not_mutate_native_parent():
    parent, child = _columns()
    before = deepcopy(parent)
    semantics._require_columns(parent, child)
    assert parent == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("attstorage", "e"),
        ("attcompression", "p"),
        ("attnotnull", False),
        ("attgenerated", "s"),
        ("attisdropped", True),
        ("attislocal", True),
        ("attinhcount", 2),
        ("attacl", "unexpected"),
    ],
)
def test_child_column_drift_is_not_a_normalized_identity(field, value):
    parent, child = _columns()
    child["native"]["columns"][0]["attribute"][field] = value
    with pytest.raises(ValueError, match="columns_changed"):
        semantics._require_columns(parent, child)


@pytest.mark.parametrize("field", ["adnum", "adbin"])
def test_only_default_native_ids_can_differ(field):
    parent, child = _columns()
    child["native"]["columns"][0]["default"][field] = "changed"
    with pytest.raises(ValueError, match="columns_changed"):
        semantics._require_columns(parent, child)


@pytest.mark.asyncio
async def test_transaction_required_before_native_io():
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=False), fetch=AsyncMock())
    with pytest.raises(ValueError, match="transaction_required"):
        await semantics.require_cms_dynamic_allocations(connection, "catalog", family=[], historical_oids=())
    connection.fetch.assert_not_called()


def _roots():
    return [dict(oid=101 + ordinal, relname=root, native={"parents": []}) for ordinal, root in enumerate(TABLES[:4])]


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [(999,), (101,), (True,)])
async def test_retained_omissions_must_be_exact_nonroot_native_oids(retained):
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=True), fetch=AsyncMock())
    with pytest.raises(ValueError, match="retained_changed|omissions_changed"):
        await semantics.require_cms_dynamic_allocations(
            connection, "catalog", family=_roots(), historical_oids=(), retained_oids=retained
        )
    connection.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_detached_historical_oids_are_caller_validated_without_prefix_omissions(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=True), fetch=AsyncMock())
    monkeypatch.setattr(semantics.custody, "_inventory", AsyncMock(return_value=_roots()))
    assert (
        await semantics.require_cms_dynamic_allocations(
            connection, "catalog", family=_roots(), historical_oids=(501, 502)
        )
        == []
    )
    connection.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_empty_dynamic_snapshot_cannot_hide_a_new_allocation(monkeypatch):
    connection = SimpleNamespace(is_in_transaction=Mock(return_value=True), fetch=AsyncMock())
    monkeypatch.setattr(semantics.custody, "_inventory", AsyncMock(return_value=[*_roots(), {"new": True}]))
    with pytest.raises(ValueError, match="family_changed"):
        await semantics.require_cms_dynamic_allocations(connection, "catalog", family=_roots(), historical_oids=())
    connection.fetch.assert_not_called()


def _fixture_hook(oid, name, kind):
    return dict(
        oid=str(oid),
        tgrelid="101",
        tgname=name,
        tgfoid="301",
        tgtype=kind,
        tgenabled="A",
        tgisinternal=False,
        tgparentid="0",
        tgconstrrelid="0",
        tgconstrindid="0",
        tgconstraint="0",
        tgdeferrable=False,
        tginitdeferred=False,
        tgnargs=0,
        tgattr="",
        tgargs="\\x",
        tgqual=None,
        tgoldtable=None,
        tgnewtable=None,
    )


def _guard_family():
    allocator_hooks = [
        _fixture_hook(10 + ordinal, "a_pd_candidate_" + name, kind)
        for ordinal, (name, kind) in enumerate(
            (("insert_new", 4), ("update_new", 16), ("update_old", 16), ("delete_old", 8))
        )
    ]
    foreign_hooks = []
    for oid, kind, function in ((21, 9, "1646"), (22, 17, "1655")):
        native = _fixture_hook(oid, "RI_ConstraintTrigger_a_" + str(oid), kind)
        native.update(
            tgisinternal=True,
            tgenabled="O",
            tgfoid=function,
            tgconstrrelid="501",
            tgconstrindid="401",
            tgconstraint="601",
        )
        foreign_hooks.append(native)
    child_allocator_hooks = [
        dict(pin, oid=str(int(pin["oid"]) + 100), tgrelid="202", tgenabled="O") for pin in allocator_hooks
    ]
    child_foreign_hooks = [
        dict(
            pin,
            oid=str(int(pin["oid"]) + 100),
            tgname="RI_ConstraintTrigger_a_" + str(int(pin["oid"]) + 100),
            tgrelid="202",
            tgparentid=pin["oid"],
            tgconstrindid="402",
            tgconstraint="602",
        )
        for pin in foreign_hooks
    ]
    return SimpleNamespace(
        parent=dict(oid=101, relname=TABLES[0], native=dict(triggers=allocator_hooks + foreign_hooks)),
        child=dict(oid=202, native=dict(triggers=child_allocator_hooks + child_foreign_hooks)),
        indexes={"402": "401"},
        clones={602: dict(oid=602, root=501, parent=601, valid=True)},
    )


def _check_guards(case):
    semantics._require_guards(case.parent, case.child)
    semantics._require_inherited_guards(case.parent, case.child, case.indexes, case.clones)


def test_exact_native_action_clones_preserve_all_four_allocator_hooks():
    case = _guard_family()
    before = deepcopy(case)
    _check_guards(case)
    assert case == before


@pytest.mark.parametrize("kind", ["missing", "duplicate", "unknown"])
def test_internal_hooks_must_cover_exact_authenticated_parent_hooks(kind):
    case = _guard_family()
    if kind == "missing":
        case.child["native"]["triggers"].pop()
    elif kind == "duplicate":
        case.child["native"]["triggers"][-1] = deepcopy(case.child["native"]["triggers"][-2])
    else:
        case.child["native"]["triggers"][-1]["tgparentid"] = "999"
    with pytest.raises(ValueError, match="foreign_guards_changed"):
        _check_guards(case)


@pytest.mark.parametrize(
    "field,value",
    [
        ("tgfoid", "999"),
        ("tgtype", 5),
        ("tgenabled", "R"),
        ("tgisinternal", False),
        ("tgconstrrelid", "999"),
        ("tgconstrindid", "999"),
        ("tgconstraint", "999"),
        ("tgrelid", "999"),
        ("tgname", "unapproved_hook"),
        ("tgdeferrable", True),
        ("tginitdeferred", True),
        ("tgnargs", 1),
        ("tgargs", "\\x01"),
        ("tgattr", "1"),
        ("tgqual", "unapproved_expression"),
        ("tgoldtable", "unapproved_rows"),
    ],
)
def test_native_clone_fields_are_not_ignored(field, value):
    case = _guard_family()
    case.child["native"]["triggers"][-1][field] = value
    with pytest.raises(ValueError, match="guards_changed"):
        _check_guards(case)


@pytest.mark.parametrize("field,value", [("root", 999), ("parent", 999), ("valid", False)])
def test_native_constraint_proof_must_match_parent_binding(field, value):
    case = _guard_family()
    case.clones[602][field] = value
    with pytest.raises(ValueError, match="foreign_guards_changed"):
        _check_guards(case)


@pytest.mark.parametrize("fault", ["missing_constraint", "wrong_index", "nonresource_parent", "parent_disabled"])
def test_foreign_guard_requires_source_root_constraint_index_and_enabled_parent(fault):
    case = _guard_family()
    if fault == "missing_constraint":
        case.clones.clear()
    elif fault == "wrong_index":
        case.indexes["402"] = "999"
    elif fault == "nonresource_parent":
        case.parent["relname"] = TABLES[1]
    else:
        case.parent["native"]["triggers"][-1]["tgenabled"] = "D"
    with pytest.raises(ValueError, match="foreign_guards_changed"):
        _check_guards(case)


@pytest.mark.parametrize("fault", ["missing", "disabled", "extra"])
def test_inherited_fk_support_does_not_relax_allocator_hooks(fault):
    case = _guard_family()
    if fault == "missing":
        case.child["native"]["triggers"].pop(0)
    elif fault == "disabled":
        case.child["native"]["triggers"][0]["tgenabled"] = "D"
    else:
        case.child["native"]["triggers"].append(dict(case.child["native"]["triggers"][0], tgname="extra_hook"))
    with pytest.raises(ValueError, match="guards_changed"):
        _check_guards(case)
