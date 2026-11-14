# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Checkpoint names retain complete native layouts and legacy data transitions."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from process import provider_directory_source_checkpoint_catalog as catalog
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding


def _guards(root):
    return [
        dict(
            tgname=name,
            tgtype=event,
            tgdeferrable=deferred,
            tginitdeferred=initial,
            tgoldtable=old,
            tgnewtable=new,
            proname=function,
            tgenabled="A",
            unconditional=True,
            update_columns=[],
            tgnargs=0,
            tgargs=b"",
            nspname="sample",
        )
        for name, (event, deferred, initial, old, new, function) in catalog._GUARDS[root].items()
    ]


@pytest.mark.parametrize("root", catalog.CHECKPOINT_LAYOUTS)
def test_complete_old_and_finished_guard_sets_are_distinct(root):
    policy = SimpleNamespace(schema="sample")
    guards = _guards(root)
    catalog._require_guards(guards, root, policy, legacy=True)
    finished_guards = [guard for guard in guards if guard["tgname"] not in catalog.RETIRED_BULK_HOOKS]
    catalog._require_guards(finished_guards, root, policy, legacy=False)
    if root == catalog.BULK_ROOT:
        with pytest.raises(GenericCandidateError, match="checkpoint_hooks_changed"):
            catalog._require_guards(guards, root, policy, legacy=False)
        with pytest.raises(GenericCandidateError, match="checkpoint_hooks_changed"):
            catalog._require_guards(guards[1:], root, policy, legacy=True)


@pytest.mark.parametrize(
    "change", ["transition", "deferred", "arguments", "event", "namespace", "function", "conditional", "disabled"]
)
def test_guard_transition_refuses_changed_invocation(change):
    guards = _guards(catalog.BULK_ROOT)
    fields_by_change = {
        "transition": ("tgoldtable", "substituted"),
        "deferred": ("tgdeferrable", True),
        "arguments": ("tgargs", b"arg\x00"),
        "event": ("tgtype", 31),
        "namespace": ("nspname", "other_schema"),
        "function": ("proname", "other_function"),
        "conditional": ("unconditional", False),
        "disabled": ("tgenabled", "D"),
    }
    key, replacement = fields_by_change[change]
    guards[0][key] = replacement
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_changed"):
        catalog._require_guards(guards, catalog.BULK_ROOT, SimpleNamespace(schema="sample"), legacy=True)


def _relation(root):
    layout = deepcopy(catalog.CHECKPOINT_LAYOUTS[root])
    return dict(
        relname=root,
        relkind="r",
        relpersistence="p",
        relispartition=False,
        parents=[],
        children=[],
        relrowsecurity=False,
        relforcerowsecurity=False,
        native_checks=True,
        columns=layout["columns"],
        constraints=[dict(type=kind, definition=definition, valid=True) for kind, definition in layout["constraints"]],
        indexes=[
            dict(
                primary=primary,
                unique=unique,
                method=method,
                predicate=predicate,
                expression=expression,
                keys=keys,
                valid=True,
            )
            for primary, unique, method, predicate, expression, keys in layout["indexes"]
        ],
    )


def _damage_relation(relation, change):
    if change == "constraint":
        relation["constraints"].pop()
    elif change == "partition":
        relation["parents"] = ['"mrf"."other_parent"']
    elif change == "child":
        relation["children"] = ['"mrf"."other_child"']
    else:
        fields_by_change = {
            "column": (relation["columns"][0], 2, False),
            "index": (relation["indexes"][0], "valid", False),
            "check": (relation, "native_checks", False),
            "hook": (relation, "inert", False),
            "namespace": (relation, "nspname", "other_schema"),
        }
        container, key, replacement = fields_by_change[change]
        container[key] = replacement


@pytest.mark.parametrize("root", catalog.CHECKPOINT_LAYOUTS)
@pytest.mark.parametrize("change", [None, "column", "constraint", "index", "partition", "child", "check"])
def test_only_the_full_fixed_native_layout_is_accepted(root, change):
    relation = _relation(root)
    if change is None:
        catalog._require_layout(relation, "mrf")
    else:
        _damage_relation(relation, change)
        with pytest.raises(GenericCandidateError, match="checkpoint_layout_changed"):
            catalog._require_layout(relation, "mrf")


@pytest.mark.parametrize("root", catalog.CHECKPOINT_LAYOUTS)
def test_private_checkpoint_layout_requires_no_foreign_keys_and_retains_native_keys(root):
    relation = _relation(root)
    with pytest.raises(GenericCandidateError, match="checkpoint_layout_changed"):
        catalog._require_layout(relation, "mrf", isolated=True)
    relation["constraints"] = [entry for entry in relation["constraints"] if entry["type"] != "f"]
    catalog._require_layout(relation, "mrf", isolated=True)
    with pytest.raises(GenericCandidateError, match="checkpoint_layout_changed"):
        catalog._require_layout(relation, "mrf")
    relation["constraints"] = [entry for entry in relation["constraints"] if entry["type"] != "p"]
    with pytest.raises(GenericCandidateError, match="checkpoint_layout_changed"):
        catalog._require_layout(relation, "mrf", isolated=True)


@pytest.fixture
def capture_case():
    """Answer actual shape and trigger queries using only qualified names and native data."""
    policy = GenericSourcePolicy("sample", "builder", "publisher", ("reader",))
    relations = [_relation(root) | {"nspname": policy.schema, "inert": True} for root in catalog.CHECKPOINT_LAYOUTS]
    for relation in relations:
        for constraint in relation["constraints"]:
            constraint["definition"] = constraint["definition"].replace("mrf.", policy.schema + ".")
    guards_by_root = {root: _guards(root) for root in catalog.CHECKPOINT_LAYOUTS}
    roots = [catalog.storage_relation(policy.schema, root) for root in catalog.CHECKPOINT_LAYOUTS]

    async def fetch(query, *arguments):
        if query == catalog._LAYOUT_SQL:
            assert arguments == (roots,)
            return deepcopy(relations)
        assert query == catalog._GUARD_SQL and arguments[0] in roots
        return deepcopy(guards_by_root[list(catalog.CHECKPOINT_LAYOUTS)[roots.index(arguments[0])]])

    async def execute(query):
        if query.startswith("SELECT "):
            assert query.endswith(" LIMIT 0") and '"sample".' in query
            return "SELECT 0"
        retired_names = [
            name
            for name in catalog.RETIRED_BULK_HOOKS
            if query == f'DROP TRIGGER IF EXISTS "{name}" ON "sample"."{catalog.BULK_ROOT}"'
        ]
        assert len(retired_names) == 1
        guards_by_root[catalog.BULK_ROOT] = [
            guard for guard in guards_by_root[catalog.BULK_ROOT] if guard["tgname"] != retired_names[0]
        ]
        return "DROP TRIGGER"

    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetch=AsyncMock(side_effect=fetch),
        execute=AsyncMock(side_effect=execute),
        fetchval=AsyncMock(return_value=True),
    )
    return SimpleNamespace(
        policy=policy,
        binding=FHIRSourceStorageBinding("catalog", "sample"),
        relations=relations,
        guards_by_root=guards_by_root,
        roots=roots,
        connection=connection,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        None,
        "foreign_key",
        "hook",
        "namespace",
        "native",
        "missing",
        "duplicate",
        "column",
        "constraint",
        "index",
        "child",
        "select",
    ],
)
async def test_private_checkpoint_capture_keeps_named_data_contract(capture_case, change):
    case = capture_case
    if change != "foreign_key":
        for relation in case.relations:
            relation["constraints"] = [entry for entry in relation["constraints"] if entry["type"] != "f"]
    if change in {"hook", "namespace", "column", "constraint", "index", "child"}:
        _damage_relation(case.relations[0], change)
    elif change == "native":
        case.connection.fetchval.return_value = False
    elif change in {"missing", "duplicate"}:
        case.relations[-1:] = [] if change == "missing" else [deepcopy(case.relations[0])]
    elif change == "select":
        case.connection.execute.side_effect = asyncpg.InsufficientPrivilegeError("SELECT denied")
    if change is not None:
        error = asyncpg.InsufficientPrivilegeError if change == "select" else GenericCandidateError
        with pytest.raises(error):
            await catalog.capture_checkpoint_controls(case.connection, case.policy, storage_binding=case.binding)
        return
    captured = await catalog.capture_checkpoint_controls(case.connection, case.policy, storage_binding=case.binding)
    assert set(captured) == set(catalog.CHECKPOINT_LAYOUTS)
    for root, control in captured.items():
        assert set(control) == {"schema", "name", "layout", "guards"}
        assert (control["schema"], control["name"], control["guards"]) == (case.policy.schema, root, [])
        assert set(control["layout"]) == {"columns", "constraints", "indexes"}
        assert all(entry["type"] != "f" for entry in control["layout"]["constraints"])
    json.dumps(captured)
    case.connection.fetch.assert_awaited_once_with(catalog._LAYOUT_SQL, case.roots)
    assert [call.args[1:] for call in case.connection.fetchval.await_args_list] == [(root,) for root in case.roots]
    assert all("to_regclass($1)" in call.args[0] for call in case.connection.fetchval.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_canonical_capture_preserves_legacy_transition_contract(capture_case, legacy):
    case = capture_case
    if not legacy:
        case.guards_by_root[catalog.BULK_ROOT] = [
            guard
            for guard in case.guards_by_root[catalog.BULK_ROOT]
            if guard["tgname"] not in catalog.RETIRED_BULK_HOOKS
        ]
    captured = await catalog.capture_checkpoint_controls(case.connection, case.policy, legacy=legacy)
    assert all(control["schema"] == case.policy.schema for control in captured.values())
    assert any(entry["type"] == "f" for entry in captured[catalog.BULK_ROOT]["layout"]["constraints"])
    assert all(
        guard["tgname"] not in catalog.RETIRED_BULK_HOOKS
        for control in captured.values()
        for guard in control["guards"]
    )
    assert [call.args[1:] for call in case.connection.fetch.await_args_list[1:]] == [(root,) for root in case.roots]
    json.dumps(captured)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "before", "after"])
async def test_retirement_compares_complete_logical_maps(capture_case, drift):
    case = capture_case
    before = await catalog.capture_checkpoint_controls(case.connection, case.policy, legacy=True)
    if drift == "before":
        before[catalog.BULK_ROOT]["name"] = "other_checkpoint"
    original_execute = case.connection.execute.side_effect

    async def execute(query):
        status = await original_execute(query)
        if drift == "after" and query.startswith("DROP TRIGGER"):
            case.relations[0]["columns"][0][2] = False
        return status

    case.connection.execute.side_effect = execute
    if drift is None:
        await catalog.retire_bulk_abandonment_hooks(case.connection, case.policy, before)
        assert await catalog.capture_checkpoint_controls(case.connection, case.policy) == before
    else:
        with pytest.raises(GenericCandidateError, match="checkpoint_identity_changed|checkpoint_layout_changed"):
            await catalog.retire_bulk_abandonment_hooks(case.connection, case.policy, before)
    drops = [
        call.args[0] for call in case.connection.execute.await_args_list if call.args[0].startswith("DROP TRIGGER")
    ]
    assert drops == (
        []
        if drift == "before"
        else [
            f'DROP TRIGGER IF EXISTS "{name}" ON "sample"."{catalog.BULK_ROOT}"'
            for name in sorted(catalog.RETIRED_BULK_HOOKS)
        ]
    )


def test_capture_queries_return_no_custody_metadata():
    for query in (catalog._LAYOUT_SQL, catalog._GUARD_SQL):
        for forbidden in (
            "relowner",
            "pg_roles",
            "acl",
            "has_table_privilege",
            "prosrc",
            "proowner",
            "'oid'",
            " AS oid",
        ):
            assert forbidden not in query
    assert "to_regclass(requested.name)" in catalog._LAYOUT_SQL
    assert "conenforced" in catalog._LAYOUT_SQL
    assert "indisvalid AND i.indisready AND i.indislive" in catalog._LAYOUT_SQL
