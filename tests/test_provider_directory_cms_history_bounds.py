# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact bound shape and read-only routing; replaced catalog rows confer no authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_history_bounds as bounds
from tests.test_provider_directory_cms_history_conversion import _request


def _observed_bound(dataset_ids):
    return dict(
        standard_strings=True,
        column=dict(number=1, type=1043, modifier=100, required=True, generated=""),
        constraints=[
            dict(
                oid=901,
                kind="c",
                validated=True,
                enforced=True,
                local=True,
                inherited=0,
                parent_oid=0,
                no_inherit=False,
                deferrable=False,
                deferred=False,
                columns=[1] if dataset_ids else [],
                tree="unit-only-native-tree",
                expression=bounds._expression(dataset_ids, True),
            )
        ],
    )


def _expectations(dataset_ids=None):
    datasets = ["one"] if dataset_ids is None else dataset_ids
    return {
        root: {"relation": {"schema": "catalog", "name": root}, "dataset_ids": list(datasets)}
        for root in bounds.history.ROOTS
    }


def _source_rows(expected, *, converted=False):
    return [
        dict(
            root=root,
            relname=entry["relation"]["name"],
            nspname=entry["relation"]["schema"],
            relkind="r",
            parents=["catalog." + root] if converted and entry["dataset_ids"] else [],
            leaf=True,
            canonical_kind="p" if converted else "r",
            canonical_parents=[],
            bound=_observed_bound(entry["dataset_ids"]),
        )
        for root, entry in expected.items()
    ]


@pytest.mark.parametrize(
    ("dataset_ids", "expected"),
    [
        ([], "false"),
        (["one"], "dataset_id::text = 'one'::text"),
        (["one", "two"], "dataset_id::text = ANY (ARRAY['one'::character varying, 'two'::character varying]::text[])"),
        (["a'quote"], "dataset_id::text = 'a''quote'::text"),
        (["a\\slash"], "dataset_id::text = 'a\\slash'::text"),
    ],
)
def test_fixed_native_expression_preserves_literals(dataset_ids, expected):
    assert bounds._expression(dataset_ids, True) == expected
    assert bounds._expression(dataset_ids, False) == expected.replace("\\", "\\\\")


def test_readback_keeps_raw_constraint_identity_separate_from_expression_formatting():
    observed = _observed_bound(["one"])
    original = deepcopy(observed)
    proof = bounds.require_cms_history_bound(observed, ["one"])
    assert observed == original and "oid" not in proof["constraint"] and "tree" not in proof["constraint"]
    assert "expression" not in proof["constraint"]
    proof["column"].clear()
    assert observed == original


def test_pending_prework_state_is_explicit_and_never_a_sealed_expectation():
    observed = _observed_bound(["one"])
    observed["constraints"][0]["validated"] = False
    pending = bounds.require_cms_history_bound(observed, ["one"], validated=False)
    assert pending["constraint"]["validated"] is False
    with pytest.raises(ValueError, match="validated_bound_changed"):
        bounds.require_cms_history_bound(observed, ["one"])
    observed["constraints"][0]["validated"] = True
    with pytest.raises(ValueError, match="validated_bound_changed"):
        bounds.require_cms_history_bound(observed, ["one"], validated=False)


@pytest.mark.parametrize("validated", [None, 0, 1, "false", []])
def test_prework_validation_state_rejects_coercion(validated):
    with pytest.raises(ValueError, match="validation_state_required"):
        bounds.require_cms_history_bound(_observed_bound(["one"]), ["one"], validated=validated)


@pytest.mark.parametrize("field", ["enforced", "local", "expression", "parent_oid"])
def test_pending_prework_bound_preserves_every_other_check_requirement(field):
    observed = _observed_bound(["one"])
    observed["constraints"][0].update(validated=False)
    observed["constraints"][0][field] = {"enforced": False, "local": False, "expression": "true", "parent_oid": 1}[
        field
    ]
    with pytest.raises(ValueError, match="validated_bound_changed"):
        bounds.require_cms_history_bound(observed, ["one"], validated=False)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("validated", False),
        ("enforced", False),
        ("local", False),
        ("inherited", 1),
        ("parent_oid", 123),
        ("no_inherit", True),
        ("deferrable", True),
        ("deferred", True),
        ("kind", "f"),
        ("columns", [2]),
        ("expression", "true"),
        ("tree", ""),
    ],
)
def test_unvalidated_different_or_nonlocal_bounds_refuse(field, value):
    observed = _observed_bound(["one"])
    observed["constraints"][0][field] = value
    with pytest.raises(ValueError, match="cms_history_bounds_|cms_history_plan_oid_required"):
        bounds.require_cms_history_bound(observed, ["one"])


@pytest.mark.parametrize("entries", [[], None, [{}, {}]])
def test_missing_or_multiple_bound_checks_refuse(entries):
    observed = _observed_bound(["one"])
    observed["constraints"] = entries
    with pytest.raises(ValueError, match="validated_bound_required"):
        bounds.require_cms_history_bound(observed, ["one"])


@pytest.mark.parametrize(("field", "value"), [("required", False), ("type", 25), ("modifier", -1), ("generated", "s")])
def test_partition_key_native_layout_must_match(field, value):
    observed = _observed_bound(["one"])
    observed["column"][field] = value
    with pytest.raises(ValueError, match="column_changed"):
        bounds.require_cms_history_bound(observed, ["one"])


async def test_only_two_declared_heap_bounds_are_read_without_mutation():
    plan = bounds.history.plan_cms_history_conversion(_request(same_schema=True))
    rows = [
        dict(relname=root, nspname="catalog", relkind="r", bound=_observed_bound(entry["dataset_ids"]))
        for root, entry in plan["request"]["heaps"].items()
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    proof = await bounds.read_cms_history_bounds(connection, plan)
    assert set(proof) == set(bounds.history.ROOTS)
    connection.fetch.assert_awaited_once_with(bounds._READ_SQL, sorted(bounds.history.ROOTS), "catalog")
    rows[0]["relkind"] = "v"
    with pytest.raises(ValueError, match="identity_changed"):
        await bounds.read_cms_history_bounds(connection, plan)


async def test_caller_sql_plan_change_refuses_before_any_query():
    plan = bounds.history.plan_cms_history_conversion(_request(same_schema=True))
    plan["critical"]["rename"] = ("arbitrary SQL",)
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="plan_changed"):
        await bounds.read_cms_history_bounds(connection, plan)
    connection.fetch.assert_not_awaited()


def test_expectations_are_detached_named_data_and_ignore_saved_physical_metadata():
    expected = _expectations()
    original = deepcopy(expected)
    expected[bounds.history.ROOTS[0]].update(old_oid=901, native={"owner": 902})
    checked = bounds.require_cms_history_expectations(expected)
    assert checked == original
    checked[bounds.history.ROOTS[0]]["dataset_ids"].clear()
    assert original == _expectations()


@pytest.mark.parametrize("value", [None, {}, [], {"unknown": {}}, {bounds.history.ROOTS[0]: {}}])
def test_missing_partial_or_nonmapping_expectations_refuse(value):
    with pytest.raises(ValueError, match="expectations_required"):
        bounds.require_cms_history_expectations(value)


@pytest.mark.parametrize("fault", ["field", "name", "alias", "datasets", "no_name"])
def test_expectations_require_portable_names_and_exact_bounds(fault):
    expected = _expectations()
    first = expected[bounds.history.ROOTS[0]]
    if fault == "field":
        first["authority"] = True
    elif fault == "name":
        first["relation"]["name"] = "invalid;name"
    elif fault == "alias":
        first["relation"] = expected[bounds.history.ROOTS[1]]["relation"]
    elif fault == "datasets":
        first["dataset_ids"] = ["two", "one"]
    else:
        first.pop("relation")
        first["old_oid"] = 901
    with pytest.raises(ValueError):
        bounds.require_cms_history_expectations(expected)


@pytest.mark.parametrize("converted", [False, True])
@pytest.mark.parametrize("datasets", [[], ["one"], ["one", "two"]])
async def test_source_bound_reads_original_or_declared_converted_context(converted, datasets):
    expected = _expectations(datasets)
    if converted:
        for index, entry in enumerate(expected.values()):
            entry["relation"]["name"] = "retained_" + str(index)
    rows = _source_rows(expected, converted=converted)
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    assert (
        await bounds.require_cms_source_history_bounds(connection, catalog_schema="catalog", expected=expected)
        == expected
    )
    connection.fetch.assert_awaited_once_with(
        bounds._SOURCE_READ_SQL,
        list(bounds.history.ROOTS),
        [expected[root]["relation"]["name"] for root in bounds.history.ROOTS],
        "catalog",
    )


@pytest.mark.parametrize("converted", [False, True])
@pytest.mark.parametrize(
    "field,value",
    [
        ("relname", "wrong"),
        ("nspname", "other"),
        ("relkind", "p"),
        ("parents", ["other.root"]),
        ("leaf", False),
        ("canonical_parents", ["other.parent"]),
        ("canonical_kind", "v"),
    ],
)
async def test_source_bound_refuses_wrong_name_layout_or_parent(converted, field, value):
    expected = _expectations()
    if converted:
        for index, entry in enumerate(expected.values()):
            entry["relation"]["name"] = "retained_" + str(index)
    rows = _source_rows(expected, converted=converted)
    rows[0][field] = value
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    with pytest.raises(ValueError, match="identity_changed"):
        await bounds.require_cms_source_history_bounds(connection, catalog_schema="catalog", expected=expected)


@pytest.mark.parametrize("field,value", [("validated", False), ("expression", "true")])
async def test_source_bound_refuses_changed_enforced_check(field, value):
    expected = _expectations()
    rows = _source_rows(expected)
    rows[0]["bound"]["constraints"][0][field] = value
    with pytest.raises(ValueError, match="cms_history_bounds_"):
        await bounds.require_cms_source_history_bounds(
            SimpleNamespace(fetch=AsyncMock(return_value=rows)), catalog_schema="catalog", expected=expected
        )


async def test_recreated_equivalent_bound_uses_semantics_and_pg_access():
    expected = _expectations()
    rows = _source_rows(expected)
    rows[0]["bound"]["constraints"][0].update(oid=9999, tree="another native parse tree")
    assert (
        await bounds.require_cms_source_history_bounds(
            SimpleNamespace(fetch=AsyncMock(return_value=rows)),
            catalog_schema="catalog",
            expected=expected,
            owner_oid=100,
        )
        == expected
    )
