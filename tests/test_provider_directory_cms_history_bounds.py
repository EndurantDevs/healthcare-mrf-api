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
    expectations_by_root = {}
    for index, root in enumerate(bounds.history.ROOTS):
        observed = _observed_bound(datasets)
        observed["constraints"][0]["oid"] += index
        expectations_by_root[root] = dict(
            old_oid=100 * (index + 1),
            dataset_ids=list(datasets),
            native=bounds.require_cms_history_bound(observed, datasets),
        )
    return expectations_by_root


def _source_rows(expected, *, converted=False):
    return [
        dict(
            root=root,
            oid=entry["old_oid"],
            relname="cms_history_" + str(entry["old_oid"]) if converted else root,
            nspname="catalog",
            relowner=4,
            relkind="r",
            parents=[entry["old_oid"] + 1] if converted and entry["dataset_ids"] else [],
            leaf=True,
            canonical_oid=entry["old_oid"] + int(converted),
            canonical_owner=4,
            canonical_kind="p" if converted else "r",
            canonical_parents=[],
            bound=dict(
                standard_strings=True,
                column=deepcopy(entry["native"]["column"]),
                constraints=[
                    {
                        **deepcopy(entry["native"]["constraint"]),
                        "expression": bounds._expression(entry["dataset_ids"], True),
                    }
                ],
            ),
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
    assert observed == original and proof["constraint"]["oid"] == 901
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
    expected = _expectations()
    expected[bounds.history.ROOTS[0]]["native"] = pending
    with pytest.raises(ValueError, match="validated_bound_changed"):
        bounds.require_cms_history_expectations(expected)
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
        ("oid", True),
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


async def test_only_two_original_heap_identities_are_read_without_mutation():
    plan = bounds.history.plan_cms_history_conversion(_request(same_schema=True))
    rows = [
        dict(
            oid=entry["old"]["oid"],
            relname=root,
            nspname="catalog",
            relowner=4,
            relkind="r",
            bound=_observed_bound(entry["dataset_ids"]),
        )
        for root, entry in plan["request"]["heaps"].items()
    ]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    proof = await bounds.read_cms_history_bounds(connection, plan)
    assert set(proof) == {"100", "200"}
    connection.fetch.assert_awaited_once_with(bounds._READ_SQL, [100, 200])
    assert "ALTER TABLE" not in bounds._READ_SQL and "LOCK TABLE" not in bounds._READ_SQL
    rows[0]["relowner"] = 5
    with pytest.raises(ValueError, match="identity_changed"):
        await bounds.read_cms_history_bounds(connection, plan)


async def test_caller_sql_or_oid_plan_change_refuses_before_any_query():
    plan = bounds.history.plan_cms_history_conversion(_request(same_schema=True))
    plan["critical"]["rename"] = ("arbitrary SQL",)
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="plan_changed"):
        await bounds.read_cms_history_bounds(connection, plan)
    connection.fetch.assert_not_awaited()


def test_expectations_are_exact_detached_native_data():
    expected = _expectations()
    checked = bounds.require_cms_history_expectations(expected)
    assert checked == expected
    checked[bounds.history.ROOTS[0]]["dataset_ids"].clear()
    checked[bounds.history.ROOTS[1]]["native"]["column"].clear()
    assert expected == _expectations()


@pytest.mark.parametrize("value", [None, {}, [], {"unknown": {}}, {bounds.history.ROOTS[0]: {}}])
def test_missing_partial_or_nonmapping_expectations_refuse(value):
    with pytest.raises(ValueError, match="expectations_required"):
        bounds.require_cms_history_expectations(value)


@pytest.mark.parametrize(
    "fault",
    ["field", "old_bool", "old_alias", "datasets", "native_field", "expression", "constraint_alias", "false", "type"],
)
def test_expectation_native_pins_refuse_unknown_fields_coercion_and_aliases(fault):
    expected = _expectations()
    witness_by_field = expected[bounds.history.ROOTS[0]]
    relationship_by_field = expected[bounds.history.ROOTS[1]]
    changes_by_fault = {
        "field": (witness_by_field, "authority", True),
        "old_bool": (witness_by_field, "old_oid", True),
        "old_alias": (witness_by_field, "old_oid", relationship_by_field["old_oid"]),
        "datasets": (witness_by_field, "dataset_ids", ["two", "one"]),
        "native_field": (witness_by_field["native"], "observed", True),
        "expression": (witness_by_field["native"]["constraint"], "expression", "true"),
        "constraint_alias": (
            witness_by_field["native"]["constraint"],
            "oid",
            relationship_by_field["native"]["constraint"]["oid"],
        ),
        "false": (witness_by_field["native"]["constraint"], "validated", False),
        "type": (witness_by_field["native"]["column"], "type", 1043.0),
    }
    target_by_field, field_name, replacement = changes_by_fault[fault]
    target_by_field[field_name] = replacement
    with pytest.raises(ValueError, match="cms_history_bounds_|cms_history_plan_"):
        bounds.require_cms_history_expectations(expected)


@pytest.mark.parametrize("converted", [False, True])
@pytest.mark.parametrize("datasets", [[], ["one"], ["one", "two"]])
async def test_source_bound_read_authenticates_original_or_exact_converted_context(converted, datasets):
    expected = _expectations(datasets)
    rows = _source_rows(expected, converted=converted)
    before = deepcopy(rows)
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    checked = await bounds.require_cms_source_history_bounds(connection, catalog_schema="catalog", expected=expected)
    assert checked == expected and rows == before
    connection.fetch.assert_awaited_once_with(
        bounds._SOURCE_READ_SQL, list(bounds.history.ROOTS), [100, 200], "catalog"
    )
    checked[bounds.history.ROOTS[0]]["native"]["constraint"]["tree"] = "changed"
    assert expected == _expectations(datasets)


@pytest.mark.parametrize("converted", [False, True])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("oid", 999),
        ("relname", "cms_history_999"),
        ("nspname", "other"),
        ("relowner", 5),
        ("relkind", "p"),
        ("parents", [999]),
        ("leaf", False),
        ("canonical_owner", 5),
        ("canonical_parents", [999]),
        ("canonical_kind", "v"),
    ],
)
async def test_source_bound_refuses_wrong_heap_owner_namespace_or_parent_context(converted, field, value):
    expected = _expectations()
    rows = _source_rows(expected, converted=converted)
    rows[0][field] = value
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    with pytest.raises(ValueError, match="identity_changed"):
        await bounds.require_cms_source_history_bounds(connection, catalog_schema="catalog", expected=expected)


@pytest.mark.parametrize("field", ["oid", "tree", "validated", "expression"])
async def test_source_bound_refuses_replaced_or_changed_native_check(field):
    expected = _expectations()
    rows = _source_rows(expected)
    row = rows[0]["bound"]["constraints"][0]
    row[field] = {"oid": 999, "tree": "replaced-tree", "validated": False, "expression": "true"}[field]
    connection = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    with pytest.raises(ValueError, match="cms_history_bounds_"):
        await bounds.require_cms_source_history_bounds(connection, catalog_schema="catalog", expected=expected)


async def test_source_bound_invalid_expectation_refuses_before_io():
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="expectations_required"):
        await bounds.require_cms_source_history_bounds(connection, catalog_schema="catalog", expected=None)
    connection.fetch.assert_not_awaited()


@pytest.mark.parametrize("owner", [4, 5, True])
async def test_source_bound_owner_can_be_pinned_by_the_issued_storage_identity(owner):
    expected = _expectations()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=_source_rows(expected)))
    if owner == 4 and type(owner) is int:
        assert (
            await bounds.require_cms_source_history_bounds(
                connection, catalog_schema="catalog", expected=expected, owner_oid=owner
            )
            == expected
        )
    else:
        with pytest.raises(ValueError):
            await bounds.require_cms_source_history_bounds(
                connection, catalog_schema="catalog", expected=expected, owner_oid=owner
            )
