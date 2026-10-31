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
