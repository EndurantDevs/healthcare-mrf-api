# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit bounds retain every original fixed-layout constraint check."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_canonical_layout as layout
from process import provider_directory_cms_history_bounds as bounds
from tests.test_provider_directory_cms_history_bounds import _expectations


def _constraint_rows(expected):
    return [
        dict(root=entry["old_oid"], oid=entry["native"]["constraint"]["oid"], conname="cms_history_bound", contype="c")
        for entry in expected.values()
    ]


def test_only_exact_authenticated_constraint_oids_are_removed():
    expected = _expectations()
    original_constraint_by_field = dict(root=100, oid=999, conname="original_check", contype="c")
    unknown_bound_by_field = dict(root=100, oid=998, conname="cms_history_bound", contype="c")
    rows = [original_constraint_by_field, *_constraint_rows(expected), unknown_bound_by_field]
    before = deepcopy(rows)
    assert layout._without_authenticated_history_bounds(rows, expected) == [
        original_constraint_by_field,
        unknown_bound_by_field,
    ]
    assert rows == before
    unknown_bound_by_field.update(
        convalidated=True,
        conenforced=True,
        native_enforcement=True,
        conislocal=True,
        coninhcount=0,
        conparentid=0,
        condeferrable=False,
        condeferred=False,
        connoinherit=False,
        columns=[],
        definition="CHECK (true)",
    )
    with pytest.raises(ValueError, match="constraint_native|constraints"):
        layout._require_constraints([unknown_bound_by_field], [], [], "example", '"catalog"', frozenset())


@pytest.mark.parametrize("fault", ["missing", "duplicate", "root", "name", "kind"])
def test_missing_duplicate_or_displaced_authenticated_checks_refuse(fault):
    expected = _expectations()
    rows = _constraint_rows(expected)
    if fault == "missing":
        rows.pop()
    elif fault == "duplicate":
        rows.append(rows[0])
    else:
        rows[0][{"root": "root", "name": "conname", "kind": "contype"}[fault]] = {
            "root": 999,
            "name": "other_check",
            "kind": "f",
        }[fault]
    with pytest.raises(ValueError, match="history_bound_changed"):
        layout._without_authenticated_history_bounds(rows, expected)


def _connection(expected):
    roots = [dict(relname=root, oid=1000 + index) for index, root in enumerate(layout._LAYOUTS)]
    for row in roots:
        if row["relname"] in expected:
            row["oid"] = expected[row["relname"]]["old_oid"]
    constraints = [dict(root=100, oid=999, conname="original", contype="c"), *_constraint_rows(expected)]
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        fetchrow=AsyncMock(return_value=dict(version=180000, search_path="pg_catalog", schema_sql='"catalog"')),
        fetchval=AsyncMock(return_value=True),
        fetch=AsyncMock(side_effect=[roots, [], [], [], constraints, []]),
    )
    return connection, constraints


@pytest.mark.parametrize("explicit", [False, True])
async def test_layout_preserves_default_and_only_projects_authenticated_bounds(monkeypatch, explicit):
    expected = _expectations()
    connection, constraints = _connection(expected)
    authenticate = AsyncMock(return_value=deepcopy(expected))
    compare = Mock()
    monkeypatch.setattr(bounds, "require_cms_source_history_bounds", authenticate)
    monkeypatch.setattr(layout, "_require_root_layouts", compare)
    options = {"expected_history_bounds": expected} if explicit else {}
    assert await layout.require_cms_canonical_layout(
        connection, catalog_schema="catalog", storage_class=layout.CONTRACT, **options
    ) == dict(contract=layout.CONTRACT, root_count=len(layout._LAYOUTS), evidence="fixed_layout_only")
    if explicit:
        authenticate.assert_awaited_once_with(connection, catalog_schema="catalog", expected=expected)
    else:
        authenticate.assert_not_awaited()
    assert compare.call_args.args[-2] == (constraints[:1] if explicit else constraints)


async def test_bad_bound_proof_stops_before_original_constraint_comparison(monkeypatch):
    expected = _expectations()
    connection, _constraints = _connection(expected)
    authenticate = AsyncMock(side_effect=ValueError("cms_history_bounds_native_pin_changed"))
    compare = Mock()
    monkeypatch.setattr(bounds, "require_cms_source_history_bounds", authenticate)
    monkeypatch.setattr(layout, "_require_root_layouts", compare)
    with pytest.raises(ValueError, match="native_pin_changed"):
        await layout.require_cms_canonical_layout(
            connection, catalog_schema="catalog", storage_class=layout.CONTRACT, expected_history_bounds=expected
        )
    compare.assert_not_called()
    assert all(call.args[0] != layout._CONSTRAINTS_SQL for call in connection.fetch.await_args_list)


async def test_wrong_canonical_old_oid_stops_before_native_bound_read(monkeypatch):
    expected = _expectations()
    connection, _constraints = _connection(expected)
    expected[next(iter(expected))]["old_oid"] = 999
    authenticate = AsyncMock()
    monkeypatch.setattr(bounds, "require_cms_source_history_bounds", authenticate)
    with pytest.raises(ValueError, match="history_bound_changed"):
        await layout.require_cms_canonical_layout(
            connection, catalog_schema="catalog", storage_class=layout.CONTRACT, expected_history_bounds=expected
        )
    authenticate.assert_not_awaited()
