# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source definitions stay fixed independently of native callable observations."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_allocator_callables as checks


@pytest.fixture
def callable_case(monkeypatch):
    bodies = checks._source_bodies("synthetic", "preparation")
    owners = dict.fromkeys(checks._SPECS, 41)
    rows = []
    for index, name in enumerate(checks._SPECS, 1):
        definition, language = checks._expected_properties(name, 31, owners[name], bodies[name])
        definition.update(oid=str(index), prolang="14" if language == "sql" else "15", proacl=None)
        rows.append(
            dict(definition=definition, language=language, trusted_language=True, acl=[[41, 41, "EXECUTE", False]])
        )
    connection = SimpleNamespace(
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        execute=AsyncMock(return_value="SET"),
        fetchval=AsyncMock(side_effect=["pg_catalog, pg_temp", 31]),
        fetch=AsyncMock(side_effect=[rows, []]),
    )
    guard = AsyncMock()
    monkeypatch.setattr(checks, "verify_generic_source_canonical_copy_guard", guard)
    return SimpleNamespace(connection=connection, owners=owners, rows=rows, guard=guard, bodies=bodies)


async def _check(case):
    return await checks.require_cms_allocator_callables(
        case.connection, catalog_schema="synthetic", preparation_schema="preparation", expected_owner_oids=case.owners
    )


def test_source_extraction_is_fixed():
    bodies = checks._source_bodies("synthetic", "preparation")
    assert '"synthetic".pd_dataset_candidate_tables(kind)' in bodies["validate_pd_dataset_bulk_relationships"]
    assert '"preparation"."preparation"' in bodies["guard_pd_dataset_candidate_parent"]
    assert "cms-npd-noncurrent-candidate-v2" in bodies["guard_pd_dataset_candidate_parent"]
    assert "cms-npd-noncurrent-candidate-v1" not in bodies["guard_pd_dataset_candidate_parent"]


@pytest.mark.parametrize("fault", ["missing", "duplicate", "header", "delimiter"])
def test_source_template_changes_refuse(monkeypatch, fault):
    original = checks.candidate_functions("synthetic")
    declaration = 'CREATE FUNCTION "synthetic".pd_dataset_candidate_tables('
    if fault == "missing":
        changed = original.replace(declaration, 'CREATE FUNCTION "synthetic".other_tables(', 1)
    elif fault == "duplicate":
        changed = original + "\n" + original
    elif fault == "header":
        changed = original.replace("(kind text) RETURNS text[]", "(kind text DEFAULT '') RETURNS text[]", 1)
    else:
        changed = original.replace("$fn$;", "$changed$;", 1).replace("$fn$;", "$changed$;", 1)
        changed = changed.replace("$fn$;", "$changed$;", 1).replace("$fn$;", "$changed$;", 1)
    monkeypatch.setattr(checks, "candidate_functions", lambda _schema: changed)
    with pytest.raises(ValueError, match="source_template_changed"):
        checks._source_bodies("synthetic", "preparation")


@pytest.mark.asyncio
async def test_complete_definitions_require_cms_guard(callable_case):
    case = callable_case
    result = await _check(case)
    assert result["functions"] == [row["definition"] for row in case.rows]
    case.guard.assert_awaited_once_with(
        case.connection, "synthetic", preparation_schema="preparation", require_cms=True
    )
    case.connection.execute.assert_awaited_once_with("SET LOCAL search_path = pg_catalog, pg_temp")


@pytest.mark.asyncio
@pytest.mark.parametrize("field", list(checks._expected_properties("pd_dataset_candidate_tables", 31, 41, "fixed")[0]))
async def test_each_proc_property_is_exact(callable_case, field):
    case = callable_case
    case.rows[0]["definition"][field] = "unapproved"
    with pytest.raises(ValueError, match="definition_changed|function_set_changed"):
        await _check(case)
    case.guard.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["extra_property", "language", "untrusted_language", "acl", "grantable"])
async def test_native_bypass_properties_refuse(callable_case, fault):
    case = callable_case
    row = case.rows[0]
    if fault == "extra_property":
        row["definition"]["future_property"] = True
    elif fault == "language":
        row["language"] = "unapproved"
    elif fault == "untrusted_language":
        row["trusted_language"] = False
    elif fault == "acl":
        row["acl"].append([0, 41, "EXECUTE", False])
    else:
        row["acl"][0][-1] = True
    with pytest.raises(ValueError, match="definition_changed|acl_changed"):
        await _check(case)
    case.guard.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "overload", "replaced_name"])
async def test_exact_function_set_refuses(callable_case, fault):
    case = callable_case
    if fault == "missing":
        case.rows.pop()
    elif fault == "overload":
        case.rows.append(deepcopy(case.rows[0]))
    else:
        case.rows[-1] = deepcopy(case.rows[0])
    with pytest.raises(ValueError, match="function_set_changed"):
        await _check(case)


@pytest.mark.asyncio
async def test_non_native_dependency_refuses(callable_case):
    case = callable_case
    case.connection.fetch.side_effect = [case.rows, [dict(trusted=False)]]
    with pytest.raises(ValueError, match="dependency_changed"):
        await _check(case)
    case.guard.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owners", [None, {}, {"unknown": 41}, dict.fromkeys(checks._SPECS, True)])
async def test_owner_context_required_before_sql(callable_case, owners):
    case = callable_case
    case.owners = owners
    with pytest.raises(ValueError, match="owners_required"):
        await _check(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_transaction_required_before_sql(callable_case):
    case = callable_case
    case.connection.is_in_transaction.return_value = False
    with pytest.raises(ValueError, match="transaction_required"):
        await _check(case)
    case.connection.execute.assert_not_awaited()
