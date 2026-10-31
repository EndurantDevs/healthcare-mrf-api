# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary staging CREATE requires the complete protected routine postimage."""

from copy import deepcopy
from unittest.mock import Mock

import pytest

from process import provider_directory_cms_canonical_writer_fence as writers
from process import provider_directory_cms_routine_bodies as bodies
from tests.test_provider_directory_cms_read_definer_binding import (
    authentic_callees as authentic_callees,
)
from tests.test_provider_directory_cms_read_definer_binding import (
    base_case as base_case,
)
from tests.test_provider_directory_cms_read_definer_binding import (
    bound_case as bound_case,
)
from tests.test_provider_directory_cms_read_definer_binding import (
    source_case as source_case,
)
from tests.test_provider_directory_cms_routine_bodies import (
    _ARGUMENTS,
)
from tests.test_provider_directory_cms_routine_bodies import (
    body_rows as body_rows,
)
from tests.test_provider_directory_cms_routine_bodies import (
    original_guards as original_guards,
)
from tests.test_provider_directory_cms_routine_bodies import (
    routines as routines,
)


def test_all_thirteen_exact_bodies_are_required_without_mutating_custody(body_rows):
    current = bodies.cms_terminal_body_postimage(body_rows, **_ARGUMENTS)
    before = deepcopy(current)
    writers._require_creator_bodies(dict(arguments=_ARGUMENTS), current)
    assert current == before
    for predecessor in body_rows:
        if predecessor["native"]["definition"]["proname"] not in bodies._targets(
            _ARGUMENTS["catalog_schema"], _ARGUMENTS["preparation_schema"]
        ):
            continue
        reverted = deepcopy(current)
        index = next(index for index, entry in enumerate(current) if entry["oid"] == predecessor["oid"])
        reverted[index] = deepcopy(predecessor)
        with pytest.raises(ValueError, match="creator_body_postimage_required"):
            writers._require_creator_bodies(dict(arguments=_ARGUMENTS), reverted)


def test_bound_dependency_query_removes_only_create_and_preserves_other_writer_guards():
    removed = " AND NOT EXISTS(SELECT FROM actors a WHERE pg_catalog.has_schema_privilege(a.oid,$3::text,'CREATE'))\n"
    assert writers._DEPENDENCIES_SQL.count(removed) == 1
    assert writers._BOUND_DEPENDENCIES_SQL == writers._DEPENDENCIES_SQL.replace(removed, "", 1)
    assert "pg_rewrite" in writers._BOUND_DEPENDENCIES_SQL
    assert "'REFERENCES,TRIGGER'" in writers._BOUND_DEPENDENCIES_SQL
    assert "has_any_column_privilege" in writers._BOUND_DEPENDENCIES_SQL
    assert "$3" not in writers._BOUND_DEPENDENCIES_SQL


@pytest.fixture
def creator_case(bound_case, monkeypatch):
    case = bound_case
    case.state.dependencies = False
    case.bounded_dependencies = True
    original = case.connection.fetchval.side_effect

    async def dependency_check(query, *arguments):
        if query == writers._BOUND_DEPENDENCIES_SQL:
            case.state.events.append(("fetchval", query))
            assert arguments == ([1, 2, 3], "migration")
            return case.bounded_dependencies
        return await original(query, *arguments)

    case.connection.fetchval.side_effect = dependency_check
    case.body_check = Mock()
    monkeypatch.setattr(writers, "_require_creator_bodies", case.body_check)
    return case


async def test_source_v3_creators_retain_other_writer_gates(creator_case):
    from tests.test_provider_directory_cms_canonical_writer_fence import _acquire, _prepare

    case = creator_case
    preparation = await _prepare(case)
    fence = await _acquire(case, preparation)
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    assert case.body_check.call_count == 3
    for native, observed in (call.args for call in case.body_check.call_args_list):
        assert native["retained_routines_sha256"] == case.arguments["cms_profile"]["routines_sha256"]
        assert observed == case.state.protected_routines
    queries = [query for _method, query in case.state.events]
    for query in (
        writers._OWNERS_SQL,
        writers._DEPENDENCIES_SQL,
        writers.custody._CALLABLE_OWNER_SQL,
        writers._DEFINER_AUTHORITY_SQL,
        writers._BOUND_DEPENDENCIES_SQL,
        writers._FOREIGN_SQL,
    ):
        assert queries.count(query) == 3
    assert queries.index(writers._DEFINER_AUTHORITY_SQL) < queries.index(writers._BOUND_DEPENDENCIES_SQL)


@pytest.mark.parametrize("fault", ["raw_pin", "body", "owner", "definer", "indirect"])
async def test_creator_route_never_waives_other_authenticated_checks(creator_case, fault):
    from tests.test_provider_directory_cms_canonical_writer_fence import _prepare

    case = creator_case
    if fault == "raw_pin":
        case.state.protected_routines[-1]["native"]["dependencies"].append({"refobjid": "999"})
    elif fault == "body":
        case.body_check.side_effect = ValueError("creator_body_postimage_required")
    elif fault == "owner":
        case.state.callable_owners = False
    elif fault == "definer":
        case.state.definers = False
    else:
        case.bounded_dependencies = False
    with pytest.raises(ValueError):
        await _prepare(case)
    if fault in {"raw_pin", "owner", "definer"}:
        case.body_check.assert_not_called()
    assert not any(query == writers._FOREIGN_SQL for _method, query in case.state.events)


async def test_legacy_creator_refuses_before_callable_or_postimage_checks(base_case, monkeypatch):
    from tests.test_provider_directory_cms_canonical_writer_fence import _prepare

    case = base_case
    case.state.dependencies = False
    body_check = Mock()
    monkeypatch.setattr(writers, "_require_creator_bodies", body_check)
    with pytest.raises(ValueError, match="indirect_writer_reachable"):
        await _prepare(case)
    body_check.assert_not_called()
    assert not any(query == writers._BOUND_DEPENDENCIES_SQL for _method, query in case.state.events)
