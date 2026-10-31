# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Terminal execution requires hardened guards while old allocation remains compatible."""

from copy import deepcopy

import pytest

from db.migration_cms_replay_resolution import GUARD, replay_guard_bodies
from db.migration_cms_retirement_resolution import retirement_resolution_bodies
from db.migration_cms_snapshot_allocator import generic_finalization_bodies
from db.migration_cms_terminal_resolution import GUARD as TERMINAL_GUARD
from db.migration_cms_terminal_resolution import terminal_guard_bodies
from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_transition_routines as transition
from tests.test_provider_directory_cms_callee_custody import _catalog_definition


def terminal_guard_routines(schema="catalog", namespace=42):
    """Use source-rendered bodies and existing native attribute fixture construction."""
    contracts = transition._contracts(schema, "preparation_schema")
    bodies_by_name = {
        "guard_pd_generic_dataset_finalization": generic_finalization_bodies(schema)[1],
        GUARD: replay_guard_bodies(schema)[1],
    }
    bodies_by_name.update({name: typed for name, (_old, typed) in retirement_resolution_bodies(schema).items()})
    bodies_by_name[TERMINAL_GUARD] = terminal_guard_bodies(schema)[1]
    bodies_by_name["guard_pd_dataset_candidate_parent"] = _cms_canonical_copy_guard_body(schema, "preparation_schema")
    routines = []
    for oid, (name, body) in enumerate(bodies_by_name.items(), 3000):
        settings = list(callables._CALLEE_SETTINGS.get(name, ["search_path=pg_catalog"]))
        spec = (*contracts[name][:-1], settings, body)
        definition = _catalog_definition(name, spec, 105, oid)
        definition["pronamespace"] = str(namespace)
        routines.append(dict(oid=oid, native=dict(definition=definition, language={"lanname": spec[3]})))
    return routines


def _require(routines):
    return callables.require_cms_terminal_guard_resolution(
        routines, catalog_schema="catalog", catalog_schema_oid=42, preparation_schema="preparation_schema"
    )


def test_hardened_executable_frontier_preserves_inputs():
    routines = terminal_guard_routines()
    before = deepcopy(routines)
    assert len(routines) == 10
    assert _require(routines) is None
    assert routines == before


@pytest.mark.parametrize("guard", range(10))
@pytest.mark.parametrize("fault", ["old", "missing", "duplicate", "body", "namespace", "security", "settings", "type"])
def test_terminal_guard_rejects_drift(guard, fault):
    routines = terminal_guard_routines()
    definition = routines[guard]["native"]["definition"]
    if fault == "old":
        if definition["proname"] == TERMINAL_GUARD:
            definition["prosrc"] = terminal_guard_bodies("catalog")[0]
        elif definition["proname"] == "guard_pd_dataset_candidate_parent":
            definition["prosrc"] = _canonical_copy_guard_bodies("catalog", "preparation_schema")[1]
        elif guard < 2:
            renderer = generic_finalization_bodies if guard == 0 else replay_guard_bodies
            definition["prosrc"] = renderer("catalog")[0]
        else:
            definition["prosrc"] = retirement_resolution_bodies("catalog")[definition["proname"]][0]
    elif fault == "missing":
        routines.pop(guard)
    elif fault == "duplicate":
        routines.append(deepcopy(routines[guard]))
    elif fault == "body":
        definition["prosrc"] += "\n-- unreviewed"
    else:
        key, value = {
            "namespace": ("pronamespace", "999"),
            "security": ("prosecdef", not definition["prosecdef"]),
            "settings": ("proconfig", ["search_path=public"]),
            "type": ("prorettype", "2278"),
        }[fault]
        definition[key] = value
    with pytest.raises(ValueError, match="terminal_guard_resolution_required|definition_changed"):
        _require(routines)


@pytest.mark.parametrize("variant", ["original", "generic", "old_preparation"])
def test_terminal_parent_requires_cms_receipt_and_bound_preparation(variant):
    routines = terminal_guard_routines()
    definition = routines[-1]["native"]["definition"]
    assert definition["proname"] == "guard_pd_dataset_candidate_parent"
    if variant == "old_preparation":
        body = _cms_canonical_copy_guard_body("catalog", "old_preparation_schema")
    else:
        body = _canonical_copy_guard_bodies("catalog", "preparation_schema")[variant == "generic"]
    definition["prosrc"] = body
    with pytest.raises(ValueError, match="terminal_guard_resolution_required"):
        _require(routines)
