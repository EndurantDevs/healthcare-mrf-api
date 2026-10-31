# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only explicit text dispatch may change the six reviewed retirement bodies."""

from copy import deepcopy

import pytest

from db import migration_cms_retirement_resolution as migration
from process.provider_directory_cms_callable_custody import _cms_callee_contracts


@pytest.mark.parametrize("schema", ["sample", "alternate_catalog", "Mixed_Catalog23"])
def test_exact_current_bodies_change_only_explicit_argument_casts(schema):
    pairs_by_name = migration.retirement_resolution_bodies(schema)
    assert set(pairs_by_name) == set(migration.ORIGINAL_SHA256)
    assert len(pairs_by_name) == 6
    assert [new.count("::pg_catalog.text") - old.count("::pg_catalog.text") for old, new in pairs_by_name.values()] == [
        26,
        26,
        2,
        2,
        6,
        3,
    ]
    for name, (original, hardened) in pairs_by_name.items():
        assert migration._digest(original, schema) == migration.ORIGINAL_SHA256[name]
        assert "::pg_catalog.text" not in original
        assert hardened.replace("::pg_catalog.text", "") == original
    assert len(_cms_callee_contracts()) == 32
    assert set(pairs_by_name) & _cms_callee_contracts().keys() == {
        migration.evidence.EVIDENCE_FUNCTION,
        migration.v2.EVIDENCE_FUNCTION,
    }


@pytest.mark.parametrize("position", range(6))
@pytest.mark.parametrize("fault", ["body", "delimiter"])
def test_unknown_predecessor_refuses_instead_of_recapturing_body(monkeypatch, position, fault):
    statements = list(migration._original_statements("sample"))
    if fault == "body":
        statements[position] = statements[position].replace("AS $function$", "AS $function$\n-- changed", 1)
    else:
        statements[position] = statements[position].replace("AS $function$", "AS $other$", 1)
    monkeypatch.setattr(migration, "_original_statements", lambda schema: tuple(statements))
    with pytest.raises(RuntimeError, match="predecessor_changed"):
        migration.retirement_resolution_bodies("sample")


@pytest.mark.parametrize(
    "position,previous",
    [
        (0, migration.evidence.evidence_function_sql),
        (4, migration.v2.parent_guard_function_sql),
        (5, migration.guards.import_run_guard_function_sql),
    ],
)
def test_historical_variants_need_separate_explicit_review(monkeypatch, position, previous):
    statements = list(migration._original_statements("sample"))
    statements[position] = previous("sample")
    monkeypatch.setattr(migration, "_original_statements", lambda schema: tuple(statements))
    with pytest.raises(RuntimeError, match="predecessor_changed"):
        migration.retirement_resolution_bodies("sample")


def test_caller_cannot_mutate_the_next_rendering():
    original = migration.retirement_resolution_bodies("sample")
    before = deepcopy(original)
    original.clear()
    assert migration.retirement_resolution_bodies("sample") == before


def test_unbound_or_repeated_callsite_refuses(monkeypatch):
    calls = migration._evidence_calls("sample")
    monkeypatch.setattr(migration, "_evidence_calls", lambda schema: [*calls, calls[0]])
    with pytest.raises(RuntimeError, match="callsite_changed"):
        migration.retirement_resolution_bodies("sample")
