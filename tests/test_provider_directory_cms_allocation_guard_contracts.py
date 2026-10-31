# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render existing optional allocation guards from their actual latest builders."""

import re
from copy import deepcopy
from types import SimpleNamespace

import pytest

from db import migration_provider_directory_first_acquisition_retirement as retirement
from process import provider_directory_cms_callable_custody as custody
from tests.test_provider_directory_cms_callee_custody import (
    _catalog_definition,
    _catalog_rows,
    _original_allocation_pins,
    _parse_statements,
)
from tests.test_source_validator_argument_types_migration import _load

_GUARD_NAMES = (
    "guard_cms_npd_covered_truncate",
    "guard_cms_npd_published_resource",
    "guard_pd_endpoint_dataset_subset_replay_evidence",
    "guard_provider_directory_endpoint_dataset_admission_truncate",
    "guard_provider_directory_reviewed_subset_activation_dataset",
    "guard_provider_directory_subset_abandonment_child",
    "guard_provider_directory_subset_abandonment_dataset",
    "guard_provider_directory_terminal_root_retirement_parent",
    "guard_tin_npi_connector_dataset_resource",
)


def _guard_statements(monkeypatch, schema):
    seal = _load("20260812020000_provider_directory_endpoint_dataset_admission_seal.py")
    activation = _load("20260808200000_provider_directory_reviewed_subset_activation.py")
    abandonment = _load("20260809000000_provider_directory_subset_abandonment.py")
    latest = _load("20260811120000_provider_directory_reviewed_subset_v5_http410_disposition.py")
    subset = _load("20260808190000_provider_directory_subset_completion_proof.py")
    statements = [
        seal._truncate_function_sql(schema),
        seal._replay_guard_function_sql(schema),
        activation._dataset_guard_function_sql(schema, replace_existing=True, reviewed_root_policy_aware=True),
        abandonment._child_guard_sql(schema),
        latest._dataset_guard_sql(schema),
        retirement.parent_sql(schema),
        subset._dataset_resource_guard_sql(schema, subset_aware=True),
    ]
    for filename, builder in (
        ("20260930020000_cms_npd_serving_coverage.py", "_create_resource_guards"),
        ("20260930090000_cms_npd_candidate_coverage.py", "_freeze_candidate_inputs"),
    ):
        migration = _load(filename)
        monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
        getattr(migration, builder)('"' + schema + '"')
    return statements


def _guard_specs(monkeypatch, schema):
    declarations = re.compile(r'CREATE(?: OR REPLACE)? FUNCTION "' + schema + r'"\."?(\w+)"?\(')
    statements = []
    for statement in _guard_statements(monkeypatch, schema):
        match = declarations.search(statement)
        if match and match[1] in _GUARD_NAMES:
            statements.append(statement)
    functions_by_name = _parse_statements(statements, schema)
    assert functions_by_name.keys() == set(_GUARD_NAMES) == custody._ORIGINAL_ALLOCATION_GUARDS.keys()
    return functions_by_name


@pytest.fixture
def original_guards(monkeypatch):
    schema = "snapshot_schema"
    rows = _catalog_rows(schema)
    for oid, (name, spec) in enumerate(sorted(_guard_specs(monkeypatch, schema).items()), 3000):
        rows.append(
            dict(
                definition=_catalog_definition(name, spec, 109, oid), language=spec[3], acl=[[0, 109, "EXECUTE", False]]
            )
        )
    return rows


@pytest.mark.parametrize("schema", ["snapshot_schema", "alternate_catalog"])
def test_latest_source_bodies_and_native_attributes_match_exact_contracts(monkeypatch, schema):
    original = custody._original_callee_contracts(schema, "preparation_schema")
    for name, spec in _guard_specs(monkeypatch, schema).items():
        assert original[name] == (*spec[:-2], custody._body_digest(spec[-1], schema))
        assert spec[-2] == ["search_path=pg_catalog"]
        calls = set(re.findall(r'"' + schema + r'"\."?(\w+)"?\s*\(', spec[-1]))
        assert calls <= custody._cms_callee_contracts().keys()
    assert len(custody._cms_callee_contracts()) == 32


def test_all_nine_optional_guards_preserve_original_pins_and_acl(original_guards):
    before = deepcopy(original_guards)
    verified = custody.require_original_cms_allocation_callees(
        original_guards, **_original_allocation_pins(original_guards)
    )
    assert len(verified) == 41 and original_guards == before


@pytest.mark.parametrize("name", _GUARD_NAMES)
def test_each_guard_is_optional_without_weakening_the_mandatory_frontier(original_guards, name):
    rows = [
        row
        for row in original_guards
        if row["definition"]["proname"] not in _GUARD_NAMES or row["definition"]["proname"] == name
    ]
    assert len(custody.require_original_cms_allocation_callees(rows, **_original_allocation_pins(rows))) == 33


@pytest.mark.parametrize("name", _GUARD_NAMES)
@pytest.mark.parametrize("damage", ["body", "security", "settings", "acl", "owner", "oid"])
def test_optional_guards_refuse_source_and_protected_pin_changes(original_guards, name, damage):
    rows = deepcopy(original_guards)
    pins = _original_allocation_pins(rows)
    row = next(row for row in rows if row["definition"]["proname"] == name)
    definition = row["definition"]
    if damage == "body":
        definition["prosrc"] += "\n-- changed"
    elif damage == "acl":
        row["acl"].append([999, 109, "EXECUTE", False])
    else:
        field, value = {
            "security": ("prosecdef", not definition["prosecdef"]),
            "settings": ("proconfig", ["search_path=pg_catalog", "TimeZone=UTC"]),
            "owner": ("proowner", "999"),
            "oid": ("oid", "999"),
        }[damage]
        definition[field] = value
    if damage in {"body", "security", "settings"}:
        pins = _original_allocation_pins(rows)
    with pytest.raises(ValueError, match="definition_changed|original_allocation_pin_changed"):
        custody.require_original_cms_allocation_callees(rows, **pins)


def test_an_unrecognized_tenth_guard_is_not_accepted_as_an_observed_contract(original_guards):
    rows = deepcopy(original_guards)
    unknown = deepcopy(rows[-1])
    unknown["definition"].update(proname="unrecognized_guard", oid="4000")
    rows.append(unknown)
    with pytest.raises(ValueError, match="original_allocation_pins_required"):
        custody.require_original_cms_allocation_callees(rows, **_original_allocation_pins(rows))


def test_superseded_original_connector_guard_is_refused(monkeypatch, original_guards):
    statements = []
    migration = _load("20260729110000_tin_npi_connector.py")
    monkeypatch.setattr(migration, "_schema", lambda: "snapshot_schema")
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    migration.upgrade()
    name = "guard_tin_npi_connector_dataset_resource"
    statement = next(sql for sql in statements if 'CREATE FUNCTION "snapshot_schema"."' + name + '"(' in sql)
    old_body = _parse_statements([statement], "snapshot_schema")[name][-1]
    row = next(row for row in original_guards if row["definition"]["proname"] == name)
    row["definition"]["prosrc"] = old_body
    with pytest.raises(ValueError, match="definition_changed"):
        custody.require_original_cms_allocation_callees(original_guards, **_original_allocation_pins(original_guards))
