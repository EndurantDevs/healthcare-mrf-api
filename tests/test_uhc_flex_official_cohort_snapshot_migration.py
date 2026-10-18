# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline structural contract checks; native migration remains a separate gate."""

import importlib.util
from pathlib import Path

import pytest

PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20261016060000_uhc_official_cohort_snapshots.py"


def migration():
    spec = importlib.util.spec_from_file_location("cohort_snapshots_test", PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_linear_structural_revision_does_not_retire_guard_authority():
    module = migration()
    assert module.down_revision == "20261016050000_acquisition_validator_argument_types"
    source = PATH.read_text()
    for forbidden in (
        "CREATE FUNCTION",
        "CREATE TRIGGER",
        "DROP TRIGGER",
        "DISABLE TRIGGER",
        "DROP INDEX",
        "DROP CONSTRAINT",
    ):
        assert forbidden not in source
    assert "PARTITION BY LIST(cohort_id)" in source
    assert "ATTACH PARTITION" in source and " ACCESS EXCLUSIVE MODE NOWAIT" in source
    assert "autocommit_block" in source
    assert (
        source.index("VALIDATE CONSTRAINT") < source.index("ACCESS EXCLUSIVE MODE NOWAIT") < source.index(" RENAME TO ")
    )
    assert "_original(connection, schema) == original" in source
    assert "_cohorts(connection, schema) == cohorts" in source


def test_history_bound_covers_the_whole_authenticated_cohort_set():
    module = migration()
    values = ["pdufc_" + letter * 48 for letter in ("a", "b")]
    assert module._scope(values) == "cohort_id IS NOT NULL AND cohort_id IN ('" + values[0] + "','" + values[1] + "')"
    assert module._scope([]) == "false"


@pytest.mark.parametrize(
    "values", [["unsafe"], [None], ["pdufc_" + "a" * 48] * 2, ["pdufc_" + "b" * 48, "pdufc_" + "a" * 48]]
)
def test_history_bound_refuses_inexact_or_ambiguous_scope(values):
    with pytest.raises(RuntimeError, match="migration_changed"):
        migration()._scope(values)


def test_expected_guards_come_from_the_authentic_predecessor():
    module = migration()
    predecessor = module._predecessor()
    body = module._guard_body(predecessor._cohort_guard_function_sql("sample"))
    assert "LOCK TABLE" in body and "public_evidence_npi_valid" in body
    assert "TG_OP <> 'INSERT'" in body


def test_downgrade_requires_explicit_data_migration():
    with pytest.raises(RuntimeError, match="explicit_data_migration"):
        migration().downgrade()


@pytest.mark.parametrize("changed", [False, True])
def test_undeclared_native_dependency_refuses_before_structural_ddl(changed):
    module = migration()

    class Catalog:
        def scalar(self, statement, params):
            sql = str(statement)
            assert all(
                name in sql for name in ("pg_rewrite", "pg_policy", "pg_publication_rel", "pg_depend", "pg_type")
            )
            assert params == {"relation": '"sample"."members"'}
            return changed

    if changed:
        with pytest.raises(RuntimeError, match="migration_changed"):
            module._member_dependencies(Catalog(), '"sample"."members"')
    else:
        module._member_dependencies(Catalog(), '"sample"."members"')


class NativeCatalog:
    def __init__(self, functions, guards):
        self.functions = functions
        self.guards = guards

    def execute(self, statement, parameters):
        if "FROM pg_proc" in str(statement):
            assert parameters["schema"] == "sample"
            return [row for row in self.functions if row[0] in parameters["names"]]
        assert "FROM pg_trigger" in str(statement)
        assert parameters == {"member": 20, "header": 10}
        return self.guards


def native_catalog():
    from db import migration_provider_directory_terminal_root_retirement_guards as retirement

    predecessor = migration()._predecessor()
    generators_by_name = {
        predecessor._COHORT_GUARD: (predecessor._cohort_guard_function_sql, "$guard$"),
        predecessor._MEMBER_GUARD: (predecessor._member_guard_function_sql, "$guard$"),
        predecessor._MEMBER_INSERT_GUARD: (predecessor._member_insert_guard_function_sql, "$guard$"),
        retirement.CHILD_GUARD: (retirement.child_guard_function_sql, "$function$"),
    }
    functions = []
    for oid, name in enumerate(sorted(generators_by_name), 100):
        generator, delimiter = generators_by_name[name]
        body = generator("sample").split("AS " + delimiter, 1)[1].rsplit(delimiter, 1)[0]
        functions.append((name, oid, body, True, "v", ["search_path=pg_catalog"], 99, 0, True, "f"))
    by_name = {function_row[0]: function_row[1] for function_row in functions}
    specifications = (
        (10, "pd_trr_flex_cohort_row", 31, retirement.CHILD_GUARD, None),
        (10, "pd_trr_flex_cohort_truncate", 34, retirement.CHILD_GUARD, None),
        (10, "pd_uhc_flex_npi_cohort_guard", 31, predecessor._COHORT_GUARD, None),
        (10, "pd_uhc_flex_npi_cohort_guard_truncate", 34, predecessor._COHORT_GUARD, None),
        (20, "pd_uhc_flex_npi_member_guard", 27, predecessor._MEMBER_GUARD, None),
        (20, "pd_uhc_flex_npi_member_guard_truncate", 34, predecessor._MEMBER_GUARD, None),
        (20, "pd_uhc_flex_npi_member_insert_guard", 4, predecessor._MEMBER_INSERT_GUARD, "new_rows"),
    )
    guards = [
        (oid, name, events, "A", by_name[function], True, True, new, None)
        for oid, name, events, function, new in specifications
    ]
    return NativeCatalog(functions, guards)


def test_authentic_retirement_header_guards_are_preserved_in_structural_receipt():
    catalog = native_catalog()
    receipt = migration()._guards(catalog, "sample", 20, 10)
    assert receipt == {"functions": catalog.functions, "guards": catalog.guards}
    assert [row[1] for row in receipt["guards"][:2]] == ["pd_trr_flex_cohort_row", "pd_trr_flex_cohort_truncate"]


@pytest.mark.parametrize(
    ("column", "value"),
    [
        (0, 20),
        (1, "unknown_header_guard"),
        (2, 27),
        (3, "O"),
        (4, 12345),
        (5, False),
        (6, False),
        (7, "new_rows"),
        (8, "old_rows"),
    ],
)
def test_retirement_header_guard_topology_changes_refuse(column, value):
    catalog = native_catalog()
    changed_columns = list(catalog.guards[0])
    changed_columns[column] = value
    catalog.guards[0] = tuple(changed_columns)
    with pytest.raises(RuntimeError, match="migration_changed"):
        migration()._guards(catalog, "sample", 20, 10)


@pytest.mark.parametrize("change", ["missing", "extra"])
def test_retirement_header_guard_set_must_remain_exact(change):
    catalog = native_catalog()
    if change == "missing":
        catalog.guards.pop(0)
    else:
        catalog.guards.append(catalog.guards[0])
    with pytest.raises(RuntimeError, match="migration_changed"):
        migration()._guards(catalog, "sample", 20, 10)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        (2, "BEGIN RETURN NEW; END;"),
        (3, False),
        (4, "s"),
        (5, ["search_path=public"]),
        (7, 1),
        (8, False),
        (9, "p"),
    ],
)
def test_retirement_child_function_contract_changes_refuse(column, value):
    catalog = native_catalog()
    changed_columns = list(catalog.functions[0])
    assert changed_columns[0] == "guard_provider_directory_terminal_root_retirement_child"
    changed_columns[column] = value
    catalog.functions[0] = tuple(changed_columns)
    with pytest.raises(RuntimeError, match="migration_changed"):
        migration()._guards(catalog, "sample", 20, 10)


@pytest.mark.parametrize("change", ["missing", "overload"])
def test_retirement_child_function_set_must_remain_exact(change):
    catalog = native_catalog()
    if change == "missing":
        catalog.functions.pop(0)
    else:
        catalog.functions.append(catalog.functions[0])
    with pytest.raises(RuntimeError, match="migration_changed"):
        migration()._guards(catalog, "sample", 20, 10)
