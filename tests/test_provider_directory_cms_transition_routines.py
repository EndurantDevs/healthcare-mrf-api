# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline migration definitions, exact ABI and complete-inventory dispatch refusals."""

import json
import re
from copy import deepcopy
from types import SimpleNamespace

import pytest

from db import migration_cms_retirement_resolution as retirement
from db.migration_cms_noncurrent_seal import GUARD, lifecycle_guard_bodies
from db.migration_cms_replay_resolution import replay_guard_bodies
from db.migration_cms_snapshot_allocator import allocator_bodies, generic_finalization_bodies
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_dependency_policy as dependencies
from process import provider_directory_cms_transition_routines as transition
from process import provider_directory_source_callable_custody as source
from tests.test_acquisition_validator_argument_types_migration import _current_functions as acquisition_functions
from tests.test_acquisition_validator_argument_types_migration import _migration as acquisition_migration
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import (
    _authentic_functions,
    _catalog_definition,
    _catalog_rows,
    _parse_statements,
)
from tests.test_provider_directory_generic_source_release_checks import legacy_function_rows
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_source_validator_argument_types_migration import _load

_SCHEMA = "snapshot_schema"
_NAMESPACE = 42
_PREPARATION_SCHEMA = "preparation_schema"
_MIGRATIONS = (
    ("20260930020000_cms_npd_serving_coverage.py", ("_create_evidence_guards", "_create_resource_guards")),
    ("20260930040000_cms_npd_resource_witness.py", ("_create_insert_guard", "_create_mutation_guard")),
    ("20260930050000_cms_npd_relationship.py", ("_create_guards", "_create_receipt_guards")),
    ("20260930070000_provider_directory_entity_redirect.py", ("_protect_reviewed_parents",)),
    ("20260930090000_cms_npd_candidate_coverage.py", ("_create_candidate_seal", "_freeze_candidate_inputs")),
)


def _migration_functions(monkeypatch, schema):
    """Run only SQL constructors with a recorder, never a database operation."""
    statements = []
    for filename, builders in _MIGRATIONS:
        migration = _load(filename)
        monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
        for builder in builders:
            getattr(migration, builder)('"' + schema + '"')
    constructors = (
        retirement.guards.valid_function_sql,
        retirement.v2.valid_function_sql,
        retirement.first.run_sql,
        retirement.guards.run_retired_function_sql,
    )
    statements.extend(constructor(schema) for constructor in constructors)
    functions = _parse_statements(
        [
            statement
            for statement in statements
            if statement.lstrip().startswith(("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION"))
        ],
        schema,
    )
    wanted = set(transition._INVOKER_BODIES) | {
        "provider_directory_terminal_root_retirement_valid",
        "provider_directory_terminal_root_retirement_v2_valid",
        "guard_provider_directory_terminal_root_retirement_run",
        "provider_directory_terminal_root_run_retired",
    }
    return {name: functions[name] for name in wanted}


def _raw(row, schema=_SCHEMA, namespace=_NAMESPACE):
    definition = deepcopy(row["definition"])
    language = row["language"]
    definition.update(prolang="14" if language == "sql" else "13563", pronamespace=str(namespace))
    return dict(
        oid=int(definition["oid"]),
        native=dict(
            definition=definition,
            language=dict(oid=definition["prolang"], lanname=language),
            namespace=[namespace, schema, 104, None],
        ),
    )


def _extra_rows(monkeypatch, schema=_SCHEMA):
    return [
        _raw(dict(definition=_catalog_definition(name, spec, 105, oid), language=spec[3]), schema)
        for oid, (name, spec) in enumerate(sorted(_migration_functions(monkeypatch, schema).items()), 5000)
    ]


def _verify(routines, schema=_SCHEMA):
    return transition.require_cms_transition_routine_definitions(
        routines, catalog_schema=schema, catalog_schema_oid=_NAMESPACE, preparation_schema=_PREPARATION_SCHEMA
    )


@pytest.mark.parametrize("schema", [_SCHEMA, "Alternate_Catalog"])
def test_all_sixteen_additions_match_exact_source_constructor_body_and_abi(monkeypatch, schema):
    specifications = transition._contracts(schema, _PREPARATION_SCHEMA)
    functions = _migration_functions(monkeypatch, schema)
    assert len(functions) == 16 and len(specifications) == 155
    for name, spec in functions.items():
        assert specifications[name] == (*spec[:-2], callables._body_digest(spec[-1], schema))
    routines = _extra_rows(monkeypatch, schema)
    before = deepcopy(routines)
    assert _verify(routines, schema) is None and routines == before


def test_full_fixed_frontier_optional_guards_and_additions_are_checked_together(original_guards, monkeypatch):
    rows = [_raw(row) for row in original_guards] + _extra_rows(monkeypatch)
    before = deepcopy(rows)
    assert len(rows) == 57
    assert _verify(rows) is None and rows == before


def test_callback_frontier_reuses_exact_existing_source_signature_contracts():
    specs = transition._contracts(_SCHEMA, _PREPARATION_SCHEMA)
    assert len(dependencies._CALLBACK_CALLEES) == 14
    for name, arguments in dependencies._CALLBACK_CALLEES.items():
        signature = name + "(" + ",".join(arguments) + ")"
        assert specs[name] == source._CONTRACTS[signature]
    assert dependencies._CALLBACK_CALLEES["ptg_wave_canonical_json_ascii_v1"] == ("jsonb",)
    assert specs["ptg_wave_canonical_json_ascii_v1"] != source._CONTRACTS["ptg_wave_canonical_json_ascii_v1(json)"]


def _checkpoint_function(schema):
    migration = _load("20260810010000_provider_directory_reviewed_subset_terminal_disposition.py")
    name = migration._abandonment()._CHECKPOINT_GUARD
    return name, _parse_statements([migration._checkpoint_guard_sql(schema)], schema)[name]


def _checkpoint_row(schema):
    name, spec = _checkpoint_function(schema)
    return _raw(dict(definition=_catalog_definition(name, spec, 105, 7000), language=spec[3]), schema)


@pytest.mark.parametrize("schema", [_SCHEMA, "Mixed_Catalog"])
def test_checkpoint_dispatcher_matches_latest_source_abi_settings_and_body(schema):
    name, spec = _checkpoint_function(schema)
    assert spec[:-1] == ((), "trigger", (), "plpgsql", "v", False, "u", True, ["search_path=pg_catalog"])
    assert transition._contracts(schema, _PREPARATION_SCHEMA)[name] == (
        *spec[:-2],
        callables._body_digest(spec[-1], schema),
    )
    row = _checkpoint_row(schema)
    before = deepcopy(row)
    assert _verify([row], schema) is None and row == before


@pytest.mark.parametrize("schema", [_SCHEMA, "Mixed_Catalog"])
def test_later_disposition_migrations_retain_same_checkpoint_constructor(schema):
    terminal = _load("20260810010000_provider_directory_reviewed_subset_terminal_disposition.py")
    direct = _load("20260810110000_provider_directory_reviewed_subset_direct_v4_disposition.py")
    latest = _load("20260811120000_provider_directory_reviewed_subset_v5_http410_disposition.py")
    assert terminal._checkpoint_guard_sql(schema) == direct._terminal()._checkpoint_guard_sql(schema)
    assert terminal._checkpoint_guard_sql(schema) == latest._direct()._terminal()._checkpoint_guard_sql(schema)


@pytest.mark.parametrize("schema", [_SCHEMA, "Mixed_Catalog"])
@pytest.mark.parametrize(
    "field,value",
    [
        ("prosrc", "BEGIN RETURN NEW; END"),
        ("prorettype", "16"),
        ("pronargs", 1),
        ("proargtypes", ["25"]),
        ("proargnames", ["extra"]),
        ("pronargdefaults", 1),
        ("provariadic", "25"),
        ("proconfig", ["search_path=public"]),
        ("prosecdef", False),
        ("provolatile", "s"),
        ("proisstrict", True),
        ("proparallel", "s"),
        ("proleakproof", True),
    ],
)
def test_checkpoint_dispatcher_rejects_body_abi_and_settings_mutations(schema, field, value):
    row = _checkpoint_row(schema)
    row["native"]["definition"][field] = value
    with pytest.raises(ValueError, match="definition_changed"):
        _verify([row], schema)


def test_checkpoint_whitespace_is_not_a_second_approved_body():
    row = _checkpoint_row(_SCHEMA)
    row["native"]["definition"]["prosrc"] += "\n"
    with pytest.raises(ValueError, match="definition_changed"):
        _verify([row])


def test_checkpoint_invocations_and_literal_closure_are_already_fixed_and_authenticated():
    from process import provider_directory_source_checkpoint_catalog as checkpoints

    name, spec = _checkpoint_function(_SCHEMA)
    invoked_functions = {
        entry[-1] for entry in checkpoints._GUARDS["provider_directory_pagination_checkpoint"].values()
    }
    assert name in invoked_functions
    expression = re.compile(r'"snapshot_schema"\."([a-z0-9_]+)"\s*\(')
    direct_callees = set(expression.findall(spec[-1]))
    assert direct_callees == {
        "provider_directory_subset_abandonment_valid",
        "provider_directory_subset_terminal_disposition_valid",
    }
    functions = _authentic_functions(_SCHEMA)
    fixed_callees = set(callables._cms_callee_contracts())
    visited, pending = set(), list(direct_callees)
    while pending:
        function = pending.pop()
        if function in visited:
            continue
        assert function in fixed_callees and function in functions
        visited.add(function)
        pending.extend(expression.findall(functions[function][-1]))
    assert len(fixed_callees) == 32 and set(dependencies.custody._CALLEE_FRONTIER) == fixed_callees
    assert "target_dataset_id text;" in spec[-1]


@pytest.mark.parametrize(
    "schema,identifier",
    [("snapshot_schema", "snapshot_schema"), ("select", '"select"'), ("Mixed_Catalog", '"Mixed_Catalog"')],
)
@pytest.mark.parametrize(
    "name,kind", [("ptg_wave_json_ascii_text_v1", "text"), ("ptg_wave_canonical_json_ascii_v1", "jsonb")]
)
def test_ptg_settings_keep_authenticated_native_identifier_and_exact_body(schema, identifier, name, kind):
    body = acquisition_functions()[name, kind][-1]
    for caller, previous, current, _count in acquisition_migration()._calls(_SCHEMA):
        if caller == name:
            body = body.replace(previous, current)
    row = _entry(name + "(" + kind + ")")
    row["definition"].update(
        prosrc=body.replace('"snapshot_schema"', '"' + schema + '"'),
        proconfig=["search_path=pg_catalog, " + identifier],
    )
    routine = _raw(row, schema)
    routine["native"]["namespace_identifier"] = identifier
    assert _verify([routine], schema) is None
    routine["native"]["definition"]["proconfig"].append("search_path=pg_temp")
    with pytest.raises(ValueError, match="definition_changed"):
        _verify([routine], schema)
    routine["native"]["namespace_identifier"] = "unbound_schema"
    with pytest.raises(ValueError, match="namespace_identifier_changed"):
        _verify([routine], schema)


def test_all_existing_legacy_candidate_definitions_still_use_source_contracts(monkeypatch):
    rows = [_raw(row, "source_schema") for row in legacy_function_rows(monkeypatch)]
    parent = next(row for row in rows if row["native"]["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    from db.migration_generic_source_candidates import _canonical_copy_guard_bodies

    parent["native"]["definition"]["prosrc"] = _canonical_copy_guard_bodies("source_schema", _PREPARATION_SCHEMA)[1]
    assert _verify(rows, "source_schema") is None


@pytest.mark.parametrize("name", tuple(transition._INVOKER_BODIES))
@pytest.mark.parametrize(
    "field,value",
    [
        ("prosrc", "BEGIN RETURN NEW; END"),
        ("proconfig", ["search_path=public"]),
        ("prosecdef", True),
        ("proargtypes", ["25"]),
        ("pronargdefaults", 1),
    ],
)
def test_each_new_invoker_refuses_body_settings_definer_and_overload_changes(monkeypatch, name, field, value):
    row = next(row for row in _extra_rows(monkeypatch) if row["native"]["definition"]["proname"] == name)
    row["native"]["definition"][field] = value
    with pytest.raises(ValueError, match="definition_changed"):
        _verify([row])


def test_reader_whitespace_normalization_is_not_an_admin_execution_contract(monkeypatch):
    row = _extra_rows(monkeypatch)[0]
    before = row["native"]["definition"]["prosrc"]
    row["native"]["definition"]["prosrc"] = "\n" + before
    assert " ".join(before.split()) == " ".join(row["native"]["definition"]["prosrc"].split())
    with pytest.raises(ValueError, match="definition_changed"):
        _verify([row])


@pytest.mark.parametrize("version", [0, 1])
def test_existing_typed_body_recognizers_preserve_old_and_exact_successor_paths(original_guards, monkeypatch, version):
    rows = [_raw(row) for row in original_guards] + _extra_rows(monkeypatch)
    bodies = retirement.retirement_resolution_bodies(_SCHEMA)
    bodies.update(
        guard_pd_generic_dataset_finalization=generic_finalization_bodies(_SCHEMA),
        guard_pd_endpoint_dataset_subset_replay_evidence=replay_guard_bodies(_SCHEMA),
        prepare_pd_generic_dataset_storage=allocator_bodies(_SCHEMA),
    )
    for row in rows:
        definition = row["native"]["definition"]
        if definition["proname"] in bodies:
            definition["prosrc"] = bodies[definition["proname"]][version]
    assert _verify(rows) is None


@pytest.mark.parametrize("version", [0, 1])
def test_noncurrent_lifecycle_guard_keeps_its_existing_exact_recognizer(version):
    row = _entry(GUARD + "()")
    row["definition"]["prosrc"] = lifecycle_guard_bodies(_SCHEMA)[version]
    assert _verify([_raw(row)]) is None


@pytest.mark.parametrize(
    "fault",
    [
        "unknown_name",
        "namespace",
        "definition_namespace",
        "language",
        "language_identity",
        "oid",
        "duplicate_oid",
        "overload",
    ],
)
def test_unknown_or_inconsistent_inventory_identity_refuses(monkeypatch, fault):
    rows = _extra_rows(monkeypatch)
    first, definition = rows[0], rows[0]["native"]["definition"]
    if fault in {"duplicate_oid", "overload"}:
        duplicate = deepcopy(first)
        if fault == "overload":
            duplicate["oid"] = 9000
            duplicate["native"]["definition"].update(oid="9000", pronargs=1, proargtypes=["25"])
        rows.append(duplicate)
    else:
        target, field, changed = {
            "unknown_name": (definition, "proname", "unreviewed_callback"),
            "namespace": (first["native"]["namespace"], 1, "another_schema"),
            "definition_namespace": (definition, "pronamespace", "99"),
            "language": (first["native"]["language"], "lanname", "sql"),
            "language_identity": (first["native"]["language"], "oid", "999"),
            "oid": (first, "oid", first["oid"] + 1),
        }[fault]
        target[field] = changed
    with pytest.raises(ValueError, match="cms_transition_routine_|definition_changed"):
        _verify(rows)


@pytest.mark.parametrize("value", [[], {}, None, True])
def test_missing_inventory_refuses_before_definition_access(value):
    with pytest.raises(ValueError, match="inventory_required"):
        _verify(value)


def test_json_raw_fields_are_decoded_without_rewriting_originals(monkeypatch):
    rows = _extra_rows(monkeypatch)
    rows[0]["native"]["definition"] = json.dumps(rows[0]["native"]["definition"])
    rows[0]["native"]["language"] = json.dumps(rows[0]["native"]["language"])
    rows[0]["native"] = json.dumps(rows[0]["native"])
    before = deepcopy(rows)
    assert _verify(rows) is None and rows == before


def test_observed_composite_type_cannot_become_its_own_expected_binding():
    key = "validate_provider_directory_rooted_graph_witnesses(regclass,regclass,@work)"
    row = _entry(key)
    row["definition"]["prosrc"] = "untrusted composite binding"
    with pytest.raises(ValueError, match="type_contract_required"):
        _verify([_raw(row)])


def _native_foreign(name):
    row = _raw(_entry("guard_provider_directory_terminal_root_retirement_child()"), "pg_catalog", 11)
    oid = transition._NATIVE_FOREIGN_OIDS[name]
    row["oid"] = oid
    row["native"]["language"] = dict(oid="12", lanname="internal")
    row["native"]["definition"].update(
        oid=str(oid),
        proname=name,
        prolang="12",
        proowner="10",
        prosrc=name,
        prosecdef=False,
        proisstrict=True,
        proparallel="s",
        procost=1,
        proconfig=None,
    )
    return row


@pytest.mark.parametrize("name", tuple(transition._NATIVE_FOREIGN_OIDS))
def test_only_existing_builtin_ri_identities_are_recognized(name):
    row = _native_foreign(name)
    assert _verify([row]) is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("oid", "2000"),
        ("proowner", "999"),
        ("pronamespace", "42"),
        ("proname", "unreviewed_internal"),
        ("prosrc", "unreviewed_internal"),
        ("prosecdef", True),
        ("proargtypes", ["25"]),
        ("proisstrict", False),
        ("proparallel", "u"),
        ("proconfig", ["search_path=public"]),
        ("procost", 100),
    ],
)
def test_native_namespace_never_exempts_unknown_or_changed_code(field, value):
    row = _native_foreign("RI_FKey_check_ins")
    row["native"]["definition"][field] = value
    if field == "oid":
        row["oid"] = int(value)
    with pytest.raises(ValueError, match="native_foreign_function_changed"):
        _verify([row])
