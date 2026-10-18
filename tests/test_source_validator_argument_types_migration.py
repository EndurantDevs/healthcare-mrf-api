# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authentic source-validator contracts and exact argument repair; no native claim."""

import importlib.util
import io
import re
from hashlib import sha256
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect

VERSIONS = Path(__file__).parents[1] / "alembic/versions"


def _load(filename):
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), VERSIONS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _migration():
    return _load("20261016040000_source_validator_argument_types.py")


def _parse_function(sql):
    compiled = text(sql).compile(dialect=dialect())
    assert not compiled.params
    sql = str(compiled)
    match = re.search(
        r'CREATE(?: OR REPLACE)? FUNCTION "snapshot_schema"\."(\w+)"\((.*?)\)\s+RETURNS\s+(\w+)'
        r"(.*?)AS \$(\w+)\$(.*?)\$\5\$",
        sql,
        re.S,
    )
    assert match
    name, arguments, returns, options, _tag, body = match.groups()
    parameters = [argument.strip().split() for argument in arguments.split(",")] if arguments else []
    argument = ",".join(parameter[0] for parameter in parameters)
    kind = ",".join(parameter[1] for parameter in parameters)
    return name, (
        argument,
        kind,
        returns,
        "plpgsql" if "plpgsql" in options else "sql",
        "i" if "IMMUTABLE" in options else "s" if "STABLE" in options else "v",
        "STRICT" in options,
        "s" if "PARALLEL SAFE" in options else "u",
        body,
    )


def _authentic_functions():
    subset = _load("20260808190000_provider_directory_subset_completion_proof.py")
    activation = _load("20260808200000_provider_directory_reviewed_subset_activation.py")
    uhc = _load("20260810080000_provider_directory_uhc_flex_practitioner_publication.py")
    rooted = _load("20260811020000_provider_directory_rooted_graph_acquisition.py")
    flex_latest = _load("20260830090000_uhc_flex_retry_exhaustion.py")
    rooted_latest = _load("20260909120000_fhir_request_failure_budget.py")
    renderer_options_by_name = dict(
        use_configured_endpoint_identity=True,
        replace_existing=True,
        reviewed_root_policy_aware=True,
        reviewed_subset_profile_aware=True,
        reviewed_subset_terminal_window_profile_aware=True,
    )
    schema = "snapshot_schema"
    statements = (
        subset._subset_published_source_guard_sql(schema, **renderer_options_by_name),
        activation._source_guard_function_sql(
            schema, allow_effective_endpoint_cutover=True, replace_existing=True, reviewed_root_policy_aware=True
        ),
        activation._activation_valid_function_sql(schema, **renderer_options_by_name),
        subset._canonical_sha256_function_sql(schema),
        subset._canonical_json_function_sql(schema),
        subset._content_proof_valid_function_sql(schema, replace_existing=True),
        uhc._source_guard_sql(schema),
        uhc._endpoint_guard_sql(schema),
        flex_latest._publication_valid_sql(schema, partial=True),
        rooted._rooted_dependency_guard_sql(schema),
        rooted_latest._intrinsic_valid_sql(schema),
    )
    return dict(_parse_function(sql) for sql in statements)


def _digest(body):
    return sha256(body.replace('"snapshot_schema"', '"__schema__"').encode()).hexdigest()


def test_forward_parent_and_exact_closed_graph():
    migration = _migration()
    assert migration.revision == "20261016040000_source_validator_argument_types"
    assert migration.down_revision == "20261016030000_cms_source_guard_argument_types"
    assert len(migration._CONTRACTS) == 11
    assert {entry[0] for entry in migration._CONTRACTS} == _authentic_functions().keys()
    assert sum(entry[-2] != entry[-1] for entry in migration._CONTRACTS) == 6
    assert sum(entry[-1] for entry in migration._REPAIRS) == 9
    assert migration._content_call("snapshot_schema", typed=True).count("::text") == 4


@pytest.mark.parametrize("index", range(11))
def test_latest_constructor_and_only_exact_cast_delta(index):
    migration = _migration()
    name, *contract = migration._CONTRACTS[index]
    *attributes, body = _authentic_functions()[name]
    assert attributes == contract[:-2]
    assert _digest(body) == contract[-2]
    replacement = body
    calls = [entry for entry in migration._calls("snapshot_schema") if entry[0] == name]
    for _, old, new, count in calls:
        assert body.count(old) == count and new not in body
        replacement = replacement.replace(old, new)
    assert _digest(replacement) == contract[-1]
    restored = replacement
    for _, old, new, count in calls:
        assert replacement.count(new) == count and old not in replacement
        restored = restored.replace(new, old)
    assert restored == body


@pytest.mark.parametrize("name,callee,argument,count", _migration()._REPAIRS)
def test_varchar_inputs_dispatch_to_declared_text(name, callee, argument, count):
    functions = _authentic_functions()
    assert functions[callee][1] == "text"
    old = f'"snapshot_schema"."{callee}"({argument})'
    new = old[:-1] + "::text)"
    calls = _migration()._calls("snapshot_schema")
    assert (name, old, new, count) in calls
    assert functions[name][-1].count(old) == count
    # Exact dispatch prevents a same-name varchar overload from replacing text validation.
    assert new.endswith("::text)") and "DEFAULT" not in new


def test_jsonb_and_recursive_canonical_calls_stay_unchanged():
    migration = _migration()
    functions = _authentic_functions()
    canonical = "provider_directory_subset_completion_canonical_json"
    assert functions[canonical][1] == "jsonb"
    assert functions[canonical][-1].count(f'"snapshot_schema"."{canonical}"(') == 2
    for name in (
        canonical,
        "provider_directory_subset_canonical_sha256",
        "provider_directory_subset_content_proof_valid",
    ):
        contract = next(entry for entry in migration._CONTRACTS if entry[0] == name)
        assert contract[-2] == contract[-1]
    reached_names_set = set()
    for attributes in functions.values():
        reached_names_set.update(re.findall(r'"snapshot_schema"\."(\w+)"\(', attributes[-1]))
    assert reached_names_set <= functions.keys()


def test_nested_content_call_uses_all_ten_exact_types():
    migration = _migration()
    functions = _authentic_functions()
    name = "provider_directory_subset_content_proof_valid"
    assert functions[name][1].split(",") == [
        "jsonb",
        "text",
        "text",
        "text",
        "jsonb",
        "jsonb",
        "text",
        "bigint",
        "jsonb",
        "jsonb",
    ]
    caller = functions["provider_directory_reviewed_subset_activation_valid"][-1]
    old_call = migration._content_call("snapshot_schema", typed=False)
    new_call = migration._content_call("snapshot_schema", typed=True)
    assert caller.count(old_call) == 1 and new_call not in caller
    assert "jsonb ->" in old_call and "-> 'dataset' ->" in old_call
    assert new_call.count("::text") == 4
    for column in ("dataset_id", "endpoint_id", "acquisition_root_run_id", "dataset_hash"):
        assert "policy_candidate." + column + "::text," in new_call
    assert new_call.replace("::text", "") == old_call
    assert ("provider_directory_reviewed_subset_activation_valid", old_call, new_call, 1) in migration._calls(
        "snapshot_schema"
    )


def test_native_overloads_and_defaults_refuse_before_replacement():
    sql = _migration()._repair_sql("snapshot_schema")
    assert "AND proname=ANY(" in sql and "))<>11 THEN" in sql
    assert "SELECT * INTO STRICT original FROM pg_proc" in sql
    assert "original.proargtypes::text<>expected.argument_oid" in sql
    assert "original.pronargdefaults<>0 OR original.provariadic<>0" in sql
    assert "original.proargdefaults IS NOT NULL" in sql
    assert sql.index("source_validator_function_set_changed") < sql.index("EXECUTE definition")
    assert sql.index("source_validator_definition_changed") < sql.index("EXECUTE definition")
    # Future default-overload ambiguity may refuse; this contract claims no availability guarantee.
    assert "source_validator_body_changed" in sql


def test_offline_sql_preserves_identity_acl_and_guard_edges(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    monkeypatch.setenv("DB_SCHEMA", "snapshot_schema")
    migration = _migration()
    output = io.StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        migration.upgrade()
    sql = output.getvalue()
    assert sql.index("SET LOCAL search_path=pg_catalog") < sql.index("LOCK TABLE") < sql.index("DO $migration$")
    assert "IN SHARE ROW EXCLUSIVE MODE NOWAIT" in sql
    assert "pg_get_functiondef(original.oid)" in sql and "WHERE oid=original.oid" in sql
    assert "(to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')" in sql
    assert "before_catalog IS DISTINCT FROM after_catalog" in sql
    assert "before_hooks IS DISTINCT FROM after_hooks" in sql
    assert "before_dependencies IS DISTINCT FROM after_dependencies" in sql
    assert "OR NOT EXISTS(SELECT FROM pg_roles WHERE oid=original.proowner)" in sql
    assert not any(word in sql.upper() for word in ("GRANT ", "REVOKE ", "OWNER TO", "CREATE TRIGGER", "DROP "))


def test_schema_mismatch_unsafe_name_and_downgrade_refuse(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "snapshot_schema")
    monkeypatch.setenv("DB_SCHEMA", "other_schema")
    with pytest.raises(RuntimeError, match="same schema"):
        migration.upgrade()
    with pytest.raises(ValueError, match="identifier_invalid"):
        migration._repair_sql("invalid;schema")
    with pytest.raises(RuntimeError, match="downgrade_unsupported"):
        migration.downgrade()


@pytest.mark.parametrize(
    "filename,renderer,options,function_name",
    [
        (
            "20260830090000_uhc_flex_retry_exhaustion.py",
            "_publication_valid_sql",
            {"partial": True},
            "provider_directory_uhc_flex_practitioner_dataset_valid",
        ),
        (
            "20260909120000_fhir_request_failure_budget.py",
            "_intrinsic_valid_sql",
            {},
            "provider_directory_rooted_graph_dataset_intrinsic_valid",
        ),
    ],
)
def test_constructor_escape_is_not_part_of_native_body(filename, renderer, options, function_name):
    sql = getattr(_load(filename), renderer)("snapshot_schema", **options)
    raw = re.search(r"AS \$function\$(.*?)\$function\$", sql, re.S).group(1)
    name, attributes = _parse_function(sql)
    assert name == function_name and raw.count(r"\:1") == 1
    body = attributes[-1]
    assert body == raw.replace(r"\:1", ":1")
    contract = next(entry for entry in _migration()._CONTRACTS if entry[0] == name)
    assert _digest(body) == contract[-2] and _digest(raw) != contract[-2]
