# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact latest acquisition constructors and minimal argument-dispatch changes."""

import io
import re
from hashlib import sha256
from types import SimpleNamespace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect

from tests.test_source_validator_argument_types_migration import _load

_SCHEMA = "snapshot_schema"
_FILENAME = "20261016050000_acquisition_validator_argument_types.py"
_PATTERN = re.compile(
    r'CREATE(?: OR REPLACE)? FUNCTION "snapshot_schema"\."?(\w+)"?\((.*?)\)\s+RETURNS\s+(.*?)'
    r"\s+LANGUAGE\s+(.*?)AS \$(\w*)\$(.*?)\$\5\$",
    re.S,
)


def _migration():
    return _load(_FILENAME)


def _parse_functions(sql):
    compiled = text(sql).compile(dialect=dialect())
    assert not compiled.params
    functions_by_signature = {}
    for name, arguments, returns, options, _tag, body in _PATTERN.findall(str(compiled)):
        if name not in {entry[0] for entry in _migration()._CONTRACTS}:
            continue
        parameters = [argument.strip().split(None, 1) for argument in arguments.split(",")] if arguments else []
        kinds = ["@work" if "provider_directory_rooted_graph_work" in kind else kind for _, kind in parameters]
        functions_by_signature[name, ",".join(kinds)] = (
            ",".join(argument for argument, _ in parameters),
            returns,
            options.strip().split()[0],
            "i" if "IMMUTABLE" in options else "s" if "STABLE" in options else "v",
            "STRICT" in options,
            "s" if "PARALLEL SAFE" in options else "u",
            "SECURITY DEFINER" in options,
            "pg_catalog," in options,
            body,
        )
    return functions_by_signature


def _rooted_statements():
    rooted = _load("20260811020000_provider_directory_rooted_graph_acquisition.py")
    latest = _load("20260909120000_fhir_request_failure_budget.py")
    scoped = _load("20260904163000_provider_directory_exact_guard_scope.py")
    return (
        *rooted._hash_functions_sql(_SCHEMA),
        rooted._rooted_official_lineage_current_function_sql(_SCHEMA),
        rooted._rooted_valid_function_sql(_SCHEMA),
        rooted._rooted_ready_function_sql(_SCHEMA),
        latest._acquisition_guard_sql(_SCHEMA),
        latest._header_guard_sql(_SCHEMA),
        latest._single_root_guard_sql(_SCHEMA),
        latest._coverage_sql(_SCHEMA),
        latest._coverage_valid_sql(_SCHEMA),
        latest._intrinsic_valid_sql(_SCHEMA),
        scoped._logical_current_guard_sql(_SCHEMA, scoped=True),
        _load("20261005100000_rooted_graph_set_validation.py")._sql(_SCHEMA),
    )


def _shared_statements():
    terminal = _load("20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py")
    publication = _load("20260810080000_provider_directory_uhc_flex_practitioner_publication.py")
    latest = _load("20260909120000_fhir_request_failure_budget.py")
    retry = _load("20260830090000_uhc_flex_retry_exhaustion.py")
    canonical_installer = _load("20260810110000_ptg_wave_receipt_authority.py")
    statements = []
    canonical_installer.op = SimpleNamespace(execute=statements.append)
    canonical_installer._install_receipt_verification_functions(_SCHEMA)
    sibling = _load("20260820030000_ptg_ordinary_terminal_json_canonical_digest.py")
    sibling._schema = lambda: _SCHEMA
    return (
        terminal._terminal_set_function_sql(_SCHEMA),
        publication._header_guard_sql(_SCHEMA),
        publication._ready_function_sql(_SCHEMA),
        latest._flex_guard_sql(_SCHEMA, single=False),
        retry._publication_valid_sql(_SCHEMA, partial=True),
        *statements,
        sibling._canonical_json_function_sql(),
    )


def _current_functions():
    functions_by_signature = {}
    for statement in (*_rooted_statements(), *_shared_statements(), *_metadata_statements()):
        functions_by_signature.update(_parse_functions(statement))
    previous = _load("20261016040000_source_validator_argument_types.py")
    for identity, attributes in tuple(functions_by_signature.items()):
        body = attributes[-1]
        for caller, old, new, count in previous._calls(_SCHEMA):
            if caller == identity[0]:
                assert body.count(old) == count
                body = body.replace(old, new)
        functions_by_signature[identity] = (*attributes[:-1], body)
    return functions_by_signature


def _metadata_statements():
    subset = _load("20260808190000_provider_directory_subset_completion_proof.py")
    admission = _load("20260812020000_provider_directory_endpoint_dataset_admission_seal.py")
    compact = _load("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    latest = _load("20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance.py")
    return (
        subset._payload_canonical_json_function_sql(_SCHEMA),
        subset._payload_sha256_function_sql(_SCHEMA),
        subset._proof_pair_valid_function_sql(_SCHEMA),
        latest._proof_shape_valid_function_sql(_SCHEMA, tail_tolerant=True),
        admission._digest_function_sql(_SCHEMA),
        admission._guard_function_sql(_SCHEMA),
        compact._endpoint_guard_sql(_SCHEMA, compact=True),
        subset._canonical_sha256_function_sql(_SCHEMA),
        subset._canonical_json_function_sql(_SCHEMA),
        subset._content_proof_valid_function_sql(_SCHEMA, replace_existing=True),
    )


def _digest(body):
    normalized = body.replace('"snapshot_schema"', '"__schema__"').replace("'snapshot_schema'", "'__schema__'")
    return sha256(normalized.encode()).hexdigest()


def test_exact_forward_parent_and_complete_repair_scope():
    migration = _migration()
    assert migration.revision == "20261016050000_acquisition_validator_argument_types"
    assert migration.down_revision == "20261016040000_source_validator_argument_types"
    assert len(migration._CONTRACTS) == 35 and len({entry[0] for entry in migration._CONTRACTS}) == 34
    assert len(migration._TEXT_REPAIRS) == 23 and len(migration._calls(_SCHEMA)) == 31
    assert sum(entry[-2] != entry[-1] for entry in migration._CONTRACTS) == 14
    assert len({entry[0] for entry in migration._TEXT_REPAIRS}) == 10


@pytest.mark.parametrize("contract", _migration()._CONTRACTS, ids=lambda contract: contract[0] + "-" + contract[2])
def test_authentic_current_constructor_and_only_argument_delta(contract):
    name, argument, kind, returns, language, volatility, strict, parallel, definer, extra_path, previous, current = (
        contract
    )
    attributes = _current_functions()[name, kind]
    assert attributes[:-1] == (argument, returns, language, volatility, strict, parallel, definer, extra_path)
    body = attributes[-1]
    assert _digest(body) == previous
    replacement = body
    for caller, old, new, count in _migration()._calls(_SCHEMA):
        if caller == name:
            assert replacement.count(old) == count and new not in replacement
            replacement = replacement.replace(old, new)
    assert _digest(replacement) == current
    assert replacement.replace("::text", "").replace("::regclass", "") == body.replace("::text", "").replace(
        "::regclass", ""
    )


def test_full_cast_accounting_and_literal_stage_identity():
    functions_by_signature = _current_functions()
    text_casts = regclass_casts = 0
    for caller, old, new, count in _migration()._calls(_SCHEMA):
        assert count == 1
        body = next(attributes[-1] for (name, _), attributes in functions_by_signature.items() if name == caller)
        assert body.count(old) == 1
        text_casts += new.count("::text") - old.count("::text")
        regclass_casts += new.count("::regclass") - old.count("::regclass")
        assert new.replace("::text", "").replace("::regclass", "") == old.replace("::text", "").replace(
            "::regclass", ""
        )
    assert (text_casts, regclass_casts) == (31, 4)
    stage_calls = [entry for entry in _migration()._calls(_SCHEMA) if "stage_checks" in entry[1]]
    assert len(stage_calls) == 3 and stage_calls[0][2].count("::regclass") == 2
    assert all('"snapshot_schema".provider_directory_rooted_graph_' in new for _, _, new, _ in stage_calls)


def test_legitimate_json_overloads_and_composite_type_are_exact():
    contracts = _migration()._CONTRACTS
    canonical_contracts = [entry for entry in contracts if entry[0] == "ptg_wave_canonical_json_ascii_v1"]
    assert {entry[2] for entry in canonical_contracts} == {"json", "jsonb"}
    assert all(entry[-2] == entry[-1] and entry[8] is False for entry in canonical_contracts)
    witness = next(entry for entry in contracts if entry[0] == "validate_provider_directory_rooted_graph_witnesses")
    assert witness[2:4] == ("regclass,regclass,@work", "bigint[]")
    composite = f'''to_regtype('"{_SCHEMA}"."provider_directory_rooted_graph_work"')'''
    generated = _migration()._contracts_sql(_SCHEMA)
    assert generated.count(composite + "::oid::text") == 1
    assert "concat_ws(' ','2205','2205'," + composite + "::oid::text)" in generated
    assert composite + "::text" not in generated
    sql = _migration()._repair_sql(_SCHEMA)
    assert "t.typrelid=c.oid" in sql and "t.oid=to_regtype(" in sql
    assert "original.prosecdef<>expected.definer" in sql
    assert "AND proargtypes::text=expected.argument_oid" in sql
    assert "original.pronargdefaults<>0 OR original.provariadic<>0" in sql


def test_metadata_transitive_call_closure_is_exact():
    functions = _current_functions()
    names = {name for name, _ in functions}
    expected_by_function = {
        "guard_tin_npi_connector_endpoint_dataset": {
            "provider_directory_subset_completion_proof_pair_valid",
            "provider_directory_subset_completion_proof_shape_valid",
            "provider_directory_endpoint_dataset_admission_metadata_sha256",
            "provider_directory_subset_payload_sha256",
            "provider_directory_subset_completion_canonical_json",
            "provider_directory_subset_canonical_sha256",
            "provider_directory_subset_content_proof_valid",
        },
        "guard_provider_directory_endpoint_dataset_admission_seal": {
            "provider_directory_endpoint_dataset_admission_metadata_sha256",
            "provider_directory_subset_payload_canonical_json",
        },
        "provider_directory_endpoint_dataset_admission_metadata_sha256": {"provider_directory_subset_payload_sha256"},
        "provider_directory_subset_completion_proof_pair_valid": {"provider_directory_subset_canonical_sha256"},
        "provider_directory_subset_completion_proof_shape_valid": {"provider_directory_subset_canonical_sha256"},
        "provider_directory_subset_payload_sha256": {"provider_directory_subset_payload_canonical_json"},
        "provider_directory_subset_payload_canonical_json": {"provider_directory_subset_payload_canonical_json"},
    }
    for name, callees in expected_by_function.items():
        body = next(attributes[-1] for (function, _), attributes in functions.items() if function == name)
        actual_callees = set(re.findall(r'"snapshot_schema"\."(\w+)"\(', body))
        assert actual_callees == callees and actual_callees <= names


def test_offline_repair_preserves_catalog_and_has_no_authority_changes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    monkeypatch.setenv("DB_SCHEMA", _SCHEMA)
    output = io.StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        _migration().upgrade()
    sql = output.getvalue()
    assert sql.index("SET LOCAL search_path=pg_catalog") < sql.index("LOCK TABLE") < sql.index("DO $migration$")
    assert "IN SHARE ROW EXCLUSIVE MODE NOWAIT" in sql
    assert sql.index("acquisition_validator_body_changed") < sql.index("EXECUTE definition")
    assert "(to_jsonb(updated)-'prosrc') IS DISTINCT FROM (to_jsonb(original)-'prosrc')" in sql
    assert "before_shared IS DISTINCT FROM after_shared" in sql
    assert not any(word in sql.upper() for word in ("GRANT ", "REVOKE ", "OWNER TO", "CREATE TRIGGER", "DROP "))
    compiled = text(_migration()._repair_sql(_SCHEMA)).compile(dialect=dialect())
    assert not compiled.params


def test_mismatch_unsafe_identifier_and_downgrade_refuse(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    monkeypatch.setenv("DB_SCHEMA", "other_schema")
    with pytest.raises(RuntimeError, match="same schema"):
        _migration().upgrade()
    with pytest.raises(ValueError, match="identifier_invalid"):
        _migration()._repair_sql("invalid;schema")
    with pytest.raises(RuntimeError, match="downgrade_unsupported"):
        _migration().downgrade()
