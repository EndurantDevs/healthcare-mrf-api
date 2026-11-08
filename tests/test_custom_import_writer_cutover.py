# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact fixed cutover inventory; native authority proofs are separate."""

from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path
from unittest.mock import Mock

import pytest
from sqlalchemy import text


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005080000_custom_import_writer_cutover.py"
    spec = importlib.util.spec_from_file_location("writer_cutover_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _statements(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic schema")
    monkeypatch.setattr(migration, "_independent_reviewed_functions", lambda _schema: [])
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return migration, statements


def test_cutover_is_schema_only_and_closes_authority_before_retiring_guards(monkeypatch):
    migration, statements = _statements(monkeypatch)
    assert migration.down_revision == "20261005070000_custom_import_materialization_storage"
    assert len(statements) == 4
    assert "LOCK TABLE" in statements[0]
    assert "REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN" in statements[1]
    assert "REVOKE ALL ON FUNCTION" in statements[2]
    assert "DROP TRIGGER" in statements[3]
    combined = "\n".join(statements)
    assert "GRANT " not in combined and "CREATE ROLE" not in combined
    assert "DISABLE TRIGGER" not in combined and "ENABLE TRIGGER" not in combined
    assert "DROP INDEX" not in combined and "DROP CONSTRAINT" not in combined
    assert "INSERT INTO" not in statements[0] and "DELETE FROM" not in statements[0]
    assert "__CONTROL__" not in combined and "__REVIEWED__" not in combined
    assert "__INDEPENDENT_REVIEWED__" not in combined
    assert '"synthetic schema".custom_import_dataset' in combined


def test_all_fifteen_hot_tables_have_exact_old_guard_inventory():
    migration = _migration()
    hot = migration._previous(migration._VERSIONS[0])._RELATION_NAMES
    specs = migration._guard_specs(hot)
    assert len(hot) == 15 and len(specs) == 65
    assert set(spec["table"] for spec in specs) == set(hot)
    assert len({(spec["table"], spec["trigger"]) for spec in specs}) == len(specs)
    assert all(spec["events"] & 1 and not spec["events"] & 32 for spec in specs)
    assert all("generation_seal" not in spec["table"] and "lease" not in spec["table"] for spec in specs)
    interners = [spec for spec in specs if spec["table"] in hot[:2]]
    assert len(interners) == 6
    assert {spec["function"] for spec in interners} == {
        "guard_custom_import_immutable_row",
        "guard_custom_import_dataset_first_insert",
        "guard_custom_import_build_frozen_row",
    }


def test_reviewed_function_receipts_pin_actual_bodies_and_owner_only_helpers():
    migration = _migration()
    receipts = migration._reviewed_functions("synthetic_schema")
    by_name = {receipt["identity"].split("(")[0].rsplit(".", 1)[1]: receipt for receipt in receipts}
    assert all(set(receipt) == {"identity", "body_sha256", "runtime"} for receipt in receipts)
    assert all(len(receipt["body_sha256"]) == 64 for receipt in receipts)
    for name in (
        "persist_custom_import_scalar_models",
        "persist_custom_import_winner_models",
        "append_custom_import_revision_home",
        "lookup_custom_import_revision_home",
        "install_custom_import_snapshot_writers",
        "create_custom_import_snapshot_family",
    ):
        assert by_name[name]["runtime"] is False
    for name in (
        "source_bulk_authorize",
        "source_set_finalize",
        "persist_custom_import_legacy_identity_set",
        "persist_custom_import_scalar_set",
        "persist_custom_import_winner_set",
        "resolve_custom_import_generation_finality_snapshot",
        "freeze_custom_import_snapshot_family",
    ):
        assert by_name[name]["runtime"] is True
    assert by_name["guard_custom_import_generation_seal_insert"]["runtime"] is False
    assert not any(signature.split("(")[0] in by_name for signature in migration._OBSOLETE)


@pytest.mark.parametrize(
    ("filename", "caller", "signature"),
    (
        ("build_source.py", "_copy_source_landing", "resolve_custom_import_source_batch_snapshot(uuid)"),
        ("build_graph.py", "_build_storage_models", "resolve_custom_import_build_base_snapshot(bigint)"),
    ),
)
def test_direct_snapshot_callers_retain_exact_reviewed_runtime_api(filename, caller, signature):
    path = Path(__file__).resolve().parents[1] / "process/custom_import" / filename
    tree = ast.parse(path.read_text())
    function = next(node for node in tree.body if getattr(node, "name", None) == caller)
    called_names = {
        name
        for node in ast.walk(function)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        for name in re.findall(r"\bresolve_custom_import_[a-z_]+\b", node.value)
    }
    assert signature.split("(")[0] in called_names
    receipt_by_identity = {
        receipt["identity"]: receipt for receipt in _migration()._reviewed_functions("synthetic_schema")
    }
    assert receipt_by_identity[f'"synthetic_schema".{signature}']["runtime"] is True


def test_acl_closure_includes_columns_sequences_and_transitive_calls(monkeypatch):
    _, statements = _statements(monkeypatch)
    privileges, functions = statements[1:3]
    assert "aclexplode(a.attacl)" in privileges
    assert "REVOKE INSERT(%I),UPDATE(%I),REFERENCES(%I)" in privileges
    assert "REVOKE USAGE,UPDATE ON SEQUENCE" in privileges
    assert "d.deptype IN ('a','i')" in privileges
    assert "CASCADE" in privileges
    assert "), callers(oid) AS (" in functions and "), surface(oid) AS (" in functions
    assert functions.count("procedure_sources AS MATERIALIZED") == 2
    assert "SELECT matched.oid FROM surface called\n        CROSS JOIN LATERAL (" in functions
    assert "WHERE target.oid=called.oid" in functions
    assert functions.count("CROSS JOIN LATERAL (") == 1 and functions.count("OFFSET 0") == 1
    assert functions.count('lower(p.prosrc COLLATE "C") AS folded_source') == 2
    assert "pg_depend" in functions and "target.proname" in functions
    assert functions.count('strpos(p.folded_source,lower(target.proname::text COLLATE "C"))=0') == 2
    assert "CASE WHEN strpos(p.folded_source,tables.name)=0 THEN false" in functions
    assert functions.count("'\\M','gi') refs(parts)") == 3
    assert functions.count("ELSE p.prosrc~*('\\m'||") == 3
    assert "CASE WHEN p.prosqlbody IS NOT NULL THEN pg_get_functiondef(p.oid)" in functions
    assert "custom_import_cutover_unreviewed_callable_definer: %" in functions
    assert "sha256(convert_to(function_row.prosrc,'UTF8'))" in functions
    assert "p.prorettype<>'trigger'::regtype" in functions
    assert "WHERE p.prosecdef AND NOT p.oid=ANY(reviewed_oids)" in functions
    assert "WHERE n.nspname='synthetic schema' AND p.prosecdef" not in functions


def test_independent_receipts_render_exact_current_and_retained_definitions(monkeypatch):
    migration = _migration()
    connection = Mock()
    connection.execute.return_value.scalars.return_value = ("synthetic_other",)
    monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
    receipts = migration._independent_reviewed_functions("synthetic_control")
    previous = migration._reviewed_functions("synthetic_other", include_obsolete=True)
    assert receipts[:-2] == previous
    correction = migration._previous("20261007000000_custom_import_rejection_anti_joins")
    body = " " + correction._body(correction._finality(), "synthetic_other", corrected=True) + " "
    validator = next(
        receipt
        for receipt in previous
        if receipt["identity"].endswith(".verify_custom_import_snapshot_structure(bigint)")
    )
    assert receipts[-2] == {**validator, "body_sha256": migration.hashlib.sha256(body.encode()).hexdigest()}
    assert receipts[-2]["body_sha256"] != validator["body_sha256"]
    child_presence = migration._previous("20261009000000_custom_import_child_presence_decode")
    installer = next(
        receipt
        for receipt in previous
        if receipt["identity"].endswith(".install_custom_import_snapshot_writers(bigint)")
    )
    body = child_presence._installer(child_presence._bulk(), "synthetic_other", corrected=True).split(
        "$bulk_snapshot$"
    )[1]
    assert receipts[-1] == {**installer, "body_sha256": migration.hashlib.sha256(body.encode()).hexdigest()}
    assert receipts[-1]["body_sha256"] != installer["body_sha256"]
    assert installer in receipts
    by_identity = {receipt["identity"]: receipt for receipt in receipts}
    assert all(f'"synthetic_other".{signature}' in by_identity for signature in migration._OBSOLETE)
    assert all(receipt["language"] in ("sql", "plpgsql") for receipt in receipts)
    assert connection.execute.call_args.args[1] == {"schema": "synthetic_control"}
    override = migration._previous("20261002030000_custom_import_identical_children")
    emitted = migration._DefinitionReceipt()
    override.op = emitted
    override._functions("synthetic_other", collapse=True)
    child = next(
        match
        for statement in emitted.statements
        if (match := migration._DEFINITION.fullmatch(statement))
        and match["qualified"].endswith(".next_custom_import_build_child")
    )
    identity = child["qualified"] + "(bigint,bigint,smallint,bytea,bigint)"
    assert by_identity[identity]["body_sha256"] == migration.hashlib.sha256(child["body"].encode()).hexdigest()


def test_independent_receipts_do_not_remove_native_or_qualified_edges(monkeypatch):
    _, statements = _statements(monkeypatch)
    functions = statements[2]
    assert functions.count("d.classid='pg_proc'::regclass") == 3
    assert functions.count("p.oid=ANY(independent_oids)") == 3
    assert functions.count("p.oid=ANY(literal_oids)") == 3
    assert "function_row.proowner=receipt.owner_oid" in functions
    assert "installation_owners.pronamespace=candidate.pronamespace" in functions
    assert "SELECT anchor.pronamespace,anchor.proowner FROM receipts expected" in functions
    assert "anchor.prokind='f' AND anchor.prosecdef" in functions
    assert "anchor.proconfig=ARRAY['search_path=pg_catalog']::text[]" in functions
    assert "lanname=expected.language" in functions
    assert "sha256(convert_to(anchor.prosrc,'UTF8')),'hex')=expected.body_sha256" in functions
    assert (
        "format('%I.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,"
        "timestamptz)',namespace.nspname)"
    ) in functions
    assert "function_row.proconfig=ARRAY['search_path=pg_catalog']::text[]" in functions
    assert "lanname=receipt.language" in functions
    assert "'hex')=receipt.body_sha256" in functions
    assert "target_namespace.nspname" in functions
    assert "refs.parts[1] IS NULL" in functions
    assert "language.lanname IN ('sql','plpgsql')" in functions


def test_cutover_blocks_incomplete_canonical_data_and_has_no_unsafe_downgrade(monkeypatch):
    migration, statements = _statements(monkeypatch)
    assert "custom_import_cutover_active_canonical_build" in statements[0]
    assert "custom_import_cutover_active_canonical_producer" in statements[0]
    assert "custom_import_generation_seal" in statements[0]
    assert "custom_import_cutover_unknown_record_guard" in statements[3]
    assert "custom_import_cutover_truncate_guard_missing" in statements[3]
    with pytest.raises(RuntimeError, match="explicit reviewed forward migration"):
        migration.downgrade()


def test_rendered_receipt_json_is_not_a_sqlalchemy_bind_parameter(monkeypatch):
    """Boolean JSON fields must not become :true/:false execution parameters."""
    migration, statements = _statements(monkeypatch)
    assert '"runtime": true' in statements[2] and '"runtime": false' in statements[2]
    assert all(not text(statement)._bindparams for statement in statements)
