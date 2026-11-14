# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed registry/model and migration contracts; native proofs are separate."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRelation
from process.custom_import import storage_layout


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261005030000_custom_import_snapshot_storage.py"
    spec = importlib.util.spec_from_file_location("snapshot_storage_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _names_migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261114010000_custom_import_snapshot_names.py"
    spec = importlib.util.spec_from_file_location("snapshot_names_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_frozen_control_and_hot_layout_matches_the_declared_models():
    """The migration freezes the actual fixed shape, not runtime-generated DDL."""

    migration = _migration()
    assert migration.down_revision == "20261005130000_provider_dataset_candidates"
    models = CustomImportSnapshotFamily, CustomImportSnapshotRelation
    for model in models:
        ddl = str(CreateTable(model.__table__).compile(dialect=postgresql.dialect()))
        assert "table_oid" not in ddl and "table_owner" not in ddl and "columns_sha256" not in ddl
    assert set(CustomImportSnapshotRelation.__table__.c.keys()) == {"family_id", "relation_slot"}
    assert all(model.__runtime_schema_sync__ is False for model in models)
    assert migration._RELATION_NAMES == tuple(model.__tablename__ for model in storage_layout.SNAPSHOT_MODELS)
    expected_load_statements = tuple(
        statement.replace("ci_snapshot_1.", "__LEAF__.").replace("mrf.", "__SCHEMA__.")
        for statement in storage_layout.snapshot_load_statements(1)[1:]
    )
    assert migration._LOAD_DDL == expected_load_statements
    assert not any("FOREIGN KEY" in statement or "TRIGGER" in statement for statement in migration._LOAD_DDL)


def test_upgrade_is_schema_only_and_does_not_grant_default_entry_points(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_schema")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    assert len([statement for statement in statements if statement.strip().startswith("CREATE TABLE")]) == 2
    functions = [statement for statement in statements if statement.strip().startswith("CREATE FUNCTION")]
    assert len(functions) == 10
    assert all("SECURITY DEFINER SET search_path=pg_catalog" in statement for statement in functions)
    assert not any(statement.strip().startswith(("INSERT", "UPDATE", "DELETE")) for statement in statements)
    assert not any("DROP TRIGGER" in statement or "DISABLE TRIGGER" in statement for statement in statements)
    assert sum("REVOKE ALL ON FUNCTION" in statement for statement in statements) == 10
    assert (
        sum("aclexplode(coalesce(c." in statement for statement in statements if statement.strip().startswith("DO "))
        == 13
    )
    assert not any(
        "__SCHEMA__" in statement or "__DDL__" in statement or "__NAMES__" in statement for statement in statements
    )


def test_snapshot_relation_resolution_and_freeze_keep_data_finality_without_physical_metadata():
    migration = _migration()
    current = _names_migration()
    relation_body = (current._RESOURCES / "relations.sql").read_text()
    assert "to_regclass(format('%I.%I',namespace,names[r.relation_slot]))::bigint" in relation_body
    assert "count(*)" in relation_body and "<> 15" in relation_body
    assert all(value not in relation_body for value in ("r.table_oid", "relowner", "columns_sha256"))
    freeze = current._freeze_body(migration._FREEZE_BODY)
    assert "IN SHARE MODE" in freeze
    assert freeze.count("resolve_custom_import_snapshot_relations") == 2
    assert "f.generation_id IS NULL" in freeze
    assert "b.output_frozen_at IS NULL" in freeze
    assert "verified_at=" not in freeze
    assert "custom_import_generation_seal" not in freeze
    assert "custom_import_snapshot_unfinished_landing" in freeze
    assert "REVOKE SELECT" in freeze and "REVOKE ALL ON TABLE" in freeze
    assert migration._CREATE_BODY.count("lock_custom_import_snapshot_attempt") == 2
    assert "ddl" not in CustomImportSnapshotFamily.__table__.c
    assert "namespace" not in CustomImportSnapshotFamily.__table__.c


def test_downgrade_refuses_retained_snapshots_instead_of_cascading(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert "custom_import_snapshot_storage_downgrade_blocked" in statements[0]
    assert not any("CASCADE" in statement or "DROP SCHEMA" in statement for statement in statements)


def test_pinned_read_binding_rejects_broken_snapshots_instead_of_legacy_fallback():
    """Only an exact sealed generation may resolve a frozen, OID-pinned leaf."""

    body = _migration()._READ_BINDING_BODY
    assert "custom_import_generation_seal" in body and "s.seal_contract=" in body
    assert "p_dataset_id,p_definition_revision_id,p_schema_revision_id" in body
    assert "execution_id=g.execution_id AND producing_fence=g.producing_fence" in body
    assert "IF f.family_id IS NULL THEN RETURN NULL;" in body
    assert "f.capture_bundle_id,f.producing_fence,f.producing_token_sha256" in body
    assert "f.frozen_at IS NULL" in body
    assert "FOR SHARE" not in body and "FOR UPDATE" not in body
    assert "IN ACCESS SHARE MODE" in body
    assert body.count("resolve_custom_import_snapshot_relations") == 2


def test_writer_binding_requires_a_live_open_family_and_cannot_fall_back():
    """Every future batch writer must share the freeze row and exact leaf locks."""

    body = _migration()._WRITE_BINDING_BODY
    assert "FOR UPDATE" in body and "f.frozen_at IS NOT NULL" in body
    assert "custom_import_generation_seal" in body and "custom_import_no_change_seal" in body
    assert "custom_import_snapshot_write_binding_mismatch" in body
    assert "IN ROW EXCLUSIVE MODE" in body
    assert body.count("resolve_custom_import_snapshot_relations") == 2
    assert body.count("lock_custom_import_snapshot_attempt") == 2
    assert "RETURN NULL" not in body


def test_finality_binding_pins_an_exact_frozen_candidate_without_a_seal_shortcut():
    """New sealing must resolve complete producer identity with writes closed."""

    body = _migration()._FINALITY_BINDING_BODY
    assert "f.family_id IS NULL OR g.generation_id IS NULL OR f.frozen_at IS NULL" in body
    assert "p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256" in body
    assert "p_generation_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id" in body
    assert "IN ACCESS SHARE MODE" in body
    assert body.count("resolve_custom_import_snapshot_relations") == 2
    assert body.count("lock_custom_import_snapshot_attempt") == 2
    assert "RETURN NULL" not in body
    assert "lock_custom_import_writable_snapshot" not in body
    assert "resolve_custom_import_generation_snapshot" not in body
    assert "custom_import_generation_seal" not in body


def test_names_refresh_covers_existing_and_future_writers_without_physical_columns(monkeypatch):
    migration = _names_migration()
    body_by_identity = {}
    monkeypatch.setattr(
        migration, "_refresh", lambda identity, transform: body_by_identity.setdefault(identity, transform("old"))
    )
    migration._refresh_storage("synthetic_schema")
    bulk = migration._previous("20261005040000_custom_import_bulk_snapshot_writers")
    legacy = migration._previous("20261005050000_custom_import_legacy_snapshot_writers")
    body_by_identity["bulk_installer"] = migration._bulk_installer("synthetic_schema")
    body_by_identity["legacy_installer"] = migration._legacy_installer("synthetic_schema")
    changed_bodies = []
    for statement in bulk._resource("snapshot_writers.sql"):
        if any(statement.startswith(f"CREATE FUNCTION __CANDIDATE__.{name}(") for name in migration._BULK_LEAVES):
            body = migration._bulk_leaf(statement.replace("__CANDIDATE__", "ci_snapshot_41"), 41)
            assert "f.family_id=41" in body
            assert "f.producing_token_sha256" in body and "f.frozen_at IS NULL" in body
            changed_bodies.append(body)
    assert len(changed_bodies) == 5
    for statement in legacy._resource("legacy_writers.sql"):
        if "owner_name name;" in statement:
            changed_bodies.append(migration._without_owner_gate(statement))
    assert len(changed_bodies) == 10
    for statement in bulk._resource("snapshot_writers.sql"):
        if "owner_name name;" in statement:
            changed_bodies.append(migration._without_owner_gate(statement))
    assert len(changed_bodies) == 16
    for function_body in (*body_by_identity.values(), *changed_bodies):
        assert all(
            field_name not in function_body
            for field_name in ("landing_table_oid", "origin_table_oid", "columns_sha256", "r.table_oid")
        )
    assert "CREATE UNLOGGED TABLE __CANDIDATE__.source_bulk_landing" in body_by_identity["bulk_installer"]
    assert "WITH admitted_children AS MATERIALIZED" in body_by_identity["bulk_installer"]
    assert (
        "PERFORM" in body_by_identity["bulk_installer"]
        and "lock_custom_import_build(b.build_id)" in body_by_identity["bulk_installer"]
    )
    assert "owner_name" not in body_by_identity["bulk_installer"]
    assert "owner_name" not in body_by_identity["legacy_installer"]
    creation = body_by_identity['"synthetic_schema".create_custom_import_snapshot_family(bigint,bigint,bytea)']
    assert creation.index("RETURN f.family_id") < creation.index("custom_import_snapshot_reader_rollout_required")
    assert creation.index("custom_import_snapshot_reader_rollout_required") < creation.index("INSERT INTO")


def test_names_refresh_changes_only_existing_function_bodies_and_preserves_failure(monkeypatch):
    migration = _names_migration()
    original = "BEGIN RETURN 1; END;"
    definition = f"CREATE OR REPLACE FUNCTION synthetic.f() RETURNS integer AS $body${original}$body$ LANGUAGE plpgsql"
    installed = SimpleNamespace(prosrc=original, definition=definition)
    result = SimpleNamespace(one_or_none=lambda: installed)
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(execute=lambda *_args: result))
    statements = []
    monkeypatch.setattr(migration.op, "execute", lambda statement: statements.append(str(statement)))
    migration._refresh("synthetic.f()", lambda source: source.replace("RETURN 1", "RETURN 2"))
    assert statements == [definition.replace("RETURN 1", "RETURN 2")]
    result.one_or_none = lambda: None
    with pytest.raises(RuntimeError, match="function_missing"):
        migration._refresh("synthetic.f()", lambda _source: "new")
    assert len(statements) == 1


def test_names_storage_refresh_preserves_retired_read_guards_and_native_locks(monkeypatch):
    migration = _names_migration()
    retirement = migration._previous("20261102010000_custom_import_snapshot_retirement")
    read_name = "resolve_custom_import_generation_snapshot"
    _name, arguments, _old, installed = next(
        definition for definition in retirement._definitions("synthetic") if definition[0] == read_name
    )
    identity = f'"synthetic".{read_name}({arguments})'
    body_by_identity = {}
    monkeypatch.setattr(
        migration,
        "_refresh",
        lambda target, transform: body_by_identity.setdefault(
            target, transform(installed if target == identity else "old")
        ),
    )
    migration._refresh_storage("synthetic")
    assert len(body_by_identity) == 6
    body = body_by_identity[identity]
    assert body != installed and "origin_table_oid" not in body
    assert "to_regclass(format('%I.legacy_copy_origin','ci_snapshot_' || f.family_id::text)) IS NOT NULL" in body
    assert body.count("custom_import_snapshot_retired") == installed.count("custom_import_snapshot_retired") == 2
    assert body.count("resolve_custom_import_snapshot_relations") == 2
    assert "IN ACCESS SHARE MODE" in body
    assert "IF f.family_id IS NULL THEN RETURN NULL; END IF;" in body
    assert "g.producing_token_sha256" in body and "f.frozen_at IS NULL" in body


def _writer_migration_sources(migration, schema):
    """Reproduce installed writers from their actual migration resources."""
    bulk = migration._previous("20261005040000_custom_import_bulk_snapshot_writers")
    legacy = migration._previous("20261005050000_custom_import_legacy_snapshot_writers")
    continuation = migration._previous("20261019010000_custom_import_build_continuation")
    child = continuation._previous("20261009000000_custom_import_child_presence_decode")
    storage = bulk._storage()
    quoted = storage._quote(schema)
    source_by_identity = {
        f"{quoted}.source_set_finalize(uuid,integer[])": bulk._control_sql(
            storage,
            schema,
            bulk._dispatcher("source_set_finalize", "p_batch uuid,p_verified_parts integer[]", "integer"),
        ),
        f"{quoted}.resolve_custom_import_source_batch_snapshot(uuid)": bulk._resource("source_batch_snapshot.sql")[
            0
        ].split("$fn$")[1],
        f"{quoted}.lock_custom_import_legacy_generation(bigint)": legacy._resource("legacy_authority.sql")[0].split(
            "$fn$"
        )[1],
    }
    for name, arguments, _old, current in continuation._materialization_definitions(schema):
        source_by_identity[f"{quoted}.{name}({arguments})"] = current
    bounded = migration._previous("20261002010000_custom_import_bounded_build")
    acceptance = migration._previous("20261020200000_custom_import_manual_acceptance")
    source_by_identity[f"{quoted}.guard_custom_import_build_owned()"] = acceptance._guard(schema)
    source_by_identity[f"{quoted}.guard_custom_import_build_evidence()"] = (
        bounded._GUARD_CUSTOM_IMPORT_BUILD_EVIDENCE_BODY
    )
    source_by_identity[f"{quoted}.guard_custom_import_build_graph()"] = bounded._GUARD_CUSTOM_IMPORT_BUILD_GRAPH_BODY
    for name, arguments, _result in (*bulk._WRITERS, *legacy._WRITERS):
        if name not in (*migration._BULK_LEAVES, *migration._BULK_OWNER_LEAVES) and (
            not name.startswith("persist_custom_import_legacy_")
            or name == "persist_custom_import_legacy_generation_family_set"
        ):
            continue
        if name in continuation._LEAF_NAMES:
            writer_statement = continuation._leaf_statement(name, schema, corrected=True)
        elif name == child._WRITER:
            writer_statement = child._leaf(bulk, schema, corrected=True)
        else:
            statements = (
                legacy._resource("legacy_writers.sql")
                if name.startswith("persist_custom_import_legacy_")
                else bulk._resource("snapshot_writers.sql")
            )
            writer_statement = next(sql for sql in statements if f"CREATE FUNCTION __CANDIDATE__.{name}(" in sql)
        writer_statement = (
            writer_statement.replace("__CANDIDATE__", "ci_snapshot_41")
            .replace("__BASE__", schema)
            .replace("__FAMILY_ID__", "41::bigint")
        )
        source_by_identity[f'"ci_snapshot_41".{name}({bulk._argument_types(arguments)})'] = writer_statement
    return source_by_identity


def test_names_refresh_migrates_each_registered_bulk_and_legacy_writer(monkeypatch):
    """Refresh every registered writer without discarding logical authority guards."""
    migration = _names_migration()
    schema = "synthetic_schema"
    quoted = migration._storage()._quote(schema)
    source_by_identity = _writer_migration_sources(migration, schema)
    connection = SimpleNamespace(
        execute=lambda *_args: SimpleNamespace(scalars=lambda: [41]),
        scalar=lambda _query, values: values["identity"] in source_by_identity,
    )
    monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
    body_by_identity = {}
    monkeypatch.setattr(
        migration,
        "_refresh",
        lambda identity, transform: body_by_identity.setdefault(
            identity, transform(source_by_identity.get(identity, "old"))
        ),
    )
    migration._refresh_writers(schema)
    assert len(body_by_identity) == 29
    finalizer = body_by_identity[f"{quoted}.source_set_finalize(uuid,integer[])"]
    assert finalizer.count("IF NOT closed_authority.accepting THEN") == 1
    assert migration._source_finalizer_body(finalizer, schema) == finalizer
    for identity, function_body in body_by_identity.items():
        assert all(
            field_name not in function_body
            for field_name in ("landing_table_oid", "origin_table_oid", "columns_sha256", "r.table_oid", "owner_name")
        ), identity
    for name in (
        "verify_custom_import_snapshot_writers",
        "verify_custom_import_legacy_snapshot_writers",
        "verify_custom_import_materialization_writers",
    ):
        function_body = body_by_identity[f"{quoted}.{name}(bigint)"]
        assert "to_regprocedure(format('%I.%s',namespace,signature)) IS NULL" in function_body
        assert "pg_proc" not in function_body and "aclexplode" not in function_body
    expected = body_by_identity[f"{quoted}.check_custom_import_materialization_expected(bigint[],bytea,timestamptz)"]
    assert "l.continuation_id IS NOT NULL" in expected and "lock_custom_import_snapshot_attempt" in expected
    assert "p_token" in expected and "p_deadline" in expected
    guarded = body_by_identity[f"{quoted}.guard_custom_import_build_owned()"]
    assert "custom_import_manual_acceptance_invalid" in guarded
    assert "to_jsonb(OLD)-allowed IS DISTINCT FROM to_jsonb(NEW)-allowed" in guarded
    assert "TG_LEVEL<>'ROW'" in guarded and "pg_trigger_depth() NOT IN (1,2)" in guarded


def test_names_migration_refuses_drops_when_an_unconverted_function_remains(monkeypatch):
    migration = _names_migration()
    connection = SimpleNamespace(execute=lambda *_args: SimpleNamespace(all=lambda: [("synthetic", "writer")]))
    monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
    with pytest.raises(RuntimeError, match="unconverted_function"):
        migration._check_remaining_references("synthetic")


def test_names_migration_is_forward_only_and_drops_only_physical_registry_fields(monkeypatch):
    names = _names_migration()
    migration = names._previous("20261114020000_custom_import_snapshot_metadata")
    assert migration.down_revision == names.revision
    drops = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_schema")
    monkeypatch.setattr(migration, "_names", lambda: names)
    monkeypatch.setattr(names, "_check_remaining_references", lambda _schema: None)
    body_by_identity = {}
    monkeypatch.setattr(
        names, "_refresh", lambda identity, transform: body_by_identity.setdefault(identity, transform("old"))
    )
    monkeypatch.setattr(migration.op, "create_check_constraint", lambda *args, **kwargs: None)
    monkeypatch.setattr(migration.op, "drop_column", lambda table, column, **_kwargs: drops.append((table, column)))
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    assert drops == [("source_bulk_authorization", "opened_by")]
    column_drops = [statement for statement in statements if "DROP COLUMN" in statement]
    assert len(column_drops) == 15
    assert all(statement.endswith(("table_oid", "table_owner", "columns_sha256")) for statement in column_drops)
    assert 'DROP FUNCTION "synthetic_schema".custom_import_snapshot_columns_sha256(bigint)' in statements
    creation = body_by_identity['"synthetic_schema".create_custom_import_snapshot_family(bigint,bigint,bytea)']
    assert "custom_import_snapshot_reader_rollout_required" not in creation
    assert "INSERT INTO" in creation and "lock_custom_import_snapshot_attempt" in creation
    with pytest.raises(RuntimeError, match="requires_forward_migration"):
        migration.downgrade()


def test_compatibility_revision_keeps_old_reader_columns_and_legacy_source_default(monkeypatch):
    migration = _names_migration()
    column_names_by_table = {
        "custom_import_snapshot_family": [
            f"{prefix}_{suffix}"
            for prefix in ("landing", "origin")
            for suffix in ("table_oid", "table_owner", "columns_sha256")
        ],
        "custom_import_snapshot_relation": ["table_oid", "table_owner", "columns_sha256"],
        "custom_import_query_projection": [
            f"{prefix}_{suffix}"
            for prefix in ("root", "child")
            for suffix in ("table_oid", "table_owner", "columns_sha256")
        ],
        "source_bulk_authorization": ["opened_by"],
    }
    connection = SimpleNamespace(
        execute=lambda _query, params: SimpleNamespace(scalars=lambda: column_names_by_table[params["table"]])
    )
    monkeypatch.setattr(migration.op, "get_bind", lambda: connection)
    statements, nullable = [], []
    monkeypatch.setattr(migration.op, "execute", lambda query: statements.append(str(query)))
    monkeypatch.setattr(migration.op, "create_check_constraint", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        migration.op, "alter_column", lambda table, column, **kwargs: nullable.append((table, column, kwargs))
    )
    migration._prepare_metadata_compatibility("synthetic")
    assert len(nullable) == 17 and all(kwargs["nullable"] is True for _, _, kwargs in nullable[:-1])
    table, column, options = nullable[-1]
    assert (table, column) == ("source_bulk_authorization", "opened_by")
    assert str(options["server_default"]) == "session_user"
    assert not any("DROP COLUMN" in query or "DROP FUNCTION" in query for query in statements)
