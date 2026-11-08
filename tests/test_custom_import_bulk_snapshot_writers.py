# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installation and exact ABI checks for the fixed snapshot writer boundary."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from process.custom_import.build_source import _SOURCE_COPY_COLUMNS


def _migration():
    path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20261005040000_custom_import_bulk_snapshot_writers.py"
    )
    spec = importlib.util.spec_from_file_location("bulk_snapshot_writers_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _compact(value):
    return re.sub(r"\s+", "", value)


def test_fixed_leaf_resource_exactly_matches_all_exported_abis():
    migration = _migration()
    landing, *writers = migration._resource("snapshot_writers.sql")
    assert "CREATE TABLE __CANDIDATE__.source_bulk_landing" in landing
    assert "FOREIGN KEY" not in landing and "REFERENCES" not in landing
    assert "PRIMARY KEY(batch_id,landing_ordinal)" in landing
    assert "UNIQUE(batch_id,source_ordinal)" in landing
    assert len(writers) == len(migration._WRITERS) == 15
    for statement, (name, arguments, result) in zip(writers, migration._WRITERS, strict=True):
        expected = f"CREATE FUNCTION __CANDIDATE__.{name}({arguments}) RETURNS {result} LANGUAGE"
        assert _compact(statement).startswith(_compact(expected))
        assert "SECURITY DEFINER SET search_path=pg_catalog" in statement
        assert "__CONTROL__.lock_custom_import_build(" in statement
        assert "CREATE OR REPLACE" not in statement
        assert "__SCHEMA__" not in statement
        assert "DROP TRIGGER" not in statement and "DISABLE TRIGGER" not in statement


def test_upgrade_preserves_existing_dispatcher_acls_and_denies_new_entrypoints(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_control")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    creates = [statement for statement in statements if statement.lstrip().startswith("CREATE OR REPLACE FUNCTION")]
    assert len(creates) == len(migration._EXISTING) == 6
    for name in migration._EXISTING:
        assert any(f'"synthetic_control".{name}(' in statement for statement in creates)
        assert not any(statement.lstrip().startswith("DO ") and f".{name}(" in statement for statement in statements)
    # New control tables, pure codecs and every new callable remove nonowner defaults.
    acl_blocks = [statement for statement in statements if statement.lstrip().startswith("DO ")]
    assert len(acl_blocks) == 19
    assert all("aclexplode" in statement and "rights.grantee<>c." in statement for statement in acl_blocks)
    assert not any(statement.lstrip().startswith(("INSERT", "UPDATE", "DELETE", "GRANT")) for statement in statements)
    assert not any("DROP TRIGGER" in statement or "DISABLE TRIGGER" in statement for statement in statements)
    assert not any("__CONTROL__" in statement or "__LEAF_DDL__" in statement for statement in statements)


@pytest.mark.parametrize("name,arguments,result", _migration()._WRITERS)
def test_each_dispatcher_has_fixed_target_real_binding_and_fresh_authority(name, arguments, result):
    migration = _migration()
    body = migration._dispatcher(name, arguments, result)
    assert f"%I.{name}(" in body
    assert "resolve_custom_import_build_base_snapshot(build_id)" in body
    assert "namespace:='ci_snapshot_'||family_id::text" in body
    assert body.rindex("lock_custom_import_build(build_id)") > body.index("EXECUTE format('SELECT")
    if name == "source_set_finalize":
        assert "resolve_custom_import_source_batch_snapshot(p_batch)" in body
        assert "verify_custom_import_snapshot_writers(family_id)" in body
        assert "a.batch_id=p_batch" in body
    else:
        assert "resolve_custom_import_build_snapshot(build_id)" in body
    assert "WHEN undefined_function" not in body and "EXCEPTION WHEN" not in body


def test_copy_grant_is_exact_transport_only_and_follows_real_batch_binding():
    migration = _migration()
    assert tuple(migration._COPY_COLUMNS.split(",")) == _SOURCE_COPY_COLUMNS
    writer = next(writer for writer in migration._WRITERS if writer[0] == "source_bulk_authorize")
    body = migration._dispatcher(*writer)
    assert body.index("resolve_custom_import_source_batch_snapshot(answer)") < body.index("GRANT INSERT")
    assert "namespace,session_user" in body
    assert "GRANT INSERT" not in migration._INSTALL_BODY
    for column in ("transaction_id", "accepting", "pack_id", "root_id", "outcome_id"):
        assert column not in migration._COPY_COLUMNS.split(",")


def test_finalize_revokes_transport_after_the_last_owned_transaction_batch():
    migration = _migration()
    writer = next(writer for writer in migration._WRITERS if writer[0] == "source_set_finalize")
    body = migration._dispatcher(*writer)
    assert body.index("INTO answer USING") < body.index("REVOKE INSERT") < body.rindex("lock_custom_import_build")
    assert f"REVOKE INSERT ({migration._COPY_COLUMNS})" in body
    assert "closed.batch_id=p_batch" in body
    assert "a.opened_by=session_user" in body and "a.transaction_id=pg_current_xact_id() AND a.accepting" in body
    assert "REVOKE USAGE" not in body


def test_installer_has_fixed_ddl_and_complete_owner_signature_default_acl_checks():
    migration = _migration()
    body = migration._body(migration._storage(), "synthetic_control", migration._INSTALL_BODY)
    assert body.count("CREATE TABLE __CANDIDATE__.source_bulk_landing") == 1
    assert body.count("CREATE FUNCTION __CANDIDATE__.") == 15
    assert "lock_custom_import_writable_snapshot" in body
    assert "landing_columns_sha256=" in body and "frozen_at IS NULL" in body
    assert "RETURN f.family_id" in body
    assert "REVOKE ALL ON FUNCTION %s FROM PUBLIC" in body
    assert "a.grantee<>p.proowner" in body and "a.grantee<>c.relowner" in body
    verify = migration._VERIFY_WRITERS_BODY
    assert "p.proowner<>owner_oid" in verify and "NOT p.prosecdef" in verify
    assert "pg_get_function_result(p.oid)" in verify and "to_regprocedure" in verify
    assert "a.grantee<>p.proowner" in verify and "a.privilege_type='CREATE'" in verify
    assert "'__FAMILY_ID__',f.family_id::text||'::bigint'" in body


def test_revision_homes_append_only_actual_fresh_bounded_insertions():
    writers_by_name = dict(
        zip(
            (spec[0] for spec in _migration()._WRITERS), _migration()._resource("snapshot_writers.sql")[1:], strict=True
        )
    )
    source = writers_by_name["source_set_finalize"]
    assert "WHERE batch_id=p_batch AND rejection_code IS NULL" in source
    assert source.index("append_custom_import_revision_home") > source.index(
        "INSERT INTO __CANDIDATE__.custom_import_child_revision"
    )
    assert source.index("append_custom_import_revision_home") < source.index(
        "DELETE FROM __CANDIDATE__.source_bulk_landing"
    )
    for name, kind in (
        ("copy_custom_import_build_retained_roots_page", "root"),
        ("copy_custom_import_build_retained_families_page", "child"),
    ):
        sql = writers_by_name[name]
        assert f"RETURNING custom_import_{kind}_revision.{kind}_revision_id AS inserted_id" in sql
        assert "INTO inserted_revision_ids FROM inserted" in sql
        assert "IF cardinality(inserted_revision_ids)>0 THEN" in sql
        assert "lookup_custom_import_revision_home" in sql
        assert "h.family_id IS DISTINCT FROM __FAMILY_ID__" in sql
        assert sql.count("append_custom_import_revision_home(") == 1


def test_child_sets_admit_physical_batches_but_check_each_logical_policy():
    migration = _migration()
    writer_sql_by_name = dict(
        zip((spec[0] for spec in migration._WRITERS), migration._resource("snapshot_writers.sql")[1:], strict=True)
    )
    for name in ("append_custom_import_build_source_families_page", "copy_custom_import_build_retained_families_page"):
        specification = next(spec for spec in migration._WRITERS if spec[0] == name)
        assert specification[1].endswith("p_page_sizes integer[]")
        sql = writer_sql_by_name[name]
        assert "sum(size::bigint)" in sql and "array_lower(p_page_sizes,1) IS DISTINCT FROM 1" in sql
        assert "n NOT BETWEEN 0 AND 100000" in sql and "+root_n>100000" in sql
        assert "generate_series(1,i.size)" in sql
        assert "GROUP BY page" in sql and ">b.page_row_limit" in sql and ">b.page_byte_limit" in sql
        assert "greatest(work_bytes,model_bytes)>268435456" in sql
        assert sql.count("UPDATE __CONTROL__.custom_import_build_attempt SET candidate_context_count=") == 1
        assert "INSERT INTO __CANDIDATE__.custom_import_child_scalar" in sql and "COPY" not in sql
    retained = writer_sql_by_name["copy_custom_import_build_retained_families_page"]
    assert "PARTITION BY root_id,page ORDER BY ordinality" in retained
    assert "GROUP BY root_id,page HAVING count(DISTINCT slot)<>1" in retained
    assert "GROUP BY m.root_id,m.page,i.replay" in retained
    assert "ORDER BY i.page,i.root_id" in retained
    assert "WHERE m.pack_id=i.pack_id GROUP BY cc.collection_name" in retained
    assert "FROM unnest(p_child_root_ids,p_child_ids,new_ids,member_slots,local_ordinals,pack_ids)" in retained


def test_root_sets_admit_physical_batches_but_check_each_logical_policy():
    migration = _migration()
    writer_sql_by_name = dict(
        zip((spec[0] for spec in migration._WRITERS), migration._resource("snapshot_writers.sql")[1:], strict=True)
    )
    for name in ("start_custom_import_build_source_roots_page", "copy_custom_import_build_retained_roots_page"):
        specification = next(spec for spec in migration._WRITERS if spec[0] == name)
        assert specification[1].endswith("p_page_sizes integer[]")
        sql = writer_sql_by_name[name]
        assert "n NOT BETWEEN 1 AND 100000" in sql and "context_n NOT BETWEEN 0 AND 100000" in sql
        assert "array_ndims(p_page_sizes) IS DISTINCT FROM 1" in sql
        assert "array_lower(p_page_sizes,1) IS DISTINCT FROM 1" in sql
        assert "size IS NULL OR size NOT BETWEEN 1 AND 256" in sql
        assert "sum(size::bigint)" in sql and "IS DISTINCT FROM n::bigint" in sql
        assert "generate_series(1,i.size)" in sql and "GROUP BY page" in sql
        assert ">b.page_row_limit" in sql and "greatest(work,model)>b.page_byte_limit" in sql
        assert "greatest(work_bytes,model_bytes)>268435456" in sql
        assert sql.count("UPDATE __CONTROL__.custom_import_build_attempt SET candidate_context_count=") == 1
        assert "ORDER BY bf.root_record_id FOR UPDATE" in sql
    source_sql = writer_sql_by_name["start_custom_import_build_source_roots_page"]
    assert "5::bigint*n+scalar_n+context_n+entity_n>100000" in source_sql
    assert "sum(scalar_count)>256 OR sum(context_count)>256" in source_sql
    assert "ON CONFLICT(dataset_id,adapter_id,canonical_value) DO NOTHING RETURNING canonical_value" in source_sql
    assert "min(i.page)::integer page" in source_sql and "WHERE i.root_id=ANY(fresh_ids)" in source_sql
    assert "octet_length(inserted.canonical_value)+35::bigint bytes" in source_sql
    assert "UNION ALL SELECT page,1,0,0 FROM unnest(entity_pages) page" in source_sql
    assert "UNION ALL SELECT i.page,0,i.bytes FROM unnest(entity_pages,entity_costs) i(page,bytes)" in source_sql
    retained = writer_sql_by_name["copy_custom_import_build_retained_roots_page"]
    assert "i.page,6::bigint cost" in retained and "work_rows>100000" in retained
    assert "context_count>256" in retained
    assert "jsonb_build_object('root_revision_id',i.revision_id)" in retained


def test_retained_root_batches_keep_singleton_pack_order_and_exact_replay():
    sql = next(
        statement
        for statement in _migration()._resource("snapshot_writers.sql")
        if "CREATE FUNCTION __CANDIDATE__.copy_custom_import_build_retained_roots_page(" in statement
    )
    pack_write = sql.split("INSERT INTO __CANDIDATE__.custom_import_pack(", 1)[1].split(
        "IF EXISTS(SELECT 1 FROM __CONTROL__.lookup_custom_import_revision_home", 1
    )[0]
    assert "build_stream.next_pack_ordinal+(row_number() OVER (ORDER BY i.root_id)-1)::integer" in pack_write
    assert "b.capture_bundle_id,1," in pack_write and "WHERE i.root_id=ANY(fresh_ids)" in pack_write
    assert "p.pack_ordinal>=build_stream.next_pack_ordinal" in sql
    assert "LIMIT 2) exact_pack)<>1" in sql
    assert "graph_roots_retry_copy_mismatch" in sql and "graph_roots_retry_scalar_mismatch" in sql
    assert "graph_roots_retry_context_mismatch" in sql and "graph_roots_lease_lost" in sql
    assert "next_pack_ordinal=bs.next_pack_ordinal+cardinality(fresh_ids)" in sql
    assert "AND cardinality(fresh_ids)>0" in sql


def test_source_identity_probe_matches_the_partial_ordinal_index():
    migration = _migration()
    sql = next(
        statement
        for statement in migration._resource("snapshot_writers.sql")
        if "CREATE FUNCTION __CANDIDATE__.source_set_finalize(" in statement
    )
    probe = sql.split("OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o", 1)[1]
    identity = probe.split("AND ROW(o.pack_id", 1)[0]
    assert (
        _compact(identity)
        == "WHEREo.build_id=b.build_idANDo.origin='source'ANDo.stream_slot=a.stream_slotANDo.source_ordinal=l.source_ordinal"
    )
    assert "LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o" not in sql
    assert "IS NOT DISTINCT FROM ROW(l.pack_id,'source'" in probe
    assert "convert_to(o.raw_parent_key_canonical,'UTF8') IS NOT DISTINCT FROM convert_to(l.raw_key,'UTF8')" in probe
    assert any(
        "(build_id, stream_slot, source_ordinal) WHERE origin = 'source'" in statement
        for statement in migration._storage()._LOAD_DDL
    )


@pytest.mark.parametrize(
    "name", ["append_custom_import_build_source_families_page", "copy_custom_import_build_retained_families_page"]
)
def test_child_physical_tips_are_bounded_and_all_charge_actual_children(name):
    sql = next(
        statement
        for statement in _migration()._resource("snapshot_writers.sql")
        if f"CREATE FUNCTION __CANDIDATE__.{name}(" in statement
    )
    assert "root_n NOT BETWEEN 1 AND 100000" in sql
    assert "n=0 AND (root_n>256 OR root_n>b.page_row_limit)" in sql
    assert "n>0 AND EXISTS(SELECT 1 FROM unnest(p_root_ids) i(root_id)" in sql
    assert "WHERE NOT root_id=ANY(p_child_root_ids)" in sql
    root_costs = sql.split("WITH costs AS (", 1)[1].split("), pages AS", 1)[0]
    assert "FROM unnest(p_child_root_ids,child_pages) i(root_id,page) GROUP BY page,root_id" in root_costs
    expected = "UNION ALL SELECT page,1,0,0" if "retained" in name else "UNION ALL SELECT page,1 FROM"
    assert expected in root_costs
    assert "size NOT BETWEEN 0 AND 256" in sql and "size>b.page_row_limit" in sql
    assert "+root_n>100000" in sql and ">268435456" in sql


def test_admission_rejections_use_the_stable_global_sequence():
    sql = next(
        statement
        for statement in _migration()._resource("snapshot_writers.sql")
        if "CREATE FUNCTION __CANDIDATE__.admit_custom_import_build_page(" in statement
    )
    insertion = sql.split("new_rejections AS (", 1)[1].split("events AS MATERIALIZED", 1)[0]
    assert "rejection_id,execution_id,rejection_ordinal" in insertion
    assert "nextval('__CONTROL__.custom_import_rejection_rejection_id_seq'::regclass)" in insertion
    assert "ORDER BY d.occurrence_id" in insertion


def test_source_refreshes_only_candidate_statistics_before_identity_validation():
    sql = next(
        statement
        for statement in _migration()._resource("snapshot_writers.sql")
        if "CREATE FUNCTION __CANDIDATE__.source_set_finalize(" in statement
    )
    promoted = sql.index("source_set_occurrence_count_mismatch")
    validation = sql.index("IF EXISTS(WITH expected AS")
    landing_analysis = "ANALYZE __CANDIDATE__.source_bulk_landing("
    first_analysis = sql.index(landing_analysis)
    assert (
        sql.index("source_bulk_row_mismatch")
        < first_analysis
        < sql.index("UPDATE __CANDIDATE__.source_bulk_landing l SET outcome_id=")
    )
    assert promoted < sql.index(landing_analysis, first_analysis + 1) < validation
    assert promoted < sql.index("ANALYZE __CANDIDATE__.custom_import_build_occurrence(") < validation
    assert "a.first_source=0 OR b.source_occurrence_count+n>2*" in sql
    assert "WHERE oid='__CANDIDATE__.custom_import_build_occurrence'::regclass" in sql
    landing_stats = sql.split("ANALYZE __CANDIDATE__.source_bulk_landing(", 1)[1].split(");", 1)[0]
    assert "payload" not in landing_stats and "raw_key" not in landing_stats
    cleanup = sql.split("DELETE FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch;", 1)[1]
    assert cleanup.startswith(
        "\n    IF NOT EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing) THEN\n"
        "        TRUNCATE __CANDIDATE__.source_bulk_landing;\n    END IF;"
    )
    assert "ANALYZE __CONTROL__" not in sql
    assert "VACUUM" not in sql


def test_source_replay_checks_one_bounded_stored_set_without_appending():
    sql = _migration()._resource("snapshot_writers.sql")[-1]
    assert "p_count NOT BETWEEN 1 AND 100000" in sql
    assert "o.part_row_ordinal<p_first_row+p_count" in sql
    assert "lookup_custom_import_revision_home(root_ids,child_ids)" in sql
    assert "h.family_id IS DISTINCT FROM __FAMILY_ID__" in sql
    assert "append_custom_import_revision_home" not in sql


def test_base_null_requires_absence_or_exact_real_sealed_legacy_resolution():
    migration = _migration()
    body = migration._SEALED_BASE_BODY
    assert "IF p_generation_id IS NULL THEN RETURN NULL" in body
    assert "g.dataset_id IS DISTINCT FROM p_dataset_id" in body
    assert "resolve_custom_import_generation_snapshot(" in body
    assert "g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id" in body
    assert "IN ACCESS SHARE MODE" in body
    assert "EXCEPTION WHEN" not in body
    assert "g.generation_id IS NULL" in migration._INSTALL_BODY
    assert "base_generation:=g.base_generation_id" in migration._INSTALL_BODY


def test_bootstrap_never_substitutes_an_empty_candidate_for_committed_shared_rows():
    body = _migration()._BUILD_SNAPSHOT_BODY
    assert "custom_import_snapshot_existing_build_requires_migration" in body
    assert "next_source_ordinal<>0" in body and "custom_import_pack WHERE execution_id=b.execution_id" in body
    assert "create_custom_import_snapshot_family" in body
    assert "lock_custom_import_snapshot_finality(b.generation_id,b.dataset_id" in body
    assert "RETURN NULL" not in body


def test_output_wrappers_bind_generation_and_close_actual_snapshot_after_phase_transition():
    migration = _migration()
    for name, hook in (
        ("open_custom_import_build_output", "bind_custom_import_snapshot_generation"),
        ("freeze_custom_import_build_output", "freeze_custom_import_snapshot_family"),
    ):
        writer = next(writer for writer in migration._WRITERS if writer[0] == name)
        body = migration._dispatcher(*writer)
        assert body.index("EXECUTE format('SELECT") < body.index(hook) < body.rindex("lock_custom_import_build")
    frozen_output = next(
        sql
        for sql in migration._resource("snapshot_writers.sql")
        if "CREATE FUNCTION __CANDIDATE__.freeze_custom_import_build_output" in sql
    )
    assert "__CANDIDATE__.custom_import_build_candidate_context" in frozen_output
    assert "__CONTROL__.custom_import_build_candidate_context" not in frozen_output


def test_identifier_and_regclass_literal_quoting_is_not_caller_text_routing():
    migration = _migration()
    storage = migration._storage()
    schema = "synthetic_\"quoted'schema"
    rendered = migration._control_sql(storage, schema, "SELECT '__CONTROL__.fixed'::regclass, __CONTROL__.fixed")
    assert rendered == 'SELECT E\'"synthetic_""quoted\'\'schema".fixed\'::regclass, "synthetic_""quoted\'schema".fixed'
    with pytest.raises(ValueError, match="unknown bulk writer resource"):
        migration._resource("untrusted.sql")


def test_downgrade_preserves_retained_snapshots_and_restores_only_existing_dispatchers(monkeypatch):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: "synthetic_control")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert "landing_table_oid IS NOT NULL" in statements[0]
    assert "custom_import_bulk_snapshot_writers_downgrade_blocked" in statements[0]
    restored_statements = [statement for statement in statements if statement.startswith("CREATE OR REPLACE FUNCTION")]
    assert len(restored_statements) == 6
    assert all("__CANDIDATE__" not in statement for statement in restored_statements)
    assert not any(
        "CASCADE" in statement or "DROP SCHEMA" in statement or "DROP TRIGGER" in statement for statement in statements
    )
