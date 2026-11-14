# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve retained snapshots by qualified name without physical registry metadata."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261114010000_custom_import_snapshot_names"
down_revision = "20261115010000_ms_drg_companion_tables"
branch_labels = None
depends_on = None

_RESOURCES = Path(__file__).resolve().parents[1] / "sql/custom_import_snapshot_names"
_AUTHORITY = "bigint,bigint,bytea"
_FINALITY = "bigint,bigint,bigint,bigint,bigint,bigint,bigint,bytea"
_BULK_LEAVES = (
    "source_bulk_authorize",
    "source_set_finalize",
    "finish_custom_import_build_source_part",
    "freeze_custom_import_build_source",
    "admit_custom_import_build_page",
)
_BULK_OWNER_LEAVES = (
    "start_custom_import_build_source_roots_page",
    "copy_custom_import_build_retained_roots_page",
    "append_custom_import_build_source_families_page",
    "copy_custom_import_build_retained_families_page",
    "membership_batch_finalize",
    "winner_batch_finalize",
)


def _previous(name):
    path = Path(__file__).with_name(name + ".py")
    spec = importlib.util.spec_from_file_location("snapshot_names_" + name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _storage():
    return _previous("20261005030000_custom_import_snapshot_storage")


def _schema():
    return _storage()._schema()


def _replace(source, old, new, *, count=1):
    if source.count(old) != count:
        raise RuntimeError("custom_import_snapshot_names_source_mismatch")
    return source.replace(old, new)


def _remove_section(source, start, end):
    if source.count(start) != 1:
        raise RuntimeError("custom_import_snapshot_names_source_mismatch")
    first = source.index(start)
    last = source.index(end, first)
    return source[:first] + source[last:]


def _present(table):
    return f"to_regclass(format('%I.{table}','ci_snapshot_' || f.family_id::text))"


def _optional_relations(source):
    for old, table in (("landing_table_oid", "source_bulk_landing"), ("origin_table_oid", "legacy_copy_origin")):
        source = source.replace(f"f.{old} IS NOT NULL", f"{_present(table)} IS NOT NULL")
        source = source.replace(f"f.{old} IS NULL", f"{_present(table)} IS NULL")
    return source


def _refresh(identity, transform):
    """Replace an existing body; PostgreSQL retains its signature and grants."""
    installed = (
        op.get_bind()
        .execute(
            text(
                "SELECT prosrc,pg_get_functiondef(oid) AS definition FROM pg_proc WHERE oid=to_regprocedure(:identity)"
            ),
            {"identity": identity},
        )
        .one_or_none()
    )
    if installed is None:
        raise RuntimeError("custom_import_snapshot_names_function_missing")
    updated = transform(installed.prosrc)
    if updated == installed.prosrc:
        raise RuntimeError("custom_import_snapshot_names_function_unchanged")
    op.execute(text(_replace(installed.definition, installed.prosrc, updated)))


def _create_body(source, *, allow_new=False):
    source = _remove_section(source, "    IF EXISTS (\n        SELECT 1 FROM unnest(names) name", "    INSERT INTO")
    source = _replace(
        source,
        "            (family_id,relation_slot,table_oid,table_owner,columns_sha256)\n"
        "        SELECT f.family_id,slot,relation::bigint,c.relowner::bigint,\n"
        "            __SCHEMA__.custom_import_snapshot_columns_sha256(relation::bigint)\n"
        "        FROM pg_class c WHERE c.oid=relation;",
        "            (family_id,relation_slot) VALUES(f.family_id,slot);",
    )
    if not allow_new:
        source = _replace(
            source,
            "    INSERT INTO __SCHEMA__.custom_import_snapshot_family",
            "    RAISE EXCEPTION 'custom_import_snapshot_reader_rollout_required';\n"
            "    INSERT INTO __SCHEMA__.custom_import_snapshot_family",
        )
    return source


def _freeze_body(source):
    source = _optional_relations(source)
    source = _replace(source, "has_landing_rows boolean;", "has_landing_rows boolean; landing_oid oid;")
    source = _replace(
        source,
        "        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)',f.landing_table_oid::oid::regclass)",
        f"        landing_oid := {_present('source_bulk_landing')};\n"
        "        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)',landing_oid::regclass)",
    )
    source = source.replace("f.landing_table_oid::oid", "landing_oid")
    return _replace(
        source,
        "rights.grantee<>f.landing_table_owner::oid",
        "rights.grantee<>(SELECT relowner FROM pg_class WHERE oid=landing_oid)",
    )


def _refresh_storage(schema):
    storage = _storage()
    quoted = storage._quote(schema)
    definition_by_name = {
        "resolve_custom_import_snapshot_relations": ("bigint", (_RESOURCES / "relations.sql").read_text()),
        "create_custom_import_snapshot_family": (_AUTHORITY, _create_body(storage._CREATE_BODY)),
        "lock_custom_import_writable_snapshot": (_AUTHORITY, _optional_relations(storage._WRITE_BINDING_BODY)),
        "lock_custom_import_snapshot_finality": (_FINALITY, _optional_relations(storage._FINALITY_BINDING_BODY)),
        "freeze_custom_import_snapshot_family": (_AUTHORITY, _freeze_body(storage._FREEZE_BODY)),
    }
    for name, (arguments, body) in definition_by_name.items():
        rendered = " " + storage._sql(schema, storage._body(schema, body)) + " "
        _refresh(f"{quoted}.{name}({arguments})", lambda _source, value=rendered: value)
    _refresh(
        f"{quoted}.resolve_custom_import_generation_snapshot(bigint,bigint,bigint,bigint)",
        _optional_relations,
    )


def _verify_body(source, *, legacy=False, materialization=False):
    source = source.replace("; owner_oid oid;", ";")
    source = _replace(source, "    SELECT oid INTO owner_oid FROM pg_roles WHERE rolname=current_user;\n", "")
    start = "    IF f.origin_table_oid" if legacy else "    IF f.family_id IS NULL"
    end = "    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations"
    source = _remove_section(source, start, end)
    required = "legacy_copy_origin" if legacy else "source_bulk_landing"
    if not materialization:
        source = _replace(
            source,
            end,
            f"    IF {_present(required)} IS NULL THEN\n"
            "        RAISE EXCEPTION 'custom_import_snapshot_writer_binding_mismatch'; END IF;\n" + end,
        )
    first = source.index("    IF EXISTS", source.index(end))
    last = source.index(" THEN", first) + len(" THEN")
    return (
        source[:first] + "    IF EXISTS(SELECT 1 FROM unnest(__SIGNATURES__) signature\n"
        "        WHERE to_regprocedure(format('%I.%s',namespace,signature)) IS NULL) THEN" + source[last:]
    )


def _without_source_login(source):
    """The native transaction and signed producer scope bind each COPY batch."""
    return (
        source.replace("source_bulk_authorization(opened_by,build_id", "source_bulk_authorization(build_id")
        .replace("VALUES(session_user,p_build", "VALUES(p_build")
        .replace(" OR a.opened_by<>session_user", "")
        .replace("authorized.opened_by=session_user AND ", "")
        .replace(" AND a.opened_by=session_user", "")
    )


def _bulk_leaf(source, family_id):
    pattern = (
        r"r\.table_oid='[^']+\.custom_import_root_record'::regclass::oid\s+"
        r"AND f\.landing_table_oid='[^']+\.source_bulk_landing'::regclass::oid\s+"
        r"AND f\.landing_table_owner IS NOT NULL AND f\.landing_columns_sha256 IS NOT NULL"
    )
    changed, count = re.subn(pattern, f"f.family_id={family_id}", source)
    if count != 1:
        raise RuntimeError("custom_import_snapshot_names_leaf_source_mismatch")
    return _without_source_login(changed)


def _without_owner_gate(source):
    source, count = re.subn(
        r"    SELECT pg_get_userbyid\(proowner\) INTO owner_name FROM pg_proc WHERE oid=\s*"
        r"E?'(?:[^']|'')+'::regprocedure;\s*"
        r"IF (?:current_user IS DISTINCT FROM owner_name|owner_name IS NULL OR current_user<>owner_name) THEN\s*"
        r"RAISE EXCEPTION '[a-z_]+owner_mismatch'; END IF;\n",
        "",
        source,
    )
    if count != 1:
        raise RuntimeError("custom_import_snapshot_names_owner_source_mismatch")
    return source.replace("owner_name name; ", "").replace(" owner_name name;", "")


def _without_trigger_owner_gate(source):
    source, count = re.subn(
        r"[ ]+SELECT pg_get_userbyid\(proowner\) INTO owner_name FROM pg_proc WHERE oid=\s*"
        r"E?'(?:[^']|'')+'::regprocedure;\n",
        "",
        source,
    )
    if count != 1:
        raise RuntimeError("custom_import_snapshot_names_trigger_source_mismatch")
    if " OR current_user<>owner_name" in source:
        source = _replace(source, " OR current_user<>owner_name", "")
    else:
        source = _replace(
            source,
            "                IF current_user<>owner_name THEN RAISE EXCEPTION 'custom_import_build_protected_write'; END IF;\n",
            "",
        )
    return source.replace("owner_name name; ", "").replace(" owner_name name;", "")


def _bulk_installer(schema):
    unlogged = _previous("20261104010000_custom_import_unlogged_landing")
    continuation = unlogged._continuation()
    child = continuation._previous("20261009000000_custom_import_child_presence_decode")
    bulk = _previous("20261005040000_custom_import_bulk_snapshot_writers")
    storage = bulk._storage()
    installer_body = next(unlogged._definitions(schema))[3].split("$bulk_snapshot$")[1]
    for statement in bulk._resource("snapshot_writers.sql"):
        if any(statement.startswith(f"CREATE FUNCTION __CANDIDATE__.{name}(") for name in _BULK_LEAVES):
            original_statement = bulk._control_sql(storage, schema, statement)
            installer_body = _replace(
                installer_body,
                storage._literal(original_statement),
                storage._literal(_bulk_leaf(original_statement, "__FAMILY_ID__")),
            )
    for name in _BULK_OWNER_LEAVES:
        if name in continuation._LEAF_NAMES:
            original_statement = continuation._leaf_statement(name, schema, corrected=True)
        elif name == child._WRITER:
            original_statement = child._leaf(bulk, schema, corrected=True)
        else:
            statement = next(
                leaf_statement
                for leaf_statement in bulk._resource("snapshot_writers.sql")
                if leaf_statement.startswith(f"CREATE FUNCTION __CANDIDATE__.{name}(")
            )
            original_statement = bulk._control_sql(storage, schema, statement)
        installer_body = _replace(
            installer_body,
            storage._literal(original_statement),
            storage._literal(_without_owner_gate(original_statement)),
        )
    installer_body = _optional_relations(installer_body)
    installer_body = installer_body.replace("relation_oid oid; owner_oid oid;", "relation_oid oid;")
    installer_body = _remove_section(
        installer_body,
        "    SELECT c.oid,c.relowner INTO relation_oid,owner_oid",
        "    EXECUTE format('REVOKE ALL ON TABLE",
    )
    installer_body = _replace(
        installer_body,
        "    EXECUTE format('REVOKE ALL ON TABLE %I.source_bulk_landing FROM PUBLIC',namespace);",
        "    relation_oid:=to_regclass(format('%I.source_bulk_landing',namespace));\n"
        "    EXECUTE format('REVOKE ALL ON TABLE %I.source_bulk_landing FROM PUBLIC',namespace);",
    )
    quoted = storage._quote(schema)
    return _without_source_login(
        _remove_section(
            installer_body,
            f"    UPDATE {quoted}.custom_import_snapshot_family SET landing_table_oid=",
            f"    PERFORM {quoted}.verify_custom_import_snapshot_writers",
        )
    )


def _legacy_installer(schema):
    legacy = _previous("20261005050000_custom_import_legacy_snapshot_writers")
    bulk = legacy._bulk()
    storage = bulk._storage()
    source = _optional_relations(legacy._INSTALL_BODY)
    source = _remove_section(
        source,
        "    UPDATE __CONTROL__.custom_import_snapshot_family SET origin_table_oid=",
        "    PERFORM __CONTROL__.verify_custom_import_legacy_snapshot_writers",
    )
    rendered = legacy._render(bulk, storage, schema, source)
    for statement in legacy._resource("legacy_writers.sql"):
        if "owner_name name;" in statement:
            old = bulk._control_sql(storage, schema, statement)
            rendered = _replace(rendered, storage._literal(old), storage._literal(_without_owner_gate(old)))
    return rendered


def _batch_body(source):
    source = _replace(
        source,
        "        OR f.landing_table_oid IS NULL OR f.landing_table_owner IS NULL OR f.landing_columns_sha256 IS NULL\n",
        "",
    )
    start = "    IF NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace"
    first = source.index(start)
    last = source.index(" END IF;", first) + len(" END IF;")
    return _without_source_login(source[:first] + source[last:])


def _source_finalizer_body(source, schema):
    """Close an ordinary completed batch through the existing owner dispatcher."""
    bulk = _previous("20261005040000_custom_import_bulk_snapshot_writers")
    block = (_RESOURCES / "source_completion.sql").read_text()
    block = block.replace("__CLOSE_SOURCE_LANDING_INSERT__", bulk._CLOSE_SOURCE_LANDING_INSERT)
    block = bulk._control_sql(bulk._storage(), schema, block)
    if block in source or _without_source_login(block) in source:
        return source
    return _replace(source, "    BEGIN\n", "    BEGIN\n" + block)


def _refresh_writers(schema):
    bulk = _previous("20261005040000_custom_import_bulk_snapshot_writers")
    legacy = _previous("20261005050000_custom_import_legacy_snapshot_writers")
    materialization = _previous("20261005070000_custom_import_materialization_storage")
    storage = bulk._storage()
    quoted = storage._quote(schema)
    definition_by_name = {
        "verify_custom_import_snapshot_writers": bulk._body(storage, schema, _verify_body(bulk._VERIFY_WRITERS_BODY)),
        "install_custom_import_snapshot_writers": _bulk_installer(schema),
        "verify_custom_import_legacy_snapshot_writers": legacy._render(
            bulk, storage, schema, _verify_body(legacy._VERIFY_BODY, legacy=True)
        ),
        "install_custom_import_legacy_snapshot_writers": _legacy_installer(schema),
    }
    statement = materialization._resource("installation.sql")[0]
    body = _verify_body(statement.split("$fn$")[1], materialization=True)
    definition_by_name["verify_custom_import_materialization_writers"] = materialization._installation(
        bulk, storage, schema, body
    )
    for name, body in definition_by_name.items():
        _refresh(f"{quoted}.{name}(bigint)", lambda _source, value=body: value)
    _refresh(f"{quoted}.resolve_custom_import_source_batch_snapshot(uuid)", _batch_body)
    _refresh(
        f"{quoted}.source_set_finalize(uuid,integer[])",
        lambda source: _without_source_login(_source_finalizer_body(source, schema)),
    )
    for signature in (
        "lock_custom_import_legacy_generation(bigint)",
        "check_custom_import_materialization_expected(bigint[],bytea,timestamptz)",
        "lock_custom_import_materialization_set(bigint,bigint[],bigint[],bigint,bigint[],bytea,timestamptz)",
    ):
        _refresh(f"{quoted}.{signature}", _without_owner_gate)
    for name in (
        "guard_custom_import_build_owned",
        "guard_custom_import_build_evidence",
        "guard_custom_import_build_graph",
    ):
        _refresh(f"{quoted}.{name}()", _without_trigger_owner_gate)
    families = op.get_bind().execute(
        text(f"SELECT family_id FROM {quoted}.custom_import_snapshot_family ORDER BY family_id")
    )
    for family_id in families.scalars():
        namespace = f"ci_snapshot_{family_id}"
        signatures = [
            (name, arguments, _bulk_leaf if name in _BULK_LEAVES else _without_owner_gate)
            for name, arguments, _result in (*bulk._WRITERS, *legacy._WRITERS)
            if name in (*_BULK_LEAVES, *_BULK_OWNER_LEAVES)
            or name.startswith("persist_custom_import_legacy_")
            and name != "persist_custom_import_legacy_generation_family_set"
        ]
        for name, arguments, transform in signatures:
            identity = f"{storage._quote(namespace)}.{name}({bulk._argument_types(arguments)})"
            if op.get_bind().scalar(text("SELECT to_regprocedure(:identity) IS NOT NULL"), {"identity": identity}):
                _refresh(
                    identity,
                    lambda source, fn=transform, family=family_id: (
                        fn(source, family) if fn is _bulk_leaf else fn(source)
                    ),
                )


def _index_body(source):
    source = _replace(source, "AND c.relowner=heap.relowner AND c.relpersistence='p'", "AND c.relpersistence='p'")
    return source.replace(
        "FROM __CONTROL__.custom_import_snapshot_relation WHERE family_id=f.family_id",
        "FROM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id)",
    )


def _check_remaining_references(schema):
    quoted = _storage()._quote(schema)
    remaining = (
        op.get_bind()
        .execute(
            text(f"""
            SELECT n.nspname,p.proname
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE (n.nspname=:schema OR n.nspname IN (
                SELECT 'ci_snapshot_'||family_id::text FROM {quoted}.custom_import_snapshot_family))
              AND p.proname<>'custom_import_snapshot_columns_sha256'
              AND p.prosrc ~ :references
        """),
            {
                "schema": schema,
                "references": r"(landing|origin|root|child)_(table_oid|table_owner|columns_sha256)"
                r"|[.]table_owner|[.]columns_sha256|r[.]table_oid|custom_import_snapshot_columns_sha256"
                r"|[.]opened_by|source_bulk_authorization[(]opened_by",
            },
        )
        .all()
    )
    if remaining:
        raise RuntimeError("custom_import_snapshot_names_unconverted_function")


def _prepare_metadata_compatibility(schema):
    """Leave old reader columns available until the matching application rollout."""
    quoted = _storage()._quote(schema)
    for table, names in (
        (
            "custom_import_snapshot_family",
            ("custom_import_snapshot_landing_shape", "custom_import_snapshot_origin_shape"),
        ),
        (
            "custom_import_snapshot_relation",
            (
                "custom_import_snapshot_relation_oid_key",
                "custom_import_snapshot_relation_shape",
                "custom_import_snapshot_layout_shape",
            ),
        ),
        ("custom_import_query_projection", ("custom_import_query_projection_custody",)),
    ):
        for name in names:
            op.execute(f"ALTER TABLE {quoted}.{table} DROP CONSTRAINT IF EXISTS {name}")
    op.create_check_constraint(
        "custom_import_snapshot_relation_shape",
        "custom_import_snapshot_relation",
        "relation_slot BETWEEN 1 AND 15",
        schema=schema,
    )
    column_names_by_table = {
        "custom_import_snapshot_family": tuple(
            f"{prefix}_{suffix}"
            for prefix in ("landing", "origin")
            for suffix in ("table_oid", "table_owner", "columns_sha256")
        ),
        "custom_import_snapshot_relation": ("table_oid", "table_owner", "columns_sha256"),
        "custom_import_query_projection": tuple(
            f"{prefix}_{suffix}"
            for prefix in ("root", "child")
            for suffix in ("table_oid", "table_owner", "columns_sha256")
        ),
        "source_bulk_authorization": ("opened_by",),
    }
    for table, names in column_names_by_table.items():
        present_names = set(
            op.get_bind()
            .execute(
                text(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema=:schema AND table_name=:table"
                ),
                {"schema": schema, "table": table},
            )
            .scalars()
        )
        for name in names:
            if name in present_names:
                op.alter_column(table, name, nullable=True, schema=schema)
    # Previous workers read this field until the second migration removes it.
    op.alter_column("source_bulk_authorization", "opened_by", server_default=text("session_user"), schema=schema)


def upgrade():
    """Keep existing families usable and defer new families until readers are upgraded."""
    schema = _schema()
    quoted = _storage()._quote(schema)
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN ACCESS EXCLUSIVE MODE")
    _refresh_storage(schema)
    _refresh_writers(schema)
    for name in ("prepare_custom_import_snapshot_indexes", "verify_custom_import_snapshot_indexes"):
        _refresh(
            f"{quoted}.{name}(bigint,text)",
            lambda source: _index_body(source.replace(quoted, "__CONTROL__")).replace("__CONTROL__", quoted),
        )
    _refresh(f"{quoted}.verify_custom_import_snapshot_structure(bigint)", _optional_relations)
    build_verifier = f"{quoted}.verify_custom_import_build_structure(bigint)"
    verifier_body = op.get_bind().scalar(
        text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"), {"identity": build_verifier}
    )
    if verifier_body is None:
        raise RuntimeError("custom_import_snapshot_names_function_missing")
    if "origin_table_oid" in verifier_body:
        _refresh(build_verifier, _optional_relations)
    _check_remaining_references(schema)
    _prepare_metadata_compatibility(schema)


def downgrade():
    """A restored snapshot cannot reconstruct discarded physical metadata."""
    raise RuntimeError("custom_import_snapshot_names_requires_forward_migration")
