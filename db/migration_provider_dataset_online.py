# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Commit dataset validation separately from bounded catalog-only cutovers."""

import json
from contextlib import contextmanager

import sqlalchemy as sa

from db.migration_provider_directory_dataset_candidates import (
    TABLES,
    _assert_known_row_guards,
    _assert_supported_dependencies,
    _convert_relation,
    _metadata,
    _normalize_relation_grants,
    authorized_candidate_writers,
    candidate_functions,
    grant_candidate_api,
    literal,
    quote,
)

PLAN_TABLE = "pd_dataset_migration_plan"
FENCE = "pd_dataset_migration_fence"
HEADERS = ("provider_directory_endpoint_dataset", *(table.removesuffix("_resource") for table in TABLES[4:]))


@contextmanager
def _phase(op, *, validation=False):
    """Bound lock waits and release every phase's locks before the next scan."""
    op.execute("BEGIN")
    try:
        op.execute("SET LOCAL standard_conforming_strings='on'")
        op.execute("SET LOCAL lock_timeout='1s'")
        if not validation:
            op.execute("SET LOCAL statement_timeout='10s'")
        yield
        op.execute("COMMIT")
    except BaseException:
        op.execute("ROLLBACK")
        raise


def _relation(schema, table):
    return f"{quote(schema)}.{quote(table)}"


def _dataset_keys(connection, schema, table):
    header = HEADERS[0] if table in TABLES[:4] else table.removesuffix("_resource")
    return list(connection.scalars(sa.text(f"SELECT dataset_id FROM {_relation(schema, header)} ORDER BY dataset_id")))


def _history_bound(keys):
    return "dataset_id IN (" + ",".join(literal(key) for key in keys) + ")" if keys else "false"


def _table_keys(plan, table):
    return plan["tables"][table].get("keys", plan["keys"])


def _save_plan(op, schema, plan):
    op.get_bind().execute(
        sa.text(f"UPDATE {_relation(schema, PLAN_TABLE)} SET plan=CAST(:plan AS jsonb) WHERE singleton"),
        {"plan": json.dumps(plan)},
    )


def _fence(op, schema, relation):
    op.execute(
        f"CREATE TRIGGER {FENCE} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {quote(schema)}.{FENCE}()"
    )


def _capture_references(connection, targets):
    return [
        dict(row)
        for row in connection.execute(
            sa.text("""
        SELECT oid, conrelid AS source_oid, conname, pg_get_constraintdef(oid) AS definition
          FROM pg_constraint WHERE contype='f' AND conparentid=0
           AND confrelid=ANY(CAST(:targets AS regclass[]))
           AND NOT conrelid=ANY(CAST(:targets AS regclass[])) ORDER BY oid
    """),
            {"targets": targets},
        ).mappings()
    ]


def _expected_bulk_relationships():
    """List the existing provider bulk contract; unrelated dependencies fail closed."""
    relationships = {(TABLES[0], "provider_directory_endpoint_dataset", ("dataset_id",), ("dataset_id",), "a")}
    relationships.update(
        (table, "provider_directory_endpoint_dataset", ("dataset_id",), ("dataset_id",), "c") for table in TABLES[1:4]
    )
    for table in TABLES[4:]:
        relationships.add((table, table.removesuffix("_resource"), ("dataset_id",), ("dataset_id",), "a"))
        resource_key = ("dataset_id", "resource_type", "resource_id")
        relationships.add((table, TABLES[0], resource_key, resource_key, "a"))
    relationships.add(
        (
            TABLES[4],
            "provider_directory_uhc_flex_practitioner_twin_admission",
            ("candidate_acquisition_id",),
            ("candidate_acquisition_id",),
            "a",
        )
    )
    relationships.add(
        (
            TABLES[5],
            "provider_directory_rooted_graph_acquisition",
            ("publication_acquisition_id",),
            ("acquisition_id",),
            "a",
        )
    )
    return relationships


def _capture_bulk_relationships(connection, relation_names, schema):
    """Freeze the bulk key pairs for indexed snapshot and metadata-delete checks."""
    relationships = [
        dict(relationship)
        for relationship in connection.execute(
            sa.text("""
        SELECT source.relname AS source_table,target.relname AS target_table,
               target_namespace.nspname AS target_schema,constraint_record.confdeltype::text AS delete_action,
               ARRAY(SELECT attribute.attname::text FROM unnest(conkey) WITH ORDINALITY key(attnum,position)
                     JOIN pg_attribute attribute ON attribute.attrelid=conrelid AND attribute.attnum=key.attnum
                     ORDER BY key.position) AS source_columns,
               ARRAY(SELECT attribute.attname::text FROM unnest(confkey) WITH ORDINALITY key(attnum,position)
                     JOIN pg_attribute attribute ON attribute.attrelid=confrelid AND attribute.attnum=key.attnum
                     ORDER BY key.position) AS target_columns
          FROM pg_constraint constraint_record JOIN pg_class source ON source.oid=conrelid
          JOIN pg_class target ON target.oid=confrelid JOIN pg_namespace target_namespace ON target_namespace.oid=target.relnamespace
         WHERE contype='f' AND conparentid=0 AND conrelid=ANY(CAST(:relation_names AS regclass[]))
           AND confmatchtype='s' AND confupdtype='a' AND NOT condeferrable
         ORDER BY source.relname,conname
    """),
            {"relation_names": relation_names},
        ).mappings()
    ]
    actual_relationships = {
        (
            relationship["source_table"],
            relationship["target_table"],
            tuple(relationship["source_columns"]),
            tuple(relationship["target_columns"]),
            relationship["delete_action"],
        )
        for relationship in relationships
    }
    count = connection.scalar(
        sa.text(
            "SELECT count(*) FROM pg_constraint WHERE contype='f' AND conparentid=0 AND conrelid=ANY(CAST(:relation_names AS regclass[]))"
        ),
        {"relation_names": relation_names},
    )
    if (
        actual_relationships != _expected_bulk_relationships()
        or count != len(actual_relationships)
        or any(relationship["target_schema"] != schema for relationship in relationships)
    ):
        raise RuntimeError("provider_dataset_bulk_relationship_drift")
    return relationships


def _capture_plan(op, schema, relation_names):
    connection = op.get_bind()
    _assert_supported_dependencies(connection, relation_names)
    for relation_name in relation_names:
        constraints, indexes, triggers, _grants = _metadata(connection, relation_name)
        _assert_known_row_guards(triggers)
        if any(not constraint["convalidated"] for constraint in constraints):
            raise RuntimeError("provider_dataset_unvalidated_constraint")
        if connection.scalar(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM pg_index WHERE indrelid=CAST(:relation_name AS regclass) AND (NOT indisvalid OR NOT indisready))"
            ),
            {"relation_name": relation_name},
        ):
            raise RuntimeError("provider_dataset_invalid_index")
    return {
        "tables": {
            table: {
                "original_oid": connection.scalar(
                    sa.text("SELECT CAST(:relation AS regclass)::oid"), {"relation": _relation(schema, table)}
                ),
                "parent_oid": None,
                "keys": _dataset_keys(connection, schema, table),
            }
            for table in TABLES
        },
        "keys": list(
            connection.scalars(
                sa.text(
                    f"SELECT dataset_id FROM {quote(schema)}.provider_directory_endpoint_dataset ORDER BY dataset_id"
                )
            )
        ),
        "references": _capture_references(connection, relation_names),
        "relationships": _capture_bulk_relationships(connection, relation_names, schema),
        "writers": authorized_candidate_writers(op, schema),
        "complete": False,
    }


def _prepare(op, schema):
    connection = op.get_bind()
    plan_relation = _relation(schema, PLAN_TABLE)
    if connection.scalar(sa.text("SELECT to_regclass(:relation)"), {"relation": plan_relation}):
        plan = connection.scalar(sa.text(f"SELECT plan FROM {plan_relation} WHERE singleton"))
        if not plan["complete"] and any("keys" not in identity for identity in plan["tables"].values()):
            _resume_legacy_bounds(op, schema, plan)
        return plan
    targets = [_relation(schema, table) for table in TABLES]
    headers = [_relation(schema, header) for header in HEADERS]
    op.execute("LOCK TABLE " + ",".join([*headers, *targets]) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
    plan = _capture_plan(op, schema, targets)
    op.execute(f"CREATE TABLE {plan_relation}(singleton boolean PRIMARY KEY CHECK(singleton), plan jsonb NOT NULL)")
    _normalize_relation_grants(op, plan_relation)
    connection.execute(
        sa.text(f"INSERT INTO {plan_relation} VALUES(true,CAST(:plan AS jsonb))"), {"plan": json.dumps(plan)}
    )
    op.execute("DO $batch$ BEGIN EXECUTE $script$" + candidate_functions(schema) + "$script$; END; $batch$;")
    grant_candidate_api(op, schema, {})
    op.execute(
        f"CREATE TRIGGER pd_dataset_plan_owner BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {plan_relation} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {quote(schema)}.guard_pd_dataset_migration_owner()"
    )
    op.execute(f"ALTER TABLE {plan_relation} ENABLE ALWAYS TRIGGER pd_dataset_plan_owner")
    _install_fence(op, schema, [*headers, *targets])
    for table, target in zip(TABLES, targets, strict=True):
        bound = _history_bound(_table_keys(plan, table))
        op.execute(f"ALTER TABLE {target} ADD CONSTRAINT pd_dataset_history_bound CHECK ({bound}) NOT VALID")
    return plan


def _resume_legacy_bounds(op, schema, plan):
    """Keep committed bounds; freeze family keys only for unfinished conversions."""
    if any("keys" in identity for identity in plan["tables"].values()):
        raise RuntimeError("provider_dataset_migration_partial_key_plan")
    pending_tables = [table for table in TABLES if not plan["tables"][table]["parent_oid"]]
    headers = [_relation(schema, header) for header in HEADERS[1:]]
    targets = [_relation(schema, table) for table in pending_tables]
    op.execute("LOCK TABLE " + ",".join([*headers, *targets]) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
    for table in TABLES:
        _verify_relation(op, schema, table, plan)
        identity = plan["tables"][table]
        if table in pending_tables:
            identity["keys"] = _dataset_keys(op.get_bind(), schema, table)
            target = _relation(schema, table)
            op.execute(f"ALTER TABLE {target} DROP CONSTRAINT pd_dataset_history_bound")
            op.execute(
                f"ALTER TABLE {target} ADD CONSTRAINT pd_dataset_history_bound CHECK ({_history_bound(identity['keys'])}) NOT VALID"
            )
        else:
            identity["keys"] = plan["keys"]
    for header in headers:
        _fence(op, schema, header)
    _save_plan(op, schema, plan)


def _install_fence(op, schema, relations):
    op.execute(f"""CREATE FUNCTION {quote(schema)}.{FENCE}() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
        BEGIN RAISE EXCEPTION 'provider_dataset_migration_incomplete_rerun_migration' USING ERRCODE='55000'; END $f$""")
    grants = (
        op.get_bind()
        .execute(
            sa.text("""
        SELECT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END
          FROM pg_proc CROSS JOIN LATERAL aclexplode(COALESCE(proacl,acldefault('f',proowner))) acl
         WHERE oid=CAST(:signature AS regprocedure) AND acl.grantee<>proowner
    """),
            {"signature": f"{quote(schema)}.{FENCE}()"},
        )
        .scalars()
    )
    for grantee in grants:
        role = "PUBLIC" if grantee == "PUBLIC" else quote(grantee)
        op.execute(f"REVOKE ALL ON FUNCTION {quote(schema)}.{FENCE}() FROM {role}")
    for relation in relations:
        _fence(op, schema, relation)


def _verify_relation(op, schema, table, plan):
    canonical_relation = _relation(schema, table)
    identity = plan["tables"][table]
    canonical_oid = op.get_bind().scalar(sa.text("SELECT to_regclass(:target)::oid"), {"target": canonical_relation})
    expected_oid = identity["parent_oid"] or identity["original_oid"]
    if canonical_oid != expected_oid:
        raise RuntimeError("provider_dataset_migration_relation_drift")
    if identity["parent_oid"]:
        history = _relation(schema, "pd_dataset_history_" + str(identity["original_oid"]))
        history_oid = op.get_bind().scalar(sa.text("SELECT to_regclass(:history)::oid"), {"history": history})
        parent_oid = op.get_bind().scalar(
            sa.text("SELECT inhparent FROM pg_inherits WHERE inhrelid=:history"),
            {"history": identity["original_oid"]},
        )
        keys = _table_keys(plan, table)
        if history_oid != identity["original_oid"] or parent_oid != (canonical_oid if keys else None):
            raise RuntimeError("provider_dataset_migration_relation_drift")
        if keys and not op.get_bind().scalar(
            sa.text("""
            SELECT pg_get_expr(relpartbound,oid) = (
                -- The deparser uses ordinary literals, never quote_literal's E prefix.
                SELECT 'FOR VALUES IN (' || string_agg(
                    chr(39) || replace(
                        CASE current_setting('standard_conforming_strings') WHEN 'on' THEN key
                            ELSE replace(key, chr(92), chr(92) || chr(92)) END,
                        chr(39), chr(39) || chr(39)) || chr(39), ', ' ORDER BY position) || ')'
                  FROM unnest(CAST(:keys AS text[])) WITH ORDINALITY AS bounds(key,position)
            ) FROM pg_class WHERE oid=:history
        """),
            {"history": history_oid, "keys": keys},
        ):
            raise RuntimeError("provider_dataset_migration_bound_drift")


def _convert_one(op, schema, table, plan):
    _verify_relation(op, schema, table, plan)
    target = _relation(schema, table)
    if plan["tables"][table]["parent_oid"]:
        return
    with _phase(op, validation=True):
        op.execute(f"ALTER TABLE {target} VALIDATE CONSTRAINT pd_dataset_history_bound")
    with _phase(op):
        op.execute(f"LOCK TABLE {target} IN ACCESS EXCLUSIVE MODE NOWAIT")
        _verify_relation(op, schema, table, plan)
        op.execute(f"DROP TRIGGER {FENCE} ON {target}")
        history = _convert_relation(op, schema, table, _table_keys(plan, table))
        _fence(op, schema, target)
        _fence(op, schema, history)
        plan["tables"][table]["parent_oid"] = op.get_bind().scalar(
            sa.text("SELECT CAST(:target AS regclass)::oid"), {"target": target}
        )
        _save_plan(op, schema, plan)


def _root_reference(connection, oid):
    return (
        connection.execute(
            sa.text("""
        WITH RECURSIVE ancestors AS (
            SELECT oid,conparentid,conrelid,conname FROM pg_constraint WHERE oid=:oid
            UNION ALL
            SELECT parent.oid,parent.conparentid,parent.conrelid,parent.conname
              FROM pg_constraint parent JOIN ancestors child ON parent.oid=child.conparentid
        ) SELECT oid,conrelid::regclass::text AS relation,conname FROM ancestors WHERE conparentid=0
    """),
            {"oid": oid},
        )
        .mappings()
        .one()
    )


def _rebind_reference(op, schema, plan, reference):
    if reference.get("complete"):
        return
    connection = op.get_bind()
    root = _root_reference(connection, reference["oid"])
    replacement = "pd_dataset_fk_" + str(reference["oid"])
    exists = connection.scalar(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND conname=:name)"
        ),
        {"relation": root["relation"], "name": replacement},
    )
    if not exists:
        with _phase(op):
            op.execute(
                f"ALTER TABLE {root['relation']} ADD CONSTRAINT {quote(replacement)} {reference['definition']} NOT VALID"
            )
    with _phase(op, validation=True):
        op.execute(f"ALTER TABLE {root['relation']} VALIDATE CONSTRAINT {quote(replacement)}")
    with _phase(op):
        op.execute(f"LOCK TABLE {root['relation']} IN ACCESS EXCLUSIVE MODE NOWAIT")
        op.execute(f"ALTER TABLE {root['relation']} DROP CONSTRAINT {quote(root['conname'])}")
        op.execute(
            f"ALTER TABLE {root['relation']} RENAME CONSTRAINT {quote(replacement)} TO {quote(reference['conname'])}"
        )
        reference["complete"] = True
        _save_plan(op, schema, plan)


def _finish(op, schema, plan):
    for table in TABLES:
        _verify_relation(op, schema, table, plan)
    header = _relation(schema, "provider_directory_endpoint_dataset")
    op.execute(
        f"CREATE TRIGGER pd_dataset_storage AFTER INSERT ON {header} FOR EACH ROW EXECUTE FUNCTION {quote(schema)}.prepare_pd_generic_dataset_storage()"
    )
    op.execute(
        f"CREATE TRIGGER pd_dataset_generic_finalization AFTER UPDATE OF status ON {header} "
        f"FOR EACH ROW EXECUTE FUNCTION {quote(schema)}.guard_pd_generic_dataset_finalization()"
    )
    for target_table in sorted({relationship["target_table"] for relationship in plan["relationships"]} - set(TABLES)):
        op.execute(
            f"CREATE TRIGGER pd_dataset_bulk_parent_delete AFTER DELETE ON {_relation(schema, target_table)} "
            f"REFERENCING OLD TABLE AS deleted_rows FOR EACH STATEMENT EXECUTE FUNCTION {quote(schema)}.guard_pd_dataset_bulk_parent_delete()"
        )
    key_columns_by_target = {
        relationship["target_table"]: relationship["target_columns"][0]
        for relationship in plan["relationships"]
        if relationship["target_table"] not in TABLES
    }
    for target_table, key_column in key_columns_by_target.items():
        op.execute(
            f"CREATE TRIGGER pd_dataset_parent_key_update BEFORE UPDATE OF {quote(key_column)} ON {_relation(schema, target_table)} "
            f"FOR EACH ROW EXECUTE FUNCTION {quote(schema)}.guard_pd_dataset_parent_key_update({literal(key_column)})"
        )
    grant_candidate_api(op, schema, plan["writers"])
    fences = list(
        op.get_bind().scalars(
            sa.text("""
        SELECT tgrelid::regclass::text FROM pg_trigger
         WHERE tgname=:fence AND tgfoid=CAST(:function AS regprocedure)
    """),
            {"fence": FENCE, "function": f"{quote(schema)}.{FENCE}()"},
        )
    )
    for relation in fences:
        op.execute(f"LOCK TABLE ONLY {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
        op.execute(f"DROP TRIGGER {FENCE} ON {relation}")
    op.execute(f"DROP FUNCTION {quote(schema)}.{FENCE}()")
    plan["complete"] = True
    _save_plan(op, schema, plan)


def migrate_dataset_candidates(op, schema):
    """Resume committed preparation, keeping readers online and writers fenced."""
    if op.get_context().as_sql:
        raise RuntimeError("provider_dataset_migration_requires_online_connection")
    connection = op.get_bind()
    if int(connection.scalar(sa.text("SHOW server_version_num"))) < 180000:
        raise RuntimeError("provider_dataset_migration_requires_postgresql_18")
    lock_key = "provider_dataset_candidates:" + schema
    with op.get_context().autocommit_block():
        locked = connection.scalar(sa.text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": lock_key})
        if not locked:
            raise RuntimeError("provider_dataset_migration_already_running")
        try:
            with _phase(op):
                plan = _prepare(op, schema)
            if plan["complete"]:
                for table in TABLES:
                    _verify_relation(op, schema, table, plan)
                return
            for table in TABLES:
                _convert_one(op, schema, table, plan)
            for reference in plan["references"]:
                _rebind_reference(op, schema, plan, reference)
            with _phase(op):
                _finish(op, schema, plan)
        finally:
            connection.execute(sa.text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": lock_key})
