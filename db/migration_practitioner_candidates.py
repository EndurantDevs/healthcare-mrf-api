# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prevalidate practitioner history before a bounded metadata-only cutover."""

from contextlib import contextmanager

import sqlalchemy as sa

_FENCE = "pd_practitioner_migration_fence"
_BOUND = "pd_uhc_flex_legacy_scope"
_PLAN = "pd_practitioner_storage_migration"


def _quote(value):
    return '"' + value.replace('"', '""') + '"'


def _relation(schema, suffix):
    return f"{_quote(schema)}.{_quote('provider_directory_uhc_flex_practitioner_' + suffix)}"


@contextmanager
def _phase(op, *, validation=False):
    """Commit validation separately and bound catalog lock waits."""
    op.execute("BEGIN")
    try:
        op.execute("SET LOCAL lock_timeout='1s'")
        if not validation:
            op.execute("SET LOCAL statement_timeout='10s'")
        yield
        op.execute("COMMIT")
    except BaseException:
        op.execute("ROLLBACK")
        raise


def _execute_script(op, sql):
    op.execute("DO $batch$ BEGIN EXECUTE $script$" + sql + "$script$; END; $batch$;")


def _relation_oid(op, schema, suffix):
    return op.get_bind().scalar(sa.text("SELECT to_regclass(:relation)::oid"), {"relation": _relation(schema, suffix)})


def _state(op, schema):
    connection = op.get_bind()
    plan = f"{_quote(schema)}.{_PLAN}"
    if not connection.scalar(sa.text("SELECT to_regclass(:relation)"), {"relation": plan}):
        kind = connection.scalar(
            sa.text("SELECT relkind::text FROM pg_class WHERE oid=CAST(:relation AS regclass)"),
            {"relation": _relation(schema, "resource")},
        )
        if kind != "r":
            raise RuntimeError("practitioner_migration_catalog_drift")
        return "initial"
    receipt = connection.execute(sa.text(f"SELECT * FROM {plan} WHERE singleton")).mappings().one()
    if _relation_oid(op, schema, "acquisition") != receipt["acquisition_oid"]:
        raise RuntimeError("practitioner_migration_relation_drift")
    if _relation_oid(op, schema, "resource") != (receipt["parent_oid"] or receipt["resource_oid"]):
        raise RuntimeError("practitioner_migration_relation_drift")
    if receipt["parent_oid"]:
        for suffix, parent_column in (("work", "work_parent_oid"), ("resource", "parent_oid")):
            if (
                _relation_oid(op, schema, suffix) != receipt[parent_column]
                or _relation_oid(op, schema, suffix + "_legacy") != receipt[suffix + "_oid"]
            ):
                raise RuntimeError("practitioner_migration_relation_drift")
            attached = connection.scalar(
                sa.text("SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=:legacy AND inhparent=:parent)"),
                {"legacy": receipt[suffix + "_oid"], "parent": receipt[parent_column]},
            )
            if not attached and connection.scalar(
                sa.text(f"SELECT EXISTS(SELECT FROM {_relation(schema, suffix + '_legacy')})")
            ):
                raise RuntimeError("practitioner_migration_relation_drift")
        return "complete"
    if _relation_oid(op, schema, "work") != receipt["work_oid"]:
        raise RuntimeError("practitioner_migration_relation_drift")
    fences = connection.scalar(
        sa.text("""
        SELECT count(*) FROM pg_trigger WHERE tgname=:fence AND tgenabled='A'
         AND tgrelid=ANY(CAST(:relations AS regclass[])) AND tgfoid=to_regprocedure(:signature)
    """),
        {
            "fence": _FENCE,
            "signature": f"{_quote(schema)}.{_FENCE}()",
            "relations": [_relation(schema, suffix) for suffix in ("acquisition", "work", "resource")],
        },
    )
    if fences != 3:
        raise RuntimeError("practitioner_migration_fence_drift")
    return "prepared"


def _create_receipt(op, schema):
    plan = f"{_quote(schema)}.{_PLAN}"
    op.execute(
        f"CREATE TABLE {plan}(singleton boolean PRIMARY KEY CHECK(singleton),acquisition_oid oid NOT NULL,work_oid oid NOT NULL,resource_oid oid NOT NULL,parent_oid oid,work_parent_oid oid)"
    )
    grantees = op.get_bind().scalars(
        sa.text("""
        SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END
          FROM pg_class CROSS JOIN LATERAL aclexplode(relacl) acl
         WHERE oid=CAST(:relation AS regclass) AND acl.grantee<>relowner
    """),
        {"relation": plan},
    )
    for grantee in grantees:
        op.execute(f"REVOKE ALL ON {plan} FROM " + ("PUBLIC" if grantee == "PUBLIC" else _quote(grantee)))
    op.get_bind().execute(
        sa.text(f"INSERT INTO {plan} VALUES(true,:acquisition,:work,:resource,NULL,NULL)"),
        {suffix: _relation_oid(op, schema, suffix) for suffix in ("acquisition", "work", "resource")},
    )


def _prepare(op, schema, conversion_sql):
    state = _state(op, schema)
    if state != "initial":
        return state
    _execute_script(op, conversion_sql.split("CREATE TEMP TABLE pd_practitioner_writer_acl", 1)[0])
    _create_receipt(op, schema)
    _install_fences(op, schema)
    acquisition_ids = list(
        op.get_bind().scalars(
            sa.text(f"SELECT acquisition_id FROM {_relation(schema, 'acquisition')} ORDER BY acquisition_id")
        )
    )
    values_sql = ",".join("'" + value.replace("'", "''") + "'" for value in acquisition_ids)
    bound = f"acquisition_id IN ({values_sql})" if values_sql else "false"
    for suffix in ("work", "resource"):
        op.execute(f"ALTER TABLE {_relation(schema, suffix)} ADD CONSTRAINT {_BOUND} CHECK({bound}) NOT VALID")
    return "prepared"


def _install_fences(op, schema):
    op.execute(f"""CREATE FUNCTION {_quote(schema)}.{_FENCE}() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
        BEGIN
            IF TG_ARGV[0]='metadata' THEN
                IF CURRENT_USER IS DISTINCT FROM (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=TG_RELID) THEN
                    RAISE EXCEPTION 'practitioner_migration_metadata_immutable' USING ERRCODE='42501';
                END IF;
                RETURN NULL;
            END IF;
            RAISE EXCEPTION 'practitioner_migration_incomplete_rerun_migration' USING ERRCODE='55000';
        END $f$""")
    grantees = op.get_bind().scalars(
        sa.text("""
        SELECT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END
          FROM pg_proc CROSS JOIN LATERAL aclexplode(COALESCE(proacl,acldefault('f',proowner))) acl
         WHERE oid=CAST(:signature AS regprocedure) AND acl.grantee<>proowner
    """),
        {"signature": f"{_quote(schema)}.{_FENCE}()"},
    )
    for grantee in grantees:
        role = "PUBLIC" if grantee == "PUBLIC" else _quote(grantee)
        op.execute(f"REVOKE ALL ON FUNCTION {_quote(schema)}.{_FENCE}() FROM {role}")
    op.execute(
        f"CREATE TRIGGER pd_practitioner_plan_write BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {_quote(schema)}.{_PLAN} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {_quote(schema)}.{_FENCE}('metadata')"
    )
    op.execute(f"ALTER TABLE {_quote(schema)}.{_PLAN} ENABLE ALWAYS TRIGGER pd_practitioner_plan_write")
    for suffix in ("acquisition", "work", "resource"):
        relation = _relation(schema, suffix)
        op.execute(
            f"CREATE TRIGGER {_FENCE} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {_quote(schema)}.{_FENCE}()"
        )
        op.execute(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_FENCE}")


def _finish(op, schema, conversion_sql):
    if _state(op, schema) != "prepared":
        raise RuntimeError("practitioner_migration_catalog_drift")
    _execute_script(op, conversion_sql)
    for suffix in ("acquisition", "work_legacy", "resource_legacy"):
        op.execute(f"DROP TRIGGER {_FENCE} ON {_relation(schema, suffix)}")
    op.get_bind().execute(
        sa.text(f"UPDATE {_quote(schema)}.{_PLAN} SET parent_oid=:oid,work_parent_oid=:work_oid WHERE singleton"),
        {"oid": _relation_oid(op, schema, "resource"), "work_oid": _relation_oid(op, schema, "work")},
    )


def migrate_practitioner_candidates(op, schema, conversion_sql):
    """Resume online history validation while preserving existing reader storage."""
    if op.get_context().as_sql:
        raise RuntimeError("practitioner_migration_requires_online_connection")
    connection = op.get_bind()
    lock_key = "practitioner_candidates:" + schema
    with op.get_context().autocommit_block():
        locked = connection.scalar(sa.text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": lock_key})
        if not locked:
            raise RuntimeError("practitioner_migration_already_running")
        try:
            with _phase(op):
                state = _prepare(op, schema, conversion_sql)
            if state == "complete":
                return
            with _phase(op, validation=True):
                for suffix in ("work", "resource"):
                    op.execute(f"ALTER TABLE {_relation(schema, suffix)} VALIDATE CONSTRAINT {_BOUND}")
            with _phase(op):
                _finish(op, schema, conversion_sql)
        finally:
            connection.execute(sa.text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": lock_key})
