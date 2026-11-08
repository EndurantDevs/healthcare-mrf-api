# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate immutable rooted history without holding its readers behind a scan."""

from contextlib import contextmanager

import sqlalchemy as sa

_TABLES = ("work", "resource", "edge")
_FENCE = "pdrg_storage_migration_fence"
_PLAN = "pdrg_storage_migration"


def _quote(value):
    return '"' + value.replace('"', '""') + '"'


def _relation(schema, suffix):
    return f"{_quote(schema)}.{_quote('provider_directory_rooted_graph_' + suffix)}"


@contextmanager
def _phase(op, *, validation=False):
    """Commit long validations independently of short metadata changes."""
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


def _revoke_default_grants(op, relation):
    grantees = op.get_bind().scalars(
        sa.text("""
        SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END
          FROM pg_class, LATERAL aclexplode(COALESCE(relacl,acldefault('r',relowner))) acl
         WHERE oid=CAST(:relation AS regclass) AND acl.grantee<>relowner
    """),
        {"relation": relation},
    )
    for grantee in grantees:
        op.execute(f"REVOKE ALL ON {relation} FROM " + ("PUBLIC" if grantee == "PUBLIC" else _quote(grantee)))


def _prepare(op, schema, conversion_sql):
    connection = op.get_bind()
    plan = f"{_quote(schema)}.{_PLAN}"
    if connection.scalar(sa.text("SELECT to_regclass(:relation)"), {"relation": plan}):
        return connection.scalar(sa.text(f"SELECT complete FROM {plan} WHERE singleton"))
    relations = [_relation(schema, suffix) for suffix in ("acquisition", *_TABLES)]
    op.execute("LOCK TABLE " + ",".join(relations) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
    # The existing catalog fence checks supported constraints and dependencies.
    fence_sql = conversion_sql.split("CREATE TEMP TABLE pdrg_work_writer_acl", 1)[0]
    op.execute("DO $batch$ BEGIN EXECUTE $script$" + fence_sql + "$script$; END; $batch$;")
    op.execute(f"CREATE TABLE {plan}(singleton boolean PRIMARY KEY CHECK(singleton),complete boolean NOT NULL)")
    _revoke_default_grants(op, plan)
    op.execute(f"""CREATE FUNCTION {_quote(schema)}.guard_provider_directory_rooted_graph_copy_owner() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
        BEGIN
            IF NOT EXISTS(SELECT FROM pg_class relation JOIN pg_roles caller ON caller.rolname=current_user
                          WHERE relation.oid=TG_RELID AND (relation.relowner=caller.oid OR caller.rolsuper)) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_copy_unauthorized' USING ERRCODE='42501';
            END IF;
            RETURN NULL;
        END $f$""")
    op.execute(
        f"REVOKE ALL ON FUNCTION {_quote(schema)}.guard_provider_directory_rooted_graph_copy_owner() FROM PUBLIC"
    )
    op.execute(
        f"CREATE TRIGGER pdrg_plan_owner BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {plan} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {_quote(schema)}.guard_provider_directory_rooted_graph_copy_owner()"
    )
    op.execute(f"ALTER TABLE {plan} ENABLE ALWAYS TRIGGER pdrg_plan_owner")
    op.execute(f"INSERT INTO {plan} VALUES(true,false)")
    op.execute(f"""CREATE FUNCTION {_quote(schema)}.{_FENCE}() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
        BEGIN RAISE EXCEPTION 'rooted_graph_migration_incomplete_rerun_migration' USING ERRCODE='55000'; END $f$""")
    op.execute(f"REVOKE ALL ON FUNCTION {_quote(schema)}.{_FENCE}() FROM PUBLIC")
    for relation in relations:
        op.execute(
            f"CREATE TRIGGER {_FENCE} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {_quote(schema)}.{_FENCE}()"
        )
        op.execute(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {_FENCE}")
    _drop_bulk_foreign_keys(op, schema)
    acquisition_ids = list(
        connection.scalars(sa.text(f"SELECT acquisition_id FROM {relations[0]} ORDER BY acquisition_id"))
    )
    values_sql = ",".join("'" + acquisition_id.replace("'", "''") + "'" for acquisition_id in acquisition_ids)
    bound = f"acquisition_id IN ({values_sql})" if values_sql else "false"
    for suffix in _TABLES:
        op.execute(
            f"ALTER TABLE {_relation(schema, suffix)} ADD CONSTRAINT pdrg_{suffix}_legacy_scope CHECK({bound}) NOT VALID"
        )
    return False


def _shadow_relation(schema, suffix):
    return f"{_quote(schema)}.pdrg_{suffix}_parent"


def _prepare_parent(op, schema, suffix):
    """Attach the unchanged history heap to an empty, private indexed parent."""
    connection = op.get_bind()
    shadow = _shadow_relation(schema, suffix)
    original = _relation(schema, suffix)
    if connection.scalar(sa.text("SELECT to_regclass(:relation)"), {"relation": shadow}):
        attached = connection.scalar(
            sa.text("""
            SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=CAST(:shadow AS regclass)
                          AND inhrelid=CAST(:original AS regclass))
        """),
            {"shadow": shadow, "original": original},
        )
        has_history = connection.scalar(sa.text(f"SELECT EXISTS(SELECT FROM {_relation(schema, 'acquisition')})"))
        if attached != has_history:
            raise RuntimeError("rooted_graph_migration_shadow_invalid")
        return
    with _phase(op):
        op.execute(f"LOCK TABLE {original} IN ACCESS EXCLUSIVE MODE NOWAIT")
        op.execute(f"CREATE TABLE {shadow}(LIKE {original} INCLUDING DEFAULTS) PARTITION BY LIST(acquisition_id)")
        _revoke_default_grants(op, shadow)
        constraints = connection.execute(
            sa.text("""
            SELECT conname,contype::text,pg_get_constraintdef(oid) AS definition
              FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND conparentid=0 AND contype IN ('p','u')
             ORDER BY contype
        """),
            {"relation": original},
        ).mappings()
        for constraint in constraints:
            name = f"pdrg_{suffix}_parent_" + ("pkey" if constraint["contype"] == "p" else "scope_key")
            op.execute(f"ALTER TABLE {shadow} ADD CONSTRAINT {_quote(name)} {constraint['definition']}")
        if suffix == "work":
            definition = connection.scalar(
                sa.text("SELECT pg_get_indexdef(CAST(:name AS regclass))"),
                {"name": f"{_quote(schema)}.provider_directory_rooted_graph_plan_census_key"},
            )
            op.execute(
                f"CREATE UNIQUE INDEX pdrg_work_parent_plan_census_key ON {shadow} USING "
                + definition.split(" USING ", 1)[1]
            )
        acquisition_ids = list(
            connection.scalars(
                sa.text(f"SELECT acquisition_id FROM {_relation(schema, 'acquisition')} ORDER BY acquisition_id")
            )
        )
        if acquisition_ids:
            bound = (
                "FOR VALUES IN ("
                + ",".join("'" + acquisition_id.replace("'", "''") + "'" for acquisition_id in acquisition_ids)
                + ")"
            )
            op.execute(f"ALTER TABLE {shadow} ATTACH PARTITION {original} {bound}")


def _drop_bulk_foreign_keys(op, schema):
    """Replace bulk relationships with snapshot checks before converting storage."""
    for suffix in _TABLES:
        relation = _relation(schema, suffix)
        names = list(
            op.get_bind().scalars(
                sa.text(
                    "SELECT conname FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND contype='f' AND conparentid=0"
                ),
                {"relation": relation},
            )
        )
        for name in names:
            op.execute(f"ALTER TABLE {relation} DROP CONSTRAINT {_quote(name)}")


def _finish(op, schema, conversion_sql):
    op.execute("DO $batch$ BEGIN EXECUTE $script$" + conversion_sql + "$script$; END; $batch$;")
    for suffix in ("acquisition", *(name + "_legacy" for name in _TABLES)):
        op.execute(f"DROP TRIGGER {_FENCE} ON {_relation(schema, suffix)}")
    op.execute(f"DROP FUNCTION {_quote(schema)}.{_FENCE}()")
    op.execute(f"UPDATE {_quote(schema)}.{_PLAN} SET complete=true WHERE singleton")


def migrate_rooted_graph_candidates(op, schema, conversion_sql):
    """Resume bounded conversion after committed, reader-compatible validation."""
    if op.get_context().as_sql:
        raise RuntimeError("rooted_graph_migration_requires_online_connection")
    connection = op.get_bind()
    lock_key = "rooted_graph_candidates:" + schema
    with op.get_context().autocommit_block():
        locked = connection.scalar(sa.text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"), {"key": lock_key})
        if not locked:
            raise RuntimeError("rooted_graph_migration_already_running")
        try:
            with _phase(op):
                complete = _prepare(op, schema, conversion_sql)
            if complete:
                return
            for suffix in _TABLES:
                with _phase(op, validation=True):
                    op.execute(
                        f"ALTER TABLE {_relation(schema, suffix)} VALIDATE CONSTRAINT pdrg_{suffix}_legacy_scope"
                    )
            _prepare_parent(op, schema, "work")
            _prepare_parent(op, schema, "resource")
            _prepare_parent(op, schema, "edge")
            with _phase(op):
                _finish(op, schema, conversion_sql)
        finally:
            connection.execute(sa.text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"), {"key": lock_key})
