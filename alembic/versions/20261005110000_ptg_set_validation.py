# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare isolated PTG snapshot partitions before attaching their indexes."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path

import sqlalchemy as sa

from alembic import op

revision = "20261005110000_ptg_set_validation"
down_revision = "20261005100000_rooted_graph_set_validation"
branch_labels = None
depends_on = None

TABLES = (
    "ptg2_v4_snapshot_map_pack",
    "ptg2_provider_tax_identity",
    "ptg2_provider_group_tax_identity",
    "ptg2_v4_npi_scope",
    "ptg2_v4_provider_component",
    "ptg2_v4_pattern",
    "ptg2_v4_provider_set_npi_prefix",
    "ptg2_v4_heavy_owner",
    "ptg2_provider_group_tax_identity_source",
)


def _q(value):
    return '"' + value.replace('"', '""') + '"'


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _grants(target):
    return list(
        op.get_bind()
        .execute(
            sa.text("""
        SELECT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
               a.privilege_type, a.is_grantable, NULL::text AS column_name
          FROM pg_class c CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r',c.relowner))) a
         WHERE c.oid=CAST(:table AS regclass)
        UNION ALL
        SELECT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
               a.privilege_type, a.is_grantable, col.attname
          FROM pg_attribute col CROSS JOIN LATERAL aclexplode(col.attacl) a
         WHERE col.attrelid=CAST(:table AS regclass) AND col.attnum>0 AND NOT col.attisdropped
    """),
            {"table": target},
        )
        .mappings()
    )


def _restrict_table(target, *, all_privileges=False):
    """Close table and column grants, including inherited default ACL grants."""
    op.execute(f"REVOKE ALL ON {target} FROM PUBLIC")
    owner = op.get_bind().scalar(
        sa.text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=CAST(:table AS regclass)"), {"table": target}
    )
    for grant in _grants(target):
        if grant["grantee"] == owner:
            continue
        privilege = grant["privilege_type"]
        if not all_privileges and privilege in ("SELECT", "REFERENCES"):
            continue
        grantee = "PUBLIC" if grant["grantee"] == "PUBLIC" else _q(grant["grantee"])
        column = f" ({_q(grant['column_name'])})" if grant["column_name"] else ""
        op.execute(f"REVOKE {privilege}{column} ON {target} FROM {grantee}")


def _table_indexes(target):
    return list(
        op.get_bind()
        .execute(
            sa.text("""
        SELECT i.indexrelid, c.relname, pg_get_indexdef(i.indexrelid) AS definition,
               EXISTS(SELECT 1 FROM pg_constraint WHERE conindid=i.indexrelid
                      AND conrelid=i.indrelid AND contype IN ('p','u')) AS constrained
          FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
         WHERE i.indrelid=CAST(:table AS regclass)
    """),
            {"table": target},
        )
        .mappings()
    )


@contextmanager
def _phase(*, validation=False):
    """Commit a restartable phase without carrying its locks into validation."""
    op.execute("BEGIN")
    try:
        op.execute("SET LOCAL lock_timeout='1s'")
        op.execute("SET LOCAL search_path=pg_catalog")
        if not validation:
            op.execute("SET LOCAL statement_timeout='10s'")
        yield
        op.execute("COMMIT")
    except BaseException:
        op.execute("ROLLBACK")
        raise


def _plans(schema):
    return list(
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT table_name, plan, parent_oid FROM {_q(schema)}.ptg2_snapshot_partition_preparation ORDER BY ordinal"
            )
        )
        .mappings()
    )


def _guard_function(table):
    """Retain the existing immutable/delete policy for each converted relation."""
    if table == "ptg2_v4_snapshot_map_pack":
        return "guard_ptg2_v4_snapshot_map_pack"
    if table in ("ptg2_provider_tax_identity", "ptg2_provider_group_tax_identity"):
        return "guard_ptg2_provider_tax_identity"
    return "guard_ptg2_v4_snapshot_metadata"


def _expected_guards(schema, table):
    """Recognize only the documented guards replaced by isolated validation."""
    if table == "ptg2_provider_group_tax_identity_source":
        guards = (
            (table + "_insert_guard", 4, "guard_ptg2_provider_tax_identity_source_insert"),
            (table + "_mutation_guard", 27, "guard_ptg2_provider_tax_identity_source_mutation"),
            (table + "_truncate_guard", 34, "guard_ptg2_provider_tax_identity_source_truncate"),
        )
    else:
        guards = ((table + "_guard", 31, _guard_function(table)),)
    return sorted(
        (
            {
                "tgname": name,
                "tgtype": kind,
                "tgenabled": "A" if table == "ptg2_provider_group_tax_identity_source" else "O",
                "tgfoid": op.get_bind().scalar(
                    sa.text("SELECT CAST(:function AS regprocedure)::oid"),
                    {"function": _q(schema) + "." + _q(function) + "()"},
                ),
            }
            for name, kind, function in guards
        ),
        key=lambda guard: guard["tgname"],
    )


def _assert_supported_table(schema, table):
    """Fail before committed changes when a replacement would lose dependencies."""
    qualified = _q(schema) + "." + _q(table)
    unsupported = op.get_bind().scalar(
        sa.text("""
        SELECT relkind<>'r' OR relrowsecurity OR relforcerowsecurity
            OR EXISTS(SELECT 1 FROM pg_policy WHERE polrelid=relation.oid)
            OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid)
            OR EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=relation.oid)
            OR EXISTS(SELECT 1 FROM pg_depend WHERE refobjid=relation.oid
                AND refclassid='pg_class'::regclass AND classid IN ('pg_rewrite'::regclass,'pg_proc'::regclass))
            OR EXISTS(SELECT 1 FROM pg_depend WHERE refobjid=relation.reltype AND deptype='n'
                AND refclassid='pg_type'::regclass AND classid IN ('pg_proc'::regclass,'pg_class'::regclass,'pg_type'::regclass))
          FROM pg_class relation WHERE relation.oid=CAST(:table AS regclass)
    """),
        {"table": qualified},
    )
    if unsupported:
        raise RuntimeError("ptg_snapshot_preparation_dependency_changed")
    guards = list(
        op.get_bind()
        .execute(
            sa.text("""
        SELECT tgname,tgtype,tgenabled::text,tgfoid FROM pg_trigger
         WHERE tgrelid=CAST(:table AS regclass) AND NOT tgisinternal
    """),
            {"table": qualified},
        )
        .mappings()
    )
    if sorted(map(dict, guards), key=lambda guard: guard["tgname"]) != _expected_guards(schema, table):
        raise RuntimeError("ptg_snapshot_preparation_guard_changed")


def _table_constraints(qualified):
    """Capture native uniqueness and the exact relationships replaced by set checks."""
    return (
        op.get_bind()
        .execute(
            sa.text("""
        SELECT c.oid,c.conname,c.contype::text AS contype,pg_get_constraintdef(c.oid) AS definition,
          CASE WHEN c.contype='f' THEN jsonb_build_object(
            'schema',n.nspname,'table',target.relname,
            'columns',(SELECT jsonb_agg(a.attname ORDER BY key.ordinality)
              FROM unnest(c.conkey) WITH ORDINALITY key(attnum,ordinality)
              JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.attnum),
            'target_columns',(SELECT jsonb_agg(a.attname ORDER BY key.ordinality)
              FROM unnest(c.confkey) WITH ORDINALITY key(attnum,ordinality)
              JOIN pg_attribute a ON a.attrelid=c.confrelid AND a.attnum=key.attnum)
          ) END AS relationship
          FROM pg_constraint c LEFT JOIN pg_class target ON target.oid=c.confrelid
          LEFT JOIN pg_namespace n ON n.oid=target.relnamespace
          WHERE c.conrelid=CAST(:table AS regclass) AND c.contype IN ('p','u','f')
            AND c.conparentid=0
    """),
            {"table": qualified},
        )
        .mappings()
    )


def _capture_plan(schema, table, ordinal, keys):
    """Capture only catalog and bounded metadata; never scan a historical heap."""
    qualified = _q(schema) + "." + _q(table)
    connection = op.get_bind()
    if connection.scalar(
        sa.text(f"SELECT 1 FROM {_q(schema)}.ptg2_snapshot_partition_preparation WHERE table_name=:table"),
        {"table": table},
    ):
        return
    op.execute(f"LOCK TABLE {qualified} IN ACCESS EXCLUSIVE MODE NOWAIT")
    _assert_supported_table(schema, table)
    constraints = _table_constraints(qualified)
    references = connection.execute(
        sa.text("""
        SELECT oid, conrelid, pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE confrelid=CAST(:table AS regclass)
          AND contype='f' AND conparentid=0 ORDER BY oid
    """),
        {"table": qualified},
    ).mappings()
    if connection.scalar(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=CAST(:table AS regclass) AND NOT convalidated) OR EXISTS(SELECT 1 FROM pg_index WHERE indrelid=CAST(:table AS regclass) AND (NOT indisvalid OR NOT indisready))"
        ),
        {"table": qualified},
    ):
        raise RuntimeError("ptg_snapshot_preparation_unvalidated_integrity")
    plan_by_field = {
        "oid": connection.scalar(sa.text("SELECT CAST(:table AS regclass)::oid"), {"table": qualified}),
        "owner": connection.scalar(
            sa.text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=CAST(:table AS regclass)"),
            {"table": qualified},
        ),
        "keys": keys,
        "grants": [dict(grant) for grant in _grants(qualified)],
        "constraints": [dict(constraint) for constraint in constraints],
        "indexes": [dict(index) for index in _table_indexes(qualified)],
        "references": [dict(reference) for reference in references],
    }
    bound = "snapshot_key IN (" + ",".join(map(str, keys)) + ")" if keys else "false"
    op.execute(f"ALTER TABLE {qualified} ADD CONSTRAINT ptg_historical_snapshot_bound CHECK ({bound}) NOT VALID")
    connection.execute(
        sa.text(
            f"INSERT INTO {_q(schema)}.ptg2_snapshot_partition_preparation VALUES (:table,:ordinal,CAST(:plan AS jsonb),NULL)"
        ),
        {"table": table, "ordinal": ordinal, "plan": json.dumps(plan_by_field)},
    )


def _fence(schema, target):
    """Reject writers while committed conversion phases remain incomplete."""
    op.execute(
        f"CREATE TRIGGER ptg_snapshot_migration_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {target} FOR EACH STATEMENT EXECUTE FUNCTION {_q(schema)}.ptg_snapshot_migration_fence()"
    )


def _verify_preparation(schema):
    """Reject changed relations instead of trusting a stale committed plan."""
    records = _plans(schema)
    if {record["table_name"] for record in records} != set(TABLES):
        raise RuntimeError("ptg_snapshot_preparation_tables_changed")
    for record in records:
        target = _q(schema) + "." + _q(record["table_name"])
        observed = op.get_bind().scalar(sa.text("SELECT to_regclass(:target)::oid"), {"target": target})
        if observed != (record["parent_oid"] or record["plan"]["oid"]):
            raise RuntimeError("ptg_snapshot_preparation_identity_changed")
        if record["parent_oid"] is not None:
            history_oid = op.get_bind().scalar(
                sa.text("SELECT to_regclass(:target)::oid"),
                {"target": _q(schema) + "." + _q(record["table_name"] + "_history")},
            )
            if history_oid != record["plan"]["oid"]:
                raise RuntimeError("ptg_snapshot_preparation_history_changed")


def _create_private_metadata(schema):
    """Fence private migration and validation records even from global table writers."""
    quoted = _q(schema)
    op.execute(
        f"CREATE TABLE IF NOT EXISTS {quoted}.ptg2_snapshot_partition_preparation (table_name text PRIMARY KEY, ordinal int NOT NULL, plan jsonb NOT NULL, parent_oid oid)"
    )
    op.execute(
        f"CREATE TABLE IF NOT EXISTS {quoted}.ptg2_snapshot_partition_boundary (table_name text PRIMARY KEY, writer_roles name[] NOT NULL, relationships jsonb NOT NULL)"
    )
    op.execute(
        f"CREATE TABLE IF NOT EXISTS {quoted}.ptg2_snapshot_candidate (table_oid oid PRIMARY KEY, table_name text NOT NULL UNIQUE, parent_name text NOT NULL, snapshot_key bigint NOT NULL, build_token text NOT NULL, writer_name name NOT NULL, prepared boolean NOT NULL DEFAULT false, prepared_xid xid8, published boolean NOT NULL DEFAULT false)"
    )
    op.execute(f"CREATE TABLE IF NOT EXISTS {quoted}.ptg2_snapshot_legacy_build (snapshot_key bigint PRIMARY KEY)")
    op.execute(
        f"CREATE TABLE {quoted}.ptg2_snapshot_completion_receipt (snapshot_key bigint PRIMARY KEY, build_token text NOT NULL, root_value jsonb NOT NULL, dependencies jsonb NOT NULL)"
    )
    op.execute(f"CREATE TABLE {quoted}.ptg2_snapshot_lifecycle_writer (role_name name PRIMARY KEY)")
    lifecycle_roles = {
        grant["grantee"]
        for grant in _grants(f"{quoted}.ptg2_v3_snapshot_layout")
        if grant["privilege_type"] == "DELETE" and grant["column_name"] is None and grant["grantee"] != "PUBLIC"
    }
    for role in sorted(lifecycle_roles):
        op.get_bind().execute(
            sa.text(f"INSERT INTO {quoted}.ptg2_snapshot_lifecycle_writer VALUES (:role)"), {"role": role}
        )
    op.execute(f"""CREATE FUNCTION {quoted}.guard_ptg_snapshot_private_write() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $body$ BEGIN
        IF current_user<>(SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=TG_RELID) THEN
          RAISE EXCEPTION 'ptg_snapshot_private_write' USING ERRCODE='42501';
        END IF;
        RETURN NULL;
      END $body$""")
    _function_permissions(schema, "guard_ptg_snapshot_private_write()", ())
    for table in (
        "ptg2_snapshot_partition_preparation",
        "ptg2_snapshot_partition_boundary",
        "ptg2_snapshot_candidate",
        "ptg2_snapshot_legacy_build",
        "ptg2_snapshot_completion_receipt",
        "ptg2_snapshot_lifecycle_writer",
    ):
        _restrict_table(f"{quoted}.{table}", all_privileges=True)
        op.execute(
            f"CREATE TRIGGER ptg_snapshot_private_write BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {quoted}.{table} FOR EACH STATEMENT EXECUTE FUNCTION {quoted}.guard_ptg_snapshot_private_write()"
        )


def _prepare(schema):
    """Persist original identities and the rootless BUILDING-layout rerun fence."""
    quoted = _q(schema)
    if op.get_bind().scalar(
        sa.text("SELECT to_regclass(:table)"), {"table": quoted + ".ptg2_snapshot_partition_preparation"}
    ):
        _verify_preparation(schema)
        return
    op.execute(f"LOCK TABLE {quoted}.ptg2_v3_snapshot_layout, {quoted}.ptg2_v4_snapshot_map_root IN SHARE MODE NOWAIT")
    _create_private_metadata(schema)
    op.execute(f"""INSERT INTO {quoted}.ptg2_snapshot_legacy_build
        SELECT snapshot_key FROM {quoted}.ptg2_v3_snapshot_layout WHERE generation='shared_blocks_v4' AND state='building'
        UNION SELECT snapshot_key FROM {quoted}.ptg2_v4_snapshot_map_root WHERE state='building'
        ON CONFLICT DO NOTHING""")
    keys = list(
        op.get_bind().scalars(
            sa.text(
                f"SELECT snapshot_key FROM {quoted}.ptg2_v3_snapshot_layout UNION SELECT snapshot_key FROM {quoted}.ptg2_v4_snapshot_map_root ORDER BY snapshot_key"
            )
        )
    )
    for ordinal, table in enumerate(TABLES):
        _capture_plan(schema, table, ordinal, keys)
    op.execute(
        f"CREATE FUNCTION {quoted}.ptg_snapshot_migration_fence() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $f$ BEGIN RAISE EXCEPTION 'ptg_snapshot_migration_incomplete_rerun_migration' USING ERRCODE='55000'; END $f$"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {quoted}.ptg_snapshot_migration_fence() FROM PUBLIC")
    for table in (*TABLES, "ptg2_v3_snapshot_layout", "ptg2_v4_snapshot_map_root"):
        _fence(schema, quoted + "." + _q(table))


def _validate_bound(schema, record):
    """Validate historical membership under a read-compatible lock and commit."""
    target = _q(schema) + "." + _q(record["table_name"])
    if record["parent_oid"] is None:
        op.execute(f"ALTER TABLE {target} VALIDATE CONSTRAINT ptg_historical_snapshot_bound")


def _parent_indexes(schema, table, plan):
    """Create empty parent indexes; bulk relationships use isolated set checks."""
    target = _q(schema) + "." + _q(table)
    for constraint in plan["constraints"]:
        if constraint["contype"] == "f":
            continue
        definition = op.get_bind().scalar(sa.text("SELECT pg_get_constraintdef(:oid)"), {"oid": constraint["oid"]})
        if not definition:
            raise RuntimeError("ptg_snapshot_preparation_constraint_changed")
        op.execute(f"ALTER TABLE {target} ADD CONSTRAINT {_q(constraint['conname'])} {definition}")
    for index in plan["indexes"]:
        if not index["constrained"]:
            op.execute(index["definition"])


def _partition_table(schema, preparation):
    """Attach the checked historical heap using its indexes and current native FKs."""
    table, plan = preparation["table_name"], preparation["plan"]
    if preparation["parent_oid"] is not None:
        return
    qualified = _q(schema) + "." + _q(table)
    historical = _q(schema) + "." + _q(table + "_history")
    op.execute(f"LOCK TABLE {qualified} IN ACCESS EXCLUSIVE MODE NOWAIT")
    connection = op.get_bind()
    identity = connection.scalar(sa.text("SELECT CAST(:table AS regclass)::oid"), {"table": qualified})
    valid = connection.scalar(
        sa.text(
            "SELECT convalidated FROM pg_constraint WHERE conrelid=:oid AND conname='ptg_historical_snapshot_bound'"
        ),
        {"oid": identity},
    )
    if identity != plan["oid"] or not valid:
        raise RuntimeError("ptg_snapshot_preparation_identity_changed")
    op.execute(f"ALTER TABLE {qualified} RENAME TO {_q(table + '_history')}")
    for constraint in plan["constraints"]:
        if constraint["contype"] == "f":
            op.execute(f"ALTER TABLE {historical} DROP CONSTRAINT {_q(constraint['conname'])}")
    for index in plan["indexes"]:
        op.execute(
            f"ALTER INDEX {_q(schema)}.{_q(index['relname'])} RENAME TO {_q('ptg_history_index_' + str(index['indexrelid']))}"
        )
    op.execute(
        f"CREATE TABLE {qualified} (LIKE {historical} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE) PARTITION BY LIST (snapshot_key)"
    )
    op.execute(f"ALTER TABLE {qualified} DROP CONSTRAINT ptg_historical_snapshot_bound")
    _parent_indexes(schema, table, plan)
    _restrict_table(historical)
    _restrict_table(qualified)
    for grant in plan["grants"]:
        if grant["privilege_type"] in ("SELECT", "REFERENCES"):
            grantee = "PUBLIC" if grant["grantee"] == "PUBLIC" else _q(grant["grantee"])
            column = f" ({_q(grant['column_name'])})" if grant["column_name"] else ""
            option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
            op.execute(f"GRANT {grant['privilege_type']}{column} ON {qualified} TO {grantee}{option}")
            op.execute(f"GRANT {grant['privilege_type']}{column} ON {historical} TO {grantee}{option}")
    if plan["keys"]:
        op.execute(
            f"ALTER TABLE {qualified} ATTACH PARTITION {historical} FOR VALUES IN ({','.join(map(str, plan['keys']))})"
        )
    _fence(schema, qualified)
    op.execute(f"ALTER TABLE {qualified} OWNER TO {_q(plan['owner'])}")
    connection.execute(
        sa.text(
            f"UPDATE {_q(schema)}.ptg2_snapshot_partition_preparation SET parent_oid=CAST(:target AS regclass)::oid WHERE table_name=:table"
        ),
        {"target": qualified, "table": table},
    )


def _original_reference_root(original_oid):
    """Find the canonical inherited root for one captured foreign key."""
    return (
        op.get_bind()
        .execute(
            sa.text("""
        WITH RECURSIVE ancestry AS (
            SELECT oid, conparentid FROM pg_constraint WHERE oid=:oid
            UNION ALL SELECT c.oid,c.conparentid FROM pg_constraint c JOIN ancestry a ON c.oid=a.conparentid
        ) SELECT c.oid,c.conname,c.conrelid,pg_get_constraintdef(c.oid) AS definition,
                 quote_ident(n.nspname)||'.'||quote_ident(t.relname) AS source
            FROM ancestry a JOIN pg_constraint c ON c.oid=a.oid
            JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE a.conparentid=0
    """),
            {"oid": original_oid},
        )
        .mappings()
        .one()
    )


def _reference_roots(schema):
    """Follow inherited FK identities after attaching their original source heap."""
    references = []
    plans = _plans(schema)
    parent_by_history_oid = {preparation["plan"]["oid"]: preparation["parent_oid"] for preparation in plans}
    for preparation in plans:
        for reference in preparation["plan"]["references"]:
            if reference["conrelid"] in parent_by_history_oid:
                continue
            original_oid = reference["oid"]
            root = _original_reference_root(original_oid)
            references.append(
                {
                    **root,
                    "definition": reference["definition"],
                    "replacement": "ptg_snapshot_fk_" + str(original_oid),
                    "target": _q(schema) + "." + _q(preparation["table_name"]),
                }
            )
            # Empty history is detached; its newly created canonical source has
            # a separate FK root which cannot be found through inheritance.
            if root["conrelid"] in parent_by_history_oid:
                parent_reference = (
                    op.get_bind()
                    .execute(
                        sa.text("""
                    SELECT c.oid,c.conname,c.conrelid,
                           quote_ident(n.nspname)||'.'||quote_ident(t.relname) AS source
                      FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
                      JOIN pg_namespace n ON n.oid=t.relnamespace
                     WHERE c.conrelid=:parent AND c.conname=:name AND c.conparentid=0
                """),
                        {"parent": parent_by_history_oid[root["conrelid"]], "name": root["conname"]},
                    )
                    .mappings()
                    .one()
                )
                references.append(
                    {
                        **parent_reference,
                        "definition": reference["definition"],
                        "replacement": "ptg_snapshot_fk_" + str(original_oid) + "_parent",
                    }
                )

    return references


def _prepare_reference(reference):
    """Keep the old FK enforced while adding its final-parent replacement."""
    observed = op.get_bind().scalar(
        sa.text("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=:source AND conname=:name"),
        {"source": reference["conrelid"], "name": reference["replacement"]},
    )
    definition = reference["definition"].removesuffix(" NOT VALID")
    if observed and observed.removesuffix(" NOT VALID") != definition:
        raise RuntimeError("ptg_snapshot_preparation_reference_changed")
    if not observed:
        op.execute(
            f"ALTER TABLE {reference['source']} ADD CONSTRAINT {_q(reference['replacement'])} {definition} NOT VALID"
        )


def _finish_references(references):
    """Retire old FK roots only after every replacement has passed validation."""
    for reference in references:
        valid = op.get_bind().scalar(
            sa.text("SELECT convalidated FROM pg_constraint WHERE conrelid=:source AND conname=:name"),
            {"source": reference["conrelid"], "name": reference["replacement"]},
        )
        if not valid:
            raise RuntimeError("ptg_snapshot_preparation_reference_unvalidated")
        op.execute(f"ALTER TABLE {reference['source']} DROP CONSTRAINT {_q(reference['conname'])}")
        op.execute(
            f"ALTER TABLE {reference['source']} RENAME CONSTRAINT {_q(reference['replacement'])} TO {_q(reference['conname'])}"
        )


def _prepare_partitions(schema):
    """Release strong locks before every scan; resume committed preparation safely."""
    with _phase():
        _prepare(schema)
    for record in _plans(schema):
        with _phase(validation=True):
            _validate_bound(schema, record)
        with _phase():
            _partition_table(schema, record)
    references = _reference_roots(schema)
    for reference in references:
        with _phase():
            _prepare_reference(reference)
        with _phase(validation=True):
            op.execute(f"ALTER TABLE {reference['source']} VALIDATE CONSTRAINT {_q(reference['replacement'])}")
    return references


def _finish_tables(schema):
    """Open only protected publishers after durable preparations are complete."""
    writers = set()
    for preparation in _plans(schema):
        table, plan = preparation["table_name"], preparation["plan"]
        for guard in _expected_guards(schema, table):
            op.execute(f"DROP TRIGGER {_q(guard['tgname'])} ON {_q(schema)}.{_q(table + '_history')}")
        table_writers = {
            grant["grantee"]
            for grant in plan["grants"]
            if grant["privilege_type"] == "INSERT" and grant["column_name"] is None and grant["grantee"] != "PUBLIC"
        }
        op.get_bind().execute(
            sa.text(
                f"INSERT INTO {_q(schema)}.ptg2_snapshot_partition_boundary VALUES (:table,CAST(:writers AS name[]),CAST(:relationships AS jsonb))"
            ),
            {
                "table": table,
                "writers": sorted(table_writers),
                "relationships": json.dumps(
                    [constraint["relationship"] for constraint in plan["constraints"] if constraint["contype"] == "f"]
                ),
            },
        )
        writers.update(table_writers)
    fences = op.get_bind().scalars(
        sa.text(
            "SELECT tgrelid::regclass::text FROM pg_trigger WHERE tgname='ptg_snapshot_migration_fence' AND tgfoid=CAST(:function AS regprocedure)"
        ),
        {"function": _q(schema) + ".ptg_snapshot_migration_fence()"},
    )
    for table_identity in fences:
        op.execute(f"DROP TRIGGER ptg_snapshot_migration_fence ON {table_identity}")
    op.execute(f"DROP FUNCTION {_q(schema)}.ptg_snapshot_migration_fence()")
    return writers


def _install_sql(schema, filename):
    """Install explicit functions with a fixed, quoted schema."""
    quoted = _q(schema)
    functions = (Path(__file__).resolve().parents[2] / "db/sql" / filename).read_text()
    for definition in functions.strip().split("END $body$;"):
        if definition.strip():
            op.execute(
                (definition + "END $body$;")
                .replace("__S__", quoted)
                .replace("__Q__", quoted.replace("'", "''"))
                .replace("__SCHEMA_LITERAL__", "'" + schema.replace("'", "''") + "'")
            )


def _function_permissions(schema, signature, writers):
    """Remove inherited default execution grants before granting exact roles."""
    qualified = f"{_q(schema)}.{signature}"
    op.execute(f"REVOKE ALL ON FUNCTION {qualified} FROM PUBLIC")
    default_grantees = op.get_bind().scalars(
        sa.text("""
        SELECT DISTINCT pg_get_userbyid(a.grantee) FROM pg_proc p
        CROSS JOIN LATERAL aclexplode(p.proacl) a
        WHERE p.oid=CAST(:signature AS regprocedure) AND a.grantee<>0 AND a.grantee<>p.proowner
    """),
        {"signature": qualified},
    )
    for grantee in default_grantees:
        op.execute(f"REVOKE ALL ON FUNCTION {qualified} FROM {_q(grantee)}")
    for writer in sorted(writers):
        op.execute(f"GRANT EXECUTE ON FUNCTION {qualified} TO {'PUBLIC' if writer == 'PUBLIC' else _q(writer)}")


def _install_candidate_functions(schema, writers):
    """Install only the protected interfaces for captured writers and GC roles."""
    quoted = _q(schema)
    for filename in ("ptg_snapshot_candidates.sql", "ptg_snapshot_completion.sql", "ptg_snapshot_gc.sql"):
        _install_sql(schema, filename)
    # Recovery checks physical ownership immediately after deleting the layout.
    # Bulk FKs are retired; this AFTER event completes their cleanup in that statement.
    op.execute(
        f"CREATE CONSTRAINT TRIGGER ptg_snapshot_candidate_cleanup AFTER DELETE ON {quoted}.ptg2_v3_snapshot_layout DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION {quoted}.cleanup_ptg_snapshot_candidates()"
    )
    for signature in (
        "begin_ptg_snapshot_candidate(text,bigint,text)",
        "finish_ptg_snapshot_candidate(text,bigint)",
        "read_ptg_snapshot_candidates(bigint,text)",
        "attach_ptg_snapshot_candidates(bigint,text)",
        "prepare_ptg_snapshot_completion(bigint,text,jsonb)",
    ):
        _function_permissions(schema, signature, writers)
    for signature in (
        "ptg_snapshot_writer_authorized(text,name)",
        "ptg_snapshot_relation(text,bigint,text)",
        "validate_ptg_snapshot_relationships(text)",
        "cleanup_ptg_snapshot_candidates()",
        f"validate_ptg_snapshot_root({quoted}.ptg2_v4_snapshot_map_root,text)",
        f"validate_ptg_snapshot_tax_completion({quoted}.ptg2_v4_snapshot_map_root,text)",
        "ptg_snapshot_completion_dependencies(bigint)",
        f"require_ptg_snapshot_completion({quoted}.ptg2_v4_snapshot_map_root)",
        "guard_ptg2_v4_snapshot_map_root()",
        "guard_ptg2_provider_tax_identity_completion()",
    ):
        _function_permissions(schema, signature, ())
    # The checker only authorizes its explicit actor; it has no write side effects.
    _function_permissions(schema, "check_ptg_snapshot_write(oid,text,name)", ("PUBLIC",))
    _function_permissions(schema, "guard_ptg_snapshot_write()", ())
    for table in TABLES:
        for relation in (table, table + "_history"):
            op.execute(
                f"CREATE TRIGGER ptg_snapshot_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {quoted}.{_q(relation)} FOR EACH STATEMENT EXECUTE FUNCTION {quoted}.guard_ptg_snapshot_write()"
            )
    lifecycle_roles = op.get_bind().scalars(sa.text(f"SELECT role_name FROM {quoted}.ptg2_snapshot_lifecycle_writer"))
    _function_permissions(schema, "delete_ptg_snapshot_history(bigint,integer,integer,text)", lifecycle_roles)


def upgrade():
    """Install protected candidate publication and preserve historical snapshots."""
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        raise RuntimeError("ptg_snapshot_migration_requires_online_connection")
    if int(op.get_bind().scalar(sa.text("SHOW server_version_num"))) < 180000:
        raise RuntimeError("ptg_snapshot_migration_requires_postgresql_18")
    with context.autocommit_block():
        locked = op.get_bind().scalar(
            sa.text("SELECT pg_try_advisory_lock(hashtextextended(:key,0))"),
            {"key": schema + ".ptg_snapshot_partition_preparation"},
        )
        if not locked:
            raise RuntimeError("ptg_snapshot_migration_already_running")
        try:
            references = _prepare_partitions(schema)
        except BaseException:
            op.get_bind().execute(
                sa.text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"),
                {"key": schema + ".ptg_snapshot_partition_preparation"},
            )
            raise
    op.get_bind().execute(
        sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": schema + ".ptg_snapshot_partition_preparation"},
    )
    op.get_bind().execute(
        sa.text("SELECT pg_advisory_unlock(hashtextextended(:key,0))"),
        {"key": schema + ".ptg_snapshot_partition_preparation"},
    )
    op.execute("SET LOCAL lock_timeout='1s'")
    op.execute("SET LOCAL statement_timeout='10s'")
    _finish_references(references)
    writers = _finish_tables(schema)
    _install_candidate_functions(schema, writers)


def downgrade():
    """Require an explicit rollback that preserves published snapshot partitions."""
    raise RuntimeError("PTG snapshot partitions require an explicit data-preserving rollback")
