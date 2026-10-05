# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Preserve existing dataset heaps while enabling detached publication stages."""

import sqlalchemy as sa

TABLES = (
    "provider_directory_dataset_resource",
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_affiliation_organization",
    "provider_directory_uhc_flex_practitioner_dataset_resource",
    "provider_directory_rooted_graph_dataset_resource",
)


def quote(value):
    """Quote one PostgreSQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def literal(value):
    """Quote one PostgreSQL text literal."""
    return "'" + value.replace("'", "''") + "'"


def _metadata(connection, qualified):
    parameters_by_name = {"relation": qualified}
    constraints = list(
        connection.execute(
            sa.text("""
        SELECT oid, conname, convalidated, contype::text AS contype, pg_get_constraintdef(oid) AS definition
          FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass)
           AND contype IN ('p','u','f') AND conparentid=0
    """),
            parameters_by_name,
        ).mappings()
    )
    indexes = list(
        connection.execute(
            sa.text("""
        SELECT i.indexrelid, c.relname, pg_get_indexdef(i.indexrelid) AS definition,
               EXISTS (SELECT 1 FROM pg_constraint WHERE conindid=i.indexrelid AND conrelid=i.indrelid AND contype IN ('p','u')) AS constrained
          FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
         WHERE i.indrelid=CAST(:relation AS regclass)
    """),
            parameters_by_name,
        ).mappings()
    )
    triggers = list(
        connection.execute(
            sa.text("""
        SELECT tgname, tgenabled::text AS tgenabled, pg_get_triggerdef(trigger.oid) AS definition,
               tgtype, procedure.proname
          FROM pg_trigger trigger JOIN pg_proc procedure ON procedure.oid=trigger.tgfoid
         WHERE tgrelid=CAST(:relation AS regclass) AND NOT tgisinternal
    """),
            parameters_by_name,
        ).mappings()
    )
    grants = list(
        connection.execute(
            sa.text("""
        SELECT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END AS grantee,
               a.privilege_type, a.is_grantable
          FROM pg_class c CROSS JOIN LATERAL
               aclexplode(COALESCE(c.relacl, acldefault('r',c.relowner))) a
         WHERE c.oid=CAST(:relation AS regclass) AND a.grantee<>c.relowner
    """),
            parameters_by_name,
        ).mappings()
    )
    return constraints, indexes, triggers, grants


def _convert_relation(op, schema, table, keys):
    connection = op.get_bind()
    parent_relation = f"{quote(schema)}.{quote(table)}"
    constraints, indexes, triggers, grants = _metadata(connection, parent_relation)
    _assert_known_row_guards(triggers)
    column_grants = _column_grants(connection, parent_relation)
    owner = connection.scalar(
        sa.text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=CAST(:table AS regclass)"),
        {"table": parent_relation},
    )
    history_name = "pd_dataset_history_" + str(
        connection.scalar(sa.text("SELECT CAST(:table AS regclass)::oid"), {"table": parent_relation})
    )
    history = f"{quote(schema)}.{quote(history_name)}"
    for constraint in constraints:
        if constraint["contype"] == "f":
            op.execute(f"ALTER TABLE {parent_relation} DROP CONSTRAINT {quote(constraint['conname'])}")
    for trigger in triggers:
        op.execute(f"DROP TRIGGER {quote(trigger['tgname'])} ON {parent_relation}")
    op.execute(f"ALTER TABLE {parent_relation} RENAME TO {quote(history_name)}")
    for index in indexes:
        op.execute(
            f"ALTER INDEX {quote(schema)}.{quote(index['relname'])} RENAME TO {quote('pd_dataset_history_idx_' + str(index['indexrelid']))}"
        )
    op.execute(
        f"CREATE TABLE {parent_relation} (LIKE {history} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING STORAGE) PARTITION BY LIST (dataset_id)"
    )
    _normalize_relation_grants(op, parent_relation)
    for constraint in constraints:
        if constraint["contype"] in ("p", "u"):
            op.execute(
                f"ALTER TABLE {parent_relation} ADD CONSTRAINT {quote(constraint['conname'])} {constraint['definition']}"
            )
    for index in indexes:
        if not index["constrained"]:
            op.execute(index["definition"])
    if keys:
        dataset_literals = ",".join(literal(key) for key in keys)
        op.execute(f"ALTER TABLE {parent_relation} ATTACH PARTITION {history} FOR VALUES IN ({dataset_literals})")
    _restore_grants_and_guards(op, schema, table, history, triggers, grants)
    _restore_column_grants(op, parent_relation, history, table in TABLES[4:], column_grants)
    op.execute(f"ALTER TABLE {parent_relation} OWNER TO {quote(owner)}")
    return history


def _deny_direct_writes(op, schema, relation):
    op.execute(
        f"CREATE TRIGGER pd_dataset_sealed BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {quote(schema)}.deny_pd_dataset_candidate_write()"
    )


def _assert_known_row_guards(triggers):
    known_guards = {
        "guard_provider_directory_terminal_root_retirement_child",
        "guard_pd_uhc_flex_practitioner_dataset_resource",
        "guard_provider_directory_rooted_graph_dataset_resource",
    }
    if any(trigger["tgtype"] & 1 and trigger["proname"] not in known_guards for trigger in triggers):
        raise RuntimeError("provider_dataset_candidate_row_guard_drift")


def _normalize_relation_grants(op, relation):
    grantees = op.get_bind().scalars(
        sa.text("""
        SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END
          FROM pg_class relation CROSS JOIN LATERAL aclexplode(relacl) acl
         WHERE relation.oid=CAST(:relation AS regclass) AND acl.grantee<>relowner
    """),
        {"relation": relation},
    )
    for grantee in grantees:
        role = "PUBLIC" if grantee == "PUBLIC" else quote(grantee)
        op.execute(f"REVOKE ALL ON {relation} FROM {role}")


def authorized_candidate_writers(op, schema):
    """Transfer provenance ACL groups without expanding inherited memberships."""
    writers_by_kind = {}
    for kind, provenance in zip(("practitioner", "rooted"), TABLES[4:], strict=True):
        writers_by_kind[kind] = list(
            op.get_bind().scalars(
                sa.text("""
            SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE role.rolname END
              FROM pg_class provenance
              CROSS JOIN LATERAL aclexplode(COALESCE(provenance.relacl,acldefault('r',provenance.relowner))) acl
              LEFT JOIN pg_roles role ON role.oid=acl.grantee
             WHERE provenance.oid=CAST(:provenance AS regclass) AND acl.privilege_type='INSERT'
               AND ((acl.grantee<>0 AND NOT role.rolsuper AND role.rolname !~ '^pg_')
                    OR (acl.grantee=0 AND EXISTS (
                        SELECT FROM pg_class resource CROSS JOIN LATERAL
                            aclexplode(COALESCE(resource.relacl,acldefault('r',resource.relowner))) resource_acl
                         WHERE resource.oid=CAST(:resource AS regclass)
                           AND resource_acl.grantee=0 AND resource_acl.privilege_type='INSERT')))
        """),
                {
                    "resource": f"{quote(schema)}.{quote(TABLES[0])}",
                    "provenance": f"{quote(schema)}.{quote(provenance)}",
                },
            )
        )
    return writers_by_kind


def grant_candidate_api(op, schema, writers_by_kind):
    """Remove default function grants before restoring each exact writer group."""
    functions = (
        "deny_pd_dataset_candidate_write",
        "guard_pd_dataset_migration_owner",
        "prepare_pd_generic_dataset_storage",
        "pd_dataset_candidate_tables",
        "guard_pd_dataset_candidate_parent",
        "prepare_pd_dataset_candidate",
        "finish_pd_dataset_candidate",
        "validate_pd_dataset_candidate",
        "guard_pd_dataset_bulk_parent_delete",
        "guard_pd_dataset_parent_key_update",
        "guard_pd_generic_dataset_finalization",
        "validate_pd_dataset_bulk_relationships",
        "prepare_pd_rooted_dataset_candidate",
        "finish_pd_rooted_dataset_candidate",
        "prepare_pd_practitioner_dataset_candidate",
        "finish_pd_practitioner_dataset_candidate",
    )
    grants = (
        op.get_bind()
        .execute(
            sa.text("""
        SELECT procedure.oid::regprocedure::text AS signature,
               CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END AS grantee
          FROM pg_proc procedure CROSS JOIN LATERAL
               aclexplode(COALESCE(proacl,acldefault('f',proowner))) acl
         WHERE pronamespace=CAST(:schema AS regnamespace) AND proname=ANY(:names)
           AND acl.grantee<>proowner
    """),
            {"schema": quote(schema), "names": list(functions)},
        )
        .mappings()
    )
    for grant in grants:
        role = "PUBLIC" if grant["grantee"] == "PUBLIC" else quote(grant["grantee"])
        op.execute(f"REVOKE ALL ON FUNCTION {grant['signature']} FROM {role}")
    for kind, writers in writers_by_kind.items():
        for writer in writers:
            role = "PUBLIC" if writer == "PUBLIC" else quote(writer)
            for action in ("prepare", "finish"):
                op.execute(
                    f"GRANT EXECUTE ON FUNCTION {quote(schema)}.{action}_pd_{kind}_dataset_candidate(text) TO {role}"
                )


def _column_grants(connection, relation):
    return list(
        connection.execute(
            sa.text("""
        SELECT attname, CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END AS grantee,
               acl.privilege_type, acl.is_grantable
          FROM pg_attribute attribute CROSS JOIN LATERAL aclexplode(attacl) acl
         WHERE attrelid=CAST(:relation AS regclass) AND attnum>0 AND NOT attisdropped
    """),
            {"relation": relation},
        ).mappings()
    )


def _restore_column_grants(op, target, history, is_provenance, column_grants):
    for grant in column_grants:
        role = "PUBLIC" if grant["grantee"] == "PUBLIC" else quote(grant["grantee"])
        privilege = f"{grant['privilege_type']} ({quote(grant['attname'])})"
        if grant["privilege_type"] not in ("SELECT", "REFERENCES"):
            op.execute(f"REVOKE {privilege} ON {history} FROM {role}")
        else:
            option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
            op.execute(f"GRANT {privilege} ON {history} TO {role}{option}")
        if not is_provenance or grant["privilege_type"] in ("SELECT", "REFERENCES"):
            option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
            op.execute(f"GRANT {privilege} ON {target} TO {role}{option}")


def _restore_grants_and_guards(op, schema, table, history, triggers, grants):
    target = f"{quote(schema)}.{quote(table)}"
    is_provenance = table in TABLES[4:]
    _normalize_relation_grants(op, history)
    for grant in grants:
        role = "PUBLIC" if grant["grantee"] == "PUBLIC" else quote(grant["grantee"])
        if not is_provenance or grant["privilege_type"] in ("SELECT", "REFERENCES"):
            option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
            op.execute(f"GRANT {grant['privilege_type']} ON {target} TO {role}{option}")
        if grant["privilege_type"] in ("SELECT", "REFERENCES"):
            op.execute(f"GRANT {grant['privilege_type']} ON {history} TO {role}")
    if is_provenance:
        _deny_direct_writes(op, schema, history)
        _deny_direct_writes(op, schema, target)
    else:
        for trigger in triggers:
            if trigger["tgtype"] & 1:
                continue
            op.execute(trigger["definition"])
            mode = {"A": "ENABLE ALWAYS", "R": "ENABLE REPLICA", "D": "DISABLE", "O": "ENABLE"}[trigger["tgenabled"]]
            op.execute(f"ALTER TABLE {target} {mode} TRIGGER {quote(trigger['tgname'])}")
        for relation in (target, history):
            _guard_partition_writes(op, schema, relation, table)


def _guard_partition_writes(op, schema, relation, table):
    for operation, image in (("INSERT", "NEW"), ("UPDATE", "NEW"), ("UPDATE", "OLD"), ("DELETE", "OLD")):
        trigger = "a_pd_candidate_" + operation.lower() + "_" + image.lower()
        op.execute(
            f"CREATE TRIGGER {trigger} AFTER {operation} ON {relation} REFERENCING {image} TABLE AS affected_rows FOR EACH STATEMENT EXECUTE FUNCTION {quote(schema)}.guard_pd_dataset_candidate_parent({literal(table)},{literal(image)})"
        )


def _assert_supported_dependencies(connection, relations):
    unsupported = connection.scalar(
        sa.text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_class relation
             WHERE relation.oid=ANY(CAST(:relations AS regclass[]))
               AND (relkind<>'r' OR relrowsecurity OR relforcerowsecurity
                    OR EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class=relation.oid)
                    OR EXISTS (SELECT 1 FROM pg_depend WHERE refobjid=relation.oid
                               AND refclassid='pg_class'::regclass
                               AND classid IN ('pg_rewrite'::regclass,'pg_proc'::regclass))
                    OR EXISTS (SELECT 1 FROM pg_depend WHERE refobjid=relation.reltype
                               AND refclassid='pg_type'::regclass AND classid='pg_proc'::regclass))
        )
    """),
        {"relations": relations},
    )
    if unsupported:
        raise RuntimeError("provider_dataset_candidate_dependency_drift")


def candidate_functions(schema):
    """Render the fixed candidate API with safely quoted schema identifiers."""
    from pathlib import Path

    source = Path(__file__).with_name("sql") / "provider_directory_dataset_candidates.sql"
    return source.read_text().replace("__SCHEMA__", quote(schema)).replace("__SCHEMA_LITERAL__", literal(schema))
