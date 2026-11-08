# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native queue audit custody shared by ordinary and office lifecycle writers.

The protected owner and bootstrap operators remain trusted. This verifies the
current runtime and every non-owner queue ACL grantee, including SET paths;
it neither installs grants nor claims protection against native administrators.
"""

from process.network_address_projection import _identifier
from process.registry_source_observation_store import _namespace

LIFECYCLE_COLUMNS = frozenset({"state", "lease_id", "lease_expires_at", "result_json", "updated_at"})
INSERT_COLUMNS = frozenset(
    {
        "request_id",
        "actor_key",
        "session_token_sha256",
        "actor_json",
        "command_json",
        "idempotency_key",
        "request_sha256",
    }
)
_COLUMNS = LIFECYCLE_COLUMNS | INSERT_COLUMNS | {"created_at"}

_CATALOG_SQL = """
SELECT c.oid::bigint AS table_oid,n.oid::bigint AS schema_oid,o.oid::bigint AS owner_oid,
 o.rolname AS owner_role,r.oid::bigint AS runtime_oid,
 c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND c.relowner=n.nspowner AND NOT o.rolcanlogin AND NOT o.rolsuper AND NOT o.rolcreatedb
 AND NOT o.rolcreaterole AND NOT o.rolreplication AND NOT o.rolbypassrls
 AND current_user=session_user AND r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolcreatedb
 AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls
 AND ($2::text IS NULL OR current_user::text=$2::text) AND ($3::text IS NULL OR o.rolname::text=$3::text)
 AND ($4::text IS NULL OR (o.rolname::text<>$4::text AND NOT pg_catalog.pg_has_role($4::name,o.oid,'SET')))
 AND pg_catalog.has_schema_privilege(r.oid,n.oid,'USAGE')
 AND pg_catalog.has_table_privilege(r.oid,c.oid,'SELECT')
 AND NOT pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT') AS closed
FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
JOIN pg_catalog.pg_roles o ON o.oid=c.relowner JOIN pg_catalog.pg_roles r ON r.rolname=current_user
WHERE c.oid=pg_catalog.to_regclass($1)
"""

_CUSTODY_SQL = """
WITH grants AS (
 SELECT NULL::text AS column_name,a.* FROM pg_catalog.pg_class c
 CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
 WHERE c.oid=$1::oid
 UNION ALL SELECT att.attname,a.* FROM pg_catalog.pg_attribute att
 CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
 WHERE att.attrelid=$1::oid AND att.attnum>0 AND NOT att.attisdropped
), subjects AS (
 SELECT $4::oid AS oid UNION SELECT grantee FROM grants WHERE grantee NOT IN (0,$3::oid)
), reachable AS (
 SELECT DISTINCT r.* FROM subjects s JOIN pg_catalog.pg_roles r
 ON r.oid=s.oid OR pg_catalog.pg_has_role(s.oid,r.oid,'SET')
)
SELECT NOT EXISTS(SELECT FROM grants g WHERE g.grantee<>$3::oid AND
 (g.is_grantable OR g.privilege_type NOT IN ('SELECT','INSERT','UPDATE')
 OR g.grantee=0 AND g.privilege_type<>'SELECT'
 OR g.privilege_type='INSERT' AND (g.column_name IS NULL OR NOT g.column_name=ANY($6::text[]))
 OR g.privilege_type='UPDATE' AND (g.column_name IS NULL OR NOT g.column_name=ANY($5::text[]))))
 AND NOT EXISTS(SELECT FROM reachable r WHERE r.oid=$3::oid
 OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
 OR pg_catalog.pg_has_role(r.oid,$3::oid,'SET')
 OR pg_catalog.pg_has_role(r.oid,'pg_write_all_data','MEMBER')
 OR pg_catalog.has_schema_privilege(r.oid,$2::oid,'CREATE')
 OR EXISTS(SELECT FROM pg_catalog.pg_namespace n
 CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) a WHERE n.oid=$2::oid AND a.is_grantable
 AND (a.grantee=r.oid OR a.grantee=0 OR pg_catalog.pg_has_role(r.oid,a.grantee,'USAGE')))
 OR pg_catalog.has_table_privilege(r.oid,$1::oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR EXISTS(SELECT FROM pg_catalog.pg_attribute att WHERE att.attrelid=$1::oid
 AND att.attnum>0 AND NOT att.attisdropped AND
 (pg_catalog.has_column_privilege(r.oid,$1::oid,att.attnum,'REFERENCES')
 OR NOT att.attname=ANY($5::text[]) AND pg_catalog.has_column_privilege(r.oid,$1::oid,att.attnum,'UPDATE')
 OR NOT att.attname=ANY($6::text[]) AND pg_catalog.has_column_privilege(r.oid,$1::oid,att.attnum,'INSERT')))) AS closed
"""

_COLUMNS_SQL = """
SELECT attname,pg_catalog.has_column_privilege($2::oid,attrelid,attnum,'UPDATE') AS allowed
FROM pg_catalog.pg_attribute WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped
"""


async def verify_registry_publication_lifecycle_permissions(
    connection, *, control_schema=None, role=None, owner_role=None, physical_owner_role=None
):
    """Require exact lifecycle rights and globally closed native queue audit ACLs."""
    for configured in (role, owner_role, physical_owner_role):
        if configured is not None:
            _identifier(configured)
    if not connection.is_in_transaction():
        raise ValueError("registry_publication_queue_permissions_unavailable")
    table = await connection.fetchrow(
        _CATALOG_SQL,
        _namespace(control_schema) + ".registry_publication_request",
        role,
        owner_role,
        physical_owner_role,
    )
    if table is None or table["closed"] is not True:
        raise ValueError("registry_publication_queue_permissions_unavailable")
    closed = await connection.fetchval(
        _CUSTODY_SQL,
        table["table_oid"],
        table["schema_oid"],
        table["owner_oid"],
        table["runtime_oid"],
        sorted(LIFECYCLE_COLUMNS),
        sorted(INSERT_COLUMNS),
    )
    columns = await connection.fetch(_COLUMNS_SQL, table["table_oid"], table["runtime_oid"])
    if (
        closed is not True
        or {column["attname"] for column in columns} != _COLUMNS
        or {column["attname"] for column in columns if column["allowed"]} != LIFECYCLE_COLUMNS
    ):
        raise ValueError("registry_publication_queue_permissions_unavailable")
    return {"owner_role": table["owner_role"], "table_oid": table["table_oid"]}
