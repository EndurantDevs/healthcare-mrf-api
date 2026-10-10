# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One server-selected office-only owner; native scope is checked, never granted."""

import re
from dataclasses import dataclass

from process.network_address_projection import _identifier
from process.registry_ptg_published_provisioning import _PERMISSION_SQL, HEADER_TABLES, LOCK_COLUMN


@dataclass(frozen=True)
class RegistryPTGOfficeCustodyProfile:
    """Installed server input, never an office review command field."""

    owner_role: str
    publisher_role: str

    def __post_init__(self):
        _identifier(self.owner_role)
        _identifier(self.publisher_role)
        if self.owner_role == self.publisher_role:
            raise ValueError("registry_ptg_office_profile_invalid")


# Reuse the original native immutable-column guard for the configured Publisher,
# even when the actual connection actor is the protected Approver. All replacement
# inputs below are fixed installed constants; no role switch or caller SQL is used.
_PUBLISHER_LOCK_SQL = _PERMISSION_SQL.replace("SELECT c.relname,", "SELECT c.oid, a.attnum, c.relname,").replace(
    "current_user", "$2::name"
)
for _parameter, _literal in (
    (":schema_name", "$6::text"),
    (":tables", "ARRAY[" + ",".join("'" + table + "'" for table in HEADER_TABLES) + "]::text[]"),
    (":lock_column", "'" + LOCK_COLUMN + "'"),
    (":constant_expression", "'" + LOCK_COLUMN + "=0'"),
    (":pin_writer", "FALSE"),
    (":pin_columns", "ARRAY[]::text[]"),
):
    _PUBLISHER_LOCK_SQL = _PUBLISHER_LOCK_SQL.replace(_parameter, _literal)

_PROFILE_SQL = (
    "WITH source_locks AS ("
    + _PUBLISHER_LOCK_SQL
    + """
), owner AS (SELECT * FROM pg_catalog.pg_roles WHERE rolname=$1),
publisher AS (SELECT * FROM pg_catalog.pg_roles WHERE rolname=$2),
owned AS (SELECT n.oid,n.nspname FROM pg_catalog.pg_namespace n,owner o WHERE n.nspowner=o.oid),
relations AS (
 SELECT c.oid,c.reltype,c.reltoastrelid FROM pg_catalog.pg_class c JOIN owned n ON n.oid=c.relnamespace),
toast AS (SELECT c.oid,c.reltype FROM pg_catalog.pg_class c WHERE c.oid IN (SELECT reltoastrelid FROM relations)),
allowed_objects AS (
 SELECT 'pg_catalog.pg_namespace'::regclass::oid AS classid,oid AS objid FROM owned
 UNION SELECT 'pg_catalog.pg_class'::regclass::oid,oid FROM relations
 UNION SELECT 'pg_catalog.pg_class'::regclass::oid,oid FROM toast
 UNION SELECT 'pg_catalog.pg_class'::regclass::oid,i.indexrelid FROM pg_catalog.pg_index i JOIN toast t ON t.oid=i.indrelid
 UNION SELECT 'pg_catalog.pg_type'::regclass::oid,t.oid FROM pg_catalog.pg_type t
 WHERE t.typrelid IN (SELECT oid FROM relations UNION SELECT oid FROM toast)
 OR t.typelem IN (SELECT reltype FROM relations UNION SELECT reltype FROM toast))
SELECT o.oid::bigint AS owner_oid,p.oid::bigint AS publisher_oid,
 current_user=session_user AND
 CASE WHEN $4::boolean THEN current_user=p.rolname ELSE current_user=ANY($3::text[]) END AS genuine,
 NOT o.rolcanlogin AND NOT o.rolsuper AND NOT o.rolcreatedb AND NOT o.rolcreaterole
 AND NOT o.rolreplication AND NOT o.rolbypassrls AND p.rolcanlogin AND NOT p.rolsuper
 AND NOT p.rolcreatedb AND NOT p.rolcreaterole AND NOT p.rolreplication AND NOT p.rolbypassrls
 AND o.rolname<>$5 AND p.rolname<>$5 AS flags,
 pg_catalog.pg_has_role(p.oid,o.oid,'SET') AND NOT EXISTS(
 SELECT FROM pg_catalog.pg_roles r WHERE r.oid NOT IN (p.oid,o.oid)
 AND (pg_catalog.pg_has_role(p.oid,r.oid,'SET') OR pg_catalog.pg_has_role(p.oid,r.oid,'USAGE'))) AND NOT EXISTS(
 SELECT FROM pg_catalog.pg_roles r WHERE r.oid<>o.oid AND (pg_catalog.pg_has_role(o.oid,r.oid,'SET') OR pg_catalog.pg_has_role(o.oid,r.oid,'USAGE'))) AS set_closed,
 NOT EXISTS(SELECT FROM unnest($3::text[]) runtime(name) LEFT JOIN pg_catalog.pg_roles r ON r.rolname=runtime.name
 WHERE r.oid IS NULL OR NOT r.rolcanlogin OR r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls
 OR pg_catalog.pg_has_role(r.oid,o.oid,'SET') OR pg_catalog.pg_has_role(r.oid,$5::name,'SET')) AS readers_closed,
 NOT EXISTS(SELECT FROM pg_catalog.pg_shdepend d WHERE d.refclassid='pg_catalog.pg_authid'::regclass
 AND d.refobjid IN (o.oid,p.oid) AND d.deptype='o' AND
 (d.dbid<>(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
 OR d.refobjid=p.oid OR NOT EXISTS(SELECT FROM allowed_objects a WHERE a.classid=d.classid AND a.objid=d.objid)))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_class c WHERE c.oid NOT IN (
 SELECT objid FROM allowed_objects WHERE classid='pg_catalog.pg_class'::regclass)
 AND (pg_catalog.has_table_privilege(o.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR pg_catalog.has_any_column_privilege(o.oid,c.oid,'INSERT,UPDATE,REFERENCES')
 OR CASE WHEN c.relkind='S' THEN pg_catalog.has_sequence_privilege(o.oid,c.oid,'USAGE,UPDATE') ELSE FALSE END
))
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_namespace n WHERE n.oid NOT IN (SELECT oid FROM owned)
 AND (pg_catalog.has_schema_privilege(o.oid,n.oid,'CREATE') OR pg_catalog.has_schema_privilege(p.oid,n.oid,'CREATE'))) AS office_only,
 (SELECT count(*) FROM source_locks)=6
 AND (SELECT bool_and(protected) FROM source_locks) IS TRUE
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_class fact
 WHERE pg_catalog.has_table_privilege(p.oid,fact.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR CASE WHEN fact.relkind='S' THEN pg_catalog.has_sequence_privilege(p.oid,fact.oid,'USAGE,UPDATE') ELSE FALSE END
 OR EXISTS(SELECT FROM pg_catalog.pg_attribute field WHERE field.attrelid=fact.oid
 AND field.attnum>0 AND NOT field.attisdropped AND (
 pg_catalog.has_column_privilege(p.oid,fact.oid,field.attnum,'INSERT,REFERENCES')
 OR (pg_catalog.has_column_privilege(p.oid,fact.oid,field.attnum,'UPDATE')
 AND NOT EXISTS(SELECT FROM source_locks source_lock WHERE source_lock.oid=fact.oid
 AND source_lock.attnum=field.attnum AND source_lock.protected IS TRUE))))) AS publisher_writes_closed,
 (SELECT count(*) FROM owned) AS schema_count,
 ARRAY(SELECT nspname FROM owned ORDER BY nspname LIMIT 129) AS schemas
FROM owner o CROSS JOIN publisher p
"""
)


async def verify_registry_ptg_office_custody(driver, context, *, publisher):
    """Refuse missing profile, inherited owner authority and foreign object writes.

    Native catalog/provisioning must remain stable during this owned transaction.
    The original exact-family custody verifier validates every retained family.
    """
    from process.registry_ptg_office_capture import _custody

    profile = context.office_custody
    if type(profile) is not RegistryPTGOfficeCustodyProfile or profile.owner_role != context.owner_role:
        raise ValueError("registry_ptg_office_profile_unavailable")
    profile_by_field = await driver.fetchrow(
        _PROFILE_SQL,
        profile.owner_role,
        profile.publisher_role,
        list(context.reader_roles),
        publisher,
        context.scope_store.owner_role,
        context.source_specification.ptg_schema_name,
    )
    if (
        profile_by_field is None
        or any(
            profile_by_field[field] is not True
            for field in (
                "genuine",
                "flags",
                "set_closed",
                "readers_closed",
                "office_only",
                "publisher_writes_closed",
            )
        )
        or type(profile_by_field["schema_count"]) is not int
        or not 0 <= profile_by_field["schema_count"] <= 128
    ):
        raise ValueError("registry_ptg_office_profile_unavailable")
    for schema_name in profile_by_field["schemas"]:
        # A name alone does not confer authority: original column/ACL/OID/shape
        # verification must also corroborate each actual native family.
        if re.fullmatch(r"registry_ptg_office_[0-9a-f]{32}", schema_name) is None:
            raise ValueError("registry_ptg_office_profile_unavailable")
        await _custody(driver, schema_name, context)
    return profile.owner_role
