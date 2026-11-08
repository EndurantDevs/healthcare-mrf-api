# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A fixed protected projection for negative NUCC retention checks."""

import json
from contextlib import asynccontextmanager

from sqlalchemy import text

LEDGERS = ("relation", "preparation_relation", "reference_preparation_relation", "legacy_archive_relation")
VIEW_NAME = "nucc_retained_relation_claims"
VIEW = '"mrf"."' + VIEW_NAME + '"'
CLAIMS_SQL = " UNION ALL ".join(f"SELECT {name}.relation_oid FROM hp_snapshot_retention.{name}" for name in LEDGERS)
CREATE_VIEW_SQL = f"CREATE VIEW {VIEW} WITH (security_barrier=true,security_invoker=false) AS {CLAIMS_SQL}"
OWNER_PRIVILEGES = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
_ACTOR_SQL = """SELECT jsonb_build_object('oid',r.oid::bigint,'name',r.rolname,
 'safe',session_user=current_user AND r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolcreatedb
 AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls
 AND NOT EXISTS(SELECT FROM pg_roles inherited WHERE pg_has_role(r.oid,inherited.oid,'MEMBER')
 AND (inherited.rolsuper OR inherited.rolcreatedb OR inherited.rolcreaterole
 OR inherited.rolreplication OR inherited.rolbypassrls))
 AND NOT EXISTS(SELECT FROM pg_namespace n WHERE n.nspname='hp_snapshot_retention'
 AND pg_has_role(r.oid,n.nspowner,'MEMBER'))
 AND current_setting('transaction_isolation')='read committed'
 AND current_setting('session_replication_role')='origin') FROM pg_roles r WHERE r.rolname=session_user"""
CATALOG_SQL = """WITH wanted(name) AS (VALUES
 ('relation'),('preparation_relation'),('reference_preparation_relation'),('legacy_archive_relation')),
 retained AS (SELECT n.*,r.rolcanlogin,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls
 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname='hp_snapshot_retention'),
 ledgers AS (SELECT c.* FROM pg_class c JOIN retained n ON n.oid=c.relnamespace JOIN wanted w ON w.name=c.relname),
 projection AS (SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='mrf' AND c.relname='nucc_retained_relation_claims')
SELECT jsonb_build_object(
 'namespace',(SELECT jsonb_build_object('oid',oid::bigint,'owner',nspowner::bigint,
 'safe',NOT (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)) FROM retained),
 'ledgers',COALESCE((SELECT jsonb_agg(jsonb_build_object('name',c.relname,'oid',c.oid::bigint,
 'owner',c.relowner::bigint,'safe',c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND NOT EXISTS(SELECT FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid)
 AND NOT EXISTS(SELECT FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal)
 AND EXISTS(SELECT FROM pg_attribute a JOIN pg_index i ON i.indrelid=a.attrelid
 WHERE a.attrelid=c.oid AND a.attname='relation_oid' AND a.attnum>0 AND NOT a.attisdropped
 AND a.atttypid='bigint'::regtype AND a.attnotnull AND i.indisunique AND i.indisvalid AND i.indisready
 AND i.indislive AND i.indimmediate AND i.indnkeyatts=1 AND i.indexprs IS NULL AND i.indpred IS NULL
 AND i.indkey[0]=a.attnum)) ORDER BY c.relname) FROM ledgers c),'[]'::jsonb),
 'view',(SELECT jsonb_build_object('oid',c.oid::bigint,'owner',c.relowner::bigint,
 'definition',CASE WHEN c.relkind='v' THEN pg_get_viewdef(c.oid,false) END,
 'safe',c.relkind='v' AND c.relpersistence='p' AND NOT c.relispartition
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND (SELECT array_agg(o ORDER BY o) FROM unnest(c.reloptions) o)=ARRAY['security_barrier=true','security_invoker=false']::text[]
 AND NOT EXISTS(SELECT FROM pg_trigger t WHERE t.tgrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_policy p WHERE p.polrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid)
 AND (SELECT count(*)=1 AND bool_and(w.rulename='_RETURN' AND w.ev_type='1' AND w.is_instead)
 FROM pg_rewrite w WHERE w.ev_class=c.oid),
 'columns',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),
 a.attidentity,a.attgenerated,a.attisdropped,a.attacl::text) ORDER BY a.attnum)
 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0),'[]'::jsonb),
 'acl',COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable)
 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a),'[]'::jsonb),
 'dependencies',COALESCE((SELECT jsonb_agg(oid::bigint ORDER BY oid) FROM (
 SELECT DISTINCT d.refobjid AS oid FROM pg_rewrite w JOIN pg_depend d ON d.classid='pg_rewrite'::regclass
 AND d.objid=w.oid WHERE w.ev_class=c.oid AND d.refclassid='pg_class'::regclass AND d.refobjid<>c.oid) x),'[]'::jsonb),
 'other_dependencies',EXISTS(SELECT FROM pg_rewrite w JOIN pg_depend d ON d.classid='pg_rewrite'::regclass
 AND d.objid=w.oid WHERE w.ev_class=c.oid AND d.refclassid<>'pg_class'::regclass)) FROM projection c))"""


class NuccRetentionReadError(RuntimeError):
    """The negative claim projection or its original caller is unqualified."""


def decoded(value):
    """Decode only the standard native JSON representation."""
    return json.loads(value) if isinstance(value, str) else value


def _require(condition, code):
    if not condition:
        raise NuccRetentionReadError("NUCC retention " + code)


def _normalized_definition(value):
    return " ".join(value.strip().removesuffix(";").split()) if isinstance(value, str) else None


def retention_catalog(captured):
    """Allow genuine total absence only; installed malformed/partial storage refuses."""
    captured = decoded(captured)
    _require(type(captured) is dict, "catalogue missing")
    namespace, ledgers = captured.get("namespace"), captured.get("ledgers")
    if namespace is None and ledgers == [] and captured.get("view") is None:
        return None
    _require(type(namespace) is dict and namespace.get("safe") is True, "owner differs")
    _require(type(ledgers) is list and len(ledgers) == 4, "catalogue incomplete")
    _require({item["name"] for item in ledgers} == set(LEDGERS), "catalogue incomplete")
    _require(
        all(item["safe"] is True and item["owner"] == namespace["owner"] for item in ledgers)
        and len({item["oid"] for item in ledgers}) == 4,
        "catalogue differs",
    )
    return namespace["owner"], tuple(sorted(item["oid"] for item in ledgers))


def validate_projection(captured, *, builder_oid, previous=None):
    """Attest a native fixed UNION projection, never a caller-supplied receipt."""
    captured = decoded(captured)
    authority = retention_catalog(captured)
    _require(authority is not None, "catalogue missing")
    owner, ledger_oids = authority
    view = captured.get("view")
    _require(type(view) is dict and view.get("safe") is True and view.get("owner") == owner, "view differs")
    _require(_normalized_definition(view["definition"]) == CLAIMS_SQL, "definition differs")
    _require(view["columns"] == [["relation_oid", "bigint", "", "", False, None]], "column differs")
    _require(tuple(view["dependencies"]) == ledger_oids and view["other_dependencies"] is False, "dependencies differ")
    expected_acl_entries = {(owner, owner, privilege, False) for privilege in OWNER_PRIVILEGES}
    expected_acl_entries.add((builder_oid, owner, "SELECT", False))
    _require(
        {tuple(item) for item in view["acl"]} == expected_acl_entries and len(view["acl"]) == len(expected_acl_entries),
        "ACL differs",
    )
    identity = (view["oid"], owner, ledger_oids, view["definition"], tuple(tuple(item) for item in view["acl"]))
    _require(previous is None or identity == previous, "view identity changed")
    return identity


@asynccontextmanager
async def catalog_search_path(session):
    """Savepoint rollback restores settings even when inspection fails or is cancelled."""
    async with session.begin_nested():
        original = await session.scalar(text("SELECT current_setting('search_path')"))
        await session.execute(text("SELECT set_config('search_path','pg_catalog,pg_temp',true)"))
        yield
        await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": original})


async def require_unretained_nucc_oids(session, relation_oids):
    """Use a fresh original-Builder RC statement while its incumbent AEX pins remain held."""
    _require(session.in_transaction(), "original transaction required")
    _require(
        type(relation_oids) in (list, tuple)
        and 1 <= len(relation_oids) <= 2
        and all(type(oid) is int and 0 < oid < 2**32 for oid in relation_oids),
        "OID scope differs",
    )
    actor = decoded(await session.scalar(text(_ACTOR_SQL)))
    _require(type(actor) is dict and actor.get("safe") is True, "original Builder RC required")
    async with catalog_search_path(session):
        captured = decoded(await session.scalar(text(CATALOG_SQL)))
        if retention_catalog(captured) is None:
            return
        identity = validate_projection(captured, builder_oid=actor["oid"])
        await session.execute(text(f"LOCK TABLE ONLY {VIEW} IN ACCESS SHARE MODE NOWAIT"))
        validate_projection(await session.scalar(text(CATALOG_SQL)), builder_oid=actor["oid"], previous=identity)
        claimed = await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {VIEW} WHERE relation_oid=ANY(CAST(:oids AS bigint[])))"),
            {"oids": sorted(set(relation_oids))},
        )
        _require(claimed is False, "protected claim exists or is uncertain")
