# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One owner-authorized read-access successor for an immutable bootstrap."""

import hashlib
import json
import re
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_bootstrap_sources import NetworkBootstrapSourceError
from process.network_membership_writer_closure import _NATIVE_RUNTIME_ROLE_UNSAFE_SQL

_CONTRACT = "network-bootstrap-read-access/v1"
_FIELDS = {"contract", "bootstrap_sha256", "reader", "reader_oid", "roles_sha256", "native_sha256", "before", "after"}


class BootstrapReadAccessError(NetworkBootstrapSourceError):
    """A bootstrap's exact additional reader or native proof differs."""


def _require(condition):
    if not condition:
        raise BootstrapReadAccessError("Bootstrap read-access successor is unavailable or differs")


def _namespace(receipt):
    return "network_bootstrap_access_" + UUID(receipt.specification.bootstrap_id).hex


def _history_namespace(proof_sha256):
    return "network_bootstrap_prior_" + proof_sha256[:32]


def _digest(value):
    from process.network_bootstrap_sources import _digest as digest

    return digest(value)


_ACL_SQL = """WITH entries AS (
 SELECT 'schema'::text AS kind,n.oid::bigint AS oid,0 AS column_number,a.*
 FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
 WHERE n.oid=$1::oid
 UNION ALL SELECT 'table',c.oid::bigint,0,a.* FROM pg_class c
 CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 WHERE c.relnamespace=$1::oid AND c.relkind='r'
 UNION ALL SELECT 'column',c.oid::bigint,att.attnum,a.* FROM pg_class c
 JOIN pg_attribute att ON att.attrelid=c.oid CROSS JOIN LATERAL aclexplode(att.attacl) a
 WHERE c.relnamespace=$1::oid
 UNION ALL SELECT 'default',d.oid::bigint,0,a.* FROM pg_default_acl d
 CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclnamespace=$1::oid
) SELECT kind,oid,column_number,grantor::bigint,grantee::bigint,privilege_type,is_grantable
 FROM entries ORDER BY kind,oid,column_number,grantor,grantee,privilege_type,is_grantable"""

_ROLE_SQL = """WITH RECURSIVE connected(oid) AS (
 SELECT $1::oid UNION SELECT CASE WHEN m.roleid=c.oid THEN m.member ELSE m.roleid END
 FROM connected c JOIN pg_auth_members m ON c.oid IN (m.roleid,m.member)
) SELECT jsonb_build_object(
 'roles',(SELECT jsonb_agg(jsonb_build_array(r.oid,r.rolname,r.rolcanlogin,r.rolinherit,r.rolsuper,
 r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,r.rolconfig) ORDER BY r.oid)
 FROM pg_roles r JOIN connected c USING(oid)),
 'memberships',(SELECT coalesce(jsonb_agg(to_jsonb(m) ORDER BY m.oid),'[]'::jsonb)
 FROM pg_auth_members m WHERE m.roleid IN (SELECT oid FROM connected)))::text"""

_NATIVE_SQL = """WITH heaps AS (SELECT * FROM pg_class WHERE relnamespace=$1::oid AND relkind='r'),
 attributes AS (SELECT a.* FROM pg_attribute a JOIN heaps h ON h.oid=a.attrelid
 WHERE a.attnum>0 AND NOT a.attisdropped),
 indexes AS (SELECT i.* FROM pg_index i JOIN heaps h ON h.oid=i.indrelid),
 operators AS (SELECT DISTINCT op.* FROM indexes i CROSS JOIN unnest(i.indclass) key(oid)
 JOIN pg_opclass op ON op.oid=key.oid),
 constraints AS (SELECT k.* FROM pg_constraint k WHERE k.conrelid IN (SELECT oid FROM heaps)
 OR k.confrelid IN (SELECT oid FROM heaps)),
 defaults AS (SELECT d.* FROM pg_attrdef d JOIN heaps h ON h.oid=d.adrelid),
 objects(classid,objid) AS (
 SELECT 'pg_constraint'::regclass,oid FROM constraints
 UNION SELECT 'pg_attrdef'::regclass,oid FROM defaults
 UNION SELECT 'pg_class'::regclass,indexrelid FROM indexes),
 dependencies AS (SELECT d.* FROM objects x JOIN pg_depend d ON (d.classid,d.objid)=(x.classid,x.objid)),
 types AS (SELECT DISTINCT t.* FROM attributes a JOIN pg_type t ON t.oid=a.atttypid OR t.oid=
 (SELECT typelem FROM pg_type WHERE oid=a.atttypid))
SELECT jsonb_build_object(
 'relations',(SELECT jsonb_agg(jsonb_build_array(c.oid,c.relname,c.relowner,c.relkind,c.relam,c.relpersistence,
 c.relrowsecurity,c.relforcerowsecurity,c.relhastriggers,c.reloptions) ORDER BY c.oid)
 FROM pg_class c WHERE c.relnamespace=$1::oid),
 'columns',(SELECT jsonb_agg(jsonb_build_array(attrelid,attnum,attname,atttypid,atttypmod,attcollation,
 attnotnull,attidentity,attgenerated) ORDER BY attrelid,attnum) FROM attributes),
 'constraints',(SELECT jsonb_agg(to_jsonb(k) ORDER BY oid) FROM constraints k),
 'defaults',(SELECT jsonb_agg(to_jsonb(d) ORDER BY oid) FROM defaults d),
 'indexes',(SELECT jsonb_agg(to_jsonb(i) ORDER BY indexrelid) FROM indexes i),
 'types',(SELECT jsonb_agg(to_jsonb(t) ORDER BY oid) FROM types t),
 'varchar_cast',(SELECT to_jsonb(p) FROM pg_proc p WHERE p.oid=669),
 'varchar_support',(SELECT to_jsonb(p) FROM pg_proc p WHERE p.oid=(SELECT prosupport FROM pg_proc WHERE oid=669)),
 'operators',(SELECT jsonb_agg(to_jsonb(op) ORDER BY oid) FROM operators op),
 'operator_functions',(SELECT jsonb_agg(to_jsonb(p) ORDER BY oid) FROM pg_amproc p
 WHERE p.amprocfamily IN (SELECT opcfamily FROM operators)),
 'operator_members',(SELECT jsonb_agg(to_jsonb(op) ORDER BY oid) FROM pg_amop op
 WHERE op.amopfamily IN (SELECT opcfamily FROM operators)),
 'dependencies',(SELECT jsonb_agg(to_jsonb(d) ORDER BY classid,objid,objsubid,refclassid,refobjid,refobjsubid,deptype)
 FROM dependencies d))::text AS document,
 NOT EXISTS(SELECT FROM attributes WHERE attidentity<>'' OR attgenerated<>'')
 AND NOT EXISTS(SELECT FROM types WHERE oid>=16384 OR typnamespace<>'pg_catalog'::regnamespace OR typtype<>'b')
 AND NOT EXISTS(SELECT FROM types WHERE greatest(typinput::oid,typoutput::oid,typreceive::oid,typsend::oid,
 typmodin::oid,typmodout::oid,typsubscript::oid,typanalyze::oid)>=16384)
 AND NOT EXISTS(SELECT FROM operators WHERE oid>=16384 OR opcmethod<>403)
 AND NOT EXISTS(SELECT FROM pg_amproc p WHERE p.amprocfamily IN (SELECT opcfamily FROM operators) AND p.amproc::oid>=16384)
 AND NOT EXISTS(SELECT FROM pg_amop op JOIN pg_operator p ON p.oid=op.amopopr
 WHERE op.amopfamily IN (SELECT opcfamily FROM operators) AND (p.oid>=16384 OR p.oprcode::oid>=16384))
 AND NOT EXISTS(SELECT FROM heaps h JOIN pg_am am ON am.oid=h.relam WHERE am.oid>=16384 OR am.amname<>'heap')
 AND NOT EXISTS(SELECT FROM attributes a JOIN pg_collation c ON c.oid=a.attcollation
 WHERE c.oid>=16384 OR c.collnamespace<>'pg_catalog'::regnamespace)
 AND NOT EXISTS(SELECT FROM constraints WHERE contype NOT IN ('p','n') OR NOT convalidated OR condeferrable)
 AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class IN (SELECT oid FROM heaps))
 AND EXISTS(SELECT FROM pg_proc p WHERE p.oid=669 AND p.pronamespace='pg_catalog'::regnamespace
 AND p.prosrc='varchar' AND p.probin IS NULL AND NOT p.prosecdef AND p.prorettype=1043
 AND p.proconfig IS NULL AND p.prosqlbody IS NULL AND NOT p.proretset
 AND EXISTS(SELECT FROM pg_proc support WHERE support.oid=p.prosupport AND support.oid<16384
 AND support.pronamespace='pg_catalog'::regnamespace AND support.prosrc='varchar_support'
 AND support.prolang=p.prolang AND support.prosupport=0 AND NOT support.prosecdef
 AND support.proconfig IS NULL AND support.prosqlbody IS NULL AND support.probin IS NULL
 AND support.prorettype=2281 AND support.proargtypes='2281'::oidvector)
 AND p.proargtypes='1043 23 16'::oidvector AND p.prolang=(SELECT oid FROM pg_language WHERE lanname='internal'))
 AND NOT EXISTS(SELECT FROM dependencies d WHERE
 (d.refclassid='pg_proc'::regclass AND d.refobjid>=16384)
 OR (d.refclassid='pg_operator'::regclass AND d.refobjid>=16384)) AS safe"""

_SERIAL_SQL = """SELECT d.oid,seq.oid AS sequence_oid,
 jsonb_build_array(seq.oid,seq.relnamespace,seq.relname,seq.relowner,seq.relkind,seq.relpersistence)::text AS sequence,
 to_jsonb(native)::text AS nextval,
 h.relname='npi' AND a.attname='npi' AND a.atttypid=20 AND a.attidentity='' AND a.attgenerated=''
 AND seq.relkind='S' AND seq.relpersistence='p'
 AND pg_get_expr(d.adbin,d.adrelid)=format('nextval(%L::regclass)',seq.oid::regclass::text)
 AND native.oid=1574 AND native.pronamespace='pg_catalog'::regnamespace AND native.proname='nextval'
 AND native.prosrc='nextval_oid' AND native.probin IS NULL AND native.prokind='f'
 AND native.prosupport=0 AND native.proconfig IS NULL AND native.prosqlbody IS NULL
 AND NOT native.proretset AND native.provariadic=0 AND native.proisstrict
 AND native.provolatile='v' AND native.proparallel='u'
 AND native.prolang=(SELECT oid FROM pg_language WHERE lanname='internal') AND NOT native.prosecdef
 AND native.prorettype=20 AND native.proargtypes='2205'::oidvector AND native.pronargs=1
 AND (SELECT count(*) FROM pg_depend dep WHERE dep.classid='pg_attrdef'::regclass AND dep.objid=d.oid)=2
 AND EXISTS(SELECT FROM pg_depend dep WHERE dep.classid='pg_attrdef'::regclass AND dep.objid=d.oid
 AND dep.refclassid='pg_class'::regclass AND dep.refobjid=h.oid AND dep.refobjsubid=a.attnum AND dep.deptype='a')
 AND EXISTS(SELECT FROM pg_depend dep WHERE dep.classid='pg_attrdef'::regclass AND dep.objid=d.oid
 AND dep.refclassid='pg_class'::regclass AND dep.refobjid=seq.oid AND dep.refobjsubid=0 AND dep.deptype='n') AS safe
 FROM pg_attrdef d JOIN pg_class h ON h.oid=d.adrelid
 JOIN pg_attribute a ON a.attrelid=h.oid AND a.attnum=d.adnum
 JOIN pg_class seq ON seq.oid=$2::oid CROSS JOIN pg_proc native
 WHERE d.oid=$1::oid AND native.oid=1574"""


async def _dormant_sequence(connection, receipt, document, reader_oid, *, roles=None):
    """The copied NPI serial default is dormant: no reader may execute its sequence."""
    own_objects = {str(oid) for _, oid in (*receipt.relation_oids, *receipt.index_oids)}
    external_dependencies = [
        row
        for row in document["dependencies"]
        if str(row["refclassid"]) == "1259" and str(row["refobjid"]) not in own_objects
    ]
    _literal_defaults(document, str(external_dependencies[0]["objid"]) if len(external_dependencies) == 1 else None)
    if not external_dependencies:
        return None
    _require(len(external_dependencies) == 1 and str(external_dependencies[0]["classid"]) == "2604")
    dependency = external_dependencies[0]
    serial = await connection.fetchrow(_SERIAL_SQL, int(dependency["objid"]), int(dependency["refobjid"]))
    _require(serial is not None and serial["safe"] is True)
    if roles is None:
        roles = json.loads(await connection.fetchval(_ROLE_SQL, reader_oid))["roles"]
    await _component_read_only(connection, receipt, roles, sequence_oid=serial["sequence_oid"])
    return {"sequence": json.loads(serial["sequence"]), "nextval": json.loads(serial["nextval"])}


async def _component_read_only(connection, receipt, roles, *, sequence_oid=None):
    """Every connected actor must preserve the retained source's owner and read boundary."""
    unsafe = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles r CROSS JOIN (SELECT $2::oid AS owner_oid) s "
        "WHERE r.oid=ANY($1::oid[]) AND ("
        + _NATIVE_RUNTIME_ROLE_UNSAFE_SQL
        + " OR has_schema_privilege(r.oid,$5::oid,'CREATE,USAGE WITH GRANT OPTION') "
        "OR EXISTS(SELECT FROM unnest($3::oid[]) source(oid) WHERE "
        "has_table_privilege(r.oid,source.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN,SELECT WITH GRANT OPTION') "
        "OR has_any_column_privilege(r.oid,source.oid,'INSERT,UPDATE,REFERENCES')) "
        "OR ($4::oid IS NOT NULL AND has_sequence_privilege(r.oid,$4::oid,'SELECT,USAGE,UPDATE'))))",
        [int(role[0]) for role in roles],
        receipt.owner_oid,
        [oid for _, oid in receipt.relation_oids],
        sequence_oid,
        receipt.schema_oid,
    )
    _require(unsafe is False)


def _literal_defaults(document, serial_oid):
    """Only literal model defaults and the separately authenticated NPI serial call are admitted."""
    for default in document["defaults"] or []:
        nodes = set(re.findall(r"\{([A-Z_]+)", default["adbin"]))
        functions = re.findall(r":funcid (\d+)", default["adbin"])
        _require(nodes and nodes <= {"CONST", "ARRAYEXPR", "FUNCEXPR"})
        if str(default["oid"]) == serial_oid:
            _require(nodes == {"CONST", "FUNCEXPR"} and functions == ["1574"])
        else:
            _require(set(functions) <= {"669"})


async def _acl(connection, receipt):
    return [list(row) for row in await connection.fetch(_ACL_SQL, receipt.schema_oid)]


async def _reader(connection, receipt, reader):
    _identifier(reader)
    _require(reader not in receipt.runtime_roles and reader != receipt.owner_role)
    row = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", reader)
    _require(
        row is not None
        and row["rolcanlogin"]
        and not any(row[key] for key in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"))
    )
    unsafe = await connection.fetchval(
        "SELECT " + _NATIVE_RUNTIME_ROLE_UNSAFE_SQL + " FROM pg_roles r CROSS JOIN "
        "(SELECT $2::oid AS owner_oid) s WHERE r.oid=$1::oid",
        row["oid"],
        receipt.owner_oid,
    )
    _require(unsafe is False)
    _require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM unnest($2::text[]) original(name) "
            "WHERE pg_has_role($1::oid,original.name::name,'MEMBER'))",
            row["oid"],
            list(receipt.runtime_roles),
        )
    )
    component = json.loads(await connection.fetchval(_ROLE_SQL, row["oid"]))
    await _component_read_only(connection, receipt, component["roles"])
    return row["oid"], _digest(component)


async def _base_catalog(connection, receipt, *, api_read=False):
    from process.network_address_artifact import _require_api_read_context
    from process.network_bootstrap_sources import _catalog, _decode, _schema_name

    await _require_api_read_context(connection, api_read)
    schema_name = _schema_name(receipt.specification)
    stored = await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", receipt.schema_oid)
    _require(_decode(stored) == receipt)
    tables = ",".join(f"{_identifier(schema_name)}.{_identifier(name)}" for name, _ in receipt.relation_oids)
    if not api_read:
        await connection.execute(f"LOCK TABLE {tables} IN ACCESS SHARE MODE NOWAIT")
    catalog = await _catalog(connection, schema_name)
    _require(catalog["namespace"] == {"oid": receipt.schema_oid, "nspowner": receipt.owner_oid})
    _require(tuple(sorted(catalog["heaps"].items())) == receipt.relation_oids)
    _require(all(row["relowner"] == receipt.owner_oid for row in catalog["relations"]))
    _require(
        tuple((row["relname"], row["oid"]) for row in catalog["relations"] if row["relkind"] == b"i")
        == receipt.index_oids
    )
    return schema_name


async def _native(connection, receipt, reader_oid, *, api_read=False):
    from process.network_registry_cms_prepared_address import _require_prepared_copy_key

    schema_name = await _base_catalog(connection, receipt, api_read=api_read)
    for name, oid in receipt.relation_oids:
        await _require_prepared_copy_key(
            connection,
            f"{_identifier(schema_name)}.{_identifier(name)}",
            oid,
            ("npi" if name == "npi" else "location_key",),
        )
    native = await connection.fetchrow(_NATIVE_SQL, receipt.schema_oid)
    _require(native["safe"] is True)
    document = json.loads(native["document"])
    document["dormant_sequence"] = await _dormant_sequence(connection, receipt, document, reader_oid)
    return _digest(document)


async def _proof(connection, receipt, *, namespace=None):
    row = await connection.fetchrow(
        "SELECT n.oid,n.nspowner,obj_description(n.oid,'pg_namespace') AS document,"
        "NOT EXISTS(SELECT FROM aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a "
        "WHERE a.grantee<>n.nspowner) AND NOT EXISTS(SELECT FROM pg_depend "
        "WHERE refclassid='pg_namespace'::regclass AND refobjid=n.oid) "
        "AND NOT EXISTS(SELECT FROM pg_default_acl WHERE defaclnamespace=n.oid) AS closed "
        "FROM pg_namespace n WHERE n.nspname=$1",
        namespace or _namespace(receipt),
    )
    if row is None:
        return None
    _require(row["nspowner"] == receipt.owner_oid and row["closed"] is True)
    _require(type(row["document"]) is str and len(row["document"].encode()) <= 32768)
    try:
        proof = json.loads(row["document"])
    except ValueError, TypeError:
        raise BootstrapReadAccessError("Bootstrap read-access proof is invalid") from None
    _require(type(proof) is dict and set(proof) == _FIELDS and proof["contract"] == _CONTRACT)
    _require(proof["bootstrap_sha256"] == _digest(receipt.as_dict()))
    return proof


def _extended_acl(receipt, before, reader_oid):
    _require(type(before) is list and all(type(row) is list and len(row) == 7 for row in before))
    _require(not any(row[4] == reader_oid for row in before))
    additions = [["schema", receipt.schema_oid, 0, receipt.owner_oid, reader_oid, "USAGE", False]]
    additions.extend(
        ["table", oid, 0, receipt.owner_oid, reader_oid, "SELECT", False] for _, oid in receipt.relation_oids
    )
    return sorted(before + additions)


async def bootstrap_read_roles(connection, receipt, *, api_read=False):
    """Accept only the original ACLs or the single native-bound read successor."""
    from process.network_custom_address_source import NetworkCustomAddressSourceError, _closed

    await _base_catalog(connection, receipt, api_read=api_read)
    try:
        await _closed(connection, receipt.schema_oid, receipt.owner_oid, receipt.runtime_roles, None)
    except NetworkCustomAddressSourceError:
        proof = await _proof(connection, receipt)
        _require(proof is not None)
        return await _successor_roles(connection, receipt, proof, api_read=api_read)
    # Restored original authority never depends on a retired successor actor.
    return receipt.runtime_roles


async def _successor_roles(connection, receipt, proof, *, allow_original=False, api_read=False):
    from process.network_custom_address_source import _closed

    reader_oid, roles_sha256 = await _reader(connection, receipt, proof["reader"])
    _require((reader_oid, roles_sha256) == (proof["reader_oid"], proof["roles_sha256"]))
    _require(await _native(connection, receipt, reader_oid, api_read=api_read) == proof["native_sha256"])
    _require(proof["after"] == _extended_acl(receipt, proof["before"], reader_oid))
    current = await _acl(connection, receipt)
    _require(current == proof["after"] or (allow_original and current == proof["before"]))
    roles = (
        receipt.runtime_roles
        if current == proof["before"]
        else tuple(sorted((*receipt.runtime_roles, proof["reader"])))
    )
    await _closed(connection, receipt.schema_oid, receipt.owner_oid, roles, None)
    return roles


async def bootstrap_pin_read_roles(connection, pin, required_roles, *, runtime_actors=(), api_read=False):
    """Preserve authenticated readers and reject unsafe input reach by output actors."""
    from process.network_bootstrap_sources import _decode
    from process.network_custom_address_source import NetworkCustomAddressSourceError

    try:
        receipt = _decode(
            await connection.fetchval(
                "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", pin.schema_name
            )
        )
        _require(pin in (receipt.address_source, receipt.npi_source))
        roles = await bootstrap_read_roles(connection, receipt, api_read=api_read)
        _require(set(required_roles) <= set(roles))
        actors = sorted(set((*roles, *runtime_actors))) if runtime_actors else ()
        sequence_oid = None
        if actors:
            caller = await connection.fetchval("SELECT current_user::text")
            caller_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", caller)
            native = await connection.fetchrow(_NATIVE_SQL, receipt.schema_oid)
            _require(native["safe"] is True)
            dormant = await _dormant_sequence(connection, receipt, json.loads(native["document"]), caller_oid, roles=())
            sequence_oid = int(dormant["sequence"][0]) if dormant is not None else None
            if api_read:
                actors = sorted(set((*actors, caller)))
        for actor in actors:
            _identifier(actor)
            actor_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", actor)
            _require(actor_oid is not None)
            component = json.loads(await connection.fetchval(_ROLE_SQL, actor_oid))
            reachable = await connection.fetchval(
                "SELECT array_agg(r.oid) FROM pg_roles r WHERE r.oid=ANY($2::oid[]) AND "
                "(r.oid=$1::oid OR pg_has_role($1::oid,r.oid,'MEMBER') OR pg_has_role($1::oid,r.oid,'USAGE') "
                "OR pg_has_role($1::oid,r.oid,'SET'))",
                actor_oid,
                [int(role[0]) for role in component["roles"]],
            )
            await _component_read_only(
                connection,
                receipt,
                [role for role in component["roles"] if int(role[0]) in reachable],
                sequence_oid=sequence_oid,
            )
        return roles
    except NetworkBootstrapSourceError as error:
        raise NetworkCustomAddressSourceError("Retained bootstrap read access differs") from error


async def _operator(connection, receipt):
    from process.network_bootstrap_sources import _transaction

    await _transaction(connection)
    _require(await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", receipt.owner_role))
    original_role = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"SET LOCAL ROLE {_identifier(receipt.owner_role)}")
    tables = ",".join(
        f"{_identifier(receipt.address_source.schema_name)}.{_identifier(name)}" for name, _ in receipt.relation_oids
    )
    await connection.execute(f"LOCK TABLE {tables} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    return original_role


async def _grant(connection, receipt, reader, *, revoke=False):
    action, direction = ("REVOKE", "FROM") if revoke else ("GRANT", "TO")
    namespace, role = _identifier(receipt.address_source.schema_name), _identifier(reader)
    for name, _ in receipt.relation_oids:
        await connection.execute(f"{action} SELECT ON {namespace}.{_identifier(name)} {direction} {role}")
    await connection.execute(f"{action} USAGE ON SCHEMA {namespace} {direction} {role}")


async def _create_proof(connection, receipt, reader, original_role):
    from process.network_bootstrap_sources import verify_network_bootstrap_sources

    await verify_network_bootstrap_sources(connection, receipt)
    reader_oid, roles_sha256 = await _reader(connection, receipt, reader)
    before = await _acl(connection, receipt)
    proof_by_name = {
        "contract": _CONTRACT,
        "bootstrap_sha256": _digest(receipt.as_dict()),
        "reader": reader,
        "reader_oid": reader_oid,
        "roles_sha256": roles_sha256,
        "native_sha256": await _native(connection, receipt, reader_oid),
        "before": before,
        "after": _extended_acl(receipt, before, reader_oid),
    }
    await _grant(connection, receipt, reader)
    _require(await _acl(connection, receipt) == proof_by_name["after"])
    namespace = _identifier(_namespace(receipt))
    await connection.execute(f"SET LOCAL ROLE {original_role}")
    await connection.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {_identifier(receipt.owner_role)}")
    await connection.execute(f"SET LOCAL ROLE {_identifier(receipt.owner_role)}")
    encoded = json.dumps(proof_by_name, sort_keys=True, separators=(",", ":"))
    _require(len(encoded.encode()) <= 32768)
    literal = await connection.fetchval("SELECT quote_literal($1::text)", encoded)
    await connection.execute(f"COMMENT ON SCHEMA {namespace} IS {literal}")
    return proof_by_name


async def install_bootstrap_read_access(connection, receipt, *, reader_role):
    """Atomically append one protected proof and its exact read-only grants."""
    async with connection.transaction():
        original_role = await _operator(connection, receipt)
        proof = await _proof(connection, receipt)
        if proof is None:
            proof = await _create_proof(connection, receipt, reader_role, original_role)
        else:
            _require(proof["reader"] == reader_role)
            await _successor_roles(connection, receipt, proof, allow_original=True)
            await _grant(connection, receipt, reader_role)
        _require(reader_role in await bootstrap_read_roles(connection, receipt))
        await connection.execute(f"SET LOCAL ROLE {original_role}")
        return proof


async def _renewal_predecessor(connection, receipt, proof, locked, expected_proof_sha256):
    if hashlib.sha256(locked.encode()).hexdigest() == expected_proof_sha256:
        return proof, locked
    history_name = _history_namespace(expected_proof_sha256)
    predecessor = await _proof(connection, receipt, namespace=history_name)
    document = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", history_name
    )
    _require(
        predecessor is not None
        and type(document) is str
        and hashlib.sha256(document.encode()).hexdigest() == expected_proof_sha256
    )
    return predecessor, document


async def renew_bootstrap_read_access(connection, receipt, *, expected_proof_sha256, expected_roles_sha256):
    """Original-admin CAS of the role census only, retaining the complete prior v1 proof."""
    from process.network_bootstrap_sources import _transaction, verify_network_bootstrap_sources

    for digest in (expected_proof_sha256, expected_roles_sha256):
        _require(type(digest) is str and re.fullmatch("[0-9a-f]{64}", digest) is not None)
    async with connection.transaction():
        await _transaction(connection)
        _require(
            await connection.fetchval(
                "SELECT session_user=current_user AND rolsuper FROM pg_roles WHERE rolname=current_user"
            )
            is True
        )
        locked = await connection.fetchval(
            "SELECT d.description FROM pg_description d JOIN pg_namespace n ON n.oid=d.objoid "
            "WHERE n.nspname=$1 AND d.classoid='pg_namespace'::regclass AND d.objsubid=0 "
            "FOR UPDATE OF d NOWAIT",
            _namespace(receipt),
        )
        proof = await _proof(connection, receipt)
        _require(proof is not None and type(locked) is str and json.loads(locked) == proof)
        original_role = await _operator(connection, receipt)
        history_name = _history_namespace(expected_proof_sha256)
        predecessor, predecessor_document = await _renewal_predecessor(
            connection, receipt, proof, locked, expected_proof_sha256
        )
        proof_by_field = {**predecessor, "roles_sha256": expected_roles_sha256}
        _require(proof == predecessor or proof == proof_by_field)
        # This deliberately reuses the unchanged v1 reader/native/ACL safety checks.
        await _successor_roles(connection, receipt, proof_by_field)
        if proof != proof_by_field:
            history = await _proof(connection, receipt, namespace=history_name)
            if history is None:
                await connection.execute(f"SET LOCAL ROLE {original_role}")
                await connection.execute(
                    f"CREATE SCHEMA {_identifier(history_name)} AUTHORIZATION {_identifier(receipt.owner_role)}"
                )
                await connection.execute(f"SET LOCAL ROLE {_identifier(receipt.owner_role)}")
                literal = await connection.fetchval("SELECT quote_literal($1::text)", predecessor_document)
                await connection.execute(f"COMMENT ON SCHEMA {_identifier(history_name)} IS {literal}")
            else:
                _require(history == predecessor)
                _require(
                    await connection.fetchval(
                        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", history_name
                    )
                    == predecessor_document
                )
            literal = await connection.fetchval(
                "SELECT quote_literal($1::text)", json.dumps(proof_by_field, sort_keys=True, separators=(",", ":"))
            )
            await connection.execute(f"COMMENT ON SCHEMA {_identifier(_namespace(receipt))} IS {literal}")
        _require(await _proof(connection, receipt) == proof_by_field)
        await verify_network_bootstrap_sources(connection, receipt)
        await connection.execute(f"SET LOCAL ROLE {original_role}")
        return proof_by_field


async def revoke_bootstrap_read_access(connection, receipt, *, reader_role):
    """Restore only the original ACLs while retaining the immutable successor proof."""
    async with connection.transaction():
        original_role = await _operator(connection, receipt)
        proof = await _proof(connection, receipt)
        _require(proof is not None and proof["reader"] == reader_role)
        await bootstrap_read_roles(connection, receipt)
        if await _acl(connection, receipt) != proof["before"]:
            await _grant(connection, receipt, reader_role, revoke=True)
        _require(await _acl(connection, receipt) == proof["before"])
        _require(await bootstrap_read_roles(connection, receipt) == receipt.runtime_roles)
        await connection.execute(f"SET LOCAL ROLE {original_role}")
        return proof
