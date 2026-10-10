# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare a protected address artifact from retained sources and approved offices."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from uuid import UUID

import asyncpg

from db.models.entity_address_unified import EntityAddressUnified
from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_site_source import copy_approved_site_rows, verify_approved_site_sources
from process.network_membership_writer_closure import _NATIVE_WRITER_PRIVILEGES_SQL, _protected_owner


class NetworkCustomAddressSourceError(ValueError):
    """The exact retained recipe, source identity or writer closure differs."""


class NetworkCustomAddressUnresolved(NetworkCustomAddressSourceError):
    """An approved custom office cannot resolve to a supported exact identity."""


@dataclass(frozen=True)
class CustomAddressSourceReceipt:
    composition_id: str
    approved_revision: int
    control_schema: str
    base_source: PinnedAddressSource
    npi_source: PinnedAddressSource | None
    source_oids: tuple[int, int, int, str]
    npi_oids: tuple[int, int, int, str] | None
    owner_role: str
    runtime_roles: tuple[str, ...]
    schema_oid: int
    address_table_oid: int
    binding_table_oid: int
    office_lookup_index_oid: int
    owner_oid: int
    custom_pair_count: int
    approved_records_sha256: str
    generation_sha256: str
    address_source: PinnedAddressSource

    def as_dict(self):
        """Return the complete bounded protected schema receipt."""
        return {"component": "network_custom_address_source", "revision": 2, **asdict(self)}


def _digest(document):
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _generation(receipt):
    document = receipt.as_dict()
    document.pop("generation_sha256")
    document.pop("address_source")
    return _digest(document)


def _arguments(base_source, composition_id, approved_revision, owner_role, runtime_roles, npi_source):
    try:
        parsed = UUID(composition_id) if type(composition_id) is str else None
    except ValueError:
        parsed = None
    if parsed is None or not parsed.int or str(parsed) != composition_id:
        raise NetworkCustomAddressSourceError("Composition identity is invalid")
    if (
        type(base_source) is not PinnedAddressSource
        or type(approved_revision) is not int
        or not 0 <= approved_revision <= 9223372036854775807
    ):
        raise NetworkCustomAddressSourceError("Pinned composition input is invalid")
    if npi_source is not None and (type(npi_source) is not PinnedAddressSource or npi_source.table_name != "npi"):
        raise NetworkCustomAddressSourceError("Pinned NPI source is invalid")
    _identifier(owner_role)
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or tuple(sorted(set(runtime_roles))) != runtime_roles
    ):
        raise NetworkCustomAddressSourceError("Runtime roles must be a sorted unique bounded tuple")
    for role in runtime_roles:
        _identifier(role)
    if owner_role in runtime_roles:
        raise NetworkCustomAddressSourceError("Protected owner cannot be a runtime role")
    schema_name = "network_composition_" + parsed.hex
    if base_source.schema_name == schema_name or (npi_source is not None and npi_source.schema_name == schema_name):
        raise NetworkCustomAddressSourceError("Composition sources must be external retained relations")
    return schema_name


async def _require_transaction(connection):
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise NetworkCustomAddressSourceError("Composition requires a caller repeatable-read transaction")


async def _source_identity(connection, pinned_source, runtime_roles, *, address=False):
    catalog = await connection.fetchrow(
        """SELECT namespace.oid::bigint AS schema_oid,relation.oid::bigint AS table_oid,
        owner.oid::bigint AS owner_oid,owner.*,relation.relkind,relation.relpersistence,
        relation.relrowsecurity,relation.relforcerowsecurity,namespace.nspowner=relation.relowner AS same_owner
        FROM pg_namespace namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
        JOIN pg_roles owner ON owner.oid=relation.relowner
        WHERE namespace.nspname=$1 AND relation.relname=$2""",
        pinned_source.schema_name,
        pinned_source.table_name,
    )
    if (
        catalog is None
        or not _protected_owner(catalog)
        or not catalog["same_owner"]
        or catalog["relkind"] != b"r"
        or catalog["relpersistence"] != b"p"
        or catalog["relrowsecurity"]
        or catalog["relforcerowsecurity"]
    ):
        raise NetworkCustomAddressSourceError("Retained source is unavailable or unprotected")
    await _closed(connection, catalog["schema_oid"], catalog["owner_oid"], runtime_roles, [pinned_source.table_name])
    columns = await connection.fetch(
        "SELECT attname,atttypid::regtype::text AS type,atttypmod,attnotnull,pg_get_expr(d.adbin,d.adrelid) AS default_expr FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        catalog["table_oid"],
    )
    if address and tuple(column["attname"] for column in columns) != tuple(
        EntityAddressUnified.__table__.columns.keys()
    ):
        raise NetworkCustomAddressSourceError("Retained unified address shape differs")
    if not address and not any(column["attname"] == "npi" and column["type"] == "bigint" for column in columns):
        raise NetworkCustomAddressSourceError("Retained NPI shape differs")
    return (
        catalog["schema_oid"],
        catalog["table_oid"],
        catalog["owner_oid"],
        _digest([dict(column) for column in columns]),
    )


async def _closed(connection, schema_oid, owner_oid, runtime_roles, relation_names):
    closed = await connection.fetchval(
        "WITH closure_scope AS (SELECT $1::oid AS schema_oid,$2::oid AS owner_oid,$3::text[] AS role_names,$4::text[] AS relation_names)"
        + _NATIVE_WRITER_PRIVILEGES_SQL,
        schema_oid,
        owner_oid,
        list(runtime_roles),
        relation_names,
    )
    if closed is not True:
        raise NetworkCustomAddressSourceError("Native owner or runtime privileges are not closed")


_APPROVED_CTE = """WITH approved AS MATERIALIZED (
 SELECT * FROM {control}.registry_approved_record WHERE approved_revision=$1
   AND record_kind IN ('provider','location','membership','site_binding')
), pairs AS MATERIALIZED (
 SELECT DISTINCT member.provider_system,member.provider_id,member.location_id
 FROM approved head CROSS JOIN LATERAL jsonb_to_recordset(
   CASE WHEN head.record_kind='membership' AND head.record_json->'archived'='false'::jsonb
     THEN head.record_json->'memberships_json' ELSE '[]'::jsonb END)
   AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), custom AS MATERIALIZED (
 SELECT pair.*,location.record_json AS site,provider.record_json AS provider,
   encode(sha256(convert_to(jsonb_build_array('network_custom',pair.provider_system,pair.provider_id,pair.location_id)::text,'UTF8')),'hex') AS location_key
 FROM pairs pair JOIN approved location ON location.record_kind='location' AND location.record_key=pair.location_id::text
 LEFT JOIN approved provider ON provider.record_kind='provider' AND provider.record_key=pair.provider_id
)
"""


async def _approved_identity(connection, control, revision, *, retained=False):
    current = await connection.fetchval(f"SELECT approved_revision FROM {control}.registry_revision_control WHERE id=1")
    if not retained and current != revision:
        raise NetworkCustomAddressSourceError("Approved revision differs from the pinned recipe")
    summary = await connection.fetchrow(
        _APPROVED_CTE.format(control=control)
        + """
 SELECT (SELECT encode(sha256(convert_to(coalesce(string_agg(
   record_kind||':'||record_key||':'||record_revision||':'||custom_revision||':'||
     encode(sha256(convert_to(record_json::text,'UTF8')),'hex'),',' ORDER BY record_kind,record_key),''),'UTF8')),'hex')
   FROM approved) AS fingerprint,count(*)::bigint AS pairs,
   count(*) FILTER(WHERE custom.provider_system='npi')::bigint AS npi_pairs,
   count(*) FILTER(WHERE (custom.location_id IS NULL AND source_binding.count<>1)
     OR (custom.location_id IS NOT NULL AND source_binding.count<>0)
     OR (custom.location_id IS NOT NULL AND (
       custom.provider_system NOT IN ('manual','npi') OR coalesce(site->'archived'<>'false'::jsonb,true)
       OR (custom.provider_system='manual' AND (provider IS NULL OR coalesce(provider->'archived'<>'false'::jsonb,true)))
       OR coalesce(site->'canonical_address_json'->>'identity_key' NOT LIKE 'v2|%|street',true)
       OR site->'canonical_address_json'->>'address_key' IS NULL)))::bigint AS unresolved
 FROM pairs pair LEFT JOIN custom USING(provider_system,provider_id,location_id)
 LEFT JOIN LATERAL (
   SELECT count(*) AS count FROM approved binding WHERE binding.record_kind='site_binding'
     AND binding.record_json->'archived'='false'::jsonb
     AND (binding.record_json->>'provider_system',binding.record_json->>'provider_id',binding.record_json->>'location_id')
       =(pair.provider_system,pair.provider_id,pair.location_id::text)
 ) source_binding ON true
 """,
        revision,
    )
    if summary["unresolved"]:
        raise NetworkCustomAddressUnresolved("Approved custom office identity is unresolved")
    return summary


async def _build_heaps(connection, schema_name, base_source, control, revision, npi_source):
    site_sources = await verify_approved_site_sources(
        connection, approved_revision=revision, control_schema=control[1:-1]
    )
    namespace = _identifier(schema_name)
    address_table = namespace + ".entity_address_unified"
    base_table = f"{_identifier(base_source.schema_name)}.{_identifier(base_source.table_name)}"
    npi_table = f"{_identifier(npi_source.schema_name)}.{_identifier(npi_source.table_name)}" if npi_source else None
    await connection.execute(f"CREATE SCHEMA {namespace}")
    await connection.execute(f"CREATE TABLE {address_table} (LIKE {base_table} INCLUDING DEFAULTS)")
    await connection.execute(f"ALTER TABLE {address_table} ADD PRIMARY KEY(location_key)")
    await connection.execute(
        f"INSERT INTO {address_table} SELECT * FROM {base_table} WHERE row_origin<>'network_custom'"
    )
    await connection.execute(f"""CREATE TABLE {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL UNIQUE,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    npi_check = (
        f"EXISTS(SELECT 1 FROM {npi_table} npi WHERE npi.npi=custom.provider_id::bigint)" if npi_table else "false"
    )
    missing = await connection.fetchval(
        _APPROVED_CTE.format(control=control)
        + f"SELECT EXISTS(SELECT 1 FROM custom WHERE provider_system='npi' AND NOT ({npi_check}))",
        revision,
    )
    if missing:
        raise NetworkCustomAddressUnresolved("Approved source NPI identity is unresolved")
    await connection.execute(
        _APPROVED_CTE.format(control=control) + _INSERT_ADDRESS_SQL.format(address_table=address_table), revision
    )
    await connection.execute(
        _APPROVED_CTE.format(control=control)
        + f"""INSERT INTO {namespace}.provider_location_binding
      SELECT provider_system,provider_id,location_id,location_key,provider_system,provider_id FROM custom""",
        revision,
    )
    await copy_approved_site_rows(connection, site_sources, schema_name)
    invalid = await connection.fetchval(f"""SELECT EXISTS(SELECT 1 FROM {namespace}.provider_location_binding binding
      LEFT JOIN {address_table} address USING(location_key,entity_type,entity_id) WHERE address.location_key IS NULL)
      OR EXISTS(SELECT 1 FROM {address_table} GROUP BY entity_type,entity_id,location_key HAVING count(*)>1)""")
    if invalid:
        raise NetworkCustomAddressSourceError("Custom address identities collide or have orphan bindings")


_INSERT_ADDRESS_SQL = """INSERT INTO {address_table} (
 entity_type,entity_id,npi,entity_name,location_key,row_origin,address_precision,checksum,type,
 first_line,second_line,city_name,state_name,postal_code,country_code,address_key,premise_key,zip5,state_code,city_norm,
 taxonomy_array,plans_network_array,procedures_array,medications_array,canonical_network_ids)
 SELECT provider_system,provider_id,CASE WHEN provider_system='npi' THEN provider_id::bigint
   ELSE (provider->>'npi')::bigint END,
 CASE WHEN provider_system='manual' THEN provider->>'display_name' ELSE 'NPI '||provider_id END,
 location_key,'network_custom','street',('x'||substr(location_key,1,16))::bit(64)::bigint,'practice',
 site->'address_json'->>'first_line',site->'address_json'->>'second_line',site->'address_json'->>'city',
 site->'address_json'->>'state',site->'address_json'->>'zip',site->'address_json'->>'country',
 (site->'canonical_address_json'->>'address_key')::uuid,(site->'canonical_address_json'->>'premise_key')::uuid,
 site->'canonical_address_json'->>'zip5',site->'canonical_address_json'->>'state_code',site->'canonical_address_json'->>'city_norm',
 '{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[] FROM custom
"""


async def _freeze(connection, schema_name, owner_role, runtime_roles):
    namespace, owner = _identifier(schema_name), _identifier(owner_role)
    role = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", owner_role)
    if not _protected_owner(role) or not await connection.fetchval(
        "SELECT pg_has_role(current_user,$1::name,'SET')", owner_role
    ):
        raise NetworkCustomAddressSourceError("Protected composition owner is unavailable")
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    for table in ("entity_address_unified", "provider_location_binding"):
        await connection.execute(f"ALTER TABLE {namespace}.{table} OWNER TO {owner}")
    acl_rows = await connection.fetch(
        """SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS grantee
      FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid
      CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))||coalesce(c.relacl,acldefault('r',c.relowner))) a
      LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE n.nspname=$1 AND a.grantee<>$2::oid""",
        schema_name,
        role["oid"],
    )
    for acl in acl_rows:
        await connection.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM {acl['grantee']} CASCADE")
        await connection.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA {namespace} FROM {acl['grantee']} CASCADE")
    for runtime in runtime_roles:
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(runtime)}")
        await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {_identifier(runtime)}")
    return role["oid"]


async def _artifact_identity(connection, schema_name, owner_oid, runtime_roles):
    if not _protected_owner(await connection.fetchrow("SELECT * FROM pg_roles WHERE oid=$1::oid", owner_oid)):
        raise NetworkCustomAddressSourceError("Composition protected owner differs")
    schema_oid = await connection.fetchval(
        "SELECT oid::bigint FROM pg_namespace WHERE nspname=$1 AND nspowner=$2::oid", schema_name, owner_oid
    )
    relations = await connection.fetch(
        """SELECT c.oid::bigint,c.relname,c.relkind,c.relowner::bigint,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,c.relhastriggers,i.indrelid::bigint,i.indisvalid,i.indisready
      FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid WHERE c.relnamespace=$1::oid ORDER BY c.relname LIMIT 9""",
        schema_oid,
    )
    heap_oids_by_name = {relation["relname"]: relation["oid"] for relation in relations if relation["relkind"] == b"r"}
    if (
        schema_oid is None
        or set(heap_oids_by_name) != {"entity_address_unified", "provider_location_binding"}
        or len(relations) > 8
        or any(
            relation["relowner"] != owner_oid
            or relation["relpersistence"] != b"p"
            or relation["relrowsecurity"]
            or relation["relforcerowsecurity"]
            or relation["relhastriggers"]
            or relation["relkind"] not in {b"r", b"i"}
            or (
                relation["relkind"] == b"i"
                and (
                    relation["indrelid"] not in heap_oids_by_name.values()
                    or not relation["indisvalid"]
                    or not relation["indisready"]
                )
            )
            for relation in relations
        )
    ):
        raise NetworkCustomAddressSourceError("Composition native relation identity differs")
    if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=$1::oid)", schema_oid):
        raise NetworkCustomAddressSourceError("Composition contains unsupported stored routines")
    await _binding_shape(connection, heap_oids_by_name["provider_location_binding"])
    lookup_oid = await _office_lookup_identity(connection, schema_oid, heap_oids_by_name["entity_address_unified"])
    await _closed(connection, schema_oid, owner_oid, runtime_roles, None)
    return (
        schema_oid,
        heap_oids_by_name["entity_address_unified"],
        heap_oids_by_name["provider_location_binding"],
        lookup_oid,
    )


async def _office_lookup_identity(connection, schema_oid, address_oid):
    index = await connection.fetchrow(
        """SELECT c.oid::bigint AS oid,am.amname,i.indrelid::bigint,i.indisvalid,i.indisready,
        i.indisunique,i.indnkeyatts,i.indnatts,i.indexprs IS NULL AS plain,i.indpred IS NULL AS complete,
        ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY key(attnum,ordinal)
          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum ORDER BY ordinal) AS columns
        FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_am am ON am.oid=c.relam
        WHERE c.relnamespace=$1::oid AND c.relname='entity_address_unified_network_office_lookup'""",
        schema_oid,
    )
    if index is None or tuple(
        index[field]
        for field in (
            "amname",
            "indrelid",
            "indisvalid",
            "indisready",
            "indisunique",
            "indnkeyatts",
            "indnatts",
            "plain",
            "complete",
            "columns",
        )
    ) != ("btree", address_oid, True, True, False, 2, 2, True, True, ["npi", "address_key"]):
        raise NetworkCustomAddressSourceError("Composition exact office lookup index differs")
    return index["oid"]


async def _prepare_office_lookup(connection, schema_name):
    address = _identifier(schema_name) + ".entity_address_unified"
    await connection.execute(
        f"CREATE INDEX entity_address_unified_network_office_lookup ON {address} USING btree(npi,address_key)"
    )
    await connection.execute(f"ANALYZE {address}(npi,address_key)")


async def _binding_shape(connection, binding_oid):
    columns = await connection.fetch(
        "SELECT attname,atttypid::regtype::text AS type,atttypmod,attnotnull FROM pg_attribute WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        binding_oid,
    )
    expected_columns = [
        ("provider_system", "text", -1, True),
        ("provider_id", "text", -1, True),
        ("location_id", "uuid", -1, True),
        ("location_key", "character varying", 68, True),
        ("entity_type", "text", -1, True),
        ("entity_id", "text", -1, True),
    ]
    constraints = await connection.fetch(
        "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid=$1::oid AND contype IN ('p','u') ORDER BY contype",
        binding_oid,
    )
    if [tuple(column.values()) for column in columns] != expected_columns or {
        constraint["definition"] for constraint in constraints
    } != {"PRIMARY KEY (provider_system, provider_id, location_id)", "UNIQUE (location_key)"}:
        raise NetworkCustomAddressSourceError("Composition binding shape differs")


def _recipe_identity(receipt):
    return (
        receipt.base_source,
        receipt.npi_source,
        receipt.source_oids,
        receipt.npi_oids,
        receipt.approved_revision,
        receipt.owner_role,
        receipt.runtime_roles,
        receipt.control_schema,
        receipt.approved_records_sha256,
    )


async def _prepare_artifact(connection, recipe, schema_name, control):
    stored = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema_name
    )
    if stored is not None:
        receipt = _from_document(stored)
        if _recipe_identity(recipe) != _recipe_identity(receipt):
            raise NetworkCustomAddressSourceError("Retained composition recipe differs")
        return await verify_custom_address_source(connection, receipt)
    await _build_heaps(
        connection, schema_name, recipe.base_source, control, recipe.approved_revision, recipe.npi_source
    )
    await _prepare_office_lookup(connection, schema_name)
    owner_oid = await _freeze(connection, schema_name, recipe.owner_role, recipe.runtime_roles)
    schema_oid, address_oid, binding_oid, lookup_oid = await _artifact_identity(
        connection, schema_name, owner_oid, recipe.runtime_roles
    )
    receipt = replace(
        recipe,
        schema_oid=schema_oid,
        address_table_oid=address_oid,
        binding_table_oid=binding_oid,
        office_lookup_index_oid=lookup_oid,
        owner_oid=owner_oid,
    )
    generation = _generation(receipt)
    receipt = replace(
        receipt, generation_sha256=generation, address_source=replace(receipt.address_source, generation_id=generation)
    )
    document = json.dumps(receipt.as_dict(), sort_keys=True, separators=(",", ":"))
    if len(document.encode()) > 65536:
        raise NetworkCustomAddressSourceError("Composition receipt exceeds its bound")
    quoted_document = await connection.fetchval("SELECT quote_literal($1::text)", document)
    await connection.execute(f"COMMENT ON SCHEMA {_identifier(schema_name)} IS {quoted_document}")
    return receipt


async def prepare_custom_address_source(
    connection,
    base_source,
    *,
    composition_id,
    approved_revision,
    owner_role,
    runtime_roles,
    control_schema=None,
    npi_source=None,
):
    """Create or replay an exact uncommitted artifact; the controller admits source generations."""
    schema_name = _arguments(base_source, composition_id, approved_revision, owner_role, runtime_roles, npi_source)
    await _require_transaction(connection)
    control_name = control_schema or registry_schema()
    control = _identifier(control_name)
    try:
        async with connection.transaction():
            summary = await _approved_identity(connection, control, approved_revision)
            source_oids = await _source_identity(connection, base_source, runtime_roles, address=True)
            effective_npi_source = npi_source or (
                PinnedAddressSource(base_source.schema_name, "npi", base_source.generation_id)
                if summary["npi_pairs"]
                else None
            )
            npi_oids = (
                await _source_identity(connection, effective_npi_source, runtime_roles)
                if effective_npi_source
                else None
            )
            recipe = CustomAddressSourceReceipt(
                composition_id,
                approved_revision,
                control_name,
                base_source,
                effective_npi_source,
                source_oids,
                npi_oids,
                owner_role,
                runtime_roles,
                0,
                0,
                0,
                0,
                0,
                summary["pairs"],
                summary["fingerprint"],
                "",
                PinnedAddressSource(schema_name, "entity_address_unified", "prepared"),
            )
            return await _prepare_artifact(connection, recipe, schema_name, control)
    except asyncpg.PostgresError:
        raise NetworkCustomAddressSourceError("Composition native preparation failed") from None


def _from_document(document):
    if type(document) is not str or len(document.encode()) > 65536:
        raise NetworkCustomAddressSourceError("Retained composition receipt exceeds its bound")
    try:
        parsed = json.loads(document)
        if parsed.pop("component") != "network_custom_address_source" or parsed.pop("revision") != 2:
            raise ValueError
        for field in ("base_source", "address_source", "npi_source"):
            if parsed[field] is not None:
                parsed[field] = PinnedAddressSource(**parsed[field])
        for field in ("source_oids", "npi_oids", "runtime_roles"):
            if parsed[field] is not None:
                parsed[field] = tuple(parsed[field])
        return CustomAddressSourceReceipt(**parsed)
    except KeyError, TypeError, ValueError:
        raise NetworkCustomAddressSourceError("Retained composition receipt is invalid") from None


async def _verify_custom_address_source(connection, receipt, *, retained=False):
    if type(receipt) is not CustomAddressSourceReceipt:
        raise NetworkCustomAddressSourceError("Composition receipt is required")
    schema_name = _arguments(
        receipt.base_source,
        receipt.composition_id,
        receipt.approved_revision,
        receipt.owner_role,
        receipt.runtime_roles,
        receipt.npi_source,
    )
    await _require_transaction(connection)
    if (
        receipt.address_source != PinnedAddressSource(schema_name, "entity_address_unified", receipt.generation_sha256)
        or _generation(receipt) != receipt.generation_sha256
    ):
        raise NetworkCustomAddressSourceError("Composition receipt digest differs")
    summary = await _approved_identity(
        connection, _identifier(receipt.control_schema), receipt.approved_revision, retained=retained
    )
    await verify_approved_site_sources(
        connection, approved_revision=receipt.approved_revision, control_schema=receipt.control_schema
    )
    if summary["fingerprint"] != receipt.approved_records_sha256 or summary["pairs"] != receipt.custom_pair_count:
        raise NetworkCustomAddressSourceError("Approved composition records differ")
    if await _source_identity(
        connection, receipt.base_source, receipt.runtime_roles, address=True
    ) != receipt.source_oids or (
        receipt.npi_source is not None
        and await _source_identity(connection, receipt.npi_source, receipt.runtime_roles) != receipt.npi_oids
    ):
        raise NetworkCustomAddressSourceError("Retained source physical identity differs")
    if await _artifact_identity(connection, schema_name, receipt.owner_oid, receipt.runtime_roles) != (
        receipt.schema_oid,
        receipt.address_table_oid,
        receipt.binding_table_oid,
        receipt.office_lookup_index_oid,
    ):
        raise NetworkCustomAddressSourceError("Composition physical identity differs")
    artifact_source = await _source_identity(connection, receipt.address_source, receipt.runtime_roles, address=True)
    if artifact_source[3] != receipt.source_oids[3]:
        raise NetworkCustomAddressSourceError("Composition address shape differs")
    stored = await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", receipt.schema_oid)
    if stored is None or _from_document(stored) != receipt:
        raise NetworkCustomAddressSourceError("Retained composition recipe differs")
    return receipt


async def verify_custom_address_source(connection, receipt):
    """Require the unchanged recipe, current approved map and native closure."""
    return await _verify_custom_address_source(connection, receipt)


async def verify_retained_custom_address_source(connection, receipt):
    """Read only: verify the exact historical map, recipe and native closure.

    This proves retained source identity, without authorizing its older approval
    as the current map. The caller binds this receipt to a serving manifest.
    """
    return await _verify_custom_address_source(connection, receipt, retained=True)
