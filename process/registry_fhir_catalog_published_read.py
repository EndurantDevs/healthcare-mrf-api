# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed published-leaf policy and task-bound native read evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from types import MappingProxyType
from weakref import WeakKeyDictionary

import asyncpg

from db.plan_release_storage import storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL

_MAX_BYTES = 1024 * 1024
_MAX_ROWS = 1000
_ISSUED = WeakKeyDictionary()
_READ_CONTEXT = ContextVar("registry_fhir_catalog_published_reads", default=None)
_HEADER = "provider_directory_endpoint_dataset"
_SOURCE = "provider_directory_source"
_ENDPOINT = "provider_directory_api_endpoint"
_RESOURCE = "provider_directory_dataset_resource"
_BINDING = "provider_directory_insurance_network_source_binding"
_RESOURCE_COLUMNS = ("dataset_id", "resource_type", "resource_id", "payload_hash", "payload_json")
_HEADER_COLUMNS = (
    "dataset_id",
    "endpoint_id",
    "dataset_hash",
    "status",
    "is_current",
    "published_at",
    "resource_count",
    "previous_dataset_id",
    "acquisition_root_run_id",
    "publication_metadata_json",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "publication_metadata_sha256",
)
_SEMANTIC_COLUMNS = (
    "content_proof_resource_types",
    "publication_metadata_summary_json",
    "completion_proof_required_version",
    "completion_proof_json",
    "completion_proof_sha256",
)
_PUBLICATION_FIELDS = (
    "published_at",
    "root_run_id",
    "resource_hash_contract",
    "original_admission_version",
    "original_admission_kind",
    "original_admission_sha256",
    "original_metadata_sha256",
    "claimed_resource_count",
)


class FHIRCatalogPublishedReadError(ValueError):
    """Refuse malformed policy or missing native published-read authority."""


@dataclass
class _ScopeValidation:
    fingerprint: str
    allowed: tuple
    publications_by_dataset: dict
    checked_datasets: set[str] = field(default_factory=set)


@dataclass
class _ReadContext:
    connection: object
    task: object
    origin: tuple | None = None
    scopes_by_profile: dict = field(default_factory=dict)
    encoded_bytes: int = 0
    active: bool = True

    def clear(self):
        """Drop every proof and dataset key flag at the owned lifetime boundary."""
        for validation in self.scopes_by_profile.values():
            validation.checked_datasets.clear()
        self.scopes_by_profile.clear()
        self.encoded_bytes = 0

    def discard(self, profile_bytes):
        """Invalidate one failed scope without retaining its resource-key proof."""
        if validation := self.scopes_by_profile.pop(profile_bytes, None):
            validation.checked_datasets.clear()
            self.encoded_bytes -= len(profile_bytes)

    def remember(self, profile_bytes, validation):
        """Retain a successful scope within the existing profile and byte bounds."""
        if len(self.scopes_by_profile) >= 64 or self.encoded_bytes + len(profile_bytes) > _MAX_BYTES:
            _fail("scope_cache_bounds")
        self.scopes_by_profile[profile_bytes] = validation
        self.encoded_bytes += len(profile_bytes)


@contextmanager
def fhir_catalog_published_reads(connection):
    """Reuse scope proofs only inside this task's explicitly owned read context."""
    existing = _owned_context(connection)
    if existing is not None:
        try:
            yield
        except BaseException:
            existing.clear()
            raise
        return
    context = _ReadContext(connection, asyncio.current_task())
    if context.task is None:
        _fail("native_pin_required")
    token = _READ_CONTEXT.set(context)
    try:
        yield
    finally:
        context.active = False
        context.clear()
        context.origin = None
        context.connection = context.task = None
        _READ_CONTEXT.reset(token)


def _owned_context(connection):
    context = _READ_CONTEXT.get()
    if (
        context is not None
        and context.active
        and context.connection is connection
        and context.task is asyncio.current_task()
    ):
        return context
    return None


def _native_origin(actor):
    return tuple(
        actor[field] for field in ("backend", "transaction", "reader_oid", "database_oid", "database_system_identifier")
    )


def _transaction_context(connection, origin):
    context = _owned_context(connection)
    if context is not None and context.origin != origin:
        context.clear()
        context.origin = origin
    return context


def _fail(reason="invalid"):
    raise FHIRCatalogPublishedReadError("registry_fhir_catalog_published_read_" + reason)


def _json(value):
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    except TypeError, ValueError, UnicodeError, RecursionError:
        _fail()


def _keys(value, keys):
    if type(value) is not dict or set(value) != set(keys.split()):
        _fail()


def _text(value, maximum):
    if type(value) is not str or not value or value.strip() != value:
        _fail()
    try:
        is_valid = len(value.encode()) <= maximum and not any(ord(c) < 32 or ord(c) == 127 for c in value)
    except UnicodeError:
        _fail()
    if not is_valid:
        _fail()
    return value


def _identifier(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value) is None:
        _fail()
    return value


def _oid(value):
    if type(value) is not int or not 0 < value < 2**32:
        _fail()
    return value


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        _fail()
    return value


def _aliases(value):
    if type(value) is not list or not 1 <= len(value) <= 1000:
        _fail()
    aliases = tuple(_text(alias, 64) for alias in value)
    if tuple(sorted(set(aliases))) != aliases:
        _fail()
    return aliases


@dataclass(frozen=True)
class RelationPin:
    schema_name: str
    relation_name: str
    relation_oid: int

    def document(self):
        """Return a detached relation identity document."""
        return asdict(self)

    @property
    def sql(self):
        """Return the safely quoted relation name."""
        return storage_relation(self.schema_name, self.relation_name)


@dataclass(frozen=True)
class FHIRCatalogPublishedHeader:
    relation: RelationPin
    projection: str


@dataclass(frozen=True)
class FHIRCatalogPublishedBindings:
    relation: RelationPin
    source_aliases: tuple[str, ...]


@dataclass(frozen=True)
class FHIRCatalogPublishedDataset:
    endpoint_id: str
    dataset_id: str
    dataset_sha256: str
    source_aliases: tuple[str, ...]
    resource_leaves: tuple[RelationPin, ...]
    publication_sha256: str


@dataclass(frozen=True)
class FHIRCatalogPublishedReadScope:
    """Explicit immutable policy; serialized pins alone never confer authority."""

    version: int
    reader_role: str
    reader_oid: int
    database_oid: int
    database_system_identifier: str
    header: FHIRCatalogPublishedHeader
    source_registration: RelationPin
    endpoint_registration: RelationPin
    resource_root: RelationPin
    bindings: FHIRCatalogPublishedBindings
    datasets: tuple[FHIRCatalogPublishedDataset, ...]

    def document(self):
        """Return a detached canonical policy document."""
        return json.loads(_json(asdict(self)))


def _relation(value):
    _keys(value, "schema_name relation_name relation_oid")
    return RelationPin(
        _identifier(value["schema_name"]), _identifier(value["relation_name"]), _oid(value["relation_oid"])
    )


def _parse_dataset(dataset_by_field):
    _keys(dataset_by_field, "endpoint_id dataset_id dataset_sha256 source_aliases resource_leaves publication_sha256")
    leaves = dataset_by_field["resource_leaves"]
    if type(leaves) is not list or not 1 <= len(leaves) <= _MAX_ROWS:
        _fail("bounds_invalid")
    leaves = tuple(_relation(leaf) for leaf in leaves)
    if [leaf.relation_oid for leaf in leaves] != sorted({leaf.relation_oid for leaf in leaves}):
        _fail()
    return FHIRCatalogPublishedDataset(
        _text(dataset_by_field["endpoint_id"], 64),
        _text(dataset_by_field["dataset_id"], 96),
        _digest(dataset_by_field["dataset_sha256"]),
        _aliases(dataset_by_field["source_aliases"]),
        leaves,
        _digest(dataset_by_field["publication_sha256"]),
    )


def _validate_scope_sets(scope):
    keys = [(entry.endpoint_id, entry.dataset_id) for entry in scope.datasets]
    leaves = [leaf for entry in scope.datasets for leaf in entry.resource_leaves]
    metadata = [
        scope.header.relation,
        scope.source_registration,
        scope.endpoint_registration,
        scope.resource_root,
        scope.bindings.relation,
    ]
    all_relations = metadata + leaves
    if (
        keys != sorted(set(keys))
        or len({entry.dataset_id for entry in scope.datasets}) != len(scope.datasets)
        or len(leaves) > _MAX_ROWS
        or sum(len(entry.source_aliases) for entry in scope.datasets) + len(scope.bindings.source_aliases) > 10_000
        or tuple(sorted({alias for entry in scope.datasets for alias in entry.source_aliases}))
        != scope.bindings.source_aliases
        or len({pin.relation_oid for pin in all_relations}) != len(all_relations)
        or len({(pin.schema_name, pin.relation_name) for pin in all_relations}) != len(all_relations)
        or [pin.relation_name for pin in metadata] != [_HEADER, _SOURCE, _ENDPOINT, _RESOURCE, _BINDING]
    ):
        _fail("scope_invalid")


def parse_fhir_catalog_published_read_scope(document):
    """Parse the bounded, non-null, closed policy into immutable canonical DTOs."""
    _keys(
        document,
        "version reader_role reader_oid database_oid database_system_identifier header source_registration "
        "endpoint_registration resource_root bindings datasets",
    )
    if len(_json(document)) > _MAX_BYTES or type(document["version"]) is not int or document["version"] != 1:
        _fail("bounds_or_version_invalid")
    system_id = document["database_system_identifier"]
    if type(system_id) is not str or re.fullmatch(r"[1-9][0-9]{0,19}", system_id) is None or int(system_id) >= 2**64:
        _fail()
    header, binding = document["header"], document["bindings"]
    _keys(header, "relation projection")
    _keys(binding, "relation source_aliases")
    if type(header["projection"]) is not str or header["projection"] not in {
        "catalog_observation_v1",
        "semantic_admission_v1",
    }:
        _fail()
    if type(document["datasets"]) is not list or not 1 <= len(document["datasets"]) <= _MAX_ROWS:
        _fail("bounds_invalid")
    scope = FHIRCatalogPublishedReadScope(
        1,
        _identifier(document["reader_role"]),
        _oid(document["reader_oid"]),
        _oid(document["database_oid"]),
        system_id,
        FHIRCatalogPublishedHeader(_relation(header["relation"]), header["projection"]),
        _relation(document["source_registration"]),
        _relation(document["endpoint_registration"]),
        _relation(document["resource_root"]),
        FHIRCatalogPublishedBindings(_relation(binding["relation"]), _aliases(binding["source_aliases"])),
        tuple(_parse_dataset(dataset_by_field) for dataset_by_field in document["datasets"]),
    )
    _validate_scope_sets(scope)
    return scope


def _scope(profile):
    from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

    if type(profile) is not FHIRCatalogSourceReadProfile:
        _fail("profile_invalid")
    scope = getattr(profile, "published_read_scope", None)
    if (
        type(scope) is not FHIRCatalogPublishedReadScope
        or parse_fhir_catalog_published_read_scope(scope.document()) != scope
    ):
        _fail("scope_required")
    if (
        scope.reader_role in (*profile.runtime_roles, profile.owner_role)
        or any(
            pin.schema_name != profile.schema_name
            for pin in (
                scope.header.relation,
                scope.resource_root,
                scope.bindings.relation,
                *(leaf for entry in scope.datasets for leaf in entry.resource_leaves),
            )
        )
        or any(
            pin.schema_name != profile.catalog_schema
            for pin in (scope.source_registration, scope.endpoint_registration)
        )
    ):
        _fail("profile_linkage_invalid")
    return scope


def _dataset(scope, endpoint_id, dataset_id):
    _text(endpoint_id, 64)
    _text(dataset_id, 96)
    matches = [entry for entry in scope.datasets if (entry.endpoint_id, entry.dataset_id) == (endpoint_id, dataset_id)]
    if len(matches) != 1:
        _fail("dataset_not_admitted")
    return matches[0]


def validate_fhir_catalog_published_descriptor(profile, descriptor):
    """Check exact serialized linkage only; native pinning is a separate boundary."""
    scope = _scope(profile)
    if type(descriptor) is not dict or type(descriptor.get("coordinates")) is not dict:
        _fail("descriptor_invalid")
    coordinates = descriptor["coordinates"]
    entry = _dataset(scope, descriptor.get("endpoint_id"), coordinates.get("dataset_id"))
    for field in ("resource_table_oid", "header_table_oid", "database_oid"):
        _oid(descriptor.get(field))
    leaves = descriptor.get("resource_leaf_oids")
    if type(leaves) is not list:
        _fail("descriptor_invalid")
    for oid in leaves:
        _oid(oid)
    if (
        descriptor.get("profile") != "published_normalized"
        or coordinates
        != {
            "source_system": "fhir",
            "source_id": entry.source_aliases[0],
            "dataset_schema": profile.schema_name,
            "dataset_id": entry.dataset_id,
            "producer_id": "registry-fhir-catalog-v1",
            "edition_id": entry.dataset_id,
        }
        or descriptor.get("native_read_profile") != profile.document()
        or descriptor.get("source_aliases") != list(entry.source_aliases)
        or descriptor.get("dataset_sha256") != entry.dataset_sha256
        or descriptor.get("resource_table_oid") != scope.resource_root.relation_oid
        or descriptor.get("resource_leaf_oids") != [leaf.relation_oid for leaf in entry.resource_leaves]
        or descriptor.get("header_table_oid") != scope.header.relation.relation_oid
        or descriptor.get("database_oid") != scope.database_oid
    ):
        _fail("descriptor_linkage_invalid")
    admission = descriptor.get("admission")
    if type(admission) is not dict:
        _fail("descriptor_invalid")
    if admission.get("kind") == "catalog_observation":
        if scope.header.projection != "catalog_observation_v1" or type(admission.get("publication")) is not dict:
            _fail("descriptor_linkage_invalid")
        if hashlib.sha256(_json(admission["publication"])).hexdigest() != entry.publication_sha256:
            _fail("publication_changed")
    elif scope.header.projection != "semantic_admission_v1":
        _fail("descriptor_linkage_invalid")
    return descriptor


_IDENTITY_SQL = """SELECT session_user::text AS login,current_user::text AS actor,r.oid::bigint AS reader_oid,
 owner.oid::bigint AS owner_oid,r.rolcanlogin,
 pg_catalog.current_setting('transaction_read_only') AS readonly,
 pg_catalog.current_setting('transaction_isolation') AS isolation,
 pg_catalog.current_setting('session_replication_role') AS replication_role,
 (pg_catalog.current_schemas(true))[1]='pg_catalog' AS safe_search_path,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 (pg_catalog.pg_control_system()).system_identifier::text AS database_system_identifier,
 pg_catalog.pg_backend_pid() AS backend,
 (SELECT virtualtransaction FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid()
  AND locktype='virtualxid' AND granted) AS transaction,
 (SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname=ANY($2::text[])) AS runtime_role_count,
 pg_catalog.has_database_privilege(r.oid,pg_catalog.current_database(),'CONNECT') AS database_connect,
 pg_catalog.has_database_privilege(r.oid,pg_catalog.current_database(),'TEMPORARY') AS database_temp,
 NOT EXISTS(SELECT FROM pg_catalog.pg_roles reachable WHERE
  (reachable.oid=r.oid OR pg_catalog.pg_has_role(r.oid,reachable.oid,'MEMBER')
   OR pg_catalog.pg_has_role(r.oid,reachable.oid,'SET') OR pg_catalog.pg_has_role(r.oid,reachable.oid,'USAGE'))
  AND (reachable.rolsuper OR reachable.rolcreatedb OR reachable.rolcreaterole OR reachable.rolreplication
   OR reachable.rolbypassrls OR reachable.oid=owner.oid OR reachable.rolname IN
   ('pg_read_all_data','pg_write_all_data','pg_read_server_files','pg_write_server_files',
    'pg_execute_server_program','pg_signal_backend','pg_checkpoint','pg_create_subscription')
   OR (reachable.oid<>r.oid AND pg_catalog.pg_has_role(r.oid,reachable.oid,'SET')))) AS no_dangerous_role,
 NOT EXISTS(SELECT FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid AND m.admin_option) AS no_admin_option
 FROM pg_catalog.pg_roles r LEFT JOIN pg_catalog.pg_roles owner ON owner.rolname=$1 WHERE r.rolname=session_user"""


async def _identity(connection, profile, scope):
    if not isinstance(connection, (asyncpg.Connection, asyncpg.pool.PoolConnectionProxy)):
        _fail("native_connection_required")
    if connection.is_in_transaction() is not True:
        _fail("readonly_snapshot_required")
    actor = await connection.fetchrow(_IDENTITY_SQL, profile.owner_role, list(profile.runtime_roles))
    if actor is None or any(
        (
            actor["login"] != scope.reader_role,
            actor["actor"] != scope.reader_role,
            actor["reader_oid"] != scope.reader_oid,
            actor["database_oid"] != scope.database_oid,
            actor["database_system_identifier"] != scope.database_system_identifier,
            actor["owner_oid"] is None,
            actor["rolcanlogin"] is not True,
            actor["readonly"] != "on",
            actor["isolation"] != "repeatable read",
            actor["replication_role"] != "origin",
            actor["safe_search_path"] is not True,
            actor["no_dangerous_role"] is not True,
            actor["database_connect"] is not True,
            actor["database_temp"] is not True,
            actor["no_admin_option"] is not True,
            actor["runtime_role_count"] != len(profile.runtime_roles),
            type(actor["backend"]) is not int,
            type(actor["transaction"]) is not str,
            not actor["transaction"],
        )
    ):
        _fail("reader_unsafe")
    return dict(actor)


def _metadata(scope):
    header_columns = _HEADER_COLUMNS + (_SEMANTIC_COLUMNS if scope.header.projection == "semantic_admission_v1" else ())
    return (
        (scope.header.relation, ("tableoid", *header_columns)),
        (scope.source_registration, ("tableoid", "source_id", "endpoint_id")),
        (scope.endpoint_registration, ("tableoid", "endpoint_id", "canonical_api_base")),
        (scope.bindings.relation, ("tableoid", "source_id", "resource_id", "network_id")),
    )


async def _lock_projection(connection, pin, columns):
    await connection.fetch("SELECT " + ",".join(columns) + " FROM ONLY " + pin.sql + " LIMIT 0")


async def pin_fhir_catalog_published_inventory(connection, profile):
    """Retain exact metadata projection locks in the caller's native read-only snapshot."""
    scope = _scope(profile)
    try:
        actor = await _identity(connection, profile, scope)
        async with connection.transaction():
            timeout = await connection.fetchval("SELECT pg_catalog.current_setting('lock_timeout')")
            await connection.execute("SET LOCAL lock_timeout='1ms'")
            for pin, columns in _metadata(scope):
                await _lock_projection(connection, pin, columns)
            await _require_relation_locks(connection, [pin for pin, _ in _metadata(scope)])
            await connection.fetchval("SELECT pg_catalog.set_config('lock_timeout',$1,true)", timeout)
            if await _identity(connection, profile, scope) != actor:
                _fail("native_transaction_changed")
    except asyncpg.PostgresError as error:
        raise FHIRCatalogPublishedReadError("registry_fhir_catalog_published_read_unavailable") from error


_PHYSICAL_SQL = (
    """WITH physical AS MATERIALIZED (
 SELECT row_to_json(c)::text AS document,c.oid FROM ("""
    + CATALOG_SQL
    + """) c LIMIT 1001
), bounded AS (
 SELECT *,sum(octet_length(document)) OVER(ORDER BY oid) AS bytes FROM physical
) SELECT CASE WHEN bytes<=$4 THEN document END AS document FROM bounded ORDER BY oid"""
)

_ACL_SQL = """SELECT c.oid::bigint,c.relacl::text AS table_acl,n.nspacl::text AS schema_acl,
 n.oid::bigint AS schema_oid,n.nspowner::bigint AS schema_owner_oid,
 ARRAY(SELECT jsonb_build_array(a.attnum,a.attname,a.attacl::text)::text FROM pg_catalog.pg_attribute a
  WHERE a.attrelid=c.oid AND a.attnum<>0 AND NOT a.attisdropped ORDER BY a.attnum) AS column_acls
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=ANY($1::oid[]) ORDER BY c.oid"""

_LOCKS_SQL = """WITH wanted AS (
 SELECT * FROM pg_catalog.jsonb_to_recordset($1::jsonb) AS w(oid bigint,schema_name text,relation_name text,locked boolean)
) SELECT NOT EXISTS(SELECT FROM wanted w WHERE NOT EXISTS(
 SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=w.oid AND c.relname=w.relation_name AND n.nspname=w.schema_name
 AND (NOT w.locked OR EXISTS(SELECT FROM pg_catalog.pg_locks held
  WHERE held.pid=pg_catalog.pg_backend_pid() AND held.locktype='relation' AND held.relation=c.oid
   AND held.granted AND held.mode='AccessShareLock'))))"""


async def _require_relation_locks(connection, locked_pins, unlocked_pins=()):
    wanted_relations = [
        {"oid": pin.relation_oid, "schema_name": pin.schema_name, "relation_name": pin.relation_name, "locked": True}
        for pin in locked_pins
    ]
    wanted_relations.extend(
        {"oid": pin.relation_oid, "schema_name": pin.schema_name, "relation_name": pin.relation_name, "locked": False}
        for pin in unlocked_pins
    )
    if await connection.fetchval(_LOCKS_SQL, _json(wanted_relations).decode()) is not True:
        _fail("native_projection_locks_changed")


_ACCESS_SQL = """WITH reachable AS (
 SELECT oid FROM pg_catalog.pg_roles WHERE oid=$1::oid OR pg_catalog.pg_has_role($1::oid,oid,'MEMBER')
 OR pg_catalog.pg_has_role($1::oid,oid,'SET') OR pg_catalog.pg_has_role($1::oid,oid,'USAGE')
), objects AS (
 SELECT c.* FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=ANY($2::text[]) AND c.relkind IN ('r','p','v','m','f','S')
), allowed AS (
 SELECT * FROM jsonb_to_recordset($3::jsonb) AS a(oid bigint,columns text[])
), required AS (
 SELECT * FROM jsonb_to_recordset($4::jsonb) AS a(oid bigint,columns text[])
)
 SELECT NOT EXISTS(SELECT FROM reachable r CROSS JOIN objects c WHERE
 pg_catalog.pg_has_role(r.oid,c.relowner,'MEMBER') OR pg_catalog.pg_has_role(r.oid,c.relowner,'SET')
 OR pg_catalog.pg_has_role(r.oid,c.relowner,'USAGE')
 OR (c.relkind<>'S' AND (pg_catalog.has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
  OR pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')
  OR pg_catalog.has_table_privilege(r.oid,c.oid,'SELECT WITH GRANT OPTION')))
 OR (c.relkind='S' AND pg_catalog.has_sequence_privilege(r.oid,c.oid,'SELECT,USAGE,UPDATE'))
 OR (c.relkind<>'S' AND EXISTS(SELECT FROM pg_catalog.pg_attribute field
  WHERE field.attrelid=c.oid AND (field.attnum>0 OR field.attname='tableoid') AND NOT field.attisdropped
  AND ((pg_catalog.has_column_privilege(r.oid,c.oid,field.attnum,'SELECT')
    AND NOT EXISTS(SELECT FROM allowed WHERE allowed.oid=c.oid AND field.attname=ANY(allowed.columns)))
   OR pg_catalog.has_column_privilege(r.oid,c.oid,field.attnum,'SELECT WITH GRANT OPTION')))))
 AND NOT EXISTS(SELECT FROM reachable r CROSS JOIN pg_catalog.pg_namespace n WHERE n.nspname=ANY($2::text[])
  AND (pg_catalog.has_schema_privilege(r.oid,n.oid,'CREATE')
   OR pg_catalog.has_schema_privilege(r.oid,n.oid,'USAGE WITH GRANT OPTION')
   OR pg_catalog.pg_has_role(r.oid,n.nspowner,'MEMBER') OR pg_catalog.pg_has_role(r.oid,n.nspowner,'SET')
   OR pg_catalog.pg_has_role(r.oid,n.nspowner,'USAGE')))
 AND NOT EXISTS(SELECT FROM reachable r WHERE
  pg_catalog.has_parameter_privilege(r.oid,'session_replication_role','SET')
  OR pg_catalog.has_database_privilege(r.oid,pg_catalog.current_database(),'CREATE')
  OR pg_catalog.has_database_privilege(r.oid,pg_catalog.current_database(),'CONNECT WITH GRANT OPTION')
  OR pg_catalog.has_database_privilege(r.oid,pg_catalog.current_database(),'TEMPORARY WITH GRANT OPTION')
  OR EXISTS(SELECT FROM pg_catalog.pg_auth_members m WHERE m.member=r.oid AND m.admin_option))
 AND NOT EXISTS(SELECT FROM reachable r CROSS JOIN pg_catalog.pg_proc p
  CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
  WHERE p.oid='pg_catalog.pg_control_system()'::regprocedure AND a.is_grantable
   AND a.privilege_type='EXECUTE' AND (a.grantee=0 OR a.grantee=r.oid))
 AND NOT EXISTS(SELECT FROM reachable r CROSS JOIN pg_catalog.pg_proc p
  WHERE p.prosecdef AND p.pronamespace<>'pg_catalog'::regnamespace
   AND pg_catalog.has_function_privilege(r.oid,p.oid,'EXECUTE'))
 AND NOT EXISTS(SELECT FROM required CROSS JOIN LATERAL unnest(required.columns) field(column_name)
  WHERE NOT coalesce(pg_catalog.has_column_privilege($1::oid,required.oid::oid,field.column_name,'SELECT'),false))
 AND NOT EXISTS(SELECT FROM unnest($2::text[]) namespaces(schema_name)
  WHERE NOT coalesce(pg_catalog.has_schema_privilege(
   $1::oid,pg_catalog.to_regnamespace(pg_catalog.quote_ident(schema_name)),'USAGE'),false))"""


def _required_projections(scope):
    required_columns = [{"oid": pin.relation_oid, "columns": list(columns)} for pin, columns in _metadata(scope)]
    required_columns.extend(
        {"oid": pin.relation_oid, "columns": ["tableoid", *_RESOURCE_COLUMNS]}
        for entry in scope.datasets
        for pin in entry.resource_leaves
    )
    return _json(required_columns).decode()


def _catalog_identity(physical_rows, scope):
    physical_by_oid = {physical_row["oid"]: physical_row for physical_row in physical_rows}
    if len(physical_by_oid) != len(physical_rows):
        _fail("physical_identity_invalid")
    pins = [pin for pin, _ in _metadata(scope)] + [scope.resource_root]
    for pin in pins:
        physical_row = physical_by_oid.get(pin.relation_oid)
        if (
            physical_row is None
            or physical_row["parents"]
            or (physical_row["nspname"], physical_row["relname"]) != (pin.schema_name, pin.relation_name)
        ):
            _fail("physical_identity_invalid")
    for physical_row in physical_rows:
        if (
            physical_row["relkind"] not in {"r", "p"}
            or physical_row["relpersistence"] != "p"
            or physical_row["relrowsecurity"] is not False
            or physical_row["relforcerowsecurity"] is not False
            or physical_row["nspname"] not in {pin.schema_name for pin in pins}
            or len(physical_row["parents"]) > 1
            or not set(physical_row["parents"]) <= set(physical_by_oid)
            or bool(physical_row["parents"]) != physical_row["relispartition"]
        ):
            _fail("physical_identity_invalid")
    return physical_by_oid


def _descendant_oids(physical_by_oid, root_oid):
    descendants = {root_oid}
    while (
        children := {
            physical_row["oid"]
            for physical_row in physical_by_oid.values()
            if set(physical_row["parents"]) & descendants
        }
        - descendants
    ):
        descendants.update(children)
    return descendants


def _dataset_routes(physical_by_oid, root_oid, entry):
    family_oids = _descendant_oids(physical_by_oid, root_oid)
    bound = "FOR VALUES IN ('" + entry.dataset_id.replace("'", "''") + "')"
    anchors = [
        physical_row["oid"]
        for physical_row in physical_by_oid.values()
        if physical_row["oid"] in family_oids
        and physical_row["parents"]
        and physical_by_oid[physical_row["parents"][0]]["partition_key"] == "LIST (dataset_id)"
        and physical_row["bound"] == bound
    ]
    if len(anchors) != 1:
        _fail("dataset_partition_required")
    descendants = _descendant_oids(physical_by_oid, anchors[0])
    leaves = {oid for oid in descendants if physical_by_oid[oid]["relkind"] == "r"}
    if leaves != {pin.relation_oid for pin in entry.resource_leaves}:
        _fail("resource_leaves_changed")
    for pin in entry.resource_leaves:
        physical_row = physical_by_oid[pin.relation_oid]
        if (physical_row["nspname"], physical_row["relname"]) != (pin.schema_name, pin.relation_name):
            _fail("physical_identity_invalid")
    ancestor = anchors[0]
    while physical_by_oid[ancestor]["parents"]:
        ancestor = physical_by_oid[ancestor]["parents"][0]
        descendants.add(ancestor)
    if ancestor != root_oid:
        _fail("dataset_partition_required")
    return descendants


def _binding_shape(binding_by_field):
    required_by_name = {
        "source_id": "character varying(64)",
        "resource_id": "character varying(256)",
        "network_id": "uuid",
    }
    columns_by_name = {column[0]: column for column in binding_by_field["columns"]}
    if (
        any(
            name not in columns_by_name
            or columns_by_name[name][1] != kind
            or columns_by_name[name][2] is not True
            or columns_by_name[name][3] is not None
            for name, kind in required_by_name.items()
        )
        or binding_by_field["inert"] is not True
        or binding_by_field["native_checks"] is not True
        or not any(
            index["unique"] is True
            and index["valid"] is True
            and index["method"] == "btree"
            and index["native"] is True
            and index["keys"] == ["source_id", "resource_id"]
            and index["predicate"] is None
            and index["expression"] is None
            for index in binding_by_field["indexes"]
        )
    ):
        _fail("binding_shape_invalid")
    if "resource_type" in columns_by_name and (
        columns_by_name["resource_type"][1] != "character varying(16)"
        or columns_by_name["resource_type"][2] is not True
        or not any(
            constraint["type"] == "c"
            and constraint["valid"] is True
            and constraint["definition"] == "CHECK (((resource_type)::text = 'Organization'::text))"
            for constraint in binding_by_field["constraints"]
        )
    ):
        _fail("binding_shape_invalid")


def _physical_scope(physical_rows, scope, owner_oid):
    """Verify exact native dataset ancestry, complete leaves and source binding custody."""
    physical_by_oid = _catalog_identity(physical_rows, scope)
    root_oid = scope.resource_root.relation_oid
    if physical_by_oid[root_oid]["relkind"] != "p":
        _fail("dataset_partition_required")
    resource_oids = {root_oid}
    for entry in scope.datasets:
        resource_oids.update(_dataset_routes(physical_by_oid, root_oid, entry))
    owned_oids = resource_oids | {scope.header.relation.relation_oid, scope.bindings.relation.relation_oid}
    if any(physical_by_oid[oid]["relowner"] != owner_oid for oid in owned_oids):
        _fail("physical_owner_changed")
    metadata_oids = {pin.relation_oid for pin, _ in _metadata(scope)}
    for metadata_oid in metadata_oids:
        if physical_by_oid[metadata_oid]["relkind"] != "r" or any(
            metadata_oid in physical_row["parents"] for physical_row in physical_rows
        ):
            _fail("metadata_shape_invalid")
    _binding_shape(physical_by_oid[scope.bindings.relation.relation_oid])
    return physical_by_oid, sorted(resource_oids | metadata_oids)


async def _native_fingerprint(connection, physical_by_oid, routed):
    acls = await connection.fetch(_ACL_SQL, routed)
    if len(acls) != len(routed):
        _fail("physical_identity_invalid")
    # Unrelated sibling definitions are inspected for routes but never enter this fingerprint.
    definitions = [
        {
            key: definition_value
            for key, definition_value in physical_by_oid[oid].items()
            if key not in {"direct_acl_closed", "readers_closed"}
        }
        for oid in routed
    ]
    fingerprint = hashlib.sha256(
        _json({"definitions": definitions, "acls": [dict(acl_row) for acl_row in acls]})
    ).hexdigest()
    return fingerprint


async def _physical(connection, profile, scope, actor):
    """Pin bounded native physical closure and effective read authority."""
    for pin, columns in _metadata(scope):
        await _lock_projection(connection, pin, columns)
    await connection.fetch(
        "SELECT * FROM ("
        + " UNION ALL ".join(_resource_sql(entry) for entry in scope.datasets)
        + ") AS published_projection LIMIT 0"
    )
    names = [pin.sql for pin, _ in _metadata(scope)] + [scope.resource_root.sql]
    catalog_records = await connection.fetch(_PHYSICAL_SQL, names, actor["owner_oid"], [scope.reader_oid], _MAX_BYTES)
    if not 6 <= len(catalog_records) <= _MAX_ROWS or any(
        catalog_record["document"] is None for catalog_record in catalog_records
    ):
        _fail("catalog_bounds")
    from process.provider_directory_generic_source_provision import fhir_source_catalog_row

    try:
        physical_rows = [
            fhir_source_catalog_row(json.loads(catalog_record["document"])) for catalog_record in catalog_records
        ]
        physical_by_oid, routed = _physical_scope(physical_rows, scope, actor["owner_oid"])
    except KeyError, TypeError, json.JSONDecodeError:
        _fail("physical_identity_invalid")
    allowed_columns = [{"oid": pin.relation_oid, "columns": list(columns)} for pin, columns in _metadata(scope)]
    allowed_columns.extend(
        {
            "oid": pin.relation_oid,
            "columns": ["tableoid", *(column[0] for column in physical_by_oid[pin.relation_oid]["columns"])],
        }
        for entry in scope.datasets
        for pin in entry.resource_leaves
    )
    if (
        await connection.fetchval(
            _ACCESS_SQL,
            scope.reader_oid,
            sorted({profile.schema_name, profile.catalog_schema}),
            _json(allowed_columns).decode(),
            _required_projections(scope),
        )
        is not True
    ):
        _fail("reader_scope_unsafe")
    fingerprint = await _native_fingerprint(connection, physical_by_oid, routed)
    return fingerprint, tuple(
        (allowed_entry["oid"], tuple(allowed_entry["columns"])) for allowed_entry in allowed_columns
    )


def _publication_document(header_by_field):
    publication_by_field = {
        field: header_by_field["resource_count" if field == "claimed_resource_count" else field]
        for field in _PUBLICATION_FIELDS
    }
    published = header_by_field["published_at"]
    publication_by_field["published_at"] = (
        published.replace(tzinfo=timezone.utc) if published.tzinfo is None else published.astimezone(timezone.utc)
    ).isoformat()
    from process.registry_fhir_catalog_extraction import RESOURCE_TYPES
    from process.registry_fhir_catalog_observation import validate_catalog_observation_receipt

    validate_catalog_observation_receipt(
        {
            "version": 1,
            "kind": "catalog_observation",
            "contract": "registry-fhir-catalog-observation-v1",
            "publication": publication_by_field,
            "families": {kind: {"rows": 0, "sha256": "0" * 64} for kind in RESOURCE_TYPES},
        }
    )
    return publication_by_field


async def _header(connection, scope, entry, require_current):
    """Require the fixed published header and exact admitted publication digest."""
    relation = "ONLY " + scope.header.relation.sql
    header_rows = await connection.fetch(
        "SELECT tableoid::oid::bigint AS oid,dataset_id,endpoint_id,left(dataset_hash,65) AS dataset_hash,status,is_current,"
        "published_at,resource_count,left(previous_dataset_id,97) AS previous_dataset_id,"
        "left(acquisition_root_run_id,65) AS root_run_id,"
        "json_typeof((publication_metadata_json->'resource_hash_contract')::json) AS hash_contract_type,"
        "left(publication_metadata_json->>'resource_hash_contract',129) AS resource_hash_contract,"
        "content_proof_admission_version AS original_admission_version,"
        "left(content_proof_admission_kind,65) AS original_admission_kind,"
        "left(content_proof_admission_sha256,65) AS original_admission_sha256,"
        "left(publication_metadata_sha256,65) AS original_metadata_sha256,"
        f"(SELECT count(*) FROM {relation} WHERE endpoint_id=$1 AND is_current) AS current_count "
        f"FROM {relation} WHERE endpoint_id=$1 AND dataset_id=$2 LIMIT 2",
        entry.endpoint_id,
        entry.dataset_id,
    )
    if len(header_rows) != 1:
        _fail("publication_unavailable")
    header_by_field = dict(header_rows[0])
    if (
        header_by_field["oid"] != scope.header.relation.relation_oid
        or header_by_field["endpoint_id"] != entry.endpoint_id
        or header_by_field["dataset_id"] != entry.dataset_id
        or header_by_field["dataset_hash"] != entry.dataset_sha256
        or header_by_field["status"] not in {"published", "superseded"}
        or type(header_by_field["is_current"]) is not bool
        or type(header_by_field["published_at"]) is not datetime
        or header_by_field["hash_contract_type"] not in {None, "null", "string"}
        or (header_by_field["status"] == "superseded" and header_by_field["is_current"])
    ):
        _fail("publication_invalid")
    _require_current_header(header_by_field, require_current)
    publication_by_field = _publication_document(header_by_field)
    if hashlib.sha256(_json(publication_by_field)).hexdigest() != entry.publication_sha256:
        _fail("publication_changed")
    if header_by_field["previous_dataset_id"] is not None:
        _text(header_by_field["previous_dataset_id"], 96)
    return header_by_field


def _require_current_header(header_by_field, require_current):
    if require_current and (
        header_by_field["status"] != "published"
        or header_by_field["is_current"] is not True
        or header_by_field["current_count"] != 1
    ):
        _fail("publication_invalid")


async def _registration(connection, scope, entry):
    endpoints = await connection.fetch(
        "SELECT tableoid::oid::bigint AS oid,endpoint_id,left(canonical_api_base,2049) AS source_uri FROM ONLY "
        + scope.endpoint_registration.sql
        + " WHERE endpoint_id=$1 LIMIT 2",
        entry.endpoint_id,
    )
    aliases = await connection.fetch(
        "SELECT tableoid::oid::bigint AS oid,endpoint_id,left(source_id,65) AS source_id FROM ONLY "
        + scope.source_registration.sql
        + ' WHERE endpoint_id=$1 ORDER BY source_id COLLATE "C" LIMIT 1001',
        entry.endpoint_id,
    )
    if (
        len(endpoints) != 1
        or endpoints[0]["oid"] != scope.endpoint_registration.relation_oid
        or endpoints[0]["endpoint_id"] != entry.endpoint_id
        or tuple(source_alias["source_id"] for source_alias in aliases) != entry.source_aliases
        or any(
            source_alias["oid"] != scope.source_registration.relation_oid
            or source_alias["endpoint_id"] != entry.endpoint_id
            for source_alias in aliases
        )
    ):
        _fail("registration_changed")
    from process.registry_fhir_catalog_extraction import fhir_source_uri

    source_uri = _text(endpoints[0]["source_uri"], 2048)
    fhir_source_uri(source_uri)
    return source_uri


async def _binding_scope(connection, scope):
    # Every readable alias row must be admitted; WHERE filters do not establish ACL isolation.
    if (
        await connection.fetchval(
            "SELECT NOT EXISTS(SELECT FROM ONLY "
            + scope.bindings.relation.sql
            + " WHERE source_id IS NULL OR NOT(source_id=ANY($1::text[])) OR resource_id IS NULL "
            "OR resource_id !~ '^[A-Za-z0-9.-]{1,64}$' OR network_id IS NULL "
            "OR network_id='00000000-0000-0000-0000-000000000000'::uuid)",
            list(scope.bindings.source_aliases),
        )
        is not True
    ):
        _fail("binding_scope_invalid")


async def _validate_scope(connection, profile, scope, actor):
    async with connection.transaction():
        timeout = await connection.fetchval("SELECT pg_catalog.current_setting('lock_timeout')")
        await connection.execute("SET LOCAL lock_timeout='1ms'")
        fingerprint, allowed = await _physical(connection, profile, scope, actor)
        await connection.fetchval("SELECT pg_catalog.set_config('lock_timeout',$1,true)", timeout)
        publications_by_dataset = {}
        for admitted in scope.datasets:
            header = await _header(connection, scope, admitted, False)
            source_uri = await _registration(connection, scope, admitted)
            publications_by_dataset[admitted.dataset_id] = (MappingProxyType(header), source_uri)
        await _binding_scope(connection, scope)
    return _ScopeValidation(fingerprint, allowed, publications_by_dataset)


async def _require_native_scope(connection, profile, scope, origin, allowed):
    pins = [pin for pin, _ in _metadata(scope)]
    pins.extend(pin for entry in scope.datasets for pin in entry.resource_leaves)
    await _require_relation_locks(connection, pins, [scope.resource_root])
    # Simple protocol reopens the admitted projection; prepared probes can retain stale ACLs.
    await connection.execute("SELECT tableoid,dataset_id FROM ONLY " + scope.header.relation.sql + " LIMIT 0")
    actor = await _identity(connection, profile, scope)
    if _native_origin(actor) != origin:
        _fail("native_transaction_changed")
    allowed_columns = [{"oid": oid, "columns": list(columns)} for oid, columns in allowed]
    if (
        await connection.fetchval(
            _ACCESS_SQL,
            scope.reader_oid,
            sorted({profile.schema_name, profile.catalog_schema}),
            _json(allowed_columns).decode(),
            _required_projections(scope),
        )
        is not True
    ):
        _fail("reader_scope_unsafe")


def _resource_sql(entry):
    return " UNION ALL ".join(
        "SELECT tableoid::oid::bigint AS source_table_oid," + ",".join(_RESOURCE_COLUMNS) + " FROM ONLY " + pin.sql
        for pin in entry.resource_leaves
    )


async def _resource_keys(connection, entry):
    keys_sql = " UNION ALL ".join(
        "SELECT dataset_id,resource_type,resource_id FROM ONLY " + pin.sql for pin in entry.resource_leaves
    )
    if (
        await connection.fetchval(
            "WITH resources AS MATERIALIZED (" + keys_sql + ") SELECT "
            "NOT EXISTS(SELECT FROM resources WHERE dataset_id IS DISTINCT FROM $1 OR resource_type IS NULL "
            "OR resource_id IS NULL OR resource_type='' OR resource_id !~ '^[A-Za-z0-9.-]{1,64}$') "
            "AND NOT EXISTS(SELECT FROM resources GROUP BY resource_type,resource_id HAVING count(*)>1)",
            entry.dataset_id,
        )
        is not True
    ):
        _fail("resource_scope_or_keys_invalid")


@dataclass(frozen=True, eq=False)
class FHIRCatalogPublishedRead:
    """A native pin usable only by its issuing task, connection and transaction."""

    _connection: object
    _task: object
    _profile: object
    _scope: FHIRCatalogPublishedReadScope
    _entry: FHIRCatalogPublishedDataset
    _origin: tuple
    _native: tuple
    _allowed: tuple
    _header: object
    source_uri: str
    source_aliases: tuple[str, ...]

    def _issued(self):
        issued = _ISSUED.get(self)
        if issued is None:
            _fail("native_pin_required")
        try:
            fields = tuple(getattr(self, name) for name in self.__dataclass_fields__)
            is_valid = (
                issued == fields
                and self._task is asyncio.current_task()
                and not self._task.done()
                and self._connection.is_in_transaction() is True
            )
        except AttributeError, RuntimeError, asyncpg.InterfaceError:
            is_valid = False
        if not is_valid:
            _fail("native_pin_required")

    @property
    def native(self):
        """Return a detached native identity for this exact selected dataset."""
        self._issued()
        return {
            "resource_table_oid": self._native[0],
            "resource_leaf_oids": list(self._native[1]),
            "header_table_oid": self._native[2],
            "database_oid": self._native[3],
            "native_catalog_sha256": self._native[4],
        }

    @property
    def header(self):
        """Return the immutable fixed publication header projection."""
        self._issued()
        return self._header

    @property
    def binding_relation(self):
        """Return the exact authenticated ONLY binding relation."""
        self._issued()
        return "ONLY " + self._scope.bindings.relation.sql

    def resource_sql(self):
        """Return a fixed projection over only authenticated physical leaves."""
        self._issued()
        return _resource_sql(self._entry)

    def _require_selected_descriptor(self, descriptor):
        validate_fhir_catalog_published_descriptor(self._profile, descriptor)
        if (
            descriptor["endpoint_id"] != self._entry.endpoint_id
            or descriptor["coordinates"]["dataset_id"] != self._entry.dataset_id
            or descriptor["resource_table_oid"] != self._native[0]
            or descriptor["resource_leaf_oids"] != list(self._native[1])
            or descriptor["header_table_oid"] != self._native[2]
            or descriptor["database_oid"] != self._native[3]
        ):
            _fail("native_pin_changed")
        if (
            descriptor.get("native_catalog_sha256") != self._native[4]
            or descriptor.get("source_uri") != self.source_uri
            or descriptor.get("previous_dataset_id") != self._header["previous_dataset_id"]
        ):
            _fail("native_catalog_changed")

    async def verify(self, connection, *, schema_name=None, dataset_id=None, descriptor=None):
        """Require this task's original snapshot, locks, authority and selected descriptor."""
        self._issued()
        if (
            connection is not self._connection
            or (schema_name is not None and schema_name != self._profile.schema_name)
            or (dataset_id is not None and dataset_id != self._entry.dataset_id)
        ):
            _fail("native_pin_changed")
        if _scope(self._profile) != self._scope:
            _fail("native_pin_changed")
        try:
            await _require_native_scope(connection, self._profile, self._scope, self._origin, self._allowed)
        except BaseException:
            if context := _owned_context(connection):
                context.discard(_json(self._profile.document()))
            raise
        if descriptor is not None:
            self._require_selected_descriptor(descriptor)
        return self


def _issue_read(connection, profile, scope, entry, origin, validation):
    header, source_uri = validation.publications_by_dataset[entry.dataset_id]
    native = (
        scope.resource_root.relation_oid,
        tuple(pin.relation_oid for pin in entry.resource_leaves),
        scope.header.relation.relation_oid,
        scope.database_oid,
        validation.fingerprint,
    )
    read = FHIRCatalogPublishedRead(
        connection,
        asyncio.current_task(),
        profile,
        scope,
        entry,
        origin,
        native,
        validation.allowed,
        header,
        source_uri,
        entry.source_aliases,
    )
    _ISSUED[read] = tuple(getattr(read, name) for name in read.__dataclass_fields__)
    return read


async def pin_fhir_catalog_published_read(
    connection, profile, *, endpoint_id, dataset_id, require_current=True, descriptor=None
):
    """Authenticate closed physical publication reads without widening any entitlement."""
    scope = _scope(profile)
    entry = _dataset(scope, endpoint_id, dataset_id)
    if type(require_current) is not bool or asyncio.current_task() is None:
        _fail()
    if descriptor is not None:
        validate_fhir_catalog_published_descriptor(profile, descriptor)
    profile_bytes = _json(profile.document())
    context = _owned_context(connection)
    try:
        actor = await _identity(connection, profile, scope)
        origin = _native_origin(actor)
        context = _transaction_context(connection, origin)
        validation = context.scopes_by_profile.get(profile_bytes) if context is not None else None
        is_reused = validation is not None
        if is_reused:
            await _require_native_scope(connection, profile, scope, origin, validation.allowed)
        else:
            validation = await _validate_scope(connection, profile, scope, actor)
        _require_current_header(validation.publications_by_dataset[entry.dataset_id][0], require_current)
        needs_key_proof = entry.dataset_id not in validation.checked_datasets
        if needs_key_proof:
            await _resource_keys(connection, entry)
            validation.checked_datasets.add(entry.dataset_id)
        read = _issue_read(connection, profile, scope, entry, origin, validation)
        if not is_reused or needs_key_proof:
            await read.verify(connection, schema_name=profile.schema_name, dataset_id=dataset_id, descriptor=descriptor)
        elif descriptor is not None:
            read._require_selected_descriptor(descriptor)
        if context is not None and not is_reused:
            context.remember(profile_bytes, validation)
        return read
    except asyncpg.PostgresError as error:
        if context is not None:
            context.discard(profile_bytes)
        raise FHIRCatalogPublishedReadError("registry_fhir_catalog_published_read_unavailable") from error
    except BaseException:
        if context is not None:
            context.discard(profile_bytes)
        raise
