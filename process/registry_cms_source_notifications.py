# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain atomic CMS notifications without granting registry admission or approval."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

from process import provider_directory_cms_serving_receipt as serving
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.network_cms_registry_source_pair import (
    RegistryCMSRetainedSourcePair,
    _sealed_scope,
    decode_registry_cms_source_pair,
    require_registry_cms_source_pair,
    resolve_committed_registry_cms_source_pair,
)
from process.network_membership_writer_closure import _NATIVE_RUNTIME_ROLE_UNSAFE_SQL
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt

CONTRACT = "registry-cms-source-notification/v1"
MAX_NOTIFICATION_BYTES = 70 * 1024
TABLE = "registry_cms_source_notification"
INSERT_COLUMNS = (
    "source_schema",
    "receipt_id",
    "capture_id",
    "publication_xid",
    "profile_generation_id",
    "notification_sha256",
    "notification_json",
)
# Migration ordering and production grants belong to the schema owner. No runtime DDL runs here.
CREATE_TABLE_SQL = """CREATE TABLE {namespace}.registry_cms_source_notification (
 source_schema text NOT NULL, receipt_id text NOT NULL CHECK(receipt_id ~ '^[0-9a-f]{{64}}$'),
 capture_id uuid NOT NULL, publication_xid xid8 NOT NULL, profile_generation_id text NOT NULL,
 notification_sha256 text NOT NULL CHECK(notification_sha256 ~ '^[0-9a-f]{{64}}$'),
 notification_json text NOT NULL CHECK(octet_length(notification_json)<=71680),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_schema,receipt_id),
 CHECK(notification_sha256=encode(sha256(convert_to(notification_json,'UTF8')),'hex'))
)"""


def _invalid():
    raise ValueError("registry_cms_source_notification_invalid")


def _json(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unique(pairs):
    document_by_field = {}
    for name, entry in pairs:
        if name in document_by_field:
            _invalid()
        document_by_field[name] = entry
    return document_by_field


@dataclass(frozen=True)
class RegistryCMSSourceNotification:
    """Closed source evidence; the attempt tag confers no lease or approval authority."""

    source_attempt: RegistryCMSSourceAttempt
    pair: RegistryCMSRetainedSourcePair

    def __post_init__(self):
        if (
            type(self.source_attempt) is not RegistryCMSSourceAttempt
            or type(self.pair) is not RegistryCMSRetainedSourcePair
        ):
            _invalid()
        RegistryCMSSourceAttempt.from_dict(self.source_attempt.as_dict())
        decode_registry_cms_source_pair(self.pair.as_dict())
        if len(self.canonical_json.encode()) > MAX_NOTIFICATION_BYTES:
            _invalid()

    def as_dict(self):
        """Return every retained source-attempt and native pair field."""
        return {
            "contract": CONTRACT,
            "source_schema": self.pair.proof.source_pin.schema_name,
            "receipt_id": self.pair.proof.serving_receipt_id,
            "capture_id": str(self.pair.recipe.source_pin.retained_epoch.epoch_id),
            "source_attempt": self.source_attempt.as_dict(),
            "pair": self.pair.as_dict(),
        }

    @property
    def canonical_json(self):
        """Encode the closed notification with one stable JSON representation."""
        return _json(self.as_dict())

    @property
    def sha256(self):
        """Bind the complete attempt and pair to the stored notification digest."""
        return hashlib.sha256(self.canonical_json.encode()).hexdigest()


def decode_registry_cms_source_notification(encoded, digest):
    """Decode bounded canonical bytes; every consumer must still revalidate native custody."""
    try:
        if type(encoded) is not str or len(encoded.encode()) > MAX_NOTIFICATION_BYTES:
            _invalid()
        if type(digest) is not str or hashlib.sha256(encoded.encode()).hexdigest() != digest:
            _invalid()
        document = json.loads(encoded, object_pairs_hook=_unique)
        expected_fields = {"contract", "source_schema", "receipt_id", "capture_id", "source_attempt", "pair"}
        if type(document) is not dict or set(document) != expected_fields or document["contract"] != CONTRACT:
            _invalid()
        notification = RegistryCMSSourceNotification(
            RegistryCMSSourceAttempt.from_dict(document["source_attempt"]),
            decode_registry_cms_source_pair(document["pair"]),
        )
        if notification.as_dict() != document or notification.canonical_json != encoded:
            _invalid()
        return notification
    except ValueError, TypeError, KeyError, RecursionError, AttributeError:
        _invalid()


_PRIVILEGES_SQL = """WITH closure_scope AS (
 SELECT n.oid AS schema_oid,o.oid AS owner_oid,$3::text[] AS role_names
 FROM pg_namespace n JOIN pg_roles o ON o.rolname=$2 WHERE n.nspname=$1
), runtime_roles AS (
 SELECT r.* FROM closure_scope s CROSS JOIN unnest(s.role_names) configured(name)
 LEFT JOIN pg_roles r ON r.rolname=configured.name
), target AS (
 SELECT c.* FROM pg_class c JOIN closure_scope s ON c.relnamespace=s.schema_oid
 WHERE c.relname='registry_cms_source_notification'
), acl AS (
 SELECT a.grantee,a.privilege_type,a.is_grantable,NULL::text AS column_name FROM target c
 CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,att.attname FROM target c
 JOIN pg_attribute att ON att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped
 CROSS JOIN LATERAL aclexplode(att.attacl) a
)
SELECT EXISTS(SELECT 1 FROM target c CROSS JOIN closure_scope s JOIN pg_roles o ON o.oid=s.owner_oid
 WHERE c.relkind='r' AND c.relpersistence='p' AND c.relowner=s.owner_oid
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relhastriggers AND NOT c.relhasrules
 AND NOT o.rolcanlogin AND NOT o.rolsuper AND NOT o.rolcreatedb AND NOT o.rolcreaterole
 AND NOT o.rolreplication AND NOT o.rolbypassrls)
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s WHERE {unsafe}
 OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 OR has_parameter_privilege(r.oid,'session_replication_role','SET'))
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s
 WHERE has_schema_privilege(r.oid,s.schema_oid,'CREATE') OR NOT has_schema_privilege(r.oid,s.schema_oid,'USAGE'))
 AND NOT EXISTS(SELECT 1 FROM acl a CROSS JOIN closure_scope s WHERE a.grantee<>s.owner_oid AND
   (a.is_grantable OR NOT(a.grantee=ANY(SELECT oid FROM runtime_roles))
    OR NOT(a.privilege_type='SELECT' OR (a.grantee=(SELECT oid FROM pg_roles WHERE rolname=$4)
      AND a.privilege_type='INSERT' AND a.column_name=ANY($5::text[])))))
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN target c
 WHERE NOT has_table_privilege(r.oid,c.oid,'SELECT')
 OR has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR has_any_column_privilege(r.oid,c.oid,'UPDATE,REFERENCES'))
 AND (SELECT bool_and(has_column_privilege($4::name,c.oid,name,'INSERT'))
 FROM target c CROSS JOIN unnest($5::text[]) columns(name))
""".format(unsafe=_NATIVE_RUNTIME_ROLE_UNSAFE_SQL)


@dataclass(frozen=True)
class RegistryCMSSourceNotificationReader:
    """Fixed read capability with no publication attempt or notification writer."""

    control_schema: str
    owner_role: str
    publisher_role: str
    reader_roles: tuple[str, ...] = ()

    def __post_init__(self):
        for name in (self.control_schema, self.owner_role, self.publisher_role):
            _identifier(name)
        if type(self.reader_roles) is not tuple or len(self.reader_roles) > 32:
            _invalid()
        for name in self.reader_roles:
            _identifier(name)
        if self.owner_role in (self.publisher_role, *self.reader_roles) or len(set(self.reader_roles)) != len(
            self.reader_roles
        ):
            _invalid()

    async def _connection(self, session, *, writing):
        if writing:
            _invalid()
        return await _notification_connection(self, session, writing=False)

    async def read(self, session, *, source_schema, receipt_id):
        """Reprove committed source evidence under the fixed native reader profile."""
        return await _read_notification(self, session, source_schema, receipt_id)

    async def matching_headers(self, session, coordinates):
        """Discover a bounded exact six-coordinate capture, never a latest alias."""
        if type(coordinates) is not RegistryNetworkSourceCoordinates:
            _invalid()
        connection = await self._connection(session, writing=False)
        headers = await connection.fetch(
            f"SELECT receipt_id,octet_length(notification_json) AS raw_byte_count "
            f"FROM {_identifier(self.control_schema)}.{TABLE} "
            "WHERE source_schema=$1 AND notification_json::jsonb#>'{pair,proof,binding_coordinates}'=$2::jsonb "
            'ORDER BY receipt_id COLLATE "C" LIMIT 101',
            coordinates.dataset_schema,
            _json(asdict(coordinates)),
        )
        if len(headers) > 100 or any(not 1 <= header["raw_byte_count"] <= MAX_NOTIFICATION_BYTES for header in headers):
            _invalid()
        return tuple(dict(header) for header in headers)


@dataclass(frozen=True)
class RegistryCMSSourceNotificationStore:
    """Bind the callback to authenticated bootstrap context, never a decoded caller tag."""

    control_schema: str
    owner_role: str
    publisher_role: str
    source_attempt: RegistryCMSSourceAttempt
    reader_roles: tuple[str, ...] = ()

    def __post_init__(self):
        RegistryCMSSourceNotificationReader(
            self.control_schema, self.owner_role, self.publisher_role, self.reader_roles
        )
        if type(self.source_attempt) is not RegistryCMSSourceAttempt:
            _invalid()
        RegistryCMSSourceAttempt.from_dict(self.source_attempt.as_dict())

    async def _connection(self, session, *, writing):
        return await _notification_connection(self, session, writing=writing)

    async def notify_receipt(self, session, *, receipt_id, pair):
        """Append only a bound pair from this exact native publication transaction.

        The configured attempt must come from the Source bootstrap's authenticated
        lease context. A closed tag is retained evidence, not operational authority.
        """
        notification = RegistryCMSSourceNotification(self.source_attempt, pair)
        if receipt_id != pair.proof.serving_receipt_id:
            _invalid()
        connection = await self._connection(session, writing=True)
        if await connection.fetchval("SHOW transaction_isolation") == "read committed":
            from process.provider_directory_cms_source_runtime import (
                require_registry_notification_publication,
            )

            await require_registry_notification_publication(session, self.source_attempt, pair)
        await _require_current_publication(session, connection, pair)
        namespace = _identifier(self.control_schema)
        values = _row_values(notification)
        await connection.execute(
            f"INSERT INTO {namespace}.{TABLE} ({','.join(INSERT_COLUMNS)}) VALUES($1,$2,$3,$4::text::xid8,$5,$6,$7) ON CONFLICT(source_schema,receipt_id) DO NOTHING",
            *values,
        )
        stored = await _read_row(connection, namespace, values[0], receipt_id)
        if stored is None or tuple(stored[column] for column in INSERT_COLUMNS) != values:
            _invalid()
        return notification

    async def read(self, session, *, source_schema, receipt_id):
        """Load closed committed evidence and revalidate it; do not approve or enqueue it."""
        return await _read_notification(self, session, source_schema, receipt_id)


async def _notification_connection(configuration, session, *, writing):
    connection = await native_driver(session)
    roles = tuple(sorted({configuration.publisher_role, *configuration.reader_roles}))
    if (
        not connection.is_in_transaction()
        or await connection.fetchval("SHOW transaction_isolation")
        not in (
            {"read committed", "repeatable read", "serializable"} if writing else {"repeatable read", "serializable"}
        )
        or await connection.fetchval("SHOW transaction_read_only") != ("off" if writing else "on")
        or await connection.fetchval("SELECT current_user")
        not in ((configuration.publisher_role,) if writing else roles)
        or not await connection.fetchval(
            _PRIVILEGES_SQL,
            configuration.control_schema,
            configuration.owner_role,
            roles,
            configuration.publisher_role,
            INSERT_COLUMNS,
        )
    ):
        _invalid()
    return connection


async def _read_notification(reader, session, source_schema, receipt_id):
    _identifier(source_schema)
    if type(receipt_id) is not str or re.fullmatch(r"[0-9a-f]{64}", receipt_id) is None:
        _invalid()
    connection = await reader._connection(session, writing=False)
    row = await _read_row(connection, _identifier(reader.control_schema), source_schema, receipt_id)
    if row is None:
        _invalid()
    notification = decode_registry_cms_source_notification(row["notification_json"], row["notification_sha256"])
    if tuple(row[column] for column in INSERT_COLUMNS) != _row_values(notification):
        _invalid()
    resolved = await resolve_committed_registry_cms_source_pair(session, notification.pair.recipe)
    if resolved.pair != notification.pair:
        _invalid()
    return notification


def _row_values(notification):
    pair = notification.pair
    return (
        pair.proof.source_pin.schema_name,
        pair.proof.serving_receipt_id,
        UUID(str(pair.recipe.source_pin.retained_epoch.epoch_id)),
        pair.proof.publication_xid,
        pair.proof.profile_generation_id,
        notification.sha256,
        notification.canonical_json,
    )


async def _read_row(connection, namespace, source_schema, receipt_id):
    return await connection.fetchrow(
        f"SELECT source_schema,receipt_id,capture_id,publication_xid::text AS publication_xid,profile_generation_id,"
        f"notification_sha256,CASE WHEN octet_length(notification_json)<=$3 THEN notification_json END AS notification_json "
        f"FROM {namespace}.{TABLE} WHERE source_schema=$1 AND receipt_id=$2",
        source_schema,
        receipt_id,
        MAX_NOTIFICATION_BYTES,
    )


async def _require_current_publication(session, connection, pair):
    proof = pair.proof
    namespace = _identifier(proof.source_pin.schema_name)
    row = await connection.fetchrow(
        f"SELECT payload::text,publication_xid::text AS publication_xid,profile_generation_id FROM {namespace}.provider_directory_cms_serving_receipt WHERE receipt_id=$1 AND publication_xid=pg_current_xact_id()",
        proof.serving_receipt_id,
    )
    payload = json.loads(pair.cms_receipt_payload)
    if (
        row is None
        or row["publication_xid"] != proof.publication_xid
        or row["profile_generation_id"] != proof.profile_generation_id
        or json.loads(row["payload"]) != payload
    ):
        _invalid()
    # The binder holds native cutover locks for this same publication transaction.
    await serving.capture_native_dependencies(session, proof.source_pin.schema_name)
    if await serving.read_current_receipt(session, proof.source_pin.schema_name) != {
        "receipt_id": proof.serving_receipt_id,
        "payload": payload,
    }:
        _invalid()
    await _sealed_scope(connection, proof)
    await require_registry_cms_source_pair(session, pair, verify_content=False)


async def plan_registry_cms_source_notification_grants(
    connection, *, control_schema, owner_role, publisher_role, reader_roles=()
):
    """Return operator-only SQL after native role/ACL preflight; execute no DDL.

    Apply the returned statements in this same explicit operator transaction.
    No role is created or altered, and unknown or unsafe existing grants refuse.
    """
    for name in (control_schema, owner_role, publisher_role):
        _identifier(name)
    if type(reader_roles) is not tuple or len(reader_roles) > 32 or len(set(reader_roles)) != len(reader_roles):
        _invalid()
    for name in reader_roles:
        _identifier(name)
    roles = tuple(sorted({publisher_role, *reader_roles}))
    if owner_role in roles or not connection.is_in_transaction():
        _invalid()
    if not await connection.fetchval(
        _OPERATOR_PREFLIGHT_SQL, control_schema, owner_role, roles, publisher_role, INSERT_COLUMNS
    ):
        _invalid()
    table = _identifier(control_schema) + "." + TABLE
    configured = ",".join(_identifier(role) for role in roles)
    columns = ",".join(_identifier(column) for column in INSERT_COLUMNS)
    return (
        f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE",
        f"ALTER TABLE {table} OWNER TO {_identifier(owner_role)}",
        f"REVOKE ALL ON {table} FROM PUBLIC",
        f"GRANT USAGE ON SCHEMA {_identifier(control_schema)} TO {configured}",
        f"GRANT SELECT ON {table} TO {configured}",
        f"GRANT INSERT ({columns}) ON {table} TO {_identifier(publisher_role)}",
    )


_OPERATOR_PREFLIGHT_SQL = """WITH closure_scope AS (
 SELECT n.oid AS schema_oid,o.oid AS owner_oid,$3::text[] AS role_names
 FROM pg_namespace n JOIN pg_roles o ON o.rolname=$2 WHERE n.nspname=$1
), runtime_roles AS (
 SELECT r.* FROM closure_scope s CROSS JOIN unnest(s.role_names) configured(name)
 LEFT JOIN pg_roles r ON r.rolname=configured.name
), target AS (
 SELECT c.* FROM pg_class c JOIN closure_scope s ON c.relnamespace=s.schema_oid
 WHERE c.relname='registry_cms_source_notification'
), acl AS (
 SELECT a.grantee,a.privilege_type,a.is_grantable,NULL::text AS column_name FROM target c
 CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 UNION ALL SELECT a.grantee,a.privilege_type,a.is_grantable,att.attname FROM target c
 JOIN pg_attribute att ON att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped
 CROSS JOIN LATERAL aclexplode(att.attacl) a
)
SELECT EXISTS(SELECT 1 FROM target c CROSS JOIN closure_scope s JOIN pg_roles o ON o.oid=s.owner_oid
 WHERE c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND NOT c.relhastriggers AND NOT c.relhasrules
 AND NOT o.rolcanlogin AND NOT o.rolsuper AND NOT o.rolcreatedb AND NOT o.rolcreaterole
 AND NOT o.rolreplication AND NOT o.rolbypassrls)
 AND NOT EXISTS(SELECT 1 FROM runtime_roles r CROSS JOIN closure_scope s WHERE {unsafe}
 OR r.rolcreatedb OR r.rolreplication OR r.rolbypassrls
 OR has_parameter_privilege(r.oid,'session_replication_role','SET')
 OR has_schema_privilege(r.oid,s.schema_oid,'CREATE'))
 AND NOT EXISTS(SELECT 1 FROM acl a CROSS JOIN target c CROSS JOIN closure_scope s
 WHERE a.grantee NOT IN (c.relowner,s.owner_oid) AND
   (a.is_grantable OR NOT(a.grantee=ANY(SELECT oid FROM runtime_roles))
    OR NOT(a.privilege_type='SELECT' OR (a.grantee=(SELECT oid FROM pg_roles WHERE rolname=$4)
      AND a.privilege_type='INSERT' AND a.column_name=ANY($5::text[])))))
""".format(unsafe=_NATIVE_RUNTIME_ROLE_UNSAFE_SQL)
