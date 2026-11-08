# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Discover bounded committed office approval/hold events without selecting bindings."""

import json
import re
from uuid import UUID

from process.network_address_projection import _identifier
from process.registry_ptg_office_capture import _digest
from process.registry_ptg_office_membership_contract import approved_office_recipe_receipt
from process.registry_ptg_office_selection import _checked_pair, _protected_tables
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore

MAX_PAGE_ROWS = 25
_OIDS = {
    "database_oid",
    "schema_oid",
    "approval_table_oid",
    "hold_table_oid",
    "owner_oid",
    "approval_role_oid",
    "reader_oid",
}
_NAMES = {"control_schema", "owner_role", "approval_role", "reader_role"}
_IDENTITY_FIELDS = _OIDS | _NAMES | {"database_name"}
_IDENTITY_SQL = """SELECT current_database() AS database_name,d.oid::bigint AS database_oid,
 n.nspname::text AS control_schema,n.oid::bigint AS schema_oid,
 a.oid::bigint AS approval_table_oid,h.oid::bigint AS hold_table_oid,
 o.rolname::text AS owner_role,o.oid::bigint AS owner_oid,
 w.rolname::text AS approval_role,w.oid::bigint AS approval_role_oid,
 r.rolname::text AS reader_role,r.oid::bigint AS reader_oid
 FROM pg_database d JOIN pg_namespace n ON n.nspname=$1
 JOIN pg_roles o ON o.rolname=$2 JOIN pg_roles w ON w.rolname=$3
 JOIN pg_roles r ON r.rolname=$4
 JOIN pg_class a ON a.relnamespace=n.oid AND a.relname='registry_ptg_office_approval'
 JOIN pg_class h ON h.relnamespace=n.oid AND h.relname='registry_ptg_office_hold'
 WHERE d.datname=current_database() AND current_user=session_user AND current_user=r.rolname
 AND current_setting('role')='none' AND r.rolcanlogin
 AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole
 AND NOT r.rolreplication AND NOT r.rolbypassrls
 AND NOT has_parameter_privilege(r.oid,'session_replication_role','SET')
 AND NOT w.rolsuper AND NOT w.rolcreatedb AND NOT w.rolcreaterole
 AND NOT w.rolreplication AND NOT w.rolbypassrls
 AND n.nspowner=o.oid AND a.relowner=o.oid AND h.relowner=o.oid"""
_COLUMNS = """a.capture_id::text AS capture_id,a.actor_key,a.idempotency_key,a.approval_sha256,
 h.hold_sha256,
 CASE WHEN octet_length(a.approval_json::text)<=131072 THEN a.approval_json::text END AS approval_json,
 CASE WHEN octet_length(h.hold_json::text)<=32768 THEN h.hold_json::text END AS hold_json"""


def _invalid():
    raise ValueError("registry_ptg_source_notification_invalid")


def _key(value):
    if type(value) is not str:
        _invalid()
    try:
        if not UUID(value).int or str(UUID(value)) != value:
            _invalid()
    except ValueError, AttributeError:
        _invalid()
    return value


def _identity(value):
    if type(value) is not dict or set(value) != _IDENTITY_FIELDS:
        _invalid()
    for field in _OIDS:
        if type(value[field]) is not int or not 0 < value[field] < 2**32:
            _invalid()
    for field in _NAMES:
        _identifier(value[field])
    if (
        type(value["database_name"]) is not str
        or not 1 <= len(value["database_name"].encode()) <= 63
        or "\0" in value["database_name"]
        or len({value["owner_oid"], value["approval_role_oid"], value["reader_oid"]}) != 3
        or value["approval_table_oid"] == value["hold_table_oid"]
    ):
        _invalid()
    return dict(value)


async def qualify_registry_ptg_source_notifications(connection, store, *, reader_role):
    """Attest the original ordinary LOGIN and both protected tables in caller-owned RR/RO."""
    if type(store) is not RegistryPTGProducerScopeStore or store.control_schema is None:
        _invalid()
    for role in (store.owner_role, store.approval_role, reader_role):
        _identifier(role)
    namespace = _identifier(store.control_schema)
    if (
        len({store.owner_role, store.approval_role, reader_role}) != 3
        or not connection.is_in_transaction()
        or await connection.fetchval("SHOW transaction_isolation") != "repeatable read"
        or await connection.fetchval("SHOW transaction_read_only") != "on"
    ):
        _invalid()
    await _protected_tables(connection, namespace, store)
    row = await connection.fetchrow(
        _IDENTITY_SQL, store.control_schema, store.owner_role, store.approval_role, reader_role
    )
    return _identity(dict(row)) if row is not None else _invalid()


def _event(event_by_name):
    """Prove the retained pair against its own command, never a current selected binding."""
    try:
        capture = _key(event_by_name["capture_id"])
        for field in ("approval_sha256", "hold_sha256", "actor_key"):
            if type(event_by_name[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", event_by_name[field]) is None:
                _invalid()
        for field, maximum in (("approval_json", 131072), ("hold_json", 32768)):
            if type(event_by_name[field]) is not str or len(event_by_name[field].encode()) > maximum:
                _invalid()
        document = json.loads(event_by_name["approval_json"])
        if type(document) is not dict or set(document) != {
            "contract",
            "capture_id",
            "client_id",
            "actor",
            "command",
            "manifest_sha256",
            "witness",
            "source_pin",
        }:
            _invalid()
        actor = document["actor"]
        if (
            type(actor) is not dict
            or set(actor) != {"kind", "user_id", "client_id"}
            or actor["kind"] != "platform_admin"
            or actor["client_id"] != "system"
            or _key(actor["user_id"]) != actor["user_id"]
            or _digest(actor) != event_by_name["actor_key"]
            or document["capture_id"] != capture
            or event_by_name["idempotency_key"] != document["command"]["idempotency_key"]
        ):
            _invalid()
        command = document["command"]
        receipt = approved_office_recipe_receipt(
            command, document["manifest_sha256"], event_by_name["approval_sha256"], event_by_name["hold_sha256"]
        )
        source_by_name = command["source"]
        comparison_by_name = {
            **source_by_name["coordinates"],
            "source_key": source_by_name["binding_source_key"],
            "network_id": source_by_name["network_id"],
            "source_scope_json": source_by_name["source_scope"],
            "office_recipe_json": {"capture_id": capture, "recipe_sha256": receipt["source_recipes_sha256"]},
        }
        _checked_pair(event_by_name, comparison_by_name)
        return {name: event_by_name[name] for name in ("capture_id", "approval_sha256", "hold_sha256")}
    except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError:
        _invalid()


async def read_registry_ptg_source_notification_headers(
    connection, store, *, reader_role, identity, after=None, upper=None, limit=MAX_PAGE_ROWS
):
    """Read one finite UUID interval; reset both cursors after completion to see late commits."""
    expected = _identity(identity)
    if type(limit) is not int or not 1 <= limit <= MAX_PAGE_ROWS or after is not None and upper is None:
        _invalid()
    after = _key(after) if after is not None else None
    upper = _key(upper) if upper is not None else None
    if after is not None and after > upper:
        _invalid()
    if await qualify_registry_ptg_source_notifications(connection, store, reader_role=reader_role) != expected:
        _invalid()
    namespace = _identifier(store.control_schema)
    approval = namespace + ".registry_ptg_office_approval"
    if upper is None:
        last = await connection.fetchrow(
            f"SELECT capture_id::text AS capture_id FROM {approval} a ORDER BY a.capture_id DESC LIMIT 1"
        )
        upper = _key(last["capture_id"]) if last is not None else None
    headers = []
    if upper is not None:
        event_rows = await connection.fetch(
            f"SELECT {_COLUMNS} FROM {approval} a LEFT JOIN {namespace}.registry_ptg_office_hold h "
            "ON (h.capture_id,h.approval_sha256)=(a.capture_id,a.approval_sha256) "
            "WHERE ($1::uuid IS NULL OR a.capture_id>$1) AND a.capture_id<=$2::uuid ORDER BY a.capture_id LIMIT $3",
            UUID(after) if after is not None else None,
            UUID(upper),
            limit,
        )
        if len(event_rows) > limit:
            _invalid()
        previous = after
        for event_by_name in event_rows:
            event = _event(event_by_name)
            if previous is not None and event["capture_id"] <= previous or event["capture_id"] > upper:
                _invalid()
            headers.append(event)
            previous = event["capture_id"]
    if await qualify_registry_ptg_source_notifications(connection, store, reader_role=reader_role) != expected:
        _invalid()
    final = headers[-1]["capture_id"] if headers else after
    return {"headers": headers, "upper": upper, "after": final, "complete": len(headers) < limit or final == upper}


def validate_registry_ptg_source_event(event):
    """An event identifies one accepted pair; it cannot carry source or runtime options."""
    if type(event) is not dict or set(event) != {"capture_id", "approval_sha256", "hold_sha256"}:
        _invalid()
    _key(event["capture_id"])
    for name in ("approval_sha256", "hold_sha256"):
        if type(event[name]) is not str or re.fullmatch(r"[0-9a-f]{64}", event[name]) is None:
            _invalid()


async def read_registry_ptg_source_notification(connection, store, *, reader_role, identity, event):
    """Read one exact accepted pair under the same qualified native reader identity."""
    from process.registry_source_recipe_store import decode_registry_source_recipes

    validate_registry_ptg_source_event(event)
    expected = _identity(identity)
    if await qualify_registry_ptg_source_notifications(connection, store, reader_role=reader_role) != expected:
        _invalid()
    namespace = _identifier(store.control_schema)
    row = await connection.fetchrow(
        f"SELECT {_COLUMNS} FROM {namespace}.registry_ptg_office_approval a "
        f"LEFT JOIN {namespace}.registry_ptg_office_hold h "
        "ON (h.capture_id,h.approval_sha256)=(a.capture_id,a.approval_sha256) WHERE a.capture_id=$1::uuid",
        UUID(event["capture_id"]),
    )
    if row is None or _event(row) != event:
        _invalid()
    document = json.loads(row["approval_json"])
    receipt = approved_office_recipe_receipt(
        document["command"], document["manifest_sha256"], row["approval_sha256"], row["hold_sha256"]
    )
    recipe = decode_registry_source_recipes(receipt["source_recipes"])[0]
    if await qualify_registry_ptg_source_notifications(connection, store, reader_role=reader_role) != expected:
        _invalid()
    return recipe
