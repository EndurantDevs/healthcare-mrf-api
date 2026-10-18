# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic accepted ACA receipts and retention holds for protected frozen exports."""

import hashlib
import json
import re
from dataclasses import dataclass
from uuid import UUID

from db.registry_aca_source_notification import CONTRACT, CREATE_TABLE_SQL, INSERT_COLUMNS, MAX_BYTES, TABLE
from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.network_legacy_membership_source import PinnedACAMembershipSource
from process.network_membership_writer_closure import _protected_owner
from process.registry_aca_global_epoch import (
    PinnedACAGlobalEpoch,
    decode_pinned_aca_global_epoch,
    require_registry_aca_global_epoch,
)
from process.registry_aca_source_notification_permissions import _RUNTIME_SQL
from process.registry_aca_source_recipe import require_registry_aca_source_recipe
from process.registry_cms_source_notifications import _PRIVILEGES_SQL as _CMS_ACL_SQL
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)

EPOCH_CONTRACT = "registry-aca-source-notification/v2"

_ACL_SQL = _CMS_ACL_SQL.replace("registry_cms_source_notification", TABLE)
_IDENTITY_SQL = """SELECT n.oid::bigint AS schema_oid,n.nspowner::bigint AS owner_oid,o.*,
 t.oid::bigint AS inbox_oid,h.oid::bigint AS head_oid,
 current_database() AS database_name,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid
 FROM pg_namespace n JOIN pg_roles o ON o.oid=n.nspowner
 JOIN pg_class h ON h.relnamespace=n.oid AND h.relname='network_serving_control' AND h.relowner=n.nspowner
 JOIN pg_class t ON t.relnamespace=n.oid AND t.relname='registry_aca_source_notification' AND t.relowner=n.nspowner
 WHERE n.nspname=$1 AND current_user=session_user AND current_setting('role')='none'"""


def _invalid():
    raise ValueError("registry_aca_source_notification_invalid")


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def validate_registry_aca_source_event(event):
    """An event supplies only one exact retained receipt, never mapping or authority."""
    if type(event) is not dict or set(event) != {"capture_id", "notification_sha256", "recipe_sha256"}:
        _invalid()
    try:
        if (
            type(event["capture_id"]) is not str
            or str(UUID(event["capture_id"])) != event["capture_id"]
            or not UUID(event["capture_id"]).int
        ):
            _invalid()
    except ValueError, AttributeError:
        _invalid()
    for field in ("notification_sha256", "recipe_sha256"):
        if type(event[field]) is not str or re.fullmatch("[0-9a-f]{64}", event[field]) is None:
            _invalid()
    return dict(event)


def _subject_payload(subject):
    if type(subject) is PinnedACAGlobalEpoch:
        return "epoch_json", _json(subject.as_dict())
    if type(subject) is not RegistrySourceMembershipRecipe or type(subject.source_pin) is not PinnedACAMembershipSource:
        _invalid()
    return "recipe_json", canonical_registry_source_recipes((subject,))


def _subject_pin(subject):
    if type(subject) is PinnedACAGlobalEpoch:
        return subject
    if type(subject) is not RegistrySourceMembershipRecipe or type(subject.source_pin) is not PinnedACAMembershipSource:
        _invalid()
    return subject.source_pin


async def _require_subject(session, subject):
    pin = _subject_pin(subject)
    verifier = (
        require_registry_aca_global_epoch
        if type(subject) is PinnedACAGlobalEpoch
        else require_registry_aca_source_recipe
    )
    actual = await verifier(session, pin.prepared, runtime_roles=pin.runtime_roles)
    if actual != subject:
        _invalid()
    return actual


def _decode(encoded, digest):
    if type(encoded) is not str or len(encoded.encode()) > MAX_BYTES or _digest(encoded) != digest:
        _invalid()
    document = json.loads(encoded)
    if type(document) is not dict or _json(document) != encoded:
        _invalid()
    if document.get("contract") == EPOCH_CONTRACT:
        payload_key = "epoch_json"
    elif document.get("contract") == CONTRACT:
        payload_key = "recipe_json"
    else:
        _invalid()
    if set(document) != {"contract", "capture_id", payload_key, "publication_xid", "producer_role"}:
        _invalid()
    if type(document[payload_key]) is not str:
        _invalid()
    if payload_key == "epoch_json":
        subject = decode_pinned_aca_global_epoch(document[payload_key])
    else:
        recipes = decode_registry_source_recipes(document[payload_key])
        if len(recipes) != 1 or type(recipes[0].source_pin) is not PinnedACAMembershipSource:
            _invalid()
        subject = recipes[0]
    if document["capture_id"] != str(_subject_pin(subject).prepared.ownership.dataset_id) or _subject_payload(
        subject
    ) != (payload_key, document[payload_key]):
        _invalid()
    if (
        type(document["publication_xid"]) is not str
        or re.fullmatch("[1-9][0-9]{0,19}", document["publication_xid"]) is None
    ):
        _invalid()
    _identifier(document["producer_role"])
    return document, subject


def _values(document, subject):
    ownership = _subject_pin(subject).prepared.ownership
    encoded = _json(document)
    payload_key, _ = _subject_payload(subject)
    if len(encoded.encode()) > MAX_BYTES:
        _invalid()
    return (
        ownership.dataset_id,
        ownership.schema_name,
        ownership.schema_oid,
        _digest(document[payload_key]),
        _digest(encoded),
        encoded,
    )


@dataclass(frozen=True)
class RegistryACASourceNotificationStore:
    """Use restart-bound actors; the protected native registry determines its owner."""

    control_schema: str
    publisher_role: str
    reader_roles: tuple[str, ...]

    def __post_init__(self):
        for role in (self.control_schema, self.publisher_role):
            _identifier(role)
        if (
            type(self.reader_roles) is not tuple
            or not 1 <= len(self.reader_roles) <= 32
            or tuple(sorted(set(self.reader_roles))) != self.reader_roles
        ):
            _invalid()
        for role in self.reader_roles:
            _identifier(role)

    async def qualify(self, connection, *, writing):
        """Require the original actor, protected native owner and closed bounded grants."""
        if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in (
            {"read committed", "repeatable read", "serializable"} if writing else {"repeatable read", "serializable"}
        ):
            _invalid()
        if await connection.fetchval("SHOW transaction_read_only") != ("off" if writing else "on"):
            _invalid()
        actor = await connection.fetchval("SELECT current_user")
        if actor not in ((self.publisher_role,) if writing else self.reader_roles):
            _invalid()
        identity_by_field = await connection.fetchrow(_IDENTITY_SQL, self.control_schema)
        roles = tuple(sorted({self.publisher_role, *self.reader_roles}))
        if (
            identity_by_field is None
            or not _protected_owner(identity_by_field)
            or not await connection.fetchval(
                _ACL_SQL, self.control_schema, identity_by_field["rolname"], roles, self.publisher_role, INSERT_COLUMNS
            )
            or not await connection.fetchval(
                _RUNTIME_SQL,
                roles,
                identity_by_field["owner_oid"],
                self.control_schema,
                self.publisher_role,
                self.reader_roles,
                INSERT_COLUMNS,
            )
        ):
            _invalid()
        return {
            name: identity_by_field[name]
            for name in ("schema_oid", "owner_oid", "inbox_oid", "head_oid", "database_name", "database_oid")
        }

    async def append(self, session, recipe):
        """Persist accepted evidence and its indefinite hold in the clone's original transaction."""
        connection = await native_driver(session)
        await self.qualify(connection, writing=True)
        await _require_subject(session, recipe)
        pin = _subject_pin(recipe)
        existing = await self._row(connection, pin.prepared.ownership.dataset_id)
        if existing is not None:
            return await self._checked(session, existing, expected=recipe)
        payload_key, payload_json = _subject_payload(recipe)
        notification_map = {
            "contract": EPOCH_CONTRACT if type(recipe) is PinnedACAGlobalEpoch else CONTRACT,
            "capture_id": str(pin.prepared.ownership.dataset_id),
            payload_key: payload_json,
            "publication_xid": await connection.fetchval("SELECT pg_current_xact_id()::text"),
            "producer_role": self.publisher_role,
        }
        values = _values(notification_map, recipe)
        await connection.execute(
            f"INSERT INTO {_identifier(self.control_schema)}.{TABLE} ({','.join(INSERT_COLUMNS)}) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(capture_id) DO NOTHING",
            *values,
        )
        stored = await self._row(connection, values[0])
        if stored is None or tuple(stored[field] for field in INSERT_COLUMNS) != values:
            _invalid()
        return await self._checked(session, stored, expected=recipe)

    async def _row(self, connection, capture_id):
        return await connection.fetchrow(
            f"SELECT capture_id,source_schema,source_schema_oid::bigint AS source_schema_oid,recipe_sha256,notification_sha256,CASE WHEN octet_length(notification_json)<=$2 THEN notification_json END AS notification_json FROM {_identifier(self.control_schema)}.{TABLE} WHERE capture_id=$1",
            capture_id,
            MAX_BYTES,
        )

    async def _checked(self, session, row, *, expected=None):
        document, recipe = _decode(row["notification_json"], row["notification_sha256"])
        if (
            tuple(row[field] for field in INSERT_COLUMNS) != _values(document, recipe)
            or document["producer_role"] != self.publisher_role
            or (expected is not None and expected != recipe)
        ):
            _invalid()
        await _require_subject(session, recipe)
        return {
            "event": validate_registry_aca_source_event(
                {
                    "capture_id": str(row["capture_id"]),
                    "recipe_sha256": row["recipe_sha256"],
                    "notification_sha256": row["notification_sha256"],
                }
            ),
            "epoch" if type(recipe) is PinnedACAGlobalEpoch else "recipe": recipe,
        }

    async def read(self, session, event, *, holding=False):
        """Reprove an exact accepted receipt; a cleanup caller may only retain it."""
        event = validate_registry_aca_source_event(event)
        connection = await native_driver(session)
        await self.qualify(connection, writing=holding)
        row = await self._row(connection, UUID(event["capture_id"]))
        if row is None or any(row[field] != event[field] for field in ("recipe_sha256", "notification_sha256")):
            _invalid()
        return await self._checked(session, row)

    async def accepted(self, session, capture_id, *, holding=False):
        """Return an existing native-proven receipt without inventing a new event."""
        if type(capture_id) is UUID:
            capture_id = str(capture_id)
        event = validate_registry_aca_source_event(
            {
                "capture_id": capture_id,
                "recipe_sha256": "a" * 64,
                "notification_sha256": "a" * 64,
            }
        )
        connection = await native_driver(session)
        await self.qualify(connection, writing=holding)
        persisted = await self._row(connection, UUID(event["capture_id"]))
        return None if persisted is None else await self._checked(session, persisted)

    async def hold(self, session, prepared):
        """Refuse ordinary export DROP while the immutable accepted row owns retention."""
        connection = await native_driver(session)
        await self.qualify(connection, writing=True)
        row = await self._row(connection, prepared.ownership.dataset_id)
        if row is None:
            _invalid()
        result = await self._checked(session, row)
        subject = result["epoch"] if "epoch" in result else result["recipe"]
        if _subject_pin(subject).prepared != prepared:
            _invalid()
        return result

    async def headers(self, connection, identity, *, after=None, upper=None, limit=25):
        """Scan a closed finite UUID interval; restart a full interval after completion."""
        if type(limit) is not int or not 1 <= limit <= 25 or (after is not None and upper is None):
            _invalid()
        if await self.qualify(connection, writing=False) != identity:
            _invalid()
        for key in (after, upper):
            if key is not None:
                validate_registry_aca_source_event(
                    {"capture_id": key, "recipe_sha256": "a" * 64, "notification_sha256": "a" * 64}
                )
        table = _identifier(self.control_schema) + "." + TABLE
        if upper is None:
            upper = await connection.fetchval(f"SELECT capture_id::text FROM {table} ORDER BY capture_id DESC LIMIT 1")
        header_rows = (
            []
            if upper is None
            else await connection.fetch(
                f"SELECT capture_id::text AS capture_id,recipe_sha256,notification_sha256 FROM {table} WHERE ($1::uuid IS NULL OR capture_id>$1) AND capture_id<=$2::uuid ORDER BY capture_id LIMIT $3",
                UUID(after) if after else None,
                UUID(upper),
                limit,
            )
        )
        headers = [validate_registry_aca_source_event(dict(header_row)) for header_row in header_rows]
        previous = after
        for header in headers:
            if (previous is not None and header["capture_id"] <= previous) or header["capture_id"] > upper:
                _invalid()
            previous = header["capture_id"]
        if len(headers) > limit or await self.qualify(connection, writing=False) != identity:
            _invalid()
        return {
            "headers": headers,
            "upper": upper,
            "after": previous,
            "complete": len(headers) < limit or previous == upper,
        }
