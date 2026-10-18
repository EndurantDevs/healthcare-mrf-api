"""Committed office events require original read-only authority and an exact protected pair."""

import json
from copy import deepcopy
from uuid import UUID

import pytest

from process import registry_ptg_source_notifications as notices
from process.registry_ptg_office_capture import _canonical, _digest
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from tests.test_registry_ptg_office_selection import _pair

STORE = RegistryPTGProducerScopeStore("office_owner", "office_approver", "office_control")
IDENTITY = {
    "database_name": "synthetic_database",
    "database_oid": 1,
    "control_schema": STORE.control_schema,
    "schema_oid": 2,
    "approval_table_oid": 3,
    "hold_table_oid": 4,
    "owner_role": STORE.owner_role,
    "owner_oid": 5,
    "approval_role": STORE.approval_role,
    "approval_role_oid": 6,
    "reader_role": "office_reader",
    "reader_oid": 7,
}


def event(capture=1):
    _, row, _ = _pair()
    document = json.loads(row["approval_json"])
    capture_id = str(UUID(int=capture))
    document["capture_id"] = document["command"]["capture_id"] = capture_id
    document["actor"] = {"kind": "platform_admin", "client_id": "system", "user_id": str(UUID(int=99))}
    document["witness"]["contract"] = "registry_ptg_office_witness.v1"
    row.update(
        capture_id=capture_id,
        actor_key=_digest(document["actor"]),
        idempotency_key=document["command"]["idempotency_key"],
    )
    hold = json.loads(row["hold_json"])
    row["approval_sha256"] = hold["approval_sha256"] = _digest(document)
    hold.update(
        capture_id=capture_id,
        schema_name="registry_ptg_office_" + UUID(capture_id).hex,
        witness_sha256=_digest(document["witness"]),
    )
    row.update(
        approval_json=_canonical(document).decode(), hold_json=_canonical(hold).decode(), hold_sha256=_digest(hold)
    )
    return row


class Reader:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.identity = dict(IDENTITY)
        self.transaction = True
        self.isolation = "repeatable read"
        self.readonly = "on"
        self.protected = True
        self.calls = []
        self.identity_reads = 0
        self.drift = False

    def is_in_transaction(self):
        return self.transaction

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if sql == "SHOW transaction_isolation":
            return self.isolation
        if sql == "SHOW transaction_read_only":
            return self.readonly
        assert "AS closed FROM scope" in sql
        assert "registry_ptg_office_approval" in sql or "registry_ptg_office_hold" in sql
        assert "NOT pg_has_role(reader_oid,approval_oid,'SET')" in sql
        return self.protected

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        if sql == notices._IDENTITY_SQL:
            assert args == (STORE.control_schema, STORE.owner_role, STORE.approval_role, "office_reader")
            self.identity_reads += 1
            if self.drift and self.identity_reads > 1:
                return {**self.identity, "hold_table_oid": 8}
            return self.identity
        assert "ORDER BY a.capture_id DESC LIMIT 1" in sql
        return {"capture_id": max(row["capture_id"] for row in self.rows)} if self.rows else None

    async def fetch(self, sql, after, upper, limit):
        self.calls.append((sql, (after, upper, limit)))
        assert "LEFT JOIN" in sql and "LIMIT $3" in sql
        return sorted(
            (
                row
                for row in self.rows
                if (after is None or UUID(row["capture_id"]) > after) and UUID(row["capture_id"]) <= upper
            ),
            key=lambda row: row["capture_id"],
        )[:limit]

    def execute(self, *_args):
        pytest.fail("discovery attempted a mutation/authority change")


async def page(reader, **kw):
    return await notices.read_registry_ptg_source_notification_headers(
        reader, STORE, reader_role="office_reader", identity=IDENTITY, **kw
    )


@pytest.mark.asyncio
async def test_genuine_paired_event_is_closed_and_discovery_does_not_require_selection():
    row = event()
    reader = Reader([row])
    result = await page(reader)
    assert result == {
        "headers": [{k: row[k] for k in ("capture_id", "approval_sha256", "hold_sha256")}],
        "after": row["capture_id"],
        "upper": row["capture_id"],
        "complete": True,
    }
    assert reader.identity_reads == 2
    assert not any("registry_approved_record" in sql or "registry_network_binding" in sql for sql, _ in reader.calls)


@pytest.mark.asyncio
async def test_bounded_rotation_fixed_upper_and_late_low_key_next_pass():
    reader = Reader([event(2), event(3)])
    first = await page(reader, limit=1)
    assert not first["complete"]
    reader.rows.extend([event(1), event(4)])
    second = await page(reader, after=first["after"], upper=first["upper"], limit=1)
    assert second["complete"] and second["headers"][0]["capture_id"] == event(3)["capture_id"]
    reset = await page(reader, after=None, upper=None)
    assert [row["capture_id"] for row in reset["headers"]] == [event(i)["capture_id"] for i in range(1, 5)]
    assert reset["complete"]


@pytest.mark.asyncio
async def test_empty_cycle_and_no_mutation_of_caller_cursor():
    assert await page(Reader()) == {"headers": [], "upper": None, "after": None, "complete": True}
    original = deepcopy(IDENTITY)
    await page(Reader([event()]))
    assert IDENTITY == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("transaction", False),
        ("isolation", "read committed"),
        ("isolation", "serializable"),
        ("readonly", "off"),
        ("protected", False),
        ("identity", None),
    ],
)
async def test_wrong_native_authority_refuses_before_header_read(field, value):
    reader = Reader([event()])
    setattr(reader, field, value)
    with pytest.raises(ValueError):
        await page(reader)
    assert not any("LEFT JOIN" in sql for sql, _ in reader.calls)


@pytest.mark.asyncio
async def test_native_identity_change_after_pair_read_refuses():
    reader = Reader([event()])
    reader.drift = True
    with pytest.raises(ValueError):
        await page(reader)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", sorted(IDENTITY))
async def test_each_full_identity_coordinate_is_bound(field):
    reader = Reader([event()])
    reader.identity[field] = (
        reader.identity[field] + 10 if type(reader.identity[field]) is int else reader.identity[field] + "_changed"
    )
    with pytest.raises(ValueError):
        await page(reader)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": True},
        {"limit": 0},
        {"limit": 26},
        {"after": str(UUID(int=1))},
        {"upper": "bad"},
        {"upper": str(UUID(int=1)), "after": str(UUID(int=2))},
    ],
)
async def test_malformed_page_refuses_without_sql(kwargs):
    reader = Reader()
    with pytest.raises(ValueError):
        await page(reader, **kwargs)
    assert not reader.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("approval_sha256", "f" * 64),
        ("hold_sha256", None),
        ("approval_json", None),
        ("hold_json", "{}"),
        ("actor_key", "f" * 64),
        ("idempotency_key", "other"),
        ("capture_id", str(UUID(int=2))),
    ],
)
async def test_unpaired_tampered_or_mismatched_header_refuses(field, value):
    row = event()
    row[field] = value
    with pytest.raises(ValueError):
        await page(Reader([row]))


def test_native_original_login_and_role_guards_are_explicit():
    for clause in (
        "current_user=session_user",
        "current_user=r.rolname",
        "r.rolcanlogin",
        "current_setting('role')='none'",
        "has_parameter_privilege",
        "NOT r.rolcreatedb",
    ):
        assert clause in notices._IDENTITY_SQL


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "store,role",
    [
        (STORE, STORE.owner_role),
        (STORE, STORE.approval_role),
        (RegistryPTGProducerScopeStore("owner", "approver"), "reader"),
        (object(), "reader"),
    ],
)
async def test_no_owner_approver_or_ambient_store_fallback(store, role):
    reader = Reader()
    with pytest.raises(ValueError):
        await notices.qualify_registry_ptg_source_notifications(reader, store, reader_role=role)
    assert not reader.calls


@pytest.mark.asyncio
async def test_cancelled_pair_read_propagates_without_advancing_any_cursor():
    import asyncio

    reader = Reader([event()])

    async def cancelled(*_args):
        raise asyncio.CancelledError

    reader.fetch = cancelled
    with pytest.raises(asyncio.CancelledError):
        await page(reader)


@pytest.mark.parametrize("field", ["manifest_sha256", "retained_serving", "custody", "approval_sha256", "capture_id"])
def test_rehashed_foreign_hold_still_refuses_exact_paired_semantics(field):
    row = event()
    hold = json.loads(row["hold_json"])
    hold[field] = "f" * 64 if field.endswith("sha256") else {}
    row["hold_json"], row["hold_sha256"] = _canonical(hold).decode(), _digest(hold)
    with pytest.raises(ValueError):
        notices._event(row)


@pytest.mark.parametrize("value", [True, 0, 2**32, None])
def test_saved_native_oid_is_exact_and_bounded(value):
    with pytest.raises(ValueError):
        notices._identity({**IDENTITY, "approval_table_oid": value})


@pytest.mark.asyncio
async def test_native_refusal_cannot_fall_back_to_publisher_or_partial_page():
    reader = Reader([event()])
    reader.identity = {**IDENTITY, "reader_role": STORE.approval_role, "reader_oid": IDENTITY["approval_role_oid"]}
    with pytest.raises(ValueError):
        await page(reader)
    assert not any("LEFT JOIN" in sql for sql, _ in reader.calls)


@pytest.mark.parametrize("field", ["actor", "command", "capture_id", "client_id", "contract", "manifest_sha256"])
def test_rehashed_approval_semantic_tamper_still_refuses(field):
    row = event()
    document = json.loads(row["approval_json"])
    if field == "actor":
        document[field]["kind"] = "client_owner"
        row["actor_key"] = _digest(document[field])
    elif field == "command":
        document[field]["source"]["source_scope"]["selection_mode"] = "partial"
    else:
        document[field] = "changed"
    row["approval_json"] = _canonical(document).decode()
    row["approval_sha256"] = _digest(document)
    with pytest.raises(ValueError):
        notices._event(row)


@pytest.mark.parametrize("field,maximum", [("approval_json", 131072), ("hold_json", 32768)])
def test_pair_payload_bounds_are_bytes(field, maximum):
    row = event()
    row[field] = "😀" * (maximum // 4 + 1)
    with pytest.raises(ValueError):
        notices._event(row)


@pytest.mark.asyncio
async def test_invalid_native_order_or_interval_never_emits_partial_events():
    for rows in ([event(2), event(1)], [event(1), event(1)], [event(3)]):
        reader = Reader([event(2)])

        async def tampered(*_args):
            return rows

        reader.fetch = tampered
        with pytest.raises(ValueError):
            await page(reader)
