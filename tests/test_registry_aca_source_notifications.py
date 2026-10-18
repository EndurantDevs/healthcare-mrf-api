# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Accepted receipt, immutable replay and native privilege refusal protocols."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_aca_source_notifications as notices
from process import registry_aca_source_recipe as producer
from tests.test_registry_aca_source_recipe import _ROLES, _prepared


class _Connection:
    def __init__(self, *, writing=True):
        self.writing, self.transaction, self.closed = writing, True, True
        self.runtime_closed = True
        self.actor = "publisher" if writing else "reader"
        self.xid, self.rows, self.statements = "77", {}, []
        self.identity = dict(
            schema_oid=1,
            owner_oid=2,
            inbox_oid=3,
            head_oid=4,
            database_name="synthetic",
            database_oid=5,
            rolname="owner",
            rolcanlogin=False,
            rolsuper=False,
            rolcreatedb=False,
            rolcreaterole=False,
            rolreplication=False,
            rolbypassrls=False,
        )

    def is_in_transaction(self):
        return self.transaction

    async def fetchval(self, query, *args):
        if query == "SHOW transaction_isolation":
            return "repeatable read"
        if query == "SHOW transaction_read_only":
            return "off" if self.writing else "on"
        if query == "SELECT current_user":
            return self.actor
        if query == "SELECT pg_current_xact_id()::text":
            return self.xid
        if query == notices._ACL_SQL:
            assert args[0] == "registry_control" and args[-1] == notices.INSERT_COLUMNS
            return self.closed
        if query == notices._RUNTIME_SQL:
            assert args == (
                ("publisher", "reader"),
                2,
                "registry_control",
                "publisher",
                ("reader",),
                notices.INSERT_COLUMNS,
            )
            return self.runtime_closed
        raise AssertionError(query)

    async def fetchrow(self, query, *args):
        if query == notices._IDENTITY_SQL:
            return self.identity
        assert query.startswith("SELECT capture_id,source_schema,")
        return self.rows.get(args[0])

    async def execute(self, query, *args):
        assert query.startswith('INSERT INTO "registry_control".registry_aca_source_notification')
        self.statements.append(query)
        self.rows.setdefault(args[0], dict(zip(notices.INSERT_COLUMNS, args, strict=True)))


def _store():
    return notices.RegistryACASourceNotificationStore("registry_control", "publisher", ("reader",))


async def _fixture(monkeypatch):
    prepared, validation = await _prepared(monkeypatch)
    recipe = producer._recipe(prepared, validation, _ROLES)
    connection = _Connection()
    monkeypatch.setattr(notices, "native_driver", AsyncMock(return_value=connection))
    verifier = AsyncMock(return_value=recipe)
    monkeypatch.setattr(notices, "require_registry_aca_source_recipe", verifier)
    return SimpleNamespace(in_transaction=lambda: True), connection, recipe, verifier


@pytest.mark.asyncio
async def test_append_replay_and_hold_reprove_exact_native_recipe(monkeypatch):
    session, connection, recipe, verifier = await _fixture(monkeypatch)
    first = await _store().append(session, recipe)
    connection.xid = "99"
    assert await _store().append(session, recipe) == first
    assert len(connection.statements) == 1
    assert await _store().hold(session, recipe.source_pin.prepared) == first
    connection.writing, connection.actor = False, "reader"
    assert await _store().read(session, first["event"]) == first
    assert verifier.await_count >= 4


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["transaction", "actor", "owner", "acl", "runtime", "readonly"])
async def test_native_authority_failure_prevents_any_insert(monkeypatch, damage):
    session, connection, recipe, _verifier = await _fixture(monkeypatch)
    if damage == "transaction":
        connection.transaction = False
    elif damage == "actor":
        connection.actor = "unconfigured"
    elif damage == "owner":
        connection.identity["rolcanlogin"] = True
    elif damage == "acl":
        connection.closed = False
    if damage == "runtime":
        connection.runtime_closed = False
    elif damage == "readonly":
        connection.writing = False
    with pytest.raises(ValueError, match="registry_aca_source_notification_invalid"):
        await _store().append(session, recipe)
    assert connection.rows == {} and connection.statements == []


@pytest.mark.asyncio
async def test_reader_requalification_rechecks_enrollment_role_policy():
    connection = _Connection(writing=False)
    await _store().qualify(connection, writing=False)
    connection.runtime_closed = False
    with pytest.raises(ValueError, match="registry_aca_source_notification_invalid"):
        await _store().qualify(connection, writing=False)
    assert connection.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["hash", "schema_oid", "missing", "native"])
async def test_hold_never_accepts_missing_changed_or_unsealed_evidence(monkeypatch, damage):
    session, connection, recipe, verifier = await _fixture(monkeypatch)
    await _store().append(session, recipe)
    row = connection.rows[recipe.source_pin.prepared.ownership.dataset_id]
    if damage == "hash":
        row["notification_sha256"] = "0" * 64
    elif damage == "schema_oid":
        row["source_schema_oid"] += 1
    elif damage == "missing":
        connection.rows.clear()
    else:
        verifier.side_effect = ValueError("native archive changed")
    with pytest.raises(ValueError):
        await _store().hold(session, recipe.source_pin.prepared)


@pytest.mark.parametrize(
    "event",
    [
        {},
        {"capture_id": str(UUID(int=0)), "recipe_sha256": "a" * 64, "notification_sha256": "b" * 64},
        {"capture_id": str(UUID(int=1)), "recipe_sha256": "a" * 64, "notification_sha256": "b" * 64, "network_id": 77},
    ],
)
def test_event_header_is_not_mapping_or_runtime_authority(event):
    with pytest.raises(ValueError):
        notices.validate_registry_aca_source_event(event)


@pytest.mark.asyncio
async def test_global_notice_replay_hold_and_read_keep_closed_union(monkeypatch):
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
    from tests.test_registry_aca_global_epoch import _global_prepared

    prepared, validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    connection = _Connection()
    session = SimpleNamespace(in_transaction=lambda: True)
    monkeypatch.setattr(notices, "native_driver", AsyncMock(return_value=connection))
    verifier = AsyncMock(return_value=epoch)
    monkeypatch.setattr(notices, "require_registry_aca_global_epoch", verifier)
    first = await _store().append(session, epoch)
    assert set(first) == {"event", "epoch"} and first["epoch"] == epoch
    assert set(first["event"]) == {"capture_id", "recipe_sha256", "notification_sha256"}
    assert first["event"]["recipe_sha256"] == epoch.generation_id
    connection.xid = "99"
    assert await _store().append(session, epoch) == first
    assert len(connection.statements) == 1
    assert await _store().hold(session, prepared) == first
    connection.writing, connection.actor = False, "reader"
    assert await _store().read(session, first["event"]) == first
    assert await _store().accepted(session, prepared.ownership.dataset_id) == first
    verifier.side_effect = ValueError("native epoch drift")
    with pytest.raises(ValueError, match="native epoch drift"):
        await _store().read(session, first["event"])


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["extra", "contract", "epoch_hash", "native", "cross_kind"])
async def test_global_notice_refuses_changed_or_cross_kind_evidence(monkeypatch, damage):
    import json

    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
    from tests.test_registry_aca_global_epoch import _global_prepared

    prepared, validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    connection = _Connection()
    session = SimpleNamespace(in_transaction=lambda: True)
    monkeypatch.setattr(notices, "native_driver", AsyncMock(return_value=connection))
    verifier = AsyncMock(return_value=epoch)
    monkeypatch.setattr(notices, "require_registry_aca_global_epoch", verifier)
    await _store().append(session, epoch)
    persisted = connection.rows[prepared.ownership.dataset_id]
    document = json.loads(persisted["notification_json"])
    if damage == "native":
        verifier.side_effect = ValueError("native epoch drift")
    elif damage == "epoch_hash":
        persisted["recipe_sha256"] = "0" * 64
    else:
        if damage == "extra":
            document["issuer_id"] = 7
        elif damage == "contract":
            document["contract"] = notices.CONTRACT
        else:
            document["recipe_json"] = document.pop("epoch_json")
        persisted["notification_json"] = notices._json(document)
        persisted["notification_sha256"] = notices._digest(persisted["notification_json"])
    with pytest.raises(ValueError):
        await _store().hold(session, prepared)


@pytest.mark.asyncio
async def test_accepted_lookup_only_returns_requalified_existing_receipts(monkeypatch):
    session, connection, recipe, verifier = await _fixture(monkeypatch)
    connection.writing, connection.actor = False, "reader"
    capture_id = recipe.source_pin.prepared.ownership.dataset_id
    assert await _store().accepted(session, capture_id) is None
    assert connection.statements == []
    connection.writing, connection.actor = True, "publisher"
    first = await _store().append(session, recipe)
    assert await _store().accepted(session, str(capture_id), holding=True) == first
    connection.writing, connection.actor = False, "reader"
    assert await _store().accepted(session, capture_id) == first
    verifier.side_effect = ValueError("retained closure changed")
    with pytest.raises(ValueError, match="retained closure changed"):
        await _store().accepted(session, capture_id)
    with pytest.raises(ValueError):
        await _store().accepted(session, str(UUID(int=0)))


@pytest.mark.asyncio
async def test_flat_v1_receipt_bytes_remain_unchanged(monkeypatch):
    session, _connection, recipe, _verifier = await _fixture(monkeypatch)
    result = await _store().append(session, recipe)
    assert result["event"] == {
        "capture_id": "11111111-1111-4111-8111-111111111111",
        "recipe_sha256": "b17874b7c7836c85fb21bcc8fd3d78a3bd9f5ee8308cc55816ee1f50067f9004",
        "notification_sha256": "05c1c69b8c65a61d0f0d3b22ed5c0e69aea7a4d2f6e6414ff9b7785ab9034c05",
    }
    assert set(result) == {"event", "recipe"}


def _native_adapter_boundary(monkeypatch, session, *, active, isolation, readonly):
    """Exercise SQLAlchemy's real lazy asyncpg adapter with mocked network I/O."""
    from unittest.mock import Mock

    import asyncpg
    from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_connection, AsyncAdapt_asyncpg_dbapi
    from sqlalchemy.util.concurrency import greenlet_spawn

    prepared = SimpleNamespace(
        get_attributes=lambda: [],
        fetch=AsyncMock(return_value=[(isolation.replace("_", " "),)]),
        get_statusmsg=lambda: "SHOW",
    )
    driver = SimpleNamespace(
        active=active,
        is_in_transaction=lambda: driver.active,
        prepare=AsyncMock(return_value=prepared),
    )

    async def start():
        driver.active = True

    physical_transaction = SimpleNamespace(start=AsyncMock(side_effect=start))
    driver.transaction = Mock(return_value=physical_transaction)
    adapter = AsyncAdapt_asyncpg_connection(AsyncAdapt_asyncpg_dbapi(asyncpg), driver, prepared_statement_cache_size=0)
    adapter._transaction = physical_transaction if active else None
    adapter.isolation_level, adapter.readonly = isolation, readonly

    async def execute(statement):
        assert statement == "SHOW transaction_isolation"
        await greenlet_spawn(adapter.cursor().execute, statement)

    connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)),
        exec_driver_sql=AsyncMock(side_effect=execute),
    )
    monkeypatch.setattr(session, "connection", AsyncMock(return_value=connection))
    return connection, driver, physical_transaction


@pytest.mark.asyncio
@pytest.mark.parametrize("already_active", [False, True])
@pytest.mark.parametrize("isolation,readonly", [("read_committed", False), ("repeatable_read", True)])
async def test_native_driver_materializes_only_original_declared_sqlalchemy_transaction(
    monkeypatch, already_active, isolation, readonly
):
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.network_cms_registry_address_capture import native_driver

    async with AsyncSession() as session, session.begin():
        original = session.get_transaction()
        connection, driver, physical = _native_adapter_boundary(
            monkeypatch, session, active=already_active, isolation=isolation, readonly=readonly
        )
        assert session.in_transaction() and driver.is_in_transaction() is already_active
        assert await native_driver(session) is driver
        assert await native_driver(session) is driver
        assert session.get_transaction() is original and driver.is_in_transaction()
        assert connection.exec_driver_sql.await_count == int(not already_active)
        assert physical.start.await_count == int(not already_active)
        if already_active:
            driver.transaction.assert_not_called()
        else:
            driver.transaction.assert_called_once_with(isolation=isolation, readonly=readonly, deferrable=False)


@pytest.mark.asyncio
async def test_native_driver_refuses_undeclared_transaction_before_connection_acquisition(monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.network_cms_registry_address_capture import native_driver

    async with AsyncSession() as session:
        connection = AsyncMock(side_effect=AssertionError("must not autobegin a caller transaction"))
        monkeypatch.setattr(session, "connection", connection)
        with pytest.raises(ValueError, match="registry_cms_address_transaction_required"):
            await native_driver(session)
        connection.assert_not_awaited()
        assert session.get_transaction() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["execute", "inactive"])
async def test_native_driver_never_returns_unmaterialized_sqlalchemy_transaction(monkeypatch, failure):
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.network_cms_registry_address_capture import native_driver

    async with AsyncSession() as session, session.begin():
        original = session.get_transaction()
        connection, driver, physical = _native_adapter_boundary(
            monkeypatch, session, active=False, isolation="read_committed", readonly=False
        )
        connection.exec_driver_sql = AsyncMock(
            side_effect=RuntimeError("materialization failed") if failure == "execute" else None
        )
        with pytest.raises((ValueError, RuntimeError), match="transaction_required|materialization failed"):
            await native_driver(session)
        assert session.get_transaction() is original and not driver.is_in_transaction()
        physical.start.assert_not_awaited()
        driver.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [False, True])
async def test_native_receipt_facade_keeps_exact_driver_and_original_qualification(active):
    from process.network_cms_registry_address_capture import NativeReceiptSession, native_driver

    connection = _Connection()
    connection.transaction = active
    session = NativeReceiptSession(connection)
    assert await native_driver(session) is connection
    assert session.in_transaction() is active
    if active:
        assert (await _store().qualify(connection, writing=True))["inbox_oid"] == 3
    else:
        connection.fetchval = AsyncMock(side_effect=AssertionError("must refuse before catalog SQL"))
        with pytest.raises(ValueError, match="registry_aca_source_notification_invalid"):
            await _store().qualify(connection, writing=True)
        connection.fetchval.assert_not_awaited()
    assert connection.rows == {} and connection.statements == []
