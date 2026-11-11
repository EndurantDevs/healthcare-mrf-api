# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reader ownership and recovery around authenticated API metadata validation."""

import asyncio
import json
import re
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api import network_serving_api_read as pins
from db import connection as database
from process import network_approved_membership_source as approved_sources
from process import registry_network_catalog_read as catalogs
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import _BOUND_NAMES_SQL, NetworkReadIdentityError
from process.network_serving_api_sql import api_serving_pin_statement
from process.network_serving_read import NetworkServingReadUnavailable, PinnedNetworkServingManifest
from process.registry_network_catalog_read import _PINNED_READ
from tests.test_network_serving_read_context import _bound_reader, _Session

_SCHEMA = "registry_control"
_REFUSALS = (NetworkServingReadUnavailable, ServiceUnavailable)


def _entry(contract=pins.INDEXED_CONTRACT, component="compact_network_serving"):
    return {
        "candidate_json": json.dumps(
            {
                "validation_json": {
                    "serving_readiness": {"component": component, "address_artifact": {"contract": contract}}
                }
            }
        ),
        "control_tables_json": json.dumps(
            [{"relname": "network_serving_manifest", "oid": 81}, {"relname": "registry_approved_record", "oid": 82}]
        ),
    }


class _PinSession(_Session):
    def __init__(self):
        super().__init__(active=True, modes=("repeatable read", "on"))
        self.entry = _entry()
        self.failure = self.after_pin = self.after_guard = None
        self.events = []
        self.guard_valid = True
        self.driver.fetchrow = AsyncMock(side_effect=self._fetchrow)

    @asynccontextmanager
    async def begin_nested(self):
        """Model savepoint cleanup separately from the caller-owned outer transaction."""
        assert self.nested is None
        self.nested = object()
        self.events.append("savepoint")
        try:
            yield self.nested
        except BaseException:
            self.events.append("rollback-savepoint")
            raise
        else:
            self.events.append("release-savepoint")
        finally:
            self.nested = None

    async def _fetchrow(self, sql, *arguments):
        if sql == _BOUND_NAMES_SQL:
            names, oids, pid = arguments
            assert names == [f'"{_SCHEMA}"."network_serving_manifest"', f'"{_SCHEMA}"."registry_approved_record"']
            assert oids == [81, 82] and pid == 42
            self.events.append("guard")
            if self.after_guard is not None:
                self.after_guard()
            return {"backend_matches": True, "relation_count": 2, "valid": self.guard_valid}
        assert sql == pins.api_serving_pin_statement(_SCHEMA)
        assert arguments == (_SCHEMA, 3, 8 * 1024 * 1024) and self.nested is not None
        self.events.append("pin")
        if self.after_pin is not None:
            self.after_pin()
        if self.failure is not None:
            raise self.failure
        return self.entry


@pytest.fixture
def pin_reader(monkeypatch):
    session = _PinSession()
    manifest = PinnedNetworkServingManifest(
        3, "10000000-0000-0000-0000-000000000001", "candidate", 1, {}, 1, "a" * 64, 80
    )
    approved = object()
    relation = CompactAddressRelation(_SCHEMA, "registry_approved_record", 82)
    result = manifest, approved, relation
    validate = Mock(return_value=result)
    legacy = AsyncMock(return_value=manifest)
    approved_pin = AsyncMock(return_value=(approved, relation))
    semantic_reads = Mock()
    semantic_reads.attach_mock(legacy, "catalog")
    semantic_reads.attach_mock(approved_pin, "approved")
    monkeypatch.setattr(pins, "validate_api_serving_pin", validate)
    monkeypatch.setattr(catalogs, "catalog_pin", legacy)
    monkeypatch.setattr(approved_sources, "_pin_verified_api_approved_source", approved_pin)
    return SimpleNamespace(
        session=session,
        result=result,
        validate=validate,
        legacy=legacy,
        approved_pin=approved_pin,
        semantic_reads=semantic_reads,
    )


async def _resolve(session):
    return await pins.resolve_api_network_pin(session, generation_id=3, control_schema=_SCHEMA)


async def test_indexed_pin_uses_one_fetch_on_the_original_reader(pin_reader):
    session = pin_reader.session
    original = session.transaction
    with _bound_reader(session) as binding:
        identity = session.info["api_reader_identity"]
        assert await _resolve(session) is pin_reader.result
        pin_reader.validate.assert_called_once_with(
            session.entry, schema=_SCHEMA, backend_pid=42, reader_identity=identity, generation_id=3
        )
        assert database._READER.get() is binding and database.current_session() is session
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()

    assert session.driver.fetchrow.await_count == 1
    assert session.events == ["savepoint", "pin", "release-savepoint"]
    assert session.transaction is original and session.nested is None
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)


@pytest.mark.parametrize("generation", [False, True, 0, -1, 2**63, 3.0, "3"])
async def test_invalid_generation_refuses_before_sql(pin_reader, generation):
    session = pin_reader.session
    with _bound_reader(session), pytest.raises(NetworkServingReadUnavailable):
        await pins.resolve_api_network_pin(session, generation_id=generation, control_schema=_SCHEMA)
    session.driver.fetchrow.assert_not_awaited()
    pin_reader.validate.assert_not_called()
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()


@pytest.mark.parametrize("damage", ["unbound", "unverified", "foreign", "child_task", "inactive", "nested"])
async def test_unowned_reader_refuses_before_sql(pin_reader, damage):
    session = pin_reader.session
    with _bound_reader(session):
        match damage:
            case "unbound":
                database._READER.set(None)
            case "unverified":
                session.info["api_reader_verified"] = False
            case "inactive":
                session.transaction = None
            case "nested":
                session.nested = object()
        with pytest.raises(_REFUSALS):
            if damage == "child_task":
                await asyncio.create_task(_resolve(session))
            else:
                await _resolve(_PinSession() if damage == "foreign" else session)
    session.driver.fetchrow.assert_not_awaited()
    pin_reader.validate.assert_not_called()
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()


@pytest.mark.parametrize(
    ("contract", "component"),
    [
        (pins.ADDRESS_CONTRACT, "compact_network_serving"),
        (pins.BOOTSTRAP_CONTRACT, "compact_network_serving"),
        (None, "unified_address_serving"),
    ],
)
async def test_known_legacy_profiles_keep_full_verification(pin_reader, contract, component):
    session = pin_reader.session
    session.entry = _entry(contract, component)
    with _bound_reader(session):
        assert await _resolve(session) == pin_reader.result
    pin_reader.validate.assert_not_called()
    pin_reader.legacy.assert_awaited_once_with(session.driver, 3, _SCHEMA, api_read=True)
    pin_reader.approved_pin.assert_awaited_once_with(session.driver, pin_reader.result[0], control_schema=_SCHEMA)
    assert pin_reader.semantic_reads.mock_calls == [
        call.catalog(session.driver, 3, _SCHEMA, api_read=True),
        call.approved(session.driver, pin_reader.result[0], control_schema=_SCHEMA),
    ]


@pytest.mark.parametrize("entry", [None, {"candidate_json": "null"}, _entry("unknown"), _entry(component="unknown")])
async def test_unknown_profile_never_selects_legacy(pin_reader, entry):
    session = pin_reader.session
    session.entry = entry
    with _bound_reader(session), pytest.raises(NetworkServingReadUnavailable):
        await _resolve(session)
    pin_reader.validate.assert_not_called()
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()


async def test_rejected_indexed_metadata_never_selects_legacy(pin_reader):
    rejection = NetworkServingReadUnavailable("Synthetic invalid seal")
    pin_reader.validate.side_effect = rejection
    with _bound_reader(pin_reader.session), pytest.raises(NetworkServingReadUnavailable) as caught:
        await _resolve(pin_reader.session)
    assert caught.value is rejection
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()


def _missing_table(schema=None, table=None, message=None):
    error = asyncpg.UndefinedTableError(message or "Synthetic missing relation")
    error.schema_name, error.table_name, error.message = schema, table, message
    return error


@pytest.mark.parametrize("structured", [False, True])
async def test_missing_optional_ledger_rolls_back_before_legacy(pin_reader, structured):
    session = pin_reader.session
    session.failure = (
        _missing_table(_SCHEMA, "network_candidate_custody")
        if structured
        else _missing_table(message=f'relation "{_SCHEMA}.network_candidate_custody" does not exist')
    )
    original = session.transaction

    async def legacy(*_arguments, **_options):
        assert session.events == ["savepoint", "pin", "rollback-savepoint"]
        assert session.nested is None and session.transaction is original
        return pin_reader.result[0]

    pin_reader.legacy.side_effect = legacy
    with _bound_reader(session):
        assert await _resolve(session) == pin_reader.result
    pin_reader.validate.assert_not_called()
    pin_reader.legacy.assert_awaited_once_with(session.driver, 3, _SCHEMA, api_read=True)
    pin_reader.approved_pin.assert_awaited_once_with(session.driver, pin_reader.result[0], control_schema=_SCHEMA)
    assert session.transaction is original and session.driver.active


@pytest.mark.parametrize(
    "failure",
    [
        _missing_table(_SCHEMA, "network_serving_manifest"),
        _missing_table("another_control", "network_candidate_custody"),
        _missing_table(
            "another_control",
            "network_candidate_custody",
            f'relation "{_SCHEMA}.network_candidate_custody" does not exist',
        ),
        asyncpg.InsufficientPrivilegeError("Synthetic denied relation"),
        RuntimeError("Synthetic query failure"),
        asyncio.CancelledError(),
    ],
)
async def test_other_query_failures_never_fall_back(pin_reader, failure):
    session = pin_reader.session
    session.failure = failure
    original = session.transaction
    with _bound_reader(session), pytest.raises(type(failure)) as caught:
        await _resolve(session)
    assert caught.value is failure
    assert session.events == ["savepoint", "pin", "rollback-savepoint"]
    assert session.transaction is original and session.nested is None
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()
    pin_reader.validate.assert_not_called()


async def test_missing_ledger_cannot_reclassify_an_indexed_result(pin_reader):
    session = pin_reader.session
    session.failure = _missing_table(_SCHEMA, "network_candidate_custody")
    object.__setattr__(pin_reader.result[0], "_supports_location_lookup", True)
    with _bound_reader(session), pytest.raises(NetworkServingReadUnavailable):
        await _resolve(session)
    pin_reader.legacy.assert_awaited_once_with(session.driver, 3, _SCHEMA, api_read=True)
    pin_reader.approved_pin.assert_not_awaited()


def _drift(session, damage):
    match damage:
        case "transaction":
            session.transaction = object()
        case "driver":
            session.driver = _PinSession().driver
        case "pid":
            session.driver.pid += 1
        case "native_transaction":
            session.driver.active = False
        case "metadata":
            session.info["api_reader_identity"] = dict(session.info["api_reader_identity"])
        case "role":
            session.info["api_reader_identity"]["role_oid"] += 1


@pytest.mark.parametrize("damage", ["transaction", "driver", "pid", "native_transaction", "metadata", "role"])
@pytest.mark.parametrize("phase", ["pin", "validation"])
async def test_pin_cannot_change_its_original_reader(pin_reader, damage, phase):
    session = pin_reader.session
    if phase == "pin":
        session.after_pin = lambda: _drift(session, damage)
    else:
        pin_reader.validate.side_effect = lambda *_args, **_kwargs: (_drift(session, damage), pin_reader.result)[1]
    with _bound_reader(session), pytest.raises(_REFUSALS):
        await _resolve(session)
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()
    assert session.nested is None and (session.begins, session.commits, session.rollbacks) == (0, 0, 0)


async def _read_catalog_body(pin_reader, outcome, failure):
    session = pin_reader.session
    inventories = execution._NAME_INVENTORIES.get()
    async with pins.api_catalog_read_snapshot(session, 3, _SCHEMA) as (manifest, relation):
        assert (manifest, relation) == (pin_reader.result[0], pin_reader.result[2])
        assert _PINNED_READ.get() == (session.driver, manifest, _SCHEMA)
        assert manifest._api_approved_source is pin_reader.result[1]
        assert len(execution._NAME_INVENTORIES.get()) == len(inventories) + 1
        if outcome in {"body_error", "cancel"}:
            raise failure
        if outcome == "invalid_names":
            session.guard_valid = False
        if outcome == "reader_drift":
            session.after_guard = lambda: _drift(session, "metadata")


@pytest.mark.parametrize("outcome", ["success", "body_error", "cancel", "invalid_names", "reader_drift"])
async def test_catalog_snapshot_restores_nested_contexts(pin_reader, outcome):
    session = pin_reader.session
    failure = asyncio.CancelledError() if outcome == "cancel" else RuntimeError("Synthetic body failure")
    expected = {
        "success": nullcontext(),
        "body_error": pytest.raises(RuntimeError),
        "cancel": pytest.raises(asyncio.CancelledError),
        "invalid_names": pytest.raises(NetworkServingReadUnavailable),
        "reader_drift": pytest.raises(NetworkServingReadUnavailable),
    }[outcome]
    previous = (object(), object(), "outer_control")
    token = _PINNED_READ.set(previous)
    original = session.transaction
    try:
        with (
            _bound_reader(session),
            execution.network_read_name_inventory(
                session.driver, (CompactAddressRelation("outer_control", "outer_table", 90),)
            ),
        ):
            inventories = execution._NAME_INVENTORIES.get()
            with expected as caught:
                await _read_catalog_body(pin_reader, outcome, failure)
            if outcome in {"body_error", "cancel"}:
                assert caught.value is failure
            if outcome == "invalid_names":
                assert isinstance(caught.value.__cause__, NetworkReadIdentityError)
            assert _PINNED_READ.get() is previous
            assert execution._NAME_INVENTORIES.get() is inventories
    finally:
        _PINNED_READ.reset(token)
    assert session.events.count("guard") == int(outcome not in {"body_error", "cancel"})
    assert session.transaction is original and session.nested is None
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()


def test_catalog_deparser_and_trigger_types_are_qualified_before_statement_parsing():
    statement = api_serving_pin_statement(_SCHEMA)
    assert "pg_catalog.pg_get_constraintdef(oid)" in statement
    assert re.search(r"(?<![.\w])pg_get_constraintdef\s*\(", statement) is None
    assert re.search(r"::(?:text|oid|uuid|jsonb)\b", statement) is None
    assert 'COLLATE "C"' not in statement
    for type_name in ("trigger", "event_trigger"):
        assert f"'pg_catalog.{type_name}'::pg_catalog.regtype" in statement
        assert f"'{type_name}'::regtype" not in statement
