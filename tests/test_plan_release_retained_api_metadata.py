# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained API metadata preserves custody without acquiring dummy reader locks."""

import asyncio
import copy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import network_read_execution as execution
from api import plan_release_read_storage as storage
from db.plan_release_storage import BINDING_COLUMNS, HEADER_COLUMNS, PIN_COLUMNS
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError
from process.plan_release_storage_catalog import BINDING_NATIVE_CHECKS, HEADER_NATIVE_CHECKS, PIN_NATIVE_CHECKS

_SCHEMA = "retained_control"
_REVISION = "hpserve_" + "0" * 25 + "1"


def _index(keys, *, primary=False, unique=False, predicate=None):
    return {
        "keys": keys,
        "primary": primary,
        "unique": unique,
        "predicate": predicate,
        "expression": None,
        "valid": True,
        "native": True,
        "method": "btree",
    }


def _checks(definitions):
    return [
        {"name": name, "type": "c", "definition": expression, "valid": True, "columns": []}
        for name, expression in definitions.items()
    ]


def _family():
    relations = []
    for name, oid, pin in ((storage.BINDING_TABLE, 101, False), (storage.PIN_TABLE, 102, True)):
        key = ["serving_revision_id", "snapshot_id"] if pin else ["serving_revision_id", "role", "binding_ordinal"]
        relations.append(
            {
                "relname": name,
                "oid": oid,
                "nspname": _SCHEMA,
                "relowner": 11,
                "relkind": "p",
                "relpersistence": "p",
                "relispartition": False,
                "parents": [],
                "relrowsecurity": False,
                "relforcerowsecurity": False,
                "native_checks": True,
                "direct_acl_closed": True,
                "readers_closed": False,
                "inert": True,
                "partition_key": "LIST (serving_revision_id)",
                "columns": [list(column) for column in (PIN_COLUMNS if pin else BINDING_COLUMNS)],
                "constraints": _checks(PIN_NATIVE_CHECKS if pin else BINDING_NATIVE_CHECKS),
                "indexes": [_index(key, primary=True, unique=True), _index(["snapshot_id"])],
            }
        )
    header = _retained_header(relations[0])
    legacy = copy.deepcopy(relations[0])
    legacy.update(
        relname=storage.LEGACY_BINDING_TABLE,
        oid=104,
        relkind="r",
        readers_closed=True,
        relispartition=True,
        parents=[101],
        partition_key=None,
        bound=f"FOR VALUES IN ('{_REVISION}')",
    )
    legacy["constraints"].extend(
        [
            {"name": "original_revision_fk", "type": "f", "valid": True},
            {
                "name": "plan_release_legacy_cohort_bound",
                "type": "c",
                "valid": True,
                "columns": [1],
                "definition": f"CHECK (((serving_revision_id)::text = '{_REVISION}'::text))",
            },
        ]
    )
    return [*relations, header, legacy]


def _retained_header(binding):
    header = copy.deepcopy(binding)
    header.update(
        relname=storage.REVISION_TABLE,
        oid=103,
        relkind="r",
        readers_closed=True,
        partition_key=None,
        columns=[list(column) for column in HEADER_COLUMNS],
        constraints=_checks(HEADER_NATIVE_CHECKS),
        indexes=[
            _index(["serving_revision_id"], primary=True, unique=True),
            _index(["plan_release_id"], unique=True, predicate="is_current"),
            _index(["healthporta_plan_id"], unique=True, predicate="is_current"),
            _index(["healthporta_plan_id", "release_month"]),
        ],
    )
    return header


class _Native:
    def __init__(self):
        self.active, self.pid, self.calls = True, 42, []
        self.modes, self.started_at = ("repeatable read", "on"), datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.family, self.readers = _family(), [{"oid": 21, "ordinary_reader": True}]
        self.access = [
            {
                "relname": name,
                "oid": oid,
                "relowner": 11,
                "reader_oid": 21,
                "readable": False,
                "owner_only": True,
                "ordinary_reader": True,
            }
            for name, oid in ((storage.BINDING_TABLE, 101), (storage.PIN_TABLE, 102))
        ]
        self.authority = {"original_fk": True, "no_rewrite": True, "old_pin_reader": True, "pin_oid": 105}
        self.allow_dummy, self.on_fetch = False, None

    def is_in_transaction(self):
        return self.active

    def get_server_pid(self):
        return self.pid

    async def fetchrow(self, sql):
        assert sql == storage._API_SNAPSHOT_SQL
        self.calls.append(("snapshot", sql))
        return {
            "backend_pid": self.pid,
            "isolation": self.modes[0],
            "read_only": self.modes[1],
            "started_at": self.started_at,
        }

    async def fetch(self, sql, *parameters):
        self.calls.append(("catalog", sql))
        assert "LOCK TABLE" not in sql and "pg_advisory" not in sql
        if "LIMIT 0" in sql:
            assert self.allow_dummy
            return []
        if self.on_fetch is not None:
            self.on_fetch(sql)
        if sql == storage._ACCESS_SQL:
            return copy.deepcopy(self.access)
        if sql == storage._RETAINED_READERS_SQL:
            return copy.deepcopy(self.readers)
        if sql == storage.CATALOG_SQL:
            return copy.deepcopy(self.family)
        if sql == storage._API_CATALOG_SQL:
            return [{**relation, "expression_aliases": {}} for relation in copy.deepcopy(self.family)]
        if sql == storage._API_AUTHORITY_SQL:
            assert storage._LEGACY_AUTHORITY_SQL in sql
            return [dict(self.authority)]
        if sql == storage._LEGACY_AUTHORITY_SQL:
            return [{key: value for key, value in self.authority.items() if key != "pin_oid"}]
        raise AssertionError(sql)


def _orm_session(native):
    session = SimpleNamespace(info={}, transaction=object(), native=native)
    session.in_transaction = lambda: session.native.active
    session.get_transaction = lambda: session.transaction
    session.connection = AsyncMock(
        return_value=SimpleNamespace(
            get_raw_connection=AsyncMock(side_effect=lambda: SimpleNamespace(driver_connection=session.native))
        )
    )
    return session


def _pins():
    return (
        CompactAddressRelation(_SCHEMA, storage.LEGACY_BINDING_TABLE, 104),
        CompactAddressRelation(_SCHEMA, storage.REVISION_TABLE, 103),
        CompactAddressRelation(_SCHEMA, "ptg2_snapshot_pin", 105),
    )


def _verify(monkeypatch, native, *, refuse=False, mutate=None):
    async def verify(connection, pins, *, expected_backend_pid, **keywords):
        assert connection is native and expected_backend_pid == 42 and pins == _pins()
        if refuse:
            raise NetworkReadIdentityError("changed_relation")
        if mutate is not None and "absent_names" in keywords:
            mutate()

    monkeypatch.setattr(storage, "verify_bound_network_names", verify)
    monkeypatch.setattr(execution, "verify_bound_network_names", verify)


@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_native_metadata_preserves_full_family_and_returns_only_consumed_heaps(monkeypatch, modes):
    native = _Native()
    native.modes = modes
    _verify(monkeypatch, native)
    assert await storage.read_retained_binding_metadata(native, _SCHEMA) == (True, _pins())
    assert all("LIMIT 0" not in sql for _, sql in native.calls)
    assert any(sql == storage._API_CATALOG_SQL for _, sql in native.calls)
    assert any(sql == storage._API_AUTHORITY_SQL for _, sql in native.calls)
    assert all(pin.table_name not in {storage.BINDING_TABLE, storage.PIN_TABLE} for pin in _pins())


async def test_original_session_installs_verified_pins_before_return(monkeypatch):
    native = _Native()
    session = _orm_session(native)
    _verify(monkeypatch, native)
    catalog = AsyncMock(side_effect=AssertionError("API metadata must bypass the ORM catalog tuner"))
    monkeypatch.setattr(storage, "_catalog_session_rows", catalog)
    assert await storage.is_retained_session_read(session, _SCHEMA, api_read=True) is True
    catalog.assert_not_awaited()
    assert any(sql == storage._API_CATALOG_SQL for _, sql in native.calls)
    assert all(sql != storage.CATALOG_SQL for _, sql in native.calls)
    installed = execution._installed_read_inventory(session)
    assert installed.connection is native and installed.relations == _pins() and installed.backend_pid == 42
    assert session.get_transaction() is session.transaction


async def test_provisioned_access_only_returns_false_without_claiming_custody(monkeypatch):
    native = _Native()
    for root in native.access:
        root["readable"] = True
    guard = AsyncMock()
    monkeypatch.setattr(storage, "verify_bound_network_names", guard)
    session = _orm_session(native)
    assert await storage.read_retained_binding_metadata(native, _SCHEMA) == (False, ())
    assert await storage.is_retained_session_read(session, _SCHEMA, api_read=True) is False
    guard.assert_not_awaited()
    assert not session.info
    assert not any(sql in {storage.CATALOG_SQL, storage._API_CATALOG_SQL} for _, sql in native.calls)


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("(oid)::text = 'oid relname'::text", "(serving_revision_id)::text = 'oid relname'::text"),
        ('"oid" = relname', '"oid" = binding_ordinal'),
        ("pg_catalog.oid(oid)::oid", "pg_catalog.oid(serving_revision_id)::oid"),
        ("oid(oid) = pg_catalog.oid", "oid(serving_revision_id) = pg_catalog.oid"),
        ("E'escaped\\'oid'::text = oid", "E'escaped\\'oid'::text = serving_revision_id"),
        ("'escaped''oid'::text = oid", "'escaped''oid'::text = serving_revision_id"),
        ("unknown_column = relname_suffix", "unknown_column = relname_suffix"),
        ("ctid IS NULL OR pg_class.* IS NULL", "ctid IS NULL OR pg_class.* IS NULL"),
        (None, None),
    ],
)
def test_catalog_aliases_restore_only_bare_column_tokens(expression, expected):
    alias_map = {"oid": "serving_revision_id", "relname": "binding_ordinal"}
    assert storage._restore_catalog_columns(expression, alias_map) == expected


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off")])
async def test_native_api_requires_readonly_snapshot_before_catalog(modes):
    native = _Native()
    native.modes = modes
    with pytest.raises(ValueError, match="snapshot"):
        await storage.read_retained_binding_metadata(native, _SCHEMA)
    assert all(kind == "snapshot" for kind, _ in native.calls)


@pytest.mark.parametrize("damage", ["detached", "bound", "cohort", "columns", "index", "owner", "acl", "namespace"])
async def test_retained_family_damage_is_refused_without_fallback_or_inventory(monkeypatch, damage):
    native = _Native()
    binding, _pin, _header, legacy = native.family
    match damage:
        case "detached":
            legacy["parents"] = []
        case "bound":
            legacy["bound"] = "FOR VALUES IN ('bad')"
        case "cohort":
            legacy["constraints"].pop()
        case "columns":
            binding["columns"].pop()
        case "index":
            binding["indexes"][0]["valid"] = False
        case "owner":
            legacy["relowner"] = 99
        case "acl":
            legacy["readers_closed"] = False
        case "namespace":
            legacy["nspname"] = "other_namespace"
    session = _orm_session(native)
    _verify(monkeypatch, native)
    with pytest.raises(ValueError, match="plan_release_storage_"):
        await storage.is_retained_session_read(session, _SCHEMA, api_read=True)
    assert not session.info


@pytest.mark.parametrize("field", ["original_fk", "no_rewrite", "old_pin_reader"])
async def test_original_authority_predicates_are_mandatory(monkeypatch, field):
    native = _Native()
    native.authority[field] = False
    _verify(monkeypatch, native)
    with pytest.raises(ValueError, match="legacy_authority"):
        await storage.read_retained_binding_metadata(native, _SCHEMA)


@pytest.mark.parametrize("damage", ["root_read", "root_acl", "root_owner", "other_reader"])
async def test_reader_and_owner_changes_never_authorize_retained_read(monkeypatch, damage):
    native = _Native()
    if damage == "root_read":
        native.access[0]["readable"] = True
    elif damage == "root_acl":
        native.access[0]["owner_only"] = False
    elif damage == "root_owner":
        native.access[0]["relowner"] = 99
    else:
        native.readers[0]["ordinary_reader"] = False
    _verify(monkeypatch, native)
    with pytest.raises(ValueError, match="retained_"):
        await storage.read_retained_binding_metadata(native, _SCHEMA)


async def test_name_identity_guard_refusal_is_normalized_before_metadata_return(monkeypatch):
    native = _Native()
    _verify(monkeypatch, native, refuse=True)
    with pytest.raises(ValueError, match="identity_changed"):
        await storage.read_retained_binding_metadata(native, _SCHEMA)


async def test_native_timestamp_is_only_drift_check_and_changed_transaction_refuses(monkeypatch):
    native = _Native()
    _verify(monkeypatch, native)

    def change(sql):
        if sql == storage._API_AUTHORITY_SQL:
            native.started_at += timedelta(seconds=1)

    native.on_fetch = change
    with pytest.raises(ValueError, match="snapshot_changed"):
        await storage.read_retained_binding_metadata(native, _SCHEMA)


@pytest.mark.parametrize("mutation", ["transaction", "driver", "pid", "inactive"])
async def test_identity_change_during_installation_refuses_before_boolean_return(monkeypatch, mutation):
    native = _Native()
    session = _orm_session(native)

    def change():
        if mutation == "transaction":
            session.transaction = object()
        elif mutation == "driver":
            session.native = _Native()
        elif mutation == "pid":
            native.pid = 43
        else:
            native.active = False

    _verify(monkeypatch, native, mutate=change)
    with pytest.raises(ValueError, match="(?:session|identity)_changed"):
        await storage.is_retained_session_read(session, _SCHEMA, api_read=True)
    assert not session.info


async def test_cancel_propagates_without_inventory_or_transaction_ownership_change(monkeypatch):
    native = _Native()
    session = _orm_session(native)
    original_transaction = session.transaction

    def cancel(_sql):
        raise asyncio.CancelledError

    native.on_fetch = cancel
    with pytest.raises(asyncio.CancelledError):
        await storage.is_retained_session_read(session, _SCHEMA, api_read=True)
    assert session.transaction is original_transaction and native.active and not session.info


async def test_default_fetch_only_path_keeps_original_dummy_lock_and_modes():
    native = _Native()
    native.allow_dummy = True
    native.modes = ("read committed", "off")
    assert await storage.is_retained_binding_read(native.fetch, _SCHEMA) is True
    assert sum("LIMIT 0" in sql for _, sql in native.calls) == 1
    assert not any(kind == "snapshot" for kind, _ in native.calls)


@pytest.mark.parametrize("use_session", [False, True])
async def test_closed_native_driver_uses_existing_read_refusal(use_session):
    native = _Native()
    native.get_server_pid = lambda: (_ for _ in ()).throw(RuntimeError("closed"))
    with pytest.raises(ValueError, match="unavailable"):
        if use_session:
            await storage.is_retained_session_read(_orm_session(native), _SCHEMA, api_read=True)
        else:
            await storage.read_retained_binding_metadata(native, _SCHEMA)
