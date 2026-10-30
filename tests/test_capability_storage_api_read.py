# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only capabilities retain native custody and guard physical heap reads."""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api import plan_pricing_code_storage as code
from api import ptg_membership_storage as membership
from api.ptg2_serving import PTG2_SCHEMA
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_code_projection import _identity
from tests.test_ptg_membership_projection import PARAMS


class _Result:
    def __init__(self, session, kind, value):
        self.session, self.kind, self.value = session, kind, value
        self.guarded, self.closed = False, False
        self.requires_guard = execution.has_network_read_inventory()

    def _decode(self):
        assert not self.requires_guard or self.guarded
        self.session.events.append(("decode", self.kind))
        return self.value

    def mappings(self):
        return self

    def first(self):
        return self._decode()

    def one(self):
        return self._decode()

    def one_or_none(self):
        return self._decode()

    def all(self):
        return self._decode()

    def scalar_one(self):
        return self._decode()

    def __iter__(self):
        return iter(self._decode())

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, storage):
        self.active, self.info, self.events, self.results = True, {}, [], []
        self.transaction, self.modes = object(), ("repeatable read", "on")
        self.control = {
            "database_oid": 1,
            "schema_oid": 2,
            "table_oid": 600,
            "relkind": "r",
            "relpersistence": "p",
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "relispartition": False,
            "heap": True,
            "inherited": False,
            "rules": False,
            "ordinary_reader": True,
            "column_updates": [],
            "immutable_read_lock": False,
        }
        self.identity = copy.deepcopy(PARAMS if storage is membership else _identity())
        self.ownership = storage.native.ReferenceFamilyStageOwnership(
            storage.SPEC.importer_id,
            UUID(int=1),
            "reference_family_archive_" + UUID(int=1).hex,
            11,
            tuple((name, 300 + index) for index, name in enumerate(storage.SPEC.table_names)),
        )
        self.dependencies = [(oid, 11, self.ownership.schema_name, name) for name, oid in self.ownership.relation_oids]
        if storage is code:
            self.dependencies.extend(
                (oid, 12, "hp_snapshot_retention", name)
                for name, oid in storage._membership_views(self.identity).items()
            )
        self.header = {"identity": self.identity, "content_digest": "a" * 64, "counts": {"code_coverage": []}}
        self.coverage, self.pin_present, self.attachment = [], True, (900, 101, True, True)
        self.driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: self.active)
        self.connection = AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(side_effect=lambda: SimpleNamespace(driver_connection=self.driver))
            )
        )

    def in_transaction(self):
        return self.active

    def get_transaction(self):
        return self.transaction

    def _response(self, sql):
        if "current_setting('transaction_isolation')" in sql:
            return "modes", self.modes
        if "AS ordinary_reader" in sql:
            return "control", self.control
        if "SELECT owner.oid" in sql:
            return "owner", (101, True)
        if "SELECT c.oid,c.relowner" in sql:
            return "attachment", self.attachment
        if "FROM pg_rewrite r" in sql:
            return "dependencies", self.dependencies
        if "SELECT identity,content_digest,counts" in sql:
            return "header", self.header
        if "SELECT code_key,occurrence_count" in sql:
            return "coverage", self.coverage
        if "SELECT EXISTS(SELECT 1 FROM" in sql:
            return "source-pin", self.pin_present
        raise AssertionError(sql)

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        assert "LOCK TABLE" not in sql and "pg_advisory" not in sql and "LIMIT 0" not in sql
        assert "SELECT identity FROM hp_snapshot_retention" not in sql
        kind, value = self._response(sql)
        result = _Result(self, kind, value)
        self.events.append(("execute", kind))
        self.results.append(result)
        return result

    async def scalar(self, statement, parameters=None):
        result = await self.execute(statement, parameters)
        return result.scalar_one()


def _custody(monkeypatch, storage, session):
    lock, capture, closed, view_acl = AsyncMock(), AsyncMock(return_value=session.ownership), AsyncMock(), AsyncMock()
    monkeypatch.setattr(storage.native, "_lock_family", lock)
    monkeypatch.setattr(storage.native, "capture_model_family_stage_ownership", capture)
    monkeypatch.setattr(storage, "_require_closed_local_custody", closed)
    monkeypatch.setattr(storage, "_require_no_untrusted_mutation", view_acl)
    return lock, capture, closed, view_acl


def _verify(monkeypatch, session, *, refuse=None):
    async def verify(driver, relations, *, expected_backend_pid, absent_names=()):
        assert absent_names == ()
        assert driver is session.driver and expected_backend_pid == 42
        expected_relations = tuple(
            CompactAddressRelation(session.ownership.schema_name, name, oid)
            for name, oid in session.ownership.relation_oids
        )
        assert relations == (*expected_relations, CompactAddressRelation(PTG2_SCHEMA, "ptg2_snapshot_pin", 600))
        current = session.results[-1]
        session.events.append(("guard", current.kind))
        if current.kind == refuse:
            raise NetworkReadIdentityError("changed_relation")
        current.guarded = True

    monkeypatch.setattr(execution, "verify_bound_network_names", verify)


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_api_read_preserves_native_custody_and_installs_real_heap_inventory(monkeypatch, storage, modes):
    session = _Session(storage)
    session.modes = modes
    lock, capture, closed, view_acl = _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session)
    found = await storage.locate(session, session.identity, api_read=True)
    assert found[0] is session.ownership and found[1] == session.header
    capture.assert_awaited_once_with(session, storage.SPEC, session.ownership.dataset_id)
    closed.assert_awaited_once_with(session, session.ownership, 101)
    view_acl.assert_awaited_once_with(session, [900], 101)
    lock.assert_not_awaited()
    installed = execution._installed_read_inventory(session)
    assert installed.connection is session.driver and installed.backend_pid == 42
    assert {relation.table_oid for relation in installed.relations} == {
        600,
        *(oid for _, oid in session.ownership.relation_oids),
    }
    assert all(
        relation.table_name not in {"attachment", storage.attachment_name(session.identity)}
        for relation in installed.relations
    )
    assert session.events.index(("guard", "header")) < session.events.index(("decode", "header"))
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off")])
async def test_api_requires_native_readonly_snapshot_before_attachment_lookup(monkeypatch, storage, modes):
    session = _Session(storage)
    session.modes = modes
    _custody(monkeypatch, storage, session)
    with pytest.raises(PTG2ManifestArtifactError, match="snapshot"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("execute", "owner") not in session.events and not session.info


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("refuse", ["header", "source-pin"])
async def test_guard_failure_closes_buffer_before_decoding_or_capability(monkeypatch, storage, refuse):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session, refuse=refuse)
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("decode", refuse) not in session.events
    assert session.results[-1].closed and not session.info
    assert not execution.has_network_read_inventory()


async def test_zero_coverage_rows_are_guarded_before_comparison(monkeypatch):
    session = _Session(membership)
    _custody(monkeypatch, membership, session)
    _verify(monkeypatch, session, refuse="coverage")
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await membership.locate(session, session.identity, api_read=True)
    assert ("decode", "coverage") not in session.events and not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_missing_source_retention_pin_never_returns_or_installs_capability(monkeypatch, storage):
    session = _Session(storage)
    session.pin_present = False
    _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session)
    with pytest.raises(PTG2ManifestArtifactError, match="retention pin"):
        await storage.locate(session, session.identity, api_read=True)
    assert not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_installation_refusal_never_returns_verified_header(monkeypatch, storage):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session)
    monkeypatch.setattr(
        membership, "install_network_read_inventory", AsyncMock(side_effect=ServiceUnavailable("changed"))
    )
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_api_capture_drift_refuses_before_payload_or_installation(monkeypatch, storage):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    session.dependencies[0] = (999, 11, session.ownership.schema_name, session.dependencies[0][3])
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("execute", "header") not in session.events and not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_absent_attachment_has_no_inventory_but_still_requires_snapshot(monkeypatch, storage):
    session = _Session(storage)
    session.attachment = None
    lock, capture, closed, _view_acl = _custody(monkeypatch, storage, session)
    assert await storage.locate(session, session.identity, api_read=True) is None
    lock.assert_not_awaited()
    capture.assert_not_awaited()
    closed.assert_not_awaited()
    assert not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_cancel_propagates_without_installation_or_transaction_end(monkeypatch, storage):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    monkeypatch.setattr(execution, "verify_bound_network_names", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await storage.locate(session, session.identity, api_read=True)
    assert session.active and not session.info
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize(
    "changed",
    [
        {"table_oid": None},
        {"schema_oid": True},
        {"database_oid": 0},
        {"table_oid": 2**32},
        {"relkind": "v"},
        {"relpersistence": "u"},
        {"relrowsecurity": True},
        {"relforcerowsecurity": True},
        {"relispartition": True},
        {"heap": False},
        {"inherited": True},
        {"rules": True},
        {"ordinary_reader": False},
    ],
)
async def test_current_control_binding_refuses_unsafe_native_metadata(monkeypatch, changed):
    session = _Session(membership)
    session.control.update(changed)
    _custody(monkeypatch, membership, session)
    with pytest.raises(PTG2ManifestArtifactError, match="control binding"):
        await membership.locate(session, session.identity, api_read=True)
    assert ("execute", "header") not in session.events and not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_exact_original_transaction_required_before_capability_return(monkeypatch, storage):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session)
    original = session.execute

    async def changed(statement, parameters=None):
        result = await original(statement, parameters)
        if result.kind == "source-pin":
            session.transaction = object()
        return result

    session.execute = changed
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert not session.info


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("invalid_flag", [None, 1, "true"])
async def test_api_opt_in_requires_a_server_boolean(storage, invalid_flag):
    session = _Session(storage)
    with pytest.raises(PTG2ManifestArtifactError, match="read mode"):
        await storage.locate(session, session.identity, api_read=invalid_flag)
    assert session.events == []


@pytest.mark.parametrize("storage", [membership, code])
@pytest.mark.parametrize("mutation", ["transaction", "driver", "pid", "inactive"])
async def test_install_guard_time_identity_change_refuses_before_capability(monkeypatch, storage, mutation):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session)
    original = execution.verify_bound_network_names

    async def changed(*arguments, **keywords):
        await original(*arguments, **keywords)
        if "absent_names" not in keywords:
            return
        if mutation == "transaction":
            session.transaction = object()
        elif mutation == "driver":
            session.driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: True)
        elif mutation == "pid":
            session.driver.get_server_pid = lambda: 43
        else:
            session.active = False

    monkeypatch.setattr(execution, "verify_bound_network_names", changed)
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize("storage", [membership, code])
async def test_initial_backend_pid_is_preserved_through_native_metadata(monkeypatch, storage):
    session = _Session(storage)
    _custody(monkeypatch, storage, session)
    original = session.execute

    async def changed(statement, parameters=None):
        result = await original(statement, parameters)
        if result.kind == "dependencies":
            session.driver.get_server_pid = lambda: 43
        return result

    session.execute = changed
    with pytest.raises(PTG2ManifestArtifactError, match="identity changed"):
        await storage.locate(session, session.identity, api_read=True)
    assert ("execute", "header") not in session.events and not session.info


@pytest.mark.parametrize("storage", [membership, code])
async def test_qualified_immutable_lock_column_update_remains_readable(monkeypatch, storage):
    session = _Session(storage)
    session.control.update(column_updates=[membership.LOCK_COLUMN], immutable_read_lock=True)
    _custody(monkeypatch, storage, session)
    _verify(monkeypatch, session)
    assert await storage.locate(session, session.identity, api_read=True) is not None


@pytest.mark.parametrize(
    "updates,qualified",
    [
        ([membership.LOCK_COLUMN], False),
        (["owner_id"], True),
        ([membership.LOCK_COLUMN, "reason"], True),
        (None, True),
        ((membership.LOCK_COLUMN,), True),
    ],
)
async def test_only_native_qualified_lock_column_can_have_update_privilege(monkeypatch, updates, qualified):
    session = _Session(membership)
    session.control.update(column_updates=updates, immutable_read_lock=qualified)
    _custody(monkeypatch, membership, session)
    with pytest.raises(PTG2ManifestArtifactError, match="control binding"):
        await membership.locate(session, session.identity, api_read=True)
    assert ("execute", "header") not in session.events and not session.info
