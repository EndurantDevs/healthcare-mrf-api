# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Profile setup and buffered readers share one checked name/OID inventory.

These protocol doubles exercise ordering and refusals, not native custody.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import network_read_execution as execution
from api import provider_profile_snapshot as snapshot
from process import network_read_identity as identity


class _Result:
    def __init__(self, session, value, *, guarded=False):
        self.session, self.value, self.guarded = session, value, guarded
        self.closed = False
        self.guard_count = session.guard_count

    def all(self):
        return self.value

    def scalar(self):
        if self.guarded:
            assert self.session.guard_count > self.guard_count
        return self.value

    def close(self):
        self.closed = True


class _Driver:
    def __init__(self, session):
        self.session = session
        self.pid, self.active = 42, True

    def get_server_pid(self):
        return self.pid

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, query, *arguments):
        assert query == identity._BOUND_NAMES_SQL
        self.session.events.append("guard")
        self.session.guard_count += 1
        names, oids, pid, *extra = arguments
        absent = extra[0] if extra else []
        current = self.session.current
        return {
            "backend_matches": self.pid == pid,
            "relation_count": len(names),
            "valid": all(current.get(name) == oid for name, oid in zip(names, oids))
            and all(current.get(name) is None for name in absent),
        }


class _Session:
    def __init__(self, *, detail=False, installed=(), verified=True):
        self.info = {"api_reader_verified": verified}
        self._transaction = object()
        self.events, self.guard_count, self.results = [], 0, []
        self.current, self.kinds, self.answers = {}, {}, {}
        self.indirect_names = set()
        self.partition_roots = set()
        self.driver = _Driver(self)
        self.catalog_change = None
        self.after_execute = None
        self.detail = detail
        for ordinal, name in enumerate(installed, 1):
            self.current[f'"mrf"."{name}"'] = ordinal

    def in_transaction(self):
        return self.driver.active

    def get_transaction(self):
        return self._transaction if self.in_transaction() else None

    async def connection(self):
        self.events.append("driver")
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )

    def _catalog_result(self, sql, names):
        assert "c.relkind::text AS relkind" in sql
        assert "NOT c.relispartition" in sql and "inhrelid=c.oid OR inhparent=c.oid" in sql
        rows = [
            (
                name,
                self.current.get(f'"mrf"."{name}"'),
                self.kinds.get(name, "r"),
                name not in self.indirect_names,
                name in self.partition_roots,
            )
            for name in names
        ]
        result = _Result(self, rows)
        if self.catalog_change:
            self.current.update(self.catalog_change)
        return result

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.events.append(sql)
        assert "pg_advisory" not in sql and "WHERE false" not in sql
        if "SELECT name, c.oid" in sql:
            result = self._catalog_result(sql, parameters["names"])
        elif "SELECT name, to_regclass" in sql:
            result = _Result(self, [(name, self.current.get(f'"mrf"."{name}"')) for name in parameters["names"]])
        elif sql.startswith("LOCK TABLE"):
            names = ", ".join(f'"mrf"."{name}"' for name in sorted(self.partition_roots))
            assert sql == f"LOCK TABLE {names} IN ACCESS SHARE MODE"
            assert self.partition_roots <= snapshot._DIRECTORY_PARTITION_ROOTS
            result = _Result(self, None)
        else:
            is_guarded = sql.startswith("SELECT")
            result = _Result(
                self, next((value for name, value in self.answers.items() if name in sql), False), guarded=is_guarded
            )
        self.results.append(result)
        if self.after_execute:
            self.after_execute(sql)
        return result


def _installed(*, detail=False):
    return tuple(
        dict.fromkeys(
            (
                *snapshot._DOCTORS_TABLES,
                *snapshot._PROFILE_TABLES,
                "provider_directory_address_overlay",
                *snapshot.address_generation.RELATION_NAMES,
                *(snapshot._DETAIL_TABLES if detail else ()),
                snapshot._PROFILE_GENERATION_TABLE,
                snapshot.reference_generation.TABLE_NAME,
                snapshot.address_generation.TABLE_NAME,
                snapshot._CMS_RECEIPT_TABLE,
                "address_alias_state_v1",
            )
        )
    )


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("installed", [False, True])
@pytest.mark.parametrize("verified", [False, True])
async def test_complete_inventory_stays_bound_until_scope_exit(detail, installed, verified):
    session = _Session(detail=detail, installed=_installed(detail=detail) if installed else (), verified=verified)
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=detail) as actual:
        assert actual is session and execution.has_network_read_inventory()
        assert set(snapshot._SNAPSHOT.get()) == {"mrf." + name for name in _installed(detail=detail)}
        assert snapshot.snapshot_relation_available("mrf.provider_directory_profile") is installed
        assert (
            "mrf.provider_directory_endpoint" in snapshot._SNAPSHOT.get()
            if detail
            else "mrf.provider_directory_endpoint" not in snapshot._SNAPSHOT.get()
        )
        assert snapshot.snapshot_cms_serving_generation("mrf") is None
        assert session.info["provider_profile_native_authorities"] == {}
        before = session.guard_count
        await execution.execute_network_read(session, text("SELECT provider_payload"))
        assert session.guard_count == before + 1
    assert not execution.has_network_read_inventory() and snapshot._SNAPSHOT.get() is None
    assert "provider_profile_native_authorities" not in session.info
    assert "provider_profile_cms_serving_receipt" not in session.info
    assert session.events.index("driver") < next(
        i for i, value in enumerate(session.events) if value.startswith("SELECT")
    )
    assert ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in session.events) is not verified


@pytest.mark.parametrize("kind", ["v", "m", "f", "p", "S"])
async def test_inventory_rejects_unsupported_installed_relation_kinds(kind):
    session = _Session(installed=("provider_directory_profile",))
    session.kinds["provider_directory_profile"] = kind
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail("unsupported relation reached a loader")
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize("topology", ["inherited-parent", "inherited-child", "partition-leaf"])
async def test_inventory_refuses_untracked_descendant_heaps(topology):
    session = _Session(installed=("provider_directory_profile",))
    session.indirect_names.add("provider_directory_profile")
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail(topology + " reached a loader")
    assert not execution.has_network_read_inventory()


async def test_declared_directory_roots_keep_recursive_locks_and_buffered_name_guards():
    roots = snapshot._DIRECTORY_PARTITION_ROOTS
    session = _Session(installed=(*roots, "npi"))
    session.kinds.update(dict.fromkeys(roots, "p"))
    session.partition_roots.update(roots)
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
        locks = [event for event in session.events if event.startswith("LOCK TABLE")]
        assert len(locks) == 1 and '"npi"' not in locks[0] and "ONLY" not in locks[0]
        assert session.events.index(locks[0]) < next(
            index for index, event in enumerate(session.events) if event.startswith("SELECT name, to_regclass")
        )
        for root in roots:
            assert snapshot.snapshot_relation_available("mrf." + root) is True
        before = session.guard_count
        await execution.execute_network_read(session, text("SELECT directory_payload"))
        assert session.guard_count == before + 1
    assert not execution.has_network_read_inventory() and snapshot._SNAPSHOT.get() is None


@pytest.mark.parametrize("root", sorted(snapshot._DIRECTORY_PARTITION_ROOTS))
async def test_declared_directory_name_does_not_authorize_invalid_partition_topology(root):
    session = _Session(installed=(root,))
    session.kinds[root] = "p"
    with pytest.raises(ServiceUnavailable):
        await snapshot._inventory_serving_relations(session, "mrf", include_detail=True)
    assert not any(event.startswith("LOCK TABLE") for event in session.events)


@pytest.mark.parametrize("replacement", [None, 999])
async def test_partition_root_lock_is_followed_by_fresh_name_oid_recheck(replacement):
    root = "provider_directory_dataset_resource"
    session = _Session(installed=(root,))
    session.kinds[root] = "p"
    session.partition_roots.add(root)

    def replace_after_lock(sql):
        if sql.startswith("LOCK TABLE"):
            session.current[f'"mrf"."{root}"'] = replacement

    session.after_execute = replace_after_lock
    with pytest.raises(snapshot._SnapshotSetupChanged):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
            pytest.fail("replaced partition root reached a loader")
    assert not execution.has_network_read_inventory() and snapshot._SNAPSHOT.get() is None


@pytest.mark.parametrize("change", [None, 101])
async def test_initial_full_catalog_recheck_rejects_rename_or_new_optional_name(change):
    session = _Session(installed=("provider_directory_profile",))
    session.catalog_change = {'"mrf"."provider_directory_profile"': change}
    with pytest.raises(snapshot._SnapshotSetupChanged):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail("stale catalog reached a loader")
    assert "provider_profile_native_authorities" not in session.info


@pytest.mark.parametrize("change", ["installed", "absent", "pid", "transaction"])
async def test_loader_result_cannot_escape_a_changed_inventory(change):
    session = _Session(installed=("provider_directory_profile",))
    cached_results = []
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):

        def mutate(sql):
            if sql != "SELECT provider_payload":
                return
            if change == "installed":
                session.current['"mrf"."provider_directory_profile"'] = 99
            elif change == "absent":
                session.current['"mrf"."provider_profile_fact"'] = 99
            elif change == "pid":
                session.driver.pid = 43
            else:
                session.driver.active = False

        session.after_execute = mutate
        with pytest.raises(ServiceUnavailable):
            cached_results.append(await execution.execute_network_read(session, text("SELECT provider_payload")))
        assert session.results[-1].closed
    assert cached_results == [] and not execution.has_network_read_inventory()


@pytest.mark.parametrize("family", ["cms-doctors", "entity-address", "receipt"])
@pytest.mark.parametrize("drift", [False, True])
async def test_process_authority_is_guarded_before_inspection_or_metadata_publication(monkeypatch, family, drift):
    session = _Session(installed=_installed(detail=True))
    session.answers = {
        snapshot.reference_generation.TABLE_NAME: family == "cms-doctors",
        snapshot.address_generation.TABLE_NAME: family == "entity-address",
        snapshot._CMS_RECEIPT_TABLE: family == "receipt",
    }
    observed_fields = []

    class Authority:
        @property
        def relation_oids(self):
            assert session.events[-1] == "guard"
            observed_fields.append("authority")
            return None

        serving_generation = None

    async def read(*_args, **_kwargs):
        session.events.append("authority-read")
        if drift:
            session.current['"mrf"."provider_directory_profile"'] = 999
        return {"receipt": "verified"} if family == "receipt" else Authority()

    monkeypatch.setattr(snapshot.reference_generation, "read_reference_family_result_generation_authority", read)
    monkeypatch.setattr(snapshot.address_generation, "read_entity_address_result_generation_authority", read)
    monkeypatch.setattr(snapshot.cms_serving_receipt, "read_serving_receipt", read)
    if drift:
        with pytest.raises(ServiceUnavailable):
            async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
                pytest.fail("drifted authority reached metadata publication")
        assert observed_fields == [] and snapshot._SNAPSHOT.get() is None
    else:
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
            assert "authority-read" in session.events
            assert observed_fields == ([] if family == "receipt" else ["authority"])
            assert session.info["provider_profile_cms_serving_receipt"] == (
                {"receipt": "verified"} if family == "receipt" else None
            )
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize("change", ["driver", "pid", "transaction"])
async def test_setup_authority_cannot_move_to_another_backend(monkeypatch, change):
    session = _Session(installed=_installed())
    session.answers[snapshot.reference_generation.TABLE_NAME] = True

    async def read(*_args, **_kwargs):
        if change == "driver":
            session.driver = _Driver(session)
        elif change == "pid":
            session.driver.pid = 43
        else:
            session.driver.active = False
        return SimpleNamespace()

    monkeypatch.setattr(snapshot.reference_generation, "read_reference_family_result_generation_authority", read)
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail("changed backend reached metadata publication")
    assert snapshot._SNAPSHOT.get() is None and not execution.has_network_read_inventory()
    assert "provider_profile_native_authorities" not in session.info


async def test_final_setup_guard_precedes_metadata_publication(monkeypatch):
    session = _Session(installed=("provider_directory_profile",))

    async def changed_inventory(*_args, **_kwargs):
        session.current['"mrf"."provider_profile_fact"'] = 99
        return {}

    monkeypatch.setattr(snapshot, "_read_native_authorities", changed_inventory)
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail("new optional heap reached metadata publication")
    assert snapshot._SNAPSHOT.get() is None and not execution.has_network_read_inventory()
    assert "provider_profile_native_authorities" not in session.info


@pytest.mark.parametrize("mismatch", [False, True])
async def test_native_authority_oid_comparison_remains_exact(mismatch):
    oid_by_name = {"mrf." + name: ordinal for ordinal, name in enumerate(snapshot._DOCTORS_TABLES, 1)}
    authority = SimpleNamespace(
        relation_oids=tuple(oid_by_name.values()) if not mismatch else (999,) * len(oid_by_name)
    )
    if mismatch:
        with pytest.raises(ServiceUnavailable):
            snapshot._require_matching_relations(authority, "mrf", snapshot._DOCTORS_TABLES, oid_by_name)
    else:
        snapshot._require_matching_relations(authority, "mrf", snapshot._DOCTORS_TABLES, oid_by_name)


@pytest.mark.parametrize("error", [ValueError("loader"), asyncio.CancelledError()])
async def test_scope_cleanup_preserves_loader_failure_and_cancellation(error):
    session = _Session(installed=("provider_directory_profile",))
    with pytest.raises(type(error)) as caught:
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            raise error
    assert caught.value is error
    assert not execution.has_network_read_inventory() and snapshot._SNAPSHOT.get() is None
    assert "provider_profile_native_authorities" not in session.info


async def test_optional_savepoint_retains_the_inventory_and_cancellation():
    session = _Session(installed=("provider_directory_profile",))
    session.begin_nested = Mock(return_value=SimpleNamespace())
    logger = SimpleNamespace(debug=Mock())
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def savepoint():
        assert execution.has_network_read_inventory()
        yield

    session.begin_nested.return_value = savepoint()
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
        async with snapshot.provider_optional_read(session, logger):
            raise ValueError("optional")
        assert execution.has_network_read_inventory()
        session.begin_nested.return_value = savepoint()
        with pytest.raises(asyncio.CancelledError):
            async with snapshot.provider_optional_read(session, logger):
                raise asyncio.CancelledError()
    logger.debug.assert_called_once()
