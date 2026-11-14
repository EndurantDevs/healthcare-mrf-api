# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Profile setup locks named families before reading portable publication metadata."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

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
        if self.guarded and execution._installed_read_inventory(self.session) is not None:
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
        self.current, self.kinds, self.answers = {}, {}, {"active_dependency_bindings": None}
        self.unreadable_names = set()
        self.indirect_names = set()
        self.partition_roots = set()
        self.driver = _Driver(self)
        self.catalog_change = None
        self.after_execute = None
        self.missing_sqlstate = "42P01"
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
                self.current.get(f'"mrf"."{name}"') is not None,
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

    def get_nested_transaction(self):
        return None

    @asynccontextmanager
    async def begin_nested(self):
        self.events.append("SAVEPOINT")
        try:
            yield
        except BaseException:
            self.events.append("ROLLBACK TO SAVEPOINT")
            raise
        else:
            self.events.append("RELEASE SAVEPOINT")

    def _statement_result(self, sql, parameters):
        if "SELECT name, c.relname" in sql:
            return self._catalog_result(sql, parameters["names"])
        if sql.startswith("SHOW"):
            return _Result(self, "repeatable read" if sql.endswith("transaction_isolation") else "on")
        if "SELECT name,to_regclass" in sql:
            rows = [
                (name, self.current.get(f'"{parameters["schema"]}"."{name}"') is not None)
                for name in parameters["names"]
            ]
            return _Result(self, rows)
        if sql.startswith("LOCK TABLE"):
            identifiers = sql.split('"')[1::2]
            for schema, name in zip(identifiers[::2], identifiers[1::2], strict=True):
                if name in self.unreadable_names:
                    raise DBAPIError(None, None, SimpleNamespace(sqlstate="42501"))
                if self.current.get(f'"{schema}"."{name}"') is None:
                    raise DBAPIError(None, None, SimpleNamespace(sqlstate=self.missing_sqlstate))
            return _Result(self, None)
        return _Result(
            self,
            next((value for name, value in self.answers.items() if name in sql), False),
            guarded=sql.startswith("SELECT"),
        )

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.events.append(sql)
        assert "pg_advisory" not in sql and "WHERE false" not in sql
        buffered = self._statement_result(sql, parameters)
        self.results.append(buffered)
        if self.after_execute:
            self.after_execute(sql)
        return buffered

    async def scalar(self, statement):
        return (await self.execute(statement)).scalar()


async def test_owned_profile_read_does_not_lock_unrelated_directory_detail():
    session = _Session()

    @asynccontextmanager
    async def reader_session():
        yield session

    database = SimpleNamespace(
        has_reader_session=lambda: False,
        _transaction_binding=lambda: None,
        reader_session=reader_session,
    )
    async with snapshot.provider_profile_read_snapshot(database, "mrf"):
        assert set(session.info["provider_profile_relations"]) == {f"mrf.{name}" for name in snapshot._COMMON_TABLES}
        assert not any('"provider_directory_endpoint_dataset"' in event for event in session.events)


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
                "entity_address_geo_assurance_state",
            )
        )
    )


async def test_complete_named_family_batches_locks_before_first_snapshot():
    installed = _installed(detail=True)
    session = _Session(installed=installed)
    names = (("mrf", name) for name in reversed((*installed, installed[0])))
    relations = await snapshot.lock_reader_relations(session, names)
    qualified = ", ".join(f'"mrf"."{name}"' for name in sorted(installed))
    assert relations == {f"mrf.{name}": f"mrf.{name}" for name in installed}
    assert session.events == [
        "SET LOCAL lock_timeout = '250ms'",
        "SET LOCAL statement_timeout = '5s'",
        "SAVEPOINT",
        f"LOCK TABLE {qualified} IN ACCESS SHARE MODE",
        "RELEASE SAVEPOINT",
    ]


@pytest.mark.parametrize("sqlstate", ["42P01", "3F000"])
async def test_optional_missing_name_rolls_back_batch_before_individual_locks(sqlstate):
    session = _Session(installed=("alpha", "zeta"))
    session.missing_sqlstate = sqlstate
    missing_schema = "missing" if sqlstate == "3F000" else "mrf"
    requested = (("mrf", "zeta"), (missing_schema, "optional"), ("mrf", "alpha"), ("mrf", "alpha"))
    relations = await snapshot.lock_reader_relations(session, iter(requested))
    ordered = sorted(set(requested))
    qualified = ", ".join(f'"{schema}"."{name}"' for schema, name in ordered)
    locks = [sql for sql in session.events if sql.startswith("LOCK TABLE")]
    assert locks == [f"LOCK TABLE {qualified} IN ACCESS SHARE MODE"] + [
        f'LOCK TABLE "{schema}"."{name}" IN ACCESS SHARE MODE' for schema, name in ordered
    ]
    assert session.events.index("ROLLBACK TO SAVEPOINT") < session.events.index(locks[1])
    assert relations == {"mrf.alpha": "mrf.alpha", "mrf.zeta": "mrf.zeta", f"{missing_schema}.optional": None}


@pytest.mark.parametrize("failure", ["55P03", "42501", "57014", "cancel"])
async def test_batch_lock_does_not_fallback_on_contention_permission_or_cancellation(failure):
    session = _Session(installed=("alpha", "zeta"))
    error = (
        asyncio.CancelledError() if failure == "cancel" else DBAPIError(None, None, SimpleNamespace(sqlstate=failure))
    )
    statement_result = session._statement_result

    def fail_lock(sql, parameters):
        if sql.startswith("LOCK TABLE"):
            raise error
        return statement_result(sql, parameters)

    session._statement_result = fail_lock
    with pytest.raises(type(error)) as caught:
        await snapshot.lock_reader_relations(session, (("mrf", "zeta"), ("mrf", "alpha")))
    assert caught.value is error
    assert len([sql for sql in session.events if sql.startswith("LOCK TABLE")]) == 1
    assert session.events[-1] == "ROLLBACK TO SAVEPOINT"


async def test_empty_reader_family_does_not_issue_lock():
    session = _Session()
    assert await snapshot.lock_reader_relations(session, iter(())) == {}
    assert not any(sql.startswith(("LOCK TABLE", "SAVEPOINT")) for sql in session.events)


async def test_unreadable_npi_metadata_disables_only_canonical_projection_cache():
    from api import ptg2_geo_projection as projection

    session = _Session(installed=("npi_result_generation", "entity_address_geo_assurance_state", "npi"))
    session.unreadable_names.add("npi_result_generation")
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
        assert snapshot.snapshot_relation_available("mrf.npi_result_generation") is False
        assert snapshot.snapshot_relation_available("mrf.npi") is True
        statement = projection.projected_boolean_sql(
            "a", "geo_identity_coherent", schema_name="mrf", legacy_sql="a.address_key IS NOT NULL"
        )
        assert "npi_result_generation" not in statement and "ELSE (a.address_key IS NOT NULL)" in statement
    session.unreadable_names.add("npi")
    with pytest.raises(DBAPIError):
        await snapshot.lock_reader_relations(session, (("mrf", "npi_result_generation"), ("mrf", "npi")))


@pytest.mark.parametrize("relation", [("mrf", "invalid;name"), ("invalid.schema", "npi")])
async def test_batch_lock_validates_every_identifier_before_locking(relation):
    session = _Session(installed=("alpha",))
    with pytest.raises(ValueError, match="provider_profile_relation_invalid"):
        await snapshot.lock_reader_relations(session, (("mrf", "alpha"), relation))
    assert not any(sql.startswith("LOCK TABLE") for sql in session.events)


@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize("installed", [False, True])
@pytest.mark.parametrize("verified", [False, True])
async def test_named_inventory_stays_bound_until_scope_exit(detail, installed, verified):
    session = _Session(detail=detail, installed=_installed(detail=detail) if installed else (), verified=verified)
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=detail) as actual:
        assert actual is session and not execution.has_network_read_inventory()
        assert set(snapshot._SNAPSHOT.get()) == {"mrf." + name for name in _installed(detail=detail)}
        assert snapshot.snapshot_relation_available("mrf.provider_directory_profile") is installed
        assert snapshot.snapshot_cms_serving_generation("mrf") is None
        assert session.info["provider_profile_native_authorities"] == {}
        await execution.execute_network_read(session, text("SELECT provider_payload"))
        assert session.guard_count == 0
    assert snapshot._SNAPSHOT.get() is None
    assert "provider_profile_native_authorities" not in session.info
    assert "provider_profile_cms_serving_receipt" not in session.info
    first_query = next(i for i, sql in enumerate(session.events) if sql.startswith("SELECT"))
    assert all(i < first_query for i, sql in enumerate(session.events) if sql.startswith("LOCK TABLE"))
    assert ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in session.events) is not verified


@pytest.mark.parametrize("kind", ["v", "m", "f", "p", "S"])
async def test_inventory_rejects_unsupported_installed_relation_kinds(kind):
    session = _Session(installed=("provider_directory_profile",))
    session.kinds["provider_directory_profile"] = kind
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail("unsupported relation reached a loader")


@pytest.mark.parametrize("topology", ["inherited-parent", "inherited-child", "partition-leaf"])
async def test_inventory_refuses_untracked_descendant_heaps(topology):
    session = _Session(installed=("provider_directory_profile",))
    session.indirect_names.add("provider_directory_profile")
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            pytest.fail(topology + " reached a loader")


async def test_declared_directory_roots_keep_recursive_named_locks():
    roots = snapshot._DIRECTORY_PARTITION_ROOTS
    session = _Session(installed=(*roots, "npi"))
    session.kinds.update(dict.fromkeys(roots, "p"))
    session.partition_roots.update(roots)
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
        locks = [event for event in session.events if event.startswith("LOCK TABLE")]
        assert all(f'LOCK TABLE "mrf"."{name}" IN ACCESS SHARE MODE' in locks for name in roots)
        assert all("ONLY" not in sql for sql in locks)
        assert all(snapshot.snapshot_relation_available("mrf." + name) for name in roots)


@pytest.mark.parametrize("root", sorted(snapshot._DIRECTORY_PARTITION_ROOTS))
async def test_declared_directory_name_does_not_authorize_invalid_partition_topology(root):
    session = _Session(installed=(root,))
    session.kinds[root] = "p"
    with pytest.raises(ServiceUnavailable):
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
            pytest.fail("invalid partition topology reached a loader")


async def test_optional_name_appearing_after_lock_attempt_refuses_setup():
    session = _Session()

    locked_by_name = {"mrf.provider_directory_profile": None}
    session.current['"mrf"."provider_directory_profile"'] = 99
    with pytest.raises(snapshot._SnapshotSetupChanged):
        await snapshot._inventory_relation_names(
            session, "mrf", ("provider_directory_profile",), locked_names=locked_by_name
        )


@pytest.mark.parametrize("family", ["cms-doctors", "entity-address", "receipt"])
async def test_logical_authority_does_not_read_legacy_physical_metadata(monkeypatch, family):
    session = _Session(installed=_installed(detail=True))
    session.answers = {
        "active_dependency_bindings": None,
        snapshot.reference_generation.TABLE_NAME: family == "cms-doctors",
        snapshot.address_generation.TABLE_NAME: family == "entity-address",
        snapshot._CMS_RECEIPT_TABLE: family == "receipt",
    }

    class Authority:
        @property
        def relation_oids(self):
            pytest.fail("legacy physical IDs are not authoritative")

        serving_generation = None

    authority = {"receipt": "logical"} if family == "receipt" else Authority()
    reader = AsyncMock(return_value=authority)
    monkeypatch.setattr(snapshot, "_read_doctors_generation", reader)
    monkeypatch.setattr(snapshot.address_generation, "read_entity_address_result_generation_authority", reader)
    monkeypatch.setattr(snapshot.cms_serving_receipt, "read_named_serving_receipt", reader)
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
        reader.assert_awaited_once()
        if family == "receipt":
            assert session.info["provider_profile_cms_serving_receipt"] is authority
        else:
            assert session.info["provider_profile_native_authorities"][family] is authority


@pytest.mark.parametrize("names", [snapshot._DOCTORS_TABLES, snapshot.address_generation.RELATION_NAMES])
def test_native_authority_requires_complete_logical_names(names):
    relations_by_name = {f"copy.{name}": f"copy.{name}" for name in names}
    snapshot._require_named_relations("copy", names, relations_by_name)
    relations_by_name.pop(next(iter(relations_by_name)))
    with pytest.raises(ServiceUnavailable):
        snapshot._require_named_relations("copy", names, relations_by_name)


@pytest.mark.parametrize("error", [ValueError("loader"), asyncio.CancelledError()])
async def test_scope_cleanup_preserves_loader_failure_and_cancellation(error):
    session = _Session(installed=("provider_directory_profile",))
    previous_by_family = {"cms-doctors": object()}
    session.info["provider_profile_native_authorities"] = previous_by_family
    with pytest.raises(type(error)) as caught:
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
            raise error
    assert caught.value is error and snapshot._SNAPSHOT.get() is None
    assert session.info["provider_profile_native_authorities"] is previous_by_family


async def test_optional_savepoint_retains_named_snapshot_and_cancellation():
    session = _Session(installed=("provider_directory_profile",))
    logger = SimpleNamespace(debug=Mock())
    async with snapshot._read_snapshot_scope(session, "mrf", include_detail=False):
        pinned = snapshot._SNAPSHOT.get()
        async with snapshot.provider_optional_read(session, logger):
            raise ValueError("optional")
        assert snapshot._SNAPSHOT.get() is pinned
        with pytest.raises(asyncio.CancelledError):
            async with snapshot.provider_optional_read(session, logger):
                raise asyncio.CancelledError()
    logger.debug.assert_called_once()


async def test_doctors_generation_reads_only_portable_fields():
    from tests.test_npi_native_cache_identity import revision_row

    row = revision_row()
    row["relation_oids"] = None
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row)))
    )
    authority = await snapshot._read_doctors_generation(session, "copied")
    assert authority.importer_id == "cms-doctors" and authority.relation_oids is None
    assert authority.serving_generation.origin_generation == row["origin_generation"]
    assert "relation_oids" not in str(session.execute.await_args.args[0])


@pytest.mark.parametrize("family", ["cms-doctors", "entity-address"])
@pytest.mark.parametrize("published", [False, True])
async def test_only_published_generations_require_complete_named_family(monkeypatch, family, published):
    table = (
        snapshot.reference_generation.TABLE_NAME if family == "cms-doctors" else snapshot.address_generation.TABLE_NAME
    )
    session = _Session(installed=(table,))
    session.answers[table] = True
    reader = AsyncMock(return_value=SimpleNamespace(serving_generation=object() if published else None))
    monkeypatch.setattr(snapshot, "_read_doctors_generation", reader)
    monkeypatch.setattr(snapshot.address_generation, "read_entity_address_result_generation_authority", reader)
    if published:
        with pytest.raises(ServiceUnavailable):
            async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
                pytest.fail("a published generation needs its complete named family")
    else:
        async with snapshot._read_snapshot_scope(session, "mrf", include_detail=True):
            assert session.info["provider_profile_native_authorities"][family].serving_generation is None
