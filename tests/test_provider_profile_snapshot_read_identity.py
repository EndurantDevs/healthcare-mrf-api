# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider names are locked before the first repeatable-read catalog snapshot."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from api import provider_profile_snapshot as snapshots
from process import entity_address_dependency_bindings as bindings


class _Session:
    def __init__(self, *, installed=(), denied=()):
        self.info = {"api_reader_verified": True}
        self.installed, self.denied = set(installed), set(denied)
        self.statements = []
        self.rollbacks = 0
        self.appearing = None

    @asynccontextmanager
    async def begin_nested(self):
        try:
            yield
        except BaseException:
            self.rollbacks += 1
            raise

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        if sql.startswith("LOCK TABLE"):
            names = sql.split('"')[3::4]
            if any(name in self.denied for name in names):
                raise DBAPIError(None, None, SimpleNamespace(sqlstate="42501"))
            if any(name not in self.installed for name in names):
                raise DBAPIError(None, None, SimpleNamespace(sqlstate="42P01"))
        if parameters is not None:
            if self.appearing is not None:
                self.installed.add(self.appearing)
            rows = [(name, name in self.installed, "r", True, False) for name in parameters["names"]]
            return SimpleNamespace(all=lambda: rows)
        return SimpleNamespace(all=lambda: [])


@pytest.fixture
def metadata(monkeypatch):
    monkeypatch.setattr(snapshots, "_requires_cms_receipt", AsyncMock(return_value=False))
    monkeypatch.setattr(snapshots, "_read_native_authorities", AsyncMock(return_value={}))
    monkeypatch.setattr(bindings, "read_active_bindings", AsyncMock(return_value=None))


@pytest.mark.parametrize("include_detail", [False, True])
@pytest.mark.parametrize("available", [False, True])
async def test_optional_relation_availability_is_scoped_and_cleaned(metadata, include_detail, available):
    names = snapshots._COMMON_TABLES + (snapshots._DETAIL_TABLES if include_detail else ())
    session = _Session(installed=names if available else ())
    async with snapshots._read_snapshot_scope(session, "published", include_detail=include_detail):
        assert snapshots.snapshot_relation_available("published.cms_doctor_education") is available
        assert session.info["provider_profile_native_authorities"] == {}
    assert snapshots.snapshot_relation_available("published.cms_doctor_education") is None
    assert session.info == {"api_reader_verified": True}
    reads = [index for index, sql in enumerate(session.statements) if sql.startswith("SELECT")]
    locks = [index for index, sql in enumerate(session.statements) if sql.startswith("LOCK TABLE")]
    assert locks and reads and max(locks) < min(reads)
    assert "pg_get_partkeydef" in session.statements[reads[0]]


@pytest.mark.parametrize("failure", [RuntimeError("loader failed"), asyncio.CancelledError()])
async def test_loader_failure_and_cancellation_cleanup_snapshot_metadata(metadata, failure):
    session = _Session()
    previous_by_family = {"previous_by_family": object()}
    session.info["provider_profile_native_authorities"] = previous_by_family
    with pytest.raises(type(failure)) as caught:
        async with snapshots._read_snapshot_scope(session, "published", include_detail=False):
            raise failure
    assert caught.value is failure
    assert session.info == {"api_reader_verified": True, "provider_profile_native_authorities": previous_by_family}
    assert snapshots.snapshot_relation_available("published.cms_doctor_education") is None


async def test_names_replaced_after_snapshot_start_fail_before_any_loader(metadata):
    session = _Session()
    session.appearing = "cms_doctor_education"
    with pytest.raises(snapshots.ServiceUnavailable, match="retry"):
        async with snapshots._read_snapshot_scope(session, "published", include_detail=False):
            pytest.fail("an unlocked newly appearing name cannot reach a loader")
    snapshots._read_native_authorities.assert_not_awaited()
    assert session.info == {"api_reader_verified": True}


async def test_complete_named_family_batches_locks_before_first_snapshot():
    session = _Session(installed=("npi", "npi_address"))
    locked = await snapshots.lock_reader_relations(session, (("published", "npi"), ("published", "npi_address")))
    assert locked == {"published.npi": "published.npi", "published.npi_address": "published.npi_address"}
    assert len([sql for sql in session.statements if sql.startswith("LOCK TABLE")]) == 1
    assert not any(sql.startswith("SELECT") for sql in session.statements)


async def test_unreadable_npi_metadata_disables_only_canonical_projection_cache():
    names = ("npi", "npi_result_generation")
    session = _Session(installed=names, denied=("npi_result_generation",))
    locked = await snapshots.lock_reader_relations(session, (("published", name) for name in names))
    assert locked == {"published.npi": "published.npi", "published.npi_result_generation": False}
    available = await snapshots._inventory_relation_names(session, "published", names, locked_names=locked)
    assert available == {"published.npi": True, "published.npi_result_generation": False}
    session.denied = {"npi"}
    with pytest.raises(DBAPIError):
        await snapshots.lock_reader_relations(session, (("published", name) for name in names))


@pytest.mark.parametrize("failure", ["55P03", asyncio.CancelledError()])
async def test_named_lock_contention_and_cancellation_propagate(failure):
    error = DBAPIError(None, None, SimpleNamespace(sqlstate=failure)) if isinstance(failure, str) else failure
    session = _Session()
    session.execute = AsyncMock(side_effect=[None, None, error])
    with pytest.raises(type(error)) as caught:
        await snapshots.lock_reader_relations(session, (("published", "npi"),))
    assert caught.value is error and session.execute.await_count == 3


@pytest.mark.parametrize("kind,direct,root", [("v", True, False), ("r", False, False), ("p", False, True)])
async def test_installed_unsupported_layout_is_refused(kind, direct, root):
    session = _Session(installed=("npi",))
    session.execute = AsyncMock(return_value=SimpleNamespace(all=lambda: [("npi", True, kind, direct, root)]))
    with pytest.raises(snapshots.ServiceUnavailable):
        await snapshots._inventory_relation_names(
            session, "published", ("npi",), locked_names={"published.npi": "published.npi"}
        )


def test_published_named_family_requires_every_member():
    with pytest.raises(snapshots.ServiceUnavailable):
        snapshots._require_named_relations("published", ("npi", "npi_address"), {"published.npi": True})
