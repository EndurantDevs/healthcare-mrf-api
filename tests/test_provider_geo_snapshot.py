# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed geo inventories retain native guards; protocol doubles are not custody proof."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import network_read_execution as execution
from api import provider_collection_network_read as collection
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi
from process.npi_canonical_publication import NPI_CANONICAL_TABLES
from tests.test_npi_near_snapshot import near_snapshot
from tests.test_provider_collection_network_read import _query, _read, boundary
from tests.test_provider_profile_snapshot_read_identity import _Session

_RAW_PARENTS = (
    "provider_directory_dataset_affiliation_organization",
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_resource",
)


def _database(session):
    @asynccontextmanager
    async def reader_session():
        yield session

    return SimpleNamespace(
        has_reader_session=lambda: True, _transaction_binding=lambda: None, reader_session=reader_session
    )


async def test_geo_inventory_excludes_only_unconsumed_families_and_preserves_full_profile_guard():
    session = _Session(installed=(*snapshot._GEO_TABLES, *_RAW_PARENTS))
    session.kinds.update(dict.fromkeys(_RAW_PARENTS, "p"))
    session.indirect_names.update(_RAW_PARENTS)
    session.partition_roots.update(_RAW_PARENTS)
    assert set(snapshot._GEO_TABLES) == set(snapshot._COMMON_TABLES) | set(NPI_CANONICAL_TABLES) | {
        "npi_canonical_publication_receipt",
        "npi_canonical_publication_receipt_seal",
        "npi_result_generation",
        "nucc_taxonomy",
    }
    async with snapshot.provider_geo_read_snapshot(_database(session), "mrf") as actual:
        assert actual is session and execution.has_network_read_inventory()
        assert set(snapshot._SNAPSHOT.get()) == {"mrf." + name for name in snapshot._GEO_TABLES}
        assert snapshot.snapshot_relation_available("mrf.nucc_taxonomy") is True
        assert not any("mrf." + name in snapshot._SNAPSHOT.get() for name in _RAW_PARENTS)
    assert not any(event.startswith("LOCK TABLE") for event in session.events)
    async with snapshot.provider_profile_read_snapshot(_database(session), "mrf", include_detail=True):
        assert all(snapshot.snapshot_relation_available("mrf." + name) for name in _RAW_PARENTS)
    assert sum(event.startswith("LOCK TABLE") for event in session.events) == 1
    assert snapshot._SNAPSHOT.get() is None and not execution.has_network_read_inventory()


@pytest.mark.parametrize("name", ("npi", "nucc_taxonomy", snapshot._CMS_RECEIPT_TABLE))
@pytest.mark.parametrize("damage", ("partitioned", "inherited"))
async def test_geo_snapshot_keeps_consumed_relation_topology_guard(name, damage):
    session = _Session(installed=snapshot._GEO_TABLES)
    if damage == "partitioned":
        session.kinds[name] = "p"
    else:
        session.indirect_names.add(name)
    with pytest.raises(ServiceUnavailable):
        async with snapshot.provider_geo_read_snapshot(_database(session), "mrf"):
            pytest.fail("unsupported consumed topology reached geo loader")
    assert snapshot._SNAPSHOT.get() is None and not execution.has_network_read_inventory()


@pytest.mark.parametrize("change", (None, 999))
async def test_geo_snapshot_keeps_fresh_name_and_oid_recheck(change):
    session = _Session(installed=snapshot._GEO_TABLES)
    session.catalog_change = {'"mrf"."npi"': change}
    with pytest.raises(snapshot._SnapshotSetupChanged):
        async with snapshot.provider_geo_read_snapshot(_database(session), "mrf"):
            pytest.fail("stale geo name reached loader")


async def test_geo_snapshot_preserves_cms_withdrawal_history_requirement(monkeypatch):
    session = _Session(installed=snapshot._GEO_TABLES)
    session.answers[snapshot._CMS_RECEIPT_TABLE] = True
    session.answers[snapshot._PROFILE_GENERATION_TABLE] = False
    missing_receipt = AsyncMock(return_value=None)
    monkeypatch.setattr(snapshot.cms_serving_receipt, "read_serving_receipt", missing_receipt)
    with pytest.raises(ServiceUnavailable):
        async with snapshot.provider_geo_read_snapshot(_database(session), "mrf"):
            pytest.fail("withdrawn CMS profile bypassed receipt history")
    missing_receipt.assert_awaited_once_with(session, "mrf")
    assert snapshot._SNAPSHOT.get() is None and not execution.has_network_read_inventory()


@pytest.mark.parametrize("drift", (False, True))
async def test_geo_snapshot_checks_native_address_authority_and_original_names(monkeypatch, drift):
    session = _Session(installed=snapshot._GEO_TABLES)
    session.answers[snapshot.address_generation.TABLE_NAME] = True
    pins = tuple(session.current[f'"mrf"."{name}"'] for name in snapshot.address_generation.RELATION_NAMES)

    async def read_authority(actual, *, schema_name):
        assert actual is session and schema_name == "mrf"
        if drift:
            session.current['"mrf"."npi"'] = 999
        return SimpleNamespace(relation_oids=pins)

    monkeypatch.setattr(snapshot.address_generation, "read_entity_address_result_generation_authority", read_authority)
    if drift:
        with pytest.raises(ServiceUnavailable):
            async with snapshot.provider_geo_read_snapshot(_database(session), "mrf"):
                pytest.fail("drifted geo authority reached loader")
    else:
        async with snapshot.provider_geo_read_snapshot(_database(session), "mrf"):
            assert session.info["provider_profile_native_authorities"]["entity-address"].relation_oids == pins
            await execution.execute_network_read(session, text("SELECT geo_payload"))
    assert "provider_profile_native_authorities" not in session.info


@pytest.mark.parametrize("failure", (ValueError("loader"), asyncio.CancelledError()))
async def test_geo_snapshot_restores_context_on_loader_failure(failure):
    session = _Session(installed=snapshot._GEO_TABLES)
    with pytest.raises(type(failure)) as caught:
        async with snapshot.provider_geo_read_snapshot(_database(session), "mrf"):
            raise failure
    assert caught.value is failure
    assert snapshot._SNAPSHOT.get() is None and not execution.has_network_read_inventory()


@pytest.mark.parametrize("loader_failure", (False, True))
async def test_geo_snapshot_reuses_setup_retry_without_replaying_loader(monkeypatch, loader_failure):
    opened, closed, loaded = [], [], []

    @asynccontextmanager
    async def reader_session():
        session = object()
        opened.append(session)
        try:
            yield session
        finally:
            closed.append(session)

    @asynccontextmanager
    async def read_scope(session, _schema, *, include_detail, table_names):
        assert include_detail is True and table_names is snapshot._GEO_TABLES
        if len(opened) == 1:
            raise snapshot._SnapshotSetupChanged("setup")
        yield session

    monkeypatch.setattr(snapshot, "_read_snapshot_scope", read_scope)
    database = SimpleNamespace(
        has_reader_session=lambda: False, _transaction_binding=lambda: None, reader_session=reader_session
    )
    try:
        async with snapshot.provider_geo_read_snapshot(database, "mrf") as session:
            loaded.append(session)
            if loader_failure:
                raise snapshot._SnapshotSetupChanged("loader")
    except snapshot._SnapshotSetupChanged:
        assert loader_failure
    assert len(opened) == 2 and opened == closed and loaded == opened[1:]


@pytest.mark.parametrize("kind", ("list", "geo", "batch"))
async def test_managed_geo_alone_selects_projection_snapshot(monkeypatch, boundary, kind):
    entries = []
    profile_read, geo_read = collection.provider_profile_read_snapshot, collection.provider_geo_read_snapshot

    @asynccontextmanager
    async def profile(*arguments, **keywords):
        entries.append("profile")
        async with profile_read(*arguments, **keywords) as session:
            yield session

    @asynccontextmanager
    async def geo(*arguments, **keywords):
        entries.append("geo")
        async with geo_read(*arguments, **keywords) as session:
            yield session

    monkeypatch.setattr(collection, "provider_profile_read_snapshot", profile)
    monkeypatch.setattr(collection, "provider_geo_read_snapshot", geo)
    await _read(boundary, _query(kind))
    assert entries == ["geo" if kind == "geo" else "profile"]


async def test_ordinary_near_uses_geo_snapshot_for_exact_count(monkeypatch, near_snapshot):
    monkeypatch.setattr(npi, "provider_profile_read_snapshot", AsyncMock(side_effect=AssertionError("full snapshot")))
    reply = await npi.get_near_npi(
        SimpleNamespace(
            args={"lat": "41", "long": "-87", "count_only": "1"},
            app=SimpleNamespace(),
        )
    )
    assert json.loads(reply.body) == {"rows": 1}
    assert near_snapshot.entered == near_snapshot.exited == 1
