# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native count, page and existing NPI hydration on one filtered heap."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Sanic, response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import MetaData, func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.endpoint import npi
from api.network_address_scope import (
    _request_selector,
    canonical_network_read,
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_cache_key,
    scoped_address_parameters,
    scoped_address_relation_sql,
    scoped_address_statement,
)
from api.provider_geo_sql import nearby_row_tiebreaker
from api.provider_list_sql import _is_unified_address_table, _provider_list_connection, _provider_list_parameters
from db.models import EntityAddressUnified, NPIData, NPIDataTaxonomy, NPIDataTaxonomyGroup
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_publication_postgres import _candidate_writer_roles, _publish
from tests.test_network_membership_serving_indexes_postgres import _prepare, serving_indexes_db
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.mark.parametrize("checksum", ("", "-1"))
def test_request_selector_keeps_checksum_alias_separate(checksum):
    request = SimpleNamespace(query_string=f"network_ids=42&plan_network_checksum={checksum}")
    for native_args in (None, {"network_ids": "42", "plan_network_checksum": checksum}):
        with pytest.raises(InvalidUsage, match="one explicit network selector namespace"):
            _request_selector(request, native_args)
    request.query_string = f"plan_network_checksum={checksum}"
    assert _request_selector(request, None) is None
    request.query_string = "network_ids=42"
    assert _request_selector(request, None) == {"network_ids": "42"}


@pytest.mark.asyncio
@pytest.mark.parametrize("native_npis", [None, (9000000000,)])
async def test_canonical_count_keeps_native_batch_bounds_on_the_pinned_session(native_npis):
    """A finite NPI batch must retain the canonical selector and caller transaction."""
    manifest = PinnedNetworkServingManifest(
        1,
        "00000000-0000-0000-0000-000000000001",
        "network_candidate_00000000000000000000000000000001",
        1,
        {},
        0,
        "c" * 64,
        1,
    )
    database = SimpleNamespace(
        acquire=AsyncMock(side_effect=AssertionError("pinned reads cannot acquire another pool"))
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [(1,)])))
    with network_address_read_scope(manifest, (42,)):
        parameters = _provider_list_parameters({}, None, native_npis=native_npis)
        expected_parameters_by_name = {"_canonical_network_ids": [42]}
        if native_npis is not None:
            expected_parameters_by_name["__native_batch_npis"] = list(native_npis)
        assert parameters == expected_parameters_by_name
        async with _provider_list_connection(database, None, session, native_npis=native_npis) as connection:
            assert await connection.all(text("SELECT 1"), **parameters) == [(1,)]
        assert session.execute.await_args.args[1] == expected_parameters_by_name
        with pytest.raises(ValueError, match="network_address_reserved_parameter"):
            _provider_list_parameters({"_canonical_network_ids": [88]}, None, native_npis=native_npis)
    database.acquire.assert_not_called()
    assert current_network_address_scope() is None


async def _manifest(fixture):
    address_oid = await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", fixture.copy_target.schema_name + ".entity_address_unified"
    )
    return PinnedNetworkServingManifest(
        1,
        fixture.copy_target.candidate_id,
        fixture.copy_target.schema_name,
        1,
        {"unified_address": fixture.address_source.generation_id},
        0,
        "c" * 64,
        address_oid,
    )


@pytest.mark.asyncio
async def test_count_page_and_existing_hydration_keep_exact_location(serving_indexes_db, serving_schema):
    fixture = serving_indexes_db
    manifest = await _manifest(fixture)
    _, _, engine = serving_schema
    async with async_sessionmaker(engine)() as session, session.begin():
        with network_address_read_scope(manifest, (42,)):
            relation_sql = scoped_address_relation_sql("mrf.entity_address_unified")
            assert _is_unified_address_table(relation_sql)
            assert "location_key" in nearby_row_tiebreaker(relation_sql)
            parameters_by_name = _provider_list_parameters({}, None)
            count = await session.scalar(
                text(f"SELECT count(DISTINCT npi) FROM {relation_sql} addresses"), parameters_by_name
            )
            page = (
                (
                    await session.execute(
                        text(f"SELECT location_key FROM {relation_sql} addresses ORDER BY location_key"),
                        parameters_by_name,
                    )
                )
                .scalars()
                .all()
            )
            assert count == 1 and page == ["a" * 64]
            candidates = await npi._fetch_npi_location_candidates_map([1111111111, 2222222222], session=session)
            assert (
                list(candidates) == [1111111111] and [entry["location_key"] for entry in candidates[1111111111]] == page
            )
            hydrated = await npi._fetch_npi_address_rows_map([1111111111, 2222222222], session=session)
            assert list(hydrated) == [1111111111] and len(hydrated[1111111111]) == 1
            statement = scoped_address_statement(select(func.count()).select_from(EntityAddressUnified.__table__))
            assert await session.scalar(statement, scoped_address_parameters({})) == 1
            await session.execute(text("SET LOCAL enable_seqscan=off"))
            plan = await session.scalar(
                text(f"EXPLAIN (FORMAT JSON) SELECT location_key FROM {relation_sql} addresses"), parameters_by_name
            )
            assert "canonical_network_ids_gin" in json.dumps(plan)
            first_key = scoped_address_cache_key("legacy:42")
        assert current_network_address_scope() is None
        assert scoped_address_cache_key("legacy:42") == "legacy:42" != first_key
        with network_address_read_scope(manifest, (88,)):
            assert scoped_address_cache_key("legacy:42") != first_key
            addresses_by_npi = await npi._fetch_npi_location_candidates_map([1111111111, 2222222222], session=session)
            assert list(addresses_by_npi) == [2222222222]
        with network_address_read_scope(replace(manifest, generation_id=2, manifest_sha256="d" * 64), (42,)):
            assert scoped_address_cache_key("legacy:42") != first_key


async def test_all_phone_classification_counts_use_retained_offices(serving_indexes_db, serving_schema, monkeypatch):
    """The actual list count branch needs only selected retained phone rows."""
    manifest = await _manifest(serving_indexes_db)
    _, schema, engine = serving_schema
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f'CREATE TABLE "{schema}".phone_test_taxonomy(int_code integer PRIMARY KEY,classification text NOT NULL)'
            )
        )
        await connection.execute(text(f"INSERT INTO \"{schema}\".phone_test_taxonomy VALUES(17,'Pharmacy')"))
    monkeypatch.setattr(npi, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    monkeypatch.setattr(
        npi,
        "_taxonomy_classification_subquery",
        lambda conditions: f'(SELECT * FROM "{schema}".phone_test_taxonomy WHERE {conditions}) AS q',
    )
    async with async_sessionmaker(engine)() as session, session.begin():
        request = SimpleNamespace(
            args={"count_only": "1", "format": "all", "phone": "2125550100", "primary_only": "0"},
            app=SimpleNamespace(),
            ctx=SimpleNamespace(sa_session=session),
        )
        for network_id, expected in ((42, {"Pharmacy": 1}), (88, {"Pharmacy": 1}), (99, {})):
            with network_address_read_scope(manifest, (network_id,)):
                result = await npi.get_all(request)
            assert json.loads(result.body) == {"rows": expected}


def _canonical_read_app(engine, invoked_generations):
    """Register request sessions and actual shared hydration for native HTTP proof."""
    app = Sanic("canonical_network_read_" + uuid4().hex)
    sessions = async_sessionmaker(engine)

    @app.middleware("request")
    async def open_session(request):
        """Reuse the request-owned SQLAlchemy/native connection boundary."""
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, result):
        """Release any pinned heap after the complete handler result."""
        await request.ctx.sa_session.close()

    @app.get("/addresses")
    @canonical_network_read
    async def read_addresses(request):
        """Run actual shared NPI hydration under the registered scope wrapper."""
        invoked_generations.append(current_network_address_scope().manifest.generation_id)
        addresses_by_npi = await npi._fetch_npi_location_candidates_map(
            [1111111111, 2222222222, 3333333333],
            session=request.ctx.sa_session,
        )
        return response.json(
            {"locations": [entry["location_key"] for rows in addresses_by_npi.values() for entry in rows]}
        )

    @app.get("/absent")
    @canonical_network_read
    async def absent_provider(request):
        """Match the detail handler's exact-location absence exception."""
        raise NotFound("Provider has no location in this network.")

    @app.get("/provider/<provider_npi>")
    async def read_provider(request, provider_npi):
        """Exercise the real provider detail guard before any mutable enrichment."""
        return await npi.get_npi(request, provider_npi)

    return app


async def _assert_rejected_selectors(app, headers_by_name, invoked_generations):
    _, denied = await app.asgi_client.get("/addresses?network_ids=42")
    assert denied.status == 403 and invoked_generations == []
    for query in (
        "network_ids=0",
        "network_ids=42&network_ids=88",
        "network_ids=42&network_generation=01",
        "network_ids=42&plan_network=42",
        "network_ids=42&checksum_network=42",
        "network_ids=42&plan_network_checksum=-1",
        "network_ids=42&plan_network_checksum=",
        "network_ids=42&custom_import=example",
        "network_generation=1",
        "network_ids=",
    ):
        _, rejected = await app.asgi_client.get("/addresses?" + query, headers=headers_by_name)
        assert rejected.status == 400 and invoked_generations == []


@pytest.mark.asyncio
async def test_registered_http_wrapper_uses_verified_manifest_and_no_fallback(
    serving_indexes_db, serving_schema, monkeypatch
):
    """Require real frozen publication for every authenticated HTTP address read."""
    fixture = serving_indexes_db
    _, _, engine = serving_schema
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-read-token")
    async with fixture.connection.transaction():
        await _prepare(fixture)
    async with _candidate_writer_roles(fixture):
        async with fixture.connection.transaction():
            manifest = await _publish(fixture)
        invoked_generations = []
        app = _canonical_read_app(engine, invoked_generations)
        headers_by_name = {"Authorization": "Bearer synthetic-network-read-token"}
        await _assert_rejected_selectors(app, headers_by_name, invoked_generations)
        _, invalid_access = await app.asgi_client.get(
            "/addresses?network_ids=42",
            headers={**headers_by_name, "X-Network-Access-Scope": "invalid"},
        )
        assert invalid_access.status == 400 and invoked_generations == []
        _, member = await app.asgi_client.get("/addresses?network_ids=42", headers=headers_by_name)
        assert member.status == 200 and member.json == {"locations": ["a" * 64]}
        assert member.headers["X-Network-Generation"] == str(manifest["generation_id"])
        assert member.headers["Cache-Control"] == "private, no-store"
        _, missing_provider = await app.asgi_client.get("/absent?network_ids=42", headers=headers_by_name)
        assert missing_provider.status == 404
        assert missing_provider.headers["X-Network-Generation"] == str(manifest["generation_id"])
        assert missing_provider.headers["Cache-Control"] == "private, no-store"
        for update_flag in ("sync_geocode", "force_address_update"):
            _, update_rejected = await app.asgi_client.get(
                "/provider/1111111111?network_ids=42&" + update_flag + "=true", headers=headers_by_name
            )
            assert update_rejected.status == 400
            assert update_rejected.headers["X-Network-Generation"] == str(manifest["generation_id"])
        _, absent = await app.asgi_client.get("/addresses?network_ids=99", headers=headers_by_name)
        assert absent.status == 200 and absent.json == {"locations": []}
        _, retained = await app.asgi_client.get(
            f"/addresses?network_ids=42&network_generation={manifest['generation_id']}",
            headers=headers_by_name,
        )
        assert retained.status == 200 and retained.json == member.json
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
            manifest["generation_id"],
        )
        _, unavailable = await app.asgi_client.get("/addresses?network_ids=42", headers=headers_by_name)
        assert unavailable.status == 503 and len(invoked_generations) == 3
        assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_detail_aggregate_and_count_use_same_native_address_heap(serving_indexes_db, serving_schema, monkeypatch):
    fixture = serving_indexes_db
    manifest = await _manifest(fixture)
    _, schema, engine = serving_schema
    monkeypatch.setenv("DB_SCHEMA", schema)
    identity_metadata = MetaData()
    for model in (NPIData, NPIDataTaxonomy, NPIDataTaxonomyGroup):
        model.__table__.to_metadata(identity_metadata, schema=schema)
    async with engine.begin() as connection:
        await connection.run_sync(identity_metadata.create_all)
        await connection.execute(text(f'INSERT INTO "{schema}".npi(npi) VALUES(1111111111),(2222222222)'))
    scoped_engine = engine.execution_options(schema_translate_map={NPIData.__table__.schema: schema})
    async with async_sessionmaker(scoped_engine)() as session, session.begin():
        with network_address_read_scope(manifest, (42,)):
            details = await npi._build_npi_details(1111111111, address_limit=10, session=session)
            assert details["npi"] == 1111111111 and details["address_total"] == 1
            assert len(details["address_list"]) == 1
            assert "canonical_network_ids" not in details["address_list"][0]
            other_details = await npi._build_npi_details(2222222222, address_limit=10, session=session)
            assert other_details["address_total"] == 0 and other_details["address_list"] == []


@pytest.mark.asyncio
async def test_native_batch_candidates_exclude_provider_wide_overlay(serving_indexes_db, serving_schema, monkeypatch):
    fixture = serving_indexes_db
    manifest = await _manifest(fixture)
    _, _, engine = serving_schema

    async def reject_overlay(*args, **kwargs):
        """Unbound provider-wide locations cannot be fetched for canonical reads."""
        pytest.fail("Canonical membership must not use provider-wide overlays")

    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay_map", reject_overlay)
    async with async_sessionmaker(engine)() as session, session.begin():
        with network_address_read_scope(manifest, (42,)):
            ranked = await npi._rank_npi_batch_addresses([1111111111, 2222222222], session=session)
            assert len(ranked[1111111111]) == 1 and ranked[2222222222] == []
            not_found, was_found = npi._npi_batch_provider_result(
                2222222222,
                {"npi": 2222222222},
                [],
                [],
                [],
                None,
                {"include_sources": False, "include_evidence": False, "address_limit": 10, "address_offset": 0},
            )
            assert not_found["status"] == 404 and was_found is False


@pytest.mark.asyncio
async def test_empty_scope_result_never_relaxes_to_checksum_or_other_office(serving_indexes_db, serving_schema):
    fixture = serving_indexes_db
    manifest = await _manifest(fixture)
    _, _, engine = serving_schema
    async with async_sessionmaker(engine)() as session, session.begin():
        with network_address_read_scope(manifest, (99,)):
            assert await npi._fetch_npi_location_candidates_map([1111111111, 3333333333], session=session) == {}
            assert await npi._fetch_npi_address_rows_map([1111111111, 3333333333], session=session) == {}
            with pytest.raises(ValueError, match="reserved_parameter"):
                scoped_address_parameters({"_canonical_network_ids": [42]})
            with pytest.raises(ValueError, match="read_only"):
                scoped_address_statement(EntityAddressUnified.__table__.update().values(archived=True))
        for selectors in ((), (True,), (0,), (2147483648,), (42, 42), (88, 42), ("42",)):
            with pytest.raises(ValueError, match="scope_invalid"):
                with network_address_read_scope(manifest, selectors):
                    pytest.fail("Invalid selectors cannot bind a scope")
        assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_count_cache_tracks_provider_publication_and_hides_membership_ids(serving_indexes_db, monkeypatch):
    """A fixed address generation cannot retain counts across provider publication."""
    manifest = await _manifest(serving_indexes_db)

    async def current_publication():
        return publication_identity

    monkeypatch.setattr(npi, "_npi_canonical_publication_identity", current_publication)
    with network_address_read_scope(manifest, (42,)):
        publication_identity = "1:synthetic-provider-generation"
        first_key = await npi._npi_count_cache_identity(EntityAddressUnified)
        publication_identity = "2:synthetic-provider-generation"
        assert await npi._npi_count_cache_identity(EntityAddressUnified) != first_key
        publication_identity = None
        assert await npi._npi_count_cache_identity(EntityAddressUnified) is None
    public_address = npi._redact_internal_address_fields({"address": "Office A", "canonical_network_ids": [42, 88]})
    assert public_address == {"address": "Office A"}


@pytest.mark.asyncio
async def test_nearby_continuation_binds_actual_manifest_and_current_access(serving_indexes_db):
    """Unchanged query text cannot retain a cursor across policy or manifest changes."""
    manifest = await _manifest(serving_indexes_db)
    query_by_name = {"network_ids": "42", "network_generation": "1", "lat": "40", "long": "-73"}
    legacy_scope = npi._nearby_cursor_scope(query_by_name)
    address_key = str(uuid4())
    with network_address_read_scope(manifest, (42,), access_scope_sha256="a" * 64):
        first_scope = npi._nearby_cursor_scope(query_by_name)
        cursor = npi._encode_nearby_cursor(first_scope, 1.0, 1111111111, address_key)
        assert npi._decode_nearby_cursor(cursor, first_scope) == (1.0, 1111111111, address_key)
        first_cache_key = scoped_address_cache_key("provider-detail")
        assert first_scope != legacy_scope
    for changed_manifest, access_digest in (
        (manifest, "b" * 64),
        (replace(manifest, manifest_sha256="d" * 64), "a" * 64),
    ):
        with network_address_read_scope(changed_manifest, (42,), access_scope_sha256=access_digest):
            assert scoped_address_cache_key("provider-detail") != first_cache_key
            with pytest.raises(InvalidUsage, match="cursor is invalid"):
                npi._decode_nearby_cursor(cursor, npi._nearby_cursor_scope(query_by_name))
    assert npi._nearby_cursor_scope(query_by_name) == legacy_scope
