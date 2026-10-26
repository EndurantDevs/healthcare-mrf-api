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

from api.endpoint import npi, plan
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
from db.models import EntityAddressUnified, Issuer, NPIData, NPIDataTaxonomy, NPIDataTaxonomyGroup, PlanNetworkTierRaw
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


@pytest.fixture
async def aca_metadata_db(serving_indexes_db, serving_schema, monkeypatch):
    """Persist raw metadata and approved-map rows; bind fixture authority separately."""
    _, schema, engine = serving_schema
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", schema)
    metadata = MetaData()
    tiers = PlanNetworkTierRaw.__table__.to_metadata(metadata, schema=schema, name="scope_plan_tiers")
    issuers = Issuer.__table__.to_metadata(metadata, schema=schema, name="scope_plan_issuers")
    monkeypatch.setattr(plan, "plan_network_tier_table", tiers)
    monkeypatch.setattr(plan, "issuer_table", issuers)
    plan_ids = tuple("12345NY000000" + str(index) for index in (1, 2, 3))
    async with engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            issuers.insert(),
            [{"issuer_id": 12345, "issuer_name": "Example issuer", "issuer_marketing_name": "Example", "state": "NY"}],
        )
        await connection.execute(
            tiers.insert(),
            [
                {
                    "plan_id": plan_id,
                    "year": 2026,
                    "checksum_network": 42,
                    "issuer_id": 12345,
                    "network_tier": "Example-tier",
                }
                for plan_id in plan_ids
            ],
        )
    manifest = replace(await _manifest(serving_indexes_db), approved_custom_revision=8)
    fixture = SimpleNamespace(schema=schema, engine=engine, manifest=manifest, plan_ids=plan_ids)
    await _insert_aca_metadata_binding(fixture, 8, plan_ids[0], 84)
    await _insert_aca_metadata_binding(fixture, 8, plan_ids[1], 42)
    return fixture


def _aca_metadata_binding(plan_id, network_id):
    return {
        "network_id": network_id,
        "archived": False,
        "source_system": "aca",
        "source_id": "example-aca",
        "dataset_schema": "example_source",
        "dataset_id": "example-dataset",
        "producer_id": "example-producer",
        "edition_id": "example-edition",
        "source_key": plan_id,
        "source_scope_json": {
            "plan_id": plan_id,
            "plan_year": 2026,
            "issuer_id": "12345",
            "state": "NY",
            "checksum_network": 42,
        },
    }


async def _insert_aca_metadata_binding(fixture, approved_revision, plan_id, network_id):
    """Use migrated approved storage without claiming approval/publication authority."""
    async with fixture.engine.begin() as connection:
        await connection.execute(
            text(
                f'INSERT INTO "{fixture.schema}".registry_approved_record '
                "(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) "
                "VALUES(:revision,'network_binding',:key,1,1,CAST(:document AS jsonb))"
            ),
            {
                "revision": approved_revision,
                "key": str(uuid4()),
                "document": json.dumps(_aca_metadata_binding(plan_id, network_id)),
            },
        )


async def test_aca_metadata_distinguishes_checksum_and_exact_approved_plan(aca_metadata_db):
    """A shared checksum cannot disclose another target or an unapproved raw plan."""
    fixture = aca_metadata_db
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        request = SimpleNamespace(args={}, query_string="", ctx=SimpleNamespace(sa_session=session))
        legacy = await plan.get_network_by_checksum(request, "42")
        assert {(entry["plan_id"], entry["year"]) for entry in json.loads(legacy.body)["plans"]} == {
            (plan_id, 2026) for plan_id in fixture.plan_ids
        }
        for network_id, plan_id in ((84, fixture.plan_ids[0]), (42, fixture.plan_ids[1])):
            with network_address_read_scope(fixture.manifest, (network_id,)):
                scoped = await plan.get_network_by_checksum(request, "42")
                document = json.loads(scoped.body)
                assert document["plans"] == [{"plan_id": plan_id, "year": 2026}]
                assert document["checksum"] == 42 and document["issuer_state"] == "NY"
                assert document["issuer"] == 12345
                batch = await plan.get_network_batch_by_checksums(request, "42,42")
                assert json.loads(batch.body) == [document]
        with network_address_read_scope(fixture.manifest, (99,)):
            assert await plan._fetch_network_entry(session, 42) is None
            with pytest.raises(NotFound):
                await plan.get_network_by_checksum(request, "42")
            with pytest.raises(NotFound):
                await plan.get_network_batch_by_checksums(request, "42")
        assert current_network_address_scope() is None
        assert (await plan.get_network_by_checksum(request, "42")).body == legacy.body


async def test_aca_metadata_retains_bound_approval_after_current_revision_changes(aca_metadata_db):
    """SQL uses the bound approved revision, not the mutable singleton head."""
    fixture = aca_metadata_db
    await _insert_aca_metadata_binding(fixture, 9, fixture.plan_ids[0], 42)
    await _insert_aca_metadata_binding(fixture, 9, fixture.plan_ids[1], 84)
    async with fixture.engine.begin() as connection:
        await connection.execute(
            text(f'UPDATE "{fixture.schema}".registry_revision_control SET draft_revision=9,approved_revision=9')
        )
    current = replace(fixture.manifest, generation_id=2, approved_custom_revision=9)
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        for manifest, plan_id in ((fixture.manifest, fixture.plan_ids[0]), (current, fixture.plan_ids[1])):
            with network_address_read_scope(manifest, (84,)):
                document = await plan._fetch_network_entry(session, 42)
                assert document["plans"] == [{"plan_id": plan_id, "year": 2026}]
        await session.execute(
            text(
                f'UPDATE "{fixture.schema}".registry_approved_record '
                "SET record_json=jsonb_set(record_json,'{archived}','true') "
                "WHERE approved_revision=9 AND record_json->>'network_id'='84'"
            )
        )
        with network_address_read_scope(current, (84,)):
            assert await plan._fetch_network_entry(session, 42) is None
        with network_address_read_scope(fixture.manifest, (84,)):
            assert (await plan._fetch_network_entry(session, 42))["plans"] == [
                {"plan_id": fixture.plan_ids[0], "year": 2026}
            ]


async def test_aca_metadata_requires_every_scope_coordinate_and_active_source(aca_metadata_db):
    """Actual SQL refuses each independent mismatch and never returns raw fallback."""
    fixture = aca_metadata_db
    approved = _aca_metadata_binding(fixture.plan_ids[0], 84)
    mutations = [
        ("plan_id", "12345NY0000099"),
        ("plan_year", 2025),
        ("issuer_id", "54321"),
        ("state", "CA"),
        ("checksum_network", -17),
    ]
    documents = [
        approved | {"source_scope_json": approved["source_scope_json"] | {field: changed}}
        for field, changed in mutations
    ]
    documents += [approved | {"network_id": 85}, approved | {"archived": True}, approved | {"source_system": "fhir"}]
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        with network_address_read_scope(fixture.manifest, (84,)):
            assert (await plan._fetch_network_entry(session, 42))["plans"] == [
                {"plan_id": fixture.plan_ids[0], "year": 2026}
            ]
            for document in documents:
                await session.execute(
                    text(
                        f'UPDATE "{fixture.schema}".registry_approved_record SET record_json=CAST(:document AS jsonb) '
                        "WHERE approved_revision=8 AND record_json->>'network_id' IN ('84','85')"
                    ),
                    {"document": json.dumps(document)},
                )
                assert await plan._fetch_network_entry(session, 42) is None, document
            await session.execute(
                text(
                    f'UPDATE "{fixture.schema}".registry_approved_record SET record_json=CAST(:document AS jsonb) '
                    "WHERE approved_revision=8 AND record_json->>'network_id'='84'"
                ),
                {"document": json.dumps(approved)},
            )
            assert (await plan._fetch_network_entry(session, 42))["plans"] == [
                {"plan_id": fixture.plan_ids[0], "year": 2026}
            ]


@pytest.fixture
async def default_address_db(validation_db, monkeypatch):
    """Seed public and custom origins before projection, with independent publisher masks."""
    from tests.test_network_membership_candidate_indexes_postgres import _project_validated
    from tests.test_network_membership_serving_indexes_postgres import _full_source, _more_source_rows
    from tests.test_network_membership_validation_postgres import _batch

    fixture = validation_db
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    await _full_source(fixture)
    await fixture.connection.execute(
        f'''UPDATE "{fixture.control_schema}".retained_addresses SET
        address_source_mask=CASE location_key WHEN $1 THEN 128 WHEN $2 THEN 0 ELSE 61 END,
        row_origin=CASE WHEN location_key IN ($1,$2) THEN 'base' ELSE 'network_custom' END''',
        "a" * 64,
        "b" * 64,
    )
    await _more_source_rows(fixture, 2)
    await fixture.connection.execute(
        f'''UPDATE "{fixture.control_schema}".retained_addresses SET
        row_origin=CASE location_key WHEN $1 THEN 'base' ELSE 'ptg_overlay' END,
        address_source_mask=CASE location_key WHEN $1 THEN 1048576 ELSE 0 END
        WHERE location_key IN ($1,$2)''',
        "0" * 63 + "1",
        "0" * 63 + "2",
    )
    location = uuid4()
    await fixture.connection.execute(
        f'''INSERT INTO "{fixture.copy_target.schema_name}".provider_location_binding
        VALUES('npi','provider-c',$1,$2,'npi','provider-c')''',
        location,
        "d" * 64,
    )
    await fixture.connection.execute(
        f'''INSERT INTO "{fixture.copy_target.schema_name}".network_membership
        VALUES(7,'npi','provider-c',$1,'archived-only')''',
        location,
    )
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=5,accepted_rows=5'
    )
    await _batch(fixture, 1)
    for revision, network_id, archived in ((8, 7, True), (8, 42, False), (8, 88, False), (9, 7, False)):
        await fixture.connection.execute(
            f'''INSERT INTO "{fixture.control_schema}".registry_approved_record
            (approved_revision,record_kind,record_key,record_revision,custom_revision,record_json)
            VALUES($1,'network',$2,1,1,$3::jsonb)''',
            revision,
            str(network_id),
            json.dumps({"network_id": network_id, "archived": archived}),
        )
    await _project_validated(fixture)
    return fixture


@pytest.fixture
async def default_scope_reader(default_address_db, serving_schema):
    """Use an actual LOGIN without administrator-pool or SET ROLE inheritance."""
    import secrets

    from sqlalchemy.ext.asyncio import create_async_engine

    fixture = default_address_db
    _, _, engine = serving_schema
    role = "default_scope_" + uuid4().hex
    password = secrets.token_urlsafe(24)
    reader_engine = None
    try:
        await fixture.connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        for schema in (fixture.control_schema, fixture.copy_target.schema_name):
            await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        await fixture.connection.execute(
            f'GRANT SELECT ON "{fixture.control_schema}".registry_approved_record TO "{role}"'
        )
        await fixture.connection.execute(
            f'GRANT SELECT ON "{fixture.copy_target.schema_name}".entity_address_unified TO "{role}"'
        )
        reader_engine = create_async_engine(engine.url.set(username=role, password=password))
        yield reader_engine, role
    finally:
        if reader_engine is not None:
            await reader_engine.dispose()
        await fixture.connection.execute(f'DROP OWNED BY "{role}"')
        await fixture.connection.execute(f'DROP ROLE IF EXISTS "{role}"')


async def _assert_default_office_reads(session, relation, parameters, expected_keys):
    """Check native counts, candidates and hydration against the same scoped offices."""
    keys = (
        (
            await session.execute(
                text(f"SELECT location_key FROM {relation} addresses ORDER BY location_key"), parameters
            )
        )
        .scalars()
        .all()
    )
    assert keys == expected_keys
    statement = scoped_address_statement(select(func.count()).select_from(EntityAddressUnified.__table__))
    assert await session.scalar(statement, parameters) == len(expected_keys)
    candidates = await npi._fetch_npi_location_candidates_map([1111111111, 2222222222, 3333333333], session=session)
    hydrated = await npi._fetch_npi_address_rows_map([1111111111, 2222222222, 3333333333], session=session)
    for addresses_by_npi in (candidates, hydrated):
        assert (
            sorted(office["location_key"] for offices in addresses_by_npi.values() for office in offices)
            == expected_keys
        )
    assert {office["telephone_number"] for office in hydrated[1111111111]} == {"212-555-0100"}


async def test_default_scope_preserves_public_offices_and_filters_custom_memberships(
    default_address_db, default_scope_reader
):
    fixture = default_address_db
    engine, role = default_scope_reader
    manifest = replace(await _manifest(fixture), approved_custom_revision=8)
    async with async_sessionmaker(engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        assert (await session.execute(text("SELECT session_user,current_user"))).one() == (role, role)
        assert not await session.scalar(
            text(
                "SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls FROM pg_roles WHERE rolname=current_user"
            )
        )
        public_keys = ["0" * 63 + "1", "0" * 63 + "2", "a" * 64, "b" * 64]
        for exclusions, expected in (
            ((), public_keys + ["c" * 64]),
            ((88,), public_keys),
            ((42, 88), public_keys),
        ):
            with network_address_read_scope(
                manifest, None, excluded_network_ids=exclusions, access_scope_sha256="a" * 64
            ):
                relation = scoped_address_relation_sql("mrf.entity_address_unified")
                parameters = scoped_address_parameters({})
                await _assert_default_office_reads(session, relation, parameters, expected)
        with network_address_read_scope(manifest, (7,)):
            relation = scoped_address_relation_sql("mrf.entity_address_unified")
            keys = (
                (
                    await session.execute(
                        text(f"SELECT location_key FROM {relation} addresses ORDER BY location_key"),
                        scoped_address_parameters({}),
                    )
                )
                .scalars()
                .all()
            )
            assert keys == ["a" * 64, "d" * 64]
        current = replace(manifest, approved_custom_revision=9)
        with network_address_read_scope(current, None, excluded_network_ids=(), access_scope_sha256="b" * 64):
            relation = scoped_address_relation_sql("mrf.entity_address_unified")
            keys = (
                (
                    await session.execute(
                        text(f"SELECT location_key FROM {relation} addresses ORDER BY location_key"),
                        scoped_address_parameters({}),
                    )
                )
                .scalars()
                .all()
            )
            assert keys == public_keys + ["d" * 64]


def _default_policy_manifest():
    return PinnedNetworkServingManifest(
        1,
        "00000000-0000-0000-0000-000000000001",
        "network_candidate_00000000000000000000000000000001",
        1,
        {},
        8,
        "c" * 64,
        1,
    )


def test_default_scope_requires_closed_policy_and_keeps_nested_selected_identity():
    manifest = _default_policy_manifest()
    for invalid in (None, [], (True,), (0,), (-1,), (2147483648,), (42, 42), (88, 42), tuple(range(1, 50002))):
        with pytest.raises(ValueError, match="scope_invalid"):
            with network_address_read_scope(manifest, None, excluded_network_ids=invalid, access_scope_sha256="a" * 64):
                pytest.fail("Invalid policy cannot bind a scope")
    with network_address_read_scope(manifest, (42,)):
        selected = current_network_address_scope()
        selected_sql, selected_key = (
            scoped_address_relation_sql("mrf.entity_address_unified"),
            scoped_address_cache_key("detail"),
        )
        assert (
            selected_sql
            == '(SELECT * FROM "network_candidate_00000000000000000000000000000001".entity_address_unified WHERE canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[]))'
        )
        with network_address_read_scope(manifest, None, excluded_network_ids=(88,), access_scope_sha256="a" * 64):
            assert current_network_address_scope().network_ids is None
            assert scoped_address_cache_key("detail") != selected_key
            assert scoped_address_parameters(scoped_address_parameters({})) == {
                "_network_excluded_ids": [88],
                "_network_approved_revision": 8,
            }
            for parameters in (
                {"_canonical_network_ids": [42]},
                {"_network_excluded_ids": [42]},
                {"_network_approved_revision": 9},
                {"_network_approved_revision": 8.0},
                {"_network_excluded_ids": [88.0]},
            ):
                with pytest.raises(ValueError, match="reserved_parameter"):
                    scoped_address_parameters(parameters)
        assert current_network_address_scope() is selected
        assert scoped_address_relation_sql("mrf.entity_address_unified") == selected_sql
        assert scoped_address_cache_key("detail") == selected_key
    assert current_network_address_scope() is None
    with pytest.raises(ValueError, match="access_scope_invalid"):
        with network_address_read_scope(manifest, None, excluded_network_ids=()):
            pytest.fail("Default policy requires its verified digest")
    with network_address_read_scope(
        manifest, None, excluded_network_ids=tuple(range(1, 50001)), access_scope_sha256="a" * 64
    ):
        assert len(scoped_address_parameters({})["_network_excluded_ids"]) == 50000
    keys = set()
    for exclusions, digest, revision in (((), "a" * 64, 8), ((88,), "a" * 64, 8), ((), "b" * 64, 8), ((), "a" * 64, 9)):
        with network_address_read_scope(
            replace(manifest, approved_custom_revision=revision),
            None,
            excluded_network_ids=exclusions,
            access_scope_sha256=digest,
        ):
            keys.add(scoped_address_cache_key("detail"))
    assert len(keys) == 4


async def test_default_aca_clause_uses_active_approved_identity_and_exact_coordinates(aca_metadata_db):
    fixture = aca_metadata_db
    async with fixture.engine.begin() as session:
        for network_id, archived in ((42, True), (84, False)):
            await session.execute(
                text(f'''INSERT INTO "{fixture.schema}".registry_approved_record
            (approved_revision,record_kind,record_key,record_revision,custom_revision,record_json)
            VALUES(8,'network',:key,1,1,CAST(:document AS jsonb))'''),
                {"key": str(network_id), "document": json.dumps({"network_id": network_id, "archived": archived})},
            )
        for exclusions, expected in (((), [fixture.plan_ids[0]]), ((84,), [])):
            with network_address_read_scope(
                fixture.manifest, None, excluded_network_ids=exclusions, access_scope_sha256="a" * 64
            ):
                result = await plan._fetch_network_entry(session, 42)
                assert ([] if result is None else [entry["plan_id"] for entry in result["plans"]]) == expected


async def test_managed_insurance_uses_visible_canonical_membership_before_count_and_page(
    default_address_db, default_scope_reader
):
    """Legacy-zero offices can be insured; public contact visibility grants no private membership."""
    from api.network_address_scope import scoped_address_insurance_predicate

    fixture = default_address_db
    engine, role = default_scope_reader
    # This fixture is still an isolated projected candidate. Preserve its genuine
    # membership projection while making the old checksum array uninformative.
    await fixture.connection.execute(
        f'UPDATE "{fixture.copy_target.schema_name}".entity_address_unified SET plans_network_array=ARRAY[0]::integer[]'
    )
    manifest = replace(await _manifest(fixture), approved_custom_revision=8)
    async with async_sessionmaker(engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        assert (await session.execute(text("SELECT session_user,current_user"))).one() == (role, role)
        request = SimpleNamespace(
            args={"has_insurance": "1", "count_only": "1"},
            ctx=SimpleNamespace(sa_session=session),
            app=SimpleNamespace(),
        )
        for revision, selected, excluded, expected in (
            (8, None, (), ["a" * 64, "c" * 64]),
            (8, None, (88,), ["a" * 64]),
            (8, None, (42,), ["c" * 64]),
            (8, None, (42, 88), []),
            (8, (42,), None, ["a" * 64]),
            (8, (88,), None, ["c" * 64]),
            (9, None, (), ["a" * 64, "d" * 64]),
        ):
            options = {"excluded_network_ids": excluded} if selected is None else {}
            with network_address_read_scope(
                replace(manifest, approved_custom_revision=revision), selected, access_scope_sha256="a" * 64, **options
            ):
                relation = scoped_address_relation_sql("mrf.entity_address_unified")
                predicate = scoped_address_insurance_predicate()
                parameters = scoped_address_parameters({})
                await _assert_insurance_scope_pages(session, request, relation, predicate, parameters, expected)


@pytest.mark.parametrize("issuer_id", ["12345", 12345], ids=["approved-string", "invalid-number"])
async def test_pharmacy_reports_keep_exact_scoped_offices_aca_and_private_policy(
    default_address_db, default_scope_reader, monkeypatch, issuer_id
):
    """Run real report SQL as the ordinary Reader, including two offices for one NPI."""
    import datetime

    from api.endpoint import reports

    fixture = default_address_db
    role = default_scope_reader[1]
    await _seed_pharmacy_report_evidence(fixture, role, monkeypatch, issuer_id=issuer_id)
    manifest = replace(await _manifest(fixture), approved_custom_revision=8)
    async with async_sessionmaker(default_scope_reader[0])() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        assert (await session.execute(text("SELECT session_user,current_user"))).one() == (role, role)

        reader = _ScopedReportSession(session, fixture.control_schema)
        for selected, exclusions, expected_total, expected_aca in (
            ((42,), None, 1, 1),
            ((88,), None, 1, 0),
            (None, (88,), 2, 1),
        ):
            options = {"excluded_network_ids": exclusions} if selected is None else {}
            with network_address_read_scope(manifest, selected, access_scope_sha256="a" * 64, **options):
                summary, _, _, helper = await reports._query_chain_summary(reader, names=["%"])
                assert summary["pharmacy_npi_count"] == expected_total and helper is False
                assert summary["insured_pharmacy_npi_count"] == 1
                states, _ = await reports._query_pharmacy_state_stats(reader)
                assert sum(state_stats_row["aca_pharmacies"] for state_stats_row in states) == (
                    expected_aca if isinstance(issuer_id, str) else 0
                )
                query = reports._MarketSummaryQuery(
                    "state", "pharmacy_count", "desc", datetime.date(2026, 1, 1), True, 1, 0
                )
                total, first = await reports._query_market_summaries(reader, query)
                assert total == (3 if selected is None else 1) and len(first) == 1
                if selected is not None:
                    assert first[0]["state"] == ("NY" if selected == (42,) else "TX")
                count, last = await reports._query_market_summaries(reader, replace(query, offset=total))
                assert count == total and last == []
                context = await reports._fetch_pharmacy_context(reader, npi=1111111111, as_of=query.as_of)
                assert context is None if selected == (88,) else context["state"] == "NY"
                if selected is None:
                    assert await reports._fetch_pharmacy_context(reader, npi=2222222222, as_of=query.as_of) is None


async def _assert_insurance_scope_pages(session, request, relation, predicate, parameters, expected):
    """Check office identity, provider count and each page within the active scope."""
    matching_offices = (
        (
            await session.execute(
                text(f"SELECT location_key FROM {relation} offices WHERE {predicate} ORDER BY location_key"),
                parameters,
            )
        )
        .scalars()
        .all()
    )
    assert matching_offices == expected
    count = json.loads((await npi.get_all(request)).body)["rows"]
    assert count == len(expected)
    pages = []
    for offset in range(len(expected) + 1):
        page = (
            (
                await session.execute(
                    text(
                        f"SELECT location_key FROM {relation} offices WHERE {predicate} ORDER BY location_key LIMIT 1 OFFSET :offset"
                    ),
                    {**parameters, "offset": offset},
                )
            )
            .scalars()
            .all()
        )
        pages.extend(page)
    assert pages == expected


async def _seed_pharmacy_report_evidence(fixture, role, monkeypatch, *, issuer_id):
    """Seed only task-owned report sources and an approved ACA binding for native reads."""
    from api.endpoint import reports

    schema = fixture.control_schema
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".report_taxonomy(int_code integer PRIMARY KEY,classification text)'
    )
    await fixture.connection.execute(
        f"INSERT INTO \"{schema}\".report_taxonomy VALUES(17,'Pharmacy'),(18,'Pharmacist')"
    )
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".report_npi(npi bigint PRIMARY KEY,provider_organization_name text,do_business_as_text text)'
    )
    await fixture.connection.execute(f"""INSERT INTO "{schema}".report_npi VALUES
        (1111111111,'Synthetic Pharmacy',''),(2222222222,'Private Pharmacy',''),(3333333333,'Public Pharmacy','')""")
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".report_geo(zip_code text PRIMARY KEY,county_name text,population bigint)'
    )
    await fixture.connection.execute(f"""INSERT INTO "{schema}".report_geo VALUES('10001','Synthetic County',1000)""")
    for table in ("report_taxonomy", "report_npi", "report_geo"):
        await fixture.connection.execute(f'GRANT SELECT ON "{schema}"."{table}" TO "{role}"')
    await fixture.connection.execute(
        f'''UPDATE "{fixture.copy_target.schema_name}".entity_address_unified
        SET state_name=CASE location_key WHEN $1 THEN 'NY' WHEN $2 THEN 'CA' WHEN $3 THEN 'TX' ELSE 'WA' END,
        plans_network_array=ARRAY[0]::integer[],
        type=CASE location_key WHEN $1 THEN 'secondary' WHEN $3 THEN 'site' ELSE 'primary' END''',
        "a" * 64,
        "b" * 64,
        "c" * 64,
    )
    await fixture.connection.execute(
        f'''INSERT INTO "{schema}".registry_approved_record
        (approved_revision,record_kind,record_key,record_revision,custom_revision,record_json)
        VALUES(8,'network_binding','report-aca',1,1,$1::jsonb)''',
        json.dumps(
            {
                "network_id": 42,
                "archived": False,
                "source_system": "aca",
                "source_scope_json": {
                    "plan_id": "12345NY0000001",
                    "plan_year": 2026,
                    "issuer_id": issuer_id,
                    "state": "NY",
                    "checksum_network": -42,
                },
            }
        ),
    )
    monkeypatch.setattr(reports, "_is_table_available", AsyncMock(return_value=False))


class _ScopedReportSession:
    def __init__(self, session, schema):
        self.session = session
        self.schema = schema

    async def execute(self, statement, parameters=None):
        import re

        sql = str(statement)
        for source, target in (
            ("nucc_taxonomy", "report_taxonomy"),
            ("geo_zip_lookup", "report_geo"),
            ("npi", "report_npi"),
        ):
            sql = re.sub(r"\bmrf\." + source + r"\b", f'"{self.schema}"."{target}"', sql)
        return await self.session.execute(text(sql), parameters or {})


@pytest.mark.asyncio
async def test_legacy_checksum_membership_matches_original_without_canonical_coverage(
    serving_indexes_db, serving_schema
):
    """One canonical network has two aliases; original_locations NPI-wide offices remain exact."""
    fixture = serving_indexes_db
    manifest = await _manifest(fixture)
    _, _, engine = serving_schema
    table = f'"{manifest.schema_name}".entity_address_unified'
    await fixture.connection.execute(
        f"UPDATE {table} SET plans_network_array=CASE WHEN location_key=$1 THEN ARRAY[84] ELSE ARRAY[42] END, "
        "canonical_network_ids=CASE WHEN location_key=ANY($2::text[]) THEN ARRAY[42] ELSE ARRAY[]::integer[] END",
        "c" * 64,
        ["a" * 64, "c" * 64],
    )
    expected_by_selector = {None: ["a", "c"], (42,): ["a", "b", "d"], (84,): ["c"], (42, 84): ["a", "b", "c", "d"]}
    async with async_sessionmaker(engine)() as session, session.begin():
        for checksums, expected_keys in expected_by_selector.items():
            expected_locations = [key * 64 for key in expected_keys]
            with network_address_read_scope(manifest, (42,), legacy_checksums=checksums):
                relation = scoped_address_relation_sql("mrf.entity_address_unified")
                parameters = scoped_address_parameters({})
                selected_locations = list(
                    await session.scalars(
                        text(f"SELECT location_key FROM {relation} addresses ORDER BY location_key"), parameters
                    )
                )
                assert selected_locations == expected_locations
                statement = scoped_address_statement(
                    select(EntityAddressUnified.__table__.c.location_key).order_by(
                        EntityAddressUnified.__table__.c.location_key
                    )
                )
                assert list(await session.scalars(statement, parameters)) == expected_locations
                assert await session.scalar(text(f"SELECT count(*) FROM {relation} addresses"), parameters) == len(
                    expected_locations
                )
                hydrated = await npi._fetch_npi_location_candidates_map(
                    [1111111111, 2222222222, 3333333333], session=session
                )
                assert (
                    sorted(address["location_key"] for offices in hydrated.values() for address in offices)
                    == expected_locations
                )
                if checksums is not None:
                    original_locations = list(
                        await session.scalars(
                            text(
                                f"SELECT location_key FROM {table} WHERE plans_network_array && CAST(:checksums AS INTEGER[]) ORDER BY location_key"
                            ),
                            {"checksums": list(checksums)},
                        )
                    )
                    assert selected_locations == original_locations


def _legacy_header_mapping_records(ambiguous):
    """One approved canonical network has two aliases; optionally add a conflicting target."""
    approved_records = [
        ("network", str(network_id), {"network_id": network_id, "archived": False}) for network_id in (42, 84)
    ]
    approved_records += [
        (
            "network_binding",
            str(checksum),
            {
                "network_id": 42,
                "source_system": "aca",
                "archived": False,
                "source_scope_json": {"checksum_network": checksum},
            },
        )
        for checksum in (42, 84)
    ]
    if ambiguous:
        approved_records.append(
            (
                "network_binding",
                "ambiguous",
                {
                    "network_id": 84,
                    "source_system": "aca",
                    "archived": False,
                    "source_scope_json": {"checksum_network": 42},
                },
            )
        )
    return approved_records


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "checksums,ids,denied",
    [
        ("42", "42", False),
        ("84", "42", False),
        ("42", "84", True),
        ("99", "42", True),
        ("42,84", "42", False),
        ("42", "42", True),
    ],
)
async def test_native_legacy_header_mapping_must_equal_authorized_ids(
    serving_indexes_db, serving_schema, monkeypatch, checksums, ids, denied
):
    """Resolve original aliases using actual pinned-revision SQL before reading any office."""
    from sanic.compat import Header
    from sanic.exceptions import Forbidden

    from api.network_address_scope import _pin_network_scope
    from process import network_serving_read

    fixture = serving_indexes_db
    manifest = replace(await _manifest(fixture), approved_custom_revision=4)
    _, schema, engine = serving_schema
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-read-token")
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    approved_records = _legacy_header_mapping_records(denied and checksums == ids == "42")
    for kind, key, document in approved_records:
        await fixture.connection.execute(
            f'INSERT INTO "{schema}".registry_approved_record(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) VALUES(4,$1,$2,1,1,$3::jsonb)',
            kind,
            key,
            json.dumps(document),
        )
    async with async_sessionmaker(engine)() as session, session.begin():
        request = SimpleNamespace(
            headers=Header(
                {
                    "Authorization": "Bearer synthetic-network-read-token",
                    "X-Network-Access-Scope": "b" * 64,
                    "X-Network-Legacy-Checksums": checksums,
                }
            ),
            ctx=SimpleNamespace(sa_session=session),
        )
        arguments_by_name = {"network_ids": ids, "network_generation": "1"}
        if denied:
            with pytest.raises(Forbidden):
                await _pin_network_scope(request, arguments_by_name)
        else:
            actual_manifest, network_ids, _, actual_checksums = await _pin_network_scope(request, arguments_by_name)
            assert actual_manifest is manifest and network_ids == (42,)
            assert actual_checksums == tuple(map(int, checksums.split(",")))
