# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable native ZIP aggregate parity, policy, adoption and rollback proof."""

import asyncio
import copy
from collections import Counter
from itertools import count
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import plan_pricing_code_build as build
from api import plan_pricing_code_zip_build as scoped
from api import ptg2_geo_projection as assurance
from api import ptg2_serving as serving
from api.plan_pricing_code_contract import ZIP_BUILD_REVISION, ZIP_CONTRACT, identity_digest
from api.plan_pricing_code_read import CodePricingCapability
from api.plan_pricing_code_storage import attachment_name, locate, receipt, retire_unreferenced
from api.plan_pricing_projection_aggregate_read import read_aggregate_pack_page
from api.plan_pricing_projection_contract import code_projection_scope
from api.plan_pricing_projection_read import geo_cells, search_plan_pricing_projection
from api.plan_pricing_projection_v3_work import _normalized_taxonomy_codes
from tests import test_plan_pricing_code_postgres as original
from tests.test_plan_pricing_code_zip import _scope


def _limits(monkeypatch):
    for name in (
        "MAX_ZIP_RAW_ADDRESS_WORK_ROWS",
        "MAX_ZIP_ADDRESS_WORK_ROWS",
        "MAX_ZIP_FIRST_FACTOR_ROWS",
        "MAX_ZIP_PRICED_FACTOR_ROWS",
        "MAX_ZIP_FACTOR_JOIN_WORK_ROWS",
        "MAX_ZIP_MEMBER_CELL_WORK_ROWS",
    ):
        monkeypatch.setattr(scoped, name, 1000)
    monkeypatch.setattr(
        original,
        "_SOURCE_TABLES",
        original._SOURCE_TABLES
        + (
            "entity_address_unified",
            "npi_taxonomy",
            "npi",
            "geo_zip_lookup",
            "npi_address",
            "mrf_address",
            "doctor_clinician_address",
        ),
    )


async def _providers(db, monkeypatch):
    taxonomy = _normalized_taxonomy_codes(("CPT", "27447"))[1][0]
    monkeypatch.setattr(assurance, "projection_state_available_sql", lambda _schema: "TRUE")
    async with db.sessions() as session, session.begin():
        for name, columns in (
            ("entity_address_unified", "npi bigint,type text,zip5 varchar(5),postal_code text,assured boolean"),
            ("npi", "npi bigint PRIMARY KEY,entity_type_code smallint"),
            ("npi_taxonomy", "npi bigint,healthcare_provider_taxonomy_code text"),
            ("geo_zip_lookup", "zip_code varchar(5) PRIMARY KEY,latitude double precision,longitude double precision"),
            ("npi_address", "npi bigint,address_key uuid,date_added date"),
            (
                "mrf_address",
                "npi bigint,address_key uuid,date_added date,source_issuer_names varchar[],source_import_ids varchar[],source_import_dates date[]",
            ),
            ("doctor_clinician_address", "npi bigint,address_key uuid,updated_at timestamp"),
        ):
            await session.execute(text(f"CREATE TABLE mrf.{name} ({columns})"))
        await session.execute(text("INSERT INTO mrf.npi SELECT npi,1 FROM generate_series(1000000001,1000000006) npi"))
        await session.execute(
            text("""INSERT INTO mrf.npi_taxonomy
            SELECT npi,CASE WHEN npi=1000000004 THEN '207Q00000X' ELSE :taxonomy END
              FROM mrf.npi"""),
            {"taxonomy": taxonomy},
        )
        await session.execute(
            text("""INSERT INTO mrf.entity_address_unified VALUES
            (1000000001,'practice','48104',NULL,true),
            (1000000001,'primary',NULL,'48104-0001',true),
            (1000000001,'secondary','48105',NULL,true),
            (1000000001,'practice','99999',NULL,true),
            (1000000002,'primary','48104',NULL,true),
            (1000000002,'primary','48105',NULL,false),
            (1000000003,'secondary','48105',NULL,true),
            (1000000003,'billing','48104',NULL,true),
            (1000000004,'site','48105',NULL,true),
            (1000000005,'site','48105',NULL,true),
            (1000000006,'practice','48104',NULL,false)""")
        )
        await session.execute(
            text("INSERT INTO mrf.geo_zip_lookup VALUES ('48104',40,-83),('48105',40.01,-83),('99999',42,-83)")
        )
        await session.execute(
            text("""ALTER TABLE mrf.entity_address_unified
            ADD location_key text PRIMARY KEY DEFAULT gen_random_uuid()::text,
            ADD address_key uuid,ADD premise_key uuid,ADD address_source_mask bigint DEFAULT 0,
            ADD geo_assurance_version smallint DEFAULT 1,ADD geo_evidence_source_id smallint""")
        )
        await session.execute(
            text("UPDATE mrf.entity_address_unified SET geo_evidence_source_id=CASE WHEN assured THEN 1 ELSE 0 END")
        )
        await session.execute(
            text(
                f'GRANT SELECT ON mrf.entity_address_unified,mrf.npi,mrf.npi_taxonomy,mrf.geo_zip_lookup TO "{db.reader}"'
            )
        )


def _scoped_inputs(monkeypatch, identity, *, drift=False):
    prior = build.resolve_inputs
    scoped_identity = identity | {"contract": ZIP_CONTRACT, "build_revision": ZIP_BUILD_REVISION, "scope": _scope()}
    attempts = count()

    async def inputs(session, selection, code, *, scope):
        attempt = next(attempts)
        _old, bindings = await prior(session, selection, code)
        cells = await geo_cells(session, scope, result_type="rate_aggregates")
        result = copy.deepcopy(scoped_identity)
        result["scope"]["geo_cells"] = sorted(set(cells))
        if drift and attempt > 0:
            result["provider_signature"] = "c" * 64
        return result, bindings

    monkeypatch.setattr(build, "resolve_inputs", inputs)
    return scoped_identity


async def _prepare(db, identity):
    from dataclasses import replace

    from api.plan_release_serving import PlanReleaseSnapshotBinding

    scope_by_field = identity.get("scope")
    if scope_by_field is not None:
        scope_by_field = {name: value for name, value in scope_by_field.items() if name != "geo_cells"}
    async with db.sessions() as session, session.begin():
        return await build.build_in_session(
            session,
            selection=replace(
                original._aliased_selection(),
                bindings=tuple(PlanReleaseSnapshotBinding(**item) for item in identity["bindings"]),
            ),
            code_system="CPT",
            code="27447",
            expected_identity_digest=identity_digest(identity),
            scope=scope_by_field,
        )


async def _page(session, capability, *, offset, order, aliases=None):
    request = SimpleNamespace(projection_id=capability.projection_id, code_system="CPT", code="27447")
    selected = capability if aliases is None else capability.contributors(aliases)
    with code_projection_scope(selected):
        rows, total = await read_aggregate_pack_page(
            session,
            request,
            _scope()["geo_cells"],
            {"order": order},
            SimpleNamespace(limit=1, offset=offset),
            **({} if aliases is None else {"binding_ordinals": tuple(item.binding_ordinal for item in aliases)}),
        )
    return orjson.loads(orjson.dumps(rows)), total


async def _assert_all_pages(session, scoped_capability, national):
    for order in ("asc", "desc"):
        complete_rows = []
        for offset in range(3):
            page, total = await _page(session, scoped_capability, offset=offset, order=order)
            assert (page, total) == await _page(session, national, offset=offset, order=order)
            assert total == 2
            complete_rows.extend(page)
            for alias in original._aliased_selection().bindings:
                assert (page, total) == await _page(
                    session, scoped_capability, offset=offset, order=order, aliases=(alias,)
                )
        assert {aggregate["geo_cell"] for aggregate in complete_rows} == {"48104", "48105"}
        assert [aggregate["rate_count"] for aggregate in complete_rows] == [6, 7]


async def _assert_reader_pages(db, identity, unscoped_identity):
    reader = create_async_engine(db.reader_url)
    try:
        async with async_sessionmaker(reader)() as session, session.begin():
            scoped_capability = CodePricingCapability(identity, await locate(session, identity))
            national = CodePricingCapability(unscoped_identity, await locate(session, unscoped_identity))
            assert receipt(scoped_capability.found)["projection_id"] == identity_digest(identity)
            await _assert_all_pages(session, scoped_capability, national)
            from api.endpoint.pagination import PaginationParams

            request_by_field = {
                "view": "card",
                "include_providers": "false",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "48104",
                "zip_radius_miles": "25",
            }
            with code_projection_scope(scoped_capability):
                response = await search_plan_pricing_projection(
                    session,
                    scoped_capability.selection(original._aliased_selection()),
                    request_by_field,
                    PaginationParams(limit=1, offset=1, page=2, source="test"),
                )
            assert response["result_type"] == "rate_aggregates"
            assert response["pagination"]["total"] == 2 and response["pagination"]["total_is_exact"]
            assert not response["pagination"]["has_more"]
            assert response["query"]["projection_contract"] == ZIP_CONTRACT
    finally:
        await reader.dispose()


async def _assert_retention(db, identity, ready, unscoped_identity, unscoped_ready):
    async with db.sessions() as pinned, pinned.begin():
        await locate(pinned, identity)
        from sqlalchemy.exc import DBAPIError

        async with db.sessions() as session:
            with pytest.raises(DBAPIError):
                async with session.begin():
                    await retire_unreferenced(session, ready)
    async with db.sessions() as session, session.begin():
        assert await retire_unreferenced(session, ready)
        assert await locate(session, identity) is None
        assert receipt(await locate(session, unscoped_identity)) == unscoped_ready


@pytest.mark.asyncio
async def test_zip_native_exact_pages_alias_policy_adoption_and_retention(monkeypatch):
    _limits(monkeypatch)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        unscoped_identity = original._fixture(monkeypatch, db, oid)
        unscoped_ready = await _prepare(db, unscoped_identity)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, unscoped_identity)
        for name, limit in (
            ("MAX_ZIP_ADDRESS_WORK_ROWS", 7),
            ("MAX_ZIP_FIRST_FACTOR_ROWS", 5),
            ("MAX_ZIP_PRICED_FACTOR_ROWS", 5),
            ("MAX_ZIP_FACTOR_JOIN_WORK_ROWS", 7),
            ("MAX_ZIP_MEMBER_CELL_WORK_ROWS", 8),
        ):
            monkeypatch.setattr(scoped, name, limit)
        ready = await _prepare(db, identity)
        assert await _prepare(db, identity) == ready
        assert len(db.stages) == 2
        assert ready["contract"] == ZIP_CONTRACT
        counts = ready["counts"]
        assert counts["zip_work"] == {
            "raw_address": 9,
            "classified_address": 7,
            "address": 6,
            "first": 5,
            "priced": 5,
            "factor": 7,
        }
        assert counts["factor_join_work_rows"] == 7 and counts["provider_membership_count"] == 6
        assert counts["provider_cell_count"] == 5
        assert counts["provider_state_count"] == counts["provider_fragment_byte_count"] == 0
        assert counts["rate_occurrence_count"] == unscoped_ready["counts"]["rate_occurrence_count"] == 4
        assert counts["work"]["member_cell_rows"] == 8 and counts["aggregate_entry_count"] == 2
        await _assert_reader_pages(db, identity, unscoped_identity)
        await _assert_retention(db, identity, ready, unscoped_identity, unscoped_ready)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["address", "first", "priced", "factor", "member_cell", "drift"])
async def test_zip_native_each_work_refusal_and_generation_drift_roll_back(monkeypatch, failure):
    _limits(monkeypatch)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        identity = original._fixture(monkeypatch, db, oid)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, identity, drift=failure == "drift")
        limits_by_name = {
            "address": "MAX_ZIP_ADDRESS_WORK_ROWS",
            "first": "MAX_ZIP_FIRST_FACTOR_ROWS",
            "priced": "MAX_ZIP_PRICED_FACTOR_ROWS",
            "factor": "MAX_ZIP_FACTOR_JOIN_WORK_ROWS",
            "member_cell": "MAX_ZIP_MEMBER_CELL_WORK_ROWS",
        }
        if failure in limits_by_name:
            exact_work_by_name = {"address": 7, "first": 5, "priced": 5, "factor": 7, "member_cell": 8}
            monkeypatch.setattr(scoped, limits_by_name[failure], exact_work_by_name[failure] - 1)
        with pytest.raises((ValueError, RuntimeError), match="bound exceeded|changed during"):
            await _prepare(db, identity)
        async with db.sessions() as session, session.begin():
            assert not await session.scalar(
                text("SELECT to_regclass(:name) IS NOT NULL"),
                {"name": "hp_snapshot_retention." + attachment_name(identity)},
            )
            assert await session.scalar(text("SELECT count(*) FROM mrf.ptg2_snapshot_pin")) == 0
            for stage in db.stages:
                assert not await session.scalar(
                    text("SELECT to_regnamespace(:name) IS NOT NULL"), {"name": stage.schema_name}
                )


@pytest.mark.asyncio
async def test_zip_native_replacement_retains_old_reader_and_exact_retirement(monkeypatch):
    _limits(monkeypatch)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        identity = original._fixture(monkeypatch, db, oid)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, identity)
        first = await _prepare(db, identity)
        async with db.sessions() as pinned, pinned.begin():
            old = CodePricingCapability(copy.deepcopy(identity), await locate(pinned, identity))
            async with db.sessions() as session, session.begin():
                await session.execute(text("INSERT INTO mrf.geo_zip_lookup VALUES ('48106',40.02,-83)"))
            identity["provider_signature"] = "c" * 64
            identity["scope"]["geo_cells"].append("48106")
            replacement = await _prepare(db, identity)
            assert replacement["projection_id"] != first["projection_id"]
            assert replacement["active_attachment"]["name"] == first["active_attachment"]["name"]
            assert (await _page(pinned, old, offset=0, order="asc"))[1] == 2
        async with db.sessions() as session, session.begin():
            assert await retire_unreferenced(session, first)
            assert receipt(await locate(session, identity)) == replacement


@pytest.mark.asyncio
async def test_zip_native_empty_centroid_scope_has_exact_zero_results(monkeypatch):
    _limits(monkeypatch)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        identity = original._fixture(monkeypatch, db, oid)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, identity)
        identity["scope"].update(zip5="48109", geo_cells=[])
        ready = await _prepare(db, identity)
        assert ready["counts"]["aggregate_entry_count"] == 0
        assert ready["counts"]["provider_membership_count"] == ready["counts"]["provider_cell_count"] == 0
        assert ready["counts"]["rate_occurrence_count"] == 4
        assert ready["counts"]["zip_work"] == {
            "raw_address": 0,
            "classified_address": 0,
            "address": 0,
            "first": 0,
            "priced": 0,
            "factor": 0,
        }


@pytest.mark.asyncio
async def test_zip_native_distinct_contributors_and_cumulative_work(monkeypatch):
    from dataclasses import asdict, replace

    _limits(monkeypatch)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        identity = original._fixture(monkeypatch, db, oid)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, identity)
        third = replace(original._aliased_selection().bindings[0], binding_ordinal=2, plan_id="other-synthetic-plan")
        identity["bindings"].append(asdict(third))
        contributor = copy.deepcopy(identity["sources"][0])
        contributor.update(binding_ordinal=2, materialized_binding_ordinal=2, read_identity_digest="d" * 64)
        identity["sources"].append(contributor)
        prior = build.resolve_inputs

        async def inputs(*args, **kwargs):
            result, bindings = await prior(*args, **kwargs)
            binding, found = bindings[0]
            return result, [*bindings, (replace(binding, binding={"ordinal": 2}), found)]

        monkeypatch.setattr(build, "resolve_inputs", inputs)
        # Each physical binding fits; their complete sum must also fit.
        monkeypatch.setattr(scoped, "MAX_ZIP_FACTOR_JOIN_WORK_ROWS", 13)
        with pytest.raises(ValueError, match="factor join work bound"):
            await _prepare(db, identity)
        monkeypatch.setattr(scoped, "MAX_ZIP_FACTOR_JOIN_WORK_ROWS", 14)
        ready = await _prepare(db, identity)
        assert ready["counts"]["zip_work"] == {
            "raw_address": 9,
            "classified_address": 7,
            "address": 6,
            "first": 10,
            "priced": 10,
            "factor": 14,
        }
        async with db.sessions() as session, session.begin():
            capability = CodePricingCapability(identity, await locate(session, identity))
            all_rows, total = await _page(session, capability, offset=0, order="asc")
            subset, subset_total = await _page(session, capability, offset=0, order="asc", aliases=(third,))
            assert total == subset_total == 2
            assert all_rows[0]["provider_count"] == subset[0]["provider_count"] == 2
            assert all_rows[0]["rate_count"] == 12 and subset[0]["rate_count"] == 6
            assert all_rows[0]["median_negotiated_rate"] == subset[0]["median_negotiated_rate"] == 15


async def _assured_membership_rows(session):
    """Use the unbatched scalar predicate as the independent multiset oracle."""
    zip5 = serving._ptg2_address_zip5_sql("addr", unified=True)
    expected = (
        await session.execute(
            text(f"""SELECT addr.npi,{zip5}
        FROM mrf.entity_address_unified addr
        WHERE addr.type IN ('practice','primary','secondary','site')
          AND {zip5}=ANY(CAST(:cells AS varchar[]))
          AND ({serving._ptg2_geo_evidence_level_sql("addr")}) IS NOT NULL
          AND EXISTS(SELECT 1 FROM mrf.code_source_dictionary dictionary
            JOIN mrf.ptg_membership_first first ON first.owner_key=dictionary.npi_key
            JOIN mrf.ptg_membership_second second ON second.owner_key=first.member_key
            JOIN mrf.ptg_membership_code code ON code.provider_set_key=second.member_key
            WHERE dictionary.snapshot_key=1 AND dictionary.npi=addr.npi AND code.code_key=7)
        """),
            {"cells": _scope()["geo_cells"]},
        )
    ).all()
    return expected


async def _dense_raw_address_oracle(session):
    """Seed duplicate dense groups and nonmatches before the independent multiset read."""
    await session.execute(
        text("""INSERT INTO mrf.entity_address_unified
        (npi,type,zip5,postal_code,assured,geo_evidence_source_id)
        VALUES(1000000001,'practice','48104',NULL,true,1)""")
    )
    await session.execute(
        text("""INSERT INTO mrf.entity_address_unified
        (npi,type,zip5,assured,geo_evidence_source_id)
        SELECT 1000000001,'practice','48104',true,1 FROM generate_series(1,101)
        UNION ALL
        SELECT 1000000005,'site','48104',true,1 FROM generate_series(1,3)""")
    )
    await session.execute(
        text("""UPDATE mrf.entity_address_unified
        SET address_key=md5('stored-zero')::uuid,address_source_mask=1
        WHERE npi=1000000002 AND zip5='48105'""")
    )
    await session.execute(
        text("""INSERT INTO mrf.npi_address
        VALUES(1000000002,md5('stored-zero')::uuid,'2026-01-01')""")
    )
    return await _assured_membership_rows(session)


def _dense_group_work_bounds(monkeypatch, batch_size):
    """Admit the exact matched-address and membership fixture work."""
    monkeypatch.setattr(scoped, "MAX_ZIP_ADDRESS_WORK_ROWS", 109)
    monkeypatch.setattr(scoped, "MAX_ZIP_FIRST_FACTOR_ROWS", 5)
    monkeypatch.setattr(scoped, "MAX_ZIP_PRICED_FACTOR_ROWS", 7 if batch_size == 1 else 5)
    monkeypatch.setattr(scoped, "MAX_ZIP_FACTOR_JOIN_WORK_ROWS", 7)
    monkeypatch.setattr(scoped, "MAX_ZIP_MEMBER_CELL_WORK_ROWS", 8)


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 2])
async def test_zip_native_batches_preserve_raw_multisets_and_exact_pages(monkeypatch, batch_size):
    """Grouped pages preserve the scalar multiset and globally deduplicated code work."""
    _limits(monkeypatch)
    monkeypatch.setattr(build, "CODE_PROVIDER_READ_BATCH_SIZE", batch_size)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        unscoped_identity = original._fixture(monkeypatch, db, oid)
        await _prepare(db, unscoped_identity)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, unscoped_identity)
        async with db.sessions() as session, session.begin():
            expected = await _dense_raw_address_oracle(session)
        observed_rows = []
        stage_cells = scoped._stage_provider_cells

        async def record_cells(session, *args):
            counts = await stage_cells(session, *args)
            observed_rows.extend(
                (await session.execute(text("SELECT npi,geo_cell FROM plan_pricing_zip_assured"))).all()
            )
            return counts

        monkeypatch.setattr(scoped, "_stage_provider_cells", record_cells)
        memberships = scoped.stage_memberships
        visited_npis = []

        async def record_memberships(session, *args):
            visited_npis.extend(
                (await session.execute(text("SELECT npi FROM plan_pricing_provider_npi_pending_stage"))).scalars()
            )
            return await memberships(session, *args)

        monkeypatch.setattr(scoped, "stage_memberships", record_memberships)
        monkeypatch.setattr(scoped, "MAX_ZIP_RAW_ADDRESS_WORK_ROWS", 113)
        copies = AsyncMock(wraps=build._copy)
        monkeypatch.setattr(build, "_copy", copies)
        with pytest.raises(ValueError, match="raw address work bound"):
            await _prepare(db, identity)
        copies.assert_not_awaited()
        observed_rows.clear()
        visited_npis.clear()
        monkeypatch.setattr(scoped, "MAX_ZIP_RAW_ADDRESS_WORK_ROWS", 114)
        _dense_group_work_bounds(monkeypatch, batch_size)
        ready = await _prepare(db, identity)
        assert Counter(visited_npis) == Counter(range(1000000001, 1000000007))
        assert Counter(map(tuple, observed_rows)) == Counter(map(tuple, expected))
        assert ready["counts"]["zip_work"] == {
            "raw_address": 114,
            "classified_address": 109,
            "address": 108,
            "first": 5,
            "priced": 7 if batch_size == 1 else 5,
            "factor": 7,
        }
        assert ready["counts"]["provider_cell_count"] == 5
        assert ready["counts"]["work"]["member_cell_rows"] == 8
        await _assert_reader_pages(db, identity, unscoped_identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["raw_address", "factor", "member_cell", "cancelled"])
async def test_zip_native_late_batch_refusal_precedes_output_and_rolls_back(monkeypatch, failure):
    _limits(monkeypatch)
    monkeypatch.setattr(build, "CODE_PROVIDER_READ_BATCH_SIZE", 1)
    async with original._database(monkeypatch) as db:
        oid = await original._source_database(db)
        identity = original._fixture(monkeypatch, db, oid)
        await _providers(db, monkeypatch)
        identity = _scoped_inputs(monkeypatch, identity)
        copies = AsyncMock(wraps=build._copy)
        monkeypatch.setattr(build, "_copy", copies)
        if failure == "cancelled":
            stage_batch = scoped._stage_address_batch
            attempts = count()

            async def cancel_later(*args):
                if next(attempts) == 3:
                    raise asyncio.CancelledError
                return await stage_batch(*args)

            monkeypatch.setattr(scoped, "_stage_address_batch", cancel_later)
        else:
            constant, limit = {
                "raw_address": ("MAX_ZIP_RAW_ADDRESS_WORK_ROWS", 8),
                "factor": ("MAX_ZIP_FACTOR_JOIN_WORK_ROWS", 6),
                "member_cell": ("MAX_ZIP_MEMBER_CELL_WORK_ROWS", 7),
            }[failure]
            monkeypatch.setattr(scoped, constant, limit)
        with pytest.raises(asyncio.CancelledError if failure == "cancelled" else ValueError):
            await _prepare(db, identity)
        copies.assert_not_awaited()
        async with db.sessions() as session, session.begin():
            assert not await session.scalar(
                text("SELECT to_regclass(:name) IS NOT NULL"),
                {"name": "hp_snapshot_retention." + attachment_name(identity)},
            )
            assert await session.scalar(text("SELECT count(*) FROM mrf.ptg2_snapshot_pin")) == 0
            for stage in db.stages:
                assert not await session.scalar(
                    text("SELECT to_regnamespace(:name) IS NOT NULL"), {"name": stage.schema_name}
                )
