# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native isolated publication, Reader parity and failure rollback for complete codes."""

import asyncio
import copy
import os
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import orjson
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import plan_pricing_code_build as build
from api import plan_pricing_projection_v3_price as prices
from api import ptg2_serving as serving
from api.plan_pricing_code_contract import SPEC, capability_identity, identity_digest
from api.plan_pricing_code_read import CodePricingCapability
from api.plan_pricing_code_storage import attachment_name, locate, receipt, retire_unreferenced
from api.plan_pricing_projection_aggregate_read import read_aggregate_pack_page
from api.plan_pricing_projection_contract import code_projection_scope
from api.plan_pricing_projection_source import BindingProjection
from api.plan_pricing_projection_v3_work import _normalized_taxonomy_codes
from api.plan_pricing_state_scan import _read_projected_occurrences, _read_state_npis, _validated_occurrences
from process import reference_family_archive as native
from tests import test_ptg_membership_projection_postgres as database
from tests.test_plan_pricing_code_projection import _selection, _source
from tests.test_plan_pricing_projection_v4_occurrence import _occurrence
from tests.test_plan_pricing_provider_state_witness import _state_provider

_SOURCE_TABLES = (
    "ptg_membership_code",
    "ptg_membership_first",
    "ptg_membership_second",
    "code_source_dictionary",
    "code_source_provider_set",
)


async def _assert_reader_scope_parity(session, scope):
    """Check pagination, aggregate multiplicity and both logical binding scopes."""
    with code_projection_scope(scope):
        npis, fragments, more = await _read_state_npis(session, scope.projection_id, "MI", 0, 2)
        assert npis == (1000000001, 1000000002) and more
        occurrence_rows = _validated_occurrences(
            await _read_projected_occurrences(session, scope.projection_id, "CPT", "27447", npis, 2)
        )
        assert len(occurrence_rows) == 4
        assert [occurrence_row["occurrence_multiplicity"] for occurrence_row in occurrence_rows] == [2, 1, 2, 1]
        last, _, more = await _read_state_npis(session, scope.projection_id, "MI", npis[-1], 2)
        assert last == (1000000003, 1000000004) and not more
        request = SimpleNamespace(projection_id=scope.projection_id, code_system="CPT", code="27447")
        page, total = await read_aggregate_pack_page(
            session, request, ["48104", "48105"], {}, SimpleNamespace(limit=10, offset=0)
        )
        aggregate_rows = orjson.loads(orjson.dumps(page))
        assert total == 2
        assert aggregate_rows[0]["provider_count"] == 2 and aggregate_rows[0]["rate_count"] == 6
        assert aggregate_rows[0]["median_negotiated_rate"] == 15
        assert aggregate_rows[1]["provider_count"] == 2 and aggregate_rows[1]["rate_count"] == 7
        partial, partial_total = await read_aggregate_pack_page(
            session,
            request,
            ["48104", "48105"],
            {},
            SimpleNamespace(limit=10, offset=0),
            binding_ordinals=(0,),
        )
        assert partial_total == total and orjson.dumps(partial) == orjson.dumps(page)
    with code_projection_scope(scope.contributors((_aliased_selection().bindings[1],))):
        alias_npis, _, _ = await _read_state_npis(session, scope.projection_id, "MI", 0, 50, binding_ordinals=(1,))
        assert alias_npis == (1000000001, 1000000002, 1000000003, 1000000004)
        alias_rows = _validated_occurrences(
            await _read_projected_occurrences(
                session, scope.projection_id, "CPT", "27447", (1000000001,), 1, binding_ordinals=(1,)
            )
        )
        assert {occurrence_row["binding_ordinal"] for occurrence_row in alias_rows} == {1}
        alias_page, alias_total = await read_aggregate_pack_page(
            session,
            request,
            ["48104", "48105"],
            {},
            SimpleNamespace(limit=10, offset=0),
            binding_ordinals=(1,),
        )
        assert alias_total == total and orjson.dumps(alias_page) == orjson.dumps(page)


async def _assert_code_retention(db, identity, ready):
    """Require dependency custody, live-reference retention and pinned-read refusal."""
    from sqlalchemy.exc import DBAPIError

    async with db.sessions() as session:
        with pytest.raises(DBAPIError):
            async with session.begin():
                await session.execute(text(f'DROP VIEW hp_snapshot_retention."pm_{"a" * 52}" RESTRICT'))
    async with db.sessions() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO mrf.plan_release_snapshot_binding VALUES('synthetic-release','in_network',0,'synthetic-snapshot')"
            )
        )
    async with db.sessions() as session:
        with pytest.raises(Exception, match="retention references"):
            async with session.begin():
                await retire_unreferenced(session, ready)
    async with db.sessions() as session, session.begin():
        await session.execute(text("DELETE FROM mrf.plan_release_snapshot_binding"))
    async with db.sessions() as pinned, pinned.begin():
        await locate(pinned, identity)
        async with db.sessions() as session:
            with pytest.raises(DBAPIError):
                async with session.begin():
                    await retire_unreferenced(session, ready)
    async with db.sessions() as session, session.begin():
        assert await retire_unreferenced(session, ready)
        assert await locate(session, identity) is None


def _fixture_provider_reader(taxonomy, failure):
    """Keep state witnesses and the provider interruption in one fixture reader."""

    async def providers(_session, npis):
        if failure == "provider":
            raise RuntimeError("synthetic interrupted preparation")
        provider_rows_by_npi = {}
        for npi in npis:
            provider_rows_by_npi[npi] = []
            zips = ("48104", "48105") if npi == 1000000001 else (("48104",) if npi == 1000000002 else ("48105",))
            for rank, zip5 in enumerate(zips, 1):
                occurrence_row = _state_provider(zip5, rank)
                occurrence_row["npi"] = npi
                occurrence_row["taxonomy_codes"] = [taxonomy if npi != 1000000004 else "207Q00000X"]
                address = orjson.loads(occurrence_row["address_payload"])
                address["npi"] = npi
                occurrence_row["address_payload"] = orjson.dumps(address).decode()
                provider_rows_by_npi[npi].append(occurrence_row)
        return provider_rows_by_npi

    return providers


def _fixture_occurrences(binding):
    """Retain duplicate rate occurrences across the complete provider sets."""
    occurrence_rows = []
    for set_key, price_key, multiplicity in ((100, 1, 2), (101, 2, 1), (102, 3, 1), (104, 4, 1)):
        occurrence_row = _occurrence()
        occurrence_row.update(
            _ptg_provider_set_key=set_key,
            provider_count=2 if set_key < 102 else 1,
            price_key=price_key,
            provider_set_global_id_128=f"{set_key:032x}",
            price_set_global_id_128=f"{price_key:032x}",
            reported_code_system="CPT",
            reported_code="27447",
            plan_id=binding.plan_id,
            plan_market_type="group",
        )
        occurrence_rows.extend(copy.deepcopy(occurrence_row) for _ in range(multiplicity))

    return occurrence_rows


def _aliased_selection():
    selection = _selection()
    return replace(selection, bindings=(selection.bindings[0], replace(selection.bindings[0], binding_ordinal=1)))


def _url():
    raw = os.getenv("HLTHPRT_PLAN_PRICING_CODE_TEST_DSN")
    if not raw:
        pytest.skip("HLTHPRT_PLAN_PRICING_CODE_TEST_DSN is not set")
    value = make_url(raw)
    if (
        value.host not in {"127.0.0.1", "postgres"}
        or not 1024 <= (value.port or 0) <= 65535
        or not re.fullmatch("hc_code_pricing_[0-9a-f]{32}", value.database or "")
    ):
        pytest.fail("code pricing native proof requires a task-owned loopback database")
    return value.set(drivername="postgresql+asyncpg")


@asynccontextmanager
async def _database(monkeypatch):
    monkeypatch.setattr(database, "_url", _url)
    monkeypatch.setattr(database, "SPEC", SPEC)
    async with database._database(monkeypatch) as db:
        try:
            yield db
        finally:
            async with db.admin.begin() as session:
                views = (
                    (
                        await session.execute(
                            text(
                                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='hp_snapshot_retention' AND c.relkind='v' AND c.relname LIKE 'pc_%'"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                for name in views:
                    await session.execute(text(f'DROP VIEW hp_snapshot_retention."{name}" RESTRICT'))
                witness = "pm_" + "a" * 52
                await session.execute(text(f'DROP VIEW IF EXISTS hp_snapshot_retention."{witness}" RESTRICT'))
                for name in _SOURCE_TABLES:
                    await session.execute(text(f'DROP TABLE IF EXISTS mrf."{name}" RESTRICT'))


async def _source_database(db):
    async with db.sessions() as session, session.begin():
        for table, columns in (
            ("ptg_membership_first", "owner_key bigint,member_key bigint,PRIMARY KEY(owner_key,member_key)"),
            ("ptg_membership_second", "owner_key bigint,member_key bigint,PRIMARY KEY(owner_key,member_key)"),
            ("ptg_membership_code", "code_key bigint,provider_set_key bigint,PRIMARY KEY(code_key,provider_set_key)"),
            (
                "code_source_dictionary",
                "snapshot_key bigint,npi_key bigint,npi bigint,PRIMARY KEY(snapshot_key,npi_key)",
            ),
            (
                "code_source_provider_set",
                "snapshot_key bigint,provider_set_key bigint,provider_count bigint,provider_set_global_id_128 bytea,PRIMARY KEY(snapshot_key,provider_set_key)",
            ),
        ):
            await session.execute(text(f'CREATE TABLE mrf."{table}" ({columns})'))
        await session.execute(text("INSERT INTO mrf.ptg_membership_first VALUES (0,10),(0,11),(1,11),(2,12),(3,13)"))
        await session.execute(
            text("INSERT INTO mrf.ptg_membership_second VALUES (10,100),(11,100),(11,101),(12,102),(13,104)")
        )
        await session.execute(text("INSERT INTO mrf.ptg_membership_code VALUES (7,100),(7,101),(7,102),(7,104)"))
        await session.execute(
            text(
                "INSERT INTO mrf.code_source_dictionary VALUES (1,0,1000000001),(1,1,1000000002),(1,2,1000000003),(1,3,1000000004)"
            )
        )
        await session.execute(
            text(
                "INSERT INTO mrf.code_source_provider_set VALUES (1,100,2,decode(repeat('01',16),'hex')),(1,101,2,decode(repeat('02',16),'hex')),(1,102,1,decode(repeat('03',16),'hex')),(1,104,1,decode(repeat('04',16),'hex'))"
            )
        )
        owner = await native.protected_publisher_owner(session)
        role = await session.scalar(text("SELECT quote_ident(rolname) FROM pg_roles WHERE oid=:oid"), {"oid": owner})
        name = "pm_" + "a" * 52
        await session.execute(text(f'CREATE VIEW hp_snapshot_retention."{name}" AS SELECT 1 AS witness'))
        await session.execute(text(f'ALTER VIEW hp_snapshot_retention."{name}" OWNER TO {role}'))
        return int(
            await session.scalar(text("SELECT to_regclass(:name)::oid"), {"name": "hp_snapshot_retention." + name})
        )


def _fixture(monkeypatch, db, oid, *, failure=None):
    """Install complete alias inputs while keeping publication and reads native."""
    monkeypatch.setattr(build, "MAX_CODE_MEMBER_CELL_WORK_ROWS", 1000)
    monkeypatch.setattr(build, "MAX_CODE_FACTOR_JOIN_WORK_ROWS", 1000)
    code_identity = ("CPT", "27447")
    source_by_key = _source()
    source_by_key["membership"]["active_attachment"]["relation_oid"] = oid
    source_by_key["code_rows"] = [{"code_key": 7, "occurrence_count": 5}]
    alias = copy.deepcopy(source_by_key)
    alias["binding_ordinal"] = 1
    identity = capability_identity(_aliased_selection(), code_identity, "b" * 64, [source_by_key, alias])
    binding = _selection().in_network_bindings[0]
    tables = SimpleNamespace(physical_binding=None, price_key_block_span=100, network_names=[])
    projection = BindingProjection({"ordinal": 0}, tables, {code_identity: [{"code_key": 7, "rate_count": 5}]}, 1)
    found = (SimpleNamespace(schema_name="mrf"),)

    async def inputs(*_args):
        return copy.deepcopy(identity), [(projection, found)]

    monkeypatch.setattr(build, "resolve_inputs", inputs)
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
    monkeypatch.setattr(serving, "_shared_v3_provider_set_table", lambda _tables: '"mrf"."code_source_provider_set"')
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: '"mrf"."code_source_dictionary"')

    async def principals(*_args):
        return ['"' + db.reader + '"']

    monkeypatch.setattr(build, "_reader_principals", principals)
    monkeypatch.setattr(build, "enqueue_live_progress", lambda **_kwargs: None)

    async def alias_metadata(*_args, **_kwargs):
        return None

    monkeypatch.setattr(build, "_preflight_price_membership_aliases_from_db", alias_metadata)
    occurrence_rows = _fixture_occurrences(binding)

    async def code_rows(*_args):
        return occurrence_rows, {f"{key:032x}": key for key in (1, 2, 3, 4)}

    monkeypatch.setattr(build, "_binding_code_rows", code_rows)

    async def price_rows(_session, _binding, batch):
        return {
            name: {1: (Decimal(10), Decimal(20)), 2: (Decimal(10), Decimal(30)), 3: (Decimal(50),), 4: (Decimal(100),)}[
                key
            ]
            for name, key in batch
        }

    monkeypatch.setattr(prices, "_price_batch_rates", price_rows)
    original = build._materialize_provider_cells
    taxonomy = _normalized_taxonomy_codes(code_identity)[1][0]

    providers = _fixture_provider_reader(taxonomy, failure)

    async def materialize(session, candidate, state, **_kwargs):
        await original(session, candidate, state, provider_rows_for_npis=providers)

    monkeypatch.setattr(build, "_materialize_provider_cells", materialize)
    return identity


@pytest.mark.asyncio
async def test_code_native_publisher_reader_parity_dependency_retention_and_adoption(monkeypatch):
    """Publish once, adopt identically and preserve reader parity through retirement."""
    for case in (
        ([7, 8], 7, 0, (6, 7), None),
        ([404], 0, 0, (0, 0), None),
        ([10], 0, 0, (0, 0), None),
        ([7, 8], 6, 0, None, "factor join exceeds"),
        ([10], 0, 1, None, "membership is incomplete"),
    ):
        with pytest.MonkeyPatch.context() as factor_patch:
            await _assert_selected_factor_preflight(factor_patch, *case)
    async with _database(monkeypatch) as db:
        oid = await _source_database(db)
        identity = _fixture(monkeypatch, db, oid)
        async with db.sessions() as session, session.begin():
            ready = await build.build_in_session(
                session,
                selection=_aliased_selection(),
                code_system="CPT",
                code="27447",
                expected_identity_digest=identity_digest(identity),
            )
        assert ready["counts"]["provider_membership_count"] == 6
        assert ready["counts"]["provider_state_count"] == 4
        assert ready["counts"]["rate_occurrence_count"] == 4
        async with db.sessions() as session, session.begin():
            assert (
                await build.build_in_session(
                    session,
                    selection=_aliased_selection(),
                    code_system="CPT",
                    code="27447",
                    expected_identity_digest=identity_digest(identity),
                )
                == ready
            )
        assert len(db.stages) == 1
        reader = create_async_engine(db.reader_url)
        try:
            async with async_sessionmaker(reader)() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
                found = await locate(session, identity)
                assert receipt(found) == ready
                scope = CodePricingCapability(identity, found)
                await _assert_reader_scope_parity(session, scope)
        finally:
            await reader.dispose()
        await _assert_code_retention(db, identity, ready)


@pytest.mark.asyncio
async def test_interrupted_code_build_rolls_back_family_attachment_and_pins(monkeypatch):
    async with _database(monkeypatch) as db:
        oid = await _source_database(db)
        identity = _fixture(monkeypatch, db, oid, failure="provider")
        async with db.sessions() as session:
            with pytest.raises(RuntimeError, match="interrupted"):
                async with session.begin():
                    await build.build_in_session(
                        session,
                        selection=_aliased_selection(),
                        code_system="CPT",
                        code="27447",
                        expected_identity_digest=identity_digest(identity),
                    )
        async with db.sessions() as session, session.begin():
            assert not await session.scalar(
                text("SELECT to_regclass(:name) IS NOT NULL"),
                {"name": "hp_snapshot_retention." + attachment_name(identity)},
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM mrf.ptg2_snapshot_pin WHERE owner_type='plan_pricing_code_v1'")
                )
                == 0
            )
            for stage in db.stages:
                assert not await session.scalar(
                    text("SELECT to_regnamespace(:name) IS NOT NULL"), {"name": stage.schema_name}
                )


async def _assert_selected_factor_preflight(monkeypatch, code_keys, factor_limit, missing_count, expected, error):
    from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

    async with _database(monkeypatch) as db:
        await _source_database(db)
        monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 1)
        monkeypatch.setattr(serving, "_shared_v3_provider_set_table", lambda _tables: "mrf.code_source_provider_set")
        monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "mrf.code_source_dictionary")
        async with db.sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET LOCAL lock_timeout='500ms'"))
            await session.execute(text("INSERT INTO mrf.ptg_membership_first VALUES (4,99),(5,99)"))
            await session.execute(text("INSERT INTO mrf.ptg_membership_second VALUES (14,105),(99,999)"))
            await session.execute(text("INSERT INTO mrf.ptg_membership_code VALUES (8,100),(9,999),(10,105)"))
            await session.execute(
                text("INSERT INTO mrf.code_source_provider_set VALUES (1,105,:count,decode(repeat('05',16),'hex'))"),
                {"count": missing_count},
            )
            await build._create_stage_tables(session)
            args = (
                session,
                SimpleNamespace(binding={"ordinal": 0}, serving_tables=object()),
                (SimpleNamespace(schema_name="mrf"),),
                [{"code_key": key} for key in code_keys],
                100,
                factor_limit,
            )
            if error:
                with pytest.raises((ValueError, PTG2ManifestArtifactError), match=error):
                    await build._stage_memberships(*args)
                assert await session.scalar(text("SELECT count(*) FROM plan_pricing_provider_member_stage")) == 0
            else:
                assert await build._stage_memberships(*args) == expected
                actual = (
                    await session.execute(text("SELECT provider_set_key,npi FROM plan_pricing_provider_member_stage"))
                ).all()
                assert set(actual) == (
                    {
                        (100, 1000000001),
                        (100, 1000000002),
                        (101, 1000000001),
                        (101, 1000000002),
                        (102, 1000000003),
                        (104, 1000000004),
                    }
                    if expected[0]
                    else set()
                )


@pytest.mark.asyncio
async def test_concurrent_code_preparation_and_provider_refresh_preserve_exact_old_reads(monkeypatch):
    async with _database(monkeypatch) as db:
        oid = await _source_database(db)
        identity = _fixture(monkeypatch, db, oid)

        async def prepare():
            async with db.sessions() as session, session.begin():
                return await build.build_in_session(
                    session,
                    selection=_aliased_selection(),
                    code_system="CPT",
                    code="27447",
                    expected_identity_digest=identity_digest(identity),
                )

        first, duplicate = await asyncio.gather(prepare(), prepare())
        assert first == duplicate and len(db.stages) == 1
        async with db.sessions() as old_reader, old_reader.begin():
            old_found = await locate(old_reader, identity)
            old_scope = CodePricingCapability(copy.deepcopy(identity), old_found)
            identity["provider_signature"] = "c" * 64
            refreshed = await prepare()
            assert first["projection_id"] != refreshed["projection_id"]
            assert first["active_attachment"]["name"] == refreshed["active_attachment"]["name"]
            with code_projection_scope(old_scope):
                npis, _, more = await _read_state_npis(old_reader, old_scope.projection_id, "MI", 0, 50)
                assert npis == (1000000001, 1000000002, 1000000003, 1000000004) and not more
        async with db.sessions() as session, session.begin():
            assert receipt(await locate(session, identity)) == refreshed
            assert await retire_unreferenced(session, first)
            assert receipt(await locate(session, identity)) == refreshed
