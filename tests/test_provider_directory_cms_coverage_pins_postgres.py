# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Coverage query/lock scope on native partitions; no CMS authority is issued."""

import json
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_serving_coverage as coverage
from tests.test_provider_directory_cms_prepared_context_postgres import native_case, publisher

SCHEMA = '"accepted_catalog"'
DATASET = "selected'quoted"
RELEASE = "a" * 64
_RESOURCE = "provider_directory_dataset_resource"
_GRAPH = "provider_directory_cms_npd_relationship"
_LAYOUTS = {
    _RESOURCE: "dataset_id text NOT NULL,resource_type text NOT NULL,resource_id text NOT NULL,"
    "payload_json jsonb,acquired_resource_sha256 text,PRIMARY KEY(dataset_id,resource_type,resource_id)",
    _GRAPH: "dataset_id text NOT NULL,note text NOT NULL,PRIMARY KEY(dataset_id,note)",
    "provider_directory_entity_source_binding": "source_id text,resource_type text,resource_id text,"
    "organization_id uuid,site_id uuid",
    "provider_directory_entity_release_evidence": "source_id text,resource_type text,resource_id text,"
    "release_id text,payload_sha256 text,payload_json jsonb",
    "provider_directory_insurance_network_source_binding": "source_id text,resource_type text,resource_id text,"
    "network_id uuid",
    "provider_directory_insurance_network_plan_evidence": "source_id text,release_id text,network_resource_id text,"
    "insurance_plan_resource_id text,plan_payload_sha256 text",
    "provider_directory_resource_identity": "source_id text,resource_type text,resource_id text,entity_id uuid",
}


async def _proof_tables(case):
    for root, columns in _LAYOUTS.items():
        should_partition = root == _RESOURCE or (root == _GRAPH and case.has_graph_partitions)
        partition = " PARTITION BY LIST(dataset_id)" if should_partition else ""
        await case.admin.execute(f'CREATE TABLE {SCHEMA}."{root}"({columns}){partition}')
        await case.admin.execute(f'GRANT SELECT ON {SCHEMA}."{root}" TO "{case.policy.publisher_role}"')
    for root, prefix in ((_RESOURCE, "resources"), (_GRAPH, "graph")):
        if root == _GRAPH and not case.has_graph_partitions:
            continue
        for suffix, dataset in (("selected", DATASET), ("sibling", "other")):
            literal = await case.admin.fetchval("SELECT quote_literal($1::text)", dataset)
            await case.admin.execute(
                f'CREATE TABLE {SCHEMA}.{prefix}_{suffix} PARTITION OF {SCHEMA}."{root}" FOR VALUES IN ({literal})'
            )


async def _proof_rows(case):
    for kind, identifier, payload_json in (
        ("Organization", "organization", {}),
        ("Organization", "network", {"type": [{"text": "ntwk"}]}),
        ("Location", "site", {}),
        ("InsurancePlan", "plan", {"network_refs": ["Organization/network"]}),
        ("PractitionerRole", "role", {}),
    ):
        await case.admin.execute(
            f'INSERT INTO {SCHEMA}."{_RESOURCE}" VALUES($1,$2,$3,$4::jsonb,$5)',
            DATASET,
            kind,
            identifier,
            json.dumps(payload_json),
            "b" * 64,
        )
        if kind in {"Organization", "Location"}:
            await case.admin.execute(
                f"INSERT INTO {SCHEMA}.provider_directory_entity_source_binding VALUES('cms-npd',$1,$2,$3,$4)",
                kind,
                identifier,
                uuid4() if kind == "Organization" else None,
                uuid4() if kind == "Location" else None,
            )
            await case.admin.execute(
                f"INSERT INTO {SCHEMA}.provider_directory_entity_release_evidence "
                "VALUES('cms-npd',$1,$2,$3,$4,$5::jsonb)",
                kind,
                identifier,
                RELEASE,
                "b" * 64,
                json.dumps(payload_json),
            )
        else:
            await case.admin.execute(
                f"INSERT INTO {SCHEMA}.provider_directory_resource_identity VALUES('cms-npd',$1,$2,$3)",
                kind,
                identifier,
                uuid4(),
            )
    await case.admin.execute(
        f"INSERT INTO {SCHEMA}.provider_directory_insurance_network_source_binding "
        "VALUES('cms-npd','Organization','network',$1)",
        uuid4(),
    )
    await case.admin.execute(
        f"INSERT INTO {SCHEMA}.provider_directory_insurance_network_plan_evidence "
        "VALUES('cms-npd',$1,'network','plan',$2)",
        RELEASE,
        "b" * 64,
    )


@pytest_asyncio.fixture
async def coverage_case(native_case, monkeypatch, request):
    case = native_case
    case.has_graph_partitions = getattr(request, "param", True)
    await _proof_tables(case)
    await _proof_rows(case)
    monkeypatch.setattr(coverage, "_schema_name", lambda: SCHEMA)
    return case


async def _settings(session):
    return dict(
        (
            await session.execute(
                text(
                    "SELECT current_setting('statement_timeout') AS statement_timeout,"
                    "current_setting('lock_timeout') AS lock_timeout,"
                    "current_setting('plan_cache_mode') AS plan_cache_mode"
                )
            )
        )
        .mappings()
        .one()
    )


async def _lock_names(driver):
    return {
        row["relname"]
        for row in await driver.fetch(
            "SELECT c.relname FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
            "WHERE l.pid=pg_backend_pid() AND l.granted AND l.locktype='relation' "
            "AND c.relnamespace='accepted_catalog'::regnamespace AND c.relkind IN ('r','p')"
        )
    }


async def _peer_lock(case, relation, *, should_block):
    if should_block:
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with case.admin.transaction():
                await case.admin.execute(f"LOCK TABLE ONLY {SCHEMA}.{relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
    else:
        async with case.admin.transaction():
            await case.admin.execute(f"LOCK TABLE ONLY {SCHEMA}.{relation} IN ACCESS EXCLUSIVE MODE NOWAIT")


@pytest.mark.parametrize("mode", ["auto", "force_generic_plan"])
@pytest.mark.parametrize("statement,lock", [("0", "0"), ("1s", "50ms")])
async def test_coverage_keeps_selected_pins_and_strict_settings(coverage_case, mode, statement, lock):
    case = coverage_case
    async with publisher(case) as (session, driver):
        for name, setting in (("plan_cache_mode", mode), ("statement_timeout", statement), ("lock_timeout", lock)):
            await driver.fetchval("SELECT set_config($1,$2,true)", name, setting)
        before = await _settings(session)
        await driver.execute(f"LOCK TABLE ONLY {SCHEMA}.transaction_probe IN ROW EXCLUSIVE MODE")
        for _ in range(10):
            await coverage.validate_cms_candidate_coverage(session, DATASET, RELEASE)
            assert await _settings(session) == before
        expected_names = {*coverage._PROOF_TABLES, "resources_selected", "graph_selected", "transaction_probe"}
        assert await _lock_names(driver) == expected_names
        await _peer_lock(case, "resources_sibling", should_block=False)
        await _peer_lock(case, "graph_sibling", should_block=False)
        await _peer_lock(case, "resources_selected", should_block=True)
        await _peer_lock(case, "graph_selected", should_block=True)
    await _peer_lock(case, "resources_selected", should_block=False)


async def test_generic_plan_negative_control_locks_siblings(coverage_case):
    class Rollback(Exception):
        pass

    case = coverage_case
    async with publisher(case) as (session, driver):
        await driver.execute("SET LOCAL plan_cache_mode='force_generic_plan'")
        with pytest.raises(Rollback):
            async with session.begin_nested():
                await coverage._lock_cms_proof_inputs(session, SCHEMA, DATASET)
                assert "resources_sibling" in await _lock_names(driver)
                await _peer_lock(case, "resources_sibling", should_block=True)
                raise Rollback
        assert not await _lock_names(driver)
        await coverage.validate_cms_candidate_coverage(session, DATASET, RELEASE)
        await _peer_lock(case, "resources_sibling", should_block=False)


async def test_coverage_pins_do_not_require_child_grants(coverage_case):
    async with publisher(coverage_case) as (session, driver):
        with pytest.raises(DBAPIError) as denied:
            async with session.begin_nested():
                await session.execute(text(f"LOCK TABLE ONLY {SCHEMA}.resources_selected IN ACCESS SHARE MODE"))
        assert denied.value.orig.sqlstate == "42501"
        await coverage.validate_cms_candidate_coverage(session, DATASET, RELEASE)
        assert "resources_selected" in await _lock_names(driver)


@pytest.mark.parametrize("coverage_case", [False], indirect=True)
async def test_coverage_preserves_legacy_graph_heap(coverage_case):
    case = coverage_case
    async with publisher(case) as (session, driver):
        await coverage.validate_cms_candidate_coverage(session, DATASET, RELEASE)
        assert await _lock_names(driver) == {*coverage._PROOF_TABLES, "resources_selected"}
        await _peer_lock(case, "resources_sibling", should_block=False)


async def test_coverage_allows_disjoint_attachment_while_reading(coverage_case, monkeypatch):
    case = coverage_case
    original = coverage.require_cms_bindings
    state = SimpleNamespace(has_attached=False)

    async def attach_after_read(*arguments):
        await original(*arguments)
        if not state.has_attached:
            state.has_attached = True
            async with case.admin.transaction():
                await case.admin.execute("SET LOCAL lock_timeout='50ms'; SET LOCAL statement_timeout='1s'")
                await case.admin.execute(
                    f'CREATE TABLE {SCHEMA}.resources_disjoint(LIKE {SCHEMA}."{_RESOURCE}" INCLUDING ALL)'
                )
                await case.admin.execute(
                    f"ALTER TABLE {SCHEMA}.\"{_RESOURCE}\" ATTACH PARTITION {SCHEMA}.resources_disjoint FOR VALUES IN ('later')"
                )

    monkeypatch.setattr(coverage, "require_cms_bindings", attach_after_read)
    async with publisher(case) as (session, driver):
        await coverage.validate_cms_candidate_coverage(session, DATASET, RELEASE)
        assert state.has_attached and "resources_disjoint" not in await _lock_names(driver)


@pytest.mark.parametrize("failure", ["proof", "statement", "root_lock"])
async def test_failed_proof_restores_settings_and_outer_locks(coverage_case, monkeypatch, failure):
    case = coverage_case
    original = coverage.require_cms_bindings

    async def fail(*arguments):
        await original(*arguments)
        if failure == "statement":
            await arguments[0].execute(text("SELECT pg_sleep(1)"))
        raise RuntimeError("proof refused")

    monkeypatch.setattr(coverage, "require_cms_bindings", fail)
    async with publisher(case) as (session, driver):
        await driver.execute(
            "SET LOCAL statement_timeout='100ms'; SET LOCAL lock_timeout='50ms'; "
            "SET LOCAL plan_cache_mode='force_generic_plan'"
        )
        await driver.execute(f"LOCK TABLE ONLY {SCHEMA}.transaction_probe IN ROW EXCLUSIVE MODE")
        before = await _settings(session)
        peer_transaction = case.admin.transaction()
        await peer_transaction.start()
        try:
            if failure == "root_lock":
                await case.admin.execute(
                    f"LOCK TABLE ONLY {SCHEMA}.provider_directory_entity_source_binding IN ACCESS EXCLUSIVE MODE"
                )
            with pytest.raises((RuntimeError, DBAPIError)) as refused:
                await coverage.validate_cms_candidate_coverage(session, DATASET, RELEASE)
            if failure != "proof":
                assert refused.value.orig.sqlstate == ("57014" if failure == "statement" else "55P03")
            assert await _settings(session) == before
            assert await _lock_names(driver) == {"transaction_probe"}
            assert await session.scalar(text("SELECT 1")) == 1
        finally:
            await peer_transaction.rollback()
