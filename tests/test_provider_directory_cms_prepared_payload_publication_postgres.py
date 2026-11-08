# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native six-table COPY/parity component, not installed-guard or authority proof."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import CheckConstraint, Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateColumn

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_payload_publication as payloads
from process import provider_directory_generic_source_policy as source
from process.provider_directory_cms_prepared_workspace import _layouts
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    full_private_case,
    native_case,
    native_derived_case,
    native_frozen_case,
    observe,
    prepare,
    publisher,
)
from tests.test_provider_directory_cms_prepared_relationships_postgres import input_snapshots


@pytest_asyncio.fixture
async def payload_case(full_private_case):
    """Add typed canonical targets and exact SELECT/INSERT in disposable storage."""
    case = full_private_case
    for root in payloads.ROOTS:
        relation = storage_relation(case.storage.catalog_schema, root)
        if root == payloads.storage.RELATIONSHIP:
            definitions, _, checks = _layouts()[root]
        else:
            table = payloads._MODELS[payloads.ROOTS.index(root)].__table__
            definitions = [
                str(
                    CreateColumn(
                        Column(
                            column.name,
                            column.type,
                            *([column.computed.copy()] if column.computed is not None else []),
                            nullable=column.nullable,
                        )
                    ).compile(dialect=dialect())
                )
                for column in table.columns
            ]
            checks = [
                str(constraint.sqltext) for constraint in table.constraints if isinstance(constraint, CheckConstraint)
            ]
        keys = ",".join(storage_identifier(key) for key in payloads.completion._KEYS[root])
        await case.admin.execute(
            f"CREATE TABLE {relation} ({','.join([*definitions, *(f'CHECK ({check})' for check in checks)])},PRIMARY KEY({keys}))"
        )
        await case.admin.execute(f'ALTER TABLE {relation} OWNER TO "{case.roles["namespace"]}"')
        await case.admin.execute(f'GRANT SELECT,INSERT ON {relation} TO "{case.policy.publisher_role}"')
        assert not await case.admin.fetchval(
            "SELECT has_table_privilege($1,$2,'UPDATE,DELETE,TRUNCATE')", case.policy.publisher_role, relation
        )
    resource = storage_relation(case.storage.catalog_schema, payloads.storage.RESOURCE)
    await case.admin.execute(
        f"INSERT INTO {resource}(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "VALUES('incumbent','Practitioner','incumbent',$1,'{}')",
        "a" * 64,
    )
    return case


async def target_snapshot(case):
    return {
        root: await case.admin.fetchval(
            f"SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) "
            f"FROM {storage_relation(case.storage.catalog_schema, root)} r"
        )
        for root in payloads.ROOTS
    }


async def target_counts(connection, case):
    return {
        root: await connection.fetchval(
            f"SELECT count(*) FROM {storage_relation(case.storage.catalog_schema, root)} WHERE dataset_id=$1",
            case.handoff.document["identity"]["dataset_id"],
        )
        for root in payloads.ROOTS
    }


async def assert_keyset_range_plan(case, sql):
    """Require generic plans to seek the complete row bound without a page filter."""
    await case.admin.execute("SET plan_cache_mode=force_generic_plan")
    await case.admin.execute("PREPARE payload_seek(text,text,text,text,integer,integer) AS " + sql)
    try:
        observed = await case.admin.fetchval(
            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE payload_seek"
            "('dataset','dataset','B-00050001','plan-50001',4096,33554411)"
        )
    finally:
        await case.admin.execute("DEALLOCATE payload_seek")
    document = json.loads(observed) if isinstance(observed, str) else observed
    scans = []

    def observe_scan(node):
        if node["Node Type"] in {"Index Scan", "Index Only Scan"}:
            scans.append(node)
        for child in node.get("Plans", []):
            observe_scan(child)

    observe_scan(document[0]["Plan"])
    assert any("ROW(" in node.get("Index Cond", "") and "> ROW(" in node["Index Cond"] for node in scans)
    assert all("Filter" not in node and node.get("Rows Removed by Filter", 0) == 0 for node in scans)


@pytest.mark.parametrize("collation", [None, "en-US-x-icu"])
async def test_native_keyset_uses_existing_collation_index_bound_and_complete_copy(native_case, collation):
    case = native_case
    suffix = "" if collation is None else ' COLLATE "en-US-x-icu"'
    source_relation = "private_source.paging_network"
    root = payloads.ROOTS[2]
    target_relation = storage_relation(case.storage.catalog_schema, root)
    definition = (
        f"dataset_id varchar(96){suffix} NOT NULL,network_resource_id varchar(256){suffix} NOT NULL,"
        f"insurance_plan_resource_id varchar(256){suffix} NOT NULL,"
        "PRIMARY KEY(dataset_id,network_resource_id,insurance_plan_resource_id)"
    )
    await case.admin.execute(f"CREATE TABLE {source_relation}({definition})")
    await case.admin.execute(f"CREATE TABLE {target_relation}({definition})")
    await case.admin.execute(
        f"INSERT INTO {source_relation} SELECT 'dataset',"
        "(CASE n%4 WHEN 0 THEN 'a-' WHEN 1 THEN 'B-' WHEN 2 THEN 'Z-' ELSE 'z-' END)||lpad(n::text,8,'0'),"
        "'plan-'||n FROM generate_series(1,100000) n"
    )
    await case.admin.execute(f"ANALYZE {source_relation}")
    await case.admin.execute(f'GRANT SELECT ON {source_relation} TO "{case.policy.publisher_role}"')
    await case.admin.execute(f'GRANT SELECT,INSERT ON {target_relation} TO "{case.policy.publisher_role}"')
    sql = payloads._page(root, source_relation)
    await assert_keyset_range_plan(case, sql)
    if collation is not None:
        assert await case.admin.fetchval(f"SELECT array_agg(k ORDER BY k{suffix}) FROM (VALUES('a'),('B')) v(k)") == [
            "a",
            "B",
        ]
    async with publisher(case) as (session, driver):
        workspace = SimpleNamespace(
            connection=driver,
            session=session,
            policy=case.policy,
            catalog_schema=case.storage.catalog_schema,
            identity={"dataset_id": "dataset"},
            verify_current=AsyncMock(),
        )
        assert await payloads._copy_table(workspace, root, {"name": "paging_network"}) == 100000
        assert not await driver.fetchval(
            f"SELECT EXISTS((TABLE {source_relation} EXCEPT ALL TABLE {target_relation}) "
            f"UNION ALL (TABLE {target_relation} EXCEPT ALL TABLE {source_relation}))"
        )
        assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
        assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0


async def test_native_six_table_copy_preserves_json_generated_null_and_graph_parity(payload_case, monkeypatch):
    case = payload_case
    before = await input_snapshots(case)
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    monkeypatch.setattr(payloads, "COPY_MAX_ROWS", 2)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            proof = await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
            assert proof["state"] == "verified_data"
            assert proof["row_counts"] == await target_counts(driver, case)
            assert proof["row_counts"][payloads.ROOTS[1]] == 7
            assert proof["row_counts"][payloads.storage.RELATIONSHIP] > 20
            assert await payloads.verify_prepared_cms_payloads(workspace, case.handoff) == proof
            plan = storage_relation(case.storage.catalog_schema, payloads.ROOTS[1])
            assert await driver.fetchval(f"SELECT count(*) FROM {plan} WHERE plan_identifier IS NULL") == 7
            assert await driver.fetchval(f"SELECT bool_and(plan_active) FROM {plan}") is True
            resource = storage_relation(case.storage.catalog_schema, payloads.storage.RESOURCE)
            assert await driver.fetchval(f"SELECT count(*) FROM {resource} WHERE acquired_resource_sha256 IS NULL") > 0
            assert await driver.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient
            assert await driver.fetchval(f"SELECT count(*) FROM {resource} WHERE dataset_id='incumbent'") == 1
    assert await input_snapshots(case) == before


@pytest.mark.parametrize("failure", ["constraint", "cancel"])
async def test_native_late_graph_copy_rolls_back_every_target_and_retries(payload_case, monkeypatch, failure):
    case = payload_case
    before = await target_snapshot(case)
    frozen = await input_snapshots(case)
    copy_table = payloads._copy_table

    async def fail_graph(workspace, root, receipt):
        if root == payloads.storage.RELATIONSHIP:
            assert all(
                count > 0 for name, count in (await target_counts(workspace.connection, case)).items() if name != root
            )
            if failure == "cancel":
                raise asyncio.CancelledError()
            await workspace.connection.execute(
                f"INSERT INTO {storage_relation(workspace.catalog_schema, root)}(dataset_id) VALUES($1)",
                workspace.identity["dataset_id"],
            )
        return await copy_table(workspace, root, receipt)

    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            with monkeypatch.context() as patch:
                patch.setattr(payloads, "_copy_table", fail_graph)
                with pytest.raises(asyncio.CancelledError if failure == "cancel" else asyncpg.NotNullViolationError):
                    await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
            assert await target_snapshot(case) == before
            assert not any((await target_counts(driver, case)).values())
            assert await driver.fetchval("SELECT current_user") == case.policy.publisher_role
            assert len(await workspace.verify_current()) == 11
            assert await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
    assert await input_snapshots(case) == frozen


async def test_native_any_preexisting_exact_dataset_target_refuses_without_appending(payload_case):
    case = payload_case
    relation = storage_relation(case.storage.catalog_schema, payloads.storage.RESOURCE)
    resource = case.normalized[0]
    await case.admin.execute(
        f"INSERT INTO {relation}(dataset_id,resource_type,resource_id,payload_hash,payload_json) VALUES($1,$2,$3,$4,$5)",
        case.handoff.document["identity"]["dataset_id"],
        resource["resource_type"],
        resource["resource_id"],
        resource["payload_hash"],
        payloads.storage._encoded(resource["payload_json"]),
    )
    before = await target_snapshot(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            with pytest.raises(ValueError, match="target_not_empty"):
                await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
            assert await target_snapshot(case) == before


async def test_native_oversized_first_value_refuses_and_retry_preserves_inputs(payload_case, monkeypatch):
    case = payload_case
    frozen = await input_snapshots(case)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            root = payloads.storage.RESOURCE
            receipt = case.handoff.document["tables"][root]
            relation = storage_relation(workspace.policy.schema, receipt["name"])
            query = payloads._page(root, relation, first=True)
            page = await driver.fetch(query, workspace.identity["dataset_id"], 4096, 107)
            assert len(page) == 1 and page[0]["payload_json"] is None and page[0]["copy_row_bytes"] > 107
            with monkeypatch.context() as patch:
                patch.setattr(payloads, "COPY_MAX_BYTES", 128)
                with pytest.raises(ValueError, match="copy_page_unbounded"):
                    await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
            assert not any((await target_counts(driver, case)).values())
            assert await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
    assert await input_snapshots(case) == frozen


@pytest.mark.parametrize("drift", ["extra", "generated"])
async def test_native_full_readback_detects_extra_or_valid_changed_generated_values(payload_case, drift):
    case = payload_case
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, evidence):
            await payloads.copy_prepared_cms_payloads(workspace, case.handoff)
            plan = storage_relation(case.storage.catalog_schema, payloads.ROOTS[1])
            if drift == "extra":
                # A separately committed extra row must not be hidden by matching counts elsewhere.
                await case.admin.execute(
                    f"INSERT INTO {plan}(dataset_id,resource_id,payload_hash,payload_json) VALUES($1,'extra',$2,'{{}}')",
                    workspace.identity["dataset_id"],
                    "a" * 64,
                )
            else:
                await driver.execute(f"SAVEPOINT unchanged_publisher")
                # Publisher cannot update canonical rows; change a source value to exercise all-column comparison.
                source_plan = storage_relation(workspace.policy.schema, workspace.receipts[payloads.ROOTS[1]]["name"])
                await driver.execute(
                    f"UPDATE {source_plan} SET payload_json='{{\"status\":\"inactive\"}}' WHERE resource_id='plan-one'"
                )
            with pytest.raises(ValueError, match="payload_changed"):
                await payloads.verify_prepared_cms_payloads(workspace, case.handoff)
            if drift == "generated":
                await driver.execute("ROLLBACK TO SAVEPOINT unchanged_publisher")
                await driver.execute("RELEASE SAVEPOINT unchanged_publisher")
