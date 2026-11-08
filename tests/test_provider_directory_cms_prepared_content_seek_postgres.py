# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full-value proof pages use a matching private index without changing map keys."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_identity_validation as validation
from process.provider_directory_resource_identity import source_resource_uuid
from tests.test_provider_directory_cms_prepared_identity_validation_postgres import (
    native_case,
    native_frozen_case,
    observe,
    parity_case,
    publisher,
    scope,
)


def scan_nodes(node):
    return ([node] if "Scan" in node["Node Type"] else []) + [
        child_scan for child in node.get("Plans", []) for child_scan in scan_nodes(child)
    ]


async def test_native_identity_proof_uses_private_c_seek_without_row_loss(parity_case):
    case, total = parity_case, 100_000
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with scope(case, session, driver, inputs) as workspace:
            root = validation.resources.RESOURCE_IDENTITY_ROOT
            created = datetime(2026, 1, 1, tzinfo=timezone.utc)
            identity_rows = (
                ("cms-npd", "InsurancePlan", key, source_resource_uuid("cms-npd", "InsurancePlan", key), created)
                for key in (f"plan-{ordinal:06}" for ordinal in range(1, total + 1))
            )
            assert (
                await workspace.copy_rows(root, validation.resources.RESOURCE_IDENTITY_COLUMNS, identity_rows) == total
            )
            await workspace.finish_indexes()
            receipt = workspace.receipts[root]
            relation = storage_relation(workspace.policy.schema, receipt["name"])
            queries = []

            async def capture(query, *arguments):
                queries.append(query)
                return []

            await validation._content_commitment(
                SimpleNamespace(connection=SimpleNamespace(fetch=capture)), {root: relation}
            )
            query = queries[0]
            seek = storage_relation(workspace.policy.schema, receipt["name"] + "_content_seek")
            await driver.execute("SET LOCAL plan_cache_mode=force_generic_plan")

            async def explain(name):
                await driver.execute(f"PREPARE {name}(text,text,text) AS {query}")
                try:
                    raw = await driver.fetchval(
                        f"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE {name}('cms-npd','InsurancePlan','plan-050000')"
                    )
                    return (json.loads(raw) if isinstance(raw, str) else raw)[0]["Plan"]
                finally:
                    await driver.execute(f"DEALLOCATE {name}")

            transaction = driver.transaction()
            await transaction.start()
            try:
                await driver.execute("DROP INDEX " + seek)
                baseline = await explain("content_baseline")
            finally:
                await transaction.rollback()
            indexed = await explain("content_indexed")
            assert any(node["Node Type"] == "Seq Scan" for node in scan_nodes(baseline))
            indexed_scans = [
                node for node in scan_nodes(indexed) if node.get("Index Name") == receipt["name"] + "_content_seek"
            ]
            assert len(indexed_scans) == 1
            assert ">" in indexed_scans[0]["Index Cond"] and "Filter" not in indexed_scans[0]
            assert indexed_scans[0]["Actual Rows"] == validation.COPY_MAX_ROWS
            counts, digest = await validation._content_commitment(workspace, {root: relation})
            assert counts == {root: total} and len(digest) == 64
            await workspace.verify_current()
            assert await driver.fetchval("SELECT current_user") == workspace.policy.publisher_role
