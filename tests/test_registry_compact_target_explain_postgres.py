# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inspect genuine compact coverage plans; this small fixture cannot establish production performance."""

import hashlib
import json
import os
from dataclasses import asdict
from pathlib import Path

import pytest

from process import registry_required_target_coverage as coverage
from process.network_compact_layout import compact_identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_compact_target_addresses import required_target_addresses
from tests.registry_compact_target_postgres_support import (
    bootstrap_db as bootstrap_db,
)
from tests.registry_compact_target_postgres_support import (
    compact_coverage_db as compact_coverage_db,
)
from tests.registry_compact_target_postgres_support import (
    custom_db as custom_db,
)
from tests.registry_compact_target_postgres_support import (
    generic_capture_db as generic_capture_db,
)
from tests.registry_compact_target_postgres_support import (
    generic_composition_db as generic_composition_db,
)
from tests.registry_compact_target_postgres_support import (
    serving_schema as serving_schema,
)
from tests.test_network_provider_routes_postgres import _durable_state

pytestmark = pytest.mark.asyncio

_PLAN_FIELDS = (
    "Node Type",
    "Parent Relationship",
    "Subplan Name",
    "CTE Name",
    "Schema",
    "Relation Name",
    "Alias",
    "Index Name",
    "Index Cond",
    "Filter",
    "Actual Rows",
    "Actual Loops",
    "Rows Removed by Filter",
    "Shared Hit Blocks",
    "Shared Read Blocks",
    "Temp Read Blocks",
    "Temp Written Blocks",
)


def _scan_summary(plan):
    summaries, pending = [], [plan["Plan"]]
    while pending:
        node = pending.pop()
        if any(field in node for field in ("Relation Name", "CTE Name", "Subplan Name")):
            summaries.append({field: node[field] for field in _PLAN_FIELDS if field in node})
        pending.extend(reversed(node.get("Plans", [])))
    return summaries


async def _explain(connection, statement, arguments):
    encoded = await connection.fetchval(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON, TIMING OFF) " + statement,
        *arguments,
    )
    plans = json.loads(encoded) if isinstance(encoded, str) else encoded
    assert len(plans) == 1 and plans[0]["Plan"]["Actual Loops"] == 1
    return {
        "statement_sha256": hashlib.sha256(statement.encode()).hexdigest(),
        "argument_count": len(arguments),
        "plan": plans[0],
        "scan_summary": _scan_summary(plans[0]),
    }


async def _coverage_plan(context, connection, manifest):
    """Explain the full production RESULT query with its original five arguments and compact pins."""
    namespace = compact_identifier(context.fixture.control_schema)
    provenance = await connection.fetchrow(coverage._sql(coverage._PROVENANCE_SQL, namespace), manifest.generation_id)
    relation, arguments = required_target_addresses(manifest, first_parameter=6)
    statement = coverage._RESULT_SQL.replace(
        "__COUNTS__",
        coverage._sql(coverage._COUNTS_SQL, namespace, compact_identifier(manifest.schema_name), addresses=relation),
    )
    return await _explain(
        connection,
        statement,
        (
            context.edition.snapshot_id,
            manifest.approved_custom_revision,
            coverage.MAX_REPORT_BYTES,
            provenance["manifest_status"],
            coverage.MAX_NATIVE_INPUT_BYTES,
            *arguments,
        ),
    )


async def _administrative_plan(context, connection, manifest):
    relation, arguments = required_target_addresses(manifest, first_parameter=2)
    statement = (
        "WITH networks AS (SELECT unnest($1::integer[]) AS network_id) "
        f"SELECT location_key,entity_type,entity_id,canonical_network_ids FROM {relation} address ORDER BY location_key"
    )
    network_ids = sorted(set(range(1, 502)) | {context.network_id})
    rows = await connection.fetch(statement, network_ids, *arguments)
    assert len(network_ids) >= 501 and len(rows) == 1
    assert rows[0]["canonical_network_ids"] == [context.network_id]
    return await _explain(connection, statement, (network_ids, *arguments))


async def _published_plans(context, selected):
    """Verify the complete report and custody before explaining both queries in the same Reader snapshot."""
    connection = context.coverage_reader
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await connection.execute("SET LOCAL statement_timeout = '30s'")
        report = await coverage.read_registry_required_target_coverage(
            connection,
            context.edition.snapshot_id,
            generation_id=selected.generation_id,
            control_schema=context.fixture.control_schema,
        )
        manifest = await resolve_network_serving_manifest(
            connection, generation_id=selected.generation_id, control_schema=context.fixture.control_schema
        )
        assert manifest == selected and manifest.compact_layout is not None
        assert report["totals"]["directory_available"] == 2 and report["totals"]["exact_membership_count"] == 1
        return {
            "manifest": asdict(manifest),
            "compact_layout": asdict(manifest.compact_layout),
            "full_result": await _coverage_plan(context, connection, manifest),
            "administrative_501": await _administrative_plan(context, connection, manifest),
        }


@pytest.mark.parametrize("generic_capture_db", ["modern"], indirect=True)
@pytest.mark.skipif(
    not os.environ.get("NETWORK_COMPACT_TARGET_EXPLAIN_OUTPUT"),
    reason="Set NETWORK_COMPACT_TARGET_EXPLAIN_OUTPUT to preserve the private plan receipt",
)
async def test_genuine_compact_coverage_explain_records_complete_and_administrative_plans(compact_coverage_db):
    output_directory = os.environ["NETWORK_COMPACT_TARGET_EXPLAIN_OUTPUT"]
    context = compact_coverage_db
    before = await _durable_state(context.fixture)
    plan_receipt_by_field = {
        "scope": "small genuine retained-source publication fixture; no representative performance qualification",
        "reader": context.fixture.roles["reader"],
        "generation_plans": [
            await _published_plans(context, manifest) for manifest in (context.original, context.current)
        ],
    }
    assert await _durable_state(context.fixture) == before
    destination = Path(output_directory)
    destination.mkdir(parents=True, exist_ok=True)
    artifact = destination / "compact-target-coverage-plans.json"
    assert not artifact.exists()
    artifact.write_text(json.dumps(plan_receipt_by_field, indent=2, sort_keys=True) + "\n")
    print("COMPACT_TARGET_PLAN_RECEIPT " + str(artifact), flush=True)
