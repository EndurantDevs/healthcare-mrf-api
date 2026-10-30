# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared native checks for compact target reads, integrity refusal and flat parity."""

from uuid import UUID

import pytest

from process import registry_required_target_coverage as coverage
from process.network_compact_layout import compact_identifier
from process.registry_compact_target_addresses import required_target_addresses
from process.registry_source_retention import verify_registry_source_retention
from tests.registry_compact_target_postgres_support import (
    bootstrap_db as bootstrap_db,
)
from tests.registry_compact_target_postgres_support import (
    compact_coverage_db as compact_coverage_db,
)
from tests.registry_compact_target_postgres_support import (
    coverage_reader,
    read_compact_coverage,
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
from tests.test_registry_generic_fhir_reuse_postgres import _close_mappings, _compose, _publish
from tests.test_registry_required_target_coverage_postgres import coverage_db as coverage_db

pytestmark = pytest.mark.asyncio


async def _address_rows(context, manifest, networks, *, fault_connection=None):
    relation, arguments = required_target_addresses(manifest, first_parameter=2)
    statement = (
        "WITH networks AS (SELECT unnest($1::integer[]) AS network_id) "
        f"SELECT location_key,entity_type,entity_id,canonical_network_ids FROM {relation} address ORDER BY location_key"
    )
    if fault_connection is not None:
        return await fault_connection.fetch(statement, list(networks), *arguments)
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        return await context.coverage_reader.fetch(statement, list(networks), *arguments)


async def _counts_result(context, manifest):
    """Inspect intentional faults in the fixture admin's uncommitted mutation transaction."""
    relation, arguments = required_target_addresses(manifest, first_parameter=6)
    query = coverage._RESULT_SQL.replace(
        "__COUNTS__",
        coverage._sql(
            coverage._COUNTS_SQL,
            compact_identifier(context.fixture.control_schema),
            compact_identifier(manifest.schema_name),
            addresses=relation,
        ),
    )
    return await context.fixture.connection.fetchrow(
        query,
        context.edition.snapshot_id,
        manifest.approved_custom_revision,
        coverage.MAX_REPORT_BYTES,
        "historical",
        coverage.MAX_NATIVE_INPUT_BYTES,
        *arguments,
    )


async def _assert_retained_source_report(context, manifest, report):
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        candidate = await context.coverage_reader.fetchrow(
            f"SELECT source_generations,validation_json,expected_rows "
            f"FROM {compact_identifier(context.fixture.control_schema)}.network_membership_candidate "
            "WHERE candidate_id=$1",
            UUID(manifest.candidate_id),
        )
    retention = verify_registry_source_retention(candidate)
    assert retention is not None
    assert retention["source_rows"] == report["provenance"]["source_selection"]["mapped_rows"] > 0


@pytest.mark.parametrize("generic_capture_db", ["modern"], indirect=True)
async def test_compact_current_historical_and_empty_edges_keep_complete_target_reports(compact_coverage_db):
    context = compact_coverage_db
    before = await _durable_state(context.fixture)
    for manifest in (context.original, context.current):
        assert (
            await context.fixture.connection.fetchval(
                "SELECT to_regclass($1)", manifest.schema_name + ".entity_address_unified"
            )
            is None
        )
        report = await read_compact_coverage(context, manifest.generation_id)
        await _assert_retained_source_report(context, manifest, report)
        assert report["provenance"]["serving"]["generation_id"] == manifest.generation_id
        assert report["totals"]["ledger_targets"] == 3
        assert report["totals"]["directory_available"] == 2
        assert report["totals"]["exact_membership_count"] == 1
        rows = await _address_rows(context, manifest, tuple(sorted(set(range(1, 502)) | {context.network_id})))
        assert len(rows) == 1 and rows[0]["canonical_network_ids"] == [context.network_id]
    assert (await read_compact_coverage(context))["provenance"]["serving"][
        "generation_id"
    ] == context.current.generation_id
    assert await _durable_state(context.fixture) == before
    revision = await _close_mappings(context, ("network-one", "network-unapproved"))
    empty = await _publish(context, await _compose(context, revision, context.current))
    assert await _address_rows(context, empty, (context.network_id,)) == []
    assert (await read_compact_coverage(context))["totals"]["directory_available"] == 0
    assert (await read_compact_coverage(context, context.original.generation_id))["totals"]["directory_available"] == 2


async def _damage(context, manifest, kind):
    connection, layout = context.fixture.connection, manifest.compact_layout
    if kind == "missing_base":
        await connection.execute(
            f"DELETE FROM {compact_identifier(layout.base.schema_name)}.{compact_identifier(layout.base.table_name)} "
            "WHERE location_key=$1",
            "1" * 64,
        )
    elif kind == "alias_contradiction":
        await connection.execute(
            f"UPDATE {compact_identifier(manifest.schema_name)}.provider_location_binding "
            "SET entity_id='contradictory-identity' WHERE location_key=$1",
            "1" * 64,
        )
    else:
        artifact = layout.artifacts[0]
        slice_id = await connection.fetchval(
            f"SELECT slice_id FROM {compact_identifier(layout.ref_map.schema_name)}.network_slice_ref "
            "WHERE network_id=$1 AND artifact_candidate_id=$2 LIMIT 1",
            context.network_id,
            UUID(artifact.candidate_id),
        )
        await connection.execute(
            f"INSERT INTO {compact_identifier(artifact.schema_name)}.network_office_edge(slice_id,location_key) "
            "VALUES($1,$2)",
            slice_id,
            "2" * 64,
        )


@pytest.mark.parametrize("generic_capture_db", ["modern"], indirect=True)
async def test_actual_edges_missing_base_and_alias_contradictions_cannot_hide_in_coverage(compact_coverage_db):
    context, manifest = compact_coverage_db, compact_coverage_db.original
    for kind in ("orphan_edge", "missing_base", "alias_contradiction"):
        transaction = context.fixture.connection.transaction()
        await transaction.start()
        try:
            await _damage(context, manifest, kind)
            if kind == "missing_base":
                rows = await _address_rows(
                    context, manifest, (context.network_id,), fault_connection=context.fixture.connection
                )
                assert len(rows) == 1 and rows[0]["location_key"] == "1" * 64
                assert rows[0]["entity_type"] is None and rows[0]["canonical_network_ids"] == [context.network_id]
            counts = await _counts_result(context, manifest)
            assert counts["valid"] is False and counts["native_input"] is None
        finally:
            await transaction.rollback()
        assert (await read_compact_coverage(context, manifest.generation_id))["totals"]["directory_available"] == 2


async def test_flat_required_target_report_keeps_existing_golden_counts(coverage_db):
    async with coverage_reader(coverage_db):
        report = await read_compact_coverage(coverage_db)
    assert report["totals"]["ledger_targets"] == 3
    assert report["totals"]["mapped_targets"] == report["totals"]["directory_available"] == 2
    assert report["totals"]["exact_membership_count"] == 1
    assert report["totals"]["unresolved_location_count"] == 0
    assert report["totals"]["pricing_evidence_count"] is None
