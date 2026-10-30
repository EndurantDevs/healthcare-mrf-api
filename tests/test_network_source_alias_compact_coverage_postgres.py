# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated Source aliases retain exact counts through compact target reads."""

import json

import pytest

from process.network_compact_layout import compact_identifier
from process.registry_compact_target_addresses import required_target_addresses
from tests.registry_compact_target_postgres_support import coverage_reader, read_compact_coverage
from tests.test_network_provider_routes_postgres import _durable_state
from tests.test_network_source_office_aliases_postgres import _compose
from tests.test_network_source_office_aliases_postgres import alias_sources_db as alias_sources_db
from tests.test_network_source_office_aliases_postgres import custom_db as custom_db
from tests.test_network_source_office_aliases_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command
from tests.test_registry_generic_fhir_reuse_postgres import _publish
from tests.test_registry_network_binding_approval_postgres import _bind
from tests.test_registry_required_target_coverage_postgres import _decision, _ledger
from tests.test_registry_required_target_review_store_postgres import _SOURCE_FIELDS, _review

pytestmark = pytest.mark.asyncio


async def _review_alias_targets(context):
    fixture = context.fixture
    binding_rows = await fixture.connection.fetch(
        f'SELECT to_jsonb(binding)::text FROM "{fixture.control_schema}".registry_network_binding binding '
        "WHERE NOT archived AND network_id=ANY($1::integer[]) ORDER BY network_id",
        list(context.network_ids),
    )
    bindings = [json.loads(binding_record[0]) for binding_record in binding_rows]
    assert {binding_record["network_id"] for binding_record in bindings} == set(context.network_ids)
    assert {binding_record["source_id"] for binding_record in bindings} == {"source-one", "source-two"}
    assert len(bindings) == 2 and all(binding_record["source_system"] == "fhir" for binding_record in bindings)
    edition, ledger, ledger_targets = await _ledger(fixture)
    review_by_field = {
        "ledger_snapshot_id": str(edition.snapshot_id),
        "ledger_artifact_sha256": ledger["artifact_sha256"],
        "decisions": [
            _decision(ledger_target["target_key"], binding_record)
            for ledger_target, binding_record in zip(ledger_targets[:2], bindings, strict=True)
        ],
    }
    _, review = await _review((fixture.connection, fixture.control_schema, fixture.engine), edition, review_by_field)
    actor, binding_records = _actor(), []
    for binding_record in bindings:
        binding_by_field = {name: binding_record[name] for name in _SOURCE_FIELDS}
        binding_by_field.update(
            network_id=binding_record["network_id"],
            operation="rebind",
            expected_revision=binding_record["revision"],
            expected_network_id=binding_record["network_id"],
            evidence_id=review["evidence_id"],
            evidence_sha256=review["artifact_sha256"],
        )
        binding_records.append(
            (await _bind(fixture.connection, fixture.control_schema, binding_by_field, actor))["records"][0]
        )
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, *binding_records),
        actor,
    )
    context.edition, context.revision = edition, approved["approved_revision"]
    return ledger_targets


async def _physical_addresses(context, manifest, networks):
    relation, arguments = required_target_addresses(manifest, first_parameter=2)
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        return await context.coverage_reader.fetch(
            "WITH networks AS (SELECT unnest($1::integer[]) AS network_id) "
            f"SELECT location_key,entity_type,entity_id,canonical_network_ids FROM {relation} address",
            list(networks),
            *arguments,
        )


async def _assert_exact_alias_rows(context, manifest):
    namespace = compact_identifier(manifest.schema_name)
    async with context.coverage_reader.transaction(isolation="repeatable_read", readonly=True):
        counts = await context.coverage_reader.fetchrow(
            f"SELECT count(*),count(DISTINCT location_id),count(DISTINCT evidence_id) "
            f"FROM {namespace}.network_membership_source"
        )
        assert tuple(counts) == (2, 2, 2)
        bindings = await context.coverage_reader.fetchrow(
            f"SELECT count(*),count(DISTINCT location_key) FROM {namespace}.provider_location_binding"
        )
        assert tuple(bindings) == (2, 1)
        memberships = await context.coverage_reader.fetch(
            f"SELECT network_id,count(*) FROM {namespace}.network_membership GROUP BY network_id"
        )
        assert {row["network_id"]: row["count"] for row in memberships} == dict.fromkeys(context.network_ids, 1)
    for networks in [(network_id,) for network_id in context.network_ids] + [context.network_ids]:
        rows = await _physical_addresses(context, manifest, networks)
        assert len(rows) == 1
        assert (rows[0]["entity_type"], rows[0]["entity_id"]) == ("npi", "1234567893")
        assert set(rows[0]["canonical_network_ids"]) == set(context.network_ids)
    assert await _physical_addresses(context, manifest, (2147483647,)) == []


def _assert_complete_report(context, targets, report):
    assert len(report["targets"]) == report["totals"]["ledger_targets"] == len(targets) == 3
    assert report["totals"]["mapped_targets"] == report["totals"]["directory_available"] == 2
    assert report["totals"]["exact_membership_count"] == 2
    assert report["totals"]["unresolved_location_count"] == 0
    assert report["totals"]["pricing_evidence_count"] is None
    evidence = report["provenance"]["target_evidence"]
    resolved_targets = [row for row in evidence if row["mapping_status"] == "resolved"]
    assert {row["network_id"] for row in resolved_targets} == set(context.network_ids)
    for row in resolved_targets:
        assert row["exact_membership_count"] == 1 and row["serving_integrity_valid"]
        assert not row["duplicated_join_or_office"] and not row["provider_identity_conflict"]
        assert row["orphan_selected_binding_count"] == row["orphan_projected_addresses"] == 0
    assert len([row for row in evidence if row["mapping_status"] == "missing"]) == 1


async def test_authenticated_aliases_share_one_compact_address_in_complete_target_coverage(alias_sources_db):
    context = alias_sources_db
    targets = await _review_alias_targets(context)
    manifest = await _publish(context, await _compose(context, context.revision), first=True)
    assert manifest.compact_layout is not None
    assert (
        await context.fixture.connection.fetchval(
            "SELECT to_regclass($1)", manifest.schema_name + ".entity_address_unified"
        )
        is None
    )
    async with coverage_reader(context):
        before = await _durable_state(context.fixture)
        await _assert_exact_alias_rows(context, manifest)
        report = await read_compact_coverage(context, manifest.generation_id)
        _assert_complete_report(context, targets, report)
        assert await _durable_state(context.fixture) == before
