# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ledger allocation, immutable source replay and concurrent import checks."""

import asyncio
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from process.registry_identity_materialization import (
    RegistryMaterializationError,
    materialize_registry_source_identities,
)
from process.registry_source_observation_store import RegistryObservationError, persist_registry_source_observations
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_source_observation_store_postgres import (
    CountedConnection,
    _assertion,
    _bindings,
    _landing,
)

pytestmark = pytest.mark.asyncio


async def _materialize(connection, schema, landing):
    return await materialize_registry_source_identities(connection, landing, control_schema=schema)


async def _persist(connection, schema, landing):
    return await persist_registry_source_observations(connection, landing, control_schema=schema)


async def test_ein_many_filings_and_hios_create_one_company_and_reported_group(serving_schema):
    connection, schema, _ = serving_schema
    first = _assertion()
    first["raw_fields"]["group_affiliation"] = " Example  Group "
    second = _assertion(3, hios="00999", state="FL")
    second["raw_fields"]["company_name"] = "\u2003Example\tCompany\u00a0"
    second["raw_fields"]["group_affiliation"] = "Example Group"
    landing = await _landing(connection, schema, [first, second])
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
        persisted = await _persist(connection, schema, landing)
    assert totals["companies_created"] == 1 and totals["groups_created"] == 1 and totals["bindings_created"] == 3
    assert totals["company_candidates"] == 1 and totals["company_conflicts"] == 0
    assert persisted["resolved_issuers"] == 2 and persisted["resolved_groups"] == 2
    assert (
        await connection.fetchval(
            f'SELECT COUNT(DISTINCT company_id) FROM "{schema}".registry_issuer_company_assertion'
        )
        == 1
    )
    assert tuple(
        await connection.fetchrow(f'SELECT display_name,roles,aliases,revision FROM "{schema}".company_registry')
    ) == ("Example Company", ["insurer"], "[]", 1)
    assert tuple(
        await connection.fetchrow(f'SELECT group_kind,display_name FROM "{schema}".company_group_registry')
    ) == ("naic_group", "Example Group")
    assert not await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM \"{schema}\".registry_identifier_binding WHERE entity_id='00000000-0000-0000-0000-000000000000'::uuid)"
    )


async def test_ein_allocates_without_naic_company_and_never_creates_unknown_group(serving_schema):
    connection, schema, _ = serving_schema
    assertion = _assertion(group=None, status="unresolved")
    assertion["normalized_naic_company"] = None
    assertion["raw_fields"]["naic_company_code"] = ""
    assertion["raw_fields"]["naic_group_code"] = "00000"
    landing = await _landing(connection, schema, [assertion])
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
    assert totals["companies_created"] == 1 and totals["groups_created"] == 0 and totals["bindings_created"] == 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_group_registry') == 0
    assert await connection.fetchval(f'SELECT identifier_system FROM "{schema}".registry_identifier_binding') == "ein"


async def test_ambiguous_group_labels_use_neutral_head_and_sorted_aliases(serving_schema):
    connection, schema, _ = serving_schema
    assertions = [_assertion(index + 2) for index in range(3)]
    for assertion, label in zip(assertions, ["Beta Group", "Alpha Group", " Beta Group "]):
        assertion["raw_fields"]["group_affiliation"] = label
    landing = await _landing(connection, schema, assertions)
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
    assert totals["groups_created"] == 1
    group_head = await connection.fetchrow(
        f'SELECT display_name,aliases,group_kind FROM "{schema}".company_group_registry'
    )
    assert tuple(group_head) == ("NAIC group 707", '["Alpha Group", "Beta Group"]', "naic_group")


@pytest.mark.parametrize("conflict", ["legal_names", "ein_many_naic", "naic_many_ein", "parser_fact"])
async def test_source_strong_identifier_conflicts_do_not_allocate_companies(serving_schema, conflict):
    connection, schema, _ = serving_schema
    first, second = _assertion(), _assertion(3)
    if conflict == "legal_names":
        second["raw_fields"]["company_name"] = "Other Company"
    elif conflict == "ein_many_naic":
        second["normalized_naic_company"] = "00457"
        second["raw_fields"]["naic_company_code"] = "00457"
    elif conflict == "naic_many_ein":
        second["normalized_ein"] = "023456789"
        second["raw_fields"]["federal_ein"] = "023456789"
    else:
        first["status"] = "unresolved"
        first["issues"] = [{"field": "company_name", "code": "conflicting_company_names_for_ein", "rejecting": False}]
        second = first.copy()
        second["source_row_number"] = 3
        second["submission_id"] = "3"
        second["raw_fields"] = first["raw_fields"] | {"mr_submission_template_id": "3"}
    landing = await _landing(connection, schema, [first, second])
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
        await _persist(connection, schema, landing)
    assert totals["companies_created"] == 0 and totals["company_conflicts"] >= 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 0
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 2
    assert not await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM \"{schema}\".registry_identifier_binding WHERE entity_kind='company')"
    )


async def test_existing_contradictory_ein_naic_bindings_remain_unchanged(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    company_id, _ = await _bindings(connection, schema, landing.snapshot_id)
    other_id = uuid4()
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Other Manual Company',ARRAY['employer'])",
        other_id,
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".registry_identifier_binding VALUES('company','naic_company','00456',$1,$2,'review-naic')",
        other_id,
        landing.snapshot_id,
    )
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
    assert totals["companies_created"] == 0 and totals["company_conflicts"] == 1 and totals["company_gaps"] == 1
    bindings = await connection.fetch(
        f"SELECT identifier_system,entity_id FROM \"{schema}\".registry_identifier_binding WHERE entity_kind='company' ORDER BY identifier_system"
    )
    assert [tuple(binding) for binding in bindings] == [("ein", company_id), ("naic_company", other_id)]
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 2


async def test_existing_naic_binding_is_not_an_ein_allocation_fallback(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    company_id = uuid4()
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Manual Company',ARRAY['employer'])",
        company_id,
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".registry_identifier_binding VALUES('company','naic_company','00456',$1,$2,'review-naic')",
        company_id,
        landing.snapshot_id,
    )
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
    assert totals["companies_created"] == 0 and totals["company_gaps"] == 1
    assert not await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM \"{schema}\".registry_identifier_binding WHERE identifier_system='ein')"
    )


async def test_retained_replay_and_newer_source_keep_identity_and_manual_heads(serving_schema):
    connection, schema, _ = serving_schema
    old_landing = await _landing(connection, schema, [_assertion()])
    async with connection.transaction():
        await _materialize(connection, schema, old_landing)
        await _persist(connection, schema, old_landing)
    company_id = await connection.fetchval(f'SELECT company_id FROM "{schema}".company_registry')
    group_id = await connection.fetchval(f'SELECT group_id FROM "{schema}".company_group_registry')
    await connection.execute(
        f"UPDATE \"{schema}\".company_registry SET display_name='Manual Corrected',revision=9,roles=ARRAY['employer']"
    )
    await connection.execute(
        f"UPDATE \"{schema}\".company_group_registry SET display_name='Manual Parent Label',revision=8"
    )
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=7,approved_revision=5')
    async with connection.transaction():
        replay = await _materialize(connection, schema, old_landing)
    assert replay == dict(source_rows=1, companies_created=0, groups_created=0, bindings_created=0, replayed=True)
    changed = _assertion()
    changed["raw_fields"]["company_name"] = "New Source Label"
    changed["raw_fields"]["group_affiliation"] = "New Group Source Label"
    new_landing = await _landing(connection, schema, [changed], year=2025)
    async with connection.transaction():
        totals = await _materialize(connection, schema, new_landing)
        await _persist(connection, schema, new_landing)
    assert totals["companies_created"] == 0 and totals["groups_created"] == 0 and totals["bindings_created"] == 0
    assert tuple(
        await connection.fetchrow(f'SELECT company_id,display_name,revision,roles FROM "{schema}".company_registry')
    ) == (company_id, "Manual Corrected", 9, ["employer"])
    assert tuple(
        await connection.fetchrow(f'SELECT group_id,display_name,revision FROM "{schema}".company_group_registry')
    ) == (group_id, "Manual Parent Label", 8)
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (7, 5)
    assert (
        await connection.fetchval(
            f'SELECT COUNT(DISTINCT company_id) FROM "{schema}".registry_issuer_company_assertion'
        )
        == 1
    )


async def test_retained_unresolved_edition_does_not_materialize_later_bindings(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    async with connection.transaction():
        before = await _persist(connection, schema, landing)
    assert before["resolved_issuers"] == 0
    await _bindings(connection, schema, landing.snapshot_id)
    async with connection.transaction():
        replay = await _materialize(connection, schema, landing)
        persisted = await _persist(connection, schema, landing)
    assert replay["replayed"] and replay["bindings_created"] == 0
    assert persisted == before | {"replayed": True}
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_identifier_binding') == 2
    assert not await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM \"{schema}\".registry_identifier_binding WHERE identifier_system='naic_company')"
    )


async def test_changed_retained_edition_rejects_before_materialization_and_preserves_caller_write(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    async with connection.transaction():
        await _materialize(connection, schema, landing)
        await _persist(connection, schema, landing)
    await connection.execute(
        f'UPDATE "{landing.schema_name}"."{landing.table_name}" SET observation_json=jsonb_set('
        "observation_json,'{raw_fields,company_name}',to_jsonb('Changed Source'::text))"
    )
    async with connection.transaction():
        await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=7')
        with pytest.raises(RegistryObservationError, match="Previously retained"):
            await _materialize(connection, schema, landing)
        assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 7
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_identifier_binding') == 3
    assert (
        await connection.fetchval(
            f"SELECT observation_json->'raw_fields'->>'company_name' FROM \"{schema}\".registry_source_observation"
        )
        == "Example Company"
    )


async def test_missing_ein_rejected_rows_and_unsupported_labels_never_allocate_by_name(serving_schema):
    connection, schema, _ = serving_schema
    missing = _assertion(ein=None, status="unresolved", group=None)
    rejected = _assertion(3, status="rejected", group=None)
    oversized = _assertion(4, group=None)
    oversized["raw_fields"]["company_name"] = "x" * 513
    for assertion in (missing, rejected, oversized):
        assertion["company_key"] = "source-anchor-is-not-an-identity"
        assertion["raw_fields"]["company_pk"] = "opaque-source-id"
    landing = await _landing(connection, schema, [missing, rejected, oversized])
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
    assert (
        totals["companies_created"] == 0 and totals["groups_created"] == 0 and totals["unsupported_company_names"] == 1
    )
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 0


@pytest.mark.parametrize("unsupported", ["oversized_label", "too_many_aliases"])
async def test_unsupported_group_labels_leave_a_reported_gap(serving_schema, unsupported):
    connection, schema, _ = serving_schema
    assertions = [_assertion(index + 2) for index in range(101 if unsupported == "too_many_aliases" else 1)]
    for index, assertion in enumerate(assertions):
        assertion["raw_fields"]["group_affiliation"] = (
            "x" * 513 if unsupported == "oversized_label" else f"Label {index:03d}"
        )
    landing = await _landing(connection, schema, assertions)
    async with connection.transaction():
        totals = await _materialize(connection, schema, landing)
    assert totals["groups_created"] == 0 and totals["group_gaps"] == 1 and totals["unsupported_group_labels"] == 1


async def test_dangling_binding_and_outer_rollback_are_atomic(serving_schema):
    connection, schema, _ = serving_schema
    landing = await _landing(connection, schema, [_assertion()])
    with pytest.raises(RegistryMaterializationError, match="caller-owned"):
        await _materialize(connection, schema, landing)
    with pytest.raises(RuntimeError, match="rollback"):
        async with connection.transaction():
            await _materialize(connection, schema, landing)
            raise RuntimeError("caller rollback")
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 0
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_identifier_binding') == 0
    await connection.execute(
        f"INSERT INTO \"{schema}\".registry_identifier_binding VALUES('company','ein','012345678',$1,$2,'bad-ref')",
        uuid4(),
        landing.snapshot_id,
    )
    async with connection.transaction():
        with pytest.raises(RegistryMaterializationError, match="durable identity"):
            await _materialize(connection, schema, landing)
        assert await connection.fetchval("SELECT 1") == 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 0


async def test_bulk_statement_count_is_constant_for_one_and_five_thousand_rows(serving_schema):
    connection, schema, _ = serving_schema
    query_counts = []
    for row_count in (1, 5000):
        assertions = []
        for index in range(row_count):
            assertion = _assertion(index + 2, ein=f"{index + 1:09d}", group=str(index + 1))
            assertion["normalized_naic_company"] = f"{index + 1:05d}"
            assertion["raw_fields"]["naic_company_code"] = assertion["normalized_naic_company"]
            assertions.append(assertion)
        landing = await _landing(connection, schema, assertions)
        counted = CountedConnection(connection)
        async with connection.transaction():
            totals = await _materialize(counted, schema, landing)
        expected_new = row_count - (1 if row_count == 5000 else 0)
        assert totals["companies_created"] == expected_new and totals["groups_created"] == expected_new
        query_counts.append(counted.statement_count)
    assert query_counts[0] == query_counts[1] and query_counts[0] <= 20


async def test_concurrent_source_imports_share_one_durable_identity(serving_schema):
    connection, schema, _ = serving_schema
    postgres_dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    connections = [await asyncpg.connect(postgres_dsn) for _ in range(3)]
    try:
        landings = [await _landing(writer, schema, [_assertion()]) for writer in connections]

        async def transfer(writer, landing):
            async with writer.transaction():
                return await _materialize(writer, schema, landing)

        totals = await asyncio.wait_for(
            asyncio.gather(*(transfer(writer, landing) for writer, landing in zip(connections, landings))), 10
        )
        assert sum(receipt["companies_created"] for receipt in totals) == 1
        assert sum(receipt["groups_created"] for receipt in totals) == 1
        assert sum(receipt["bindings_created"] for receipt in totals) == 3
    finally:
        await asyncio.gather(*(writer.close() for writer in connections))
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_group_registry') == 1
