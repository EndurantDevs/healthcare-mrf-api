# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native typed producer/checker parity on canonical synthetic plan-tier evidence."""

import json
import os
from copy import deepcopy
from datetime import date
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_fhir_typed_source as typed
from process.network_fhir_plan_tier_graph import plan_tier_source_projection
from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_fhir_source_witness_contract import SourceWitnessOrigin
from process.network_fhir_typed_contract import ADDRESS_FIELDS, CELL_COLUMNS, KINDS, PAIR_COLUMNS
from process.network_fhir_typed_mapping import _approved_graph_keys
from process.network_fhir_typed_sql import PLAN_TIER_PROVIDER_ONLY_SQL
from process.registry_fhir_plan_tier import source_key
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_network_fhir_source_witness_contract import origin_fields
from tests.test_registry_fhir_plan_tier_bindings import tier_document
from tests.test_registry_fhir_plan_tier_pipeline import plan_tier_rows


@pytest.fixture
async def native_connection():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        async with connection.transaction():
            yield connection
    finally:
        await connection.close()


def _wire(source_rows):
    return [
        {
            "resource_type": source_row["resource_type"],
            "resource_id": source_row["resource_id"],
            "payload_hash": source_row["payload_hash"],
            "payload_json": json.dumps(source_row["payload_json"]),
            "site_id": str(registry_location_id(UHC_PROVIDER_FILE_SOURCE_ID, source_row["resource_id"]))
            if source_row["resource_type"] == "Location"
            else None,
        }
        for source_row in source_rows
        if source_row["resource_type"] in KINDS
    ]


async def _project(connection, stage, source_rows, *, checker=False, plan_tiers=True):
    async with plan_tier_source_projection(connection, table_name=stage.names.get("plans")) as projection:
        for source_row in sorted(
            source_rows, key=lambda source_row: (source_row["resource_type"], source_row["resource_id"])
        ):
            wire = (
                await projection.prepare((source_row,), _wire((source_row,)), include_references=checker)
                if plan_tiers
                else _wire((source_row,))
            )
            if checker:
                await typed._check_page(
                    connection, stage, wire, "https://example.test/fhir", date(2026, 1, 1), plan_tiers=plan_tiers
                )
            else:
                await typed._project_page(connection, stage, wire, plan_table=projection.table if plan_tiers else None)
    # This pair-query component uses one fixed synthetic canonical office; canonicalization is unchanged.
    await connection.execute(
        "INSERT INTO "
        + stage.relation("addresses")
        + " SELECT "
        + typed._raw_key("l.")
        + ","
        + ",".join("l." + field for field in ADDRESS_FIELDS)
        + ",$1::uuid,'synthetic|street' FROM "
        + stage.relation("cells")
        + " l WHERE l.resource_type='Location'",
        UUID(int=91),
    )
    await typed._pairs(
        connection,
        stage,
        SimpleNamespace(source_uri="https://example.test/fhir/"),
        date(2026, 1, 1),
        checker=checker,
        plan_tiers=plan_tiers,
    )


@pytest.mark.asyncio
async def test_canonical_plan_tier_maps_exact_provider_office_and_independent_checker(native_connection):
    connection = native_connection
    source_rows = plan_tier_rows(1, "dataset-one")
    before = deepcopy(source_rows)
    async with typed._workspace(connection) as producer, typed._workspace(connection) as checker:
        await _project(connection, producer, source_rows)
        await _project(connection, checker, source_rows, checker=True)
        await typed._same_rows(
            connection,
            producer.relation("cells"),
            checker.relation("cells"),
            CELL_COLUMNS,
            ("resource_type", "resource_id"),
        )
        await typed._same_rows(
            connection,
            producer.relation("pairs"),
            checker.relation("pairs"),
            PAIR_COLUMNS,
            ("resource_type", "resource_id", "network_source_key", "location_resource_id"),
        )
        pair = await connection.fetchrow("SELECT * FROM " + producer.relation("pairs"))
        assert (pair["network_source_key"], pair["provider_id"], pair["location_id"]) == (
            source_key(tier_document()),
            "1234567893",
            registry_location_id(UHC_PROVIDER_FILE_SOURCE_ID, "office-one"),
        )
        assert await connection.fetchval("SELECT count(*) FROM " + producer.relation("pairs")) == 1
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM " + producer.relation("cells") + " WHERE resource_type='Organization')"
        )
        assert source_rows == before
        await connection.execute("UPDATE " + producer.relation("pairs") + " SET provider_id='1999999999'")
        with pytest.raises(ValueError, match="raw_graph_differs"):
            await typed._same_rows(
                connection,
                producer.relation("pairs"),
                checker.relation("pairs"),
                PAIR_COLUMNS,
                ("resource_type", "resource_id", "network_source_key", "location_resource_id"),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("serialized", (False, True))
async def test_location_writer_stays_within_the_encoded_source_page(native_connection, serialized):
    origin = SourceWitnessOrigin(**(origin_fields() | {"dataset_id": "x"}))
    payload = {"first_line": 'Rue de l’Église "Nord"', "note": "東京" * 4096}
    page = (
        {
            "dataset_id": origin.dataset_id,
            "resource_type": "Location",
            "resource_id": "site",
            "payload_hash": "a" * 64,
            "payload_json": json.dumps(payload, ensure_ascii=False) if serialized else payload,
            "acquired_resource_sha256": None,
        },
    )
    source_size = len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode())
    wire_rows, _ = typed._page_rows(page, origin, {"Location": 0}, None)

    async def execute(query, encoded_rows, kinds):
        assert len(encoded_rows.encode()) == source_size
        return await native_connection.execute(query, encoded_rows, kinds)

    async with typed._workspace(native_connection) as stage:
        await typed._project_page(SimpleNamespace(execute=execute), stage, wire_rows)
        assert (
            await native_connection.fetchval("SELECT first_line FROM " + stage.relation("cells"))
            == payload["first_line"]
        )


@pytest.mark.asyncio
async def test_plan_tier_checker_rejects_inactive_organization_network(native_connection):
    source_rows = plan_tier_rows(1, "dataset-one")
    for row in source_rows:
        if row["resource_type"] == "PractitionerRole":
            row["payload_json"]["network_refs"] = ["Organization/current", "Organization/inactive"]
        if row["resource_type"] == "InsurancePlan":
            row["payload_json"]["network_refs"] = [
                "Organization/current",
                "Organization/plan-only",
                "Organization/plan-only",
            ]
    for identity in ("current", "inactive", "plan-only"):
        source_rows.append(
            {
                "resource_type": "Organization",
                "resource_id": identity,
                "payload_json": {"active": identity != "inactive"},
                "payload_hash": "a" * 64,
            }
        )
    for checker in (False, True):
        async with typed._workspace(native_connection) as stage:
            await _project(native_connection, stage, source_rows, checker=checker)
            keys = await native_connection.fetch("SELECT network_source_key FROM " + stage.relation("pairs"))
            assert sorted(row["network_source_key"] for row in keys) == [
                source_key(tier_document()),
                "current",
                "plan-only",
            ]


@pytest.mark.asyncio
async def test_provider_only_tier_is_retained_and_legacy_v3_stays_unchanged(native_connection):
    connection = native_connection
    source_rows = plan_tier_rows(1, "dataset-one")
    for source_row in source_rows:
        if source_row["resource_type"] == "PractitionerRole":
            source_row["payload_json"]["location_refs"] = []
    async with typed._workspace(connection) as current, typed._workspace(connection) as legacy:
        await _project(connection, current, source_rows)
        await _project(connection, legacy, source_rows, plan_tiers=False)
        role = next(source_row for source_row in source_rows if source_row["resource_type"] == "PractitionerRole")
        query = PLAN_TIER_PROVIDER_ONLY_SQL.replace("{cells}", current.relation("cells"))
        providers = await connection.fetch(
            query,
            "https://example.test/fhir",
            date(2026, 1, 1),
            "PractitionerRole",
            "",
            "PractitionerRole",
            role["resource_id"],
        )
        assert [(source_row["network_source_key"], source_row["provider_id"]) for source_row in providers] == [
            (source_key(tier_document()), "1234567893")
        ]
        assert await connection.fetchval("SELECT count(*) FROM " + legacy.relation("pairs")) == 0
        assert (
            await connection.fetchval(
                "SELECT network_refs FROM " + legacy.relation("cells") + " WHERE resource_type='PractitionerRole'"
            )
            == []
        )


@pytest.mark.asyncio
async def test_precreated_plan_scratch_keeps_raw_checker_read_only(native_connection):
    connection = native_connection
    async with typed._workspace(connection, plan_tiers=True) as checker:
        snapshot = connection.transaction()
        await snapshot.start()
        try:
            await connection.execute("SET LOCAL transaction_read_only=on")
            await _project(connection, checker, plan_tier_rows(1, "dataset-one"), checker=True)
            assert await connection.fetchval("SHOW transaction_read_only") == "on"
            assert await connection.fetchval("SELECT count(*) FROM " + checker.relation("pairs")) == 1
            assert await connection.fetchval("SELECT count(*) FROM " + checker.relation("plans")) == 0
        finally:
            await snapshot.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing-plan", "plan-mismatch", "forged-tier", "cross-plan", "namespace"])
async def test_changed_plan_or_tier_cannot_produce_graph(native_connection, damage):
    source_rows = plan_tier_rows(1, "dataset-one")
    if damage == "missing-plan":
        source_rows = [source_row for source_row in source_rows if source_row["resource_type"] != "InsurancePlan"]
    for source_row in source_rows:
        payload = source_row["payload_json"]
        if damage == "plan-mismatch" and source_row["resource_type"] == "InsurancePlan":
            payload["plan_json"]["plan_key"] = tier_document(year=2027)["plan_scope"]
        if source_row["resource_type"] != "PractitionerRole":
            continue
        if damage == "forged-tier":
            payload["network_tier"] = "forged"
        if damage == "cross-plan":
            payload["insurance_plan_refs"] = ["InsurancePlan/another-plan"]
        if damage == "namespace":
            payload["network_refs"] = ["UHCPlanTier/" + "f" * 64]
    async with typed._workspace(native_connection) as stage:
        with pytest.raises(ValueError):
            await _project(native_connection, stage, source_rows)
        assert await native_connection.fetchval("SELECT count(*) FROM " + stage.relation("pairs")) == 0


@pytest.mark.asyncio
async def test_native_catalog_tier_scope_is_validated_before_mapping(native_connection):
    connection = native_connection
    table = "tier_bindings_" + uuid4().hex
    tier, generation = tier_document(), "a" * 64
    binding_by_field = {
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "producer_id": "registry-fhir-catalog-v1",
        "source_key": source_key(tier),
        "source_scope_json": {**tier, "source_generation": generation},
    }
    await connection.execute('CREATE TEMP TABLE "' + table + '"(record_key text,record_json jsonb) ON COMMIT DROP')
    await connection.execute(
        'INSERT INTO "' + table + '" VALUES($1,$2::jsonb)', str(uuid4()), json.dumps(binding_by_field)
    )
    bindings = 'approved_network_bindings AS (SELECT * FROM pg_temp."' + table + '" WHERE $1::bigint>=0'
    bindings += "".join(" AND $" + str(index) + "::text IS NOT NULL" for index in range(2, 8)) + ")"
    arguments = (
        7,
        "fhir",
        UHC_PROVIDER_FILE_SOURCE_ID,
        "source_payload",
        "dataset",
        "registry-fhir-catalog-v1",
        "dataset",
        generation,
    )
    assert await _approved_graph_keys(connection, bindings, arguments, plan_tiers=True) == [source_key(tier)]
    with pytest.raises(ValueError, match="approved_catalog_scope"):
        await _approved_graph_keys(connection, bindings, arguments)
    missing_scope_by_field = {
        key: field_value for key, field_value in binding_by_field.items() if key != "source_scope_json"
    }
    await connection.execute('UPDATE "' + table + '" SET record_json=$1::jsonb', json.dumps(missing_scope_by_field))
    with pytest.raises(ValueError, match="approved_catalog_scope"):
        await _approved_graph_keys(connection, bindings, arguments, plan_tiers=True)
    binding_by_field["source_scope_json"]["network_tier"] = "forged"
    await connection.execute('UPDATE "' + table + '" SET record_json=$1::jsonb', json.dumps(binding_by_field))
    with pytest.raises(ValueError):
        await _approved_graph_keys(connection, bindings, arguments, plan_tiers=True)
