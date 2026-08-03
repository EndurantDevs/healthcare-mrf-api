# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from api.endpoint import pricing
from db.connection import Database
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    ZCTA5_ZIP_INDEX_NAME,
    _schema_sql,
    _temporary_schema,
)


async def _create_allowed_snapshot_tables(database: Database, schema: str) -> None:
    statements = (
        f"""CREATE TABLE {schema}.ptg2_snapshot (
            snapshot_id varchar PRIMARY KEY,
            status varchar NOT NULL,
            manifest jsonb NOT NULL,
            import_run_id varchar
        )""",
        f"""CREATE TABLE {schema}.ptg2_current_source_snapshot (
            snapshot_id varchar NOT NULL,
            source_key varchar NOT NULL
        )""",
        f"""CREATE TABLE {schema}.ptg2_allowed_amount_plan (
            snapshot_id varchar NOT NULL,
            file_id bigint NOT NULL,
            plan_id varchar NOT NULL,
            plan_market_type varchar
        )""",
        f"""CREATE TABLE {schema}.ptg2_allowed_amount_item (
            snapshot_id varchar NOT NULL,
            file_id bigint NOT NULL,
            allowed_item_hash varchar NOT NULL,
            billing_code_type varchar,
            billing_code varchar NOT NULL,
            name varchar,
            description varchar
        )""",
    )
    for statement in statements:
        await database.status(statement)


async def _create_allowed_payment_tables(database: Database, schema: str) -> None:
    await database.status(
        f"""CREATE TABLE {schema}.ptg2_allowed_amount_payment (
            snapshot_id varchar NOT NULL,
            allowed_item_hash varchar NOT NULL,
            payment_hash varchar NOT NULL,
            tin_type varchar,
            tin_value varchar,
            service_code varchar[],
            billing_class varchar,
            setting varchar,
            allowed_amount numeric,
            billing_code_modifier varchar[],
            network_status varchar,
            network_semantics varchar
        )"""
    )
    await database.status(
        f"""CREATE TABLE {schema}.ptg2_allowed_amount_provider_payment (
            snapshot_id varchar NOT NULL,
            payment_hash varchar NOT NULL,
            npi bigint[] NOT NULL,
            billed_charge numeric
        )"""
    )


async def _create_allowed_provider_tables(database: Database, schema: str) -> None:
    statements = (
        f"""CREATE TABLE {schema}.npi (
            npi bigint PRIMARY KEY,
            provider_organization_name varchar,
            provider_first_name varchar,
            provider_middle_name varchar,
            provider_last_name varchar,
            provider_sex_code varchar
        )""",
        f"""CREATE TABLE {schema}.npi_taxonomy (
            npi bigint,
            healthcare_provider_taxonomy_code varchar,
            healthcare_provider_primary_taxonomy_switch varchar,
            checksum bigint
        )""",
        f"""CREATE TABLE {schema}.nucc_taxonomy (
            code varchar PRIMARY KEY,
            display_name varchar,
            classification varchar,
            specialization varchar
        )""",
    )
    for statement in statements:
        await database.status(statement)


async def _create_allowed_tables(database: Database, schema: str) -> None:
    await _create_allowed_snapshot_tables(database, schema)
    await _create_allowed_payment_tables(database, schema)
    await _create_allowed_provider_tables(database, schema)


async def _insert_allowed_snapshot(database: Database, schema: str) -> None:
    allowed_index = {
        "allowed_amount_index": {
            "contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
            "arch_version": "postgres_binary_v3",
            "storage": "postgresql",
            "snapshot_scoped": "true",
            "source_key": "synthetic-allowed",
            "current_source_key": "synthetic-allowed",
        }
    }
    await database.status(
        f"""INSERT INTO {schema}.ptg2_snapshot (
            snapshot_id, status, manifest, import_run_id
        ) VALUES (
            'synthetic-snapshot', 'published', CAST(:manifest AS jsonb),
            'ptg2:synthetic-import'
        )""",
        manifest=json.dumps(allowed_index),
    )
    await database.status(
        f"""INSERT INTO {schema}.ptg2_current_source_snapshot (
            snapshot_id, source_key
        ) VALUES ('synthetic-snapshot', 'synthetic-allowed')"""
    )
    await database.status(
        f"""INSERT INTO {schema}.ptg2_allowed_amount_plan (
            snapshot_id, file_id, plan_id, plan_market_type
        ) VALUES ('synthetic-snapshot', 1, 'TESTPLAN001', 'group')"""
    )
    await database.status(
        f"""INSERT INTO {schema}.ptg2_allowed_amount_item (
            snapshot_id, file_id, allowed_item_hash,
            billing_code_type, billing_code, name, description
        ) VALUES (
            'synthetic-snapshot', 1, 'synthetic-item',
            'CPT', '00000', 'Synthetic service', 'Synthetic test service'
        )"""
    )


async def _insert_allowed_payments(database: Database, schema: str) -> None:
    payment_rows = (
        ("payment-a", 1990000205, "133.125", "155.25"),
        ("payment-b", 1990000213, "177.375", "199.50"),
        ("payment-c", 1990000221, "211.625", "244.75"),
        ("payment-d", 1990000239, "255.875", "288.00"),
    )
    for payment_hash, provider_npi, allowed_amount, billed_charge in payment_rows:
        await database.status(
            f"""INSERT INTO {schema}.ptg2_allowed_amount_payment (
                snapshot_id, allowed_item_hash, payment_hash,
                tin_type, tin_value, service_code, billing_class, setting,
                allowed_amount, billing_code_modifier,
                network_status, network_semantics
            ) VALUES (
                'synthetic-snapshot', 'synthetic-item', :payment_hash,
                'ein', '000000000', ARRAY['11']::varchar[], 'professional',
                'office', CAST(:allowed_amount AS numeric), ARRAY[]::varchar[],
                'in_network', 'in_network_historical_allowed_amounts'
            )""",
            payment_hash=payment_hash,
            allowed_amount=allowed_amount,
        )
        await database.status(
            f"""INSERT INTO {schema}.ptg2_allowed_amount_provider_payment (
                snapshot_id, payment_hash, npi, billed_charge
            ) VALUES (
                'synthetic-snapshot', :payment_hash,
                ARRAY[:provider_npi]::bigint[], CAST(:billed_charge AS numeric)
            )""",
            payment_hash=payment_hash,
            provider_npi=provider_npi,
            billed_charge=billed_charge,
        )


async def _insert_allowed_providers(database: Database, schema: str) -> None:
    await database.status(
        f"""INSERT INTO {schema}.npi (npi, provider_organization_name)
        VALUES
            (1990000205, 'Synthetic Provider A'),
            (1990000213, 'Synthetic Provider B'),
            (1990000221, 'Synthetic Provider C'),
            (1990000239, 'Synthetic Provider D')"""
    )
    await database.status(
        f"""INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key,
            address_source_mask, address_sources, source_count, source_mask,
            type, checksum, first_line, city_name, state_name, state_code,
            postal_code, zip5, country_code, address_precision, lat, long
        ) VALUES
            ('allowed-exact-no-point', 1990000205,
             '00000000-0000-0000-0000-000000000301',
             '10000000-0000-0000-0000-000000000301',
             1, ARRAY['nppes']::varchar[], 1, 1, 'practice', 301,
             '301 SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
             '00001', '00001', 'US', 'street', NULL, NULL),
            ('allowed-nearby-point', 1990000213,
             '00000000-0000-0000-0000-000000000302',
             '10000000-0000-0000-0000-000000000302',
             1, ARRAY['nppes']::varchar[], 1, 1, 'practice', 302,
             '302 SYNTHETIC WAY', 'NEARBY CITY', 'TS', 'TS',
             '00002', '00002', 'US', 'street', 42.0, -83.18),
            ('allowed-incoherent-point', 1990000221,
             '00000000-0000-0000-0000-000000000303',
             '10000000-0000-0000-0000-000000000303',
             1, ARRAY['nppes']::varchar[], 1, 1, 'practice', 303,
             '303 SYNTHETIC WAY', 'OTHER CITY', 'OS', 'OS',
             '00003', '00003', 'US', 'street', 42.0, -83.05),
            ('allowed-outside-radius', 1990000239,
             '00000000-0000-0000-0000-000000000304',
             '10000000-0000-0000-0000-000000000304',
             1, ARRAY['nppes']::varchar[], 1, 1, 'practice', 304,
             '304 SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
             '00001', '00001', 'US', 'street', 42.65, -83.0)"""
    )
    await database.status(
        f"""INSERT INTO {schema}.npi_address (
            npi, address_key, type, checksum, date_added
        ) SELECT npi, address_key, type, checksum, '2026-07-31'::date
            FROM {schema}.entity_address_unified"""
    )


def _allowed_page_query(schema: str) -> tuple[str, dict[str, object]]:
    request_args_by_name = {
        "plan_id": "TESTPLAN001",
        "plan_market_type": "group",
        "code": "00000",
        "code_system": "CPT",
        "zip5": "00001",
        "lat": 42.0,
        "long": -83.0,
        "radius_miles": 30,
    }
    query_parameters = pricing._allowed_amount_query_params(
        request_args_by_name,
        SimpleNamespace(limit=10, offset=0),
        plan_id="TESTPLAN001",
        code="00000",
        code_system="CPT",
        npi=None,
        current_snapshots=[
            {
                "snapshot_id": "synthetic-snapshot",
                "source_key": "synthetic-allowed",
                "plan_id": "TESTPLAN001",
                "plan_market_type": "group",
            }
        ],
    )
    page_query = pricing._allowed_amount_page_sql(
        request_args_by_name,
        address_table=f"{pricing.PTG2_SCHEMA}.entity_address_unified",
        parameter_map=query_parameters,
    )
    return _schema_sql(str(page_query), schema), query_parameters


def _rate_byte_projection(page_query: str) -> str:
    return f"""SELECT total, npi,
        encode(float8send(allowed_amount_min), 'hex') AS minimum_bytes,
        encode(float8send(allowed_amount_max), 'hex') AS maximum_bytes,
        encode(float8send(allowed_amount_avg), 'hex') AS average_bytes
      FROM ({page_query}) AS allowed_page
     WHERE npi IS NOT NULL"""


def _rate_byte_control_query(schema: str) -> str:
    return f"""SELECT expanded.npi,
        encode(float8send(MIN(payment.allowed_amount::double precision)), 'hex')
            AS minimum_bytes,
        encode(float8send(MAX(payment.allowed_amount::double precision)), 'hex')
            AS maximum_bytes,
        encode(float8send(AVG(payment.allowed_amount::double precision)), 'hex')
            AS average_bytes
      FROM {schema}.ptg2_allowed_amount_payment AS payment
      JOIN {schema}.ptg2_allowed_amount_provider_payment AS provider_payment
        ON provider_payment.snapshot_id = payment.snapshot_id
       AND provider_payment.payment_hash = payment.payment_hash
      CROSS JOIN LATERAL unnest(provider_payment.npi) AS expanded(npi)
     WHERE expanded.npi = ANY(CAST(:selected_npis AS bigint[]))
     GROUP BY expanded.npi
     ORDER BY expanded.npi"""


def _rate_bytes_by_provider(rows) -> dict[int, tuple[str, str, str]]:
    return {
        int(row._mapping["npi"]): (
            row._mapping["minimum_bytes"],
            row._mapping["maximum_bytes"],
            row._mapping["average_bytes"],
        )
        for row in rows
    }


def _plan_relation_names(plan_value) -> set[str]:
    if isinstance(plan_value, dict):
        relation_names = {
            str(plan_value["Relation Name"])
            for _key in ("Relation Name",)
            if "Relation Name" in plan_value
        }
        for nested_value in plan_value.values():
            relation_names.update(_plan_relation_names(nested_value))
        return relation_names
    if isinstance(plan_value, list):
        relation_names: set[str] = set()
        for nested_value in plan_value:
            relation_names.update(_plan_relation_names(nested_value))
        return relation_names
    return set()


def _plan_index_names(plan_value) -> set[str]:
    if isinstance(plan_value, dict):
        index_names = (
            {str(plan_value["Index Name"])}
            if "Index Name" in plan_value
            else set()
        )
        for nested_value in plan_value.values():
            index_names.update(_plan_index_names(nested_value))
        return index_names
    if isinstance(plan_value, list):
        index_names: set[str] = set()
        for nested_value in plan_value:
            index_names.update(_plan_index_names(nested_value))
        return index_names
    return set()


@pytest.mark.asyncio
async def test_allowed_page_preserves_rates_and_rejects_incoherent_locations():
    async with _temporary_schema() as (database, schema):
        await _create_allowed_tables(database, schema)
        await _insert_spatial_reference_rows(database, schema)
        await _insert_allowed_snapshot(database, schema)
        await _insert_allowed_payments(database, schema)
        await _insert_allowed_providers(database, schema)
        page_query, query_parameters = _allowed_page_query(schema)

        page_rows = await database.all(
            _rate_byte_projection(page_query),
            **query_parameters,
        )
        selected_npis = sorted(
            page_record._mapping["npi"] for page_record in page_rows
        )
        assert selected_npis == [1990000205, 1990000213]
        assert {
            page_record._mapping["total"] for page_record in page_rows
        } == {2}

        control_rows = await database.all(
            _rate_byte_control_query(schema),
            selected_npis=selected_npis,
        )
        assert _rate_bytes_by_provider(page_rows) == _rate_bytes_by_provider(
            control_rows
        )

        async with database.transaction():
            await database.status("SET LOCAL enable_seqscan = off")
            query_plan = await database.scalar(
                f"EXPLAIN (FORMAT JSON, COSTS OFF) {page_query}",
                **query_parameters,
            )
        relation_names = _plan_relation_names(query_plan)
        assert "entity_address_unified" in relation_names
        assert "zcta5" in relation_names
        assert ZCTA5_ZIP_INDEX_NAME in _plan_index_names(query_plan)
