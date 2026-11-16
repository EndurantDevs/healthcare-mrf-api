# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source thresholds retain inferred identities and established office preference."""

import datetime
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from api.endpoint import pricing, reports
from tests.test_plan_pricing_projection_v3_differential_postgres import migrated_v3_database


async def _allowed_offices(connection, schema):
    await connection.execute(
        text(f'''CREATE TABLE "{schema}".entity_address_unified (
        npi bigint, inferred_npi bigint, type text, source_count integer, checksum bigint,
        first_line text, second_line text, state_name text, city_name text, zip5 text,
        postal_code text, country_code text, telephone_number text, fax_number text,
        phone_number text, phone_extension text, fax_number_digits text, fax_extension text,
        address_key text, lat double precision, long double precision
    )''')
    )
    await connection.execute(
        text(f'''INSERT INTO "{schema}".entity_address_unified
        (npi,inferred_npi,type,source_count,checksum,first_line,city_name,zip5) VALUES
        (NULL,101,'practice',3,1,'1 Example Street','Inferred City','10001'),
        (102,NULL,'practice',1,2,'2 Example Street','Low Support City','10002'),
        (104,105,'practice',3,3,'3 Example Street','Primary Identity City','10003')
    ''')
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3, 4])
async def test_threshold_only_allowed_amounts_match_the_coalesced_office_identity(migrated_v3_database, minimum):
    database = migrated_v3_database
    parameters_by_name = {}
    async with database.engine.begin() as connection:
        await _allowed_offices(connection, database.schema)
        join, selected, required = pricing._allowed_amount_location_sql(
            {"min_address_sources": str(minimum)},
            address_table=f'"{database.schema}".entity_address_unified' if minimum else None,
            parameter_map=parameters_by_name,
        )
        result = await connection.execute(
            text(f"""WITH provider_rollup AS (
                SELECT npi FROM (VALUES (101::bigint),(102),(103),(104),(105)) providers(npi)
            ) SELECT provider_rollup.npi, {selected}
              FROM provider_rollup {join} WHERE TRUE {required} ORDER BY provider_rollup.npi"""),
            parameters_by_name,
        )
        rows = result.mappings().all()
    expected_npis = [101, 102, 103, 104, 105] if minimum == 0 else ([101, 104] if minimum == 3 else [])
    assert [row["npi"] for row in rows] == expected_npis
    if minimum == 3:
        assert json.loads(rows[0]["address_payload"])["city"] == "Inferred City"
    if minimum == 0:
        assert all(row["address_payload"] is None for row in rows)


async def _pharmacy_context_tables(connection, schema):
    await connection.execute(
        text(f'''CREATE TABLE "{schema}".report_addresses (
        npi bigint, type text, source_count integer, checksum bigint, address_precision text,
        telephone_number text, location_key text, taxonomy_array integer[], city_name text,
        state_name text, zip5 text
    )''')
    )
    await connection.execute(
        text(f'''INSERT INTO "{schema}".report_addresses VALUES
        (101,'practice',3,1,'city',NULL,'city',ARRAY[11],'City Office','CA','10001'),
        (101,'practice',3,2,'street',NULL,'street',ARRAY[11],'Street Office','CA','10002'),
        (101,'practice',1,0,'street',NULL,'low',ARRAY[11],'Low Support Office','CA','10003')
    ''')
    )
    for table_name, columns in (
        ("nucc_taxonomy", "int_code integer,classification text"),
        ("npi", "npi bigint,provider_organization_name text,do_business_as_text text"),
        ("geo_zip_lookup", "zip_code text,county_name text"),
    ):
        await connection.execute(text(f'CREATE TABLE "{schema}".{table_name}({columns})'))
    await connection.execute(text(f'''INSERT INTO "{schema}".nucc_taxonomy VALUES(11,'Pharmacy')'''))
    await connection.execute(text(f'''INSERT INTO "{schema}".npi VALUES(101,'Sample Pharmacy',NULL)'''))
    await connection.execute(
        text(f'''INSERT INTO "{schema}".geo_zip_lookup
        VALUES('10001','City County'),('10002','Street County')''')
    )


class _ReportSession:
    def __init__(self, connection, schema):
        self.connection = connection
        self.schema = schema

    async def execute(self, statement, parameters):
        statement = text(str(statement).replace("mrf.", f'"{self.schema}".'))
        return await self.connection.execute(statement, parameters)


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3, 4])
async def test_filtered_managed_pharmacy_context_keeps_street_office_preference(
    monkeypatch, migrated_v3_database, minimum
):
    database = migrated_v3_database
    relation = f'(SELECT * FROM "{database.schema}".report_addresses)'
    monkeypatch.setattr(reports, "_is_table_available", AsyncMock(return_value=False))
    monkeypatch.setattr(reports, "_resolve_pharmacy_address_table_sql", AsyncMock(return_value=relation))
    monkeypatch.setattr(reports, "is_scoped_address_relation", lambda candidate: candidate == relation)
    monkeypatch.setattr(reports, "current_network_address_scope", lambda: object())
    monkeypatch.setattr(reports, "scoped_address_parameters", lambda parameters: parameters)
    async with database.engine.begin() as connection:
        await _pharmacy_context_tables(connection, database.schema)
        context = await reports._fetch_pharmacy_context(
            _ReportSession(connection, database.schema),
            npi=101,
            as_of=datetime.date(2026, 6, 14),
            min_address_sources=minimum,
        )
    if minimum > 3:
        assert context is None
    else:
        assert context["city"] == "Street Office"
        assert context["zip_code"] == "10002" and context["county"] == "Street County"
