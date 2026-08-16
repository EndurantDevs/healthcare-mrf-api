# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared probes for evidence-gated address alias PostgreSQL tests."""

from process.ext.address_match import AddressRecord
from tests.test_address_numeric_grid_alias_runtime_db import db


async def _insert_visible_address(
    schema: str,
    *,
    location_key: str,
    npi: int,
    address_key: str,
    formatted_address: str | None = None,
) -> None:
    await db.status(
        f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, inferred_npi, type, address_key,
            address_sources, formatted_address
        ) VALUES (
            :location_key, :npi, NULL, 'practice', CAST(:address_key AS uuid),
            ARRAY['provider_directory_fhir']::varchar[], :formatted_address
        );
        """,
        location_key=location_key,
        npi=npi,
        address_key=address_key,
        formatted_address=formatted_address,
    )


def _match_record(
    first_line: str,
    second_line: str | None,
    *,
    address_key: str | None = None,
    formatted_address: str | None = None,
    visible: bool = False,
) -> AddressRecord:
    return AddressRecord(
        npi="1234567893",
        first_line=first_line,
        second_line=second_line,
        city="Example City",
        state="TX",
        zip_code="75001",
        country="US",
        address_key=address_key,
        formatted_address=formatted_address,
        is_healthporta_visible=visible,
    )


async def _sql_evidence_rule(
    connection,
    schema: str,
    source: AddressRecord,
    target: AddressRecord,
) -> str | None:
    row = await connection.fetchrow(
        f"""
        SELECT match_rule
        FROM "{schema}".addr_evidence_alias_match_v1(
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12, $13::text::uuid
        );
        """,
        source.first_line,
        source.second_line,
        source.city,
        source.state,
        source.zip_code,
        source.country,
        target.first_line,
        target.second_line,
        target.city,
        target.state,
        target.zip_code,
        target.country,
        target.address_key,
    )
    return str(row["match_rule"]) if row else None
