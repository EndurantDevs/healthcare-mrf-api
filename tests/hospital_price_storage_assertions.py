# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reusable PostgreSQL assertions for hospital-price storage tests."""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
import sqlalchemy as sa

from process import hospital_price_store


_REIMPORT_STAGE = '"hospital_price_reimport_stage"'


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class _StoreConnection:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def all(self, statement: str, **values):
        return (await self.connection.execute(sa.text(statement), values)).all()

    async def scalar(self, statement: str, **values):
        return (await self.connection.execute(sa.text(statement), values)).scalar()

    async def status(self, statement: str, **values) -> int:
        return (await self.connection.execute(sa.text(statement), values)).rowcount


async def _insert_reimport_attempt(
    connection,
    quoted: str,
    attempt_id: str,
    expected_generation: int,
) -> None:
    await connection.execute(sa.text(
        f"INSERT INTO {quoted}.hospital_price_import_attempt ("
        "attempt_id, hospital_id, locator_id, locator_observation_id, "
        "registry_version, requested_source_url, expected_generation, status, "
        "lease_owner, heartbeat_at, lease_expires_at) VALUES ("
        ":attempt_id, 'hospital-a', 'locator-1', 'observation-1', 1, "
        "'https://hospital.example/prices.json', :expected_generation, 'running', "
        "'hospital-prices:test', clock_timestamp(), "
        "clock_timestamp() + interval '5 minutes')"
    ), {"attempt_id": attempt_id, "expected_generation": expected_generation})
    await connection.execute(sa.text(
        f"UPDATE {quoted}.hospital_price_current SET latest_attempt_id=:attempt_id "
        "WHERE hospital_id='hospital-a'"
    ), {"attempt_id": attempt_id})


async def _create_reimport_stage(connection) -> None:
    await connection.execute(sa.text(
        "CREATE TEMP TABLE hospital_price_reimport_stage ("
        "hospital_id varchar(64), attempt_id varchar(64), expected_generation bigint, "
        "source_location_ordinal integer, final_source_url text, "
        "source_http_status integer, ein varchar(9)) ON COMMIT DROP"
    ))


async def _replace_stage_attempt(
    connection, attempt_id: str, expected_generation: int, ein: str
) -> None:
    await connection.execute(sa.text("TRUNCATE hospital_price_reimport_stage"))
    await connection.execute(sa.text(
        "INSERT INTO hospital_price_reimport_stage VALUES ("
        "'hospital-a', :attempt_id, :expected_generation, 0, "
        "'https://cdn.hospital.example/prices.json', 200, :ein)"
    ), {
        "attempt_id": attempt_id,
        "expected_generation": expected_generation,
        "ein": ein,
    })


async def _bind_and_publish(
    connection, version_id: str, content_sha: str
) -> tuple[int, int, int]:
    adapter = _StoreConnection(connection)
    await hospital_price_store._bind_evidence(
        adapter, _REIMPORT_STAGE, version_id, content_sha, 1
    )
    return await hospital_price_store._cas_publish(
        adapter, _REIMPORT_STAGE, version_id
    )


async def _current_state(connection, quoted: str, fields: str) -> dict[str, Any]:
    return dict((await connection.execute(sa.text(
        f"SELECT {fields} FROM {quoted}.hospital_price_current "
        "WHERE hospital_id='hospital-a'"
    ))).mappings().one())


async def _prove_unchanged_publication(
    connection, quoted: str, content_sha: str, version_id: str, fields: str
) -> dict[str, Any]:
    state_before_by_field = await _current_state(connection, quoted, fields)
    await _insert_reimport_attempt(connection, quoted, "attempt-unchanged", 1)
    await _create_reimport_stage(connection)
    await _replace_stage_attempt(
        connection, "attempt-unchanged", 1, "009876543"
    )
    assert await _bind_and_publish(connection, version_id, content_sha) == (0, 0, 1)
    state_after_by_field = await _current_state(connection, quoted, fields)
    retained_fields = set(state_after_by_field) - {"tax_identity_count"}
    assert {
        field_name: state_after_by_field[field_name] for field_name in retained_fields
    } == {
        field_name: state_before_by_field[field_name] for field_name in retained_fields
    }
    assert (
        state_after_by_field["tax_identity_count"]
        == state_before_by_field["tax_identity_count"] + 1
    )
    assert await connection.scalar(sa.text(
        f"SELECT status FROM {quoted}.hospital_price_import_attempt "
        "WHERE attempt_id='attempt-unchanged'"
    )) == "unchanged"
    assert await connection.scalar(sa.text(
        f"SELECT count(*) FROM {quoted}.hospital_price_version"
    )) == 1
    return state_after_by_field


async def _prove_superseded_cleanup(
    connection,
    quoted: str,
    content_sha: str,
    version_id: str,
    unchanged_state_by_field: dict[str, Any],
) -> None:
    await _insert_reimport_attempt(connection, quoted, "attempt-superseded", 0)
    await _replace_stage_attempt(
        connection, "attempt-superseded", 0, "008765432"
    )
    assert await _bind_and_publish(connection, version_id, content_sha) == (0, 1, 0)
    assert await connection.scalar(sa.text(
        f"SELECT count(*) FROM {quoted}.hospital_price_hospital_tax_identity "
        "WHERE tin_value='008765432'"
    )) == 0
    assert await connection.scalar(sa.text(
        f"SELECT tax_identity_count FROM {quoted}.hospital_price_current "
        "WHERE hospital_id='hospital-a'"
    )) == unchanged_state_by_field["tax_identity_count"]


async def prove_unchanged_reimport(
    engine, schema: str, content_sha: str, version_id: str
) -> None:
    """Prove unchanged publication and superseded-evidence cleanup atomically."""

    quoted = _quote(schema)
    preserved_fields = (
        "version_id, generation, published_attempt_id, service_count, charge_count, "
        "payer_charge_count, npi_count, tax_identity_count, last_success_at"
    )
    async with engine.begin() as connection:
        unchanged_state_by_field = await _prove_unchanged_publication(
            connection, quoted, content_sha, version_id, preserved_fields
        )
        await _prove_superseded_cleanup(
            connection, quoted, content_sha, version_id, unchanged_state_by_field
        )


async def _assert_v3_metadata(connection, quoted: str) -> None:
    """Verify optional metadata and ordered header arrays survive exactly."""
    header = await connection.fetchrow(
        f"SELECT count(*) FILTER (WHERE license_number IS NOT NULL) AS licenses, "
        "array_agg(state ORDER BY license_ordinal) AS states "
        f"FROM {quoted}.hospital_price_version_license"
    )
    assert dict(header) == {"licenses": 2, "states": ["CA", "NV"]}
    locations = await connection.fetch(
        f"SELECT location_name, hospital_address "
        f"FROM {quoted}.hospital_price_version_location "
        "ORDER BY location_ordinal"
    )
    assert [tuple(location) for location in locations] == [
        ("Hospital A", "Address A"), ("Hospital B", None),
    ]
    raw_npis = await connection.fetch(
        f"SELECT npi_ordinal, npi FROM {quoted}.hospital_price_version_npi "
        "ORDER BY npi_ordinal"
    )
    assert [tuple(npi) for npi in raw_npis] == [
        (0, "0000000001"),
        (1, "0000000002"),
        (2, "taxonomy-not-an-npi"),
    ]
    financial_aid = await connection.fetchval(
        f"SELECT financial_aid_policy FROM {quoted}.hospital_price_version"
    )
    assert financial_aid.endswith("/financial-aid")
    provisions = await connection.fetch(
        f"SELECT payer_name, plan_name, provisions "
        f"FROM {quoted}.hospital_price_contract_provision "
        "ORDER BY provision_ordinal"
    )
    assert [tuple(provision) for provision in provisions] == [
        (None, None, "Default contract provision"), ("Payer", "Plan", "Plan-specific provision"),
    ]
    assert await connection.fetchval(
        f"SELECT billing_class FROM {quoted}.hospital_price_charge"
    ) == "professional"


async def _assert_publication_evidence(connection, quoted: str) -> None:
    shared = await connection.fetchrow(
        f"SELECT count(DISTINCT hospital.locator_id) AS locators, "
        "count(DISTINCT binding.hospital_id) AS hospitals "
        f"FROM {quoted}.hospital_price_hospital hospital "
        f"JOIN {quoted}.hospital_price_version_hospital binding ON true"
    )
    assert dict(shared) == {"locators": 1, "hospitals": 2}
    public_npis = await connection.fetch(
        f"SELECT source_ordinal, npi FROM {quoted}.hospital_price_hospital_npi "
        "WHERE hospital_id='hospital-a' ORDER BY source_ordinal"
    )
    assert [tuple(npi) for npi in public_npis] == [
        (0, "0000000001"),
        (1, "0000000002"),
    ]
    publication = await connection.fetchrow(
        f"SELECT current.npi_count, binding.source_location_ordinal "
        f"FROM {quoted}.hospital_price_current current "
        f"JOIN {quoted}.hospital_price_version_hospital binding "
        "USING (hospital_id, version_id) WHERE current.hospital_id='hospital-a'"
    )
    assert dict(publication) == {
        "npi_count": 2,
        "source_location_ordinal": None,
    }
    await _assert_v3_metadata(connection, quoted)
    observation = await connection.fetchrow(
        f"SELECT registry_version, requested_url, final_url, result_status, http_status "
        f"FROM {quoted}.hospital_price_locator_observation"
    )
    assert dict(observation) == {
        "registry_version": 1,
        "requested_url": "https://hospital.example/cms-hpt.txt",
        "final_url": "https://www.hospital.example/cms-hpt.txt",
        "result_status": "redirected_verified",
        "http_status": 200,
    }
    unbound = await connection.fetchval(
        f"SELECT facility_anchor_id IS NULL FROM {quoted}.hospital_price_hospital "
        "WHERE hospital_id='hospital-unbound'"
    )
    assert unbound is True


async def assert_lossless_values(connection, quoted: str) -> None:
    """Verify identifiers, typed values, and provenance survived persistence."""
    exact = await connection.fetchval(
        f"SELECT hospital_id FROM {quoted}.hospital_price_hospital_tax_identity "
        "WHERE tin_type=$1 AND tin_value=$2",
        "ein",
        "001234567",
    )
    inexact = await connection.fetchval(
        f"SELECT hospital_id FROM {quoted}.hospital_price_hospital_tax_identity "
        "WHERE tin_type=$1 AND tin_value=$2",
        "EIN",
        "1234567",
    )
    decimal_and_count = await connection.fetchrow(
        f"SELECT standard_charge_percentage::text AS percentage, allowed_count "
        f"FROM {quoted}.hospital_price_payer_charge"
    )
    assert exact == "hospital-a" and inexact is None
    assert dict(decimal_and_count) == {
        "percentage": "70.5001",
        "allowed_count": "1 through 10",
    }
    modifier_evidence = await connection.fetchrow(
        f"SELECT modifier.additional_generic_notes, "
        "payer.standard_charge_percentage::text AS percentage "
        f"FROM {quoted}.hospital_price_modifier modifier "
        f"JOIN {quoted}.hospital_price_modifier_payer payer "
        "USING (version_id, modifier_ordinal)"
    )
    assert dict(modifier_evidence) == {
        "additional_generic_notes": "modifier generic note",
        "percentage": "93.7501",
    }
    await _assert_publication_evidence(connection, quoted)


async def assert_bad_allowed_count_rejected(connection, quoted, version_id) -> None:
    """Verify storage rejects invalid count and NPI evidence rows."""

    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"INSERT INTO {quoted}.hospital_price_payer_charge VALUES ("
            "$1, 0, 0, 1, 'Payer', 'Bad Plan', 'other', NULL, NULL, "
            "'algorithm', 50, 40, 60, '10', 'invalid count')",
            version_id,
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await connection.execute(
            f"INSERT INTO {quoted}.hospital_price_hospital_npi VALUES "
            "('hospital-b', $1, 99, '0000000099', 'facility')",
            version_id,
        )
