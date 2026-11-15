# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounded key/NPI reads over a portable compact layout."""

import json

import pytest

from process import provider_directory_compact_edition as edition
from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_candidate as candidate
from tests.test_provider_directory_compact_payload import resource
from tests.test_provider_directory_compact_payload_postgres import storage

RESOURCES = (
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)


async def _published_tables(connection):
    for original, target in (
        ("aliases", edition.OUTPUT_ALIASES),
        ("sets", edition.REFERENCE_SETS),
        ("blocks", edition.BODY_BLOCKS),
    ):
        await connection.execute(f'ALTER TABLE compact_test."{original}" RENAME TO "{target}"')
    await connection.execute(f"CREATE TABLE compact_test.{edition.FORMAT_TABLE}(format_id text PRIMARY KEY)")
    await connection.execute(f"INSERT INTO compact_test.{edition.FORMAT_TABLE} VALUES($1)", compact.FORMAT)
    return await edition.compact_edition_tables(connection, "compact_test")


@pytest.mark.asyncio
async def test_compact_edition_keyset_and_npi_reads_preserve_payloads():
    original_resources = [
        {
            **resource(str(i), healthcare_service_refs=["HealthcareService/" + "a" * 32] * 3, npi=1000000000),
            "resource_type": resource_type,
        }
        for i, resource_type in enumerate(RESOURCES)
    ]
    async with storage() as (driver, loading):
        async with driver.transaction():
            await compact.append_compact_payload_batch(
                driver, loading, original_resources, dataset_id="dataset", batch_ordinal=1, format_id=compact.FORMAT
            )
            published = await _published_tables(driver)
        actual_resources, after = [], ("", "")
        async with driver.transaction(isolation="repeatable_read", readonly=True):
            while True:
                page = await edition.compact_edition_page(
                    driver, published, "dataset", resource_types=RESOURCES, after=after, page_rows=2, page_bytes=8192
                )
                if not page:
                    break
                actual_resources.extend(page)
                after = page[-1]["resource_type"], page[-1]["resource_id"]
            assert len(actual_resources) == len(original_resources)
            assert [resource_row["resource_type"] for resource_row in actual_resources] == list(RESOURCES)
            for expected, observed in zip(original_resources, actual_resources, strict=True):
                assert {key: observed[key] for key in expected if key != "payload_json"} == {
                    key: expected_value for key, expected_value in expected.items() if key != "payload_json"
                }
                assert json.loads(observed["payload_json"]) == expected["payload_json"]
            matching = await edition.compact_edition_page(
                driver, published, "dataset", resource_types=RESOURCES, page_bytes=8192, npi="1000000000"
            )
            assert [resource_row["resource_type"] for resource_row in matching] == ["Practitioner", "PractitionerRole"]
            assert not await edition.compact_edition_page(
                driver, published, "dataset", resource_types=RESOURCES, page_bytes=8192, npi="other"
            )


@pytest.mark.asyncio
async def test_compact_format_metadata_is_closed_on_the_native_reader():
    async with storage() as (driver, _loading):
        async with driver.transaction():
            await _published_tables(driver)
            await driver.execute(f"ALTER TABLE compact_test.{edition.FORMAT_TABLE} ADD COLUMN unexpected text")
        await driver.reload_schema_state()
        async with driver.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(candidate.GenericCandidateError, match="format_changed"):
                await edition.compact_edition_tables(driver, "compact_test")
