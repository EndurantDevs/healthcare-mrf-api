# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the native encoder, binary COPY and durable set persistence together."""

import csv
import hashlib
import io
import json
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from asyncpg import CheckViolationError

from process.registry_source_admission import RegistrySourceAdmissionError, RegistrySourceEdition, admit_cms_mlr_edition
from tests.test_network_serving_schema_postgres import serving_schema

pytest.importorskip("ptg2_address_canon")
_HEADERS = (
    "mr_submission_template_id",
    "business_state",
    "group_affiliation",
    "company_pk",
    "hios_issuer_id",
    "company_name",
    "company_address",
    "domiciliary_state",
    "naic_group_code",
    "naic_company_code",
    "federal_ein",
    "am_best_number",
    "dba_marketing_name",
    "not_for_profit",
    "created_date",
    "merge_markets_ind_small_grp",
    "fit_exempt",
)


def _input(company_name="Synthetic Company"):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=_HEADERS, lineterminator="\r\n")
    writer.writeheader()
    for index, state in enumerate(("CA", "NY")):
        writer.writerow(
            {
                "mr_submission_template_id": f"submission-{index}",
                "business_state": state,
                "group_affiliation": "Synthetic Group",
                "hios_issuer_id": f"00{123 + index}",
                "company_name": company_name,
                "naic_group_code": "00707",
                "naic_company_code": "00456",
                "federal_ein": "01-2345678",
            }
        )
    return output.getvalue().encode()


def _edition(input_bytes):
    return RegistrySourceEdition(
        uuid4(),
        "cms",
        "commercial-mlr",
        "2024",
        "https://example.test/source.zip",
        "a" * 64,
        hashlib.sha256(input_bytes).hexdigest(),
        "cms-header-v1",
        2024,
        datetime(2025, 9, 12, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_native_binary_copy_persistence_and_exact_replay(serving_schema):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    edition = _edition(input_bytes)
    async with connection.transaction():
        receipt = await admit_cms_mlr_edition(connection, input_bytes, edition, control_schema=schema)
    assert receipt["native_counts"]["input_rows"] == 2 and receipt["copy_bytes"] > 21
    observations = await connection.fetch(
        f'SELECT * FROM "{schema}".registry_source_observation ORDER BY source_row_number'
    )
    assert [observation["source_record_key"] for observation in observations] == ["row:2", "row:3"]
    assert [json.loads(observation["observation_json"])["hios"] for observation in observations] == ["00123", "00124"]
    assert all(
        json.loads(observation["observation_json"])["raw_fields"]["company_pk"] == "" for observation in observations
    )
    async with connection.transaction():
        replay = await admit_cms_mlr_edition(connection, input_bytes, edition, control_schema=schema)
    assert replay["replayed"] is True and replay["copy_sha256"] == receipt["copy_sha256"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 2
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 1
    assert receipt["identity_materialization"]["companies_created"] == 1
    assert receipt["identity_materialization"]["groups_created"] == 1
    assert receipt["resolved_issuers"] == 2 and receipt["resolved_groups"] == 2
    assert replay["identity_materialization"]["companies_created"] == 0
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_landing_%'"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_native_admission_preserves_outer_rollback_and_metadata(serving_schema):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    edition = _edition(input_bytes)
    outer = connection.transaction()
    await outer.start()
    await admit_cms_mlr_edition(connection, input_bytes, edition, control_schema=schema)
    await outer.rollback()
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 0
    async with connection.transaction():
        await admit_cms_mlr_edition(connection, input_bytes, edition, control_schema=schema)
    async with connection.transaction():
        with pytest.raises(RegistrySourceAdmissionError, match="immutable"):
            await admit_cms_mlr_edition(
                connection, input_bytes, replace(edition, reporting_year=2026), control_schema=schema
            )
    assert await connection.fetchval(f'SELECT reporting_year FROM "{schema}".registry_source_snapshot') == 2024
    bad_input = _input("Synthetic\0Company")
    async with connection.transaction():
        with pytest.raises(ValueError):
            await admit_cms_mlr_edition(connection, bad_input, _edition(bad_input), control_schema=schema)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1


@pytest.mark.asyncio
async def test_admission_checks_trusted_context_and_digest(serving_schema):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    edition = _edition(input_bytes)
    with pytest.raises(RegistrySourceAdmissionError, match="caller-owned"):
        await admit_cms_mlr_edition(connection, input_bytes, edition, control_schema=schema)
    async with connection.transaction():
        with pytest.raises(RegistrySourceAdmissionError, match="digest"):
            await admit_cms_mlr_edition(
                connection, input_bytes, replace(edition, input_sha256="b" * 64), control_schema=schema
            )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 0
    with pytest.raises(RegistrySourceAdmissionError):
        replace(edition, reporting_year=True)


@pytest.mark.asyncio
async def test_identical_input_bytes_can_describe_distinct_reporting_editions(serving_schema):
    connection, schema, _ = serving_schema
    input_bytes = _input()
    earlier = _edition(input_bytes)
    later = replace(earlier, snapshot_id=uuid4(), edition_id="2025", reporting_year=2025)
    async with connection.transaction():
        await admit_cms_mlr_edition(connection, input_bytes, earlier, control_schema=schema)
        await admit_cms_mlr_edition(connection, input_bytes, later, control_schema=schema)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 2
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 4
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".hios_issuer_registry') == 2
    with pytest.raises(CheckViolationError, match="registry_hios_id"):
        async with connection.transaction():
            await connection.execute(
                f"INSERT INTO \"{schema}\".hios_issuer_registry(hios_issuer_id,business_state) VALUES ('00000','CA')"
            )
