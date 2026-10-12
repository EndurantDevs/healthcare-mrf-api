# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the native encoder, binary COPY and durable set persistence together."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from asyncpg import CheckViolationError

from process import registry_source_admission as admission
from process.registry_source_admission import RegistrySourceAdmissionError, admit_cms_mlr_edition
from tests.registry_source_fixtures import _HEADERS as _HEADERS
from tests.registry_source_fixtures import _edition as _edition
from tests.registry_source_fixtures import _input as _input
from tests.test_network_serving_schema_postgres import serving_schema

pytest.importorskip("ptg2_address_canon")


@pytest.mark.asyncio
async def test_native_edition_identity_absent_does_not_register(serving_schema):
    connection, schema, _ = serving_schema
    async with connection.transaction():
        assert (
            await admission.read_registry_source_edition_identity(connection, _edition(_input()), control_schema=schema)
            is None
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("different_uuid", [False, True])
async def test_native_edition_identity_restores_original_context_without_completion(serving_schema, different_uuid):
    connection, schema, _ = serving_schema
    original = _edition(_input())
    provisional = replace(original, snapshot_id=uuid4()) if different_uuid else original
    async with connection.transaction():
        await admission._register_edition(connection, f'"{schema}"', original)
        await connection.execute("SELECT pg_catalog.set_config('search_path','public,pg_temp,pg_catalog',true)")
        assert (
            await admission.read_registry_source_edition_identity(connection, provisional, control_schema=schema)
            == original
        )
        assert (
            await connection.fetchval("SELECT pg_catalog.current_setting('search_path')") == "public,pg_temp,pg_catalog"
        )
        assert connection.is_in_transaction()
        assert (
            await admission.read_registry_source_admission_receipt(
                connection, original, header_sha256=admission._source_header_sha256(_HEADERS), control_schema=schema
            )
            is None
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_admission_receipt') == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("source_url", "https://example.test/other.zip"),
        ("artifact_sha256", "f" * 64),
        ("reporting_year", 2025),
        ("published_at", None),
    ],
)
async def test_native_edition_identity_refuses_immutable_drift(serving_schema, field, value):
    connection, schema, _ = serving_schema
    original = _edition(_input())
    async with connection.transaction():
        await admission._register_edition(connection, f'"{schema}"', original)
        with pytest.raises(RegistrySourceAdmissionError, match="immutable"):
            await admission.read_registry_source_edition_identity(
                connection, replace(original, snapshot_id=uuid4(), **{field: value}), control_schema=schema
            )
        assert connection.is_in_transaction() and await connection.fetchval("SELECT 1") == 1
        assert (
            await admission.read_registry_source_edition_identity(connection, original, control_schema=schema)
            == original
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "plan-finder"),
        ("edition_id", "2024-as-of-2025-09-12"),
        ("input_sha256", "f" * 64),
        ("parser_version", "cms-header-v2"),
    ],
)
async def test_native_edition_identity_does_not_alias_distinct_keys(serving_schema, field, value):
    connection, schema, _ = serving_schema
    original = _edition(_input())
    async with connection.transaction():
        await admission._register_edition(connection, f'"{schema}"', original)
        assert (
            await admission.read_registry_source_edition_identity(
                connection, replace(original, snapshot_id=uuid4(), **{field: value}), control_schema=schema
            )
            is None
        )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 1


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
