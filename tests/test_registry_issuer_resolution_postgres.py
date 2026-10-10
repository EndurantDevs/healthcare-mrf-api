# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual native issuer period selection, ambiguity and caller snapshot pinning."""

import hashlib
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest

from process.registry_issuer_resolution import (
    RegistryIssuerResolutionError,
    RegistryIssuerResolutionUnavailable,
    read_registry_issuer_resolutions,
)
from tests.test_network_serving_schema_postgres import serving_schema

pytestmark = pytest.mark.asyncio


async def _source(connection, schema, *, year=2024, retrieved=2026):
    snapshot_id = uuid4()
    digest = hashlib.sha256(snapshot_id.bytes).hexdigest()
    await connection.execute(
        f'INSERT INTO "{schema}".registry_source_snapshot '
        "(snapshot_id,source_system,source_id,edition_id,source_url,artifact_sha256,input_sha256,parser_version,reporting_year,retrieved_at) "
        "VALUES($1,'cms_mlr','example-source',$2,'https://example.org/source',$3,$3,'fixture-v1',$4,$5)",
        snapshot_id,
        f"edition-{snapshot_id.hex}",
        digest,
        year,
        datetime(retrieved, 1, 1, tzinfo=timezone.utc),
    )
    return snapshot_id


async def _company_group(connection, schema):
    company_id, group_id = uuid4(), uuid4()
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Editable draft company',ARRAY['insurer'])",
        company_id,
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_group_registry(group_id,group_kind,display_name) VALUES($1,'naic_group','Editable draft group')",
        group_id,
    )
    return company_id, group_id


async def _evidence(
    connection, schema, snapshot_id, company_id, group_id, *, issuer=123, state="CA", status="resolved"
):
    key = f"row:{uuid4().hex}"
    await connection.execute(
        f"INSERT INTO \"{schema}\".hios_issuer_registry(hios_issuer_id,business_state) VALUES($1,'CA') ON CONFLICT DO NOTHING",
        f"{issuer:05d}",
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".registry_source_observation VALUES($1,$2,1,'accepted',$3::jsonb,'[]')",
        snapshot_id,
        key,
        json.dumps(
            {"raw_fields": {"company_name": "Source legal company", "group_affiliation": "Source reported group"}}
        ),
    )
    await connection.execute(
        f'INSERT INTO "{schema}".registry_issuer_company_assertion '
        "(snapshot_id,source_record_key,hios_issuer_id,state,company_id,resolution_status) VALUES($1,$2,$3,$4,$5,$6)",
        snapshot_id,
        key,
        f"{issuer:05d}",
        state,
        company_id if status == "resolved" else None,
        status,
    )
    if company_id is not None:
        await connection.execute(
            f'INSERT INTO "{schema}".registry_company_group_assertion '
            "(snapshot_id,source_record_key,company_id,group_id,relationship_kind,resolution_status) VALUES($1,$2,$3,$4,'reported_affiliation',$5)",
            snapshot_id,
            key,
            company_id,
            group_id,
            "resolved" if group_id else "unresolved",
        )
    return key


async def _read(connection, schema, ids=(123,), **kwargs):
    return await read_registry_issuer_resolutions(connection, ids, control_schema=schema, **kwargs)


class CountedConnection:
    def __init__(self, connection):
        self.connection, self.statements = connection, 0

    def is_in_transaction(self):
        return self.connection.is_in_transaction()

    async def fetchrow(self, *args):
        self.statements += 1
        return await self.connection.fetchrow(*args)


async def test_source_labels_and_leading_zero_identity(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    snapshot_id = await _source(connection, schema)
    await _evidence(connection, schema, snapshot_id, company_id, group_id)
    async with connection.transaction(isolation="repeatable_read"):
        result = (await _read(connection, schema))[0]
        assert result["issuer_id"] == 123 and result["hios_issuer_id"] == "00123"
        assert result["reporting_year"] == 2024 and result["resolution_status"] == "resolved"
        assert result["legal_company"] == {"company_id": str(company_id), "source_labels": ["Source legal company"]}
        assert result["reported_group"] == {
            "group_id": str(group_id),
            "source_labels": ["Source reported group"],
            "relationship_kind": "reported_affiliation",
        }
        assert "Editable draft" not in json.dumps(result)
        assert result["relationship_scope"] == "source_reporting_period"
        assert (await _read(connection, schema, ("00123",)))[0] == result


async def test_reporting_year_precedes_retrieval_and_explicit_period_never_falls_back(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    for year, retrieved in ((2023, 2027), (2024, 2025)):
        await _evidence(
            connection, schema, await _source(connection, schema, year=year, retrieved=retrieved), company_id, group_id
        )
    async with connection.transaction(isolation="repeatable_read"):
        assert (await _read(connection, schema))[0]["reporting_year"] == 2024
        historical = (await _read(connection, schema, reporting_year=2023))[0]
        assert historical["reporting_year"] == 2023 and len(historical["evidence"]) == 1
        missing = (await _read(connection, schema, reporting_year=2025))[0]
        assert missing["resolution_status"] == "missing" and missing["evidence"] == []


@pytest.mark.parametrize("status", ["unresolved", "conflicting"])
async def test_latest_period_never_hides_unresolved_or_conflicting_evidence(serving_schema, status):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    await _evidence(connection, schema, await _source(connection, schema, year=2023), company_id, group_id)
    await _evidence(connection, schema, await _source(connection, schema, year=2024), None, None, status=status)
    async with connection.transaction(isolation="repeatable_read"):
        result = (await _read(connection, schema))[0]
        assert result["reporting_year"] == 2024 and result["resolution_status"] == status
        assert result["legal_company"] is None and result["reported_group"] is None
        assert len(result["evidence"]) == 1 and result["evidence"][0]["resolution_status"] == status


async def test_same_period_company_and_group_conflicts_remain_visible(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    other_company, other_group = await _company_group(connection, schema)
    for company, group in ((company_id, group_id), (other_company, other_group)):
        await _evidence(connection, schema, await _source(connection, schema), company, group)
    async with connection.transaction(isolation="repeatable_read"):
        result = (await _read(connection, schema))[0]
        assert result["resolution_status"] == result["company_resolution_status"] == "conflicting"
        assert result["group_resolution_status"] == "conflicting" and len(result["evidence"]) == 2
        assert result["legal_company"] is None and result["reported_group"] is None


@pytest.mark.parametrize("gap", ["company", "group", "group_assertion", "state", "observation"])
async def test_missing_references_and_state_mismatch_are_explicit(serving_schema, gap):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    snapshot_id = await _source(connection, schema)
    key = await _evidence(connection, schema, snapshot_id, company_id, group_id, state="NY" if gap == "state" else "CA")
    if gap in ("company", "group"):
        table, identity = (
            ("company_registry", "company_id") if gap == "company" else ("company_group_registry", "group_id")
        )
        await connection.execute(
            f'DELETE FROM "{schema}".{table} WHERE {identity}=$1', company_id if gap == "company" else group_id
        )
    elif gap == "group_assertion":
        await connection.execute(
            f'DELETE FROM "{schema}".registry_company_group_assertion WHERE snapshot_id=$1', snapshot_id
        )
    elif gap == "observation":
        await connection.execute(
            f'DELETE FROM "{schema}".registry_source_observation WHERE snapshot_id=$1', snapshot_id
        )
    async with connection.transaction(isolation="repeatable_read"):
        issuer_resolution = (await _read(connection, schema))[0]
        assert issuer_resolution["resolution_status"] == ("conflicting" if gap == "state" else "unresolved")
        issue = {
            "company": "company_reference_missing",
            "group": "group_reference_missing",
            "group_assertion": "group_assertion_missing",
            "state": "company_assertions_conflicting",
            "observation": "source_observation_missing_or_rejected",
        }[gap]
        assert issue in issuer_resolution["issues"] and issuer_resolution["evidence"][0]["source_record_key"] == key


async def test_one_and_two_hundred_issuers_use_one_statement(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    snapshot_id = await _source(connection, schema)
    for issuer in range(1, 201):
        await _evidence(connection, schema, snapshot_id, company_id, group_id, issuer=issuer)
    counted = CountedConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        assert len(await _read(counted, schema, (1,))) == 1 and counted.statements == 1
        assert len(await _read(counted, schema, tuple(range(1, 201)))) == 200 and counted.statements == 2


@pytest.mark.parametrize(
    "ids,year",
    [
        ((), None),
        ((0,), None),
        ((True,), None),
        (("123",), None),
        (("00000",), None),
        ((123, "00123"), None),
        (tuple(range(1, 202)), None),
        ((123,), 2009),
        ((123,), True),
    ],
)
async def test_invalid_selectors_do_not_read_database(serving_schema, ids, year):
    connection, schema, _ = serving_schema
    counted = CountedConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistryIssuerResolutionError):
            await _read(counted, schema, ids, reporting_year=year)
    assert counted.statements == 0


async def test_requires_caller_pinned_transaction_and_preserves_rollback(serving_schema):
    connection, schema, _ = serving_schema
    with pytest.raises(RegistryIssuerResolutionError, match="caller-owned"):
        await _read(connection, schema)
    async with connection.transaction():
        with pytest.raises(RegistryIssuerResolutionError, match="repeatable read"):
            await _read(connection, schema)
    transaction = connection.transaction(isolation="serializable")
    await transaction.start()
    company_id, group_id = await _company_group(connection, schema)
    await _evidence(connection, schema, await _source(connection, schema), company_id, group_id)
    assert (await _read(connection, schema))[0]["resolution_status"] == "resolved"
    await transaction.rollback()
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_snapshot') == 0


async def test_caller_snapshot_stays_pinned_when_new_period_commits(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    await _evidence(connection, schema, await _source(connection, schema, year=2023), company_id, group_id)
    postgres_dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://", 1)
    other = await asyncpg.connect(postgres_dsn)
    try:
        async with connection.transaction(isolation="repeatable_read"):
            pinned = (await _read(connection, schema))[0]
            await _evidence(other, schema, await _source(other, schema, year=2024), None, None, status="conflicting")
            assert (await _read(connection, schema))[0] == pinned
        async with connection.transaction(isolation="repeatable_read"):
            latest = (await _read(connection, schema))[0]
            assert latest["reporting_year"] == 2024 and latest["resolution_status"] == "conflicting"
    finally:
        await other.close()


async def test_group_conflict_does_not_invent_an_inherited_or_current_owner(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    _, other_group = await _company_group(connection, schema)
    for group in (group_id, other_group):
        await _evidence(connection, schema, await _source(connection, schema), company_id, group)
    async with connection.transaction(isolation="repeatable_read"):
        result = (await _read(connection, schema))[0]
        assert result["company_resolution_status"] == "resolved"
        assert result["resolution_status"] == result["group_resolution_status"] == "conflicting"
        assert result["legal_company"]["company_id"] == str(company_id) and result["reported_group"] is None
        assert {entry["group_assertion"]["group_id"] for entry in result["evidence"]} == {
            str(group_id),
            str(other_group),
        }
        assert all(
            entry["group_assertion"]["relationship_kind"] == "reported_affiliation" for entry in result["evidence"]
        )


async def test_undated_evidence_and_unknown_issuer_are_explicit(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    await _evidence(connection, schema, await _source(connection, schema, year=None), company_id, group_id)
    async with connection.transaction(isolation="repeatable_read"):
        result, unknown = await _read(connection, schema, (123, 999))
        assert result["reporting_year"] is None and result["resolution_status"] == "unresolved"
        assert "reporting_period_missing" in result["issues"] and len(result["evidence"]) == 1
        assert result["legal_company"] is None and result["reported_group"] is None
        assert unknown["issuer_id"] == 999 and unknown["hios_issuer_id"] == "00999"
        assert unknown["resolution_status"] == "missing" and unknown["evidence"] == []


async def test_same_identity_keeps_source_labels_separate_from_draft_edits(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    for label in ("Earlier source label", "Later source label"):
        snapshot_id = await _source(connection, schema)
        await _evidence(connection, schema, snapshot_id, company_id, group_id)
        await connection.execute(
            f'UPDATE "{schema}".registry_source_observation SET observation_json=jsonb_set('
            "observation_json,'{raw_fields,company_name}',to_jsonb($2::text)) WHERE snapshot_id=$1",
            snapshot_id,
            label,
        )
    async with connection.transaction(isolation="repeatable_read"):
        result = (await _read(connection, schema))[0]
        assert result["resolution_status"] == "resolved"
        assert result["legal_company"]["source_labels"] == ["Earlier source label", "Later source label"]
        await connection.execute(f"UPDATE \"{schema}\".company_registry SET display_name='Different editable name'")
        assert (await _read(connection, schema))[0] == result


async def test_full_period_row_bound_fails_without_truncating_conflicts(serving_schema):
    connection, schema, _ = serving_schema
    snapshot_id = await _source(connection, schema)
    await connection.execute(f"INSERT INTO \"{schema}\".hios_issuer_registry VALUES('00123','CA',now())")
    await connection.execute(
        f'INSERT INTO "{schema}".registry_issuer_company_assertion '
        "(snapshot_id,source_record_key,hios_issuer_id,state,resolution_status) "
        "SELECT $1,'row:'||ordinal,'00123','CA',CASE WHEN ordinal=5001 THEN 'conflicting' ELSE 'unresolved' END FROM generate_series(1,5001) ordinal",
        snapshot_id,
    )
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistryIssuerResolutionError, match="complete-response bound"):
            await _read(connection, schema)
        assert connection.is_in_transaction() and await connection.fetchval("SELECT 1") == 1


async def test_byte_bound_does_not_transfer_oversized_source_labels(serving_schema):
    connection, schema, _ = serving_schema
    company_id, group_id = await _company_group(connection, schema)
    snapshot_id = await _source(connection, schema)
    await _evidence(connection, schema, snapshot_id, company_id, group_id)
    await connection.execute(
        f'UPDATE "{schema}".registry_source_observation SET observation_json=jsonb_set('
        "observation_json,'{raw_fields,company_name}',to_jsonb(repeat('x',4300000)))",
    )
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistryIssuerResolutionError, match="complete-response bound"):
            await _read(connection, schema)


async def test_missing_native_prerequisites_are_sanitized(serving_schema):
    connection, schema, _ = serving_schema
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(
            RegistryIssuerResolutionUnavailable, match="Issuer source evidence is unavailable"
        ) as failure:
            await read_registry_issuer_resolutions(connection, (123,), control_schema=schema + "_absent")
    assert schema not in str(failure.value)
