# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native COPY, typed NAIC identity conflicts and complete-edition rollback."""

import json

import pytest

from process.registry_naic_source import admit_naic_edition
from tests.registry_naic_fixtures import listing_row, prepared_listings
from tests.test_network_serving_schema_postgres import serving_schema

pytest.importorskip("ptg2_address_canon")
pytestmark = pytest.mark.asyncio


async def test_three_members_keep_typed_namespaces_unknown_dates_raw_bytes_and_replay(serving_schema, tmp_path):
    connection, schema, _ = serving_schema
    health = listing_row(ADDRESS=b"Synthetic \xd3 address", **{"COMPANY CODE": "12345", "GROUP CODE": "12345"})
    other = listing_row(
        **{"COMPANY CODE": "808", "GROUP CODE": "12345", "FEIN NUMBER": "02-3456789", "COMPANY NAME": "Synthetic Other"}
    )
    prepared = prepared_listings(
        tmp_path,
        rows_by_member={
            "HLTH.csv": [health],
            "GPAL.csv": [listing_row(**{"GROUP CODE": "12345"})],
            "GPNM.csv": [health, other],
        },
    )
    async with connection.transaction():
        admission_by_field = await admit_naic_edition(connection, prepared, control_schema=schema)
        replay = await admit_naic_edition(connection, prepared, control_schema=schema)
    assert (
        admission_by_field["observations"] == 4
        and admission_by_field["issuer_assertions"] == 0
        and admission_by_field["group_assertions"] == 2
    )
    assert admission_by_field["identity_materialization"]["companies_created"] == 1
    assert admission_by_field["identity_materialization"]["groups_created"] == 1
    assert admission_by_field["identity_materialization"]["unresolved_gpnm_companies"] == 1
    assert replay["replayed"] and replay["identity_materialization"]["companies_created"] == 0
    bindings = await connection.fetch(
        f"SELECT entity_kind,identifier_system,identifier_value,entity_id FROM \"{schema}\".registry_identifier_binding WHERE identifier_value ='12345' ORDER BY entity_kind"
    )
    assert [
        (binding["entity_kind"], binding["identifier_system"], binding["identifier_value"]) for binding in bindings
    ] == [
        ("company", "naic_company", "12345"),
        ("group", "naic_group", "12345"),
    ]
    assert bindings[0]["entity_id"] != bindings[1]["entity_id"]
    assert await connection.fetchval(f'SELECT roles FROM "{schema}".company_registry') == ["insurer"]
    assert not await connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{schema}".registry_company_group_assertion WHERE valid_from IS NOT NULL OR valid_to IS NOT NULL)'
    )
    raw = await connection.fetchval(
        f"SELECT observation_json FROM \"{schema}\".registry_source_observation WHERE source_record_key='HLTH.csv:row:2'"
    )
    assert bytes.fromhex(json.loads(raw)["source_evidence"]["raw_fields_hex"]["ADDRESS"]) == b"Synthetic \xd3 address"
    assert (
        await connection.fetchval(
            f"SELECT COUNT(*) FROM \"{schema}\".registry_identifier_observation WHERE identifier_value='00808' AND resolution_status='unresolved'"
        )
        == 1
    )


@pytest.mark.parametrize("conflict", ["ein", "company", "group"])
async def test_cross_member_conflicts_retain_every_row_before_company_allocation(serving_schema, tmp_path, conflict):
    connection, schema, _ = serving_schema
    changed = {
        "ein": {"FEIN NUMBER": "02-3456789"},
        "company": {"COMPANY CODE": "808"},
        "group": {"GROUP CODE": "808"},
    }[conflict]
    prepared = prepared_listings(
        tmp_path,
        rows_by_member={"HLTH.csv": [listing_row()], "GPAL.csv": [listing_row()], "GPNM.csv": [listing_row(**changed)]},
    )
    async with connection.transaction():
        admission_by_field = await admit_naic_edition(connection, prepared, control_schema=schema)
    assert (
        admission_by_field["observations"] == 3
        and admission_by_field["identity_materialization"]["companies_created"] == 0
    )
    assert admission_by_field["identity_materialization"]["company_conflicts"] >= 1
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 3
    assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 0
    assert not await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM \"{schema}\".registry_identifier_observation WHERE entity_kind='company' AND entity_id IS NOT NULL)"
    )


async def test_last_member_refusal_rolls_back_snapshot_and_all_prior_copy(serving_schema, tmp_path):
    connection, schema, _ = serving_schema
    prepared = prepared_listings(tmp_path)
    data = prepared.members[-1].input_path.read_bytes().replace(b"GROUP NAME", b"WRONG NAME")
    prepared.members[-1].input_path.write_bytes(data)
    with pytest.raises(ValueError):
        async with connection.transaction():
            await admit_naic_edition(connection, prepared, control_schema=schema)
    for table in (
        "registry_source_snapshot",
        "registry_source_observation",
        "registry_identifier_binding",
        "company_registry",
        "company_group_registry",
    ):
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".{table}') == 0


async def test_existing_role_bearing_company_resolves_gpnm_without_manual_head_changes(serving_schema, tmp_path):
    from uuid import uuid4

    connection, schema, _ = serving_schema
    company_id = uuid4()
    await connection.execute(
        f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Synthetic Manual',ARRAY['employer'])",
        company_id,
    )
    other = listing_row(**{"COMPANY CODE": "808", "FEIN NUMBER": "02-3456789", "COMPANY NAME": "Synthetic Other"})
    prepared = prepared_listings(
        tmp_path, rows_by_member={"HLTH.csv": [listing_row()], "GPAL.csv": [listing_row()], "GPNM.csv": [other]}
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".registry_identifier_binding(entity_kind,identifier_system,identifier_value,entity_id,snapshot_id,evidence_key) VALUES('company','naic_company','00808',$1,$2,'synthetic-review')",
        company_id,
        prepared.edition.snapshot_id,
    )
    async with connection.transaction():
        admission_by_field = await admit_naic_edition(connection, prepared, control_schema=schema)
    assert admission_by_field["identity_materialization"]["unresolved_gpnm_companies"] == 0
    assert tuple(
        await connection.fetchrow(
            f'SELECT display_name,roles,revision FROM "{schema}".company_registry WHERE company_id=$1', company_id
        )
    ) == ("Synthetic Manual", ["employer"], 1)


async def test_late_admission_refusal_preserves_prior_caller_work(serving_schema, tmp_path, monkeypatch):
    from uuid import uuid4

    from process import registry_source_admission_receipt

    connection, schema, _ = serving_schema
    prepared = prepared_listings(tmp_path)
    manual_id = uuid4()
    original_search_path = await connection.fetchval("SHOW search_path")

    async def refuse_seal(*_args):
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 3
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_identifier_binding') > 0
        raise ValueError("synthetic late refusal")

    monkeypatch.setattr(registry_source_admission_receipt, "seal_registry_source_admission", refuse_seal)
    async with connection.transaction():
        await connection.execute(
            f"INSERT INTO \"{schema}\".company_registry(company_id,display_name,roles) VALUES($1,'Synthetic Prior',ARRAY['employer'])",
            manual_id,
        )
        with pytest.raises(ValueError, match="synthetic late refusal"):
            await admit_naic_edition(connection, prepared, control_schema=schema)
        assert await connection.fetchval("SHOW search_path") == original_search_path
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_snapshot') == 0
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_source_observation') == 0
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".registry_identifier_binding') == 0
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_group_registry') == 0
        assert await connection.fetchval(f'SELECT COUNT(*) FROM "{schema}".company_registry') == 1
        assert await connection.fetchval(f'SELECT company_id FROM "{schema}".company_registry') == manual_id
