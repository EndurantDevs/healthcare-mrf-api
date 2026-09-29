# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact CMS role-to-provider navigation from a retained Practitioner identifier."""

import json
import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from api.provider_directory_cms_entities import _relationships
from api.provider_directory_cms_queries import cms_practitioner_npi, cms_relationship_rows
from api.provider_directory_entities_contract import DirectoryRead, directory_cursor_key
from tests.provider_directory_cms_postgres_support import cms_database, publication_summary

RELEASE = publication_summary()["source_release"]["vector_sha256"]
HASH = "b" * 64
REFERENCE = "Practitioner/practitioner-example"
NPI = "1234567893"


async def prepare_practitioner(session):
    await session.execute(
        text("""CREATE TABLE provider_directory_cms_npd_resource_witness (
        dataset_id text, source_id text, release_id text, resource_type text, resource_id text,
        normalized_payload_hash text, raw_payload_json jsonb)""")
    )
    await session.execute(
        text("""UPDATE provider_directory_dataset_resource
        SET payload_json=CAST(:payload AS json)
        WHERE resource_type='Practitioner' AND resource_id='practitioner-example'"""),
        {"payload": json.dumps({"npi": int(NPI)})},
    )
    await session.execute(
        text("""INSERT INTO provider_directory_cms_npd_resource_witness VALUES
        ('synthetic-dataset','cms-npd',:release,'Practitioner','practitioner-example',:hash,CAST(:raw AS jsonb))"""),
        {
            "release": RELEASE,
            "hash": HASH,
            "raw": json.dumps(
                {
                    "resourceType": "Practitioner",
                    "id": "practitioner-example",
                    "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": NPI}],
                }
            ),
        },
    )


async def _assert_role_profile_links(session, schema, query, generation_map):
    """Check resolved, unresolved and conflicting public role links."""
    page = await _relationships(
        session,
        schema,
        directory_cursor_key(),
        query,
        {**generation_map, "generation_id": "gen_1", "observed_at": datetime.now(timezone.utc)},
        {
            "resource_id": "role-example",
            "resource_type": "PractitionerRole",
            "period_start": None,
            "period_end": None,
        },
    )
    role_links = [role_link for role_link in page["items"] if role_link["relationship_type"] == "role-practitioner"]
    assert sorted((role_link["status"], role_link["target_id"]) for role_link in role_links) == [
        ("conflict", None),
        ("resolved", NPI),
        ("unresolved", None),
    ]
    resolved = next(role_link for role_link in role_links if role_link["status"] == "resolved")
    assert resolved["provider_npi"] == NPI
    assert resolved["provider_profile_path"] == f"/api/v1/providers/{NPI}/profile"


@pytest.mark.asyncio
async def test_role_practitioner_ledger_and_exact_profile_npi(monkeypatch):
    """Only an exact valid NPI turns a role link into provider navigation."""
    async with cms_database(monkeypatch, prepare=prepare_practitioner, seal=False) as sessions:
        async with sessions() as session, session.begin():
            await session.execute(
                text("""INSERT INTO provider_directory_cms_npd_relationship VALUES
                ('synthetic-dataset','cms-npd',:release,'PractitionerRole','role-example',:hash,:hash,
                 'practitioner',0,1,:reference,'resolved',NULL,NULL),
                ('synthetic-dataset','cms-npd',:release,'PractitionerRole','role-example',:hash,:hash,
                 'practitioner',0,2,'Practitioner/missing','unresolved',NULL,NULL),
                ('synthetic-dataset','cms-npd',:release,'PractitionerRole','role-example',:hash,:hash,
                 'practitioner',0,3,:reference,'ambiguous',NULL,NULL)"""),
                {"release": RELEASE, "hash": HASH, "reference": REFERENCE},
            )
        async with sessions() as session:
            generation_map = {"dataset_id": "synthetic-dataset", "release_id": RELEASE}
            query = DirectoryRead("practitioner-roles", "cms-npd", "relationships", None, None, 100)
            relationship_rows = await cms_relationship_rows(
                session, os.environ["HLTHPRT_DB_SCHEMA"], query, generation_map, "role-example", None
            )
            practitioner_rows = [
                relationship_row
                for relationship_row in relationship_rows
                if relationship_row["relationship_type"] == "role-practitioner"
            ]
            assert [
                (relationship_row["target_kind"], relationship_row["resolution_status"])
                for relationship_row in practitioner_rows
            ] == [("providers", "resolved"), ("providers", "unresolved"), ("providers", "ambiguous")]
            assert (
                await cms_practitioner_npi(
                    session, os.environ["HLTHPRT_DB_SCHEMA"], generation_map, practitioner_rows[0]["reference"]
                )
                == NPI
            )
            assert (
                await cms_practitioner_npi(
                    session, os.environ["HLTHPRT_DB_SCHEMA"], generation_map, "Practitioner/missing"
                )
                is None
            )
            assert (
                await cms_practitioner_npi(
                    session,
                    os.environ["HLTHPRT_DB_SCHEMA"],
                    generation_map,
                    "https://elsewhere.invalid/Practitioner/practitioner-example",
                )
                is None
            )
            await _assert_role_profile_links(session, os.environ["HLTHPRT_DB_SCHEMA"], query, generation_map)


@pytest.mark.asyncio
async def test_practitioner_npi_requires_matching_release_hash_and_unambiguous_identifier(monkeypatch):
    async with cms_database(monkeypatch, prepare=prepare_practitioner, seal=False) as sessions:
        schema = os.environ["HLTHPRT_DB_SCHEMA"]
        generation_map = {"dataset_id": "synthetic-dataset", "release_id": RELEASE}
        async with sessions() as session:
            assert await cms_practitioner_npi(session, schema, generation_map, REFERENCE) == NPI
            assert (
                await cms_practitioner_npi(session, schema, {**generation_map, "release_id": "c" * 64}, REFERENCE)
                is None
            )
            assert (
                await cms_practitioner_npi(
                    session, schema, {**generation_map, "dataset_id": "another-dataset"}, REFERENCE
                )
                is None
            )
        async with sessions() as session, session.begin():
            await session.execute(
                text("""UPDATE provider_directory_cms_npd_resource_witness
                SET normalized_payload_hash=repeat('c',64)""")
            )
        async with sessions() as session:
            assert await cms_practitioner_npi(session, schema, generation_map, REFERENCE) is None
        async with sessions() as session, session.begin():
            await session.execute(
                text("""UPDATE provider_directory_cms_npd_resource_witness
                SET normalized_payload_hash=:hash, raw_payload_json=CAST(:raw AS jsonb)"""),
                {
                    "hash": HASH,
                    "raw": json.dumps(
                        {
                            "resourceType": "Practitioner",
                            "id": "practitioner-example",
                            "identifier": [
                                {"system": "http://hl7.org/fhir/sid/us-npi", "value": NPI},
                                {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000004"},
                            ],
                        }
                    ),
                },
            )
        async with sessions() as session:
            assert await cms_practitioner_npi(session, schema, generation_map, REFERENCE) is None
