# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for dataset-scoped Flex Profile replacement."""

from __future__ import annotations

import json

import pytest

from process import provider_directory_profile as profile
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from tests.test_provider_directory_profile_affiliations_db import (
    _profile_database,
)


OFFICIAL_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
NPI = 1000000491


def _ref(schema: str, relation: str) -> str:
    return profile.qualified_table(schema, relation)


def _evidence_sql(schema: str, target: str) -> str:
    return profile.profile_evidence_insert_sql(
        target_ref=_ref(schema, target),
        source_ref=_ref(schema, "provider_directory_source"),
        practitioner_ref=_ref(schema, "provider_directory_practitioner"),
        role_ref=_ref(schema, "provider_directory_practitioner_role"),
        organization_ref=_ref(schema, "provider_directory_organization"),
        service_ref=_ref(schema, "provider_directory_healthcare_service"),
        endpoint_ref=_ref(schema, "provider_directory_endpoint"),
        dataset_resource_ref=_ref(
            schema,
            "provider_directory_dataset_resource",
        ),
    )


async def _seed_sources_and_rows(database, schema: str) -> None:
    """Seed two authorities plus selected and deliberately stale Flex rows."""

    await _seed_profile_source_rows(database, schema)
    await _seed_flex_dataset_row(database, schema)


async def _seed_profile_source_rows(database, schema: str) -> None:
    """Seed source identities and typed rows used to detect stale evidence."""
    await database.status(
        f"""
        INSERT INTO {_ref(schema, 'provider_directory_source')} (
            source_id, endpoint_id, canonical_api_base, org_name, plan_name
        ) VALUES
            (:official_source, 'official-endpoint',
             'https://files.example.test', 'Official files', NULL),
            (:flex_source, 'flex-endpoint',
             'https://directory.example.test/R4',
             'Practitioner enrichment', NULL);
        """,
        official_source=OFFICIAL_SOURCE_ID,
        flex_source=UHC_FLEX_PRACTITIONER_SOURCE_ID,
    )
    await database.status(
        f"""
        INSERT INTO {_ref(schema, 'provider_directory_practitioner')} (
            source_id, resource_id, npi, active, names, family_name,
            given_names, full_name, communications, updated_at
        ) VALUES
            (:official_source, 'official-practitioner', :npi, true,
             '[{{"text":"Shared Provider","family":"Provider",'
             '"given":["Shared"]}}]'::jsonb,
             'Provider', '["Shared"]'::jsonb, 'Shared Provider',
             '[]'::jsonb, TIMESTAMP '2026-08-09'),
            (:flex_source, 'stale-typed-practitioner', :npi, true,
             '[{{"text":"Stale Typed Flex"}}]'::jsonb,
             'Flex', '["Stale"]'::jsonb, 'Stale Typed Flex',
             '[{{"codes":[{{"code":"xx"}}]}}]'::jsonb,
             TIMESTAMP '2026-08-01');
        """,
        official_source=OFFICIAL_SOURCE_ID,
        flex_source=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        npi=NPI,
    )


async def _seed_flex_dataset_row(database, schema: str) -> None:
    """Seed the immutable normalized Practitioner selected for Profile."""

    payload_by_field = {
        "active": True,
        "communication_codes": [{"code": "es"}],
        "communications": [{"codes": [{"code": "es"}]}],
        "family_name": "Provider",
        "full_name": "Shared Provider",
        "given_names": ["Shared"],
        "identifiers": [],
        "names": [
            {
                "family": "Provider",
                "given": ["Shared"],
                "text": "Shared Provider",
            }
        ],
        "npi": NPI,
        "qualification_codes": [],
        "qualifications": [],
        "resource_id": "fresh-dataset-practitioner",
        "telecom": [{"system": "email", "value": "profile@example.test"}],
    }
    await database.status(
        f"""
        INSERT INTO {_ref(schema, 'provider_directory_dataset_resource')} (
            dataset_id, resource_type, resource_id, payload_hash, payload_json
        ) VALUES (
            'flex-dataset-v1', 'Practitioner',
            'fresh-dataset-practitioner', repeat('f', 64),
            CAST(:payload AS jsonb)
        );
        """,
        payload=json.dumps(payload_by_field, sort_keys=True),
    )


async def _build_first_generation(database, schema: str) -> None:
    await database.status(
        _evidence_sql(schema, "profile_evidence"),
        source_ids=[OFFICIAL_SOURCE_ID, UHC_FLEX_PRACTITIONER_SOURCE_ID],
        dataset_ids=["official-dataset", "flex-dataset-v1"],
        profile_as_of="2026-08-09",
    )
    await database.status(
        profile.profile_insert_sql(
            evidence_ref=_ref(schema, "profile_evidence"),
            target_ref=_ref(schema, "profile"),
            old_evidence_ref=None,
            rebuild_all=True,
        ),
        generation_id="profile-flex-v1",
        profile_as_of="2026-08-09",
    )


async def _build_replacement_without_flex_row(database, schema: str) -> None:
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            "profile_evidence_v2",
            logged=True,
        )
    )
    await database.status(
        profile.profile_table_sql(schema, "profile_v2", logged=True)
    )
    await database.status(
        profile.copy_existing_evidence_sql(
            source_ref=_ref(schema, "profile_evidence"),
            target_ref=_ref(schema, "profile_evidence_v2"),
        ),
        source_ids=[UHC_FLEX_PRACTITIONER_SOURCE_ID],
        retained_source_ids=[
            OFFICIAL_SOURCE_ID,
            UHC_FLEX_PRACTITIONER_SOURCE_ID,
        ],
        profile_as_of="2026-08-10",
    )
    await database.status(
        _evidence_sql(schema, "profile_evidence_v2"),
        source_ids=[UHC_FLEX_PRACTITIONER_SOURCE_ID],
        dataset_ids=["flex-dataset-v2"],
        profile_as_of="2026-08-10",
    )
    await database.status(
        profile.profile_insert_sql(
            evidence_ref=_ref(schema, "profile_evidence_v2"),
            target_ref=_ref(schema, "profile_v2"),
            old_evidence_ref=None,
            rebuild_all=True,
        ),
        generation_id="profile-flex-v2",
        profile_as_of="2026-08-10",
    )


@pytest.mark.asyncio
async def test_flex_profile_uses_exact_dataset_and_removes_stale_source(
    monkeypatch,
):
    async with _profile_database(monkeypatch) as (database, schema):
        await _seed_sources_and_rows(database, schema)
        await _build_first_generation(database, schema)

        flex_evidence = await database.all(
            f"""
            SELECT fact_type, value_json
              FROM {_ref(schema, 'profile_evidence')}
             WHERE source_id = :source_id AND npi = :npi
             ORDER BY fact_type, value_json::text;
            """,
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
            npi=NPI,
        )
        assert flex_evidence
        assert "Stale Typed Flex" not in json.dumps(
            [dict(evidence_row._mapping) for evidence_row in flex_evidence],
            default=str,
        )
        assert any(
            evidence_row._mapping["fact_type"] == "language"
            for evidence_row in flex_evidence
        )

        first_profile = await database.first(
            f"SELECT profile_json FROM {_ref(schema, 'profile')} "
            "WHERE npi = :npi;",
            npi=NPI,
        )
        first_json = first_profile._mapping["profile_json"]
        assert first_json["source_count"] == 2
        assert first_json["independent_source_count"] == 1
        name_item = first_json["facts"]["name"]["items"][0]
        assert name_item["source_count"] == 2
        assert name_item["independent_source_count"] == 1

        await _build_replacement_without_flex_row(database, schema)
        flex_count = await database.scalar(
            f"SELECT count(*) FROM {_ref(schema, 'profile_evidence_v2')} "
            "WHERE source_id = :source_id;",
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        )
        assert flex_count == 0
        second_profile = await database.first(
            f"SELECT profile_json FROM {_ref(schema, 'profile_v2')} "
            "WHERE npi = :npi;",
            npi=NPI,
        )
        second_json = second_profile._mapping["profile_json"]
        assert second_json["source_count"] == 1
        assert second_json["independent_source_count"] == 1
        assert "language" not in second_json["facts"]
