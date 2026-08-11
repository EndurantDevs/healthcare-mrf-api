# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for dataset-scoped Flex Profile replacement."""

from __future__ import annotations

import json

import pytest

from process import provider_directory_profile as profile
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from tests.provider_directory_profile_graph_postgres_test_support import (
    graph_payloads,
    seed_graph_typed_leak_rows,
)


OFFICIAL_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
NPI = 1000000491
GRAPH_NPI = 1000000004
OLD_GRAPH_NPI = 1000000012
FOREIGN_GRAPH_NPI = 1000000020
GRAPH_DATASET_ID = "rooted-graph-dataset-v2"
OLD_GRAPH_DATASET_ID = "rooted-graph-dataset-v1"
FOREIGN_GRAPH_DATASET_ID = "rooted-graph-dataset-foreign"


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
             'Practitioner enrichment', NULL),
            (:graph_source, 'graph-endpoint',
             'https://directory.example.test/R4',
             'Rooted graph enrichment', NULL);
        """,
        official_source=OFFICIAL_SOURCE_ID,
        flex_source=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        graph_source=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
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


def _graph_payloads(marker: str, npi: int) -> tuple[tuple[str, str, dict], ...]:
    return graph_payloads(marker, npi)


async def _seed_graph_dataset(
    database,
    schema: str,
    *,
    dataset_id: str,
    marker: str,
    npi: int,
) -> None:
    resource_payloads = _graph_payloads(marker, npi)
    for resource_type, resource_id, resource_payload in resource_payloads:
        await database.status(
            f"""
            INSERT INTO {_ref(schema, 'provider_directory_dataset_resource')} (
                dataset_id, resource_type, resource_id,
                payload_hash, payload_json
            ) VALUES (
                :dataset_id, :resource_type, :resource_id,
                repeat('d', 64), CAST(:payload AS jsonb)
            );
            """,
            dataset_id=dataset_id,
            resource_type=resource_type,
            resource_id=resource_id,
            payload=json.dumps(resource_payload, sort_keys=True),
        )
    await database.status(
        f"""
        INSERT INTO {
            _ref(
                schema,
                'provider_directory_dataset_affiliation_organization',
            )
        } (
            dataset_id, participating_organization_resource_id,
            affiliation_resource_id
        ) VALUES (
            :dataset_id, :participating_organization_resource_id,
            :affiliation_resource_id
        );
        """,
        dataset_id=dataset_id,
        participating_organization_resource_id=(f"{marker}-participating-organization"),
        affiliation_resource_id=f"{marker}-affiliation",
    )


async def _seed_graph_typed_leak_rows(database, schema: str) -> None:
    await seed_graph_typed_leak_rows(
        database,
        schema,
        _ref,
        source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        npi=GRAPH_NPI,
        dataset_id=GRAPH_DATASET_ID,
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
    await database.status(profile.profile_table_sql(schema, "profile_v2", logged=True))
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
) -> None:
    from tests.provider_directory_profile_uhc_flex_generation_postgres_cases import (
        test_flex_profile_uses_exact_dataset_and_removes_stale_source as run_case,
    )

    await run_case(monkeypatch)


@pytest.mark.asyncio
async def test_rooted_profile_reads_only_selected_graph_rows(monkeypatch) -> None:
    from tests.provider_directory_profile_uhc_flex_generation_postgres_cases import (
        test_rooted_profile_reads_only_selected_graph_rows as run_case,
    )

    await run_case(monkeypatch)


@pytest.mark.asyncio
async def test_v5_to_v6_promotion_refreshes_both_variant_npis(monkeypatch) -> None:
    from tests.provider_directory_profile_uhc_flex_generation_postgres_cases import (
        test_v5_to_v6_promotion_refreshes_both_variant_npis as run_case,
    )

    await run_case(monkeypatch)
