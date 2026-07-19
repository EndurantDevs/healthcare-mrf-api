# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from typing import Any

import pytest

from db.connection import Database
from process import provider_directory_profile as profile
from scripts.research import provider_directory_api_evidence_db as evidence_db
from tests.test_provider_directory_profile_affiliations_db import _profile_database


def _table_ref(schema: str, table_name: str) -> str:
    return profile.qualified_table(schema, table_name)


async def _seed_existing_profile_artifacts(
    database: Database,
    schema: str,
) -> tuple[str, str]:
    """Seed retained, removed, inactive, and expired evidence."""
    old_evidence_ref = _table_ref(schema, "profile_evidence")
    old_profile_ref = _table_ref(schema, "profile")
    await database.status(
        f"""
        INSERT INTO {old_evidence_ref} (
            evidence_key, npi, fact_type, fact_key, value_json,
            source_id, endpoint_id, dataset_id, canonical_api_base,
            source_org_name, source_plan_name, resource_type,
            resource_id, role_resource_id, active, effective_start,
            effective_end, observed_at
        )
        SELECT md5(row_key), npi, 'specialty', md5(fact_key),
               jsonb_build_object('code', '207Q00000X'),
               source_id, endpoint_id, dataset_id,
               'https://payer.test/fhir', 'Example Payer', 'Example Plan',
               'PractitionerRole', row_key, row_key, active,
               '2026-01-01', effective_end, '2026-07-01'::timestamp
          FROM (VALUES
                ('keep-only', 1588616783::bigint, 'keep-fact',
                 'source-keep', 'endpoint-keep', 'dataset-keep',
                 TRUE, '2026-12-31'),
                ('shared-keep', 1234567893::bigint, 'shared-fact',
                 'source-keep', 'endpoint-keep', 'dataset-keep',
                 TRUE, '2026-12-31'),
                ('shared-drop', 1234567893::bigint, 'shared-fact',
                 'source-drop', 'endpoint-drop', 'dataset-drop',
                 TRUE, '2026-12-31'),
                ('drop-only', 1000000491::bigint, 'drop-fact',
                 'source-drop', 'endpoint-drop', 'dataset-drop',
                 TRUE, '2026-12-31'),
                ('expired', 2000000002::bigint, 'expired-fact',
                 'source-keep', 'endpoint-keep', 'dataset-keep',
                 TRUE, '2026-01-01'),
                ('inactive', 2999999990::bigint, 'inactive-fact',
                 'source-keep', 'endpoint-keep', 'dataset-keep',
                 FALSE, '2026-12-31')
          ) AS fixture_rows(
                row_key, npi, fact_key, source_id, endpoint_id,
                dataset_id, active, effective_end
          );
        """
    )
    await database.status(
        profile.profile_insert_sql(
            evidence_ref=old_evidence_ref,
            target_ref=old_profile_ref,
            old_evidence_ref=None,
            rebuild_all=True,
        ),
        generation_id="old-generation",
    )
    return old_evidence_ref, old_profile_ref


async def _create_incremental_profile_stages(
    database: Database,
    schema: str,
    suffix: str,
) -> tuple[str, str]:
    """Create isolated evidence and compact-profile stages."""
    evidence_stage = f"profile_evidence_{suffix}"
    profile_stage = f"profile_{suffix}"
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            evidence_stage,
            logged=True,
        )
    )
    await database.status(
        profile.profile_table_sql(
            schema,
            profile_stage,
            logged=True,
        )
    )
    return _table_ref(schema, evidence_stage), _table_ref(schema, profile_stage)


async def _build_incremental_profile_artifacts(
    database: Database,
    schema: str,
    old_evidence_ref: str,
    old_profile_ref: str,
    suffix: str,
    retained_source_ids: list[str],
) -> tuple[list[Any], list[Any]]:
    """Build one incremental stage and return its evidence and profiles."""
    evidence_stage_ref, profile_stage_ref = (
        await _create_incremental_profile_stages(database, schema, suffix)
    )
    profile_params_by_name = {
        "source_ids": [],
        "retained_source_ids": retained_source_ids,
        "profile_as_of": "2026-07-19",
    }
    await database.status(
        profile.copy_existing_evidence_sql(
            source_ref=old_evidence_ref,
            target_ref=evidence_stage_ref,
        ),
        **profile_params_by_name,
    )
    await database.status(
        profile.copy_unaffected_profiles_sql(
            profile_source_ref=old_profile_ref,
            evidence_source_ref=old_evidence_ref,
            evidence_stage_ref=evidence_stage_ref,
            profile_stage_ref=profile_stage_ref,
        ),
        **profile_params_by_name,
    )
    await database.status(
        profile.profile_insert_sql(
            evidence_ref=evidence_stage_ref,
            target_ref=profile_stage_ref,
            old_evidence_ref=old_evidence_ref,
            rebuild_all=False,
        ),
        generation_id=f"new-{suffix}",
        **profile_params_by_name,
    )
    evidence_rows = await database.all(
        f"SELECT npi, source_id FROM {evidence_stage_ref} "
        "ORDER BY npi, source_id;"
    )
    profile_rows = await database.all(
        f"SELECT npi, source_ids, source_count, generation_id "
        f"FROM {profile_stage_ref} ORDER BY npi;"
    )
    return evidence_rows, profile_rows


def _assert_incremental_trust_results(
    retained_evidence_rows: list[Any],
    retained_profile_rows: list[Any],
    removed_evidence_rows: list[Any],
    removed_profile_rows: list[Any],
) -> None:
    """Require deconfiguration and currentness to revoke profile facts."""
    assert [
        (evidence_row.npi, evidence_row.source_id)
        for evidence_row in retained_evidence_rows
    ] == [
        (1234567893, "source-keep"),
        (1588616783, "source-keep"),
    ]
    assert [profile_row.npi for profile_row in retained_profile_rows] == [
        1234567893,
        1588616783,
    ]
    assert retained_profile_rows[0].source_ids == ["source-keep"]
    assert retained_profile_rows[0].source_count == 1
    assert retained_profile_rows[0].generation_id == "new-retained"
    assert retained_profile_rows[1].generation_id == "old-generation"
    assert removed_evidence_rows == []
    assert removed_profile_rows == []


@pytest.mark.asyncio
async def test_incremental_profile_build_revokes_removed_and_noncurrent_evidence(
    monkeypatch,
):
    """Prove purge-only rebuilds remove stale sources, roles, and profiles."""
    async with _profile_database(monkeypatch) as (database, schema):
        old_refs = await _seed_existing_profile_artifacts(database, schema)
        retained_artifacts = await _build_incremental_profile_artifacts(
            database, schema, *old_refs, "retained", ["source-keep"]
        )
        removed_artifacts = await _build_incremental_profile_artifacts(
            database, schema, *old_refs, "removed", []
        )

    _assert_incremental_trust_results(*retained_artifacts, *removed_artifacts)


@pytest.mark.asyncio
async def test_fhir_reference_normalization_executes_in_postgresql(monkeypatch):
    """Prove bare, absolute, query, and versioned references share one ID."""
    async with _profile_database(monkeypatch) as (database, _schema):
        reference_sql = profile.fhir_reference_resource_id_sql(
            "refs.reference",
            "Organization",
        )
        reference_rows = await database.all(
            f"""
            SELECT refs.reference, {reference_sql} AS resource_id
              FROM (VALUES
                    ('org-1'),
                    ('Organization/org-1'),
                    ('https://payer.test/fhir/Organization/org-1'),
                    ('Organization/org-1/_history/7'),
                    ('https://payer.test/fhir/Organization/org-1/_history/7?_format=json#top'),
                    ('Practitioner/org-1')
              ) AS refs(reference)
             ORDER BY refs.reference;
            """
        )

    resource_id_by_reference = {
        reference_row.reference: reference_row.resource_id
        for reference_row in reference_rows
    }
    assert resource_id_by_reference["org-1"] == "org-1"
    assert resource_id_by_reference["Organization/org-1"] == "org-1"
    absolute_ref = "https://payer.test/fhir/Organization/org-1"
    versioned_ref = "Organization/org-1/_history/7"
    absolute_versioned_ref = (
        "https://payer.test/fhir/Organization/org-1/_history/7?_format=json#top"
    )
    assert resource_id_by_reference[absolute_ref] == "org-1"
    assert resource_id_by_reference[versioned_ref] == "org-1"
    assert resource_id_by_reference[absolute_versioned_ref] == "org-1"
    assert resource_id_by_reference["Practitioner/org-1"] is None


async def _create_overlay_evidence_tables(
    database: Database,
    schema: str,
) -> tuple[str, str, str]:
    """Create the narrow dataset, overlay, and evidence tables."""
    dataset_ref = _table_ref(schema, "provider_directory_endpoint_dataset")
    overlay_ref = _table_ref(schema, "provider_directory_address_overlay")
    evidence_ref = _table_ref(schema, profile.PROFILE_EVIDENCE_TABLE)
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            profile.PROFILE_EVIDENCE_TABLE,
            logged=True,
        )
    )
    await database.status(
        f"""
        CREATE TABLE {dataset_ref} (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(96),
            import_run_id varchar(96),
            is_current boolean NOT NULL,
            status varchar(32) NOT NULL,
            published_at timestamp,
            superseded_at timestamp
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {overlay_ref} (
            source_id varchar(64) NOT NULL,
            last_seen_run_id varchar(96) NOT NULL,
            npi bigint,
            phone_number varchar(64),
            address_key varchar(128),
            lat double precision,
            long double precision,
            resource_type varchar(64),
            resource_id varchar(256)
        );
        """
    )
    return dataset_ref, overlay_ref, evidence_ref


async def _insert_overlay_evidence_fixture(
    database: Database,
    dataset_ref: str,
    overlay_ref: str,
    evidence_ref: str,
) -> None:
    """Insert one current dataset, overlay witness, and evidence fact."""
    await database.status(
        f"""
        INSERT INTO {dataset_ref} VALUES (
            'dataset-current', 'profile-endpoint-1', 'run-current', NULL,
            TRUE, 'published', now(), NULL
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO {overlay_ref} VALUES (
            'profile-source-a', 'run-current', 1588616783,
            '312-555-0100', 'address-1', 41.88, -87.63,
            'PractitionerRole', 'role-a'
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO {evidence_ref} (
            evidence_key, npi, fact_type, fact_key, value_json,
            source_id, endpoint_id, dataset_id, resource_type,
            resource_id
        ) VALUES (
            md5('evidence-current'), 1588616783, 'specialty',
            md5('specialty-current'), '{{"code":"207Q00000X"}}'::jsonb,
            'profile-source-a', 'profile-endpoint-1', 'dataset-current',
            'PractitionerRole', 'role-a'
        );
        """
    )


@pytest.mark.asyncio
async def test_overlay_profile_evidence_query_executes_in_postgresql(monkeypatch):
    """Prove the current-source CTE exposes its dataset fence to sampling."""
    async with _profile_database(monkeypatch) as (database, schema):
        fixture_refs = await _create_overlay_evidence_tables(database, schema)
        await _insert_overlay_evidence_fixture(database, *fixture_refs)
        sample_sql = evidence_db.overlay_sample_sql(schema).replace(
            "$1::varchar[]",
            "CAST(:source_ids AS varchar[])",
        ).replace("$2", ":sample_limit")
        overlay_samples = await database.all(
            sample_sql,
            source_ids=["profile-source-a"],
            sample_limit=1,
        )

    assert len(overlay_samples) == 1
    assert overlay_samples[0].source_id == "profile-source-a"
    assert overlay_samples[0].npi == 1588616783
    assert overlay_samples[0].phone_number == "312-555-0100"
