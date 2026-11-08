# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native normalized NPI parity and sparse final coverage refusal."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process import provider_directory_profile as profile
from process import provider_directory_profile_compact_inventory as inventory
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_profile_affiliations_db import _build_profile_artifacts, _profile_database


@pytest.mark.asyncio
async def test_native_composite_aligns_reordered_fields_and_preserves_integer_arrays(monkeypatch):
    """The projected order can differ from a migrated composite without changing any value."""
    async with _delta_database(monkeypatch) as (database, schema):
        native_type = profile.qualified_table(schema, "reordered_resource")
        await database.status("CREATE TABLE " + native_type + " (metadata jsonb,npi bigint,identities integer[])")
        await database.status(
            "INSERT INTO "
            + native_type
            + " VALUES (jsonb_build_object('retained',true),1000000004,ARRAY[7,9]::integer[])"
        )
        projected = (
            "SELECT 1234567893::bigint AS npi,jsonb_build_object('projected',true) AS metadata,"
            "ARRAY[11,13]::integer[] AS identities"
        )
        aligned = inventory._native_rows(projected, native_type)
        records = await database.all(
            "SELECT * FROM (" + aligned + ") projected UNION ALL SELECT * FROM " + native_type + " ORDER BY npi"
        )
        assert [(npi_record[0], npi_record[1], npi_record[2]) for npi_record in records] == [
            ({"retained": True}, 1000000004, [7, 9]),
            ({"projected": True}, 1234567893, [11, 13]),
        ]


def _fixture_references(schema):
    return {
        name + "_ref": profile.qualified_table(schema, model_name)
        for name, model_name in (
            ("source", "provider_directory_source"),
            ("practitioner", "provider_directory_practitioner"),
            ("role", "provider_directory_practitioner_role"),
            ("organization", "provider_directory_organization"),
            ("service", "provider_directory_healthcare_service"),
            ("endpoint", "provider_directory_endpoint"),
        )
    }


@pytest.mark.asyncio
async def test_native_inventory_matches_all_writer_evidence_and_refuses_unplanned_final_npi(monkeypatch):
    """Compare native normalization with writer results before testing the actual sparse refusal SQL."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        reference_by_name = _fixture_references(schema)
        observed = await database.all(
            profile.profile_evidence_npi_inventory_sql(**reference_by_name),
            source_ids=["profile-source-a", "profile-source-b", "profile-source-uhc"],
            dataset_ids=["profile-dataset-a", "profile-dataset-b", "profile-dataset-uhc"],
            profile_as_of="2026-07-19",
        )
        expected = await database.all(f'SELECT DISTINCT npi FROM "{schema}".profile_evidence')
        assert {npi_record[0] for npi_record in observed} == {npi_record[0] for npi_record in expected}
        assert expected
        buckets = sorted(
            {(npi_record[0] - profile.NPI_MIN) // profile.PROFILE_NPI_BATCH_SIZE for npi_record in expected}
        )
        plan = SimpleNamespace(
            has_existing_artifacts=False,
            compact_batches=tuple(
                SimpleNamespace(kind="npi", npi_start=inventory._range(bucket)[0], npi_end=inventory._range(bucket)[1])
                for bucket in buckets
            ),
        )
        build = SimpleNamespace(
            schema=schema,
            evidence_stage="profile_evidence",
            profile_stage="profile",
            affected_npi_stage="affected",
            materialization_mode="source_delta",
            source_ids=["profile-source-a"],
            removed_source_ids=["removed"],
        )
        await database.status(f'CREATE TABLE "{schema}".affected (npi bigint PRIMARY KEY)')
        directory = SimpleNamespace(
            db=database,
            _provider_directory_profile_build_ref=lambda build, name: profile.qualified_table(build.schema, name),
        )

        @asynccontextmanager
        async def transaction(_directory):
            async with database.transaction():
                yield

        monkeypatch.setattr(inventory.source, "is_source_storage_required", lambda: True)
        monkeypatch.setattr(inventory.scope_copy, "prepared_read_transaction", transaction)
        await inventory.require_complete_inventory(directory, build, plan)
        missing = next(bucket for bucket in range(inventory._BUCKET_COUNT) if bucket not in buckets)
        await database.status(f'INSERT INTO "{schema}".affected VALUES (:npi)', npi=inventory._range(missing)[0])
        with pytest.raises(RuntimeError, match="compact_inventory_changed"):
            await inventory.require_complete_inventory(directory, build, plan)
