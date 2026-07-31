# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL truth tests for exact artifact-scope crash recovery."""

from __future__ import annotations

import datetime
import importlib
from dataclasses import replace

import pytest

from tests.provider_directory_profile_artifact_pg_support import (
    _admission,
    _artifact_fixture,
    _capacity_geometry,
)


importer = importlib.import_module("process.provider_directory_fhir")
_CURRENT_RUN_ID = "run_" + "a" * 32
_PRIOR_RUN_ID = "run_" + "b" * 32


async def _drop_fixture_scope(fixture) -> None:
    for relation_name in fixture.relation_by_table.values():
        await fixture.database.status(
            f"DROP TABLE {importer._unscoped_qt(fixture.schema, relation_name)};"
        )


async def _create_recovery_metadata_tables(fixture) -> None:
    schema = fixture.schema
    await fixture.database.status(
        f"""
        CREATE TABLE {importer._unscoped_qt(schema, "import_run")} (
            run_id varchar(64) PRIMARY KEY,
            importer varchar(64) NOT NULL,
            status varchar(32) NOT NULL,
            finished_at timestamptz
        );
        """
    )
    await fixture.database.status(
        f"""
        CREATE TABLE {
            importer._unscoped_qt(
                schema,
                "provider_directory_profile_capacity_lease_consumption",
            )
        } (
            attestation_id varchar(64) NOT NULL,
            lease_digest varchar(64) NOT NULL,
            capacity_geometry_hash varchar(64) NOT NULL,
            executable_plan_hash varchar(64) NOT NULL,
            selection_proof_id varchar(64) NOT NULL,
            source_vector_hash varchar(64) NOT NULL,
            source_context_vector_hash varchar(64) NOT NULL,
            run_id varchar(64) PRIMARY KEY,
            build_id varchar(64) NOT NULL,
            profile_as_of varchar(10) NOT NULL
        );
        """
    )
    checkpoint_columns = ", ".join(
        f"{column_name} bigint"
        for column_name in (
            "evidence_stage_oid",
            "profile_stage_oid",
            "affected_npi_stage_oid",
            "evidence_target_oid",
            "profile_target_oid",
        )
    )
    await fixture.database.status(
        f"CREATE TABLE {importer._unscoped_qt(schema, 'provider_directory_profile_build_checkpoint')} "
        f"({checkpoint_columns});"
    )
    for table_name in (
        "provider_directory_profile_serving_generation",
        "provider_directory_profile_delta_receipt",
    ):
        await fixture.database.status(
            f"CREATE TABLE {importer._unscoped_qt(schema, table_name)} "
            "(evidence_target_oid bigint, profile_target_oid bigint);"
        )


async def _insert_owner(
    fixture,
    admission,
    *,
    run_id: str,
    status: str,
    finished_at: datetime.datetime | None,
) -> None:
    owner_fields_by_name = dict(
        importer._expected_profile_capacity_consumption(admission)
    )
    owner_fields_by_name["run_id"] = run_id
    owner_fields_by_name["attestation_id"] = (
        admission.lease.attestation_id
        if run_id == admission.run_id
        else "3" * 64
    )
    owner_fields_by_name["build_id"] = (
        admission.build_id
        if run_id == admission.run_id
        else "pdpb_" + "c" * 32
    )
    fields = tuple(owner_fields_by_name)
    await fixture.database.status(
        f"INSERT INTO {importer._unscoped_qt(fixture.schema, 'provider_directory_profile_capacity_lease_consumption')} "
        f"({', '.join(importer._q(field) for field in fields)}) "
        f"VALUES ({', '.join(':' + field for field in fields)});",
        **owner_fields_by_name,
    )
    await fixture.database.status(
        f"""
        INSERT INTO {importer._unscoped_qt(fixture.schema, "import_run")}
            (run_id, importer, status, finished_at)
        VALUES (
            :run_id, 'provider-directory-fhir', :status, :finished_at
        );
        """,
        run_id=run_id,
        status=status,
        finished_at=finished_at,
    )


async def _create_owner_scope(
    fixture,
    owner_run_id: str,
) -> tuple[dict[str, str], dict[str, int]]:
    relation_by_base: dict[str, str] = {}
    oid_by_base: dict[str, int] = {}
    for model in (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS):
        base_name = model.__tablename__
        relation_name = importer._owned_artifact_scope_name(
            base_name,
            run_id=owner_run_id,
        )
        await importer._create_provider_directory_artifact_scope_layout(
            model,
            fixture.schema,
            relation_name,
        )
        relation_by_base[base_name] = relation_name
        oid_by_base[base_name] = int(
            await fixture.database.scalar(
                "SELECT to_regclass(:relation_ref)::oid::bigint;",
                relation_ref=importer._unscoped_qt(
                    fixture.schema,
                    relation_name,
                ),
            )
        )
    return relation_by_base, oid_by_base


async def _recovery_admission(fixture):
    geometry = _capacity_geometry(
        fixture,
        artifact_scratch_cap=1024 * 1024 * 1024,
        artifact_wal_cap=1024 * 1024 * 1024,
    )
    initial_wal_lsn = str(
        await fixture.database.scalar(
            "SELECT pg_current_wal_insert_lsn()::text;"
        )
    )
    return _admission(geometry, initial_wal_lsn)


async def _assert_scope_replaced(
    fixture,
    prior_relations: dict[str, str],
    prior_oids: dict[str, int],
    current_relations: dict[str, str],
) -> None:
    for base_name, prior_oid in prior_oids.items():
        prior_ref = importer._unscoped_qt(
            fixture.schema,
            prior_relations[base_name],
        )
        current_ref = importer._unscoped_qt(
            fixture.schema,
            current_relations[base_name],
        )
        assert (
            await fixture.database.scalar(
                "SELECT to_regclass(:relation_ref);",
                relation_ref=prior_ref,
            )
            is None
        )
        assert int(
            await fixture.database.scalar(
                "SELECT to_regclass(:relation_ref)::oid::bigint;",
                relation_ref=current_ref,
            )
        ) != prior_oid


@pytest.mark.asyncio
async def test_pg18_fresh_child_recovers_only_terminal_parent_scope(
    monkeypatch,
):
    async with _artifact_fixture(monkeypatch) as fixture:
        await _drop_fixture_scope(fixture)
        await _create_recovery_metadata_tables(fixture)
        prior_relations, prior_oids = await _create_owner_scope(
            fixture,
            _PRIOR_RUN_ID,
        )
        admission = await _recovery_admission(fixture)
        await _insert_owner(
            fixture,
            admission,
            run_id=_PRIOR_RUN_ID,
            status="failed",
            finished_at=datetime.datetime.now(datetime.UTC),
        )
        await _insert_owner(
            fixture,
            admission,
            run_id=_CURRENT_RUN_ID,
            status="running",
            finished_at=None,
        )
        token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
            admission
        )
        try:
            plan = importer._artifact_scope_materialization_plan(
                _CURRENT_RUN_ID
            )
            recovered = (
                await importer._recover_provider_directory_artifact_scope(
                    fixture.schema,
                    plan,
                )
            )
        finally:
            importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
                token
            )

        assert set(recovered) == set(prior_relations.values())
        assert set(plan.created_tables) == set(
            plan.relation_by_table.values()
        )
        await _assert_scope_replaced(
            fixture,
            prior_relations,
            prior_oids,
            plan.relation_by_table,
        )


@pytest.mark.asyncio
async def test_pg18_current_owner_refusal_preserves_every_scope_oid(
    monkeypatch,
):
    async with _artifact_fixture(monkeypatch) as fixture:
        await _drop_fixture_scope(fixture)
        await _create_recovery_metadata_tables(fixture)
        current_relations, current_oids = await _create_owner_scope(
            fixture,
            _CURRENT_RUN_ID,
        )
        admission = await _recovery_admission(fixture)
        await _insert_owner(
            fixture,
            admission,
            run_id=_CURRENT_RUN_ID,
            status="running",
            finished_at=None,
        )
        token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
            admission
        )
        try:
            plan = importer._artifact_scope_materialization_plan(
                _CURRENT_RUN_ID
            )
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="current_owner_present",
            ):
                await importer._recover_provider_directory_artifact_scope(
                    fixture.schema,
                    plan,
                )
        finally:
            importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
                token
            )

        assert plan.created_tables == []
        for base_name, relation_name in current_relations.items():
            assert int(
                await fixture.database.scalar(
                    "SELECT to_regclass(:relation_ref)::oid::bigint;",
                    relation_ref=importer._unscoped_qt(
                        fixture.schema,
                        relation_name,
                    ),
                )
            ) == current_oids[base_name]
