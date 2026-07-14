# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")


REPO_ROOT = Path(__file__).resolve().parents[1]
PROFILE_SOURCE_CLASSIFICATIONS = {
    "acquisition",
    "bulk_acquisition",
    "external",
}


def test_profile_source_spec_matches_all_reviewed_acquisition_entries():
    """Require every importable source and exclude every probe-only source."""
    source_spec = profile.load_profile_source_spec()
    manifest = json.loads(
        (
            REPO_ROOT
            / "specs/provider_directory_endpoint_acquisition_manifest.json"
        ).read_text(encoding="utf-8")
    )
    entries_by_id = {
        entry["entry_id"]: entry for entry in manifest["entries"]
    }
    expected_profile_entry_ids = {
        entry_id
        for entry_id, entry in entries_by_id.items()
        if entry["classification"] in PROFILE_SOURCE_CLASSIFICATIONS
    }
    expected_source_ids = {
        source_id
        for entry_id in expected_profile_entry_ids
        for source_id in entries_by_id[entry_id]["source_ids"]
    }

    assert set(source_spec["entry_ids"]) == expected_profile_entry_ids
    assert set(source_spec["source_ids"]) == expected_source_ids
    assert all(
        entries_by_id[entry_id]["classification"]
        in PROFILE_SOURCE_CLASSIFICATIONS
        for entry_id in source_spec["entry_ids"]
    )


def test_profile_tables_and_indexes_are_bounded_and_npi_indexed():
    profile_sql = profile.profile_table_sql(
        "mrf",
        "profile_stage",
        logged=True,
    )
    evidence_sql = profile.profile_evidence_table_sql(
        "mrf",
        "evidence_stage",
        logged=True,
    )
    profile_indexes = profile.profile_index_statements(
        "mrf",
        "profile_stage",
        evidence=False,
    )
    evidence_indexes = profile.profile_index_statements(
        "mrf",
        "evidence_stage",
        evidence=True,
    )

    assert 'CREATE TABLE "mrf"."profile_stage"' in profile_sql
    assert "UNLOGGED" not in profile_sql
    assert "npi bigint PRIMARY KEY" in profile_sql
    assert "evidence_json jsonb NOT NULL" in profile_sql
    assert 'CREATE TABLE "mrf"."evidence_stage"' in evidence_sql
    assert "UNLOGGED" not in evidence_sql
    assert "evidence_key char(32) PRIMARY KEY" in evidence_sql
    assert any("(generation_id)" in statement for statement in profile_indexes)
    assert any("(npi, fact_type, fact_key)" in statement for statement in evidence_indexes)
    assert profile.PROFILE_FACT_LIMIT == 100
    assert profile.PROFILE_FACT_EVIDENCE_LIMIT == 25


@pytest.mark.parametrize(
    "npi",
    [1000000491, 1234567893, 1588616783, 2000000002, 2999999990],
)
def test_profile_npi_validation_accepts_valid_check_digits(npi):
    assert profile.is_valid_npi(npi)


@pytest.mark.parametrize(
    "npi",
    [
        None,
        "",
        "not-an-npi",
        999999999,
        3_000_000_000,
        10_000_000_000,
        1000000492,
    ],
)
def test_profile_npi_validation_rejects_invalid_values(npi):
    assert not profile.is_valid_npi(npi)


@pytest.mark.asyncio
async def test_profile_npi_sql_predicate_matches_python_in_postgresql():
    dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set the profile PostgreSQL DSN to run predicate parity")
    candidates = [
        999_999_999,
        1_000_000_491,
        1_000_000_492,
        2_000_000_002,
        2_999_999_990,
        3_000_000_000,
        10_000_000_000,
    ]
    connection = await asyncpg.connect(dsn)
    try:
        predicate_rows = await connection.fetch(
            f"""
            SELECT candidate, {profile.valid_npi_sql("candidate")} AS is_valid
              FROM unnest($1::bigint[]) AS candidate_values(candidate)
             ORDER BY candidate;
            """,
            candidates,
        )
    finally:
        await connection.close()

    assert {
        int(predicate_row["candidate"]): bool(predicate_row["is_valid"])
        for predicate_row in predicate_rows
    } == {
        candidate: profile.is_valid_npi(candidate)
        for candidate in candidates
    }


def test_profile_publication_filters_invalid_npis_from_new_and_copied_rows():
    evidence_insert_sql = profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        endpoint_ref='"fixture"."endpoint"',
    )
    evidence_copy_sql = profile.copy_existing_evidence_sql(
        source_ref='"fixture"."old_evidence"',
        target_ref='"fixture"."new_evidence"',
    )
    profile_copy_sql = profile.copy_unaffected_profiles_sql(
        profile_source_ref='"fixture"."old_profile"',
        evidence_source_ref='"fixture"."old_evidence"',
        evidence_stage_ref='"fixture"."new_evidence"',
        profile_stage_ref='"fixture"."new_profile"',
    )

    assert "(npi) BETWEEN 1000000000 AND 2999999999" in evidence_insert_sql
    assert "(npi) BETWEEN 1000000000 AND 2999999999" in evidence_copy_sql
    assert (
        "(profile.npi) BETWEEN 1000000000 AND 2999999999"
        in profile_copy_sql
    )
    assert "JOIN \"mrf\".\"npi\"" not in evidence_insert_sql


def test_profile_evidence_sql_retains_derived_and_source_backed_facts():
    sql = profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        endpoint_ref='"fixture"."endpoint"',
    )

    for fact_type in (
        "age",
        "years_of_practice",
        "credential",
        "taxonomy_qualification",
        "qualification_detail",
        "language",
        "contact",
        "specialty",
        "new_patient_acceptance",
        "telehealth",
        "accepting_medicaid",
        "role_identifier",
        "organization",
        "service",
        "endpoint",
    ):
        assert f"'{fact_type}'" in sql
    assert "practitioner.birth_date" not in sql
    assert "practitioner.birthDate" not in sql
    assert "basis_start_date" in sql
    assert "'identifiers', role.identifiers::jsonb" in sql
    assert "'identifiers', service.identifiers::jsonb" in sql
    assert "'accepting_patients', service.accepting_patients::jsonb" in sql
    assert "'comment', service.comment" in sql
    assert "JOIN \"fixture\".\"endpoint\" AS endpoint" in sql
    assert "'accepting_patients', COALESCE(" in sql
    assert "npi) BETWEEN 1000000000 AND 2999999999" in sql
    assert "AND MOD(" in sql
    assert "{{VALID_NPI_SQL}}" not in sql
    assert "ON CONFLICT (evidence_key) DO NOTHING" in sql


def test_profile_artifact_scope_materializes_endpoint_resources():
    assert "Endpoint" in importer.PROVIDER_DIRECTORY_ARTIFACT_TARGET_RESOURCE_TYPES[
        "profile"
    ]
    assert "Endpoint" in importer.PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_TYPES


def test_profile_aggregation_is_deterministic_and_evidence_bounded():
    sql = profile.profile_insert_sql(
        evidence_ref='"fixture"."evidence"',
        target_ref='"fixture"."profile"',
        old_evidence_ref=None,
        rebuild_all=True,
    )

    assert "evidence_rank <= 25" in sql
    assert "fact_rank <= 100" in sql
    assert "ORDER BY evidence.source_id, evidence.endpoint_id" in sql
    assert "array_agg(DISTINCT evidence.source_id ORDER BY evidence.source_id)" in sql
    assert "'api_base', regexp_replace(" in sql
    assert "evidence.canonical_api_base," in sql
    assert "'[?#].*$'" in sql
    assert "'^([^:/?#]+://)[^/?#@]*@'" in sql


def test_profile_source_dataset_pairs_preserve_sorted_alignment():
    datasets = [
        SimpleNamespace(source_id="source_b", dataset_id="dataset_b"),
        SimpleNamespace(source_id="source_a", dataset_id="dataset_a"),
    ]

    assert profile.profile_source_dataset_pairs(
        datasets,
        ["source_b", "source_a"],
    ) == (["source_a", "source_b"], ["dataset_a", "dataset_b"])

    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_dataset_missing:source_c",
    ):
        profile.profile_source_dataset_pairs(datasets, ["source_c"])


@pytest.mark.asyncio
async def test_profile_scope_filters_to_current_immutable_dataset_fence(
    monkeypatch,
):
    captured_by_name = {}

    async def fake_all(sql, **params):
        captured_by_name["sql"] = sql
        captured_by_name["params"] = params
        return [
            {"source_id": "source_allowed"},
            {"source_id": "source_outside_fence"},
        ]

    monkeypatch.setattr(
        profile,
        "configured_profile_source_ids",
        lambda: ("source_allowed", "source_outside_fence"),
    )
    monkeypatch.setattr(importer.db, "all", fake_all)

    source_ids = await importer._provider_directory_profile_scope_source_ids(
        "mrf",
        {"source_allowed"},
    )

    assert source_ids == ["source_allowed"]
    assert captured_by_name["params"]["configured_source_ids"] == [
        "source_allowed",
        "source_outside_fence",
    ]
    assert "endpoint_id" in captured_by_name["sql"]


@pytest.mark.asyncio
async def test_profile_stage_build_creates_logged_tables_without_rewrite(
    monkeypatch,
):
    status = AsyncMock()
    scalar_queries = []

    async def scalar(sql, **_params):
        scalar_queries.append(sql)
        return "p" if "cls.relpersistence" in sql else 0

    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "scalar", scalar)
    monkeypatch.setattr(
        importer,
        "_table_exists",
        AsyncMock(return_value=False),
    )
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    _metrics, stages = await importer._build_provider_directory_profile_stages(
        build,
        fence,
        fence,
    )

    statements = [call.args[0] for call in status.await_args_list]
    assert any(
        'CREATE TABLE "mrf"."evidence_stage"' in statement
        for statement in statements
    )
    assert any(
        'CREATE TABLE "mrf"."profile_stage"' in statement
        for statement in statements
    )
    assert not any("CREATE UNLOGGED TABLE" in statement for statement in statements)
    assert not any("SET LOGGED" in statement for statement in statements)
    assert sum("cls.relpersistence" in query for query in scalar_queries) == 2
    assert [stage.stage_table for stage in stages] == [
        "evidence_stage",
        "profile_stage",
    ]


def test_artifact_bundle_collects_profile_and_evidence_stages_together():
    async def rename_indexes(_schema, _stage):
        return None

    stages = (
        importer.ProviderDirectoryPreparedArtifactStage(
            schema="mrf",
            stage_table="evidence_stage",
            target_relation=profile.PROFILE_EVIDENCE_TABLE,
            rename_stage_indexes=rename_indexes,
        ),
        importer.ProviderDirectoryPreparedArtifactStage(
            schema="mrf",
            stage_table="profile_stage",
            target_relation=profile.PROFILE_TABLE,
            rename_stage_indexes=rename_indexes,
        ),
    )
    bundle = importer.ProviderDirectoryArtifactBundle()

    metrics = importer._collect_provider_directory_artifact_stage(
        ({"profile_rows": 1}, stages),
        bundle,
    )

    assert metrics == {"profile_rows": 1}
    assert bundle.stages == list(stages)


@pytest.mark.asyncio
async def test_profile_stages_are_logged_at_creation_without_set_logged(
    monkeypatch,
):
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation_1",
        source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )
    build_fence = importer.ProviderDirectoryArtifactBuildFence(
        target_oid=None
    )
    status = AsyncMock()
    assert_logged = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer,
        "_has_provider_directory_profile_artifacts",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_logged_relation",
        assert_logged,
    )

    _metrics, stages = await importer._build_provider_directory_profile_stages(
        build,
        build_fence,
        build_fence,
    )

    joined_sql = "\n".join(
        str(awaited.args[0]) for awaited in status.await_args_list
    )
    assert 'CREATE TABLE "mrf"."evidence_stage"' in joined_sql
    assert 'CREATE TABLE "mrf"."profile_stage"' in joined_sql
    assert "CREATE UNLOGGED TABLE" not in joined_sql
    assert "SET LOGGED" not in joined_sql
    assert [
        awaited.args for awaited in assert_logged.await_args_list
    ] == [
        ("mrf", "evidence_stage"),
        ("mrf", "profile_stage"),
    ]
    assert [stage.target_relation for stage in stages] == [
        profile.PROFILE_EVIDENCE_TABLE,
        profile.PROFILE_TABLE,
    ]


@pytest.mark.asyncio
async def test_profile_publish_refuses_a_partial_artifact_pair(monkeypatch):
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source_a",
        endpoint_id="endpoint_a",
        dataset_id="dataset_a",
        evidence_run_id="run_a",
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))

    @contextlib.asynccontextmanager
    async def fake_build_guard(_schema, _target):
        yield importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(return_value=["source_a"]),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_build_guard",
        fake_build_guard,
    )
    monkeypatch.setattr(
        importer,
        "_table_exists",
        AsyncMock(side_effect=[True, False]),
    )
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        fence
    )
    try:
        with pytest.raises(
            RuntimeError,
            match="provider_directory_profile_artifact_pair_incomplete",
        ):
            await importer.publish_provider_directory_profile()
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
