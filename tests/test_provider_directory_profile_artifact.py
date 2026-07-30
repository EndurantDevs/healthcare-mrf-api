# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import datetime
import importlib
import json
import logging
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


def _profile_source_scope(
    *,
    plan_name: str | None = "Example Plan",
):
    """Return one exact source scope for Profile build-resolution tests."""
    return (
        ["source_a"],
        ["source_a"],
        (
            importer._ProviderDirectoryProfileSourceContext(
                source_id="source_a",
                endpoint_id="endpoint_a",
                canonical_api_base="https://example.test/fhir",
                org_name="Example",
                plan_name=plan_name,
            ),
        ),
    )


def _patch_fresh_profile_stage_identity(monkeypatch) -> None:
    """Install deterministic physical identities for a mocked fresh build."""
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(
            side_effect=[None, None, (21, "r", "p"), (22, "r", "p")]
        ),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_checkpoint_ready",
        AsyncMock(),
    )


def _profile_build_identity_fixture():
    """Return the immutable dataset and target fences used by resume tests."""
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source_a",
        endpoint_id="endpoint_a",
        dataset_id="dataset_a",
        evidence_run_id="run_a",
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
    )
    return (
        importer.ProviderDirectoryArtifactDatasetFence((dataset,)),
        importer.ProviderDirectoryArtifactBuildFence(target_oid=11),
        importer.ProviderDirectoryArtifactBuildFence(target_oid=12),
    )


def _resumable_profile_checkpoint(
    build: importer._ProviderDirectoryProfileBuild,
) -> dict[str, object]:
    """Describe the exact logged checkpoint expected to resume."""
    return {
        "build_id": build.build_id,
        "strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "schema_version": profile.PROFILE_SCHEMA_VERSION,
        "resume_lineage_hash": build.resume_lineage_hash,
        "profile_as_of": "2026-07-19",
        "source_ids": ["source_a"],
        "retained_source_ids": ["source_a"],
        "dataset_ids": ["dataset_a"],
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "evidence_stage_oid": 21,
        "profile_stage_oid": 22,
        "evidence_target_oid": 11,
        "profile_target_oid": 12,
        "has_existing_artifacts": False,
        "evidence_total_batches": 115,
        "profile_total_batches": 400,
    }


@pytest.mark.asyncio
async def test_profile_build_identity_resumes_original_as_of_across_days(
    monkeypatch,
):
    """Keep a valid logged checkpoint resumable after the calendar day changes."""
    dataset_fence, evidence_fence, profile_fence = (
        _profile_build_identity_fixture()
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(return_value=_profile_source_scope()),
    )
    checkpoint = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", checkpoint)
    monkeypatch.setattr(
        importer,
        "_now",
        lambda: datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC),
    )

    initial_build = await importer._resolve_provider_directory_profile_build(
        "mrf",
        "run-first",
        dataset_fence,
        evidence_fence,
        profile_fence,
    )
    checkpoint.return_value = _resumable_profile_checkpoint(initial_build)
    stage_identity = AsyncMock(
        side_effect=[(21, "r", "p"), (22, "r", "p")]
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(
        importer,
        "_now",
        lambda: datetime.datetime(2026, 7, 21, tzinfo=datetime.UTC),
    )

    resumed_build = await importer._resolve_provider_directory_profile_build(
        "mrf",
        "run-retry",
        dataset_fence,
        evidence_fence,
        profile_fence,
    )

    assert resumed_build.build_id == initial_build.build_id
    assert resumed_build.profile_as_of == "2026-07-19"
    assert resumed_build.owner_run_id == "run-retry"
    assert stage_identity.await_count == 2


def test_profile_source_spec_matches_reviewed_and_retained_entries():
    """Require every importable source plus explicit retained-only sources."""
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
    retained_registry = json.loads(
        (
            REPO_ROOT
            / "specs/provider_directory_source_neutral_registry.json"
        ).read_text(encoding="utf-8")
    )
    retained_entries_by_id = {
        entry["entry_id"]: entry for entry in retained_registry["entries"]
    }
    retained_entry_ids = set(source_spec.get("retained_entry_ids", ()))
    importable_entry_ids = {
        entry_id
        for entry_id, entry in entries_by_id.items()
        if entry["classification"] in PROFILE_SOURCE_CLASSIFICATIONS
    }
    expected_profile_entry_ids = retained_entry_ids | importable_entry_ids
    expected_source_ids = {
        source_id
        for entry_id in importable_entry_ids
        for source_id in entries_by_id[entry_id]["source_ids"]
    } | {
        retained_entries_by_id[entry_id]["registered_source_id"]
        for entry_id in retained_entry_ids
    }

    assert set(source_spec["entry_ids"]) == expected_profile_entry_ids
    assert set(source_spec["source_ids"]) == expected_source_ids
    assert all(
        entries_by_id[entry_id]["classification"]
        in PROFILE_SOURCE_CLASSIFICATIONS
        for entry_id in importable_entry_ids
    )
    assert all(
        retained_entries_by_id[entry_id]["acquisition_runnable"] is True
        and retained_entries_by_id[entry_id]["profile_eligible"] is True
        and retained_entries_by_id[entry_id]["publication_ready"] is True
        for entry_id in retained_entry_ids
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
    assert "source_id = ANY(CAST(:retained_source_ids AS varchar[]))" in (
        evidence_copy_sql
    )
    assert "source_id <> ALL(CAST(:retained_source_ids AS varchar[]))" in (
        profile_copy_sql
    )
    assert (
        "(profile.npi) BETWEEN 1000000000 AND 2999999999"
        in profile_copy_sql
    )
    assert "JOIN \"mrf\".\"npi\"" not in evidence_insert_sql


def test_profile_artifact_scope_materializes_endpoint_resources():
    assert "Endpoint" in importer.PROVIDER_DIRECTORY_ARTIFACT_TARGET_RESOURCE_TYPES[
        "profile"
    ]
    assert (
        "OrganizationAffiliation"
        in importer.PROVIDER_DIRECTORY_ARTIFACT_TARGET_RESOURCE_TYPES["profile"]
    )
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
    assert sql.count('FROM "fixture"."evidence" AS evidence') == 1
    assert "FROM scoped_evidence AS evidence" in sql


def test_profile_aggregation_supports_bounded_npi_ranges():
    sql = profile.profile_insert_sql(
        evidence_ref='"fixture"."evidence"',
        target_ref='"fixture"."profile"',
        old_evidence_ref=None,
        rebuild_all=True,
        npi_start=1_000_000_000,
        npi_end=1_005_000_000,
    )

    assert "npi >= CAST(:profile_npi_start AS bigint)" in sql
    assert "npi < CAST(:profile_npi_end AS bigint)" in sql
    assert (
        "WHERE evidence.npi >= CAST(:profile_npi_start AS bigint)" in sql
    )
    assert "FROM scoped_evidence AS evidence" in sql

    with pytest.raises(ValueError, match="requires both bounds"):
        profile.profile_insert_sql(
            evidence_ref='"fixture"."evidence"',
            target_ref='"fixture"."profile"',
            old_evidence_ref=None,
            rebuild_all=True,
            npi_start=1_000_000_000,
        )

    for npi_start, npi_end in (
        (999_999_999, 1_000_000_001),
        (1_000_000_000, 1_000_000_000),
        (1_000_000_000, 3_000_000_001),
    ):
        with pytest.raises(ValueError, match="outside the assignable bounds"):
            profile.profile_insert_sql(
                evidence_ref='"fixture"."evidence"',
                target_ref='"fixture"."profile"',
                old_evidence_ref=None,
                rebuild_all=True,
                npi_start=npi_start,
                npi_end=npi_end,
            )


def test_candidate_profile_metrics_allow_only_the_explicit_empty_scope_skip():
    """Keep a deliberate empty Profile scope distinct from missing artifacts."""
    importer._assert_candidate_artifact_metrics_complete(
        {"profile"},
        {
            "profile": {
                "skipped": True,
                "reason": "no_profile_enabled_sources_in_scope",
            }
        },
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_candidate_artifact_metric_missing:profile",
    ):
        importer._assert_candidate_artifact_metrics_complete({"profile"}, {})

    with pytest.raises(
        RuntimeError,
        match="provider_directory_candidate_artifact_skipped:corroboration",
    ):
        importer._assert_candidate_artifact_metrics_complete(
            {"corroboration"},
            {},
        )


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
            {
                "source_id": "source_allowed",
                "endpoint_id": "endpoint_allowed",
                "canonical_api_base": "https://allowed.test/fhir",
                "org_name": "Allowed",
                "plan_name": "Allowed Plan",
            },
            {
                "source_id": "source_outside_fence",
                "endpoint_id": "endpoint_outside",
                "canonical_api_base": "https://outside.test/fhir",
                "org_name": "Outside",
                "plan_name": None,
            },
        ]

    monkeypatch.setattr(
        profile,
        "configured_profile_source_ids",
        lambda: ("source_allowed", "source_outside_fence"),
    )
    monkeypatch.setattr(importer.db, "all", fake_all)

    source_ids, retained_source_ids, source_contexts = (
        await importer._provider_directory_profile_scope_source_ids(
            "mrf",
            {"source_allowed"},
        )
    )

    assert source_ids == ["source_allowed"]
    assert retained_source_ids == [
        "source_allowed",
        "source_outside_fence",
    ]
    assert source_contexts == (
        importer._ProviderDirectoryProfileSourceContext(
            source_id="source_allowed",
            endpoint_id="endpoint_allowed",
            canonical_api_base="https://allowed.test/fhir",
            org_name="Allowed",
            plan_name="Allowed Plan",
        ),
    )
    assert captured_by_name["params"]["configured_source_ids"] == [
        "source_allowed",
        "source_outside_fence",
    ]
    assert "SELECT source.source_id," in captured_by_name["sql"]
    assert "source.endpoint_id," in captured_by_name["sql"]


@pytest.mark.asyncio
async def test_profile_stage_build_creates_logged_tables_without_rewrite(
    monkeypatch,
):
    """Create logged stages directly and never rewrite their persistence."""
    status = AsyncMock(return_value=1)
    scalar_queries = []

    @contextlib.asynccontextmanager
    async def transaction():
        yield None

    async def scalar(sql, **_params):
        scalar_queries.append(sql)
        return "p" if "cls.relpersistence" in sql else 0

    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "scalar", scalar)
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    monkeypatch.setattr(importer.db, "transaction", transaction)
    _patch_fresh_profile_stage_identity(monkeypatch)
    monkeypatch.setattr(
        importer,
        "_is_table_present",
        AsyncMock(return_value=False),
    )
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
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
    """Create durable stages directly instead of rewriting them after hours."""
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation_1",
        source_ids=("source_a",),
        retained_source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )
    build_fence = importer.ProviderDirectoryArtifactBuildFence(
        target_oid=None
    )
    status = AsyncMock(return_value=1)
    assert_logged = AsyncMock()

    @contextlib.asynccontextmanager
    async def transaction():
        yield None
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    monkeypatch.setattr(importer.db, "transaction", transaction)
    _patch_fresh_profile_stage_identity(monkeypatch)
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


def _assert_profile_fact_population_calls(insert_calls, build):
    """Require every source/fact/resource partition exactly once."""
    assert len(insert_calls) == 115 * len(build.source_ids)
    assert all(len(call.kwargs["source_ids"]) == 1 for call in insert_calls)
    assert {
        (call.kwargs["source_ids"][0], call.kwargs["dataset_ids"][0])
        for call in insert_calls
    } == {("source_a", "dataset_a"), ("source_b", "dataset_b")}
    role_bucket_calls = [
        call
        for call in insert_calls
        if "profile_role_bucket" in call.kwargs
    ]
    assert len(role_bucket_calls) == (
        len(build.source_ids)
        * 3
        * profile.PROFILE_AFFILIATION_ROLE_BUCKETS
    )
    assert {
        fact_type
        for call in role_bucket_calls
        for fact_type in ("affiliation", "organization", "plan_membership")
        if f"fact_type = '{fact_type}'" in str(call.args[0])
    } == {"affiliation", "organization", "plan_membership"}
    assert {
        call.kwargs["profile_role_bucket"] for call in role_bucket_calls
    } == set(range(profile.PROFILE_AFFILIATION_ROLE_BUCKETS))


@pytest.mark.asyncio
async def test_profile_evidence_population_bounds_each_fact_and_role_bucket(
    monkeypatch,
    caplog,
):
    """Emit one checkpointed statement per source/fact/role bucket."""
    status = AsyncMock(return_value=1)
    create_indexes = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        create_indexes,
    )
    caplog.set_level(logging.INFO, logger=importer.LOGGER.name)
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a", "source_b"),
        retained_source_ids=("source_a", "source_b"),
        dataset_ids=("dataset_a", "dataset_b"),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )

    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=False,
        bounded=True,
    )

    insert_calls = [
        call
        for call in status.await_args_list
        if "dataset_ids" in call.kwargs
    ]
    _assert_profile_fact_population_calls(insert_calls, build)
    log_messages = [log_record.getMessage() for log_record in caplog.records]
    assert any(
        "kind=fact source_id=source_a fact_type=name role_bucket=1/1"
        in message
        for message in log_messages
    )
    assert any(
        "Completed Provider Directory Profile evidence batch" in message
        and "rows=1 elapsed_seconds=" in message
        for message in log_messages
    )
    assert any(
        "Completed Provider Directory Profile evidence indexes" in message
        for message in log_messages
    )


def _profile_write_calls(status):
    """Return artifact writes without maintenance statements."""
    return [
        call
        for call in status.await_args_list
        if "ANALYZE" not in str(call.args[0])
    ]


def _assert_incremental_evidence_writes(write_calls):
    """Assert one retained-source copy plus bounded evidence writes."""
    assert len(write_calls) == 116
    assert "source_id <> ALL" in write_calls[0].args[0]
    assert write_calls[0].kwargs["retained_source_ids"] == [
        "source_a",
        "source_b",
    ]
    assert all(
        call.kwargs["dataset_ids"] == ["dataset_a"]
        for call in write_calls[1:]
    )
    assert all(
        "profile_role_bucket" in call.kwargs
        for call in write_calls[1:]
        if any(
            f"fact_type = '{fact_type}'" in call.args[0]
            for fact_type in ("affiliation", "organization", "plan_membership")
        )
    )


def _assert_incremental_compact_writes(write_calls):
    """Assert one affected-NPI copy plus bounded compact writes."""
    assert len(write_calls) == 401
    assert "affected_npis" in write_calls[0].args[0]
    assert all("generation_id" in call.kwargs for call in write_calls[1:])
    assert all(
        "profile_npi_start" in call.kwargs for call in write_calls[1:]
    )


@pytest.mark.asyncio
async def test_profile_refresh_copies_unaffected_global_rows_before_source(
    monkeypatch,
):
    """Incremental rebuilds retain every reviewed source outside the refresh."""
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a", "source_b"),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )

    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=True,
        bounded=True,
    )

    _assert_incremental_evidence_writes(_profile_write_calls(status))
    status.reset_mock()
    await importer._populate_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=True,
        npi_batch_size=profile.PROFILE_NPI_BATCH_SIZE,
    )
    _assert_incremental_compact_writes(_profile_write_calls(status))


@pytest.mark.asyncio
async def test_profile_compact_population_batches_affected_npi_ranges(
    monkeypatch,
    caplog,
):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    caplog.set_level(logging.INFO, logger=importer.LOGGER.name)
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )

    await importer._populate_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=False,
        npi_batch_size=500_000_000,
    )

    batch_calls = [
        call
        for call in status.await_args_list
        if "profile_npi_start" in call.kwargs
    ]
    assert [
        (call.kwargs["profile_npi_start"], call.kwargs["profile_npi_end"])
        for call in batch_calls
    ] == [
        (1_000_000_000, 1_500_000_000),
        (1_500_000_000, 2_000_000_000),
        (2_000_000_000, 2_500_000_000),
        (2_500_000_000, 3_000_000_000),
    ]
    log_messages = [log_record.getMessage() for log_record in caplog.records]
    assert any(
        "kind=npi npi_start=1000000000 npi_end=1500000000" in message
        for message in log_messages
    )
    assert any(
        "Completed Provider Directory Profile compact batch" in message
        and "rows=1 elapsed_seconds=" in message
        for message in log_messages
    )
    assert any(
        "Completed Provider Directory Profile compact indexes" in message
        for message in log_messages
    )


@pytest.mark.asyncio
async def test_profile_population_failure_is_retained_in_checkpoint(
    monkeypatch,
):
    async def status(sql, **_params):
        if 'INSERT INTO "mrf"."evidence_stage"' in sql:
            raise RuntimeError("forced evidence failure")
        return None

    mark_failed = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_has_provider_directory_profile_artifacts",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_claim_provider_directory_profile_build_checkpoint",
        AsyncMock(
            return_value=importer._ProviderDirectoryProfileBuildCheckpointState(
                evidence_next_batch=0,
                evidence_total_batches=1,
                profile_next_batch=0,
                profile_total_batches=1,
                state="building_evidence",
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_mark_profile_build_checkpoint_failed",
        mark_failed,
    )
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    with pytest.raises(RuntimeError, match="forced evidence failure"):
        await importer._build_provider_directory_profile_stages(
            build,
            fence,
            fence,
        )

    mark_failed.assert_awaited_once()
    assert isinstance(mark_failed.await_args.args[1], RuntimeError)


def _completed_evidence_profile_fixture():
    """Build a profile checkpoint whose evidence phase is complete."""
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a",),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )
    checkpoint_state = importer._ProviderDirectoryProfileBuildCheckpointState(
        evidence_next_batch=1,
        evidence_total_batches=1,
        profile_next_batch=0,
        profile_total_batches=1,
        state="building_profile",
    )
    return build, checkpoint_state


@pytest.mark.asyncio
async def test_profile_resume_does_not_reopen_completed_evidence_phase(
    monkeypatch,
):
    """Start the next compact batch without rewriting phase state first."""
    build, checkpoint_state = _completed_evidence_profile_fixture()
    evidence_population = AsyncMock(
        side_effect=AssertionError("completed evidence phase was reopened")
    )
    compact_population = AsyncMock(
        side_effect=RuntimeError("hard stop before next compact batch")
    )
    mark_failed = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_has_provider_directory_profile_artifacts",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_claim_provider_directory_profile_build_checkpoint",
        AsyncMock(return_value=checkpoint_state),
    )
    monkeypatch.setattr(
        importer,
        "_populate_provider_directory_profile_evidence_stage",
        evidence_population,
    )
    monkeypatch.setattr(
        importer,
        "_populate_provider_directory_profile_compact_stage",
        compact_population,
    )
    monkeypatch.setattr(
        importer,
        "_mark_profile_build_checkpoint_failed",
        mark_failed,
    )
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    with pytest.raises(
        RuntimeError,
        match="hard stop before next compact batch",
    ):
        await importer._build_provider_directory_profile_stages(
            build,
            fence,
            fence,
        )

    evidence_population.assert_not_awaited()
    assert compact_population.await_args.kwargs["start_batch"] == 0
    mark_failed.assert_awaited_once()


@pytest.mark.asyncio
async def test_profile_publish_refuses_a_partial_artifact_pair(monkeypatch):
    """Reject publication when only one serving relation exists."""
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
        AsyncMock(return_value=_profile_source_scope(plan_name=None)),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_build_guard",
        fake_build_guard,
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_reap_stale_provider_directory_profile_builds",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        importer,
        "_is_table_present",
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


def test_retained_profile_source_spec_rejects_missing_rows_and_mapping(
    tmp_path,
):
    spec = profile.load_profile_source_spec()
    missing_matrix_by_field = {
        **spec,
        "verification_matrix": None,
    }
    missing_path = tmp_path / "missing-matrix.json"
    missing_path.write_text(
        json.dumps(missing_matrix_by_field),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="source_spec_invalid"):
        profile.configured_retained_profile_source_ids(missing_path)

    incomplete = json.loads(json.dumps(spec))
    incomplete["verification_matrix"]["sources"] = []
    incomplete_path = tmp_path / "incomplete-matrix.json"
    incomplete_path.write_text(json.dumps(incomplete), encoding="utf-8")
    with pytest.raises(RuntimeError, match="source_spec_invalid"):
        profile.configured_retained_profile_source_ids(incomplete_path)
