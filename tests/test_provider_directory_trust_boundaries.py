# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_sources
from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _catalog_manifest(entries: list[object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "importer": "provider-directory-fhir",
        "entries": entries,
    }


@pytest.mark.parametrize(
    ("payload", "profile_source_ids", "error"),
    [
        ([], (), "provider_directory_source_manifest_invalid"),
        (
            _catalog_manifest([None]),
            (),
            "provider_directory_source_manifest_invalid",
        ),
        (
            _catalog_manifest(
                [
                    {
                        "entry_id": "malformed",
                        "classification": "probe_only",
                        "source_ids": [],
                    }
                ]
            ),
            (),
            "provider_directory_source_manifest_invalid",
        ),
        (
            _catalog_manifest(
                [
                    {
                        "entry_id": "runnable",
                        "classification": "acquisition",
                        "source_ids": ["pdfhir_runnable"],
                    }
                ]
            ),
            ("pdfhir_other",),
            "provider_directory_profile_source_catalog_drift",
        ),
    ],
)
def test_source_catalog_rejects_invalid_or_profile_drifted_manifests(
    tmp_path,
    monkeypatch,
    payload,
    profile_source_ids,
    error,
):
    """Fail closed before exposing malformed or non-Profile runnable sources."""
    manifest_path = _write_json(tmp_path / "manifest.json", payload)
    monkeypatch.setattr(
        provider_directory_sources.profile_artifact,
        "configured_profile_source_ids",
        lambda: profile_source_ids,
    )

    with pytest.raises(RuntimeError, match=error):
        provider_directory_sources.provider_directory_source_catalog(
            manifest_path
        )


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {
            "schema_version": 1,
            "source_ids": [],
            "entry_ids": ["entry"],
        },
    ],
)
def test_profile_source_spec_rejects_invalid_contracts(tmp_path, payload):
    """Reject both an invalid envelope and invalid source membership."""
    spec_path = _write_json(tmp_path / "profile-sources.json", payload)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_source_spec_invalid",
    ):
        profile.load_profile_source_spec(spec_path)


def test_profile_sql_template_renderer_rejects_contract_drift(monkeypatch):
    """Surface both missing requested tokens and unresolved template tokens."""
    monkeypatch.setattr(profile, "_sql_template", lambda _filename: "SELECT 1")
    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_sql_token_missing:EXPECTED",
    ):
        profile._render_sql_template("fixture.sql", {"EXPECTED": "value"})

    monkeypatch.setattr(
        profile,
        "_sql_template",
        lambda _filename: "{{KNOWN}} {{LEFTOVER}}",
    )
    with pytest.raises(
        RuntimeError,
        match=r"provider_directory_profile_sql_tokens_unresolved:\{\{LEFTOVER\}\}",
    ):
        profile._render_sql_template("fixture.sql", {"KNOWN": "value"})


def test_profile_helpers_cover_incremental_and_bounded_identifier_paths():
    """Keep incremental invalidation and PostgreSQL name bounds explicit."""
    assert profile.sibling_table_ref("unqualified", "sibling") == '"sibling"'

    index_name = profile.profile_index_name("p" * 64, "generation_idx")
    assert len(index_name) <= 63
    assert index_name.startswith("p" * 50 + "_")

    incremental_sql = profile.profile_insert_sql(
        evidence_ref='"fixture"."new_evidence"',
        target_ref='"fixture"."new_profile"',
        old_evidence_ref='"fixture"."old_evidence"',
        rebuild_all=False,
    )
    assert 'FROM "fixture"."old_evidence"' in incremental_sql
    assert "source_id <> ALL(CAST(:retained_source_ids AS varchar[]))" in (
        incremental_sql
    )
    assert "OR NOT" in incremental_sql


def test_fhir_reference_sql_rejects_non_resource_type_fragments():
    """Do not permit a resource type to become an injected SQL regex fragment."""
    with pytest.raises(ValueError, match="invalid FHIR resource type"):
        profile.fhir_reference_resource_id_sql("role.reference", "Organization|")


def test_artifact_stage_normalization_and_collection_reject_invalid_shapes():
    """Reject malformed deferred stages before an atomic artifact promotion."""
    bundle = importer.ProviderDirectoryArtifactBundle()

    assert importer._normalize_provider_directory_artifact_stages(None) is None
    with pytest.raises(
        RuntimeError,
        match="provider_directory_artifact_stage_result_invalid",
    ):
        importer._collect_provider_directory_artifact_stage("invalid", bundle)
    with pytest.raises(
        RuntimeError,
        match="provider_directory_artifact_stage_result_invalid",
    ):
        importer._collect_provider_directory_artifact_stage(
            ("invalid-metrics", ()),
            bundle,
        )


@contextlib.asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
async def test_retry_state_cleanup_only_clears_all_finalized_datasets(
    monkeypatch,
):
    """Clear checkpoints only after every selected dataset is finalized."""
    all_rows = AsyncMock(
        return_value=[
            {"dataset_id": "dataset_b"},
            {"dataset_id": "dataset_a"},
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", all_rows)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "transaction", _transaction)

    cleared = await importer._clear_promoted_endpoint_dataset_retry_state(
        [" dataset_b ", "dataset_a", "", None]
    )

    assert cleared == ["dataset_a", "dataset_b"]
    assert all_rows.await_args.kwargs["dataset_ids"] == [
        "dataset_a",
        "dataset_b",
    ]
    delete_sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "provider_directory_dataset_resource" in delete_sql
    assert "provider_directory_pagination_checkpoint" in delete_sql


@pytest.mark.asyncio
async def test_retry_state_cleanup_keeps_partial_or_empty_selections(
    monkeypatch,
):
    """Preserve resumability when no IDs or only some IDs are finalized."""
    all_rows = AsyncMock(return_value=[{"dataset_id": "dataset_a"}])
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", all_rows)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "transaction", _transaction)

    assert await importer._clear_promoted_endpoint_dataset_retry_state([]) == []
    assert (
        await importer._clear_promoted_endpoint_dataset_retry_state(
            ["dataset_a", "dataset_b"]
        )
        == []
    )
    assert "DELETE FROM" not in "\n".join(
        call.args[0] for call in status.await_args_list
    )


@pytest.mark.asyncio
async def test_retry_state_cleanup_is_best_effort_on_lock_failure(monkeypatch):
    """Leave checkpoints intact when the cutover cleanup lock cannot be acquired."""

    @contextlib.asynccontextmanager
    async def failed_transaction():
        raise RuntimeError("lock unavailable")
        yield

    monkeypatch.setattr(importer.db, "transaction", failed_transaction)

    assert (
        await importer._clear_promoted_endpoint_dataset_retry_state(
            ["dataset_a"]
        )
        == []
    )


@pytest.mark.asyncio
async def test_deferred_corroboration_requires_unified_address_table(monkeypatch):
    """Do not claim a prepared corroboration artifact without its base table."""
    monkeypatch.setattr(
        importer,
        "_table_exists",
        AsyncMock(return_value=False),
    )

    assert not await importer._is_provider_directory_corroboration_artifact_published(
        source_ids=["source_a"],
        network_catalog_metrics={},
        artifact_bundle=importer.ProviderDirectoryArtifactBundle(),
    )


@pytest.mark.asyncio
async def test_profile_index_renames_cover_compact_and_evidence_sets(monkeypatch):
    """Rename every prepared index to its stable post-cutover name."""
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._rename_provider_directory_profile_table_indexes(
        "mrf",
        "profile_stage",
    )
    await importer._rename_provider_directory_profile_evidence_indexes(
        "mrf",
        "evidence_stage",
    )

    statements = [call.args[0] for call in status.await_args_list]
    assert len(statements) == 1 + len(profile.PROFILE_EVIDENCE_INDEX_SUFFIXES)
    assert all(statement.startswith("ALTER INDEX") for statement in statements)
    assert all("RENAME TO" in statement for statement in statements)


def _profile_build() -> importer._ProviderDirectoryProfileBuild:
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a", "source_b"),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )


@pytest.mark.asyncio
async def test_existing_profile_stages_receive_retention_and_currentness_params(
    monkeypatch,
):
    """Pass the trust fence into both evidence copy and compact invalidation."""
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    build = _profile_build()

    await importer._create_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=True,
    )
    await importer._create_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=True,
    )

    parameter_sets = [call.kwargs for call in status.await_args_list]
    retained_calls = [
        params
        for params in parameter_sets
        if params.get("retained_source_ids") == ["source_a", "source_b"]
    ]
    assert len(retained_calls) == 3
    assert all(params["profile_as_of"] == "2026-07-19" for params in retained_calls)


@pytest.mark.asyncio
async def test_profile_stage_finalization_supports_deferred_and_immediate_cutover(
    monkeypatch,
):
    """Return prepared stages when deferred and clean both after immediate cutover."""
    stages = (SimpleNamespace(stage_table="a"), SimpleNamespace(stage_table="b"))
    metric_map = {"profile_rows": 2}
    promote = AsyncMock()
    remove = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        promote,
    )
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_artifact_stage",
        remove,
    )

    assert await importer._finalize_provider_directory_profile_stages(
        metric_map,
        stages,
        defer_cutover=True,
    ) == (metric_map, stages)
    assert await importer._finalize_provider_directory_profile_stages(
        metric_map,
        stages,
        defer_cutover=False,
    ) == metric_map
    promote.assert_awaited_once_with(stages)
    assert [call.args[0] for call in remove.await_args_list] == list(
        reversed(stages)
    )


@pytest.mark.asyncio
async def test_profile_stage_build_removes_created_stage_after_failure(monkeypatch):
    """Drop the evidence stage if compact profile construction fails."""
    build = _profile_build()
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
    remove = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_has_provider_directory_profile_artifacts",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_evidence_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_compact_stage",
        AsyncMock(side_effect=RuntimeError("compact failed")),
    )
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_profile_stage_table",
        remove,
    )

    with pytest.raises(RuntimeError, match="compact failed"):
        await importer._build_provider_directory_profile_stages(
            build,
            fence,
            fence,
        )

    remove.assert_awaited_once_with("mrf", "evidence_stage")

