# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Recovery coverage for Provider Directory artifact scope."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .provider_directory_profile_execution_test_support import (
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")


def _owner(run_id, *, status="failed", finished_at="done", consumption=None):
    """Return one capacity-ledger scratch owner."""
    return importer._ArtifactScopeConsumptionOwner(
        run_id=run_id,
        build_id="pdpb_" + "1" * 32,
        capacity_geometry_hash="2" * 64,
        status=status,
        finished_at=finished_at,
        consumption=consumption or {},
    )


def _prior_names(run_id):
    """Return a complete scratch relation family for one owner."""
    return {
        model.__tablename__: importer._owned_artifact_scope_name(
            model.__tablename__,
            run_id=run_id,
        )
        for model in (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS)
    }


def _plan(run_id=None):
    """Return deterministic current-run artifact-scope coordinates."""
    return importer._artifact_scope_materialization_plan(
        run_id or "run_" + "a" * 32
    )


def test_current_owner_refuses_consumption_and_status_drift(monkeypatch):
    """Current scratch ownership must match the signed running lease."""
    admission = _wal_tracker_admission()
    expected_by_name = {"run_id": admission.run_id}
    monkeypatch.setattr(
        importer,
        "_expected_profile_capacity_consumption",
        Mock(return_value=expected_by_name),
    )
    invalid = _owner(admission.run_id, status="running", consumption={})
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="current_owner_invalid",
    ):
        importer._current_artifact_scope_owner((invalid,), admission)

    inactive = _owner(
        admission.run_id,
        status="succeeded",
        consumption=expected_by_name,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="current_owner_inactive",
    ):
        importer._current_artifact_scope_owner((inactive,), admission)


def test_prior_owner_must_be_terminal_with_completion_time():
    """A competing scratch owner must be terminal and completed."""
    current_run_id = "run_" + "a" * 32
    competitors = (
        _owner(current_run_id, status="running", finished_at=None),
        _owner("run_" + "b" * 32, status="failed", finished_at=None),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="competing_owner",
    ):
        importer._assert_prior_artifact_scope_owners_terminal(
            competitors,
            current_run_id,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_ids", (("one", "two"), ("one",)))
async def test_consumption_owner_set_rejects_ambiguity_or_missing_row(
    monkeypatch,
    prior_ids,
):
    """Owner lookup rejects more than one prior run and incomplete ledgers."""
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_artifact_scope_owner_rows",
        AsyncMock(return_value=()),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_ambiguous|owner_invalid",
    ):
        await importer._artifact_scope_consumption_owners(
            "mrf",
            admission,
            prior_ids,
        )


@pytest.mark.parametrize(
    "relation_name",
    (
        "unknown_artifact_scope_" + "a" * 32,
        importer._provider_directory_artifact_scope_table_prefix(
            importer.ProviderDirectorySource.__tablename__
        )
        + "_not-a-run",
    ),
)
def test_prior_owner_parser_rejects_unowned_relations(relation_name):
    """Recovery refuses scratch names without one reversible run owner."""
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="stale_scope_present",
    ):
        importer._artifact_scope_prior_owner(relation_name)


def test_prior_coordinates_require_complete_collision_free_family():
    """Recovery accepts neither duplicate bases nor partial scratch families."""
    prior_run = "run_" + "c" * 32
    name_by_base = _prior_names(prior_run)
    duplicated_names = [
        *name_by_base.values(),
        name_by_base[importer.ProviderDirectorySource.__tablename__],
    ]
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_collision",
    ):
        importer._artifact_scope_prior_coordinates(duplicated_names)

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="stale_scope_present",
    ):
        importer._artifact_scope_prior_coordinates(
            [name_by_base[importer.ProviderDirectorySource.__tablename__]]
        )


def test_prior_owner_refuses_current_ambiguous_or_wrong_family():
    """Prior ownership must be one terminal run with the exact relation set."""
    admission = _wal_tracker_admission()
    current_by_base = {
        name: (relation, admission.run_id)
        for name, relation in _prior_names(admission.run_id).items()
    }
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="current_owner_present",
    ):
        importer._assert_artifact_scope_prior_owner(
            current_by_base,
            (),
            admission,
        )

    ambiguous_by_base = dict(current_by_base)
    first_key = next(iter(ambiguous_by_base))
    ambiguous_by_base[first_key] = (
        ambiguous_by_base[first_key][0],
        "run_" + "d" * 32,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_ambiguous",
    ):
        importer._assert_artifact_scope_prior_owner(
            ambiguous_by_base,
            (),
            admission,
        )

    prior_run = "run_" + "e" * 32
    wrong_family_by_base = {
        name: (relation, prior_run)
        for name, relation in _prior_names(prior_run).items()
    }
    wrong_family_by_base[first_key] = ("wrong-relation", prior_run)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_invalid",
    ):
        importer._assert_artifact_scope_prior_owner(
            wrong_family_by_base,
            (_owner(prior_run),),
            admission,
        )


@pytest.mark.asyncio
async def test_recovery_coordinates_and_identity_empty_paths(monkeypatch):
    """No discovered residue produces no takeover coordinates or DB query."""
    monkeypatch.setattr(
        importer,
        "_discover_provider_directory_artifact_scope_tables",
        AsyncMock(return_value=[]),
    )
    admission = _wal_tracker_admission()
    assert await importer._artifact_scope_recovery_coordinates(
        "mrf",
        _plan(),
        admission,
    ) == ()

    query = AsyncMock()
    monkeypatch.setattr(importer.db, "all", query)
    assert await importer._artifact_scope_relation_identities("mrf", []) == {}
    query.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("referenced", (False, True))
async def test_recovery_refuses_protected_or_referenced_relations(
    monkeypatch,
    referenced,
):
    """Serving and checkpoint identities can never be reclaimed as residue."""
    admission = _wal_tracker_admission()
    if referenced:
        relation_oids = [999_999]
        error = "scope_referenced"
    else:
        relation_oids = [admission.geometry.evidence_target_oid]
        error = "protected_oid"
    monkeypatch.setattr(
        importer,
        "_is_artifact_scope_recovery_referenced",
        AsyncMock(return_value=referenced),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match=error,
    ):
        await importer._assert_artifact_scope_recovery_unreferenced(
            "mrf",
            relation_oids,
            admission,
        )


@pytest.mark.parametrize(
    "observed",
    (
        {},
        {"scratch": (11, "v", "u")},
        {"scratch": (11, "r", "p")},
    ),
)
def test_recovery_identity_requires_exact_unlogged_heap(observed):
    """Recovery requires the exact named unlogged heap postimage."""
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="recovery_identity_changed",
    ):
        importer._assert_artifact_scope_recovery_identity(
            ("scratch",),
            observed,
        )


@pytest.mark.asyncio
async def test_recovery_layout_refuses_structure_or_tablespace_drift(
    monkeypatch,
):
    """Takeover requires structural identity in the admitted tablespace."""
    admission = _wal_tracker_admission()
    coordinate = importer._ArtifactScopeRecoveryCoordinate(
        base_table_name="provider_directory_source",
        prior_table_name="prior",
        current_table_name="current",
    )
    monkeypatch.setattr(
        importer,
        "_artifact_scope_relation_identities",
        AsyncMock(return_value={"current": (22, "r", "u")}),
    )
    fingerprint = AsyncMock(
        side_effect=(
            SimpleNamespace(
                structural_fingerprint="prior",
                effective_tablespace_oids=(admission.geometry.tablespace_oid,),
            ),
            SimpleNamespace(
                structural_fingerprint="current",
                effective_tablespace_oids=(admission.geometry.tablespace_oid,),
            ),
        )
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        fingerprint,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="recovery_layout_changed",
    ):
        await importer._assert_artifact_scope_recovery_layouts(
            "mrf",
            (coordinate,),
            {"prior": (11, "r", "u")},
            admission,
        )


@pytest.mark.asyncio
async def test_recovery_noop_and_postimage_drift_paths(monkeypatch):
    """Recovery is a no-op without residue and rejects retained prior OIDs."""
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        Mock(return_value=admission),
    )
    coordinates = AsyncMock(return_value=())
    monkeypatch.setattr(
        importer,
        "_artifact_scope_recovery_coordinates",
        coordinates,
    )
    assert await importer._recover_provider_directory_artifact_scope(
        "mrf",
        _plan(admission.run_id),
    ) == ()

    coordinate = importer._ArtifactScopeRecoveryCoordinate(
        base_table_name="provider_directory_source",
        prior_table_name="prior",
        current_table_name="current",
    )
    coordinates.return_value = (coordinate,)
    identities = AsyncMock(
        side_effect=(
            {"prior": (11, "r", "u")},
            {"prior": (11, "r", "u")},
        )
    )
    monkeypatch.setattr(
        importer,
        "_artifact_scope_relation_identities",
        identities,
    )
    monkeypatch.setattr(
        importer,
        "_reserve_provider_directory_profile_wal_budget",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_reserve_artifact_scope_layout_wal",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_replace_terminal_artifact_scope",
        AsyncMock(),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="recovery_postimage_changed",
    ):
        await importer._recover_provider_directory_artifact_scope(
            "mrf",
            _plan(admission.run_id),
        )


@pytest.mark.asyncio
async def test_takeover_rechecks_locked_relation_identity(monkeypatch):
    """Atomic takeover rejects an OID change after the exclusive lock."""
    admission = _wal_tracker_admission()

    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        transaction,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_artifact_scope_relation_identities",
        AsyncMock(return_value={"prior": (12, "r", "u")}),
    )
    coordinate = importer._ArtifactScopeRecoveryCoordinate(
        base_table_name="provider_directory_source",
        prior_table_name="prior",
        current_table_name="current",
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="recovery_oid_changed",
    ):
        await importer._replace_terminal_artifact_scope(
            "mrf",
            _plan(),
            (coordinate,),
            ("prior",),
            {"prior": (11, "r", "u")},
            admission,
        )
