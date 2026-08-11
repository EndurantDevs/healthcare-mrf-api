# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant sequential orchestration for rooted-graph baseline/candidate runs."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.provider_directory_rooted_graph_acquisition_contract import (
    ProviderDirectoryRootedGraphAcquisitionConfig,
    ProviderDirectoryRootedGraphAcquisitionDependencies,
    ProviderDirectoryRootedGraphAcquisitionError,
    ProviderDirectoryRootedGraphAcquisitionReceipt,
    ProviderDirectoryRootedGraphInputSnapshot,
    ProviderDirectoryRootedGraphRootReceipt,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ATTEMPTS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_CONCURRENCY,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ROOT_TIMEOUT_SECONDS,
    strict_nonnegative_seconds,
)
from process.provider_directory_rooted_graph_acquisition_runtime import (
    default_dependencies,
    run_root,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
)


def _shared_source_identity(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> tuple[object, ...]:
    return (
        identity.scope_id,
        identity.root_dataset_variant,
        identity.root_publication_contract_id,
        identity.root_source_id,
        identity.root_endpoint_id,
        identity.acquisition_source_id,
        identity.acquisition_endpoint_id,
        identity.source_authority_id,
        identity.endpoint_signature_sha256,
        identity.root_dataset_id,
        identity.root_dataset_hash,
        identity.root_content_proof_sha256,
        identity.root_resource_count,
        identity.root_cohort_id,
        identity.dataset_intent_id,
        identity.max_work_items,
        identity.max_resource_rows,
        identity.max_edge_rows,
        identity.max_payload_bytes,
    )


def _validate_root_pair(
    baseline: object,
    candidate: object,
) -> tuple[
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    ProviderDirectoryRootedGraphAcquisitionIdentity,
]:
    if (
        type(baseline) is not ProviderDirectoryRootedGraphAcquisitionIdentity
        or type(candidate) is not ProviderDirectoryRootedGraphAcquisitionIdentity
        or baseline.acquisition_role != "baseline"
        or candidate.acquisition_role != "candidate"
        or baseline.acquisition_id == candidate.acquisition_id
        or baseline.run_id == candidate.run_id
        or _shared_source_identity(baseline) != _shared_source_identity(candidate)
    ):
        raise ValueError("provider_directory_rooted_graph_root_pair_invalid")
    return baseline, candidate


def _runtime_dependencies(
    config: ProviderDirectoryRootedGraphAcquisitionConfig,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies | None,
) -> ProviderDirectoryRootedGraphAcquisitionDependencies:
    if type(config) is not ProviderDirectoryRootedGraphAcquisitionConfig:
        raise ValueError("provider_directory_rooted_graph_config_invalid")
    if config.enabled is not True:
        raise ProviderDirectoryRootedGraphAcquisitionError("disabled")
    runtime_dependencies = dependencies or default_dependencies()
    if (
        type(runtime_dependencies)
        is not ProviderDirectoryRootedGraphAcquisitionDependencies
    ):
        raise ValueError("provider_directory_rooted_graph_dependencies_invalid")
    return runtime_dependencies


async def _revalidated_snapshot(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies,
    database: Any,
    expected_source: tuple[object, ...] | None = None,
) -> ProviderDirectoryRootedGraphInputSnapshot:
    snapshot = await dependencies.revalidate_inputs(identity, database=database)
    if (
        type(snapshot) is not ProviderDirectoryRootedGraphInputSnapshot
        or not snapshot.is_identity_match(identity)
        or (
            expected_source is not None
            and snapshot.source_identity() != expected_source
        )
    ):
        raise ProviderDirectoryRootedGraphAcquisitionError("input_drift")
    return snapshot


async def _initialize_revalidated_root(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies,
    database: Any,
    expected_source: tuple[object, ...],
) -> ProviderDirectoryRootedGraphInputSnapshot:
    created_count = await dependencies.initialize_root(identity, database=database)
    if type(created_count) is not int or created_count not in {0, 1}:
        raise ProviderDirectoryRootedGraphAcquisitionError("state")
    snapshot = await _revalidated_snapshot(
        identity,
        dependencies=dependencies,
        database=database,
        expected_source=expected_source,
    )
    if snapshot.acquisition_status not in {"building", "sealed"}:
        raise ProviderDirectoryRootedGraphAcquisitionError("state")
    return snapshot


def _root_receipt(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    summary: ProviderDirectoryRootedGraphAcquisitionSummary,
    elapsed_seconds: float,
) -> ProviderDirectoryRootedGraphRootReceipt:
    if (
        summary.acquisition_id != identity.acquisition_id
        or summary.scope_id != identity.scope_id
        or summary.error_count != 0
    ):
        raise ProviderDirectoryRootedGraphAcquisitionError("state")
    return ProviderDirectoryRootedGraphRootReceipt(
        acquisition_role=identity.acquisition_role,
        acquisition_id=identity.acquisition_id,
        run_id=identity.run_id,
        completed_count=summary.completed_count,
        resource_count=summary.resource_count,
        edge_count=summary.edge_count,
        rooted_graph_sha256=summary.rooted_graph_sha256,
        elapsed_seconds=elapsed_seconds,
    )


async def _acquire_root(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    config: ProviderDirectoryRootedGraphAcquisitionConfig,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies,
    database: Any,
    expected_source: tuple[object, ...] | None = None,
) -> tuple[
    ProviderDirectoryRootedGraphAcquisitionSummary,
    float,
    tuple[object, ...],
]:
    initial_snapshot = await _revalidated_snapshot(
        identity,
        dependencies=dependencies,
        database=database,
        expected_source=expected_source,
    )
    source_identity = initial_snapshot.source_identity()
    snapshot = await _initialize_revalidated_root(
        identity,
        dependencies=dependencies,
        database=database,
        expected_source=source_identity,
    )
    summary, elapsed_seconds = await run_root(
        identity,
        snapshot,
        config=config,
        dependencies=dependencies,
        database=database,
    )
    return summary, elapsed_seconds, source_identity


async def _require_sealed_roots(
    identities: tuple[ProviderDirectoryRootedGraphAcquisitionIdentity, ...],
    *,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies,
    database: Any,
    expected_source: tuple[object, ...],
) -> None:
    for identity in identities:
        final_snapshot = await _revalidated_snapshot(
            identity,
            dependencies=dependencies,
            database=database,
            expected_source=expected_source,
        )
        if final_snapshot.acquisition_status != "sealed":
            raise ProviderDirectoryRootedGraphAcquisitionError("state")


def _pair_receipt(
    baseline: ProviderDirectoryRootedGraphAcquisitionIdentity,
    candidate: ProviderDirectoryRootedGraphAcquisitionIdentity,
    baseline_summary: ProviderDirectoryRootedGraphAcquisitionSummary,
    candidate_summary: ProviderDirectoryRootedGraphAcquisitionSummary,
    baseline_elapsed: float,
    candidate_elapsed: float,
    elapsed_seconds: float,
) -> ProviderDirectoryRootedGraphAcquisitionReceipt:
    baseline_receipt = _root_receipt(
        baseline,
        baseline_summary,
        baseline_elapsed,
    )
    candidate_receipt = _root_receipt(
        candidate,
        candidate_summary,
        candidate_elapsed,
    )
    return ProviderDirectoryRootedGraphAcquisitionReceipt(
        scope_id=baseline.scope_id,
        dataset_intent_id=baseline.dataset_intent_id,
        baseline=baseline_receipt,
        candidate=candidate_receipt,
        rooted_graphs_match=(
            baseline_receipt.rooted_graph_sha256
            == candidate_receipt.rooted_graph_sha256
        ),
        elapsed_seconds=elapsed_seconds,
    )


async def acquire_provider_directory_rooted_graph_twins(
    baseline_identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    candidate_identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    config: ProviderDirectoryRootedGraphAcquisitionConfig = (
        ProviderDirectoryRootedGraphAcquisitionConfig()
    ),
    database: Any = db,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies | None = None,
) -> ProviderDirectoryRootedGraphAcquisitionReceipt:
    """Acquire two isolated roots; return comparison evidence, never authority."""

    baseline, candidate = _validate_root_pair(
        baseline_identity,
        candidate_identity,
    )
    runtime = _runtime_dependencies(config, dependencies)
    started_at = runtime.monotonic()

    baseline_summary, baseline_elapsed, expected_source = await _acquire_root(
        baseline,
        config=config,
        dependencies=runtime,
        database=database,
    )
    candidate_summary, candidate_elapsed, _candidate_source = await _acquire_root(
        candidate,
        config=config,
        dependencies=runtime,
        database=database,
        expected_source=expected_source,
    )
    await _require_sealed_roots(
        (baseline, candidate),
        dependencies=runtime,
        database=database,
        expected_source=expected_source,
    )
    elapsed_seconds = runtime.monotonic() - started_at
    strict_nonnegative_seconds(elapsed_seconds, "elapsed_seconds")
    return _pair_receipt(
        baseline,
        candidate,
        baseline_summary,
        candidate_summary,
        baseline_elapsed,
        candidate_elapsed,
        elapsed_seconds,
    )


__all__ = (
    "acquire_provider_directory_rooted_graph_twins",
    "ProviderDirectoryRootedGraphAcquisitionConfig",
    "ProviderDirectoryRootedGraphAcquisitionDependencies",
    "ProviderDirectoryRootedGraphAcquisitionError",
    "ProviderDirectoryRootedGraphAcquisitionReceipt",
    "ProviderDirectoryRootedGraphInputSnapshot",
    "ProviderDirectoryRootedGraphRootReceipt",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ATTEMPTS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_CONCURRENCY",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ROOT_TIMEOUT_SECONDS",
)
