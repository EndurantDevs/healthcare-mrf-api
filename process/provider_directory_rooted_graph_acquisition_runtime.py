# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source fences and bounded execution for dormant rooted-graph acquisition."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import aiohttp

from db.connection import db
from process.provider_directory_rooted_graph_acquisition_contract import (
    CENSUS_STATES,
    ProviderDirectoryRootedGraphAcquisitionConfig,
    ProviderDirectoryRootedGraphAcquisitionDependencies,
    ProviderDirectoryRootedGraphAcquisitionError,
    ProviderDirectoryRootedGraphInputSnapshot,
    strict_nonnegative_seconds,
)
from process.provider_directory_rooted_graph_acquisition_worker import (
    _RootRunner,
    drain_operation,
)
from process.provider_directory_rooted_graph_http import (
    fetch_provider_directory_rooted_graph_query,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
)
from process.provider_directory_rooted_graph_store import (
    claim_provider_directory_rooted_graph_census,
    claim_provider_directory_rooted_graph_work,
    complete_provider_directory_rooted_graph_error,
    complete_provider_directory_rooted_graph_missing,
    complete_provider_directory_rooted_graph_result,
    heartbeat_provider_directory_rooted_graph_work,
    initialize_provider_directory_rooted_graph_acquisition,
    release_provider_directory_rooted_graph_work,
    seal_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
)
from process.provider_directory_rooted_graph_store_support import (
    ACQUISITION_TABLE,
    WORK_TABLE,
    assert_identity_row,
    row_fields,
    table_ref,
)


@asynccontextmanager
async def default_session_scope(
    connection_limit: int,
) -> AsyncIterator[aiohttp.ClientSession]:
    """Yield one isolated verified-TLS, identity-encoded client session."""

    connector = aiohttp.TCPConnector(
        limit=connection_limit,
        limit_per_host=connection_limit,
        ttl_dns_cache=300,
    )
    async with aiohttp.ClientSession(
        connector=connector,
        auto_decompress=False,
        cookie_jar=aiohttp.DummyCookieJar(),
        headers={"Accept-Encoding": "identity"},
        skip_auto_headers={"Accept-Encoding"},
        trust_env=False,
    ) as session:
        yield session


async def _locked_acquisition_status(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    database: Any,
    lock_identity: str,
    pair_factory: Any,
    current_selector: Any,
    current_matcher: Any,
) -> str:
    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:identity, 0));",
            identity=lock_identity,
        )
        current = await current_selector(database, pair=pair_factory())
        if not current_matcher(current, identity):
            raise ProviderDirectoryRootedGraphAcquisitionError("input_drift")
        acquisition_row = await database.first(
            f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
            "WHERE acquisition_id = :acquisition_id FOR SHARE;",
            acquisition_id=identity.acquisition_id,
        )
        if acquisition_row is None:
            return "absent"
        return assert_identity_row(identity, acquisition_row)["status"]


def _snapshot_from_identity(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    status: str,
) -> ProviderDirectoryRootedGraphInputSnapshot:
    from process.provider_directory_rooted_graph_source_contract import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    )

    return ProviderDirectoryRootedGraphInputSnapshot(
        api_base=PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        root_dataset_variant=identity.root_dataset_variant,
        root_publication_contract_id=identity.root_publication_contract_id,
        root_source_id=identity.root_source_id,
        root_endpoint_id=identity.root_endpoint_id,
        acquisition_source_id=identity.acquisition_source_id,
        acquisition_endpoint_id=identity.acquisition_endpoint_id,
        source_authority_id=identity.source_authority_id,
        endpoint_signature_sha256=identity.endpoint_signature_sha256,
        root_dataset_id=identity.root_dataset_id,
        root_dataset_hash=identity.root_dataset_hash,
        root_content_proof_sha256=identity.root_content_proof_sha256,
        root_resource_count=identity.root_resource_count,
        root_cohort_id=identity.root_cohort_id,
        max_work_items=identity.max_work_items,
        max_resource_rows=identity.max_resource_rows,
        max_edge_rows=identity.max_edge_rows,
        max_payload_bytes=identity.max_payload_bytes,
        acquisition_status=status,
    )


async def revalidate_provider_directory_rooted_graph_inputs(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    database: Any = db,
) -> ProviderDirectoryRootedGraphInputSnapshot:
    """Lock and revalidate exactly one reviewed legacy/rooted current."""

    if type(identity) is not ProviderDirectoryRootedGraphAcquisitionIdentity:
        raise ValueError("provider_directory_rooted_graph_identity_invalid")
    try:
        from process.provider_directory_dataset_scoped_publication import (
            EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
            exact_current_matches_root,
            exact_uhc_dataset_pair,
            lock_exact_current_dataset,
        )

        status = await _locked_acquisition_status(
            identity,
            database,
            EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
            exact_uhc_dataset_pair,
            lock_exact_current_dataset,
            exact_current_matches_root,
        )
        return _snapshot_from_identity(identity, status)
    except ProviderDirectoryRootedGraphAcquisitionError:
        raise
    except (ImportError, TypeError, ValueError, RuntimeError):
        raise ProviderDirectoryRootedGraphAcquisitionError("input_drift") from None


async def provider_directory_rooted_graph_census_state(
    acquisition_id: str,
    *,
    database: Any = db,
) -> str:
    """Distinguish a completed census from every non-claimable state."""

    fields = row_fields(
        await database.first(
            f"""
            SELECT count(*)::bigint AS census_count,
                   min(status) AS census_status
              FROM {table_ref(WORK_TABLE)}
             WHERE acquisition_id = :acquisition_id
               AND kind = 'full_insurance_plan_census';
            """,
            acquisition_id=acquisition_id,
        )
    )
    count = fields.get("census_count")
    status = fields.get("census_status")
    if count == 0 and status is None:
        return "absent"
    if count == 1 and status in CENSUS_STATES - {"absent"}:
        return status
    raise ProviderDirectoryRootedGraphAcquisitionError("state")


def default_dependencies() -> ProviderDirectoryRootedGraphAcquisitionDependencies:
    """Build the dormant production dependency surface without running work."""

    return ProviderDirectoryRootedGraphAcquisitionDependencies(
        revalidate_inputs=revalidate_provider_directory_rooted_graph_inputs,
        initialize_root=initialize_provider_directory_rooted_graph_acquisition,
        claim_work=claim_provider_directory_rooted_graph_work,
        claim_census=claim_provider_directory_rooted_graph_census,
        census_state=provider_directory_rooted_graph_census_state,
        fetch=fetch_provider_directory_rooted_graph_query,
        heartbeat=heartbeat_provider_directory_rooted_graph_work,
        complete_result=complete_provider_directory_rooted_graph_result,
        complete_missing=complete_provider_directory_rooted_graph_missing,
        complete_error=complete_provider_directory_rooted_graph_error,
        release_work=release_provider_directory_rooted_graph_work,
        seal_root=seal_provider_directory_rooted_graph_acquisition,
        session_scope=default_session_scope,
    )


async def run_root(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    snapshot: ProviderDirectoryRootedGraphInputSnapshot,
    *,
    config: ProviderDirectoryRootedGraphAcquisitionConfig,
    dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies,
    database: Any,
) -> tuple[ProviderDirectoryRootedGraphAcquisitionSummary, float]:
    """Drain root closure, census, derived plan closure, then seal."""

    runner = _RootRunner(
        identity,
        snapshot,
        config=config,
        dependencies=dependencies,
        database=database,
    )
    started_at = dependencies.monotonic()
    try:
        async with asyncio.timeout(float(config.root_timeout_seconds)):
            async with dependencies.session_scope(config.concurrency) as session:
                await runner.drain_generic_frontier(session)
                await runner.process_census(session)
                await runner.drain_generic_frontier(session)
            summary = await dependencies.seal_root(identity, database=database)
    except TimeoutError:
        raise ProviderDirectoryRootedGraphAcquisitionError("root_unsealable") from None
    if (
        type(summary) is not ProviderDirectoryRootedGraphAcquisitionSummary
        or summary.acquisition_id != identity.acquisition_id
        or summary.scope_id != identity.scope_id
    ):
        raise ProviderDirectoryRootedGraphAcquisitionError("state")
    elapsed_seconds = dependencies.monotonic() - started_at
    strict_nonnegative_seconds(elapsed_seconds, "root_elapsed_seconds")
    return summary, elapsed_seconds


__all__ = (
    "default_dependencies",
    "default_session_scope",
    "drain_operation",
    "provider_directory_rooted_graph_census_state",
    "revalidate_provider_directory_rooted_graph_inputs",
    "run_root",
)
