# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run network graph acquisition through the normal named SOURCE import path."""

import asyncio
from dataclasses import dataclass

from process import provider_directory_affiliation_hydration as hydration
from process import provider_directory_network_traversal as traversal
from process.provider_directory_generic_source_policy import current_source_storage_binding
from process.provider_directory_network_first_store import GraphStore, require
from process.provider_directory_source_storage_binding import NamedFHIRSourceStorage

RESOURCE_TYPES = ("Organization", "OrganizationAffiliation")
CONCURRENCY = 8
FETCH_MODE = "network_institutional_snapshot"


def is_source(directory, source):
    """Select only an explicitly configured, verified network snapshot source."""
    metadata = directory._source_metadata(source)
    if "provider_directory_network_snapshot" not in metadata:
        return False
    require(traversal.is_source(source), "source_mismatch")
    return True


async def _settle(operations, commit):
    """Commit completed pages serially; drain failures and join canceled requests."""
    index_by_task = {asyncio.ensure_future(operation): index for index, operation in enumerate(operations)}
    failure = None
    try:
        async for task in asyncio.as_completed(index_by_task):
            try:
                result = await task
                await commit(index_by_task[task], result)
            except Exception as exc:
                failure = failure or exc
    finally:
        for task in index_by_task:
            task.cancel()
        await asyncio.gather(*index_by_task, return_exceptions=True)
    if failure is not None:
        raise failure


def concurrency_limit(directory, source, requested):
    """Respect the requested, source and shared transport concurrency caps."""
    limit = min(
        CONCURRENCY,
        directory._validated_resource_scan_concurrency(requested),
        directory._source_resource_scan_concurrency(source),
    )
    session = directory._SOURCE_HTTP_SESSION.get()
    connector = session.connector if session is not None else None
    if connector is None:
        return 1
    caps = [value for value in (connector.limit, connector.limit_per_host) if value > 0]
    return min([limit, *caps])


@dataclass(frozen=True)
class IntakeOptions:
    """Normal worker limits for one candidate's graph acquisition."""

    run_id: str | None
    timeout: int
    page_count: int
    deadline_seconds: int
    requested_concurrency: int


class _GraphIntake:
    def __init__(self, directory, source_record, options, persist, progress_callback, cancel_check):
        self.directory, self.source = directory, source_record
        self.options, self.persist = options, persist
        self.progress_callback, self.cancel_check = progress_callback, cancel_check
        self.context = source_record.get("_pagination_checkpoint_context")
        require(
            isinstance(current_source_storage_binding(), NamedFHIRSourceStorage)
            and directory._has_bound_resource_scan_checkpoint(source_record, self.context)
            and self.context is not None
            and self.context.dataset_id == source_record.get("_endpoint_dataset_id")
            and self.context.acquisition_root_run_id
            and source_record.get("_resource_hash_contract"),
            "prepared_candidate_required",
        )
        source_record.setdefault("_provider_directory_request_pacing_state", {})
        self.http_concurrency = {"active": 0, "peak": 0}
        self.source = {
            **source_record,
            "_network_snapshot_run_id": options.run_id,
            "_http_concurrency_observation": self.http_concurrency,
        }
        self.concurrency = concurrency_limit(directory, source_record, options.requested_concurrency)
        self.store = GraphStore(directory, self.source)
        self.chain_peak = 0

    def _collection_url(self, resource_type):
        return (self.source.get("canonical_api_base") or self.source["api_base"]).rstrip("/") + "/" + resource_type

    async def _prepare(self):
        await self.cancel_check()
        await self.store.prepare(traversal.seed_work(self.source))
        async with self.store.transaction():
            for resource_type in RESOURCE_TYPES:
                start_url = self._collection_url(resource_type) + "?_count=" + str(self.options.page_count)
                await self.directory._load_or_initialize_pagination_checkpoint(self.context, resource_type, start_url)

    async def _checkpoints(self, *, complete=False):
        counts_by_type = await self.store.counts()
        pages = await self.store.connection.fetchval(f"SELECT pages FROM {self.store.table('state')} WHERE singleton")
        for resource_type in RESOURCE_TYPES:
            continuation = self._collection_url(resource_type) + "?_network_snapshot_cursor=" + str(pages)
            await self.directory._save_pagination_checkpoint(
                self.context,
                resource_type,
                next_url=None if complete else continuation,
                pages_processed=max(1, pages),
                rows_processed=counts_by_type.get(resource_type, 0),
                recent_url_hashes=[],
                completeness={
                    "network_snapshot": {
                        "dataset_id": self.context.dataset_id,
                        "pages": pages,
                        "cursor_kind": "local_frontier",
                    }
                },
            )
        return counts_by_type

    async def _commit_work(self, work_items, page_result):
        await self.cancel_check()
        async with self.store.transaction():
            await self.store.apply(work_items, page_result, self.persist)
            counts_by_type = await self._checkpoints()
        await self.progress_callback(
            {
                "resource_counts": counts_by_type,
                "phase": "network_graph",
                **self._concurrency_metrics(),
            }
        )

    def _concurrency_metrics(self):
        return {
            "concurrency_requested": self.options.requested_concurrency,
            "concurrency_limit": self.concurrency,
            "chain_concurrency_peak": self.chain_peak,
            "http_concurrency_peak": self.http_concurrency["peak"],
        }

    async def _run_wave(self, operations, work_batches):
        self.chain_peak = max(self.chain_peak, len(operations))

        async def commit(index, page_result):
            """Persist each successful response against its durable work batch."""
            await self._commit_work(work_batches[index], page_result)

        await _settle(operations, commit)

    async def _read_affiliations(self, expected_networks_by_id):
        entries = await hydration.hydrate(
            self.directory, self.source, expected_networks_by_id, timeout=self.options.timeout
        )
        return traversal.affiliation_result(self.source, entries)

    async def _has_processed_wave(self):
        async with self.store.transaction():
            work_items = await self.store.pending(limit=self.concurrency)
            organization_work = (
                []
                if work_items
                else await self.store.pending(organizations=True, limit=self.concurrency * hydration.BATCH_SIZE)
            )
            networks_by_id = (
                {}
                if work_items or organization_work
                else await self.store.pending_affiliations(self.concurrency * hydration.BATCH_SIZE)
            )
        if work_items:
            operations = [
                traversal.step(
                    self.directory, self.source, work, timeout=self.options.timeout, page_count=self.options.page_count
                )
                for work in work_items
            ]
            await self._run_wave(operations, [[work] for work in work_items])
        elif organization_work:
            batches = [
                organization_work[offset : offset + hydration.BATCH_SIZE]
                for offset in range(0, len(organization_work), hydration.BATCH_SIZE)
            ]
            operations = [
                traversal.read_organizations(
                    self.directory, self.source, [work["key"] for work in batch], timeout=self.options.timeout
                )
                for batch in batches
            ]
            await self._run_wave(operations, batches)
        elif networks_by_id:
            resource_ids = list(networks_by_id)
            batches = [
                {key: networks_by_id[key] for key in resource_ids[offset : offset + hydration.BATCH_SIZE]}
                for offset in range(0, len(resource_ids), hydration.BATCH_SIZE)
            ]
            await self._run_wave([self._read_affiliations(batch) for batch in batches], [[] for _ in batches])
        else:
            return False
        return True

    async def run(self):
        """Resume durable pages, reconcile closure and return normal worker counters."""
        await self._prepare()
        async with asyncio.timeout(self.options.deadline_seconds or None):
            while True:
                await self.cancel_check()
                if not await self._has_processed_wave():
                    break
            await self.cancel_check()
            async with self.store.transaction():
                proof = await self.store.finish()
                counts_by_type = await self._checkpoints(complete=True)
        return _group_summary(self.directory, self.source, proof, counts_by_type, self._concurrency_metrics())


def _group_summary(directory, source_record, proof, counts_by_type, concurrency):
    from process.provider_directory_network_snapshot_scope import make_scope

    config = directory._source_metadata(source_record)["provider_directory_network_snapshot"]
    scope = make_scope(
        root_name=config["name"],
        additional_names=list(config.get("name_search_characters", "")),
        owner_reference=config["owner_reference"],
        discovery_counts=proof,
    )
    diagnostics_by_type = {
        kind: {
            **concurrency,
            "fetch_mode": FETCH_MODE,
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
            "rows_fetched": counts_by_type.get(kind, 0),
            "rows_written": counts_by_type.get(kind, 0),
            "network_institutional_scope": scope,
        }
        for kind in RESOURCE_TYPES
    }
    stats_by_type = {
        kind: {
            "sources_attempted": 1,
            "sources_completed": 1,
            "sources_empty": int(counts_by_type.get(kind, 0) == 0),
            "collection_complete_sources": 1,
            "rows_fetched": counts_by_type.get(kind, 0),
        }
        for kind in RESOURCE_TYPES
    }
    return [source_record["source_id"]], diagnostics_by_type, counts_by_type, {}, stats_by_type, {}, {}


async def import_group(directory, source_record, options, *, persist, progress_callback, cancel_check):
    """Acquire one named candidate with normal persistence and cancellation callbacks."""
    return await _GraphIntake(directory, source_record, options, persist, progress_callback, cancel_check).run()
