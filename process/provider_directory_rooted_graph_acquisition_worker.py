# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lease-safe workers for one bounded rooted-graph acquisition."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import math
from typing import Any, Awaitable

from process.provider_directory_rooted_graph_acquisition_contract import (
    ProviderDirectoryRootedGraphAcquisitionConfig,
    ProviderDirectoryRootedGraphAcquisitionDependencies,
    ProviderDirectoryRootedGraphAcquisitionError,
    ProviderDirectoryRootedGraphInputSnapshot,
)
from process.provider_directory_rooted_graph_http import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS,
    ProviderDirectoryRootedGraphHTTPError,
    ProviderDirectoryRootedGraphHTTPResult,
)
from process.provider_directory_rooted_graph_result_contract import (
    build_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    ProviderDirectoryRootedGraphCensusClaim,
    ProviderDirectoryRootedGraphWorkClaim,
)


class _LeaseHeartbeatError(RuntimeError):
    pass


@dataclass(slots=True)
class _ClaimState:
    materialized: bool = False
    released: bool = False


async def drain_operation(
    operation: Awaitable[Any],
    *,
    preserve_cancellation: bool,
) -> Any:
    """Shield and drain one fence-changing operation through cancellation."""

    operation_task = asyncio.ensure_future(operation)
    cancellation: asyncio.CancelledError | None = None
    while not operation_task.done():
        try:
            await asyncio.shield(operation_task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
        except BaseException:
            break
    operation_result = operation_task.result()
    if cancellation is not None and preserve_cancellation:
        raise cancellation
    return operation_result


async def _cancel_and_drain(task: asyncio.Task[Any]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


class _RootRunner:
    def __init__(
        self,
        identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
        snapshot: ProviderDirectoryRootedGraphInputSnapshot,
        *,
        config: ProviderDirectoryRootedGraphAcquisitionConfig,
        dependencies: ProviderDirectoryRootedGraphAcquisitionDependencies,
        database: Any,
    ) -> None:
        self.identity = identity
        self.snapshot = snapshot
        self.config = config
        self.dependencies = dependencies
        self.database = database
        self._claim_lock = asyncio.Lock()
        self._no_delayed_retries = asyncio.Event()
        self._no_delayed_retries.set()
        self._delayed_retry_count = 0

    def _require_work_claim(
        self, claim: object
    ) -> ProviderDirectoryRootedGraphWorkClaim:
        if (
            type(claim) is not ProviderDirectoryRootedGraphWorkClaim
            or claim.acquisition_id != self.identity.acquisition_id
            or claim.scope_id != self.identity.scope_id
        ):
            raise ProviderDirectoryRootedGraphAcquisitionError("state")
        return claim

    async def claim(self) -> ProviderDirectoryRootedGraphWorkClaim | None:
        """Claim generic work only when no released retry is delayed."""

        while True:
            await self._no_delayed_retries.wait()
            claim = None
            try:
                async with self._claim_lock:
                    if not self._no_delayed_retries.is_set():
                        continue
                    claim = await self.dependencies.claim_work(
                        self.identity.acquisition_id,
                        lease_seconds=self.config.lease_seconds,
                        database=self.database,
                    )
                return None if claim is None else self._require_work_claim(claim)
            except asyncio.CancelledError:
                if claim is not None:
                    await self.release_unmaterialized(self._require_work_claim(claim))
                raise

    def _finish_delayed_retry(self) -> None:
        self._delayed_retry_count -= 1
        if self._delayed_retry_count < 0:
            raise ProviderDirectoryRootedGraphAcquisitionError("state")
        if self._delayed_retry_count == 0:
            self._no_delayed_retries.set()

    async def release_for_retry(
        self,
        claim: ProviderDirectoryRootedGraphWorkClaim,
        state: _ClaimState,
    ) -> None:
        """Release a transient failure before beginning its retry delay."""

        async with self._claim_lock:
            self._delayed_retry_count += 1
            self._no_delayed_retries.clear()
            try:
                await drain_operation(
                    self.dependencies.release_work(
                        claim,
                        database=self.database,
                    ),
                    preserve_cancellation=True,
                )
                state.released = True
            except BaseException:
                self._finish_delayed_retry()
                raise

    async def claim_retry(
        self,
        query_id: str,
        delay_seconds: float,
    ) -> ProviderDirectoryRootedGraphWorkClaim | None:
        """Reclaim the same exact query after its bounded delay."""

        claim = None
        try:
            await self.dependencies.sleep(delay_seconds)
            async with self._claim_lock:
                claim = await self.dependencies.claim_work(
                    self.identity.acquisition_id,
                    query_id=query_id,
                    lease_seconds=self.config.lease_seconds,
                    database=self.database,
                )
        finally:
            self._finish_delayed_retry()
        if claim is None:
            return None
        try:
            return self._require_work_claim(claim)
        except asyncio.CancelledError:
            await self.release_unmaterialized(self._require_work_claim(claim))
            raise

    async def release_unmaterialized(
        self,
        claim: ProviderDirectoryRootedGraphWorkClaim,
    ) -> None:
        """Best-effort release while preserving the caller's public failure."""

        try:
            await drain_operation(
                self.dependencies.release_work(
                    claim,
                    database=self.database,
                ),
                preserve_cancellation=False,
            )
        except Exception:
            return

    def retry_delay(
        self,
        error: ProviderDirectoryRootedGraphHTTPError,
        attempt: int,
    ) -> float:
        """Combine bounded exponential backoff with bounded Retry-After."""

        exponent = min(max(attempt - 1, 0), 30)
        exponential = min(
            float(self.config.retry_base_seconds) * (2**exponent),
            float(self.config.max_retry_seconds),
        )
        raw_retry_after = error.retry_after_seconds
        retry_after = (
            float(raw_retry_after)
            if not isinstance(raw_retry_after, bool)
            and isinstance(raw_retry_after, (int, float))
            and math.isfinite(float(raw_retry_after))
            else 0.0
        )
        return min(
            max(exponential, max(0.0, retry_after)),
            float(self.config.max_retry_seconds),
            PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS,
        )

    async def _fetch_with_heartbeat(
        self,
        session: Any,
        claim: ProviderDirectoryRootedGraphWorkClaim,
    ) -> ProviderDirectoryRootedGraphHTTPResult:
        fetch_task = asyncio.create_task(
            self.dependencies.fetch(
                session,
                self.snapshot.api_base,
                claim,
                bounds=self.config.http_bounds(),
            )
        )
        try:
            while True:
                done_tasks, _pending_tasks = await asyncio.wait(
                    {fetch_task},
                    timeout=float(self.config.heartbeat_seconds),
                )
                if done_tasks:
                    fetch_result = fetch_task.result()
                    if type(fetch_result) is not ProviderDirectoryRootedGraphHTTPResult:
                        raise ProviderDirectoryRootedGraphAcquisitionError("state")
                    return fetch_result
                try:
                    await self.dependencies.heartbeat(
                        claim,
                        lease_seconds=self.config.lease_seconds,
                        database=self.database,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise _LeaseHeartbeatError from None
        finally:
            if not fetch_task.done():
                await _cancel_and_drain(fetch_task)

    async def _terminalize(
        self,
        operation: Awaitable[Any],
        state: _ClaimState,
    ) -> None:
        async def _materialize() -> None:
            """Mark the lease durable only after the terminal write returns."""

            await operation
            state.materialized = True

        await drain_operation(_materialize(), preserve_cancellation=True)

    async def _complete_error(
        self,
        claim: ProviderDirectoryRootedGraphWorkClaim,
        error_code: str,
        state: _ClaimState,
    ) -> None:
        await self._terminalize(
            self.dependencies.complete_error(
                claim,
                error_code=error_code,
                database=self.database,
            ),
            state,
        )

    async def _fetch_or_retry(
        self,
        session: Any,
        claim: ProviderDirectoryRootedGraphWorkClaim,
        state: _ClaimState,
    ) -> ProviderDirectoryRootedGraphHTTPResult | tuple[str, float]:
        try:
            return await self._fetch_with_heartbeat(session, claim)
        except ProviderDirectoryRootedGraphHTTPError as error:
            if error.retryable and claim.attempt < self.config.max_attempts:
                await self.release_for_retry(claim, state)
                return claim.query_id, self.retry_delay(error, claim.attempt)
            terminal_code = "retry_exhausted" if error.retryable else error.code
            await self._complete_error(claim, terminal_code, state)
            raise ProviderDirectoryRootedGraphAcquisitionError("root_unsealable")
        except _LeaseHeartbeatError:
            raise ProviderDirectoryRootedGraphAcquisitionError("state") from None
        except (asyncio.CancelledError, ProviderDirectoryRootedGraphAcquisitionError):
            raise
        except Exception:
            await self._complete_error(claim, "transport_failure", state)
            raise ProviderDirectoryRootedGraphAcquisitionError(
                "root_unsealable"
            ) from None

    async def _complete_response(
        self,
        claim: ProviderDirectoryRootedGraphWorkClaim,
        response: ProviderDirectoryRootedGraphHTTPResult,
        state: _ClaimState,
        root_network_references: tuple[str, ...],
    ) -> None:
        if response.missing_http_status is not None:
            await self._terminalize(
                self.dependencies.complete_missing(
                    claim,
                    missing_http_status=response.missing_http_status,
                    missing_response_sha256=response.missing_response_sha256,
                    missing_response_bytes=response.total_bytes,
                    missing_response_json_text=response.missing_response_json_text,
                    database=self.database,
                ),
                state,
            )
            return
        try:
            query_result = build_provider_directory_rooted_graph_query_result(
                claim,
                response.resources,
                advertised_total=response.advertised_total,
                terminal_page_count=response.terminal_page_count,
                reachable_network_references=root_network_references,
            )
        except (TypeError, ValueError):
            await self._complete_error(claim, "response_invalid", state)
            raise ProviderDirectoryRootedGraphAcquisitionError(
                "root_unsealable"
            ) from None
        await self._terminalize(
            self.dependencies.complete_result(
                claim,
                query_result,
                database=self.database,
            ),
            state,
        )

    async def process_claim(
        self,
        session: Any,
        claim: ProviderDirectoryRootedGraphWorkClaim,
        *,
        root_network_references: tuple[str, ...] = (),
    ) -> tuple[str, float] | None:
        """Fetch and atomically terminalize one claim, or request a retry."""

        state = _ClaimState()
        try:
            if claim.attempt > self.config.max_attempts:
                await self._complete_error(claim, "retry_exhausted", state)
                raise ProviderDirectoryRootedGraphAcquisitionError("root_unsealable")
            fetch_outcome = await self._fetch_or_retry(session, claim, state)
            if type(fetch_outcome) is tuple:
                return fetch_outcome
            await self._complete_response(
                claim,
                fetch_outcome,
                state,
                root_network_references,
            )
            return None
        finally:
            if not state.materialized and not state.released:
                await self.release_unmaterialized(claim)

    async def worker(self, session: Any) -> None:
        """Drain generic claims, including delayed exact-query retries."""

        retry_request: tuple[str, float] | None = None
        while True:
            if retry_request is None:
                claim = await self.claim()
            else:
                query_id, delay_seconds = retry_request
                claim = await self.claim_retry(query_id, delay_seconds)
                retry_request = None
                if claim is None:
                    continue
            if claim is None:
                return
            retry_request = await self.process_claim(session, claim)

    async def drain_generic_frontier(self, session: Any) -> None:
        """Drain a delayed-retry-aware generic fixed point."""

        worker_tasks = [
            asyncio.create_task(self.worker(session))
            for _worker_index in range(self.config.concurrency)
        ]
        try:
            await asyncio.gather(*worker_tasks)
        except BaseException:
            for worker_task in worker_tasks:
                if not worker_task.done():
                    worker_task.cancel()
            await drain_operation(
                asyncio.gather(*worker_tasks, return_exceptions=True),
                preserve_cancellation=False,
            )
            raise

    async def _claim_census(
        self,
    ) -> ProviderDirectoryRootedGraphCensusClaim | None:
        census_claim = await self.dependencies.claim_census(
            self.identity,
            lease_seconds=self.config.lease_seconds,
            database=self.database,
        )
        if census_claim is None:
            return None
        if (
            type(census_claim) is not ProviderDirectoryRootedGraphCensusClaim
            or census_claim.work_claim.acquisition_id != self.identity.acquisition_id
            or census_claim.work_claim.scope_id != self.identity.scope_id
        ):
            raise ProviderDirectoryRootedGraphAcquisitionError("state")
        return census_claim

    async def process_census(self, session: Any) -> None:
        """Run the dedicated census or prove its prior completion."""

        census_claim = await self._claim_census()
        if census_claim is None:
            state = await self.dependencies.census_state(
                self.identity.acquisition_id,
                database=self.database,
            )
            if state != "completed":
                raise ProviderDirectoryRootedGraphAcquisitionError("root_unsealable")
            return
        expected_references = census_claim.root_network_references
        while True:
            retry_request = await self.process_claim(
                session,
                census_claim.work_claim,
                root_network_references=expected_references,
            )
            if retry_request is None:
                return
            _query_id, delay_seconds = retry_request
            try:
                await self.dependencies.sleep(delay_seconds)
                replayed_claim = await self._claim_census()
            finally:
                self._finish_delayed_retry()
            if (
                replayed_claim is None
                or replayed_claim.root_network_references != expected_references
            ):
                if replayed_claim is not None:
                    await self.release_unmaterialized(replayed_claim.work_claim)
                raise ProviderDirectoryRootedGraphAcquisitionError("state")
            census_claim = replayed_claim


__all__ = ("drain_operation", "_RootRunner")
