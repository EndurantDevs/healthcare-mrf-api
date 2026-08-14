# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded worker runtime for exact-cohort Flex Practitioner acquisition."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import inspect
import math
from typing import Any, AsyncIterator

import aiohttp

from process.uhc_flex_practitioner_acquisition_contract import (
    ProgressCallback,
    strict_nonnegative_seconds,
    UHCFlexPractitionerAcquisitionConfig,
    UHCFlexPractitionerAcquisitionDependencies,
    UHCFlexPractitionerAcquisitionError,
    UHCFlexPractitionerAcquisitionProgress,
    UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS,
)
from process.uhc_flex_official_cohort_store import (
    sync_uhc_flex_official_cohort,
)
from process.uhc_flex_practitioner_query import (
    UHC_FLEX_PRACTITIONER_MATCHED,
    UHC_FLEX_PRACTITIONER_UNMATCHED,
)
from process.uhc_flex_practitioner_registration import (
    register_uhc_flex_practitioner_source,
)
from process.uhc_flex_practitioner_store import (
    claim_uhc_flex_practitioner_work,
    complete_uhc_flex_practitioner_error,
    complete_uhc_flex_practitioner_result,
    initialize_uhc_flex_practitioner_acquisition,
    release_uhc_flex_practitioner_work,
    seal_uhc_flex_practitioner_acquisition,
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerAcquisitionSummary,
    UHCFlexPractitionerStoreError,
    UHCFlexPractitionerWorkClaim,
)
from process.uhc_flex_practitioner_transport import (
    fetch_uhc_flex_practitioner,
    UHCFlexPractitionerTransportError,
)


@asynccontextmanager
async def default_session_scope(
    connection_limit: int,
) -> AsyncIterator[aiohttp.ClientSession]:
    """Yield one identity-encoded, connection-bounded HTTP session."""

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


def default_dependencies() -> UHCFlexPractitionerAcquisitionDependencies:
    """Build the dormant production dependency surface."""

    # The admission store follows this orchestrator's prerequisite contracts.
    # Resolve it lazily so importing this dormant module cannot activate work.
    from process.uhc_flex_practitioner_twin_store import (
        admit_uhc_flex_practitioner_single_root,
        admit_uhc_flex_practitioner_twins,
    )

    return UHCFlexPractitionerAcquisitionDependencies(
        register_source=register_uhc_flex_practitioner_source,
        sync_cohort=sync_uhc_flex_official_cohort,
        initialize_root=initialize_uhc_flex_practitioner_acquisition,
        claim_work=claim_uhc_flex_practitioner_work,
        fetch=fetch_uhc_flex_practitioner,
        complete_result=complete_uhc_flex_practitioner_result,
        complete_error=complete_uhc_flex_practitioner_error,
        release_work=release_uhc_flex_practitioner_work,
        seal_root=seal_uhc_flex_practitioner_acquisition,
        admit_twins=admit_uhc_flex_practitioner_twins,
        session_scope=default_session_scope,
        admit_single_root=admit_uhc_flex_practitioner_single_root,
    )


async def drain_operation(
    operation: Awaitable[Any],
    *,
    preserve_cancellation: bool,
) -> Any:
    """Shield and drain one fence-changing operation through cancellation."""

    operation_task = asyncio.create_task(operation)
    cancellation: asyncio.CancelledError | None = None
    while not operation_task.done():
        try:
            await asyncio.shield(operation_task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
        except BaseException:
            break
    if cancellation is not None and preserve_cancellation:
        if not operation_task.cancelled():
            operation_task.exception()
        raise cancellation
    return operation_task.result()


@dataclass(slots=True)
class _AggregateCounters:
    claim_count: int = 0
    retry_count: int = 0
    matched_count: int = 0
    unmatched_count: int = 0
    error_count: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class _RootRunner:
    def __init__(
        self,
        identity: UHCFlexPractitionerAcquisitionIdentity,
        *,
        config: UHCFlexPractitionerAcquisitionConfig,
        dependencies: UHCFlexPractitionerAcquisitionDependencies,
        database: Any,
        progress_callback: ProgressCallback | None,
    ) -> None:
        self.identity = identity
        self.config = config
        self.dependencies = dependencies
        self.database = database
        self.progress_callback = progress_callback
        self.counters = _AggregateCounters()
        self._claim_lock = asyncio.Lock()
        self._delayed_retry_npis: set[int] = set()
        self._fresh_work_exhausted = False

    async def emit(self, phase: str) -> None:
        """Emit one aggregate-only progress observation."""

        callback = self.progress_callback
        if callback is None:
            return
        async with self.counters.lock:
            progress = UHCFlexPractitionerAcquisitionProgress(
                acquisition_role=self.identity.acquisition_role,
                phase=phase,
                worker_count=self.config.concurrency,
                claim_count=self.counters.claim_count,
                retry_count=self.counters.retry_count,
                matched_count=self.counters.matched_count,
                unmatched_count=self.counters.unmatched_count,
                error_count=self.counters.error_count,
            )
        try:
            callback_result = callback(progress)
            if inspect.isawaitable(callback_result):
                await callback_result
        except asyncio.CancelledError:
            raise
        except Exception:
            raise UHCFlexPractitionerAcquisitionError("progress") from None

    async def _record_claim(self) -> None:
        async with self.counters.lock:
            self.counters.claim_count += 1

    async def _record_retry(self) -> None:
        async with self.counters.lock:
            self.counters.retry_count += 1
        await self.emit("retry_released")

    async def _record_terminal(self, outcome: str) -> None:
        async with self.counters.lock:
            if outcome == UHC_FLEX_PRACTITIONER_MATCHED:
                self.counters.matched_count += 1
            elif outcome == UHC_FLEX_PRACTITIONER_UNMATCHED:
                self.counters.unmatched_count += 1
            elif outcome == "error":
                self.counters.error_count += 1
            else:
                raise UHCFlexPractitionerAcquisitionError("state")
        await self.emit("terminal")

    async def record_sealed(
        self,
        summary: UHCFlexPractitionerAcquisitionSummary,
    ) -> None:
        """Replace live counters with a sealed-root summary."""

        async with self.counters.lock:
            self.counters.matched_count = summary.matched_count
            self.counters.unmatched_count = summary.unmatched_count
            self.counters.error_count = summary.error_count
        await self.emit("root_sealed")

    async def claim(self) -> UHCFlexPractitionerWorkClaim | None:
        """Claim the next available member."""
        claim: UHCFlexPractitionerWorkClaim | None = None
        try:
            async with self._claim_lock:
                fresh_modes = (False,) if self._fresh_work_exhausted else (True, False)
                for fresh_only in fresh_modes:
                    claim = await self.dependencies.claim_work(
                        self.identity.acquisition_id,
                        excluded_npis=tuple(sorted(self._delayed_retry_npis)),
                        fresh_only=fresh_only, lease_seconds=self.config.lease_seconds,
                        database=self.database,
                    )
                    if claim is not None or fresh_only is False:
                        break
                    self._fresh_work_exhausted = True
            if claim is not None:
                if type(claim) is not UHCFlexPractitionerWorkClaim:
                    raise UHCFlexPractitionerAcquisitionError("state")
                await self._record_claim()
            return claim
        except asyncio.CancelledError:
            if claim is not None:
                await self.release_for_cancellation(claim)
            raise

    async def release_for_retry(
        self,
        claim: UHCFlexPractitionerWorkClaim,
    ) -> None:
        """Release a transiently failed claim before its retry delay."""

        async with self._claim_lock:
            self._delayed_retry_npis.add(claim.requested_npi)
            try:
                await drain_operation(
                    self.dependencies.release_work(
                        claim,
                        database=self.database,
                    ),
                    preserve_cancellation=True,
                )
            except BaseException:
                self._delayed_retry_npis.discard(claim.requested_npi)
                raise
        await self._record_retry()

    async def claim_retry(
        self,
        requested_npi: int,
        delay_seconds: float,
    ) -> UHCFlexPractitionerWorkClaim | None:
        """Reclaim one member after its bounded retry delay."""

        claim: UHCFlexPractitionerWorkClaim | None = None
        try:
            await self.dependencies.sleep(delay_seconds)
            async with self._claim_lock:
                claim = await self.dependencies.claim_work(
                    self.identity.acquisition_id, requested_npi=requested_npi,
                    lease_seconds=self.config.lease_seconds, database=self.database,
                )
            if claim is not None:
                if type(claim) is not UHCFlexPractitionerWorkClaim:
                    raise UHCFlexPractitionerAcquisitionError("state")
                await self._record_claim()
            return claim
        except asyncio.CancelledError:
            if claim is not None:
                await self.release_for_cancellation(claim)
            raise
        finally:
            self._delayed_retry_npis.discard(requested_npi)

    async def release_for_cancellation(
        self,
        claim: UHCFlexPractitionerWorkClaim,
    ) -> None:
        """Best-effort release a fenced claim while preserving cancellation."""

        try:
            await drain_operation(
                self.dependencies.release_work(
                    claim,
                    database=self.database,
                ),
                preserve_cancellation=False,
            )
        except Exception:
            # Cancellation remains the public outcome. An expired exact lease
            # is still reclaimable if the immediate release lost its fence.
            return

    def retry_delay(
        self,
        error: UHCFlexPractitionerTransportError,
        attempt: int,
    ) -> float:
        """Calculate the bounded exponential and server-directed retry delay."""

        exponent = min(max(attempt - 1, 0), 30)
        exponential_delay = min(
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
        retry_after = max(0.0, retry_after)
        return min(
            max(exponential_delay, retry_after),
            float(self.config.max_retry_seconds),
            UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS,
        )

    async def terminal_error(
        self,
        claim: UHCFlexPractitionerWorkClaim,
        error_code: str,
    ) -> None:
        """Persist one bounded terminal error for a claimed member."""

        try:
            await self.dependencies.complete_error(
                claim,
                error_code=error_code,
                database=self.database,
            )
        except asyncio.CancelledError:
            await self.release_for_cancellation(claim)
            raise
        await self._record_terminal("error")

    async def release_final_retryable(self, claim: UHCFlexPractitionerWorkClaim) -> None:
        """Release the final retryable lease if it is still owned."""

        async with self._claim_lock:
            self._delayed_retry_npis.add(claim.requested_npi)
            try:
                await drain_operation(
                    self.dependencies.release_work(
                        claim,
                        database=self.database,
                    ),
                    preserve_cancellation=True,
                )
            except BaseException as error:
                if not isinstance(error, UHCFlexPractitionerStoreError) or error.code != "lease_lost":
                    self._delayed_retry_npis.discard(claim.requested_npi)
                    raise
            else:
                await self._record_retry()

    async def process_claim(
        self,
        session: Any,
        claim: UHCFlexPractitionerWorkClaim,
        invocation_attempt: int = 1,
    ) -> tuple[int, float] | None:
        """Fetch and terminalize a claim or return its retry request."""

        try:
            query_result = await self.dependencies.fetch(
                session,
                claim.requested_npi,
            )
        except asyncio.CancelledError:
            await self.release_for_cancellation(claim)
            raise
        except UHCFlexPractitionerTransportError as error:
            if error.retryable:
                if invocation_attempt < self.config.max_attempts:
                    retry_delay = self.retry_delay(error, invocation_attempt)
                    await self.release_for_retry(claim)
                    return claim.requested_npi, retry_delay
                await self.release_final_retryable(claim)
                raise UHCFlexPractitionerAcquisitionError("root_retryable")
            terminal_code = (
                f"response_validation_{error.validation_code}"
                if error.validation_code is not None
                else error.code
            )
            await self.terminal_error(claim, terminal_code)
            raise UHCFlexPractitionerAcquisitionError("root_unsealable")
        except Exception:
            await self.terminal_error(claim, "transport_failure")
            raise UHCFlexPractitionerAcquisitionError("root_unsealable")
        try:
            await self.dependencies.complete_result(
                claim,
                query_result,
                database=self.database,
            )
        except asyncio.CancelledError:
            await self.release_for_cancellation(claim)
            raise
        await self._record_terminal(query_result.outcome)
        return None

    async def worker(self, session: Any) -> None:
        """Consume exact cohort claims until the root has no remaining work."""

        retry_request: tuple[int, float] | None = None
        invocation_attempt = 1
        while True:
            if retry_request is None:
                claim = await self.claim()
                invocation_attempt = 1
            else:
                requested_npi, delay_seconds = retry_request
                claim = await self.claim_retry(requested_npi, delay_seconds)
                retry_request = None
                if claim is None:
                    continue
                invocation_attempt += 1
            if claim is None:
                return
            try:
                retry_request = await self.process_claim(
                    session,
                    claim,
                    invocation_attempt,
                )
            except UHCFlexPractitionerAcquisitionError as error:
                if error.code != "root_retryable":
                    raise
                try:
                    await self.dependencies.sleep(UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS)
                finally:
                    self._delayed_retry_npis.discard(claim.requested_npi)
                retry_request = None


async def gather_worker_cleanup(tasks: list[asyncio.Task[None]]) -> None:
    """Drain cancelled worker tasks without replacing their public error."""

    await asyncio.gather(*tasks, return_exceptions=True)


async def run_root(
    identity: UHCFlexPractitionerAcquisitionIdentity,
    *,
    config: UHCFlexPractitionerAcquisitionConfig,
    dependencies: UHCFlexPractitionerAcquisitionDependencies,
    database: Any,
    progress_callback: ProgressCallback | None,
) -> tuple[UHCFlexPractitionerAcquisitionSummary, float]:
    """Run, seal, and time one independently acquired cohort root."""

    runner = _RootRunner(
        identity,
        config=config,
        dependencies=dependencies,
        database=database,
        progress_callback=progress_callback,
    )
    started_at = dependencies.monotonic()
    await runner.emit("root_started")
    async with dependencies.session_scope(config.concurrency) as session:
        worker_tasks = [
            asyncio.create_task(runner.worker(session))
            for _worker_index in range(config.concurrency)
        ]
        try:
            await asyncio.gather(*worker_tasks)
        except BaseException:
            for worker_task in worker_tasks:
                if not worker_task.done():
                    worker_task.cancel()
            await drain_operation(
                gather_worker_cleanup(worker_tasks),
                preserve_cancellation=False,
            )
            raise
    summary = await dependencies.seal_root(identity, database=database)
    if type(summary) is not UHCFlexPractitionerAcquisitionSummary:
        raise UHCFlexPractitionerAcquisitionError("state")
    elapsed_seconds = dependencies.monotonic() - started_at
    strict_nonnegative_seconds(elapsed_seconds, "root timing")
    await runner.record_sealed(summary)
    return summary, elapsed_seconds
