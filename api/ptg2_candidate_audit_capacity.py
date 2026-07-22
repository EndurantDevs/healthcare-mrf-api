# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Decoded-memory accounting for bounded PTG candidate-audit retention."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable

from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES = 64 * 1024 * 1024
PTG2_CANDIDATE_AUDIT_DEFAULT_PROCESS_BYTES = 1024 * 1024 * 1024
PTG2_CANDIDATE_AUDIT_MAX_PROCESS_BYTES = 8 * 1024 * 1024 * 1024
INTEGER_KEY_SET_BYTES = 256
INTEGER_KEY_SET_MEMBERSHIP_BYTES = 112
INTEGER_KEY_ORDERING_BYTES = 112
INTEGER_KEY_ORDERING_MEMBERSHIP_BYTES = 16
INTEGER_KEY_TUPLE_BYTES = 56
INTEGER_KEY_TUPLE_MEMBERSHIP_BYTES = 8


class CandidateAuditDecodedRetentionError(PTG2ManifestArtifactError):
    """Raised before decoded candidate state can exceed its memory budget."""


class CandidateAuditProcessAdmissionError(RuntimeError):
    """Raised when one partition cannot fit the process audit allowance."""

    def __init__(self, requested_bytes: int, maximum_bytes: int) -> None:
        self.requested_bytes = int(requested_bytes)
        self.maximum_bytes = int(maximum_bytes)
        super().__init__(
            "PTG2 candidate audit partition requires "
            f"{self.requested_bytes} admitted bytes but the process limit is "
            f"{self.maximum_bytes}"
        )


class CandidateAuditProcessConfigurationError(RuntimeError):
    """Raised when the process-wide audit allowance is not usable."""


@dataclass
class CandidateAuditDecodedRetentionBudget:
    """Conservatively account request-local decoded objects retained together."""

    maximum_bytes: int = PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES
    retained_bytes: int = 0
    peak_retained_bytes: int = 0

    def __post_init__(self) -> None:
        if type(self.maximum_bytes) is not int or self.maximum_bytes < 1:
            raise ValueError("candidate audit decoded-retention limit must be positive")

    def claim(self, byte_count: int, *, category: str) -> None:
        """Reserve a conservative decoded allocation before retaining it."""

        normalized_count = int(byte_count)
        if normalized_count < 0:
            raise ValueError("candidate audit decoded-retention claim is invalid")
        if normalized_count > self.maximum_bytes - self.retained_bytes:
            raise CandidateAuditDecodedRetentionError(
                "PTG2 candidate audit decoded retention exceeds the byte limit "
                f"while retaining {category}"
            )
        self.retained_bytes += normalized_count
        self.peak_retained_bytes = max(
            self.peak_retained_bytes,
            self.retained_bytes,
        )

    def release(self, byte_count: int) -> None:
        """Release a temporary decoded allocation after its final consumer."""

        normalized_count = int(byte_count)
        if normalized_count < 0 or normalized_count > self.retained_bytes:
            raise ValueError("candidate audit decoded-retention release is invalid")
        self.retained_bytes -= normalized_count


def retain_unique_integer_keys(
    integer_keys: Iterable[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    *,
    category: str,
) -> tuple[tuple[int, ...], int]:
    """Normalize integer keys with their set-to-tuple peak claimed."""

    if retention_budget is None:
        return tuple(sorted({int(integer_key) for integer_key in integer_keys})), 0
    retained_set_bytes = INTEGER_KEY_SET_BYTES
    retention_budget.claim(
        retained_set_bytes,
        category=f"the {category} set",
    )
    normalized_keys: set[int] = set()
    ordering_bytes = 0
    try:
        for raw_integer_key in integer_keys:
            integer_key = int(raw_integer_key)
            if integer_key in normalized_keys:
                continue
            retention_budget.claim(
                INTEGER_KEY_SET_MEMBERSHIP_BYTES,
                category=f"a {category} key",
            )
            retained_set_bytes += INTEGER_KEY_SET_MEMBERSHIP_BYTES
            normalized_keys.add(integer_key)
        ordering_bytes = (
            INTEGER_KEY_ORDERING_BYTES
            + len(normalized_keys) * INTEGER_KEY_ORDERING_MEMBERSHIP_BYTES
        )
        retention_budget.claim(
            ordering_bytes,
            category=f"the ordered {category} keys",
        )
        ordered_keys = tuple(sorted(normalized_keys))
    except BaseException:
        retention_budget.release(retained_set_bytes + ordering_bytes)
        raise
    retained_tuple_bytes = (
        INTEGER_KEY_TUPLE_BYTES + len(ordered_keys) * INTEGER_KEY_TUPLE_MEMBERSHIP_BYTES
    )
    retention_budget.release(retained_set_bytes + ordering_bytes - retained_tuple_bytes)
    return ordered_keys, retained_tuple_bytes


def retain_unique_integer_key_set(
    integer_keys: Iterable[int],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    *,
    category: str,
) -> tuple[set[int], int]:
    """Retain unique integer keys only after each membership is claimed."""

    retained_bytes = INTEGER_KEY_SET_BYTES
    if retention_budget is not None:
        retention_budget.claim(retained_bytes, category=f"the {category} set")
    retained_keys: set[int] = set()
    try:
        for raw_integer_key in integer_keys:
            integer_key = int(raw_integer_key)
            if integer_key in retained_keys:
                continue
            if retention_budget is not None:
                retention_budget.claim(
                    INTEGER_KEY_SET_MEMBERSHIP_BYTES,
                    category=f"a {category} key",
                )
                retained_bytes += INTEGER_KEY_SET_MEMBERSHIP_BYTES
            retained_keys.add(integer_key)
    except BaseException:
        if retention_budget is not None:
            retention_budget.release(retained_bytes)
        raise
    return retained_keys, retained_bytes


@dataclass(frozen=True)
class CandidateAuditAdmissionSnapshot:
    """Observable process-local weighted-admission counters."""

    maximum_bytes: int
    retained_bytes: int
    peak_retained_bytes: int
    queued_count: int
    admitted_count: int
    waited_count: int
    cancelled_count: int
    rejected_count: int
    released_count: int


@dataclass
class _CandidateAuditAdmissionWaiter:
    weight_bytes: int
    future: asyncio.Future[None]
    granted: bool = False


class CandidateAuditProcessAdmissionLease:
    """One exact process-byte reservation with synchronous safe release."""

    def __init__(
        self,
        admission: CandidateAuditProcessAdmission,
        weight_bytes: int,
        *,
        waited: bool,
    ) -> None:
        self._admission = admission
        self.weight_bytes = int(weight_bytes)
        self.waited = bool(waited)
        self._released = False

    @property
    def is_released(self) -> bool:
        """Return whether this lease has already returned its weight."""

        return self._released

    def release(self) -> None:
        """Return this reservation exactly once without another await point."""

        if self._released:
            raise RuntimeError("candidate audit admission lease was released twice")
        self._released = True
        self._admission._release(self.weight_bytes)

    async def __aenter__(self) -> CandidateAuditProcessAdmissionLease:
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


class CandidateAuditProcessAdmission:
    """FIFO process-local gate for raw plus decoded partition allowances."""

    def __init__(self, maximum_bytes: int) -> None:
        normalized_maximum = int(maximum_bytes)
        if (
            type(maximum_bytes) is not int
            or normalized_maximum < 1
            or normalized_maximum > PTG2_CANDIDATE_AUDIT_MAX_PROCESS_BYTES
        ):
            raise ValueError("candidate audit process-byte limit is invalid")
        self.maximum_bytes = normalized_maximum
        self._retained_bytes = 0
        self._peak_retained_bytes = 0
        self._waiters: list[_CandidateAuditAdmissionWaiter] = []
        self._admitted_count = 0
        self._waited_count = 0
        self._cancelled_count = 0
        self._rejected_count = 0
        self._released_count = 0

    @property
    def snapshot(self) -> CandidateAuditAdmissionSnapshot:
        """Return immutable truthful counters for metrics and diagnostics."""

        return CandidateAuditAdmissionSnapshot(
            maximum_bytes=self.maximum_bytes,
            retained_bytes=self._retained_bytes,
            peak_retained_bytes=self._peak_retained_bytes,
            queued_count=len(self._waiters),
            admitted_count=self._admitted_count,
            waited_count=self._waited_count,
            cancelled_count=self._cancelled_count,
            rejected_count=self._rejected_count,
            released_count=self._released_count,
        )

    async def acquire(
        self,
        weight_bytes: int,
    ) -> CandidateAuditProcessAdmissionLease:
        """Wait fairly for one weight and unwind cancellation races safely."""

        normalized_weight = int(weight_bytes)
        if type(weight_bytes) is not int or normalized_weight < 1:
            raise ValueError("candidate audit admission weight is invalid")
        if normalized_weight > self.maximum_bytes:
            self._rejected_count += 1
            raise CandidateAuditProcessAdmissionError(
                normalized_weight,
                self.maximum_bytes,
            )
        if (
            not self._waiters
            and normalized_weight <= self.maximum_bytes - self._retained_bytes
        ):
            self._grant(normalized_weight)
            return self._new_lease(normalized_weight, waited=False)
        loop = asyncio.get_running_loop()
        waiter = _CandidateAuditAdmissionWaiter(
            weight_bytes=normalized_weight,
            future=loop.create_future(),
        )
        self._waiters.append(waiter)
        self._waited_count += 1
        self._grant_waiters()
        try:
            await waiter.future
        except BaseException:
            if waiter.granted:
                self._cancelled_count += 1
                self._release(normalized_weight)
            else:
                if self._has_removed_waiter(waiter):
                    self._cancelled_count += 1
                self._grant_waiters()
            raise
        return self._new_lease(normalized_weight, waited=True)

    def _new_lease(
        self,
        weight_bytes: int,
        *,
        waited: bool,
    ) -> CandidateAuditProcessAdmissionLease:
        """Construct one lease or return its already-granted weight."""

        try:
            return CandidateAuditProcessAdmissionLease(
                self,
                weight_bytes,
                waited=waited,
            )
        except BaseException:
            self._release(weight_bytes)
            raise

    def _grant(self, weight_bytes: int) -> None:
        self._retained_bytes += weight_bytes
        self._peak_retained_bytes = max(
            self._peak_retained_bytes,
            self._retained_bytes,
        )
        self._admitted_count += 1

    def _grant_waiters(self) -> None:
        while self._waiters:
            waiter = self._waiters[0]
            if waiter.future.cancelled():
                self._waiters.pop(0)
                self._cancelled_count += 1
                continue
            if waiter.weight_bytes > self.maximum_bytes - self._retained_bytes:
                return
            self._waiters.pop(0)
            waiter.granted = True
            self._grant(waiter.weight_bytes)
            waiter.future.set_result(None)

    def _has_removed_waiter(
        self,
        waiter: _CandidateAuditAdmissionWaiter,
    ) -> bool:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            return False
        return True

    def _release(self, weight_bytes: int) -> None:
        normalized_weight = int(weight_bytes)
        if normalized_weight < 1 or normalized_weight > self._retained_bytes:
            raise RuntimeError("candidate audit process-byte release is invalid")
        self._retained_bytes -= normalized_weight
        self._released_count += 1
        self._grant_waiters()


__all__ = [
    "CandidateAuditAdmissionSnapshot",
    "CandidateAuditDecodedRetentionBudget",
    "CandidateAuditDecodedRetentionError",
    "CandidateAuditProcessAdmission",
    "CandidateAuditProcessAdmissionError",
    "CandidateAuditProcessConfigurationError",
    "CandidateAuditProcessAdmissionLease",
    "PTG2_CANDIDATE_AUDIT_DEFAULT_PROCESS_BYTES",
    "PTG2_CANDIDATE_AUDIT_MAX_PROCESS_BYTES",
    "PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES",
    "retain_unique_integer_key_set",
    "retain_unique_integer_keys",
]
