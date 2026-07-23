from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from api.endpoint import pricing
from api import ptg2_candidate_audit_capacity as capacity
from api.ptg2_candidate_audit_capacity import (
    PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES,
    PTG2_CANDIDATE_AUDIT_MAX_PROCESS_BYTES,
    PTG2_CANDIDATE_AUDIT_PARTITION_MAX_RETAINED_DECODED_BYTES,
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
    CandidateAuditProcessAdmission,
    CandidateAuditProcessAdmissionError,
    retain_unique_integer_key_set,
    retain_unique_integer_keys,
)
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


class _AuditAbort(BaseException):
    pass


class _LeaseConstructionAbort(BaseException):
    pass


def test_default_partition_capacity_fits_one_process_admission_slot():
    raw_limit = pricing._candidate_audit_batch_raw_limit()
    partition_weight = pricing._candidate_audit_partition_weight(raw_limit)

    assert raw_limit == 512 * 1024 * 1024
    assert partition_weight == 1024 * 1024 * 1024
    assert partition_weight <= pricing.PTG2_CANDIDATE_AUDIT_DEFAULT_PROCESS_BYTES
    assert (
        2 * partition_weight
        > pricing.PTG2_CANDIDATE_AUDIT_DEFAULT_PROCESS_BYTES
    )


def test_dense_partition_has_bounded_headroom_without_changing_legacy_cap():
    assert PTG2_CANDIDATE_AUDIT_MAX_RETAINED_DECODED_BYTES == 64 * 1024 * 1024
    assert (
        PTG2_CANDIDATE_AUDIT_PARTITION_MAX_RETAINED_DECODED_BYTES
        == 512 * 1024 * 1024
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=PTG2_CANDIDATE_AUDIT_PARTITION_MAX_RETAINED_DECODED_BYTES
    )

    budget.claim(384 * 1024 * 1024, category="a dense provider/code request")
    budget.claim(128 * 1024 * 1024, category="bounded headroom")
    with pytest.raises(CandidateAuditDecodedRetentionError, match="byte limit"):
        budget.claim(1, category="an unbounded extra byte")

    assert budget.retained_bytes == 512 * 1024 * 1024
    assert budget.peak_retained_bytes == 512 * 1024 * 1024


@pytest.mark.parametrize("maximum_bytes", (0, -1, True))
def test_decoded_retention_budget_rejects_invalid_limits(maximum_bytes):
    with pytest.raises(ValueError, match="limit must be positive"):
        CandidateAuditDecodedRetentionBudget(maximum_bytes=maximum_bytes)


def test_audit_counts_reject_incomplete_persisted_occurrence_validation():
    batch_result = SimpleNamespace(
        matched_challenge_count=1,
        persisted_audit_occurrence_count=1,
        validated_persisted_audit_occurrence_count=0,
    )
    audit_request = SimpleNamespace(challenge_count=1)

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="incomplete persisted occurrence count",
    ):
        pricing._validate_candidate_audit_batch_counts(batch_result, audit_request)


@pytest.mark.asyncio
async def test_candidate_audit_endpoint_rejects_oversized_request_before_auth():
    request = SimpleNamespace(body=b"x" * (2 * 1024 * 1024 + 1))

    with pytest.raises(Exception, match="request is too large") as exc_info:
        await pricing.audit_ptg2_source_witness_batch(request)

    assert type(exc_info.value).__name__ == "BadRequest"


def test_raw_audit_capacity_rejects_non_positive_configuration(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_AUDIT_BATCH_MAX_RAW_BYTES", "0")

    with pytest.raises(PTG2SharedBlockError, match="must be positive"):
        pricing._candidate_audit_batch_raw_limit()


def test_partition_access_rejects_non_mapping_payload():
    with pytest.raises(
        Exception,
        match="audit_batch_request_fields_invalid",
    ) as exc_info:
        pricing._candidate_audit_batch_access(object(), ())

    assert exc_info.value.status_code == 400


def test_partition_access_requires_verified_candidate_scope(monkeypatch):
    monkeypatch.setattr(
        pricing,
        "candidate_audit_access_from_verified_request",
        lambda *_args: None,
    )

    with pytest.raises(
        Exception,
        match="candidate audit access is required",
    ) as exc_info:
        pricing._candidate_audit_batch_access(object(), {})

    assert type(exc_info.value).__name__ == "Forbidden"


def _aborting_keys():
    yield 7
    raise _AuditAbort("stop")


def test_unique_integer_tuple_releases_claims_on_base_exception():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    with pytest.raises(_AuditAbort, match="stop"):
        retain_unique_integer_keys(
            _aborting_keys(),
            budget,
            category="test",
        )

    assert budget.retained_bytes == 0


def test_unique_integer_tuple_releases_claims_when_ordering_is_rejected():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=400)

    with pytest.raises(CandidateAuditDecodedRetentionError, match="byte limit"):
        retain_unique_integer_keys(
            (7,),
            budget,
            category="test",
        )

    assert budget.retained_bytes == 0


def test_unique_integer_helpers_dedupe_and_reject_invalid_bounds():
    with pytest.raises(ValueError, match="limit must be positive"):
        retain_unique_integer_keys(
            (),
            None,
            category="test",
            maximum_count=0,
        )

    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    ordered_keys, retained_tuple_bytes = retain_unique_integer_keys(
        (7, 7),
        budget,
        category="test",
    )
    assert ordered_keys == (7,)
    budget.release(retained_tuple_bytes)

    retained_keys, retained_set_bytes = retain_unique_integer_key_set(
        (7, 7),
        budget,
        category="test",
    )
    assert retained_keys == {7}
    budget.release(retained_set_bytes)
    assert budget.retained_bytes == 0


@pytest.mark.parametrize("with_budget", (False, True))
def test_unique_integer_tuple_stops_at_bounded_count(with_budget):
    budget = (
        CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
        if with_budget
        else None
    )
    consumed_keys = []

    def integer_keys():
        for integer_key in range(10):
            consumed_keys.append(integer_key)
            yield integer_key

    with pytest.raises(PTG2ManifestArtifactError, match="bounded test keys"):
        retain_unique_integer_keys(
            integer_keys(),
            budget,
            category="test",
            maximum_count=2,
            limit_error_message="bounded test keys",
        )

    assert consumed_keys == [0, 1, 2]
    if budget is not None:
        assert budget.retained_bytes == 0


def test_unique_integer_set_releases_claims_on_base_exception():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    with pytest.raises(_AuditAbort, match="stop"):
        retain_unique_integer_key_set(
            _aborting_keys(),
            budget,
            category="test",
        )

    assert budget.retained_bytes == 0


def test_unique_integer_set_propagates_base_exception_without_budget():
    with pytest.raises(_AuditAbort, match="stop"):
        retain_unique_integer_key_set(
            _aborting_keys(),
            None,
            category="test",
        )


@pytest.mark.parametrize(
    "maximum_bytes",
    (0, -1, True, PTG2_CANDIDATE_AUDIT_MAX_PROCESS_BYTES + 1),
)
def test_process_admission_rejects_invalid_limits(maximum_bytes):
    with pytest.raises(ValueError, match="process-byte limit is invalid"):
        CandidateAuditProcessAdmission(maximum_bytes)


@pytest.mark.asyncio
async def test_process_admission_rejects_one_partition_above_limit():
    admission = CandidateAuditProcessAdmission(99)

    with pytest.raises(CandidateAuditProcessAdmissionError) as exc_info:
        await admission.acquire(100)

    assert exc_info.value.requested_bytes == 100
    assert exc_info.value.maximum_bytes == 99
    assert admission.snapshot.rejected_count == 1
    assert admission.snapshot.retained_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("weight_bytes", (0, -1, True))
async def test_process_admission_rejects_invalid_weights(weight_bytes):
    admission = CandidateAuditProcessAdmission(99)

    with pytest.raises(ValueError, match="admission weight is invalid"):
        await admission.acquire(weight_bytes)


@pytest.mark.asyncio
async def test_process_admission_lease_context_releases_base_exception_once():
    admission = CandidateAuditProcessAdmission(10)
    lease = await admission.acquire(10)

    assert lease.is_released is False
    with pytest.raises(_AuditAbort, match="stop"):
        async with lease as entered_lease:
            assert entered_lease is lease
            raise _AuditAbort("stop")

    assert lease.is_released is True
    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.released_count == 1
    with pytest.raises(RuntimeError, match="released twice"):
        lease.release()


@pytest.mark.asyncio
async def test_immediate_lease_construction_abort_returns_granted_weight(
    monkeypatch,
):
    admission = CandidateAuditProcessAdmission(10)
    original_lease = capacity.CandidateAuditProcessAdmissionLease

    def abort_lease(*_args, **_kwargs):
        raise _LeaseConstructionAbort("stop")

    monkeypatch.setattr(
        capacity,
        "CandidateAuditProcessAdmissionLease",
        abort_lease,
    )
    with pytest.raises(_LeaseConstructionAbort, match="stop"):
        await admission.acquire(10)

    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.admitted_count == 1
    assert admission.snapshot.released_count == 1
    monkeypatch.setattr(
        capacity,
        "CandidateAuditProcessAdmissionLease",
        original_lease,
    )
    retry_lease = await admission.acquire(10)
    retry_lease.release()
    assert admission.snapshot.retained_bytes == 0


@pytest.mark.asyncio
async def test_waited_lease_construction_abort_returns_granted_weight(
    monkeypatch,
):
    admission = CandidateAuditProcessAdmission(10)
    active_lease = await admission.acquire(10)
    waiting_acquire = asyncio.create_task(admission.acquire(10))
    await asyncio.sleep(0)
    original_lease = capacity.CandidateAuditProcessAdmissionLease

    def abort_lease(*_args, **_kwargs):
        raise _LeaseConstructionAbort("stop")

    monkeypatch.setattr(
        capacity,
        "CandidateAuditProcessAdmissionLease",
        abort_lease,
    )
    active_lease.release()
    with pytest.raises(_LeaseConstructionAbort, match="stop"):
        await waiting_acquire

    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.queued_count == 0
    assert admission.snapshot.admitted_count == 2
    assert admission.snapshot.released_count == 2
    monkeypatch.setattr(
        capacity,
        "CandidateAuditProcessAdmissionLease",
        original_lease,
    )
    retry_lease = await admission.acquire(10)
    retry_lease.release()
    assert admission.snapshot.retained_bytes == 0


@pytest.mark.asyncio
async def test_process_admission_serializes_concurrent_weight_and_retries():
    admission = CandidateAuditProcessAdmission(10)
    first_lease = await admission.acquire(6)
    waiting_acquire = asyncio.create_task(admission.acquire(6))
    await asyncio.sleep(0)

    assert not waiting_acquire.done()
    assert admission.snapshot.retained_bytes == 6
    assert admission.snapshot.queued_count == 1

    first_lease.release()
    second_lease = await waiting_acquire
    assert second_lease.waited is True
    assert admission.snapshot.retained_bytes == 6
    second_lease.release()

    retry_lease = await admission.acquire(10)
    retry_lease.release()
    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.peak_retained_bytes == 10
    assert admission.snapshot.admitted_count == 3
    assert admission.snapshot.released_count == 3


@pytest.mark.asyncio
async def test_process_admission_fifo_blocks_lighter_follower():
    admission = CandidateAuditProcessAdmission(10)
    active_lease = await admission.acquire(6)
    heavy_acquire = asyncio.create_task(admission.acquire(6))
    light_acquire = asyncio.create_task(admission.acquire(4))
    await asyncio.sleep(0)

    assert not heavy_acquire.done()
    assert not light_acquire.done()
    assert admission.snapshot.queued_count == 2

    active_lease.release()
    heavy_lease, light_lease = await asyncio.gather(heavy_acquire, light_acquire)
    assert heavy_lease.waited is True
    assert light_lease.waited is True
    assert admission.snapshot.retained_bytes == 10
    heavy_lease.release()
    light_lease.release()
    assert admission.snapshot.retained_bytes == 0


@pytest.mark.asyncio
async def test_process_admission_pending_cancellation_removes_waiter_once():
    admission = CandidateAuditProcessAdmission(10)
    active_lease = await admission.acquire(6)
    waiting_acquire = asyncio.create_task(admission.acquire(6))
    await asyncio.sleep(0)

    waiting_acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_acquire

    assert admission.snapshot.retained_bytes == 6
    assert admission.snapshot.queued_count == 0
    assert admission.snapshot.cancelled_count == 1
    active_lease.release()
    retry_lease = await admission.acquire(10)
    retry_lease.release()
    assert admission.snapshot.retained_bytes == 0


@pytest.mark.asyncio
async def test_process_admission_discards_cancelled_head_before_task_cleanup():
    admission = CandidateAuditProcessAdmission(10)
    active_lease = await admission.acquire(10)
    waiting_acquire = asyncio.create_task(admission.acquire(1))
    await asyncio.sleep(0)
    waiter = admission._waiters[0]

    waiter.future.cancel()
    admission._grant_waiters()
    with pytest.raises(asyncio.CancelledError):
        await waiting_acquire

    assert admission.snapshot.cancelled_count == 1
    assert admission.snapshot.queued_count == 0
    assert admission.snapshot.retained_bytes == 10
    active_lease.release()
    assert admission.snapshot.retained_bytes == 0


@pytest.mark.asyncio
async def test_process_admission_cancel_after_grant_releases_exactly_once():
    admission = CandidateAuditProcessAdmission(10)
    active_lease = await admission.acquire(6)
    waiting_acquire = asyncio.create_task(admission.acquire(6))
    await asyncio.sleep(0)

    active_lease.release()
    waiting_acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_acquire

    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.queued_count == 0
    assert admission.snapshot.released_count == 2
    retry_lease = await admission.acquire(10)
    retry_lease.release()
    assert admission.snapshot.retained_bytes == 0


def test_process_admission_rejects_invalid_internal_release():
    admission = CandidateAuditProcessAdmission(10)

    with pytest.raises(RuntimeError, match="release is invalid"):
        admission._release(0)


@pytest.mark.asyncio
async def test_process_admission_caps_eight_nominal_partition_weights():
    partition_weight = 576
    admission = CandidateAuditProcessAdmission(4 * partition_weight)
    release_wave = asyncio.Event()
    counts = SimpleNamespace(active=0, peak=0)

    async def run_partition() -> None:
        lease = await admission.acquire(partition_weight)
        counts.active += 1
        counts.peak = max(counts.peak, counts.active)
        await release_wave.wait()
        counts.active -= 1
        lease.release()

    tasks = [asyncio.create_task(run_partition()) for _ in range(8)]
    await asyncio.sleep(0)

    assert counts.peak == 4
    assert admission.snapshot.retained_bytes == 4 * partition_weight
    assert admission.snapshot.queued_count == 4

    release_wave.set()
    await asyncio.gather(*tasks)
    assert counts.peak == 4
    assert admission.snapshot.retained_bytes == 0
    assert admission.snapshot.admitted_count == 8
    assert admission.snapshot.released_count == 8
