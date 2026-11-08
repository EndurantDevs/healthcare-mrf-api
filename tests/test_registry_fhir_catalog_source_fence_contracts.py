# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Session-fence protocol proof only; no native locks or Source authority issued."""

import asyncio
from contextlib import nullcontext

import pytest

from process import registry_fhir_catalog_source_fence as fences
from process import registry_fhir_catalog_sources as sources
from tests.test_registry_fhir_catalog_sources import native_inventory, published_row, read_profile


def _descriptor(endpoint="endpoint-one"):
    return sources._disposition(
        published_row(endpoint_id=endpoint), read_profile(), native_inventory(), None, sources.DEFAULT_BYTES
    ).descriptor


class LockConnection:
    """Model the existing driver's advisory-lock responses, never native admission."""

    def __init__(self):
        self.active = False
        self.shared_keys = set()
        self.external_keys = set()
        self.events = []
        self.terminated = False
        self.busy = False
        self.release_mode = None
        self.acquire_entered = None
        self.acquire_release = None

    def is_in_transaction(self):
        return self.active

    def terminate(self):
        self.terminated = True

    async def fetch(self, query, values, *, timeout):
        assert timeout > 0
        if "hashtextextended" in query:
            self.events.append("keys")
            return [{"key": {"endpoint-one": 101, "endpoint-two": 202}[endpoint]} for endpoint in values]
        if "pg_catalog.pg_locks" in query:
            self.events.append("held")
            held = self.shared_keys | (self.external_keys if "IN ('ShareLock','ExclusiveLock')" in query else set())
            return [{"lock_key": key} for key in sorted(held & set(values))]
        if "pg_try_advisory_lock_shared" in query:
            self.events.append("acquire")
            if self.acquire_entered is not None:
                self.acquire_entered.set()
                await self.acquire_release.wait()
            if not self.busy:
                self.shared_keys.update(values)
            return [{"key": key, "acquired": not self.busy} for key in values]
        assert "pg_advisory_unlock_shared" in query
        self.events.append("release")
        if self.release_mode != "still_held":
            self.shared_keys.difference_update(values)
        return [{"key": key, "released": self.release_mode != "refused"} for key in values]


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, 0, -1, 61, float("inf")])
async def test_invalid_lock_deadline_refuses_before_observing_any_session(timeout):
    connection = LockConnection()
    with pytest.raises(fences.FHIRCatalogSourceFenceError, match="invalid"):
        async with fences.fhir_catalog_source_publication_fence(connection, (_descriptor(),), timeout_seconds=timeout):
            pytest.fail("invalid deadline entered a source fence")
    assert connection.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("descriptors", [(), [], (None,)])
async def test_missing_or_untyped_source_inventory_never_acquires_a_lock(descriptors):
    connection = LockConnection()
    with pytest.raises(ValueError):
        async with fences.fhir_catalog_source_publication_fence(connection, descriptors):
            pytest.fail("invalid inventory entered a source fence")
    assert connection.events == []


@pytest.mark.asyncio
async def test_fence_is_original_connection_and_task_scoped_with_subset_verification():
    connection = LockConnection()
    first, second = _descriptor(), _descriptor("endpoint-two")
    async with fences.fhir_catalog_source_publication_fence(connection, (second, first, first)):
        assert connection.shared_keys == {101, 202}
        await fences.require_fhir_catalog_source_publication_fence(connection, (first,))
        with pytest.raises(fences.FHIRCatalogSourceFenceError, match="unavailable"):
            await fences.require_fhir_catalog_source_publication_fence(LockConnection(), (first,))
        with pytest.raises(fences.FHIRCatalogSourceFenceError, match="unavailable"):
            await asyncio.create_task(fences.require_fhir_catalog_source_publication_fence(connection, (first,)))
        with pytest.raises(fences.FHIRCatalogSourceFenceError, match="pre_snapshot_required"):
            async with fences.fhir_catalog_source_publication_fence(connection, (first,)):
                pytest.fail("nested fence entered")
    assert not connection.shared_keys and not connection.terminated
    assert fences._ISSUED.get() is None
    await fences.require_fhir_catalog_source_publication_fence(connection, (first,), required=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transaction", "existing_lock"])
async def test_existing_transaction_or_advisory_lock_is_preserved_on_refusal(failure):
    connection = LockConnection()
    connection.active = failure == "transaction"
    connection.external_keys = {101} if failure == "existing_lock" else set()
    reason = "pre_snapshot_required" if connection.active else "preexisting_lock"
    with pytest.raises(fences.FHIRCatalogSourceFenceError, match=reason):
        async with fences.fhir_catalog_source_publication_fence(connection, (_descriptor(),)):
            pytest.fail("pre-existing state was adopted")
    assert "acquire" not in connection.events and "release" not in connection.events
    assert connection.external_keys == ({101} if failure == "existing_lock" else set())
    assert not connection.terminated


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["different_endpoint", "lost_lock"])
async def test_issued_fence_refuses_endpoint_expansion_and_lost_shared_lock(failure):
    connection = LockConnection()
    selected = (_descriptor(),)
    async with fences.fhir_catalog_source_publication_fence(connection, selected):
        if failure == "different_endpoint":
            selected = (_descriptor("endpoint-two"),)
        else:
            connection.shared_keys.clear()
        reason = "scope_changed" if failure == "different_endpoint" else "lost"
        with pytest.raises(fences.FHIRCatalogSourceFenceError, match=reason):
            await fences.require_fhir_catalog_source_publication_fence(connection, selected)
    assert not connection.shared_keys and fences._ISSUED.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("release_mode", ["refused", "still_held"])
@pytest.mark.parametrize("body_error", [False, True])
async def test_failed_release_terminates_connection_without_replacing_body_failure(release_mode, body_error):
    connection = LockConnection()
    connection.release_mode = release_mode
    original = RuntimeError("publication refused")
    expected = RuntimeError if body_error else fences.FHIRCatalogSourceFenceError
    with pytest.raises(expected) as caught:
        async with fences.fhir_catalog_source_publication_fence(connection, (_descriptor(),)):
            if body_error:
                raise original
    if body_error:
        assert caught.value is original
    else:
        assert "release_failed" in str(caught.value)
    assert connection.terminated and fences._ISSUED.get() is None


@pytest.mark.asyncio
async def test_busy_acquisition_exits_at_its_bound_without_issuing_a_fence():
    connection = LockConnection()
    connection.busy = True
    with pytest.raises(fences.FHIRCatalogSourceFenceError, match="busy"):
        async with fences.fhir_catalog_source_publication_fence(connection, (_descriptor(),), timeout_seconds=0.001):
            pytest.fail("busy source lock entered publication")
    assert not connection.shared_keys and fences._ISSUED.get() is None
    assert not connection.terminated


@pytest.mark.asyncio
async def test_cancellation_drains_inflight_lock_reply_then_releases_every_acquired_key():
    connection = LockConnection()
    connection.acquire_entered, connection.acquire_release = asyncio.Event(), asyncio.Event()

    async def run():
        async with fences.fhir_catalog_source_publication_fence(connection, (_descriptor(),)):
            pytest.fail("canceled acquisition entered publication")

    task = asyncio.create_task(run())
    try:
        await asyncio.wait_for(connection.acquire_entered.wait(), 1)
        task.cancel()
        connection.acquire_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert connection.events.count("release") == 1
        assert not connection.shared_keys and not connection.terminated
    finally:
        connection.acquire_release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("body_error", [None, RuntimeError("read failed"), asyncio.CancelledError("read canceled")])
async def test_normal_fence_exit_releases_only_owned_keys_and_preserves_original_failure(body_error):
    connection = LockConnection()
    connection.external_keys = {999}
    with pytest.raises(type(body_error)) if body_error is not None else nullcontext() as caught:
        async with fences.fhir_catalog_source_publication_fence(connection, (_descriptor(),)):
            if body_error is not None:
                raise body_error
    if body_error is not None:
        assert caught.value is body_error
    assert not connection.shared_keys and connection.external_keys == {999}
    assert connection.events.count("release") == 1 and not connection.terminated
