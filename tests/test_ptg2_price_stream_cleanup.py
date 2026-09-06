# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Price publication owns both streams until their COPY cleanup finishes."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_price as price


KINDS = (price._PRICE_MEMBERSHIP_ARTIFACT_KIND, price._PRICE_ATOM_ARTIFACT_KIND)


class _ControlledStreams:
    def __init__(self, failure_kind: str | None = None) -> None:
        self.failure_kind = failure_kind
        self.error = TimeoutError("second driver acquisition timed out")
        self.started = {kind: asyncio.Event() for kind in KINDS}
        self.finish = {kind: asyncio.Event() for kind in KINDS}
        self.closed = {kind: asyncio.Event() for kind in KINDS}
        self.cleanup_started = asyncio.Event()
        self.release_cleanup = asyncio.Event()
        self.tasks: list[asyncio.Task] = []
        self.summary_by_kind = {
            KINDS[0]: {
                "artifact_kind": KINDS[0],
                "row_count": 1,
                "atom_reference_count": 1,
                "price_set_count": 1,
                "maximum_price_key": 0,
                "atom_key_bits": 32,
                "atom_key_bytes": 4,
            },
            KINDS[1]: {
                "artifact_kind": KINDS[1],
                "atom_count": 1,
                "attribute_count": len(price._V3_ATTRIBUTE_KEY_COLUMNS),
                "atom_key_bits": 32,
                "atom_key_bytes": 4,
            },
        }

    async def stream(self, *, kind: str, **_kwargs) -> dict:
        self.tasks.append(asyncio.current_task())
        self.started[kind].set()
        try:
            await self.finish[kind].wait()
            if kind == self.failure_kind:
                raise self.error
            return self.summary_by_kind[kind]
        finally:
            if kind != self.failure_kind:
                self.cleanup_started.set()
                await self.release_cleanup.wait()
            self.closed[kind].set()

    async def wait_started(self) -> None:
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in self.started.values())), 1
        )

    async def finish_test(self, publication: asyncio.Task) -> None:
        self.release_cleanup.set()
        for event in self.finish.values():
            event.set()
        await asyncio.gather(publication, *self.tasks, return_exceptions=True)


@pytest.fixture
def prepared() -> price.PreparedSharedPriceArtifacts:
    return price.PreparedSharedPriceArtifacts(
        schema_name="mrf",
        price_atom_table="atoms",
        price_set_atom_table="memberships",
        price_attr_dictionary_table="attributes",
        price_key_map="price_keys",
        atom_key_map="atom_keys",
        price_set_count=1,
        atom_count=1,
        atom_key_bits=32,
        lean_manifest={},
        stage_metrics={},
    )


def _start_publication(monkeypatch, prepared, streams):
    status = AsyncMock()
    monkeypatch.setattr(price.db, "status", status)
    monkeypatch.setattr(price, "create_shared_block_stage", AsyncMock())
    monkeypatch.setattr(price, "_stream_shared_price_copy", streams.stream)
    publication = asyncio.create_task(
        price.publish_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest",
            snapshot_key=7,
            build_token="build",
            expected_price_set_count=1,
            expected_price_key_order=price.PTG2_V3_PRICE_KEY_ORDER,
            prepared=prepared,
        )
    )
    return publication, status


async def _wait_for_stream_drain(streams, publication) -> None:
    cleanup_waiter = asyncio.create_task(streams.cleanup_started.wait())
    try:
        completed, _pending = await asyncio.wait(
            (cleanup_waiter, publication),
            timeout=1,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert cleanup_waiter in completed, "publication escaped its active stream"
    finally:
        cleanup_waiter.cancel()
        await asyncio.gather(cleanup_waiter, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", KINDS)
@pytest.mark.parametrize("cancel_during_drain", (False, True))
async def test_stream_failure_drains_before_drop(
    monkeypatch, prepared, failure_kind, cancel_during_drain
) -> None:
    streams = _ControlledStreams(failure_kind)
    publication, status = _start_publication(monkeypatch, prepared, streams)
    try:
        await streams.wait_started()
        streams.finish[failure_kind].set()
        await _wait_for_stream_drain(streams, publication)
        if cancel_during_drain:
            for _ in range(3):
                publication.cancel()
                await asyncio.sleep(0)
        assert not publication.done()
        status.assert_not_awaited()
        streams.release_cleanup.set()
        with pytest.raises(TimeoutError) as failure:
            await publication
        assert failure.value is streams.error
        assert all(event.is_set() for event in streams.closed.values())
        status.assert_awaited_once()
    finally:
        await streams.finish_test(publication)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancellation_count", (1, 3))
async def test_stream_cancellation_drains_before_drop(
    monkeypatch, prepared, cancellation_count
) -> None:
    streams = _ControlledStreams()
    publication, status = _start_publication(monkeypatch, prepared, streams)
    try:
        await streams.wait_started()
        publication.cancel("initial cancellation")
        await _wait_for_stream_drain(streams, publication)
        for _ in range(cancellation_count - 1):
            publication.cancel()
            await asyncio.sleep(0)
        assert not publication.done()
        status.assert_not_awaited()
        streams.release_cleanup.set()
        with pytest.raises(asyncio.CancelledError) as cancellation:
            await publication
        assert cancellation.value.args == ("initial cancellation",)
        assert all(event.is_set() for event in streams.closed.values())
        status.assert_awaited_once()
    finally:
        await streams.finish_test(publication)


@pytest.mark.asyncio
@pytest.mark.parametrize("first_kind", KINDS)
async def test_stream_completion_keeps_summary_order(monkeypatch, prepared, first_kind):
    streams = _ControlledStreams()
    streams.release_cleanup.set()
    monkeypatch.setattr(price, "create_shared_block_stage", AsyncMock())
    monkeypatch.setattr(price, "_stream_shared_price_copy", streams.stream)
    staging = asyncio.create_task(
        price._stage_shared_price_blocks("mrf", "stage", prepared)
    )
    try:
        await streams.wait_started()
        streams.finish[first_kind].set()
        await asyncio.wait_for(streams.closed[first_kind].wait(), 1)
        assert not staging.done()
        streams.finish[next(kind for kind in KINDS if kind != first_kind)].set()
        staged = await staging
        assert staged.membership_summary == streams.summary_by_kind[KINDS[0]]
        assert staged.atom_summary == streams.summary_by_kind[KINDS[1]]
    finally:
        await streams.finish_test(staging)
