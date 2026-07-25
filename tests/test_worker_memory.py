# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import worker_memory


@pytest.fixture(autouse=True)
def reset_worker_memory_state(monkeypatch):
    monkeypatch.setattr(
        worker_memory,
        "_WORKER_MEMORY_STATE",
        worker_memory._WorkerMemoryState(),
    )


def _install_gc_spies(monkeypatch, *, freeze_error=None):
    call_names = []
    enabled_by_name = {"value": False}

    def enable():
        call_names.append("enable")
        enabled_by_name["value"] = True

    def collect():
        call_names.append("collect")
        return 0

    def freeze():
        call_names.append("freeze")
        if freeze_error is not None:
            raise freeze_error

    def unfreeze():
        call_names.append("unfreeze")

    monkeypatch.setattr(
        worker_memory.gc,
        "isenabled",
        lambda: enabled_by_name["value"],
    )
    monkeypatch.setattr(worker_memory.gc, "enable", enable)
    monkeypatch.setattr(worker_memory.gc, "collect", collect)
    monkeypatch.setattr(worker_memory.gc, "freeze", freeze)
    monkeypatch.setattr(worker_memory.gc, "unfreeze", unfreeze)
    monkeypatch.setattr(worker_memory.gc, "get_freeze_count", lambda: 73)
    return call_names, enabled_by_name


def test_worker_heap_freeze_keeps_normal_gc_enabled_and_unfreezes_on_shutdown(
    monkeypatch,
):
    call_names, enabled_by_name = _install_gc_spies(monkeypatch)
    monkeypatch.delenv("HLTHPRT_API_WORKER_GC_FREEZE_ENABLED", raising=False)

    worker_memory.freeze_api_worker_heap()
    worker_memory.freeze_api_worker_heap()
    assert enabled_by_name["value"]
    assert call_names == ["enable", "collect", "freeze"]
    frozen_metrics = worker_memory.worker_memory_metrics()
    assert frozen_metrics.is_frozen
    assert frozen_metrics.freeze_successes == 1
    assert frozen_metrics.permanent_objects == 73

    worker_memory.unfreeze_api_worker_heap()
    worker_memory.unfreeze_api_worker_heap()
    assert call_names == ["enable", "collect", "freeze", "unfreeze", "collect"]
    assert not worker_memory.worker_memory_metrics().is_frozen


def test_worker_shutdown_reenables_gc_if_an_external_hook_disabled_it(monkeypatch):
    """Shutdown restores normal GC even if another hook disabled it after freeze."""

    call_names, enabled_by_name = _install_gc_spies(monkeypatch)
    worker_memory.freeze_api_worker_heap()
    enabled_by_name["value"] = False

    worker_memory.unfreeze_api_worker_heap()

    assert call_names == [
        "enable",
        "collect",
        "freeze",
        "unfreeze",
        "enable",
        "collect",
    ]
    assert enabled_by_name["value"]


@pytest.mark.parametrize("disabled_value", ["0", "false", "NO", "off", "disabled"])
def test_worker_heap_freeze_has_explicit_disable_switch(monkeypatch, disabled_value):
    call_names, _enabled_by_name = _install_gc_spies(monkeypatch)
    monkeypatch.setenv("HLTHPRT_API_WORKER_GC_FREEZE_ENABLED", disabled_value)

    worker_memory.freeze_api_worker_heap()
    assert call_names == []
    assert not worker_memory.worker_memory_metrics().is_enabled


def test_worker_heap_freeze_failure_rolls_back_without_disabling_gc(monkeypatch):
    call_names, enabled_by_name = _install_gc_spies(
        monkeypatch,
        freeze_error=RuntimeError("freeze failed"),
    )

    worker_memory.freeze_api_worker_heap()
    assert call_names == ["enable", "collect", "freeze", "unfreeze", "collect"]
    assert enabled_by_name["value"]
    metrics = worker_memory.worker_memory_metrics()
    assert not metrics.is_frozen
    assert metrics.freeze_failures == 1
    assert metrics.explicit_collections == 2


def test_worker_heap_rollback_survives_unfreeze_failure(monkeypatch, caplog):
    """A partial-freeze rollback still restores GC when unfreeze itself fails."""

    call_names, enabled_by_name = _install_gc_spies(monkeypatch)

    def fail_unfreeze():
        call_names.append("unfreeze")
        raise RuntimeError("unfreeze failed")

    monkeypatch.setattr(worker_memory.gc, "unfreeze", fail_unfreeze)

    worker_memory._rollback_failed_freeze()

    assert call_names == ["unfreeze", "enable", "collect"]
    assert enabled_by_name["value"]
    assert "could not unfreeze" in caplog.text


@pytest.mark.asyncio
async def test_worker_memory_listeners_are_worker_scoped(monkeypatch):
    listener_by_event = {}
    call_names = []

    class FakeApp:
        def listener(self, event_name):
            def register(listener):
                listener_by_event[event_name] = listener
                return listener

            return register

    monkeypatch.setattr(
        worker_memory,
        "freeze_api_worker_heap",
        lambda: call_names.append("freeze"),
    )
    monkeypatch.setattr(
        worker_memory,
        "unfreeze_api_worker_heap",
        lambda: call_names.append("unfreeze"),
    )
    worker_memory.register_worker_memory_lifecycle(FakeApp())

    assert set(listener_by_event) == {"after_server_start", "before_server_stop"}
    await listener_by_event["after_server_start"](object())
    await listener_by_event["before_server_stop"](object())
    assert call_names == ["freeze", "unfreeze"]
