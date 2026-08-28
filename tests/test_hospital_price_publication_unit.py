# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused hospital-price publication ordering and binding proof."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from tests.test_hospital_price_store_unit import _Connection, _store_module


def test_location_binding_is_exact_and_rejects_ambiguity() -> None:
    store, _native = _store_module()
    attempts = (
        SimpleNamespace(
            hospital_id="hospital-a", locator_name=" Same ", hospital_name="A"
        ),
        SimpleNamespace(
            hospital_id="hospital-b", locator_name=None, hospital_name="Unique"
        ),
    )
    assert store._location_ordinals(
        attempts, ((0, None), (1, "Same"), (2, " same "), (3, "Unique"))
    ) == {"hospital-a": None, "hospital-b": None}


@pytest.mark.asyncio
async def test_publication_stage_keeps_content_only_location_unbound() -> None:
    store, _native = _store_module()
    connection = _Connection()
    attempt = SimpleNamespace(
        hospital_id="hospital-a",
        attempt_id="attempt-a",
        expected_generation=3,
        locator_name=None,
        hospital_name="Catalog Sublocation",
        final_source_url="https://cdn.hospital.example/prices.json",
        source_http_status=200,
        source_url="https://hospital.example/12-3456789_prices.json",
    )

    await store._publication_stage(
        connection, (attempt,), ((7, "Catalog Sublocation"),)
    )

    assert connection.driver.records[0][3] is None


@pytest.mark.asyncio
async def test_bind_and_publish_locks_current_before_attempt_evidence(
    monkeypatch,
) -> None:
    store, _native = _store_module()
    events: list[str] = []

    class Connection(_Connection):
        async def all(self, statement: str, **kwargs: Any) -> list[Any]:
            events.append("current")
            assert "ORDER BY current.hospital_id FOR UPDATE OF current" in statement
            return [("hospital-a",), ("hospital-b",)]

    async def publication(*_args: Any) -> tuple[str, str]:
        return "stage", '"stage"'

    async def bind(*_args: Any) -> None:
        events.append("attempt")

    async def publish(*_args: Any) -> tuple[int, int, int]:
        return 2, 0, 0

    monkeypatch.setattr(store, "_publication_stage", publication)
    monkeypatch.setattr(store, "_bind_evidence", bind)
    monkeypatch.setattr(store, "_cas_publish", publish)
    attempts = (
        SimpleNamespace(hospital_id="hospital-b"),
        SimpleNamespace(hospital_id="hospital-a"),
    )

    assert await store._bind_and_publish(
        Connection(), "v", "c", attempts, ()
    ) == (2, 0, 0)
    assert events == ["current", "attempt"]


@pytest.mark.asyncio
async def test_bind_and_publish_rejects_a_changed_current_set(monkeypatch) -> None:
    store, _native = _store_module()

    class Connection(_Connection):
        async def all(self, _statement: str, **_kwargs: object) -> list[Any]:
            return []

    async def publication(*_args: object) -> tuple[str, str]:
        return "stage", '"stage"'

    monkeypatch.setattr(store, "_publication_stage", publication)

    with pytest.raises(RuntimeError, match="current rows changed"):
        await store._bind_and_publish(
            Connection(), "v", "c", (SimpleNamespace(hospital_id="hospital-a"),), ()
        )
