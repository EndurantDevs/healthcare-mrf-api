# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy bootstrap discovery never substitutes for fresh artifact authority."""

from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_compact_address_origin as origin
from process import network_serving_read
from process.network_address_projection import PinnedAddressSource
from process.network_compact_readiness import NetworkCompactReadinessError

pytestmark = pytest.mark.asyncio


def _case(monkeypatch, *, compact=False):
    source = PinnedAddressSource("network_bootstrap_" + "1" * 32, "entity_address_unified", "a" * 64)
    candidate_by_name = {
        "source_generations": {"unified_address": source.generation_id, "retained_network_source": "b" * 64}
    }
    report_by_name = {"address_source": asdict(source)}
    if compact:
        report_by_name["serving_readiness"] = {"component": "compact_network_serving"}
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"generation_id": 1, "validation_json": report_by_name}])
    )
    manifest = SimpleNamespace(manifest_sha256="b" * 64, compact_layout=None, eligible=False)
    resolver = AsyncMock(return_value=manifest)
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", resolver)
    return source, candidate_by_name, connection, manifest, resolver


async def test_legacy_locator_requires_fresh_bootstrap_capture(monkeypatch):
    source, candidate_by_name, connection, _, resolver = _case(monkeypatch)
    capture = AsyncMock(return_value={"table": {"oid": 42}})
    retained = AsyncMock(side_effect=AssertionError("legacy heads are not compact seals"))
    monkeypatch.setattr(origin, "capture_network_address_artifact", capture)
    monkeypatch.setattr(origin, "resolve_registry_retained_generation", retained)
    descriptor, relation = await origin.resolve_compact_address_origin(
        connection, candidate_by_name, source, runtime_roles=("reader",), control_schema="registry"
    )
    capture.assert_awaited_once_with(connection, source, runtime_roles=("reader",))
    resolver.assert_awaited_once_with(connection, generation_id=1, control_schema="registry")
    assert descriptor["table"]["oid"] == relation.table_oid == 42
    retained.assert_not_awaited()


@pytest.mark.parametrize("damage", ["unknown", "ambiguous", "locator", "source"])
async def test_legacy_locator_or_source_mismatch_refuses_before_capture(monkeypatch, damage):
    source, candidate_by_name, connection, manifest, _ = _case(monkeypatch)
    if damage == "unknown":
        connection.fetch.return_value = []
    elif damage == "ambiguous":
        connection.fetch.return_value *= 2
    elif damage == "locator":
        manifest.manifest_sha256 = "c" * 64
    else:
        connection.fetch.return_value[0]["validation_json"]["address_source"]["generation_id"] = "c" * 64
    capture = AsyncMock(side_effect=AssertionError("invalid locator must not reach capture"))
    monkeypatch.setattr(origin, "capture_network_address_artifact", capture)
    with pytest.raises(NetworkCompactReadinessError):
        await origin.resolve_compact_address_origin(
            connection, candidate_by_name, source, runtime_roles=("reader",), control_schema="registry"
        )
    capture.assert_not_awaited()


async def test_compact_head_keeps_independent_retained_resolver(monkeypatch):
    source, candidate_by_name, connection, _, resolver = _case(monkeypatch, compact=True)
    retained = AsyncMock(return_value=None)
    monkeypatch.setattr(origin, "resolve_registry_retained_generation", retained)
    assert await origin._retained_origin(connection, candidate_by_name, source, ("reader",), "registry") is None
    retained.assert_awaited_once_with(connection, candidate_by_name, control_schema="registry")
    resolver.assert_not_awaited()
