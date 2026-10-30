# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Controlled observations for runtime scheduling tests; these are not native custody evidence."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from process import registry_membership_refresh_runtime as runtime
from process import registry_membership_refresh_sources as sources
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from tests.test_registry_membership_refresh_runtime import configuration


class ObservationCase:
    def __init__(self, monkeypatch):
        self.pin = PinnedAddressSource("mrf", "entity_address_unified", "a" * 64)
        self.descriptor = {"descriptor_sha256": "b" * 64}
        self.selected = sources.AcceptedMembershipRefreshSources(
            (
                {
                    "source_id": "example",
                    "capture_generation": "accepted",
                    "bindings_sha256": "c" * 64,
                    "transform_version": "extract-v1",
                },
            ),
            (),
        )
        self.approved = ApprovedMembershipSource(7, "d" * 64)
        self.head = {"approved_revision": 7, "generation_id": 0, "manifest_sha256": None}
        self.events = []
        self.has_source_fence = False
        self.resources = SimpleNamespace(
            settings=runtime._settings(configuration()),
            readers=SimpleNamespace(ptg_store=None),
            address=self.address,
            identity=("example", 3),
            fence_sessions=object(),
        )
        self.prepare = AsyncMock(return_value=True)
        self.service = runtime.RegistryMembershipRefreshInputsService(self.resources, None)
        monkeypatch.setattr(self.service, "_has_office_correspondence", self.prepare)
        monkeypatch.setattr(runtime, "_driver", AsyncMock(return_value=object()))
        monkeypatch.setattr(runtime, "_heads", self.heads)
        monkeypatch.setattr(runtime, "_approved", self.approved_pin)
        monkeypatch.setattr(runtime, "_manifest", AsyncMock(return_value=None))
        monkeypatch.setattr(runtime, "_published_request", AsyncMock(return_value=None))
        monkeypatch.setattr(runtime, "_recovery", AsyncMock(return_value=None))
        monkeypatch.setattr(sources, "observe_registry_membership_refresh_sources", self.observe)
        monkeypatch.setattr(sources, "pin_registry_membership_refresh_sources", self.full)

    async def heads(self, *_coordinates):
        return dict(self.head)

    async def approved_pin(self, *_coordinates):
        return self.approved

    async def observe(self, *_coordinates, **_capabilities):
        self.events.append("observe")
        return self.selected

    @asynccontextmanager
    async def full(self, *_coordinates, **_capabilities):
        self.events.append("full_source_pin")
        self.has_source_fence = True
        try:
            yield self.selected
        finally:
            self.has_source_fence = False
            self.events.append("source_release")

    @asynccontextmanager
    async def address(self):
        self.events.append("address_fenced" if self.has_source_fence else "address_metadata")
        yield object(), self.pin, dict(self.descriptor)
