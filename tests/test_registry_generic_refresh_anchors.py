# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generic enrollment dispatch is closed; native capture custody has connected coverage."""

import asyncio
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from process import registry_generic_refresh_anchors as generic
from process import registry_refresh_anchor_service as anchors
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import (
    GENERIC_FHIR_PRODUCER,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)
from tests.test_registry_refresh_anchor_adapters import _binding


def _generic_binding(index=1, **changes):
    fields_by_name = dict(
        source_id="generic-directory",
        producer_id=GENERIC_FHIR_PRODUCER,
        edition_id=str(UUID(int=1000)),
        source_scope_json={"organization_id": "network-one", "capture_sha256": "b" * 64},
    )
    return _binding(index=index, **{**fields_by_name, **changes})


def test_generic_grouping_keeps_original_coordinates_and_refuses_expansion():
    first, second = _generic_binding(), _generic_binding(2)
    groups = generic._capture_groups((first, second))
    assert len(groups) == 1 and next(iter(groups.values())) == [first, second]
    for bindings in ((), (first, first), (_binding(),)):
        with pytest.raises(RegistryRefreshAnchorMismatch):
            generic._capture_groups(bindings)
    many_bindings = tuple(_generic_binding(index + 1, dataset_id=str(UUID(int=index + 1))) for index in range(101))
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        generic._capture_groups(many_bindings)


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [{"resource_id": "network-one"}] * 2, [{"resource_id": "other"}]])
async def test_organization_scope_refuses_missing_duplicate_or_unrelated_evidence(rows):
    capture = SimpleNamespace(
        origin=SimpleNamespace(dataset_id="dataset-one", source_id="generic-directory"),
        schema_name="retained_generic",
        release_id="release-one",
        capture_sha256="b" * 64,
    )
    retained = SimpleNamespace(capture=capture, verify=AsyncMock())
    driver = SimpleNamespace(fetch=AsyncMock(return_value=rows))
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await generic._organization_scopes(driver, retained, (_generic_binding(),), RegistryPTGGraphReadBudget(1048576))
    retained.verify.assert_awaited_once_with(driver, capture)


@pytest.mark.asyncio
async def test_scope_keeps_original_capture_hash_and_native_approved_binding_fingerprint():
    capture = SimpleNamespace(
        origin=SimpleNamespace(dataset_id="dataset-one", source_id="generic-directory"),
        schema_name="retained_generic",
        release_id="release-one",
        capture_sha256="b" * 64,
    )
    binding = _generic_binding()
    retained = SimpleNamespace(capture=capture, verify=AsyncMock())
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[{"resource_id": "network-one"}]))
    scopes = await generic._organization_scopes(driver, retained, (binding,), RegistryPTGGraphReadBudget(1048576))
    scope = scopes[binding.binding_id]
    assert scope["capture_generation"] == "b" * 64
    assert scope["approved_binding_sha256"] == binding.approved_binding_sha256
    wrong = _generic_binding(source_scope_json={"organization_id": "network-one", "capture_sha256": "c" * 64})
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await generic._organization_scopes(driver, retained, (wrong,), RegistryPTGGraphReadBudget(1048576))


@pytest.mark.asyncio
async def test_generic_dispatch_does_not_require_or_invent_a_cms_reader(monkeypatch):
    binding = _generic_binding()
    scope_by_field = {
        **{
            name: binding.record_json[name]
            for name in ("source_system", "source_id", "dataset_schema", "producer_id", "source_key")
        },
        "approved_binding_sha256": binding.approved_binding_sha256,
        "capture_generation": "b" * 64,
    }
    native = AsyncMock(return_value={binding.binding_id: scope_by_field})
    monkeypatch.setattr(generic, "resolve_generic_refresh_anchors", native)
    service, session, budget = SimpleNamespace(cms_reader=None), object(), RegistryPTGGraphReadBudget(1048576)
    assert await anchors._derive_sources(service, session, (binding,), budget) == {binding.binding_id: scope_by_field}
    native.assert_awaited_once_with(session, (binding,), read_budget=budget)


@pytest.mark.asyncio
async def test_scope_json_is_not_a_native_session_capability():
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await generic.resolve_generic_refresh_anchors(object(), (), read_budget=RegistryPTGGraphReadBudget(1048576))


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["coordinates", "producer", "transaction", "edition", "physical_oid"])
async def test_original_capture_requires_exact_typed_epoch_and_catalog_identity(monkeypatch, fault):
    from process import network_fhir_retained_capture as captures
    from tests.test_network_fhir_retained_capture import retained

    capture = retained()
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        capture.origin.source_id,
        capture.origin.schema_name,
        capture.origin.dataset_id,
        GENERIC_FHIR_PRODUCER,
        capture.capture_id,
    )
    if fault == "producer":
        coordinates = replace(coordinates, producer_id="other-producer")
    elif fault == "edition":
        coordinates = replace(coordinates, edition_id=str(UUID(int=0)))
    driver = SimpleNamespace(
        is_in_transaction=lambda: fault != "transaction",
        fetchrow=AsyncMock(
            return_value={
                "schema_oid": capture.schema_oid + 1,
                "capture_comment": generic._PREFIX + json.dumps(asdict(capture)),
            }
        ),
    )
    verify = AsyncMock()
    monkeypatch.setattr(captures, "require_retained_fhir_capture", verify)
    with pytest.raises((RegistryRefreshAnchorUnavailable, RegistryRefreshAnchorMismatch), match="generic_capture"):
        await generic.read_original_generic_capture_metadata(
            driver, {} if fault == "coordinates" else coordinates, RegistryPTGGraphReadBudget(1048576)
        )
    assert driver.fetchrow.await_count == int(fault == "physical_oid")
    verify.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        SQLAlchemyError("synthetic backend refusal"),
        RegistryRefreshAnchorMismatch("exact epoch changed"),
        asyncio.CancelledError(),
    ],
)
async def test_generic_reader_preserves_refusal_and_cancellation_without_partial_scopes(monkeypatch, failure):
    monkeypatch.setattr(generic, "native_driver", AsyncMock(side_effect=failure))
    expected = RegistryRefreshAnchorUnavailable if isinstance(failure, SQLAlchemyError) else type(failure)
    async with AsyncSession() as session:
        with pytest.raises(expected) as rejected:
            async with session.begin():
                await generic.resolve_generic_refresh_anchors(
                    session, (_generic_binding(),), read_budget=RegistryPTGGraphReadBudget(1048576)
                )
        assert not session.in_transaction()
    if isinstance(failure, SQLAlchemyError):
        assert rejected.value.__cause__ is failure
        assert str(rejected.value) == "registry_refresh_generic_capture_unavailable"
    else:
        assert rejected.value is failure
