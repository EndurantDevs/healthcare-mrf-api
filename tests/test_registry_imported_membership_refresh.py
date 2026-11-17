# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Imported publication uses native mapping inputs rather than a second service grant."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_imported_membership_refresh as imported
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_contract import (
    SERVICE_ACTOR,
    accepted_membership_source_sha256,
    validated_membership_refresh_command,
)
from process.registry_membership_refresh_execution import _refresh_completion
from process.registry_membership_refresh_queue import _validated_completion
from tests.test_registry_membership_refresh_contract import command


def imported_command():
    sources = [
        {
            "source_id": "source-one",
            "capture_generation": "d" * 64,
            "bindings_sha256": "e" * 64,
            "transform_version": imported.TRANSFORM_VERSION,
        }
    ]
    return command(
        accepted_sources=sources,
        accepted_source_vector_sha256=accepted_membership_source_sha256(sources),
        enrolled_scopes=[],
    )


@pytest.mark.asyncio
async def test_native_admission_needs_exact_imported_vector_and_never_fabricates_grant_receipts():
    admission = imported.RegistryImportedMembershipAdmission()
    selected = imported_command()
    assert validated_membership_refresh_command(selected) == selected
    assert imported.imported_catalog_generation(selected) == "d" * 64
    assert await admission(actor=SERVICE_ACTOR, command=selected) is None
    assert await admission.authorize_commit(actor=SERVICE_ACTOR, command=selected) is None
    with pytest.raises(PermissionError):
        await admission(actor=SERVICE_ACTOR, command=command())
    with pytest.raises(ValueError):
        await admission(actor={"kind": "platform_admin"}, command=selected)


@pytest.mark.parametrize("damage", ["legacy_empty", "mixed", "generation", "uppercase", "enrollment"])
def test_native_scope_cannot_be_partial_or_borrow_legacy_authority(damage):
    selected = imported_command()
    if damage == "legacy_empty":
        selected = command(enrolled_scopes=[])
    elif damage == "enrollment":
        selected["enrolled_scopes"] = command()["enrolled_scopes"]
    elif damage == "uppercase":
        selected["accepted_sources"][0]["capture_generation"] = "D" * 64
    else:
        second = deepcopy(selected["accepted_sources"][0])
        second["source_id"] = "source-two"
        second["transform_version" if damage == "mixed" else "capture_generation"] = (
            "legacy-v1" if damage == "mixed" else "f" * 64
        )
        selected["accepted_sources"].append(second)
    selected["accepted_source_vector_sha256"] = accepted_membership_source_sha256(selected["accepted_sources"])
    with pytest.raises(ValueError):
        validated_membership_refresh_command(selected)


@pytest.mark.asyncio
async def test_native_catalog_recheck_rejects_a_partial_or_stale_source_vector(monkeypatch):
    selected = imported_command()
    loader = AsyncMock(return_value=("d" * 64, selected["accepted_sources"], ["native-descriptor"]))
    monkeypatch.setattr(imported, "imported_catalog_inputs", loader)
    assert await imported.require_imported_catalog_inputs(object(), selected, "registry") == (
        "d" * 64,
        ["native-descriptor"],
    )
    loader.return_value = ("d" * 64, [], [])
    with pytest.raises(RegistryApprovalConflict):
        await imported.require_imported_catalog_inputs(object(), selected, "registry")


def test_native_completion_is_bound_to_command_without_an_external_policy_revision():
    selected = imported_command()
    target = SimpleNamespace(candidate_id="00000000-0000-0000-0000-000000000001")
    result = _refresh_completion(
        selected,
        target,
        {"generation_id": 10, "manifest_sha256": "f" * 64},
        None,
        {"validation_json": {"orphan_bindings": 0}},
    )
    assert "policy_revision" not in result
    _validated_completion(result, selected)
    with pytest.raises(ValueError):
        _validated_completion({**result, "policy_revision": 1}, selected)


def test_native_api_configuration_needs_no_app_origin_or_service_grant_token():
    from process import registry_membership_refresh_runtime as runtime
    from tests.test_registry_membership_refresh_runtime import configuration

    configured = configuration()
    configured.pop("app_origin")
    configured.pop("service_token")
    settings = runtime._settings(configured)
    assert settings.app_origin is settings.service_token is None
    assert settings.control_schema == "registry"
    for missing in ("app_origin", "service_token"):
        damaged = configuration()
        damaged.pop(missing)
        with pytest.raises(RuntimeError):
            runtime._settings(damaged)


@pytest.mark.asyncio
async def test_native_observation_uses_current_catalog_and_address_without_enrollment_http(monkeypatch):
    from process import registry_membership_refresh_runtime as runtime
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    selected = imported_command()["accepted_sources"]
    observe = AsyncMock(return_value=("d" * 64, selected, ["native-descriptor"]))
    monkeypatch.setattr(runtime, "imported_catalog_inputs", observe)
    document = await case.service._observe_imported(object())
    assert document["accepted_sources"] == selected
    assert document["enrolled_scopes"] == []
    assert document["ready_address_generation"] == case.pin.generation_id
    assert document["address_artifact_sha256"] == case.descriptor["descriptor_sha256"]
    observe.assert_awaited_once()
    assert case.events == ["address_metadata"]


def test_native_worker_configuration_cannot_be_used_as_an_unscoped_manual_worker():
    import json

    from process import registry_publication_worker as worker
    from tests.test_registry_source_witness_runtime import _document

    document = _document()
    document.pop("app_origin")
    document.pop("service_token")
    parsed = worker.parse_registry_publication_worker_config(json.dumps(document))
    assert parsed.app_origin is parsed.service_token is None
    document.pop("source_witness")
    with pytest.raises(ValueError):
        worker.parse_registry_publication_worker_config(json.dumps(document))
