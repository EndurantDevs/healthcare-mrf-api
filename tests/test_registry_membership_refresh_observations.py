# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native-row association checks with controlled SQL results; no fixture JSON grants custody."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid5

import pytest

from process import registry_membership_refresh_ptg_observation as ptg_observation
from process import registry_membership_refresh_runtime as runtime
from process.registry_membership_refresh_contract import (
    SERVICE_ACTOR,
    RegistryMembershipRefreshUnavailable,
    membership_refresh_command_sha256,
)
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_publication_queue import _request_metadata
from process.registry_publication_worker import parse_registry_publication_worker_config
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable
from tests.test_registry_membership_refresh_contract import command
from tests.test_registry_membership_refresh_runtime import configuration
from tests.test_registry_publication_worker import configuration as worker_configuration

REQUEST = UUID("d69a5cb2-4333-43eb-bba8-e03b6108599b")
CANDIDATE = "d0cbbacb-0f0f-4813-97ef-1e05e38542e4"


def published_rows():
    original = command(expected_serving_generation=0, expected_serving_manifest_sha256=None)
    stored_by_field = {field: field_value for field, field_value in original.items() if field != "idempotency_key"}
    native_request_by_field = {
        **_request_metadata(stored_by_field, SERVICE_ACTOR, None, original["idempotency_key"]),
        "request_id": REQUEST,
        "session_token_sha256": None,
        "idempotency_key": original["idempotency_key"],
        "state": "completed",
        "result_json": {
            "operation": "refresh_memberships",
            "previous_generation_id": 0,
            "generation_id": 1,
            "candidate_id": CANDIDATE,
            "manifest_sha256": "f" * 64,
            "approved_revision": original["expected_approved_revision"],
            "command_sha256": membership_refresh_command_sha256(original),
            "policy_revision": 9,
            "orphan_bindings": 0,
        },
    }
    candidate_by_field = {name + "_id": str(uuid5(REQUEST, name)) for name in ("dataset", "schema", "producer")}
    candidate_by_field.update(
        approved_custom_revision=original["expected_approved_revision"],
        expected_head=0,
        address_generation="native-address-generation",
        address_artifact_sha256=original["address_artifact_sha256"],
    )
    manifest = SimpleNamespace(
        candidate_id=CANDIDATE,
        generation_id=1,
        manifest_sha256="f" * 64,
        approved_custom_revision=original["expected_approved_revision"],
        source_generations={
            "network_address_origin": original["address_artifact_sha256"],
            "unified_address": "native-address-generation",
        },
    )
    return original, native_request_by_field, candidate_by_field, manifest


@pytest.mark.asyncio
async def test_published_digest_is_derived_from_typed_command_and_native_candidate_scope():
    original, native_request_by_field, candidate_by_field, manifest = published_rows()
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[native_request_by_field]), fetchrow=AsyncMock(return_value=candidate_by_field)
    )
    digest, restored_command = await runtime._published_request(connection, "registry", manifest)
    assert restored_command == original
    assert digest == runtime._semantic_digest(
        original["accepted_sources"],
        original["expected_approved_revision"],
        candidate_by_field["address_generation"],
        original["address_artifact_sha256"],
        original["enrolled_scopes"],
    )
    assert connection.fetch.await_args.args[2] == CANDIDATE


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["duplicate", "scope", "manifest", "address", "command", "candidate"])
async def test_receipt_or_identity_alone_cannot_claim_a_published_input_digest(damage):
    _original, native_request_by_field, candidate_by_field, manifest = published_rows()
    requests = [native_request_by_field]
    match damage:
        case "duplicate":
            requests.append(native_request_by_field)
        case "scope":
            candidate_by_field["dataset_id"] = CANDIDATE
        case "manifest":
            native_request_by_field["result_json"]["manifest_sha256"] = "e" * 64
        case "address":
            candidate_by_field["address_artifact_sha256"] = "e" * 64
        case "command":
            native_request_by_field["request_sha256"] = "e" * 64
        case _:
            candidate_by_field = None
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=requests), fetchrow=AsyncMock(return_value=candidate_by_field)
    )
    with pytest.raises((ValueError, RegistryMembershipRefreshUnavailable)):
        await runtime._published_request(connection, "registry", manifest)


@pytest.mark.asyncio
async def test_no_associated_service_request_means_unknown_published_digest():
    *_unused, manifest = published_rows()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[]))
    assert await runtime._published_request(connection, "registry", manifest) is None
    assert await runtime._published_request(connection, "registry", None) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["namespace", "database", "roles", "limits", "disabled"])
async def test_worker_bootstrap_rejects_misaligned_host_profile_before_any_pool(monkeypatch, damage):
    worker = parse_registry_publication_worker_config(json.dumps(worker_configuration()))
    configured_by_field = configuration()
    configured_by_field.update(runtime_roles=list(worker.consumer.reader_roles))
    for name in ("fence_dsn", "source_reader_dsn", "address_reader_dsn"):
        configured_by_field[name] = configured_by_field[name].replace("db.example/", "db.example.test/")
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "registry")
    if damage == "namespace":
        configured_by_field["control_schema"] = "other_registry"
    elif damage == "database":
        for name in ("fence_dsn", "source_reader_dsn", "address_reader_dsn"):
            configured_by_field[name] += "2"
    elif damage == "roles":
        configured_by_field["runtime_roles"] = ["other_reader"]
    elif damage == "limits":
        configured_by_field["sql_limits"] = {
            **asdict(worker.sql_limits),
            "work_mem_bytes": worker.sql_limits.work_mem_bytes * 2,
        }
    else:
        configured_by_field = {"enabled": False}
    monkeypatch.setenv(runtime.CONFIG_ENV, json.dumps(configured_by_field))
    build = AsyncMock(side_effect=AssertionError("unexpected native allocation"))
    monkeypatch.setattr(runtime, "_build_native_bundle", build)
    async with AsyncExitStack() as cleanup:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime.membership_refresh_worker_runtime(worker, cleanup)
    build.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError("capture changed"), asyncio.CancelledError()])
@pytest.mark.parametrize("restore_fails", [False, True])
async def test_ptg_observation_restores_search_path_without_replacing_the_original_failure(
    monkeypatch, failure, restore_fails
):
    from process import network_cms_registry_address_capture

    restore_error = RuntimeError("synthetic restore refusal")
    session = SimpleNamespace(
        scalar=AsyncMock(return_value="original_schema,public"),
        execute=AsyncMock(side_effect=[None, restore_error if restore_fails else None]),
    )
    readers = SimpleNamespace(control_schema="registry", ptg_store=object())
    monkeypatch.setattr(network_cms_registry_address_capture, "native_driver", AsyncMock(return_value=object()))
    monkeypatch.setattr(ptg_observation, "require_ptg_office_authority", AsyncMock())
    observed = AsyncMock(return_value=({}, ()), side_effect=failure)
    monkeypatch.setattr(ptg_observation, "_observed_captures", observed)
    expected = failure if failure is not None else restore_error if restore_fails else None
    if expected is None:
        assert await ptg_observation.observe_accepted_ptg_recipes(
            session, (), None, readers, RegistryPTGGraphReadBudget(1048576)
        ) == ({}, ())
    else:
        with pytest.raises(type(expected)) as rejected:
            await ptg_observation.observe_accepted_ptg_recipes(
                session, (), None, readers, RegistryPTGGraphReadBudget(1048576)
            )
        assert rejected.value is expected
    assert session.execute.await_count == 2
    assert session.execute.await_args.args[1] == {"path": "original_schema,public"}


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ["replay", "partial", "conflicting_capture"])
async def test_ptg_metadata_deduplicates_exact_capture_and_refuses_partial_or_conflicting_recipes(
    monkeypatch, selection
):
    from process.registry_source_recipe_store import canonical_registry_source_recipes
    from tests.test_registry_publication_worker import _configured_recipe
    from tests.test_registry_refresh_anchor_adapters import _binding

    recipe = _configured_recipe("ptg")
    bindings = tuple(
        _binding(
            "ptg",
            index,
            **{
                key: field_value
                for key, field_value in asdict(recipe.binding_coordinates).items()
                if key != "source_system"
            },
            source_scope_json={"review_type": "published_complete_snapshot_plan"},
            office_recipe_json={
                "capture_id": recipe.source_pin.capture_id,
                "recipe_sha256": hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest(),
            },
        )
        for index in (1, 2)
    )
    second = (
        replace(recipe, source_pin=replace(recipe.source_pin, manifest_sha256="d" * 64))
        if selection == "conflicting_capture"
        else recipe
    )
    recipes = (recipe,) if selection == "partial" else (recipe, second)
    monkeypatch.setattr(ptg_observation, "_read_references", AsyncMock(return_value=recipes))
    verify = AsyncMock()
    monkeypatch.setattr(ptg_observation, "_capture_metadata", verify)
    readers = SimpleNamespace(control_schema="registry", ptg_store=object())
    arguments = (object(), object(), bindings, readers, RegistryPTGGraphReadBudget(1048576))
    if selection == "replay":
        scopes, actual = await ptg_observation._observed_captures(*arguments)
        assert actual == recipes and set(scopes) == {binding.binding_id for binding in bindings}
        assert all(scope["capture_generation"] == recipe.source_pin.generation_id for scope in scopes.values())
    else:
        with pytest.raises(
            (RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable), match="ptg_incomplete|capture_mismatch"
        ):
            await ptg_observation._observed_captures(*arguments)
    assert verify.await_count == (0 if selection == "partial" else 1)
