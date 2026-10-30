# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact resume identity is a hint, never a new candidate or execution authority."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_composition_resume as resume
from process.registry_candidate_composition import _target
from process.registry_source_contribution_store import (
    SUMMARY_KEY,
    registry_source_contribution_receipt,
    registry_source_contribution_sha256,
)
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from tests.test_registry_source_contributions import _descriptor, _recipe


def _registered(request_id, revision=1):
    recipe = _recipe()
    canonical = canonical_registry_source_recipes((recipe,))
    receipt = registry_source_contribution_receipt(
        SimpleNamespace(contribution_receipts=(_descriptor(recipe),), contribution_revision=revision)
    )
    generations_by_name = {
        RECIPE_DIGEST_KEY: registry_source_recipes_sha256(canonical),
        SUMMARY_KEY: registry_source_contribution_sha256(receipt),
        "custom_membership": "b" * 64,
    }
    copy_target = _target(request_id, 7, 9, generations_by_name)
    candidate_by_field = {
        **{
            field: getattr(copy_target, field)
            for field in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
        },
        "bounded": True,
        "state": "open",
        "approved_custom_revision": 7,
        "expected_head": 9,
        "source_recipes_json": canonical,
        "source_generations": generations_by_name,
        "validation_json": {SUMMARY_KEY: receipt},
    }
    return copy_target, candidate_by_field


def _connection(candidates, *, isolation="repeatable read", active=True):
    return SimpleNamespace(
        is_in_transaction=lambda: active,
        fetchval=AsyncMock(return_value=isolation),
        fetch=AsyncMock(return_value=candidates),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [1, 2])
@pytest.mark.parametrize("state", ["open", "sealed", "validated", "ready", "published"])
async def test_exact_registered_contract_preserves_revision_target_and_published_replay(revision, state):
    request_id = uuid4()
    copy_target, candidate = _registered(request_id, revision)
    candidate["state"] = state
    selected = await resume.resolve_registered_contribution_resume(
        _connection([candidate]), request_id, 7, 9, control_schema="synthetic_control"
    )
    assert selected == resume.RegisteredContributionResume(copy_target, revision)
    changed = _target(request_id, 7, 9, {**candidate["source_generations"], "custom_membership": "c" * 64})
    assert selected.target != changed


@pytest.mark.asyncio
async def test_no_matching_registration_alone_returns_none():
    connection = _connection([])
    assert (
        await resume.resolve_registered_contribution_resume(
            connection, uuid4(), 7, 9, control_schema="synthetic_control"
        )
        is None
    )
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,changed,reason",
    [
        ("dataset_id", str(uuid4()), "request_scope_changed"),
        ("schema_id", str(uuid4()), "request_scope_changed"),
        ("producer_id", str(uuid4()), "request_scope_changed"),
        ("approved_custom_revision", 8, "approval_scope_changed"),
        ("expected_head", 10, "approval_scope_changed"),
        ("state", "rejected", "state_invalid"),
        ("state", "unknown", "state_invalid"),
        ("bounded", False, "metadata_unbounded"),
        ("schema_name", "other_schema", "unavailable"),
    ],
)
async def test_registered_mismatch_never_becomes_a_fresh_candidate(field, changed, reason):
    request_id = uuid4()
    _, candidate = _registered(request_id)
    candidate[field] = changed
    with pytest.raises(resume.RegistryCompositionResumeError, match=reason):
        await resume.resolve_registered_contribution_resume(
            _connection([candidate]), request_id, 7, 9, control_schema="synthetic_control"
        )


@pytest.mark.asyncio
async def test_conflicting_or_unversioned_rows_refuse_without_ignoring_matches():
    request_id = uuid4()
    _, candidate = _registered(request_id)
    with pytest.raises(resume.RegistryCompositionResumeError, match="ambiguous"):
        await resume.resolve_registered_contribution_resume(
            _connection([candidate, copy.deepcopy(candidate)]), request_id, 7, 9, control_schema="synthetic_control"
        )
    candidate["source_generations"].pop(SUMMARY_KEY)
    candidate["validation_json"] = {}
    with pytest.raises(resume.RegistryCompositionResumeError, match="unversioned"):
        await resume.resolve_registered_contribution_resume(
            _connection([candidate]), request_id, 7, 9, control_schema="synthetic_control"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("request_id,revision,head", [("not-a-uuid", 7, 9), (uuid4(), True, 9), (uuid4(), 7, -1)])
async def test_bad_request_scope_never_queries_metadata(request_id, revision, head):
    connection = _connection([])
    with pytest.raises(resume.RegistryCompositionResumeError, match="scope_invalid"):
        await resume.resolve_registered_contribution_resume(connection, request_id, revision, head)
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("active,isolation", [(False, "repeatable read"), (True, "read committed")])
async def test_resume_selection_requires_the_callers_repeatable_snapshot(active, isolation):
    connection = _connection([], active=active, isolation=isolation)
    with pytest.raises(resume.RegistryCompositionResumeError, match="transaction_required"):
        await resume.resolve_registered_contribution_resume(connection, uuid4(), 7, 9)
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_final_identity_guard_preserves_manual_and_fact_targets_and_refuses_siblings():
    request_id = uuid4()
    copy_target, candidate = _registered(request_id)
    for declared in (True, False):
        current = copy.deepcopy(candidate)
        if not declared:
            current["source_generations"].pop(SUMMARY_KEY)
            current["validation_json"] = {}
        connection = _connection([current])
        assert (
            await resume.require_registered_composition_target(
                connection, request_id, 7, 9, copy_target, control_schema="synthetic_control"
            )
            is None
        )
        changed = _target(request_id, 7, 9, {"changed": "d" * 64})
        with pytest.raises(resume.RegistryCompositionResumeError, match="target_changed"):
            await resume.require_registered_composition_target(
                connection, request_id, 7, 9, changed, control_schema="synthetic_control"
            )
    assert (
        await resume.require_registered_composition_target(
            _connection([]), request_id, 7, 9, copy_target, control_schema="synthetic_control"
        )
        is None
    )


@pytest.mark.asyncio
async def test_final_identity_guard_rejects_wrong_computed_request_scope_before_query():
    copy_target, _ = _registered(uuid4())
    connection = _connection([])
    with pytest.raises(resume.RegistryCompositionResumeError, match="computed_scope_invalid"):
        await resume.require_registered_composition_target(connection, uuid4(), 7, 9, copy_target)
    connection.fetch.assert_not_awaited()


@pytest.mark.parametrize("revision", [True, 0, 3])
def test_registered_contribution_revision_cannot_select_an_unknown_resume_contract(revision):
    target, _ = _registered(uuid4())
    with pytest.raises(resume.RegistryCompositionResumeError, match="resume_invalid"):
        resume.RegisteredContributionResume(target, revision)


@pytest.mark.asyncio
async def test_mapping_marker_default_is_explicit_and_never_coerces_a_caller_value():
    connection = _connection([])
    with pytest.raises(resume.RegistryCompositionResumeError, match="resume_invalid"):
        await resume.resolve_registered_legacy_mapping_marker(connection, uuid4(), 7, 9, "a" * 64, default=1)
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolver,field",
    [
        (resume.resolve_registered_contribution_resume, "validation_json"),
        (resume.resolve_registered_source_inventory, "source_recipes_json"),
        (resume.resolve_registered_compact_format, "source_generations"),
    ],
)
async def test_corrupt_registered_metadata_cannot_fall_back_to_fresh_defaults(resolver, field):
    request_id = uuid4()
    _, candidate = _registered(request_id)
    candidate[field] = "[]"
    connection = _connection([candidate])
    with pytest.raises(resume.RegistryCompositionResumeError, match="resume_unavailable"):
        await resolver(connection, request_id, 7, 9, control_schema="synthetic_control")
    connection.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_absent_legacy_office_registration_is_a_noop_not_a_new_candidate():
    connection = _connection([])
    assert await resume.resolve_registered_legacy_office_inputs(connection, uuid4(), 7, 9) is None
    connection.fetch.assert_awaited_once()
