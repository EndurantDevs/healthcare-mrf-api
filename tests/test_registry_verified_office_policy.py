# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed metadata and synthetic replay protocols; no native Source authority claim."""

import asyncio
import copy
import hashlib
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import registry_composition_resume as resume
from process import registry_verified_office_policy as policy
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import _generation_id
from process.registry_candidate_composition import _target
from process.registry_initial_source_composition import (
    SUMMARY_KEY as OFFICE_KEY,
)
from process.registry_initial_source_composition import (
    RegistryInitialSourceError,
    _office_receipt,
    initial_source_office_receipt_sha256,
)
from process.registry_source_recipe_composition import RegistrySourceRecipeStage, _source_stage_generation
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from process.registry_source_retention import (
    SUMMARY_KEY as RETENTION_KEY,
)
from process.registry_source_retention import (
    registry_source_retention_sha256,
)
from process.registry_source_selection_receipt import (
    SUMMARY_KEY as SELECTION_KEY,
)
from process.registry_source_selection_receipt import (
    build_registry_source_selection_receipt,
)
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_composition_resume import _connection


def _source_fixture(mode):
    """Build typed synthetic source coordinates and the unchanged generation algorithms."""
    capture = retained()
    source_pin = pin(capture)
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source_pin.source_id,
        source_pin.schema_name,
        source_pin.dataset_id,
        "registry-retained-normalized-fhir-v1",
        capture.capture_id,
    )
    recipes = (RegistrySourceMembershipRecipe(source_pin, coordinates),)
    encoded = canonical_registry_source_recipes(recipes)
    recipe_sha = registry_source_recipes_sha256(encoded)
    approved = ApprovedMembershipSource(7, "b" * 64, 0)
    address = PinnedAddressSource("address_fixture", "entity_address_unified", "c" * 64)
    summary = _office_receipt(recipes, approved, address, 1, 1, "d" * 64)
    raw = _generation_id(source_pin, approved, coordinates, True)
    mapped = hashlib.sha256(
        json.dumps([raw, summary["correspondence_sha256"]], separators=(",", ":")).encode()
    ).hexdigest()
    generations = (mapped,) if mode == "verified" else (raw,)
    stage_hash = _source_stage_generation(recipe_sha, approved, generations, 2, 3)
    matches = SimpleNamespace(table_name="initial_cms_matches_" + "f" * 32, page_proofs_by_key={})
    stage = RegistrySourceRecipeStage(
        "registry_source_page_" + "a" * 32,
        2,
        3,
        stage_hash,
        generations,
        raw_source_rows=4,
        accepted_raw_source_rows=2,
        omitted_raw_source_rows=1,
        contribution_revision=1,
        office_matches=matches,
        raw_source_generations=(raw,),
    )
    return SimpleNamespace(
        recipes=recipes,
        encoded=encoded,
        recipe_sha=recipe_sha,
        approved=approved,
        address=address,
        summary=summary,
        raw=raw,
        stage=stage,
    )


def _retention_metadata(stage):
    return {
        "component": RETENTION_KEY,
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": stage.generation_sha256,
        "source_rows": stage.membership_rows,
        "add_rows": 0,
        "remove_rows": 0,
        "suppressed_source_rows": 0,
        "effective_rows": stage.membership_rows,
    }


def _fixture(*, mode="verified"):
    """Bind the closed synthetic registration to an exact deterministic target."""
    source_scope = _source_fixture(mode)
    recipes, approved, address, summary = (
        source_scope.recipes,
        source_scope.approved,
        source_scope.address,
        source_scope.summary,
    )
    encoded, recipe_sha, raw, stage = (
        source_scope.encoded,
        source_scope.recipe_sha,
        source_scope.raw,
        source_scope.stage,
    )
    selection = build_registry_source_selection_receipt(recipes, approved, stage)
    retention_by_field = _retention_metadata(stage)
    generations_by_name = {
        RECIPE_DIGEST_KEY: recipe_sha,
        "custom_membership": approved.generation_id,
        "registry_source_membership": stage.generation_sha256,
        "initial_source_recipe_0": raw,
        "unified_address": address.generation_id,
        OFFICE_KEY: initial_source_office_receipt_sha256(summary),
        RETENTION_KEY: registry_source_retention_sha256(retention_by_field),
    }
    request = uuid4()
    copy_target = _target(request, 7, 0, generations_by_name)
    candidate_by_field = {
        **{
            name: getattr(copy_target, name)
            for name in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
        },
        "state": "ready",
        "bounded": True,
        "expected_rows": 2,
        "approved_custom_revision": 7,
        "expected_head": 0,
        "source_recipes_json": encoded,
        "source_generations": generations_by_name,
        "validation_json": {
            OFFICE_KEY: summary,
            SELECTION_KEY: selection,
            RETENTION_KEY: retention_by_field,
            "address_source": asdict(address),
            "writer_closure": {"loader_roles": [], "reader_roles": ["fixture_reader"]},
        },
    }
    return SimpleNamespace(
        candidate=candidate_by_field,
        recipes=recipes,
        approved=approved,
        address=address,
        inputs=(address, ()),
        stage=stage,
        summary=summary,
        request=request,
        target=copy_target,
    )


def _native_pin(monkeypatch, fixture, *, failure=None, returned=None):
    pinning = AsyncMock(side_effect=failure, return_value=fixture.approved if returned is None else returned)
    monkeypatch.setattr(policy, "pin_retained_approved_membership_source", pinning)
    return pinning


async def test_legacy_metadata_selection_requires_fresh_native_approval(monkeypatch):
    fixture, connection = _fixture(), object()
    pinning = _native_pin(monkeypatch, fixture)
    assert (
        await policy.select_legacy_verified_office_inputs(
            connection, fixture.candidate, control_schema="fixture", approved=fixture.approved
        )
        == fixture.inputs
    )
    pinning.assert_awaited_once_with(connection, approved_revision=7, control_schema="fixture")


async def test_structurally_eligible_strict_candidate_never_selects_verified_policy(monkeypatch):
    fixture = _fixture(mode="strict")
    _native_pin(monkeypatch, fixture)
    assert await policy.select_legacy_verified_office_inputs(object(), fixture.candidate) is None


async def test_historical_extra_raw_indices_do_not_change_current_policy_commitment(monkeypatch):
    fixture = _fixture()
    _native_pin(monkeypatch, fixture)
    fixture.candidate["source_generations"]["initial_source_recipe_99"] = "e" * 64
    assert await policy.select_legacy_verified_office_inputs(object(), fixture.candidate) == fixture.inputs


@pytest.mark.parametrize("failure", [RuntimeError("authority"), asyncio.CancelledError()])
async def test_native_approval_failure_is_terminal_not_an_alternate_policy(monkeypatch, failure):
    fixture = _fixture()
    _native_pin(monkeypatch, fixture, failure=failure)
    with pytest.raises(type(failure)) as refused:
        await policy.select_legacy_verified_office_inputs(object(), fixture.candidate)
    assert refused.value is failure


@pytest.mark.parametrize("field", ["native_generation", "provided_pin", "raw_commitment", "address"])
async def test_recognized_policy_refuses_changed_native_or_raw_commitments(monkeypatch, field):
    fixture = _fixture()
    other = ApprovedMembershipSource(7, "e" * 64)
    _native_pin(monkeypatch, fixture, returned=other if field == "native_generation" else fixture.approved)
    if field == "raw_commitment":
        fixture.candidate["source_generations"]["initial_source_recipe_0"] = "e" * 64
    if field == "address":
        fixture.candidate["validation_json"]["address_source"]["generation_id"] = "e" * 64
    with pytest.raises(RegistryInitialSourceError):
        await policy.select_legacy_verified_office_inputs(
            object(), fixture.candidate, approved=other if field == "provided_pin" else None
        )


async def test_unknown_and_versioned_modes_never_gain_legacy_selection(monkeypatch):
    fixture = _fixture()
    pinning = _native_pin(monkeypatch, fixture)
    fixture.candidate["source_generations"].pop(OFFICE_KEY)
    fixture.candidate["validation_json"].pop(OFFICE_KEY)
    assert await policy.select_legacy_verified_office_inputs(object(), fixture.candidate) is None
    pinning.assert_not_awaited()


async def test_resume_and_composer_metadata_seam_recognize_same_exact_legacy_target(monkeypatch):
    fixture = _fixture()
    _native_pin(monkeypatch, fixture)
    connection = _connection([fixture.candidate])
    assert await resume.resolve_registered_contribution_resume(
        connection, fixture.request, 7, 0, control_schema="fixture"
    ) == resume.RegisteredContributionResume(fixture.target, 1)
    assert (
        await resume.resolve_registered_legacy_office_inputs(
            connection, fixture.request, 7, 0, control_schema="fixture"
        )
        == fixture.inputs
    )
    assert "c.expected_rows" in connection.fetch.await_args.args[0]


async def test_strict_unversioned_resume_still_refuses(monkeypatch):
    fixture = _fixture(mode="strict")
    _native_pin(monkeypatch, fixture)
    with pytest.raises(resume.RegistryCompositionResumeError, match="unversioned"):
        await resume.resolve_registered_contribution_resume(_connection([fixture.candidate]), fixture.request, 7, 0)
    assert (
        await resume.resolve_registered_legacy_office_inputs(_connection([fixture.candidate]), fixture.request, 7, 0)
        is None
    )


async def test_resume_native_statement_failure_is_bounded_and_never_becomes_no_match(monkeypatch):
    fixture = _fixture()
    _native_pin(monkeypatch, fixture, failure=asyncpg.PostgresError("synthetic refusal"))
    with pytest.raises(resume.RegistryCompositionResumeError, match="unavailable"):
        await resume.resolve_registered_legacy_office_inputs(_connection([fixture.candidate]), fixture.request, 7, 0)


@pytest.mark.parametrize("compact", [False, True])
async def test_registered_physical_format_is_independent_of_recipe_inventory(compact):
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY

    fixture = _fixture()
    if compact:
        fixture.candidate["source_generations"][COMPACT_FORMAT_KEY] = COMPACT_FORMAT
    assert (
        await resume.resolve_registered_compact_format(_connection([fixture.candidate]), fixture.request, 7, 0)
        is compact
    )
    assert await resume.resolve_registered_compact_format(_connection([]), fixture.request, 7, 0) is None


@pytest.mark.parametrize("marker", ["unknown", True, None])
async def test_unknown_registered_physical_format_cannot_become_an_address_copy(marker):
    from process.registry_compact_composition import COMPACT_FORMAT_KEY

    fixture = _fixture()
    fixture.candidate["source_generations"][COMPACT_FORMAT_KEY] = marker
    with pytest.raises(resume.RegistryCompositionResumeError, match="format_unknown"):
        await resume.resolve_registered_compact_format(_connection([fixture.candidate]), fixture.request, 7, 0)


async def test_registered_physical_format_preserves_request_scope_and_cancellation():
    fixture = _fixture()
    changed = copy.deepcopy(fixture.candidate)
    changed["dataset_id"] = str(uuid4())
    with pytest.raises(resume.RegistryCompositionResumeError, match="request_scope_changed"):
        await resume.resolve_registered_compact_format(_connection([changed]), fixture.request, 7, 0)
    cancellation = asyncio.CancelledError("lookup")
    connection = _connection([fixture.candidate])
    connection.fetch.side_effect = cancellation
    with pytest.raises(asyncio.CancelledError) as refused:
        await resume.resolve_registered_compact_format(connection, fixture.request, 7, 0)
    assert refused.value is cancellation


async def test_explicit_compact_storage_never_selects_incoming_full_address_policy(monkeypatch):
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY

    fixture = _fixture()
    fixture.candidate["source_generations"][COMPACT_FORMAT_KEY] = COMPACT_FORMAT
    pinning = _native_pin(monkeypatch, fixture)
    assert await policy.select_legacy_verified_office_inputs(object(), fixture.candidate) is None
    pinning.assert_not_awaited()


async def test_prepared_address_supports_sealed_replay(monkeypatch):
    from process import registry_candidate_composition as composition

    fixture = _fixture()
    candidate = fixture.candidate
    generations_by_source = dict(candidate["source_generations"])
    expected = candidate["validation_json"].pop("address_source")
    candidate["state"] = "open"

    async def record(query, *parameters):
        assert "state='open'" in query and "source_generations->>'unified_address'=$3" in query
        assert "OR validation_json->'address_source'=$2::jsonb" in query
        assert parameters == (
            UUID(fixture.target.candidate_id),
            json.dumps(expected, sort_keys=True, separators=(",", ":")),
            fixture.address.generation_id,
        )
        candidate["validation_json"]["address_source"] = json.loads(parameters[1])
        return [{"recorded": True}]

    connection = SimpleNamespace(fetch=record, execute=AsyncMock())
    inputs = SimpleNamespace(
        connection=connection,
        approved=fixture.approved,
        office_context=None,
        retained_fhir_reads={},
        source_manifest=None,
    )
    artifact = SimpleNamespace(address_source=fixture.address)
    receipts = AsyncMock()
    monkeypatch.setattr(composition, "_record_source_receipts", receipts)
    await composition._record_prepared_composition(
        inputs, fixture.target, (fixture.recipes, fixture.stage, artifact, fixture.summary, None, ()), "fixture"
    )
    receipts.assert_awaited_once()
    connection.execute.assert_awaited_once_with(
        "CREATE INDEX IF NOT EXISTS provider_location_binding_location_idx "
        f'ON "{fixture.target.schema_name}".provider_location_binding USING hash(location_key)'
    )
    candidate["state"] = "sealed"
    pinning = AsyncMock(return_value=fixture.approved)
    monkeypatch.setattr(policy, "pin_retained_approved_membership_source", pinning)
    assert await policy.select_legacy_verified_office_inputs(object(), candidate) == fixture.inputs
    assert (
        await resume.resolve_registered_legacy_office_inputs(
            _connection([candidate]), fixture.request, 7, 0, control_schema="fixture"
        )
        == fixture.inputs
    )
    assert pinning.await_count == 2 and candidate["source_generations"] == generations_by_source
    candidate["validation_json"]["address_source"]["generation_id"] = "e" * 64
    with pytest.raises(policy.RegistryInitialSourceError, match="address_differs"):
        await policy.select_legacy_verified_office_inputs(object(), candidate)


@pytest.mark.parametrize("recorded", [[], [{"recorded": True}, {"recorded": True}], [{"recorded": False}]])
async def test_prepared_address_conflicts_precede_receipts(monkeypatch, recorded):
    from process import registry_candidate_composition as composition

    fixture = _fixture()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=recorded), execute=AsyncMock())
    inputs = SimpleNamespace(connection=connection)
    artifact = SimpleNamespace(address_source=fixture.address)
    receipts = AsyncMock()
    monkeypatch.setattr(composition, "_record_source_receipts", receipts)
    with pytest.raises(composition.RegistryCompositionError, match="address_scope_changed"):
        await composition._record_prepared_composition(
            inputs, fixture.target, (fixture.recipes, fixture.stage, artifact, fixture.summary, None, ()), "fixture"
        )
    receipts.assert_not_awaited()
    connection.fetch.assert_awaited_once()
    connection.execute.assert_not_awaited()
