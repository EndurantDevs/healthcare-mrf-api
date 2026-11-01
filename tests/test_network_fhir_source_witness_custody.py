# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure scope and refusal checks; guard doubles never confer Source or candidate custody."""

import builtins
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from api.network_provider_only_read import _binding_scope
from process import network_approved_source_bindings as bindings
from process import network_fhir_source_witness_capture as capture_reads
from process import network_fhir_source_witness_contract as witness
from process import registry_source_recipe_composition as composition
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.test_network_fhir_source_witness_recipe import _LEGACY_COORDINATES, _reader_recipe, _source_recipe

pytestmark = pytest.mark.asyncio


def _profile(schema_name="source_example"):
    return FHIRCatalogSourceReadProfile(schema_name, "source_owner", ("source_publisher", "source_reader"))


def _runtime(**changes):
    runtime_by_field = {
        "source_export": Mock(side_effect=AssertionError("Source must not open in a refusal test")),
        "source_profiles": (_profile(),),
        "capture_owner_role": "owner_example",
        "runtime_roles": ("reader_example",),
    }
    return composition.SourceWitnessRuntime(**(runtime_by_field | changes))


def _with_capture(recipe, capture):
    anchor = witness.SourceWitnessCaptureAnchor(capture.capture_id, capture.release_id, capture.observed_at)
    pin = replace(
        recipe.source_pin,
        release_id=capture.release_id,
        source_witness_capture=capture,
        source_witness_anchor=anchor,
    )
    coordinates = replace(recipe.binding_coordinates, edition_id=capture.capture_id)
    return RegistrySourceMembershipRecipe(pin, coordinates)


@pytest.fixture
def source_refusals(monkeypatch):
    """All success-capable Source seams fail the test if a negative reaches them."""
    verify = AsyncMock(side_effect=AssertionError("Source verification must not be reached"))
    pin = AsyncMock(side_effect=AssertionError("A Source read must not be minted"))
    monkeypatch.setattr(capture_reads, "verify_source_fhir_witness_capture", verify)
    monkeypatch.setattr(capture_reads, "pin_source_fhir_witness_read", pin)
    yield verify, pin
    verify.assert_not_awaited()
    pin.assert_not_awaited()


@pytest.fixture
def protected_selection(monkeypatch):
    """Model a protected selection predicate only; no native admission is represented."""
    recipe = _source_recipe()
    approved = ApprovedMembershipSource(7, "a" * 64)
    expected = (
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        recipe.source_pin.source_witness_capture.capture_sha256,
    )
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=lambda _query, *parameters: parameters == expected))
    guard = AsyncMock()
    monkeypatch.setattr(bindings, "require_approved_network_source_bindings", guard)
    return connection, recipe, approved, guard


@pytest.mark.parametrize(
    "changes",
    [
        {"source_export": None},
        {"source_profiles": ()},
        {"source_profiles": [_profile()]},
        {"source_profiles": (_profile(), _profile())},
        {"source_profiles": (SimpleNamespace(schema_name="source_example"),)},
        {"runtime_roles": ()},
        {"runtime_roles": ["reader_example"]},
        {"runtime_roles": ("reader_example", "reader_example")},
        {"runtime_roles": ("z_reader", "a_reader")},
        {"runtime_roles": ("reader_example", None)},
        {"runtime_roles": ("owner_example", "reader_example")},
        {"runtime_roles": tuple(f"reader_{index:02}" for index in range(33))},
    ],
)
async def test_runtime_requires_complete_independent_configuration(changes):
    with pytest.raises(ValueError):
        _runtime(**changes)


async def test_default_read_context_preserves_legacy_absence():
    context = composition.SourceRecipeReadContext()
    assert context.source_witness_runtime is None
    assert context.retained_fhir_reads is None and context.verified_office_inputs is None
    assert context.office_authority is None and context.office_matches is None


@pytest.mark.parametrize("kind", ["Source", "Reader", "legacy"])
async def test_provider_only_scope_preserves_each_distinct_pin(kind):
    if kind == "legacy":
        pin = PinnedFHIRMembershipSource(**_LEGACY_COORDINATES)
        expected = ("alias_scope", "medical")
    else:
        pin = (_source_recipe() if kind == "Source" else _reader_recipe()).source_pin
        capture = pin.source_witness_capture if kind == "Source" else pin.retained_capture
        expected = ("capture_sha256", capture.capture_sha256)
    wire, generation = pin.coordinates, pin.generation_id
    assert _binding_scope(pin) == expected
    assert pin.coordinates == wire and pin.generation_id == generation
    if kind != "legacy":
        assert _binding_scope(replace(pin, alias_scope="different_alias")) == expected


@pytest.mark.parametrize("runtime", [None, SimpleNamespace(source_export=Mock())])
@pytest.mark.parametrize("cached", [False, True])
async def test_missing_runtime_cannot_borrow_a_producer_read(runtime, cached, source_refusals):
    recipe = _source_recipe()
    fake_read = SimpleNamespace(verify=AsyncMock())
    cache = {recipe.source_pin.source_witness_capture: fake_read} if cached else None
    connection = SimpleNamespace(fetchval=AsyncMock())
    with pytest.raises(composition.RegistrySourceCompositionError, match="custody_unavailable"):
        await composition.require_registry_source_recipe_custody(
            connection,
            recipe,
            ApprovedMembershipSource(7, "a" * 64),
            "control_example",
            retained_fhir_reads=cache,
            source_witness_runtime=runtime,
        )
    connection.fetchval.assert_not_awaited()
    fake_read.verify.assert_not_awaited()


@pytest.mark.parametrize(
    "changes",
    [
        {"capture_owner_role": "other_capture_owner"},
        {"runtime_roles": ("reader_example", "source_operator")},
        {"runtime_roles": ("metadata_owner_actor", "reader_example")},
    ],
)
async def test_approved_receipt_cannot_choose_its_owner_or_actor_census(changes, protected_selection, source_refusals):
    connection, recipe, approved, guard = protected_selection
    runtime = _runtime(**changes)
    with pytest.raises(composition.RegistrySourceCompositionError, match="census_differs"):
        await composition.require_source_witness_read(connection, recipe, approved, "control_example", runtime)
    guard.assert_awaited_once()
    runtime.source_export.assert_not_called()


async def test_metadata_owner_label_is_not_an_actor_exemption():
    with pytest.raises(composition.RegistrySourceCompositionError, match="runtime_invalid"):
        _runtime(capture_owner_role="metadata_owner_actor", runtime_roles=("metadata_owner_actor", "reader_example"))


async def test_profile_is_selected_from_configuration_not_receipt(protected_selection, source_refusals):
    connection, recipe, approved, _guard = protected_selection
    runtime = _runtime(source_profiles=(_profile("another_source"),))
    with pytest.raises(composition.RegistrySourceCompositionError, match="profile_unavailable"):
        await composition.require_source_witness_read(connection, recipe, approved, "control_example", runtime)
    runtime.source_export.assert_not_called()


async def test_configured_profile_refusal_does_not_use_producer_cache(protected_selection, source_refusals):
    connection, recipe, approved, _guard = protected_selection
    fake_read = SimpleNamespace(verify=AsyncMock())
    runtime = _runtime(source_profiles=(_profile("another_source"),))
    with pytest.raises(composition.RegistrySourceCompositionError, match="profile_unavailable"):
        await composition.require_registry_source_recipe_custody(
            connection,
            recipe,
            approved,
            "control_example",
            retained_fhir_reads={recipe.source_pin.source_witness_capture: fake_read},
            source_witness_runtime=runtime,
        )
    fake_read.verify.assert_not_awaited()
    runtime.source_export.assert_not_called()


@pytest.mark.parametrize("label", ["capture_id", "observed_at"])
async def test_rehashed_core_labels_do_not_replace_protected_capture_sha(label, protected_selection, source_refusals):
    connection, original, approved, guard = protected_selection
    capture = original.source_pin.source_witness_capture
    if label == "capture_id":
        identity = str(UUID(int=2))
        changed = replace(capture, capture_id=identity, schema_name=witness.SCHEMA_PREFIX + UUID(identity).hex)
    else:
        changed = replace(capture, observed_at="2026-01-03T00:00:00.000000+00:00")
    changed = replace(changed, capture_sha256=witness.capture_digest(changed))
    assert witness.validate_capture(changed) == changed
    recipe = _with_capture(original, changed)
    assert recipe.source_pin.as_of == capture.origin.semantic_projection_as_of
    assert recipe.source_pin.as_of != changed.observed_at[:10]
    runtime = _runtime()
    with pytest.raises(composition.RegistrySourceCompositionError, match="approval_unavailable"):
        await composition.require_source_witness_read(connection, recipe, approved, "control_example", runtime)
    guard.assert_awaited_once()
    assert connection.fetchval.await_args.args[-1] == changed.capture_sha256
    runtime.source_export.assert_not_called()


@pytest.mark.parametrize("field", ["catalog_sha256", "content_sha256"])
async def test_whole_capture_digest_precedes_approval_and_receipt_labels(field, monkeypatch, source_refusals):
    recipe = _source_recipe()
    damaged = replace(recipe.source_pin.source_witness_capture, **{field: "f" * 64})
    untrusted = SimpleNamespace(source_pin=SimpleNamespace(source_witness_capture=damaged))
    guard = AsyncMock()
    monkeypatch.setattr(bindings, "require_approved_network_source_bindings", guard)
    connection = SimpleNamespace(fetchval=AsyncMock())
    with pytest.raises(ValueError):
        await composition.require_source_witness_read(
            connection, untrusted, ApprovedMembershipSource(7, "a" * 64), "control_example", _runtime()
        )
    guard.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("field", ["producer_id", "source_id", "dataset_id", "dataset_schema", "edition_id"])
async def test_mixed_coordinates_refuse_before_source_open(field, protected_selection, source_refusals):
    connection, original, approved, _guard = protected_selection
    coordinates = replace(original.binding_coordinates, **{field: "other_example"})
    untrusted = SimpleNamespace(source_pin=original.source_pin, binding_coordinates=coordinates)
    runtime = _runtime()
    with pytest.raises(ValueError):
        await composition.require_source_witness_read(connection, untrusted, approved, "control_example", runtime)
    connection.fetchval.assert_not_awaited()
    runtime.source_export.assert_not_called()


async def test_absent_protected_approval_does_not_admit_source(protected_selection, source_refusals):
    connection, recipe, _approved, guard = protected_selection
    runtime = _runtime()
    with pytest.raises(ValueError):
        await composition.require_source_witness_read(connection, recipe, None, "control_example", runtime)
    guard.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    runtime.source_export.assert_not_called()


async def test_missing_installed_source_adapter_has_no_reader_fallback(
    monkeypatch, protected_selection, source_refusals
):
    connection, recipe, approved, _guard = protected_selection
    original_import = builtins.__import__

    def missing_source(name, *arguments, **options):
        if name == "directory_membership_runtime":
            raise ModuleNotFoundError(name)
        return original_import(name, *arguments, **options)

    monkeypatch.setattr(builtins, "__import__", missing_source)
    runtime = _runtime()
    with pytest.raises(composition.RegistrySourceCompositionError, match="factory_unavailable"):
        await composition.require_source_witness_read(connection, recipe, approved, "control_example", runtime)
    runtime.source_export.assert_not_called()
