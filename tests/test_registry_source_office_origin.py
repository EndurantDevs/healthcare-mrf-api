# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed origin locators and legacy-compatible office pin receipts."""

import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.network_address_projection import PinnedAddressSource
from process.network_initial_cms_office_bindings import PinnedCMSOfficeAddressSource
from process.registry_initial_source_composition import (
    SUMMARY_KEY,
    RegistryInitialSourceError,
    _is_receipt_match,
    initial_source_office_receipt_sha256,
    validate_initial_source_office_receipt,
    verify_initial_source_office_receipt,
)
from process.registry_source_office_origin import resolve_source_office_origin, source_office_pin_document


def _receipt(**changes):
    receipt_by_field = dict(
        component=SUMMARY_KEY,
        revision=1,
        recipe_sha256="a" * 64,
        approved_revision=3,
        address_generation_sha256="b" * 64,
        recipe_count=1,
        office_rows=1,
        page_count=1,
        correspondence_sha256="c" * 64,
    )
    return {**receipt_by_field, **changes}


def test_legacy_pin_document_keeps_its_exact_original_shape():
    pin = PinnedCMSOfficeAddressSource(
        PinnedAddressSource("source", "entity_address_unified", "b" * 64),
        (1, 2, 3, "d" * 64),
        "e" * 64,
        "f" * 64,
        "a" * 64,
        "c" * 64,
        None,
        1,
    )
    legacy = asdict(pin)
    legacy.pop("origin_manifest_sha256")
    assert source_office_pin_document(pin) == legacy
    located = source_office_pin_document(replace(pin, origin_manifest_sha256="d" * 64))
    assert located == {**legacy, "origin_manifest_sha256": "d" * 64}


@pytest.mark.parametrize("revision", [1, 2])
def test_summary_revision_binds_only_its_exact_origin_contract(revision):
    receipt = _receipt(revision=revision, **({"origin_manifest_sha256": "d" * 64} if revision == 2 else {}))
    assert validate_initial_source_office_receipt(receipt) == receipt
    candidate_by_field = dict(
        approved_custom_revision=3,
        source_generations=dict(
            registry_source_recipes="a" * 64,
            unified_address="b" * 64,
            registry_initial_source_offices=initial_source_office_receipt_sha256(receipt),
            retained_network_source="d" * 64,
        ),
    )
    assert _is_receipt_match(candidate_by_field, receipt, (object(),))
    candidate_by_field["source_generations"]["retained_network_source"] = "e" * 64
    assert _is_receipt_match(candidate_by_field, receipt, (object(),)) is (revision == 1)


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": True},
        {"revision": 3},
        {"origin_manifest_sha256": "d" * 64},
        {"revision": 2},
        {"revision": 2, "origin_manifest_sha256": None},
        {"revision": 2, "origin_manifest_sha256": "D" * 64},
        {"revision": 2, "origin_manifest_sha256": "d" * 64, "verified": True},
    ],
)
def test_summary_refuses_missing_extra_or_unclosed_origin_fields(changes):
    with pytest.raises(RegistryInitialSourceError):
        validate_initial_source_office_receipt(_receipt(**changes))


@pytest.mark.asyncio
@pytest.mark.parametrize("locator", [True, "", "a" * 63, "A" * 64, {}])
async def test_origin_locator_shape_cannot_reach_native_source_queries(locator):
    connection = AsyncMock()
    with pytest.raises(ValueError, match="origin_invalid"):
        await resolve_source_office_origin(
            connection,
            PinnedAddressSource("source", "entity_address_unified", "a" * 64),
            runtime_roles=("reader",),
            control_schema="control",
            origin_manifest_sha256=locator,
        )
    connection.fetchval.assert_not_awaited()


@pytest.mark.parametrize("receipt", [None, [], "null", '{"component":"wrong"}', '{"component":1}'])
def test_office_receipt_refuses_missing_or_wrong_component(receipt):
    with pytest.raises(RegistryInitialSourceError):
        validate_initial_source_office_receipt(receipt)


@pytest.mark.parametrize("stored", [False, True])
def test_office_receipt_cannot_claim_a_different_recipe_count(stored):
    receipt = _receipt(recipe_count=2)
    candidate_by_field = {
        "approved_custom_revision": 3,
        "source_generations": {"registry_source_recipes": "a" * 64, "unified_address": "b" * 64},
    }
    assert _is_receipt_match(candidate_by_field, receipt, (object(),)) is False
    if stored:
        candidate_by_field["validation_json"] = {SUMMARY_KEY: receipt}
        candidate_by_field["source_generations"][SUMMARY_KEY] = "f" * 64
        with pytest.raises(RegistryInitialSourceError):
            verify_initial_source_office_receipt(candidate_by_field)


def test_persisted_receipt_json_has_the_same_closed_content_as_object_form():
    receipt = _receipt()
    assert validate_initial_source_office_receipt(json.dumps(receipt)) == receipt
    assert initial_source_office_receipt_sha256(json.dumps(receipt)) == initial_source_office_receipt_sha256(receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "untyped_source",
        "other_table",
        "roles_type",
        "roles_order",
        "role_duplicate",
        "parameter_type",
        "parameter_limit",
    ],
)
async def test_office_origin_requires_exact_source_roles_and_parameter_scope_before_sql(fault):
    source = PinnedAddressSource("source", "entity_address_unified", "a" * 64)
    if fault == "other_table":
        source = replace(source, table_name="other_addresses")
    roles_by_fault = {
        "roles_type": ["reader"],
        "roles_order": ("reader", "loader"),
        "role_duplicate": ("reader", "reader"),
    }
    connection = SimpleNamespace(fetchval=AsyncMock(), fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="origin_invalid"):
        await resolve_source_office_origin(
            connection,
            {} if fault == "untyped_source" else source,
            runtime_roles=roles_by_fault.get(fault, ("reader",)),
            control_schema="registry",
            first_parameter=True if fault == "parameter_type" else 32768 if fault == "parameter_limit" else 1,
        )
    connection.fetchval.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "noncompact", "source", "generation", "roles"])
async def test_retained_locator_cannot_relax_the_complete_office_origin_contract(monkeypatch, fault):
    from process import registry_source_office_origin as origins
    from tests.test_network_raw_provider_api_identity import _manifest

    address_source = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "a" * 64)
    manifest = _manifest(compact=fault != "noncompact")
    manifest.source_generations["unified_address"] = "b" * 64 if fault == "generation" else address_source.generation_id
    candidate_by_field = {
        "validation_json": {
            "address_source": asdict(
                replace(address_source, generation_id="b" * 64) if fault == "source" else address_source
            ),
            "serving_readiness": {
                "address_artifact": {"runtime_roles": ["other_reader"] if fault == "roles" else ["reader"]}
            },
        }
    }
    resolve = AsyncMock(return_value=None if fault == "missing" else (manifest, candidate_by_field))
    monkeypatch.setattr(origins, "resolve_registry_retained_generation", resolve)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="repeatable read"), fetchrow=AsyncMock()
    )
    with pytest.raises(ValueError, match="origin_unavailable|origin_differs"):
        await resolve_source_office_origin(
            connection,
            address_source,
            runtime_roles=("reader",),
            control_schema="registry",
            origin_manifest_sha256="a" * 64,
        )
    assert resolve.await_args.args[1] == {"source_generations": {"retained_network_source": "a" * 64}}
    connection.fetchrow.assert_not_awaited()


def _office_candidate():
    from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
    from tests.test_network_membership_pipeline import _phase_context
    from tests.test_registry_source_contributions import _recipe

    connection, copy_target, source, events = _phase_context("ready")
    candidate_by_field = connection.fetchrow.return_value
    canonical = canonical_registry_source_recipes((_recipe(),))
    recipe_digest = registry_source_recipes_sha256(canonical)
    receipt_by_field = _receipt(
        recipe_sha256=recipe_digest, approved_revision=7, address_generation_sha256=source.generation_id
    )
    candidate_by_field.update(
        source_recipes_json=canonical,
        source_generations={
            "registry_source_recipes": recipe_digest,
            "unified_address": source.generation_id,
            SUMMARY_KEY: initial_source_office_receipt_sha256(receipt_by_field),
        },
        validation_json={
            SUMMARY_KEY: receipt_by_field,
            "address_source": asdict(source),
            "writer_closure": {"loader_roles": ["loader"], "reader_roles": ["reader"]},
        },
    )
    return connection, copy_target, candidate_by_field, events


@pytest.mark.asyncio
async def test_compact_office_recipes_require_a_paired_summary_before_source_pinning(monkeypatch):
    from process import registry_source_office_custody as custody
    from process.network_compact_readiness import COMPONENT

    connection, copy_target, candidate, _ = _office_candidate()
    candidate["source_generations"].pop(SUMMARY_KEY)
    candidate["validation_json"].pop(SUMMARY_KEY)
    candidate["validation_json"]["serving_readiness"] = {"component": COMPONENT}
    pin = AsyncMock()
    monkeypatch.setattr(custody, "pin_retained_approved_membership_source", pin)
    with pytest.raises(RegistryInitialSourceError, match="summary_required"):
        await custody._recipe_bindings(connection, copy_target, candidate, "expected_offices", "registry", None)
    pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_office_correspondence_refuses_a_changed_approved_source_pin(monkeypatch):
    from process import registry_source_office_custody as custody
    from process.network_approved_membership_source import ApprovedMembershipSource

    connection, copy_target, candidate, _ = _office_candidate()
    candidate["source_generations"]["custom_membership"] = "b" * 64
    pin = AsyncMock(return_value=ApprovedMembershipSource(7, "c" * 64))
    monkeypatch.setattr(custody, "pin_retained_approved_membership_source", pin)
    summary = AsyncMock()
    monkeypatch.setattr(custody, "_office_summary", summary)
    with pytest.raises(RegistryInitialSourceError, match="approved_pin_differs"):
        await custody._recipe_bindings(connection, copy_target, candidate, "expected_offices", "registry", None)
    assert pin.await_args.kwargs == {"approved_revision": 7, "control_schema": "registry"}
    summary.assert_not_awaited()


@pytest.mark.asyncio
async def test_registered_recipes_cannot_treat_a_missing_native_source_relation_as_empty(monkeypatch):
    from process import registry_source_office_custody as custody

    connection, copy_target, candidate, events = _office_candidate()
    connection.fetchval.side_effect = ["repeatable read", None]
    copy = AsyncMock()
    monkeypatch.setattr(custody, "copy_source_office_binding_rows", copy)
    with pytest.raises(RegistryInitialSourceError, match="source_unavailable"):
        async with connection.transaction(isolation="repeatable_read"):
            await custody.verify_independent_initial_source_offices(
                connection, copy_target, candidate, control_schema="registry"
            )
    copy.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert events[-1] == "rollback"


@pytest.mark.asyncio
async def test_inventory_custody_refuses_a_legacy_office_summary_before_staging(monkeypatch):
    from process import registry_source_office_custody as custody
    from process.network_approved_membership_source import ApprovedMembershipSource
    from tests.test_registry_source_contributions import _recipe

    connection, copy_target, candidate, events = _office_candidate()
    connection.fetchval.return_value = "repeatable read"
    copy = AsyncMock()
    monkeypatch.setattr(custody, "copy_source_office_binding_rows", copy)
    context = custody.InventoryOfficeCustodyContext((_recipe(),), ApprovedMembershipSource(7, "a" * 64), lambda _: None)
    with pytest.raises(RegistryInitialSourceError, match="summary_required"):
        async with connection.transaction(isolation="repeatable_read"):
            await custody._verify_inventory_offices(
                connection, copy_target, candidate, context, control_schema="registry"
            )
    copy.assert_not_awaited()
    assert events[-1] == "rollback"


@pytest.mark.asyncio
async def test_office_staging_preserves_its_database_error_when_cleanup_also_fails(monkeypatch):
    import asyncpg

    from process import registry_source_office_custody as custody

    connection, copy_target, candidate, events = _office_candidate()
    connection.fetchval.side_effect = ["repeatable read", 42]
    staging_error = asyncpg.QueryCanceledError("staging statement timed out")
    cleanup_error = asyncpg.ConnectionDoesNotExistError("connection closed during cleanup")
    copy = AsyncMock(side_effect=staging_error)
    monkeypatch.setattr(custody, "copy_source_office_binding_rows", copy)
    connection.execute.side_effect = cleanup_error
    with pytest.raises(asyncpg.QueryCanceledError) as rejected:
        async with connection.transaction(isolation="repeatable_read"):
            await custody.verify_independent_initial_source_offices(
                connection, copy_target, candidate, control_schema="registry"
            )
    assert rejected.value is staging_error
    assert rejected.value._registry_office_cleanup_error is cleanup_error
    assert any("rollback" in note for note in rejected.value.__notes__)
    assert copy.await_args.args[1] == "pg_temp" and copy.await_args.args[3] == ()
    assert connection.execute.await_args.args[0].startswith('DROP TABLE IF EXISTS pg_temp."registry_source_offices_')
    assert events[-1] == "rollback" and not connection.is_in_transaction()
