# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native linkage cannot repair changed catalogs or outlive its original transaction."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest

from process import provider_directory_cms_native_registration as registration
from tests.test_cms_native_input_registration import NativeProtocol, _enroll, _writes
from tests.test_nucc_protected_bootstrap import native_registration


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["verify", "catalog", "guard", "adopt", "retired"])
async def test_native_observers_refuse_closed_original_transactions_before_catalog_io(operation):
    native = NativeProtocol()
    native.active = False
    candidate = native.candidate()
    with pytest.raises(RuntimeError, match="cms_native_input_original_transaction_required"):
        match operation:
            case "verify":
                await registration.verify_native_input_candidates(
                    native, ledger_schema="sample", candidates=(candidate,)
                )
            case "catalog":
                await registration.verify_native_input_candidate_catalog(
                    native, ledger_schema="sample", candidates=(candidate,), registrations=[]
                )
            case "guard":
                await registration.resolve_installed_native_input_guard(native, ledger_schema="sample", relation_oid=41)
            case "adopt":
                await registration.capture_native_input_adoption(native, ledger_schema="sample", candidate=candidate)
            case "retired":
                await registration.verify_retired_native_input_adoption(native, evidence=native_registration())
    assert not native.calls
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
async def test_actor_mismatch_refuses_registration_before_opening_mutation_scope():
    native = NativeProtocol()
    with pytest.raises(RuntimeError, match="cms_native_input_operator_actor_changed"):
        await registration.register_native_input_candidates(
            native, ledger_schema="sample", candidates=(native.candidate(),), expected_actor_oid=18
        )
    assert native.rows == native.guards == {}
    assert not _writes(native)
    assert len(native.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["foreign", "missing", "duplicate"])
async def test_initial_root_census_cannot_hide_or_substitute_an_input(fault):
    native = NativeProtocol()
    fetch = native.fetch

    async def changed_roster(statement, *arguments):
        catalog_records = await fetch(statement, *arguments)
        if fault == "foreign":
            catalog_records[0]["namespace"] = "foreign"
        elif fault == "missing":
            catalog_records.pop()
        else:
            catalog_records.append(catalog_records[0])
        return catalog_records

    native.fetch = changed_roster
    with pytest.raises(RuntimeError, match="cms_native_input_candidate_scope_changed"):
        await registration.capture_current_native_input_candidates(native, ledger_schema="sample")
    assert len(native.calls) == 1
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
async def test_adoption_cannot_turn_an_unpublished_stage_into_a_retained_predecessor():
    native = NativeProtocol(target_root=("sample", "nucc_taxonomy"))
    with pytest.raises(RuntimeError, match="cms_native_input_candidate_scope_changed"):
        await registration.capture_native_input_adoption(native, ledger_schema="sample", candidate=native.candidate())
    assert not native.calls
    assert native.rows == native.guards == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("registrations", [None, [], {}])
async def test_provider_catalog_linkage_requires_a_complete_roster_before_catalog_reads(registrations):
    native = NativeProtocol()
    with pytest.raises(RuntimeError, match="cms_native_input_registration_changed"):
        await registration.verify_native_input_candidate_catalog(
            native, ledger_schema="sample", candidates=(native.candidate(),), registrations=registrations
        )
    assert not native.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_catalog", [True, False])
async def test_native_inventory_transport_cannot_replace_catalog_objects_with_a_json_sequence(valid_catalog):
    native = NativeProtocol()
    await _enroll(native)
    before = deepcopy((native.rows, native.guards))
    native.calls.clear()
    fetchval = native.fetchval

    async def encoded_inventory(statement, *arguments):
        catalog = await fetchval(statement, *arguments)
        if "jsonb_build_object" in statement:
            return json.dumps(catalog if valid_catalog else [])
        return catalog

    native.fetchval = encoded_inventory
    if valid_catalog:
        evidence = await registration.verify_native_input_candidates(
            native, ledger_schema="sample", candidates=(native.candidate(),)
        )
        assert evidence["count"] == 1
    else:
        with pytest.raises(RuntimeError, match="cms_native_input_candidate_inventory_changed"):
            await registration.verify_native_input_candidates(
                native, ledger_schema="sample", candidates=(native.candidate(),)
            )
    assert (native.rows, native.guards) == before
    assert not _writes(native)


@pytest.mark.asyncio
async def test_native_registration_rejects_structurally_similar_unissued_candidates():
    native = NativeProtocol()

    class ForeignCandidate(registration.NativeInputCandidate):
        """A structurally equal candidate from an unrecognized contract type."""

    candidate = ForeignCandidate(**vars(replace(native.candidate(), relation_oid=42)))
    with pytest.raises(RuntimeError, match="cms_native_input_candidate_invalid"):
        await registration.verify_native_input_candidates(native, ledger_schema="sample", candidates=(candidate,))
    assert not native.calls
