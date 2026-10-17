# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider receipts survive validation and gate every serving-head writer."""

import copy
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import network_em_provider_inputs as provider
from process import network_membership_pipeline as pipeline
from process import network_membership_publication as publication
from process import network_membership_validation as validation
from process import network_serving_reactivation as reactivation
from process import registry_candidate_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_membership_copy import MembershipCopyTarget
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_network_em_provider_inputs import family, prepare

_BASE_RECEIPT = prepare()
pytestmark = pytest.mark.asyncio


def _candidate(state="ready"):
    receipt_by_field = copy.deepcopy(_BASE_RECEIPT)
    ids = [str(UUID(int=n)) for n in range(1, 5)]
    target = MembershipCopyTarget(*ids, "network_candidate_" + UUID(ids[-1]).hex)
    candidate_by_field = {
        name: getattr(target, name)
        for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    candidate_by_field.update(
        state=state,
        schema_revision=1,
        approved_custom_revision=2,
        expected_head=16,
        expected_rows=0,
        accepted_rows=0,
        source_recipes_json=[],
        source_generations={
            "unified_address": "address-edition",
            "em_provider_inputs": receipt_by_field["receipt_sha256"],
        },
        validation_json={"em_provider_inputs": receipt_by_field},
    )
    return candidate_by_field, target, receipt_by_field


def _closure():
    return {"owner_role": "protected_owner", "loader_roles": [], "reader_roles": ["runtime_reader"]}


def _connection():
    @asynccontextmanager
    async def transaction(**options):
        yield

    return SimpleNamespace(
        transaction=transaction,
        fetchrow=AsyncMock(return_value=None),
        fetchval=AsyncMock(),
        execute=AsyncMock(return_value="UPDATE 1"),
    )


async def test_scope_and_actual_validation_update_preserve_exact_receipt(monkeypatch):
    candidate, target, receipt = _candidate("sealed")
    address = PinnedAddressSource("retained_addresses", "entity_address_unified", "address-edition")
    scope = validation._scope_report(candidate, address)
    assert scope["em_provider_inputs"] == receipt
    conn = _connection()
    monkeypatch.setattr(validation, "_lock_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(
        validation,
        "_membership_accounting",
        AsyncMock(
            return_value={
                name: 0
                for name in ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")
            }
        ),
    )
    monkeypatch.setattr(
        validation,
        "_native_accounting",
        AsyncMock(return_value={name: 0 for name in ("raw_rows", "unknown_network_rows", "batch_rows", "batch_count")}),
    )
    report = await validation._validate_sealed(conn, target, address, "registry")
    assert report["em_provider_inputs"] == receipt
    stored = json.loads(conn.execute.await_args.args[2])
    assert stored["em_provider_inputs"] == receipt and stored["source_generations"] == candidate["source_generations"]
    candidate.update(state="validated", validation_json=stored)
    assert validation._validated_replay(candidate, validation._scope_report(candidate, address)) == stored


@pytest.mark.parametrize("damage", ["marker_missing", "receipt_missing", "null_receipt", "hash", "shape", "extra"])
async def test_unpaired_or_malformed_recipe_is_refused_before_validation(damage):
    candidate, _, _ = _candidate()
    if damage == "marker_missing":
        candidate["source_generations"].pop("em_provider_inputs")
    if damage == "receipt_missing":
        candidate["validation_json"].pop("em_provider_inputs")
    if damage == "null_receipt":
        candidate["validation_json"]["em_provider_inputs"] = None
    if damage == "hash":
        candidate["source_generations"]["em_provider_inputs"] = "0" * 64
    if damage == "shape":
        candidate["validation_json"]["em_provider_inputs"]["relations"].pop()
    if damage == "extra":
        candidate["validation_json"]["em_provider_inputs"]["extra"] = True
    with pytest.raises(validation.NetworkMembershipValidationError):
        validation._em_provider_receipt(candidate)


async def test_legacy_candidate_does_not_require_provider_receipt_or_native_read(monkeypatch):
    candidate, _, _ = _candidate()
    candidate["source_generations"].pop("em_provider_inputs")
    candidate["validation_json"] = {}
    prepare_native = AsyncMock()
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", prepare_native)
    assert await provider.verify_candidate_em_provider_inputs(object(), candidate, runtime_roles=()) is None
    prepare_native.assert_not_awaited()


@pytest.mark.parametrize("damage", ["native", "changed", "roles"])
async def test_native_custody_and_writer_runtime_roles_must_match(monkeypatch, damage):
    candidate, _, receipt = _candidate()
    current = copy.deepcopy(receipt)
    if damage == "changed":
        current["relations"][0]["owner_oid"] += 1
    native = AsyncMock(return_value=current, side_effect=ValueError("stale") if damage == "native" else None)
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", native)
    with pytest.raises(ValueError) as caught:
        await provider.verify_candidate_em_provider_inputs(
            object(), candidate, runtime_roles=("other_reader",) if damage == "roles" else ("runtime_reader",)
        )
    assert caught.value.__cause__ is None
    if damage == "roles":
        native.assert_not_awaited()


@pytest.mark.parametrize(
    "failure",
    [asyncpg.CannotConnectNowError("temporary custody outage"), asyncpg.QueryCanceledError("custody query timeout")],
)
async def test_candidate_custody_preserves_actual_prepare_database_cause(failure):
    candidate, _, _ = _candidate()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(side_effect=failure),
    )
    with pytest.raises(ValueError, match="^Managed provider input custody is unavailable$") as caught:
        await provider.verify_candidate_em_provider_inputs(connection, candidate, runtime_roles=("runtime_reader",))
    assert caught.value.__cause__ is failure
    connection.fetch.assert_awaited_once()


async def test_managed_native_guard_requires_existing_rr_before_catalog_reads():
    candidate, _, _ = _candidate()
    conn = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="read committed"), fetch=AsyncMock()
    )
    with pytest.raises(ValueError):
        await provider.verify_candidate_em_provider_inputs(conn, candidate, runtime_roles=("runtime_reader",))
    conn.fetch.assert_not_awaited()


@pytest.mark.parametrize("state", ["ready", "published"])
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_publication_gate_precedes_insert_or_retained_replay(monkeypatch, state, stale):
    candidate, copy_target, receipt = _candidate(state)
    conn = _connection()
    events = []
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    monkeypatch.setattr(publication, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(publication, "_publication_readiness", lambda _candidate: ({}, {}, {}, _closure()))
    monkeypatch.setattr(publication, "verify_network_candidate_writer_closure", AsyncMock())

    async def native(*args, **kwargs):
        events.append("custody")
        if stale:
            raise ValueError("stale")
        return receipt

    monkeypatch.setattr(provider, "prepare_em_provider_inputs", native)

    async def insert(*args):
        events.append("publish")
        return {"replayed": False}

    monkeypatch.setattr(publication, "_insert_manifest", insert)
    monkeypatch.setattr(
        publication, "_retained_manifest", lambda *args, **kwargs: events.append("replay") or {"replayed": True}
    )
    conn.fetchrow.return_value = {} if state == "published" else None
    if stale:
        with pytest.raises(ValueError):
            await publication._publish_candidate(conn, copy_target, '"registry"')
        assert events == ["custody"]
        conn.fetchrow.assert_not_awaited()
    else:
        assert (await publication._publish_candidate(conn, copy_target, '"registry"'))["replayed"] == (
            state == "published"
        )
        assert events == ["custody", "replay" if state == "published" else "publish"]
    conn.execute.assert_not_awaited()


async def test_phase_owned_publication_starts_rr_before_control_locks(monkeypatch):
    candidate, target, _ = _candidate()
    options_by_name = {}
    conn = _connection()

    @asynccontextmanager
    async def transaction(**options):
        options_by_name.update(options)
        yield

    conn.transaction = transaction

    async def controls(*args):
        assert options_by_name == {"isolation": "repeatable_read"}
        return 2, 16

    monkeypatch.setattr(pipeline, "_lock_publication_controls", controls)
    monkeypatch.setattr(pipeline, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "freeze_network_candidate_writers", AsyncMock(return_value=_closure()))
    monkeypatch.setattr(pipeline, "_retain_closure", AsyncMock())
    publish = AsyncMock(return_value={"generation_id": 17})
    monkeypatch.setattr(pipeline, "publish_network_candidate", publish)
    assert await pipeline._publish_phase(conn, target, {}, "registry") == {"generation_id": 17}
    publish.assert_awaited_once()


async def test_pipeline_retained_replay_checks_custody_before_serving_reads(monkeypatch):
    candidate, target, _ = _candidate("published")
    conn = _connection()
    monkeypatch.setattr(pipeline, "_inspect_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})
    monkeypatch.setattr(pipeline, "_validated_replay", lambda *args: None)
    monkeypatch.setattr(pipeline, "_publication_readiness", lambda *args: ({}, {}, {}, _closure()))
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", AsyncMock(side_effect=ValueError("stale")))
    with pytest.raises(ValueError):
        await pipeline._published_replay(conn, target, object(), {}, "registry")
    conn.fetchrow.assert_not_awaited()
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("head", [16, 17])
@pytest.mark.parametrize("stale", [False, True])
async def test_reactivation_checks_custody_before_cas_even_on_noop(monkeypatch, head, stale):
    candidate, _, receipt = _candidate("published")
    conn = _connection()
    conn.fetchrow.return_value = candidate
    pinned = PinnedNetworkServingManifest(
        17, candidate["candidate_id"], candidate["schema_name"], 1, candidate["source_generations"], 2, "f" * 64, 100
    )
    monkeypatch.setattr(reactivation, "_lock_publication_controls", AsyncMock(return_value=(2, head)))
    monkeypatch.setattr(reactivation, "resolve_network_serving_manifest", AsyncMock(return_value=pinned))
    monkeypatch.setattr(reactivation, "_publication_readiness", lambda *args: ({}, {}, {}, _closure()))
    native = AsyncMock(return_value=receipt, side_effect=ValueError("stale") if stale else None)
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", native)
    if stale:
        with pytest.raises(ValueError):
            await reactivation._reactivate(conn, '"registry"', 17, head, 2)
        conn.execute.assert_not_awaited()
    else:
        result = await reactivation._reactivate(conn, '"registry"', 17, head, 2)
        assert result.changed == (head != 17)
        assert conn.execute.await_count == int(head != 17)
    native.assert_awaited_once()


async def test_trusted_refresh_requires_explicit_family_and_replaces_inherited_pin(monkeypatch):
    candidate, _, _ = _candidate()
    pinned = PinnedNetworkServingManifest(
        17, candidate["candidate_id"], candidate["schema_name"], 1, candidate["source_generations"], 2, "f" * 64, 100
    )
    old = family()
    fresh = provider.EMProviderInputs(tuple(replace(item, generation_id="fresh-edition") for item in old.relations))
    address = composition.RegistryCompositionAddressSources(
        PinnedAddressSource("retained", "entity_address_unified", "edition"), em_provider_inputs=old
    )
    compose = AsyncMock(return_value=("target", "address"))
    monkeypatch.setattr(composition, "compose_registry_membership_candidate", compose)
    request = UUID(int=9)
    assert await composition.compose_registry_provider_refresh(
        object(), request, 2, pinned, address, fresh, {}, "registry"
    ) == ("target", "address")
    kwargs = compose.await_args.kwargs
    assert kwargs["expected_head"] == 17 and kwargs["source_manifest"] is pinned
    assert kwargs["address_sources"].em_provider_inputs == fresh
    assert address.em_provider_inputs == old
    compose.reset_mock()
    with pytest.raises(composition.RegistryCompositionError):
        await composition.compose_registry_provider_refresh(object(), request, 2, pinned, address, None, {}, "registry")
    compose.assert_not_awaited()


@pytest.mark.parametrize("is_database_failure", [False, True])
async def test_reactivation_wrapper_preserves_explicit_custody_classification(monkeypatch, is_database_failure):
    from process.registry_publication_consumer import _has_database_cause

    candidate, _, _ = _candidate("published")
    storage_failure = asyncpg.CannotConnectNowError("temporary custody outage")
    native = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read" if is_database_failure else "read committed"),
        fetch=AsyncMock(side_effect=storage_failure),
    )

    async def actual_verifier(*arguments):
        return await provider.verify_candidate_em_provider_inputs(native, candidate, runtime_roles=("runtime_reader",))

    monkeypatch.setattr(reactivation, "_reactivate", actual_verifier)
    connection = _connection()
    connection.is_in_transaction = lambda: True
    with pytest.raises(
        reactivation.NetworkServingReactivationError, match="network_reactivation_controls_unavailable"
    ) as caught:
        await reactivation.reactivate_network_serving_generation(
            connection, generation_id=17, expected_head=16, expected_approved_revision=2, control_schema="registry"
        )
    assert isinstance(caught.value.__cause__, ValueError)
    assert _has_database_cause(caught.value) is is_database_failure
    if is_database_failure:
        assert caught.value.__cause__.__cause__ is storage_failure
    else:
        native.fetch.assert_not_awaited()
        assert caught.value.__cause__.__cause__ is None
