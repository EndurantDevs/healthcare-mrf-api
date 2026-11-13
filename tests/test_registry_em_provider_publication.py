# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider receipts survive validation and gate every serving-head writer."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import asyncpg
import pytest

from process import network_em_provider_inputs as provider
from process import network_membership_pipeline as pipeline
from process import network_membership_publication as publication
from process import network_membership_validation as validation
from process import network_serving_reactivation as reactivation
from process import network_serving_read as serving
from process import registry_candidate_composition as composition
from process import registry_fhir_serving_preservation as preservation
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation
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


def _predecessor_lookup(monkeypatch, candidate):
    """Keep the non-FHIR predecessor lookup explicit in publication tests."""
    predecessor = PinnedNetworkServingManifest(
        16,
        candidate["candidate_id"],
        candidate["schema_name"],
        1,
        {},
        2,
        "f" * 64,
        CompactAddressRelation(candidate["schema_name"], "entity_address_unified"),
    )
    resolver = AsyncMock(return_value=predecessor)
    monkeypatch.setattr(preservation, "resolve_network_serving_manifest", resolver)
    return resolver


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
    report, prepared_work = await validation._validate_sealed(conn, target, address, "registry")
    assert prepared_work is None
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


async def test_native_provider_data_refusal_is_not_accepted(monkeypatch):
    candidate, _, _ = _candidate()
    failure = ValueError("stale")
    native = AsyncMock(side_effect=failure)
    monkeypatch.setattr(provider, "prepare_em_provider_inputs", native)
    with pytest.raises(ValueError) as caught:
        await provider.verify_candidate_em_provider_inputs(object(), candidate)
    assert caught.value is failure
    native.assert_awaited_once()


@pytest.mark.parametrize(
    "failure",
    [asyncpg.CannotConnectNowError("temporary data outage"), asyncpg.QueryCanceledError("data query timeout")],
)
async def test_candidate_custody_preserves_actual_prepare_database_cause(failure):
    candidate, _, _ = _candidate()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetch=AsyncMock(side_effect=failure),
    )
    with pytest.raises(ValueError, match="^Managed provider input data is unavailable$") as caught:
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


def _entry(candidate):
    manifest_by_field = {
        field: candidate[field]
        for field in ("candidate_id", "schema_revision", "source_generations", "approved_custom_revision")
    }
    manifest_by_field.update(generation_id=17, manifest_sha256="f" * 64, eligible=True)
    return {"candidate_json": json.dumps(candidate), "manifest_json": json.dumps(manifest_by_field)}


def _complete_data(candidate):
    fields = (
        "raw_rows",
        "batch_rows",
        "batch_count",
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "unknown_network_rows",
    )
    candidate["validation_json"].update(dict.fromkeys(fields, 0))
    candidate["validation_json"]["final_validation"] = {
        "candidate_id": candidate["candidate_id"],
        "schema_name": candidate["schema_name"],
        "source_generations": candidate["source_generations"],
        "complete": True,
        "counts": {field: candidate["validation_json"][field] for field in fields},
    }


@pytest.mark.parametrize("state", ["ready", "published"])
@pytest.mark.parametrize("stale", [False, True])
async def test_actual_logical_readiness_precedes_insert_or_retained_replay(monkeypatch, state, stale):
    candidate, copy_target, _ = _candidate(state)
    _complete_data(candidate)
    connection, events = _connection(), []
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=(2, 16)))
    monkeypatch.setattr(publication, "_locked_candidate", AsyncMock(return_value=candidate))

    def readiness(actual):
        assert actual is candidate
        events.append("readiness")
        if stale:
            raise ValueError("stale_readiness")
        return {}, {}, {}, {}

    monkeypatch.setattr(publication, "_publication_readiness", readiness)
    predecessor = _predecessor_lookup(monkeypatch, candidate)
    insert = AsyncMock(side_effect=lambda *args: events.append("publish") or {"replayed": False})
    monkeypatch.setattr(publication, "_insert_manifest", insert)
    monkeypatch.setattr(
        publication, "_retained_manifest", lambda *args, **kwargs: events.append("replay") or {"replayed": True}
    )
    connection.fetchrow.return_value = {} if state == "published" else None
    if stale:
        with pytest.raises(ValueError, match="stale_readiness"):
            await publication._publish_candidate(connection, copy_target, '"registry"')
        assert events == ["readiness"]
        connection.fetchrow.assert_not_awaited()
        insert.assert_not_awaited()
    else:
        publication_by_field = await publication._publish_candidate(connection, copy_target, '"registry"')
        assert publication_by_field["replayed"] is (state == "published")
        assert events == ["readiness", "replay" if state == "published" else "publish"]
    assert predecessor.await_count == int(not stale and state == "ready")
    if not stale and state == "ready":
        predecessor.assert_awaited_once_with(connection, generation_id=16, control_schema="registry")
    connection.execute.assert_not_awaited()


async def test_phase_owned_publication_starts_repeatable_read_before_publish(monkeypatch):
    _, target, _ = _candidate()
    connection, options = _connection(), []

    @asynccontextmanager
    async def transaction(**supplied):
        options.append(supplied)
        yield

    connection.transaction = transaction

    async def publish(actual, selected, **arguments):
        assert actual is connection and selected is target
        assert options == [{"isolation": "repeatable_read"}]
        assert arguments == {"control_schema": "registry"}
        return {"generation_id": 17}

    monkeypatch.setattr(pipeline, "publish_network_candidate", publish)
    assert await pipeline._publish_phase(connection, target, "registry") == {"generation_id": 17}


@pytest.mark.parametrize("stale", [False, True])
async def test_pipeline_retained_replay_checks_logical_readiness_before_manifest(monkeypatch, stale):
    candidate, target, _ = _candidate("published")
    connection = _connection()
    monkeypatch.setattr(pipeline, "_inspect_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})
    monkeypatch.setattr(pipeline, "_validated_replay", lambda *args: None)

    def readiness(*args):
        if stale:
            raise ValueError("changed_readiness")
        return {}, {}, {}, {}

    monkeypatch.setattr(pipeline, "_publication_readiness", readiness)
    read = AsyncMock(return_value=_entry(candidate))
    monkeypatch.setattr(pipeline, "_read_manifest", read)
    monkeypatch.setattr(pipeline, "_retained_manifest", lambda *args, **kwargs: {"replayed": True})
    connection.fetchrow.return_value = {"generation_id": 17}
    if stale:
        with pytest.raises(ValueError, match="changed_readiness"):
            await pipeline._published_replay(connection, target, object(), "registry")
        read.assert_not_awaited()
    else:
        assert await pipeline._published_replay(connection, target, object(), "registry") == {"replayed": True}
        read.assert_awaited_once_with(connection, '"registry"', 17, include_ineligible=True)
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("head", [16, 17])
@pytest.mark.parametrize("stale", [False, True])
async def test_reactivation_checks_logical_readiness_before_cas_even_on_noop(monkeypatch, head, stale):
    candidate, _, _ = _candidate("published")
    connection = _connection()
    connection.fetchrow.return_value = candidate
    pinned = serving._pinned_manifest(_entry(candidate))
    monkeypatch.setattr(reactivation, "_lock_publication_controls", AsyncMock(return_value=(2, head)))
    monkeypatch.setattr(reactivation, "resolve_network_serving_manifest", AsyncMock(return_value=pinned))

    def readiness(*args):
        connection.execute.assert_not_awaited()
        if stale:
            raise ValueError("stale_readiness")

    monkeypatch.setattr(reactivation, "_publication_readiness", readiness)
    if stale:
        with pytest.raises(ValueError, match="stale_readiness"):
            await reactivation._reactivate(connection, '"registry"', 17, head, 2)
        connection.execute.assert_not_awaited()
    else:
        result = await reactivation._reactivate(connection, '"registry"', 17, head, 2)
        assert result.changed is (head != 17)
        assert connection.execute.await_count == int(head != 17)


async def test_trusted_refresh_requires_explicit_family_and_replaces_inherited_pin(monkeypatch):
    candidate, _, _ = _candidate()
    pinned = PinnedNetworkServingManifest(
        17,
        candidate["candidate_id"],
        candidate["schema_name"],
        1,
        candidate["source_generations"],
        2,
        "f" * 64,
        CompactAddressRelation(candidate["schema_name"], "entity_address_unified"),
    )
    old = family()
    fresh = provider.EMProviderInputs(
        tuple(replace(input_relation, generation_id="fresh-edition") for input_relation in old.relations)
    )
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
    storage_failure = asyncpg.CannotConnectNowError("temporary data outage")
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


@pytest.mark.parametrize("failure", [None, ValueError("seal refused"), asyncio.CancelledError()])
async def test_seal_callback_runs_after_committed_prepare_and_before_publish(monkeypatch, failure):
    candidate, copy_target, _ = _candidate()
    connection = SimpleNamespace(is_in_transaction=lambda: False)
    events = []

    async def prepare(*args, **kwargs):
        events.append("committed_prepare")
        return candidate

    async def seal(actual):
        assert actual is copy_target and not connection.is_in_transaction()
        events.append("seal")
        if failure is not None:
            raise failure

    async def publish(*args):
        events.append("publish")
        return {"generation_id": 17}

    monkeypatch.setattr(pipeline, "prepare_network_candidate", prepare)
    final_publish = AsyncMock(side_effect=publish)
    monkeypatch.setattr(pipeline, "_publish_phase", final_publish)
    arguments_by_name = dict(
        owner_role="protected_owner",
        loader_roles=(),
        reader_roles=("runtime_reader",),
        control_schema="registry",
        seal_candidate=seal,
    )
    if failure is None:
        assert await pipeline.prepare_and_publish_network_candidate(
            connection, copy_target, object(), **arguments_by_name
        ) == {"generation_id": 17}
        assert events == ["committed_prepare", "seal", "publish"]
    else:
        with pytest.raises(type(failure)):
            await pipeline.prepare_and_publish_network_candidate(connection, copy_target, object(), **arguments_by_name)
        assert events == ["committed_prepare", "seal"]
        final_publish.assert_not_awaited()


async def test_published_replay_never_reinvokes_seal_callback(monkeypatch):
    candidate, target, _ = _candidate("published")
    monkeypatch.setattr(pipeline, "prepare_network_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_published_replay", AsyncMock(return_value={"replayed": True}))
    seal = AsyncMock()
    assert await pipeline.prepare_and_publish_network_candidate(
        object(), target, object(), owner_role="protected_owner", loader_roles=(), reader_roles=(), seal_candidate=seal
    ) == {"replayed": True}
    seal.assert_not_awaited()


@pytest.mark.parametrize("phase", ["prepare", "publish"])
async def test_ready_phase_reuses_committed_preparation_without_new_writes(monkeypatch, phase):
    candidate, target, _ = _candidate()
    connection = _connection()
    monkeypatch.setattr(pipeline, "_locked_candidate", AsyncMock(return_value=candidate))
    monkeypatch.setattr(pipeline, "_scope_report", lambda *args: {})
    monkeypatch.setattr(pipeline, "_validated_replay", lambda *args: None)
    monkeypatch.setattr(pipeline, "_publication_readiness", lambda *args: ())
    mutators = [AsyncMock() for _ in range(3)]
    for name, operation in zip(
        ["validate_network_membership_candidate", "project_network_address_arrays", "prepare_network_serving_indexes"],
        mutators,
        strict=True,
    ):
        monkeypatch.setattr(pipeline, name, operation)
    publish = AsyncMock(return_value={"generation_id": 17})
    monkeypatch.setattr(pipeline, "publish_network_candidate", publish)
    if phase == "prepare":
        await pipeline._prepare_phase(connection, target, object(), {}, "registry")
        publish.assert_not_awaited()
    else:
        assert await pipeline._publish_phase(connection, target, "registry") == {"generation_id": 17}
    assert all(operation.await_count == 0 for operation in mutators)
    connection.execute.assert_not_awaited()


async def test_serving_pin_keeps_named_manifest_and_public_logical_fields():
    candidate, _, _ = _candidate("published")
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    entry = _entry(candidate)
    read = AsyncMock(return_value=entry)
    # The selected metadata query owns SQL eligibility and generation selection.
    from unittest.mock import patch

    with patch.object(serving, "_read_manifest", read):
        candidate["validation_json"] = None
        read.return_value = _entry(candidate)
        pinned = await serving.resolve_network_serving_manifest(connection, generation_id=17, control_schema="registry")
    assert pinned.generation_id == 17 and pinned.address_relation == CompactAddressRelation(
        candidate["schema_name"], "entity_address_unified"
    )
    assert set(asdict(pinned)) == {
        "generation_id",
        "candidate_id",
        "schema_name",
        "schema_revision",
        "source_generations",
        "approved_custom_revision",
        "manifest_sha256",
        "address_relation",
    }
    assert replace(pinned) == pinned
