# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent Source replay routing; fake contexts establish no native authority."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_grant as grants
from process import registry_fhir_catalog_validation as validation
from process.registry_fhir_catalog_observation import catalog_observation_descriptor_digest
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from tests.test_registry_fhir_catalog_grant import CatalogConnection, command, snapshot, standing_grant
from tests.test_registry_fhir_catalog_observation import _receipt
from tests.test_registry_fhir_catalog_source_producer import _boundary
from tests.test_registry_fhir_catalog_sources import descriptor, read_profile, scoped_read_profile


def observed_descriptor(profile=None, dataset="dataset-one"):
    profile = read_profile() if profile is None else profile
    selected = descriptor()
    selected["coordinates"].update(dataset_schema=profile.schema_name, dataset_id=dataset, edition_id=dataset)
    selected.update(native_read_profile=profile.document(), admission=_receipt())
    digest = catalog_observation_descriptor_digest(selected)
    selected.update(
        accepted_receipt_id=grants.CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest
    )
    return validate_fhir_catalog_source_descriptor(selected)


def scoped_observed_descriptor():
    selected = observed_descriptor()
    profile = scoped_read_profile(projection="catalog_observation_v1")
    scope = profile.published_read_scope
    publication_sha = hashlib.sha256(
        validation.canonical_json(selected["admission"]["publication"]).encode()
    ).hexdigest()
    scope = replace(scope, datasets=(replace(scope.datasets[0], publication_sha256=publication_sha),))
    profile = replace(profile, published_read_scope=scope)
    selected.update(
        native_read_profile=profile.document(),
        resource_table_oid=scope.resource_root.relation_oid,
        resource_leaf_oids=[leaf.relation_oid for leaf in scope.datasets[0].resource_leaves],
        header_table_oid=scope.header.relation.relation_oid,
    )
    digest = catalog_observation_descriptor_digest(selected)
    selected.update(
        accepted_receipt_id=grants.CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest
    )
    return validate_fhir_catalog_source_descriptor(selected), profile


def compact_rows(selected):
    return (
        (
            fhir_catalog_source_generation(selected),
            "Organization",
            "network-one",
            "organization",
            0,
            '{"name":"Example"}',
        ),
    )


def compact_digest(rows):
    encoded = b""
    for generation, kind, identity, observation, ordinal, document in rows:
        encoded += (
            validation.canonical_json(
                dict(
                    source_generation=generation,
                    resource_type=kind,
                    resource_id=identity,
                    observation_kind=observation,
                    ordinal=ordinal,
                    document_json=json.loads(document),
                )
            ).encode()
            + b"\n"
        )
    return {"rows": len(rows), "sha256": hashlib.sha256(encoded).hexdigest()}


def verifier_case(monkeypatch, descriptors=None, profiles=None):
    descriptors = (observed_descriptor(),) if descriptors is None else descriptors
    profiles = (read_profile(),) if profiles is None else profiles
    grant = standing_grant(source_profiles=profiles, observation_contract=grants.CATALOG_OBSERVATION_CONTRACT)
    selected = snapshot(grant, descriptors)
    by_generation = {fhir_catalog_source_generation(descriptor): descriptor for descriptor in descriptors}
    factory, export, connection, events = _boundary()

    async def replay(value, collect):
        events.append(("replay", value["coordinates"]["dataset_id"]))
        await collect(compact_rows(value))
        return 1

    async def accounting(_connection, _schema, generation):
        value = by_generation[generation]
        return {
            "source_rows": 1,
            "observation_rows": 1,
            "accepted_at": datetime.fromisoformat(value["admission"]["publication"]["published_at"]),
        }

    async def digest(_connection, *, schema_name, source_generation):
        assert schema_name == selected["schema_name"]
        return compact_digest(compact_rows(by_generation[source_generation]))

    async def retained(*_args):
        events.append(("retained",))

    async def projection(*_args, **_kwargs):
        events.append(("projection",))

    export["replay"].side_effect = replay
    monkeypatch.setattr(grants, "_descriptors", AsyncMock(return_value=by_generation))
    monkeypatch.setattr(grants, "_prior_observations", AsyncMock(return_value=(None, {})))
    monkeypatch.setattr(grants, "_observation_accounting", AsyncMock(side_effect=accounting))
    monkeypatch.setattr(grants, "fhir_catalog_observation_digest", AsyncMock(side_effect=digest))
    monkeypatch.setattr(grants, "_verify_retained_product_observations", AsyncMock(side_effect=retained))
    monkeypatch.setattr(validation, "validate_fhir_catalog_observed_projection", AsyncMock(side_effect=projection))
    pool = SimpleNamespace(acquire=Mock(side_effect=AssertionError("Reader fallback used")))
    copy_target = SimpleNamespace(schema_name=selected["schema_name"])
    return SimpleNamespace(
        grant=grant,
        snapshot=selected,
        descriptors=by_generation,
        factory=factory,
        export=export,
        connection=connection,
        events=events,
        pool=pool,
        target=copy_target,
    )


async def verify(case, **options):
    callback = grants.build_fhir_catalog_observation_verifier(
        case.grant, case.pool, source_export=case.factory, **options
    )
    await callback(
        connection=case.connection, copy_target=case.target, snapshot=case.snapshot, standing_grant=case.grant
    )


@pytest.mark.asyncio
async def test_sealer_replay_groups_exact_profiles_and_exits_before_success(monkeypatch):
    first = read_profile()
    second = replace(first, schema_name="second_payload", owner_role="second_owner", runtime_roles=("second_reader",))
    descriptors = (
        observed_descriptor(first),
        observed_descriptor(second, "dataset-two"),
        observed_descriptor(first, "dataset-three"),
    )
    case = verifier_case(monkeypatch, descriptors, (first, second))
    registry_reader, workspace = object(), object()
    await verify(case, registry_reader=registry_reader, registry_workspace=workspace)
    assert {call.kwargs["source_profile"] for call in case.factory.call_args_list} == {first, second}
    assert case.factory.call_count == 2
    assert len([event for event in case.events if event[0] == "exit"]) == 2
    assert len([event for event in case.events if event[0] == "replay"]) == 3
    assert case.events[-3:] == [("exit",), ("retained",), ("projection",)]
    case.export["inventory"].assert_not_called()
    case.export["legacy_bindings"].assert_not_called()
    case.pool.acquire.assert_not_called()
    grants._verify_retained_product_observations.assert_awaited_once_with(
        case.connection, case.snapshot, case.grant, registry_reader, workspace, case.descriptors
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["bounded_document", "source_identity", "vector_identity", "missing"])
async def test_protected_descriptor_inventory_refuses_bounds_missing_rows_or_identity_drift(monkeypatch, failure):
    source_descriptor = observed_descriptor()
    generation = fhir_catalog_source_generation(source_descriptor)
    grant = standing_grant(observation_contract=grants.CATALOG_OBSERVATION_CONTRACT)
    source_entries = [
        {
            "source_generation": generation,
            "source_id": "other" if failure == "vector_identity" else "alias-a",
            "status": "current",
        }
    ]
    selected = snapshot(grant, source_vector_json=source_entries)
    entry_by_field = {
        "source_generation": generation,
        "source_id": "other" if failure == "source_identity" else "alias-a",
        "document": None if failure == "bounded_document" else validation.canonical_json(source_descriptor),
    }
    writer = CatalogConnection((selected,))
    writer.fetch = AsyncMock(side_effect=[[] if failure == "missing" else [entry_by_field], []])
    monkeypatch.setattr(grants, "verify_fhir_catalog_snapshot", AsyncMock())
    runtime = grants.build_fhir_catalog_publication_runtime(grant, None)
    reason = {"bounded_document": "descriptor_bounds", "missing": "descriptor_unavailable"}.get(
        failure, "descriptor_changed"
    )
    with pytest.raises(grants.FHIRCatalogGrantError, match=reason):
        await runtime.authorize_metadata(command=command(grant, selected), snapshot=selected, connection=writer)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["missing", "writer"])
async def test_semantic_source_fence_never_borrows_the_catalog_writer_as_its_reader(monkeypatch, route):
    selected_source = descriptor()
    grant = standing_grant()
    selected = snapshot(grant, (selected_source,))
    writer = CatalogConnection((selected,), (selected_source,))

    @asynccontextmanager
    async def acquire_writer():
        yield writer

    monkeypatch.setattr(grants, "verify_fhir_catalog_snapshot", AsyncMock())
    pool = None if route == "missing" else SimpleNamespace(acquire=acquire_writer)
    runtime = grants.build_fhir_catalog_publication_runtime(grant, pool)
    with pytest.raises(grants.FHIRCatalogGrantError, match="reader_required"):
        async with runtime.source_fence(snapshot=selected, connection=writer):
            pytest.fail("writer was accepted as a Source Reader")
    assert not writer.active


@pytest.mark.asyncio
async def test_observation_history_cannot_disappear_before_any_source_export(monkeypatch):
    case = verifier_case(monkeypatch)
    omitted = observed_descriptor(dataset="previous-dataset")
    monkeypatch.setattr(
        grants,
        "_prior_observations",
        AsyncMock(return_value=(case.snapshot, {fhir_catalog_source_generation(omitted): omitted})),
    )
    with pytest.raises(grants.FHIRCatalogGrantError, match="observation_history_changed"):
        await verify(case)
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_carried_observation_accounting_drift_refuses_without_source_reacquisition(monkeypatch):
    case = verifier_case(monkeypatch)
    monkeypatch.setattr(grants, "_prior_observations", AsyncMock(return_value=(case.snapshot, case.descriptors)))
    monkeypatch.setattr(
        grants, "_observation_accounting", AsyncMock(side_effect=[{"source_rows": 1}, {"source_rows": 2}])
    )
    with pytest.raises(grants.FHIRCatalogGrantError, match="observation_history_changed"):
        await verify(case)
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["source_rows", "observation_rows", "accepted_at", "digest_rows", "digest_sha"])
async def test_source_replay_requires_exact_sealed_accounting_and_digest(monkeypatch, field):
    case = verifier_case(monkeypatch)
    if field.startswith("digest_"):
        stored = compact_digest(compact_rows(next(iter(case.descriptors.values()))))
        stored["rows" if field == "digest_rows" else "sha256"] = 2 if field == "digest_rows" else "f" * 64
        grants.fhir_catalog_observation_digest.side_effect = None
        grants.fhir_catalog_observation_digest.return_value = stored
    else:
        accounting = await grants._observation_accounting(case.connection, "ignored", next(iter(case.descriptors)))
        accounting[field] = 2 if field != "accepted_at" else datetime(2025, 1, 1)
        grants._observation_accounting.side_effect = None
        grants._observation_accounting.return_value = accounting
    with pytest.raises(grants.FHIRCatalogGrantError, match="observation_facts_changed"):
        await verify(case)
    assert case.events[-1] == ("exit",)
    grants._verify_retained_product_observations.assert_not_awaited()
    validation.validate_fhir_catalog_observed_projection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 2, True, None])
async def test_source_replay_cannot_return_partial_or_untyped_row_counts(monkeypatch, count):
    case = verifier_case(monkeypatch)
    original = case.export["replay"].side_effect

    async def replay(value, collect):
        await original(value, collect)
        return count

    case.export["replay"].side_effect = replay
    with pytest.raises(grants.FHIRCatalogGrantError, match="observation_facts_changed"):
        await verify(case)
    assert case.events[-1] == ("exit",)


@pytest.mark.asyncio
async def test_database_oid_mismatch_refuses_before_replay(monkeypatch):
    value = observed_descriptor()
    value["database_oid"] = 105
    digest = catalog_observation_descriptor_digest(value)
    value.update(accepted_receipt_id=grants.CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest)
    case = verifier_case(monkeypatch, (value,))
    with pytest.raises(grants.FHIRCatalogGrantError, match="database_changed"):
        await verify(case)
    case.export["replay"].assert_not_awaited()
    assert case.events[-1] == ("exit",)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["profile", "contract"])
async def test_unadmitted_profile_or_contract_refuses_before_source_entry(monkeypatch, damage):
    case = verifier_case(monkeypatch)
    if damage == "profile":
        case.grant = replace(case.grant, source_profiles=(replace(read_profile(), owner_role="other_owner"),))
    else:
        case.grant = replace(case.grant, observation_contract=None)
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_not_admitted|reader_required"):
        await verify(case)
    case.factory.assert_not_called()


@pytest.mark.parametrize(
    "builder", [grants.build_fhir_catalog_publication_runtime, grants.build_fhir_catalog_observation_verifier]
)
def test_export_hook_is_validated_only_at_trusted_runtime_construction(builder):
    with pytest.raises(grants.FHIRCatalogGrantError, match="source_export_invalid"):
        builder(standing_grant(), None, source_export={})


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("source descriptor changed"), OSError("source unavailable")])
async def test_source_refusal_or_replay_error_exits_without_reader_fallback(monkeypatch, failure):
    case = verifier_case(monkeypatch)
    case.export["replay"].side_effect = failure
    with pytest.raises(type(failure)) as raised:
        await verify(case)
    assert raised.value is failure
    assert case.events[-2:] == [("error", failure), ("exit",)]
    case.pool.acquire.assert_not_called()
    validation.validate_fhir_catalog_observed_projection.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_exit_drift_invalidates_successful_replay(monkeypatch):
    case = verifier_case(monkeypatch)
    failure = ValueError("source exit changed")

    @asynccontextmanager
    async def export(*, source_profile):
        yield case.export
        case.events.append(("exit",))
        raise failure

    case.factory = export
    with pytest.raises(ValueError) as raised:
        await verify(case)
    assert raised.value is failure and case.events[-1] == ("exit",)
    grants._verify_retained_product_observations.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancellation_closes_source_before_verifier_finishes(monkeypatch):
    case = verifier_case(monkeypatch)
    started = asyncio.Event()

    async def replay(_descriptor, _collect):
        started.set()
        await asyncio.Event().wait()

    case.export["replay"].side_effect = replay
    task = asyncio.create_task(verify(case))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert case.events[-1] == ("exit",)
    assert isinstance(case.events[-2][1], asyncio.CancelledError)
    validation.validate_fhir_catalog_observed_projection.assert_not_awaited()


@pytest.mark.asyncio
async def test_replay_keeps_strict_primary_key_order_across_batches(monkeypatch):
    case = verifier_case(monkeypatch)

    async def replay(value, collect):
        rows = compact_rows(value)
        await collect(rows)
        await collect(rows)
        return 1

    case.export["replay"].side_effect = replay
    with pytest.raises(grants.FHIRCatalogGrantError, match="observation_order_changed"):
        await verify(case)
    assert case.events[-1] == ("exit",)


@pytest.mark.asyncio
async def test_carried_observations_keep_prior_verification_without_source_export(monkeypatch):
    case = verifier_case(monkeypatch)
    prior_snapshot_by_field = {"schema_name": "prior_catalog"}
    grants._prior_observations.return_value = (prior_snapshot_by_field, case.descriptors)
    carried = AsyncMock()
    monkeypatch.setattr(grants, "_verify_carried_observation", carried)
    await verify(case)
    case.factory.assert_not_called()
    carried.assert_awaited_once_with(
        case.connection, case.snapshot, prior_snapshot_by_field, next(iter(case.descriptors))
    )


@pytest.mark.asyncio
async def test_publication_fence_for_observations_stays_reader_free(monkeypatch):
    case = verifier_case(monkeypatch)
    writer = CatalogConnection((case.snapshot,), tuple(case.descriptors.values()))
    monkeypatch.setattr(grants, "verify_fhir_catalog_snapshot", AsyncMock())
    runtime = grants.build_fhir_catalog_publication_runtime(case.grant, case.pool, source_export=case.factory)
    async with runtime.source_fence(snapshot=case.snapshot, connection=writer):
        await runtime.authorize_metadata(
            command=command(case.grant, case.snapshot), snapshot=case.snapshot, connection=writer
        )
        assert await runtime.verify_sources(snapshot=case.snapshot, connection=writer) is True
    case.factory.assert_not_called()
    case.pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_default_verifier_keeps_original_reader_replay(monkeypatch):
    case = verifier_case(monkeypatch)
    reader, workspace = object(), object()

    @asynccontextmanager
    async def acquire():
        yield reader

    @asynccontextmanager
    async def reference(_reader):
        assert _reader is reader
        yield workspace

    from process import registry_fhir_catalog_reference_rollup as rollup

    monkeypatch.setattr(rollup, "fhir_catalog_reference_workspace", reference)
    monkeypatch.setattr(grants, "_readonly_snapshot", reference)
    identity, fresh = AsyncMock(), AsyncMock()
    monkeypatch.setattr(grants, "_reader_identity", identity)
    monkeypatch.setattr(grants, "_verify_fresh_observation", fresh)
    callback = grants.build_fhir_catalog_observation_verifier(case.grant, SimpleNamespace(acquire=acquire))
    await callback(
        connection=case.connection, copy_target=case.target, snapshot=case.snapshot, standing_grant=case.grant
    )
    descriptor = next(iter(case.descriptors.values()))
    identity.assert_awaited_once_with(reader, case.connection, (descriptor,))
    fresh.assert_awaited_once_with(
        reader, case.connection, case.snapshot, next(iter(case.descriptors)), descriptor, workspace
    )
    case.factory.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("native_pool_available", [False, True])
async def test_mixed_profiles_keep_scoped_replay_on_required_native_reader(monkeypatch, native_pool_available):
    scoped, profile = scoped_observed_descriptor()
    ordinary = observed_descriptor(dataset="dataset-ordinary")
    case = verifier_case(monkeypatch, (ordinary, scoped), (read_profile(), profile))
    reader, workspace = object(), object()

    @asynccontextmanager
    async def acquire():
        yield reader

    @asynccontextmanager
    async def reference(selected_reader):
        assert selected_reader is reader
        yield workspace

    from process import registry_fhir_catalog_reference_rollup as rollup

    monkeypatch.setattr(rollup, "fhir_catalog_reference_workspace", reference)
    monkeypatch.setattr(grants, "_readonly_snapshot", reference)
    identity, fresh = AsyncMock(), AsyncMock()
    monkeypatch.setattr(grants, "_reader_identity", identity)
    monkeypatch.setattr(grants, "_verify_fresh_observation", fresh)
    case.pool = SimpleNamespace(acquire=acquire) if native_pool_available else None
    if not native_pool_available:
        with pytest.raises(grants.FHIRCatalogGrantError, match="reader_required"):
            await verify(case)
        case.factory.assert_not_called()
        identity.assert_not_awaited()
        validation.validate_fhir_catalog_observed_projection.assert_not_awaited()
        return
    await verify(case)
    case.factory.assert_called_once_with(source_profile=read_profile())
    assert case.export["replay"].await_count == 1
    assert case.export["replay"].await_args.args[0] == ordinary
    identity.assert_awaited_once_with(reader, case.connection, (scoped,))
    fresh.assert_awaited_once_with(
        reader, case.connection, case.snapshot, fhir_catalog_source_generation(scoped), scoped, workspace
    )
    assert case.events[-3:] == [("exit",), ("retained",), ("projection",)]
