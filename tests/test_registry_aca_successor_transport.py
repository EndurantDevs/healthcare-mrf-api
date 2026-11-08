# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed two-archive ACA transport without changing approved origin coordinates."""

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_initial_source_office_bindings as offices
from process import network_legacy_membership_source as aca
from process import registry_aca_source_consumer as consumer
from process import registry_aca_source_recipe as producer
from process import registry_imported_selection_preview as preview
from process import registry_source_recipe_store as codec
from process.network_approved_membership_source import ApprovedMembershipSource
from tests import test_registry_aca_source_recipe as fixture
from tests.test_registry_aca_source_consumer import config, transaction

_METADATA = fixture._metadata
_COMPLETED = fixture._completed


async def source_recipe(monkeypatch, ordinal):
    monkeypatch.setattr(fixture, "_DATASET", UUID(int=ordinal))
    metadata = _METADATA()
    metadata["network_membership"].update(import_id=f"import-{ordinal}", release_id=f"release-{ordinal}")
    completed = _COMPLETED()
    completed["import_id"] = f"import-{ordinal}"
    monkeypatch.setattr(fixture, "_metadata", lambda: metadata)
    monkeypatch.setattr(fixture, "_completed", lambda: completed)
    prepared, validation = await fixture._prepared(monkeypatch)
    return producer._recipe(prepared, validation, fixture._ROLES)


async def recipe_pair(monkeypatch):
    old = await source_recipe(monkeypatch, 1)
    fresh = await source_recipe(monkeypatch, 2)
    combined = codec.RegistrySourceMembershipRecipe(
        aca.with_aca_successor(old.source_pin, fresh.source_pin), old.binding_coordinates
    )
    return old, fresh, combined


@pytest.mark.asyncio
async def test_absent_successor_preserves_legacy_wire_and_generation(monkeypatch):
    old = await source_recipe(monkeypatch, 1)
    pin = old.source_pin
    encoded = codec.canonical_registry_source_recipes((old,))
    document = json.loads(encoded)[0]
    assert "retained_successor" not in document["source_pin"]
    original_generation_by_name = {
        "coordinates": pin.coordinates,
        "manifest": pin.prepared.manifest.as_dict(),
        "validation": pin.validation.as_dict(),
        "dataset_id": str(pin.prepared.ownership.dataset_id),
        "auxiliary_oid": pin.prepared.ownership.auxiliary_oid,
        "sequence_oids": pin.prepared.ownership.sequence_oids,
    }
    assert pin.generation_id == hashlib.sha256(aca._canonical_json(original_generation_by_name)).hexdigest()
    assert codec.decode_registry_source_recipes(encoded) == (old,)
    assert codec.canonical_registry_source_recipes(codec.decode_registry_source_recipes(encoded)) == encoded


@pytest.mark.asyncio
async def test_repeated_successors_keep_original_anchor_and_flat_wire(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    newer = await source_recipe(monkeypatch, 3)
    final = replace(combined, source_pin=aca.with_aca_successor(combined.source_pin, newer.source_pin))
    assert final.source_pin.prepared == old.source_pin.prepared
    assert final.binding_coordinates == old.binding_coordinates
    assert aca.aca_physical_source(final.source_pin) == newer.source_pin
    assert final.source_pin.retained_successor.retained_successor is None
    assert final.source_pin.generation_id != combined.source_pin.generation_id != old.source_pin.generation_id
    assert aca.with_aca_successor(final.source_pin, newer.source_pin) is final.source_pin
    encoded = codec.canonical_registry_source_recipes((final,))
    assert codec.decode_registry_source_recipes(encoded) == (final,)
    assert encoded.count('"retained_successor"') == 1
    with pytest.raises(aca.LegacyMembershipSourceError, match="flat"):
        aca.with_aca_successor(old.source_pin, combined.source_pin)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["null", "nested", "unknown", "producer", "issuer", "year"])
async def test_closed_successor_refuses_changed_authority_and_nested_wire(monkeypatch, damage):
    old, fresh, combined = await recipe_pair(monkeypatch)
    document = json.loads(codec.canonical_registry_source_recipes((combined,)))
    physical = document[0]["source_pin"]["retained_successor"]
    if damage == "null":
        document[0]["source_pin"]["retained_successor"] = None
    elif damage == "nested":
        physical["retained_successor"] = physical.copy()
    elif damage == "unknown":
        physical["untrusted"] = True
    else:
        if damage == "producer":
            physical["prepared"]["manifest"]["source_metadata"]["network_bindings"]["producer_id"] = "other"
        else:
            physical[damage + "_id" if damage == "issuer" else damage] += 1
    with pytest.raises(codec.RegistrySourceRecipeError):
        codec.decode_registry_source_recipes(document)


@pytest.mark.asyncio
async def test_native_proof_checks_origin_and_genuine_successor_stamp(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    origin = AsyncMock()
    stamp = AsyncMock(return_value=fresh)
    monkeypatch.setattr(aca, "_require_flat_archive", origin)
    monkeypatch.setattr(producer, "require_registry_aca_source_recipe", stamp)
    await aca._require_archive(object(), combined.source_pin)
    origin.assert_awaited_once_with(origin.await_args.args[0], old.source_pin)
    assert stamp.await_args.args[1] == fresh.source_pin.prepared
    stamp.side_effect = ValueError("protected successor changed")
    with pytest.raises(ValueError):
        await aca._require_archive(object(), combined.source_pin)


@pytest.mark.asyncio
async def test_reviewed_transport_uses_new_facts_and_proven_hios_anchor(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    approved = ApprovedMembershipSource(1, "a" * 64)
    gate = AsyncMock()
    monkeypatch.setattr(aca, "require_approved_network_source_bindings", gate)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"successor_invalid": False}))
    await aca._read_reviewed(
        connection,
        combined.source_pin,
        approved,
        old.binding_coordinates,
        (None, 10, None, True),
        "registry_control",
        True,
    )
    sql, *parameters = connection.fetchrow.await_args.args
    assert '"' + fresh.source_pin.prepared.ownership.schema_name + '".mrf_address_evidence' in sql
    assert '"' + old.source_pin.prepared.ownership.schema_name + '".plan ' in sql
    assert "count(DISTINCT network_id)>1" in sql and "transported_network_bindings" in sql
    assert "tier.checksum_network" in sql and "'checksum_network',page.checksum_network" not in sql
    assert tuple(parameters[1:7]) == old.binding_coordinates.sql_parameters
    gate.assert_awaited_once_with(connection, approved, old.binding_coordinates, control_schema="registry_control")
    connection.fetchrow.return_value = {"successor_invalid": True}
    with pytest.raises(aca.LegacyMembershipSourceError, match="ambiguous"):
        await aca._read_reviewed(
            connection,
            combined.source_pin,
            approved,
            old.binding_coordinates,
            (None, 10, None, True),
            "registry_control",
            True,
        )


@pytest.mark.asyncio
async def test_preview_reads_new_physical_archive_under_prospective_original_map(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    sql = preview._impact_sql(combined, '"registry_control"', '"selected"', "registry_control")
    assert '"' + fresh.source_pin.prepared.ownership.schema_name + '".mrf_address_evidence' in sql
    assert '"' + old.source_pin.prepared.ownership.schema_name + '".plan ' in sql
    assert "successor_invalid" in sql and "transported_network_bindings" in sql
    assert "FROM prospective binding" in sql and "LIMIT 5001" not in sql
    proof = AsyncMock()
    monkeypatch.setattr(preview, "require_aca_archive", proof)
    parameters = await preview._require_recipe_source(object(), combined)
    assert parameters == (
        fresh.source_pin.import_id,
        fresh.source_pin.issuer_id,
        fresh.source_pin.year,
        fresh.source_pin.source_url,
    )
    assert proof.await_args.args[1] == combined.source_pin


@pytest.mark.asyncio
async def test_consumer_reuses_current_original_anchor_across_new_export_uuid(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    approved = ApprovedMembershipSource(1, "a" * 64)
    pin = SimpleNamespace(generation_id=4)
    connection = SimpleNamespace(transaction=transaction, fetchval=AsyncMock(side_effect=[1, 4, True]))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    retained = AsyncMock(return_value=(old,))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", retained)
    native = AsyncMock()
    monkeypatch.setattr(consumer, "_require_archive", native)
    bindings = AsyncMock()
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", bindings)
    selected = await consumer._selection(connection, fresh, config())
    assert selected[3] == (combined,) and selected[2] is pin
    assert connection.fetchval.await_args.args[2:] == old.binding_coordinates.sql_parameters
    assert bindings.await_args.args[2] == old.binding_coordinates
    assert [call.args[1] for call in native.await_args_list] == [old.source_pin, fresh.source_pin]
    retained.return_value = (combined,)
    connection.fetchval.side_effect = [1, 4, True]
    assert await consumer._selection(connection, fresh, config()) == {"state": "replayed"}
    connection.fetchval.side_effect = [1, 4, False]
    assert await consumer._selection(connection, fresh, config()) == {"state": "review_required"}


@pytest.mark.asyncio
async def test_office_projection_uses_successor_canonical_oid_and_evidence(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    expected_oid, expected_sql = offices._canonical_source(fresh.source_pin)
    assert offices._canonical_source(combined.source_pin) == (expected_oid, expected_sql)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"invalid": False, "pairs": 0, "covered_pairs": 0, "records_json": "[]"})
    )
    batch = SimpleNamespace(source=combined.source_pin, input_bytes=b"[]", accepted_evidence_checksums=())
    address = SimpleNamespace(schema_name="candidate_addresses", table_name="entity_address_unified")
    assert await offices._office_records(connection, batch, address) == []
    sql, *parameters = connection.fetchrow.await_args.args
    assert '"' + fresh.source_pin.prepared.ownership.schema_name + '".mrf_address_evidence' in sql
    assert '"' + old.source_pin.prepared.ownership.schema_name + '".mrf_address_evidence' not in sql
    assert parameters[1:5] == [
        fresh.source_pin.import_id,
        fresh.source_pin.issuer_id,
        fresh.source_pin.year,
        fresh.source_pin.source_url,
    ]


@pytest.mark.asyncio
async def test_replay_revalidates_composite_physical_source_and_current_original_map(monkeypatch):
    old, fresh, combined = await recipe_pair(monkeypatch)
    approved = ApprovedMembershipSource(1, "a" * 64)
    candidate_by_name = {
        "source_recipes_json": codec.canonical_registry_source_recipes((combined,)),
        "approved_custom_revision": 1,
    }
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    selected = AsyncMock(return_value=True)
    native = AsyncMock()
    monkeypatch.setattr(consumer, "_selected_anchor", selected)
    monkeypatch.setattr(consumer, "_require_archive", native)
    assert await consumer._replay_recipes(object(), candidate_by_name, fresh, config()) == (combined,)
    assert selected.await_args.args[2] == old.binding_coordinates
    assert native.await_args.args[1] == combined.source_pin
    selected.return_value = False
    with pytest.raises(ValueError):
        await consumer._replay_recipes(object(), candidate_by_name, fresh, config())
    selected.return_value = True
    foreign = await source_recipe(monkeypatch, 3)
    with pytest.raises(ValueError):
        await consumer._replay_recipes(object(), candidate_by_name, foreign, config())


@pytest.mark.asyncio
async def test_final_fence_uses_flat_notice_while_rechecking_original_anchor(monkeypatch):
    from contextlib import asynccontextmanager

    from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime

    old, fresh, combined = await recipe_pair(monkeypatch)
    approved = ApprovedMembershipSource(1, "a" * 64)
    pin = SimpleNamespace(generation_id=3, schema_name="prior_candidate", manifest_sha256="b" * 64)
    copy_target = SimpleNamespace(candidate_id=str(UUID(int=8)))
    runtime = object.__new__(RegistryPublicationPTGRuntime)
    monkeypatch.setattr(consumer, "_selection", AsyncMock(return_value=(1, approved, pin, (combined,))))
    address_source = object()
    compose = AsyncMock(return_value=(copy_target, address_source))
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", compose)
    prepare = AsyncMock()
    monkeypatch.setattr(consumer, "prepare_registry_candidate_for_context", prepare)
    monkeypatch.setattr(consumer, "_checkpoint", AsyncMock(return_value=({}, {})))
    notice = AsyncMock(return_value=fresh)
    monkeypatch.setattr(consumer, "_notice", notice)
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    bindings, native, publish = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", bindings)
    monkeypatch.setattr(consumer, "_require_archive", native)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)

    @asynccontextmanager
    async def fence(actual, _config):
        assert actual == fresh and actual.source_pin.retained_successor is None
        yield

    monkeypatch.setattr(consumer, "current_aca_source_fence", fence)
    connection = SimpleNamespace(transaction=transaction, execute=AsyncMock(return_value="UPDATE 1"))
    event_by_name = {"event_id": str(UUID(int=9)), "header": {}}
    settings = config(composition_runtime=runtime)
    assert await consumer._attempt(connection, None, None, {}, event_by_name, fresh, settings) == {"state": "completed"}
    prepare.assert_awaited_once_with(connection, copy_target, address_source, settings)
    assert compose.await_args.kwargs["source_manifest"] is pin
    assert compose.await_args.kwargs["address_sources"].replacement_source_recipes == (combined,)
    notice.assert_awaited_once_with(None, None, {}, event_by_name["header"])
    assert bindings.await_args.args[2] == old.binding_coordinates
    assert native.await_args.args[1] == combined.source_pin
    publish.assert_awaited_once_with(connection, copy_target, control_schema="registry_control")


@pytest.mark.asyncio
async def test_stale_replayed_successor_preserves_source_freshness_refusal(monkeypatch):
    replay = AsyncMock(side_effect=consumer.StaleACASourceError())
    attempt = AsyncMock()
    monkeypatch.setattr(consumer, "_replay", replay)
    monkeypatch.setattr(consumer, "_attempt", attempt)
    assert await consumer._retry_source_attempt(None, None, None, {}, {}, None, config()) == {"state": "stale_source"}
    attempt.assert_not_awaited()


@pytest.mark.asyncio
async def test_original_producer_recipe_keeps_exact_baseline_hashes(monkeypatch):
    monkeypatch.setattr(fixture, "_DATASET", UUID("11111111-1111-4111-8111-111111111111"))
    monkeypatch.setattr(fixture, "_metadata", _METADATA)
    monkeypatch.setattr(fixture, "_completed", _COMPLETED)
    prepared, validation = await fixture._prepared(monkeypatch)
    recipe = producer._recipe(prepared, validation, fixture._ROLES)
    assert recipe.source_pin.generation_id == "3b1838cfca670d80e4408d1ac4921e922fc5c44a23cc4053d5ea1092166a5e30"
    encoded = codec.canonical_registry_source_recipes((recipe,))
    assert (
        hashlib.sha256(encoded.encode()).hexdigest()
        == "b17874b7c7836c85fb21bcc8fd3d78a3bd9f5ee8308cc55816ee1f50067f9004"
    )
