# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Global epochs preserve original reviewed anchors while replacing physical facts."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_initial_source_office_bindings as offices
from process import network_legacy_membership_source as aca
from process import reference_family_archive as archive
from process import registry_aca_global_epoch as global_epoch
from process import registry_aca_source_consumer as consumer
from process import registry_aca_source_recipe as producer
from process import registry_aca_source_worker as worker
from process import registry_imported_selection_preview as preview
from process import registry_source_recipe_store as codec
from process.network_approved_membership_source import ApprovedMembershipSource
from tests import test_registry_aca_source_recipe as fixture
from tests.test_registry_aca_global_epoch import _global_prepared
from tests.test_registry_aca_source_consumer import transaction
from tests.test_registry_aca_source_worker import configuration


async def global_family(monkeypatch):
    original_metadata = fixture._metadata
    anchors = []
    for issuer in (7, 8):
        metadata = original_metadata()
        metadata["network_membership"].update(issuer_id=issuer, source_id=f"source-{issuer}")
        metadata["network_bindings"]["source_id"] = f"source-{issuer}"
        with monkeypatch.context() as scoped:
            scoped.setattr(fixture, "_metadata", lambda: metadata)
            prepared, validation = await fixture._prepared(scoped)
            anchors.append(producer._recipe(prepared, validation, fixture._ROLES))
    prepared, validation = await _global_prepared(monkeypatch)
    dataset = UUID(int=20)
    ownership = replace(
        prepared.ownership, dataset_id=dataset, schema_name=archive.reference_family_stage_schema(dataset)
    )
    fields_map = validation.as_dict()
    fields_map.pop("validation_sha256")
    fields_map["stage_schema"] = ownership.schema_name
    validation = archive.validate_reference_family_validation_receipt(
        {**fields_map, "validation_sha256": archive._validation_digest(fields_map)}
    )
    epoch = global_epoch.PinnedACAGlobalEpoch(replace(prepared, ownership=ownership), validation, fixture._ROLES)
    return tuple(anchors), epoch


def global_config():
    return consumer.RegistryACASourceConsumerConfig("registry_control", "executor", "loading_owner", global_epoch=True)


def attach(anchor, epoch):
    return replace(anchor, source_pin=aca.with_aca_global_epoch(anchor.source_pin, epoch))


@pytest.mark.asyncio
async def test_global_scope_keeps_original_identity_and_real_physical_archive(monkeypatch):
    anchors, epoch = await global_family(monkeypatch)
    combined = attach(anchors[0], epoch)
    physical = aca.aca_physical_source(combined.source_pin)
    assert type(physical) is aca.ACAGlobalEpochReadScope
    assert physical.epoch is epoch and physical.prepared is epoch.prepared
    assert physical.validation is epoch.validation and physical.runtime_roles == fixture._ROLES
    assert (physical.issuer_id, physical.year, physical.import_id, physical.source_url) == (7, 2026, None, None)
    assert combined.binding_coordinates == anchors[0].binding_coordinates
    assert combined.source_pin.prepared == anchors[0].source_pin.prepared
    assert physical.prepared.ownership.dataset_id != combined.source_pin.prepared.ownership.dataset_id
    assert set(epoch.prepared.manifest.source_metadata) == {"registry_aca_global_epoch"}
    assert combined.source_pin.generation_id != anchors[0].source_pin.generation_id
    encoded = codec.canonical_registry_source_recipes((combined,))
    assert codec.decode_registry_source_recipes(encoded) == (combined,)
    assert "retained_successor" not in json.loads(encoded)[0]["source_pin"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["null", "both", "nested", "foreign_lineage"])
async def test_global_wire_refuses_ambiguous_or_foreign_authority(monkeypatch, damage):
    anchors, epoch = await global_family(monkeypatch)
    combined = attach(anchors[0], epoch)
    document = json.loads(codec.canonical_registry_source_recipes((combined,)))
    pin = document[0]["source_pin"]
    if damage == "null":
        pin["retained_epoch"] = None
    elif damage == "both":
        pin["retained_successor"] = json.loads(codec.canonical_registry_source_recipes((anchors[0],)))[0]["source_pin"]
    elif damage == "nested":
        pin["retained_epoch"]["retained_epoch"] = {}
    else:
        pin["prepared"]["manifest"]["source_serving_generation"]["origin_lineage_id"] = str(UUID(int=99))
    with pytest.raises(codec.RegistrySourceRecipeError):
        codec.decode_registry_source_recipes(document)


@pytest.mark.asyncio
async def test_global_native_gate_proves_original_and_exact_epoch(monkeypatch):
    anchors, epoch = await global_family(monkeypatch)
    combined = attach(anchors[0], epoch)
    original = AsyncMock()
    stamp = AsyncMock(return_value=epoch)
    monkeypatch.setattr(aca, "_require_flat_archive", original)
    monkeypatch.setattr(global_epoch, "require_registry_aca_global_epoch", stamp)
    connection = object()
    await aca._require_archive(connection, combined.source_pin)
    original.assert_awaited_once_with(connection, anchors[0].source_pin)
    assert stamp.await_args.args[1] is epoch.prepared
    assert stamp.await_args.kwargs == {"runtime_roles": epoch.runtime_roles}
    stamp.return_value = object()
    with pytest.raises(aca.LegacyMembershipSourceError):
        await aca._require_archive(connection, combined.source_pin)


@pytest.mark.asyncio
async def test_global_raw_preview_and_office_filters_keep_row_provenance(monkeypatch):
    anchors, epoch = await global_family(monkeypatch)
    combined = attach(anchors[0], epoch)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"successor_invalid": False}))
    monkeypatch.setattr(aca, "require_approved_network_source_bindings", AsyncMock())
    approved = ApprovedMembershipSource(1, "a" * 64)
    await aca._read_reviewed(
        connection,
        combined.source_pin,
        approved,
        combined.binding_coordinates,
        (None, 10, None, True),
        "registry_control",
        True,
    )
    sql, *parameters = connection.fetchrow.await_args.args
    assert "($8::text IS NULL OR evidence.import_id=$8)" in sql
    assert "($11::text IS NULL OR evidence.source_url=$11)" in sql
    assert "evidence.(" not in sql and "WHERE issuer_id=$9 AND year=$10" in sql
    assert parameters[7:11] == [None, 7, 2026, None]
    assert '"' + epoch.prepared.ownership.schema_name + '".mrf_address_evidence' in sql
    assert '"' + anchors[0].source_pin.prepared.ownership.schema_name + '".plan ' in sql
    impact = preview._impact_sql(combined, '"registry_control"', '"selected"', "registry_control")
    assert "($8::text IS NULL OR import_id=$8)" in impact and "transported_network_bindings" in impact
    connection.fetchrow.return_value = {"invalid": False, "pairs": 0, "covered_pairs": 0, "records_json": "[]"}
    batch = SimpleNamespace(source=combined.source_pin, input_bytes=b"[]", accepted_evidence_checksums=())
    address = SimpleNamespace(schema_name="candidate_addresses", table_name="entity_address_unified")
    assert await offices._office_records(connection, batch, address) == []
    office_sql, *office_parameters = connection.fetchrow.await_args.args
    assert "($2::text IS NULL OR e.import_id=$2)" in office_sql
    assert "($5::text IS NULL OR e.source_url=$5)" in office_sql
    assert office_parameters[1:5] == [None, 7, 2026, None]
    assert offices._canonical_source(combined.source_pin) == offices._canonical_source(
        aca.aca_physical_source(combined.source_pin)
    )


def test_global_configuration_is_closed_and_never_accepts_logical_fields():
    document = configuration()
    fields_map = document["consumer"]
    for name in ("source_id", "producer_id", "issuer_id", "year"):
        fields_map.pop(name)
    fields_map["global_epoch"] = True
    parsed = worker.parse_registry_aca_source_worker_config(json.dumps(document))
    assert parsed.consumer.global_epoch and parsed.consumer.source_id is None
    for value in (False, "true", 1, None):
        fields_map["global_epoch"] = value
        with pytest.raises(ValueError):
            worker.parse_registry_aca_source_worker_config(json.dumps(document))
    fields_map["global_epoch"] = True
    for name, value in (("source_id", "source-7"), ("producer_id", "producer"), ("issuer_id", 7), ("year", 2026)):
        fields_map[name] = value
        with pytest.raises(ValueError):
            worker.parse_registry_aca_source_worker_config(json.dumps(document))
        fields_map.pop(name)


@pytest.mark.asyncio
async def test_only_global_worker_accepts_global_notice(monkeypatch):
    anchors, epoch = await global_family(monkeypatch)
    from tests.test_registry_aca_source_consumer import config

    assert consumer._is_accepted_notice(epoch, global_config())
    assert not consumer._is_accepted_notice(anchors[0], global_config())
    assert not consumer._is_accepted_notice(epoch, config())


@pytest.mark.asyncio
@pytest.mark.parametrize("head", [0, 4])
async def test_global_selection_updates_all_anchors_in_one_family(monkeypatch, head):
    anchors, epoch = await global_family(monkeypatch)
    config = global_config()
    if not head:
        from process.network_address_projection import PinnedAddressSource
        from process.registry_candidate_composition import RegistryCompositionAddressSources

        config = replace(
            config,
            initial_address_sources=RegistryCompositionAddressSources(
                PinnedAddressSource("addresses", "entity_address_unified", "a" * 64), initial_source_recipes=anchors
            ),
        )
    connection = SimpleNamespace(transaction=transaction, fetchval=AsyncMock(side_effect=[1, head]))
    approved = ApprovedMembershipSource(1, "a" * 64)
    pin = SimpleNamespace(generation_id=head)
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", AsyncMock(return_value=anchors))
    selected = AsyncMock(return_value=True)
    native = AsyncMock()
    monkeypatch.setattr(consumer, "_selected_anchor", selected)
    monkeypatch.setattr(consumer, "_require_archive", native)
    selection = await consumer._selection(connection, epoch, config)
    assert selection[:2] == (1, approved) and selection[2] == (pin if head else None)
    assert selection[3] == tuple(attach(anchor, epoch) for anchor in anchors)
    assert [call.args[1] for call in native.await_args_list] == [recipe.source_pin for recipe in selection[3]]
    assert {call.args[2] for call in selected.await_args_list} == {anchor.binding_coordinates for anchor in anchors}
    connection.fetchval.side_effect = [1, head]
    selected.side_effect = [True, False]
    native.reset_mock()
    assert await consumer._selection(connection, epoch, config) == {"state": "review_required"}
    native.assert_not_awaited()


@pytest.mark.asyncio
async def test_global_replay_rechecks_all_original_maps_and_composite_native_proofs(monkeypatch):
    anchors, epoch = await global_family(monkeypatch)
    recipes = tuple(attach(anchor, epoch) for anchor in anchors)
    candidate_by_name = {
        "source_recipes_json": codec.canonical_registry_source_recipes(recipes),
        "approved_custom_revision": 1,
    }
    monkeypatch.setattr(
        consumer, "pin_approved_membership_source", AsyncMock(return_value=ApprovedMembershipSource(1, "a" * 64))
    )
    selected = AsyncMock(return_value=True)
    native = AsyncMock()
    monkeypatch.setattr(consumer, "_selected_anchor", selected)
    monkeypatch.setattr(consumer, "_require_archive", native)
    assert await consumer._replay_recipes(object(), candidate_by_name, epoch, global_config()) == recipes
    assert [call.args[1] for call in native.await_args_list] == [recipe.source_pin for recipe in recipes]
    selected.side_effect = [True, False]
    with pytest.raises(ValueError):
        await consumer._replay_recipes(object(), candidate_by_name, epoch, global_config())


@pytest.mark.asyncio
async def test_shared_projection_preserves_trusted_caller_parameter_positions(monkeypatch):
    anchors, epoch = await global_family(monkeypatch)
    combined = attach(anchors[0], epoch)
    cte = "approved_network_bindings AS MATERIALIZED (SELECT $1 AS revision,$7 AS source_id,$12 AS edition_id)"
    projection = aca.reviewed_aca_binding_projection(
        combined.source_pin, registry_schema="registry_control", binding_cte=cte
    )
    assert projection.cte_sql.startswith(cte + ",")
    assert "$7 AS source_id,$12 AS edition_id" in projection.cte_sql
    assert projection.relation_name == "transported_network_bindings" and projection.scope_without_checksum
    assert projection.invalid_sql == "(SELECT invalid FROM successor_guard)"
    assert (
        "count(DISTINCT network_id)>1" in projection.cte_sql
        and "array_agg(record_key ORDER BY record_key)" in projection.cte_sql
    )
    flat = aca.reviewed_aca_binding_projection(
        anchors[0].source_pin, registry_schema="registry_control", binding_cte=cte
    )
    assert flat.cte_sql == cte and flat.relation_name == "approved_network_bindings" and not flat.scope_without_checksum


@pytest.mark.asyncio
async def test_current_global_fence_recaptures_full_live_publication(monkeypatch):
    from process import reference_family_result_generation as generation

    anchors, epoch = await global_family(monkeypatch)
    publication_by_name = epoch.prepared.manifest.source_metadata["registry_aca_global_epoch"]["publication"]
    serving = generation.validate_reference_family_serving_generation(publication_by_name["serving_generation"])
    live = AsyncMock(return_value=serving)
    capture = AsyncMock(return_value=epoch.prepared.manifest.source_metadata)
    monkeypatch.setattr(generation, "capture_reference_family_serving_generation", live)
    monkeypatch.setattr(global_epoch, "capture_registry_aca_global_metadata", capture)
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="mrf"))
    session = object()
    assert await consumer._current_global_metadata(session, epoch, publisher) == epoch.prepared.manifest.source_metadata
    live.assert_awaited_once_with(session, importer_id="mrf", schema_name="mrf")
    capture.assert_awaited_once_with(session, publication_by_name, schema="mrf", runtime_roles=epoch.runtime_roles)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["none", "cancel", "changed_map"])
async def test_global_attempt_composes_one_family_and_holds_fence_through_cas(monkeypatch, failure):
    import asyncio
    from contextlib import asynccontextmanager

    from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
    from tests.test_registry_aca_source_worker import event

    anchors, epoch = await global_family(monkeypatch)
    recipes = tuple(attach(anchor, epoch) for anchor in anchors)
    pin = SimpleNamespace(generation_id=2, manifest_sha256="a" * 64, schema_name="old_candidate")
    runtime = object.__new__(RegistryPublicationPTGRuntime)
    copy_target = SimpleNamespace(candidate_id=str(UUID(int=6)))
    compose = AsyncMock(return_value=(copy_target, object()))
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", compose)
    monkeypatch.setattr(
        consumer, "_selection", AsyncMock(return_value=(1, SimpleNamespace(generation_id="approved"), pin, recipes))
    )
    monkeypatch.setattr(consumer, "prepare_network_candidate", AsyncMock())
    monkeypatch.setattr(consumer, "_checkpoint", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=epoch))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock())
    verify = AsyncMock(side_effect=ValueError("changed map") if failure == "changed_map" else None)
    monkeypatch.setattr(consumer, "_verify_selected_recipes", verify)
    events = []

    @asynccontextmanager
    async def source_fence(*_args):
        events.append("source_locked")
        try:
            if failure == "cancel":
                raise asyncio.CancelledError()
            yield
        finally:
            events.append("source_released")

    monkeypatch.setattr(consumer, "current_aca_source_fence", source_fence)
    connection = SimpleNamespace(transaction=transaction, execute=AsyncMock(return_value="UPDATE 1"))
    publish = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("published"))
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    config = replace(global_config(), composition_runtime=runtime, seal_candidate=AsyncMock())
    arguments = (connection, None, None, {}, consumer._event(event(), {}), epoch, config)
    if failure == "none":
        assert await consumer._attempt(*arguments) == {"state": "completed"}
        assert events == ["source_locked", "published", "source_released"]
        verify.assert_awaited_once()
    else:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError):
            await consumer._attempt(*arguments)
        publish.assert_not_awaited()
        connection.execute.assert_not_awaited()
        assert events == ["source_locked", "source_released"]
    compose.assert_awaited_once()
    assert compose.await_args.kwargs["address_sources"].replacement_source_recipes == recipes
