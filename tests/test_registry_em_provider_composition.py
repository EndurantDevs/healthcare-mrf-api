# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider dependencies belong to the candidate recipe before publication."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_candidate_composition as composition
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from process.network_em_provider_inputs import _ROLES, EMProviderInputRelation, EMProviderInputs
from process.network_serving_read import PinnedNetworkServingManifest


def _inputs():
    return EMProviderInputs(
        tuple(
            EMProviderInputRelation(role, "retained_provider", "held_" + str(i), "edition-one", 100 + i, 200 + i)
            for i, role in enumerate(_ROLES)
        )
    )


def _sources(inputs=None):
    return composition.RegistryCompositionAddressSources(
        PinnedAddressSource("retained_addresses", "entity_address_unified", "edition-one"), em_provider_inputs=inputs
    )


def _manifest(generations):
    identity = UUID("11111111-1111-4111-8111-111111111111")
    return PinnedNetworkServingManifest(
        3, str(identity), "network_candidate_" + identity.hex, 1, generations, 2, "f" * 64, 1000
    )


def test_worker_config_preserves_explicit_full_provider_family():
    family = _inputs()
    parsed = worker._initial_sources(
        {
            "base_source": asdict(_sources().base_source),
            "em_provider_inputs": [asdict(item) for item in family.relations],
        }
    )
    assert parsed.em_provider_inputs == family
    assert parsed.base_source == _sources().base_source
    assert worker._initial_sources({"base_source": asdict(_sources().base_source)}).em_provider_inputs is None


@pytest.mark.parametrize("value", [False, {}, [], [None], [asdict(_inputs().relations[0])]])
def test_worker_refuses_partial_or_ambiguous_provider_config(value):
    with pytest.raises((ValueError, TypeError)):
        worker._initial_sources({"base_source": asdict(_sources().base_source), "em_provider_inputs": value})


def test_composition_accepts_only_the_frozen_provider_carrier():
    with pytest.raises(composition.RegistryCompositionError, match="em_provider_inputs"):
        _sources({"relations": []})


@pytest.mark.parametrize("state", ["sealed", "ready", "published"])
@pytest.mark.parametrize("encoded", [False, True])
def test_closed_replay_requires_the_existing_exact_provider_receipt(state, encoded):
    receipt_by_field = {"receipt_sha256": "a" * 64}
    validation_by_field = {"em_provider_inputs": receipt_by_field}
    composition._require_retained_em_provider_receipt(
        {"state": state, "validation_json": json.dumps(validation_by_field) if encoded else validation_by_field},
        receipt_by_field,
    )
    with pytest.raises(composition.RegistryCompositionError, match="em_provider_inputs_changed"):
        composition._require_retained_em_provider_receipt({"state": state, "validation_json": {}}, receipt_by_field)


def test_open_candidate_may_record_recipe_and_legacy_replay_remains_unchanged():
    composition._require_retained_em_provider_receipt({"state": "open"}, {"receipt_sha256": "a" * 64})
    composition._require_retained_em_provider_receipt({"state": "published"}, None)


@pytest.mark.asyncio
async def test_legacy_composition_does_not_capture_provider_inputs(monkeypatch):
    prepare = AsyncMock()
    resolve = AsyncMock()
    monkeypatch.setattr(composition, "prepare_em_provider_inputs", prepare)
    monkeypatch.setattr(composition, "resolve_em_provider_inputs", resolve)
    assert await composition._em_provider_receipt(object(), _sources(), _manifest({}), {}, "registry") is None
    prepare.assert_not_awaited()
    resolve.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_composition_inherits_or_explicitly_replaces_retained_dependencies(monkeypatch, explicit):
    connection = object()
    family = _inputs()
    manifest = _manifest({"em_provider_inputs": "a" * 64})
    resolve = AsyncMock(return_value=family)
    receipt_by_field = {"receipt_sha256": "b" * 64}
    prepare = AsyncMock(return_value=receipt_by_field)
    monkeypatch.setattr(composition, "resolve_em_provider_inputs", resolve)
    monkeypatch.setattr(composition, "prepare_em_provider_inputs", prepare)
    roles_by_kind = {"loader_roles": ["reader_b", "reader_a"], "reader_roles": ["reader_a"]}
    assert (
        await composition._em_provider_receipt(
            connection, _sources(family if explicit else None), manifest, roles_by_kind, "registry"
        )
        == receipt_by_field
    )
    prepare.assert_awaited_once_with(connection, family, runtime_roles=("reader_a", "reader_b"))
    if explicit:
        resolve.assert_not_awaited()
    else:
        resolve.assert_awaited_once_with(connection, manifest, "registry")


@pytest.mark.asyncio
async def test_stale_retained_dependencies_cannot_be_silently_dropped(monkeypatch):
    prepare = AsyncMock()
    monkeypatch.setattr(composition, "prepare_em_provider_inputs", prepare)
    monkeypatch.setattr(composition, "resolve_em_provider_inputs", AsyncMock(side_effect=ValueError("stale")))
    with pytest.raises(ValueError, match="stale"):
        await composition._em_provider_receipt(
            object(), _sources(), _manifest({"em_provider_inputs": "a" * 64}), {}, None
        )
    prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_digest_enters_identity_before_candidate_creation_and_record(monkeypatch):
    connection = object()
    request_id = UUID("22222222-2222-4222-8222-222222222222")
    address_sources = _sources(_inputs())
    receipt_by_field = {"receipt_sha256": "a" * 64}
    approved = SimpleNamespace(approved_revision=2, generation_id="b" * 64)
    artifact = SimpleNamespace(generation_sha256="c" * 64, address_source=address_sources.base_source)
    source_stage, command_stage = object(), object()
    calls = []
    monkeypatch.setattr(composition, "_preparation_driver", AsyncMock(return_value=(connection, None)))
    monkeypatch.setattr(composition, "_em_provider_receipt", AsyncMock(return_value=receipt_by_field))
    monkeypatch.setattr(composition, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(composition, "_composition_sources", AsyncMock(return_value=({}, 0, (), source_stage)))
    monkeypatch.setattr(
        composition, "_composition_retention", AsyncMock(return_value=(command_stage, {"effective_rows": 0}))
    )
    monkeypatch.setattr(composition, "registry_source_retention_sha256", lambda _receipt: "d" * 64)
    monkeypatch.setattr(composition, "_composition_artifact", AsyncMock(return_value=(artifact, None)))
    monkeypatch.setattr(composition, "_bindings", AsyncMock())
    monkeypatch.setattr(composition, "_record_source_receipts", AsyncMock())

    async def create(_connection, target, **kwargs):
        assert kwargs["source_generations"]["em_provider_inputs"] == receipt_by_field["receipt_sha256"]
        assert target == composition._target(request_id, 2, 0, kwargs["source_generations"])
        calls.append(("create", target))
        return {"state": "open"}

    async def record(_connection, target, recorded, control_schema):
        assert recorded is receipt_by_field and control_schema == "registry"
        assert calls == [("create", target)]
        calls.append(("record", target))

    monkeypatch.setattr(composition, "create_network_candidate", create)
    monkeypatch.setattr(composition, "record_em_provider_inputs", record)
    prepared_snapshot = await composition._prepare_composition_snapshot(
        connection, request_id, 2, 0, address_sources, None, {"loader_roles": [], "reader_roles": []}, "registry"
    )
    assert calls == [("create", prepared_snapshot[0]), ("record", prepared_snapshot[0])]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_closed_candidate_checks_both_receipts_without_recording(monkeypatch, changed):
    provider_receipt_by_field = {"receipt_sha256": "a" * 64}
    initial_summary_by_field = {"source": "retained-edition"}
    candidate_by_field = {"state": "published", "validation_json": {"em_provider_inputs": provider_receipt_by_field}}
    monkeypatch.setattr(composition, "create_network_candidate", AsyncMock(return_value=candidate_by_field))
    monkeypatch.setattr(
        composition,
        "verify_initial_source_office_receipt",
        lambda _candidate: {} if changed else initial_summary_by_field,
    )
    record = AsyncMock()
    monkeypatch.setattr(composition, "record_em_provider_inputs", record)
    call = composition._composition_candidate(
        object(),
        object(),
        {},
        2,
        0,
        ({"effective_rows": 0}, (), provider_receipt_by_field, initial_summary_by_field),
        "registry",
    )
    if changed:
        with pytest.raises(composition.RegistryCompositionError, match="initial_sources_changed"):
            await call
    else:
        assert await call is candidate_by_field
    record.assert_not_awaited()
