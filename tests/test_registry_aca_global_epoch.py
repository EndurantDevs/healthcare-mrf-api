# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed global-epoch protocol; genuine native admission is qualified separately."""

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from process import registry_aca_global_epoch as global_epoch
from process.mrf_address_publication import REFERENCES
from process.mrf_publication_receipt import SUMMARY_INPUTS
from process.reference_family_result_generation import ReferenceFamilyResultGenerationAuthority
from tests.test_registry_aca_source_recipe import (
    _DATASET,
    _ROLES,
    _assert_comment_literal,
    _comment_session,
    _completed,
    _prepared,
)


def _global_completion():
    completed = _completed()
    from process.reference_family_result_generation import validate_reference_family_serving_generation

    generation = validate_reference_family_serving_generation(completed["generation"]["serving_generation"])
    completed["generation"] = ReferenceFamilyResultGenerationAuthority(
        "mrf", str(_DATASET), 2, generation, tuple(range(100, 112))
    ).as_dict()
    completed["summary_inputs"] = {name: index + 200 for index, name in enumerate(SUMMARY_INPUTS)}
    completed["native_address_coverage"] = {
        "contract": "mrf-address-coverage.indexed-set.v2",
        "archive_name": "address_archive_v2",
        "archive_oid": 300,
        "tables": {name: {"rows": 5, "uncovered": 0} for name in REFERENCES},
    }
    return completed


def _publication():
    return {
        "contract": global_epoch.PUBLICATION_CONTRACT,
        "importer_id": "mrf",
        "serving_generation": _completed()["generation"]["serving_generation"],
    }


async def _global_prepared(monkeypatch, *, import_id=None):
    prepared, validation = await _prepared(monkeypatch)
    completion = _global_completion()
    if import_id is not None:
        completion["import_id"] = import_id
    monkeypatch.setattr(global_epoch, "require_completed_publication", AsyncMock(return_value=completion))
    metadata = await global_epoch.capture_registry_aca_global_metadata(
        object(), _publication(), schema="mrf", runtime_roles=_ROLES
    )
    manifest = replace(
        prepared.manifest, source_metadata=metadata, source_metadata_sha256=archive._source_metadata(metadata)[1]
    )
    fields_map = validation.as_dict()
    fields_map.pop("validation_sha256")
    fields_map["manifest_sha256"] = hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest()
    validation = archive.validate_reference_family_validation_receipt(
        {**fields_map, "validation_sha256": archive._validation_digest(fields_map)}
    )
    return replace(prepared, manifest=manifest), validation


@pytest.mark.asyncio
async def test_global_capture_binds_complete_native_publication_without_logical_scope(monkeypatch):
    gate = AsyncMock(return_value=_global_completion())
    monkeypatch.setattr(global_epoch, "require_completed_publication", gate)
    publication = _publication()
    metadata = await global_epoch.capture_registry_aca_global_metadata(
        object(), publication, schema="mrf", runtime_roles=_ROLES
    )
    assert metadata["registry_aca_global_epoch"]["publication"] == publication
    assert set(metadata) == {"registry_aca_global_epoch"}
    completion = metadata["registry_aca_global_epoch"]["completed_publication"]
    assert completion["import_id"] == "import-one"
    assert completion["native_address_coverage"]["tables"] == _global_completion()["native_address_coverage"]["tables"]
    gate.assert_awaited_once_with(gate.await_args.args[0], "mrf", native_set=True)
    assert publication == _publication()
    gate.return_value["generation"]["serving_generation"]["origin_generation"] += 1
    with pytest.raises(ValueError):
        await global_epoch.capture_registry_aca_global_metadata(
            object(), publication, schema="mrf", runtime_roles=_ROLES
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["logical", "roles", "generation", "coverage", "inputs", "manual", "untyped"])
async def test_global_epoch_refuses_partial_foreign_or_logical_evidence(monkeypatch, damage):
    prepared, validation = await _global_prepared(monkeypatch)
    metadata = deepcopy(prepared.manifest.source_metadata)
    fields_map = metadata["registry_aca_global_epoch"]
    if damage == "logical":
        fields_map["issuer_id"] = 7
    if damage == "roles":
        fields_map["runtime_roles"] = ["unconfigured_reader"]
    if damage == "generation":
        fields_map["publication"]["serving_generation"]["origin_generation"] += 1
    if damage == "coverage":
        coverage_tables = fields_map["completed_publication"]["native_address_coverage"]["tables"]
        first_counts = next(iter(coverage_tables.values()))
        first_counts["uncovered"] = 1
    if damage == "inputs":
        fields_map["completed_publication"]["summary_inputs"].pop("plan")
    manifest = replace(prepared.manifest, source_metadata=metadata)
    if damage == "manual":
        manifest = replace(manifest, publication_authority="manual-only")
    if damage == "untyped":
        validation = replace(validation, profile_contract=archive.MRF_CONTRACT)
    with pytest.raises((ValueError, RuntimeError)):
        global_epoch.PinnedACAGlobalEpoch(replace(prepared, manifest=manifest), validation, _ROLES)


@pytest.mark.asyncio
async def test_global_document_roundtrip_bounds_and_full_identity(monkeypatch):
    prepared, validation = await _global_prepared(monkeypatch)
    epoch = global_epoch.PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    assert global_epoch.decode_pinned_aca_global_epoch(epoch.as_dict()) == epoch
    assert global_epoch.decode_pinned_aca_global_epoch(json.dumps(epoch.as_dict())) == epoch
    assert len(epoch.generation_id) == 64
    assert (
        epoch.generation_id
        != replace(
            epoch, prepared=replace(prepared, ownership=replace(prepared.ownership, auxiliary_oid=999))
        ).generation_id
    )
    for document in (
        {**epoch.as_dict(), "scope": []},
        {**epoch.as_dict(), "runtime_roles": []},
        " " * (global_epoch.MAX_BYTES + 1),
    ):
        with pytest.raises(ValueError):
            global_epoch.decode_pinned_aca_global_epoch(document)


@pytest.mark.asyncio
async def test_global_admission_stamps_only_after_validation_and_native_custody(monkeypatch):
    prepared, validation = await _global_prepared(monkeypatch)
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=None), execute=AsyncMock())
    monkeypatch.setattr(global_epoch, "protected_publisher_owner", AsyncMock(return_value=13))
    activation = AsyncMock(return_value=validation)
    native = AsyncMock()
    connection = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(global_epoch, "native_driver", AsyncMock(return_value=connection))
    monkeypatch.setattr(global_epoch, "prepare_reference_family_activation", activation)
    monkeypatch.setattr(global_epoch, "_native_epoch", native)
    epoch = await global_epoch.admit_registry_aca_global_epoch(session, prepared, runtime_roles=_ROLES)
    activation.assert_awaited_once()
    native.assert_awaited_once_with(session, epoch)
    assert global_epoch._PREFIX in connection.execute.await_args.args[0]
    session.scalar.return_value = global_epoch._PREFIX + archive._canonical_json(epoch.as_dict()).decode()
    assert await global_epoch.require_registry_aca_global_epoch(session, prepared, runtime_roles=_ROLES) == epoch
    native.side_effect = ValueError("native custody changed")
    with pytest.raises(ValueError, match="native custody changed"):
        await global_epoch.require_registry_aca_global_epoch(session, prepared, runtime_roles=_ROLES)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["validation", "custody", "cancel", "conflicting_stamp", "stamp_io"])
async def test_global_admission_failure_never_writes_stamp(monkeypatch, failure):
    prepared, validation = await _global_prepared(monkeypatch)
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value=None if failure == "stamp_io" else "foreign"),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(global_epoch, "protected_publisher_owner", AsyncMock(return_value=13))
    activation, native = AsyncMock(return_value=validation), AsyncMock()
    if failure == "validation":
        activation.side_effect = ValueError("invalid clone")
    elif failure in {"custody", "cancel"}:
        native.side_effect = asyncio.CancelledError() if failure == "cancel" else ValueError("invalid native closure")
    monkeypatch.setattr(global_epoch, "prepare_reference_family_activation", activation)
    monkeypatch.setattr(global_epoch, "_native_epoch", native)
    connection = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("stamp rejected")))
    monkeypatch.setattr(global_epoch, "native_driver", AsyncMock(return_value=connection))
    with pytest.raises((ValueError, RuntimeError, asyncio.CancelledError)):
        await global_epoch.admit_registry_aca_global_epoch(session, prepared, runtime_roles=_ROLES)
    session.execute.assert_not_awaited()
    assert connection.execute.await_count == int(failure == "stamp_io")


@pytest.mark.asyncio
async def test_native_epoch_forwards_exact_physical_proofs_and_census(monkeypatch):
    from process import network_legacy_membership_source as retained

    prepared, validation = await _global_prepared(monkeypatch)
    epoch = global_epoch.PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    verifier = AsyncMock()
    monkeypatch.setattr(retained, "_require_native_archive", verifier)
    monkeypatch.setattr(global_epoch, "native_driver", AsyncMock(return_value="native"))
    await global_epoch._native_epoch("session", epoch)
    verifier.assert_awaited_once_with("native", prepared, validation, _ROLES)
    verifier.side_effect = ValueError("native closure drift")
    with pytest.raises(ValueError, match="native closure drift"):
        await global_epoch._native_epoch("session", epoch)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sqlalchemy_session", "native_facade"])
async def test_global_comment_preserves_json_and_original_physical_transaction(monkeypatch, kind):
    label = "import:7 O'Reilly\\folder café"
    prepared, validation = await _global_prepared(monkeypatch, import_id=label)
    session, connection = _comment_session(kind)
    monkeypatch.setattr(global_epoch, "protected_publisher_owner", AsyncMock(return_value=13))
    monkeypatch.setattr(global_epoch, "prepare_reference_family_activation", AsyncMock(return_value=validation))

    async def custody(original, epoch):
        assert original is session and epoch.prepared is prepared
        assert await global_epoch.native_driver(original) is connection

    monkeypatch.setattr(global_epoch, "_native_epoch", custody)
    epoch = await global_epoch.admit_registry_aca_global_epoch(session, prepared, runtime_roles=_ROLES)
    encoded = global_epoch._PREFIX + archive._canonical_json(epoch.as_dict()).decode()
    (statement,) = connection.execute.await_args.args
    _assert_comment_literal(statement, encoded, prepared.ownership.schema_name)
    assert (
        label
        == json.loads(encoded[len(global_epoch._PREFIX) :])["prepared"]["manifest"]["source_metadata"][
            "registry_aca_global_epoch"
        ]["completed_publication"]["import_id"]
    )
    assert session.in_transaction()
    connection.transaction.assert_not_called()
    connection.close.assert_not_awaited()
