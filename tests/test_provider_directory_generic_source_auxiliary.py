# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Global identity stages retain one header, publisher and atomic publication cut."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_auxiliary as auxiliary
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_candidate import GenericCandidateError


@asynccontextmanager
async def _transaction():
    yield


def _case():
    identity_by_field = {
        "dataset_id": "dataset",
        "endpoint_id": "endpoint",
        "acquisition_root_run_id": "acquisition",
        "metadata": {
            "source_ids": ["cms-npd"],
            "source_release": {"source_id": "cms-npd", "vector_sha256": "a" * 64},
            "network_bindings": {
                "source_system": "fhir",
                "source_id": "cms-npd",
                "dataset_schema": "source_schema",
                "dataset_id": "dataset",
                "edition_id": "dataset",
                "producer_id": "cms-npd-bulk-fhir-v1",
                "alias_scope": "cms-npd",
                "source_key_kind": "organization_resource_id",
            },
        },
    }
    receipts_by_root = {
        root: {
            "name": "pdgs_" + "b" * 32 + "_" + storage._SUFFIXES[root],
            "oid": index + 40,
            "owner": 31,
            "layout_sha256": "c" * 64,
        }
        for index, root in enumerate(storage.AUXILIARY_ROOTS)
    }
    publisher = SimpleNamespace(
        connection=SimpleNamespace(execute=AsyncMock(), transaction=_transaction),
        policy=SimpleNamespace(schema="source_schema"),
    )
    binding_by_field = {
        "identity": identity_by_field,
        "phase": "prepared",
        "auxiliary": {"phase": "loading", "tables": receipts_by_root, "sealed_sha256": None},
    }
    return publisher, binding_by_field


@pytest.mark.asyncio
async def test_wrong_declared_producer_refuses_before_allocating_any_stage(monkeypatch):
    publisher, binding = _case()
    binding["identity"]["metadata"]["network_bindings"]["producer_id"] = "another-producer"
    helper = SimpleNamespace(prepare_identity_stages=AsyncMock())
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: helper)
    with pytest.raises(GenericCandidateError, match="auxiliary_producer_changed"):
        await auxiliary.prepare_identity_candidate(publisher, binding["identity"], "pdgs_" + "b" * 32)
    publisher.connection.execute.assert_not_awaited()
    helper.prepare_identity_stages.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_family", ["cms", "resource"])
async def test_failed_index_or_set_check_preserves_loading_binding_before_header_cas(monkeypatch, failed_family):
    publisher, binding = _case()
    previous = deepcopy(binding)
    token = auxiliary._ACTIVE.set((publisher, binding))

    @asynccontextmanager
    async def owner(database):
        yield publisher

    failure = RuntimeError("invalid global facts")
    helper = SimpleNamespace(finish_identity_stages=AsyncMock(side_effect=failure if failed_family == "cms" else None))
    resource_helper = SimpleNamespace(
        finish_resource_identity_stage=AsyncMock(side_effect=failure if failed_family == "resource" else None)
    )
    monkeypatch.setattr(auxiliary, "_resource_helper", lambda: resource_helper)
    monkeypatch.setattr(auxiliary, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(auxiliary, "source_owner_transaction", owner)
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: helper)
    monkeypatch.setattr(storage, "candidate_identity", lambda *args: binding["identity"])
    monkeypatch.setattr(storage, "_locked", AsyncMock(return_value=deepcopy(binding)))
    monkeypatch.setattr(storage, "_verify_tables", AsyncMock())
    store = AsyncMock()
    monkeypatch.setattr(storage, "_store", store)
    try:
        with pytest.raises(RuntimeError, match="invalid global facts"):
            await auxiliary.seal_identity_candidate(
                object(), SimpleNamespace(_schema=lambda: "source_schema"), object()
            )
    finally:
        auxiliary._ACTIVE.reset(token)
    assert binding == previous
    store.assert_not_awaited()


@pytest.mark.asyncio
async def test_seal_updates_native_receipts_only_after_cas(monkeypatch):
    publisher, binding = _case()
    old_receipts = binding["auxiliary"]["tables"]
    recaptured_by_name = {proof["name"]: {**proof, "layout_sha256": "d" * 64} for proof in old_receipts.values()}
    token = auxiliary._ACTIVE.set((publisher, binding))

    @asynccontextmanager
    async def owner(database):
        yield publisher

    helper = SimpleNamespace(finish_identity_stages=AsyncMock())
    resource_helper = SimpleNamespace(finish_resource_identity_stage=AsyncMock())
    monkeypatch.setattr(auxiliary, "_resource_helper", lambda: resource_helper)
    monkeypatch.setattr(auxiliary, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(auxiliary, "source_owner_transaction", owner)
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: helper)
    monkeypatch.setattr(storage, "candidate_identity", lambda *args: binding["identity"])
    monkeypatch.setattr(storage, "_locked", AsyncMock(side_effect=lambda *args, **kw: deepcopy(binding)))
    monkeypatch.setattr(storage, "_verify_tables", AsyncMock())
    monkeypatch.setattr(storage, "_capture", AsyncMock(return_value=recaptured_by_name))

    async def store(connection, policy, identity, before, after):
        assert binding["auxiliary"]["phase"] == "loading"
        assert all(proof["layout_sha256"] == "c" * 64 for proof in old_receipts.values())
        assert after["auxiliary"]["sealed_sha256"] == auxiliary._seal_digest(identity, after["auxiliary"]["tables"])

    monkeypatch.setattr(storage, "_store", AsyncMock(side_effect=store))
    try:
        await auxiliary.seal_identity_candidate(object(), SimpleNamespace(_schema=lambda: "source_schema"), object())
        await auxiliary.seal_identity_candidate(object(), SimpleNamespace(_schema=lambda: "source_schema"), object())
    finally:
        auxiliary._ACTIVE.reset(token)
    assert binding["auxiliary"]["phase"] == "sealed"
    assert all(proof["layout_sha256"] == "d" * 64 for proof in old_receipts.values())
    helper.finish_identity_stages.assert_awaited_once()


@pytest.mark.asyncio
async def test_global_conflict_aborts_before_any_stage_drop_or_snapshot_attachment(monkeypatch):
    publisher, binding = _case()
    binding["auxiliary"]["phase"] = "sealed"
    binding["auxiliary"]["sealed_sha256"] = auxiliary._seal_digest(binding["identity"], binding["auxiliary"]["tables"])
    helper = SimpleNamespace(publish_identity_stages=AsyncMock(side_effect=RuntimeError("whole candidate reprepare")))
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: helper)
    verify = AsyncMock()
    monkeypatch.setattr(storage, "_verify_auxiliary_tables", verify)
    with pytest.raises(RuntimeError, match="whole candidate reprepare"):
        await auxiliary.publish_identity_candidate(publisher, binding)
    verify.assert_awaited_once()
    publisher.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsealed_global_candidate_cannot_publish(monkeypatch):
    publisher, binding = _case()
    helper = SimpleNamespace(publish_identity_stages=AsyncMock())
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: helper)
    with pytest.raises(GenericCandidateError, match="auxiliary_seal_required"):
        await auxiliary.publish_identity_candidate(publisher, binding)
    helper.publish_identity_stages.assert_not_awaited()
    publisher.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reference_seal_builds_indexes_once_and_updates_the_protected_receipt(monkeypatch):
    from process import provider_directory_generic_source_completion as completion

    publisher, binding = _case()
    proof_by_field = {"name": "pdgs_" + "b" * 32 + "_rl", "oid": 91, "owner": 31, "layout_sha256": "c" * 64}
    binding.update(tables={storage.RELATIONSHIP: proof_by_field}, relationship_sha256=None)
    token = auxiliary._ACTIVE.set((publisher, binding))

    @asynccontextmanager
    async def owner(database):
        yield publisher

    monkeypatch.setattr(auxiliary, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(auxiliary, "source_owner_transaction", owner)
    monkeypatch.setattr(storage, "candidate_identity", lambda *args: binding["identity"])
    monkeypatch.setattr(storage, "_locked", AsyncMock(side_effect=lambda *args, **kw: deepcopy(binding)))
    monkeypatch.setattr(storage, "_verify_tables", AsyncMock())
    completed_by_field = {**proof_by_field, "layout_sha256": "d" * 64}
    monkeypatch.setattr(storage, "_capture", AsyncMock(return_value={proof_by_field["name"]: completed_by_field}))
    store = AsyncMock()
    indexes = AsyncMock()
    monkeypatch.setattr(storage, "_store", store)
    monkeypatch.setattr(completion, "_indexes", indexes)
    try:
        for _ in range(2):
            await auxiliary.seal_relationship_candidate(
                object(), SimpleNamespace(_schema=lambda: "source_schema"), object()
            )
    finally:
        auxiliary._ACTIVE.reset(token)
    indexes.assert_awaited_once_with(
        publisher.connection, publisher.policy, storage.RELATIONSHIP, proof_by_field["name"]
    )
    store.assert_awaited_once()
    assert binding["tables"][storage.RELATIONSHIP] == completed_by_field
    assert binding["relationship_sha256"] == auxiliary.relationship_seal(binding["identity"], completed_by_field)


def test_bulk_models_cannot_recreate_replaced_fks():
    from db import models

    names = (
        "ProviderDirectoryEntitySourceBinding",
        "ProviderDirectoryEntityReleaseEvidence",
        "ProviderDirectoryInsuranceNetworkSourceBinding",
        "ProviderDirectoryInsuranceNetworkPlanEvidence",
        "ProviderDirectoryCMSDoctorsGroupBinding",
        "ProviderDirectoryCMSDoctorsSiteBinding",
    )
    for name in names:
        model = getattr(models, name)
        assert not model.__table__.foreign_keys
        assert model.__runtime_schema_sync__ is False
        assert model.__table__.info["managed_immutable_storage"] is True
        assert model.__table__.primary_key.columns


@pytest.mark.asyncio
async def test_fifth_global_conflict_prevents_all_auxiliary_storage_cleanup(monkeypatch):
    publisher, binding = _case()
    binding["auxiliary"]["phase"] = "sealed"
    binding["auxiliary"]["sealed_sha256"] = auxiliary._seal_digest(binding["identity"], binding["auxiliary"]["tables"])
    cms_helper = SimpleNamespace(publish_identity_stages=AsyncMock())
    resource_helper = SimpleNamespace(
        publish_resource_identity_stage=AsyncMock(side_effect=RuntimeError("whole candidate reprepare"))
    )
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: cms_helper)
    monkeypatch.setattr(auxiliary, "_resource_helper", lambda: resource_helper)
    monkeypatch.setattr(storage, "_verify_auxiliary_tables", AsyncMock())
    with pytest.raises(RuntimeError, match="whole candidate reprepare"):
        await auxiliary.publish_identity_candidate(publisher, binding)
    cms_helper.publish_identity_stages.assert_awaited_once()
    assert set(cms_helper.publish_identity_stages.await_args.args[2]) == set(storage.CMS_IDENTITY_ROOTS)
    resource_helper.publish_resource_identity_stage.assert_awaited_once_with(
        publisher, binding["identity"], binding["auxiliary"]["tables"][storage.CMS_RESOURCE_IDENTITY_ROOT]
    )
    publisher.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "publisher", "identity"])
async def test_nested_validation_reuses_only_the_exact_active_candidate(monkeypatch, change):
    publisher, binding = _case()
    original = deepcopy(binding)
    token = auxiliary._ACTIVE.set((publisher, original))
    monkeypatch.setattr(auxiliary, "bound_publisher", lambda: original_publisher)
    original_publisher = publisher
    helper = SimpleNamespace(bind_cms_identity_candidate=AsyncMock(side_effect=AssertionError("scope rebound")))
    monkeypatch.setattr(auxiliary, "_identity_helper", lambda: helper)
    if change == "identity":
        binding["identity"]["dataset_id"] = "another-candidate"
    elif change == "publisher":
        publisher = SimpleNamespace(connection=publisher.connection, policy=publisher.policy)
    try:
        if change is None:
            async with auxiliary.identity_candidate_scope(publisher, binding):
                assert auxiliary._ACTIVE.get() == (publisher, original)
        else:
            with pytest.raises(GenericCandidateError, match="auxiliary_binding_changed"):
                async with auxiliary.identity_candidate_scope(publisher, binding):
                    raise AssertionError("changed scope entered")
    finally:
        auxiliary._ACTIVE.reset(token)
    helper.bind_cms_identity_candidate.assert_not_called()
