# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_contract as contract
from process import provider_directory_profile_selection_snapshot as snapshot

from .test_provider_directory_profile_selection_admission import (
    _profile_params_map,
)
from .test_provider_directory_profile_selection_attestation import (
    _attestation,
    _catalog,
    _computed,
    _execution,
)


def _source_row() -> dict:
    return {
        "source_id": "pdfhir_payer",
        "endpoint_id": "endpoint-1",
        "canonical_api_base": "https://payer.example/fhir",
        "org_name": "Payer",
        "plan_name": "Payer Plan",
    }


def _dataset_row() -> dict:
    return {
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "run-root-1",
        "dataset_hash": "b" * 64,
        "resource_count": 42,
        "validated_at": "2026-07-20 10:00:00",
        "published_at": "2026-07-20 11:00:00",
        "publication_metadata_json": {"source_ids": ["pdfhir_payer"]},
    }


def _valid_request() -> dict:
    return selection._expected_request(_computed(), "dev-node")


@asynccontextmanager
async def _transaction():
    yield


def test_request_validation_rejects_every_noncanonical_shape():
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection._validated_request(None)

    wrong_contract = _valid_request()
    wrong_contract["contract_id"] = "wrong"
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection._validated_request(wrong_contract)

    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection._validated_dataset_collection(None)
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection._validated_dataset_collection([
            {"source_id": "b", "dataset_id": "1"},
            {"source_id": "a", "dataset_id": "2"},
        ])
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection._validated_dataset_collection([
            {"source_id": "a", "dataset_id": "1"},
            {"source_id": "a", "dataset_id": "1"},
        ])
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
        selection._validated_dataset_projection({"source_id": "a"})

    assert selection._validated_request(_valid_request()) == _valid_request()


@pytest.mark.asyncio
async def test_registry_row_helpers_reject_corruption_and_preserve_json(
    monkeypatch,
):
    identity = _computed().identity_payload
    proof_id = selection._proof_id(identity)
    monkeypatch.setattr(
        selection.db,
        "first",
        AsyncMock(side_effect=[
            None,
            {"proof_id": proof_id, "identity_json": "bad"},
            {"proof_id": proof_id, "identity_json": identity},
            None,
            {"input_identity_digest": "digest", "payload_json": "bad"},
            {"input_identity_digest": "digest", "payload_json": {"ok": True}},
        ]),
    )

    assert await selection._registered_proof("digest") is None
    with pytest.raises(RuntimeError, match="registry_corrupt"):
        await selection._registered_proof("digest")
    assert await selection._registered_proof("digest") == {
        "proof_id": proof_id,
        "identity_json": identity,
    }
    assert await selection._latest_registered_observation() is None
    with pytest.raises(RuntimeError, match="registry_corrupt"):
        await selection._latest_registered_observation()
    assert await selection._latest_registered_observation() == {
        "input_identity_digest": "digest",
        "payload_json": {"ok": True},
    }


@pytest.mark.asyncio
async def test_revision_proof_and_observation_writes_cover_failure_edges(
    monkeypatch,
):
    status = AsyncMock()
    monkeypatch.setattr(selection.db, "status", status)
    monkeypatch.setattr(
        selection.db,
        "first",
        AsyncMock(side_effect=[{"last_revision": 0}, {"last_revision": 9}]),
    )
    with pytest.raises(RuntimeError, match="revision_invalid"):
        await selection._next_authority_revision()
    assert await selection._next_authority_revision() == 9

    identity = _computed().identity_payload
    expected_map = {
        "proof_id": selection._proof_id(identity),
        "identity_json": identity,
    }
    registered = AsyncMock(
        side_effect=[expected_map, {**expected_map, "proof_id": "bad"}, None]
    )
    monkeypatch.setattr(selection, "_registered_proof", registered)
    await selection._ensure_selection_proof("digest", identity)
    with pytest.raises(RuntimeError, match="registry_corrupt"):
        await selection._ensure_selection_proof("digest", identity)
    await selection._ensure_selection_proof("digest", identity)

    await selection._store_observation(
        "digest",
        {"authority_revision": 9, "proof_id": expected_map["proof_id"]},
    )
    assert status.await_count == 4


@pytest.mark.asyncio
async def test_register_selection_rejects_registry_and_proof_corruption(
    monkeypatch,
):
    computed = _computed()
    latest_map = {
        "input_identity_digest": selection._input_identity_digest(
            computed.identity_payload
        ),
        "payload_json": _attestation().payload,
    }
    monkeypatch.setattr(selection.db, "scalar", AsyncMock())
    monkeypatch.setattr(
        selection,
        "_latest_registered_observation",
        AsyncMock(return_value=latest_map),
    )
    monkeypatch.setattr(selection, "_identity_without_authority", lambda _value: {})
    with pytest.raises(RuntimeError, match="registry_corrupt"):
        await selection._register_selection_proof(computed)

    monkeypatch.setattr(
        selection,
        "_latest_registered_observation",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(selection, "_ensure_selection_proof", AsyncMock())
    monkeypatch.setattr(selection, "_next_authority_revision", AsyncMock(return_value=4))
    monkeypatch.setattr(
        selection,
        "_attestation_payload",
        lambda _identity, _revision: {"proof_id": "bad"},
    )
    monkeypatch.setattr(selection, "_proof_id", lambda _identity: "expected")
    with pytest.raises(RuntimeError, match="registry_corrupt"):
        await selection._register_selection_proof(computed)


@pytest.mark.asyncio
async def test_attest_profile_selection_maps_drift_and_returns_registered_proof(
    monkeypatch,
):
    computed = _computed()
    attestation = _attestation()
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    monkeypatch.setattr(selection.db, "transaction", lambda: _transaction())
    monkeypatch.setattr(selection.db, "status", AsyncMock())
    monkeypatch.setattr(
        selection,
        "_compute_current_selection",
        AsyncMock(return_value=computed),
    )
    monkeypatch.setattr(
        selection,
        "_register_selection_proof",
        AsyncMock(return_value=attestation),
    )
    current = AsyncMock()
    monkeypatch.setattr(selection, "assert_registered_profile_selection_current", current)

    drifted_request = _valid_request()
    drifted_request["selection_fingerprint"] = "c" * 64
    with pytest.raises(selection.ProviderDirectoryProfileSelectionDrift):
        await selection.attest_profile_selection(drifted_request, _catalog())

    current.side_effect = selection.ProviderDirectoryProfileSelectionStale("stale")
    with pytest.raises(selection.ProviderDirectoryProfileSelectionDrift):
        await selection.attest_profile_selection(_valid_request(), _catalog())

    current.side_effect = None
    assert await selection.attest_profile_selection(
        _valid_request(),
        _catalog(),
    ) == attestation.payload


@pytest.mark.asyncio
async def test_current_assertion_rejects_changed_unregistered_and_old_proofs(
    monkeypatch,
):
    computed = _computed()
    attestation = _attestation()
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    compute = AsyncMock()
    monkeypatch.setattr(selection, "_compute_current_selection", compute)
    registered = AsyncMock()
    monkeypatch.setattr(selection, "_registered_proof", registered)
    latest = AsyncMock()
    monkeypatch.setattr(selection, "_latest_registered_observation", latest)

    changed_identity_map = dict(computed.identity_payload)
    changed_identity_map["source_context_digest"] = "c" * 64
    compute.return_value = selection._ComputedProfileSelection(
        computed.request_projection,
        changed_identity_map,
    )
    with pytest.raises(selection.ProviderDirectoryProfileSelectionStale, match="changed"):
        await selection._assert_registered_current_in_transaction(attestation, _catalog())

    compute.return_value = computed
    registered.return_value = None
    with pytest.raises(selection.ProviderDirectoryProfileSelectionStale, match="not_registered"):
        await selection._assert_registered_current_in_transaction(attestation, _catalog())

    identity_digest = selection._input_identity_digest(computed.identity_payload)
    registered.return_value = {
        "proof_id": attestation.proof_id,
        "identity_json": computed.identity_payload,
    }
    latest.return_value = None
    with pytest.raises(selection.ProviderDirectoryProfileSelectionStale, match="not_registered"):
        await selection._assert_registered_current_in_transaction(attestation, _catalog())

    latest.return_value = {
        "input_identity_digest": identity_digest,
        "payload_json": attestation.payload,
    }
    await selection._assert_registered_current_in_transaction(attestation, _catalog())


@pytest.mark.asyncio
async def test_current_assertion_wrappers_cover_both_transaction_modes(monkeypatch):
    inner = AsyncMock()
    monkeypatch.setattr(selection, "_assert_registered_current_in_transaction", inner)
    monkeypatch.setattr(selection.db, "transaction", lambda: _transaction())
    monkeypatch.setattr(selection.db, "status", AsyncMock())

    await selection.assert_registered_profile_selection_current(
        _attestation(),
        _catalog(),
    )
    await selection.assert_profile_selection_current_in_transaction(
        _attestation(),
        _catalog(),
    )
    assert inner.await_count == 2


def test_contract_validation_covers_rejected_scalar_and_pair_edges(monkeypatch):
    assert contract._clean_text(1) is None
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._required_text({"value": " spaced "}, "value", limit=20)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._required_hash({"value": "z" * 64}, "value")
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._validated_pair(None)

    invalid_pair_map = dict(_attestation().pairs[0])
    invalid_pair_map["is_current"] = False
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._validated_pair(invalid_pair_map)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._normalized_attestation_pairs({"pairs": None})

    duplicate_pair_map = dict(_attestation().pairs[0])
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._normalized_attestation_pairs(
            {"pairs": [duplicate_pair_map, duplicate_pair_map]}
        )

    monkeypatch.delenv("HLTHPRT_IMPORT_NODE_ID", raising=False)
    with pytest.raises(RuntimeError, match="node_not_configured"):
        contract.configured_node_id()


def test_contract_rejects_noncanonical_attestation_execution_and_results(
    monkeypatch,
):
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_selection_attestation(None)

    noncanonical_map = dict(_attestation().payload)
    noncanonical_map["operation"] = "purge"
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError, match="canonical"):
        contract.validated_profile_selection_attestation(noncanonical_map)

    params = _profile_params_map()
    params["probe"] = True
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_execution(params)

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "other-node")
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError, match="does not match"):
        contract.validated_profile_execution(_profile_params_map())

    with pytest.raises(contract.ProviderDirectoryProfileSelectionError, match="row count"):
        contract._profile_result_counts(_execution(), True, 0)

    purge_map = dict(_attestation().payload)
    purge_map.update(operation="purge", pairs=[])
    purge_map["proof_id"] = contract._proof_id(purge_map)
    purge_execution = contract.ProviderDirectoryProfileExecution(
        contract.validated_profile_selection_attestation(purge_map),
        1,
    )
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError, match="not empty"):
        contract._profile_result_counts(purge_execution, 1, 0)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError, match="generation identity"):
        contract.profile_selection_result(
            _execution(),
            profile_generation_id=" ",
            profile_rows=0,
            profile_source_evidence_rows=0,
        )


def test_snapshot_mapping_identifiers_and_catalog_validation(monkeypatch):
    assert snapshot._row_mapping(SimpleNamespace(_mapping={"a": 1})) == {"a": 1}
    assert snapshot._row_mapping({"b": 2}) == {"b": 2}
    assert snapshot._row_mapping(object()) == {}

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", 'profile"scope')
    assert snapshot._schema() == 'profile"scope'
    model = SimpleNamespace(__tablename__='source"table')
    assert snapshot._table_ref(model) == '"profile""scope"."source""table"'

    with pytest.raises(RuntimeError, match="catalog_invalid"):
        snapshot._catalog_source_groups({"catalog_digest": "short", "items": []})
    with pytest.raises(RuntimeError, match="catalog_invalid"):
        snapshot._catalog_source_groups({
            "catalog_digest": "a" * 64,
            "items": [{"runnable": True, "profile_enabled": True, "source_ids": []}],
        })
    with pytest.raises(RuntimeError, match="catalog_invalid"):
        snapshot._catalog_source_groups({
            "catalog_digest": "a" * 64,
            "items": [
                {"runnable": True, "profile_enabled": True, "source_ids": ["a"]},
                {"runnable": True, "profile_enabled": True, "source_ids": ["a"]},
            ],
        })
    assert snapshot._catalog_source_groups({
        "catalog_digest": "a" * 64,
        "items": [{"runnable": False, "profile_enabled": True, "source_ids": ["a"]}],
    }) == ()


@pytest.mark.parametrize(
    "metadata",
    [None, {}, {"source_ids": []}, {"source_ids": ["a", "a"]}, {"source_ids": [" "]}],
)
def test_snapshot_metadata_source_ids_rejects_noncanonical_values(metadata):
    assert snapshot._metadata_source_ids(metadata) is None


def test_snapshot_source_indexes_and_record_guards_cover_invalid_rows():
    with pytest.raises(RuntimeError, match="source_invalid"):
        snapshot._source_selection_indexes([{"source_id": None}])
    with pytest.raises(RuntimeError, match="source_invalid"):
        snapshot._source_selection_indexes([
            {"source_id": "a", "endpoint_id": "e"},
            {"source_id": "a", "endpoint_id": "e"},
        ])
    assert snapshot._source_selection_indexes([
        {"source_id": "a", "endpoint_id": None},
        {"source_id": "b", "endpoint_id": "e"},
    ])[1] == {"e": {"b"}}
    assert snapshot._dataset_selection_by_group(
        [{"endpoint_id": None, "publication_metadata_json": {"source_ids": ["a"]}}],
        (("a",),),
        {},
    ) == {}

    invalid_dataset = _dataset_row()
    invalid_dataset["publication_metadata_json"] = None
    with pytest.raises(RuntimeError, match="dataset_invalid"):
        snapshot._selection_records_for_group(
            ("pdfhir_payer",),
            invalid_dataset,
            {"pdfhir_payer": _source_row()},
        )
    with pytest.raises(RuntimeError, match="source_invalid"):
        snapshot._selection_records_for_group(
            ("pdfhir_payer",),
            _dataset_row(),
            {},
        )

    selection_records = snapshot._selection_records(
        (("pdfhir_payer",),),
        {},
        {"pdfhir_payer": _source_row()},
    )
    assert selection_records.pairs == []


@pytest.mark.asyncio
async def test_snapshot_database_queries_and_optional_lock(monkeypatch):
    status = AsyncMock()
    all_rows = AsyncMock(side_effect=[
        [],
        [SimpleNamespace(_mapping={"source_id": "a"})],
        [{"dataset_id": "d"}],
    ])
    monkeypatch.setattr(snapshot.db, "status", status)
    monkeypatch.setattr(snapshot.db, "all", all_rows)

    await snapshot._lock_profile_selection_tables()
    assert await snapshot._selection_source_rows() == [{"source_id": "a"}]
    assert await snapshot._selection_dataset_rows() == [{"dataset_id": "d"}]
    assert status.await_count == 3

    lock = AsyncMock()
    monkeypatch.setattr(snapshot, "_lock_profile_selection_tables", lock)
    monkeypatch.setattr(
        snapshot,
        "_selection_source_rows",
        AsyncMock(return_value=[_source_row()]),
    )
    monkeypatch.setattr(
        snapshot,
        "_selection_dataset_rows",
        AsyncMock(return_value=[_dataset_row()]),
    )
    unlocked = await snapshot._compute_current_selection(
        _catalog(),
        node_id="dev-node",
        lock_selection=False,
    )
    locked = await snapshot._compute_current_selection(
        _catalog(),
        node_id="dev-node",
        lock_selection=True,
    )
    assert unlocked == locked
    lock.assert_awaited_once_with()
