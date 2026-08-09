# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for selector-free reviewed subset activation."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_contract as contract
from process import provider_directory_fhir_subset_activation_selection as selection
from process import provider_directory_fhir_subset_activation_store as store
from tests.provider_directory_fhir_subset_activation_support import (
    activation_inputs,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _authorize(monkeypatch, evidence) -> None:
    manifest = activation.ReviewedSubsetActivationManifest(
        activation.VERIFIED_STATUS,
        evidence,
    )
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        activation,
        "reviewed_subset_activation_manifest",
        lambda: manifest,
    )


def _resolve_source(monkeypatch, source_id: str) -> None:
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_id,
    )


@pytest.mark.asyncio
async def test_sync_maps_manual_resolver_failure_to_evidence(monkeypatch):
    """Keep selector resolution failures inside the neutral error contract."""

    source_record, _dataset_rows, evidence = activation_inputs()
    _authorize(monkeypatch, evidence)

    def fail_resolution():
        raise RuntimeError("private resolver detail")

    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        fail_resolution,
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await activation.sync_reviewed_subset_verified_state(database=object())

    assert error.value.code == "evidence"
    assert source_record["source_id"] not in str(error.value)


@pytest.mark.asyncio
async def test_sync_uses_default_database_and_maps_unknown_failure(
    monkeypatch,
):
    """Exercise default storage selection and redact unexpected DB failures."""

    source_record, _dataset_rows, evidence = activation_inputs()
    _authorize(monkeypatch, evidence)
    _resolve_source(monkeypatch, source_record["source_id"])
    from db import connection as db_connection

    expected_result = activation.ReviewedSubsetActivationResult(True)
    transaction_sync = AsyncMock(return_value=expected_result)
    monkeypatch.setattr(activation, "sync_reviewed_subset_transaction", transaction_sync)
    default_database = object()
    monkeypatch.setattr(db_connection, "db", default_database)

    assert await activation.sync_reviewed_subset_verified_state() == expected_result
    transaction_sync.assert_awaited_once_with(
        default_database,
        source_record["source_id"],
        evidence,
    )

    transaction_sync.side_effect = LookupError("private database detail")
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await activation.sync_reviewed_subset_verified_state()
    assert error.value.code == "state"


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt", (TimeoutError(), asyncio.CancelledError()))
async def test_sync_preserves_timeout_and_cancellation(monkeypatch, interrupt):
    """Do not translate task-control exceptions into domain state errors."""

    source_record, _dataset_rows, evidence = activation_inputs()
    _authorize(monkeypatch, evidence)
    _resolve_source(monkeypatch, source_record["source_id"])
    monkeypatch.setattr(
        activation,
        "sync_reviewed_subset_transaction",
        AsyncMock(side_effect=interrupt),
    )

    with pytest.raises(type(interrupt)):
        await activation.sync_reviewed_subset_verified_state(database=object())


def test_result_renderer_rejects_foreign_objects():
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation.reviewed_subset_activation_result_json(object())
    assert error.value.code == "state"


def test_contract_mapping_and_identifier_boundaries(monkeypatch):
    """Accept driver rows and reject malformed cutoff and SQL identifiers."""

    with pytest.raises(ValueError, match="cutoff shape"):
        contract._canonical_cutoff(None)
    driver_row = SimpleNamespace(_mapping={"value": 1})
    assert contract._row_mapping(driver_row) == {"value": 1}
    with pytest.raises(activation.ReviewedSubsetActivationError):
        contract._row_mapping(object())

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-schema")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    with pytest.raises(activation.ReviewedSubsetActivationError):
        contract._quoted_relation("provider_directory_source")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "valid_schema")
    with pytest.raises(activation.ReviewedSubsetActivationError):
        contract._quoted_relation("invalid-table")


def test_source_selector_rejects_partial_importer_identity(monkeypatch):
    """Translate a partial reviewed-source predicate into evidence failure."""

    source_record, _dataset_rows, evidence = activation_inputs()
    monkeypatch.setattr(
        importer,
        "_is_reviewed_subset_source_metadata",
        lambda _metadata: False,
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        selection._activation_source(
            [source_record],
            source_record["source_id"],
            evidence,
        )

    assert error.value.code == "evidence"


@pytest.mark.parametrize(
    "candidate_by_field",
    (
        {"status": "validated", "validated_at": None},
        {"status": "unknown", "validated_at": "2026-08-09T00:01:00Z"},
    ),
)
def test_candidate_lifecycle_rejects_unsealed_or_unknown_state(
    candidate_by_field,
):
    assert selection._is_candidate_lifecycle_valid(candidate_by_field) is False


def test_root_proof_and_metadata_boundaries(monkeypatch):
    """Reject invalid parent summaries and non-object twin proof metadata."""

    _source_record, dataset_rows, evidence = activation_inputs()
    monkeypatch.setattr(importer, "_twin_root_baseline_proof", lambda _row: {})
    monkeypatch.setattr(
        importer,
        "_assert_matched_twin_root_dataset_proof",
        lambda _row: None,
    )
    monkeypatch.setattr(
        importer,
        "_validated_parent_subset_completion_pair",
        lambda _row: None,
    )
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._validated_root_proofs(
            dataset_rows[0],
            dataset_rows[1],
            evidence,
        )

    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._metadata({"publication_metadata_json": None})
    assert selection._root_neutral_proof(None) is None
    assert selection._root_neutral_proof({"proof": None}) is None


def test_replay_and_coverage_digest_boundaries(monkeypatch):
    """Reject missing and noncanonical replay or coverage commitments."""

    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._selection_digest({}, "replay_sha256")
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._selection_coverage_sha256({})

    monkeypatch.setattr(
        selection,
        "canonical_sha256",
        lambda _value: (_ for _ in ()).throw(ValueError("invalid coverage")),
    )
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._selection_coverage_sha256(
            {"server_issued_subset_coverage": {"closed": True}}
        )


def test_selection_rejects_invalid_source_status_shape():
    """Treat an unhashable JSON status as evidence drift, not a raw error."""

    _source_record, _dataset_rows, evidence = activation_inputs()
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        selection._selection_from_roots(
            {
                "source_id": "synthetic-source",
                "metadata_json": {
                    "provider_directory_candidate_status": [],
                },
            },
            {},
            {},
            {},
            {},
            evidence,
            "endpoint-a",
            evidence.source_contract_sha256,
        )
    assert error.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_rows",
    ([], [{"source_id": "other", "endpoint_id": "endpoint-a"}]),
)
async def test_initial_source_record_requires_one_exact_identity(source_rows):
    database = SimpleNamespace(all=AsyncMock(return_value=source_rows))
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await store._initial_source_record(database, "synthetic-source")
    assert error.value.code == "state"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint_rows",
    ([], [{"endpoint_id": "other-endpoint"}]),
)
async def test_endpoint_lock_requires_one_exact_identity(endpoint_rows):
    database = SimpleNamespace(all=AsyncMock(return_value=endpoint_rows))
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await store._lock_activation_api_endpoint(database, "endpoint-a")
    assert error.value.code == "state"


@pytest.mark.parametrize("metadata", (None, {}))
def test_activation_campaign_requires_object_and_text(metadata):
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        store._activation_campaign_id({"metadata_json": metadata})
    assert error.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize("source_metadata", (None, {"unrelated": True}))
async def test_source_activation_rejects_invalid_prior_state(source_metadata):
    source_record, dataset_rows, evidence = activation_inputs()
    selected = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    database = SimpleNamespace(status=AsyncMock(return_value=1))

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await store._activate_source(
            database,
            selection=selected,
            source_row={"metadata_json": source_metadata},
        )

    assert error.value.code == "state"
    database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_activation_requires_one_cas_update():
    source_record, dataset_rows, evidence = activation_inputs()
    selected = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    database = SimpleNamespace(status=AsyncMock(return_value=0))

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await store._activate_source(
            database,
            selection=selected,
            source_row=source_record,
        )
    assert error.value.code == "state"


class _TransactionDatabase:
    @asynccontextmanager
    async def transaction(self):
        yield self

    async def scalar(self, _statement):
        return "read committed"


@pytest.mark.asyncio
async def test_transaction_rejects_multiple_locked_source_aliases(monkeypatch):
    """Fail before proof selection when the locked endpoint has two aliases."""

    source_record, _dataset_rows, evidence = activation_inputs()
    monkeypatch.setattr(
        store,
        "_initial_source_record",
        AsyncMock(return_value=source_record),
    )
    for helper_name in (
        "_lock_activation_endpoint",
        "_lock_activation_api_endpoint",
        "_lock_activation_source_table",
    ):
        monkeypatch.setattr(store, helper_name, AsyncMock())
    monkeypatch.setattr(
        store,
        "_locked_source_rows",
        AsyncMock(return_value=(source_record, deepcopy(source_record))),
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await store.sync_reviewed_subset_transaction(
            _TransactionDatabase(),
            source_record["source_id"],
            evidence,
        )

    assert error.value.code == "evidence"
