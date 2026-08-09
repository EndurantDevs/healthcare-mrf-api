# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundaries for configured Provider Directory identity."""

from __future__ import annotations

import copy

import pytest

from process import provider_directory_fhir_subset_abandonment_selection as abandonment_selection
from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_evidence as evidence_api
from process import provider_directory_fhir_subset_activation_selection as selection_api
from process import provider_directory_fhir_subset_activation_store as activation_store
from process import provider_directory_fhir_subset_identity as subset_identity
from tests.test_provider_directory_effective_endpoint_runtime import (
    _ConfiguredActivationDatabase,
    _split_endpoint_inputs,
)


class _LockedSourceDriftDatabase(_ConfiguredActivationDatabase):
    def __init__(self, source_record, dataset_rows, locked_source_mutation):
        super().__init__(source_record, dataset_rows)
        self.locked_source_mutation = locked_source_mutation

    async def all(self, statement, **parameters):
        source_rows = await super().all(statement, **parameters)
        if "SELECT source.*" not in statement:
            return source_rows
        locked_source = copy.deepcopy(source_rows[0])
        self.locked_source_mutation(locked_source)
        return [locked_source]


def test_effective_endpoint_consumers_reject_missing_identity():
    source_record, _, evidence = _split_endpoint_inputs()
    source_record["endpoint_id"] = None

    assert abandonment_selection._configured_endpoint_id(source_record) is None
    with pytest.raises(
        activation.ReviewedSubsetActivationError,
        match="evidence",
    ):
        selection_api._activation_source(
            [source_record],
            source_record["source_id"],
            evidence,
        )


@pytest.mark.asyncio
async def test_evidence_identity_requires_campaign():
    source_record, _, _ = _split_endpoint_inputs()
    source_record["metadata_json"].pop(
        "provider_directory_verification_campaign_id"
    )

    class _InitialEvidenceDatabase:
        async def all(self, _statement, **_parameters):
            return [source_record]

    with pytest.raises(
        activation.ReviewedSubsetActivationError,
        match="evidence",
    ):
        await evidence_api._initial_evidence_identity(
            _InitialEvidenceDatabase(),
            source_record["source_id"],
        )


@pytest.mark.asyncio
async def test_activation_store_rejects_identity_and_status_drift():
    source_record, dataset_rows, evidence = _split_endpoint_inputs()
    database = _ConfiguredActivationDatabase(source_record, dataset_rows)
    with pytest.raises(
        activation.ReviewedSubsetActivationError,
        match="state",
    ):
        await activation_store._initial_source_record(
            database,
            "synthetic-unexpected-source",
        )
    selection = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    configured_drift = copy.deepcopy(source_record)
    configured_drift["metadata_json"][
        subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD
    ] = "endpoint-other"
    with pytest.raises(
        activation.ReviewedSubsetActivationError,
        match="state",
    ):
        activation_store._activation_endpoint_identity(
            selection,
            configured_drift,
        )
    status_drift = copy.deepcopy(source_record)
    status_drift["metadata_json"][
        "provider_directory_candidate_status"
    ] = "unexpected"
    with pytest.raises(
        activation.ReviewedSubsetActivationError,
        match="state",
    ):
        await activation_store._activate_source(
            object(),
            selection=selection,
            source_row=status_drift,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "locked_source_mutation",
    (
        lambda source_record: source_record.update(endpoint_id=None),
        lambda source_record: source_record.update(
            endpoint_id="endpoint-serving-drift"
        ),
    ),
)
async def test_activation_store_rejects_locked_endpoint_drift(
    locked_source_mutation,
):
    source_record, dataset_rows, evidence = _split_endpoint_inputs()
    database = _LockedSourceDriftDatabase(
        source_record,
        dataset_rows,
        locked_source_mutation,
    )

    with pytest.raises(
        activation.ReviewedSubsetActivationError,
        match="evidence",
    ):
        await activation_store.sync_reviewed_subset_transaction(
            database,
            source_record["source_id"],
            evidence,
        )
