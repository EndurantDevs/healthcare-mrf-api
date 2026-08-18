# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime proof for configured Provider Directory endpoint identity."""

from __future__ import annotations

from contextlib import asynccontextmanager
import copy
import json
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_evidence as evidence_api
from process import provider_directory_fhir_subset_identity as subset_identity
from tests.provider_directory_fhir_subset_activation_support import (
    activation_inputs,
)
from tests.test_provider_directory_artifact_verification_contract import (
    _selected_dataset,
)
from tests.test_provider_directory_fhir_subset_activation import (
    _ActivationDatabase,
    _authorize_sync,
)
from tests.test_provider_directory_fhir_subset_activation_evidence import (
    _EvidenceDatabase,
)
from tests.test_provider_directory_fhir_subset_runtime_boundaries import (
    CUTOFF,
    _reviewed_source_record,
    importer,
)
from tests.test_provider_directory_trust_boundaries import (
    _artifact_stage,
    _promotion_dataset,
)


class _ConfiguredActivationDatabase(_ActivationDatabase):
    async def all(self, statement, **parameters):
        if "provider_directory_api_endpoint" not in statement:
            return await super().all(statement, **parameters)
        self.calls.append(("all", statement, parameters))
        configured_endpoint_id = self.source_record["metadata_json"][
            subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD
        ]
        return [{"endpoint_id": configured_endpoint_id}]


class _ConfiguredEvidenceDatabase(_EvidenceDatabase):
    def __init__(self, source_record, dataset_rows):
        super().__init__(source_record, dataset_rows)
        self.endpoint_parameters = []

    async def all(self, statement, **parameters):
        if "endpoint_id" in parameters:
            self.endpoint_parameters.append(parameters["endpoint_id"])
        return await super().all(statement, **parameters)


def _split_endpoint_inputs():
    source_record, dataset_rows, evidence = activation_inputs()
    source_record["endpoint_id"] = "endpoint-serving"
    return source_record, dataset_rows, evidence


def _authorize_activation(monkeypatch, tmp_path, source_record, evidence):
    _authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )


def test_v1_identities_project_configured_endpoint_across_serving_cutover():
    persisted_source = _reviewed_source_record()
    persisted_source["endpoint_id"] = "synthetic-serving-endpoint"
    source_record = importer._artifact_source_with_subset_contract(
        persisted_source,
        CUTOFF,
    )
    metadata = source_record["metadata_json"]
    configured_endpoint_id = metadata[
        subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD
    ]
    source_ids = (source_record["source_id"],)
    activation_payload = (
        subset_identity.subset_activation_source_contract_payload(source_record)
    )
    scope_payload = subset_identity.server_issued_subset_source_scope_payload(
        source_record,
        source_ids,
        CUTOFF,
        source_record["canonical_api_base"],
    )
    scope_sha256 = importer.subset_canonical_sha256(scope_payload)

    assert source_record["endpoint_id"] != configured_endpoint_id
    assert activation_payload["source"]["endpoint_id"] == configured_endpoint_id
    assert scope_payload["source"]["endpoint_id"] == configured_endpoint_id

    published_source = copy.deepcopy(source_record)
    published_source["endpoint_id"] = configured_endpoint_id
    assert activation.reviewed_subset_source_contract_sha256(
        published_source
    ) == activation.reviewed_subset_source_contract_sha256(source_record)
    assert scope_sha256 == importer._server_issued_subset_source_scope_hash(
        [published_source],
        list(source_ids),
    )
    campaign_id = source_record[
        importer.CURRENT_VERSION_CENSUS_CONTRACT_FIELD
    ].campaign_id
    artifact_contract_by_field = {
        "verification_campaign_id": campaign_id,
        "verification_source_scope_hash": scope_sha256,
        "source_ids": source_ids,
        "completion_proof_cutoff": CUTOFF,
    }
    assert importer._artifact_source_verification_contract(
        source_record,
        **artifact_contract_by_field,
    ) == importer._artifact_source_verification_contract(
        published_source,
        **artifact_contract_by_field,
    )


@pytest.mark.parametrize("missing_identity", ("serving", "configured"))
def test_v1_identities_require_both_endpoint_domains(missing_identity):
    source_record = _reviewed_source_record()
    if missing_identity == "serving":
        source_record["endpoint_id"] = None
    else:
        source_record["metadata_json"].pop(
            subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD
        )

    with pytest.raises(ValueError, match="endpoint_identity_invalid"):
        subset_identity.subset_source_endpoint_identity(source_record)
    with pytest.raises(ValueError, match="activation_source_invalid"):
        subset_identity.subset_activation_source_contract_payload(source_record)
    with pytest.raises(ValueError, match="source_scope_invalid"):
        subset_identity.server_issued_subset_source_scope_payload(
            source_record,
            (source_record["source_id"],),
            CUTOFF,
            source_record["canonical_api_base"],
        )


@pytest.mark.asyncio
async def test_activation_uses_configured_endpoint_and_serving_snapshot_cas(
    monkeypatch,
    tmp_path,
):
    source_record, dataset_rows, evidence = _split_endpoint_inputs()
    database = _ConfiguredActivationDatabase(source_record, dataset_rows)
    _authorize_activation(
        monkeypatch,
        tmp_path,
        source_record,
        evidence,
    )

    activation_result = await activation.sync_reviewed_subset_verified_state(
        database=database
    )

    assert activation_result.activated is True
    advisory_call = next(
        call for call in database.calls if "pg_try_advisory_xact_lock" in call[1]
    )
    endpoint_call = next(
        call for call in database.calls if "provider_directory_api_endpoint" in call[1]
    )
    dataset_call = next(
        call for call in database.calls if "SELECT dataset.*" in call[1]
    )
    update_call = next(
        call
        for call in database.calls
        if call[0] == "status" and "UPDATE" in call[1]
    )
    assert advisory_call[2]["endpoint_id"] == "endpoint-a"
    assert endpoint_call[2]["endpoint_id"] == "endpoint-a"
    assert dataset_call[2]["endpoint_id"] == "endpoint-a"
    assert update_call[2]["serving_endpoint_id"] == "endpoint-serving"
    assert update_call[2]["configured_endpoint_id"] == "endpoint-a"
    assert "->> :configured_endpoint_key" in update_call[1]
    marker = json.loads(update_call[2]["activation_marker"])
    assert marker["endpoint_id"] == "endpoint-a"


@pytest.mark.asyncio
async def test_activation_replay_is_idempotent_after_serving_alias_cutover(
    monkeypatch,
    tmp_path,
):
    source_record, dataset_rows, evidence = _split_endpoint_inputs()
    selection = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    source_metadata = source_record["metadata_json"]
    source_metadata["provider_directory_candidate_status"] = (
        activation.VERIFIED_STATUS
    )
    source_metadata[activation.ACTIVATION_METADATA_KEY] = (
        selection.metadata_marker()
    )
    source_record["endpoint_id"] = "endpoint-a"
    dataset_rows[1].update(
        status="published",
        is_current=True,
        published_at="2026-08-09T00:02:00Z",
    )
    database = _ConfiguredActivationDatabase(source_record, dataset_rows)
    _authorize_activation(
        monkeypatch,
        tmp_path,
        source_record,
        evidence,
    )

    activation_result = await activation.sync_reviewed_subset_verified_state(
        database=database
    )

    assert activation_result.is_already_applied is True
    assert not any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in database.calls
    )


@pytest.mark.asyncio
async def test_evidence_reader_uses_configured_endpoint_from_serving_snapshot(
    monkeypatch,
):
    source_record, dataset_rows, expected_evidence = _split_endpoint_inputs()
    database = _ConfiguredEvidenceDatabase(source_record, dataset_rows)
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    observed_evidence = await evidence_api.reviewed_subset_activation_evidence(
        database=database
    )

    assert observed_evidence == expected_evidence
    assert database.endpoint_parameters == ["endpoint-a", "endpoint-a"]


@pytest.mark.asyncio
async def test_reviewed_artifact_alias_cutover_requires_configured_cas(
    monkeypatch,
):
    reviewed_dataset = importer.replace(
        _selected_dataset(),
        completion_proof_required_version=3,
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    relation_token = (
        importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
            {"provider_directory_source": "private_source_scope"}
        )
    )
    try:
        await importer._cutover_provider_directory_artifact_sources(
            importer.ProviderDirectoryArtifactDatasetFence(
                (reviewed_dataset,)
            )
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(
            relation_token
        )

    statement = status.await_args.args[0]
    assert '"mrf"."provider_directory_source"' in statement
    assert '"mrf"."private_source_scope"' not in statement
    assert subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD in statement
    assert status.await_args.kwargs["endpoint_id"] == "candidate_endpoint"
    assert status.await_args.kwargs["serving_endpoint_id"] == (
        "serving_endpoint_old"
    )

    status.reset_mock()
    ordinary_dataset = _promotion_dataset()
    await importer._cutover_provider_directory_artifact_sources(
        importer.ProviderDirectoryArtifactDatasetFence((ordinary_dataset,))
    )
    assert subset_identity.CONFIGURED_ENDPOINT_ID_METADATA_FIELD not in (
        status.await_args.args[0]
    )


@pytest.mark.asyncio
async def test_artifact_alias_cas_failure_stops_before_publication(monkeypatch):
    dataset = _promotion_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    supersede = AsyncMock()
    publish = AsyncMock()
    cutover = AsyncMock(
        side_effect=importer.ProviderDirectoryArtifactBuildStale(
            "provider_directory_source_endpoint_dataset_changed"
        )
    )
    monkeypatch.setattr(
        importer, "_supersede_artifact_dataset_incumbent", supersede
    )
    monkeypatch.setattr(
        importer, "_publish_validated_artifact_dataset", publish
    )
    monkeypatch.setattr(
        importer, "_cutover_provider_directory_artifact_sources", cutover
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_endpoint_dataset_changed",
    ):
        await importer._promote_provider_directory_artifact_datasets(fence)

    supersede.assert_awaited_once_with(dataset)
    cutover.assert_awaited_once_with(fence)
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_atomic_artifact_promotion_failure_rolls_back_transaction(
    monkeypatch,
):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except RuntimeError:
            events.append("rollback")
            raise
        events.append("commit")

    stage = _artifact_stage()
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer,
        "_configure_provider_directory_artifact_promotion",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_lock_provider_directory_artifact_bundle_targets",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_reserve_provider_directory_artifact_cutover_budget",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_apply_locked_provider_directory_artifact_bundle",
        AsyncMock(side_effect=RuntimeError("publication failed")),
    )

    with pytest.raises(RuntimeError, match="publication failed"):
        await importer._promote_provider_directory_artifact_bundle_transaction(
            (stage,)
        )

    assert events == ["begin", "rollback"]
