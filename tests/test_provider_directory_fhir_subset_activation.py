# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-contract tests for reviewed subset activation desired state."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from copy import deepcopy

import pytest

from process import provider_directory_fhir_subset_activation as activation
from tests.provider_directory_fhir_subset_activation_support import (
    activation_inputs as _activation_inputs,
)
from tests.provider_directory_subset_completion_pg_support import (
    CUTOFF,
    valid_source_record,
)


def _verified_manifest() -> dict:
    return {
        "schema_version": 1,
        "importer": "provider-directory-fhir",
        "operation": "reviewed-subset-source-state-sync",
        "desired_candidate_status": activation.VERIFIED_STATUS,
        "evidence": {
            "source_contract_sha256": "1" * 64,
            "cutoff": "2026-08-09T00:00:00.000000Z",
            "verification_source_scope_sha256": "2" * 64,
            "completion_proof_sha256": "3" * 64,
        },
    }


def _write_manifest(tmp_path, manifest_by_field):
    manifest_path = tmp_path / "activation.json"
    manifest_path.write_text(
        json.dumps(manifest_by_field, sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


def _manifest_for_evidence(evidence):
    manifest_by_field = _verified_manifest()
    manifest_by_field["evidence"] = evidence.evidence_document()
    return manifest_by_field


def _authorize_sync(monkeypatch, tmp_path, evidence):
    manifest = activation.reviewed_subset_activation_manifest(
        _write_manifest(tmp_path, _manifest_for_evidence(evidence))
    )
    monkeypatch.setattr(
        activation,
        "reviewed_subset_activation_manifest",
        lambda: manifest,
    )


class _ActivationDatabase:
    def __init__(self, source_record, dataset_rows):
        self.source_record = source_record
        self.dataset_rows = dataset_rows
        self.calls = []
        self.isolation = "read committed"
        self.lock_acquired = True
        self.updated_count = 1

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin", {}))
        try:
            yield self
        finally:
            self.calls.append(("transaction", "end", {}))

    async def scalar(self, statement, **parameters):
        self.calls.append(("scalar", statement, parameters))
        if "transaction_isolation" in statement:
            return self.isolation
        if "pg_try_advisory_xact_lock" in statement:
            return self.lock_acquired
        raise AssertionError("unexpected scalar statement")

    async def all(self, statement, **parameters):
        self.calls.append(("all", statement, parameters))
        if "SELECT source.source_id, source.endpoint_id" in statement:
            return [
                {
                    "source_id": self.source_record["source_id"],
                    "endpoint_id": self.source_record["endpoint_id"],
                    "metadata_json": self.source_record["metadata_json"],
                }
            ]
        if "provider_directory_api_endpoint" in statement:
            return [{"endpoint_id": self.source_record["endpoint_id"]}]
        if "SELECT source.*" in statement:
            return [self.source_record]
        if "SELECT dataset.*" in statement:
            return self.dataset_rows
        raise AssertionError("unexpected all statement")

    async def status(self, statement, **parameters):
        self.calls.append(("status", statement, parameters))
        if "LOCK TABLE" in statement:
            return None
        if "UPDATE" in statement:
            return self.updated_count
        if "SET CONSTRAINTS" in statement:
            return None
        raise AssertionError("unexpected status statement")


def test_checked_in_manifest_is_valid_and_operator_is_default_off(
    monkeypatch,
):
    manifest = activation.reviewed_subset_activation_manifest()

    assert manifest.desired_candidate_status in {
        activation.PENDING_STATUS,
        activation.VERIFIED_STATUS,
    }
    assert (manifest.evidence is not None) is manifest.is_verified
    if manifest.is_verified:
        assert manifest.require_verified_evidence() is manifest.evidence
    else:
        with pytest.raises(activation.ReviewedSubsetActivationError) as error:
            manifest.require_verified_evidence()
        assert error.value.code == "disabled"

    monkeypatch.delenv(activation.STATE_SYNC_ENABLED_ENV, raising=False)
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation.require_reviewed_subset_state_sync_gate()
    assert error.value.code == "disabled"


def test_state_sync_gate_requires_exact_lowercase_true(monkeypatch):
    for disabled_value in (None, "", "1", "TRUE", "yes"):
        if disabled_value is None:
            monkeypatch.delenv(activation.STATE_SYNC_ENABLED_ENV, raising=False)
        else:
            monkeypatch.setenv(
                activation.STATE_SYNC_ENABLED_ENV,
                disabled_value,
            )
        with pytest.raises(activation.ReviewedSubsetActivationError) as error:
            activation.require_reviewed_subset_state_sync_gate()
        assert error.value.code == "disabled"

    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    activation.require_reviewed_subset_state_sync_gate()


def test_verified_manifest_returns_exact_neutral_evidence(tmp_path):
    manifest = activation.reviewed_subset_activation_manifest(
        _write_manifest(tmp_path, _verified_manifest())
    )

    evidence = manifest.require_verified_evidence()
    assert manifest.is_verified is True
    assert evidence.evidence_document() == {
        "source_contract_sha256": "1" * 64,
        "cutoff": "2026-08-09T00:00:00.000000Z",
        "verification_source_scope_sha256": "2" * 64,
        "completion_proof_sha256": "3" * 64,
    }


def test_activation_source_contract_excludes_mutable_state_and_marker():
    source_record = valid_source_record(activation.PENDING_STATUS)
    expected_sha256 = activation.reviewed_subset_source_contract_sha256(
        source_record
    )
    activated_source_record = deepcopy(source_record)
    activated_metadata = activated_source_record["metadata_json"]
    activated_metadata["provider_directory_candidate_status"] = (
        activation.VERIFIED_STATUS
    )
    activated_metadata[activation.ACTIVATION_METADATA_KEY] = {
        "private": "database-only"
    }

    assert activation.reviewed_subset_source_contract_sha256(
        activated_source_record
    ) == expected_sha256

    activated_metadata[
        "provider_directory_current_version_census_page_count"
    ] += 1
    assert activation.reviewed_subset_source_contract_sha256(
        activated_source_record
    ) != expected_sha256


def test_exact_matching_twins_build_closed_private_activation_marker():
    source_record, dataset_rows, evidence = _activation_inputs()

    selection = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )

    marker_by_field = selection.metadata_marker()
    assert set(marker_by_field) == {
        "contract_version",
        "source_contract_sha256",
        "cutoff",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
        "source_id",
        "endpoint_id",
        "verification_campaign_id",
        "baseline",
        "candidate",
    }
    assert set(marker_by_field["baseline"]) == {
        "dataset_id",
        "acquisition_root_run_id",
        "replay_evidence_sha256",
        "coverage_sha256",
    }
    assert set(marker_by_field["candidate"]) == set(
        marker_by_field["baseline"]
    )
    assert marker_by_field["cutoff"] == CUTOFF
    assert marker_by_field["completion_proof_sha256"] == (
        evidence.completion_proof_sha256
    )


@pytest.mark.asyncio
async def test_selector_free_sync_locks_and_activates_exact_pending_state(
    monkeypatch,
    tmp_path,
):
    source_record, dataset_rows, evidence = _activation_inputs()
    database = _ActivationDatabase(source_record, dataset_rows)
    _authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    activation_result = await activation.sync_reviewed_subset_verified_state(
        database=database,
    )

    assert activation_result == activation.ReviewedSubsetActivationResult(
        activated=True
    )
    assert activation.reviewed_subset_activation_result_json(
        activation_result
    ) == (
        '{"activated":true,"already_applied":false,"status":"ok"}'
    )
    statements = [call[1] for call in database.calls]
    assert statements[0] == "begin"
    assert "transaction_isolation" in statements[1]
    assert "SELECT source.source_id, source.endpoint_id" in statements[2]
    assert "pg_try_advisory_xact_lock" in statements[3]
    assert "provider_directory_api_endpoint" in statements[4]
    assert "FOR UPDATE OF endpoint" in statements[4]
    assert "LOCK TABLE" in statements[5]
    assert "IN SHARE MODE" in statements[5]
    assert "SELECT source.*" in statements[6]
    assert "FOR UPDATE OF source" in statements[6]
    assert "SELECT dataset.*" in statements[7]
    assert "FOR UPDATE OF dataset" in statements[7]
    assert "UPDATE" in statements[8]
    assert "SET CONSTRAINTS" in statements[9]
    update_parameters = database.calls[8][2]
    marker_by_field = json.loads(update_parameters["activation_marker"])
    assert marker_by_field["completion_proof_sha256"] == (
        evidence.completion_proof_sha256
    )
    assert source_record["source_id"] not in (
        activation.reviewed_subset_activation_result_json(activation_result)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "candidate_status",
    ("validated", "published", "superseded"),
)
async def test_selector_free_sync_accepts_exact_idempotent_verified_state(
    monkeypatch,
    tmp_path,
    candidate_status,
):
    source_record, dataset_rows, evidence = _activation_inputs()
    selection = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    source_record["metadata_json"][
        "provider_directory_candidate_status"
    ] = activation.VERIFIED_STATUS
    source_record["metadata_json"][activation.ACTIVATION_METADATA_KEY] = (
        selection.metadata_marker()
    )
    if candidate_status != "validated":
        dataset_rows[1]["status"] = candidate_status
        dataset_rows[1]["published_at"] = "2026-08-09T00:02:00Z"
    if candidate_status == "published":
        dataset_rows[1]["is_current"] = True
    elif candidate_status == "superseded":
        dataset_rows[1]["superseded_at"] = "2026-08-09T00:03:00Z"
    database = _ActivationDatabase(source_record, dataset_rows)
    _authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    activation_result = await activation.sync_reviewed_subset_verified_state(
        database=database,
    )

    assert activation_result.is_already_applied is True
    assert not any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in database.calls
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("isolation", "lock_acquired", "error_code"),
    (
        ("repeatable read", True, "state"),
        ("read committed", False, "busy"),
    ),
)
async def test_selector_free_sync_fails_closed_before_row_mutation(
    monkeypatch,
    tmp_path,
    isolation,
    lock_acquired,
    error_code,
):
    source_record, dataset_rows, evidence = _activation_inputs()
    database = _ActivationDatabase(source_record, dataset_rows)
    database.isolation = isolation
    database.lock_acquired = lock_acquired
    _authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await activation.sync_reviewed_subset_verified_state(
            database=database,
        )

    assert error.value.code == error_code
    assert not any(call[0] == "status" for call in database.calls)


@pytest.mark.parametrize(
    "mutation",
    (
        lambda source_rows, dataset_rows, evidence: source_rows.append(
            deepcopy(source_rows[0])
        ),
        lambda source_rows, dataset_rows, evidence: dataset_rows.pop(),
        lambda source_rows, dataset_rows, evidence: dataset_rows.append(
            deepcopy(dataset_rows[1])
        ),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1].update(
            status="verification_mismatch"
        ),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1].update(
            is_current=True
        ),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1].update(
            acquisition_root_run_id="root-baseline"
        ),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1].update(
            completion_proof_sha256="f" * 64
        ),
        lambda source_rows, dataset_rows, evidence: source_rows[0][
            "metadata_json"
        ].update(provider_directory_verification_campaign_id="drift"),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1][
            "publication_metadata_json"
        ].update(verification_baseline_dataset_id="other"),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1][
            "publication_metadata_json"
        ]["twin_root_verification_v1"].update(
            baseline_acquisition_root_run_id="other"
        ),
        lambda source_rows, dataset_rows, evidence: dataset_rows[1][
            "publication_metadata_json"
        ]["twin_root_verification_v1"]["proof"].update(
            dataset_hash="f" * 64
        ),
        lambda source_rows, dataset_rows, evidence: source_rows[0].update(
            requires_api_key=True
        ),
    ),
)
def test_activation_selection_rejects_ambiguous_or_drifting_evidence(
    mutation,
):
    source_record, dataset_rows, evidence = _activation_inputs()
    source_rows = [source_record]
    mutation(source_rows, dataset_rows, evidence)

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation.validated_reviewed_subset_activation_selection(
            source_rows=source_rows,
            dataset_rows=dataset_rows,
            expected_source_id=source_record["source_id"],
            evidence=evidence,
        )

    assert error.value.code == "evidence"


def test_activation_result_rejects_non_boolean_state():
    with pytest.raises(ValueError):
        activation.ReviewedSubsetActivationResult(activated=1)


def test_schema_selection_rejects_conflicting_runtime_names(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation._quoted_relation("provider_directory_source")

    assert error.value.code == "state"


@pytest.mark.parametrize(
    "mutation",
    (
        lambda manifest: manifest.update(extra=True),
        lambda manifest: manifest.update(schema_version=True),
        lambda manifest: manifest.update(importer="other"),
        lambda manifest: manifest.update(operation="other"),
        lambda manifest: manifest.update(desired_candidate_status="other"),
        lambda manifest: manifest.update(evidence=None),
        lambda manifest: manifest["evidence"].update(extra=True),
        lambda manifest: manifest["evidence"].update(
            source_contract_sha256="A" * 64
        ),
        lambda manifest: manifest["evidence"].update(
            cutoff="2026-08-09T00:00:00Z"
        ),
        lambda manifest: manifest["evidence"].update(
            verification_source_scope_sha256=True
        ),
        lambda manifest: manifest["evidence"].update(
            completion_proof_sha256="3" * 63
        ),
    ),
)
def test_manifest_rejects_open_or_noncanonical_shapes(tmp_path, mutation):
    manifest_by_field = _verified_manifest()
    mutation(manifest_by_field)

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation.reviewed_subset_activation_manifest(
            _write_manifest(tmp_path, manifest_by_field)
        )

    assert error.value.code == "manifest"
