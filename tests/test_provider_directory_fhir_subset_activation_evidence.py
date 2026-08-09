# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only neutral evidence tests for reviewed subset activation."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_evidence as evidence_api
from tests.provider_directory_fhir_subset_activation_support import (
    activation_inputs,
)


class _EvidenceDatabase:
    def __init__(self, source_record, dataset_rows):
        self.source_record = source_record
        self.dataset_rows = dataset_rows
        self.calls = []

    @asynccontextmanager
    async def transaction(self):
        self.calls.append(("transaction", "begin"))
        yield self
        self.calls.append(("transaction", "end"))

    async def status(self, statement, **_parameters):
        self.calls.append(("status", statement))
        return None

    async def all(self, statement, **_parameters):
        self.calls.append(("all", statement))
        if "SELECT source.source_id, source.endpoint_id" in statement:
            return [
                {
                    "source_id": self.source_record["source_id"],
                    "endpoint_id": self.source_record["endpoint_id"],
                    "metadata_json": self.source_record["metadata_json"],
                }
            ]
        if "SELECT source.*" in statement:
            return [self.source_record]
        if "SELECT dataset.*" in statement:
            return self.dataset_rows
        raise AssertionError("unexpected evidence statement")


def test_derived_evidence_revalidates_exact_twins_and_renders_manifest():
    source_record, dataset_rows, expected_evidence = activation_inputs()

    observed_evidence = evidence_api._derived_activation_evidence(
        [source_record],
        dataset_rows,
        source_record["source_id"],
    )

    assert observed_evidence == expected_evidence
    rendered_manifest = (
        evidence_api.reviewed_subset_activation_verified_manifest_json(
            observed_evidence
        )
    )
    assert '"desired_candidate_status":"verified_two_matching_' in (
        rendered_manifest
    )
    assert source_record["source_id"] not in rendered_manifest
    assert source_record["endpoint_id"] not in rendered_manifest
    assert "dataset-baseline" not in rendered_manifest
    assert "root-candidate" not in rendered_manifest


@pytest.mark.parametrize("source_rows", ([], [{}, {}]))
def test_derived_evidence_rejects_ambiguous_source_rows(source_rows):
    _source_record, dataset_rows, _expected_evidence = activation_inputs()
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        evidence_api._derived_activation_evidence(
            source_rows,
            dataset_rows,
            "synthetic-source",
        )
    assert error.value.code == "evidence"


@pytest.mark.parametrize("drift_kind", ("completion", "scope"))
def test_derived_evidence_rejects_root_pair_drift(drift_kind):
    source_record, dataset_rows, _expected_evidence = activation_inputs()
    drifted_rows = deepcopy(dataset_rows)
    if drift_kind == "completion":
        drifted_rows[1]["completion_proof_sha256"] = "f" * 64
    else:
        drifted_rows[1]["publication_metadata_json"][
            "verification_source_scope_hash"
        ] = "f" * 64

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        evidence_api._derived_activation_evidence(
            [source_record],
            drifted_rows,
            source_record["source_id"],
        )
    assert error.value.code == "evidence"


@pytest.mark.parametrize("drift_kind", ("pair", "scope"))
def test_derived_evidence_rejects_post_validation_drift(
    monkeypatch,
    drift_kind,
):
    """Exercise explicit pair and scope equality after proof validation."""

    source_record, dataset_rows, _expected_evidence = activation_inputs()
    importer = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(importer, "_twin_root_baseline_proof", lambda _row: {})
    monkeypatch.setattr(
        importer,
        "_assert_matched_twin_root_dataset_proof",
        lambda _row: None,
    )
    proof_pair = (
        dataset_rows[0]["completion_proof_json"],
        dataset_rows[0]["completion_proof_sha256"],
    )
    if drift_kind == "pair":
        monkeypatch.setattr(
            importer,
            "_validated_parent_subset_completion_pair",
            lambda _row: None,
        )
    else:
        monkeypatch.setattr(
            importer,
            "_validated_parent_subset_completion_pair",
            lambda _row: proof_pair,
        )
        dataset_rows[1]["publication_metadata_json"][
            "verification_source_scope_hash"
        ] = "f" * 64

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        evidence_api._derived_activation_evidence(
            [source_record],
            dataset_rows,
            source_record["source_id"],
        )
    assert error.value.code == "evidence"


@pytest.mark.asyncio
async def test_evidence_reader_uses_one_repeatable_read_only_snapshot(
    monkeypatch,
):
    source_record, dataset_rows, expected_evidence = activation_inputs()
    database = _EvidenceDatabase(source_record, dataset_rows)
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    observed_evidence = await evidence_api.reviewed_subset_activation_evidence(
        database=database
    )

    assert observed_evidence == expected_evidence
    assert database.calls[0] == ("transaction", "begin")
    assert database.calls[1][0] == "status"
    assert "REPEATABLE READ READ ONLY" in database.calls[1][1]
    assert database.calls[-1] == ("transaction", "end")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial_rows",
    (
        [],
        [{"source_id": "synthetic-source", "endpoint_id": None}],
    ),
)
async def test_evidence_reader_rejects_missing_initial_identity(initial_rows):
    database = SimpleNamespace(
        transaction=lambda: _empty_transaction(),
        status=AsyncMock(return_value=None),
        all=AsyncMock(return_value=initial_rows),
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await evidence_api._read_activation_evidence(
            database,
            "synthetic-source",
        )
    assert error.value.code == "evidence"


@asynccontextmanager
async def _empty_transaction():
    yield


@pytest.mark.asyncio
async def test_evidence_api_maps_resolver_and_database_failures(monkeypatch):
    source_record, _dataset_rows, _expected_evidence = activation_inputs()

    def fail_resolution():
        raise RuntimeError("private resolver detail")

    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        fail_resolution,
    )
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await evidence_api.reviewed_subset_activation_evidence(
            database=object()
        )
    assert error.value.code == "evidence"

    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )
    monkeypatch.setattr(
        evidence_api,
        "_read_activation_evidence",
        AsyncMock(side_effect=LookupError("private database detail")),
    )
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await evidence_api.reviewed_subset_activation_evidence(
            database=object()
        )
    assert error.value.code == "state"


@pytest.mark.asyncio
async def test_evidence_api_uses_default_database(monkeypatch):
    source_record, _dataset_rows, expected_evidence = activation_inputs()
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )
    from db import connection as db_connection

    default_database = object()
    monkeypatch.setattr(db_connection, "db", default_database)
    evidence_reader = AsyncMock(return_value=expected_evidence)
    monkeypatch.setattr(
        evidence_api,
        "_read_activation_evidence",
        evidence_reader,
    )

    assert await evidence_api.reviewed_subset_activation_evidence() == (
        expected_evidence
    )
    evidence_reader.assert_awaited_once_with(
        default_database,
        source_record["source_id"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interrupt",
    (
        TimeoutError(),
        asyncio.CancelledError(),
        activation.ReviewedSubsetActivationError("evidence"),
    ),
)
async def test_evidence_api_preserves_control_and_domain_errors(
    monkeypatch,
    interrupt,
):
    source_record, _dataset_rows, _expected_evidence = activation_inputs()
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )
    monkeypatch.setattr(
        evidence_api,
        "_read_activation_evidence",
        AsyncMock(side_effect=interrupt),
    )

    with pytest.raises(type(interrupt)) as error:
        await evidence_api.reviewed_subset_activation_evidence(
            database=object()
        )
    if isinstance(interrupt, activation.ReviewedSubsetActivationError):
        assert error.value.code == "evidence"


def test_verified_manifest_renderer_requires_exact_evidence_type():
    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        evidence_api.reviewed_subset_activation_verified_manifest_json(
            object()
        )
    assert error.value.code == "evidence"
