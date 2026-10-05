# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Candidate sets reject malformed normalized output before serving cutover."""

import datetime
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from process import provider_directory_rooted_graph_publication_materialization as rooted_materialization
from process import uhc_flex_practitioner_materialization as normalizer
from process import uhc_flex_practitioner_publication_materialization as materialization
from process.uhc_flex_practitioner_store_contract import UHCFlexPractitionerResourceRow
from tests.test_provider_directory_rooted_graph_publication_postgres import (
    _complete_success,
    _identity,
    _lifecycle_scope,
    _locked_exact_current,
    _publish_legacy_root,
    admit_provider_directory_rooted_graph_twins,
    publish_provider_directory_rooted_graph_dataset,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    ENDPOINT_ID,
    _publication_test_scope,
    _sealed_pair,
    publication,
)
from tests.test_uhc_flex_practitioner_materialization import (
    DATASET_ID,
    PROJECTION_AS_OF,
    REQUESTED_NPI,
    RUN_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
    _practitioner,
)


@pytest.mark.parametrize("number", [1, 1.0, -0.0, 1e-7, 1e21])
def test_admitted_normalization_preserves_external_canonical_encoding(number):
    payload = _practitioner("synthetic", meta={"lastUpdated": "2026-08-09T00:00:00Z", "number": number})
    payload["name"][0]["text"] = "Dr Élodie 😀 Example"
    encoded = normalizer._canonical_json(payload)
    source_hash = hashlib.sha256(encoded.encode()).hexdigest()
    stored = UHCFlexPractitionerResourceRow(REQUESTED_NPI, "synthetic", source_hash, encoded)
    external = normalizer.materialize_uhc_flex_practitioner_stored_resource(
        stored,
        dataset_id=DATASET_ID,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        run_id=RUN_ID,
        semantic_projection_as_of=PROJECTION_AS_OF,
    )
    admitted = normalizer._materialize_admitted_practitioner_resource(
        {"npi": REQUESTED_NPI, "resource_id": "synthetic", "payload_sha256": source_hash, "payload_json_text": encoded},
        dataset_id=DATASET_ID,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        run_id=RUN_ID,
        projection_date=datetime.date.fromisoformat(PROJECTION_AS_OF),
    )
    assert admitted.pop("requested_npi") == REQUESTED_NPI
    assert normalizer._canonical_json(admitted) == normalizer._canonical_json(external.dataset_resource)


def _tampered_copy(copier, field_name, value, candidate_names):
    async def copy_rows(database, table, columns, records):
        candidate_names.add(publication._table(table))
        is_targeted = table == (
            publication._PROVENANCE if field_name == "requested_npi" else publication._DATASET_RESOURCE
        )
        if is_targeted:
            records = [
                tuple(value if name == field_name else item for name, item in zip(columns, record, strict=True))
                for record in records
            ]
        return await copier(database, table, columns, records)

    return copy_rows


async def _assert_invalid_candidate(database, admission, monkeypatch, field_name, value):
    copier = materialization.copy_dataset_candidate_rows
    candidate_names = set()
    with monkeypatch.context() as patch:
        patch.setattr(
            materialization, "copy_dataset_candidate_rows", _tampered_copy(copier, field_name, value, candidate_names)
        )
        with pytest.raises(DBAPIError) as failure:
            await publication.publish_uhc_flex_practitioner_dataset(
                admission.candidate_acquisition_id,
                database=database,
                batch_size=1,
            )
        assert failure.value.orig.sqlstate == "23514"
    for candidate_name in candidate_names:
        assert await database.scalar("SELECT to_regclass(:relation)", relation=candidate_name) is None


@pytest.mark.asyncio
async def test_candidate_shape_identity_and_npi_are_checked_as_a_set(monkeypatch):
    async with _publication_test_scope(monkeypatch) as (_, _, database, *_):
        monkeypatch.setattr(
            publication,
            "register_uhc_flex_practitioner_source",
            AsyncMock(return_value=SimpleNamespace(endpoint_id=ENDPOINT_ID)),
        )
        admission = await _sealed_pair(database, operation_key="8" * 64, matched=True)
        invalid_fields = (
            ("resource_type", "PractitionerX"),
            ("resource_id", "different"),
            ("payload_hash", "bad"),
            ("payload_json", "[]"),
            ("acquired_resource_sha256", "a" * 64),
            ("requested_npi", 1000000001),
        )
        for field_name, value in invalid_fields:
            await _assert_invalid_candidate(database, admission, monkeypatch, field_name, value)
        published = await publication.publish_uhc_flex_practitioner_dataset(
            admission.candidate_acquisition_id,
            database=database,
            batch_size=1,
        )
        assert published.readiness.resource_count == 1


@pytest.mark.asyncio
async def test_content_commitment_still_rejects_payload_tamper_after_jsonb_storage(monkeypatch):
    async with _publication_test_scope(monkeypatch) as (_, _, database, *_):
        monkeypatch.setattr(
            publication,
            "register_uhc_flex_practitioner_source",
            AsyncMock(return_value=SimpleNamespace(endpoint_id=ENDPOINT_ID)),
        )
        admission = await _sealed_pair(database, operation_key="9" * 64, matched=True)
        copier = materialization.copy_dataset_candidate_rows

        async def tamper_payload(database, table, columns, records):
            if table == publication._DATASET_RESOURCE:
                payload_index = columns.index("payload_json")
                changed_records = []
                for record in records:
                    payload = json.loads(record[payload_index])
                    payload["gender"] = "synthetic-change"
                    changed_values = list(record)
                    changed_values[payload_index] = json.dumps(payload)
                    changed_records.append(tuple(changed_values))
                records = changed_records
            return await copier(database, table, columns, records)

        monkeypatch.setattr(materialization, "copy_dataset_candidate_rows", tamper_payload)
        with pytest.raises(publication.UHCFlexPractitionerPublicationError, match="content is invalid"):
            await publication.publish_uhc_flex_practitioner_dataset(
                admission.candidate_acquisition_id,
                database=database,
                batch_size=1,
            )


async def _admitted_rooted_candidate(database, current):
    """Seal and admit a complete rooted pair against the retained current root."""
    baseline = _identity(current, "baseline", "1", "3")
    candidate = _identity(current, "candidate", "2", "3")
    await _complete_success(database, baseline)
    await _complete_success(database, candidate)
    return await admit_provider_directory_rooted_graph_twins(
        baseline.acquisition_id, candidate.acquisition_id, database=database
    )


async def _candidate_relations(context):
    """Capture physical publication candidates, including the incumbent snapshot."""
    return await context.connection.fetch(
        "SELECT oid FROM pg_class WHERE relnamespace=$1::regnamespace AND relname LIKE 'pd_ds_%' ORDER BY oid",
        context.schema_name,
    )


@pytest.mark.asyncio
async def test_rooted_normalizer_identity_drift_fails_set_proof_and_rolls_back(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        admission = await _admitted_rooted_candidate(context.database, current)
        original_relations = await _candidate_relations(context)
        normalize = rooted_materialization.materialize_provider_directory_dataset_fhir_resource

        def drift_identity(**keywords):
            resource = normalize(**keywords)
            if resource["resource_type"] == "Organization":
                return {**resource, "resource_type": "Location"}
            return resource

        with monkeypatch.context() as patch:
            patch.setattr(
                rooted_materialization, "materialize_provider_directory_dataset_fhir_resource", drift_identity
            )
            with pytest.raises(DBAPIError, match="provider_directory_dataset_candidate_content") as failure:
                await publish_provider_directory_rooted_graph_dataset(
                    admission.publication_acquisition_id, database=context.database, batch_size=4
                )
            assert failure.value.orig.sqlstate == "23514"
        assert await _candidate_relations(context) == original_relations
        assert await _locked_exact_current(context.database) == current
        assert (
            await context.database.scalar(
                f"SELECT count(*) FROM {context.schema}.provider_directory_rooted_graph_dataset"
            )
            == 0
        )
        published = await publish_provider_directory_rooted_graph_dataset(
            admission.publication_acquisition_id, database=context.database, batch_size=4
        )
        assert published.readiness.previous_dataset_id == current.dataset_id
        assert published.readiness.resource_counts["Organization"] == 1
