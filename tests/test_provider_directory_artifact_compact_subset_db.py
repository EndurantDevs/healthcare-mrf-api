# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compact single-root subset admission authority database checks."""

from __future__ import annotations

import copy
import importlib
import json

import pytest

from tests import test_provider_directory_artifact_eligibility_db as shared
from tests.test_provider_directory_artifact_single_root_eligibility_db import (
    _compact_eligible_ids,
    _insert_single_root_candidate,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _sealed_candidate(dataset_row: dict):
    """Return full metadata and its validated compact admission seal."""

    metadata_by_key = copy.deepcopy(dataset_row["publication_metadata_json"])
    completion_pair = (
        dataset_row["completion_proof_json"],
        dataset_row["completion_proof_sha256"],
    )
    metadata_by_key[importer.PROVIDER_DIRECTORY_SUBSET_ADMISSION_SUMMARY_KEY] = (
        importer._subset_admission_summary_projection(
            metadata_by_key,
            *completion_pair,
        )
    )
    seal = importer.admission_seal_from_validated_metadata(
        importer._subset_admission_seal_metadata(
            metadata_by_key,
            completion_pair,
        )
    )
    assert seal is not None
    return metadata_by_key, seal


async def _store_seal(database, schema: str, dataset_id: str, metadata, seal):
    """Persist one writer-produced compact seal on the synthetic candidate."""

    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json = CAST(:metadata AS jsonb),
               publication_metadata_summary_json = CAST(:summary AS jsonb),
               publication_metadata_sha256 =
                   {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
                       CAST(:summary AS jsonb), :version, :kind,
                       :proof_sha256, CAST(:resource_types AS varchar[])
                   ),
               content_proof_admission_version = :version,
               content_proof_admission_kind = :kind,
               content_proof_admission_sha256 = :proof_sha256,
               content_proof_resource_types = CAST(:resource_types AS varchar[])
         WHERE dataset_id = :dataset_id;
        """,
        metadata=json.dumps(metadata),
        summary=json.dumps(seal.metadata_summary),
        version=seal.admission_version,
        kind=seal.admission_kind,
        proof_sha256=seal.proof_sha256,
        resource_types=list(seal.resource_types),
        dataset_id=dataset_id,
    )


async def _replace_summary(database, schema: str, dataset_id: str, seal, summary):
    """Reseal a semantic mutation so selection, not digest drift, rejects it."""

    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET publication_metadata_summary_json = CAST(:summary AS jsonb),
               publication_metadata_sha256 =
                   {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
                       CAST(:summary AS jsonb), :version, :kind,
                       :proof_sha256, CAST(:resource_types AS varchar[])
                   )
         WHERE dataset_id = :dataset_id;
        """,
        summary=json.dumps(summary),
        version=seal.admission_version,
        kind=seal.admission_kind,
        proof_sha256=seal.proof_sha256,
        resource_types=list(seal.resource_types),
        dataset_id=dataset_id,
    )


async def _assert_selected(database, schema: str, dataset_id: str, expected: bool):
    """Require full and compact selectors to make the same decision."""

    expected_ids = [dataset_id] if expected else []
    assert shared._option_ids(
        await shared._artifact_options(database, schema, "endpoint-a")
    ) == expected_ids
    assert await _compact_eligible_ids(
        database,
        schema,
        "endpoint-a",
    ) == expected_ids


def _invalid_summaries(summary: dict):
    """Yield bounded semantic and shape mutations that must fail closed."""

    compact_key = importer.PROVIDER_DIRECTORY_SUBSET_ADMISSION_SUMMARY_KEY
    coverage_key = importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[compact_key]["completion_proof"]["dataset"]["count"] += 1
    yield invalid_summary

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[compact_key]["completion_proof"]["cutoff"] = "invalid"
    yield invalid_summary

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[compact_key]["raw_metadata_sha256"] = None
    yield invalid_summary

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[compact_key]["replay_evidence"]["proof_sha256"] = "invalid"
    yield invalid_summary

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[coverage_key]["resources"] = "invalid"
    yield invalid_summary

    resource_types = tuple(summary[coverage_key]["resources"])
    invalid_summary = copy.deepcopy(summary)
    invalid_summary[coverage_key]["resources"].pop(resource_types[0])
    yield invalid_summary

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[coverage_key]["resources"]["Unexpected"] = {
        "twin_state": "not_required"
    }
    yield invalid_summary

    invalid_summary = copy.deepcopy(summary)
    invalid_summary[coverage_key]["resources"] = {}
    yield invalid_summary


@pytest.mark.asyncio
async def test_sealed_single_root_candidate_uses_compact_subset_authority(
    monkeypatch,
):
    """Select the sealed compact proof while rejecting every bounded drift."""

    async with shared._candidate_database(monkeypatch) as (database, schema):
        dataset_row = await _insert_single_root_candidate(database, schema)
        metadata_by_key, seal = _sealed_candidate(dataset_row)
        dataset_id = dataset_row["dataset_id"]
        await _store_seal(
            database,
            schema,
            dataset_id,
            metadata_by_key,
            seal,
        )
        await _assert_selected(database, schema, dataset_id, True)

        raw_metadata_by_key = copy.deepcopy(metadata_by_key)
        resource_type = next(iter(dataset_row["completion_proof_json"]["resources"]))
        raw_metadata_by_key[
            importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_KEY
        ]["resources"][resource_type]["continuation_hop_sha256"] = []
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = CAST(:metadata AS jsonb),
                   completion_proof_json = '"ignored-after-seal"'::jsonb
             WHERE dataset_id = :dataset_id;
            """,
            metadata=json.dumps(raw_metadata_by_key),
            dataset_id=dataset_id,
        )
        await _assert_selected(database, schema, dataset_id, True)

        for invalid_summary in _invalid_summaries(seal.metadata_summary):
            await _replace_summary(
                database,
                schema,
                dataset_id,
                seal,
                invalid_summary,
            )
            await _assert_selected(database, schema, dataset_id, False)
