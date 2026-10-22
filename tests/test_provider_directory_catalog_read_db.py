# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native parity for bounded source-catalog reads and exact attestation."""

import json

import pytest

from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as snapshot
from tests.test_provider_directory_profile_selection_db import (
    _catalog_map,
    _selection_database,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_type", ["jsonb", "json"])
async def test_proposal_keeps_exact_request_without_hashing_ordinary_proof(monkeypatch, metadata_type):
    async with _selection_database(monkeypatch) as (database, schema):
        if metadata_type == "json":
            await database.status(
                f"ALTER TABLE {schema}.provider_directory_endpoint_dataset "
                "ALTER COLUMN publication_metadata_json TYPE json "
                "USING publication_metadata_json::json;"
            )
        metadata = {"source_ids": ["pdfhir_test_payer"], "proof": "x" * 2_000_000}
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(:metadata AS jsonb);",
            metadata=json.dumps(metadata),
        )
        full = await snapshot._compute_current_selection(_catalog_map(), node_id="test-node", lock_selection=False)
        exact_rows = await snapshot._selection_dataset_rows()
        proposal_rows = await snapshot._selection_dataset_rows(exact_readiness=False)
        assert exact_rows[0]["publication_metadata_json"] == metadata
        assert proposal_rows[0]["publication_metadata_json"] == metadata
        original_input = snapshot._profile_input
        input_calls = []

        def tracked_input(*args):
            input_calls.append(args[0]["source_id"])
            return original_input(*args)

        monkeypatch.setattr(snapshot, "_profile_input", tracked_input)
        request = await selection.current_profile_selection_request(_catalog_map())
        assert input_calls == []
        assert request == selection._expected_request(full, "test-node")
        attestation = await selection.attest_profile_selection(request, _catalog_map())
        assert input_calls == ["pdfhir_test_payer"]
        assert attestation["profile_input_digest"] == full.identity_payload["profile_input_digest"]
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = publication_metadata_json::jsonb "
            "|| CAST(:changed AS jsonb);",
            changed=json.dumps({"changed": True}),
        )
        changed = await selection.attest_profile_selection(request, _catalog_map())
        assert changed["profile_input_digest"] != attestation["profile_input_digest"]
        assert changed["authority_revision"] > attestation["authority_revision"]


@pytest.mark.asyncio
async def test_proposal_preserves_dedicated_and_malformed_metadata(monkeypatch):
    async with _selection_database(monkeypatch) as (database, schema):
        metadata = {"source_ids": ["pdfhir_test_payer"], "proof": {"complete": True}}
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(:metadata AS jsonb);",
            metadata=json.dumps(metadata),
        )
        await database.status(
            f"INSERT INTO {schema}.provider_directory_rooted_graph_dataset (dataset_id) VALUES ('dataset-1');"
        )
        rows = await snapshot._selection_dataset_rows(exact_readiness=False)
        assert rows[0]["publication_metadata_json"] == metadata
        for malformed in (None, [], "invalid", {"source_ids": ["pdfhir_test_payer"] * 2}):
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                "SET publication_metadata_json = CAST(:metadata AS jsonb);",
                metadata=json.dumps(malformed),
            )
            expected = await snapshot._compute_current_selection(
                _catalog_map(), node_id="test-node", lock_selection=False
            )
            actual = await selection.current_profile_selection_request(_catalog_map())
            assert actual == selection._expected_request(expected, "test-node")
