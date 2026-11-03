# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Proof-free original publications still authenticate raw bytes and complete summaries."""

import hashlib
import json
import struct
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.network_fhir_source_witness_contract import source_origin
from process.network_fhir_source_witness_publication import verify_publication
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID,
    SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS,
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
)
from tests.test_network_fhir_source_witness_contract import legacy_origin_fields


def _publication_header(origin, summary):
    """Return the original proof-free header bound to the supplied metadata bytes."""
    return {
        "hash_contract": None,
        "hash_contract_type": None,
        "proof_absent": True,
        "uhc_proof_absent": True,
        "completion_proof_required_version": None,
        "completion_absent": True,
        "candidate_storage_json": None,
        "admission_absent": True,
        "proof_sha256": None,
        "semantic_as_of_absent": True,
        "semantic_as_of": None,
        "oid": origin.header_oid,
        "dataset_id": origin.dataset_id,
        "endpoint_id": origin.endpoint_id,
        "acquisition_root_run_id": origin.root_run_id,
        "dataset_hash": origin.dataset_sha256,
        "resource_count": sum(count for _, count in origin.resource_counts),
        "metadata_sha256": origin.metadata_text_sha256,
        "metadata_bytes": origin.metadata_bytes,
        "publication_metadata_sha256": None,
        "status": "published",
        "published_at": datetime(2026, 1, 1, tzinfo=UTC),
        "summary": summary,
    }


def _original_publication(proof_key=None):
    """Pair original bytes with a header and rollback-only binary COPY reader."""
    fields = legacy_origin_fields()
    resource_count_by_type = dict(fields["resource_counts"])
    summary = build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            fields["dataset_id"], fields["endpoint_id"], fields["root_run_id"], fields["dataset_sha256"]
        ),
        source_ids=[fields["source_id"]],
        selected_resources=sorted(resource_count_by_type),
        count_by_resource=resource_count_by_type,
        hash_by_resource=dict(fields["resource_hashes"]),
        count_by_field={**dict.fromkeys(SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS, 0), "organization_resources": 1},
        count_by_category={"unknown_field_counts": {}, "intentional_drop_counts": {}},
        identity_by_field={"semantic_contract_id": SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID},
    )
    metadata = {"source_ids": [fields["source_id"]], "source_summary_v1": summary}
    if proof_key is not None:
        metadata[proof_key] = {"contract_id": "unexpected-proof"}
    encoded = json.dumps(metadata).encode()
    fields.update(
        metadata_bytes=len(encoded),
        metadata_text_sha256=hashlib.sha256(encoded).hexdigest(),
        summary_sha256=summary["summary_sha256"],
    )
    origin = source_origin(fields)
    header_by_name = _publication_header(origin, summary)
    start, rollback = AsyncMock(), AsyncMock()

    async def copy(query, dataset_id, *, output, **options):
        assert dataset_id == origin.dataset_id and options == {"format": "binary"}
        assert query.startswith("SELECT publication_metadata_json::text")
        await output(
            b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, len(encoded)) + encoded + struct.pack("!h", -1)
        )
        return "COPY 1"

    reader = SimpleNamespace(transaction=lambda: SimpleNamespace(start=start, rollback=rollback), copy_from_query=copy)
    return reader, origin, header_by_name, start, rollback


@pytest.mark.asyncio
async def test_observed_legacy_publication_validates_original_copy_without_admission():
    reader, origin, header, start, rollback = _original_publication()
    await verify_publication(reader, origin, header)
    assert origin.proof_sha256 is None and origin.admission_metadata_sha256 is None
    start.assert_awaited_once()
    rollback.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("proof_key", ["provider_directory_content_proof_v1", "uhc_canonical_content_proof_v1"])
async def test_observed_legacy_publication_refuses_proof_hidden_from_header(proof_key):
    reader, origin, header, _, _ = _original_publication(proof_key)
    with pytest.raises(ValueError):
        await verify_publication(reader, origin, header)
