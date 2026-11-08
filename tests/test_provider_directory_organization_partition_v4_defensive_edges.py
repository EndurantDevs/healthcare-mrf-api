# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Defensive unit edges for semantic-v4 Organization partitions."""

from __future__ import annotations

import copy
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_storage as source_storage
from process.plan_release_storage_catalog import CATALOG_SQL
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from tests.test_provider_directory_generic_source_policy import synthetic_source_database
from tests.test_provider_directory_generic_source_storage import _case, _catalog_row, _sequence_row
from tests.test_provider_directory_organization_partition_v4 import (
    _context,
    _proof_rows,
    _source,
)
from tests.test_provider_directory_organization_partition_v4_streaming import (
    _one_row_stage,
    _plan,
)

importer = importlib.import_module("process.provider_directory_fhir")


def _pass_proof(**overrides) -> importer.LastUpdatedPartitionPassProof:
    """Return one exact pass proof with optional field overrides."""

    proof_by_field = {
        "resource_type": "Organization",
        "window_id": "window-a",
        "pass_number": 2,
        "fingerprints_by_id": {"occurrence-a": "a" * 64},
        "candidate_hashes_by_id": {},
        "candidate_records_by_id": {},
        "source_fingerprints_by_id": {},
    }
    proof_by_field.update(overrides)
    return importer.LastUpdatedPartitionPassProof(**proof_by_field)


def _fingerprint_row(**payload_overrides) -> dict[str, object]:
    """Return one durable partition fingerprint row."""

    payload = {
        "window_id": "window-a",
        "fingerprint": "a" * 64,
        **payload_overrides,
    }
    return {
        "resource_id": "occurrence-a",
        "payload_hash": importer._last_updated_partition_window_proof_hash(
            "Organization",
            "window-a",
            2,
        ),
        "payload_json": payload,
    }


@pytest.mark.parametrize("resource_id", (None, "", 7))
def test_v4_bindings_reject_missing_or_untyped_resource_id(resource_id) -> None:
    """Leave invalid observations unbound so downstream staging fails closed."""

    resources = ({"resourceType": "Organization", "id": resource_id},)
    assert importer._v4_partition_resource_bindings(
        resources,
        "window-a",
    ) == (resources, resources, (), {})


def test_v4_partition_proof_decoder_rejects_malformed_record() -> None:
    """Translate proof-store shape failures to the partition fence error."""

    with pytest.raises(RuntimeError, match="fingerprint_row_invalid"):
        importer._partition_proof_row_fields(
            _fingerprint_row(candidate_proof_record=[]),
            "window-a",
        )


def _partition_source_catalog(context):
    """Describe exact native input tables and their protected acquisition header."""
    _, _, header_by_field = _case()
    binding_by_field = header_by_field["candidate_storage_json"]
    binding_by_field["ordinal"] = 1
    metadata = {
        "source_ids": list(context.source_ids),
        "selected_resources": ["Organization"],
        "resource_hash_contract": importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-08-10",
    }
    header_by_field.update(
        endpoint_id=context.endpoint_id,
        acquisition_root_run_id=context.acquisition_root_run_id,
        publication_metadata_json=metadata,
    )
    binding_by_field["identity"] = dict(
        dataset_id=context.dataset_id,
        endpoint_id=context.endpoint_id,
        acquisition_root_run_id=context.acquisition_root_run_id,
        metadata=metadata,
    )
    catalog_rows = []
    for table in binding_by_field["tables"].values():
        catalog_by_field = _catalog_row() | dict(relname=table["name"], oid=table["oid"], relowner=1, nspname="mrf")
        table.update(source_storage._layout_proof(catalog_by_field))
        if table["name"].endswith("_ri"):
            table["layout_sha256"] = source_storage._input_layout_hash(
                table["layout_sha256"], _sequence_row(owner=1, schema="mrf")
            )
        catalog_rows.append(catalog_by_field)

    return header_by_field, catalog_rows


@pytest.mark.asyncio
async def test_v4_partition_proof_loader_retains_candidate_record(monkeypatch) -> None:
    """Round-trip a non-null occurrence record through durable proof loading."""

    stage = await _one_row_stage()
    proof_rows = _proof_rows(stage, "window-a")
    database_executor = SimpleNamespace(all=AsyncMock(return_value=proof_rows))

    context = _context()
    header_by_field, catalog_rows = _partition_source_catalog(context)

    async with synthetic_source_database(monkeypatch, database_executor) as database, source_transaction(database):
        monkeypatch.setattr(importer, "db", database)
        native = bound_publisher().connection
        role_fetch = native.fetch
        native.fetchval.side_effect = lambda query, *args: (
            "repeatable read" if query == "SHOW transaction_isolation" else 1
        )
        queries = []

        async def fetch(query, *arguments):
            queries.append(query)
            if query == source_storage._INPUT_SEQUENCES_SQL:
                return [_sequence_row(owner=1, schema="mrf")]
            if query == CATALOG_SQL:
                assert arguments == (
                    [f'"mrf"."{catalog_by_field["relname"]}"' for catalog_by_field in catalog_rows],
                    1,
                    [],
                )
                return copy.deepcopy(catalog_rows)
            if query.startswith("SELECT resource_id,payload_json FROM"):
                assert arguments == (
                    importer._last_updated_partition_fingerprint_resource_type("Organization", 2),
                    0,
                    importer._last_updated_partition_window_proof_hash("Organization", "window-a", 2),
                    None,
                    1,
                )
                return proof_rows
            return await role_fetch(query, *arguments)

        async def parent(query, *arguments):
            if "candidate_storage_json FROM" in query:
                assert arguments == (context.dataset_id,)
                return copy.deepcopy(header_by_field)
            assert query == "SELECT session_user::text AS session,current_user::text AS current"
            return native.fetchrow.return_value

        monkeypatch.setattr(native, "fetch", fetch)
        native.fetchrow.side_effect = parent
        loaded_proof = await importer._load_partition_pass_proof(
            context, "Organization", "window-a", 2, database_connection=database_executor
        )
        native.copy_records_to_table.assert_not_awaited()
        assert sum(query.startswith("SELECT resource_id,payload_json FROM") for query in queries) == 1
    assert loaded_proof.candidate_records_by_id == stage.candidate_proof_records_by_id
    database_executor.all.assert_not_awaited()


@pytest.mark.parametrize(
    ("overrides", "error"),
    (
        (
            {"candidate_hashes_by_id": {"other": "b" * 64}},
            "candidate_hash_mismatch",
        ),
        (
            {"candidate_records_by_id": {"other": []}},
            "candidate_proof_mismatch",
        ),
        (
            {"source_fingerprints_by_id": {"other": "b" * 64}},
            "occurrence_identity_invalid",
        ),
    ),
)
def test_v4_partition_proof_requires_exact_optional_keysets(
    overrides,
    error,
) -> None:
    """Bind every optional pass commitment to the fingerprint identities."""

    with pytest.raises(RuntimeError, match=error):
        importer._assert_partition_proof_keysets(_pass_proof(**overrides))


@pytest.mark.parametrize(
    ("parsed_rows", "occurrence_ids"),
    (
        ([{"resource_id": "organization-a"}], ()),
        (
            [
                {"resource_id": "organization-a"},
                {"resource_id": "organization-a"},
            ],
            ("occurrence-a", "occurrence-a"),
        ),
    ),
)
def test_v4_candidate_proofs_require_exact_occurrence_cardinality(
    parsed_rows,
    occurrence_ids,
) -> None:
    """Reject missing or duplicate raw occurrence identities."""

    with pytest.raises(RuntimeError, match="occurrence_identity_invalid"):
        importer._v4_partition_candidate_proofs(
            importer.ProviderDirectoryOrganization,
            parsed_rows,
            "dataset-a",
            importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            occurrence_ids,
        )


def test_v4_candidate_proof_requires_one_retained_row(monkeypatch) -> None:
    """Reject an occurrence that cannot produce exactly one proof row."""

    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_resource_rows",
        lambda *_args, **_kwargs: [],
    )
    with pytest.raises(RuntimeError, match="occurrence_proof_invalid"):
        importer._v4_partition_candidate_proofs(
            importer.ProviderDirectoryOrganization,
            [{"resource_id": "organization-a"}],
            "dataset-a",
            importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            ("occurrence-a",),
        )


@pytest.mark.parametrize("failure", ("source_keyset", "retained_identity"))
def test_v4_partition_stage_requires_exact_identity(failure) -> None:
    """Bind occurrence sources and retained rows to parsed identities."""

    options = importer.LastUpdatedPartitionStageOptions(
        run_id="run-a",
        fetch_url="https://directory.example.test/fhir/Organization",
        occurrence_ids=("occurrence-a",),
        source_fingerprints_by_id={"occurrence-a": "a" * 64},
    )
    parsed_rows = [{"resource_id": "organization-a"}]
    staged_rows = [{"resource_id": "organization-a"}]
    expected_error = "stage_identity_mismatch"
    if failure == "source_keyset":
        options = importer.LastUpdatedPartitionStageOptions(
            run_id="run-a",
            fetch_url="https://directory.example.test/fhir/Organization",
            occurrence_ids=("occurrence-a",),
            source_fingerprints_by_id={"other": "a" * 64},
        )
        expected_error = "occurrence_identity_invalid"
    else:
        staged_rows[0]["resource_id"] = "organization-b"
    with pytest.raises(RuntimeError, match=expected_error):
        importer._assert_partition_stage_identity(
            True,
            1,
            parsed_rows,
            staged_rows,
            options,
        )


def test_v4_partition_identity_rejects_absent_and_duplicate_sources() -> None:
    """Require a source digest and count repeated proof identities as invalid."""

    missing_source_row = _fingerprint_row()
    with pytest.raises(RuntimeError, match="fingerprint_row_invalid"):
        importer._v4_partition_row_identity(missing_source_row, 2)

    complete_row = _fingerprint_row(source_fingerprint="b" * 64)
    identities, invalid_count = importer._v4_partition_identity_set(
        [complete_row, copy.deepcopy(complete_row)],
        2,
    )
    assert len(identities) == 1
    assert invalid_count == 1


@pytest.mark.asyncio
async def test_v4_candidate_rows_decode_json_and_reject_bad_rows() -> None:
    """Accept serialized candidate payloads without masking malformed rows."""

    stage = await _one_row_stage()
    serialized = copy.deepcopy(stage.rows[0])
    serialized["payload_json"] = json.dumps(serialized["payload_json"])
    duplicate = copy.deepcopy(serialized)
    malformed_by_field = {**serialized, "resource_id": "organization-b"}
    malformed_by_field["payload_json"] = "{"

    hashes, records, invalid_count = importer._v4_partition_candidate_hashes(
        [serialized, duplicate, malformed_by_field]
    )

    assert hashes == {stage.rows[0]["resource_id"]: stage.rows[0]["payload_hash"]}
    assert set(records) == set(hashes)
    assert invalid_count == 2


@pytest.mark.asyncio
async def test_v4_occurrence_records_reject_malformed_payload_and_identity() -> None:
    """Count non-mapping payloads and candidate identity drift as invalid."""

    stage = await _one_row_stage()
    valid_row = _proof_rows(stage, "window-a")[0]
    invalid_identity = copy.deepcopy(valid_row)
    invalid_identity["payload_json"]["candidate_resource_id"] = "other"

    records, occurrence_groups, invalid_count = importer._v4_partition_occurrence_records(
        [{"payload_json": []}, invalid_identity]
    )

    assert records == {}
    assert occurrence_groups == {}
    assert invalid_count == 2


def test_v4_window_count_is_optional_without_a_partition_plan() -> None:
    """Allow callers without a planner to validate the remaining proof."""

    assert importer._v4_partition_window_count_error([], None) == 0


@pytest.mark.asyncio
async def test_dataset_accumulator_preserves_legacy_compatibility(monkeypatch) -> None:
    """Authenticate Source, preserve rows, and issue no direct retained DML."""

    incoming_rows = [
        {
            "dataset_id": "dataset-a",
            "resource_type": "Organization",
            "resource_id": "organization-a",
        }
    ]
    before = copy.deepcopy(incoming_rows)
    executor = SimpleNamespace(first=AsyncMock(), all=AsyncMock(), scalar=AsyncMock(), status=AsyncMock())
    async with synthetic_source_database(monkeypatch, executor) as database, source_transaction(database):
        native = bound_publisher().connection
        native.execute.reset_mock()
        accumulated_rows = await importer._accumulated_endpoint_dataset_rows(
            database,
            incoming_rows,
            resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
        )
        native.copy_records_to_table.assert_not_awaited()
        native.execute.assert_not_awaited()
    assert accumulated_rows is incoming_rows
    assert accumulated_rows == before
    executor.first.assert_not_awaited()
    executor.all.assert_not_awaited()
    executor.scalar.assert_not_awaited()
    executor.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_v4_nonempty_partition_streams_verified_candidate(monkeypatch) -> None:
    """Release a valid nonempty snapshot through the compatibility writer."""

    stage = await _one_row_stage()
    proof_counts = importer.LastUpdatedPartitionProofCounts(
        leaf_count_sum=1,
        pass1_unique=1,
        pass2_unique=1,
        staged_candidate_count=1,
        invalid_candidate_count=0,
        orphan_proof_count=0,
        candidate_hashes_by_id={stage.rows[0]["resource_id"]: stage.rows[0]["payload_hash"]},
    )
    monkeypatch.setattr(
        importer,
        "_assert_last_updated_partition_candidate_proof",
        AsyncMock(return_value=proof_counts),
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(side_effect=[[stage.rows[0]], []]),
    )
    row_handler = AsyncMock(return_value=1)

    stream_counts = await importer._stream_last_updated_partition_staged_rows(
        _context(),
        _source(),
        "Organization",
        importer.ProviderDirectoryOrganization,
        _plan(),
        run_id="run-organization-partition",
        row_batch_handler=row_handler,
        row_batch_size=10,
    )

    assert stream_counts == (1, 1)
    assert row_handler.await_args.args[1][0]["resource_id"] == "organization-a"
