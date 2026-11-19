# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL completion of roles merged across acquisition batches."""

from __future__ import annotations

import datetime
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitionerRole
from process import provider_directory_bulk_grouped_load as grouped_load
from process import provider_directory_bulk_grouping as grouping
from process import provider_directory_bulk_spool as spool
from process import provider_directory_generic_source_storage as source_storage
from process import provider_directory_role_merge as role_merge
from process.provider_directory_generic_source_adapters import candidate_resource_count
from process.provider_directory_generic_source_completion import complete_generic_source_candidate
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    PROJECTION_AS_OF,
    SOURCE_ID,
    _candidate_stored_proof,
    _insert_parent,
    _semantic_database,
    _write_scope,
    importer,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    semantic_source_template as semantic_source_template,
)


def _role_row(role_id: str, network: str) -> dict[str, object]:
    observed_at = datetime.datetime(2026, 8, 9, 12, 0, 0)
    return {
        "resource_id": role_id,
        "active": True,
        "practitioner_ref": "Practitioner/practitioner-one",
        "organization_ref": "Organization/organization-one",
        "location_refs": ["Location/location-one"],
        "network_refs": [f"Organization/{network}"],
        "identifiers": [{"system": "urn:role", "value": role_id}],
        "specialty_codes": [{"code": "207Q00000X"}],
        "fhir_meta": {"versionId": role_id, "lastUpdated": "2026-08-09T12:00:00Z"},
        "resource_url": f"{CANONICAL_BASE}/PractitionerRole/{role_id}",
        "fhir_self_url": f"{CANONICAL_BASE}/PractitionerRole/{role_id}",
        "fhir_fetch_url": f"{CANONICAL_BASE}/PractitionerRole?page=1",
        "fhir_fetch_mode": "rest_bundle",
        "source_id": SOURCE_ID,
        "last_seen_run_id": "run-semantic-proof",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


async def _staged_payloads(database, schema: str, dataset_id: str) -> list[dict[str, object]]:
    """Read the loader's private input in arrival order."""
    async with source_transaction(database):
        binding = source_storage._json(
            await database.scalar(
                f'SELECT candidate_storage_json FROM "{schema}".provider_directory_endpoint_dataset '
                "WHERE dataset_id=:dataset_id",
                dataset_id=dataset_id,
            )
        )
        relation = importer._unscoped_qt(schema, binding["tables"]["resource_input"]["name"])
        staged_rows = await bound_publisher().connection.fetch(
            f"SELECT payload_json FROM {relation} ORDER BY batch_ordinal,row_ordinal"
        )
    return [source_storage._json(staged_row["payload_json"]) for staged_row in staged_rows]


async def _write_role_page(dataset_id: str, role_row: dict[str, object]) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryPractitionerRole,
        [role_row],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-semantic-proof",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=_write_scope(dataset_id),
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_opted_in", [True, False])
async def test_postgres_roles_appended_in_separate_batches_complete_as_one_role(monkeypatch, is_opted_in):
    """Complete two per-network roles of one provider and office that arrived in different batches."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID if is_opted_in else "")
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        await _write_role_page(dataset_id, _role_row("role-two", "network-two"))
        staged_payloads = await _staged_payloads(database, schema, dataset_id)
        assert len(staged_payloads) == 2
        if is_opted_in:
            # Each observation commits to its own source role; completion checks and drops these.
            native_roles = [staged_payload.pop(role_merge.NATIVE_ROLES) for staged_payload in staged_payloads]
            assert len({*native_roles[0], *native_roles[1]}) == 2
            # The second observation is stored as a repeat: only what the first did not already say.
            assert staged_payloads[1] == {
                "resource_id": staged_payloads[0]["resource_id"],
                role_merge._REPEAT: True,
                "identifiers": [{"system": "urn:role", "value": "role-two"}],
                "network_refs": ["Organization/network-two"],
            }
        else:
            assert role_merge._REPEAT not in staged_payloads[1]
        async with source_transaction(database):
            completed = await complete_generic_source_candidate(
                database, importer, database.semantic_candidates[dataset_id]
            )
            reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
            retained_rows = await bound_publisher().connection.fetch(
                f"SELECT resource_id,payload_hash,payload_json FROM {reduced} ORDER BY resource_id"
            )
        if not is_opted_in:
            assert [retained_row["resource_id"] for retained_row in retained_rows] == ["role-one", "role-two"]
            return
        (retained_row,) = retained_rows
        payload_by_field = source_storage._json(retained_row["payload_json"])
        assert role_merge.NATIVE_ROLES not in payload_by_field
        assert (
            retained_row["resource_id"]
            == payload_by_field["resource_id"]
            == role_merge.merged_role_id(payload_by_field)
        )
        assert payload_by_field["network_refs"] == ["Organization/network-one", "Organization/network-two"]
        assert [identifier["value"] for identifier in payload_by_field["identifiers"]] == ["role-one", "role-two"]
        assert retained_row["payload_hash"] == resource_payload_sha256_for_contract(
            payload_by_field, SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT, resource_type="PractitionerRole"
        )


@pytest.mark.asyncio
async def test_postgres_merged_roles_validate_against_the_source_count_and_publish(monkeypatch):
    """Validate with the two roles the source returned, then publish the one retained role."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles-published"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        await _write_role_page(dataset_id, _role_row("role-two", "network-two"))
        candidate = database.semantic_candidates[dataset_id]
        proof = await _candidate_stored_proof(database, candidate)
        assert proof.resource_counts["PractitionerRole"] == 1
        async with source_transaction(database):
            await database.status(
                f'UPDATE "{schema}".provider_directory_source SET endpoint_id=:endpoint WHERE source_id=:source',
                endpoint=candidate.endpoint_id,
                source=SOURCE_ID,
            )
        validated = await importer._validate_endpoint_dataset_candidate(
            candidate, {"PractitionerRole": {"rows_fetched": 2, "complete": True}}
        )
        assert validated["validated"] and not validated["published"]
        async with source_transaction(database):
            await importer._publish_validated_artifact_dataset(
                importer.ProviderDirectoryArtifactDataset(
                    SOURCE_ID,
                    candidate.endpoint_id,
                    dataset_id,
                    candidate.acquisition_root_run_id,
                    dataset_hash=validated["dataset_hash"],
                    selected_resources=candidate.selected_resources,
                    retained_resources=tuple(sorted(proof.resource_counts)),
                )
            )
            stored_payload_rows = await database.all(
                f'SELECT payload_json FROM "{schema}".provider_directory_dataset_resource WHERE dataset_id=:dataset_id',
                dataset_id=dataset_id,
            )
        assert len(stored_payload_rows) == 1
        payload_by_field = source_storage._json(stored_payload_rows[0][0])
        assert payload_by_field["network_refs"] == ["Organization/network-one", "Organization/network-two"]


@pytest.mark.asyncio
async def test_postgres_role_observed_again_after_a_reset_is_stored_in_full(monkeypatch):
    """A reset discards earlier observations, so the next one must carry the whole role again."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles-reset"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        async with source_transaction(database):
            await source_storage.reset_generic_source_family(
                database, importer, database.semantic_candidates[dataset_id]
            )
        await _write_role_page(dataset_id, _role_row("role-two", "network-two"))
        staged_payloads = await _staged_payloads(database, schema, dataset_id)
        assert [role_merge._REPEAT in staged_payload for staged_payload in staged_payloads] == [False, False]
        async with source_transaction(database):
            completed = await complete_generic_source_candidate(
                database, importer, database.semantic_candidates[dataset_id]
            )
            reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
            retained_payloads = await bound_publisher().connection.fetch(f"SELECT payload_json FROM {reduced}")
        assert len(retained_payloads) == 1
        assert source_storage._json(retained_payloads[0]["payload_json"])["network_refs"] == [
            "Organization/network-two"
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [{}, {"network_refs": ["Organization/network-two"]}, {"specialty_codes": [{"code": "208D00000X"}]}],
    ids=["same content", "another network", "another specialty"],
)
async def test_postgres_repeated_source_role_keeps_the_conflict_rule(monkeypatch, changes):
    """One source role may arrive twice with the same content, never with a changed network or body."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles-native"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        await _write_role_page(dataset_id, {**_role_row("role-one", "network-one"), **changes})
        async with source_transaction(database):
            if changes:
                with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
                    await complete_generic_source_candidate(
                        database, importer, database.semantic_candidates[dataset_id]
                    )
                return
            completed = await complete_generic_source_candidate(
                database, importer, database.semantic_candidates[dataset_id]
            )
            reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
            retained_payloads = await bound_publisher().connection.fetch(f"SELECT payload_json FROM {reduced}")
        (retained_payload,) = retained_payloads
        payload_by_field = source_storage._json(retained_payload["payload_json"])
        assert payload_by_field["network_refs"] == ["Organization/network-one"]
        assert role_merge.NATIVE_ROLES not in payload_by_field


def _other_practitioner(role_row: dict[str, object]) -> dict[str, object]:
    return {**role_row, "practitioner_ref": "Practitioner/practitioner-two"}


def _two_outputs() -> list[list[dict[str, object]]]:
    """Two saved outputs; each practitioner's per-network roles are split between them."""
    unnetworked_by_field = {
        **_role_row("role-c", "network-one"),
        "network_refs": [],
        "location_refs": ["Location/location-two"],
    }
    return [
        [
            _role_row("role-a1", "network-one"),
            _other_practitioner(_role_row("role-b1", "network-one")),
            unnetworked_by_field,
        ],
        [
            _role_row("role-a2", "network-two"),
            _role_row("role-a3", "network-three"),
            _other_practitioner(_role_row("role-b2", "network-two")),
        ],
    ]


def _prepared_roles(dataset_id, role_rows):
    """The rows the preparation of a saved output gives: retained rows folded per merged role."""
    contract = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    prepared_rows = importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitionerRole, role_rows, dataset_id=dataset_id, resource_hash_contract=contract
    )
    return role_merge.fold_merged_roles(prepared_rows, contract)


async def _load_grouped(group_root, dataset_id, outputs, partitions, replayed_partitions=(), identity=None):
    """Group each output's prepared roles on disk, then reduce and append every partition and the replayed ones."""
    contract = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    for output_index, role_rows in enumerate(outputs):
        writer = grouping.GroupWriter(group_root, output_index, partitions)
        writer.write(_prepared_roles(dataset_id, role_rows))
        writer.finish(len(role_rows))
    source_by_field = {
        "source_id": SOURCE_ID,
        "_endpoint_dataset_id": dataset_id,
        "_resource_hash_contract": contract,
        "_semantic_projection_as_of": PROJECTION_AS_OF,
    }
    options = SimpleNamespace(row_batch_size=2, ownership_probe=AsyncMock())
    load = SimpleNamespace(
        copy=SimpleNamespace(directory=importer, source_record=source_by_field, options=options, identity=identity),
        stream_state=SimpleNamespace(rows_written_this_run=0, outputs_processed=0),
    )
    for partition in [*range(partitions), *replayed_partitions]:
        reduced_rows = grouping.reduce_partition(group_root, partition, range(len(outputs)), contract, partitions)
        await grouped_load._append(load, reduced_rows)
    return load


def _content(payload_by_field):
    """A merged role without the provenance of the source role it was taken from."""
    return {
        name: field_value
        for name, field_value in payload_by_field.items()
        if name == "resource_id" or name not in role_merge._ROLE_IDENTITY_FIELDS
    }


async def _completed_rows(database, schema: str, dataset_id: str) -> list[tuple[str, str, dict[str, object]]]:
    async with source_transaction(database):
        completed = await complete_generic_source_candidate(
            database, importer, database.semantic_candidates[dataset_id]
        )
        reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
        retained_rows = await bound_publisher().connection.fetch(
            f"SELECT resource_id,payload_hash,payload_json FROM {reduced} ORDER BY resource_id"
        )
    return [
        (retained["resource_id"], retained["payload_hash"], source_storage._json(retained["payload_json"]))
        for retained in retained_rows
    ]


async def _staged_count(database, dataset_id: str, *, distinct: bool) -> int:
    candidate = database.semantic_candidates[dataset_id]
    scope = SimpleNamespace(
        dataset_id=dataset_id,
        endpoint_id=candidate.endpoint_id,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
        source_ids=[SOURCE_ID],
    )
    return await candidate_resource_count(importer, scope, "PractitionerRole", distinct=distinct)


async def _validate_and_publish(database, schema: str, dataset_id: str, rows_fetched: int):
    """Validate with the rows the outputs report, publish, and read the published roles."""
    candidate = database.semantic_candidates[dataset_id]
    async with source_transaction(database):
        await database.status(
            f'UPDATE "{schema}".provider_directory_source SET endpoint_id=:endpoint WHERE source_id=:source',
            endpoint=candidate.endpoint_id,
            source=SOURCE_ID,
        )
    validated = await importer._validate_endpoint_dataset_candidate(
        candidate, {"PractitionerRole": {"rows_fetched": rows_fetched, "complete": True}}
    )
    assert validated["validated"] and not validated["published"]
    async with source_transaction(database):
        await importer._publish_validated_artifact_dataset(
            importer.ProviderDirectoryArtifactDataset(
                SOURCE_ID,
                candidate.endpoint_id,
                dataset_id,
                candidate.acquisition_root_run_id,
                dataset_hash=validated["dataset_hash"],
                selected_resources=candidate.selected_resources,
                retained_resources=("PractitionerRole",),
            )
        )
        published = await database.all(
            f'SELECT resource_id,payload_hash,payload_json FROM "{schema}".provider_directory_dataset_resource '
            "WHERE dataset_id=:dataset_id ORDER BY resource_id",
            dataset_id=dataset_id,
        )
    return [
        (resource_id, payload_hash, source_storage._json(stored)) for resource_id, payload_hash, stored in published
    ]


@pytest.mark.asyncio
async def test_postgres_grouped_load_matches_streaming_then_publishes(monkeypatch, tmp_path):
    """Grouped on disk with one partition replayed, the roles complete, prove and publish as streamed ones do."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    outputs, partitions = _two_outputs(), 8
    async with _semantic_database(monkeypatch) as (database, schema):
        for dataset_id in ("dataset-streamed-roles", "dataset-grouped-roles"):
            await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        for role_row in [*outputs[0], *outputs[1]]:
            await _write_role_page("dataset-streamed-roles", role_row)
        (merged_role,) = _prepared_roles("dataset-grouped-roles", outputs[0][:1])
        replayed_partition = grouping.partition_of(merged_role["resource_id"], partitions)
        load = await _load_grouped(
            tmp_path, "dataset-grouped-roles", outputs, partitions, replayed_partitions=(replayed_partition,)
        )
        rows_written = load.stream_state.rows_written_this_run
        # The finish count: every active staged row, so the replayed role counts twice.
        staged_count = await _staged_count(database, "dataset-grouped-roles", distinct=False)
        assert staged_count == rows_written == len(await _staged_payloads(database, schema, "dataset-grouped-roles"))
        assert staged_count == await _staged_count(database, "dataset-grouped-roles", distinct=True) + 1
        streamed_proof = await _candidate_stored_proof(database, database.semantic_candidates["dataset-streamed-roles"])
        grouped_proof = await _candidate_stored_proof(database, database.semantic_candidates["dataset-grouped-roles"])
        assert (grouped_proof.resource_hashes, grouped_proof.resource_counts) == (
            streamed_proof.resource_hashes,
            streamed_proof.resource_counts,
        )
        assert grouped_proof.resource_counts["PractitionerRole"] == 3
        streamed_rows = await _completed_rows(database, schema, "dataset-streamed-roles")
        published_rows = await _validate_and_publish(database, schema, "dataset-grouped-roles", rows_written)
    assert [(role_id, role_hash, _content(role_payload)) for role_id, role_hash, role_payload in published_rows] == [
        (role_id, role_hash, _content(role_payload)) for role_id, role_hash, role_payload in streamed_rows
    ]
    # Only the provenance fields differ: the reducer keeps the observation with the largest identity fields, while
    # streaming keeps the first full one staged (later sightings are repeats), which depends on arrival order.
    assert {role_payload["fhir_meta"]["versionId"] for _, _, role_payload in published_rows} == {
        "role-a3",
        "role-b2",
        "role-c",
    }


@pytest.mark.asyncio
async def test_postgres_grouped_native_role_conflict_across_partitions_fails(monkeypatch, tmp_path):
    """One source role with two specialties lands in two partitions; completion still refuses it."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    first_by_field = _role_row("role-one", "network-one")
    second_by_field = {**first_by_field, "specialty_codes": [{"code": "208D00000X"}]}
    merged_ids = [
        _prepared_roles("dataset-grouped-conflict", [role_row])[0]["resource_id"]
        for role_row in (first_by_field, second_by_field)
    ]
    partitions = next(
        count for count in range(2, 64) if len({grouping.partition_of(role_id, count) for role_id in merged_ids}) == 2
    )
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-grouped-conflict"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _load_grouped(tmp_path, dataset_id, [[first_by_field], [second_by_field]], partitions)
        async with source_transaction(database):
            with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
                await complete_generic_source_candidate(database, importer, database.semantic_candidates[dataset_id])


async def _bulk_checkpoint(dataset_id, candidate, output_count):
    """A real role checkpoint of the candidate with validated, still incomplete outputs."""
    start_url = f"{CANONICAL_BASE}/$export"
    identity = importer.BulkExportCheckpointIdentity(
        checkpoint_id="f" * 64,
        canonical_api_base=CANONICAL_BASE,
        resource_type="PractitionerRole",
        source_scope_hash="scope-grouped",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
        owner_run_id="run-semantic-proof",
        retry_of_run_id=None,
        endpoint_id=candidate.endpoint_id,
        dataset_id=dataset_id,
        start_url=start_url,
        start_url_hash=hashlib.sha256(start_url.encode()).hexdigest(),
    )
    manifest = importer._bulk_export_manifest_from_payload(
        {
            "transactionTime": datetime.datetime.now(datetime.UTC).isoformat(),
            "request": start_url,
            "requiresAccessToken": False,
            "output": [
                {"type": "PractitionerRole", "url": f"https://files.example.test/part-{index}.ndjson"}
                for index in range(output_count)
            ],
        },
        "PractitionerRole",
        expected_request_url=start_url,
    )
    await importer._reserve_bulk_export_checkpoint(identity)
    await importer._persist_bulk_export_manifest(identity, manifest)
    for checkpoint in await importer._load_bulk_output_checkpoints(identity.checkpoint_id):
        etag = f'"etag-{checkpoint["output_index"]}"'
        validator = importer.BulkExportOutputValidator(4096, etag, hashlib.sha256(etag.encode()).hexdigest(), None)
        await importer._persist_bulk_output_validator(identity, checkpoint, validator)
    checkpoints = await importer._load_bulk_output_checkpoints(identity.checkpoint_id)
    return identity, manifest, checkpoints


@pytest.mark.asyncio
async def test_postgres_grouped_finish_writes_the_active_staged_count(monkeypatch, tmp_path):
    """Finish completes every role output in one transaction; their rows add up to the active staged rows."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    monkeypatch.setenv(spool.SPOOL_ROOT_ENV, str(tmp_path))
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "grouped-finish-test-key")
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-grouped-finish"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        identity, manifest, checkpoints = await _bulk_checkpoint(
            dataset_id, database.semantic_candidates[dataset_id], 2
        )
        group_root = spool.group_directory(identity.checkpoint_id)
        load = await _load_grouped(
            group_root, dataset_id, _two_outputs(), 4, replayed_partitions=(0, 1, 2, 3), identity=identity
        )
        load.group_root = group_root
        load.copy.options.ownership_probe = AsyncMock()
        pending_pairs = list(zip(manifest.outputs, checkpoints, strict=True))
        await grouped_load._finish(load, pending_pairs)
        completed = await importer._load_bulk_output_checkpoints(identity.checkpoint_id)
        staged_count = len(await _staged_payloads(database, schema, dataset_id))
    assert {checkpoint["state"] for checkpoint in completed} == {importer.BULK_EXPORT_OUTPUT_COMPLETE}
    assert [checkpoint["committed_bytes"] for checkpoint in completed] == [4096, 4096]
    assert sum(checkpoint["rows_written"] for checkpoint in completed) == staged_count == 6
    assert load.stream_state.rows_written_this_run == staged_count and not group_root.parent.exists()
