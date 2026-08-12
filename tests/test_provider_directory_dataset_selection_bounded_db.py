# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for bounded Provider Directory dataset selection."""

from __future__ import annotations

import hashlib
import json

import pytest

from api.provider_directory_source_catalog_outcomes import (
    _canonical_validated_datasets_by_source_id,
)
from process.provider_directory_admission_seal import (
    admission_seal_from_validated_metadata,
    backfill_provider_directory_admission_seal,
)
from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_sha256,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
    importer,
)


_HASH_CASES = (
    ({}, True),
    ({"value": None}, True),
    ({"value": 7}, True),
    ({"nested": {"enabled": True, "items": [1, None, "ascii"]}}, True),
    ({"value": "synthetic-ž"}, False),
    ({"value": 0.0}, False),
    ({"value": -0.0}, False),
)


def _proof_line_hash(value_list: list[dict[str, object]]) -> str:
    """Hash canonical proof descriptors with the production framing."""

    digest = hashlib.sha256()
    for value_index, value_by_field in enumerate(value_list):
        if value_index:
            digest.update(b"\n")
        digest.update(
            json.dumps(
                value_by_field,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
    return digest.hexdigest()


def _large_semantic_shard_list(
    source_id_list: list[str],
    shard_count: int = 512,
    *,
    dataset_id: str = "dataset_shared",
    endpoint_id: str = "endpoint_shared",
    root_run_id: str = "root-shared",
) -> list[dict[str, object]]:
    """Return many valid synthetic shard descriptors."""

    shard_list: list[dict[str, object]] = []
    for shard_index in range(shard_count):
        input_hash = hashlib.sha256(f"input-{shard_index}".encode()).hexdigest()
        identity_part_list = [
            "Location",
            f"location-{shard_index:05d}",
            "d" * 64,
        ]
        shard_list.append(
            {
                "shard_id": importer._identity_hash(
                    [
                        dataset_id,
                        endpoint_id,
                        root_run_id,
                        source_id_list,
                        input_hash,
                    ]
                ),
                "dataset_id": dataset_id,
                "endpoint_id": endpoint_id,
                "acquisition_root_run_id": root_run_id,
                "source_ids": source_id_list,
                "resource_count": 1,
                "resource_counts": {"Location": 1},
                "first_identity": identity_part_list,
                "last_identity": identity_part_list,
                "input_sha256": input_hash,
                "artifact_sha256": hashlib.sha256(
                    f"artifact-{shard_index}".encode()
                ).hexdigest(),
                "artifact_byte_count": 1,
            }
        )
    shard_list.sort(key=lambda shard: str(shard["shard_id"]))
    return shard_list


def _large_semantic_proof_by_field(
    shard_count: int = 512,
    *,
    dataset_id: str = "dataset_shared",
    endpoint_id: str = "endpoint_shared",
    root_run_id: str = "root-shared",
    source_id_list: list[str] | None = None,
) -> dict[str, object]:
    """Return a valid semantic proof with a large discarded shard array."""

    source_id_list = source_id_list or ["source_primary", "source_sibling"]
    shard_list = _large_semantic_shard_list(
        source_id_list,
        shard_count,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        root_run_id=root_run_id,
    )
    proof_by_field: dict[str, object] = {
        "contract_id": (
            importer.PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID
        ),
        "complete": True,
        "dataset_id": dataset_id,
        "endpoint_id": endpoint_id,
        "acquisition_root_run_id": root_run_id,
        "source_ids": source_id_list,
        "selected_resources": ["Location"],
        "proof_resource_scope": ["Location"],
        "resource_hash_contract": (
            importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
        "semantic_projection_as_of": "2026-08-09",
        "semantic_union": {
            "added_name_count": 0,
            "collision_identities": 0,
            "observation_variants": 0,
            "union_name_count": 0,
        },
        "dataset_hash": "e" * 64,
        "resource_count": len(shard_list),
        "resource_hashes": {"Location": "f" * 64},
        "resource_counts": {"Location": len(shard_list)},
        "source_metrics": {
            "address_records": 0,
            "addressed_locations": 0,
            "distinct_npis": 0,
            "geocoded_locations": 0,
        },
        "npi_set_sha256": "a" * 64,
        "shard_count": len(shard_list),
        "shard_set_sha256": _proof_line_hash(shard_list),
        "shards": shard_list,
    }
    proof_by_field["proof_sha256"] = importer._identity_hash(proof_by_field)
    return proof_by_field


def _large_metadata_by_field(
    shard_count: int = 512,
    **proof_identity: object,
) -> dict[str, object]:
    """Return metadata containing one valid large semantic proof."""

    return {
        "source_ids": proof_identity.get("source_id_list")
        or ["source_primary", "source_sibling"],
        "selected_resources": ["Location"],
        "expected_resources": ["Location"],
        "resource_hash_contract": importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": "2026-08-09",
        "proof_resource_scope": ["Location"],
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: (
            _large_semantic_proof_by_field(
                shard_count,
                **proof_identity,
            )
        ),
    }


async def _replace_shared_metadata(
    database,
    schema: str,
    metadata_by_field: dict[str, object],
) -> None:
    """Replace one disposable fixture's publication metadata."""

    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET publication_metadata_json = CAST(:metadata AS json) "
        "WHERE dataset_id = 'dataset_shared';",
        metadata=json.dumps(metadata_by_field, ensure_ascii=False),
    )


async def _set_shared_semantic_proof(
    database,
    schema: str,
    metadata_by_field: dict[str, object],
) -> None:
    """Install one proof and its matching parent scalar identity."""

    await _replace_shared_metadata(database, schema, metadata_by_field)
    proof_by_field = metadata_by_field[
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ]
    assert isinstance(proof_by_field, dict)
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET dataset_hash = :dataset_hash, resource_count = :resource_count "
        "WHERE dataset_id = 'dataset_shared';",
        dataset_hash=proof_by_field["dataset_hash"],
        resource_count=proof_by_field["resource_count"],
    )


async def _all_source_projected_rows(database) -> list[dict[str, object]]:
    """Return the bounded all-source rows from the disposable fixture."""

    projected_rows = await database.all(
        importer._provider_directory_artifact_dataset_selection_sql(None),
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        select_validated_candidates=False,
    )
    return [dict(database_row._mapping) for database_row in projected_rows]


async def _install_unrelated_large_proof_hash_sentinel(database, schema: str) -> None:
    """Fail if exact-source selection hashes an unrelated large proof."""
    metadata_by_field = _large_metadata_by_field()
    metadata_by_field.update(source_ids=["source_unrelated"], unrelated_probe=True)
    await database.status(
        f"ALTER FUNCTION {schema}.provider_directory_subset_payload_sha256(jsonb) "
        "RENAME TO provider_directory_subset_payload_sha256_original;"
    )
    await database.status(
        f"""CREATE FUNCTION {schema}.provider_directory_subset_payload_sha256(candidate jsonb)
        RETURNS text LANGUAGE plpgsql AS $function$ BEGIN
            IF candidate ? 'unrelated_probe' THEN RAISE EXCEPTION
                'unrelated_provider_directory_dataset_evaluated'; END IF;
            RETURN {schema}.provider_directory_subset_payload_sha256_original(candidate);
        END; $function$;"""
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint (endpoint_id) "
        "VALUES ('endpoint_unrelated');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "dataset_hash, status, is_current, resource_count, validated_at, published_at, "
        "publication_metadata_json"
        ") VALUES ("
        "'dataset_unrelated', 'endpoint_unrelated', 'run-unrelated', "
        "'root-unrelated', repeat('9', 64), :published_status, true, 1, NULL, now(), "
        "CAST(:metadata AS json)"
        "), ("
        "'dataset_unrelated_candidate', 'endpoint_unrelated', 'run-unrelated-candidate', "
        "'root-unrelated-candidate', repeat('8', 64), :validated_status, false, 1, now(), NULL, "
        "CAST(:metadata AS json)"
        ");",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        metadata=json.dumps(metadata_by_field),
    )


def _replace_with_legacy_contract(proof_by_field: dict[str, object]) -> None:
    """Make a sealed proof disagree with its semantic parent contract."""

    proof_by_field["contract_id"] = (
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
    )
    for semantic_field in (
        "proof_resource_scope",
        "resource_hash_contract",
        "semantic_projection_as_of",
        "semantic_union",
    ):
        proof_by_field.pop(semantic_field)


def _mutate_shard_descriptor(metadata_by_field, proof_by_field) -> None:
    del metadata_by_field
    proof_by_field["shards"][0]["artifact_sha256"] = "0" * 64


def _mutate_source_metrics(metadata_by_field, proof_by_field) -> None:
    del metadata_by_field
    proof_by_field["source_metrics"]["address_records"] = -1


def _mutate_resource_keyset(metadata_by_field, proof_by_field) -> None:
    del metadata_by_field
    proof_by_field["resource_counts"]["Practitioner"] = 0
    proof_by_field["resource_hashes"]["Practitioner"] = "0" * 64


def _mutate_cross_contract(metadata_by_field, proof_by_field) -> None:
    del metadata_by_field
    _replace_with_legacy_contract(proof_by_field)


def _mutate_projection_date(metadata_by_field, proof_by_field) -> None:
    proof_by_field["semantic_projection_as_of"] = "2026-02-30"
    metadata_by_field["semantic_projection_as_of"] = "2026-02-30"


def _mutate_selected_scope(metadata_by_field, proof_by_field) -> None:
    proof_by_field["selected_resources"] = ["Practitioner"]
    metadata_by_field["selected_resources"] = ["Practitioner"]


def _mutate_dataset_hash(metadata_by_field, proof_by_field) -> None:
    del metadata_by_field
    proof_by_field["dataset_hash"] = "malformed"


_PROOF_MUTATION_BY_NAME = {
    "shard_descriptor": _mutate_shard_descriptor,
    "source_metrics": _mutate_source_metrics,
    "resource_keyset": _mutate_resource_keyset,
    "cross_contract": _mutate_cross_contract,
    "projection_date": _mutate_projection_date,
    "selected_outside_scope": _mutate_selected_scope,
    "dataset_hash": _mutate_dataset_hash,
}


def _resealed_proof_mutation(
    metadata_by_field: dict[str, object],
    mutation_name: str,
) -> dict[str, object]:
    """Reseal a malformed proof so its outer digest alone still passes."""

    mutated_metadata = json.loads(json.dumps(metadata_by_field))
    proof_by_field = mutated_metadata[
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ]
    proof_by_field.pop("proof_sha256")
    _PROOF_MUTATION_BY_NAME[mutation_name](mutated_metadata, proof_by_field)
    proof_by_field["proof_sha256"] = importer._identity_hash(proof_by_field)
    return mutated_metadata


@pytest.mark.asyncio
async def test_all_source_projection_keeps_large_proof_server_side(monkeypatch):
    """Exclude the full shard proof and enforce a small returned row."""

    async with _dataset_database(monkeypatch) as (database, schema):
        large_metadata_by_field = _large_metadata_by_field()
        serialized_metadata = json.dumps(large_metadata_by_field, sort_keys=True)
        assert len(serialized_metadata) > 200_000
        await _set_shared_semantic_proof(
            database,
            schema,
            large_metadata_by_field,
        )

        projected_row_list = await _all_source_projected_rows(database)

        assert len(projected_row_list) == 2
        assert {
            projected_row["publication_metadata_hash"]
            for projected_row in projected_row_list
        } == {canonical_payload_sha256(large_metadata_by_field)}
        assert all(
            importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
            not in projected_row["publication_metadata_json"]
            for projected_row in projected_row_list
        )
        assert all(
            "completion_proof_json" not in projected_row
            for projected_row in projected_row_list
        )
        assert all(
            projected_row["content_proof_valid"] is True
            for projected_row in projected_row_list
        )
        assert all(
            projected_row["content_proof_resources"] == ["Location"]
            for projected_row in projected_row_list
        )
        projected_byte_count_list = [
            len(json.dumps(projected_row, sort_keys=True, default=str).encode())
            for projected_row in projected_row_list
        ]
        assert max(projected_byte_count_list) < 8_192
        assert sum(projected_byte_count_list) < 16_384
