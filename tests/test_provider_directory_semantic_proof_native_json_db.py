# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete genuine JSON candidates without changing semantic union or provenance."""

import pytest

from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import source_transaction
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    RESOURCE_ID,
    SOURCE_ID,
    _completed_semantic_scope,
    _insert_parent,
    _materialize_semantic_dataset,
    _observation,
    _proof,
    _semantic_database,
    _write_page,
    importer,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    semantic_source_template as semantic_source_template,
)


async def _json_candidate_tables(database, schema, dataset_id, *, phase):
    """Check the actual protected binding, input heap, and completed resource heap."""
    binding = storage._json(
        await database.scalar(
            f'SELECT candidate_storage_json FROM "{schema}".provider_directory_endpoint_dataset '
            "WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    )
    assert binding["phase"] == phase
    keys = ("resource_input", storage.RESOURCE) if phase == "prepared" else ("resource_input",)
    relation_oids_by_name = {
        storage.RESOURCE: None,
        **{binding["tables"][key]["name"]: binding["tables"][key]["oid"] for key in keys},
    }
    for name, expected_oid in relation_oids_by_name.items():
        native = await database.first(
            "SELECT c.oid::bigint,a.atttypid='pg_catalog.json'::regtype,a.attnotnull "
            "FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid "
            "WHERE c.oid=to_regclass(:relation) AND a.attname='payload_json' AND NOT a.attisdropped",
            relation=importer._unscoped_qt(schema, name),
        )
        assert native is not None and tuple(native[1:]) == (True, True)
        assert expected_oid is None or native[0] == expected_oid
    return binding["tables"]


async def _completed_payload(database, schema, dataset_id):
    async with _completed_semantic_scope(database, dataset_id):
        tables = await _json_candidate_tables(database, schema, dataset_id, phase="prepared")
        relation = importer._unscoped_qt(schema, tables[storage.RESOURCE]["name"])
        rows = await database.all(f"SELECT payload_hash,payload_json FROM {relation}")
        assert len(rows) == 1
        return rows[0][0], storage._json(rows[0][1])


async def _assert_materialized_provenance(database, schema, dataset_id, payload_hash, payload):
    """Require the typed and canonical output to retain one complete winning observation."""
    await _materialize_semantic_dataset(database, dataset_id)
    fields = ("resource_url", "fhir_self_url", "fhir_fetch_url", "fhir_fetch_mode", "fhir_meta")
    expected_provenance_values = tuple(payload[name] for name in fields)
    async with source_transaction(database):
        typed = await database.first(
            f'SELECT {",".join(fields)},names FROM "{schema}".provider_directory_practitioner '
            "WHERE source_id=:source_id AND resource_id=:resource_id",
            source_id=SOURCE_ID,
            resource_id=RESOURCE_ID,
        )
        canonical = await database.first(
            f'SELECT {",".join(fields)},payload_hash FROM "{schema}".provider_directory_canonical_resource '
            "WHERE canonical_api_base=:base AND resource_type='Practitioner' AND resource_id=:resource_id",
            base=CANONICAL_BASE,
            resource_id=RESOURCE_ID,
        )
    assert tuple(typed[:5]) == tuple(canonical[:5]) == expected_provenance_values
    assert list(typed[5]) == payload["names"] and canonical[5] == payload_hash


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["identical", "payload", "provenance"])
async def test_native_json_completion_preserves_union_and_provenance_in_both_orders(monkeypatch, change):
    """Exercise the genuine duplicate comparison, reduction, proof, publication, and materialization."""
    first = _observation("Alex Example", page_number=1)
    second = _observation(
        "Avery Sample" if change == "payload" else "Alex Example",
        page_number=1 if change == "identical" else 2,
    )
    outputs, proofs = [], []
    async with _semantic_database(monkeypatch, resource_json_type="json") as (database, schema):
        for direction, observations in (("forward", (first, second)), ("reverse", (second, first))):
            dataset_id = f"dataset-native-json-{direction}"
            await _insert_parent(database, schema, dataset_id)
            for observation in observations:
                await _write_page(dataset_id, observation)
            async with source_transaction(database):
                tables = await _json_candidate_tables(database, schema, dataset_id, phase="loading")
                relation = importer._unscoped_qt(schema, tables["resource_input"]["name"])
                counts = await database.first(f"SELECT count(*),count(DISTINCT payload_hash) FROM {relation}")
                assert tuple(counts) == (2, 2 if change == "payload" else 1)
            outputs.append(await _completed_payload(database, schema, dataset_id))
            proof = await _proof(database, schema, dataset_id)
            proofs.append((proof.dataset_hash, proof.resource_hashes, proof.resource_counts))
            assert proof.resource_counts == {"Practitioner": 1}
            if change == "payload":
                assert proof.metadata["semantic_union"] == {
                    "added_name_count": 1,
                    "collision_identities": 1,
                    "observation_variants": 2,
                    "union_name_count": 2,
                }
        assert outputs[0] == outputs[1] and proofs[0] == proofs[1]
        payload_hash, reduced_payload = outputs[0]
        assert {name["text"] for name in reduced_payload["names"]} == (
            {"Alex Example", "Avery Sample"} if change == "payload" else {"Alex Example"}
        )
        winner_page = 1 if change == "identical" else 2
        assert reduced_payload["fhir_fetch_url"] == f"{CANONICAL_BASE}/Practitioner?page={winner_page}"
        assert reduced_payload["fhir_meta"]["lastUpdated"] == f"2026-08-09T12:00:0{winner_page}Z"
        for direction in ("forward", "reverse"):
            await _assert_materialized_provenance(
                database, schema, f"dataset-native-json-{direction}", payload_hash, reduced_payload
            )
