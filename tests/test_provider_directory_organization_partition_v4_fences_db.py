# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL tamper and rollback fences for v4 Organization partitions."""

from __future__ import annotations

import json

import pytest

from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    _dataset_and_shard_counts,
    _insert_parent,
    _semantic_database,
    importer,
)
from tests.test_provider_directory_organization_partition_v4_db import (
    DATASET_ID,
    _SplitPartitionEndpoint,
    _context,
    _organization,
    _partition_counts,
    _resume,
    _run_partition,
    _source,
)
from tests.provider_directory_organization_union_v4_postgres_support import (
    stored_dataset_row,
)


async def _insert_v4_parent(database, schema: str) -> None:
    """Create the exact semantic-v4 Organization parent row."""

    await _insert_parent(
        database,
        schema,
        DATASET_ID,
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        selected_resources=("Organization",),
    )


def _completed_plan():
    """Return the single-leaf plan used by the stable endpoint."""

    source_by_field = _source()
    config, error = importer._last_updated_partition_config(
        source_by_field,
        "Organization",
    )
    assert config is not None and error is None
    plan = _resume(config).plan
    plan.observe_count("root", importer.CountObservation.exact(2))
    return plan


async def _mutate_candidate(database, schema: str, target: str) -> None:
    """Alter one retained candidate field without updating its proof."""

    if target == "acquired_hash":
        await database.status(
            f'UPDATE "{schema}".provider_directory_dataset_resource '
            "SET acquired_resource_sha256=:value WHERE dataset_id=:dataset_id "
            "AND resource_type='Organization';",
            value="f" * 64,
            dataset_id=DATASET_ID,
        )
        return
    record = await database.first(
        f'SELECT payload_json FROM "{schema}".'
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='Organization';",
        dataset_id=DATASET_ID,
    )
    payload_by_field = dict(record[0])
    payload_by_field["active"] = False
    await database.status(
        f'UPDATE "{schema}".provider_directory_dataset_resource '
        "SET payload_json=CAST(:payload_json AS jsonb) "
        "WHERE dataset_id=:dataset_id AND resource_type='Organization';",
        payload_json=json.dumps(payload_by_field),
        dataset_id=DATASET_ID,
    )


async def _mutate_occurrence(
    database,
    schema: str,
    tamper_target: str,
) -> None:
    """Alter one pass-two occurrence commitment in durable storage."""

    proof_row = await database.first(
        f'SELECT resource_id, payload_json FROM "{schema}".'
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='LU:Organization:pass:2' ORDER BY resource_id "
        "LIMIT 1;",
        dataset_id=DATASET_ID,
    )
    if tamper_target == "proof_hash":
        await database.status(
            f'UPDATE "{schema}".provider_directory_dataset_resource '
            "SET payload_hash=:value WHERE dataset_id=:dataset_id "
            "AND resource_type='LU:Organization:pass:2' "
            "AND resource_id=:resource_id;",
            value="f" * 64,
            dataset_id=DATASET_ID,
            resource_id=proof_row[0],
        )
        return
    payload_by_field = dict(proof_row[1])
    payload_by_field["source_fingerprint"] = "f" * 64
    await database.status(
        f'UPDATE "{schema}".provider_directory_dataset_resource '
        "SET payload_json=CAST(:payload_json AS jsonb) "
        "WHERE dataset_id=:dataset_id "
        "AND resource_type='LU:Organization:pass:2' "
        "AND resource_id=:resource_id;",
        payload_json=json.dumps(payload_by_field),
        dataset_id=DATASET_ID,
        resource_id=proof_row[0],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target",
    ("candidate_payload", "acquired_hash", "source_fingerprint", "proof_hash"),
)
async def test_postgres_v4_partition_rejects_tamper(monkeypatch, target) -> None:
    """Reject retained or occurrence drift from one consistent DB snapshot."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_v4_parent(database, schema)
        result, _batches, _endpoint, _save_plan = await _run_partition(
            monkeypatch
        )
        assert result.complete is True
        await importer._assert_last_updated_partition_candidate_proof(
            _context(),
            "Organization",
            _completed_plan(),
            SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        )
        if target in {"candidate_payload", "acquired_hash"}:
            await _mutate_candidate(database, schema, target)
        else:
            await _mutate_occurrence(database, schema, target)
        with pytest.raises(RuntimeError, match="staged_dataset_mismatch"):
            await importer._assert_last_updated_partition_candidate_proof(
                _context(),
                "Organization",
                _completed_plan(),
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
            )


@pytest.mark.asyncio
async def test_postgres_v4_partition_rolls_back_pass_two(monkeypatch) -> None:
    """Rollback candidate, shard, and occurrence marker with checkpoint failure."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_v4_parent(database, schema)

        async def fail_completed_plan(
            _context_by_field,
            _resource_type,
            _config,
            plan,
            **_options,
        ):
            if plan.status is importer.PlanStatus.SUCCEEDED:
                raise RuntimeError("partition checkpoint failed")

        with pytest.raises(RuntimeError, match="checkpoint failed"):
            await _run_partition(
                monkeypatch,
                save_callback=fail_completed_plan,
            )
        assert await _partition_counts(database, schema) == (0, 2, 0)
        assert await _dataset_and_shard_counts(
            database,
            schema,
            DATASET_ID,
        ) == (2, 0)


class _DriftPartitionEndpoint(_SplitPartitionEndpoint):
    """Change a non-name semantic field in the second leaf."""

    def _resources(self, bounds):
        resources = super()._resources(bounds)
        if bounds[0].startswith("ge2024-01-02"):
            return ({**resources[0], "active": False},)
        return resources


@pytest.mark.asyncio
async def test_postgres_v4_partition_rejects_non_name_drift(monkeypatch) -> None:
    """Fail the second leaf without overwriting the first retained payload."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_v4_parent(database, schema)
        with pytest.raises(
            ValueError,
            match="organization_identity_payload_conflict",
        ):
            await _run_partition(
                monkeypatch,
                source_by_field=_source(ceiling=1, page_count=1),
                endpoint=_DriftPartitionEndpoint(),
            )
        assert await _partition_counts(database, schema) == (1, 2, 1)
        _payload_hash, payload_by_field = await stored_dataset_row(
            database,
            schema,
            DATASET_ID,
        )
        assert payload_by_field["active"] is True
        assert payload_by_field["name_variants"] == [
            "Community Health Center"
        ]
