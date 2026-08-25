# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed unit boundaries for retained projection finalization."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.provider_directory_projection_finalizer as finalizer
import process.provider_directory_projection_finalizer_proof as finalizer_proof
import process.provider_directory_projection_finalizer_semantic as finalizer_semantic
from process.provider_directory_projection_types import (
    PhysicalProjectionProof,
    PreparedProjectionStage,
    ProjectionProofShard,
    ProjectionStage,
    ProviderDirectoryProjectionError,
    ProviderDirectoryProjectionLeaseLost,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)


def _unit_values():
    lease = synthetic_projection_context("bundle").claim.recipe_lease
    proof = PhysicalProjectionProof(
        physical_projection_id=lease.recipe.recipe_id,
        canonical_row_sha256="1" * 64,
        dataset_hash="2" * 64,
        resource_count=1,
        resource_counts={"Organization": 1},
        proof={"raw_shards": [{}], "source_summary": {}},
    )
    return lease, proof, PreparedProjectionStage(
        ProjectionStage("mrf", "stage_rows", 1), 1, proof
    )


def _sealed_recipe_fields(lease):
    identity = lease.recipe.identity_payload
    return {
        "status": "sealed",
        "attempt": lease.attempt,
        "physical_projection_id": lease.recipe.recipe_id,
        "decoder_contract_id": identity["decoder_contract_id"],
        "input_set_sha256": identity["input_set_sha256"],
        "transform_contract_id": identity["transform_contract_id"],
        "scope_contract_id": identity["scope_contract_id"],
        "transform_context_hash": identity["transform_context_hash"],
        "transform_context_json": identity["transform_context"],
        "resource_profile_hash": identity["resource_profile_hash"],
        "selected_resources_json": identity["selected_resources"],
        "required_resources_json": identity["required_resources"],
    }


def _proof_shard(lease):
    return ProjectionProofShard(
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        partition_attempt=1,
        partition_id="a" * 64,
        partition_ordinal=0,
        resource_type="Organization",
        input_sha256="b" * 64,
        canonical_row_sha256="c" * 64,
        resource_count=1,
        first_identity=("Organization", "one"),
        last_identity=("Organization", "one"),
        proof={},
    )


def _cursor_for(*rows):
    async def cursor(*_args, **_kwargs):
        for row in rows:
            yield dict(row)

    return cursor


def _stage_row(shard, resource_id):
    return {
        "proof_partition_id": shard.partition_id,
        "resource_type": "Organization",
        "resource_id": resource_id,
        "source_rank": resource_id,
        "payload_hash": "d" * 64,
        "summary_npi": None,
        "summary_address_count": 0,
        "summary_addressed_location": False,
        "summary_geocoded_location": False,
        "summary_network_link_count": 0,
        "summary_affiliation_link_count": 0,
        "profile_evidence_json": None,
    }


@pytest.mark.asyncio
async def test_finalizer_transition_counts_are_fenced(monkeypatch):
    lease, proof, prepared = _unit_values()

    async def status(statement, **_kwargs):
        return 0 if "UPDATE" in statement else 1

    async def scalar(_statement, **kwargs):
        return kwargs.get("setting_value")

    connection = SimpleNamespace(
        status=AsyncMock(side_effect=status),
        scalar=AsyncMock(side_effect=scalar),
    )
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        await finalizer._mark_proof_ready(connection, "mrf", lease, proof)

    monkeypatch.setattr(finalizer, "_insert_physical_projection", AsyncMock(return_value=1))
    monkeypatch.setattr(finalizer, "_insert_source_summary", AsyncMock(return_value=1))
    monkeypatch.setattr(finalizer, "_insert_partitions", AsyncMock(return_value=0))
    with pytest.raises(ProviderDirectoryProjectionError, match="catalog_mismatch"):
        await finalizer._insert_projection_catalog(
            connection, "mrf", lease, prepared, proof, 60
        )

    monkeypatch.setattr(finalizer, "_insert_projection_catalog", AsyncMock())
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        await finalizer._seal(connection, "mrf", lease, prepared, proof, 60)
    seal_statements = [
        call.args[0] for call in connection.status.await_args_list[-2:]
    ]
    assert "ATTACH PARTITION" in seal_statements[0]
    assert "WITH seal_clock AS" in seal_statements[1]
    assert "physical_seal AS" in seal_statements[1]
    assert "provider_directory_projection_recipe" in seal_statements[1]


@pytest.mark.asyncio
async def test_finalizer_replay_and_input_are_fenced():
    lease, _proof, _prepared = _unit_values()
    recipe_fields = _sealed_recipe_fields(lease)
    with pytest.raises(ProviderDirectoryProjectionLeaseLost, match="lease_lost"):
        await finalizer._sealed_replay(
            SimpleNamespace(), "mrf", lease, {**recipe_fields, "attempt": 2}
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="replay_invalid"):
        await finalizer._sealed_replay(
            SimpleNamespace(first=AsyncMock(return_value={})),
            "mrf",
            lease,
            recipe_fields,
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="input_invalid"):
        await finalizer.finalize_projection(object(), retain_seconds=0)


@pytest.mark.asyncio
async def test_finalizer_proof_primitives_fail_closed(monkeypatch):
    lease, proof, _prepared = _unit_values()
    with pytest.raises(ProviderDirectoryProjectionError, match="driver_missing"):
        await anext(
            finalizer_proof._cursor(
                SimpleNamespace(raw_connection=object()), "SELECT 1"
            )
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="binding_invalid"):
        finalizer_proof.projection_stage(
            {"stage_schema": "", "stage_relation": "rows", "stage_relation_oid": 1}
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="shards_incomplete"):
        await finalizer_proof.completed_shards(
            SimpleNamespace(all=AsyncMock(return_value=[])), "mrf", lease, 1
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="child_live"):
        await finalizer_proof.assert_no_live_children(
            SimpleNamespace(scalar=AsyncMock(return_value=1)), "mrf", lease
        )
    shard = _proof_shard(lease)
    with pytest.raises(ProviderDirectoryProjectionError, match="source_rank_invalid"):
        finalizer_proof._assert_native_source_rank(
            shard, {"payload_hash": "d" * 64, "source_rank": "invalid"}
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="shard_mismatch"):
        finalizer_proof._assert_partition_digest(
            shard, hashlib.sha256(), 0, None, None
        )
    monkeypatch.setattr(finalizer_proof, "_validate_bound_stage", AsyncMock())
    with pytest.raises(ProviderDirectoryProjectionError, match="trigger_invalid"):
        await finalizer_proof.prepare_immutable_stage(
            SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=None)),
            "mrf",
            ProjectionStage("mrf", "stage_rows", 1),
            proof,
        )


@pytest.mark.asyncio
async def test_stage_stream_rejects_missing_unknown_and_unsorted_shards(monkeypatch):
    lease, _proof, prepared = _unit_values()
    shard = _proof_shard(lease)
    monkeypatch.setattr(finalizer_proof, "_cursor", _cursor_for())
    with pytest.raises(ProviderDirectoryProjectionError, match="shard_mismatch"):
        await finalizer_proof.verify_stage_shards(
            object(), prepared.stage, lease, (shard,)
        )
    monkeypatch.setattr(
        finalizer_proof, "_cursor", _cursor_for({"proof_partition_id": "unknown"})
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="shard_mismatch"):
        await finalizer_proof.verify_stage_shards(
            object(), prepared.stage, lease, (shard,)
        )
    monkeypatch.setattr(
        finalizer_proof,
        "_cursor",
        _cursor_for(
            _stage_row(shard, "one"),
            _stage_row(shard, "two"),
            _stage_row(shard, "two"),
        ),
    )
    monkeypatch.setattr(finalizer_proof, "_assert_native_source_rank", lambda *_: None)
    with pytest.raises(ProviderDirectoryProjectionError, match="strictly_sorted"):
        await finalizer_proof.verify_stage_shards(
            object(), prepared.stage, lease, (shard,)
        )


@pytest.mark.asyncio
async def test_semantic_pair_empty_stream_and_npi_guards(monkeypatch):
    lease, _proof, prepared = _unit_values()
    with pytest.raises(ProviderDirectoryProjectionError, match="payload_hash_mismatch"):
        finalizer_semantic._semantic_pair(
            {"payload_json_text": "{}", "payload_hash": "0" * 64}
        )
    monkeypatch.setattr(finalizer_semantic, "_cursor", _cursor_for())
    with pytest.raises(ProviderDirectoryProjectionError, match="pair_set_mismatch"):
        await finalizer_semantic._stream_rows(object(), prepared.stage, lease)
    monkeypatch.setattr(
        finalizer_semantic,
        "_cursor",
        _cursor_for({"summary_npi": 123, "occurrence_count": 1}),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="occurrence_invalid"):
        await finalizer_semantic._stream_npis(object(), prepared.stage, lease)


@pytest.mark.asyncio
async def test_semantic_stream_order_and_group_totals_are_fenced(monkeypatch):
    lease, _proof, prepared = _unit_values()
    monkeypatch.setattr(
        finalizer_semantic,
        "_cursor",
        _cursor_for(
            {"resource_id": "one"},
            {"resource_id": "two"},
            {"resource_id": "two"},
        ),
    )
    monkeypatch.setattr(
        finalizer_semantic,
        "_semantic_pair",
        lambda row: (
            {
                "resource_type": "Organization",
                "resource_id": row["resource_id"],
                "payload_hash": "d" * 64,
            },
            {},
        ),
    )
    monkeypatch.setattr(
        finalizer_semantic, "canonical_semantic_resource_row", lambda _row: b"row"
    )
    monkeypatch.setattr(finalizer_semantic, "_record_resource_counts", lambda *_: None)
    monkeypatch.setattr(
        finalizer_semantic,
        "_record_resource_npi",
        lambda _row, total, accumulator: (total, accumulator),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="strictly_sorted"):
        await finalizer_semantic._stream_rows(object(), prepared.stage, lease)

    monkeypatch.setattr(
        finalizer_semantic,
        "_stream_rows",
        AsyncMock(
            return_value=(
                hashlib.sha256(),
                hashlib.sha256(),
                SimpleNamespace(hexdigest=lambda: "e" * 64),
                {},
                {},
                1,
                1,
                1,
            )
        ),
    )
    monkeypatch.setattr(
        finalizer_semantic,
        "_stream_npis",
        AsyncMock(return_value=(0, 1, 0, "f" * 64)),
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_mismatch"):
        await finalizer_semantic.semantic_proof(object(), prepared.stage, lease)
