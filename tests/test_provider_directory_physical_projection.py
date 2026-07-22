# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import json

import pytest

import process.provider_directory_physical_projection as projection_facade
from process.provider_directory_projection_contract import (
    _basic_physical_projection_proof,
    projection_proof_shard,
)
from process.provider_directory_projection_db import (
    assert_stage_trigger,
    set_local_projection_synchronous_commit,
    set_local_projection_wal_compression,
)
from process.provider_directory_projection_types import (
    PhysicalProjectionProof,
    PreparedProjectionStage,
    ProjectionStage,
)
from process.provider_directory_physical_projection import (
    PROJECTION_MIXED_RESOURCE_TYPE,
    ProviderDirectoryProjectionError,
    projection_completeness_manifest,
    projection_recipe_identity,
)


class _WalCompressionDatabase:
    def __init__(self, *, permitted: bool = True, supports_zstd: bool = True):
        self.permitted = permitted
        self.supports_zstd = supports_zstd
        self.status_statements: list[str] = []

    async def scalar(self, statement: str):
        if "server_version_num" in statement:
            return 180002
        if "has_parameter_privilege" in statement:
            return self.permitted
        if "ANY(enumvals)" in statement:
            return self.supports_zstd
        if "current_setting('wal_compression')" in statement:
            return "pglz"
        raise AssertionError(statement)

    async def status(self, statement: str):
        self.status_statements.append(statement)


class _SynchronousCommitDatabase:
    def __init__(self, current_value: str):
        self.current_value = current_value
        self.status_statements: list[str] = []

    async def scalar(self, statement: str):
        assert "current_setting('synchronous_commit')" in statement
        return self.current_value

    async def status(self, statement: str):
        self.status_statements.append(statement)


class _StageCatalogDatabase:
    def __init__(self, trigger_fields):
        self.trigger_fields = trigger_fields

    async def first(self, statement: str, **parameters):
        assert "pg_trigger" in statement
        assert parameters["relation_oid"] == 123
        return self.trigger_fields


def _prepared_stage() -> PreparedProjectionStage:
    return PreparedProjectionStage(
        stage=ProjectionStage("mrf", "projection_stage", 123),
        storage_trigger_oid=456,
        proof=PhysicalProjectionProof(
            physical_projection_id="a" * 64,
            canonical_row_sha256="b" * 64,
            dataset_hash="c" * 64,
            resource_count=1,
            resource_counts={"Organization": 1},
            proof={"complete": True},
        ),
    )


def _exact_stage_trigger_fields():
    return {
        "trigger_oid": 456,
        "relation_name": "projection_stage",
        "relation_schema": "mrf",
        "relation_kind": "r",
        "function_name": "reject_provider_directory_projection_stage_mutation",
        "function_schema": "mrf",
        "tgenabled": "O",
        "tgtype": 31,
        "has_no_when": True,
        "tgnargs": 0,
        "tgoldtable": None,
        "tgnewtable": None,
        "trigger_columns": "",
        "trigger_definition": (
            "CREATE TRIGGER provider_directory_projection_stage_immutable "
            "BEFORE INSERT OR DELETE OR UPDATE ON mrf.projection_stage "
            "FOR EACH ROW EXECUTE FUNCTION "
            "mrf.reject_provider_directory_projection_stage_mutation()"
        ),
    }


def test_projection_stage_trigger_requires_exact_catalog_identity():
    asyncio.run(
        assert_stage_trigger(
            _StageCatalogDatabase(_exact_stage_trigger_fields()),
            _prepared_stage(),
        )
    )

    for field_name, invalid_value in (
        ("relation_name", "replacement_stage"),
        ("relation_schema", "other_schema"),
        ("relation_kind", "v"),
        ("has_no_when", False),
        ("tgnargs", 1),
        ("trigger_columns", "1"),
    ):
        trigger_fields = _exact_stage_trigger_fields()
        trigger_fields[field_name] = invalid_value
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="stage_trigger_mismatch",
        ):
            asyncio.run(
                assert_stage_trigger(
                    _StageCatalogDatabase(trigger_fields),
                    _prepared_stage(),
                )
            )


def test_projection_synchronous_commit_is_validated_after_set():
    database = _SynchronousCommitDatabase("on")

    asyncio.run(set_local_projection_synchronous_commit(database, "on"))

    assert database.status_statements == ["SET LOCAL synchronous_commit = on;"]
    with pytest.raises(ProviderDirectoryProjectionError, match="durability_invalid"):
        asyncio.run(set_local_projection_synchronous_commit(database, "off"))
    with pytest.raises(ProviderDirectoryProjectionError, match="durability_invalid"):
        asyncio.run(set_local_projection_synchronous_commit(database, "remote_apply"))


def test_projection_wal_compression_is_bounded_and_privilege_aware(monkeypatch):
    database = _WalCompressionDatabase()
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
        "auto",
    )
    assert asyncio.run(set_local_projection_wal_compression(database)) == "zstd"
    assert database.status_statements == ["SET LOCAL wal_compression = zstd;"]

    fallback = _WalCompressionDatabase(supports_zstd=False)
    assert asyncio.run(set_local_projection_wal_compression(fallback)) == "on"
    assert fallback.status_statements == ["SET LOCAL wal_compression = on;"]

    unavailable = _WalCompressionDatabase(permitted=False)
    assert asyncio.run(set_local_projection_wal_compression(unavailable)) is None
    assert unavailable.status_statements == []

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
        "on",
    )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="wal_compression_unavailable",
    ):
        asyncio.run(set_local_projection_wal_compression(unavailable))


def test_projection_wal_compression_rejects_invalid_configuration(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_WAL_COMPRESSION",
        "sometimes",
    )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="wal_compression_invalid",
    ):
        asyncio.run(set_local_projection_wal_compression(_WalCompressionDatabase()))


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _rows(resource_type: str, *resource_ids: str):
    return [
        {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "payload_hash": _digest(f"{resource_type}:{resource_id}"),
            "payload_json": {"resourceType": resource_type, "id": resource_id},
            "source_rank": f"00000000:{resource_id}",
            "summary_npi": None,
            "summary_address_count": 0,
            "summary_addressed_location": False,
            "summary_geocoded_location": False,
            "summary_network_link_count": 0,
            "summary_affiliation_link_count": 0,
        }
        for resource_id in resource_ids
    ]


def _recipe(
    *,
    source_ids=("source-b", "source-a"),
    required=("Organization",),
    input_label="input-set",
    transform_contract_id="provider-directory-normalization-v3",
):
    selected = ("Practitioner", "Organization")
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=_digest("endpoint-campaign"),
        partition_strategy_contract_id="test-partitions.v1",
        selected_resources=selected,
        required_resources=required,
        terminal_partitions=(
            {
                "resource_type": resource_type,
                "partition_key_hash": _digest(f"partition:{resource_type}"),
                "terminal": True,
                "block_count": 0,
                "row_count": 0,
                "byte_count": 0,
                "zero_row_proof_sha256": _digest(f"zero:{resource_type}"),
            }
            for resource_type in selected
        ),
        complete=True,
    )
    return projection_recipe_identity(
        decoder_contract_id="fhir-r4-plan-net-decoder.v1",
        acquisition_adapter_id="fhir-rest-r4.v1",
        input_set_sha256=_digest(input_label),
        source_ids=source_ids,
        transform_contract_id=transform_contract_id,
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-21",
            "time_rule_contract_id": "fhir-time-rules.v1",
        },
        selected_resources=selected,
        completeness_manifest=completeness,
        required_resources=required,
    )


def test_recipe_identity_is_canonical_and_predecode_reusable():
    first = _recipe()
    second = _recipe(source_ids=("source-a", "source-b", "source-a"))

    assert first == second
    assert first.source_ids == ("source-a", "source-b")
    assert first.selected_resources == ("Organization", "Practitioner")
    assert len(first.recipe_id) == 64
    assert json.dumps(first.identity_payload, sort_keys=True)


def test_logical_source_is_binding_only_while_resource_profile_is_physical():
    baseline = _recipe()
    other_source = _recipe(source_ids=("source-c",))
    optional_profile = _recipe(required=())

    assert baseline.recipe_id == other_source.recipe_id
    assert baseline.source_scope_hash != other_source.source_scope_hash
    assert baseline.recipe_id != optional_profile.recipe_id
    assert baseline.resource_profile_hash != optional_profile.resource_profile_hash


def test_recipe_reuse_requires_exact_artifact_and_transform_contract_hashes():
    baseline = _recipe()
    exact_replay = _recipe(source_ids=("source-a", "source-b"))
    different_artifact = _recipe(input_label="different-input-set")
    different_transform = _recipe(transform_contract_id="normalization-v4")

    assert baseline.recipe_id == exact_replay.recipe_id
    assert baseline.recipe_id != different_artifact.recipe_id
    assert baseline.recipe_id != different_transform.recipe_id


def test_public_facade_exposes_only_structural_projection_foundation():
    structural_symbols = {
        "PROJECTION_ADMISSION_CONTRACT_ID",
        "PROJECTION_INPUT_BLOCK_MAX_BYTES",
        "PROJECTION_INPUT_BLOCK_MAX_DOCUMENTS",
        "PROJECTION_INPUT_BLOCK_MAX_RESOURCES",
        "PROJECTION_MIXED_RESOURCE_TYPE",
        "PROJECTION_RECIPE_CONTRACT_ID",
        "PhysicalProjectionRecipeIdentity",
        "ProjectionAdmissionIdentity",
        "ProjectionAdmissionInputBlock",
        "ProjectionAdmissionTerminalZero",
        "ProjectionClaim",
        "ProjectionCompletenessManifest",
        "ProjectionInputBlock",
        "ProjectionLease",
        "ProjectionRecipeIdentity",
        "ProjectionShardClaim",
        "ProjectionShardSpec",
        "ProviderDirectoryProjectionBusy",
        "ProviderDirectoryProjectionError",
        "ProviderDirectoryProjectionLeaseLost",
        "claim_projection_recipe",
        "claim_projection_shard",
        "heartbeat_projection_lease",
        "heartbeat_projection_shard",
        "projection_admission_consumer_id",
        "projection_admission_identity",
        "projection_admission_input_block",
        "projection_admission_terminal_zero",
        "projection_completeness_manifest",
        "projection_input_block",
        "projection_input_set_sha256",
        "projection_recipe_identity",
        "projection_shard_spec",
        "register_projection_admission",
        "register_projection_workset",
        "validated_physical_projection_recipe_identity",
        "validated_projection_recipe_identity",
    }
    candidate_symbols = {
        "PROJECTION_CONTENT_HASH_CONTRACT_ID",
        "PROJECTION_PROOF_SHARD_CONTRACT_ID",
        "PhysicalProjectionProof",
        "ProjectionProofShard",
        "canonical_row_digest",
        "canonical_row_line",
        "physical_projection_proof",
        "prepare_projection_proof_shard",
        "projection_proof_shard",
        "projection_reducer_proof",
        "reduced_physical_projection_proof",
    }

    assert set(projection_facade.__all__) == structural_symbols
    assert all(
        not hasattr(projection_facade, symbol) for symbol in candidate_symbols
    )


def test_candidate_projection_identity_covers_content_counts_and_scope():
    recipe = _recipe()
    organization = projection_proof_shard(
        _rows("Organization", "o-1", "o-2"),
        recipe=recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type="Organization",
        input_sha256=_digest("organization-input"),
    )
    practitioner = projection_proof_shard(
        _rows("Practitioner", "p-1"),
        recipe=recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type="Practitioner",
        input_sha256=_digest("practitioner-input"),
    )

    proof = _basic_physical_projection_proof(
        recipe,
        [practitioner, organization],
        dataset_hash=_digest("dataset"),
    )
    replay = _basic_physical_projection_proof(
        recipe,
        [organization, practitioner],
        dataset_hash=_digest("dataset"),
    )

    assert proof == replay
    assert proof.resource_count == 3
    assert proof.resource_counts == {"Organization": 2, "Practitioner": 1}
    assert "source_scope_hash" not in proof.proof
    assert proof.proof["decoder_contract_id"] == recipe.decoder_contract_id
    assert proof.proof["input_set_sha256"] == recipe.input_set_sha256
    assert proof.proof["transform_contract_id"] == recipe.transform_contract_id
    assert proof.proof["resource_profile_hash"] == recipe.resource_profile_hash


def test_candidate_mixed_resource_shard_reports_exact_type_counts():
    recipe = _recipe()
    mixed = projection_proof_shard(
        [
            *_rows("Organization", "o-1"),
            *_rows("Practitioner", "p-1", "p-2"),
        ],
        recipe=recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type=PROJECTION_MIXED_RESOURCE_TYPE,
        input_sha256=_digest("mixed-input"),
    )

    proof = _basic_physical_projection_proof(
        recipe,
        [mixed],
        dataset_hash=_digest("mixed-dataset"),
    )

    assert mixed.proof["resource_counts"] == {
        "Organization": 1,
        "Practitioner": 2,
    }
    assert mixed.first_identity == ("Organization", "o-1")
    assert mixed.last_identity == ("Practitioner", "p-2")
    assert proof.resource_counts == {
        "Organization": 1,
        "Practitioner": 2,
    }


def test_candidate_projection_rejects_cross_recipe_or_incomplete_profile():
    recipe = _recipe()
    other_recipe = _recipe(input_label="other-input-set")
    organization = projection_proof_shard(
        _rows("Organization", "o-1"),
        recipe=other_recipe,
        attempt=1,
        partition_ordinal=0,
        resource_type="Organization",
        input_sha256=_digest("organization-input"),
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="shard_recipe_mismatch",
    ):
        _basic_physical_projection_proof(
            recipe,
            [organization],
            dataset_hash=_digest("dataset"),
        )


def test_required_resource_must_be_selected():
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="required_resource_not_selected",
    ):
        projection_recipe_identity(
            decoder_contract_id="decoder.v1",
            acquisition_adapter_id="adapter.v1",
            input_set_sha256=_digest("input"),
            source_ids=("source",),
            transform_contract_id="transform",
            scope_contract_id="healthporta.provider-directory.global-scope.v1",
            transform_context={
                "as_of_date": "2026-07-21",
                "time_rule_contract_id": "time.v1",
            },
            selected_resources=("Organization",),
            completeness_manifest=projection_completeness_manifest(
                endpoint_campaign_hash=_digest("campaign"),
                partition_strategy_contract_id="partition.v1",
                selected_resources=("Organization",),
                required_resources=(),
                terminal_partitions=(
                    {
                        "resource_type": "Organization",
                        "partition_key_hash": _digest("partition"),
                        "terminal": True,
                        "block_count": 0,
                        "row_count": 0,
                        "byte_count": 0,
                        "zero_row_proof_sha256": _digest("zero"),
                    },
                ),
                complete=True,
            ),
            required_resources=("Practitioner",),
        )
