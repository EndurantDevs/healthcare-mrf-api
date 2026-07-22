# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small SQL contracts for the native Provider Directory benchmark."""

import hashlib

from process.provider_directory_projection_contract import (
    projection_completeness_manifest,
    projection_input_block,
    projection_input_set_sha256,
    projection_recipe_identity,
    projection_shard_spec,
)
from process.provider_directory_projection_copy_summary import (
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
)


RESOURCE_TYPES = (
    "Endpoint",
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)
STATUS_RESOURCE_TYPES = frozenset({"Endpoint", "InsurancePlan", "Location"})


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def retained_payload(shard_ordinal: int, row_count: int) -> bytes:
    """Build a compact deterministic eight-resource retained NDJSON block."""

    prefix = f"s{shard_ordinal}-"
    payload = bytearray()
    for row_ordinal in range(row_count):
        resource_id = f"{prefix}{row_ordinal:06d}"
        resource_type = RESOURCE_TYPES[row_ordinal % len(RESOURCE_TYPES)]
        encoded_identity = (
            b'"id":"' + resource_id.encode("ascii")
            + b'","resourceType":"' + resource_type.encode("ascii") + b'"'
        )
        encoded_resource = (
            b"{" + encoded_identity + b',"status":"active"}\n'
            if resource_type in STATUS_RESOURCE_TYPES
            else b'{"active":true,' + encoded_identity + b"}\n"
        )
        payload.extend(encoded_resource)
    return bytes(payload)


def _terminal_census(payload: bytes, shard_ordinal: int, row_count: int):
    return tuple(
        {
            "resource_type": resource_type,
            "partition_key_hash": _digest(
                f"partition:{shard_ordinal}:{resource_type}"
            ),
            "source_partition_ordinal": shard_ordinal,
            "terminal_cursor_hmac": _digest(
                f"cursor:{shard_ordinal}:{resource_type}"
            ),
            "terminal": True,
            "block_count": 1,
            "row_count": row_count // len(RESOURCE_TYPES),
            "byte_count": len(payload),
            "zero_row_proof_sha256": None,
        }
        for resource_type in RESOURCE_TYPES
    )


def _input_block(payload: bytes, shard_ordinal: int, row_count: int):
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    return projection_input_block(
        upstream_artifact_id=_digest(f"artifact:{shard_ordinal}"),
        source_object_id=_digest(f"layout:{shard_ordinal}"),
        block_kind="ndjson",
        input_contract_id="synthetic-offline-fhir-ndjson.v1",
        record_start=0,
        record_count=row_count,
        content_sha256=payload_sha256,
        payload_sha256=payload_sha256,
        payload_bytes=len(payload),
        summary={"resource_count": row_count},
    )


def _recipe(block, payload: bytes, shard_ordinal: int, row_count: int):
    completeness = projection_completeness_manifest(
        endpoint_campaign_hash=_digest(f"campaign:{shard_ordinal}"),
        partition_strategy_contract_id="synthetic-offline-partition.v1",
        selected_resources=RESOURCE_TYPES,
        required_resources=RESOURCE_TYPES,
        terminal_partitions=_terminal_census(payload, shard_ordinal, row_count),
        complete=True,
    )
    return projection_recipe_identity(
        decoder_contract_id=NATIVE_DECODER_CONTRACT_ID,
        acquisition_adapter_id="synthetic-offline-retained.v1",
        input_set_sha256=projection_input_set_sha256(
            (block,), decoder_contract_id=NATIVE_DECODER_CONTRACT_ID
        ),
        source_ids=(f"synthetic-shard-{shard_ordinal}",),
        transform_contract_id=NATIVE_TRANSFORM_CONTRACT_ID,
        scope_contract_id="healthporta.provider-directory.global-scope.v1",
        transform_context={
            "as_of_date": "2026-07-22",
            "time_rule_contract_id": "synthetic-offline-time-rules.v1",
        },
        selected_resources=RESOURCE_TYPES,
        required_resources=RESOURCE_TYPES,
        completeness_manifest=completeness,
    )


def build_benchmark_coordinates(
    retained_bytes: bytes,
    shard_ordinal: int,
    row_count: int,
):
    """Build source-free claim and child coordinates for one retained block."""

    input_block = _input_block(retained_bytes, shard_ordinal, row_count)
    recipe_identity = _recipe(
        input_block,
        retained_bytes,
        shard_ordinal,
        row_count,
    )
    recipe_lease = ProjectionLease(
        recipe_identity.physical, 1, _digest(f"recipe-lease:{shard_ordinal}")
    )
    shard_spec = projection_shard_spec(
        recipe=recipe_identity,
        partition_ordinal=shard_ordinal,
        input_block=input_block,
    )
    shard_claim = ProjectionShardClaim(
        recipe_lease,
        _digest(f"admission:{shard_ordinal}"),
        shard_spec,
        1,
        _digest(f"shard-lease:{shard_ordinal}"),
    )
    payload_sha256 = hashlib.sha256(retained_bytes).hexdigest()
    child_lease = ProjectionRetainedChildLease(
        shard_claim=shard_claim,
        binding_id=_digest(f"binding:{shard_ordinal}"),
        block_id=input_block.block_id,
        retained_campaign_id=_digest(f"retained-campaign:{shard_ordinal}"),
        retained_campaign_sha256=_digest(f"campaign-proof:{shard_ordinal}"),
        retained_consumer_recipe_id="synthetic-benchmark-consumer",
        retained_claim_generation=1,
        retained_source_item_id=_digest(f"source-item:{shard_ordinal}"),
        retained_artifact_sha256=input_block.upstream_artifact_id,
        retained_layout_sha256=input_block.source_object_id,
        retained_range_ordinal=0,
        artifact_byte_count=len(retained_bytes),
        raw_byte_start=0,
        expected_byte_count=len(retained_bytes),
        expected_record_count=row_count,
        input_sha256=payload_sha256,
        expected_payload_sha256=payload_sha256,
        child_generation=1,
        child_lease_token=_digest(f"child-lease:{shard_ordinal}"),
    )
    return shard_claim, child_lease


def projection_stage_ddl(schema: str, relation: str) -> str:
    """Return the exact 18-column logged staging relation used by the benchmark."""

    return f'''
        CREATE TABLE "{schema}"."{relation}" (
            physical_projection_id varchar(64) NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            proof_partition_id varchar(64) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            source_rank text NOT NULL,
            summary_npi bigint,
            summary_address_count integer NOT NULL,
            summary_addressed_location boolean NOT NULL,
            summary_geocoded_location boolean NOT NULL,
            summary_network_link_count integer NOT NULL,
            summary_affiliation_link_count integer NOT NULL,
            active boolean,
            effective_start varchar(64),
            effective_end varchar(64),
            observed_at varchar(64),
            profile_evidence_json jsonb
        )
    '''


__all__ = (
    "build_benchmark_coordinates",
    "projection_stage_ddl",
    "retained_payload",
)
