# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reusable assertions for sealed source-local PostgreSQL evidence."""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from process.ptg_parts import ptg2_tax_identity_source_projection as projection
from process.ptg_parts import ptg2_tax_identity_source_validation as validation
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_provider_tax_identity_postgres_support import quoted
from tests.ptg2_tax_identity_source_projection_fixture import (
    POLICY,
    ordinal_digest,
)


def aggregate_metadata() -> dict[str, object]:
    """Return the exact synthetic aggregate metadata sealed by the fixture."""

    return {
        "snapshot_key": 18,
        "contract": "ptg2_provider_group_tax_identity_v1",
        "token_policy_id": POLICY,
        "token_policy_descriptor_sha256": token_policy_descriptor_sha256(POLICY),
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "source_ordinal_contract": "snapshot_shard_id_sorted_lsb0_bitmap_v1",
        "source_ordinal_map": [
            {"shard_id": "shard-a", "ordinal": 0},
            {"shard_id": "shard-b", "ordinal": 1},
        ],
        "source_ordinal_map_digest": ordinal_digest(("shard-a", "shard-b")).hex(),
        "source_shard_count": 2,
        "provider_group_count": 4,
        "tax_identity_count": 2,
        "matched_ein_count": 2,
        "missing_count": 0,
        "malformed_count": 1,
        "unsupported_type_count": 1,
        "content_digest": "33" * 32,
    }


async def validate_sealed_reuse(
    schema_name: str,
    published,
    *,
    sealed_metadata=None,
    aggregate_metadata_by_field=None,
):
    """Validate the exact two-source projection without source artifacts."""

    return await validation.validate_reused_tax_identity_source_projection(
        schema_name=schema_name,
        snapshot_key=18,
        expected_bindings=(
            {
                "source_key": 0,
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "1" * 64,
            },
            {
                "source_key": 1,
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "2" * 64,
            },
        ),
        sealed_metadata=sealed_metadata or published.as_dict(),
        aggregate_metadata=(aggregate_metadata_by_field or aggregate_metadata()),
    )


async def _assert_sealed_metadata_rejections(schema_name: str, published) -> None:
    invalid_metadata_records = (
        {
            **published.as_dict(),
            "artifact_byte_count": published.artifact_byte_count + 1,
        },
        {**published.as_dict(), "binding_vector_digest": "0" * 64},
        {
            key: value
            for key, value in published.as_dict().items()
            if key != "binding_vector_digest"
        },
    )
    for invalid_metadata_by_field in invalid_metadata_records:
        with pytest.raises(
            projection.TaxIdentitySourceProjectionError,
            match="ptg2_tax_identity_source_projection_invalid",
        ):
            await validate_sealed_reuse(
                schema_name,
                published,
                sealed_metadata=invalid_metadata_by_field,
            )


async def _assert_aggregate_metadata_rejection(schema_name: str, published) -> None:
    invalid_aggregate_metadata_by_field = {
        **aggregate_metadata(),
        "source_ordinal_map": [
            {"shard_id": "shard-a", "ordinal": 0},
            {"shard_id": "shard-c", "ordinal": 1},
        ],
    }
    with pytest.raises(
        projection.TaxIdentitySourceProjectionError,
        match="ptg2_tax_identity_source_projection_invalid",
    ):
        await validate_sealed_reuse(
            schema_name,
            published,
            aggregate_metadata_by_field=invalid_aggregate_metadata_by_field,
        )


async def _tamper_stored_binding(engine, schema_name: str) -> None:
    schema = quoted(schema_name)
    binding_table = f"{schema}.ptg2_provider_tax_identity_source_binding"
    mutation_trigger = quoted(
        "ptg2_provider_tax_identity_source_binding_mutation_guard"
    )
    async with engine.begin() as connection:
        await connection.execute(sa.text(f"""
            ALTER TABLE {binding_table}
            DISABLE TRIGGER {mutation_trigger}
            """))
        await connection.execute(sa.text(f"""
            UPDATE {binding_table}
               SET artifact_sha256 = decode(repeat('aa', 32), 'hex')
             WHERE snapshot_key = 18 AND source_key = 0
            """))
        await connection.execute(sa.text(f"""
            ALTER TABLE {binding_table}
            ENABLE ALWAYS TRIGGER {mutation_trigger}
            """))


async def assert_sealed_reuse_proofs(
    engine,
    *,
    schema_name: str,
    published,
) -> None:
    """Prove exact reuse and fail-closed metadata or durable-row drift."""

    assert await validate_sealed_reuse(schema_name, published) == published
    await _assert_sealed_metadata_rejections(schema_name, published)
    await _assert_aggregate_metadata_rejection(schema_name, published)
    await _tamper_stored_binding(engine, schema_name)
    with pytest.raises(
        projection.TaxIdentitySourceProjectionError,
        match="ptg2_tax_identity_source_projection_invalid",
    ):
        await validate_sealed_reuse(schema_name, published)


__all__ = ["assert_sealed_reuse_proofs"]
