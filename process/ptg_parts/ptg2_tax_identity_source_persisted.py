# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate persisted source-binding and observation seals."""

from __future__ import annotations

import hmac
from typing import Any

from db.connection import db
from process.ptg_parts.ptg2_tax_identity_source_binding_vector import (
    tax_identity_source_binding_vector_digest,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
    _fail,
    _strict_int,
)

SOURCE_BINDING_FIELDS = (
    "source_key",
    "source_type",
    "identity_kind",
    "identity_sha256",
    "token_policy_id",
    "token_policy_descriptor_sha256",
    "record_format",
    "format_version",
    "record_bytes",
    "artifact_sha256",
    "artifact_byte_count",
    "provider_group_count",
    "matched_ein_count",
    "missing_count",
    "malformed_count",
    "unsupported_type_count",
)


async def load_source_bindings(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> tuple[dict[str, object], ...]:
    """Load the exact ordered binding vector persisted for one snapshot."""

    stored_bindings = (
        await session.execute(
            db.text(f"""
                SELECT {", ".join(SOURCE_BINDING_FIELDS)}
                  FROM {schema}.ptg2_provider_tax_identity_source_binding
                 WHERE snapshot_key = :snapshot_key
                 ORDER BY source_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).all()
    return tuple(
        dict(zip(SOURCE_BINDING_FIELDS, stored_binding))
        for stored_binding in stored_bindings
    )


def validate_source_binding_seal(
    stored_binding_records: tuple[dict[str, object], ...],
    *,
    expected: TaxIdentitySourcePublication,
) -> None:
    """Require persisted source bindings to match the prepared publication."""

    stored_artifact_byte_count = sum(
        int(binding_by_field["artifact_byte_count"])
        for binding_by_field in stored_binding_records
    )
    if (
        len(stored_binding_records) != expected.source_count
        or stored_artifact_byte_count != expected.artifact_byte_count
        or not hmac.compare_digest(
            tax_identity_source_binding_vector_digest(stored_binding_records),
            expected.binding_vector_digest,
        )
    ):
        raise _fail()


async def validate_source_observation_counts(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    expected: TaxIdentitySourcePublication,
) -> None:
    """Require persisted source observations to match the manifest totals."""

    stored_counts = (
        await session.execute(
            db.text(f"""
                SELECT COUNT(*)::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'matched_ein'
                       )::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'missing'
                       )::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'malformed'
                       )::bigint,
                       COUNT(*) FILTER (
                           WHERE tax_identity_state = 'unsupported_type'
                       )::bigint
                  FROM {schema}.ptg2_provider_group_tax_identity_source
                 WHERE snapshot_key = :snapshot_key
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one()
    expected_counts = (
        expected.provider_group_occurrence_count,
        expected.matched_ein_count,
        expected.missing_count,
        expected.malformed_count,
        expected.unsupported_type_count,
    )
    if tuple(int(stored_count) for stored_count in stored_counts) != expected_counts:
        raise _fail()


__all__ = [
    "SOURCE_BINDING_FIELDS",
    "load_source_bindings",
    "validate_source_binding_seal",
    "validate_source_observation_counts",
]
