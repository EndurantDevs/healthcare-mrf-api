# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve opaque billing references to exact source-local provider groups."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hmac
from typing import Any

from sqlalchemy import text

from api.ptg2_billing_associations import _normalized_provider_group_refs
from api.ptg2_billing_entity_group_resolution import (
    _resolve_billing_entity_ref_tin_key,
)
from api.ptg2_billing_entity_refs import PTG2BillingAssociationDataError
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_binding_vector import (
    tax_identity_source_binding_vector_digest,
)
from process.ptg_parts.ptg2_tax_identity_source_persisted import (
    SOURCE_BINDING_FIELDS,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)

_MAX_SOURCE_BINDINGS = 8192
_MAX_SOURCE_WITNESSES = 8192


@dataclass(frozen=True, slots=True, repr=False)
class BillingEntitySourceWitness:
    """One internal source and provider-group witness for a billing identity."""

    source_key: int
    source_record_ordinal: int
    provider_group_ref: str

    def __repr__(self) -> str:
        return "<billing-entity-source-witness value=<redacted>>"


@dataclass(frozen=True, slots=True, repr=False)
class ResolvedBillingEntitySourceScope:
    """Bounded source-local witnesses authenticated by one opaque reference."""

    snapshot_key: int
    publication: TaxIdentitySourcePublication
    witnesses: tuple[BillingEntitySourceWitness, ...]

    @property
    def provider_group_refs(self) -> tuple[str, ...]:
        """Return stable distinct group identifiers in deterministic order."""

        return tuple(
            dict.fromkeys(witness.provider_group_ref for witness in self.witnesses)
        )

    @property
    def source_keys(self) -> tuple[int, ...]:
        """Return stable distinct source keys in deterministic order."""

        return tuple(dict.fromkeys(witness.source_key for witness in self.witnesses))

    def __repr__(self) -> str:
        return (
            "<resolved-billing-entity-source-scope "
            f"snapshot_key={self.snapshot_key} "
            f"witness_count={len(self.witnesses)}>"
        )


def _source_tables(schema_name: str) -> tuple[str, str, str, str]:
    schema = _quote_ident(schema_name)
    return tuple(
        f"{schema}.{_quote_ident(table_name)}"
        for table_name in (
            "ptg2_provider_tax_identity_source_manifest",
            "ptg2_provider_tax_identity_manifest",
            "ptg2_provider_tax_identity_source_binding",
            "ptg2_provider_group_tax_identity_source",
        )
    )


def _source_geometry_query(schema_name: str):
    source_manifest, aggregate_manifest, binding, _ = _source_tables(schema_name)
    binding_columns = ",\n".join(
        f"                   binding.{field_name} AS binding_{field_name}"
        for field_name in SOURCE_BINDING_FIELDS
    )
    return text(f"""
        WITH source_geometry AS MATERIALIZED (
            SELECT manifest.contract,
                   manifest.binding_contract,
                   manifest.token_policy_id,
                   manifest.token_policy_descriptor_sha256,
                   manifest.source_count,
                   manifest.provider_group_occurrence_count,
                   manifest.matched_ein_count,
                   manifest.missing_count,
                   manifest.malformed_count,
                   manifest.unsupported_type_count,
                   manifest.content_digest,
                   aggregate.source_shard_count AS aggregate_source_count,
                   aggregate.source_ordinal_map_digest
              FROM {source_manifest} AS manifest
              JOIN {aggregate_manifest} AS aggregate
                ON aggregate.snapshot_key = manifest.snapshot_key
             WHERE manifest.snapshot_key = :snapshot_key
        ), bounded_bindings AS MATERIALIZED (
            SELECT {", ".join(SOURCE_BINDING_FIELDS)}
              FROM {binding}
             WHERE snapshot_key = :snapshot_key
             ORDER BY source_key
             LIMIT :binding_limit
        )
        SELECT
            (SELECT COUNT(*) FROM {source_manifest}
              WHERE snapshot_key = :snapshot_key) AS manifest_count,
            (SELECT COUNT(*) FROM {aggregate_manifest}
              WHERE snapshot_key = :snapshot_key) AS aggregate_manifest_count,
            geometry.contract,
            geometry.binding_contract,
            geometry.token_policy_id,
            geometry.token_policy_descriptor_sha256,
            geometry.source_count,
            geometry.provider_group_occurrence_count,
            geometry.matched_ein_count,
            geometry.missing_count,
            geometry.malformed_count,
            geometry.unsupported_type_count,
            geometry.content_digest,
            geometry.aggregate_source_count,
            geometry.source_ordinal_map_digest,
{binding_columns}
          FROM (SELECT TRUE AS anchor) AS anchor
          LEFT JOIN source_geometry AS geometry ON TRUE
          LEFT JOIN bounded_bindings AS binding ON TRUE
         ORDER BY binding.source_key NULLS LAST
        """)


def _source_witness_query(schema_name: str):
    _, _, binding, observation = _source_tables(schema_name)
    return text(f"""
        WITH bounded_witnesses AS MATERIALIZED (
            SELECT association.source_key,
                   association.provider_group_global_id_128,
                   association.source_record_ordinal
              FROM {observation} AS association
             WHERE association.snapshot_key = :snapshot_key
               AND association.tin_key = :tin_key
               AND association.tax_identity_state = 'matched_ein'
             ORDER BY association.source_key,
                      association.provider_group_global_id_128
             LIMIT :witness_limit
        )
        SELECT witness.source_key,
               witness.source_record_ordinal,
               binding.provider_group_count AS source_provider_group_count,
               encode(
                   witness.provider_group_global_id_128,
                   'hex'
               ) AS provider_group_ref
          FROM bounded_witnesses AS witness
          JOIN {binding} AS binding
            ON binding.snapshot_key = :snapshot_key
           AND binding.source_key = witness.source_key
         ORDER BY witness.source_key,
                  witness.source_record_ordinal,
                  witness.provider_group_global_id_128
        """)


def _canonical_source_publication(
    expected: object,
) -> TaxIdentitySourcePublication:
    """Require the canonical strict representation of one sealed publication."""

    if type(expected) is not TaxIdentitySourcePublication:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )
    try:
        canonical_expected = tax_identity_source_publication_from_metadata(
            expected.as_dict()
        )
    except Exception:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        ) from None
    if canonical_expected != expected:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )
    if canonical_expected.source_count > _MAX_SOURCE_BINDINGS:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope exceeds its source limit"
        )
    return canonical_expected


_GEOMETRY_FIELDS = (
    "manifest_count",
    "aggregate_manifest_count",
    "contract",
    "binding_contract",
    "token_policy_id",
    "token_policy_descriptor_sha256",
    "source_count",
    "provider_group_occurrence_count",
    "matched_ein_count",
    "missing_count",
    "malformed_count",
    "unsupported_type_count",
    "content_digest",
    "aggregate_source_count",
    "source_ordinal_map_digest",
)


def _persisted_geometry(source_row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(source_row.get(field_name) for field_name in _GEOMETRY_FIELDS)


def _expected_geometry(
    publication: TaxIdentitySourcePublication,
) -> tuple[Any, ...]:
    return (
        1,
        1,
        PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
        PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
        publication.token_policy_id,
        publication.token_policy_descriptor_sha256,
        publication.source_count,
        publication.provider_group_occurrence_count,
        publication.matched_ein_count,
        publication.missing_count,
        publication.malformed_count,
        publication.unsupported_type_count,
        publication.content_digest,
        publication.source_count,
        publication.source_ordinal_map_digest,
    )


def _source_binding_records(
    source_rows: tuple[Mapping[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    records: list[dict[str, Any]] = []
    for source_row in source_rows:
        values = tuple(
            source_row.get(f"binding_{field_name}")
            for field_name in SOURCE_BINDING_FIELDS
        )
        if all(value is None for value in values):
            continue
        records.append(dict(zip(SOURCE_BINDING_FIELDS, values, strict=True)))
    return tuple(records)


def _validate_source_bindings(
    source_rows: tuple[Mapping[str, Any], ...],
    *,
    expected: TaxIdentitySourcePublication,
) -> None:
    binding_records = _source_binding_records(source_rows)
    try:
        binding_digest = tax_identity_source_binding_vector_digest(binding_records)
        artifact_byte_count = sum(
            int(binding_record["artifact_byte_count"])
            for binding_record in binding_records
        )
    except Exception:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        ) from None
    if (
        len(binding_records) != expected.source_count
        or artifact_byte_count != expected.artifact_byte_count
        or not hmac.compare_digest(binding_digest, expected.binding_vector_digest)
    ):
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )


def _validated_source_geometry(
    source_rows: tuple[Mapping[str, Any], ...],
    *,
    expected: TaxIdentitySourcePublication,
) -> TaxIdentitySourcePublication:
    """Bind durable source tables to the exact trusted sealed geometry."""

    if not source_rows:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope returned no state"
        )
    geometry = _persisted_geometry(source_rows[0])
    if any(_persisted_geometry(source_row) != geometry for source_row in source_rows):
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is inconsistent"
        )
    if geometry != _expected_geometry(expected):
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )
    _validate_source_bindings(source_rows, expected=expected)
    return expected


def _validated_source_publication(
    source_rows: tuple[Mapping[str, Any], ...],
    *,
    expected: object,
) -> TaxIdentitySourcePublication:
    """Preserve the #430 validation hook over the stronger geometry proof."""

    return _validated_source_geometry(
        source_rows,
        expected=_canonical_source_publication(expected),
    )


def _source_witness_from_row(
    source_state_row: Mapping[str, Any],
    *,
    source_count: int,
) -> BillingEntitySourceWitness:
    """Validate one source-local provider-group observation."""

    source_key = source_state_row.get("source_key")
    source_record_ordinal = source_state_row.get("source_record_ordinal")
    source_provider_group_count = source_state_row.get("source_provider_group_count")
    normalized_group_refs = _normalized_provider_group_refs(
        (source_state_row.get("provider_group_ref"),)
    )
    if (
        type(source_key) is not int
        or not 0 <= source_key < source_count
        or type(source_record_ordinal) is not int
        or type(source_provider_group_count) is not int
        or not 0 <= source_record_ordinal < source_provider_group_count
        or len(normalized_group_refs) != 1
    ):
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope contains an invalid witness"
        )
    return BillingEntitySourceWitness(
        source_key=source_key,
        source_record_ordinal=source_record_ordinal,
        provider_group_ref=normalized_group_refs[0],
    )


def _normalized_source_witnesses(
    source_state_rows: tuple[Mapping[str, Any], ...],
    *,
    source_count: int,
) -> tuple[BillingEntitySourceWitness, ...]:
    """Require bounded, unique, canonically ordered source witnesses."""

    witnesses = [
        _source_witness_from_row(source_state_row, source_count=source_count)
        for source_state_row in source_state_rows
    ]
    if not witnesses:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope contains no witnesses"
        )
    if len(witnesses) > _MAX_SOURCE_WITNESSES:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope exceeds its witness limit"
        )
    ordered_witnesses = tuple(
        sorted(
            witnesses,
            key=lambda witness: (
                witness.source_key,
                witness.source_record_ordinal,
                witness.provider_group_ref,
            ),
        )
    )
    source_record_coordinates = {
        (witness.source_key, witness.source_record_ordinal) for witness in witnesses
    }
    source_group_coordinates = {
        (witness.source_key, witness.provider_group_ref) for witness in witnesses
    }
    if (
        tuple(witnesses) != ordered_witnesses
        or len(source_record_coordinates) != len(witnesses)
        or len(source_group_coordinates) != len(witnesses)
    ):
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope contains inconsistent witnesses"
        )
    return ordered_witnesses


async def resolve_billing_entity_ref_source_scope(
    session,
    *,
    schema_name: str,
    snapshot_key: int,
    billing_entity_ref: object,
    source_publication: TaxIdentitySourcePublication,
) -> ResolvedBillingEntitySourceScope | None:
    """Resolve one ref to exact source/group witnesses in a sealed snapshot."""

    publication = _canonical_source_publication(source_publication)
    tin_key = await _resolve_billing_entity_ref_tin_key(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        billing_entity_ref=billing_entity_ref,
    )
    geometry_result = await session.execute(
        _source_geometry_query(schema_name),
        {
            "snapshot_key": snapshot_key,
            "binding_limit": publication.source_count + 1,
        },
    )
    geometry_rows = tuple(
        dict(geometry_row) for geometry_row in geometry_result.mappings()
    )
    publication = _validated_source_geometry(
        geometry_rows,
        expected=publication,
    )
    if tin_key is None:
        return None
    witness_result = await session.execute(
        _source_witness_query(schema_name),
        {
            "snapshot_key": snapshot_key,
            "tin_key": tin_key,
            "witness_limit": _MAX_SOURCE_WITNESSES + 1,
        },
    )
    witnesses = _normalized_source_witnesses(
        tuple(dict(witness_row) for witness_row in witness_result.mappings()),
        source_count=publication.source_count,
    )
    return ResolvedBillingEntitySourceScope(
        snapshot_key=snapshot_key,
        publication=publication,
        witnesses=witnesses,
    )


__all__ = [
    "BillingEntitySourceWitness",
    "ResolvedBillingEntitySourceScope",
    "resolve_billing_entity_ref_source_scope",
]
