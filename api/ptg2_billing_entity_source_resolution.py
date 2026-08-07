# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve opaque billing references to exact source-local provider groups."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from api.ptg2_billing_associations import _normalized_provider_group_refs
from api.ptg2_billing_entity_group_resolution import (
    _resolve_billing_entity_ref_tin_key,
)
from api.ptg2_billing_entity_refs import PTG2BillingAssociationDataError
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
    PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)

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
        """Return stable distinct source ordinals in deterministic order."""

        return tuple(dict.fromkeys(witness.source_key for witness in self.witnesses))

    def __repr__(self) -> str:
        return (
            "<resolved-billing-entity-source-scope "
            f"snapshot_key={self.snapshot_key} "
            f"witness_count={len(self.witnesses)}>"
        )


def _source_witness_query(schema_name: str):
    schema = _quote_ident(schema_name)
    manifest = f"{schema}.ptg2_provider_tax_identity_source_manifest"
    binding = f"{schema}.ptg2_provider_tax_identity_source_binding"
    observation = f"{schema}.ptg2_provider_group_tax_identity_source"
    return text(f"""
        WITH witnesses AS (
            SELECT association.source_key,
                   association.source_record_ordinal,
                   binding.provider_group_count AS source_provider_group_count,
                   encode(
                       association.provider_group_global_id_128,
                       'hex'
                   ) AS provider_group_ref
              FROM {observation} AS association
              JOIN {binding} AS binding
                ON binding.snapshot_key = association.snapshot_key
               AND binding.source_key = association.source_key
             WHERE association.snapshot_key = :snapshot_key
               AND association.tin_key = :tin_key
               AND association.tax_identity_state = 'matched_ein'
             ORDER BY association.source_key,
                      association.source_record_ordinal,
                      association.provider_group_global_id_128
             LIMIT :witness_limit
        )
        SELECT
            (SELECT COUNT(*) FROM {manifest}
              WHERE snapshot_key = :snapshot_key) AS manifest_count,
            manifest.contract,
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
            witnesses.source_key,
            witnesses.source_record_ordinal,
            witnesses.source_provider_group_count,
            witnesses.provider_group_ref
          FROM (SELECT TRUE AS anchor) AS anchor
          LEFT JOIN {manifest} AS manifest
            ON manifest.snapshot_key = :snapshot_key
          LEFT JOIN witnesses ON TRUE
         ORDER BY witnesses.source_key NULLS LAST,
                  witnesses.source_record_ordinal NULLS LAST,
                  witnesses.provider_group_ref NULLS LAST
        """)


def _canonical_source_publication(
    expected: TaxIdentitySourcePublication,
) -> TaxIdentitySourcePublication:
    """Require one canonical sealed source publication."""

    if type(expected) is not TaxIdentitySourcePublication:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )
    try:
        canonical_expected = tax_identity_source_publication_from_metadata(
            expected.as_dict()
        )
    except TaxIdentitySourceProjectionError as exc:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        ) from exc
    if canonical_expected != expected:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )
    return expected


_SOURCE_STATE_FIELDS = (
    "manifest_count",
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
)


def _persisted_source_state(source_state_row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(source_state_row.get(field) for field in _SOURCE_STATE_FIELDS)


def _validated_source_publication(
    source_state_rows: tuple[Mapping[str, Any], ...],
    *,
    expected: TaxIdentitySourcePublication,
) -> TaxIdentitySourcePublication:
    """Bind persisted source state to the exact sealed source geometry."""

    if not source_state_rows:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope returned no state"
        )
    expected = _canonical_source_publication(expected)
    states = {
        _persisted_source_state(source_state_row)
        for source_state_row in source_state_rows
    }
    if len(states) != 1:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is inconsistent"
        )
    expected_state = (
        1,
        PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
        PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
        expected.token_policy_id,
        expected.token_policy_descriptor_sha256,
        expected.source_count,
        expected.provider_group_occurrence_count,
        expected.matched_ein_count,
        expected.missing_count,
        expected.malformed_count,
        expected.unsupported_type_count,
        expected.content_digest,
    )
    if states.pop() != expected_state:
        raise PTG2BillingAssociationDataError(
            "sealed billing source scope is unavailable"
        )
    return expected


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

    tin_key = await _resolve_billing_entity_ref_tin_key(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        billing_entity_ref=billing_entity_ref,
    )
    source_query_result = await session.execute(
        _source_witness_query(schema_name),
        {
            "snapshot_key": snapshot_key,
            "tin_key": tin_key,
            "witness_limit": _MAX_SOURCE_WITNESSES + 1,
        },
    )
    source_state_rows = tuple(
        dict(source_state_row) for source_state_row in source_query_result.mappings()
    )
    publication = _validated_source_publication(
        source_state_rows,
        expected=source_publication,
    )
    if tin_key is None:
        return None
    witnesses = _normalized_source_witnesses(
        source_state_rows,
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
