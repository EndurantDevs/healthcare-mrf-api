# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve opaque PTG billing references to exact snapshot-local groups."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from api.ptg2_billing_associations import (
    _normalized_provider_group_refs,
    _sidecar_state,
)
from api.ptg2_billing_entity_refs import (
    DecodedBillingEntityRef,
    PTG2BillingAssociationDataError,
    decode_billing_entity_ref,
    is_billing_ref_valid_for_token,
)
from process.ptg_parts.db_tables import _quote_ident

_MAX_BILLING_REF_CANDIDATES = 8
_MAX_PROVIDER_GROUPS = 2048


@dataclass(frozen=True, slots=True, repr=False)
class ResolvedBillingEntityGroupScope:
    """Snapshot-local candidate groups authenticated by one opaque billing ref.

    This scope is not source-local rate authorization. Callers must still
    intersect each group with the exact rate occurrence and source witness.
    """

    snapshot_key: int
    provider_group_refs: tuple[str, ...]

    def __repr__(self) -> str:
        return (
            "<resolved-billing-entity-group-scope "
            f"snapshot_key={self.snapshot_key} "
            f"provider_group_count={len(self.provider_group_refs)}>"
        )


def _sidecar_tables(schema_name: str) -> tuple[str, str, str, str]:
    schema = _quote_ident(schema_name)
    return tuple(
        f"{schema}.{_quote_ident(table_name)}"
        for table_name in (
            "ptg2_provider_tax_identity_manifest",
            "ptg2_provider_tax_identity_legacy_layout",
            "ptg2_provider_group_tax_identity",
            "ptg2_provider_tax_identity",
        )
    )


def _billing_ref_candidates_query(schema_name: str):
    manifest, legacy_layout, _, tax_identity = _sidecar_tables(schema_name)
    schema = _quote_ident(schema_name)
    layout = f"{schema}.{_quote_ident('ptg2_v3_snapshot_layout')}"
    root = f"{schema}.{_quote_ident('ptg2_v4_snapshot_map_root')}"
    return text(f"""
        WITH sidecar_state AS (
            SELECT
                (SELECT COUNT(*) FROM {manifest}
                  WHERE snapshot_key = :snapshot_key) AS manifest_count,
                (SELECT COUNT(*) FROM {legacy_layout}
                  WHERE snapshot_key = :snapshot_key) AS legacy_count,
                (SELECT COUNT(*) FROM {layout}
                  WHERE snapshot_key = :snapshot_key
                    AND generation = 'shared_blocks_v4'
                    AND state = 'sealed') AS layout_count,
                (SELECT COUNT(*) FROM {root}
                  WHERE snapshot_key = :snapshot_key
                    AND state = 'complete') AS root_count
        ), candidates AS (
            SELECT identity.tin_key,
                   identity.tin_hmac_sha256
              FROM {tax_identity} AS identity
             WHERE identity.snapshot_key = :snapshot_key
               AND identity.tin_id_128 = :tin_id_128
             ORDER BY identity.tin_hmac_sha256,
                      identity.tin_key
             LIMIT :candidate_limit
        )
        SELECT sidecar_state.manifest_count,
               sidecar_state.legacy_count,
               sidecar_state.layout_count,
               sidecar_state.root_count,
               manifest.contract,
               manifest.token_policy_id,
               manifest.token_policy_descriptor_sha256,
               manifest.normalization_contract,
               manifest.hmac_contract,
               candidates.tin_key,
               candidates.tin_hmac_sha256
          FROM sidecar_state
          LEFT JOIN {manifest} AS manifest
            ON manifest.snapshot_key = :snapshot_key
          LEFT JOIN candidates ON TRUE
         ORDER BY candidates.tin_hmac_sha256 NULLS LAST,
                  candidates.tin_key NULLS LAST
        """)


def _billing_ref_groups_query(schema_name: str):
    _, _, group_identity, _ = _sidecar_tables(schema_name)
    return text(f"""
        SELECT encode(association.provider_group_global_id_128, 'hex')
                   AS provider_group_ref
          FROM {group_identity} AS association
         WHERE association.snapshot_key = :snapshot_key
           AND association.tin_key = :tin_key
           AND association.tax_identity_state = 'matched_ein'
         ORDER BY association.provider_group_global_id_128
         LIMIT :provider_group_limit
        """)


def _sealed_sidecar_state(
    candidate_records: tuple[Mapping[str, Any], ...],
) -> str:
    """Validate the repeated sidecar contract and completed V4 root."""

    if not candidate_records:
        raise PTG2BillingAssociationDataError(
            "sealed billing reference lookup returned no state"
        )
    states: set[str] = set()
    for candidate_record in candidate_records:
        states.add(_sidecar_state(candidate_record))
        if (
            candidate_record.get("layout_count") != 1
            or candidate_record.get("root_count") != 1
        ):
            raise PTG2BillingAssociationDataError(
                "sealed billing reference snapshot is not complete"
            )
    if len(states) != 1:
        raise PTG2BillingAssociationDataError(
            "sealed billing reference sidecar state is inconsistent"
        )
    return states.pop()


def _verified_tin_key(
    candidate_records: tuple[Mapping[str, Any], ...],
    *,
    decoded_reference: DecodedBillingEntityRef,
    snapshot_key: int,
) -> int | None:
    """Select exactly one collision-safe token without returning token bytes."""

    state = _sealed_sidecar_state(candidate_records)
    token_candidates: list[tuple[int, bytes]] = []
    for candidate_record in candidate_records:
        tin_key = candidate_record.get("tin_key")
        full_hmac = candidate_record.get("tin_hmac_sha256")
        if tin_key is None and full_hmac is None:
            continue
        if (
            type(tin_key) is not int
            or tin_key < 0
            or type(full_hmac) is not bytes
            or len(full_hmac) != 32
        ):
            raise PTG2BillingAssociationDataError(
                "sealed billing reference candidate is invalid"
            )
        token_candidates.append((tin_key, full_hmac))
    if state == "legacy":
        if token_candidates:
            raise PTG2BillingAssociationDataError(
                "legacy billing reference sidecar contains token candidates"
            )
        return None
    if len(token_candidates) > _MAX_BILLING_REF_CANDIDATES:
        raise PTG2BillingAssociationDataError(
            "sealed billing reference locator exceeds its collision limit"
        )
    if len({tin_key for tin_key, _ in token_candidates}) != len(token_candidates):
        raise PTG2BillingAssociationDataError(
            "sealed billing reference candidates are inconsistent"
        )
    verified_tin_keys = [
        tin_key
        for tin_key, full_hmac in token_candidates
        if is_billing_ref_valid_for_token(
            decoded_reference,
            snapshot_key=snapshot_key,
            tin_hmac_sha256=full_hmac,
        )
    ]
    if len(verified_tin_keys) > 1:
        raise PTG2BillingAssociationDataError(
            "sealed billing reference resolved ambiguously"
        )
    return verified_tin_keys[0] if verified_tin_keys else None


def _returned_provider_group_ref(group_record: Mapping[str, Any]) -> str:
    raw_provider_group_ref = group_record.get("provider_group_ref")
    return raw_provider_group_ref.lower() if type(raw_provider_group_ref) is str else ""


def _resolved_group_refs(
    group_records: Iterable[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Validate one exact identity's bounded, stable provider-group refs."""

    raw_refs = tuple(
        _returned_provider_group_ref(group_record) for group_record in group_records
    )
    if len(raw_refs) > _MAX_PROVIDER_GROUPS:
        raise PTG2BillingAssociationDataError(
            "sealed billing reference scope exceeds its provider-group limit"
        )
    normalized_refs = _normalized_provider_group_refs(raw_refs)
    if not normalized_refs or len(normalized_refs) != len(raw_refs):
        raise PTG2BillingAssociationDataError(
            "sealed billing reference returned invalid provider-group rows"
        )
    return normalized_refs


async def resolve_billing_entity_ref_group_scope(
    session,
    *,
    schema_name: str,
    snapshot_key: int,
    billing_entity_ref: object,
) -> ResolvedBillingEntityGroupScope | None:
    """Resolve one EIN-backed ref to candidate groups in one sealed snapshot.

    The result deliberately omits rate/source authorization. It must not be
    used to serve prices without an exact source-occurrence intersection.
    Callers must first resolve authentication, plan entitlement, and a trusted
    immutable snapshot; this low-level reader does not establish those facts.
    """

    if type(snapshot_key) is not int or not 1 <= snapshot_key < 2**63:
        raise PTG2BillingAssociationDataError(
            "billing association snapshot key is invalid"
        )
    decoded_reference = decode_billing_entity_ref(billing_entity_ref)
    candidate_result = await session.execute(
        _billing_ref_candidates_query(schema_name),
        {
            "snapshot_key": snapshot_key,
            "tin_id_128": decoded_reference.tin_id_128,
            "candidate_limit": _MAX_BILLING_REF_CANDIDATES + 1,
        },
    )
    candidate_records = tuple(
        dict(candidate_record) for candidate_record in candidate_result.mappings()
    )
    tin_key = _verified_tin_key(
        candidate_records,
        decoded_reference=decoded_reference,
        snapshot_key=snapshot_key,
    )
    if tin_key is None:
        return None
    group_result = await session.execute(
        _billing_ref_groups_query(schema_name),
        {
            "snapshot_key": snapshot_key,
            "tin_key": tin_key,
            "provider_group_limit": _MAX_PROVIDER_GROUPS + 1,
        },
    )
    provider_group_refs = _resolved_group_refs(
        dict(group_record) for group_record in group_result.mappings()
    )
    return ResolvedBillingEntityGroupScope(
        snapshot_key=snapshot_key,
        provider_group_refs=provider_group_refs,
    )
