# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded public billing references for exact PTG provider groups."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from sqlalchemy import text

from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    encode_billing_entity_ref,
)
from api.ptg2_billing_response import attach_billing_associations
from process.ptg_parts.db_tables import _quote_ident
from process.tin_npi_connector_security import TinTokenPolicyDescriptor
from process.tin_npi_connector_support import TinNpiConnectorError


_PROVIDER_GROUP_REF_BYTES = 16
_MAX_PROVIDER_GROUPS = 2048
_TAX_IDENTITY_STATES = frozenset(
    {"matched_ein", "missing", "malformed", "unsupported_type"}
)


def _normalized_provider_group_refs(
    provider_group_refs: Iterable[str],
) -> tuple[str, ...]:
    raw_refs = tuple(provider_group_refs)
    if any(type(provider_group_ref) is not str for provider_group_ref in raw_refs):
        raise PTG2BillingAssociationDataError(
            "exact billing association scope contains an invalid provider-group reference"
        )
    normalized_refs = tuple(
        dict.fromkeys(provider_group_ref.strip().lower() for provider_group_ref in raw_refs)
    )
    if len(normalized_refs) > _MAX_PROVIDER_GROUPS:
        raise PTG2BillingAssociationDataError(
            "exact billing association scope exceeds its provider-group limit"
        )
    if any(
        len(provider_group_ref) != _PROVIDER_GROUP_REF_BYTES * 2
        or any(character not in "0123456789abcdef" for character in provider_group_ref)
        for provider_group_ref in normalized_refs
    ):
        raise PTG2BillingAssociationDataError(
            "exact billing association scope contains an invalid provider-group reference"
        )
    return normalized_refs


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


def _billing_association_query(schema_name: str):
    manifest, legacy_layout, group_identity, tax_identity = _sidecar_tables(
        schema_name
    )
    return text(
        f"""
        WITH requested(provider_group_ref) AS (
            SELECT unnest(CAST(:provider_group_refs AS bytea[]))
        ), sidecar_state AS (
            SELECT
                (SELECT COUNT(*) FROM {manifest}
                  WHERE snapshot_key = :snapshot_key) AS manifest_count,
                (SELECT COUNT(*) FROM {legacy_layout}
                  WHERE snapshot_key = :snapshot_key) AS legacy_count
        )
        SELECT encode(requested.provider_group_ref, 'hex')
                   AS provider_group_ref,
               sidecar_state.manifest_count,
               sidecar_state.legacy_count,
               manifest.contract,
               manifest.token_policy_id,
               manifest.token_policy_descriptor_sha256,
               manifest.normalization_contract,
               manifest.hmac_contract,
               association.tax_identity_state,
               identity.tin_id_128,
               identity.tin_hmac_sha256
          FROM requested
          CROSS JOIN sidecar_state
          LEFT JOIN {manifest} AS manifest
            ON manifest.snapshot_key = :snapshot_key
          LEFT JOIN {group_identity} AS association
            ON association.snapshot_key = :snapshot_key
           AND association.provider_group_global_id_128 =
               requested.provider_group_ref
          LEFT JOIN {tax_identity} AS identity
            ON identity.snapshot_key = association.snapshot_key
           AND identity.tin_key = association.tin_key
         ORDER BY requested.provider_group_ref
        """
    )


def _legacy_association(provider_group_ref: str) -> dict[str, Any]:
    return {
        "provider_group_ref": provider_group_ref,
        "tax_identity_status": "unavailable",
        "unavailable_reason": "legacy_snapshot_without_tax_identity_sidecar",
    }


def _active_association(
    association_record: Mapping[str, Any],
    *,
    provider_group_ref: str,
    snapshot_key: int,
) -> dict[str, Any]:
    tax_identity_status = association_record.get("tax_identity_state")
    if tax_identity_status not in _TAX_IDENTITY_STATES:
        raise PTG2BillingAssociationDataError(
            "sealed billing association sidecar is incomplete"
        )
    association_by_field: dict[str, Any] = {
        "provider_group_ref": provider_group_ref,
        "tax_identity_status": tax_identity_status,
    }
    raw_id = association_record.get("tin_id_128")
    raw_hmac = association_record.get("tin_hmac_sha256")
    if tax_identity_status != "matched_ein":
        if raw_id is not None or raw_hmac is not None:
            raise PTG2BillingAssociationDataError(
                "sealed billing association sidecar has an invalid unresolved token"
            )
        return association_by_field
    if raw_id is None or raw_hmac is None:
        raise PTG2BillingAssociationDataError(
            "sealed billing association sidecar is missing a matched token"
        )
    if type(raw_id) is not bytes or type(raw_hmac) is not bytes:
        raise PTG2BillingAssociationDataError(
            "sealed billing association sidecar has an invalid matched token"
        )
    association_by_field.update(
        {
            "tin_type": "ein",
            "billing_entity_ref": encode_billing_entity_ref(
                snapshot_key=snapshot_key,
                tin_id_128=raw_id,
                tin_hmac_sha256=raw_hmac,
            ),
        }
    )
    return association_by_field


def _sidecar_state(sidecar_record: Mapping[str, Any]) -> str:
    manifest_count = sidecar_record.get("manifest_count")
    legacy_count = sidecar_record.get("legacy_count")
    if type(manifest_count) is not int or type(legacy_count) is not int:
        raise PTG2BillingAssociationDataError(
            "sealed billing association sidecar state is invalid"
        )
    if manifest_count == 1 and legacy_count in {0, 1}:
        if (
            sidecar_record.get("contract")
            != "ptg2_provider_group_tax_identity_v1"
            or sidecar_record.get("normalization_contract")
            != "ein_ascii_digits_or_2_7_hyphen_v1"
            or sidecar_record.get("hmac_contract")
            != "hmac_sha256_ptg_tin_v1"
        ):
            raise PTG2BillingAssociationDataError(
                "sealed billing association sidecar contract is invalid"
            )
        try:
            descriptor_digest = sidecar_record.get(
                "token_policy_descriptor_sha256"
            )
            if type(descriptor_digest) is not bytes:
                raise TypeError("token policy digest must be bytes")
            TinTokenPolicyDescriptor(
                token_policy_id=sidecar_record.get("token_policy_id"),
                token_policy_descriptor_sha256=descriptor_digest.hex(),
            )
        except (TypeError, ValueError, TinNpiConnectorError) as exc:
            raise PTG2BillingAssociationDataError(
                "sealed billing association token policy is invalid"
            ) from exc
        return "active"
    if (manifest_count, legacy_count) == (0, 1):
        return "legacy"
    raise PTG2BillingAssociationDataError(
        "sealed billing association sidecar state is invalid"
    )


def _returned_provider_group_ref(
    association_record: Mapping[str, Any],
) -> str:
    raw_provider_group_ref = association_record.get("provider_group_ref")
    return (
        raw_provider_group_ref.lower()
        if type(raw_provider_group_ref) is str
        else ""
    )


async def load_provider_group_billing_associations(
    session,
    *,
    schema_name: str,
    snapshot_key: int,
    provider_group_refs: Iterable[str],
) -> dict[str, dict[str, Any]]:
    """Read every requested exact provider-group association in one query."""

    if type(snapshot_key) is not int or not 1 <= snapshot_key < 2**63:
        raise PTG2BillingAssociationDataError(
            "billing association snapshot key is invalid"
        )
    normalized_refs = _normalized_provider_group_refs(provider_group_refs)
    if not normalized_refs:
        return {}
    query_result = await session.execute(
        _billing_association_query(schema_name),
        {
            "snapshot_key": snapshot_key,
            "provider_group_refs": [
                bytes.fromhex(provider_group_ref)
                for provider_group_ref in normalized_refs
            ],
        },
    )
    association_records = [
        dict(association_record)
        for association_record in query_result.mappings()
    ]
    if len(association_records) != len(normalized_refs):
        raise PTG2BillingAssociationDataError(
            "sealed billing association sidecar returned incomplete rows"
        )
    association_by_group: dict[str, dict[str, Any]] = {}
    requested_refs = frozenset(normalized_refs)
    for association_record in association_records:
        provider_group_ref = _returned_provider_group_ref(association_record)
        if (
            provider_group_ref not in requested_refs
            or provider_group_ref in association_by_group
        ):
            raise PTG2BillingAssociationDataError(
                "sealed billing association sidecar returned invalid group rows"
            )
        state = _sidecar_state(association_record)
        association_by_group[provider_group_ref] = (
            _legacy_association(provider_group_ref)
            if state == "legacy"
            else _active_association(
                association_record,
                provider_group_ref=provider_group_ref,
                snapshot_key=snapshot_key,
            )
        )
    return association_by_group
