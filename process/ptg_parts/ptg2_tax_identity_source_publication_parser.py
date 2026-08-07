# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict parser for sealed tax-identity source publication metadata."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
)

_PUBLICATION_FIELDS = frozenset(
    {
        "contract",
        "content_contract",
        "binding_contract",
        "binding_vector_contract",
        "token_policy_id",
        "token_policy_descriptor_sha256",
        "source_ordinal_map_digest",
        "source_count",
        "provider_group_occurrence_count",
        "matched_ein_count",
        "missing_count",
        "malformed_count",
        "unsupported_type_count",
        "content_digest",
        "artifact_byte_count",
        "binding_vector_digest",
    }
)

__all__ = ["tax_identity_source_publication_from_metadata"]


def _publication_from_metadata_values(
    metadata_by_field: Mapping[str, Any],
) -> TaxIdentitySourcePublication:
    """Build a publication after its outer contract has been validated."""

    from process.ptg_parts.ptg2_tax_identity_source_projection import (
        _strict_int,
        _strict_policy,
        _strict_sha256,
    )

    return TaxIdentitySourcePublication(
        token_policy_id=_strict_policy(metadata_by_field.get("token_policy_id")),
        token_policy_descriptor_sha256=bytes.fromhex(
            _strict_sha256(metadata_by_field.get("token_policy_descriptor_sha256"))
        ),
        source_ordinal_map_digest=bytes.fromhex(
            _strict_sha256(metadata_by_field.get("source_ordinal_map_digest"))
        ),
        source_count=_strict_int(metadata_by_field.get("source_count"), minimum=1),
        provider_group_occurrence_count=_strict_int(
            metadata_by_field.get("provider_group_occurrence_count")
        ),
        matched_ein_count=_strict_int(metadata_by_field.get("matched_ein_count")),
        missing_count=_strict_int(metadata_by_field.get("missing_count")),
        malformed_count=_strict_int(metadata_by_field.get("malformed_count")),
        unsupported_type_count=_strict_int(
            metadata_by_field.get("unsupported_type_count")
        ),
        content_digest=bytes.fromhex(
            _strict_sha256(metadata_by_field.get("content_digest"))
        ),
        artifact_byte_count=_strict_int(metadata_by_field.get("artifact_byte_count")),
        binding_vector_digest=bytes.fromhex(
            _strict_sha256(metadata_by_field.get("binding_vector_digest"))
        ),
    )


def tax_identity_source_publication_from_metadata(
    metadata_by_field: Mapping[str, Any],
) -> TaxIdentitySourcePublication:
    """Return one canonical publication while preserving pathless failures."""

    from process.ptg_parts.ptg2_tax_identity_source_binding_vector import (
        PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT,
    )
    from process.ptg_parts.ptg2_tax_identity_source_projection import (
        PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT,
        PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT,
        PTG2_TAX_IDENTITY_SOURCE_CONTRACT,
        TaxIdentitySourceProjectionError,
        _fail,
    )

    try:
        if (
            not isinstance(metadata_by_field, Mapping)
            or set(metadata_by_field) != _PUBLICATION_FIELDS
            or metadata_by_field.get("contract")
            != PTG2_TAX_IDENTITY_SOURCE_CONTRACT
            or metadata_by_field.get("content_contract")
            != PTG2_TAX_IDENTITY_SOURCE_CONTENT_CONTRACT
            or metadata_by_field.get("binding_contract")
            != PTG2_TAX_IDENTITY_SOURCE_BINDING_CONTRACT
            or metadata_by_field.get("binding_vector_contract")
            != PTG2_TAX_IDENTITY_SOURCE_BINDING_VECTOR_CONTRACT
        ):
            raise _fail()
        return _publication_from_metadata_values(metadata_by_field)
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None
