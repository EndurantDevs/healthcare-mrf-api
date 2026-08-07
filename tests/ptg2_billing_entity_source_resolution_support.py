# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic records for source-local billing-reference reader tests."""

from __future__ import annotations

from typing import Any

from api import ptg2_billing_associations as billing
from process.ptg_parts.ptg2_tax_identity_source_binding_vector import (
    tax_identity_source_binding_vector_digest,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
)
from process.tin_npi_connector_security import token_policy_descriptor_sha256

POLICY_ID = "ptg-tin-hmac-sha256-v1:2026-07"
POLICY_DESCRIPTOR = bytes.fromhex(token_policy_descriptor_sha256(POLICY_ID))
SNAPSHOT_KEY = 41


class QueryResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


class QuerySession:
    def __init__(self, *responses: list[dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), dict(parameters)))
        if not self.responses:
            raise AssertionError("unexpected database query")
        return QueryResult(self.responses.pop(0))


def candidate_row(*, tin_key: int | None, full_hmac: bytes | None):
    return {
        "manifest_count": 1,
        "legacy_count": 0,
        "layout_count": 1,
        "root_count": 1,
        "contract": "ptg2_provider_group_tax_identity_v1",
        "token_policy_id": POLICY_ID,
        "token_policy_descriptor_sha256": POLICY_DESCRIPTOR,
        "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "tin_key": tin_key,
        "tin_hmac_sha256": full_hmac,
    }


def legacy_candidate_row() -> dict[str, Any]:
    return {
        **candidate_row(tin_key=None, full_hmac=None),
        "manifest_count": 0,
        "legacy_count": 1,
        "contract": None,
        "token_policy_id": None,
        "token_policy_descriptor_sha256": None,
        "normalization_contract": None,
        "hmac_contract": None,
    }


def source_binding(
    source_key: int,
    *,
    provider_group_count: int = 2,
    matched_ein_count: int = 1,
    missing_count: int = 1,
) -> dict[str, Any]:
    artifact_byte_count = (
        13 + len(POLICY_ID.encode("ascii")) + provider_group_count * 65
    )
    return {
        "source_key": source_key,
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": str(source_key + 1) * 64,
        "token_policy_id": POLICY_ID,
        "token_policy_descriptor_sha256": POLICY_DESCRIPTOR,
        "record_format": "ptg2_provider_group_tax_identity_v1",
        "format_version": 1,
        "record_bytes": 65,
        "artifact_sha256": bytes((source_key + 1,)) * 32,
        "artifact_byte_count": artifact_byte_count,
        "provider_group_count": provider_group_count,
        "matched_ein_count": matched_ein_count,
        "missing_count": missing_count,
        "malformed_count": 0,
        "unsupported_type_count": 0,
    }


def default_bindings() -> tuple[dict[str, Any], ...]:
    return (
        source_binding(0, matched_ein_count=2, missing_count=0),
        source_binding(1),
    )


def source_publication(
    bindings: tuple[dict[str, Any], ...] | None = None,
) -> TaxIdentitySourcePublication:
    binding_records = bindings or default_bindings()
    return TaxIdentitySourcePublication(
        token_policy_id=POLICY_ID,
        token_policy_descriptor_sha256=POLICY_DESCRIPTOR,
        source_ordinal_map_digest=b"o" * 32,
        source_count=len(binding_records),
        provider_group_occurrence_count=sum(
            binding["provider_group_count"] for binding in binding_records
        ),
        matched_ein_count=sum(
            binding["matched_ein_count"] for binding in binding_records
        ),
        missing_count=sum(binding["missing_count"] for binding in binding_records),
        malformed_count=0,
        unsupported_type_count=0,
        content_digest=b"c" * 32,
        artifact_byte_count=sum(
            binding["artifact_byte_count"] for binding in binding_records
        ),
        binding_vector_digest=tax_identity_source_binding_vector_digest(
            binding_records
        ),
    )


def _geometry_state(
    publication: TaxIdentitySourcePublication,
) -> dict[str, Any]:
    return {
        "manifest_count": 1,
        "aggregate_manifest_count": 1,
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "token_policy_id": publication.token_policy_id,
        "token_policy_descriptor_sha256": (
            publication.token_policy_descriptor_sha256
        ),
        "source_count": publication.source_count,
        "provider_group_occurrence_count": (
            publication.provider_group_occurrence_count
        ),
        "matched_ein_count": publication.matched_ein_count,
        "missing_count": publication.missing_count,
        "malformed_count": publication.malformed_count,
        "unsupported_type_count": publication.unsupported_type_count,
        "content_digest": publication.content_digest,
        "aggregate_source_count": publication.source_count,
        "source_ordinal_map_digest": publication.source_ordinal_map_digest,
    }


def geometry_rows(
    publication: TaxIdentitySourcePublication,
    bindings: tuple[dict[str, Any], ...] | None = None,
    **state_overrides: Any,
) -> list[dict[str, Any]]:
    state_by_field = {**_geometry_state(publication), **state_overrides}
    binding_records = default_bindings() if bindings is None else bindings
    if not binding_records:
        return [state_by_field]
    return [
        {
            **state_by_field,
            **{
                f"binding_{field_name}": field_value
                for field_name, field_value in binding.items()
            },
        }
        for binding in binding_records
    ]


def witness_row(
    source_key: Any,
    group_ref: Any,
    *,
    source_record_ordinal: Any = 0,
    source_provider_group_count: Any = 2,
) -> dict[str, Any]:
    return {
        "source_key": source_key,
        "source_record_ordinal": source_record_ordinal,
        "source_provider_group_count": source_provider_group_count,
        "provider_group_ref": group_ref,
    }


def billing_reference(full_hmac: bytes, snapshot_key: int = SNAPSHOT_KEY) -> str:
    return billing.encode_billing_entity_ref(
        snapshot_key=snapshot_key,
        tin_id_128=full_hmac[:16],
        tin_hmac_sha256=full_hmac,
    )


def binding_drift_rows(
    case: str,
    publication: TaxIdentitySourcePublication,
) -> list[dict[str, Any]]:
    bindings = [dict(binding) for binding in default_bindings()]
    if case == "missing":
        bindings.pop()
    if case == "extra":
        bindings.append(dict(bindings[-1]))
    if case == "non-dense":
        bindings[-1]["source_key"] = 2
    if case == "artifact-count":
        bindings[-1]["artifact_byte_count"] += 1
    if case == "durable-field":
        bindings[-1]["identity_sha256"] = "9" * 64
    return geometry_rows(publication, tuple(bindings))


def bounded_witness_rows(count: int) -> tuple[dict[str, Any], ...]:
    return tuple(
        witness_row(
            0,
            f"{ordinal:032x}",
            source_record_ordinal=ordinal,
            source_provider_group_count=count,
        )
        for ordinal in range(count)
    )
