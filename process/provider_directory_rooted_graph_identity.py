# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic identities for dormant rooted-graph acquisitions."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any, Mapping

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)


ROOTED_GRAPH_SCOPE_PATTERN = re.compile(r"pdrgs_[0-9a-f]{48}\Z")
ROOTED_GRAPH_QUERY_PATTERN = re.compile(r"pdrgq_[0-9a-f]{48}\Z")
FHIR_RESOURCE_ID_PATTERN = re.compile(r"[A-Za-z0-9\-.]{1,64}\Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_QUERY_IDENTITY_MAX_BYTES = 8192


def canonical_fhir_resource_id(candidate: object) -> str:
    """Require one relative-read-safe FHIR logical resource ID."""

    if (
        type(candidate) is not str
        or FHIR_RESOURCE_ID_PATTERN.fullmatch(candidate) is None
    ):
        raise ValueError("provider_directory_rooted_graph_resource_id_invalid")
    return candidate


def _strict_hash(candidate: object, label: str) -> str:
    if type(candidate) is not str or SHA256_PATTERN.fullmatch(candidate) is None:
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid")
    return candidate


def _strict_text(candidate: object, label: str, maximum_length: int) -> str:
    if (
        type(candidate) is not str
        or not candidate
        or len(candidate) > maximum_length
        or candidate != candidate.strip()
        or any(not character.isprintable() for character in candidate)
    ):
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid")
    return candidate


def _positive_count(candidate: object) -> int:
    if type(candidate) is not int or candidate < 1 or candidate > (1 << 63) - 1:
        raise ValueError("provider_directory_rooted_graph_root_resource_count_invalid")
    return candidate


def _bounded_positive_count(candidate: object, maximum: int, label: str) -> int:
    if type(candidate) is not int or not 0 < candidate <= maximum:
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid")
    return candidate


def _canonical_json(payload: Mapping[str, Any], label: str) -> str:
    try:
        canonical_json = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (
        MemoryError,
        OverflowError,
        RecursionError,
        TypeError,
        UnicodeError,
        ValueError,
    ):
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid") from None
    if len(canonical_json.encode("utf-8")) > _QUERY_IDENTITY_MAX_BYTES:
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid")
    return canonical_json


_SCOPE_FIELD_NAMES = (
    "root_dataset_variant",
    "root_publication_contract_id",
    "root_source_id",
    "root_endpoint_id",
    "acquisition_source_id",
    "acquisition_endpoint_id",
    "source_authority_id",
    "root_dataset_id",
    "root_dataset_hash",
    "root_content_proof_sha256",
    "root_resource_count",
    "max_work_items",
    "max_resource_rows",
    "max_edge_rows",
    "max_payload_bytes",
)
_SCOPE_BUDGET_DEFAULTS = {
    "max_work_items": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS,
    "max_resource_rows": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS,
    "max_edge_rows": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS,
    "max_payload_bytes": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES,
}


def _scope_lineage_by_field(scope_by_field: Mapping[str, object]) -> dict[str, object]:
    return {
        "acquisition_source_id": _strict_text(
            scope_by_field["acquisition_source_id"], "acquisition_source_id", 64
        ),
        "acquisition_endpoint_id": _strict_hash(
            scope_by_field["acquisition_endpoint_id"], "acquisition_endpoint_id"
        ),
        "root_content_proof_sha256": _strict_hash(
            scope_by_field["root_content_proof_sha256"],
            "root_content_proof_sha256",
        ),
        "root_dataset_variant": _strict_text(
            scope_by_field["root_dataset_variant"], "root_dataset_variant", 32
        ),
        "root_dataset_hash": _strict_hash(
            scope_by_field["root_dataset_hash"], "root_dataset_hash"
        ),
        "root_dataset_id": _strict_text(
            scope_by_field["root_dataset_id"], "root_dataset_id", 96
        ),
        "root_endpoint_id": _strict_hash(
            scope_by_field["root_endpoint_id"], "root_endpoint_id"
        ),
        "root_resource_count": _positive_count(scope_by_field["root_resource_count"]),
        "root_source_id": _strict_text(
            scope_by_field["root_source_id"], "root_source_id", 64
        ),
        "root_publication_contract_id": _strict_text(
            scope_by_field["root_publication_contract_id"],
            "root_publication_contract_id",
            96,
        ),
        "source_authority_id": _strict_text(
            scope_by_field["source_authority_id"], "source_authority_id", 64
        ),
    }


def _scope_budgets_by_field(scope_by_field: Mapping[str, object]) -> dict[str, int]:
    return {
        field_name: _bounded_positive_count(
            scope_by_field[field_name],
            maximum,
            field_name,
        )
        for field_name, maximum in _SCOPE_BUDGET_DEFAULTS.items()
    }


def _validated_scope_by_field(
    scope_by_field: Mapping[str, object],
) -> dict[str, object]:
    if set(scope_by_field) != set(_SCOPE_FIELD_NAMES):
        raise ValueError("provider_directory_rooted_graph_scope_identity_invalid")
    try:
        identity_by_field = {
            **_scope_lineage_by_field(scope_by_field),
            **_scope_budgets_by_field(scope_by_field),
            "connector_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
            "contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
            "identity_contract_id": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID
            ),
            "root_resource_type": "Practitioner",
        }
    except KeyError:
        raise ValueError(
            "provider_directory_rooted_graph_scope_identity_invalid"
        ) from None
    root_pair = (
        identity_by_field["root_source_id"],
        identity_by_field["root_endpoint_id"],
    )
    acquisition_pair = (
        identity_by_field["acquisition_source_id"],
        identity_by_field["acquisition_endpoint_id"],
    )
    is_lineage_variant_valid = (
        identity_by_field["root_dataset_variant"] == "uhc_flex_practitioner"
        and root_pair[0] != acquisition_pair[0]
        and root_pair[1] != acquisition_pair[1]
    ) or (
        identity_by_field["root_dataset_variant"] == "rooted_combined"
        and root_pair == acquisition_pair
    )
    if (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT.get(
            identity_by_field["root_dataset_variant"]
        )
        != identity_by_field["root_publication_contract_id"]
        or not is_lineage_variant_valid
        or identity_by_field["max_work_items"]
        <= identity_by_field["root_resource_count"]
    ):
        raise ValueError("provider_directory_rooted_graph_scope_identity_invalid")
    return identity_by_field


def provider_directory_rooted_graph_scope_id(**scope_by_field: object) -> str:
    """Bind one exact published root dataset to one acquisition endpoint."""

    identity_by_field = _validated_scope_by_field(scope_by_field)
    canonical_identity = _canonical_json(identity_by_field, "scope_identity")
    digest = hashlib.sha256(canonical_identity.encode("utf-8")).hexdigest()
    return "pdrgs_" + digest[:48]


def provider_directory_rooted_graph_query_id(
    scope_id: str,
    query_identity: Mapping[str, Any],
) -> str:
    """Derive a restart-safe ID from one validated query identity document."""

    if (
        type(scope_id) is not str
        or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(scope_id) is None
    ):
        raise ValueError("provider_directory_rooted_graph_scope_id_invalid")
    if type(query_identity) is not dict or not query_identity:
        raise ValueError("provider_directory_rooted_graph_query_identity_invalid")
    canonical_query = _canonical_json(query_identity, "query_identity")
    identity = "\x1f".join(
        (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID,
            scope_id,
            canonical_query,
        )
    )
    return "pdrgq_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:48]


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphScope:
    """Integrity-check the immutable root and endpoint lineage."""

    scope_id: str = field(repr=False)
    root_dataset_variant: str
    root_publication_contract_id: str = field(repr=False)
    root_source_id: str = field(repr=False)
    root_endpoint_id: str = field(repr=False)
    acquisition_source_id: str = field(repr=False)
    acquisition_endpoint_id: str = field(repr=False)
    source_authority_id: str = field(repr=False)
    root_dataset_id: str = field(repr=False)
    root_dataset_hash: str = field(repr=False)
    root_content_proof_sha256: str = field(repr=False)
    root_resource_count: int
    max_work_items: int
    max_resource_rows: int
    max_edge_rows: int
    max_payload_bytes: int
    contract_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
    connector_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
    identity_contract_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID
    root_resource_type: str = "Practitioner"

    def __post_init__(self) -> None:
        scope_by_field = {
            field_name: getattr(self, field_name) for field_name in _SCOPE_FIELD_NAMES
        }
        expected_scope_id = provider_directory_rooted_graph_scope_id(**scope_by_field)
        if (
            self.scope_id != expected_scope_id
            or self.contract_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
            or self.connector_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
            or self.identity_contract_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID
            or self.root_resource_type != "Practitioner"
        ):
            raise ValueError("provider_directory_rooted_graph_scope_inconsistent")

    def __repr__(self) -> str:
        return (
            "<provider-directory-rooted-graph-scope "
            f"root_resources={self.root_resource_count}>"
        )


def build_provider_directory_rooted_graph_scope(
    **scope_by_field: object,
) -> ProviderDirectoryRootedGraphScope:
    """Build one validated, source-neutral rooted graph scope."""

    complete_scope_by_field = {**_SCOPE_BUDGET_DEFAULTS, **scope_by_field}
    identity_by_field = _validated_scope_by_field(complete_scope_by_field)
    dataclass_by_field = {
        field_name: identity_by_field[field_name] for field_name in _SCOPE_FIELD_NAMES
    }
    scope_id = provider_directory_rooted_graph_scope_id(**dataclass_by_field)
    return ProviderDirectoryRootedGraphScope(scope_id=scope_id, **dataclass_by_field)


__all__ = (
    "build_provider_directory_rooted_graph_scope",
    "canonical_fhir_resource_id",
    "FHIR_RESOURCE_ID_PATTERN",
    "provider_directory_rooted_graph_query_id",
    "provider_directory_rooted_graph_scope_id",
    "ProviderDirectoryRootedGraphScope",
    "ROOTED_GRAPH_QUERY_PATTERN",
    "ROOTED_GRAPH_SCOPE_PATTERN",
    "SHA256_PATTERN",
)
