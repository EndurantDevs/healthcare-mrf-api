# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed normalized native collection options, separate from policy authority."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sanic.exceptions import InvalidUsage

from api.provider_detail_network_contract import _decimal, _selector

if TYPE_CHECKING:
    from process.registry_network_catalog_read import RegistryNetworkCatalogLegacyBatchSelector

MAX_COLLECTION_QUERY_BYTES = 1024 * 1024
MAX_NATIVE_QUERY_BYTES = 16 * 1024
_FIELDS = {"kind", "native_query", "native_batch", "selector", "network_generation"}
_SERVICE_NATIVE_FIELDS = frozenset(
    "billing_code_modifier city classification code code_system cursor expand_codes include_allowed_amounts "
    "include_code_details include_debug include_details include_evidence include_legacy_fields include_providers "
    "include_sources include_subspecialties include_unverified_addresses lat limit long min_claims min_total_cost "
    "mode modifier modifiers negotiated_rate negotiated_rate_tolerance npi offset order order_by page place_of_service "
    "plan_release_id pos primary_only provider_sex_code q radius radius_miles rate rate_tolerance service_code specialty "
    "state taxonomy_classification taxonomy_code taxonomy_codes taxonomy_section taxonomy_specialization view year "
    "zip5 zip_radius_miles plan_id plan_external_id plan_id_type plan_market_type market_type source_key snapshot_id".split()
)
_NETWORK_FIELDS = {
    "network_ids",
    "network_generation",
    "plan_network",
    "plan_network_checksum",
    "network_checksum",
    "checksum",
}


@dataclass(frozen=True)
class ProviderCollectionNativeBatch:
    npis: tuple[str, ...]
    address_limit: int
    address_offset: int
    include_sources: bool
    include_evidence: bool

    def document(self):
        """Restore the bounded native batch wire without exposing mutable DTO state."""
        return {
            "npis": list(self.npis),
            "address_limit": self.address_limit,
            "address_offset": self.address_offset,
            "include_sources": self.include_sources,
            "include_evidence": self.include_evidence,
        }


@dataclass(frozen=True)
class ProviderCollectionNetworkQuery:
    """Immutable input options; selectors never convey authority themselves."""

    kind: str
    native_query: tuple[tuple[str, str | tuple[str, ...]], ...]
    native_batch: ProviderCollectionNativeBatch | None
    selector: tuple[int, ...] | RegistryNetworkCatalogLegacyBatchSelector | None
    network_generation: int | None


def _native_document(kind, document):
    from api.custom_import_provider_geo import _GEO_QUERY_FIELDS
    from api.custom_import_provider_http import _PROVIDER_QUERY_FIELDS

    fields = _GEO_QUERY_FIELDS if kind == "geo" else _PROVIDER_QUERY_FIELDS
    if kind == "by-service":
        fields = _SERVICE_NATIVE_FIELDS
    elif kind == "group-plan":
        from api.provider_group_plan_network_read import GROUP_PLAN_NATIVE_FIELDS

        fields = GROUP_PLAN_NATIVE_FIELDS
    elif kind == "billing-search":
        from api.provider_billing_network_read import BILLING_SEARCH_NATIVE_FIELDS

        fields = BILLING_SEARCH_NATIVE_FIELDS
    fields = fields - _NETWORK_FIELDS
    fields = fields | (
        {
            "count_only",
            "format",
            "response_format",
            "address_limit",
            "address_offset",
            "include_address_total",
            "extra_info",
            "address_grouping",
        }
        if kind == "list"
        else set()
    )
    fields = fields | ({"count_only"} if kind == "geo" else set())
    fields = fields | ({"extra_info", "address_grouping"} if kind == "batch" else set())
    if type(document) is not dict or not set(document) <= fields:
        raise ValueError("managed_collection_query_invalid")
    byte_count = 0
    for name, field_value in document.items():
        strings = field_value if name == "name_like" and kind == "list" and type(field_value) is list else [field_value]
        if not 1 <= len(strings) <= 16 or any(
            type(text) is not str or len(text) > 2048 or "\x00" in text for text in strings
        ):
            raise ValueError("managed_collection_query_invalid")
        byte_count += len(name.encode("utf-8")) + sum(len(text.encode("utf-8")) for text in strings)
    if byte_count > MAX_NATIVE_QUERY_BYTES:
        raise ValueError("managed_collection_query_invalid")
    return {
        name: tuple(field_value) if type(field_value) is list else field_value for name, field_value in document.items()
    }


def _validate_service_native_query(native_by_name):
    """Keep ordinary service pagination and domain normalization with its handler."""
    from api.endpoint.pagination import _parse_non_negative_int, parse_pagination

    if not any(native_by_name.get(name, "").strip() for name in ("q", "code")):
        raise ValueError("managed_collection_query_invalid")
    try:
        limit = _parse_non_negative_int(native_by_name.get("limit"), "limit")
        if limit is not None and limit > 200:
            raise ValueError("managed_collection_query_invalid")
        parse_pagination(native_by_name, default_limit=25, max_limit=200)
    except InvalidUsage as error:
        raise ValueError("managed_collection_query_invalid") from error


def _validate_release_native_query(kind, native_by_name):
    from api.plan_release_serving import normalize_plan_release_id

    if normalize_plan_release_id(native_by_name.get("plan_release_id")) is None:
        raise ValueError("managed_collection_query_invalid")
    if kind == "billing-search":
        from api.custom_import_billing_http import _parse_billing_native_query

        billing_by_name = dict(native_by_name)
        digest = billing_by_name.pop("billing_transport_context_sha256", None)
        if type(digest) is not str or len(digest) != 64 or set(digest) - set("0123456789abcdef"):
            raise ValueError("managed_collection_query_invalid")
        try:
            _parse_billing_native_query(billing_by_name)
        except Exception as error:
            raise ValueError("managed_collection_query_invalid") from error
        return
    for name, maximum in (("limit", 1000), ("cursor", 9999999999)):
        raw = native_by_name.get(name)
        if raw not in (None, "") and (not raw.isascii() or not raw.isdecimal() or not 1 <= int(raw) <= maximum):
            raise ValueError("managed_collection_query_invalid")


def _native_batch(document):
    from api.provider_batch import normalize_native_batch_request

    if type(document) is not dict:
        raise ValueError("managed_collection_query_invalid")
    try:
        params_by_name = normalize_native_batch_request(document)
    except InvalidUsage as error:
        raise ValueError("managed_collection_query_invalid") from error
    return ProviderCollectionNativeBatch(
        tuple(str(npi).zfill(10) for npi in params_by_name["npis"]),
        params_by_name["address_limit"],
        params_by_name["address_offset"],
        params_by_name["include_sources"],
        params_by_name["include_evidence"],
    )


def parse_managed_collection_query(document):
    """Close the wire shape while preserving native paging, counts and formats."""
    if type(document) is not dict or not {"kind", "native_query"} <= set(document) <= _FIELDS:
        raise ValueError("managed_collection_query_invalid")
    kind = document["kind"]
    if (
        type(kind) is not str
        or kind not in {"list", "geo", "batch", "by-service", "group-plan", "billing-search"}
        or (kind == "batch") != ("native_batch" in document)
    ):
        raise ValueError("managed_collection_query_invalid")
    if len(json.dumps(document, ensure_ascii=False, allow_nan=False).encode("utf-8")) > MAX_COLLECTION_QUERY_BYTES:
        raise ValueError("managed_collection_query_invalid")
    native_by_name = _native_document(kind, document["native_query"])
    if kind == "by-service":
        _validate_service_native_query(native_by_name)
    elif kind in {"group-plan", "billing-search"}:
        _validate_release_native_query(kind, native_by_name)
    if kind == "batch":
        from api.provider_batch import parse_native_batch_query

        try:
            args = parse_native_batch_query(document["native_query"])
        except InvalidUsage as error:
            raise ValueError("managed_collection_query_invalid") from error
        native_by_name = _native_document(kind, {name: args.get(name) for name in args})
    generation = (
        _decimal(document["network_generation"], 9223372036854775807, minimum=1)
        if "network_generation" in document
        else None
    )
    return ProviderCollectionNetworkQuery(
        kind,
        tuple(sorted(native_by_name.items())),
        _native_batch(document["native_batch"]) if kind == "batch" else None,
        _selector(document["selector"]) if "selector" in document else None,
        generation,
    )
