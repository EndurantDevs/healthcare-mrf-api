# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed managed provider detail inputs, separate from trusted policy authority."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from process.registry_network_catalog_read import RegistryNetworkCatalogLegacyBatchSelector

_BOOLEAN_FIELDS = ("include_sources", "include_evidence", "debug", "extra_info")
_UUID_FIELDS = ("address_key", "address_site_key")
_NATIVE_FIELDS = {*_BOOLEAN_FIELDS, *_UUID_FIELDS, "address_grouping", "address_limit", "address_offset"}
_FIELDS = {"npi", "include", "selector", "network_generation", *_NATIVE_FIELDS}


@dataclass(frozen=True)
class ProviderDetailNetworkQuery:
    """Normalized detail options; selectors confer no read authority themselves."""

    npi: str
    include: tuple[str, ...]
    native_args: tuple[tuple[str, str], ...]
    selector: tuple[int, ...] | RegistryNetworkCatalogLegacyBatchSelector | None
    network_generation: int | None


def _decimal(value, maximum, *, minimum=0):
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]{0,18}", value) is None:
        raise ValueError("managed_detail_query_invalid")
    number = int(value)
    if not minimum <= number <= maximum:
        raise ValueError("managed_detail_query_invalid")
    return number


def _native_args(document):
    args_by_name = {name: document[name] for name in _NATIVE_FIELDS if name in document}
    for name in _BOOLEAN_FIELDS:
        if name in args_by_name and (
            type(args_by_name[name]) is not str or args_by_name[name] not in {"true", "false"}
        ):
            raise ValueError("managed_detail_query_invalid")
    for name in _UUID_FIELDS:
        if name in args_by_name:
            uuid_text = args_by_name[name]
            if type(uuid_text) is not str or len(uuid_text) != 36 or str(UUID(uuid_text)) != uuid_text:
                raise ValueError("managed_detail_query_invalid")
    grouping = args_by_name.get("address_grouping", "flat")
    if type(grouping) is not str or grouping not in {"flat", "premise"}:
        raise ValueError("managed_detail_query_invalid")
    limit = args_by_name.get("address_limit", "5")
    if type(limit) is not str:
        raise ValueError("managed_detail_query_invalid")
    if limit != "all":
        limit = str(_decimal(limit, 1000))
        if limit == "0":
            limit = "all"
    args_by_name["address_limit"] = limit
    if "address_offset" in args_by_name:
        _decimal(args_by_name["address_offset"], 1_000_000)
        if limit == "all":
            raise ValueError("managed_detail_query_invalid")
    if grouping == "premise" and (
        any(name in args_by_name for name in _UUID_FIELDS) or limit == "all" or int(limit) > 5
    ):
        raise ValueError("managed_detail_query_invalid")
    return tuple(sorted(args_by_name.items()))


def _selector(document):
    if type(document) is not dict or set(document) != {"namespace", "values"}:
        raise ValueError("managed_detail_query_invalid")
    namespace, values = document["namespace"], document["values"]
    if type(namespace) is not str or type(values) is not list:
        raise ValueError("managed_detail_query_invalid")
    if namespace == "canonical":
        if not 1 <= len(values) <= 100 or any(
            type(value) is not int or not 1 <= value <= 2147483647 for value in values
        ):
            raise ValueError("managed_detail_query_invalid")
        if len(set(values)) != len(values):
            raise ValueError("managed_detail_query_invalid")
        return tuple(sorted(values))
    if not 1 <= len(values) <= 50:
        raise ValueError("managed_detail_query_invalid")
    from process.registry_network_catalog_read import RegistryNetworkCatalogLegacyBatchSelector

    return RegistryNetworkCatalogLegacyBatchSelector(namespace, tuple(values))


def parse_managed_detail_query(document):
    """Validate the normalized query after strict JSON envelope decoding."""
    if type(document) is not dict or not {"npi"} <= set(document) <= _FIELDS:
        raise ValueError("managed_detail_query_invalid")
    npi = document["npi"]
    if type(npi) is not str or re.fullmatch(r"[0-9]{10}", npi) is None:
        raise ValueError("managed_detail_query_invalid")
    includes = document.get("include", [])
    if type(includes) is not list or len(includes) > 2:
        raise ValueError("managed_detail_query_invalid")
    if any(type(value) is not str or value not in {"taxonomy", "plans"} for value in includes):
        raise ValueError("managed_detail_query_invalid")
    if len(set(includes)) != len(includes):
        raise ValueError("managed_detail_query_invalid")
    generation = (
        _decimal(document["network_generation"], 9223372036854775807, minimum=1)
        if "network_generation" in document
        else None
    )
    return ProviderDetailNetworkQuery(
        npi,
        tuple(sorted(includes)),
        _native_args(document),
        _selector(document["selector"]) if "selector" in document else None,
        generation,
    )
