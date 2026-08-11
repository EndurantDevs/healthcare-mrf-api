# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded extraction of Plan-Net network references from FHIR extensions."""

from __future__ import annotations

import re
from typing import Any

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_NETWORK_EXTENSION_ALLOWED_FIELDS,
)

_EXTENSION_VERSION_PATTERN = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")


class RootedGraphPlanNetError(ValueError):
    """Reject malformed or unbounded Plan-Net extension trees."""


def _validated_extension_url(candidate: object) -> str:
    if (
        type(candidate) is not str
        or not candidate
        or len(candidate) > 1024
        or candidate != candidate.strip()
    ):
        raise RootedGraphPlanNetError
    base_url, separator, version = candidate.partition("|")
    if separator and _EXTENSION_VERSION_PATTERN.fullmatch(version) is None:
        raise RootedGraphPlanNetError
    return base_url


def _has_reference_shaped_value(candidate: object) -> bool:
    if type(candidate) is dict:
        if "reference" in candidate:
            return True
        return any(
            _has_reference_shaped_value(field_value)
            for field_value in candidate.values()
        )
    if type(candidate) is list:
        return any(_has_reference_shaped_value(item) for item in candidate)
    return False


def _reviewed_network_reference(raw_extension: dict[str, Any]) -> object:
    if not set(raw_extension).issubset(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_NETWORK_EXTENSION_ALLOWED_FIELDS
    ):
        raise RootedGraphPlanNetError
    value_reference = raw_extension.get("valueReference")
    if type(value_reference) is not dict or "reference" not in value_reference:
        raise RootedGraphPlanNetError
    if any(
        _has_reference_shaped_value(field_value)
        for field_name, field_value in value_reference.items()
        if field_name != "reference"
    ):
        raise RootedGraphPlanNetError
    return value_reference["reference"]


def _validate_unreviewed_extension(raw_extension: dict[str, Any]) -> None:
    value_fields = tuple(
        field_name for field_name in raw_extension if field_name.startswith("value")
    )
    if (
        any(
            field_name not in {"id", "url", "extension"}
            and not field_name.startswith("value")
            for field_name in raw_extension
        )
        or len(value_fields) > 1
        or "valueReference" in value_fields
        or (value_fields and "extension" in raw_extension)
        or any(
            _has_reference_shaped_value(raw_extension[field_name])
            for field_name in value_fields
        )
    ):
        raise RootedGraphPlanNetError


def _extension_reference_values(
    raw_extensions: object,
    *,
    parent_path: str,
    depth: int,
    node_count: int,
    max_depth: int,
    max_nodes: int,
    network_extension_urls: tuple[str, ...],
) -> tuple[tuple[tuple[str, object], ...], int]:
    """Traverse one extension array and return references plus node census."""

    if depth > max_depth or type(raw_extensions) is not list:
        raise RootedGraphPlanNetError
    reference_values: list[tuple[str, object]] = []
    observed_node_count = node_count
    for index, raw_extension in enumerate(raw_extensions):
        observed_node_count += 1
        if observed_node_count > max_nodes or type(raw_extension) is not dict:
            raise RootedGraphPlanNetError
        extension_path = (
            f"{parent_path}.extension[{index}]"
            if parent_path
            else f"extension[{index}]"
        )
        base_url = _validated_extension_url(raw_extension.get("url"))
        if base_url in network_extension_urls:
            reference_values.append(
                (
                    extension_path + ".valueReference",
                    _reviewed_network_reference(raw_extension),
                )
            )
        else:
            _validate_unreviewed_extension(raw_extension)
        if "extension" in raw_extension:
            child_values, observed_node_count = _extension_reference_values(
                raw_extension["extension"],
                parent_path=extension_path,
                depth=depth + 1,
                node_count=observed_node_count,
                max_depth=max_depth,
                max_nodes=max_nodes,
                network_extension_urls=network_extension_urls,
            )
            reference_values.extend(child_values)
    return tuple(reference_values), observed_node_count


def indexed_plan_net_reference_values(
    resource_by_field: dict[str, Any],
    *,
    max_depth: int,
    max_nodes: int,
    network_extension_urls: tuple[str, ...],
) -> tuple[tuple[str, object], ...]:
    """Validate every extension tree and return reviewed role references."""

    if "extension" not in resource_by_field:
        return ()
    reviewed_network_extension_urls = (
        network_extension_urls
        if resource_by_field.get("resourceType") == "PractitionerRole"
        else ()
    )
    reference_values, _node_count = _extension_reference_values(
        resource_by_field["extension"],
        parent_path="",
        depth=1,
        node_count=0,
        max_depth=max_depth,
        max_nodes=max_nodes,
        network_extension_urls=reviewed_network_extension_urls,
    )
    return reference_values


__all__ = (
    "indexed_plan_net_reference_values",
    "RootedGraphPlanNetError",
)
