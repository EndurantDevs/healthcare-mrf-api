# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict local-reference and plan-intersection semantics for rooted graphs."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
from typing import Any

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_DEPTH,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_NODES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_FIELD_CONTRACT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
)
from process.provider_directory_rooted_graph_identity import (
    SHA256_PATTERN,
    canonical_fhir_resource_id,
)
from process.provider_directory_rooted_graph_plan_net import (
    indexed_plan_net_reference_values,
    RootedGraphPlanNetError,
)
from process.provider_directory_rooted_graph_plan_census import (
    insurance_plan_census_sha256,
)
from process.provider_directory_rooted_graph_reference_scan import (
    reference_shaped_paths,
    ReferencePath,
    RootedGraphReferenceScanError,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_TYPES = (
    "Practitioner",
    *PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
)
_REFERENCE_FIELD_SPECS = {
    resource_type: tuple(
        (field_name, cardinality == "repeated", target_type)
        for field_name, cardinality, target_type in field_contracts
    )
    for resource_type, field_contracts in (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_FIELD_CONTRACT
    )
}


class ProviderDirectoryRootedGraphReferenceError(ValueError):
    """Reject reference shapes that could hide or broaden graph edges."""

    def __init__(self, code: str = "reference_invalid") -> None:
        message_by_code = {
            "census_incomplete": "rooted graph InsurancePlan census is incomplete",
            "census_invalid": "rooted graph InsurancePlan census is invalid",
            "network_reference_invalid": ("rooted graph network reference is invalid"),
            "reference_invalid": "rooted graph resource reference is invalid",
            "resource_invalid": "rooted graph resource is invalid",
            "resource_type_forbidden": "rooted graph resource type is forbidden",
        }
        self.code = code if code in message_by_code else "reference_invalid"
        super().__init__(message_by_code[self.code])


@dataclass(frozen=True, slots=True, order=True)
class ProviderDirectoryFHIRReference:
    """One canonical local FHIR reference suitable for a direct read."""

    resource_type: str
    resource_id: str

    def __post_init__(self) -> None:
        if self.resource_type not in (PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_TYPES):
            raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
        try:
            canonical_fhir_resource_id(self.resource_id)
        except ValueError:
            raise ProviderDirectoryRootedGraphReferenceError(
                "reference_invalid"
            ) from None

    @property
    def canonical(self) -> str:
        """Return the relative ``ResourceType/id`` representation."""

        return f"{self.resource_type}/{self.resource_id}"


def canonical_provider_directory_fhir_reference(
    candidate: object,
    *,
    expected_resource_type: str | None = None,
) -> ProviderDirectoryFHIRReference:
    """Parse only literal local references; absolute and contained refs fail."""

    if (
        type(candidate) is not str
        or candidate != candidate.strip()
        or candidate.count("/") != 1
    ):
        raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
    resource_type, resource_id = candidate.split("/", 1)
    if expected_resource_type is not None and resource_type != expected_resource_type:
        raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
    return ProviderDirectoryFHIRReference(resource_type, resource_id)


def _field_references(
    resource_by_field: dict[str, Any],
    field_name: str,
    repeated: bool,
    expected_resource_type: str,
) -> tuple[ProviderDirectoryFHIRReference, ...]:
    return tuple(
        reference
        for _field_path, reference in _indexed_field_references(
            resource_by_field,
            field_name,
            repeated,
            expected_resource_type,
        )
    )


def _indexed_field_references(
    resource_by_field: dict[str, Any],
    field_name: str,
    repeated: bool,
    expected_resource_type: str,
) -> tuple[tuple[str, ProviderDirectoryFHIRReference], ...]:
    if field_name not in resource_by_field:
        return ()
    raw_value = resource_by_field[field_name]
    if repeated:
        if type(raw_value) is not list:
            raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
        raw_references = raw_value
    else:
        if type(raw_value) is not dict:
            raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
        raw_references = [raw_value]
    indexed_references = []
    for index, raw_reference in enumerate(raw_references):
        if type(raw_reference) is not dict:
            raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
        if "reference" not in raw_reference:
            continue
        reference_value = raw_reference["reference"]
        field_path = f"{field_name}[{index}]" if repeated else field_name
        indexed_references.append(
            (
                field_path,
                canonical_provider_directory_fhir_reference(
                    reference_value,
                    expected_resource_type=expected_resource_type,
                ),
            )
        )
    return tuple(indexed_references)


def _indexed_plan_net_extension_references(
    resource_by_field: dict[str, Any],
) -> tuple[tuple[str, ProviderDirectoryFHIRReference], ...]:
    """Return bounded PractitionerRole Plan-Net extension references."""

    try:
        reference_values = indexed_plan_net_reference_values(
            resource_by_field,
            max_depth=(PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_DEPTH),
            max_nodes=(PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_NODES),
            network_extension_urls=(
                PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS
            ),
        )
        return tuple(
            (
                field_path,
                canonical_provider_directory_fhir_reference(
                    reference_value,
                    expected_resource_type="Organization",
                ),
            )
            for field_path, reference_value in reference_values
        )
    except RootedGraphPlanNetError:
        raise ProviderDirectoryRootedGraphReferenceError("reference_invalid") from None


def _structural_reviewed_path(rendered_path: str) -> ReferencePath:
    components: list[tuple[str, str | int]] = []
    for segment in rendered_path.split("."):
        field_name, separator, raw_index = segment.partition("[")
        components.append(("field", field_name))
        if separator:
            components.append(("index", int(raw_index.removesuffix("]"))))
    return tuple(components)


def _validated_indexed_references(
    resource_by_field: object,
) -> tuple[tuple[str, ProviderDirectoryFHIRReference], ...]:
    if type(resource_by_field) is not dict:
        raise ProviderDirectoryRootedGraphReferenceError("resource_invalid")
    resource_type = resource_by_field.get("resourceType")
    if resource_type not in _REFERENCE_FIELD_SPECS:
        raise ProviderDirectoryRootedGraphReferenceError("resource_type_forbidden")
    try:
        canonical_fhir_resource_id(resource_by_field.get("id"))
    except ValueError:
        raise ProviderDirectoryRootedGraphReferenceError("resource_invalid") from None
    indexed_references = []
    for field_name, repeated, expected_type in _REFERENCE_FIELD_SPECS[resource_type]:
        indexed_references.extend(
            _indexed_field_references(
                resource_by_field,
                field_name,
                repeated,
                expected_type,
            )
        )
    indexed_references.extend(_indexed_plan_net_extension_references(resource_by_field))
    try:
        observed_paths = Counter(reference_shaped_paths(resource_by_field))
        reviewed_paths = Counter(
            _structural_reviewed_path(field_path)
            for field_path, _reference in indexed_references
        )
    except (RootedGraphReferenceScanError, ValueError):
        raise ProviderDirectoryRootedGraphReferenceError("reference_invalid") from None
    if observed_paths != reviewed_paths:
        raise ProviderDirectoryRootedGraphReferenceError("reference_invalid")
    return tuple(indexed_references)


def provider_directory_rooted_graph_resource_references(
    resource_by_field: object,
) -> tuple[ProviderDirectoryFHIRReference, ...]:
    """Return deterministic, deduplicated traversable references."""

    return tuple(
        sorted(
            {
                reference
                for _field_path, reference in _validated_indexed_references(
                    resource_by_field
                )
            }
        )
    )


def provider_directory_rooted_graph_indexed_references(
    resource_by_field: object,
) -> tuple[tuple[str, ProviderDirectoryFHIRReference], ...]:
    """Return every traversable local reference with its exact field path."""

    return _validated_indexed_references(resource_by_field)


def _canonical_resource_json(resource_by_field: dict[str, Any]) -> str:
    try:
        canonical_json = json.dumps(
            resource_by_field,
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
        raise ProviderDirectoryRootedGraphReferenceError("resource_invalid") from None
    if (
        len(canonical_json.encode("utf-8"))
        > PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES
    ):
        raise ProviderDirectoryRootedGraphReferenceError("resource_invalid")
    return canonical_json


def _validated_plan_rows(
    plan_resources: object,
) -> tuple[tuple[str, str], ...]:
    if type(plan_resources) not in {list, tuple}:
        raise ProviderDirectoryRootedGraphReferenceError("census_invalid")
    canonical_json_by_id: dict[str, str] = {}
    for resource_by_field in plan_resources:
        if (
            type(resource_by_field) is not dict
            or resource_by_field.get("resourceType") != "InsurancePlan"
        ):
            raise ProviderDirectoryRootedGraphReferenceError("census_invalid")
        try:
            resource_id = canonical_fhir_resource_id(resource_by_field.get("id"))
            provider_directory_rooted_graph_resource_references(resource_by_field)
        except (ValueError, ProviderDirectoryRootedGraphReferenceError):
            raise ProviderDirectoryRootedGraphReferenceError("census_invalid") from None
        if resource_id in canonical_json_by_id:
            raise ProviderDirectoryRootedGraphReferenceError("census_invalid")
        canonical_json_by_id[resource_id] = _canonical_resource_json(resource_by_field)
    return tuple(sorted(canonical_json_by_id.items()))


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryInsurancePlanCensus:
    """A terminal, finite, source-wide plan census retained before filtering."""

    advertised_total: int
    terminal_page_count: int
    census_sha256: str = field(repr=False)
    _resource_json_rows: tuple[tuple[str, str], ...] = field(repr=False)
    census_complete: bool = True
    pagination_terminal: bool = True
    selection: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION

    def __post_init__(self) -> None:
        try:
            stored_resources = [
                json.loads(resource_json)
                for _resource_id, resource_json in self._resource_json_rows
            ]
            validated_rows = _validated_plan_rows(stored_resources)
        except (
            MemoryError,
            RecursionError,
            UnicodeError,
            ValueError,
            ProviderDirectoryRootedGraphReferenceError,
        ):
            raise ProviderDirectoryRootedGraphReferenceError("census_invalid") from None
        expected_sha256 = insurance_plan_census_sha256(
            self.advertised_total,
            self.terminal_page_count,
            validated_rows,
        )
        if (
            type(self.advertised_total) is not int
            or self.advertised_total < 0
            or self.advertised_total != len(validated_rows)
            or type(self.terminal_page_count) is not int
            or self.terminal_page_count < 1
            or type(self.census_sha256) is not str
            or SHA256_PATTERN.fullmatch(self.census_sha256) is None
            or self.census_sha256 != expected_sha256
            or self._resource_json_rows != validated_rows
            or self.census_complete is not True
            or self.pagination_terminal is not True
            or self.selection != PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION
        ):
            raise ProviderDirectoryRootedGraphReferenceError("census_invalid")

    @property
    def resource_count(self) -> int:
        """Return the complete distinct plan count."""

        return len(self._resource_json_rows)

    def resources(self) -> tuple[dict[str, Any], ...]:
        """Return fresh plan payloads in deterministic resource-ID order."""

        return tuple(
            json.loads(resource_json)
            for _resource_id, resource_json in self._resource_json_rows
        )

    def __repr__(self) -> str:
        return (
            "<provider-directory-insurance-plan-census "
            f"resources={self.resource_count} "
            f"pages={self.terminal_page_count}>"
        )


def build_provider_directory_insurance_plan_census(
    plan_resources: object,
    *,
    advertised_total: int,
    terminal_page_count: int,
) -> ProviderDirectoryInsurancePlanCensus:
    """Seal a full finite census only after terminal count reconciliation."""

    resource_rows = _validated_plan_rows(plan_resources)
    if (
        type(advertised_total) is not int
        or advertised_total < 0
        or advertised_total != len(resource_rows)
        or type(terminal_page_count) is not int
        or terminal_page_count < 1
    ):
        raise ProviderDirectoryRootedGraphReferenceError("census_incomplete")
    return ProviderDirectoryInsurancePlanCensus(
        advertised_total=advertised_total,
        terminal_page_count=terminal_page_count,
        census_sha256=insurance_plan_census_sha256(
            advertised_total,
            terminal_page_count,
            resource_rows,
        ),
        _resource_json_rows=resource_rows,
    )


def _network_reference_set(
    reachable_network_references: object,
) -> frozenset[ProviderDirectoryFHIRReference]:
    if type(reachable_network_references) not in {
        list,
        tuple,
        set,
        frozenset,
    }:
        raise ProviderDirectoryRootedGraphReferenceError("network_reference_invalid")
    canonical_references: set[ProviderDirectoryFHIRReference] = set()
    for candidate in reachable_network_references:
        try:
            reference = (
                candidate
                if type(candidate) is ProviderDirectoryFHIRReference
                else canonical_provider_directory_fhir_reference(candidate)
            )
        except ProviderDirectoryRootedGraphReferenceError:
            raise ProviderDirectoryRootedGraphReferenceError(
                "network_reference_invalid"
            ) from None
        if reference.resource_type != "Organization":
            raise ProviderDirectoryRootedGraphReferenceError(
                "network_reference_invalid"
            )
        canonical_references.add(reference)
    return frozenset(canonical_references)


def intersect_provider_directory_insurance_plan_census(
    census: ProviderDirectoryInsurancePlanCensus,
    reachable_network_references: object,
) -> tuple[dict[str, Any], ...]:
    """Select rooted plans locally; this never issues a ``network`` query."""

    if type(census) is not ProviderDirectoryInsurancePlanCensus:
        raise ProviderDirectoryRootedGraphReferenceError("census_incomplete")
    network_references = _network_reference_set(reachable_network_references)
    selected_resources: list[dict[str, Any]] = []
    for resource_by_field in census.resources():
        plan_networks = frozenset(
            _field_references(
                resource_by_field,
                "network",
                True,
                "Organization",
            )
        )
        if plan_networks & network_references:
            selected_resources.append(resource_by_field)
    return tuple(selected_resources)


__all__ = (
    "build_provider_directory_insurance_plan_census",
    "canonical_provider_directory_fhir_reference",
    "intersect_provider_directory_insurance_plan_census",
    "ProviderDirectoryFHIRReference",
    "ProviderDirectoryInsurancePlanCensus",
    "ProviderDirectoryRootedGraphReferenceError",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_TYPES",
    "provider_directory_rooted_graph_indexed_references",
    "provider_directory_rooted_graph_resource_references",
)
