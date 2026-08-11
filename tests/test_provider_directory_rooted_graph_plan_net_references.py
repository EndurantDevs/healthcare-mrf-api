# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Plan-Net extension tests for rooted Provider Directory references."""

import pytest

from process import provider_directory_rooted_graph_references as references
from process import provider_directory_rooted_graph_reference_scan as reference_scan
from process.provider_directory_rooted_graph_references import (
    ProviderDirectoryFHIRReference,
    ProviderDirectoryRootedGraphReferenceError,
    provider_directory_rooted_graph_indexed_references,
    provider_directory_rooted_graph_resource_references,
)


def _reference(resource_type: str, resource_id: str) -> dict:
    return {"reference": f"{resource_type}/{resource_id}"}


def test_whole_resource_scan_reports_every_reference_shaped_object() -> None:
    assert reference_scan.reference_shaped_paths(
        {
            "reviewed": {"reference": "Organization/reviewed"},
            "nested": [{"assigner": {"reference": "Organization/hidden"}}],
        }
    ) == (
        (("field", "reviewed"),),
        (
            ("field", "nested"),
            ("index", 0),
            ("field", "assigner"),
        ),
    )


def test_whole_resource_scan_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(
        reference_scan,
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES",
        2,
    )

    with pytest.raises(reference_scan.RootedGraphReferenceScanError):
        reference_scan.reference_shaped_paths({"nested": {"value": "too-many"}})


def test_whole_resource_scan_rejects_non_json_object_keys() -> None:
    with pytest.raises(reference_scan.RootedGraphReferenceScanError):
        reference_scan.reference_shaped_paths({1: {"reference": "Organization/x"}})


def test_reference_scan_failure_maps_to_the_closed_reference_error(monkeypatch) -> None:
    def fail_scan(_resource):
        raise reference_scan.RootedGraphReferenceScanError

    monkeypatch.setattr(references, "reference_shaped_paths", fail_scan)
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {"resourceType": "Organization", "id": "synthetic-resource"}
        )


def test_plan_net_extensions_preserve_exact_nested_paths() -> None:
    network_url = (
        references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[0]
    )
    participating_url = (
        references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[2]
    )
    role_by_field = {
        "resourceType": "PractitionerRole",
        "id": "role-network-extensions",
        "extension": [
            {
                "url": network_url + "|1.2.0",
                "valueReference": {
                    "reference": "Organization/network-a",
                    "display": "retained but not identity",
                },
            },
            {
                "url": "urn:synthetic:nesting",
                "extension": [
                    {
                        "url": participating_url,
                        "valueReference": {"reference": "Organization/network-b"},
                    }
                ],
            },
        ],
    }

    assert provider_directory_rooted_graph_indexed_references(role_by_field) == (
        (
            "extension[0].valueReference",
            ProviderDirectoryFHIRReference("Organization", "network-a"),
        ),
        (
            "extension[1].extension[0].valueReference",
            ProviderDirectoryFHIRReference("Organization", "network-b"),
        ),
    )
    assert provider_directory_rooted_graph_resource_references(role_by_field) == (
        ProviderDirectoryFHIRReference("Organization", "network-a"),
        ProviderDirectoryFHIRReference("Organization", "network-b"),
    )


@pytest.mark.parametrize(
    "extension_value",
    [
        None,
        {"url": "urn:synthetic:not-an-array"},
        [{"url": None}],
        [
            {
                "url": references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[
                    0
                ],
                "valueReference": {},
            }
        ],
        [
            {
                "url": references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[
                    0
                ]
                + "|bad/version",
                "valueReference": _reference("Organization", "network-a"),
            }
        ],
    ],
)
def test_plan_net_extension_shapes_fail_closed(extension_value: object) -> None:
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "PractitionerRole",
                "id": "role-invalid-extension",
                "extension": extension_value,
            }
        )


def test_plan_net_extension_depth_is_bounded() -> None:
    root_extension_by_field = {"url": "urn:synthetic:level-1"}
    extension_cursor_by_field = root_extension_by_field
    for depth in range(
        2,
        references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_DEPTH + 2,
    ):
        child_extension_by_field = {"url": f"urn:synthetic:level-{depth}"}
        extension_cursor_by_field["extension"] = [child_extension_by_field]
        extension_cursor_by_field = child_extension_by_field

    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "PractitionerRole",
                "id": "role-too-deep",
                "extension": [root_extension_by_field],
            }
        )


def test_nested_unknown_reference_extension_fails_closed() -> None:
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "PractitionerRole",
                "id": "role-unknown-reference",
                "extension": [
                    {
                        "url": "urn:synthetic:container",
                        "extension": [
                            {
                                "url": "urn:synthetic:unknown-reference",
                                "valueReference": {
                                    "reference": "InsurancePlan/plan-hidden"
                                },
                            }
                        ],
                    }
                ],
            }
        )


@pytest.mark.parametrize(
    "resource_type",
    (
        "Organization",
        "Location",
        "HealthcareService",
        "Endpoint",
        "OrganizationAffiliation",
        "InsurancePlan",
    ),
)
def test_every_non_role_family_rejects_hidden_extension_references(
    resource_type: str,
) -> None:
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": resource_type,
                "id": "synthetic-resource",
                "extension": [
                    {
                        "url": "urn:synthetic:container",
                        "extension": [
                            {
                                "url": "urn:synthetic:hidden-reference",
                                "valueFoo": {
                                    "nested": {"reference": "Organization/hidden"}
                                },
                            }
                        ],
                    }
                ],
            }
        )


def test_non_role_family_accepts_bounded_non_reference_extensions() -> None:
    assert (
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "Organization",
                "id": "synthetic-resource",
                "extension": [
                    {
                        "url": "urn:synthetic:ordinary-value",
                        "valueString": "ordinary",
                    }
                ],
            }
        )
        == ()
    )


@pytest.mark.parametrize(
    "resource_type",
    (
        "PractitionerRole",
        "Organization",
        "Location",
        "HealthcareService",
        "Endpoint",
        "OrganizationAffiliation",
        "InsurancePlan",
    ),
)
def test_every_family_rejects_references_hidden_outside_reviewed_paths(
    resource_type: str,
) -> None:
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": resource_type,
                "id": "synthetic-resource",
                "unreviewedField": {"nested": {"reference": "Organization/hidden"}},
            }
        )


def test_extension_path_text_cannot_collide_with_a_reviewed_structural_path() -> None:
    network_url = (
        references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[0]
    )
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "PractitionerRole",
                "id": "synthetic-resource",
                "extension": [
                    {
                        "url": network_url,
                        "valueReference": {"reference": "Organization/reviewed"},
                    }
                ],
                "extension[0].valueReference": {"reference": "Organization/hidden"},
            }
        )


@pytest.mark.parametrize(
    "smuggled_field",
    [
        {"valueFoo": {"nested": {"reference": "InsurancePlan/plan-hidden"}}},
        {
            "extension": [
                {
                    "url": "urn:synthetic:hidden-plan",
                    "valueReference": {"reference": "InsurancePlan/plan-hidden"},
                }
            ]
        },
        {
            "valueReference": {
                "reference": "Organization/network-reviewed",
                "extension": [
                    {
                        "url": "urn:synthetic:hidden-plan",
                        "valueReference": {"reference": "InsurancePlan/plan-hidden"},
                    }
                ],
            }
        },
    ],
)
def test_reviewed_network_node_rejects_same_node_reference_smuggling(
    smuggled_field: dict[str, object],
) -> None:
    network_url = (
        references.PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[0]
    )
    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "PractitionerRole",
                "id": "role-smuggled-plan-reference",
                "extension": [
                    {
                        "url": network_url,
                        "valueReference": {
                            "reference": "Organization/network-reviewed"
                        },
                        **smuggled_field,
                    }
                ],
            }
        )


def test_unreviewed_extension_enforces_ext_1_and_nested_reference_rejection() -> None:
    invalid_extensions = (
        {
            "url": "urn:synthetic:mixed-container",
            "valueString": "ordinary",
            "extension": [{"url": "urn:synthetic:child"}],
        },
        {
            "url": "urn:synthetic:nested-reference",
            "valueFoo": {"items": [{"reference": "InsurancePlan/plan-hidden"}]},
        },
        {
            "url": "urn:synthetic:empty-reference-shape",
            "valueReference": {},
        },
    )
    for invalid_extension in invalid_extensions:
        with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
            provider_directory_rooted_graph_resource_references(
                {
                    "resourceType": "PractitionerRole",
                    "id": "role-invalid-unreviewed-extension",
                    "extension": [invalid_extension],
                }
            )


def test_plan_net_extension_node_count_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(
        references,
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_NODES",
        2,
    )

    with pytest.raises(ProviderDirectoryRootedGraphReferenceError):
        provider_directory_rooted_graph_resource_references(
            {
                "resourceType": "PractitionerRole",
                "id": "role-too-many-extensions",
                "extension": [
                    {"url": "urn:synthetic:one"},
                    {"url": "urn:synthetic:two"},
                    {"url": "urn:synthetic:three"},
                ],
            }
        )
