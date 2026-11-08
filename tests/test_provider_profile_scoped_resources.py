# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Per-association projection preserves tuples without granting source authority."""

import copy

import pytest

from api.provider_profile_scoped_resources import project_scoped_profile_resources

NPI = "1000000491"


def _row(kind, identifier, **payload):
    return {"resource_type": kind, "resource_id": identifier, "payload_json": {"resource_id": identifier, **payload}}


def _graph():
    return [
        _row("Practitioner", "doctor", npi=NPI, qualifications=[{"license": "synthetic"}], languages=["en"]),
        _row(
            "PractitionerRole",
            "role-a",
            practitioner_ref="Practitioner/doctor",
            location_refs=["Location/a", "Location/b"],
            network_refs=["Organization/x", "Organization/y"],
            insurance_plan_refs=["InsurancePlan/one", "InsurancePlan/two"],
            healthcare_service_refs=["HealthcareService/shared"],
            endpoint_refs=["Endpoint/online"],
            organization_ref="Organization/employer",
        ),
        _row(
            "PractitionerRole",
            "role-b",
            npi=NPI,
            practitioner_ref="Practitioner/doctor",
            location_refs=["Location/b"],
            network_refs=["Organization/y"],
            insurance_plan_refs=["InsurancePlan/two"],
            healthcare_service_refs=["HealthcareService/shared"],
            accepting_medicaid=False,
            source_lineage={"hidden": True},
        ),
        _row("Location", "a", addresses=[{"line": "Office A"}]),
        _row("Location", "b", addresses=[{"line": "Office B"}]),
        _row("Organization", "x", name="Network X"),
        _row("Organization", "y", name="Network Y"),
        _row(
            "Organization",
            "employer",
            name="Employer",
            addresses=[{"line": "Unproved office"}],
            candidate_addresses=[{"line": "Hidden"}],
            plan_scope={"private": True},
            source_lineage={"hidden": True},
        ),
        _row("InsurancePlan", "one", network_refs=["Organization/x"]),
        _row("InsurancePlan", "two", network_refs=["Organization/y"]),
        _row(
            "HealthcareService",
            "shared",
            name="Shared service",
            location_refs=["Location/a", "Location/b"],
            insurance_plan_refs=["InsurancePlan/one", "InsurancePlan/two"],
            endpoint_refs=["Endpoint/online"],
        ),
        _row(
            "Endpoint",
            "online",
            address="https://example.test/service",
            managing_organization_ref="Organization/employer",
        ),
        _row("HealthcareService", "unrelated", name="Hidden service", npi=NPI),
    ]


def _association(role="role-a", location="a", network="x", plan="one", kind="PractitionerRole"):
    return {
        "role_type": kind,
        "role_id": role,
        "location_resource_id": location,
        "network_resource_id": network,
        "plan_id": plan,
    }


def _payload(bundle, kind, identifier):
    return next(
        row["payload_json"]
        for row in bundle["resources"]
        if (row["resource_type"], row["resource_id"]) == (kind, identifier)
    )


def test_hidden_role_and_office_do_not_enter_the_qualified_graph():
    graph = _graph()
    projected = project_scoped_profile_resources(graph, NPI, [_association()])
    bundle = projected["bundles"][0]
    role = _payload(bundle, "PractitionerRole", "role-a")
    assert role["location_refs"] == ["Location/a"]
    assert role["network_refs"] == ["Organization/x"]
    assert role["insurance_plan_refs"] == ["InsurancePlan/one"]
    assert {row["resource_id"] for row in bundle["resources"]}.isdisjoint({"b", "y", "two", "role-b", "unrelated"})
    organization = _payload(bundle, "Organization", "employer")
    assert organization == {"resource_id": "employer", "name": "Employer"}
    assert _payload(bundle, "Endpoint", "online")["address"] == "https://example.test/service"


@pytest.mark.parametrize("second_role", ["role-a", "role-b"])
def test_each_role_office_plan_tuple_keeps_its_own_shared_service(second_role):
    associations = [_association(), _association(second_role, "b", "y", "two")]
    projected = project_scoped_profile_resources(_graph(), NPI, associations)
    assert [bundle["association"] for bundle in projected["bundles"]] == associations
    for bundle, role_id, location, network, plan in zip(
        projected["bundles"], ("role-a", second_role), ("a", "b"), ("x", "y"), ("one", "two"), strict=True
    ):
        role = _payload(bundle, "PractitionerRole", role_id)
        service = _payload(bundle, "HealthcareService", "shared")
        assert role["location_refs"] == service["location_refs"] == ["Location/" + location]
        assert role["insurance_plan_refs"] == service["insurance_plan_refs"] == ["InsurancePlan/" + plan]
        assert role["network_refs"] == ["Organization/" + network]
    assert _payload(projected["bundles"][0], "HealthcareService", "shared") is not _payload(
        projected["bundles"][1], "HealthcareService", "shared"
    )


def test_independent_practitioner_and_medicaid_survive_without_network_authority():
    graph = _graph()
    before = copy.deepcopy(graph)
    projected = project_scoped_profile_resources(graph, NPI, [])
    assert projected["bundles"] == []
    assert projected["independent_resources"][0] == graph[0]
    assert projected["independent_resources"][1]["payload_json"] == {
        "resource_id": "role-b",
        "npi": NPI,
        "practitioner_ref": "Practitioner/doctor",
        "accepting_medicaid": False,
    }
    projected["independent_resources"][0]["payload_json"]["languages"].append("fr")
    assert graph == before


def test_explicit_service_office_conflict_is_removed_without_certifying_empty_scope():
    graph = _graph()
    next(row for row in graph if row["resource_id"] == "shared")["payload_json"]["location_refs"] = ["Location/b"]
    projected = project_scoped_profile_resources(graph, NPI, [_association()])
    assert all(row["resource_id"] != "shared" for row in projected["bundles"][0]["resources"])
    assert _payload(projected["bundles"][0], "PractitionerRole", "role-a")["healthcare_service_refs"] == []


def test_affiliation_requires_its_exact_participating_provider_and_plan():
    graph = _graph() + [
        _row("Organization", "provider", npi=NPI),
        _row(
            "OrganizationAffiliation",
            "affiliation",
            participating_organization_ref="Organization/provider",
            location_refs=["Location/a", "Location/b"],
            network_refs=["Organization/x"],
            insurance_plan_refs=["InsurancePlan/one", "InsurancePlan/two"],
            plan_scope={"private": True},
        ),
    ]
    projected = project_scoped_profile_resources(
        graph, NPI, [_association("affiliation", kind="OrganizationAffiliation")]
    )
    affiliation = _payload(projected["bundles"][0], "OrganizationAffiliation", "affiliation")
    assert affiliation["location_refs"] == ["Location/a"]
    assert affiliation["insurance_plan_refs"] == ["InsurancePlan/one"]
    assert "plan_scope" not in affiliation


@pytest.mark.parametrize(
    "failure",
    [
        "duplicate-resource",
        "wrong-resource-id",
        "wrong-provider",
        "foreign-reference",
        "malformed-reference",
        "non-string-json-key",
        "missing-role",
        "wrong-plan",
        "wrong-office",
        "malformed-kind",
        "duplicate-association",
        "nonfinite",
    ],
)
def test_malformed_or_unproved_input_refuses(failure):
    graph, associations = _graph(), [_association()]
    mutations_by_failure = {
        "duplicate-resource": lambda: graph.append(copy.deepcopy(graph[0])),
        "wrong-resource-id": lambda: graph[0]["payload_json"].__setitem__("resource_id", "foreign"),
        "wrong-provider": lambda: graph[0]["payload_json"].__setitem__("npi", "1000000004"),
        "foreign-reference": lambda: graph[1]["payload_json"].__setitem__(
            "practitioner_ref", "https://foreign.test/Practitioner/doctor"
        ),
        "malformed-reference": lambda: graph[1]["payload_json"].__setitem__("location_refs", "Location/a"),
        "non-string-json-key": lambda: graph[0]["payload_json"].__setitem__(1, "invalid"),
        "missing-role": lambda: associations[0].__setitem__("role_id", "absent"),
        "wrong-plan": lambda: associations[0].__setitem__("plan_id", "two"),
        "wrong-office": lambda: associations[0].__setitem__("location_resource_id", "absent"),
        "malformed-kind": lambda: associations[0].__setitem__("role_type", []),
        "duplicate-association": lambda: associations.append(copy.deepcopy(associations[0])),
        "nonfinite": lambda: graph[0]["payload_json"].__setitem__("age", float("nan")),
    }
    mutations_by_failure[failure]()
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        project_scoped_profile_resources(graph, NPI, associations)


def test_projection_never_mutates_resources_or_associations():
    graph, associations = _graph(), [_association()]
    before = copy.deepcopy((graph, associations))
    projected = project_scoped_profile_resources(graph, NPI, associations)
    projected["bundles"][0]["resources"][0]["payload_json"]["mutated"] = True
    projected["bundles"][0]["association"]["role_id"] = "mutated"
    assert (graph, associations) == before


@pytest.mark.parametrize("failure", ["circular", "input-bytes", "resource-count", "association-count"])
def test_malformed_and_oversized_graphs_refuse_before_projection(failure):
    graph, associations = _graph(), [_association()]
    if failure == "circular":
        graph[0]["payload_json"]["cycle"] = graph[0]
    elif failure == "input-bytes":
        graph[0]["payload_json"]["large"] = "x" * (8 * 1024 * 1024)
    elif failure == "resource-count":
        graph = [graph[0]] * 5001
    else:
        associations = [associations[0]] * 5001
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        project_scoped_profile_resources(graph, NPI, associations)


def test_role_only_association_cannot_retain_unproved_plan_service():
    graph = _graph()
    association = _association(plan=None)
    bundle = project_scoped_profile_resources(graph, NPI, [association])["bundles"][0]
    role = _payload(bundle, "PractitionerRole", "role-a")
    assert role["insurance_plan_refs"] == []
    assert role["healthcare_service_refs"] == []
    assert not any(row["resource_type"] in {"InsurancePlan", "HealthcareService"} for row in bundle["resources"])


def test_shared_graph_fanout_cannot_exceed_the_complete_output_byte_budget():
    graph = _graph()
    graph[0]["payload_json"]["qualification_detail"] = "x" * 200_000
    associations = []
    for ordinal in range(50):
        role = copy.deepcopy(graph[1])
        role["resource_id"] = role["payload_json"]["resource_id"] = "fanout-" + str(ordinal)
        graph.append(role)
        associations.append(_association(role["resource_id"]))
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        project_scoped_profile_resources(graph, NPI, associations)


def test_bare_and_mixed_references_compose_as_canonical_typed_ids():
    from api.provider_profile_scoped_resources import _ARRAY_REFS, _SCALAR_REFS
    from api.provider_profile_scoped_sql import normalize_scoped_profile_projection

    canonical = project_scoped_profile_resources(_graph(), NPI, [_association()])
    graph = _graph()
    for resource in graph:
        payload_by_field = resource["payload_json"]
        for field, kind in _ARRAY_REFS.items():
            if payload_by_field.get(field) is not None:
                payload_by_field[field] = [reference.removeprefix(kind + "/") for reference in payload_by_field[field]]
        for field, kind in _SCALAR_REFS.items():
            if payload_by_field.get(field) is not None:
                payload_by_field[field] = payload_by_field[field].removeprefix(kind + "/")
    graph[1]["payload_json"]["location_refs"].append("Location/a")
    graph[1]["payload_json"]["network_refs"].append("Organization/x")
    graph[1]["payload_json"]["insurance_plan_refs"].append("InsurancePlan/one")
    before = copy.deepcopy(graph)
    projected = project_scoped_profile_resources(graph, NPI, [_association()])
    assert projected == canonical
    normalize_scoped_profile_projection(projected, source_id="cms-npd")
    assert graph == before
    assert projected["independent_resources"][1]["payload_json"]["practitioner_ref"] == "Practitioner/doctor"


def test_known_hidden_network_scalar_dependencies_are_removed_before_projection():
    from api.provider_profile_scoped_sql import normalize_scoped_profile_projection

    graph = _graph()
    graph[1]["payload_json"]["organization_ref"] = "y"
    next(resource for resource in graph if resource["resource_id"] == "online")["payload_json"][
        "managing_organization_ref"
    ] = "Organization/y"
    next(resource for resource in graph if resource["resource_id"] == "shared")["payload_json"]["provided_by_ref"] = "y"
    before = copy.deepcopy(graph)
    projection = project_scoped_profile_resources(graph, NPI, [_association()], excluded_network_resource_ids=("y",))
    bundle = projection["bundles"][0]
    assert all(resource["resource_id"] != "y" for resource in bundle["resources"])
    role = _payload(bundle, "PractitionerRole", "role-a")
    assert "organization_ref" not in role
    assert role["network_refs"] == ["Organization/x"]
    assert "provided_by_ref" not in _payload(bundle, "HealthcareService", "shared")
    assert "managing_organization_ref" not in _payload(bundle, "Endpoint", "online")
    encoded = normalize_scoped_profile_projection(projection, source_id="cms-npd")
    assert "Organization/y" not in encoded
    assert "Network Y" not in encoded
    assert graph == before
    assert len(projection["independent_resources"]) == 2


def test_denied_network_does_not_hide_an_unrelated_independent_employer():
    projection = project_scoped_profile_resources(_graph(), NPI, [_association()], excluded_network_resource_ids=("y",))
    bundle = projection["bundles"][0]
    assert _payload(bundle, "PractitionerRole", "role-a")["organization_ref"] == "Organization/employer"
    assert _payload(bundle, "Organization", "employer")["name"] == "Employer"
    assert _payload(bundle, "Endpoint", "online")["managing_organization_ref"] == "Organization/employer"
    assert projection["independent_resources"][0]["payload_json"]["qualifications"] == [{"license": "synthetic"}]
    assert projection["independent_resources"][1]["payload_json"]["accepting_medicaid"] is False


def test_allowed_association_cannot_contradict_the_trusted_denied_network_set():
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        project_scoped_profile_resources(_graph(), NPI, [_association()], excluded_network_resource_ids=("x",))


@pytest.mark.parametrize(
    "denied",
    [
        None,
        "y",
        {"y"},
        ("y", "y"),
        ("",),
        ("Organization/y",),
        ("bad/id",),
        (1,),
        ("x" * 65,),
        tuple("network-" + str(index) for index in range(5001)),
    ],
)
def test_malformed_or_unbounded_denied_network_ids_refuse(denied):
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        project_scoped_profile_resources(_graph(), NPI, [_association()], excluded_network_resource_ids=denied)
