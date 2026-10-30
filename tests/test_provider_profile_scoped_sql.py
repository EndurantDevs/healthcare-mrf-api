# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import json
from datetime import date, datetime, timezone

import pytest

from api.provider_profile_scoped_sql import build_scoped_profile_sql, normalize_scoped_profile_projection


def _resource(kind, identity, **payload):
    return {"resource_type": kind, "resource_id": identity, "payload_json": {"resource_id": identity, **payload}}


def _projection():
    practitioner = _resource(
        "Practitioner",
        "provider",
        npi=1234567893,
        names=[{"text": "Example Provider"}],
        communications=[{"codes": [{"code": "en"}]}],
    )
    bundles = []
    for office, network, plan in (("office-a", "network-a", "plan-a"), ("office-b", "network-b", "plan-b")):
        bundles.append(
            {
                "association": {
                    "role_type": "PractitionerRole",
                    "role_id": "shared-role",
                    "location_resource_id": office,
                    "network_resource_id": network,
                    "plan_id": plan,
                },
                "resources": [
                    practitioner,
                    _resource(
                        "PractitionerRole",
                        "shared-role",
                        practitioner_ref="Practitioner/provider",
                        location_refs=["Location/" + office],
                        network_refs=["Organization/" + network],
                        insurance_plan_refs=["InsurancePlan/" + plan],
                        active=True,
                    ),
                    _resource("Location", office),
                    _resource("Organization", network),
                    _resource("InsurancePlan", plan, name="Permitted " + plan),
                ],
            }
        )
    return {"independent_resources": [practitioner], "bundles": bundles}


def _arguments():
    return {
        "npi": 1234567893,
        "source": {
            "source_id": "example-source",
            "endpoint_id": "example-endpoint",
            "dataset_id": "example-dataset",
            "canonical_api_base": "https://example.test/fhir",
            "org_name": "Example source",
            "plan_name": "Hidden global plan",
        },
        "row_type_refs": {
            kind: ("fixture", "provider_directory_" + name)
            for kind, name in (
                ("Practitioner", "practitioner"),
                ("PractitionerRole", "practitioner_role"),
                ("Organization", "organization"),
                ("OrganizationAffiliation", "organization_affiliation"),
                ("HealthcareService", "healthcare_service"),
                ("Endpoint", "endpoint"),
            )
        },
        "projection": _projection(),
        "generation_id": "a" * 64,
        "profile_as_of": date(2026, 6, 1),
        "published_at": datetime(2026, 6, 2, tzinfo=timezone.utc),
    }


def test_nonrectangular_bundles_remain_separate_and_input_is_not_mutated():
    projection = _projection()
    before = copy.deepcopy(projection)
    decoded = json.loads(normalize_scoped_profile_projection(projection, source_id="example-source"))
    assert projection == before
    assert len(decoded["independent_resources"]) == 1
    pairs = []
    for bundle in decoded["bundles"]:
        role = next(row["payload_json"] for row in bundle["resources"] if row["resource_type"] == "PractitionerRole")
        pairs.append((role["location_refs"], role["insurance_plan_refs"]))
        assert role["source_id"] == "example-source"
    assert pairs == [
        (["Location/office-a"], ["InsurancePlan/plan-a"]),
        (["Location/office-b"], ["InsurancePlan/plan-b"]),
    ]


def test_sql_renders_each_bundle_before_deduplication_counts_and_caps():
    sql, parameters = build_scoped_profile_sql(**_arguments())
    assert sql.lstrip().startswith("WITH projection")
    assert "jsonb_to_recordset(slices.resources)" in sql
    assert "FROM slices CROSS JOIN LATERAL" in sql
    assert "SELECT slices.kind,evidence.*" in sql
    assert "slices.kind='bundle'" in sql
    assert "evidence.fact_type NOT IN" in sql
    assert "'accepting_medicaid'" in sql
    assert "md5(fact_key || '|' || value_json::text)" in sql
    assert sql.index("SELECT DISTINCT ON (evidence_key)") < sql.index("WITH affected_npis")
    assert "WHERE evidence_rank <= 25" in sql
    assert "WHERE fact_rank <= 100" in sql
    assert "FROM scoped_evidence" in sql
    assert parameters["profile_npi"] == 1234567893
    for forbidden in ("INSERT INTO", "UPDATE ", "CREATE ", "ON CONFLICT", ";"):
        assert forbidden not in sql
    for schema, table in _arguments()["row_type_refs"].values():
        assert f'"{schema}"."{table}"' not in sql
    assert "jsonb_populate_record" not in sql
    assert sql.count("CROSS JOIN LATERAL jsonb_to_record(") == 12
    assert '"fhir_meta" JSON' in sql and '"resource_id" VARCHAR(256)' in sql
    assert "office-a" not in sql


def test_plan_descriptors_are_per_exact_bundle_and_publication_time_is_pinned():
    sql, parameters = build_scoped_profile_sql(**_arguments())
    assert "CASE WHEN slices.kind='bundle'" in sql
    assert "resource_type='InsurancePlan' AND resource_id=slices.association->>'plan_id'" in sql
    assert "Hidden global plan" not in json.dumps(parameters, default=str)
    assert "CAST(:profile_published_at AS timestamptz) AS published_at" in sql
    assert "AS aggregated\n" in sql
    assert parameters["profile_as_of"] == "2026-06-01"
    assert parameters["profile_published_at"] == datetime(2026, 6, 2, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p.update(extra=True),
        lambda p: p["bundles"][0]["association"].update(extra=True),
        lambda p: p["bundles"][0]["association"].update(role_type="Location"),
        lambda p: p["bundles"][0]["association"].update(plan_id="bad/id"),
        lambda p: p["bundles"][0]["resources"][1]["payload_json"].update(location_refs=["Location/office-b"]),
        lambda p: p["bundles"][0]["resources"][1]["payload_json"].update(insurance_plan_refs=["InsurancePlan/plan-b"]),
        lambda p: p["bundles"][0]["resources"][1]["payload_json"].update(network_refs=["Organization/network-b"]),
        lambda p: p["bundles"][0]["resources"][1]["payload_json"].update(source_id="other-source"),
        lambda p: p["bundles"][0]["resources"][1]["payload_json"].update(resource_id="other-role"),
        lambda p: p["bundles"].append(copy.deepcopy(p["bundles"][0])),
        lambda p: p["independent_resources"].append(copy.deepcopy(p["independent_resources"][0])),
        lambda p: p["bundles"][0]["resources"].pop(1),
    ],
)
def test_projection_refuses_cross_bundle_identity_or_open_shapes(change):
    projection = _projection()
    change(projection)
    with pytest.raises(ValueError, match="scoped_profile_projection_invalid"):
        normalize_scoped_profile_projection(projection, source_id="example-source")


def test_projection_bounds_and_non_json_values_fail_closed():
    for bad in ("x" * (1024 * 1024), float("nan"), "\ud800", object()):
        projection = _projection()
        projection["independent_resources"][0]["payload_json"]["extra"] = bad
        with pytest.raises(ValueError, match="scoped_profile_projection_invalid"):
            normalize_scoped_profile_projection(projection, source_id="example-source")
    projection = _projection()
    projection["bundles"] *= 5001
    with pytest.raises(ValueError, match="scoped_profile_projection_invalid"):
        normalize_scoped_profile_projection(projection, source_id="example-source")


@pytest.mark.parametrize(
    "field,value",
    [
        ("npi", True),
        ("generation_id", "bad"),
        ("profile_as_of", "2026-06-01"),
        ("published_at", datetime(2026, 6, 2)),
        ("row_type_refs", {}),
        ("source", {}),
    ],
)
def test_build_refuses_incomplete_or_unpinned_metadata(field, value):
    args = _arguments()
    args[field] = value
    with pytest.raises(ValueError):
        build_scoped_profile_sql(**args)


def test_empty_inputs_still_use_one_independent_slice_and_no_source_fallback():
    args = _arguments()
    args["projection"] = {"independent_resources": [], "bundles": []}
    sql, parameters = build_scoped_profile_sql(**args)
    assert json.loads(parameters["profile_projection"]) == {"independent_resources": [], "bundles": []}
    assert "FROM jsonb_to_recordset(slices.resources)" in sql
    assert "provider_directory_source AS" not in sql


def test_effective_date_never_claims_an_unverified_publication_time():
    args = _arguments()
    args.pop("published_at")
    sql, parameters = build_scoped_profile_sql(**args)
    assert "NULL::timestamptz AS published_at" in sql
    assert parameters["profile_published_at"] is None
    assert parameters["profile_as_of"] == "2026-06-01"


def test_sql_guards_both_documents_before_transfer_without_erasing_overflow_row():
    sql, parameters = build_scoped_profile_sql(**_arguments())
    assert "octet_length(to_jsonb(aggregated)::text) AS response_bytes" in sql
    for field in ("profile_json", "evidence_json"):
        assert (
            f'CASE WHEN response_bound.response_bytes <= :profile_max_bytes THEN aggregated."{field}" END AS "{field}"'
            in sql
        )
    assert parameters["profile_max_bytes"] == 1024 * 1024
    assert "WHERE response_bound" not in sql
