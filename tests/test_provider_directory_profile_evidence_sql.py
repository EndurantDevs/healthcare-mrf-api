# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from process import provider_directory_profile as profile


def _render_profile_evidence_sql() -> str:
    """Render the evidence SQL shared by focused contract assertions."""
    return profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        endpoint_ref='"fixture"."endpoint"',
    )


def test_profile_evidence_sql_retains_derived_and_source_backed_facts():
    sql = _render_profile_evidence_sql()

    for fact_type in (
        "age",
        "years_of_practice",
        "credential",
        "taxonomy_qualification",
        "qualification_detail",
        "language",
        "contact",
        "specialty",
        "new_patient_acceptance",
        "telehealth",
        "accepting_medicaid",
        "role_identifier",
        "organization",
        "affiliation",
        "service",
        "endpoint",
    ):
        assert f"'{fact_type}'" in sql
    assert "practitioner.birth_date" not in sql
    assert "practitioner.birthDate" not in sql
    assert "practitioner.age_years BETWEEN 18 AND 100" in sql
    assert "'derivation', 'FHIR Practitioner.birthDate'" in sql
    assert "basis_start_date" in sql
    assert "'identifiers', role.identifiers::jsonb" in sql
    assert "'identifiers', service.identifiers::jsonb" in sql
    assert "'accepting_patients', service.accepting_patients::jsonb" in sql
    assert "'comment', service.comment" in sql
    assert "JOIN \"fixture\".\"endpoint\" AS endpoint" in sql
    assert (
        "JOIN \"fixture\".\"provider_directory_dataset_affiliation_organization\" "
        "AS affiliation_edge"
    ) in sql
    assert (
        "JOIN \"fixture\".\"provider_directory_organization_affiliation\" "
        "AS affiliation"
    ) in sql
    assert "affiliation.participating_organization_ref" in sql
    assert "affiliation_edge.dataset_id = role_rows.dataset_id" in sql
    assert "affiliation.organization_ref = role_rows.organization_ref" not in sql
    assert "'accepting_patients', COALESCE(" in sql
    assert "npi) BETWEEN 1000000000 AND 2999999999" in sql
    assert "AND MOD(" in sql
    assert "{{VALID_NPI_SQL}}" not in sql
    assert "ON CONFLICT (evidence_key) DO NOTHING" in sql


def test_profile_evidence_sql_filters_current_normalized_references():
    sql = _render_profile_evidence_sql()

    assert "active IS DISTINCT FROM FALSE" in sql
    assert "LEFT(effective_start, 10) <= CAST(:profile_as_of AS varchar)" in sql
    assert "LEFT(effective_end, 10) >= CAST(:profile_as_of AS varchar)" in sql
    for resource_type in (
        "Practitioner",
        "HealthcareService",
        "Endpoint",
        "Organization",
    ):
        assert f"{resource_type}/([A-Za-z0-9.-]{{1,64}})" in sql
    assert "(?:/_history/[A-Za-z0-9.-]{1,64})?" in sql


def test_profile_evidence_sql_supports_bounded_fact_and_role_scopes():
    sql = profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        endpoint_ref='"fixture"."endpoint"',
        fact_type="affiliation",
        role_bucket_count=32,
        role_bucket=7,
    )

    assert "fact_type = 'affiliation'" in sql
    assert ":profile_fact_type" not in sql
    assert "hashtextextended(role.resource_id, 0)" in sql
    assert "CAST(:profile_role_bucket_count AS bigint)" in sql
    assert "CAST(:profile_role_bucket AS bigint)" in sql


def test_profile_evidence_sql_accepts_exact_dataset_scoped_affiliations():
    sql = profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source_scope"',
        practitioner_ref='"fixture"."practitioner_scope"',
        role_ref='"fixture"."role_scope"',
        organization_ref='"fixture"."organization_scope"',
        affiliation_ref='"fixture"."affiliation_scope_a"',
        affiliation_organization_ref='"fixture"."affiliation_edge"',
        service_ref='"fixture"."service_scope"',
        endpoint_ref='"fixture"."endpoint_scope"',
    )

    assert 'JOIN "fixture"."affiliation_scope_a" AS affiliation' in sql
    assert 'JOIN "fixture"."affiliation_edge" AS affiliation_edge' in sql
    assert "provider_directory_organization_affiliation" not in sql
    assert "affiliation_edge.dataset_id = role_rows.dataset_id" in sql


def test_profile_evidence_sql_rejects_invalid_bounded_scopes():
    sql_refs_by_name = {
        "target_ref": '"fixture"."evidence"',
        "source_ref": '"fixture"."source"',
        "practitioner_ref": '"fixture"."practitioner"',
        "role_ref": '"fixture"."role"',
        "organization_ref": '"fixture"."organization"',
        "service_ref": '"fixture"."service"',
    }

    for invalid_args in (
        {"fact_type": "unknown"},
        {"role_bucket_count": 0},
        {"role_bucket_count": 4, "role_bucket": 4},
        {"role_bucket_count": 4, "role_bucket": -1},
    ):
        try:
            profile.profile_evidence_insert_sql(
                **sql_refs_by_name,
                **invalid_args,
            )
        except ValueError:
            continue
        raise AssertionError(f"scope accepted unexpectedly: {invalid_args}")
