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


def test_profile_evidence_sql_preserves_uhc_facility_semantics():
    """UHC profile evidence is dataset-bound, TIN-safe, and non-ownership."""
    sql = _render_profile_evidence_sql()

    assert "'plan_membership'" in sql
    assert "affiliation_edge.dataset_id = source_context.dataset_id" in sql
    assert (
        "affiliation.relationship_type =\n"
        "                   'payer_reported_provider_plan_membership'"
    ) in sql
    assert "affiliation.ownership_status = 'not_asserted'" in sql
    assert "'candidate_addresses'," in sql
    assert "'payer_directory_candidate'" in sql
    assert "'tin_status', organization.tin_status" in sql
    assert "jsonb_build_object('tax_id', NULL)" in sql
    assert "jsonb_build_object('tax_id', organization.tax_id)" not in sql
    assert "organization.tax_id IS NULL" in sql
    assert (
        "NULLIF(BTRIM(affiliation.organization_ref), '') IS NULL"
    ) in sql
    assert "jsonb_typeof(\n                       affiliation.plan_scope::jsonb" in sql
    assert (
        "affiliation.plan_scope::jsonb\n"
        "                       ->> 'logical_scope_id' ="
    ) in sql
    assert "ELSE '[]'::jsonb" in sql
    assert (
        "THEN affiliation.insurance_plan_refs::jsonb ->> 0"
    ) in sql
    assert (
        "jsonb_array_length(\n"
        "                       affiliation.insurance_plan_refs::jsonb"
    ) not in sql
    assert "'InsurancePlan/uhcplan-'" in sql
    assert (
        "organization.source_lineage::jsonb =\n"
        "                   affiliation.source_lineage::jsonb"
    ) in sql
    for lineage_field in (
        "catalog_set_sha256",
        "source_file_id",
        "artifact_sha256",
        "logical_scope_id",
        "record_ordinal",
        "file_name",
    ):
        assert f"->> '{lineage_field}'" in sql
    assert "FROM organization_rows AS organization" not in sql
    assert (
        "FROM plan_membership_rows AS membership\n"
        "             WHERE TRUE"
    ) in sql
    assert (
        "COALESCE(organization.tin_status, '') <>\n"
        "                   'unavailable_from_uhc_source'"
    ) in sql


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


def test_profile_evidence_sql_partitions_direct_organization_memberships():
    sql = profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        endpoint_ref='"fixture"."endpoint"',
        fact_type="plan_membership",
        role_bucket_count=32,
        role_bucket=7,
    )

    assert "fact_type = 'plan_membership'" in sql
    assert "hashtextextended(affiliation.resource_id, 0)" in sql
    assert "hashtextextended(organization.resource_id, 0)" not in sql
    bounded_affiliation = sql.index(
        "membership_affiliation_rows AS MATERIALIZED"
    )
    membership_join = sql.index("plan_membership_rows AS MATERIALIZED")
    assert bounded_affiliation < membership_join
    assert (
        'FROM "fixture"."provider_directory_organization_affiliation" '
        "AS affiliation"
    ) in sql[bounded_affiliation:membership_join]
    assert (
        "FROM membership_affiliation_rows AS affiliation"
    ) in sql[membership_join:]


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


def test_profile_evidence_count_sql_is_read_only_and_uses_normalized_rows():
    sql = profile.profile_evidence_count_sql(
        target_ref='"fixture"."must_not_be_written"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        fact_type="plan_membership",
        role_bucket_count=32,
        role_bucket=7,
    )

    assert "INSERT INTO" not in sql
    assert "ON CONFLICT" not in sql
    assert "count(*)::bigint AS projected_rows" in sql
    assert "sum(pg_column_size(normalized_facts))" in sql
    assert 'fact_type = \'plan_membership\'' in sql
    assert '"fixture"."must_not_be_written"' not in sql


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
