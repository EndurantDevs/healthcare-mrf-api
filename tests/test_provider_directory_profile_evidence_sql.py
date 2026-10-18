# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib

import pytest

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
    assert 'FROM "fixture"."endpoint" AS endpoint' in sql
    assert ('JOIN "fixture"."provider_directory_dataset_affiliation_organization" AS affiliation_edge') in sql
    assert ('FROM "fixture"."provider_directory_organization_affiliation" AS affiliation') in sql
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
    assert "affiliation_edge.dataset_id = affiliation.dataset_id" in sql
    assert ("affiliation.relationship_type =\n                   'payer_reported_provider_plan_membership'") in sql
    assert "affiliation.ownership_status = 'not_asserted'" in sql
    assert "'candidate_addresses'," in sql
    assert "'payer_directory_candidate'" in sql
    assert "'tin_status', organization.tin_status" in sql
    assert "jsonb_build_object('tax_id', NULL)" in sql
    assert "jsonb_build_object('tax_id', organization.tax_id)" not in sql
    assert "organization.tax_id IS NULL" in sql
    assert ("NULLIF(BTRIM(affiliation.organization_ref), '') IS NULL") in sql
    assert "jsonb_typeof(\n                       affiliation.plan_scope::jsonb" in sql
    assert ("affiliation.plan_scope::jsonb\n                       ->> 'logical_scope_id' =") in sql
    assert "ELSE '[]'::jsonb" in sql
    assert ("THEN affiliation.insurance_plan_refs::jsonb ->> 0") in sql
    assert ("jsonb_array_length(\n                       affiliation.insurance_plan_refs::jsonb") not in sql
    assert "'InsurancePlan/uhcplan-'" in sql
    assert ("organization.source_lineage::jsonb =\n                   affiliation.source_lineage::jsonb") in sql
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
    assert ("FROM plan_membership_rows AS membership\n             WHERE TRUE") in sql
    assert ("COALESCE(organization.tin_status, '') <>\n                   'unavailable_from_uhc_source'") in sql


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
    bounded_affiliation = sql.index("membership_affiliation_rows AS MATERIALIZED")
    membership_join = sql.index("plan_membership_rows AS MATERIALIZED")
    assert bounded_affiliation < membership_join
    assert "FROM affiliation_resource_rows AS affiliation" in sql[bounded_affiliation:membership_join]
    assert ("FROM membership_affiliation_rows AS affiliation") in sql[membership_join:]


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

    assert 'FROM "fixture"."affiliation_scope_a" AS affiliation' in sql
    assert 'JOIN "fixture"."affiliation_edge" AS affiliation_edge' in sql
    assert "provider_directory_organization_affiliation" not in sql
    assert "affiliation_edge.dataset_id = role_rows.dataset_id" in sql


def test_dataset_graph_rows_are_normalized_and_never_source_wide() -> None:
    sql = _render_profile_evidence_sql()

    assert "source_id = ANY(" in sql
    assert "typed_source_context AS MATERIALIZED" in sql
    assert "dataset_source_context AS MATERIALIZED" in sql
    for relation_ref, resource_type in (
        ('"fixture"."practitioner"', "Practitioner"),
        ('"fixture"."role"', "PractitionerRole"),
        ('"fixture"."organization"', "Organization"),
        (
            '"fixture"."provider_directory_organization_affiliation"',
            "OrganizationAffiliation",
        ),
        ('"fixture"."service"', "HealthcareService"),
        ('"fixture"."endpoint"', "Endpoint"),
    ):
        assert f"NULL::{relation_ref}" in sql
        assert f"resource.resource_type = '{resource_type}'" in sql
    assert sql.count("dataset_source_context.dataset_id = resource.dataset_id") == 6
    for exact_join in (
        "practitioner.dataset_id = role.dataset_id",
        "service.dataset_id = role_rows.dataset_id",
        "endpoint.dataset_id = role_rows.dataset_id",
        "affiliation.dataset_id = role_rows.dataset_id",
        "organization.dataset_id = affiliation.dataset_id",
        "affiliation_edge.dataset_id = affiliation.dataset_id",
    ):
        assert exact_join in sql

    # These two families stay retained references/relations; Profile has no
    # standalone Location or InsurancePlan fact category.
    assert "resource.resource_type = 'Location'" not in sql
    assert "resource.resource_type = 'InsurancePlan'" not in sql
    assert "location" not in profile.PROFILE_EVIDENCE_FACT_TYPES
    assert "insurance_plan" not in profile.PROFILE_EVIDENCE_FACT_TYPES


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
    assert "fact_type = 'plan_membership'" in sql
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


def _profile_read_refs():
    return {
        "source_ref": '"fixture"."source"',
        "practitioner_ref": '"fixture"."practitioner"',
        "role_ref": '"fixture"."role"',
        "organization_ref": '"fixture"."organization"',
        "service_ref": '"fixture"."service"',
        "endpoint_ref": '"fixture"."endpoint"',
    }


def test_profile_select_renderers_preserve_writer_and_count_sql_bytes():
    """Adding read paths must not alter immutable artifact writer SQL."""
    sql_by_digest = {
        "e79d3494a25e3c6cfcdda10ed92bcba3846ae2ac0eea4bf9da64bf63043e665c": (
            profile.profile_evidence_insert_sql(target_ref='"fixture"."evidence"', **_profile_read_refs())
        ),
        "39f7ec66bbb3f4126f3caafdfb5baa2a59017027f83d4be140b9f6c0f3ec4d57": (
            profile.profile_evidence_count_sql(target_ref='"fixture"."evidence"', **_profile_read_refs())
        ),
        "2429743d3eddc7d2ed56e4f9933e81a450fc5d63ceb29d0af60bcf5e75fdab5a": (
            profile.profile_insert_sql(
                target_ref='"fixture"."profile"',
                evidence_ref='"fixture"."evidence"',
                old_evidence_ref='"fixture"."old_evidence"',
                rebuild_all=True,
            )
        ),
    }
    for expected, sql in sql_by_digest.items():
        assert hashlib.sha256(sql.encode()).hexdigest() == expected


def test_profile_evidence_select_filters_roots_before_reference_and_array_fanout():
    sql = profile.profile_evidence_select_sql(npi=1234567893, **_profile_read_refs())

    assert sql.lstrip().startswith("WITH selected_dataset")
    assert ";" not in sql
    assert "INSERT INTO" not in sql
    assert "ON CONFLICT" not in sql
    assert "CREATE " not in sql
    assert "UPDATE " not in sql
    assert '"evidence_key"' in sql
    assert "1234567893" not in sql
    practitioner_scope = sql.index("AND practitioner.npi = CAST(:profile_npi AS bigint)")
    dataset_scope = sql.index("AND resource.payload_json::jsonb ->> 'npi' = CAST(:profile_npi AS text)")
    role_scope = sql.index("AND COALESCE(role.npi, practitioner.npi) = CAST(:profile_npi AS bigint)")
    assert practitioner_scope < sql.index("qualification_values AS MATERIALIZED")
    assert dataset_scope < sql.index("role_rows AS MATERIALIZED")
    assert role_scope < sql.index("role_service_rows AS MATERIALIZED")
    assert role_scope < sql.index("role_endpoint_reference_rows AS MATERIALIZED")
    assert role_scope < sql.index("affiliation_rows AS MATERIALIZED")
    membership_scope = sql.index("AND EXISTS (SELECT 1 FROM organization_rows AS provider")
    assert membership_scope < sql.index("plan_membership_rows AS MATERIALIZED")
    assert "provider.source_id = affiliation.source_id" in sql
    assert "provider.dataset_id = affiliation.dataset_id" in sql
    assert "provider.npi = CAST(:profile_npi AS bigint)" in sql
    assert "affiliation.participating_organization_ref" in sql[membership_scope:]
    assert "AND service.npi = CAST(:profile_npi AS bigint)" in sql
    assert "AND npi = CAST(:profile_npi AS bigint)" in sql
    assert "FROM normalized_facts" in sql
    assert "active IS DISTINCT FROM FALSE" in sql


def test_profile_evidence_select_keeps_relation_scopes_separate_from_row_types():
    refs = _profile_read_refs()
    refs.update(
        practitioner_ref="allowed_practitioners",
        role_ref="allowed_roles",
        organization_ref="allowed_organizations",
        affiliation_ref="allowed_affiliations",
        affiliation_organization_ref="allowed_affiliation_edges",
        service_ref="allowed_services",
        endpoint_ref="allowed_endpoints",
        dataset_resource_ref="retained_resources",
    )
    type_ref_by_resource = {
        "practitioner": '"fixture"."practitioner"',
        "role": '"fixture"."role"',
        "organization": '"fixture"."organization"',
        "affiliation": '"fixture"."affiliation"',
        "service": '"fixture"."service"',
        "endpoint": '"fixture"."endpoint"',
    }
    sql = profile.profile_evidence_select_sql(npi=1234567893, resource_type_refs=type_ref_by_resource, **refs)

    for resource, type_ref in type_ref_by_resource.items():
        assert f"NULL::{type_ref}" in sql
        assert f"NULL::{refs[resource + '_ref']}" not in sql
    assert "FROM allowed_roles AS role" in sql
    assert "FROM retained_resources AS resource" in sql
    assert "JOIN allowed_affiliation_edges AS affiliation_edge" in sql


def test_profile_select_aggregates_only_prequalified_npi_before_counts_and_caps():
    sql = profile.profile_select_sql(evidence_ref="qualified_evidence", npi=1234567893)

    assert sql.lstrip().startswith("WITH affected_npis")
    assert ";" not in sql
    assert "INSERT INTO" not in sql
    assert "ON CONFLICT" not in sql
    assert "CREATE " not in sql
    assert "FROM qualified_evidence AS evidence" in sql
    assert "SELECT CAST(:profile_npi AS bigint) AS npi" in sql
    scoped_start = sql.index("scoped_evidence AS MATERIALIZED")
    facts_start = sql.index("facts AS MATERIALIZED")
    assert "WHERE evidence.npi = CAST(:profile_npi AS bigint)" in sql[scoped_start:facts_start]
    assert "FROM scoped_evidence" in sql[facts_start:]
    assert "count(DISTINCT source_id)::integer AS source_count" in sql
    assert "WHERE evidence_rank <= 25" in sql
    assert "WHERE fact_rank <= 100" in sql
    assert "'truncated', total > 100" in sql
    source_start = sql.index("profile_source_rows AS MATERIALIZED")
    assert "FROM scoped_evidence" in sql[source_start:]
    assert 'SELECT "npi", "profile_json", "evidence_json"' in sql
    assert "CAST(:generation_id AS varchar) AS generation_id" in sql


def test_profile_select_renderers_reject_invalid_npi_and_row_type_inputs():
    for npi in (True, "1234567893", 1234567890, 0, -1, 12345678930, None):
        with pytest.raises(ValueError, match="invalid profile read NPI"):
            profile.profile_evidence_select_sql(npi=npi, **_profile_read_refs())
        with pytest.raises(ValueError, match="invalid profile read NPI"):
            profile.profile_select_sql(npi=npi, evidence_ref="qualified_evidence")
    for resource_types in ({"unknown": '"fixture"."role"'}, {"role": ""}, {"role": None}):
        with pytest.raises(ValueError, match="invalid profile resource row type"):
            profile.profile_evidence_select_sql(
                npi=1234567893, resource_type_refs=resource_types, **_profile_read_refs()
            )
