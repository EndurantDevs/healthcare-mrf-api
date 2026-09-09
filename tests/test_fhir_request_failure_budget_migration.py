# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic DDL proofs for source-wide logical request failure accounting."""

from pathlib import Path
from unittest.mock import Mock

from tests.formulary_fhir_twin_admission_pg_support import load_migration


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260909120000_fhir_request_failure_budget.py"


def migration():
    return load_migration(MIGRATION_PATH, "fhir_request_failure_budget_test")


def normalized(sql):
    return " ".join(sql.split())


def test_guarded_builders_preserve_historical_branches_and_exact_hashes():
    budget_migration = migration()
    assert budget_migration.down_revision == "20260907220000_hospital_price_missing_plan"
    single = normalized(budget_migration._single_root_guard_sql("test_schema"))
    intrinsic = normalized(budget_migration._intrinsic_valid_sql("test_schema"))
    assert "COALESCE(NEW.insurance_plan_count::text, 'None')" in single
    assert "COALESCE(NEW.insurance_plan_page_count::text, 'None')" in single
    assert "NEW.acquisition_operation_key, CASE WHEN NEW.request_failure_coverage IS NULL THEN NULL ELSE" in single
    assert "NEW.request_failure_coverage IS DISTINCT FROM candidate.request_failure_coverage" in single
    assert ('NEW.request_failure_coverage IS DISTINCT FROM '
            '"test_schema"."provider_directory_rooted_graph_request_failure_coverage"(candidate.acquisition_id)') in single
    assert "header.publication_contract_id, header.admission_id" in intrinsic
    assert "healthporta.provider-directory.rooted-graph-dataset-root.v1" in intrinsic
    assert "healthporta.provider-directory.rooted-graph-publication.v2" in intrinsic
    assert "header.request_failure_coverage IS NULL THEN" in intrinsic
    assert "'retry_exhausted_count'" in intrinsic
    assert "'rooted_graph_complete', header.rooted_graph_complete" in intrinsic
    assert "failed_target.reference_id = expected.network_id" in intrinsic
    assert "failed_target.reference_id = expected.organization_id" in intrinsic
    assert "failed_target.error_code = 'transport_timeout'" in intrinsic


def test_budget_is_terminal_unique_requests_and_exact_inherited_cohort():
    budget_migration = migration()
    sql = normalized(budget_migration._coverage_sql("test_schema"))
    assert "official_cohort.npi_count" in sql
    assert "count(*) FILTER (WHERE status NOT IN ('completed', 'error'))" in sql
    assert "error_code IS DISTINCT FROM 'transport_timeout'" in sql
    assert "lineage.npi_count + rooted_total" in sql
    assert "inherited_failed + rooted_failed" in sql
    assert "root_header.cohort_id = candidate.root_cohort_id" in sql
    assert "root_header.content_proof = candidate.root_content_proof_sha256" in sql
    assert "sum(attempt_count)" not in sql
    assert "root_parent.request_failure_coverage" not in sql
    valid = normalized(budget_migration._coverage_valid_sql("test_schema"))
    assert "count(*) FROM jsonb_object_keys(proof)) <> 6" in valid
    assert "50 * (proof ->> 'failed_requests')::numeric < (proof ->> 'total_requests')::numeric" in valid


def test_seal_and_census_keep_successful_witnesses_but_allow_exact_timeouts():
    budget_migration = migration()
    sql = normalized(budget_migration._acquisition_guard_sql("test_schema"))
    assert "NEW.request_failure_coverage IS DISTINCT FROM" in sql
    assert "actual_completed + actual_error IS DISTINCT FROM actual_work_count" in sql
    assert "query.status = 'completed';" in sql
    assert "target_query.error_code = 'transport_timeout'" in sql
    assert "affiliation_query.error_code = 'transport_timeout'" in sql
    assert "NEW.insurance_plan_count IS DISTINCT FROM plan_total" in sql
    assert "NEW.terminal_set_sha256 IS DISTINCT FROM" in sql
    assert "NEW.resource_set_sha256 IS DISTINCT FROM" in sql
    census = normalized(budget_migration._work_guard_sql("test_schema"))
    assert "NOT (root_query.status = 'completed' OR" in census
    assert "source_query.status = 'completed'" in census
    assert "root_closure_frozen" in census


def test_upgrade_is_locked_additive_and_downgrade_is_proof_fenced(monkeypatch):
    budget_migration = migration()
    operations = Mock()
    monkeypatch.setattr(budget_migration, "op", operations)
    budget_migration.upgrade()
    statements = [call.args[0] for call in operations.execute.call_args_list]
    assert statements[0] == "SET LOCAL lock_timeout = '5s';"
    assert statements[1].startswith("LOCK TABLE ")
    sql = normalized(" ".join(statements))
    assert sql.count("ADD COLUMN request_failure_coverage jsonb") == 3
    assert "actual_error_count::numeric * 50 >= NEW.expected_npi_count" in sql
    assert "candidate.error_count::numeric * 50 >= candidate.expected_npi_count" in sql
    assert "NEW.request_failure_coverage IS DISTINCT FROM OLD.request_failure_coverage" in sql
    assert "UPDATE " not in " ".join(statements[:9])
    operations.reset_mock()
    budget_migration.downgrade()
    downgrade = normalized(" ".join(call.args[0] for call in operations.execute.call_args_list))
    assert "request_failure_downgrade_blocked" in downgrade
    assert downgrade.count("WHERE request_failure_coverage IS NOT NULL") == 3


def test_current_orm_checks_match_latest_versioned_migration():
    from db.models.provider_directory_rooted_graph import ProviderDirectoryRootedGraphAcquisition
    from db.models.provider_directory_rooted_graph_twin import ProviderDirectoryRootedGraphTwinAdmission
    from db.models.provider_directory_rooted_graph_publication import ProviderDirectoryRootedGraphDataset
    from tests.test_provider_directory_rooted_graph_acquisition_migration import _named_checks

    budget_migration = migration()
    cases = (
        (ProviderDirectoryRootedGraphAcquisition, budget_migration._acquisition_check_sql,
         "provider_directory_rooted_graph_acquisition_state_check"),
        (ProviderDirectoryRootedGraphAcquisition, budget_migration._acquisition_identity_check_sql,
         "provider_directory_rooted_graph_acquisition_identity_check"),
        (ProviderDirectoryRootedGraphTwinAdmission, budget_migration._admission_check_sql,
         "pd_rooted_graph_twin_admission_check"),
        (ProviderDirectoryRootedGraphDataset, budget_migration._dataset_check_sql,
         "pd_rooted_graph_dataset_check"),
    )
    for model, builder, name in cases:
        sql = normalized(builder(model.__table__.schema))
        expected = sql.split("CHECK (", 1)[1].rsplit(")", 1)[0]
        expected = expected.replace("\\:", ":")
        assert _named_checks(model.__table__.constraints)[name] == expected


def test_current_orm_columns_and_keys_preserve_predecessor_plus_proof():
    from db.models.provider_directory_rooted_graph import ProviderDirectoryRootedGraphAcquisition
    from db.models.provider_directory_rooted_graph_twin import ProviderDirectoryRootedGraphTwinAdmission
    from db.models.provider_directory_rooted_graph_publication import ProviderDirectoryRootedGraphDataset
    from tests.test_provider_directory_rooted_graph_acquisition_migration import (
        _column_specs, _migration_table_items, _named_key_specs,
    )

    model = ProviderDirectoryRootedGraphAcquisition
    predecessor = _migration_table_items()[model.__tablename__]
    expected = _column_specs(predecessor) | {"request_failure_coverage": ("JSONB", True, None)}
    assert _column_specs(model.__table__.columns) == expected
    assert _named_key_specs(model.__table__.constraints) == _named_key_specs(predecessor)
    for model in (ProviderDirectoryRootedGraphTwinAdmission, ProviderDirectoryRootedGraphDataset):
        columns = model.__table__.columns
        assert _column_specs(columns)["request_failure_coverage"] == ("JSONB", True, None)
        assert columns.insurance_plan_page_count.nullable is True
        plan_count = "insurance_plan_count" if model is ProviderDirectoryRootedGraphTwinAdmission else "census_insurance_plan_count"
        assert columns[plan_count].nullable is True
