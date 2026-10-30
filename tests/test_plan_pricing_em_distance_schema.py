# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema contract for the release-bound E&M distance projection."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

from api import plan_pricing_em_distance_build as projection_build

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = REPOSITORY_ROOT / ("alembic/versions/20260901103000_plan_pricing_em_distance.py")


class _Recorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(" ".join(str(statement).split()))


def _migration():
    module_spec = importlib.util.spec_from_file_location("plan_pricing_em_distance_schema", MIGRATION_PATH)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_location_copy_uses_shared_geo_and_taxonomy_contracts():
    sql = projection_build._store_locations_sql()
    for marker in (
        "geo_evidence_source_id",
        "geo_identity_coherent",
        "geo_point_coherent",
        "npi_taxonomy",
        "address_precision",
        "public.ST_MakePoint",
    ):
        assert marker in sql
    assert "WHEN 'primary' THEN 1 ELSE 2 END" in sql
    assert "BTRIM(COALESCE(addr.address_precision, '')) NOT IN ('', 'city_zip')" in sql
    assert sql.count("ORDER BY addr.npi, addr.location_key") == 1


def _assert_registry_migration_chain():
    """Load the actual ScriptDirectory and preserve the complete installed head chain."""
    alembic_config = Config(str(REPOSITORY_ROOT / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(REPOSITORY_ROOT / "alembic"))
    script = ScriptDirectory.from_config(alembic_config)
    assert script.get_heads() == ["20261029010000_registry_source_inventory"]
    assert script.get_revision("20261029010000_registry_source_inventory").down_revision == (
        "20261028020000_registry_membership_refresh"
    )
    assert script.get_revision("20261028020000_registry_membership_refresh").down_revision == (
        "20261028010000_registry_fhir_catalog"
    )
    assert (
        script.get_revision("20261019030000_provider_directory_role_schema").down_revision
        == "20261019020000_registry_continuation_join"
    )
    assert script.get_revision("20261019020000_registry_continuation_join").down_revision == (
        "20261019010000_registry_raw_object_join",
        "20261019010000_custom_import_build_continuation",
    )
    assert script.get_revision("20261019010000_custom_import_build_continuation").down_revision == (
        "20261017010000_custom_import_raw_objects"
    )
    assert script.get_revision("20261017020000_source_registry_snapshot_join").down_revision == (
        "20261016060000_uhc_official_cohort_snapshots",
        "20261017010000_registry_cms_source_notifications",
    )
    assert script.get_revision("20261017010000_registry_cms_source_notifications").down_revision == (
        "20261016070000_registry_allowed_plan_scope"
    )
    assert script.get_revision("20261016070000_registry_allowed_plan_scope").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261016060000_uhc_official_cohort_snapshots").down_revision == (
        "20261016050000_acquisition_validator_argument_types"
    )
    assert script.get_revision("20261015010000_plan_release_binding_cohorts").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261015020000_custom_import_query_projections").down_revision == (
        "20261013010000_registry_custom_history_events"
    )
    assert script.get_revision("20261010010000_custom_import_materialization_contract").down_revision == (
        "20261010000000_custom_import_admission_indexes"
    )
    assert script.get_revision("20261010030000_registry_source_admission_receipt").down_revision == (
        "20261010020000_registry_ptg_office_retention"
    )
    assert script.get_revision("20261010020000_registry_ptg_office_retention").down_revision == (
        "20261010010000_registry_ptg_office_approval"
    )
    assert script.get_revision("20261010010000_registry_ptg_office_approval").down_revision == (
        "20261009040000_registry_ptg_published_plan_scope"
    )
    assert script.get_revision("20261009040000_registry_ptg_published_plan_scope").down_revision == (
        "20261009030000_network_catalog_evidence"
    )
    assert script.get_revision("20261006010000_nucc_reference_result_generation").down_revision == (
        "20261007000000_custom_import_rejection_anti_joins"
    )


def test_em_distance_projection_schema_is_exact_immutable_and_additive(
    monkeypatch,
) -> None:
    """Projection SQL stays immutable and additive across the actual current migration chain."""
    migration = _migration()
    upgrade = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "em_distance_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", upgrade)

    migration.upgrade()

    sql = " ".join(upgrade.statements)
    assert migration.revision == "20260901103000_plan_pricing_em_distance"
    assert migration.down_revision == ("20260901000000_hospital_price_csv_short_v2")
    _assert_registry_migration_chain()
    for table_name in (
        "plan_pricing_em_distance_candidate",
        "plan_pricing_em_distance_attachment",
        "plan_pricing_em_distance_rate",
        "plan_pricing_em_distance_location",
    ):
        assert f'CREATE TABLE "em_distance_test"."{table_name}"' in sql
    assert "plan_pricing_em_distance_v1" in sql
    assert "cardinality(minimums) = 6" in sql
    assert "mask_value BETWEEN 1 AND 63" in sql
    assert "public.geography(Point, 4326)" in sql
    assert "CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public" in sql
    assert "USING gist (projection_id, point)" in sql
    assert "geo_evidence_level IN" in sql
    assert "address_precision <> 'city_zip'" in sql
    assert "receipt counts do not match rows" in sql
    assert "attachment requires an exact ready candidate" in sql
    assert "content_digest IS NOT NULL" in sql
    assert "build_seconds IS NOT NULL" in sql
    assert sql.count("BEFORE TRUNCATE ON") == 4
    assert "import_run_plan_pricing_idempotency_idx" in sql
    assert "'plan-pricing-em-distance'" in sql

    downgrade = _Recorder()
    monkeypatch.setattr(migration, "op", downgrade)
    migration.downgrade()
    downgrade_sql = " ".join(downgrade.statements)
    assert "cannot downgrade while E&M distance projections exist" in (downgrade_sql)
    assert "'plan-pricing-projection', 'plan-pricing-prewarm'" in downgrade_sql
