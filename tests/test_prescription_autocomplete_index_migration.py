# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from db.models import PricingProviderPrescription


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260820010000_prescription_autocomplete_trigram_index.py"
)
MIGRATION_SPEC = spec_from_file_location(
    "prescription_autocomplete_trigram_index_migration",
    MIGRATION_PATH,
)
migration = module_from_spec(MIGRATION_SPEC)
assert MIGRATION_SPEC and MIGRATION_SPEC.loader
MIGRATION_SPEC.loader.exec_module(migration)


def test_migration_matches_staging_index_contract():
    runtime_indexes = [
        index
        for index in PricingProviderPrescription.__my_additional_indexes__
        if index.get("name") == migration.INDEX_NAME
    ]

    assert runtime_indexes == [
        {
            "index_elements": migration.INDEX_EXPRESSIONS,
            "using": "gin",
            "where": migration.INDEX_PREDICATE,
            "name": migration.INDEX_NAME,
            "staging_name": migration.STAGING_INDEX_NAME,
        }
    ]
    assert migration.down_revision == "202608200001_ptg_v13_json_null_guard"


def test_migration_uses_concurrent_partial_gin_ddl():
    create_sql = migration._create_index_sql("fixture")

    assert create_sql.startswith(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        '"pricing_provider_rx_autocomplete_trgm_idx" '
        'ON "fixture"."pricing_provider_prescription" USING gin '
    )
    assert all(expression in create_sql for expression in migration.INDEX_EXPRESSIONS)
    assert create_sql.endswith("WHERE rx_code_system = 'HP_RX_CODE'")
    assert migration._drop_index_sql("fixture") == (
        'DROP INDEX CONCURRENTLY IF EXISTS '
        '"fixture"."pricing_provider_rx_autocomplete_trgm_idx"'
    )


def test_migration_rejects_wrong_valid_index_shape(monkeypatch):
    monkeypatch.setattr(
        migration,
        "_index_catalog_record",
        lambda *_args: {"indisvalid": True, "indisready": True},
    )
    monkeypatch.setattr(migration, "_shape_from_catalog", lambda _record: "wrong")

    with pytest.raises(
        RuntimeError,
        match="existing_schema_index_mismatch:fixture.pricing_provider_rx_autocomplete_trgm_idx",
    ):
        migration._matching_index_record("fixture", "expected")


@pytest.mark.parametrize(
    ("is_valid", "is_ready"),
    ((False, False), (False, True), (True, False)),
)
def test_migration_rebuilds_interrupted_index(monkeypatch, is_valid, is_ready):
    monkeypatch.setattr(
        migration,
        "_index_catalog_record",
        lambda *_args: {"indisvalid": is_valid, "indisready": is_ready},
    )

    assert migration._matching_index_record("fixture", "expected") is False
