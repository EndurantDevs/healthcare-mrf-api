# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from db.models import PricingProviderPrescriptionAutocomplete


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260820140000_prescription_autocomplete_rollup.py"
)
MIGRATION_SPEC = spec_from_file_location(
    "prescription_autocomplete_rollup_migration",
    MIGRATION_PATH,
)
migration = module_from_spec(MIGRATION_SPEC)
assert MIGRATION_SPEC and MIGRATION_SPEC.loader
MIGRATION_SPEC.loader.exec_module(migration)
drug_claims = importlib.import_module("process.drug_claims")


def test_rollup_model_matches_migration_contract():
    model = PricingProviderPrescriptionAutocomplete

    assert migration.down_revision == (
        "20260820130000_site_intelligence_fast_paths"
    )
    assert migration.TABLE_COLUMNS == {
        column.name for column in model.__table__.columns
    }
    assert model.__my_index_elements__ == [
        "year",
        "rx_code_system",
        "rx_code",
        "variant_id",
    ]
    assert list(model.__table__.primary_key.columns.keys()) == (
        model.__my_index_elements__
    )


def test_drug_claims_stages_rollup_model():
    staged_models = drug_claims._staging_classes(
        "abcdefghijkl_12345678",
        "mrf",
    )
    staged_rollup = staged_models["PricingProviderPrescriptionAutocomplete"]

    assert staged_rollup.__main_table__ == migration.TABLE_NAME
    assert staged_rollup.__tablename__.startswith(f"{migration.TABLE_NAME}_")
    assert len(f"{staged_rollup.__tablename__}_idx_primary") <= 63


@pytest.mark.asyncio
async def test_drug_claims_materializes_exact_name_variants(monkeypatch):
    statements = []

    async def capture(statement, **_parameters):
        statements.append(str(statement))

    monkeypatch.setattr(drug_claims.db, "status", capture)
    await drug_claims._materialize_prescription_autocomplete_rollup(
        "mrf",
        "autocomplete_stage",
        "provider_stage",
    )

    assert statements[0] == "TRUNCATE TABLE mrf.autocomplete_stage;"
    assert '"mrf"."autocomplete_stage"' in statements[1]
    assert '"mrf"."provider_stage"' in statements[1]
    materialization_sql = " ".join(statements[1].split())
    assert (
        "GROUP BY year, rx_code_system, rx_code, rx_name, generic_name, "
        "brand_name"
    ) in materialization_sql
    assert "source_relation_fingerprint" in statements[1]
