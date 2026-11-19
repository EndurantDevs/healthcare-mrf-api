# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

import pytest

from db.models import (
    PricingProcedure,
    PricingProcedureTaxonomySignal,
    PricingProviderProcedure,
)

claims_pricing = importlib.import_module("process.claims_pricing")
provider_quality = importlib.import_module("process.provider_quality")


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260820130000_site_intelligence_fast_paths.py"
)
MIGRATION_SPEC = spec_from_file_location(
    "site_intelligence_fast_paths_migration",
    MIGRATION_PATH,
)
migration = module_from_spec(MIGRATION_SPEC)
assert MIGRATION_SPEC and MIGRATION_SPEC.loader
MIGRATION_SPEC.loader.exec_module(migration)


def _named_index(model, name):
    return next(index for index in model.__my_additional_indexes__ if index.get("name") == name)


def test_migration_matches_claims_and_signal_models():
    provider_count = PricingProcedure.__table__.c.provider_count
    page_index = _named_index(PricingProviderProcedure, migration.PAGE_INDEX_NAME)
    signal_index = _named_index(
        PricingProcedureTaxonomySignal,
        migration.SIGNAL_INDEX_NAME,
    )

    assert not provider_count.nullable
    assert str(provider_count.server_default.arg) == "0"
    assert page_index == {
        "index_elements": migration.PAGE_INDEX_EXPRESSIONS,
        "name": migration.PAGE_INDEX_NAME,
        "staging_name": "amt_page",
    }
    assert signal_index == {
        "index_elements": migration.SIGNAL_INDEX_EXPRESSIONS,
        "name": migration.SIGNAL_INDEX_NAME,
        "staging_name": "taxonomy_lookup",
    }
    assert migration.SIGNAL_COLUMNS == {column.name for column in PricingProcedureTaxonomySignal.__table__.columns}
    assert migration.down_revision == ("20260820030000_ptg_ordinary_terminal_json_canonical_digest")


def test_staged_index_names_fit_postgresql_identifiers():
    claims_stage = claims_pricing._staging_classes("abcdefghijkl_12345678", "mrf")["PricingProviderProcedure"]
    assert len(f"{claims_stage.__tablename__}_amt_page") <= 63


def test_source_importers_leave_taxonomy_signal_staging_to_derived_publication():
    for source in (claims_pricing, provider_quality):
        stages = source._staging_classes("source_stage", "mrf")
        assert "PricingProcedureTaxonomySignal" not in stages


def test_migration_uses_concurrent_exact_page_index():
    create_sql = migration._create_page_index_sql("fixture")

    assert create_sql == (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        '"pricing_provider_proc_amount_page_idx" '
        'ON "fixture"."pricing_provider_procedure" '
        "(year, procedure_code, total_allowed_amount DESC, npi)"
    )
    assert migration._drop_page_index_sql("fixture") == (
        'DROP INDEX CONCURRENTLY IF EXISTS "fixture"."pricing_provider_proc_amount_page_idx"'
    )


def test_migration_resets_only_stale_nonzero_provider_counts(monkeypatch):
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration._backfill_provider_count("mrf")

    assert "provider_count <> 0" in statements[0]
    assert "NOT EXISTS" in statements[0]
    assert "COUNT(DISTINCT pp.npi)::integer" in statements[1]


@pytest.mark.asyncio
async def test_claims_materializer_counts_distinct_providers(
    monkeypatch,
):
    statements = []

    async def capture(statement, **_parameters):
        statements.append(statement)

    monkeypatch.setattr(claims_pricing.db, "status", capture)
    classes_by_name = {
        "PricingProvider": SimpleNamespace(__tablename__="provider_stage"),
        "PricingProcedure": SimpleNamespace(__tablename__="procedure_stage"),
        "PricingProviderProcedure": SimpleNamespace(__tablename__="provider_procedure_stage"),
    }

    await claims_pricing._materialize_procedure_provider_counts(
        classes_by_name,
        "mrf",
    )

    assert len(statements) == 1
    assert "SET provider_count = 0" not in statements[0]
    assert "COUNT(DISTINCT pp.npi)::int" in statements[0]
    assert "provider.year = pp.year" not in statements[0]
    assert "p.year = pp.year" in statements[0]
