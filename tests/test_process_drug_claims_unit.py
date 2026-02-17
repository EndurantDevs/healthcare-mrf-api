# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_unit", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_unit"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


def test_find_dataset_normalizes_landing_page():
    catalog = {
        "dataset": [
            {
                "landingPage": "https://data.cms.gov/x/y/data",
                "title": "dataset-1",
                "distribution": [],
            }
        ]
    }
    result = drug_claims._find_dataset(catalog, "https://data.cms.gov/x/y")
    assert result["title"] == "dataset-1"


def test_resolve_sources_extracts_urls_and_years():
    catalog = {
        "dataset": [
            {
                "landingPage": (
                    "https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/"
                    "medicare-part-d-prescribers-by-provider-and-drug"
                ),
                "title": "provider_drug",
                "distribution": [
                    {"downloadURL": "https://example.com/provider_drug_DY21.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/provider_drug_DY22.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/provider_drug_DY23.csv", "mediaType": "text/csv"},
                ],
            },
            {
                "landingPage": (
                    "https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-spending-by-drug/"
                    "medicare-part-d-spending-by-drug"
                ),
                "title": "drug_spending",
                "distribution": [
                    {"downloadURL": "https://example.com/drug_spending_DY21.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/drug_spending_DY22.csv", "mediaType": "text/csv"},
                    {"downloadURL": "https://example.com/drug_spending_DY23.csv", "mediaType": "text/csv"},
                ],
            },
        ]
    }
    resolved = drug_claims._resolve_sources(catalog)
    assert [entry["reporting_year"] for entry in resolved["provider_drug"]] == [2023]
    assert [entry["reporting_year"] for entry in resolved["drug_spending"]] == [2023]
    assert resolved["drug_spending"][0]["url"].startswith("https://example.com/")


@pytest.mark.asyncio
async def test_main_enqueues_drug_claims_start(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(drug_claims, "create_pool", AsyncMock(return_value=fake_pool))

    result = await drug_claims.main(test_mode=True, import_id="dev1")

    assert result["ok"] is True
    assert result["queued"] is True
    fake_pool.enqueue_job.assert_awaited_once()
    enqueue_call = fake_pool.enqueue_job.await_args
    assert enqueue_call.args[0] == "drug_claims_start"
    assert enqueue_call.kwargs["_queue_name"] == drug_claims.DRUG_CLAIMS_QUEUE_NAME
    assert enqueue_call.kwargs["_job_id"].startswith("drug_claims_start_")
    assert enqueue_call.args[1]["test_mode"] is True
    assert enqueue_call.args[1]["import_id"] == "dev1"


@pytest.mark.asyncio
async def test_finish_main_enqueues_finalize(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(drug_claims, "create_pool", AsyncMock(return_value=fake_pool))

    result = await drug_claims.finish_main(import_id="dev1", run_id="run_a", test_mode=True)

    assert result["ok"] is True
    assert result["queued"] is True
    assert result["run_id"] == "run_a"
    assert result["import_id"] == "dev1"
    assert result["stage_suffix"]
    fake_pool.enqueue_job.assert_awaited_once()
    enqueue_call = fake_pool.enqueue_job.await_args
    assert enqueue_call.args[0] == "drug_claims_finalize"
    assert enqueue_call.kwargs["_queue_name"] == drug_claims.DRUG_CLAIMS_FINISH_QUEUE_NAME
    assert enqueue_call.kwargs["_job_id"] == "drug_claims_finalize_run_a"
    assert enqueue_call.args[1]["test_mode"] is True


def test_normalize_rx_name_key_strips_non_alnum():
    assert drug_claims._normalize_rx_name_key(" Metformin HCL 500-mg ") == "METFORMINHCL500MG"
    assert drug_claims._normalize_rx_name_key(None) == ""


def test_normalize_external_codes():
    assert drug_claims._normalize_rxnorm_code("  860975  ") == "860975"
    assert drug_claims._normalize_rxnorm_code("rxnorm") is None
    assert drug_claims._normalize_ndc11_code("0002-8215-01") is None
    assert drug_claims._normalize_ndc11_code("00028215001") == "00028215001"
    assert drug_claims._normalize_ndc11_code("12345") is None


def test_extract_product_ndcs_from_lookup_payload():
    payload = {
        "generic": [{"product_ndc": "0002-8215"}, {"product_ndc": "0002-8215"}],
        "brand": [{"product_ndc": "00555-1234"}],
    }
    assert drug_claims._extract_product_ndcs(payload) == ["0002-8215", "00555-1234"]


@pytest.mark.asyncio
async def test_snapshot_crosswalk_uses_durable_stage_tables(monkeypatch):
    monkeypatch.setattr(drug_claims, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(drug_claims, "_column_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(drug_claims.secrets, "token_hex", lambda _: "abc123")

    statements: list[str] = []

    class _FakeDB:
        async def status(self, stmt, **params):
            statements.append(str(stmt))
            if "INSERT INTO mrf.code_crosswalk_stage" in str(stmt):
                return 8
            return 1

        async def scalar(self, stmt, **params):
            statements.append(str(stmt))
            if "COUNT(DISTINCT rx_code)" in str(stmt):
                return 2
            return None

    monkeypatch.setattr(drug_claims, "db", _FakeDB())

    result = await drug_claims._enrich_rx_crosswalk_from_snapshot(
        schema="mrf",
        prescription_table="pricing_prescription_stage",
        code_catalog_table="code_catalog_stage",
        code_crosswalk_table="code_crosswalk_stage",
    )

    assert result == {"mapped_codes": 2, "edges": 8}
    assert not any("CREATE TEMP TABLE" in sql for sql in statements)
    assert any("CREATE UNLOGGED TABLE mrf.tmp_hp_rx_codes_abc123" in sql for sql in statements)
    assert any("CREATE UNLOGGED TABLE mrf.tmp_rx_snapshot_codes_abc123" in sql for sql in statements)
    assert any("CREATE UNLOGGED TABLE mrf.tmp_rx_crosswalk_candidates_abc123" in sql for sql in statements)
    assert any("DROP TABLE IF EXISTS mrf.tmp_hp_rx_codes_abc123" in sql for sql in statements)
    assert any("DROP TABLE IF EXISTS mrf.tmp_rx_snapshot_codes_abc123" in sql for sql in statements)
    assert any("DROP TABLE IF EXISTS mrf.tmp_rx_crosswalk_candidates_abc123" in sql for sql in statements)


@pytest.mark.asyncio
async def test_snapshot_crosswalk_falls_back_to_scalar_rxnorm_column(monkeypatch):
    monkeypatch.setattr(drug_claims, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(drug_claims.secrets, "token_hex", lambda _: "abc123")

    async def _fake_column_exists(schema: str, table: str, column: str) -> bool:
        mapping = {
            ("rx_data", "product", "rxnorm_ids"): False,
            ("rx_data", "product", "rxnorm_id"): True,
            ("rx_data", "product", "rxnorm"): False,
            ("rx_data", "product", "rxcui"): False,
            ("rx_data", "package", "product_ndc"): True,
            ("rx_data", "package", "ndc11"): True,
            ("rx_data", "package", "package_ndc"): True,
        }
        return mapping.get((schema, table, column), False)

    monkeypatch.setattr(drug_claims, "_column_exists", _fake_column_exists)

    statements: list[str] = []

    class _FakeDB:
        async def status(self, stmt, **params):
            statements.append(str(stmt))
            if "INSERT INTO mrf.code_crosswalk_stage" in str(stmt):
                return 8
            return 1

        async def scalar(self, stmt, **params):
            statements.append(str(stmt))
            if "COUNT(DISTINCT rx_code)" in str(stmt):
                return 2
            return None

    monkeypatch.setattr(drug_claims, "db", _FakeDB())

    result = await drug_claims._enrich_rx_crosswalk_from_snapshot(
        schema="mrf",
        prescription_table="pricing_prescription_stage",
        code_catalog_table="code_catalog_stage",
        code_crosswalk_table="code_crosswalk_stage",
    )

    assert result == {"mapped_codes": 2, "edges": 8}
    sql_blob = "\n".join(statements)
    assert "p.rxnorm_id::varchar" in sql_blob
    assert "unnest(COALESCE(p.rxnorm_ids, ARRAY[]::varchar[]))" not in sql_blob


@pytest.mark.asyncio
async def test_live_upsert_casts_bind_params_to_varchar(monkeypatch):
    statements: list[str] = []

    class _FakeDB:
        async def status(self, stmt, **params):
            statements.append(str(stmt))
            # catalog insert + forward crosswalk + reverse crosswalk
            return 1

    monkeypatch.setattr(drug_claims, "db", _FakeDB())

    inserted = await drug_claims._upsert_external_code_and_edges(
        schema="mrf",
        code_catalog_table="code_catalog",
        code_crosswalk_table="code_crosswalk",
        hp_code="HP123",
        to_system="RXNORM",
        to_code="259255",
        display_name="atorvastatin calcium",
        confidence=0.9,
        source="drug_api_live",
    )

    assert inserted == 2
    sql_blob = "\n".join(statements)
    assert "CAST(:code_system AS varchar)" in sql_blob
    assert "CAST(:code AS varchar)" in sql_blob
    assert "CAST(:from_system AS varchar)" in sql_blob
    assert "CAST(:from_code AS varchar)" in sql_blob
    assert "CAST(:to_system AS varchar)" in sql_blob
    assert "CAST(:to_code AS varchar)" in sql_blob
