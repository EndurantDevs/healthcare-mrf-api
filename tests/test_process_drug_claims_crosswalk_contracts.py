# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_crosswalk_contracts", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_crosswalk_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


class _DatabaseProbe:
    def __init__(self):
        self.scalar_values = iter([True, False])
        self.statements = []
        self.created_tables = []

    async def scalar(self, statement, **parameters):
        self.statements.append((str(statement), parameters))
        return next(self.scalar_values)

    async def status(self, statement, **parameters):
        self.statements.append((str(statement), parameters))
        return 1

    async def create_table(self, table, checkfirst):
        self.created_tables.append((table, checkfirst))


@pytest.mark.asyncio
async def test_schema_presence_queries_use_bound_identifiers(monkeypatch):
    database_probe = _DatabaseProbe()
    monkeypatch.setattr(drug_claims, "db", database_probe)

    assert await drug_claims._is_table_present("mrf", "codes") is True
    assert await drug_claims._is_column_present("mrf", "codes", "code") is False
    assert database_probe.statements[0][1] == {"qualified_name": "mrf.codes"}
    assert database_probe.statements[1][1] == {
        "schema_name": "mrf",
        "table_name": "codes",
        "column_name": "code",
    }


@pytest.mark.asyncio
async def test_index_builder_preserves_model_index_contract(monkeypatch):
    database_probe = _DatabaseProbe()
    monkeypatch.setattr(drug_claims, "db", database_probe)
    stage_model = SimpleNamespace(
        __tablename__="prescription_stage",
        __main_table__="prescription",
        __my_index_elements__=("rx_code_system", "rx_code"),
        __my_additional_indexes__=(
            {},
            {"index_elements": ("source_year",)},
            {
                "name": "active_code_idx",
                "index_elements": ("rx_code",),
                "using": "btree",
                "where": "rx_code IS NOT NULL",
            },
        ),
    )

    await drug_claims._ensure_indexes(stage_model, "mrf")
    sql_statements = "\n".join(statement for statement, _ in database_probe.statements)
    assert "CREATE UNIQUE INDEX IF NOT EXISTS prescription_stage_idx_primary" in sql_statements
    assert "prescription_stage_prescription_stage_source_year_idx" in sql_statements
    assert "USING btree (rx_code) WHERE rx_code IS NOT NULL" in sql_statements

    database_probe.statements.clear()
    await drug_claims._ensure_indexes(SimpleNamespace(__tablename__="plain"), "mrf")
    assert database_probe.statements == []


@pytest.mark.asyncio
async def test_prepare_tables_and_deferred_indexes_contract(monkeypatch):
    database_probe = _DatabaseProbe()
    staged_model_by_name = {
        "PricingPrescription": SimpleNamespace(
            __tablename__="prescription_stage",
            __table__="prescription-table",
        ),
        "PricingProviderPrescription": SimpleNamespace(
            __tablename__="provider_stage",
            __table__="provider-table",
        ),
    }
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(drug_claims, "db", database_probe)
    monkeypatch.setattr(drug_claims, "get_import_schema", lambda *args: "synthetic")
    monkeypatch.setattr(
        drug_claims,
        "make_class",
        lambda model, suffix, schema_override: staged_model_by_name[model.__name__],
    )
    monkeypatch.setattr(drug_claims, "_ensure_indexes", ensure_indexes)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_DEFER_STAGE_INDEXES", False)

    classes_by_name, schema = await drug_claims._prepare_tables("stage", False)
    assert schema == "synthetic"
    assert classes_by_name == staged_model_by_name
    assert len(database_probe.created_tables) == 2
    assert ensure_indexes.await_count == 2

    ensure_indexes.reset_mock()
    await drug_claims._build_staging_indexes(staged_model_by_name, schema)
    assert ensure_indexes.await_count == 2


@pytest.mark.asyncio
async def test_live_code_tables_create_both_shared_models(monkeypatch):
    database_probe = _DatabaseProbe()
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(drug_claims, "db", database_probe)
    monkeypatch.setattr(drug_claims, "_ensure_indexes", ensure_indexes)

    await drug_claims._ensure_live_code_tables("mrf")
    assert database_probe.created_tables == [
        (drug_claims.CodeCatalog.__table__, True),
        (drug_claims.CodeCrosswalk.__table__, True),
    ]
    assert ensure_indexes.await_count == 2


@pytest.mark.asyncio
async def test_materialization_runs_all_sql_contracts(monkeypatch):
    database_probe = _DatabaseProbe()
    enrich_crosswalk = AsyncMock()
    classes_by_name = {
        "PricingPrescription": SimpleNamespace(__tablename__="prescription_stage"),
        "PricingProviderPrescription": SimpleNamespace(__tablename__="provider_stage"),
    }
    monkeypatch.setattr(drug_claims, "db", database_probe)
    monkeypatch.setattr(drug_claims, "_enrich_external_rx_crosswalk", enrich_crosswalk)

    await drug_claims._materialize_prescription_and_code_rows(classes_by_name, "mrf")
    sql_statements = "\n".join(statement for statement, _ in database_probe.statements)
    assert "FROM mrf.provider_stage" in sql_statements
    assert "INSERT INTO mrf.code_catalog" in sql_statements
    assert "INSERT INTO mrf.code_crosswalk" in sql_statements
    enrich_crosswalk.assert_awaited_once_with(
        schema="mrf",
        prescription_table="prescription_stage",
        code_catalog_table="code_catalog",
        code_crosswalk_table="code_crosswalk",
    )


@pytest.mark.asyncio
async def test_crosswalk_mode_selects_snapshot_and_live(monkeypatch):
    snapshot_enrichment = AsyncMock(return_value={"mapped_codes": 2, "edges": 4})
    live_enrichment = AsyncMock(
        return_value={"attempted": 1, "mapped_codes": 1, "edges": 2}
    )
    monkeypatch.setattr(drug_claims, "_enrich_rx_crosswalk_from_snapshot", snapshot_enrichment)
    monkeypatch.setattr(drug_claims, "_enrich_rx_crosswalk_from_live", live_enrichment)
    monkeypatch.setattr(drug_claims, "_step_start", lambda label: 0.0)
    monkeypatch.setattr(drug_claims, "_step_end", lambda label, started_at: None)
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_SOURCE", "unsupported")
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_LIVE_FALLBACK", True)

    await drug_claims._enrich_external_rx_crosswalk(
        schema="mrf",
        prescription_table="prescription",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
    )
    snapshot_enrichment.assert_awaited_once()
    live_enrichment.assert_awaited_once()

    snapshot_enrichment.reset_mock()
    live_enrichment.reset_mock()
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_SOURCE", "snapshot")
    await drug_claims._enrich_external_rx_crosswalk(
        schema="mrf",
        prescription_table="prescription",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
    )
    snapshot_enrichment.assert_awaited_once()
    live_enrichment.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_snapshot_and_cleanup_failure_are_safe(monkeypatch):
    monkeypatch.setattr(
        drug_claims,
        "_is_table_present",
        AsyncMock(side_effect=[True, False]),
    )
    assert await drug_claims._resolve_snapshot_source_layout() is None

    database_probe = SimpleNamespace(status=AsyncMock(side_effect=RuntimeError("locked")))
    monkeypatch.setattr(drug_claims, "db", database_probe)
    stage_tables = drug_claims.SnapshotStageTables("hp", "snapshot", "candidates")
    await drug_claims._drop_snapshot_stage_tables(stage_tables)
    assert database_probe.status.await_count == 3


@pytest.mark.asyncio
async def test_snapshot_returns_zero_when_no_candidates(monkeypatch):
    source_layout = drug_claims.SnapshotSourceLayout(
        schema="rx",
        product_table="product",
        package_table="package",
        rxnorm_join_sql="",
        rxnorm_source_sql="''",
        package_join_sql="",
        ndc_source_sql="p.product_ndc",
    )
    database_probe = SimpleNamespace(
        status=AsyncMock(return_value=1),
        scalar=AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        drug_claims,
        "_resolve_snapshot_source_layout",
        AsyncMock(return_value=source_layout),
    )
    monkeypatch.setattr(drug_claims, "_prepare_snapshot_candidate_tables", AsyncMock())
    monkeypatch.setattr(drug_claims, "db", database_probe)
    monkeypatch.setattr(drug_claims.secrets, "token_hex", lambda size: "stage")

    summary_by_metric = await drug_claims._enrich_rx_crosswalk_from_snapshot(
        schema="mrf",
        prescription_table="prescription",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
    )
    assert summary_by_metric == {"mapped_codes": 0, "edges": 0}
    assert database_probe.status.await_count == 6


class _HttpResponse:
    def __init__(self, status, response_json=None):
        self.status = status
        self.response_json = response_json

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def json(self, content_type=None):
        return self.response_json


class _HttpClient:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.request_urls = []

    def get(self, url, timeout):
        self.request_urls.append(url)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_live_json_status_and_error_contract():
    timeout = drug_claims.aiohttp.ClientTimeout(total=1)
    client = _HttpClient(
        [
            _HttpResponse(404),
            _HttpResponse(503),
            _HttpResponse(200, {"ok": True}),
            RuntimeError("network"),
        ]
    )
    assert await drug_claims._live_get_json(client, "http://drug/404", timeout) is None
    assert await drug_claims._live_get_json(client, "http://drug/503", timeout) is None
    assert await drug_claims._live_get_json(client, "http://drug/ok", timeout) == {
        "ok": True
    }
    assert await drug_claims._live_get_json(client, "http://drug/error", timeout) is None


@pytest.mark.asyncio
async def test_live_package_and_name_resolution_contract(monkeypatch):
    client = object()
    timeout = object()
    live_json = AsyncMock(
        side_effect=[
            {"generic": [{"product_ndc": "0001-0001"}]},
            {"brand": [{"product_ndc": "0002-0002"}]},
            "invalid-packages",
            [
                {"ndc11": "00000000001"},
                {"package_ndc": "00000000002"},
                None,
            ],
        ]
    )
    monkeypatch.setattr(drug_claims, "_live_get_json", live_json)
    monkeypatch.setattr(drug_claims, "RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE", 10)

    product_ndcs = await drug_claims._live_product_ndcs_for_terms(
        client,
        timeout,
        ["Generic A", "Brand A"],
    )
    assert product_ndcs == ["0001-0001", "0002-0002"]
    assert await drug_claims._live_package_ndcs(client, timeout, "invalid") == set()
    assert await drug_claims._live_package_ndcs(client, timeout, "valid") == {
        "00000000001",
        "00000000002",
    }


@pytest.mark.asyncio
async def test_live_external_code_resolution_contract(monkeypatch):
    assert await drug_claims._resolve_live_external_codes_for_entry(
        object(),
        object(),
        {},
    ) == {
        "rx_code": "",
        "rxnorm_codes": [],
        "ndc_codes": [],
        "display_name": None,
    }
    monkeypatch.setattr(
        drug_claims,
        "_live_product_ndcs_for_terms",
        AsyncMock(return_value=["0001-0001", "0002-0002"]),
    )
    monkeypatch.setattr(
        drug_claims,
        "_live_get_json",
        AsyncMock(
            side_effect=[
                {
                    "generic_name": "Resolved Generic",
                    "rxnorm_ids": [" 123 ", "bad"],
                    "product_ndc": "00000000003",
                },
                None,
            ]
        ),
    )
    monkeypatch.setattr(
        drug_claims,
        "_live_package_ndcs",
        AsyncMock(side_effect=[{"00000000004"}, set()]),
    )

    resolved_codes = await drug_claims._resolve_live_external_codes_for_entry(
        object(),
        object(),
        {
            "rx_code": "HP1",
            "generic_name": "Generic A",
            "brand_name": "Generic A",
            "rx_name": "Display A",
        },
    )
    assert resolved_codes == {
        "rx_code": "HP1",
        "display_name": "Resolved Generic",
        "rxnorm_codes": ["123"],
        "ndc_codes": ["00000000003", "00000000004"],
    }


@pytest.mark.asyncio
async def test_live_resolution_upsert_filters_invalid_codes(monkeypatch):
    upsert = AsyncMock(return_value=2)
    monkeypatch.setattr(drug_claims, "_upsert_external_code_and_edges", upsert)
    assert await drug_claims._upsert_live_resolution(
        {},
        schema="mrf",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
        default_confidence=0.9,
        crosswalk_source="synthetic",
    ) == (0, 0)

    mapped_codes, inserted_edges = await drug_claims._upsert_live_resolution(
        {
            "rx_code": "HP1",
            "display_name": "Generic A",
            "rxnorm_codes": ["123", "bad"],
            "ndc_codes": ["00000000001", "short"],
        },
        schema="mrf",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
        default_confidence=0.9,
        crosswalk_source="synthetic",
    )
    assert (mapped_codes, inserted_edges) == (1, 4)
    assert upsert.await_count == 2


@pytest.mark.asyncio
async def test_live_enrichment_batches_success_and_task_error(monkeypatch):
    unresolved_codes = [
        {"rx_code": "HP1"},
        {"rx_code": "HP2"},
    ]
    client = _HttpClient([_HttpResponse(200, {"ok": True})])
    monkeypatch.setattr(
        drug_claims,
        "_collect_unresolved_hp_rx_codes",
        AsyncMock(return_value=unresolved_codes),
    )
    monkeypatch.setattr(
        drug_claims,
        "_live_crosswalk_client_and_timeout",
        AsyncMock(return_value=(client, object())),
    )
    monkeypatch.setattr(
        drug_claims,
        "_is_live_drug_api_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        drug_claims,
        "_resolve_live_external_codes_for_entry",
        AsyncMock(side_effect=[{"rx_code": "HP1"}, RuntimeError("resolver")]),
    )
    monkeypatch.setattr(
        drug_claims,
        "_upsert_live_resolution",
        AsyncMock(return_value=(1, 2)),
    )

    summary_by_metric = await drug_claims._enrich_rx_crosswalk_from_live(
        schema="mrf",
        prescription_table="prescription",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
    )
    assert summary_by_metric == {"attempted": 2, "mapped_codes": 1, "edges": 2}

    monkeypatch.setattr(
        drug_claims,
        "_collect_unresolved_hp_rx_codes",
        AsyncMock(return_value=[]),
    )
    assert await drug_claims._enrich_rx_crosswalk_from_live(
        schema="mrf",
        prescription_table="prescription",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
    ) == {"attempted": 0, "mapped_codes": 0, "edges": 0}


@pytest.mark.asyncio
async def test_unavailable_live_api_preserves_attempt_count(monkeypatch):
    client = _HttpClient([])
    monkeypatch.setattr(
        drug_claims,
        "_collect_unresolved_hp_rx_codes",
        AsyncMock(return_value=[{"rx_code": "HP1"}]),
    )
    monkeypatch.setattr(
        drug_claims,
        "_live_crosswalk_client_and_timeout",
        AsyncMock(return_value=(client, object())),
    )
    monkeypatch.setattr(
        drug_claims,
        "_is_live_drug_api_available",
        AsyncMock(return_value=False),
    )
    assert await drug_claims._enrich_rx_crosswalk_from_live(
        schema="mrf",
        prescription_table="prescription",
        code_catalog_table="catalog",
        code_crosswalk_table="crosswalk",
    ) == {"attempted": 1, "mapped_codes": 0, "edges": 0}
