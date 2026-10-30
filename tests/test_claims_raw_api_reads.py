# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Raw-consumer protocol tests; synthetic native receipts confer no Source authority."""

import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api import network_read_execution as execution
from api import ptg2_code_context, ptg2_code_details
from api.endpoint import npi, pricing
from api.network_address_scope import current_network_address_scope
from db.connection import ConnectionProxy
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError
from tests.test_pricing_api import FakeResult, FakeSession

_NAMES = (
    "pricing_provider_procedure",
    "pricing_provider_prescription",
    "pricing_provider",
    "pricing_procedure",
    "pricing_provider_quality_feature",
    "code_catalog",
    "code_crosswalk",
)


async def _session(rows=(), *, installed=True, only_names=_NAMES):
    result = FakeResult(rows=list(rows), scalar=2024)
    result.close = Mock()
    session = FakeSession([result])
    session.info = {}
    if installed:
        pins = tuple(CompactAddressRelation("mrf", name, index + 1) for index, name in enumerate(_NAMES))
        await execution.install_network_read_inventory(
            session, pins, only_relations=tuple(pin for pin in pins if pin.table_name in only_names)
        )
    assert current_network_address_scope() is None
    return session, result


async def _consume(monkeypatch, consumer, session):
    if consumer == "crosswalk":
        return await ptg2_code_context._query_ptg2_code_crosswalk_edges(session, {("CPT", "00100")})
    if consumer == "details":
        return await ptg2_code_details._enrich_ptg2_code_details(
            session,
            {"items": [{"reported_code_system": "CPT", "reported_code": "00100"}]},
            {"include_code_details": "1"},
        )
    if consumer == "year":
        monkeypatch.delenv("HLTHPRT_NPI_FILTER_DEFAULT_YEAR", raising=False)
        monkeypatch.setattr(npi, "_is_table_available", AsyncMock(return_value=True))
        return await npi._resolve_filter_year(None, True, True, session=session)
    if consumer == "autocomplete":
        return await pricing._query_procedure_autocomplete_catalog(
            session, search_query="x_%\\", target_systems=("CPT",), year=2024, limit=7
        )
    if consumer == "profile":
        monkeypatch.setattr(pricing, "_is_table_available", AsyncMock(return_value=False))
        return await pricing._load_provider_quality_profile(session, npi=1000000001, year=2024)
    if consumer == "taxonomy_quality":
        return await pricing._load_quality_procedure_taxonomy_evidence(session, year=2024, internal_codes=[7], limit=9)
    if consumer == "taxonomy_fallback":
        monkeypatch.setattr(pricing, "_procedure_taxonomy_evidence_cache_get", Mock(return_value=None))
        monkeypatch.setattr(pricing, "_is_table_available", AsyncMock(return_value=False))
        return await pricing._load_procedure_taxonomy_evidence(session, year=2024, internal_codes=[7, 8], limit=9)
    raise AssertionError(consumer)


@pytest.mark.parametrize("installed", [False, True])
@pytest.mark.parametrize(
    "consumer,names",
    [
        ("crosswalk", ("code_crosswalk",)),
        ("details", ("code_catalog",)),
        ("year", ("pricing_provider_procedure", "pricing_provider_prescription")),
        ("autocomplete", ("pricing_procedure",)),
        ("profile", ("pricing_provider",)),
        ("taxonomy_quality", ("pricing_provider_procedure", "pricing_provider", "pricing_provider_quality_feature")),
        ("taxonomy_fallback", ("pricing_provider_procedure", "pricing_provider")),
    ],
)
async def test_raw_consumers_render_only_exact_proved_slots_and_preserve_parameters(
    monkeypatch, installed, consumer, names
):
    session, _ = await _session([(2024,)] if consumer == "year" else [], installed=installed)
    await _consume(monkeypatch, consumer, session)
    (statement, parameters), _ = session.executions[-1]
    sql = str(statement)
    for name in names:
        relation_sql = f'ONLY "mrf"."{name}"' if installed else f"mrf.{name}"
        assert relation_sql in sql
    assert "ONLY base" not in sql and "ONLY normalized" not in sql and "ONLY derived" not in sql
    assert "ONLY mrf.nucc_taxonomy" not in sql and 'ONLY "mrf"."npi"' not in sql
    if consumer == "autocomplete":
        assert "^[A-Z0-9]{5}$" in sql and "^[0-9]{5}$" in sql
        assert parameters == {
            "q_like": "%x\\_\\%\\\\%",
            "q_prefix": "x\\_\\%\\\\%",
            "target_system": "CPT",
            "year": 2024,
            "limit": 7,
        }
    elif consumer == "crosswalk":
        assert parameters == {"system_0": "CPT", "code_0": "00100"}
    elif consumer.startswith("taxonomy"):
        assert parameters == {
            "year": 2024,
            "internal_codes": [7, 8] if consumer.endswith("fallback") else [7],
            "limit": 9,
        }
    elif consumer == "profile":
        assert parameters == {"npi": 1000000001, "year": 2024}


@pytest.mark.parametrize(
    "consumer", ["crosswalk", "details", "year", "autocomplete", "profile", "taxonomy_quality", "taxonomy_fallback"]
)
async def test_raw_consumers_refuse_changed_inventory_before_rows_escape_without_network_scope(monkeypatch, consumer):
    session, result = await _session()
    result.first = result.all = Mock(side_effect=AssertionError("unverified rows escaped"))
    monkeypatch.setattr(
        execution, "verify_bound_network_names", AsyncMock(side_effect=NetworkReadIdentityError("changed"))
    )
    with pytest.raises(ServiceUnavailable):
        await _consume(monkeypatch, consumer, session)
    result.close.assert_called_once()
    result.all.assert_not_called()


async def test_unselected_feature_slot_and_other_names_never_gain_only(monkeypatch):
    session, _ = await _session(only_names=("pricing_provider",))
    await _consume(monkeypatch, "taxonomy_quality", session)
    sql = str(session.executions[-1][0][0])
    assert 'JOIN ONLY "mrf"."pricing_provider" p' in sql
    assert 'FROM "mrf"."pricing_provider_procedure" pp' in sql
    assert 'JOIN "mrf"."pricing_provider_quality_feature" qf' in sql
    assert sql.count("ONLY") == 1


async def test_default_autocomplete_keeps_existing_overridable_constant(monkeypatch):
    session, _ = await _session(installed=False)
    selected = text("SELECT :year AS source_year")
    monkeypatch.setattr(pricing, "_PROCEDURE_AUTOCOMPLETE_CATALOG_SQL", selected)
    await _consume(monkeypatch, "autocomplete", session)
    assert session.executions[-1][0][0] is selected


@pytest.mark.parametrize("refuse", [False, True])
async def test_guarded_taxonomy_does_not_read_or_write_an_unbound_prior_cache(monkeypatch, refuse):
    session, result = await _session()
    cache_get = Mock(return_value=[{"taxonomy_code": "PRIOR"}])
    cache_set = Mock(side_effect=AssertionError("guarded result entered ordinary cache"))
    monkeypatch.setattr(pricing, "_procedure_taxonomy_evidence_cache_get", cache_get)
    monkeypatch.setattr(pricing, "_procedure_taxonomy_evidence_cache_set", cache_set)
    monkeypatch.setattr(pricing, "_is_table_available", AsyncMock(return_value=False))
    if refuse:
        monkeypatch.setattr(
            execution, "verify_bound_network_names", AsyncMock(side_effect=NetworkReadIdentityError("changed"))
        )
        with pytest.raises(ServiceUnavailable):
            await pricing._load_procedure_taxonomy_evidence(session, year=2024, internal_codes=[7, 8], limit=9)
        result.close.assert_called_once()
    else:
        assert await pricing._load_procedure_taxonomy_evidence(session, year=2024, internal_codes=[7, 8], limit=9) == []
    assert session.executions
    cache_get.assert_not_called()
    cache_set.assert_not_called()


@pytest.mark.parametrize(
    "method,arguments",
    [
        ("_load_provider_qpp_scores", {"npi": 1000000001, "year": 2024}),
        ("_load_provider_rx_totals", {"npi": 1000000001, "year": 2024}),
        ("_load_provider_svi", {"zip5": "10001", "year": 2024}),
        ("_load_quality_peer_targets", {"year": 2024}),
    ],
)
async def test_optional_quality_fallbacks_do_not_swallow_identity_refusal(monkeypatch, method, arguments):
    session, result = await _session()
    monkeypatch.setattr(
        execution, "verify_bound_network_names", AsyncMock(side_effect=NetworkReadIdentityError("changed"))
    )
    with pytest.raises(ServiceUnavailable):
        await getattr(pricing, method)(session, **arguments)
    result.close.assert_called_once()


@pytest.mark.parametrize(
    "method,arguments",
    [
        ("_is_table_available", ("pricing_provider_procedure",)),
        ("_table_columns", ("pricing_provider_procedure",)),
        ("_npi_canonical_publication_identity", ()),
        ("_fetch_provider_enrichment_summary_rows", ([1000000001],)),
    ],
)
async def test_npi_optional_metadata_fallbacks_propagate_original_session_refusal(monkeypatch, method, arguments):
    session, result = await _session()
    monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", False)
    monkeypatch.setattr(
        execution, "verify_bound_network_names", AsyncMock(side_effect=NetworkReadIdentityError("changed"))
    )
    with pytest.raises(ServiceUnavailable):
        await getattr(npi, method)(*arguments, session=session)
    result.close.assert_called_once()


@pytest.mark.parametrize("endpoint", ["list", "near"])
@pytest.mark.parametrize("refuse", [False, True])
async def test_npi_claims_filters_keep_guarded_reader_without_network_scope(monkeypatch, endpoint, refuse):
    session, buffered_result = await _session([(0,)] if endpoint == "list" else [])
    monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    monkeypatch.setattr(
        npi,
        "_resolve_npi_filter_capabilities",
        AsyncMock(
            return_value={
                "npi_procedures_array_available": False,
                "npi_medications_array_available": False,
                "pricing_provider_procedure_available": True,
                "pricing_provider_prescription_available": True,
            }
        ),
    )

    @asynccontextmanager
    async def acquire():
        yield ConnectionProxy(None, session, None)

    @asynccontextmanager
    async def snapshot(*_args, **_kwargs):
        yield session

    monkeypatch.setattr(npi, "db", SimpleNamespace(acquire=acquire))
    monkeypatch.setattr(npi, "provider_profile_read_snapshot", snapshot)
    monkeypatch.setattr(npi, "provider_geo_read_snapshot", snapshot)
    if refuse:
        monkeypatch.setattr(
            execution, "verify_bound_network_names", AsyncMock(side_effect=NetworkReadIdentityError("changed"))
        )
    request_args_by_name = {"procedure_codes": "7", "medication_codes": "8", "year": "2024", "limit": "1"}
    request_args_by_name.update(
        {"count_only": "1"} if endpoint == "list" else {"lat": "40", "long": "-73", "view": "card"}
    )
    request = SimpleNamespace(args=request_args_by_name, app=SimpleNamespace(), ctx=SimpleNamespace(sa_session=session))
    operation = npi.list_providers(request) if endpoint == "list" else npi.get_near_npi(request)
    if refuse:
        with pytest.raises(ServiceUnavailable):
            await operation
        buffered_result.close.assert_called_once()
    else:
        assert (await operation).status == 200
    (statement, parameters), _ = session.executions[-1]
    sql = str(statement)
    assert 'FROM ONLY "mrf"."pricing_provider_procedure" AS pp' in sql
    assert 'FROM ONLY "mrf"."pricing_provider_prescription" AS pr' in sql
    assert parameters["procedure_code_0"] == 7 and parameters["medication_code_0"] == 8
    assert parameters["filter_year"] == 2024


@pytest.mark.parametrize("method", ["_is_table_available", "_table_columns"])
async def test_guarded_pricing_metadata_ignores_stale_global_hits_and_leaves_cache_unchanged(monkeypatch, method):
    session, _ = await _session([("year",)])
    table_exists_by_key = {"mrf.pricing_provider_procedure": (time.monotonic(), False)}
    table_columns_by_key = {"mrf.pricing_provider_procedure": (time.monotonic(), ("prior_column",))}
    before = (table_exists_by_key.copy(), table_columns_by_key.copy())
    monkeypatch.setattr(pricing, "ENABLE_PRICING_SCHEMA_CACHE", True)
    monkeypatch.setattr(pricing, "_PRICING_TABLE_EXISTS_CACHE", table_exists_by_key)
    monkeypatch.setattr(pricing, "_PRICING_TABLE_COLUMNS_CACHE", table_columns_by_key)
    current = await getattr(pricing, method)(session, "pricing_provider_procedure")
    assert current == (True if method == "_is_table_available" else {"year"})
    assert len(session.executions) == 1
    assert (table_exists_by_key, table_columns_by_key) == before


@pytest.mark.parametrize("method", ["_is_table_available", "_table_columns", "_resolve_npi_filter_capabilities"])
async def test_guarded_npi_metadata_skips_ordinary_cache_reads_and_writes(monkeypatch, method):
    session, _ = await _session([("procedures_array",)])
    ordinary_get = Mock(return_value={"npi_procedures_array_available": False})
    ordinary_set = Mock(side_effect=AssertionError("request-bound metadata entered process cache"))
    monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", True)
    for name in ("_cache_get", "_filter_cache_get"):
        monkeypatch.setattr(npi, name, ordinary_get)
    for name in ("_cache_set", "_filter_cache_set"):
        monkeypatch.setattr(npi, name, ordinary_set)
    arguments = () if method == "_resolve_npi_filter_capabilities" else ("pricing_provider_procedure",)
    current = await getattr(npi, method)(*arguments, session=session)
    if method == "_resolve_npi_filter_capabilities":
        assert current["npi_procedures_array_available"] is True
        assert current["npi_medications_array_available"] is False
        assert len(session.executions) == 3
    else:
        assert current == (True if method == "_is_table_available" else {"procedures_array"})
        assert len(session.executions) == 1
    ordinary_get.assert_not_called()
    ordinary_set.assert_not_called()


@pytest.mark.parametrize("basis", ["quality_signal", "fallback_signal", "quality_feature", "raw"])
async def test_each_current_taxonomy_evidence_basis_bypasses_the_ordinary_cache(monkeypatch, basis):
    taxonomy_evidence_by_field = {
        "taxonomy_code": "207Q00000X",
        "distinct_npis": 100000,
        "total_services": 100000,
        "total_beneficiaries": 100000,
        "provider_types": ["Family Medicine"],
    }
    session, _ = await _session([taxonomy_evidence_by_field])
    if basis == "fallback_signal":
        session._results.insert(0, FakeResult(rows=[]))
    ordinary_get = Mock(return_value=[{"taxonomy_code": "PRIOR"}])
    ordinary_set = Mock(side_effect=AssertionError("guarded taxonomy basis entered process cache"))
    monkeypatch.setattr(pricing, "_procedure_taxonomy_evidence_cache_get", ordinary_get)
    monkeypatch.setattr(pricing, "_procedure_taxonomy_evidence_cache_set", ordinary_set)
    monkeypatch.setattr(
        pricing,
        "_is_table_available",
        AsyncMock(
            side_effect=lambda _session, name: (
                name
                == (
                    pricing.procedure_taxonomy_signal_table.name
                    if basis.endswith("signal")
                    else pricing.QUALITY_FEATURE_TABLE_NAME
                )
                and basis != "raw"
            )
        ),
    )
    current = await pricing._load_procedure_taxonomy_evidence(session, year=2024, internal_codes=[7], limit=9)
    assert current[0]["taxonomy_code"] == "207Q00000X"
    assert len(session.executions) == (2 if basis == "fallback_signal" else 1)
    ordinary_get.assert_not_called()
    ordinary_set.assert_not_called()
