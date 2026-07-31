# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


def _request(args, session=None):
    return SimpleNamespace(
        args=args,
        ctx=SimpleNamespace(sa_session=session),
    )


def test_npi_detail_cache_disabled_and_expired(monkeypatch):
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0)
    assert npi_module._npi_detail_response_cache_get("disabled") is None

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 1)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 10)
    npi_module._NPI_DETAIL_RESPONSE_CACHE["expired"] = (
        time.monotonic() - 10,
        b"old",
    )
    assert npi_module._npi_detail_response_cache_get("expired") is None
    assert "expired" not in npi_module._NPI_DETAIL_RESPONSE_CACHE


def test_bounded_fhir_codings_skip_invalid_and_empty_values():
    result = npi_module._bounded_provider_directory_fhir_codings(
        [
            "not-a-mapping",
            {},
            {"code": "282N00000X", "userSelected": True},
        ]
    )

    assert result == [{"code": "282N00000X", "user_selected": True}]


def test_geo_candidate_pairs_skip_invalid_and_duplicate_rows(monkeypatch):
    monkeypatch.setattr(npi_module, "_MATCH_CANDIDATES_MAX_INTERNAL_ROWS", 1)
    rows = [
        {"npi": None, "address_key": "one"},
        {"npi": 1, "address_key": ""},
        {"npi": 1, "address_key": "one"},
        {"npi": 1, "address_key": "one"},
        {"npi": 2, "address_key": "two"},
    ]

    assert npi_module._geo_candidate_address_pairs(rows) == [(1, "one")]


@pytest.mark.asyncio
async def test_match_candidate_params_reject_mismatched_terms():
    with pytest.raises(
        sanic.exceptions.InvalidUsage,
        match="provider_type and specialty must match",
    ):
        await npi_module._normalize_match_candidate_params(
            _request(
                {
                    "phone": "5550100000",
                    "provider_type": "hospital",
                    "specialty": "clinic",
                }
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("suggestions", "expected_fragment"),
    [
        ((), "Unrecognized provider_type: missing."),
        (("Hospital",), "Suggestions: Hospital."),
    ],
)
async def test_match_candidate_params_report_unresolved_specialty(
    suggestions,
    expected_fragment,
    monkeypatch,
):
    ensure_cache = AsyncMock()
    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: _AsyncContext("connection"),
    )
    monkeypatch.setattr(
        npi_module,
        "ensure_specialty_resolution_cache",
        ensure_cache,
    )
    monkeypatch.setattr(
        npi_module,
        "resolve_provider_specialty_filter",
        lambda _args: SimpleNamespace(
            unresolved_specialty=True,
            suggested_specialties=suggestions,
        ),
    )

    with pytest.raises(sanic.exceptions.InvalidUsage) as error:
        await npi_module._normalize_match_candidate_params(
            _request(
                {
                    "phone": "5550100000",
                    "provider_type": "missing",
                }
            )
        )

    assert expected_fragment in str(error.value)
    ensure_cache.assert_awaited_once_with("connection")


@pytest.mark.parametrize(
    ("specialty_filter", "expected_fragment"),
    [
        (
            SimpleNamespace(
                taxonomy_codes=("282N00000X",),
                classification="Hospital",
            ),
            "healthcare_provider_taxonomy_code = ANY",
        ),
        (
            SimpleNamespace(
                taxonomy_codes=(),
                classification="Hospital",
            ),
            "nu.classification = :match_taxonomy_classification",
        ),
    ],
)
def test_taxonomy_filter_uses_specialty_fallbacks(
    specialty_filter,
    expected_fragment,
):
    query_parameter_map = {}
    sql = npi_module._match_candidate_taxonomy_filter_sql(
        {
            "specialty_filter": specialty_filter,
            "taxonomy_exact": (),
            "taxonomy_prefixes": (),
        },
        query_parameter_map,
    )

    assert expected_fragment in sql


def test_build_npi_where_clause_normalizes_prefix_and_empty_name(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_names_like_filter_clause",
        lambda _alias, _names: ("", {}),
    )

    where_sql, params = npi_module._build_npi_where_clause(
        "candidate",
        ["ignored"],
        "Ada",
        None,
        None,
        None,
    )

    assert "candidate.provider_first_name" in where_sql
    assert params == {"first_name": "%ada%"}


@pytest.mark.asyncio
async def test_evidence_capability_refuses_missing_required_table(monkeypatch):
    result = SimpleNamespace(
        all=lambda: [
            {"table_name": "required", "is_available": False},
            {"table_name": "optional", "is_available": True},
        ]
    )
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(return_value=result),
    )

    assert (
        await npi_module._provider_directory_evidence_tables(
            object(),
            required_names=("required",),
            optional_names=("optional",),
        )
        is None
    )


@pytest.mark.asyncio
async def test_evidence_fetchers_short_circuit_empty_and_missing_tables(
    monkeypatch,
):
    assert await npi_module._fetch_provider_directory_role_evidence_map([]) == {}
    assert (
        await npi_module._fetch_provider_directory_affiliation_evidence_map([])
        == {}
    )

    monkeypatch.setattr(
        npi_module,
        "_provider_directory_evidence_tables",
        AsyncMock(return_value=None),
    )
    session = object()
    assert (
        await npi_module._fetch_provider_directory_role_evidence_map(
            [("source", "role")],
            session=session,
        )
        == {}
    )
    assert (
        await npi_module._fetch_provider_directory_affiliation_evidence_map(
            [("source", "affiliation")],
            session=session,
        )
        == {}
    )


@pytest.mark.asyncio
async def test_source_detail_fetch_short_circuits_empty_and_unavailable(
    monkeypatch,
):
    assert await npi_module._fetch_provider_directory_source_detail_map([]) == {}

    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(return_value=False),
    )
    assert (
        await npi_module._fetch_provider_directory_source_detail_map(["source"])
        == {}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        (),
        (SimpleNamespace(_mapping={"npi": 1}),),
    ],
)
async def test_match_candidate_rows_map_empty_and_mapping_results(
    rows,
    monkeypatch,
):
    result = SimpleNamespace(all=lambda: rows)
    monkeypatch.setattr(
        npi_module,
        "_address_serving_table_sql",
        AsyncMock(return_value="mrf.address"),
    )
    monkeypatch.setattr(
        npi_module,
        "_match_candidate_query",
        lambda _params, _table: ("query", {"limit": 5}),
    )
    monkeypatch.setattr(
        npi_module,
        "_execute_match_candidate_query",
        AsyncMock(return_value=result),
    )

    mapped_rows = await npi_module._fetch_match_candidate_rows({"limit": 5})

    assert mapped_rows == ([{"npi": 1}] if rows else [])
