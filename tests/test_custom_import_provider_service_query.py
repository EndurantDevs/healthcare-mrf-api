# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Claims provider-service composition stays unpaged until the final page."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import literal, select
from sqlalchemy.dialects import postgresql

from api import custom_import_provider_service_sql as service_sql
from api.endpoint import pricing
from process.custom_import.read_contracts import CustomImportReadUnavailableError
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadOrderTerm


class _Result:
    def __init__(self, *, scalar=None, rows=()):
        self._scalar = scalar
        self._rows = list(rows)

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class _Session:
    def __init__(self, results):
        self._results = list(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return self._results.pop(0)


def _prepared(direction: str = "desc") -> PreparedNpiEntityRelation:
    return PreparedNpiEntityRelation(
        select(
            literal("1000000001").label("entity_value"),
            literal(7).label("sort_0"),
        ),
        (ReadOrderTerm("metric", direction, "last"),),
        "a" * 64,
        "b" * 64,
    )


def _request(session):
    return SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session))


def _compiled(statement) -> str:
    return str(statement.compile(dialect=postgresql.dialect()))


def _assert_imported_response(response, session, no_precomputed_count) -> None:
    response_document = json.loads(response.body)
    assert set(response_document) == {"items", "pagination", "query"}
    assert response_document["pagination"] == {"total": 2, "limit": 1, "offset": 0, "page": 1}
    assert response_document["items"][0]["npi"] == 1000000001
    assert len(session.statements) == 2
    no_precomputed_count.assert_not_awaited()


def _assert_imported_claims_sql(statements, direction: str) -> None:
    count_sql, page_sql = map(_compiled, statements)
    assert "custom_import_provider_service" in count_sql
    assert "ORDER BY" not in count_sql and "LIMIT" not in count_sql
    assert "GROUP BY" in page_sql
    assert page_sql.index("GROUP BY") < page_sql.index(") AS native_provider_service")
    assert page_sql.index(") AS native_provider_service") < page_sql.index("LEFT OUTER JOIN")
    assert page_sql.index("LEFT OUTER JOIN") < page_sql.rindex("WHERE native_provider_service.total_services")
    assert "pricing_provider_procedure.total_services >=" not in page_sql
    assert "custom_import_provider_service.entity_value IS NULL ASC" in page_sql
    assert f"custom_import_provider_service.sort_0 {direction.upper()} NULLS LAST" in page_sql
    assert "native_provider_service.npi ASC" in page_sql
    assert "LIMIT" in page_sql


@pytest.mark.parametrize("direction", ("asc", "desc"))
@pytest.mark.asyncio
async def test_by_service_import_order_groups_then_joins_counts_and_pages(monkeypatch, direction):
    session = _Session(
        (
            _Result(scalar=2),
            _Result(
                rows=(
                    {
                        "npi": 1000000001,
                        "provider_name": "Synthetic Provider",
                        "provider_type": "Synthetic Type",
                        "city": "Example",
                        "state": "EX",
                        "zip5": "12345",
                        "total_services": 12.0,
                        "total_submitted_charges": 24.0,
                        "total_allowed_amount": 36.0,
                        "total_beneficiaries": 4.0,
                        "matched_service_codes": 1,
                    },
                )
            ),
        )
    )
    resolve_year = AsyncMock(return_value=(2024, "request"))
    resolve_codes = AsyncMock(
        return_value=(
            [99213],
            {
                "input_code": {"code_system": "CPT", "code": "99213"},
                "resolved_codes": [],
                "matched_via": [],
            },
        )
    )
    no_precomputed_count = AsyncMock()
    monkeypatch.setattr(pricing, "_resolve_year", resolve_year)
    monkeypatch.setattr(pricing, "_resolve_internal_codes_for_request", resolve_codes)
    monkeypatch.setattr(pricing, "_precomputed_procedure_provider_count", no_precomputed_count)
    monkeypatch.setattr(pricing, "_enrich_provider_service_cost_indices", AsyncMock())

    response = await pricing.list_providers_by_procedure(
        _request(session),
        native_args={
            "code": "99213",
            "code_system": "CPT",
            "min_claims": "10",
            "min_total_cost": "20",
            "limit": "1",
            "offset": "0",
        },
        import_context=service_sql.ProviderServiceImportQuery(_prepared(direction), require_match=False),
    )

    _assert_imported_response(response, session, no_precomputed_count)
    _assert_imported_claims_sql(session.statements, direction)


def test_membership_only_relation_uses_exists_without_duplicate_native_rows():
    duplicate_npis = (
        select(literal("1000000001").label("entity_value"))
        .union_all(select(literal("1000000001").label("entity_value")))
        .subquery()
    )
    prepared = PreparedNpiEntityRelation(
        select(duplicate_npis.c.entity_value),
        (),
        "a" * 64,
        "b" * 64,
    )
    relation = service_sql.compose_provider_service_claims_relation(
        select(literal(1000000001).label("npi")),
        service_sql.ProviderServiceImportQuery(prepared, require_match=True),
    )

    compiled = _compiled(relation.statement)

    assert "EXISTS" in compiled
    assert "JOIN" not in compiled


def test_import_context_rejects_malformed_prepared_relation():
    malformed = PreparedNpiEntityRelation(
        select(literal("1000000001").label("not_an_npi")),
        (),
        "a" * 64,
        "b" * 64,
    )

    with pytest.raises(CustomImportReadUnavailableError, match="provider-service import relation"):
        service_sql.ProviderServiceImportQuery(malformed, require_match=True)


@pytest.mark.parametrize(
    "statement",
    (
        select(literal("1000000001").label("entity_value")).limit(1),
        select(literal("1000000001").label("entity_value")).order_by(literal(1)),
    ),
)
def test_import_context_rejects_paged_or_ordered_prepared_relation(statement):
    prepared = PreparedNpiEntityRelation(statement, (), "a" * 64, "b" * 64)

    with pytest.raises(CustomImportReadUnavailableError, match="not unpaged"):
        service_sql.ProviderServiceImportQuery(prepared, require_match=True)


def test_claims_composition_rejects_paged_native_relation():
    with pytest.raises(CustomImportReadUnavailableError, match="claims relation is not unpaged"):
        service_sql.compose_provider_service_claims_relation(
            select(literal(1000000001).label("npi")).limit(1),
            service_sql.ProviderServiceImportQuery(_prepared(), require_match=True),
        )


@pytest.mark.asyncio
async def test_by_service_import_context_requires_native_args_pair():
    with pytest.raises(pricing.InvalidUsage, match="arguments are invalid"):
        await pricing.list_providers_by_procedure(_request(_Session(())), native_args={})


@pytest.mark.parametrize("branch_args", ({"cursor": "opaque"}, {"plan_id": "TESTPLAN001"}))
@pytest.mark.asyncio
async def test_by_service_import_context_rejects_nonclaims_branches(branch_args):
    with pytest.raises(pricing.InvalidUsage, match="require the claims lane"):
        await pricing.list_providers_by_procedure(
            _request(_Session(())),
            native_args={"code": "99213", "code_system": "CPT", **branch_args},
            import_context=service_sql.ProviderServiceImportQuery(_prepared(), require_match=True),
        )
