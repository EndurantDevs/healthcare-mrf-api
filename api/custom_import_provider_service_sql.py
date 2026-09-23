# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose one prepared imported-NPI relation with provider-service claims."""

from __future__ import annotations

from dataclasses import dataclass

from sanic.exceptions import InvalidUsage
from sqlalchemy import Float, String, case, cast, exists, func, select
from sqlalchemy.sql import Select

from process.custom_import.read_contracts import CustomImportReadUnavailableError
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadOrderTerm


def _is_sha256(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _require_unpaged(statement: Select, *, relation: str) -> None:
    if statement._limit_clause is not None or statement._offset_clause is not None or statement._order_by_clauses:
        raise CustomImportReadUnavailableError(f"{relation} is not unpaged")


@dataclass(frozen=True, slots=True)
class ProviderServiceImportQuery:
    """Trusted imported membership and ordering for one claims-provider query."""

    prepared: PreparedNpiEntityRelation
    require_match: bool

    def __post_init__(self) -> None:
        if type(self.prepared) is not PreparedNpiEntityRelation or type(self.require_match) is not bool:
            raise CustomImportReadUnavailableError("provider-service import query is invalid")
        if not isinstance(self.prepared.statement, Select):
            raise CustomImportReadUnavailableError("provider-service import relation is invalid")
        _require_unpaged(self.prepared.statement, relation="provider-service import relation")
        if not _is_sha256(self.prepared.query_fingerprint) or not _is_sha256(self.prepared.authorization_scope_sha256):
            raise CustomImportReadUnavailableError("provider-service import query is invalid")
        if type(self.prepared.normalized_order_terms) is not tuple or any(
            type(term) is not ReadOrderTerm for term in self.prepared.normalized_order_terms
        ):
            raise CustomImportReadUnavailableError("provider-service import order is invalid")
        expected_columns = ("entity_value",) + tuple(
            f"sort_{ordinal}" for ordinal in range(len(self.prepared.normalized_order_terms))
        )
        if tuple(self.prepared.statement.selected_columns.keys()) != expected_columns:
            raise CustomImportReadUnavailableError("provider-service import relation is invalid")


@dataclass(frozen=True, slots=True)
class ProviderServiceClaimsRelation:
    """Unpaged native claims relation with optional imported ordering."""

    statement: Select
    native_relation: object
    import_order_terms: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class ProviderServiceClaimsStatements:
    """Exact count and unpaged page statements for one claims-provider query."""

    count_statement: Select
    page_statement: Select


def compose_provider_service_claims_relation(
    native_grouped_statement: Select,
    import_context: ProviderServiceImportQuery,
) -> ProviderServiceClaimsRelation:
    """Compose winners before a caller applies aggregate metrics, count, and page."""

    if type(import_context) is not ProviderServiceImportQuery:
        raise CustomImportReadUnavailableError("provider-service import query is invalid")
    if not isinstance(native_grouped_statement, Select):
        raise CustomImportReadUnavailableError("provider-service claims relation is invalid")
    _require_unpaged(native_grouped_statement, relation="provider-service claims relation")
    native_relation = native_grouped_statement.subquery("native_provider_service")
    if "npi" not in native_relation.c:
        raise CustomImportReadUnavailableError("provider-service claims relation is invalid")

    imported_relation = import_context.prepared.statement.subquery("custom_import_provider_service")
    is_matching_npi = imported_relation.c.entity_value == cast(native_relation.c.npi, String)
    order_terms = import_context.prepared.normalized_order_terms
    if not order_terms:
        statement = select(native_relation)
        if import_context.require_match:
            statement = statement.where(exists(select(1).select_from(imported_relation).where(is_matching_npi)))
        return ProviderServiceClaimsRelation(statement, native_relation, ())

    statement = select(native_relation).select_from(native_relation.outerjoin(imported_relation, is_matching_npi))
    if import_context.require_match:
        statement = statement.where(imported_relation.c.entity_value.is_not(None))
    import_order_terms: list[object] = [imported_relation.c.entity_value.is_(None).asc()]
    for ordinal, term in enumerate(order_terms):
        expression = imported_relation.c[f"sort_{ordinal}"]
        directed_expression = expression.asc() if term.direction == "asc" else expression.desc()
        import_order_terms.append(
            directed_expression.nullsfirst() if term.nulls == "first" else directed_expression.nullslast()
        )
    import_order_terms.append(native_relation.c.npi.asc())
    return ProviderServiceClaimsRelation(statement, native_relation, tuple(import_order_terms))


def build_provider_service_claims_statements(
    native_grouped_statement: Select,
    import_context: ProviderServiceImportQuery | None,
    min_claims: float | None,
    min_total_cost: float | None,
    order_by: str,
    order: str,
) -> ProviderServiceClaimsStatements:
    """Return count and page statements before the caller applies pagination."""

    if import_context is None:
        return _native_claims_statements(native_grouped_statement, order_by, order)
    return _imported_claims_statements(
        native_grouped_statement,
        import_context,
        min_claims,
        min_total_cost,
        order_by,
        order,
    )


def _native_claims_statements(
    native_grouped_statement: Select,
    order_by: str,
    order: str,
) -> ProviderServiceClaimsStatements:
    native_relation = native_grouped_statement.subquery()
    count_statement = select(func.count()).select_from(native_relation)
    page_statement = _ordered_provider_service_statement(
        select(native_relation), native_relation, order_by, order, include_npi_tiebreak=False
    )
    return ProviderServiceClaimsStatements(count_statement, page_statement)


def _imported_claims_statements(
    native_grouped_statement: Select,
    import_context: ProviderServiceImportQuery,
    min_claims: float | None,
    min_total_cost: float | None,
    order_by: str,
    order: str,
) -> ProviderServiceClaimsStatements:
    claims_relation = compose_provider_service_claims_relation(native_grouped_statement, import_context)
    page_statement = claims_relation.statement
    if min_claims is not None:
        page_statement = page_statement.where(claims_relation.native_relation.c.total_services >= min_claims)
    if min_total_cost is not None:
        page_statement = page_statement.where(claims_relation.native_relation.c.total_allowed_amount >= min_total_cost)
    count_statement = select(func.count()).select_from(page_statement.order_by(None).subquery())
    if claims_relation.import_order_terms:
        page_statement = page_statement.order_by(*claims_relation.import_order_terms)
    else:
        page_statement = _ordered_provider_service_statement(
            page_statement,
            claims_relation.native_relation,
            order_by,
            order,
            include_npi_tiebreak=True,
        )
    return ProviderServiceClaimsStatements(count_statement, page_statement)


def _ordered_provider_service_statement(
    statement: Select,
    native_relation: object,
    order_by: str,
    order: str,
    *,
    include_npi_tiebreak: bool,
) -> Select:
    cost_index_expr = case(
        (
            native_relation.c.total_services > 0,
            cast(native_relation.c.total_allowed_amount, Float) / native_relation.c.total_services,
        ),
        else_=None,
    ).label("cost_index")
    if order_by == "cost_index":
        statement = statement.add_columns(cost_index_expr)
    order_fields = _provider_service_order_fields(native_relation, cost_index_expr)
    order_column = order_fields.get(order_by)
    if order_column is None:
        allowed = ", ".join(sorted(order_fields))
        raise InvalidUsage(f"Unsupported order_by '{order_by}'. Allowed: {allowed}")
    ordered_statement = statement.order_by(order_column.asc() if order == "asc" else order_column.desc())
    return ordered_statement.order_by(native_relation.c.npi.asc()) if include_npi_tiebreak else ordered_statement


def _provider_service_order_fields(native_relation: object, cost_index_expr: object) -> dict[str, object]:
    return {
        "npi": native_relation.c.npi,
        "provider_name": native_relation.c.provider_name,
        "total_services": native_relation.c.total_services,
        "total_submitted_charges": native_relation.c.total_submitted_charges,
        "total_allowed_amount": native_relation.c.total_allowed_amount,
        "total_beneficiaries": native_relation.c.total_beneficiaries,
        "matched_service_codes": native_relation.c.matched_service_codes,
        "cost_index": cost_index_expr,
    }


__all__ = (
    "ProviderServiceClaimsRelation",
    "ProviderServiceClaimsStatements",
    "ProviderServiceImportQuery",
    "build_provider_service_claims_statements",
    "compose_provider_service_claims_relation",
)
