# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded read services for already materialized ``custom-import/v1`` data.

This module deliberately has no HTTP registration or policy implementation.
An extension-facing adapter verifies its own authorization material, supplies a
stable authorization scope through :class:`ExtensionReadAuthorizer`, and maps
these typed results to its transport.  The read core then permits only an
explicit sealed generation and one exact selection profile.

Search starts from a persisted ``CustomImportWinner``.  A child predicate or
order therefore always addresses that winner's one selected context child; it
never joins a second child collection or falls back to a lower-ranked family
member.  Detail hydration takes the selected winner's family and returns every
member of that one immutable family, subject to a hard all-or-nothing bound.

Integration assumes a host constructs the pinned target from its own trusted
configuration, injects the authorization verifier and cursor secret, and
calls this service only after the P1 publication flow has sealed the selected
generation.  Transport registration and materialization ownership remain
outside this package.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import hmac
import re
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation

from sqlalchemy import and_, exists, func, not_, select, text, tuple_
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportDefinitionRevision,
    CustomImportEntityBinding,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportField,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportWinner,
)
from process.custom_import.definition import (
    CustomImportDefinition,
    DerivedQueryField,
    Field,
    canonical_json,
    canonical_sha256,
    load_json_definition,
)
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    MAX_CURSOR_TTL_SECONDS,
    MAX_DETAIL_CHILDREN,
    MAX_FAMILY_RESPONSE_BYTES,
    MAX_FILTER_TERMS,
    MAX_FULL_FAMILY_PAGE_SIZE,
    MAX_GROUPED_CHILD_PREDICATE_TERMS,
    MAX_GROUPED_PREDICATE_TERMS,
    MAX_NPI_PAGE_SIZE,
    MAX_ORDER_TERMS,
    MAX_PAGE_OFFSET,
    MAX_PAGE_SIZE,
    MAX_PROVIDER_FILTER_TERMS,
    MAX_PROVIDER_ORDER_TERMS,
    MAX_READ_TIMEOUT_MS,
    READ_CORE_CONTRACT,
    CustomImportReadAuthorizationError,
    CustomImportReadCache,
    CustomImportReadCursorError,
    CustomImportReadEntityAbsentError,
    CustomImportReadError,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
    ExtensionReadAuthorizer,
    ExtensionReadScope,
    PinnedReadTarget,
    _bounded_identifier,
    _positive_integer,
)
from process.custom_import.read_contracts import (
    canonical_read_document as _canonical_bytes,
)
from process.custom_import.read_cursor import (
    MAX_CURSOR_CHARACTERS,
    ReadCursorCodec,
    ReadCursorState,
)
from process.custom_import.read_identity import (
    resolve_generation_snapshot,
    verified_definition,
    verify_published_generation,
)
from process.custom_import.read_payload import full_family_payload
from process.custom_import.storage_layout import snapshot_models

_VALUE_STATES = frozenset({"value", "null", "missing"})
_FILTER_OPERATORS = frozenset({"eq", "neq", "gt", "gte", "lt", "lte", "is_null", "is_missing"})
_RANGE_FIELD_TYPES = frozenset({"integer", "decimal", "date", "timestamp"})
_DECIMAL_FILTER_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)")
_SCALAR_COLUMNS = {
    "string": "string_value",
    "integer": "integer_value",
    "decimal": "decimal_value",
    "boolean": "boolean_value",
    "date": "date_value",
    "timestamp": "timestamp_value",
}
_READ_TIMEOUT_SETTINGS = text(
    """
    SELECT current_setting('statement_timeout') AS timeout_text,
           setting AS timeout_milliseconds
    FROM pg_catalog.pg_settings
    WHERE name = 'statement_timeout'
    """
)
_MAX_ENTITY_VALUE_BYTES = 512
_FULL_FAMILY_ENTITLEMENT = "full_family"
_NPI_ENTITY_RELATION_FINGERPRINT_DOMAIN = b"custom-import-read-core/v1\x00npi-entity-relation/v1\x00"


@dataclass(frozen=True, slots=True)
class ReadFilter:
    """One declared root or selected-context scalar predicate."""

    field_id: str
    operator: str
    value: object | None = None

    def __post_init__(self) -> None:
        _bounded_identifier(self.field_id, "filter field_id")
        if type(self.operator) is not str or self.operator not in _FILTER_OPERATORS:
            raise CustomImportReadRequestError("filter operator is unsupported")


@dataclass(frozen=True, slots=True)
class ReadOrderTerm:
    """One declaration-owned order term supplied for exact request matching."""

    field_id: str
    direction: str
    nulls: str

    def __post_init__(self) -> None:
        _bounded_identifier(self.field_id, "order field_id")
        if (
            type(self.direction) is not str
            or type(self.nulls) is not str
            or self.direction not in {"asc", "desc"}
            or self.nulls not in {"first", "last"}
        ):
            raise CustomImportReadRequestError("order term is malformed")


@dataclass(frozen=True, slots=True)
class SearchRequest:
    """A bounded search over the exact pinned target and declared query surface."""

    target: PinnedReadTarget
    filters: tuple[ReadFilter, ...] = ()
    order_terms: tuple[ReadOrderTerm, ...] | None = None
    page_size: int = 50
    cursor: str | None = None

    def __post_init__(self) -> None:
        if type(self.target) is not PinnedReadTarget:
            raise CustomImportReadRequestError("search target is malformed")
        if type(self.filters) is not tuple:
            raise CustomImportReadRequestError("filters must be a tuple")
        if type(self.page_size) is not int or not 1 <= self.page_size <= MAX_PAGE_SIZE:
            raise CustomImportReadRequestError("page_size is outside the read-core limit")
        if self.order_terms is not None and type(self.order_terms) is not tuple:
            raise CustomImportReadRequestError("order_terms must be a tuple")
        if self.cursor is not None and (type(self.cursor) is not str or len(self.cursor) > MAX_CURSOR_CHARACTERS):
            raise CustomImportReadRequestError("cursor is malformed")


@dataclass(frozen=True, slots=True)
class EntityLocator:
    """One adapter-qualified entity value used to select a root family."""

    adapter_id: str
    value: str

    def __post_init__(self) -> None:
        _bounded_identifier(self.adapter_id, "entity adapter_id")
        if type(self.value) is not str:
            raise CustomImportReadRequestError("entity value is malformed")
        try:
            value_size = len(self.value.encode("utf-8"))
        except UnicodeEncodeError:
            raise CustomImportReadRequestError("entity value is malformed") from None
        if not 1 <= value_size <= _MAX_ENTITY_VALUE_BYTES:
            raise CustomImportReadRequestError("entity value is malformed")


@dataclass(frozen=True, slots=True)
class WinnerLocator:
    """The exact persisted winner used to select a root-family detail view."""

    root_record_id: int
    family_revision_id: int
    entity_binding_id: int
    context_key_sha256: bytes

    def __post_init__(self) -> None:
        _positive_integer(self.root_record_id, "root_record_id")
        _positive_integer(self.family_revision_id, "family_revision_id")
        _positive_integer(self.entity_binding_id, "entity_binding_id")
        if type(self.context_key_sha256) is not bytes or len(self.context_key_sha256) != 32:
            raise CustomImportReadRequestError("winner context key is malformed")


@dataclass(frozen=True, slots=True)
class RootDetailRequest:
    """One exact target, entity selector, and full-family entitlement."""

    target: PinnedReadTarget
    entity: EntityLocator
    family_entitlement: str
    context_filters: tuple[ReadFilter, ...] | None = None
    grouped_entity_selection: dict[str, object] | None = None
    grouped_child_query: dict[str, object] | None = None

    def __post_init__(self) -> None:
        if (
            type(self.target) is not PinnedReadTarget
            or type(self.entity) is not EntityLocator
            or type(self.family_entitlement) is not str
            or self.family_entitlement != _FULL_FAMILY_ENTITLEMENT
        ):
            raise CustomImportReadRequestError("root detail request is malformed")
        _verify_grouped_child_shape(self.grouped_child_query, self.grouped_entity_selection)


@dataclass(frozen=True, slots=True)
class ReadFieldValue:
    """One projected field with a distinct materialized value/null/missing state."""

    field_id: str
    field_type: str
    state: str
    value: str | int | Decimal | bool | dt.date | dt.datetime | None


@dataclass(frozen=True, slots=True)
class ReadChild:
    """One child retained by the selected root family."""

    collection: str
    child_revision_id: int
    fields: tuple[ReadFieldValue, ...]


@dataclass(frozen=True, slots=True)
class SearchItem:
    """A winner-derived search row with root and selected-context projections."""

    winner: WinnerLocator
    root_fields: tuple[ReadFieldValue, ...]
    context_child_revision_id: int | None
    context_fields: tuple[ReadFieldValue, ...]
    children: tuple[ReadChild, ...] = ()


@dataclass(frozen=True, slots=True)
class SearchPage:
    """One exact-count page over a sealed winner selection."""

    target: PinnedReadTarget
    total: int
    items: tuple[SearchItem, ...]
    next_cursor: str | None
    expires_at: int
    query_fingerprint: str
    authorization_scope_sha256: str


@dataclass(frozen=True, slots=True)
class RootDetail:
    """All scalar-projected children from the winner's one immutable family."""

    target: PinnedReadTarget
    winner: WinnerLocator
    root_fields: tuple[ReadFieldValue, ...]
    children: tuple[ReadChild, ...]
    authorization_scope_sha256: str


@dataclass(frozen=True, slots=True)
class EntityFamilySet:
    """At most two independently projected families at one selected value."""

    target: PinnedReadTarget
    projection: str
    selection_field_id: str
    selection_value: int
    families: tuple[tuple[str, SearchItem | RootDetail], ...]
    missing_group_values: tuple[str, ...]
    query_fingerprint: str
    authorization_scope_sha256: str


@dataclass(frozen=True, slots=True)
class _NormalizedFilter:
    """A typed predicate with a canonical cursor/cache representation."""

    field: Field
    operator: str
    value: object | None
    canonical_value: object | None

    @property
    def descriptor(self) -> dict[str, object]:
        """Return the stable request representation without SQLAlchemy values."""

        return {"field": self.field.field_id, "operator": self.operator, "value": self.canonical_value}


@dataclass(frozen=True, slots=True)
class _SearchPlan:
    """The canonical query/order/page shape bound into a page cursor."""

    filters: tuple[_NormalizedFilter, ...]
    order_terms: tuple[ReadOrderTerm, ...]
    page_size: int
    fingerprint: str


@dataclass(frozen=True, slots=True)
class PreparedNpiEntityRelation:
    """One unpaged imported-NPI relation prepared for one host request.

    A host must compose this only in the same request's bounded SQL window.
    Its count and page statements must use the same pinned target, predicates,
    and bound parameters; this result has no cursor, cache, or source read.
    """

    statement: Select
    normalized_order_terms: tuple[ReadOrderTerm, ...]
    query_fingerprint: str
    authorization_scope_sha256: str
    effective_require_match: bool | None = None


@dataclass(frozen=True, slots=True)
class NpiEntityRelationQuery:
    """Separate selected-context and metric predicates for one provider relation."""

    context_filters: tuple[ReadFilter, ...] | None = None
    filters: tuple[ReadFilter, ...] = ()
    order_terms: tuple[ReadOrderTerm, ...] | None = None
    require_match: bool = True
    require_exact_context: bool = False
    grouped_entity_selection: dict[str, object] | None = None
    family_entitlement: str | None = None
    grouped_child_query: dict[str, object] | None = None
    entity_values: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.family_entitlement is not None and (
            type(self.family_entitlement) is not str
            or self.family_entitlement != _FULL_FAMILY_ENTITLEMENT
            or type(self.grouped_entity_selection) is not dict
        ):
            raise CustomImportReadRequestError("page family entitlement requires grouped entity selection")
        _verify_grouped_child_shape(self.grouped_child_query, self.grouped_entity_selection)


def _verify_grouped_child_shape(child, grouped):
    if child is not None and (type(child) is not dict or type(grouped) is not dict):
        raise CustomImportReadRequestError("grouped child queries require grouped entity selection")


def _relation_predicate_limit(query):
    """Bound input before loading; pinned capability verification follows."""

    if query.grouped_child_query is not None:
        return MAX_GROUPED_CHILD_PREDICATE_TERMS
    return MAX_GROUPED_PREDICATE_TERMS if query.grouped_entity_selection is not None else MAX_FILTER_TERMS


@dataclass(frozen=True, slots=True)
class _PageWindow:
    """The exact page window used to derive one bounded next cursor."""

    offset: int
    total: int
    returned_count: int
    issued_at: int
    expires_at: int


@dataclass(frozen=True, slots=True)
class _ReadContext:
    """Definition, profile, and P1 identities verified for one pinned read."""

    target: PinnedReadTarget
    definition: CustomImportDefinition
    profile_slot: int
    profile_context_slot: int
    collection_slots_by_name: Mapping[str, int]
    collection_names_by_slot: Mapping[int, str]
    default_profile_slot: int | None = None
    storage_models: Mapping | None = None

    def model(self, canonical_model):
        """Bind hot reads to this verified family without redirecting control rows."""

        return canonical_model if self.storage_models is None else self.storage_models[canonical_model]


class CustomImportReadService:
    """Read exact winner materialization results after a host policy decision."""

    def __init__(
        self,
        *,
        authorizer: ExtensionReadAuthorizer | None,
        cursor_secret: bytes | None = None,
        cache: CustomImportReadCache | None = None,
        cursor_ttl_seconds: int = 300,
        statement_timeout_ms: int = DEFAULT_READ_TIMEOUT_MS,
        now: Callable[[], int] | None = None,
    ) -> None:
        if type(cursor_ttl_seconds) is not int or not 1 <= cursor_ttl_seconds <= MAX_CURSOR_TTL_SECONDS:
            raise CustomImportReadRequestError("cursor_ttl_seconds is outside the read-core limit")
        if type(statement_timeout_ms) is not int or not 1 <= statement_timeout_ms <= MAX_READ_TIMEOUT_MS:
            raise CustomImportReadRequestError("statement_timeout_ms is outside the read-core limit")
        self._authorizer = authorizer
        self._cache = cache
        self._cursor_codec = None if cursor_secret is None else ReadCursorCodec(cursor_secret)
        self._cursor_ttl_seconds = cursor_ttl_seconds
        self._statement_timeout_ms = statement_timeout_ms
        self._now = time.time if now is None else now

    async def search(
        self,
        session: AsyncSession,
        *,
        authorization: ExtensionReadAuthorization,
        request: SearchRequest,
    ) -> SearchPage:
        """Search persisted winners after auth, eligibility, filtering, and exact count."""

        authorization_scope = self._authorize(authorization, request.target)
        if self._cursor_codec is None:
            raise CustomImportReadUnavailableError("search cursor is unavailable")
        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, request.target)
            plan = _normalize_search_plan(request, context)
            trusted_now = self._trusted_now()
            scope_digest = _scope_digest(authorization_scope)
            offset = self._cursor_offset(request, plan, scope_digest, trusted_now)
            cache_key = _cache_key("search", context.target, plan.fingerprint, scope_digest, offset)
            cached_page = await _cached_search_page(self._cache, cache_key, context, plan, scope_digest, trusted_now)
            if cached_page is not None:
                await verify_published_generation(session, context.target)
                return cached_page
            statement = _filtered_winner_statement(context, plan.filters)
            total = await _exact_count(session, statement)
            selected_rows = await _page_winner_rows(session, statement, context, plan, offset)
            page_items = await _hydrate_search_page_items(session, context, selected_rows)
            await verify_published_generation(session, context.target)
            expires_at = trusted_now + self._cursor_ttl_seconds
            next_cursor = self._next_search_cursor(
                context,
                plan,
                scope_digest,
                _PageWindow(
                    offset=offset,
                    total=total,
                    returned_count=len(page_items),
                    issued_at=trusted_now,
                    expires_at=expires_at,
                ),
            )
            page = SearchPage(
                target=context.target,
                total=total,
                items=page_items,
                next_cursor=next_cursor,
                expires_at=expires_at,
                query_fingerprint=plan.fingerprint,
                authorization_scope_sha256=scope_digest,
            )
        await _cache_result(self._cache, cache_key, page, expires_at)
        return page

    async def prepare_npi_entity_relation(
        self,
        session: AsyncSession,
        *,
        authorization: ExtensionReadAuthorization,
        target: PinnedReadTarget,
        query: NpiEntityRelationQuery = NpiEntityRelationQuery(),
    ) -> PreparedNpiEntityRelation:
        """Prepare one unpaged NPI relation; ``None`` order is filter-only."""

        authorization_scope = self._authorize(authorization, target)
        if type(query) is not NpiEntityRelationQuery:
            raise CustomImportReadRequestError("imported membership mode is invalid")
        return await self._prepare_npi_entity_relation(
            session,
            pinned_target=target,
            query=query,
            authorization_scope=authorization_scope,
        )

    async def _prepare_npi_entity_relation(
        self,
        session: AsyncSession,
        *,
        pinned_target: PinnedReadTarget,
        query: NpiEntityRelationQuery,
        authorization_scope: ExtensionReadScope,
    ) -> PreparedNpiEntityRelation:
        """Normalize one validated query and construct its unpaged relation."""

        _validate_npi_entity_relation_request(
            query.filters,
            query.order_terms,
            context_filters=query.context_filters,
            maximum_terms=_relation_predicate_limit(query),
        )
        if type(query.require_match) is not bool or type(query.require_exact_context) is not bool:
            raise CustomImportReadRequestError("imported membership mode is invalid")
        if query.entity_values is not None:
            _validate_npi_page(query.entity_values)
        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, pinned_target)
            if context.definition.query.entity_selection is not None or query.grouped_entity_selection is not None:
                from process.custom_import.grouped_read import prepare_relation

                return prepare_relation(context, query, authorization_scope)
            normalized_context_filters, normalized_filters, normalized_order_terms = _normalized_npi_query(
                context,
                query.context_filters,
                query.filters,
                query.order_terms,
                query.require_exact_context,
            )
            if not query.require_match:
                if query.context_filters is None:
                    _require_context_only_filters(normalized_filters, context)
                elif normalized_filters:
                    raise CustomImportReadRequestError("metric predicates require imported membership")
            query_sha256 = _npi_entity_relation_fingerprint(
                context.target,
                normalized_filters,
                normalized_order_terms,
                context_filters=normalized_context_filters,
                entity_values=query.entity_values,
            )
            return PreparedNpiEntityRelation(
                statement=_npi_entity_relation_statement(
                    context,
                    normalized_filters,
                    normalized_order_terms,
                    context_filters=normalized_context_filters,
                    entity_values=query.entity_values,
                ),
                normalized_order_terms=normalized_order_terms,
                query_fingerprint=query_sha256,
                authorization_scope_sha256=_scope_digest(authorization_scope),
            )

    async def hydrate_npi_page(
        self,
        session: AsyncSession,
        *,
        authorization: ExtensionReadAuthorization,
        pinned_target: PinnedReadTarget,
        prepared: PreparedNpiEntityRelation,
        entity_values: tuple[str, ...],
        query: NpiEntityRelationQuery = NpiEntityRelationQuery(),
        full_family: bool = False,
    ) -> dict[str, SearchItem | RootDetail | EntityFamilySet]:
        """Batch the same eligible winners with the default query projection.

        Exact detail batches opt into complete ordinary root and child
        projections with ``full_family``; grouped reads retain their entitlement.
        """

        authorization_scope = self._authorize(authorization, pinned_target)
        if type(query) is not NpiEntityRelationQuery or type(full_family) is not bool:
            raise CustomImportReadRequestError("provider relation query is invalid")
        _validate_npi_entity_relation_request(
            query.filters,
            query.order_terms,
            context_filters=query.context_filters,
            maximum_terms=_relation_predicate_limit(query),
        )
        _validate_npi_page(entity_values)
        if query.entity_values is not None:
            _validate_npi_page(query.entity_values)
            if not set(entity_values) <= set(query.entity_values):
                raise CustomImportReadUnavailableError("provider page identities exceed the native query scope")
        if full_family and len(entity_values) > MAX_FULL_FAMILY_PAGE_SIZE:
            raise CustomImportReadRequestError("full-family provider page exceeds its bound")
        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, pinned_target)
            if context.definition.query.entity_selection is not None or query.grouped_entity_selection is not None:
                from process.custom_import.grouped_read import hydrate_page

                return await hydrate_page(session, context, query, prepared, entity_values, authorization_scope)
            return await self._hydrate_single_family_page(
                session, context, query, prepared, entity_values, authorization_scope, full_family=full_family
            )

    async def _hydrate_single_family_page(
        self, session, context, query, prepared, entity_values, authorization_scope, *, full_family=False
    ):
        """Retain the legacy single-family page selection and fingerprint."""

        entity_model = context.model(CustomImportEntityBinding)

        normalized_context_filters, normalized_filters, normalized_order = _normalized_npi_query(
            context, query.context_filters, query.filters, query.order_terms
        )
        if (
            type(prepared) is not PreparedNpiEntityRelation
            or prepared.query_fingerprint
            != _npi_entity_relation_fingerprint(
                context.target,
                normalized_filters,
                normalized_order,
                context_filters=normalized_context_filters,
                entity_values=query.entity_values,
            )
            or prepared.authorization_scope_sha256 != _scope_digest(authorization_scope)
        ):
            raise CustomImportReadUnavailableError("provider page query identity is unavailable")
        statement = (
            _filtered_npi_winner_statement(context, normalized_filters, context_filters=normalized_context_filters)
            .add_columns(entity_model.canonical_value)
            .where(entity_model.canonical_value.in_(entity_values))
        )
        if full_family and entity_values:
            await _require_unambiguous_npi_families(session, context, statement)
        statement = statement.distinct(entity_model.canonical_value).order_by(
            entity_model.canonical_value, *_winner_order_terms(context, ())
        )
        selected_rows = (await session.execute(statement)).all() if entity_values else ()
        winners = tuple(
            _winner_row(selected_row[:-1], context.profile_context_slot > 0) for selected_row in selected_rows
        )
        if full_family:
            hydrated_items = await _hydrate_complete_family_entities(
                session,
                context,
                winners,
                tuple(selected_row[-1] for selected_row in selected_rows),
                _scope_digest(authorization_scope),
                is_page=True,
            )
        else:
            hydrated_items = await _hydrate_provider_page_items(
                session, context, winners, _scope_digest(authorization_scope)
            )
        await verify_published_generation(session, context.target)
        return {
            selected_row[-1]: hydrated_item
            for selected_row, hydrated_item in zip(selected_rows, hydrated_items, strict=True)
        }

    async def root_detail(
        self,
        session: AsyncSession,
        *,
        authorization: ExtensionReadAuthorization,
        target: PinnedReadTarget,
        winner: WinnerLocator,
    ) -> RootDetail:
        """Hydrate all children in the exact family selected by one persisted winner."""

        authorization_scope = self._authorize(authorization, target)
        return await self._root_detail(
            session,
            target=target,
            winner=winner,
            authorization_scope=authorization_scope,
        )

    async def root_detail_for_entity(
        self,
        session: AsyncSession,
        *,
        authorization: ExtensionReadAuthorization,
        request: RootDetailRequest,
    ) -> RootDetail | EntityFamilySet:
        """Hydrate one unambiguous entity family through the normal detail path."""

        if type(request) is not RootDetailRequest:
            raise CustomImportReadRequestError("root detail request is malformed")
        authorization_scope = self._authorize(authorization, request.target)
        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, request.target)
            if context.definition.query.entity_selection is not None or request.grouped_entity_selection is not None:
                from process.custom_import.grouped_read import hydrate_detail

                return await hydrate_detail(session, context, request, authorization_scope)
            if request.context_filters is not None:
                raise CustomImportReadRequestError("detail context requires grouped entity selection")
            winner = await _entity_winner_locator(session, context, request.entity)
            trusted_now = self._trusted_now()
            detail, cache_key = await self._root_detail_from_context(session, context, winner, authorization_scope)
        if cache_key is not None:
            await _cache_result(self._cache, cache_key, detail, trusted_now + self._cursor_ttl_seconds)
        return detail

    async def _root_detail(
        self,
        session: AsyncSession,
        *,
        target: PinnedReadTarget,
        winner: WinnerLocator,
        authorization_scope: ExtensionReadScope,
    ) -> RootDetail:
        """Run one authorized detail hydration under one bounded read window."""

        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, target)
            trusted_now = self._trusted_now()
            detail, cache_key = await self._root_detail_from_context(session, context, winner, authorization_scope)
        if cache_key is not None:
            await _cache_result(self._cache, cache_key, detail, trusted_now + self._cursor_ttl_seconds)
        return detail

    async def _root_detail_from_context(
        self,
        session: AsyncSession,
        context: _ReadContext,
        winner: WinnerLocator,
        authorization_scope: ExtensionReadScope,
    ) -> tuple[RootDetail, str | None]:
        """Reuse exact winner hydration after a target context is verified."""

        if context.definition.query.entity_selection is not None:
            raise CustomImportReadRequestError("grouped entity detail requires an entity selector")
        scope_digest = _scope_digest(authorization_scope)
        cache_key = _detail_cache_key(context.target, winner, scope_digest)
        cached_detail = await _cached_root_detail(self._cache, cache_key, context, winner, scope_digest)
        if cached_detail is not None:
            await verify_published_generation(session, context.target)
            return cached_detail, None
        selected_row = await _selected_winner_row(session, context, winner)
        detail = await _hydrate_root_detail(session, context, selected_row, scope_digest)
        await verify_published_generation(session, context.target)
        return detail, cache_key

    def _authorize(
        self,
        authorization: ExtensionReadAuthorization,
        target: PinnedReadTarget,
    ) -> ExtensionReadScope:
        """Check the distinct extension authorization before cache or result work."""

        if type(authorization) is not ExtensionReadAuthorization or self._authorizer is None:
            raise CustomImportReadAuthorizationError("extension read is not authorized")
        try:
            scope = self._authorizer.authorize(authorization, target=target)
        except Exception:
            raise CustomImportReadAuthorizationError("extension read is not authorized") from None
        if type(scope) is not ExtensionReadScope:
            raise CustomImportReadAuthorizationError("extension read is not authorized")
        return scope

    def _trusted_now(self) -> int:
        """Read one bounded integer clock value suitable for cursor expiry checks."""

        try:
            current_time = self._now()
        except Exception:
            raise CustomImportReadUnavailableError("trusted read clock is unavailable") from None
        if type(current_time) is float:
            try:
                current_time = int(current_time)
            except OverflowError, ValueError:
                raise CustomImportReadUnavailableError("trusted read clock is unavailable") from None
        if type(current_time) is not int or not 0 <= current_time < 2**63 - MAX_CURSOR_TTL_SECONDS:
            raise CustomImportReadUnavailableError("trusted read clock is unavailable")
        return current_time

    def _cursor_offset(
        self,
        request: SearchRequest,
        plan: _SearchPlan,
        scope_digest: str,
        trusted_now: int,
    ) -> int:
        """Return zero or an authenticated cursor offset for the exact request."""

        if request.cursor is None:
            return 0
        if self._cursor_codec is None:
            raise CustomImportReadUnavailableError("search cursor is unavailable")
        state = self._cursor_codec.open(
            request.cursor,
            pinned_target=request.target,
            query_fingerprint=plan.fingerprint,
            authorization_scope_sha256=scope_digest,
            trusted_now=trusted_now,
        )
        return state.offset

    def _next_search_cursor(
        self,
        context: _ReadContext,
        plan: _SearchPlan,
        scope_digest: str,
        page_window: _PageWindow,
    ) -> str | None:
        """Issue the next offset only when the exact counted result has another page."""

        next_offset = page_window.offset + page_window.returned_count
        if page_window.returned_count == 0 or next_offset >= page_window.total or next_offset > MAX_PAGE_OFFSET:
            return None
        if self._cursor_codec is None:
            raise CustomImportReadUnavailableError("search cursor is unavailable")
        return self._cursor_codec.issue(
            ReadCursorState(
                target=context.target,
                query_fingerprint=plan.fingerprint,
                authorization_scope_sha256=scope_digest,
                offset=next_offset,
                issued_at=page_window.issued_at,
                expires_at=page_window.expires_at,
            )
        )


def _scope_digest(scope: ExtensionReadScope) -> str:
    return hashlib.sha256(scope.value.encode("ascii")).hexdigest()


def _cache_key(kind: str, target: PinnedReadTarget, fingerprint: str, scope_digest: str, offset: int) -> str:
    cache_key_map = {
        "kind": kind,
        "target": _target_document(target),
        "fingerprint": fingerprint,
        "scope": scope_digest,
        "offset": offset,
    }
    return f"custom-import-read:{hashlib.sha256(_canonical_bytes(cache_key_map)).hexdigest()}"


def _detail_cache_key(target: PinnedReadTarget, winner: WinnerLocator, scope_digest: str) -> str:
    detail_cache_key_map = {
        "kind": "detail",
        "target": _target_document(target),
        "scope": scope_digest,
        "winner": {
            "binding": winner.entity_binding_id,
            "context": winner.context_key_sha256.hex(),
            "family": winner.family_revision_id,
            "root": winner.root_record_id,
        },
    }
    return f"custom-import-read:{hashlib.sha256(_canonical_bytes(detail_cache_key_map)).hexdigest()}"


def _target_document(target: PinnedReadTarget) -> dict[str, object]:
    return {
        "dataset": target.dataset_id,
        "definition": target.definition_revision_id,
        "generation": target.generation_id,
        "profile": target.profile_id,
        "schema": target.schema_revision_id,
    }


async def _cached_search_page(
    cache: CustomImportReadCache | None,
    cache_key: str,
    context: _ReadContext,
    plan: _SearchPlan,
    scope_digest: str,
    trusted_now: int,
) -> SearchPage | None:
    cached = await _cache_get(cache, cache_key)
    if type(cached) is not SearchPage:
        return None
    if (
        cached.target != context.target
        or cached.query_fingerprint != plan.fingerprint
        or cached.authorization_scope_sha256 != scope_digest
        or cached.expires_at <= trusted_now
    ):
        return None
    return cached


async def _cached_root_detail(
    cache: CustomImportReadCache | None,
    cache_key: str,
    context: _ReadContext,
    winner: WinnerLocator,
    scope_digest: str,
) -> RootDetail | None:
    cached = await _cache_get(cache, cache_key)
    if type(cached) is not RootDetail:
        return None
    if cached.target != context.target or cached.winner != winner or cached.authorization_scope_sha256 != scope_digest:
        return None
    return cached


async def _cache_get(cache: CustomImportReadCache | None, cache_key: str) -> object | None:
    if cache is None:
        return None
    try:
        return await cache.get(cache_key)
    except Exception:
        return None


async def _cache_result(cache: CustomImportReadCache | None, cache_key: str, value: object, expires_at: int) -> None:
    if cache is None:
        return
    try:
        await cache.set(cache_key, value, expires_at=expires_at)
    except Exception:
        return


@asynccontextmanager
async def _bounded_read_window(session: AsyncSession, *, timeout_ms: int) -> AsyncIterator[None]:
    """Apply one cumulative, caller-transaction-local database read budget."""

    monotonic_deadline = time.monotonic() + timeout_ms / 1_000
    try:
        async with asyncio.timeout(timeout_ms / 1_000):
            async with _local_statement_timeout(session, timeout_ms=timeout_ms):
                yield
                if time.monotonic() >= monotonic_deadline:
                    raise TimeoutError
    except TimeoutError:
        raise CustomImportReadUnavailableError("bounded read is unavailable") from None
    except DBAPIError as error:
        if _is_statement_timeout(error):
            raise CustomImportReadUnavailableError("bounded read is unavailable") from None
        raise


@asynccontextmanager
async def _local_statement_timeout(session: AsyncSession, *, timeout_ms: int) -> AsyncIterator[None]:
    """Temporarily narrow the caller's transaction-local statement timeout."""

    previous_timeout_text, previous_timeout_ms = await _current_statement_timeout(session)
    effective_timeout_ms = timeout_ms if previous_timeout_ms == 0 else min(timeout_ms, previous_timeout_ms)
    await session.execute(select(func.set_config("statement_timeout", str(effective_timeout_ms), True)))
    has_read_failed = False
    try:
        yield
    except BaseException:
        has_read_failed = True
        raise
    finally:
        await _restore_statement_timeout(session, previous_timeout_text, has_read_failed=has_read_failed)


async def _current_statement_timeout(session: AsyncSession) -> tuple[str, int]:
    timeout_row = (await session.execute(_READ_TIMEOUT_SETTINGS)).one_or_none()
    if timeout_row is None or not isinstance(timeout_row.timeout_text, str):
        raise CustomImportReadUnavailableError("bounded read is unavailable")
    try:
        previous_timeout_ms = int(timeout_row.timeout_milliseconds)
    except TypeError, ValueError:
        raise CustomImportReadUnavailableError("bounded read is unavailable") from None
    if previous_timeout_ms < 0:
        raise CustomImportReadUnavailableError("bounded read is unavailable")
    return timeout_row.timeout_text, previous_timeout_ms


async def _restore_statement_timeout(
    session: AsyncSession,
    previous_timeout_text: str,
    *,
    has_read_failed: bool,
) -> None:
    try:
        await session.execute(select(func.set_config("statement_timeout", previous_timeout_text, True)))
    except Exception:
        if not has_read_failed:
            raise


def _is_statement_timeout(error: DBAPIError) -> bool:
    """Recognize PostgreSQL query cancellation without inspecting messages."""

    driver_error = error.orig
    return any(getattr(driver_error, attribute, None) == "57014" for attribute in ("sqlstate", "pgcode"))


async def _load_read_context(session: AsyncSession, target: PinnedReadTarget) -> _ReadContext:
    definition_row, schema_row = await _eligible_definition_rows(session, target)
    definition = verified_definition(definition_row, schema_row)
    collection_slots = await _collection_slots(session, target, definition)
    profile = await _exact_profile(session, target)
    profile_slot, profile_context_slot = _verified_profile(profile, definition, collection_slots)
    await _verified_field_rows(session, target, definition, collection_slots)
    snapshot_family_id = await resolve_generation_snapshot(session, target)
    context = _ReadContext(
        target=target,
        definition=definition,
        profile_slot=profile_slot,
        profile_context_slot=profile_context_slot,
        collection_slots_by_name=collection_slots,
        collection_names_by_slot={slot: name for name, slot in collection_slots.items()},
        storage_models=None if snapshot_family_id is None else snapshot_models(snapshot_family_id),
    )
    if definition.query.entity_selection is not None:
        from process.custom_import.grouped_read import bind_default_profile

        return await bind_default_profile(session, context)
    return context


async def _eligible_definition_rows(
    session: AsyncSession,
    pinned_target: PinnedReadTarget,
) -> tuple[CustomImportDefinitionRevision, CustomImportSchemaRevision]:
    await verify_published_generation(session, pinned_target)
    statement = (
        select(
            CustomImportDefinitionRevision,
            CustomImportSchemaRevision,
        )
        .select_from(CustomImportDefinitionRevision)
        .join(
            CustomImportSchemaRevision,
            and_(
                CustomImportSchemaRevision.schema_revision_id == CustomImportDefinitionRevision.schema_revision_id,
                CustomImportSchemaRevision.dataset_id == CustomImportDefinitionRevision.dataset_id,
            ),
        )
        .join(
            CustomImportGeneration,
            and_(
                CustomImportGeneration.generation_id == pinned_target.generation_id,
                CustomImportGeneration.dataset_id == CustomImportDefinitionRevision.dataset_id,
                CustomImportGeneration.definition_revision_id == CustomImportDefinitionRevision.definition_revision_id,
                CustomImportGeneration.schema_revision_id == CustomImportDefinitionRevision.schema_revision_id,
            ),
        )
        .join(
            CustomImportGenerationSeal,
            and_(
                CustomImportGenerationSeal.generation_id == CustomImportGeneration.generation_id,
                CustomImportGenerationSeal.dataset_id == CustomImportGeneration.dataset_id,
                CustomImportGenerationSeal.definition_revision_id == CustomImportGeneration.definition_revision_id,
                CustomImportGenerationSeal.schema_revision_id == CustomImportGeneration.schema_revision_id,
            ),
        )
        .where(
            CustomImportDefinitionRevision.dataset_id == pinned_target.dataset_id,
            CustomImportDefinitionRevision.definition_revision_id == pinned_target.definition_revision_id,
            CustomImportDefinitionRevision.schema_revision_id == pinned_target.schema_revision_id,
            CustomImportGenerationSeal.seal_contract == "custom-import-generation-seal/v1",
        )
    )
    definition_identity_row = (await session.execute(statement)).one_or_none()
    if definition_identity_row is None:
        raise CustomImportReadUnavailableError("pinned generation is not eligible for extension reads")
    return definition_identity_row


async def _collection_slots(
    session: AsyncSession,
    target: PinnedReadTarget,
    definition: CustomImportDefinition,
) -> dict[str, int]:
    expected_names = {collection.name for collection in definition.child_collections}
    rows = (
        (
            await session.execute(
                select(CustomImportChildCollection).where(
                    CustomImportChildCollection.dataset_id == target.dataset_id,
                    CustomImportChildCollection.schema_revision_id == target.schema_revision_id,
                )
            )
        )
        .scalars()
        .all()
    )
    slots_by_name = {row.collection_name: row.collection_slot for row in rows}
    if set(slots_by_name) != expected_names or len(set(slots_by_name.values())) != len(slots_by_name):
        raise CustomImportReadUnavailableError("persisted child collections do not match the definition")
    return slots_by_name


async def _exact_profile(session: AsyncSession, target: PinnedReadTarget) -> CustomImportSelectionProfile:
    profile = (
        await session.execute(
            select(CustomImportSelectionProfile).where(
                CustomImportSelectionProfile.dataset_id == target.dataset_id,
                CustomImportSelectionProfile.definition_revision_id == target.definition_revision_id,
                CustomImportSelectionProfile.schema_revision_id == target.schema_revision_id,
                CustomImportSelectionProfile.profile_id == target.profile_id,
            )
        )
    ).scalar_one_or_none()
    if profile is None:
        raise CustomImportReadUnavailableError("pinned selection profile does not exist")
    return profile


def _verified_profile(
    profile: CustomImportSelectionProfile,
    definition: CustomImportDefinition,
    collection_slots_by_name: Mapping[str, int],
) -> tuple[int, int]:
    declared_profile = next(
        (item for item in definition.selection_profiles if item.profile_id == profile.profile_id), None
    )
    if declared_profile is None:
        raise CustomImportReadUnavailableError("selection profile is not declared by the definition")
    expected_slot, expected_collection = _profile_scope_slot(declared_profile, definition, collection_slots_by_name)
    persisted_slot = profile.context_collection_slot or 0
    expected_document = _profile_document(declared_profile, expected_collection)
    try:
        parsed_document = load_json_definition(profile.canonical_profile)
        expected_digest = bytes.fromhex(canonical_sha256(parsed_document, domain="profile"))
    except ValueError, TypeError:
        raise CustomImportReadUnavailableError("persisted selection profile is invalid") from None
    if (
        profile.profile_slot <= 0
        or persisted_slot != expected_slot
        or canonical_json(parsed_document) != profile.canonical_profile
        or canonical_json(expected_document) != profile.canonical_profile
        or not hmac.compare_digest(bytes(profile.profile_sha256), expected_digest)
    ):
        raise CustomImportReadUnavailableError("persisted selection profile does not match the definition")
    return profile.profile_slot, expected_slot


def _profile_scope_slot(
    profile,
    definition: CustomImportDefinition,
    collection_slots_by_name: Mapping[str, int],
) -> tuple[int, str | None]:
    child_field_ids = set(definition.query.child_fields)
    referenced_field_ids = {term.field_id for term in profile.selection_terms} | set(profile.context_dimensions)
    if not referenced_field_ids & child_field_ids:
        return 0, None
    collection = definition.query.child_collection
    if collection is None or collection not in collection_slots_by_name:
        raise CustomImportReadUnavailableError("child-scoped profile has no declared child collection")
    return collection_slots_by_name[collection], collection


def _profile_document(profile, collection: str | None) -> dict[str, object]:
    scope_map: dict[str, str] = {"kind": "root"}
    if collection is not None:
        scope_map = {"kind": "child", "collection": collection}
    return {
        "context_dimensions": list(profile.context_dimensions),
        "id": profile.profile_id,
        "selection": [
            {"field": term.field_id, "direction": term.direction, "nulls": term.nulls}
            for term in profile.selection_terms
        ],
        "scope": scope_map,
    }


async def _verified_field_rows(
    session: AsyncSession,
    pinned_target: PinnedReadTarget,
    definition: CustomImportDefinition,
    collection_slots_by_name: Mapping[str, int],
) -> None:
    persisted_fields = (
        (
            await session.execute(
                select(CustomImportField).where(
                    CustomImportField.dataset_id == pinned_target.dataset_id,
                    CustomImportField.schema_revision_id == pinned_target.schema_revision_id,
                )
            )
        )
        .scalars()
        .all()
    )
    persisted_fields_by_name = {persisted_field.field_name: persisted_field for persisted_field in persisted_fields}
    if len(persisted_fields_by_name) != len(persisted_fields) or set(persisted_fields_by_name) != set(
        definition.fields_by_id
    ):
        raise CustomImportReadUnavailableError("persisted field bindings do not match the definition")
    for field in definition.fields:
        persisted_field = persisted_fields_by_name[field.field_id]
        expected_collection_slot = 0 if field.collection is None else collection_slots_by_name[field.collection]
        expected_projection_slot = 0 if field.projection_slot is None else field.projection_slot
        if (
            persisted_field.field_slot != field.field_slot
            or persisted_field.collection_slot != expected_collection_slot
            or persisted_field.field_type != field.value_type
            or persisted_field.is_nullable != field.nullable
            or persisted_field.projection_slot != expected_projection_slot
        ):
            raise CustomImportReadUnavailableError("persisted field binding is invalid")


def _normalize_search_plan(request: SearchRequest, context: _ReadContext) -> _SearchPlan:
    if context.definition.query.entity_selection is not None:
        raise CustomImportReadRequestError("grouped entity selection is not supported by generic search")
    if type(request) is not SearchRequest or request.target != context.target:
        raise CustomImportReadRequestError("search target does not match the verified read context")
    normalized_filters = _normalized_filters(request.filters, context)
    _verify_filter_permissions(normalized_filters, context, allow_context_selectors=True)
    declared_order_terms = tuple(
        ReadOrderTerm(field_id=term.field_id, direction=term.direction, nulls=term.nulls)
        for term in context.definition.query.order_terms
    )
    requested_order_terms = _normalize_query_order_terms(
        declared_order_terms if request.order_terms is None else request.order_terms,
        context,
        explicit=request.order_terms is not None,
    )

    search_shape_map = {
        "filters": [normalized_filter.descriptor for normalized_filter in normalized_filters],
        "order": [
            {"field": term.field_id, "direction": term.direction, "nulls": term.nulls} for term in requested_order_terms
        ],
        "page_size": request.page_size,
    }
    return _SearchPlan(
        filters=normalized_filters,
        order_terms=requested_order_terms,
        page_size=request.page_size,
        fingerprint=hashlib.sha256(_canonical_bytes(search_shape_map)).hexdigest(),
    )


def _normalize_query_order_terms(
    raw_order_terms: tuple[ReadOrderTerm, ...],
    context: _ReadContext,
    *,
    explicit: bool,
    query_child_collection: str | None = None,
    maximum_terms: int = MAX_ORDER_TERMS,
    allow_derived: bool = False,
) -> tuple[ReadOrderTerm, ...]:
    """Normalize and validate an explicit order against the query contract."""

    if type(raw_order_terms) is not tuple:
        raise CustomImportReadRequestError("order_terms must be a tuple")
    declared_order_terms = tuple(
        ReadOrderTerm(field_id=term.field_id, direction=term.direction, nulls=term.nulls)
        for term in context.definition.query.order_terms
    )
    requested_order_terms = _normalized_order_terms(raw_order_terms, context)
    if len(declared_order_terms) > maximum_terms or len(requested_order_terms) > maximum_terms:
        raise CustomImportReadRequestError("order term count exceeds the read-core limit")
    if explicit and context.definition.query.sortable_fields:
        sortable_field_ids = set(context.definition.query.sortable_fields)
        requested_field_ids = [term.field_id for term in requested_order_terms]
        if (
            not requested_order_terms
            or len(requested_field_ids) != len(set(requested_field_ids))
            or any(term.field_id not in sortable_field_ids or term.nulls != "last" for term in requested_order_terms)
        ):
            raise CustomImportReadRequestError("order terms are not permitted by the query contract")
    elif requested_order_terms != declared_order_terms:
        raise CustomImportReadRequestError("order terms must exactly match the bounded definition order")
    _verify_order_context(
        requested_order_terms, context, query_child_collection=query_child_collection, allow_derived=allow_derived
    )
    return requested_order_terms


def _validate_npi_entity_relation_request(
    filters: tuple[ReadFilter, ...],
    order_terms: tuple[ReadOrderTerm, ...] | None,
    *,
    context_filters: tuple[ReadFilter, ...] | None = None,
    maximum_terms: int = MAX_FILTER_TERMS,
) -> None:
    """Reject unbounded relation shapes before context loading."""

    if type(filters) is not tuple:
        raise CustomImportReadRequestError("filters must be a tuple")
    metric_limit = MAX_PROVIDER_FILTER_TERMS if maximum_terms > MAX_FILTER_TERMS else MAX_FILTER_TERMS
    order_limit = MAX_PROVIDER_ORDER_TERMS if maximum_terms > MAX_FILTER_TERMS else MAX_ORDER_TERMS
    if len(filters) > metric_limit:
        raise CustomImportReadRequestError("filter count exceeds the read-core limit")
    if context_filters is not None and (type(context_filters) is not tuple or len(context_filters) > MAX_FILTER_TERMS):
        raise CustomImportReadRequestError("context filter count exceeds the read-core limit")
    if context_filters is not None and len(filters) + len(context_filters) > maximum_terms:
        raise CustomImportReadRequestError("filter count exceeds the read-core limit")
    if order_terms is not None and type(order_terms) is not tuple:
        raise CustomImportReadRequestError("order_terms must be a tuple")
    if order_terms is not None and len(order_terms) > order_limit:
        raise CustomImportReadRequestError("order term count exceeds the read-core limit")


def _validate_npi_page(entity_values: tuple[str, ...]) -> None:
    """Accept only bounded unique canonical NPI values from a native page."""

    if (
        type(entity_values) is not tuple
        or len(entity_values) > MAX_NPI_PAGE_SIZE
        or any(
            type(value) is not str or len(value) != 10 or not value.isascii() or not value.isdigit()
            for value in entity_values
        )
        or len(set(entity_values)) != len(entity_values)
    ):
        raise CustomImportReadRequestError("provider page identities are invalid")


def _normalized_npi_query(context, context_filters, filters, order_terms, require_exact_context=False):
    """Normalize legacy combined filters or v2 context and metric roles."""

    normalized_filters = _normalized_filters(filters, context)
    normalized_order = () if order_terms is None else _normalize_query_order_terms(order_terms, context, explicit=True)
    if context_filters is None:
        _verify_filter_permissions(normalized_filters, context, allow_context_selectors=True)
        _require_order_context_filters(
            normalized_order, normalized_filters, context, require_exact_context=require_exact_context
        )
        return (), normalized_filters, normalized_order
    normalized_context_filters = _normalized_filters(context_filters, context)
    context_dimensions = _verify_context_filters(normalized_context_filters, context)
    _verify_metric_filters(normalized_filters, context_dimensions)
    _verify_filter_permissions(normalized_filters, context)
    _require_order_context_filters(
        normalized_order, normalized_context_filters, context, require_exact_context=require_exact_context
    )
    return normalized_context_filters, normalized_filters, normalized_order


def _normalized_filters(
    raw_filters: tuple[ReadFilter, ...],
    context: _ReadContext,
    *,
    query_child_collection: str | None = None,
    maximum_terms: int = MAX_FILTER_TERMS,
    allow_derived: bool = False,
) -> tuple[_NormalizedFilter, ...]:
    if len(raw_filters) > maximum_terms:
        raise CustomImportReadRequestError("filter count exceeds the read-core limit")
    normalized_filters: list[_NormalizedFilter] = []
    for raw_filter in raw_filters:
        if type(raw_filter) is not ReadFilter:
            raise CustomImportReadRequestError("filter field is not declared by the query contract")
        field_id = context.definition.query.resolve_field_id(raw_filter.field_id)
        if field_id is None:
            raise CustomImportReadRequestError("filter field is not declared by the query contract")
        field = context.definition.fields_by_id.get(field_id)
        if field is None and allow_derived:
            field = context.definition.query.derived_by_id.get(field_id)
        if field is None:
            raise CustomImportReadUnavailableError("declared query field has no persisted binding")
        _verify_field_context(field, context, query_child_collection=query_child_collection)
        typed_filter_value, canonical_value = _normalized_filter_value(field, raw_filter.operator, raw_filter.value)
        normalized_filters.append(
            _NormalizedFilter(
                field=field,
                operator=raw_filter.operator,
                value=typed_filter_value,
                canonical_value=canonical_value,
            )
        )
    filter_descriptors = [normalized_filter.descriptor for normalized_filter in normalized_filters]
    descriptor_texts = [_canonical_bytes(descriptor).decode("ascii") for descriptor in filter_descriptors]
    if len(set(descriptor_texts)) != len(descriptor_texts):
        raise CustomImportReadRequestError("filters cannot repeat the same predicate")
    return tuple(
        normalized_filter
        for _, normalized_filter in sorted(
            zip(descriptor_texts, normalized_filters, strict=True), key=lambda descriptor_pair: descriptor_pair[0]
        )
    )


def _verify_filter_permissions(filters, context, *, allow_context_selectors=False):
    """Enforce canonical metric grants while preserving exact context selectors."""

    permitted_fields = context.definition.query.filterable_fields
    if permitted_fields is None:
        return
    context_dimensions = ()
    if allow_context_selectors:
        profile = next(
            (item for item in context.definition.selection_profiles if item.profile_id == context.target.profile_id),
            None,
        )
        if profile is None:
            raise CustomImportReadUnavailableError("selection profile is not declared by the definition")
        context_dimensions = profile.context_dimensions
    for predicate in filters:
        if predicate.field.field_id in permitted_fields:
            continue
        if (
            predicate.field.field_id in context_dimensions
            and predicate.operator == "eq"
            and predicate.value is not None
            and (predicate.field.value_type != "integer" or type(predicate.value) is int)
        ):
            continue
        raise CustomImportReadRequestError("filter field is not opted in by the query contract")


def _verify_context_filters(filters: tuple[_NormalizedFilter, ...], context: _ReadContext) -> tuple[str, ...]:
    """Accept only one non-null equality selector for each declared context dimension."""

    profile = next(
        (
            profile_candidate
            for profile_candidate in context.definition.selection_profiles
            if profile_candidate.profile_id == context.target.profile_id
        ),
        None,
    )
    if profile is None:
        raise CustomImportReadUnavailableError("selection profile is not declared by the definition")
    if len({predicate.field.field_id for predicate in filters}) != len(filters) or any(
        predicate.field.field_id not in profile.context_dimensions
        or predicate.operator != "eq"
        or predicate.value is None
        or (predicate.field.value_type == "integer" and type(predicate.value) is not int)
        for predicate in filters
    ):
        raise CustomImportReadRequestError("context selectors are invalid")
    return profile.context_dimensions


def _verify_metric_filters(filters: tuple[_NormalizedFilter, ...], context_dimensions: tuple[str, ...]) -> None:
    """Limit v2 metric predicates to the metric comparison operators."""

    if any(
        predicate.operator not in {"eq", "gt", "gte", "lt", "lte"} or predicate.field.field_id in context_dimensions
        for predicate in filters
    ):
        raise CustomImportReadRequestError("metric filters are invalid")


def _normalized_order_terms(
    raw_order_terms: tuple[ReadOrderTerm, ...],
    context: _ReadContext,
) -> tuple[ReadOrderTerm, ...]:
    """Resolve query aliases to canonical order field identifiers."""

    normalized_terms: list[ReadOrderTerm] = []
    for raw_term in raw_order_terms:
        if type(raw_term) is not ReadOrderTerm:
            raise CustomImportReadRequestError("order field is not declared by the query contract")
        field_id = context.definition.query.resolve_field_id(raw_term.field_id)
        if field_id is None:
            raise CustomImportReadRequestError("order field is not declared by the query contract")
        normalized_terms.append(ReadOrderTerm(field_id, raw_term.direction, raw_term.nulls))
    return tuple(normalized_terms)


def _verify_order_context(
    order_terms: tuple[ReadOrderTerm, ...],
    context: _ReadContext,
    *,
    query_child_collection: str | None = None,
    allow_derived: bool = False,
) -> None:
    permitted_ids = set(context.definition.query.root_fields) | set(context.definition.query.child_fields)
    if allow_derived:
        permitted_ids.update(context.definition.query.derived_by_id)
    for term in order_terms:
        if type(term) is not ReadOrderTerm or term.field_id not in permitted_ids:
            raise CustomImportReadRequestError("order field is not declared by the query contract")
        field = context.definition.fields_by_id.get(term.field_id)
        if field is None and allow_derived:
            field = context.definition.query.derived_by_id.get(term.field_id)
        if field is None:
            raise CustomImportReadUnavailableError("declared order field has no persisted binding")
        _verify_field_context(field, context, query_child_collection=query_child_collection)


def _verify_field_context(field: Field, context: _ReadContext, *, query_child_collection: str | None = None) -> None:
    if field.collection is None:
        return
    expected_slot = context.collection_slots_by_name.get(field.collection)
    if (
        expected_slot is not None
        and field.collection == query_child_collection == context.definition.query.child_collection
    ):
        return
    if expected_slot is None or context.profile_context_slot != expected_slot:
        raise CustomImportReadRequestError("selected profile has no permitted context for this child field")


def _normalized_filter_value(
    field: Field, operator: str, raw_value: object | None
) -> tuple[object | None, object | None]:
    if operator in {"is_null", "is_missing"}:
        if raw_value is not None:
            raise CustomImportReadRequestError("state predicates cannot carry a comparison value")
        return None, None
    if operator in {"gt", "gte", "lt", "lte"} and field.value_type not in _RANGE_FIELD_TYPES:
        raise CustomImportReadRequestError("range predicates require a metric-compatible field")
    if raw_value is None:
        raise CustomImportReadRequestError("comparison predicates require a typed value")
    if field.value_type in {"integer", "decimal"} and type(raw_value) is dict:
        raw_value = _decoded_decimal_filter(raw_value, field.field_id)
    if field.value_type == "string":
        return _normalized_string(raw_value, field.field_id)
    if field.value_type == "integer":
        return _normalized_integer_filter(raw_value, field.field_id)
    if field.value_type == "decimal":
        return _normalized_decimal(
            raw_value, field.field_id, maximum_integer_digits=19 if isinstance(field, DerivedQueryField) else 18
        )
    if field.value_type == "boolean":
        return _normalized_boolean(raw_value, field.field_id)
    if field.value_type == "date":
        return _normalized_date(raw_value, field.field_id)
    if field.value_type == "timestamp":
        return _normalized_timestamp(raw_value, field.field_id)
    raise CustomImportReadUnavailableError("declared query field has an unsupported scalar type")


def _normalized_string(value: object, field_id: str) -> tuple[str, str]:
    if type(value) is not str or "\x00" in value:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not an indexed string")
    try:
        encoded_length = len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not an indexed string") from None
    if encoded_length > 2_048:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not an indexed string")
    return value, value


def _normalized_integer(value: object, field_id: str) -> tuple[int, int]:
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a BIGINT")
    return value, value


def _decoded_decimal_filter(value: dict, field_id: str) -> Decimal:
    """Decode the signed transport's exact JSON-number envelope without floats."""

    number = value.get("decimal")
    if (
        set(value) != {"decimal"}
        or type(number) is not str
        or len(number) > 2_048
        or _DECIMAL_FILTER_NUMBER.fullmatch(number) is None
    ):
        raise CustomImportReadRequestError(f"filter value for {field_id} is not an exact decimal number")
    try:
        return Decimal(number)
    except InvalidOperation, ValueError:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not an exact decimal number") from None


def _normalized_integer_filter(value: object, field_id: str) -> tuple[int | Decimal, int | str]:
    """Compare integer storage to exact numeric operands without rounding either."""

    if not isinstance(value, Decimal):
        return _normalized_integer(value, field_id)
    if not value.is_finite() or _decimal_scale(value) > 12 or not -(2**63) <= value < 2**63:
        raise CustomImportReadRequestError(f"filter value for {field_id} exceeds integer comparison bounds")
    if value == value.to_integral_value():
        return _normalized_integer(int(value), field_id)
    return value, format(value, "f")


def _normalized_decimal(value: object, field_id: str, *, maximum_integer_digits: int = 18) -> tuple[Decimal, str]:
    if isinstance(value, (bool, float)):
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a decimal")
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation, ValueError:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a decimal") from None
    if (
        not decimal_value.is_finite()
        or _decimal_scale(decimal_value) > 12
        or _decimal_integer_digits(decimal_value) > maximum_integer_digits
    ):
        raise CustomImportReadRequestError(f"filter value for {field_id} exceeds decimal storage")
    if decimal_value.is_zero():
        decimal_value = Decimal(0)
    return decimal_value, format(decimal_value, "f")


def _decimal_scale(value: Decimal) -> int:
    return max(-value.as_tuple().exponent, 0)


def _decimal_integer_digits(value: Decimal) -> int:
    return 0 if value.is_zero() else max(value.adjusted() + 1, 0)


def _normalized_boolean(value: object, field_id: str) -> tuple[bool, bool]:
    if type(value) is not bool:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not boolean")
    return value, value


def _normalized_date(value: object, field_id: str) -> tuple[dt.date, str]:
    if type(value) is str:
        try:
            parsed = dt.date.fromisoformat(value)
        except ValueError:
            parsed = None
        if parsed is None or parsed.isoformat() != value:
            raise CustomImportReadRequestError(f"filter value for {field_id} is not a date")
        return parsed, value
    if not isinstance(value, dt.date) or isinstance(value, dt.datetime):
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a date")
    return value, value.isoformat()


def _normalized_timestamp(value: object, field_id: str) -> tuple[dt.datetime, str]:
    if type(value) is str:
        try:
            parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
            raise CustomImportReadRequestError(f"filter value for {field_id} is not a timezone-aware timestamp")
        normalized = parsed.astimezone(dt.UTC)
        canonical = normalized.isoformat().replace("+00:00", "Z")
        if canonical != value:
            raise CustomImportReadRequestError(f"filter value for {field_id} is not a timezone-aware timestamp")
        return normalized, canonical
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a timezone-aware timestamp")
    normalized = value.astimezone(dt.UTC)
    return normalized, normalized.isoformat().replace("+00:00", "Z")


def _filtered_winner_statement(context: _ReadContext, filters: tuple[_NormalizedFilter, ...]):
    statement = _winner_statement(context)
    for predicate in filters:
        statement = statement.where(_predicate_condition(predicate, context))
    return statement


def _require_order_context_filters(
    order_terms: tuple[ReadOrderTerm, ...],
    filters: tuple[_NormalizedFilter, ...],
    context: _ReadContext,
    *,
    require_exact_context: bool = False,
) -> None:
    """Require an exact context for imported order or an explicit host contract."""

    if not order_terms and not require_exact_context:
        return
    profile = next(
        (
            profile_candidate
            for profile_candidate in context.definition.selection_profiles
            if profile_candidate.profile_id == context.target.profile_id
        ),
        None,
    )
    if profile is None:
        raise CustomImportReadUnavailableError("selection profile is not declared by the definition")
    if not profile.context_dimensions:
        return
    context_equality_by_field: dict[str, object] = {}
    for predicate in filters:
        if predicate.field.field_id not in profile.context_dimensions or predicate.operator != "eq":
            continue
        if predicate.value is None:
            continue
        if predicate.field.value_type == "integer" and type(predicate.value) is not int:
            raise CustomImportReadRequestError("context_required")
        previous_value = context_equality_by_field.get(predicate.field.field_id)
        if previous_value is not None and previous_value != predicate.canonical_value:
            raise CustomImportReadRequestError("context_required")
        context_equality_by_field[predicate.field.field_id] = predicate.canonical_value
    if any(field_id not in context_equality_by_field for field_id in profile.context_dimensions):
        raise CustomImportReadRequestError("context_required")


def _require_context_only_filters(filters: tuple[_NormalizedFilter, ...], context: _ReadContext) -> None:
    """Keep optional membership limited to declared context equality selectors."""

    profile = next(
        item for item in context.definition.selection_profiles if item.profile_id == context.target.profile_id
    )
    if any(
        predicate.field.field_id not in profile.context_dimensions
        or predicate.operator != "eq"
        or predicate.value is None
        or (predicate.field.value_type == "integer" and type(predicate.value) is not int)
        for predicate in filters
    ):
        raise CustomImportReadRequestError("metric predicates require imported membership")


def _npi_entity_relation_fingerprint(
    target: PinnedReadTarget,
    filters: tuple[_NormalizedFilter, ...],
    order_terms: tuple[ReadOrderTerm, ...],
    *,
    context_filters: tuple[_NormalizedFilter, ...] = (),
    entity_values: tuple[str, ...] | None = None,
) -> str:
    """Bind normalized imported query shape to the NPI relation domain."""

    query_shape_map = {
        "target": _target_document(target),
        "adapter": "npi",
        "context": [normalized_filter.descriptor for normalized_filter in context_filters],
        "filters": [normalized_filter.descriptor for normalized_filter in filters],
        "order": [{"field": term.field_id, "direction": term.direction, "nulls": term.nulls} for term in order_terms],
    }
    if entity_values is not None:
        query_shape_map["entity_values"] = sorted(entity_values)
    return hashlib.sha256(_NPI_ENTITY_RELATION_FINGERPRINT_DOMAIN + _canonical_bytes(query_shape_map)).hexdigest()


def _npi_entity_relation_statement(
    context: _ReadContext,
    filters: tuple[_NormalizedFilter, ...],
    order_terms: tuple[ReadOrderTerm, ...],
    *,
    context_filters: tuple[_NormalizedFilter, ...] = (),
    entity_values: tuple[str, ...] | None = None,
) -> Select:
    """Project exact NPI bindings and optional winner-local typed sort values."""

    entity_model = context.model(CustomImportEntityBinding)

    statement = _filtered_npi_winner_statement(context, filters, context_filters=context_filters)
    if entity_values is not None:
        statement = statement.where(entity_model.canonical_value.in_(entity_values))
    columns: list[object] = [entity_model.canonical_value.label("entity_value")]
    for ordinal, term in enumerate(order_terms):
        field = context.definition.fields_by_id[term.field_id]
        columns.append(_order_scalar_expression(field, context).label(f"sort_{ordinal}"))
    statement = statement.with_only_columns(*columns, maintain_column_froms=True)
    return statement if order_terms else statement.distinct()


def _filtered_npi_winner_statement(
    context: _ReadContext,
    filters: tuple[_NormalizedFilter, ...],
    *,
    context_filters: tuple[_NormalizedFilter, ...] = (),
) -> Select:
    """Apply metric predicates only after one selected winner and context relation."""

    winner_model = context.model(CustomImportWinner)
    entity_model = context.model(CustomImportEntityBinding)

    statement = (
        _filtered_winner_statement(context, context_filters)
        .join(
            entity_model,
            and_(
                entity_model.entity_binding_id == winner_model.entity_binding_id,
                entity_model.dataset_id == winner_model.dataset_id,
            ),
        )
        .where(
            entity_model.dataset_id == context.target.dataset_id,
            entity_model.adapter_id == "npi",
        )
    )
    for predicate in filters:
        statement = statement.where(_predicate_condition(predicate, context))
    return statement


def _winner_statement(context: _ReadContext):
    """Return only winners eligible for this pinned profile and context shape."""

    winner_model = context.model(CustomImportWinner)

    winner_statement = _winner_identity_statement(context)
    winner_statement = _pinned_generation_winner_statement(winner_statement, context)
    if context.profile_context_slot == 0:
        return winner_statement.where(
            winner_model.context_collection_slot == 0,
            winner_model.context_child_revision_id.is_(None),
        )
    return _context_child_winner_statement(winner_statement, context)


def _winner_identity_statement(context: _ReadContext):
    """Join each persisted winner to its exact generated family and root revision."""

    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    root_model = context.model(CustomImportRootRevision)
    generation_family_model = context.model(CustomImportGenerationFamily)

    return (
        select(winner_model, family_model, root_model)
        .select_from(winner_model)
        .join(
            generation_family_model,
            and_(
                generation_family_model.generation_id == winner_model.generation_id,
                generation_family_model.dataset_id == winner_model.dataset_id,
                generation_family_model.family_revision_id == winner_model.family_revision_id,
            ),
        )
        .join(
            family_model,
            and_(
                family_model.family_revision_id == winner_model.family_revision_id,
                family_model.entity_binding_id == winner_model.entity_binding_id,
                family_model.dataset_id == winner_model.dataset_id,
                family_model.schema_revision_id == winner_model.schema_revision_id,
                family_model.root_record_id == generation_family_model.root_record_id,
            ),
        )
        .join(
            root_model,
            and_(
                root_model.root_revision_id == family_model.root_revision_id,
                root_model.dataset_id == family_model.dataset_id,
                root_model.schema_revision_id == family_model.schema_revision_id,
                root_model.root_record_id == family_model.root_record_id,
            ),
        )
    )


def _pinned_generation_winner_statement(winner_statement, context: _ReadContext):
    """Restrict winner rows to the exact sealed pinned generation."""

    winner_model = context.model(CustomImportWinner)

    pinned_target = context.target
    return (
        winner_statement.join(
            CustomImportGeneration,
            and_(
                CustomImportGeneration.dataset_id == winner_model.dataset_id,
                CustomImportGeneration.generation_id == winner_model.generation_id,
                CustomImportGeneration.definition_revision_id == winner_model.definition_revision_id,
                CustomImportGeneration.schema_revision_id == winner_model.schema_revision_id,
            ),
        )
        .join(
            CustomImportGenerationSeal,
            and_(
                CustomImportGenerationSeal.generation_id == winner_model.generation_id,
                CustomImportGenerationSeal.dataset_id == winner_model.dataset_id,
                CustomImportGenerationSeal.definition_revision_id == winner_model.definition_revision_id,
                CustomImportGenerationSeal.schema_revision_id == winner_model.schema_revision_id,
            ),
        )
        .where(
            winner_model.dataset_id == pinned_target.dataset_id,
            winner_model.generation_id == pinned_target.generation_id,
            winner_model.definition_revision_id == pinned_target.definition_revision_id,
            winner_model.schema_revision_id == pinned_target.schema_revision_id,
            winner_model.profile_slot == context.profile_slot,
            CustomImportGenerationSeal.seal_contract == "custom-import-generation-seal/v1",
        )
    )


def _context_child_winner_statement(winner_statement, context: _ReadContext):
    """Join the one winner-selected context child, never an arbitrary sibling."""

    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    family_child_model = context.model(CustomImportFamilyChild)
    child_model = context.model(CustomImportChildRevision)

    return (
        winner_statement.add_columns(child_model)
        .join(
            family_child_model,
            and_(
                family_child_model.family_revision_id == winner_model.family_revision_id,
                family_child_model.dataset_id == winner_model.dataset_id,
                family_child_model.schema_revision_id == winner_model.schema_revision_id,
                family_child_model.root_record_id == family_model.root_record_id,
                family_child_model.collection_slot == winner_model.context_collection_slot,
                family_child_model.child_revision_id == winner_model.context_child_revision_id,
            ),
        )
        .join(
            child_model,
            and_(
                child_model.child_revision_id == winner_model.context_child_revision_id,
                child_model.dataset_id == winner_model.dataset_id,
                child_model.schema_revision_id == winner_model.schema_revision_id,
                child_model.root_record_id == family_model.root_record_id,
                child_model.collection_slot == winner_model.context_collection_slot,
            ),
        )
        .where(winner_model.context_collection_slot == context.profile_context_slot)
    )


def _predicate_condition(predicate: _NormalizedFilter, context: _ReadContext):
    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    root_scalar_model = context.model(CustomImportRootScalar)
    child_scalar_model = context.model(CustomImportChildScalar)

    if predicate.field.collection is None:
        return _scalar_predicate(
            root_scalar_model,
            predicate,
            _root_scalar_conditions(predicate.field, family_model.root_revision_id, context),
        )
    return _scalar_predicate(
        child_scalar_model,
        predicate,
        _child_scalar_conditions(predicate.field, winner_model.context_child_revision_id, context),
    )


def _scalar_predicate(scalar_model, predicate: _NormalizedFilter, identity_conditions: tuple[object, ...]):
    scalar_exists = exists(select(1).where(*identity_conditions))
    if predicate.operator == "is_missing":
        return not_(scalar_exists)
    if predicate.operator == "is_null":
        return exists(select(1).where(*identity_conditions, scalar_model.value_state == "null"))
    value_column = getattr(scalar_model, _SCALAR_COLUMNS[predicate.field.value_type])
    return exists(
        select(1).where(
            *identity_conditions,
            scalar_model.value_state == "value",
            _has_scalar_comparison(value_column, predicate.operator, predicate.value),
        )
    )


def _has_scalar_comparison(column, operator: str, value: object | None):
    if operator == "eq":
        return column == value
    if operator == "neq":
        return column != value
    if operator == "gt":
        return column > value
    if operator == "gte":
        return column >= value
    if operator == "lt":
        return column < value
    if operator == "lte":
        return column <= value
    raise CustomImportReadRequestError("filter operator is unsupported")


def _root_scalar_conditions(field: Field, root_revision_id, context: _ReadContext) -> tuple[object, ...]:
    family_model = context.model(CustomImportFamilyRevision)
    root_scalar_model = context.model(CustomImportRootScalar)

    return (
        root_scalar_model.root_revision_id == root_revision_id,
        root_scalar_model.dataset_id == context.target.dataset_id,
        root_scalar_model.schema_revision_id == context.target.schema_revision_id,
        root_scalar_model.root_record_id == family_model.root_record_id,
        root_scalar_model.field_slot == field.field_slot,
    )


def _child_scalar_conditions(field: Field, child_revision_id, context: _ReadContext) -> tuple[object, ...]:
    family_model = context.model(CustomImportFamilyRevision)
    child_scalar_model = context.model(CustomImportChildScalar)

    assert field.collection is not None
    collection_slot = context.collection_slots_by_name[field.collection]
    return (
        child_scalar_model.child_revision_id == child_revision_id,
        child_scalar_model.dataset_id == context.target.dataset_id,
        child_scalar_model.schema_revision_id == context.target.schema_revision_id,
        child_scalar_model.root_record_id == family_model.root_record_id,
        child_scalar_model.collection_slot == collection_slot,
        child_scalar_model.field_slot == field.field_slot,
    )


async def _exact_count(session: AsyncSession, statement) -> int:
    counted_statement = select(func.count()).select_from(statement.order_by(None).subquery())
    total = await session.scalar(counted_statement)
    if type(total) is not int or total < 0:
        raise CustomImportReadUnavailableError("exact winner count is unavailable")
    return total


async def _page_winner_rows(
    session: AsyncSession,
    statement,
    context: _ReadContext,
    plan: _SearchPlan,
    offset: int,
) -> tuple[
    tuple[CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None],
    ...,
]:
    if not 0 <= offset <= MAX_PAGE_OFFSET:
        raise CustomImportReadCursorError("custom import cursor is invalid")
    ordered_statement = statement.order_by(*_winner_order_terms(context, plan.order_terms))
    rows = (await session.execute(ordered_statement.offset(offset).limit(plan.page_size))).all()
    return tuple(_winner_row(row, context.profile_context_slot > 0) for row in rows)


def _winner_order_terms(context: _ReadContext, order_terms: tuple[ReadOrderTerm, ...]) -> tuple[object, ...]:
    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)

    expressions: list[object] = []
    for term in order_terms:
        field = context.definition.fields_by_id[term.field_id]
        expression = _order_scalar_expression(field, context)
        directed_expression = expression.asc() if term.direction == "asc" else expression.desc()
        expressions.append(
            directed_expression.nullsfirst() if term.nulls == "first" else directed_expression.nullslast()
        )
    expressions.extend(
        (
            family_model.root_record_id.asc(),
            winner_model.entity_binding_id.asc(),
            winner_model.context_key_sha256.asc(),
        )
    )
    return tuple(expressions)


def _order_scalar_expression(field: Field, context: _ReadContext):
    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    root_scalar_model = context.model(CustomImportRootScalar)
    child_scalar_model = context.model(CustomImportChildScalar)

    if field.collection is None:
        scalar_model = root_scalar_model
        conditions = _root_scalar_conditions(field, family_model.root_revision_id, context)
    else:
        scalar_model = child_scalar_model
        conditions = _child_scalar_conditions(field, winner_model.context_child_revision_id, context)
    value_column = getattr(scalar_model, _SCALAR_COLUMNS[field.value_type])
    return select(value_column).where(*conditions, scalar_model.value_state == "value").scalar_subquery()


def _winner_row(
    row,
    has_context_child: bool,
) -> tuple[CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None]:
    values = tuple(row)
    if has_context_child:
        winner, family, root_revision, context_child = values
        return winner, family, root_revision, context_child
    winner, family, root_revision = values
    return winner, family, root_revision, None


async def _hydrate_provider_page_items(session, context, selected_rows, scope_digest) -> tuple[SearchItem, ...]:
    """Attach all children from the distinct exact selected provider families."""

    unique_rows = tuple({selected_row[1].family_revision_id: selected_row for selected_row in selected_rows}.values())
    if len(unique_rows) > MAX_FULL_FAMILY_PAGE_SIZE:
        raise CustomImportReadUnavailableError("full-family provider page exceeds its bound")
    details = await _hydrate_complete_family_entities(
        session,
        context,
        unique_rows,
        tuple(selected_row[1].family_revision_id for selected_row in unique_rows),
        scope_digest,
        is_page=True,
    )
    children_by_family = {detail.winner.family_revision_id: detail.children for detail in details}
    provider_items = await _hydrate_search_page_items(session, context, selected_rows)
    return tuple(
        replace(provider_item, children=children_by_family[provider_item.winner.family_revision_id])
        for provider_item in provider_items
    )


async def _hydrate_search_page_items(
    session: AsyncSession,
    context: _ReadContext,
    selected_rows: tuple[
        tuple[
            CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None
        ],
        ...,
    ],
) -> tuple[SearchItem, ...]:
    if not selected_rows:
        return ()
    root_fields = _query_root_fields(context.definition)
    child_fields = _query_child_fields(context.definition, context.profile_context_slot)
    root_rows_by_key = await _root_scalar_rows(
        session, context, tuple(row[2].root_revision_id for row in selected_rows), root_fields
    )
    child_ids = tuple(row[3].child_revision_id for row in selected_rows if row[3] is not None)
    child_rows_by_key = await _child_scalar_rows(session, context, child_ids, child_fields)
    return tuple(
        _selected_row_to_search_item(row, root_fields, child_fields, root_rows_by_key, child_rows_by_key)
        for row in selected_rows
    )


def _query_root_fields(definition: CustomImportDefinition) -> tuple[Field, ...]:
    return tuple(definition.fields_by_id[field_id] for field_id in definition.query.root_fields)


def _query_child_fields(definition: CustomImportDefinition, profile_context_slot: int) -> tuple[Field, ...]:
    if profile_context_slot == 0:
        return ()
    return tuple(definition.fields_by_id[field_id] for field_id in definition.query.child_fields)


async def _root_scalar_rows(
    session: AsyncSession,
    context: _ReadContext,
    root_revision_ids: tuple[int, ...],
    fields: tuple[Field, ...],
) -> dict[tuple[int, int], CustomImportRootScalar]:
    root_scalar_model = context.model(CustomImportRootScalar)

    if not fields:
        return {}
    rows = (
        (
            await session.execute(
                select(root_scalar_model).where(
                    root_scalar_model.dataset_id == context.target.dataset_id,
                    root_scalar_model.schema_revision_id == context.target.schema_revision_id,
                    root_scalar_model.root_revision_id.in_(root_revision_ids),
                    root_scalar_model.field_slot.in_(tuple(field.field_slot for field in fields)),
                )
            )
        )
        .scalars()
        .all()
    )
    return {(row.root_revision_id, row.field_slot): row for row in rows}


async def _child_scalar_rows(
    session: AsyncSession,
    context: _ReadContext,
    child_revision_ids: tuple[int, ...],
    fields: tuple[Field, ...],
) -> dict[tuple[int, int], CustomImportChildScalar]:
    child_scalar_model = context.model(CustomImportChildScalar)

    if not fields or not child_revision_ids:
        return {}
    rows = (
        (
            await session.execute(
                select(child_scalar_model).where(
                    child_scalar_model.dataset_id == context.target.dataset_id,
                    child_scalar_model.schema_revision_id == context.target.schema_revision_id,
                    child_scalar_model.child_revision_id.in_(child_revision_ids),
                    child_scalar_model.field_slot.in_(tuple(field.field_slot for field in fields)),
                )
            )
        )
        .scalars()
        .all()
    )
    return {(row.child_revision_id, row.field_slot): row for row in rows}


def _selected_row_to_search_item(
    selected_row: tuple[
        CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None
    ],
    root_fields: tuple[Field, ...],
    child_fields: tuple[Field, ...],
    root_rows_by_key: Mapping[tuple[int, int], CustomImportRootScalar],
    child_rows_by_key: Mapping[tuple[int, int], CustomImportChildScalar],
) -> SearchItem:
    winner, family, root_revision, context_child = selected_row
    locator = _winner_locator(winner, family)
    root_values = _project_field_values(root_fields, root_revision.root_revision_id, root_rows_by_key)
    context_values = (
        ()
        if context_child is None
        else _project_field_values(child_fields, context_child.child_revision_id, child_rows_by_key)
    )
    return SearchItem(
        winner=locator,
        root_fields=root_values,
        context_child_revision_id=None if context_child is None else context_child.child_revision_id,
        context_fields=context_values,
    )


def _winner_locator(winner: CustomImportWinner, family: CustomImportFamilyRevision) -> WinnerLocator:
    return WinnerLocator(
        root_record_id=family.root_record_id,
        family_revision_id=family.family_revision_id,
        entity_binding_id=winner.entity_binding_id,
        context_key_sha256=bytes(winner.context_key_sha256),
    )


def _project_field_values(
    fields: tuple[Field, ...], revision_id: int, rows_by_key: Mapping[tuple[int, int], object]
) -> tuple[ReadFieldValue, ...]:
    return tuple(_field_value(field, rows_by_key.get((revision_id, field.field_slot))) for field in fields)


def _field_value(field: Field, scalar_row: object | None) -> ReadFieldValue:
    if scalar_row is None:
        return ReadFieldValue(field.field_id, field.value_type, "missing", None)
    if getattr(scalar_row, "field_type", None) != field.value_type or getattr(
        scalar_row, "value_state", None
    ) not in _VALUE_STATES - {"missing"}:
        raise CustomImportReadUnavailableError("persisted scalar row is invalid")
    state = scalar_row.value_state
    if state == "null":
        if not field.nullable:
            raise CustomImportReadUnavailableError("persisted scalar null violates the field contract")
        return ReadFieldValue(field.field_id, field.value_type, "null", None)
    value = getattr(scalar_row, _SCALAR_COLUMNS[field.value_type])
    if value is None:
        raise CustomImportReadUnavailableError("persisted scalar value is invalid")
    return ReadFieldValue(field.field_id, field.value_type, "value", value)


async def _selected_winner_row(
    session: AsyncSession,
    context: _ReadContext,
    locator: WinnerLocator,
) -> tuple[CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None]:
    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)

    statement = _winner_statement(context).where(
        winner_model.entity_binding_id == locator.entity_binding_id,
        winner_model.family_revision_id == locator.family_revision_id,
        family_model.root_record_id == locator.root_record_id,
        winner_model.context_key_sha256 == locator.context_key_sha256,
    )
    row = (await session.execute(statement)).one_or_none()
    if row is None:
        raise CustomImportReadUnavailableError("selected winner is not eligible for root detail")
    return _winner_row(row, context.profile_context_slot > 0)


async def _require_unambiguous_npi_families(session, context, statement):
    """Apply the scalar detail family identity rule to the complete NPI page."""

    entity_model = context.model(CustomImportEntityBinding)
    family_model = context.model(CustomImportFamilyRevision)
    winner_model = context.model(CustomImportWinner)
    family_count = func.count(
        func.distinct(
            tuple_(family_model.root_record_id, winner_model.family_revision_id, winner_model.entity_binding_id)
        )
    )
    ambiguity_statement = (
        statement.with_only_columns(entity_model.canonical_value, maintain_column_froms=True)
        .group_by(entity_model.canonical_value)
        .having(family_count > 1)
        .limit(1)
    )
    if (await session.execute(ambiguity_statement)).first() is not None:
        raise CustomImportReadUnavailableError("selected entity is not eligible for root detail")


async def _entity_winner_locator(
    session: AsyncSession,
    context: _ReadContext,
    entity: EntityLocator,
) -> WinnerLocator:
    """Resolve one entity only when its pinned profile has one root family."""

    winner_model = context.model(CustomImportWinner)
    family_model = context.model(CustomImportFamilyRevision)
    entity_model = context.model(CustomImportEntityBinding)

    entity_statement = (
        _winner_statement(context)
        .join(
            entity_model,
            and_(
                entity_model.entity_binding_id == winner_model.entity_binding_id,
                entity_model.dataset_id == winner_model.dataset_id,
            ),
        )
        .where(
            entity_model.adapter_id == entity.adapter_id,
            entity_model.canonical_value == entity.value,
        )
    )
    family_rows = (
        await session.execute(
            entity_statement.with_only_columns(
                family_model.root_record_id,
                winner_model.family_revision_id,
                winner_model.entity_binding_id,
                maintain_column_froms=True,
            )
            .distinct()
            .limit(2)
        )
    ).all()
    if not family_rows:
        await verify_published_generation(session, context.target)
        raise CustomImportReadEntityAbsentError("selected entity has no eligible root family")
    if len(family_rows) != 1:
        raise CustomImportReadUnavailableError("selected entity is not eligible for root detail")
    root_record_id, family_revision_id, entity_binding_id = family_rows[0]
    selected_row = (
        await session.execute(
            entity_statement.where(
                family_model.root_record_id == root_record_id,
                winner_model.family_revision_id == family_revision_id,
                winner_model.entity_binding_id == entity_binding_id,
            )
            .order_by(winner_model.context_key_sha256)
            .limit(1)
        )
    ).one_or_none()
    if selected_row is None:
        raise CustomImportReadUnavailableError("selected entity is not eligible for root detail")
    winner, family = _winner_row(selected_row, context.profile_context_slot > 0)[:2]
    return _winner_locator(winner, family)


async def _hydrate_root_detail(
    session: AsyncSession,
    context: _ReadContext,
    selected_row: tuple[
        CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None
    ],
    scope_digest: str,
) -> RootDetail:
    return (await _hydrate_selected_families(session, context, (selected_row,), scope_digest))[0]


async def _hydrate_complete_family_entities(
    session, context, selected_rows, entity_values, scope_digest, *, is_page=False
):
    """Preflight every entity and bound projections retained for ASCII-wire pages.

    Each scalar/membership batch keeps the existing detail child bound; a page
    cannot multiply the size of an unchecked database hydration by its row count.
    Detail keeps its existing final UTF-8 response bound instead.
    """

    rows_by_entity = {}
    for index, (selected_row, entity_value) in enumerate(zip(selected_rows, entity_values, strict=True)):
        rows_by_entity.setdefault(entity_value, []).append((index, selected_row))
    batches = [[]]
    batch_children = 0
    for entity_rows in rows_by_entity.values():
        children = sum(selected_row[1].child_count for _, selected_row in entity_rows)
        if any(selected_row[1].child_count < 0 for _, selected_row in entity_rows) or children > MAX_DETAIL_CHILDREN:
            raise CustomImportReadUnavailableError("selected families exceed the bounded detail child limit")
        if batch_children + children > MAX_DETAIL_CHILDREN:
            batches.append([])
            batch_children = 0
        batches[-1].append(entity_rows)
        batch_children += children
    projections = [None] * len(selected_rows)
    for batch in batches:
        batch_rows = [selected_row for entity_rows in batch for _, selected_row in entity_rows]
        hydrated = iter(
            await _hydrate_selected_families(
                session,
                context,
                tuple(batch_rows),
                scope_digest,
            )
        )
        for entity_rows in batch:
            families = tuple(next(hydrated) for _ in entity_rows)
            if is_page and (
                len(_canonical_bytes({"families": [full_family_payload(family) for family in families]}))
                > MAX_FAMILY_RESPONSE_BYTES
            ):
                raise CustomImportReadUnavailableError("selected family response exceeds its bound")
            for (index, _), family in zip(entity_rows, families, strict=True):
                projections[index] = family
    return tuple(projections)


async def _hydrate_selected_families(
    session: AsyncSession,
    context: _ReadContext,
    selected_rows: tuple[
        tuple[
            CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None
        ],
        ...,
    ],
    scope_digest: str,
) -> tuple[RootDetail, ...]:
    """Hydrate a bounded selected page with batched scalar and membership reads."""

    if not selected_rows:
        return ()
    if (
        any(selected_row[1].child_count < 0 for selected_row in selected_rows)
        or sum(selected_row[1].child_count for selected_row in selected_rows) > MAX_DETAIL_CHILDREN
    ):
        raise CustomImportReadUnavailableError("selected families exceed the bounded detail child limit")
    root_fields = _detail_root_fields(context.definition)
    root_rows_by_key = await _root_scalar_rows(
        session, context, tuple(selected_row[2].root_revision_id for selected_row in selected_rows), root_fields
    )
    child_rows = await _family_child_rows(session, context, *(selected_row[1] for selected_row in selected_rows))
    children_by_family = {selected_row[1].family_revision_id: [] for selected_row in selected_rows}
    for family_child, child in child_rows:
        children_by_family[family_child.family_revision_id].append((family_child, child))
    if any(
        len(children_by_family[selected_row[1].family_revision_id]) != selected_row[1].child_count
        for selected_row in selected_rows
    ):
        raise CustomImportReadUnavailableError("selected family child membership is incomplete")
    child_fields_by_slot = _detail_child_fields_by_slot(context)
    projected_child_fields = tuple(field for fields in child_fields_by_slot.values() for field in fields)
    child_scalar_rows = await _child_scalar_rows(
        session,
        context,
        tuple(child.child_revision_id for _, child in child_rows),
        projected_child_fields,
    )
    return tuple(
        RootDetail(
            target=context.target,
            winner=_winner_locator(winner, family),
            root_fields=_project_field_values(root_fields, root_revision.root_revision_id, root_rows_by_key),
            children=tuple(
                _detail_child(family_child, child, context, child_fields_by_slot, child_scalar_rows)
                for family_child, child in children_by_family[family.family_revision_id]
            ),
            authorization_scope_sha256=scope_digest,
        )
        for winner, family, root_revision, _context_child in selected_rows
    )


def _detail_root_fields(definition: CustomImportDefinition) -> tuple[Field, ...]:
    return tuple(
        sorted(
            (field for field in definition.root_fields if field.projection_slot is not None),
            key=lambda field: field.projection_slot,
        )
    )


def _detail_child_fields_by_slot(context: _ReadContext) -> dict[int, tuple[Field, ...]]:
    fields_by_slot: dict[int, tuple[Field, ...]] = {}
    for collection_name, collection_slot in context.collection_slots_by_name.items():
        collection_fields = tuple(
            sorted(
                (
                    field
                    for field in context.definition.child_fields
                    if field.collection == collection_name and field.projection_slot is not None
                ),
                key=lambda field: field.projection_slot,
            )
        )
        fields_by_slot[collection_slot] = collection_fields
    return fields_by_slot


async def _family_child_rows(
    session: AsyncSession,
    context: _ReadContext,
    *families: CustomImportFamilyRevision,
) -> tuple[tuple[CustomImportFamilyChild, CustomImportChildRevision], ...]:
    family_child_model = context.model(CustomImportFamilyChild)
    child_model = context.model(CustomImportChildRevision)

    family_keys = tuple((family.family_revision_id, family.root_record_id) for family in families if family.child_count)
    if not family_keys:
        return ()
    statement = (
        select(family_child_model, child_model)
        .select_from(family_child_model)
        .join(
            child_model,
            and_(
                child_model.child_revision_id == family_child_model.child_revision_id,
                child_model.dataset_id == family_child_model.dataset_id,
                child_model.schema_revision_id == family_child_model.schema_revision_id,
                child_model.root_record_id == family_child_model.root_record_id,
                child_model.collection_slot == family_child_model.collection_slot,
            ),
        )
        .where(
            tuple_(family_child_model.family_revision_id, family_child_model.root_record_id).in_(family_keys),
            family_child_model.dataset_id == context.target.dataset_id,
            family_child_model.schema_revision_id == context.target.schema_revision_id,
        )
        .order_by(
            family_child_model.family_revision_id,
            family_child_model.collection_slot,
            child_model.source_ordinal,
            child_model.child_revision_id,
        )
    )
    return tuple((family_child, child) for family_child, child in (await session.execute(statement)).all())


def _detail_child(
    family_child: CustomImportFamilyChild,
    child: CustomImportChildRevision,
    context: _ReadContext,
    fields_by_slot: Mapping[int, tuple[Field, ...]],
    scalar_rows_by_key: Mapping[tuple[int, int], CustomImportChildScalar],
) -> ReadChild:
    fields = fields_by_slot.get(family_child.collection_slot)
    collection_name = context.collection_names_by_slot.get(family_child.collection_slot)
    if fields is None or collection_name is None:
        raise CustomImportReadUnavailableError("selected family has an undeclared child collection")
    return ReadChild(
        collection=collection_name,
        child_revision_id=child.child_revision_id,
        fields=_project_field_values(fields, child.child_revision_id, scalar_rows_by_key),
    )


__all__ = (
    "CustomImportReadAuthorizationError",
    "CustomImportReadCache",
    "CustomImportReadCursorError",
    "CustomImportReadEntityAbsentError",
    "CustomImportReadError",
    "CustomImportReadRequestError",
    "CustomImportReadService",
    "CustomImportReadUnavailableError",
    "DEFAULT_READ_TIMEOUT_MS",
    "EntityFamilySet",
    "EntityLocator",
    "ExtensionReadAuthorization",
    "ExtensionReadAuthorizer",
    "ExtensionReadScope",
    "MAX_CURSOR_TTL_SECONDS",
    "MAX_DETAIL_CHILDREN",
    "MAX_FILTER_TERMS",
    "MAX_ORDER_TERMS",
    "MAX_PAGE_SIZE",
    "MAX_READ_TIMEOUT_MS",
    "NpiEntityRelationQuery",
    "PinnedReadTarget",
    "PreparedNpiEntityRelation",
    "READ_CORE_CONTRACT",
    "ReadChild",
    "ReadCursorCodec",
    "ReadCursorState",
    "ReadFieldValue",
    "ReadFilter",
    "ReadOrderTerm",
    "RootDetailRequest",
    "RootDetail",
    "SearchItem",
    "SearchPage",
    "SearchRequest",
    "WinnerLocator",
)
