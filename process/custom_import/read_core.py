# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded read services for already materialized ``custom-import/v1`` data.

This module deliberately has no HTTP registration or policy implementation.
An extension-facing adapter verifies its own authorization material, supplies a
stable authorization scope through :class:`ExtensionReadAuthorizer`, and maps
these typed results to its transport.  The read core then permits only an
explicit current, sealed generation and one exact selection profile.

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
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from sqlalchemy import and_, exists, func, not_, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportDefinitionRevision,
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
    Field,
    canonical_json,
    canonical_sha256,
    load_json_definition,
)
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    MAX_CURSOR_TTL_SECONDS,
    MAX_DETAIL_CHILDREN,
    MAX_FILTER_TERMS,
    MAX_ORDER_TERMS,
    MAX_PAGE_OFFSET,
    MAX_PAGE_SIZE,
    MAX_READ_TIMEOUT_MS,
    READ_CORE_CONTRACT,
    CustomImportReadAuthorizationError,
    CustomImportReadCache,
    CustomImportReadCursorError,
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
    verified_definition,
    verify_current_generation,
)

_VALUE_STATES = frozenset({"value", "null", "missing"})
_FILTER_OPERATORS = frozenset({"eq", "neq", "gt", "gte", "lt", "lte", "is_null", "is_missing"})
_RANGE_FIELD_TYPES = frozenset({"integer", "decimal", "date", "timestamp"})
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
        if self.direction not in {"asc", "desc"} or self.nulls not in {"first", "last"}:
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


@dataclass(frozen=True, slots=True)
class SearchPage:
    """One exact-count page over a sealed, current winner selection."""

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
    pointer_version: int
    definition: CustomImportDefinition
    profile_slot: int
    profile_context_slot: int
    collection_slots_by_name: Mapping[str, int]
    collection_names_by_slot: Mapping[int, str]


class CustomImportReadService:
    """Read exact winner materialization results after a host policy decision."""

    def __init__(
        self,
        *,
        authorizer: ExtensionReadAuthorizer | None,
        cursor_secret: bytes,
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
        self._cursor_codec = ReadCursorCodec(cursor_secret)
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
        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, request.target)
            plan = _normalize_search_plan(request, context)
            trusted_now = self._trusted_now()
            scope_digest = _scope_digest(authorization_scope)
            offset = self._cursor_offset(request, plan, scope_digest, trusted_now)
            cache_key = _cache_key("search", context.target, plan.fingerprint, scope_digest, offset)
            cached_page = await _cached_search_page(self._cache, cache_key, context, plan, scope_digest, trusted_now)
            if cached_page is not None:
                await verify_current_generation(session, context.target, context.pointer_version)
                return cached_page
            statement = _filtered_winner_statement(context, plan.filters)
            total = await _exact_count(session, statement)
            selected_rows = await _page_winner_rows(session, statement, context, plan, offset)
            page_items = await _hydrate_search_page_items(session, context, selected_rows)
            await verify_current_generation(session, context.target, context.pointer_version)
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
        async with _bounded_read_window(session, timeout_ms=self._statement_timeout_ms):
            context = await _load_read_context(session, target)
            trusted_now = self._trusted_now()
            scope_digest = _scope_digest(authorization_scope)
            cache_key = _detail_cache_key(context.target, winner, scope_digest)
            cached_detail = await _cached_root_detail(self._cache, cache_key, context, winner, scope_digest)
            if cached_detail is not None:
                await verify_current_generation(session, context.target, context.pointer_version)
                return cached_detail
            selected_row = await _selected_winner_row(session, context, winner)
            detail = await _hydrate_root_detail(session, context, selected_row, scope_digest)
            await verify_current_generation(session, context.target, context.pointer_version)
        await _cache_result(self._cache, cache_key, detail, trusted_now + self._cursor_ttl_seconds)
        return detail

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
        if page_window.returned_count == 0 or next_offset >= page_window.total:
            return None
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
    definition_row, schema_row, pointer_version = await _eligible_definition_rows(session, target)
    if type(pointer_version) is not int or pointer_version <= 0:
        raise CustomImportReadUnavailableError("current generation pointer is invalid")
    definition = verified_definition(definition_row, schema_row)
    collection_slots = await _collection_slots(session, target, definition)
    profile = await _exact_profile(session, target)
    profile_slot, profile_context_slot = _verified_profile(profile, definition, collection_slots)
    await _verified_field_rows(session, target, definition, collection_slots)
    return _ReadContext(
        target=target,
        pointer_version=pointer_version,
        definition=definition,
        profile_slot=profile_slot,
        profile_context_slot=profile_context_slot,
        collection_slots_by_name=collection_slots,
        collection_names_by_slot={slot: name for name, slot in collection_slots.items()},
    )


async def _eligible_definition_rows(
    session: AsyncSession,
    pinned_target: PinnedReadTarget,
) -> tuple[CustomImportDefinitionRevision, CustomImportSchemaRevision, int]:
    statement = (
        select(
            CustomImportDefinitionRevision,
            CustomImportSchemaRevision,
            CustomImportCurrentGeneration.pointer_version,
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
            CustomImportCurrentGeneration,
            and_(
                CustomImportCurrentGeneration.dataset_id == CustomImportDefinitionRevision.dataset_id,
                CustomImportCurrentGeneration.definition_revision_id
                == CustomImportDefinitionRevision.definition_revision_id,
                CustomImportCurrentGeneration.schema_revision_id == CustomImportDefinitionRevision.schema_revision_id,
            ),
        )
        .join(
            CustomImportGeneration,
            and_(
                CustomImportGeneration.generation_id == CustomImportCurrentGeneration.generation_id,
                CustomImportGeneration.dataset_id == CustomImportCurrentGeneration.dataset_id,
                CustomImportGeneration.definition_revision_id == CustomImportCurrentGeneration.definition_revision_id,
                CustomImportGeneration.schema_revision_id == CustomImportCurrentGeneration.schema_revision_id,
            ),
        )
        .join(
            CustomImportGenerationSeal,
            and_(
                CustomImportGenerationSeal.generation_id == CustomImportCurrentGeneration.generation_id,
                CustomImportGenerationSeal.dataset_id == CustomImportCurrentGeneration.dataset_id,
                CustomImportGenerationSeal.definition_revision_id
                == CustomImportCurrentGeneration.definition_revision_id,
                CustomImportGenerationSeal.schema_revision_id == CustomImportCurrentGeneration.schema_revision_id,
            ),
        )
        .where(
            CustomImportDefinitionRevision.dataset_id == pinned_target.dataset_id,
            CustomImportDefinitionRevision.definition_revision_id == pinned_target.definition_revision_id,
            CustomImportDefinitionRevision.schema_revision_id == pinned_target.schema_revision_id,
            CustomImportCurrentGeneration.generation_id == pinned_target.generation_id,
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
    if type(request) is not SearchRequest or request.target != context.target:
        raise CustomImportReadRequestError("search target does not match the verified read context")
    filters = _normalized_filters(request.filters, context)
    declared_order_terms = tuple(
        ReadOrderTerm(field_id=term.field_id, direction=term.direction, nulls=term.nulls)
        for term in context.definition.query.order_terms
    )
    requested_order_terms = declared_order_terms if request.order_terms is None else request.order_terms
    if (
        len(declared_order_terms) > MAX_ORDER_TERMS
        or len(requested_order_terms) > MAX_ORDER_TERMS
        or requested_order_terms != declared_order_terms
    ):
        raise CustomImportReadRequestError("order terms must exactly match the bounded definition order")
    _verify_order_context(requested_order_terms, context)
    search_shape_map = {
        "filters": [item.descriptor for item in filters],
        "order": [
            {"field": term.field_id, "direction": term.direction, "nulls": term.nulls} for term in requested_order_terms
        ],
        "page_size": request.page_size,
    }
    return _SearchPlan(
        filters=filters,
        order_terms=requested_order_terms,
        page_size=request.page_size,
        fingerprint=hashlib.sha256(_canonical_bytes(search_shape_map)).hexdigest(),
    )


def _normalized_filters(raw_filters: tuple[ReadFilter, ...], context: _ReadContext) -> tuple[_NormalizedFilter, ...]:
    if len(raw_filters) > MAX_FILTER_TERMS:
        raise CustomImportReadRequestError("filter count exceeds the read-core limit")
    normalized_filters: list[_NormalizedFilter] = []
    permitted_ids = set(context.definition.query.root_fields) | set(context.definition.query.child_fields)
    for raw_filter in raw_filters:
        if type(raw_filter) is not ReadFilter or raw_filter.field_id not in permitted_ids:
            raise CustomImportReadRequestError("filter field is not declared by the query contract")
        field = context.definition.fields_by_id.get(raw_filter.field_id)
        if field is None:
            raise CustomImportReadUnavailableError("declared query field has no persisted binding")
        _verify_field_context(field, context)
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


def _verify_order_context(order_terms: tuple[ReadOrderTerm, ...], context: _ReadContext) -> None:
    permitted_ids = set(context.definition.query.root_fields) | set(context.definition.query.child_fields)
    for term in order_terms:
        if type(term) is not ReadOrderTerm or term.field_id not in permitted_ids:
            raise CustomImportReadRequestError("order field is not declared by the query contract")
        field = context.definition.fields_by_id.get(term.field_id)
        if field is None:
            raise CustomImportReadUnavailableError("declared order field has no persisted binding")
        _verify_field_context(field, context)


def _verify_field_context(field: Field, context: _ReadContext) -> None:
    if field.collection is None:
        return
    expected_slot = context.collection_slots_by_name.get(field.collection)
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
    if field.value_type == "string":
        return _normalized_string(raw_value, field.field_id)
    if field.value_type == "integer":
        return _normalized_integer(raw_value, field.field_id)
    if field.value_type == "decimal":
        return _normalized_decimal(raw_value, field.field_id)
    if field.value_type == "boolean":
        return _normalized_boolean(raw_value, field.field_id)
    if field.value_type == "date":
        return _normalized_date(raw_value, field.field_id)
    if field.value_type == "timestamp":
        return _normalized_timestamp(raw_value, field.field_id)
    raise CustomImportReadUnavailableError("declared query field has an unsupported scalar type")


def _normalized_string(value: object, field_id: str) -> tuple[str, str]:
    if type(value) is not str or "\x00" in value or len(value.encode("utf-8")) > 2_048:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not an indexed string")
    return value, value


def _normalized_integer(value: object, field_id: str) -> tuple[int, int]:
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a BIGINT")
    return value, value


def _normalized_decimal(value: object, field_id: str) -> tuple[Decimal, str]:
    if isinstance(value, (bool, float)):
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a decimal")
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation, ValueError:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a decimal") from None
    if (
        not decimal_value.is_finite()
        or _decimal_scale(decimal_value) > 12
        or _decimal_integer_digits(decimal_value) > 18
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
    if not isinstance(value, dt.date) or isinstance(value, dt.datetime):
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a date")
    return value, value.isoformat()


def _normalized_timestamp(value: object, field_id: str) -> tuple[dt.datetime, str]:
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CustomImportReadRequestError(f"filter value for {field_id} is not a timezone-aware timestamp")
    normalized = value.astimezone(dt.UTC)
    return normalized, normalized.isoformat().replace("+00:00", "Z")


def _filtered_winner_statement(context: _ReadContext, filters: tuple[_NormalizedFilter, ...]):
    statement = _winner_statement(context)
    for predicate in filters:
        statement = statement.where(_predicate_condition(predicate, context))
    return statement


def _winner_statement(context: _ReadContext):
    """Return only winners eligible for this pinned profile and context shape."""

    winner_statement = _winner_identity_statement()
    winner_statement = _current_generation_winner_statement(winner_statement, context)
    if context.profile_context_slot == 0:
        return winner_statement.where(
            CustomImportWinner.context_collection_slot == 0,
            CustomImportWinner.context_child_revision_id.is_(None),
        )
    return _context_child_winner_statement(winner_statement, context)


def _winner_identity_statement():
    """Join each persisted winner to its exact generated family and root revision."""

    return (
        select(CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision)
        .select_from(CustomImportWinner)
        .join(
            CustomImportGenerationFamily,
            and_(
                CustomImportGenerationFamily.generation_id == CustomImportWinner.generation_id,
                CustomImportGenerationFamily.dataset_id == CustomImportWinner.dataset_id,
                CustomImportGenerationFamily.family_revision_id == CustomImportWinner.family_revision_id,
            ),
        )
        .join(
            CustomImportFamilyRevision,
            and_(
                CustomImportFamilyRevision.family_revision_id == CustomImportWinner.family_revision_id,
                CustomImportFamilyRevision.dataset_id == CustomImportWinner.dataset_id,
                CustomImportFamilyRevision.schema_revision_id == CustomImportWinner.schema_revision_id,
                CustomImportFamilyRevision.root_record_id == CustomImportGenerationFamily.root_record_id,
            ),
        )
        .join(
            CustomImportRootRevision,
            and_(
                CustomImportRootRevision.root_revision_id == CustomImportFamilyRevision.root_revision_id,
                CustomImportRootRevision.dataset_id == CustomImportFamilyRevision.dataset_id,
                CustomImportRootRevision.schema_revision_id == CustomImportFamilyRevision.schema_revision_id,
                CustomImportRootRevision.root_record_id == CustomImportFamilyRevision.root_record_id,
            ),
        )
    )


def _current_generation_winner_statement(winner_statement, context: _ReadContext):
    """Restrict winner rows to the exact current and sealed pinned generation."""

    pinned_target = context.target
    return (
        winner_statement.join(
            CustomImportCurrentGeneration,
            and_(
                CustomImportCurrentGeneration.dataset_id == CustomImportWinner.dataset_id,
                CustomImportCurrentGeneration.generation_id == CustomImportWinner.generation_id,
                CustomImportCurrentGeneration.definition_revision_id == CustomImportWinner.definition_revision_id,
                CustomImportCurrentGeneration.schema_revision_id == CustomImportWinner.schema_revision_id,
            ),
        )
        .join(
            CustomImportGenerationSeal,
            and_(
                CustomImportGenerationSeal.generation_id == CustomImportWinner.generation_id,
                CustomImportGenerationSeal.dataset_id == CustomImportWinner.dataset_id,
                CustomImportGenerationSeal.definition_revision_id == CustomImportWinner.definition_revision_id,
                CustomImportGenerationSeal.schema_revision_id == CustomImportWinner.schema_revision_id,
            ),
        )
        .where(
            CustomImportWinner.dataset_id == pinned_target.dataset_id,
            CustomImportWinner.generation_id == pinned_target.generation_id,
            CustomImportWinner.definition_revision_id == pinned_target.definition_revision_id,
            CustomImportWinner.schema_revision_id == pinned_target.schema_revision_id,
            CustomImportWinner.profile_slot == context.profile_slot,
            CustomImportGenerationSeal.seal_contract == "custom-import-generation-seal/v1",
        )
    )


def _context_child_winner_statement(winner_statement, context: _ReadContext):
    """Join the one winner-selected context child, never an arbitrary sibling."""

    return (
        winner_statement.add_columns(CustomImportChildRevision)
        .join(
            CustomImportFamilyChild,
            and_(
                CustomImportFamilyChild.family_revision_id == CustomImportWinner.family_revision_id,
                CustomImportFamilyChild.dataset_id == CustomImportWinner.dataset_id,
                CustomImportFamilyChild.schema_revision_id == CustomImportWinner.schema_revision_id,
                CustomImportFamilyChild.root_record_id == CustomImportFamilyRevision.root_record_id,
                CustomImportFamilyChild.collection_slot == CustomImportWinner.context_collection_slot,
                CustomImportFamilyChild.child_revision_id == CustomImportWinner.context_child_revision_id,
            ),
        )
        .join(
            CustomImportChildRevision,
            and_(
                CustomImportChildRevision.child_revision_id == CustomImportWinner.context_child_revision_id,
                CustomImportChildRevision.dataset_id == CustomImportWinner.dataset_id,
                CustomImportChildRevision.schema_revision_id == CustomImportWinner.schema_revision_id,
                CustomImportChildRevision.root_record_id == CustomImportFamilyRevision.root_record_id,
                CustomImportChildRevision.collection_slot == CustomImportWinner.context_collection_slot,
            ),
        )
        .where(CustomImportWinner.context_collection_slot == context.profile_context_slot)
    )


def _predicate_condition(predicate: _NormalizedFilter, context: _ReadContext):
    if predicate.field.collection is None:
        return _scalar_predicate(
            CustomImportRootScalar,
            predicate,
            _root_scalar_conditions(predicate.field, CustomImportFamilyRevision.root_revision_id, context),
        )
    return _scalar_predicate(
        CustomImportChildScalar,
        predicate,
        _child_scalar_conditions(predicate.field, CustomImportWinner.context_child_revision_id, context),
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
    return (
        CustomImportRootScalar.root_revision_id == root_revision_id,
        CustomImportRootScalar.dataset_id == context.target.dataset_id,
        CustomImportRootScalar.schema_revision_id == context.target.schema_revision_id,
        CustomImportRootScalar.root_record_id == CustomImportFamilyRevision.root_record_id,
        CustomImportRootScalar.field_slot == field.field_slot,
    )


def _child_scalar_conditions(field: Field, child_revision_id, context: _ReadContext) -> tuple[object, ...]:
    assert field.collection is not None
    collection_slot = context.collection_slots_by_name[field.collection]
    return (
        CustomImportChildScalar.child_revision_id == child_revision_id,
        CustomImportChildScalar.dataset_id == context.target.dataset_id,
        CustomImportChildScalar.schema_revision_id == context.target.schema_revision_id,
        CustomImportChildScalar.root_record_id == CustomImportFamilyRevision.root_record_id,
        CustomImportChildScalar.collection_slot == collection_slot,
        CustomImportChildScalar.field_slot == field.field_slot,
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
            CustomImportFamilyRevision.root_record_id.asc(),
            CustomImportWinner.entity_binding_id.asc(),
            CustomImportWinner.context_key_sha256.asc(),
        )
    )
    return tuple(expressions)


def _order_scalar_expression(field: Field, context: _ReadContext):
    if field.collection is None:
        scalar_model = CustomImportRootScalar
        conditions = _root_scalar_conditions(field, CustomImportFamilyRevision.root_revision_id, context)
    else:
        scalar_model = CustomImportChildScalar
        conditions = _child_scalar_conditions(field, CustomImportWinner.context_child_revision_id, context)
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
    if not fields:
        return {}
    rows = (
        (
            await session.execute(
                select(CustomImportRootScalar).where(
                    CustomImportRootScalar.dataset_id == context.target.dataset_id,
                    CustomImportRootScalar.schema_revision_id == context.target.schema_revision_id,
                    CustomImportRootScalar.root_revision_id.in_(root_revision_ids),
                    CustomImportRootScalar.field_slot.in_(tuple(field.field_slot for field in fields)),
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
    if not fields or not child_revision_ids:
        return {}
    rows = (
        (
            await session.execute(
                select(CustomImportChildScalar).where(
                    CustomImportChildScalar.dataset_id == context.target.dataset_id,
                    CustomImportChildScalar.schema_revision_id == context.target.schema_revision_id,
                    CustomImportChildScalar.child_revision_id.in_(child_revision_ids),
                    CustomImportChildScalar.field_slot.in_(tuple(field.field_slot for field in fields)),
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
    statement = _winner_statement(context).where(
        CustomImportWinner.entity_binding_id == locator.entity_binding_id,
        CustomImportWinner.family_revision_id == locator.family_revision_id,
        CustomImportFamilyRevision.root_record_id == locator.root_record_id,
        CustomImportWinner.context_key_sha256 == locator.context_key_sha256,
    )
    row = (await session.execute(statement)).one_or_none()
    if row is None:
        raise CustomImportReadUnavailableError("selected winner is not eligible for root detail")
    return _winner_row(row, context.profile_context_slot > 0)


async def _hydrate_root_detail(
    session: AsyncSession,
    context: _ReadContext,
    selected_row: tuple[
        CustomImportWinner, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportChildRevision | None
    ],
    scope_digest: str,
) -> RootDetail:
    winner, family, root_revision, _context_child = selected_row
    if family.child_count > MAX_DETAIL_CHILDREN:
        raise CustomImportReadUnavailableError("selected family exceeds the bounded detail child limit")
    root_fields = _detail_root_fields(context.definition)
    root_rows_by_key = await _root_scalar_rows(session, context, (root_revision.root_revision_id,), root_fields)
    child_rows = await _family_child_rows(session, context, family)
    if len(child_rows) != family.child_count:
        raise CustomImportReadUnavailableError("selected family child membership is incomplete")
    child_fields_by_slot = _detail_child_fields_by_slot(context)
    projected_child_fields = tuple(field for fields in child_fields_by_slot.values() for field in fields)
    child_scalar_rows = await _child_scalar_rows(
        session,
        context,
        tuple(child.child_revision_id for _, child in child_rows),
        projected_child_fields,
    )
    family_child_records = tuple(
        _detail_child(family_child, child, context, child_fields_by_slot, child_scalar_rows)
        for family_child, child in child_rows
    )
    return RootDetail(
        target=context.target,
        winner=_winner_locator(winner, family),
        root_fields=_project_field_values(root_fields, root_revision.root_revision_id, root_rows_by_key),
        children=family_child_records,
        authorization_scope_sha256=scope_digest,
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
    family: CustomImportFamilyRevision,
) -> tuple[tuple[CustomImportFamilyChild, CustomImportChildRevision], ...]:
    if family.child_count == 0:
        return ()
    statement = (
        select(CustomImportFamilyChild, CustomImportChildRevision)
        .select_from(CustomImportFamilyChild)
        .join(
            CustomImportChildRevision,
            and_(
                CustomImportChildRevision.child_revision_id == CustomImportFamilyChild.child_revision_id,
                CustomImportChildRevision.dataset_id == CustomImportFamilyChild.dataset_id,
                CustomImportChildRevision.schema_revision_id == CustomImportFamilyChild.schema_revision_id,
                CustomImportChildRevision.root_record_id == CustomImportFamilyChild.root_record_id,
                CustomImportChildRevision.collection_slot == CustomImportFamilyChild.collection_slot,
            ),
        )
        .where(
            CustomImportFamilyChild.family_revision_id == family.family_revision_id,
            CustomImportFamilyChild.dataset_id == context.target.dataset_id,
            CustomImportFamilyChild.schema_revision_id == context.target.schema_revision_id,
            CustomImportFamilyChild.root_record_id == family.root_record_id,
        )
        .order_by(
            CustomImportFamilyChild.collection_slot,
            CustomImportChildRevision.source_ordinal,
            CustomImportChildRevision.child_revision_id,
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
    "CustomImportReadError",
    "CustomImportReadRequestError",
    "CustomImportReadService",
    "CustomImportReadUnavailableError",
    "DEFAULT_READ_TIMEOUT_MS",
    "ExtensionReadAuthorization",
    "ExtensionReadAuthorizer",
    "ExtensionReadScope",
    "MAX_CURSOR_TTL_SECONDS",
    "MAX_DETAIL_CHILDREN",
    "MAX_FILTER_TERMS",
    "MAX_ORDER_TERMS",
    "MAX_PAGE_SIZE",
    "MAX_READ_TIMEOUT_MS",
    "PinnedReadTarget",
    "READ_CORE_CONTRACT",
    "ReadChild",
    "ReadCursorCodec",
    "ReadCursorState",
    "ReadFieldValue",
    "ReadFilter",
    "ReadOrderTerm",
    "RootDetail",
    "SearchItem",
    "SearchPage",
    "SearchRequest",
    "WinnerLocator",
)
