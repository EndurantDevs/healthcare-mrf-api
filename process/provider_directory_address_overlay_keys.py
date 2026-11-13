# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native batch address keys and component inserts for one overlay stage build."""

from __future__ import annotations

import asyncio
import inspect
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Awaitable, Callable, Iterator

from process.ext import address_fast

LOCATION_KEY_PAGE_ROWS = 50_000
LOCATION_KEY_COLUMNS = ("source_id", "resource_id", "address_key")
LOCATION_KEYED_COMPONENTS = frozenset({"practitioner_role", "organization_affiliation"})
# Without native keys the component templates join this empty relation and keep their SQL expression.
EMPTY_LOCATION_KEYS_SQL = (
    "(SELECT NULL::varchar AS source_id, NULL::varchar AS resource_id, NULL::text AS address_key WHERE FALSE)"
)

StepReporter = Callable[..., Awaitable[None]]
_BUILD_SCOPE: ContextVar[tuple[str | None, StepReporter | None]] = ContextVar(
    "provider_directory_overlay_build_scope", default=(None, None)
)


@contextmanager
def build_scope(owner_run_id: str | None, reporter: StepReporter | None) -> Iterator[None]:
    """Name the control run that owns this build's scratch and receives its step progress."""
    token = _BUILD_SCOPE.set((owner_run_id, reporter))
    try:
        yield
    finally:
        _BUILD_SCOPE.reset(token)


async def report_step(message: str) -> None:
    """Tell the run what the overlay build just finished, when a reporter is attached."""
    reporter = _BUILD_SCOPE.get()[1]
    if reporter is not None:
        await reporter(message=f"Provider Directory address overlay: {message}")


def _native_location_batch(fhir: Any) -> Any | None:
    """Return the native batch canonicalizer for ordinary builds, otherwise None."""
    from process.provider_directory_cms_preparation import _ACTIVE as nonprofile_admission

    # shortcut: admitted CMS and Profile builds account for every relation they create, so they
    # keep SQL keys; give them a native sidecar when their admission can own one.
    if nonprofile_admission.get() is not None or fhir._provider_directory_profile_capacity_admission() is not None:
        return None
    canonicalize = getattr(address_fast._fast_module(), "canonicalize_location_batch", None)
    return canonicalize if inspect.isbuiltin(canonicalize) else None


def _has_regular_whitespace_sql(line_sql: str) -> str:
    """Match a NULL line or one with single inner spaces, no edge spaces and no non-breaking space."""
    return (
        f"COALESCE({line_sql} = regexp_replace(btrim({line_sql}), '\\s+', ' ', 'g') AND {line_sql} !~ U&'\\00A0', TRUE)"
    )


def location_key_page_sql(context: dict[str, str], is_source_scoped: bool) -> str:
    """Select one keyset page of the exact inputs the overlay passes to addr_key_v1."""
    source_scope = "AND loc.source_id = ANY(CAST(:source_ids AS varchar[]))" if is_source_scoped else ""
    return f"""
        SELECT loc.source_id, loc.resource_id, loc.first_line, loc.second_line, loc.city_name,
               COALESCE(NULLIF(loc.state_name, ''), loc.state_code) AS state_name,
               loc.postal_code, {context["location_country_expr"]} AS country_code
          FROM {context["location_table"]} AS loc
         WHERE (loc.source_id, loc.resource_id)
               > (CAST(:after_source_id AS varchar), CAST(:after_resource_id AS varchar))
           AND (loc.address_key ~* '{context["uuid_re"]}') IS NOT TRUE
           -- addr_key_v1 compares a repeated unit before collapsing whitespace, the native parser after;
           -- lines with irregular whitespace stay unkeyed here, so the SQL function keys them.
           AND {_has_regular_whitespace_sql("loc.first_line")}
           AND {_has_regular_whitespace_sql("loc.second_line")}
           {source_scope}
         ORDER BY loc.source_id, loc.resource_id
         LIMIT :page_rows;
    """


async def _stage_location_key_pages(
    fhir: Any,
    schema: str,
    key_table: str,
    source_ids: list[str],
    canonicalize: Any,
) -> int:
    """Key every scoped Location without a stamped key, one native batch per page."""
    page_sql = location_key_page_sql(fhir._address_overlay_sql_context(schema, None, None), bool(source_ids))
    scope_params_by_name = {"source_ids": source_ids} if source_ids else {}
    # The caller's executor reads and writes, so an open transaction still sees its own key table.
    insert_sql = (
        f"INSERT INTO {fhir._unscoped_qt(schema, key_table)} ({', '.join(LOCATION_KEY_COLUMNS)}) "
        "SELECT * FROM unnest(CAST(:key_source_ids AS varchar[]), CAST(:key_resource_ids AS varchar[]), "
        "CAST(:address_keys AS text[]));"
    )
    keyed_count = 0
    cursor = ("", "")
    while True:
        locations = list(
            await fhir.db.all(
                page_sql,
                **scope_params_by_name,
                after_source_id=cursor[0],
                after_resource_id=cursor[1],
                page_rows=LOCATION_KEY_PAGE_ROWS,
            )
        )
        if not locations:
            return keyed_count
        canonical_keys = await asyncio.to_thread(canonicalize, [tuple(location[2:8]) for location in locations])
        if len(canonical_keys) != len(locations):
            raise RuntimeError("provider_directory_address_overlay_native_key_count_mismatch")
        await fhir.db.status(
            insert_sql,
            key_source_ids=[location[0] for location in locations],
            key_resource_ids=[location[1] for location in locations],
            address_keys=[canonical[0] for canonical in canonical_keys],
        )
        keyed_count += len(locations)
        cursor = (locations[-1][0], locations[-1][1])


@asynccontextmanager
async def staged_location_keys(
    fhir: Any,
    schema: str,
    run_id: str | None,
    source_ids: list[str],
    components: tuple[str, ...],
) -> AsyncIterator[str | None]:
    """Yield a filled key sidecar, or None when the SQL expression must key Locations."""
    canonicalize = _native_location_batch(fhir) if LOCATION_KEYED_COMPONENTS.intersection(components) else None
    if canonicalize is None:
        yield None
        return
    # A stage-style name owned by the control run lets the orphan-stage cleanup reap it after a hard crash,
    # also when the build's query scope carries no run id.
    key_table = fhir._address_overlay_stage_table_name(_BUILD_SCOPE.get()[0] or run_id)
    key_ref = fhir._unscoped_qt(schema, key_table)
    started = time.monotonic()
    await fhir.db.status(
        f"CREATE UNLOGGED TABLE {key_ref} (source_id varchar NOT NULL, resource_id varchar NOT NULL, address_key text);"
    )
    try:
        keyed_count = await _stage_location_key_pages(fhir, schema, key_table, source_ids, canonicalize)
        # The affiliation insert probes one Location at a time inside a lateral subquery.
        await fhir.db.status(
            f"CREATE INDEX {fhir._q(fhir._bounded_identifier(key_table + '_key_idx'))} "
            f"ON {key_ref} (source_id, resource_id);"
        )
        await fhir.db.status(f"ANALYZE {key_ref};")
        await report_step(f"keyed locations rows={keyed_count} seconds={time.monotonic() - started:.0f}")
        yield key_table
    finally:
        await fhir._drop_address_overlay_stage_best_effort(key_ref)


async def insert_components(
    fhir: Any,
    schema: str,
    stage_table: str,
    run_id: str | None,
    source_ids: list[str],
    query_param_dict: dict[str, Any],
    selected_components: tuple[str, ...],
) -> tuple[dict[str, int], dict[str, float]]:
    """Insert scoped overlay components independently and record their durations."""
    inserted_rows_by_component: dict[str, int] = {}
    duration_seconds_by_component: dict[str, float] = {}
    async with staged_location_keys(fhir, schema, run_id, source_ids, selected_components) as key_table:
        for component in selected_components:
            component_started = time.monotonic()
            inserted_rows_by_component[component] = fhir._coerce_rowcount(
                await fhir.db.status(
                    fhir._address_overlay_component_insert_sql(
                        schema,
                        stage_table,
                        component=component,
                        run_id=run_id,
                        source_ids=source_ids,
                        location_key_table=key_table,
                    ),
                    **query_param_dict,
                )
            )
            duration_seconds_by_component[component] = round(time.monotonic() - component_started, 3)
            await report_step(
                f"inserted {component} rows={inserted_rows_by_component[component]} "
                f"seconds={duration_seconds_by_component[component]:.0f}",
            )
    return inserted_rows_by_component, duration_seconds_by_component
