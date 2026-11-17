# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native batch address keys and component inserts for one overlay stage build."""

from __future__ import annotations

import inspect
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Awaitable, Callable, Iterator

from process.ext import address_fast

LOCATION_KEYED_COMPONENTS = frozenset({"practitioner_role", "organization_affiliation"})
# Without native keys the component templates join this empty relation and keep their SQL expression.
EMPTY_LOCATION_KEYS_SQL = (
    "(SELECT NULL::varchar AS source_id, NULL::varchar AS resource_id, NULL::text AS address_key, "
    "NULL::text AS row_version WHERE FALSE)"
)
# A real key table is only built over the physical Location table, whose xmin it records; the SQL fallback,
# including CMS overlays that read Locations from a CTE without xmin, joins with no version match.
ROW_VERSION_MATCH_SQL_BY_NAME = {
    "loc_key_row_version": "AND keyed.row_version = loc.xmin::text",
    "location_key_row_version": "AND keyed.row_version = location.xmin::text",
}
NO_ROW_VERSION_MATCH_SQL_BY_NAME = dict.fromkeys(ROW_VERSION_MATCH_SQL_BY_NAME, "")

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


@asynccontextmanager
async def staged_location_keys(
    fhir: Any,
    schema: str,
    run_id: str | None,
    source_ids: list[str],
    components: tuple[str, ...],
) -> AsyncIterator[str | None]:
    """Yield a filled key sidecar, or None when the SQL expression must key Locations."""
    from process import provider_directory_address_batch

    relation = provider_directory_address_batch.current()
    if relation is None or not LOCATION_KEYED_COMPONENTS.intersection(components):
        yield None
        return
    # A stage-style name owned by the control run lets the orphan-stage cleanup reap it after a hard crash,
    # also when the build's query scope carries no run id.
    key_table = fhir._address_overlay_stage_table_name(_BUILD_SCOPE.get()[0] or run_id)
    key_ref = fhir._unscoped_qt(schema, key_table)
    started = time.monotonic()
    # row_version is the Location's xmin when keyed; the inserts trust a key only while it still matches.
    await fhir.db.status(
        f"CREATE UNLOGGED TABLE {key_ref} "
        "(source_id varchar NOT NULL, resource_id varchar NOT NULL, address_key text, row_version text NOT NULL);"
    )
    try:
        keyed_count = await provider_directory_address_batch.stage_location_keys(
            fhir, schema, key_table, source_ids, relation
        )
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
