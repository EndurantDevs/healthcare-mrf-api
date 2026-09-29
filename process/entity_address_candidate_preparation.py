# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare exact directory inputs for a caller-owned native address cutover."""

from __future__ import annotations

import asyncio
import importlib
import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from process import entity_address_preparation_admission as admitted
from process import entity_address_prepared_doctors as prepared_doctors

if TYPE_CHECKING:
    from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection


@dataclass(frozen=True)
class ProviderDirectoryAddressDatasetPin:
    source_id: str
    endpoint_id: str
    dataset_id: str
    dataset_hash: str
    acquisition_root_run_id: str


@dataclass(frozen=True)
class ProviderDirectoryAddressPreparationInput:
    """Internal desired inputs; this is never decoded from a worker task."""

    dataset_pins: tuple[ProviderDirectoryAddressDatasetPin, ...]
    overlay_table: str
    overlay_relation_oid: int
    address_alias_generation: int
    relation_overrides: tuple[tuple[str, str], ...] = ()
    semantic_as_of: str | None = None


@dataclass
class PreparedEntityAddressGeneration:
    """Finalized native stages, with ownership retained until the caller commits."""

    db_schema: str
    swaps: list
    patch_statements: list[tuple[str, str]]
    relation_names: list[str]
    required_names: list[str]
    stage_oids: tuple[tuple[str, str, int], ...]
    context: dict
    publish_validation: dict
    preparation_input: ProviderDirectoryAddressPreparationInput
    incumbent_overlay_fence: tuple[int, int] | None
    committed: bool = False
    native_dependencies: prepared_doctors.PreparedAddressDependencies | None = None

    @property
    def native_receipt(self) -> dict | None:
        """Expose the provisional native receipt for the caller's commit recovery."""
        return self.context.get("result_generation")

    async def mark_committed(self) -> None:
        """Release stage ownership only after verifying the outer commit's native receipt."""
        native = _native()
        if native.db._transaction_binding() is not None or self.native_receipt is None:
            raise RuntimeError("entity-address prepared generation has not committed")
        async with native.db.transaction() as session:
            authority = await native.result_generation.read_entity_address_result_generation_authority(
                session, schema_name=self.db_schema
            )
            relation_oids = await native.result_generation._current_relation_oids(native.db, self.db_schema)
            if authority.as_dict() != self.native_receipt or authority.relation_oids != relation_oids:
                raise RuntimeError("entity-address prepared generation has not committed")
        self.committed = True
        self.context["publication_state"] = "published"


_PREPARATION: ContextVar[ProviderDirectoryAddressPreparationInput | None] = ContextVar(
    "entity_address_candidate_preparation", default=None
)
_NATIVE_DEPENDENCIES: ContextVar[prepared_doctors.PreparedAddressDependencies | None] = ContextVar(
    "entity_address_candidate_dependencies", default=None
)


@dataclass(frozen=True)
class ProviderDirectoryAddressSourceQueryInput:
    """Read-only SQL inputs with no staged relation identity or preparation authority."""

    dataset_pins: tuple[ProviderDirectoryAddressDatasetPin, ...]
    relation_overrides: tuple[tuple[str, str], ...]
    semantic_as_of: str
    overlay: DesiredOverlayProjection


_SOURCE_QUERY: ContextVar[ProviderDirectoryAddressSourceQueryInput | None] = ContextVar(
    "entity_address_source_query", default=None
)


def has_source_query() -> bool:
    """Identify SQL-only scope without implying an owned physical preparation."""
    return _SOURCE_QUERY.get() is not None


@contextmanager
def source_query_scope(inputs: ProviderDirectoryAddressSourceQueryInput):
    """Generate source SQL only; this scope never satisfies physical preparation checks."""
    from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection

    if current() is not None or has_source_query() or not isinstance(inputs, ProviderDirectoryAddressSourceQueryInput):
        raise RuntimeError("entity-address source query scope is invalid")
    if not isinstance(inputs.overlay, DesiredOverlayProjection) or not inputs.semantic_as_of:
        raise RuntimeError("entity-address source query requires an exact desired overlay")
    _validate_semantic_date(inputs.semantic_as_of)
    _validate_source_inputs(inputs.dataset_pins, inputs.relation_overrides)
    token = _SOURCE_QUERY.set(inputs)
    try:
        yield
    finally:
        _SOURCE_QUERY.reset(token)


_RELATION_OVERRIDES = frozenset(
    {
        "provider_directory_practitioner",
        "provider_directory_organization",
        "provider_directory_location",
        "provider_directory_practitioner_role",
        "provider_directory_insurance_plan",
        "provider_directory_healthcare_service",
        "provider_directory_organization_affiliation",
        "provider_directory_network_catalog",
        "doctor_clinician_address",
        "address_archive_v2",
    }
)


def _native():
    return importlib.import_module("process.entity_address_unified")


def current() -> ProviderDirectoryAddressPreparationInput | None:
    """Return desired inputs only in the internal preparation task's scope."""
    return _PREPARATION.get()


def geo_dependency_options() -> dict:
    """Pass exact desired native inputs to the existing held-dependency geo projection."""
    dependencies = _NATIVE_DEPENDENCIES.get()
    if dependencies is None:
        return {}
    return {
        "dependency_bindings": prepared_doctors.projection.validate_projection_dependency_bindings(
            dependencies.schema, dependencies.dependency_bindings
        )
    }


def has_prepared_doctors() -> bool:
    """Keep native backend guarding inactive for ordinary canonical-source imports."""
    dependencies = _NATIVE_DEPENDENCIES.get()
    return dependencies is not None and (dependencies.doctors is not None or dependencies.archive is not None)


async def lock_prepared_doctors(database) -> None:
    """Bind each executing backend to the exact immutable source heaps it may read."""
    dependencies = _NATIVE_DEPENDENCIES.get()
    if dependencies is not None and (dependencies.doctors is not None or dependencies.archive is not None):
        await prepared_doctors.lock_prepared_relations(database, dependencies)


def _validate_semantic_date(value: str | None) -> None:
    """Require the exact canonical date carried by the desired serving selection."""
    if value is not None and (
        not isinstance(value, str)
        or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)
        or date.fromisoformat(value).isoformat() != value
    ):
        raise ValueError("entity-address semantic date is invalid")


def semantic_now_sql() -> str:
    """Use midnight UTC for semantic address timestamps; ordinary imports retain NOW()."""
    inputs = _SOURCE_QUERY.get() or current()
    semantic_date = inputs.semantic_as_of if inputs is not None else None
    _validate_semantic_date(semantic_date)
    return f"TIMESTAMP '{semantic_date} 00:00:00'" if semantic_date is not None else "NOW()"


def overlay_updated_at_sql() -> str:
    """Exclude a new overlay's publication clock from pinned source freshness."""
    inputs = _SOURCE_QUERY.get() or current()
    timestamp_sources = "overlay.source_updated_at"
    if inputs is None or inputs.semantic_as_of is None:
        timestamp_sources += ", overlay.published_at"
    return f"COALESCE({timestamp_sources}, {semantic_now_sql()})::timestamp"


def source_observed_at_sql(expression: str) -> str:
    """Interpret pinned address timestamps as UTC when writing timestamped evidence."""
    inputs = _SOURCE_QUERY.get() or current()
    if inputs is None or inputs.semantic_as_of is None:
        return f"{expression}::timestamptz"
    return f"({expression} AT TIME ZONE 'UTC')"


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value):
        raise ValueError("entity-address preparation relation is invalid")
    return value


def _validate_pin(pin: ProviderDirectoryAddressDatasetPin) -> None:
    if not isinstance(pin, ProviderDirectoryAddressDatasetPin):
        raise ValueError("entity-address preparation dataset pin is invalid")
    for value in (pin.source_id, pin.endpoint_id, pin.dataset_id, pin.acquisition_root_run_id):
        if not isinstance(value, str) or not value or value != value.strip() or len(value) > 160:
            raise ValueError("entity-address preparation dataset identity is invalid")
    if not isinstance(pin.dataset_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", pin.dataset_hash):
        raise ValueError("entity-address preparation dataset hash is invalid")


def validate_preparation_input(inputs: ProviderDirectoryAddressPreparationInput) -> None:
    """Reject ambiguous source vectors or unsafe relation substitutions."""
    if not isinstance(inputs, ProviderDirectoryAddressPreparationInput):
        raise ValueError("entity-address preparation input is invalid")
    if not isinstance(inputs.dataset_pins, tuple) or not isinstance(inputs.relation_overrides, tuple):
        raise ValueError("entity-address preparation inputs must be immutable")
    _identifier(inputs.overlay_table)
    if inputs.overlay_table == "provider_directory_address_overlay":
        raise ValueError("entity-address preparation requires a staged overlay")
    if type(inputs.overlay_relation_oid) is not int or inputs.overlay_relation_oid <= 0:
        raise ValueError("entity-address preparation overlay OID is invalid")
    if type(inputs.address_alias_generation) is not int or inputs.address_alias_generation < 0:
        raise ValueError("entity-address preparation alias generation is invalid")
    _validate_semantic_date(inputs.semantic_as_of)
    _validate_source_inputs(inputs.dataset_pins, inputs.relation_overrides)


def _validate_source_inputs(dataset_pins, relation_overrides) -> None:
    """Share exact pin and override validation between physical and query-only inputs."""
    if not isinstance(dataset_pins, tuple) or not isinstance(relation_overrides, tuple):
        raise ValueError("entity-address preparation inputs must be immutable")
    sources, endpoints = set(), {}
    for pin in dataset_pins:
        _validate_pin(pin)
        if pin.source_id in sources or endpoints.get(pin.endpoint_id, pin.dataset_id) != pin.dataset_id:
            raise ValueError("entity-address preparation source vector is ambiguous")
        sources.add(pin.source_id)
        endpoints[pin.endpoint_id] = pin.dataset_id
    seen_relations = set()
    for logical, staged in relation_overrides:
        if logical not in _RELATION_OVERRIDES or logical in seen_relations:
            raise ValueError("entity-address preparation relation override is invalid")
        _identifier(staged)
        seen_relations.add(logical)


def table_name(logical: str) -> str:
    """Resolve a staged source name while leaving ordinary worker reads unchanged."""
    inputs = current()
    if inputs is None:
        return logical
    if logical == "provider_directory_address_overlay":
        return inputs.overlay_table
    return dict(inputs.relation_overrides).get(logical, logical)


def source_sql(db_schema: str, statement: str) -> str:
    """Resolve only the closed source-relation set during internal preparation."""
    inputs = _SOURCE_QUERY.get() or current()
    if inputs is None:
        return statement
    for logical, staged in inputs.relation_overrides:
        statement = re.sub(
            rf"\b{re.escape(db_schema)}\.{re.escape(logical)}\b",
            f"{db_schema}.{staged}",
            statement,
        )
    return statement


def _pins_sql(inputs: ProviderDirectoryAddressPreparationInput) -> str:
    quote = _native()._sql_literal
    if not inputs.dataset_pins:
        return "SELECT NULL::varchar, NULL::varchar, NULL::varchar, NULL::varchar, NULL::varchar WHERE FALSE"
    return "VALUES " + ", ".join(
        "("
        + ", ".join(
            quote(value) + "::varchar"
            for value in (pin.source_id, pin.endpoint_id, pin.dataset_id, pin.dataset_hash, pin.acquisition_root_run_id)
        )
        + ")"
        for pin in inputs.dataset_pins
    )


def _desired_datasets_sql(db_schema: str) -> str:
    return f"""SELECT pin.source_id, dataset.endpoint_id, dataset.dataset_id,
        dataset.acquisition_root_run_id AS run_id,
        COALESCE(dataset.published_at, dataset.validated_at) AS published_at
      FROM desired_dataset_pins AS pin
      JOIN {db_schema}.provider_directory_endpoint_dataset AS dataset
        ON dataset.dataset_id = pin.dataset_id AND dataset.endpoint_id = pin.endpoint_id
       AND dataset.dataset_hash = pin.dataset_hash
       AND dataset.acquisition_root_run_id = pin.run_id
     WHERE dataset.validated_at IS NOT NULL AND dataset.superseded_at IS NULL
       AND ((dataset.status = 'validated' AND dataset.is_current IS FALSE AND dataset.published_at IS NULL)
         OR (dataset.status = 'published' AND dataset.is_current IS TRUE AND dataset.published_at IS NOT NULL))
       AND (dataset.publication_metadata_json::jsonb -> 'source_ids') @> jsonb_build_array(pin.source_id)"""


def _pins_cte(inputs: ProviderDirectoryAddressPreparationInput) -> str:
    return (
        "desired_dataset_pins(source_id, endpoint_id, dataset_id, dataset_hash, run_id) AS (" + _pins_sql(inputs) + ")"
    )


async def assert_dataset_pins(db_schema: str, inputs: ProviderDirectoryAddressPreparationInput) -> None:
    """Require every desired dataset to retain its exact admitted content identity."""
    matched = await _native().db.scalar(
        f"WITH {_pins_cte(inputs)} SELECT count(*) FROM ({_desired_datasets_sql(db_schema)}) AS selected"
    )
    if matched != len(inputs.dataset_pins):
        raise RuntimeError("entity-address preparation dataset pins changed")


async def assert_selected_dataset(db_schema: str, source_id: str, dataset_id: str, run_id: str) -> None:
    """Verify a requested source within the closed desired vector, without requiring publication."""
    inputs = current()
    if inputs is None or not any(
        (pin.source_id, pin.dataset_id, pin.acquisition_root_run_id) == (source_id, dataset_id, run_id)
        for pin in inputs.dataset_pins
    ):
        raise RuntimeError("entity-address preparation selected dataset is not pinned")
    await assert_dataset_pins(db_schema, inputs)


def desired_overlay_ctes(db_schema: str, *, source_ids=None, run_id=None, affected_group_table=None) -> str:
    """Use exact desired rows without changing the public current-dataset selector."""
    native, inputs = _native(), _SOURCE_QUERY.get() or current()
    overlay_ref = f"({inputs.overlay.statement_sql})" if has_source_query() else f"{db_schema}.{inputs.overlay_table}"
    source_filter = f"WHERE source_id = ANY({native._string_array_literal(list(source_ids))})" if source_ids else ""
    run_filter = f"AND overlay.last_seen_run_id = {native._sql_literal(run_id)}" if run_id else ""
    affected_filter = (
        f"AND EXISTS (SELECT 1 FROM {db_schema}.{affected_group_table} affected WHERE affected.entity_npi = overlay.npi)"
        if affected_group_table
        else ""
    )
    return f"""WITH {_pins_cte(inputs)}, desired_datasets AS MATERIALIZED (
        {_desired_datasets_sql(db_schema)}
    ), requested_sources AS MATERIALIZED (
        SELECT source_id, endpoint_id FROM desired_dataset_pins {source_filter}
    ), endpoint_aliases AS MATERIALIZED (
        SELECT pin.source_id, pin.endpoint_id FROM desired_dataset_pins pin
         WHERE pin.endpoint_id IN (SELECT endpoint_id FROM requested_sources)
    ), current_datasets AS MATERIALIZED (
        SELECT DISTINCT endpoint_id, dataset_id, run_id, published_at FROM desired_datasets
    ), current_overlay AS MATERIALIZED (
        SELECT overlay.*, dataset.dataset_id, dataset.run_id AS dataset_run_id,
               dataset.published_at AS dataset_published_at
          FROM {overlay_ref} AS overlay
          JOIN endpoint_aliases alias ON alias.source_id = overlay.source_id
          JOIN current_datasets dataset ON dataset.endpoint_id = alias.endpoint_id
         WHERE overlay.last_seen_run_id = dataset.run_id {run_filter} {affected_filter}
           AND EXISTS (SELECT 1 FROM {db_schema}.provider_directory_dataset_resource resource
                WHERE resource.dataset_id = dataset.dataset_id
                  AND resource.resource_type = overlay.resource_type AND resource.resource_id = overlay.resource_id)
    )"""


async def _assert_staged_overlay(db_schema: str, inputs: ProviderDirectoryAddressPreparationInput) -> None:
    native = _native()
    if await native._address_alias_generation(db_schema) != inputs.address_alias_generation:
        raise RuntimeError("entity-address preparation address aliases changed")
    desired_oid = await native.db.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{db_schema}.{inputs.overlay_table}"
    )
    if desired_oid != inputs.overlay_relation_oid:
        raise RuntimeError("entity-address preparation overlay changed")


async def capture_overlay_fence(db_schema: str, context: dict) -> None:
    """Keep the incumbent fence separate from the desired overlay's future canonical OID."""
    native, inputs = _native(), current()
    await assert_dataset_pins(db_schema, inputs)
    await _assert_staged_overlay(db_schema, inputs)
    incumbent_oid = await native.db.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{db_schema}.provider_directory_address_overlay"
    )
    context["incumbent_provider_directory_overlay_fence"] = (
        await native._provider_directory_overlay_alias_fence(db_schema) if incumbent_oid is not None else None
    )
    context["provider_directory_overlay_alias_generation"] = inputs.address_alias_generation
    context["provider_directory_overlay_relation_oid"] = inputs.overlay_relation_oid


def scope_index_sql(statement: str) -> str:
    """Check the staged overlay's required index definition independently of its temporary name."""
    inputs = current()
    if inputs is None:
        return statement
    return statement.replace(
        "table_meta.relname = 'provider_directory_address_overlay'",
        f"table_meta.relname = {_native()._sql_literal(inputs.overlay_table)}",
    ).replace(
        f"AND index_relation.relname = {_native()._sql_literal(_native().PROVIDER_DIRECTORY_PARTIAL_SCOPE_INDEX)}", ""
    )


async def _require_stage_indexes(db_schema: str, stage_cls) -> None:
    native = _native()
    index_names = [
        native._stage_index_name(stage_cls.__tablename__, index.get("name", "_".join(index["index_elements"])))
        for index in getattr(stage_cls, "__my_additional_indexes__", ())
    ]
    if not index_names:
        return
    valid_count = await native.db.scalar(
        "SELECT count(*) FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid "
        "WHERE i.indrelid=to_regclass(:relation) AND idx.relname=ANY(CAST(:names AS text[])) "
        "AND i.indisvalid AND i.indisready AND i.indislive",
        relation=f"{db_schema}.{stage_cls.__tablename__}",
        names=index_names,
    )
    if valid_count != len(index_names):
        raise RuntimeError("entity-address preparation requires all serving indexes")


def _cutover_plan(db_schema, stage_cls, support_stage_class_map, context):
    """Reuse the native replacement plan for admission and final prepared authority."""
    return _native()._entity_address_cutover_plan(
        db_schema,
        stage_cls,
        support_stage_class_map,
        partial_support_patch=False,
        affected_group_table="",
        context=context,
    )


async def _capture_stage_oids(db_schema, swaps):
    """Capture every target and physical stage before the first logging operation."""
    stage_oids = []
    for swap in swaps:
        stage = swap.stage_cls.__tablename__
        relation_oid = await _native().db.scalar(
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:relation) AND relkind='r'",
            relation=f"{db_schema}.{stage}",
        )
        if relation_oid is not None:
            stage_oids.append((swap.live_cls.__main_table__, stage, int(relation_oid)))
    return tuple(stage_oids)


async def _register_admitted_stages(ctx):
    """Bind the completed native build to its exact admission before geo compaction logs it."""
    scope = admitted._ADMISSION.get()
    if scope is None:
        return
    native = _native()
    if scope.db_schema is None:
        raise RuntimeError("entity-address admitted build has no owned stages")
    context = ctx.get("context") or {}
    stage_cls = native.make_class(native.EntityAddressUnified, ctx["import_date"])
    support_classes = {} if context.get("serving_only_refresh") else native._support_stage_classes(ctx["import_date"])
    plan = _cutover_plan(scope.db_schema, stage_cls, support_classes, context)
    scope.stage_oids = await _capture_stage_oids(scope.db_schema, plan[0])
    if len(scope.stage_oids) != len(plan[0]):
        raise RuntimeError("entity-address prepared stage is missing")
    if set(scope.owned_oids) != {name for _target, name, _oid in scope.stage_oids} or any(
        scope.owned_oids.get(name) != oid for _target, name, oid in scope.stage_oids
    ):
        raise RuntimeError("entity-address prepared stage was not created by this build")
    fhir = importlib.import_module("process.provider_directory_fhir")
    await scope.admission.register_address_stages(
        fhir, scope.db_schema, scope.stage_oids, input_hash=scope.native_input_hash
    )


async def prepare_finalized_generation(db_schema, stage_cls, support_stage_class_map, *, context):
    """Prepare persistence and exact native swap identity after full finalization."""
    native, inputs = _native(), current()
    if inputs is None or context.get("publish_validation_deferred"):
        raise RuntimeError("entity-address candidate preparation is incomplete")
    await assert_dataset_pins(db_schema, inputs)
    await _assert_staged_overlay(db_schema, inputs)
    plan = _cutover_plan(db_schema, stage_cls, support_stage_class_map, context)
    stage_oids = await _capture_stage_oids(db_schema, plan[0])
    if len(stage_oids) != len(plan[0]):
        raise RuntimeError("entity-address prepared stage is missing")
    scope = admitted._ADMISSION.get()
    if scope is not None and (db_schema != scope.db_schema or stage_oids != scope.stage_oids):
        raise RuntimeError("entity-address prepared stages changed after admission")
    for swap in plan[0]:
        await _require_stage_indexes(db_schema, swap.stage_cls)
        await native._ensure_promoted_stage_logged(db_schema, swap.stage_cls.__tablename__)
        await native._run_sql_phase(
            f"ANALYZE {db_schema}.{swap.stage_cls.__tablename__};",
            context=context,
            phase="entity-address-unified analyzing prepared table",
        )
    context.update(stage_persistence="p", result_generation_mode="ordinary", publication_state="prepared")
    return PreparedEntityAddressGeneration(
        db_schema,
        *plan,
        stage_oids,
        context,
        context["publish_validation"],
        inputs,
        context.get("incumbent_provider_directory_overlay_fence"),
        native_dependencies=_NATIVE_DEPENDENCIES.get(),
    )


def _assert_archive_admission(archive, scope):
    """Require the prepared archive's exact original native reservation before dependency capture."""
    if archive is not None and (
        scope is None
        or getattr(archive, "admission", None) is not scope.admission
        or getattr(archive, "native_input_hash", None) != scope.native_input_hash
    ):
        raise RuntimeError("entity-address archive differs from its admission")


async def prepare_provider_directory_entity_address(
    ctx,
    task,
    *,
    preparation_input,
    admission=None,
    native_input_hash=None,
    doctors=None,
    dependency_bindings=None,
    archive=None,
):
    """Build and fully validate a native replacement without publishing any table."""
    if has_source_query():
        raise RuntimeError("entity-address source query cannot prepare stages")
    validate_preparation_input(preparation_input)
    native = _native()
    if native.db._transaction_binding() is not None:
        raise RuntimeError("entity-address preparation must precede the cutover transaction")
    scope = admitted._admitted_preparation(admission, native_input_hash)
    task_options_by_name = {**task, "publish": True, "skip_publish": False}
    if scope is not None:
        admitted.assert_full_recipe(ctx, task_options_by_name)
        if preparation_input.semantic_as_of != scope.admission.plan.desired_profile_as_of:
            raise RuntimeError("entity-address semantic date differs from its admission")
    if doctors is not None and scope is None:
        admitted.assert_full_recipe(ctx, task_options_by_name)
    _assert_archive_admission(archive, scope)
    dependencies = await prepared_doctors.capture_dependencies(
        native.db,
        os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
        preparation_input.relation_overrides,
        doctors=doctors,
        dependency_bindings=dependency_bindings,
        archive=archive,
    )
    token = _PREPARATION.set(preparation_input)
    admission_token = admitted._ADMISSION.set(scope)
    dependency_token = _NATIVE_DEPENDENCIES.set(dependencies)
    try:
        await native.process_entity_address_unified_data(ctx, task_options_by_name)
        await _register_admitted_stages(ctx)
        prepared = await native.publish_entity_address_unified_generation(ctx, prepare_only=True)
        if not isinstance(prepared, PreparedEntityAddressGeneration):
            raise RuntimeError("entity-address candidate finalization did not prepare a generation")
        await prepared_doctors.assert_prepared_dependencies(native.db, dependencies)
        return prepared
    except BaseException as error:
        if scope is not None and scope.owned_oids:
            try:
                await _drain_owned_stages(scope.db_schema, scope.cleanup_oids)
            except BaseException as cleanup_error:
                raise BaseExceptionGroup(
                    "entity-address preparation and cleanup failed", [error, cleanup_error]
                ) from error
        raise
    finally:
        _NATIVE_DEPENDENCIES.reset(dependency_token)
        admitted._ADMISSION.reset(admission_token)
        _PREPARATION.reset(token)


async def _assert_prepared_stages(prepared):
    native = _native()
    for swap, (_target, stage, expected_oid) in zip(prepared.swaps, prepared.stage_oids, strict=True):
        actual_oid = await native.db.scalar(
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:relation) AND relpersistence='p'",
            relation=f"{prepared.db_schema}.{stage}",
        )
        if actual_oid != expected_oid:
            raise RuntimeError("entity-address prepared stage changed")
        await _require_stage_indexes(prepared.db_schema, swap.stage_cls)


async def publish_prepared_entity_address_generation(prepared, *, callbacks=None):
    """Swap the prepared local result in the coordinator's transaction, without committing."""
    native = _native()
    native.require_caller_owned_cutover_transaction(native.db)
    if prepared.committed:
        raise RuntimeError("entity-address prepared generation is already committed")

    async def verify_locked_stages():
        """Check prepared identities after native locks and before the first swap."""
        await assert_dataset_pins(prepared.db_schema, prepared.preparation_input)
        await _assert_prepared_stages(prepared)
        await prepared_doctors.assert_applied_dependencies(native.db, prepared.native_dependencies)
        if callbacks is not None and callbacks.before_cutover is not None:
            await callbacks.before_cutover()
        prepared.context.pop("result_generation", None)

    native_callbacks = native.EntityAddressCutoverCallbacks(
        before_cutover=verify_locked_stages,
        after_publish=callbacks.after_publish if callbacks is not None else None,
    )
    await native._run_entity_address_cutover(
        prepared.db_schema,
        prepared.swaps,
        prepared.patch_statements,
        prepared.relation_names,
        prepared.required_names,
        prepared.context,
        callbacks=native_callbacks,
        require_caller_owned_transaction=True,
    )
    if prepared.native_receipt is None:
        raise RuntimeError("entity-address prepared publication requires native result authority")
    return prepared.native_receipt


async def cleanup_prepared_entity_address_generation(prepared):
    """Finish bounded, identity-checked cleanup before propagating cancellation."""
    native = _native()
    if native.db._transaction_binding() is not None:
        raise RuntimeError("entity-address stage cleanup requires the outer transaction to finish")
    if prepared.committed:
        return
    await _drain_owned_stages(prepared.db_schema, prepared.stage_oids)


async def _drain_owned_stages(db_schema, stage_oids):
    """Wait for the bounded owned cleanup even if the owner task is cancelled."""
    task = asyncio.create_task(_cleanup_stage_family(db_schema, stage_oids))
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    task.result()
    if cancellation is not None:
        raise cancellation


async def _cleanup_stage_family(db_schema, stage_oids):
    """Drop captured names only while the native transaction holds their exclusive locks."""
    token = admitted._ADMISSION.set(None)
    try:
        async with asyncio.timeout(10):
            for _target, stage, expected_oid in stage_oids:
                await _drop_owned_stage(db_schema, stage, expected_oid)
    finally:
        admitted._ADMISSION.reset(token)


async def _drop_owned_stage(db_schema, stage, expected_oid):
    """Recheck ownership after acquiring native locks; preserve a reused stage name."""
    native = _native()
    relation = f"{_identifier(db_schema)}.{_identifier(stage)}"
    async with native.db.transaction():
        await native.db.status("SET LOCAL lock_timeout='500ms'")
        await native.db.status("SET LOCAL statement_timeout='5s'")
        actual_oid = await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=relation)
        if actual_oid != expected_oid:
            return
        await native._acquire_cutover_locks(db_schema, [stage], [stage])
        actual_oid = await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=relation)
        if actual_oid == expected_oid:
            await native.db.status(f"DROP TABLE {relation} RESTRICT")


_PROVIDER_DIRECTORY_CURRENT_OVERLAY_CTES_TEMPLATE = """
WITH requested_sources AS MATERIALIZED (
    SELECT
        source.source_id::varchar AS source_id,
        source.endpoint_id::varchar AS endpoint_id
      FROM {source_ref} AS source
      {requested_source_filter}
), endpoint_aliases AS MATERIALIZED (
    SELECT
        sibling.source_id::varchar AS source_id,
        sibling.endpoint_id::varchar AS endpoint_id
      FROM {source_ref} AS sibling
      JOIN (
            SELECT DISTINCT endpoint_id
              FROM requested_sources
             WHERE endpoint_id IS NOT NULL
      ) AS selected_endpoint
        ON selected_endpoint.endpoint_id = sibling.endpoint_id
), current_endpoint_counts AS MATERIALIZED (
    SELECT dataset.endpoint_id
      FROM {dataset_ref} AS dataset
     WHERE dataset.is_current IS TRUE
  GROUP BY dataset.endpoint_id
    HAVING COUNT(*) = 1
), current_datasets AS MATERIALIZED (
    SELECT
        dataset.endpoint_id::varchar AS endpoint_id,
        dataset.dataset_id::varchar AS dataset_id,
        COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id,
        dataset.published_at
      FROM {dataset_ref} AS dataset
      JOIN current_endpoint_counts AS current_endpoint
        ON current_endpoint.endpoint_id = dataset.endpoint_id
     WHERE dataset.is_current IS TRUE
       AND dataset.status = 'published'
       AND dataset.published_at IS NOT NULL
       AND dataset.superseded_at IS NULL
       AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
), {affected_overlay_ctes}current_overlay AS MATERIALIZED (
    SELECT
        overlay.*,
        dataset.dataset_id,
        dataset.run_id AS dataset_run_id,
        dataset.published_at AS dataset_published_at
      FROM {current_overlay_ref} AS overlay
      JOIN endpoint_aliases AS alias
        ON alias.source_id = overlay.source_id
      JOIN current_datasets AS dataset
        ON dataset.endpoint_id = alias.endpoint_id
     WHERE overlay.last_seen_run_id = dataset.run_id
       {run_filter}
       AND EXISTS (
            SELECT 1
              FROM {dataset_resource_ref} AS dataset_resource
             WHERE dataset_resource.dataset_id = dataset.dataset_id
               AND dataset_resource.resource_type = overlay.resource_type
               AND dataset_resource.resource_id = overlay.resource_id
       )
)
"""

_PROVIDER_DIRECTORY_PARTIAL_OVERLAY_SOURCE_TEMPLATE = """
{current_overlay_ctes}
SELECT
    'npi'::varchar AS entity_type,
    overlay.npi::varchar AS entity_id,
    overlay.npi::bigint AS npi,
    NULL::bigint AS inferred_npi,
    NULL::float8 AS inference_confidence,
    NULL::varchar AS inference_method,
    {entity_name} AS entity_name,
    {entity_subtype} AS entity_subtype,
    'practice'::varchar AS type,
    {taxonomy_array} AS taxonomy_array,
    {plans_network_array} AS plans_network_array,
    {procedures_array} AS procedures_array,
    {medications_array} AS medications_array,
    ARRAY[]::varchar[] AS aca_plan_array,
    ARRAY[]::varchar[] AS aca_network_array,
    ARRAY[]::varchar[] AS ptg_plan_array,
    ARRAY[]::varchar[] AS ptg_source_array,
    ARRAY[]::varchar[] AS group_plan_array,
    '{base_address_version}'::varchar AS base_address_version,
    overlay.first_line::varchar AS first_line,
    overlay.second_line::varchar AS second_line,
    COALESCE(overlay.city_name, '')::varchar AS city_name,
    COALESCE(overlay.state_name, overlay.state_code, '')::varchar AS state_name,
    overlay.postal_code::varchar AS postal_code,
    COALESCE(NULLIF(overlay.country_code, ''), 'US')::varchar AS country_code,
    overlay.telephone_number::varchar AS telephone_number,
    overlay.fax_number::varchar AS fax_number,
    overlay.formatted_address::varchar AS formatted_address,
    overlay.lat::numeric AS lat,
    overlay.long::numeric AS long,
    NULL::date AS date_added,
    NULL::varchar AS place_id,
    overlay.address_key::uuid AS address_key,
    {overlay_updated_at} AS updated_at,
    'provider_directory_fhir'::varchar AS address_source,
    overlay.source_record_id::varchar AS source_record_id
  FROM current_overlay AS overlay
  {npi_join}
  {primary_npi_address_join}
 WHERE overlay.npi BETWEEN 1000000000 AND 9999999999
   AND {address_predicate}
"""
