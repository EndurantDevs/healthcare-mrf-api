# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish ordinary address overlays from one issued immutable REST Source."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from uuid import uuid4

from process import provider_directory_generic_source_policy as source
from process import provider_directory_profile_source_inputs as inputs
from process import provider_directory_published_replacement_inputs as serving_inputs
from process import provider_directory_published_source_replacement as replacement
from process import provider_directory_serving_recovery as recovery
from process import vsp_directory_integration as vsp_directory
from process.provider_directory_artifact_scratch_inputs import (
    _loading_projection,
    _repair_loading_rows,
    _source_pages,
    _verify_set_parity,
    typed_input_projection,
)
from process.provider_directory_cms_preparation import desired_fence_hash
from process.provider_directory_dataset_rehydrate_pages import READ_MAX_ROWS, projected_pages
from process.provider_directory_output_snapshot_catalog import require_output
from process.provider_directory_output_snapshot_stage import _record_batches
from process.provider_directory_profile_uhc_flex import (
    ROOTED_COMBINED_VARIANT,
    is_uhc_flex_dataset_variant_matching,
    uhc_flex_profile_source_variant,
)
from process.provider_directory_projection_stage import _copy_driver

_ACTIVE = ContextVar("provider_directory_rest_artifact_admission", default=None)
_TERMINAL = ContextVar("provider_directory_rest_artifact_terminal", default=None)
_SOURCE_ONLY = ContextVar("provider_directory_rest_source_publication", default=None)
_FOLLOWUP_PREFIX = "provider_directory_dataset_followup_"
_FOLLOWUP_IDENTITIES = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "parent_run_id")


def _supports_overlay_source(directory, source_id, dataset_id):
    return source_id not in {"cms-npd", directory.UHC_RETAINED_SOURCE_ID} and (
        not directory.is_uhc_flex_profile_source(source_id)
        or uhc_flex_profile_source_variant(source_id) == ROOTED_COMBINED_VARIANT
        and is_uhc_flex_dataset_variant_matching(source_id, dataset_id)
    )


async def prepare_job_tables(directory, ctx, profile_execution, *, overlay_followup=None):
    """Keep acquisition setup separate from reads of an already published dataset."""
    if ctx["context"].get("provider_directory_tables_ready"):
        return
    if overlay_followup is not None and not source.is_source_storage_required():
        # The publication fence validates its inputs without acquisition-table privileges.
        return
    if profile_execution is not None and not source.is_source_storage_required():
        for model in (*directory.SOURCE_MODELS, *directory.CANONICAL_RESOURCE_MODELS):
            columns = ",".join(directory._q(str(column.name)) for column in model.__table__.columns)
            await directory.db.all(
                f"SELECT {columns} FROM {directory._qt(directory._schema(), model.__tablename__)} LIMIT 0"
            )
        # These reads never mark acquisition preflight complete.
        return
    await directory._ensure_provider_directory_tables()
    ctx["context"]["provider_directory_tables_ready"] = True


def ordinary_overlay_followup(directory, task):
    """Recognize only the closed ordinary overlay request, without granting database authority."""
    if source.is_source_storage_required() or task.get(_FOLLOWUP_PREFIX + "stage") != "address_overlay":
        return None
    markers_by_field = {
        name: task.get(_FOLLOWUP_PREFIX + name) for name in (*_FOLLOWUP_IDENTITIES, "version", "stage", "attempt")
    }
    flags_by_name = {
        "publish_artifacts_only": True,
        "publish_corroboration": False,
        "probe": False,
        "import_resources": False,
    }
    expected_fields = {*flags_by_name, "publish_artifacts_targets", "source_ids"} | {
        _FOLLOWUP_PREFIX + name for name in markers_by_field
    }
    require_output(
        set(task) - {"test_mode", "run_id", recovery.GENERATION_PARAM} == expected_fields
        and task.get("test_mode", False) is False
        and (
            "run_id" not in task
            or type(task["run_id"]) is str
            and bool(task["run_id"])
            and task["run_id"] == task["run_id"].strip()
        )
        and all(task.get(name) is enabled for name, enabled in flags_by_name.items())
        and task.get("publish_artifacts_targets") == "address_overlay"
        and all(
            type(markers_by_field[name]) is str
            and bool(markers_by_field[name])
            and markers_by_field[name] == markers_by_field[name].strip()
            for name in _FOLLOWUP_IDENTITIES
        )
        and len(markers_by_field["dataset_hash"]) == 64
        and set(markers_by_field["dataset_hash"]) <= set("0123456789abcdef")
        and type(markers_by_field["version"]) is int
        and markers_by_field["version"] == 1
        and type(markers_by_field["attempt"]) is int
        and markers_by_field["attempt"] > 0
        and task.get("source_ids") == [markers_by_field["source_id"]]
        and _supports_overlay_source(directory, markers_by_field["source_id"], markers_by_field["dataset_id"]),
        "ordinary_overlay_request_invalid",
    )
    if recovery.GENERATION_PARAM in task:
        require_output(
            task[recovery.GENERATION_PARAM]
            == recovery.publication_generation({name: markers_by_field[name] for name in _FOLLOWUP_IDENTITIES}),
            "ordinary_overlay_request_invalid",
        )
    return {_FOLLOWUP_PREFIX + name: marker for name, marker in markers_by_field.items()}


def require_overlay_followup_fence(directory, fence, metrics):
    """Bind ordinary followup markers to the current selection before any staging."""
    if _FOLLOWUP_PREFIX + "version" not in metrics:
        return
    if len(fence.datasets) != 1 or fence.promotion_datasets:
        raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_overlay_followup_selection_changed")
    dataset = fence.datasets[0]
    expected_by_field = {name: metrics.get(_FOLLOWUP_PREFIX + name) for name in _FOLLOWUP_IDENTITIES}
    actual_by_field = dict(
        source_id=dataset.source_id,
        endpoint_id=dataset.endpoint_id,
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        parent_run_id=dataset.evidence_run_id,
    )
    if (
        actual_by_field != expected_by_field
        or dataset.status != "published"
        or dataset.is_current is not True
        or replacement.selected(directory, fence) is None
        and any(
            (alias.serving_endpoint_id or alias.endpoint_id) != alias.endpoint_id
            for alias in fence.locked_alias_datasets
        )
    ):
        raise directory.ProviderDirectoryArtifactBuildStale("provider_directory_overlay_followup_selection_changed")


def validate_source_only_task(directory, task, source_ids, census_request):
    """Keep the new publication contract explicit and independent of ordinary imports."""
    is_publication_requested = task.get("publish_source_dataset_only", False)
    if type(is_publication_requested) is not bool:
        raise ValueError("provider_directory_publish_source_dataset_only_invalid")
    if not is_publication_requested:
        return
    require_output(
        source.is_source_storage_required()
        and len(source_ids) == 1
        and source_ids[0] not in {"cms-npd", directory.UHC_RETAINED_SOURCE_ID}
        and not directory.is_uhc_flex_profile_source(source_ids[0])
        and census_request is None
        and task.get("import_resources") is True
        and task.get("defer_typed_materialization") is True
        and task.get("publish_artifacts") is False
        and task.get("publish_after_acquisition", False) is False
        and task.get("publish_corroboration", False) is False
        and not task.get("publish_artifacts_targets")
        and not any(
            task.get(name)
            for name in (
                "publish_artifacts_only",
                "dataset_followup_only",
                "dataset_rehydrate_only",
                "canonical_backfill_only",
                "contact_backfill_only",
                "require_complete_global_profile_fence",
                "profile_selection_proof_id",
                "probe",
                "seed_only",
                "bulk_export",
                "stale_cleanup",
            )
        ),
        "rest_source_publication_task_invalid",
    )


@contextmanager
def source_publication_request(directory, source_records, enabled):
    """Bind opt-in to the actual sole REST source, original task and issued factory."""
    if not enabled:
        yield
        return
    require_output(
        type(enabled) is bool and _SOURCE_ONLY.get() is None and len(source_records) == 1,
        "rest_source_publication_scope_invalid",
    )
    source_record_by_field = source_records[0]
    require_output(
        source.is_source_storage_required()
        and source_record_by_field.get("source_id") not in {None, "cms-npd", directory.UHC_RETAINED_SOURCE_ID}
        and not directory.is_uhc_flex_profile_source(source_record_by_field["source_id"])
        and not directory._uses_alohr_graphql_connector(source_record_by_field)
        and not directory._uses_uhc_official_file_connector(source_record_by_field)
        and not vsp_directory.is_source(source_record_by_field),
        "rest_source_publication_source_invalid",
    )
    request_lifecycle_by_field = {"active": True}
    token = _SOURCE_ONLY.set(
        (
            directory,
            source._FACTORY.get(),
            asyncio.current_task(),
            source.source_producer_identity(),
            source_record_by_field["source_id"],
            request_lifecycle_by_field,
            getattr(source._FACTORY.get(), "policy", None),
        )
    )
    try:
        yield
    finally:
        request_lifecycle_by_field["active"] = False
        _SOURCE_ONLY.reset(token)


async def import_resources_with_source_session(directory, source_records, rest_page_prefetch, source_only, options):
    """Retain the HTTP-session lifecycle and bind publication only to this request."""
    proxy_run = directory.source_proxy.current_run()
    strict_sources = [
        directory.source_selection.requires_verified_session(directory, record) for record in source_records
    ]
    require_verified_tls = bool(strict_sources) and all(strict_sources)
    force_session = any(strict_sources) or bool(proxy_run is not None and proxy_run.bindings)
    token = directory._REST_PAGE_PREFETCH_OVERRIDE.set(rest_page_prefetch)
    try:
        with source_publication_request(directory, source_records, source_only):
            async with directory._source_http_session_scope(
                force=force_session,
                require_verified_tls=require_verified_tls,
                request_concurrency=max(
                    directory._validated_resource_scan_concurrency(options.get("resource_scan_concurrency", 1)),
                    min(100, max(1, options.get("linked_resource_concurrency", 5)))
                    if options.get("linked_resource_limit", 0) > 0
                    else 1,
                ),
                source_concurrency=max(1, min(len(source_records), options.get("source_concurrency", 1))),
            ):
                return await directory._import_resources(source_records, **options)
    finally:
        directory._REST_PAGE_PREFETCH_OVERRIDE.reset(token)


async def finalize_candidate(directory, candidate, diagnostics):
    """Keep validation intact; only the explicit REST scope publishes its Source."""
    if candidate is None:
        return None
    if directory._is_endpoint_dataset_publishable(candidate, diagnostics):
        result = await directory._validate_endpoint_dataset_candidate(candidate, diagnostics)
        if result.get("validated") is True and _SOURCE_ONLY.get() is not None:
            await publish_source_candidate(directory, candidate)
            return {**result, "published": True, "is_current": True, "status": directory.ENDPOINT_DATASET_PUBLISHED}
        return result
    status = (
        directory.ENDPOINT_DATASET_FAILED
        if any(value.get("error") for value in diagnostics.values())
        else directory.ENDPOINT_DATASET_INCOMPLETE
    )
    await directory._mark_endpoint_dataset_candidate(candidate, status, diagnostics)
    return {"dataset_id": candidate.dataset_id, "status": status, "published": False}


def record_dataset_followup(task, metrics, descriptor):
    """Require genuine current-pointer resolution for the explicit publication task."""
    if descriptor is not None:
        metrics["dataset_followup"] = recovery.publication_descriptor(task, descriptor)
    elif task.get("publish_source_dataset_only") is True:
        raise RuntimeError("provider_directory_rest_source_publication_incomplete")


async def publish_source_candidate(directory, candidate):
    """Publish or resume only this active acquisition's fully validated Source."""
    selected = _SOURCE_ONLY.get()
    if selected is None:
        return
    require_output(
        selected[0] is directory
        and selected[1] is source._FACTORY.get()
        and selected[3] == source.source_producer_identity()
        and not selected[2].done()
        and selected[5]["active"] is True
        and candidate.source_ids == (selected[4],),
        "rest_source_publication_scope_changed",
    )
    continuation = getattr(selected[1], "generic_fhir_capture_continuation", None)
    if not getattr(candidate, "already_published", False):
        from process.provider_directory_source_local_publication import publish_validated_source_local_dataset

        await publish_validated_source_local_dataset(directory, candidate, selected[4])
    if continuation is not None:
        from process.provider_directory_generic_capture_continuation import (
            GenericSourceCaptureContinuation,
            _require_scope,
        )

        require_output(
            type(continuation) is GenericSourceCaptureContinuation, "rest_source_capture_continuation_invalid"
        )
        _require_scope(directory, candidate, selected, continuation)
        await continuation.capture(directory, candidate, selected)


@dataclass(frozen=True)
class _Admission:
    directory: object
    factory: object
    task: object
    fence: object
    fence_hash: str
    resource_types: frozenset
    producer: tuple | None
    prepared: object = None
    recovery: object = None
    replacement_scope: object = None


def current(directory):
    """Require the original task, factory, producer and immutable publication."""
    admission = _ACTIVE.get()
    if admission is None:
        return None
    require_output(
        admission.directory is directory
        and admission.factory is source._FACTORY.get()
        and admission.task is asyncio.current_task()
        and admission.producer == (source.source_producer_identity() if admission.factory is not None else None)
        and admission.fence_hash == desired_fence_hash(admission.fence)
        and admission.replacement_scope is replacement.selected(directory, admission.fence),
        "rest_artifact_admission_changed",
    )
    return admission


def reuse_scope(directory, fence, resource_fence, resource_types):
    """Expose only the complete already prepared ordinary scratch family."""
    admission = current(directory)
    require_output(
        admission is not None
        and desired_fence_hash(fence) == desired_fence_hash(resource_fence or fence) == admission.fence_hash
        and frozenset(resource_types) == admission.resource_types,
        "rest_artifact_scope_changed",
    )
    return fence


async def reuse_artifact_scope(directory, fence, resource_fence, metrics, resource_types):
    """Reuse the exact REST scope or retain the existing admitted CMS contract."""
    if current(directory) is not None:
        return reuse_scope(directory, fence, resource_fence, resource_types)
    from process.provider_directory_artifact_scratch_scope import reuse_artifact_scope as admitted_scope

    return await admitted_scope(directory, fence, resource_fence, metrics, resource_types)


async def publish_bundle(directory, fence, request, *, artifact_resource_types, resource_fence=None):
    """Keep the selected ordinary overlay and its original atomic bundle owner."""
    if (resumed := await recovery.resume_overlay(directory, fence, request)) is not None:
        return resumed
    async with publication_scope(directory, fence, request, artifact_resource_types):
        async with directory._prepare_artifact_bundle_from_fence(
            fence, request, artifact_resource_types=artifact_resource_types, resource_fence=resource_fence
        ) as (bundle, metrics):
            recovery.attach_overlay_stage(directory, bundle)
            promoted = await bundle.promote()
            selected = current(directory)
            if selected is not None and selected.recovery is not None:
                metrics["serving_recovery"] = {
                    "generation": selected.recovery["generation"],
                    "phase": "overlay",
                    "preparation_seconds": selected.recovery["preparation_seconds"],
                }
            await directory._refresh_bundle_profile_delta_metrics(bundle, metrics)
            return await directory._record_artifact_promotion_metrics(fence, metrics, promoted)


@asynccontextmanager
async def prepare_published_replacement(directory, request, *, artifact_resource_types):
    """Expose existing stages to the common owner without an early alias cut."""
    async with replacement.preparation_scope(directory, request.metrics) as fence:
        directory._assert_provider_directory_artifact_target_dependencies(
            fence,
            publish_artifacts_targets=request.publish_artifacts_targets,
            publish_corroboration=request.publish_corroboration,
        )
        await directory._publish_current_dataset_relation_artifacts(
            fence,
            run_id=request.run_id,
            metrics=request.metrics,
            publish_artifacts_targets=request.publish_artifacts_targets,
        )
        serving_snapshot = await serving_inputs.capture(directory, fence)
        async with publication_scope(directory, fence, request, artifact_resource_types):
            dataset = fence.datasets[0]
            pin_by_field = dict(
                source_id=dataset.source_id,
                endpoint_id=dataset.endpoint_id,
                dataset_id=dataset.dataset_id,
                dataset_hash=dataset.dataset_hash,
                parent_run_id=dataset.evidence_run_id,
            )
            from process.provider_directory_published_replacement_journal import generation

            recovery_by_field = dict(
                generation=generation(pin_by_field),
                source=pin_by_field,
                run_id=request.run_id,
                serving_inputs=serving_snapshot,
            )
            async with directory.db.transaction():
                require_output(
                    not await recovery._load_history(
                        directory.db, directory._schema(), recovery_by_field["generation"]
                    ),
                    "rest_artifact_replacement_already_recorded",
                )
            admission = current(directory)
            token = _ACTIVE.set(replace(admission, recovery=recovery_by_field))
            try:
                async with directory._prepare_artifact_bundle_from_fence(
                    fence,
                    request,
                    artifact_resource_types=artifact_resource_types,
                ) as (bundle, metrics):
                    recovery.attach_overlay_stage(directory, bundle)
                    yield fence, bundle, metrics
            finally:
                _ACTIVE.reset(token)


def _replacement_recovery_for_address(directory, fence, bundle, address):
    """Bind the exact coupled bundle and prepared Address input before opening a writer."""
    from process.entity_address_candidate_preparation import PreparedEntityAddressGeneration

    admission = current(directory)
    require_output(
        admission is not None
        and admission.factory is None
        and admission.recovery is not None
        and replacement.selected(directory, fence) is admission.replacement_scope
        and admission.fence is fence
        and directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get() is fence
        and type(address) is PreparedEntityAddressGeneration
        and directory.db._transaction_binding() is None
        and len(address.swaps) == 1
        and address.swaps[0].live_cls.__main_table__ == recovery.receipts.ROOTS[1],
        "rest_artifact_replacement_preparation_required",
    )
    selected = admission.recovery
    require_output(
        selected.get("bundle") is bundle
        and not bundle.promoted
        and len(bundle.stages) == 1
        and bundle.stages[0] is selected["stage"]
        and bundle.profile_delta is None
        and bundle.archive_delta is None,
        "rest_artifact_replacement_bundle_changed",
    )
    serving_snapshot = selected.get("serving_inputs")
    serving_inputs.validate(serving_snapshot, selected["source"])
    require_output(
        address.preparation_input.dataset_pins == serving_inputs.desired_pins(serving_snapshot)
        and address.preparation_input.ordinary_serving_peer_pins == serving_inputs.peer_pins(serving_snapshot)
        and address.preparation_input.overlay_table == selected["stage"].stage_table
        and address.db_schema == directory._schema(),
        "rest_artifact_replacement_address_changed",
    )
    return selected


async def publish_prepared_replacement(directory, fence, bundle, address):
    """Apply the real prepared overlay and Address result before the one owner commits."""
    from process.entity_address_candidate_preparation import (
        publish_prepared_entity_address_generation,
    )
    from process.entity_address_serving_receipt import ordinary_address_receipt_callbacks
    from process.entity_address_unified import _sql_literal
    from process.provider_directory_artifact_bundle_preparation import apply_prepared_artifact_bundle

    selected = _replacement_recovery_for_address(directory, fence, bundle, address)
    context = address.context
    require_output(recovery.GENERATION_PARAM not in context, "rest_artifact_replacement_address_changed")
    context[recovery.GENERATION_PARAM] = selected["generation"]
    context["serving_recovery_run_id"] = selected["run_id"]
    require_output(_TERMINAL.get() is None, "rest_artifact_terminal_changed")
    async with recovery.address_transaction(directory.db, "500ms", _sql_literal, context):
        await recovery.begin_overlay(directory, selected)
        await recovery.prepare_replacement_address(directory.db, directory._schema(), address, context, selected)
        wall = context["serving_recovery_wall"]
        wall.reschedule(min(wall.when(), asyncio.get_running_loop().time() + 2.0))
        await directory.db.status("SET LOCAL statement_timeout='1000ms'")
        await serving_inputs.lock_and_check(directory, selected["serving_inputs"])
        await directory._lock_and_verify_artifact_dataset_fence(fence, directory.db)
        token = _TERMINAL.set((current(directory), _publication_session(directory.db)))
        try:
            await apply_prepared_artifact_bundle(
                directory,
                tuple(bundle.stages),
                cutover_timeout=context["serving_recovery_wall"],
                extra_live_relations=tuple(address.relation_names),
            )
        finally:
            _TERMINAL.reset(token)
        document = await recovery.finish_overlay(directory, selected)
        callbacks = ordinary_address_receipt_callbacks(directory.db, directory._schema(), _sql_literal)
        async with recovery.coupled_address_scope(directory, selected, address, document):
            await publish_prepared_entity_address_generation(address, callbacks=callbacks)
    await address.mark_committed()
    await bundle.mark_promoted()
    return {
        "serving_recovery": {"generation": selected["generation"], "phase": "published"},
        "result_generation": address.native_receipt,
    }


async def replay_published_replacement(directory, metrics):
    """Resolve a completed coupled cut from its journal before recapturing an incumbent."""
    pin = replacement._descriptor_fields(directory, metrics)
    from process import provider_directory_published_replacement_journal as replacement_journal

    generation = replacement_journal.generation(pin)
    async with asyncio.timeout(recovery.receipts.PREPARATION_SECONDS), directory.db.transaction():
        if not await recovery.replacement_journal_present(directory.db, directory._schema()):
            return None
        await directory.db.status("SET LOCAL lock_timeout='500ms'")
        latest = await recovery._resume_state(directory, generation)
    if latest is None:
        return None
    require_output(
        latest["source"] == pin and latest["phase"] == "published", "rest_artifact_replacement_replay_changed"
    )
    return {
        "address_overlay": {
            "published": True,
            "rows": latest["after"]["roots"][recovery.receipts.ROOTS[0]]["rows"],
            "source_ids": [pin["source_id"]],
        },
        "artifact_dataset_ids": [pin["dataset_id"]],
        "artifact_dataset_evidence_run_ids": [pin["parent_run_id"]],
        "serving_recovery": {"generation": generation, "phase": "published"},
        "result_generation": latest["after"]["metadata"]["address"],
    }


async def _catalog(directory, name):
    return await directory.db.first(
        "SELECT c.oid::bigint,c.relowner::bigint,c.relkind::text,c.relpersistence::text "
        "FROM pg_class c WHERE c.oid=to_regclass(:relation)",
        relation=directory._unscoped_qt(directory._schema(), name),
    )


async def _resource_pages(directory, model, dataset, enrichment):
    """Seek native resource keys before expanding one bounded typed page."""
    columns = ",".join(directory._q(str(column.name)) for column in model.__table__.columns)
    projection = directory._artifact_resource_batch_select_sql(model, directory._schema(), dataset=dataset)
    statement = f"WITH projected({columns}) AS ({projection}) SELECT * FROM projected"
    params_by_name = dict(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        evidence_run_id=dataset.evidence_run_id,
        resource_type=directory.RESOURCE_TYPES_BY_MODEL[model],
        after_resource_id=None,
        scope_batch_size=READ_MAX_ROWS,
    )
    while True:
        keys = await directory.db.all(
            f"SELECT p.resource_id FROM ({statement}) p ORDER BY p.resource_id", **params_by_name
        )
        if not keys:
            return
        frontier = keys[-1]._mapping["resource_id"]
        page, _expected, page_params, _kind, _counts = _loading_projection(
            directory, model.__tablename__, statement, params_by_name, enrichment
        )
        async for rows in projected_pages(directory.db, page, page_params, model):
            yield rows
        # Advance by native keys even if the typed projection rejects the whole page.
        params_by_name["after_resource_id"] = frontier


async def _copy(
    directory,
    session,
    model,
    name,
    statement,
    params_by_name,
    *,
    admission,
    kind=None,
):
    """Bound both read transport and native COPY on the ordinary stage owner."""
    from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner

    driver = await _copy_driver(session)
    columns = tuple(str(column.name) for column in model.__table__.columns)
    json_columns = directory._json_columns(model.__table__)
    total = 0
    enrichment = admission.prepared
    if model is directory.ProviderDirectorySource:
        pages = _source_pages(directory, statement, params_by_name["source_ids"])
    elif admission.factory is not None:
        pages = projected_pages(directory.db, statement, params_by_name, model)
    else:
        pages = _resource_pages(directory, model, admission.fence.datasets[0], enrichment)
    try:
        async for resource_rows in pages:
            if kind is not None:
                resource_rows, changed = await _repair_loading_rows(directory, resource_rows, kind, enrichment)
                enrichment["metrics"]["resource_id_npis_backfilled"][directory.RESOURCE_TYPES_BY_MODEL[model]] += (
                    changed
                )
            copy_records = (
                directory._copy_record(resource_row, columns, json_columns) for resource_row in resource_rows
            )
            for batch in _record_batches(copy_records):
                copy_status = await driver.copy_records_to_table(
                    name, schema_name=directory._schema(), columns=columns, records=batch
                )
                require_output(copy_status == f"COPY {len(batch)}", "rest_artifact_copy_incomplete")
                total += len(batch)
    except asyncio.CancelledError, TimeoutError:
        await close_interrupted_copy(driver)
        await invalidate_copy_owner(session, driver)
        raise
    return total


async def _prepare_model(directory, admission, model, name, created_by_relation):
    """Prepare indexes before validating every expected tuple in one private heap."""
    root, schema, dataset = model.__tablename__, directory._schema(), admission.fence.datasets[0]
    async with directory.db.transaction() as session:
        require_output(await _catalog(directory, name) is None, "rest_artifact_stage_exists")
        await directory._create_provider_directory_artifact_scope_layout(
            model, schema, name, status_executor=directory.db.status
        )
        native_identity_values = tuple(await _catalog(directory, name))
        created_by_relation[name] = native_identity_values
        require_output(native_identity_values[2:] == ("r", "u"), "rest_artifact_stage_changed")
        if model is directory.ProviderDirectorySource:
            statement = directory._provider_directory_artifact_source_select_sql(model, schema)
            params_by_name = {"source_ids": [dataset.source_id]}
            expected, kind = statement, None
        else:
            if admission.factory is None:
                statement = directory._provider_directory_artifact_resource_select_sql(model, schema, dataset=dataset)
                params_by_name = dict(
                    source_ids=[dataset.source_id],
                    dataset_ids=[dataset.dataset_id],
                    evidence_run_ids=[dataset.evidence_run_id],
                    resource_type=directory.RESOURCE_TYPES_BY_MODEL[model],
                )
            else:
                statement, params_by_name = typed_input_projection(
                    directory, model, dataset, inputs.binding(directory, dataset)
                )
            statement, expected, params_by_name, kind, _counts = _loading_projection(
                directory, root, statement, params_by_name, admission.prepared
            )
        if (
            model is directory.ProviderDirectorySource
            or directory.RESOURCE_TYPES_BY_MODEL[model] in admission.resource_types
        ):
            await _copy(
                directory,
                session,
                model,
                name,
                statement,
                params_by_name,
                admission=admission,
                kind=kind,
            )
        await directory._build_artifact_scope_pk(model, schema, name, status_executor=directory.db.status)
        if model in {
            directory.ProviderDirectoryPractitionerRole,
            directory.ProviderDirectoryOrganizationAffiliation,
        }:
            _index, sql = directory._provider_directory_profile_bucket_index_sql(schema, name)
            await directory.db.status(sql)
        await directory.db.status(f"ANALYZE {directory._unscoped_qt(schema, name)}")
        if (
            model is directory.ProviderDirectorySource
            or directory.RESOURCE_TYPES_BY_MODEL[model] in admission.resource_types
        ):
            await _verify_set_parity(session, expected, directory._unscoped_qt(schema, name), params_by_name)


async def _prepare(directory, admission, relation_by_table, created_by_relation):
    """Reuse typed encoders, native layouts and whole-snapshot tuple validation."""
    fence, schema = admission.fence, directory._schema()
    admission.prepared.update(
        plan=dict(
            publish_targets=["address_overlay"],
            decision_at=(await directory.db.scalar("SELECT localtimestamp")).isoformat(),
        ),
        metrics=dict(resource_id_npis_backfilled=dict(Organization=0, Practitioner=0)),
        native=None,
    )
    async with inputs.read_transaction(directory):
        projection = await directory._provider_directory_artifact_scope_projection(fence, admission.resource_types)
    directory._assert_provider_directory_artifact_scope_capacity(projection)
    for model in (directory.ProviderDirectorySource, *directory.RESOURCE_MODELS):
        await _prepare_model(directory, admission, model, relation_by_table[model.__tablename__], created_by_relation)
    await directory._assert_provider_directory_artifact_scope_observed_capacity(schema, relation_by_table, projection)
    admission.prepared["relations"] = {
        root: (name, *created_by_relation[name]) for root, name in relation_by_table.items()
    }
    return projection


async def prepared_npi_backfill(directory, db_schema, *, run_id, source_ids, seen_table):
    """Read genuine prepared repairs only after reattesting the complete private family."""
    admission = current(directory)
    if admission is None:
        return None
    require_output(
        (db_schema is None or db_schema == directory._schema())
        and run_id is None
        and seen_table is None
        and (not source_ids or sorted(set(directory._clean_source_id_list(source_ids))) == admission.fence.source_ids),
        "rest_artifact_npi_selector_changed",
    )
    overrides = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    for root, (name, *identity) in admission.prepared["relations"].items():
        require_output(
            overrides.get(root) == name and tuple(await _catalog(directory, name) or ()) == tuple(identity),
            "rest_artifact_stage_changed",
        )
    return dict(admission.prepared["metrics"]["resource_id_npis_backfilled"])


async def _cleanup(directory, created_by_relation):
    """Drop only this invocation's unchanged native relations."""
    for name, expected in reversed(tuple(created_by_relation.items())):
        async with directory.db.transaction():
            observed = await _catalog(directory, name)
            if observed is None:
                continue
            require_output(tuple(observed) == expected, "rest_artifact_cleanup_custody_changed")
            await directory.db.status(f"DROP TABLE {directory._unscoped_qt(directory._schema(), name)}")


async def _drain_cleanup(directory, created_by_relation):
    task = asyncio.create_task(_cleanup(directory, created_by_relation))
    cancellation = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
    task.result()
    if cancellation is not None:
        raise cancellation


def _require_published_overlay(directory, fence, request):
    require_output(
        _ACTIVE.get() is None
        and request.publish_artifacts_targets == {"address_overlay"}
        and not request.publish_corroboration
        and len(fence.datasets) == 1
        and not fence.promotion_datasets
        and _supports_overlay_source(directory, fence.datasets[0].source_id, fence.datasets[0].dataset_id)
        and fence.datasets[0].status == "published"
        and fence.datasets[0].is_current is True
        and (
            replacement.selected(directory, fence) is not None
            or all(
                (alias_dataset.serving_endpoint_id or alias_dataset.endpoint_id) == alias_dataset.endpoint_id
                for alias_dataset in fence.locked_alias_datasets
            )
        ),
        "rest_artifact_published_overlay_required",
    )


def _publication_admission(directory, fence, request, resource_types):
    """Select the existing Source recipe or exact ordinary published-dataset followup."""
    from process.provider_directory_cms_preparation import _ACTIVE as cms_admission

    is_source_required = source.is_source_storage_required()
    is_ordinary_followup = (
        not is_source_required
        and type(request.metrics.get(_FOLLOWUP_PREFIX + "version")) is int
        and request.metrics[_FOLLOWUP_PREFIX + "version"] == 1
        and request.metrics.get(_FOLLOWUP_PREFIX + "stage") == "address_overlay"
    )
    if (
        (not is_source_required and not is_ordinary_followup)
        or cms_admission.get() is not None
        or directory._provider_directory_profile_capacity_admission() is not None
    ):
        require_output(
            request.metrics.get(recovery.GENERATION_PARAM) is None, "rest_artifact_recovery_admission_required"
        )
        return None
    _require_published_overlay(directory, fence, request)
    factory = source._FACTORY.get()
    if is_ordinary_followup:
        require_output(
            request.metrics.get(recovery.GENERATION_PARAM) is None
            or replacement.selected(directory, fence) is not None
            and recovery.overlay_request(fence, request) is not None,
            "rest_artifact_recovery_admission_required",
        )
        require_overlay_followup_fence(directory, fence, request.metrics)
    else:
        require_output(callable(getattr(factory, "artifact_fence", None)), "rest_artifact_fence_required")
    return _Admission(
        directory,
        factory,
        asyncio.current_task(),
        fence,
        desired_fence_hash(fence),
        frozenset(resource_types),
        source.source_producer_identity() if factory is not None else None,
        {},
        None if is_ordinary_followup else recovery.overlay_request(fence, request),
        replacement.selected(directory, fence),
    )


@asynccontextmanager
async def publication_scope(directory, fence, request, resource_types):
    """Prepare the existing recipe and always drain its exact native scratch tables."""
    admission = _publication_admission(directory, fence, request, resource_types)
    if admission is None:
        yield
        return
    factory = admission.factory
    relation_by_table = {
        model.__tablename__: "pdra_" + uuid4().hex
        for model in (directory.ProviderDirectorySource, *directory.RESOURCE_MODELS)
    }
    created_by_relation = {}
    async with inputs.profile_source_input_scope(directory, fence, fence):
        try:
            actor = await directory.db.first("SELECT session_user::text,current_user::text")
            require_output(
                tuple(actor)
                == ((factory.policy.control_runtime_roles[0],) * 2 if factory is not None else (actor[0],) * 2),
                "rest_artifact_ordinary_actor_required",
            )
            projection = await _prepare(directory, admission, relation_by_table, created_by_relation)
            oid_by_table = {root: created_by_relation[name][0] for root, name in relation_by_table.items()}
            token = _ACTIVE.set(admission)
            try:
                with directory._artifact_scope_tokens(fence, fence, relation_by_table, oid_by_table):
                    directory._record_artifact_scope_metrics(request.metrics, projection, [])
                    yield
                    current(directory)
            finally:
                _ACTIVE.reset(token)
        finally:
            await _drain_cleanup(directory, created_by_relation)


def is_fence_held(directory, fence, executor):
    """Recognize only the ordinary transaction retaining the original endpoint locks."""
    held = _TERMINAL.get()
    if held is None or executor not in (None, directory.db):
        return False
    admission = current(directory)
    require_output(
        held[0] is admission
        and held[1] is _publication_session(directory.db)
        and desired_fence_hash(fence) == admission.fence_hash,
        "rest_artifact_terminal_changed",
    )
    return True


def _publication_session(database):
    binding = database._transaction_binding()
    require_output(binding is not None and binding.session.in_transaction(), "rest_artifact_transaction_required")
    return binding.session


async def _lock_ordinary_controls(directory, fence, database):
    """Retain native endpoint and alias locks on the connection that will commit."""
    await directory._lock_artifact_fence_endpoint_advisories(fence, database)
    await directory._lock_artifact_fence_endpoints(fence, database)
    aliases = await directory._lock_artifact_fence_aliases(fence, database)
    directory._assert_locked_artifact_fence_aliases(fence, aliases)


async def _verify_header(directory, fence, executor, *, for_update):
    rows = await directory._artifact_fence_dataset_rows(fence, executor, for_update=for_update)
    eligible_ids = await directory._artifact_eligible_validated_ids(fence, executor)
    directory._assert_locked_artifact_fence_datasets(fence, rows, eligible_ids)
    await directory._assert_reviewed_activation_ready(fence, executor)
    await directory._assert_uhc_flex_profile_fence_ready(fence, executor)


async def assert_address_source_current(address, schema, context):
    """Preserve the original prepublish scope and current-pointer checks."""
    dataset_id = address._clean_optional(context.get("partial_provider_directory_dataset_id"))
    if dataset_id is None:
        return
    source_ids = address._coerce_str_list(context.get("partial_provider_directory_source_ids"))
    root_run_id = address._clean_optional(context.get("partial_provider_directory_run_id"))
    address._validate_provider_directory_dataset_fence_scope(
        dataset_id=dataset_id,
        source_ids=source_ids,
        run_id=root_run_id,
        partial_scope=address._clean_optional(context.get("partial_provider_directory_scope")),
    )
    await address._assert_current_provider_directory_dataset(
        schema,
        source_id=source_ids[0],
        expected_dataset_id=dataset_id,
        expected_root_run_id=root_run_id,
    )


async def lock_address_source_fence(directory, database, schema, context):
    """Keep the requested partial Address source current through its own commit."""
    dataset_id = context.get("partial_provider_directory_dataset_id")
    if dataset_id is None:
        return
    source_ids = context.get("partial_provider_directory_source_ids")
    root_run_id = context.get("partial_provider_directory_run_id")
    require_output(
        schema == directory._schema()
        and directory.db is database
        and type(dataset_id) is str
        and bool(dataset_id)
        and type(source_ids) in (list, tuple)
        and len(source_ids) == 1
        and type(root_run_id) is str
        and bool(root_run_id),
        "address_source_fence_invalid",
    )
    _publication_session(database)
    admission = current(directory)
    if admission is not None and admission.replacement_scope is not None:
        from process import provider_directory_published_replacement_journal as journal

        selected = admission.recovery
        require_output(
            selected is not None
            and (source_ids[0], dataset_id, root_run_id)
            == tuple(selected["source"][field] for field in ("source_id", "dataset_id", "parent_run_id")),
            "address_source_fence_changed",
        )
        await journal.check(directory, selected["source"], selected.get(journal.FIELD))
        return
    fence = await directory._resolve_provider_directory_artifact_datasets(source_ids)
    require_output(
        len(fence.datasets) == 1
        and not fence.promotion_datasets
        and fence.datasets[0].dataset_id == dataset_id
        and fence.datasets[0].evidence_run_id == root_run_id
        and fence.datasets[0].source_id == source_ids[0]
        and fence.datasets[0].status == "published"
        and fence.datasets[0].is_current is True,
        "address_source_fence_changed",
    )
    await _lock_ordinary_controls(directory, fence, database)
    await _verify_header(directory, fence, database, for_update=False)


@asynccontextmanager
async def terminal_fence(directory):
    """Authenticate Source before commit; keep ordinary-owned locks until settlement."""
    admission = current(directory)
    if admission is None or admission.factory is None:
        yield
        return
    require_output(_TERMINAL.get() is None, "rest_artifact_terminal_nested")
    session = _publication_session(directory.db)
    await directory.db.status(f"SET LOCAL lock_timeout='{directory.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT}'")
    await directory.db.status(
        f"SET LOCAL statement_timeout='{directory.PROVIDER_DIRECTORY_ARTIFACT_FENCE_STATEMENT_TIMEOUT}'"
    )
    await _lock_ordinary_controls(directory, admission.fence, directory.db)
    async with admission.factory.artifact_fence(directory.db) as executor:
        await executor.status(f"SET LOCAL lock_timeout='{directory.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT}'")
        await executor.status(
            f"SET LOCAL statement_timeout='{directory.PROVIDER_DIRECTORY_ARTIFACT_FENCE_STATEMENT_TIMEOUT}'"
        )
        await _verify_header(directory, admission.fence, executor, for_update=True)
        await recovery.begin_overlay(directory, admission.recovery)
        token = _TERMINAL.set((admission, session))
        try:
            yield
            await recovery.finish_overlay(directory, admission.recovery)
            current(directory)
            require_output(_publication_session(directory.db) is session, "rest_artifact_transaction_changed")
        finally:
            _TERMINAL.reset(token)


def current_dataset_fence_query(db_schema: str) -> str:
    """Build the immutable publication fence query for one source."""

    return f"""
        SELECT dataset.dataset_id
          FROM {db_schema}.provider_directory_source AS source
          JOIN {db_schema}.provider_directory_endpoint_dataset AS dataset
            ON dataset.dataset_id = :expected_dataset_id
         WHERE source.source_id = :source_id
           AND dataset.acquisition_root_run_id = :expected_root_run_id
           AND dataset.status = 'published'
           AND dataset.is_current IS TRUE
           AND dataset.published_at IS NOT NULL
           AND dataset.superseded_at IS NULL
           AND jsonb_typeof(
                   dataset.publication_metadata_json::jsonb -> 'source_ids'
               ) = 'array'
           AND (
                   dataset.publication_metadata_json::jsonb -> 'source_ids'
               ) @> jsonb_build_array(source.source_id)
           AND NOT EXISTS (
                SELECT 1
                  FROM {db_schema}.provider_directory_endpoint_dataset AS competing
                 WHERE competing.dataset_id <> dataset.dataset_id
                   AND competing.status = 'published'
                   AND competing.is_current IS TRUE
                   AND competing.published_at IS NOT NULL
                   AND competing.superseded_at IS NULL
                   AND jsonb_typeof(
                           competing.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) = 'array'
                   AND (
                           competing.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) @> jsonb_build_array(source.source_id)
           )
         LIMIT 1;
        """
