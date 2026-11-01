# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reconcile admitted FHIR metadata with bounded, set-based catalog writes."""

import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import asyncpg

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import PROJECTOR_VERSION, SNAPSHOT_COLUMNS
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_writer_closure import writer_role_names
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_fhir_catalog_custody import (
    FHIRCatalogTarget,
    freeze_fhir_catalog_snapshot,
    verify_fhir_catalog_snapshot,
)
from process.registry_fhir_catalog_enrichment import (
    FHIRCatalogEnrichmentContext,
    company_group_rows,
    enrich_fhir_catalog_edges,
)
from process.registry_fhir_catalog_extraction import (
    FHIRCatalogOrganization,
    FHIRCatalogReferenceEdge,
    extract_fhir_catalog_batch,
    iter_fhir_catalog_pages,
)
from process.registry_fhir_catalog_fingerprints import (
    approved_fhir_catalog_fingerprints,
    require_fhir_catalog_membership_reuse,
)
from process.registry_fhir_catalog_grant import (
    build_fhir_catalog_observation_verifier,
    build_fhir_catalog_publication_runtime,
)
from process.registry_fhir_catalog_identity import FHIRNetworkIdentityInput, allocate_fhir_catalog_network_ids
from process.registry_fhir_catalog_permissions import verify_fhir_catalog_preparation_permissions
from process.registry_fhir_catalog_projection import SourceCatalogContext, format_fhir_catalog_projection
from process.registry_fhir_catalog_publication import OPERATION, enqueue_fhir_catalog_publication
from process.registry_fhir_catalog_published_read import fhir_catalog_published_reads, pin_fhir_catalog_published_read
from process.registry_fhir_catalog_reference_rollup import (
    fhir_catalog_reference_workspace,
    staged_fhir_catalog_references,
)
from process.registry_fhir_catalog_reporting import current_fhir_catalog_report
from process.registry_fhir_catalog_retained import (
    pin_retained_fhir_catalog_source,
    resolve_fhir_catalog_retained_sources,
    resolve_granted_fhir_catalog_sources,
    retained_fhir_catalog_read_source,
    retained_fhir_catalog_recipe,
)
from process.registry_fhir_catalog_source_producer import (
    iter_source_owned_catalog_legacy_bindings,
    open_source_owned_catalog_export,
)
from process.registry_fhir_catalog_sources import (
    FHIRCatalogSourceDisposition,
    fhir_catalog_source_generation,
    iter_fhir_catalog_sources,
    parse_fhir_catalog_source_profile,
    require_current_fhir_catalog_sources,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_fhir_catalog_store import (
    TABLE_COLUMNS,
    copy_fhir_catalog_snapshot,
    create_fhir_catalog_candidate,
    create_fhir_catalog_indexes,
    discard_fhir_catalog_candidate,
    replace_fhir_catalog_relationships,
    write_fhir_catalog_rows,
)
from process.registry_fhir_catalog_validation import canonical_json, metadata_generation
from process.registry_publication_consumer import consume_registry_publication_once

PAGE_ROWS = 1000
EDGE_ROWS = 100


def _snapshot(record):
    if record is None:
        return None
    snapshot_by_field = dict(record)
    for key in ("source_vector_json", "custody_json", "accounting_json"):
        if type(snapshot_by_field[key]) is str:
            snapshot_by_field[key] = json.loads(snapshot_by_field[key])
    return snapshot_by_field


async def reconcile_fhir_catalog(config, *, source_export=None):
    """Run one catalog reconciliation and publish only its exact queued request."""
    if config.source_url is None and source_export is None:
        raise ValueError("registry_fhir_catalog_source_reader_required")
    producer_options = {"source_export": source_export} if source_export is not None else {}
    async with AsyncExitStack() as resources:
        pools = []
        for url in (
            config.source_url,
            config.loader_url,
            config.sealer_url,
            config.lifecycle_url,
            config.execution_url,
        ):
            if url is None and not pools:
                pools.append(None)
                continue
            pool = await resources.enter_async_context(
                asyncpg.create_pool(
                    url.set(drivername="postgresql").render_as_string(hide_password=False),
                    min_size=1,
                    max_size=2,
                )
            )
            pools.append(pool)
        source_pool, loader_pool, sealer_pool, lifecycle_pool, execution_pool = pools
        runtime = build_fhir_catalog_publication_runtime(config.grant, source_pool, **producer_options)
        async with loader_pool.acquire() as loader:
            async with loader.transaction(isolation="repeatable_read", readonly=True):
                await verify_fhir_catalog_preparation_permissions(
                    loader,
                    replace(config.consumer, metadata_runtime=runtime, metadata_only=True),
                    loader_role=config.loader_role,
                    sealer_role=config.sealer_url.username,
                    role=config.loader_role,
                )
            lock_key = "registry-fhir-catalog:" + config.grant.control_schema
            locked = await loader.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock_key)
            if not locked:
                raise ValueError("registry_fhir_catalog_sync_already_running")
            try:
                snapshot, previous, accounting = await prepare_fhir_catalog_snapshot(
                    config,
                    loader,
                    source_pool,
                    sealer_pool,
                    **producer_options,
                )
                if snapshot is None:
                    return {"state": "unchanged", **accounting}
                if report := current_fhir_catalog_report():
                    report.publication_started()
                published = await _publish_catalog_snapshot(
                    config, loader, snapshot, previous, runtime, lifecycle_pool, execution_pool
                )
                return {**published, **accounting}
            finally:
                await loader.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key)


async def _publish_catalog_snapshot(config, loader, snapshot, previous, runtime, lifecycle_pool, execution_pool):
    """Queue and consume only the exact sealed catalog candidate."""
    command_by_field = {
        "operation": OPERATION,
        "metadata_generation": snapshot["metadata_generation"],
        "expected_metadata_generation": previous["metadata_generation"] if previous else None,
        "expected_serving_generation": snapshot["network_generation"],
        "expected_approved_revision": snapshot["metadata_approved_revision"],
        "standing_grant_sha256": config.grant.sha256,
    }
    async with loader.transaction(isolation="repeatable_read"):
        request = await enqueue_fhir_catalog_publication(
            loader,
            command_by_field,
            authorize_metadata=runtime.authorize_metadata,
            control_schema=config.grant.control_schema,
        )
    consumer = replace(
        config.consumer, metadata_runtime=runtime, metadata_only=True, only_request_id=UUID(request["request_id"])
    )
    if request["state"] == "completed":
        return {"state": "completed", "request_id": request["request_id"]}
    published = await consume_registry_publication_once(
        lifecycle_pool=lifecycle_pool,
        execution_pool=execution_pool,
        config=consumer,
        authorize=runtime.authorize_metadata,
    )
    if published is None:
        raise ValueError("registry_fhir_catalog_publication_in_progress")
    return published


async def prepare_fhir_catalog_snapshot(config, loader, source_pool, sealer_pool, *, source_export=None):
    """Build and seal one isolated candidate; the current reader pointer stays intact."""
    staged_by_kind = {}
    try:
        options_by_name = {"source_export": source_export} if source_export is not None else {}
        return await _prepare_fhir_catalog_snapshot(
            config, loader, source_pool, sealer_pool, staged_by_kind, **options_by_name
        )
    except BaseException as error:
        if target := staged_by_kind.get("target"):
            try:
                async with loader.transaction():
                    await discard_fhir_catalog_candidate(
                        loader, target, loader_role=config.loader_role, control_schema=config.grant.control_schema
                    )
            except Exception:
                error.add_note("Unsealed catalog candidate cleanup failed: " + target.schema_name)
        raise


@asynccontextmanager
async def _catalog_source_reads(source_pool, sealer_pool, grant, source_export):
    profiles_by_key = {}
    if source_export is not None:
        for profile in grant.source_profiles:
            if profile.published_read_scope is not None:
                if source_pool is None:
                    raise ValueError("registry_fhir_catalog_source_reader_required")
            else:
                profiles_by_key[canonical_json(profile.document())] = profile
        if len(profiles_by_key) > 1:
            raise ValueError("registry_fhir_catalog_source_export_profiles_unsupported")
    async with AsyncExitStack() as reads:
        source_connection, workspace = None, None
        if source_pool is not None:
            source_connection = await reads.enter_async_context(source_pool.acquire())
            workspace = await reads.enter_async_context(fhir_catalog_reference_workspace(source_connection))
            await reads.enter_async_context(source_connection.transaction(isolation="repeatable_read", readonly=True))
            reads.enter_context(fhir_catalog_published_reads(source_connection))
            await source_connection.execute("SET LOCAL search_path = pg_catalog, pg_temp, public")
        elif source_export is None:
            raise ValueError("registry_fhir_catalog_source_reader_required")
        exports_by_profile = {} if source_export is not None else None
        if profiles_by_key:
            registry_connection = await reads.enter_async_context(sealer_pool.acquire())
            await reads.enter_async_context(registry_connection.transaction(isolation="repeatable_read", readonly=True))
            for key, profile in profiles_by_key.items():
                exports_by_profile[key] = await reads.enter_async_context(
                    open_source_owned_catalog_export(source_export, profile, registry_connection)
                )
        yield source_connection, workspace, exports_by_profile


async def _prepare_fhir_catalog_snapshot(
    config, loader, source_pool, sealer_pool, staged_by_kind, *, source_export=None
):
    """Pin source and registry reads, prepare privately, then seal the candidate."""
    started = perf_counter()
    producer_options = {"source_export": source_export} if source_export is not None else {}
    async with _catalog_source_reads(source_pool, sealer_pool, config.grant, source_export) as source_reads:
        export_options = {"exports_by_profile": source_reads[2]} if source_reads[2] is not None else {}
        accepted, unavailable = await _discover_catalog_sources(source_reads[0], config.grant, **export_options)
        if not accepted and any(
            disposition.reason in {"catalog_observation_unavailable", "catalog_observation_timeout"}
            for disposition in unavailable
        ):
            raise ValueError("registry_fhir_catalog_all_source_observations_failed")
        async with loader.transaction(isolation="repeatable_read"):
            head, manifest, previous = await _catalog_registry_head(loader, config.grant)
            retained_sources = await _retained_catalog_sources(loader, config.grant, manifest)
            _report_retained_sources(retained_sources)
            accepted.extend(retained_sources)
            _validate_catalog_sources(accepted, previous)
            snapshot, current_by_generation = await _catalog_snapshot_inputs(
                loader, config.grant, head, manifest, previous, accepted
            )
            if report := current_fhir_catalog_report():
                report.inventory_ready(current_by_generation, previous)
            accounting_by_phase = {"accepted_sources": len(accepted), "unavailable_sources": len(unavailable)}
            if previous and snapshot["metadata_generation"] == previous["metadata_generation"]:
                return None, previous, accounting_by_phase
            if manifest is not None:
                await require_fhir_catalog_membership_reuse(
                    loader, manifest, snapshot, control_schema=config.grant.control_schema
                )
            retained = await _reusable_catalog_snapshot(loader, config.grant, snapshot["metadata_generation"])
            if retained is not None:
                accounting_by_phase["resumed_snapshot"] = True
                return retained, previous, accounting_by_phase
            candidate = uuid4()
            copy_target = FHIRCatalogTarget(
                str(uuid4()), str(uuid4()), str(uuid4()), str(candidate), "fhir_catalog_" + candidate.hex
            )
            staged_by_kind["target"] = copy_target
            snapshot["schema_name"] = copy_target.schema_name
            accounting_by_phase.update(
                await _build_source_catalog_candidate(
                    config, loader, source_reads, copy_target, snapshot, previous, current_by_generation
                )
            )
    await _seal_catalog_snapshot(
        config, copy_target, snapshot, sealer_pool, source_pool=source_pool, **producer_options
    )
    accounting_by_phase["prepare_seconds"] = perf_counter() - started
    return snapshot, previous, accounting_by_phase


def _report_retained_sources(retained_sources):
    if report := current_fhir_catalog_report():
        report.add_sources(
            retained_sources,
            generations=tuple(fhir_catalog_source_generation(source.descriptor) for source in retained_sources),
        )


async def _discover_catalog_sources(source_connection, grant, *, exports_by_profile=None):
    accepted, unavailable = [], []
    for profile in grant.source_profiles:
        report = current_fhir_catalog_report()
        profile_index = report.add_profile(profile) if report else None
        export = exports_by_profile.get(canonical_json(profile.document())) if exports_by_profile is not None else None
        if export is None and source_connection is None:
            raise ValueError("registry_fhir_catalog_source_reader_required")
        pages = (
            export["inventory"]()
            if export is not None
            else iter_fhir_catalog_sources(source_connection, profile, observation_contract=grant.observation_contract)
        )
        async for page in pages:
            if report:
                report.add_sources(
                    page,
                    profile_index=profile_index,
                    generations=tuple(
                        fhir_catalog_source_generation(disposition.descriptor)
                        if disposition.status == "accepted"
                        else None
                        for disposition in page
                    ),
                )
            for disposition in page:
                (accepted if disposition.status == "accepted" else unavailable).append(disposition)
            if len(accepted) > 1000:
                raise ValueError("registry_fhir_catalog_source_vector_bounds")
        if report:
            report.document["profiles"][profile_index]["complete"] = True
    return accepted, unavailable


async def _catalog_registry_head(loader, grant):
    control = storage_identifier(grant.control_schema)
    head = await loader.fetchrow(
        f"SELECT s.generation_id,s.metadata_generation,r.approved_revision FROM {control}.network_serving_control s "
        f"CROSS JOIN {control}.registry_revision_control r WHERE s.id=1 AND r.id=1"
    )
    if head is None:
        raise ValueError("registry_fhir_catalog_registry_unavailable")
    manifest = (
        await resolve_network_serving_manifest(loader, control_schema=grant.control_schema)
        if head["generation_id"] is not None
        else None
    )
    previous = (
        _snapshot(
            await loader.fetchrow(
                f"SELECT {','.join(SNAPSHOT_COLUMNS)} FROM {control}.registry_fhir_catalog_snapshot "
                "WHERE metadata_generation=$1 AND published_at IS NOT NULL",
                head["metadata_generation"],
            )
        )
        if head["metadata_generation"]
        else None
    )
    if previous:
        await verify_fhir_catalog_snapshot(loader, previous)
    return head, manifest, previous


async def _retained_catalog_sources(loader, grant, manifest):
    source_uri_by_endpoint = dict(grant.retained_source_uris)
    published = ()
    if grant.retained_source_recipes and manifest is not None:
        published = await resolve_fhir_catalog_retained_sources(
            loader, manifest, control_schema=grant.control_schema, source_uris=source_uri_by_endpoint
        )
    explicit = await resolve_granted_fhir_catalog_sources(
        loader, grant.retained_sources, source_uris=source_uri_by_endpoint
    )
    published_anchors = {
        (canonical_json(source.descriptor["coordinates"]), source.descriptor["capture_sha256"]) for source in published
    }
    sources_by_generation = {
        fhir_catalog_source_generation(source.descriptor): source
        for source in explicit
        if retained_fhir_catalog_recipe(source.descriptor).source_pin.retained_successor is not None
        or (canonical_json(source.descriptor["coordinates"]), source.descriptor["capture_sha256"])
        not in published_anchors
    }
    sources_by_generation.update((fhir_catalog_source_generation(source.descriptor), source) for source in published)
    return tuple(sources_by_generation.values())


async def resolve_fhir_catalog_projection_generations(connection, snapshot, *, grant, descriptors):
    """Recover active projection order from the pinned manifest and standing grant."""
    manifest = None
    if snapshot["network_generation"] is not None:
        manifest = await resolve_network_serving_manifest(
            connection, generation_id=snapshot["network_generation"], control_schema=grant.control_schema
        )
        if manifest.manifest_sha256 != snapshot["manifest_sha256"]:
            raise ValueError("registry_fhir_catalog_manifest_changed")
    retained = await _retained_catalog_sources(connection, grant, manifest)
    generations = {fhir_catalog_source_generation(source.descriptor) for source in retained}
    generations.update(
        entry["source_generation"] for entry in snapshot["source_vector_json"] if entry["status"] == "current"
    )
    if not generations <= descriptors.keys():
        raise ValueError("registry_fhir_catalog_projection_source_missing")
    return tuple(
        sorted(
            generations,
            key=lambda generation: (descriptors[generation]["profile"] != "retained_normalized", generation),
        )
    )


def _validate_catalog_sources(accepted, previous):
    if previous is None and not accepted:
        raise ValueError("registry_fhir_catalog_no_accepted_sources")
    keys = [
        (
            disposition.descriptor["coordinates"]["dataset_schema"],
            disposition.endpoint_id,
            disposition.descriptor["profile"],
            disposition.descriptor.get("capture_sha256"),
        )
        for disposition in accepted
    ]
    if len(accepted) > 1000 or len(set(keys)) != len(keys):
        raise ValueError("registry_fhir_catalog_duplicate_source_profile")


async def _reusable_catalog_snapshot(loader, grant, generation):
    if generation is None:
        return None
    control = storage_identifier(grant.control_schema)
    retained = _snapshot(
        await loader.fetchrow(
            f"SELECT {','.join(SNAPSHOT_COLUMNS)} FROM {control}.registry_fhir_catalog_snapshot "
            "WHERE metadata_generation=$1",
            generation,
        )
    )
    if retained is not None:
        await verify_fhir_catalog_snapshot(loader, retained)
    return retained


async def _catalog_snapshot_inputs(loader, grant, head, manifest, previous, accepted):
    vector_by_generation = (
        {entry["source_generation"]: {**entry, "status": "last_good"} for entry in previous["source_vector_json"]}
        if previous
        else {}
    )
    # A current native edition supersedes overlapping historical capture facts.
    current_by_generation = {
        fhir_catalog_source_generation(disposition.descriptor): disposition
        for disposition in sorted(
            accepted,
            key=lambda disposition: (
                disposition.descriptor["profile"] != "retained_normalized",
                fhir_catalog_source_generation(disposition.descriptor),
            ),
        )
    }
    for generation, disposition in current_by_generation.items():
        vector_by_generation[generation] = {
            "source_generation": generation,
            "source_id": disposition.descriptor["coordinates"]["source_id"],
            "status": "last_good" if disposition.descriptor["profile"] == "retained_normalized" else "current",
        }
    fingerprints_by_revision = await approved_fhir_catalog_fingerprints(
        loader, (head["approved_revision"],), control_schema=grant.control_schema
    )
    mapping = fingerprints_by_revision[head["approved_revision"]]
    inputs_by_field = {
        "network_generation": manifest.generation_id if manifest is not None else None,
        "manifest_sha256": manifest.manifest_sha256 if manifest is not None else None,
        "metadata_approved_revision": head["approved_revision"],
        "membership_mapping_sha256": mapping["membership"],
        "mapping_sha256": mapping["metadata"],
        "standing_grant_sha256": grant.sha256,
        "source_vector_json": sorted(
            vector_by_generation.values(), key=lambda entry: (entry["source_id"], entry["source_generation"])
        ),
        "projector_version": PROJECTOR_VERSION,
        "as_of": datetime.now(timezone.utc).date(),
    }
    inputs_by_field["metadata_generation"] = metadata_generation(inputs_by_field)
    return inputs_by_field, current_by_generation


async def _build_catalog_candidate(
    config, loader, source_connection, reference_workspace, copy_target, snapshot, previous, current_by_generation
):
    return await _build_source_catalog_candidate(
        config,
        loader,
        (source_connection, reference_workspace, None),
        copy_target,
        snapshot,
        previous,
        current_by_generation,
    )


async def _build_source_catalog_candidate(
    config, loader, source_reads, copy_target, snapshot, previous, current_by_generation
):
    options_by_name = {"loader_role": config.loader_role, "control_schema": config.grant.control_schema}
    await create_fhir_catalog_candidate(loader, copy_target, **options_by_name)
    accounting_by_phase = {}
    if previous:
        accounting_by_phase["retained"] = await copy_fhir_catalog_snapshot(
            loader, previous, copy_target, snapshot["metadata_generation"], **options_by_name
        )
    accounting_by_phase["manual"] = await project_manual_fhir_catalog(loader, copy_target, snapshot, **options_by_name)
    accounting_by_phase["sources"] = await _project_catalog_sources(
        config, loader, source_reads, copy_target, snapshot, previous, current_by_generation
    )
    accounting_by_phase["carried_relationships"] = await refresh_carried_fhir_catalog_relationships(
        loader,
        copy_target,
        snapshot,
        current_generations=tuple(current_by_generation),
        grant=config.grant,
        loader_role=config.loader_role,
    )
    accounting_by_phase["products"] = await project_reviewed_fhir_catalog_products(
        loader,
        copy_target,
        snapshot,
        grant=config.grant,
        loader_role=config.loader_role,
        source_generations=tuple(current_by_generation),
    )
    await loader.execute(
        f"UPDATE {storage_identifier(copy_target.schema_name)}.registry_fhir_catalog_binding "
        "SET document_json=jsonb_set(document_json,'{status}',to_jsonb(CASE WHEN "
        "NOT(document_json->'source_scope' ? 'insurance_plan_id') "
        "AND split_part(document_json->>'evidence_id',':',1)=ANY($1::text[]) "
        "THEN 'current'::text ELSE 'last_good'::text END))",
        [
            key
            for key, disposition in current_by_generation.items()
            if disposition.descriptor["profile"] != "retained_normalized"
        ],
    )
    accounting_by_phase["indexes"] = await create_fhir_catalog_indexes(loader, copy_target, **options_by_name)
    return accounting_by_phase


async def project_reviewed_fhir_catalog_products(
    connection, copy_target, snapshot, *, grant, loader_role, source_generations
):
    """Replace only derived reviewed-product rows after ordinary source projection."""
    from process.registry_fhir_catalog_products import (
        clear_reviewed_fhir_catalog_product_rows,
        iter_reviewed_fhir_catalog_product_rows,
        require_reviewed_product_binding_slots,
    )

    namespace = storage_identifier(copy_target.schema_name)
    binding_relation = namespace + ".registry_fhir_catalog_binding"
    await clear_reviewed_fhir_catalog_product_rows(
        connection,
        binding_relation=binding_relation,
        relationship_relation=namespace + ".registry_fhir_catalog_relationship",
    )
    companies, groups = await _approved_maps(connection, grant.control_schema, snapshot["metadata_approved_revision"])
    counts_by_kind = {"binding": 0, "relationship": 0}
    async for kind, catalog_rows in iter_reviewed_fhir_catalog_product_rows(
        connection,
        observation_schema=copy_target.schema_name,
        snapshot=snapshot,
        control_schema=grant.control_schema,
        companies=companies,
        groups=groups,
        crosswalk_for=grant.crosswalk_for,
        source_generations=source_generations,
    ):
        if kind not in counts_by_kind:
            raise ValueError("registry_fhir_catalog_product_invalid")
        if kind == "binding":
            await require_reviewed_product_binding_slots(
                connection, binding_relation, tuple(catalog_row[2] for catalog_row in catalog_rows)
            )
        await write_fhir_catalog_rows(
            connection,
            copy_target,
            "registry_fhir_catalog_" + kind,
            catalog_rows,
            loader_role=loader_role,
            control_schema=grant.control_schema,
        )
        counts_by_kind[kind] += len(catalog_rows)
    return counts_by_kind


async def _project_current_sources(
    config, loader, source_connection, reference_workspace, copy_target, snapshot, previous, current_by_generation
):
    return await _project_catalog_sources(
        config,
        loader,
        (source_connection, reference_workspace, None),
        copy_target,
        snapshot,
        previous,
        current_by_generation,
    )


async def _project_catalog_sources(
    config, loader, source_reads, copy_target, snapshot, previous, current_by_generation
):
    source_connection, reference_workspace, exports_by_profile = source_reads
    cached_generations = {entry["source_generation"] for entry in previous["source_vector_json"]} if previous else set()
    projection_receipts = []
    for generation, disposition in current_by_generation.items():
        export = _source_catalog_export(disposition.descriptor, exports_by_profile)
        if export is None and source_connection is None:
            raise ValueError("registry_fhir_catalog_source_reader_required")
        export_options = {"source_export": export} if export is not None else {}
        report = current_fhir_catalog_report()
        if report:
            report.source_started(generation)
        extraction_by_kind = {"reused_observations": True}
        if generation not in cached_generations:
            extraction_by_kind = await stage_fhir_catalog_source(
                source_connection,
                loader,
                copy_target,
                disposition,
                loader_role=config.loader_role,
                control_schema=config.grant.control_schema,
                reference_workspace=reference_workspace,
                **export_options,
            )
        elif disposition.descriptor["profile"] == "retained_normalized":
            accepted_at = await loader.fetchval(
                f"UPDATE {storage_identifier(copy_target.schema_name)}.registry_fhir_catalog_source "
                "SET descriptor_json=$2::jsonb WHERE source_generation=$1 RETURNING accepted_at",
                generation,
                canonical_json(disposition.descriptor),
            )
            if accepted_at is None:
                raise ValueError("registry_fhir_catalog_retained_source_missing")
            disposition = replace(disposition, accepted_at=accepted_at)
        projection = await project_fhir_catalog_source(
            loader,
            copy_target,
            disposition,
            snapshot,
            grant=config.grant,
            loader_role=config.loader_role,
            source_connection=source_connection,
            **export_options,
        )
        projection_receipts.append({"extraction": extraction_by_kind, "projection": projection})
        if report:
            report.source_prepared(generation, extraction_by_kind, projection)
    return projection_receipts


def _source_catalog_export(descriptor, exports_by_profile):
    if (
        exports_by_profile is None
        or descriptor["profile"] == "retained_normalized"
        or descriptor["admission"]["kind"] != "catalog_observation"
    ):
        return None
    profile = parse_fhir_catalog_source_profile(descriptor["native_read_profile"])
    if profile.published_read_scope is not None:
        return None
    export = exports_by_profile.get(canonical_json(profile.document()))
    if export is None:
        raise ValueError("registry_fhir_catalog_source_export_required")
    return export


async def _seal_catalog_snapshot(config, copy_target, snapshot, sealer_pool, *, source_pool=None, source_export=None):
    async with (
        sealer_pool.acquire() as registry_reader,
        fhir_catalog_reference_workspace(registry_reader) as registry_workspace,
        registry_reader.transaction(isolation="repeatable_read", readonly=True),
        sealer_pool.acquire() as sealer,
        sealer.transaction(),
    ):
        receipt = await freeze_fhir_catalog_snapshot(
            sealer,
            copy_target,
            writer_roles=writer_role_names(
                config.consumer.physical_owner_role, (config.loader_role,), config.reader_roles
            ),
            expected_snapshot=snapshot,
            control_schema=config.grant.control_schema,
            standing_grant=config.grant,
            observation_verifier=build_fhir_catalog_observation_verifier(
                config.grant,
                source_pool,
                registry_reader=registry_reader,
                registry_workspace=registry_workspace,
                **({"source_export": source_export} if source_export is not None else {}),
            ),
        )
        snapshot.update(custody_json=receipt, accounting_json=receipt["validation"]["tables"])
        columns = tuple(key for key in SNAPSHOT_COLUMNS if key != "published_at")
        json_columns = {"custody_json", "accounting_json", "source_vector_json"}
        parameters = ",".join(
            f"${index}" + ("::jsonb" if key in json_columns else "") for index, key in enumerate(columns, 1)
        )
        await sealer.execute(
            f"INSERT INTO {storage_identifier(config.grant.control_schema)}.registry_fhir_catalog_snapshot"
            f"({','.join(columns)}) VALUES({parameters})",
            *(canonical_json(snapshot[key]) if key in json_columns else snapshot[key] for key in columns),
        )


async def project_manual_fhir_catalog(connection, target, snapshot, *, loader_role, control_schema):
    """Carry approved network names and explicit insurer/group relationships forward."""
    network_count = await _project_manual_networks(connection, target, snapshot, loader_role, control_schema)
    relationship_count = await _project_manual_links(connection, target, snapshot, loader_role, control_schema)
    return {"networks": network_count, "relationships": relationship_count}


async def _project_manual_networks(connection, copy_target, snapshot, loader_role, control_schema):
    control = storage_identifier(control_schema)
    generation, revision = snapshot["metadata_generation"], snapshot["metadata_approved_revision"]
    options_by_name = {"loader_role": loader_role, "control_schema": control_schema}
    after, network_count = 0, 0
    while True:
        network_records = await connection.fetch(
            f"SELECT record_json::text AS document FROM {control}.registry_approved_record "
            "WHERE approved_revision=$1 AND record_kind='network' AND record_key::integer>$2 "
            "ORDER BY record_key::integer LIMIT $3",
            revision,
            after,
            PAGE_ROWS,
        )
        if not network_records:
            break
        network_documents = [json.loads(network_document["document"]) for network_document in network_records]
        network_rows = [
            (
                generation,
                network_document["network_id"],
                network_document["display_name"],
                canonical_json(network_document.get("aliases", [])),
                network_document["archived"],
                network_document["revision"],
                network_document.get("benefit_codes"),
                "\n".join((network_document["display_name"], *network_document.get("aliases", []))).lower(),
            )
            for network_document in network_documents
        ]
        await write_fhir_catalog_rows(
            connection, copy_target, "registry_fhir_catalog_network", network_rows, **options_by_name
        )
        after = network_documents[-1]["network_id"]
        network_count += len(network_rows)
    return network_count


async def _project_manual_links(connection, copy_target, snapshot, loader_role, control_schema):
    control, namespace = storage_identifier(control_schema), storage_identifier(copy_target.schema_name)
    revision = snapshot["metadata_approved_revision"]
    options_by_name = {"loader_role": loader_role, "control_schema": control_schema}
    relationship_count = 0
    await connection.execute(
        f"DELETE FROM {namespace}.registry_fhir_catalog_relationship WHERE source_id='registry-manual'"
    )
    cursor = ("", 0)
    while True:
        link_records = await connection.fetch(
            f"WITH approved AS (SELECT record_kind,record_key,record_json FROM {control}.registry_approved_record "
            "WHERE approved_revision=$1), links AS ("
            "SELECT company.record_key AS company_key,company.record_json AS company,network_id::integer "
            "FROM approved link JOIN approved company ON company.record_kind='company' "
            "AND company.record_key=link.record_key CROSS JOIN LATERAL "
            "jsonb_array_elements_text(link.record_json->'network_ids') AS network_id "
            "WHERE link.record_kind='company_links' AND link.record_json->'archived'='false'::jsonb "
            "AND company.record_json->'archived'='false'::jsonb) "
            "SELECT company_key,network_id,company::text FROM links "
            f"WHERE EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_network n WHERE n.network_id=links.network_id) "
            'AND (company_key COLLATE "C",network_id)>($2 COLLATE "C",$3) '
            'ORDER BY company_key COLLATE "C",network_id LIMIT $4',
            revision,
            *cursor,
            PAGE_ROWS,
        )
        if not link_records:
            break
        company_pairs = [(link_record, json.loads(link_record["company"])) for link_record in link_records]
        groups_by_id = await _manual_group_lookup(connection, control, revision, company_pairs)
        receipt = await write_fhir_catalog_rows(
            connection,
            copy_target,
            "registry_fhir_catalog_relationship",
            format_manual_fhir_catalog_relationship_rows(company_pairs, groups_by_id, snapshot),
            **options_by_name,
        )
        cursor = (link_records[-1]["company_key"], link_records[-1]["network_id"])
        relationship_count += receipt["input_rows"]
    return relationship_count


async def _manual_group_lookup(connection, control, revision, company_pairs):
    group_ids = sorted(
        {
            assertion["group_id"]
            for _, company_document in company_pairs
            for assertion in company_document.get("group_assertions", ())
            if isinstance(assertion, dict) and isinstance(assertion.get("group_id"), str)
        }
    )
    group_records = await connection.fetch(
        f"SELECT record_json::text FROM {control}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='group' AND record_key=ANY($2::text[])",
        revision,
        group_ids,
    )
    groups_by_id = {
        group_document["group_id"]: group_document
        for group_record in group_records
        if (group_document := json.loads(group_record[0]))
    }
    return groups_by_id


def format_manual_fhir_catalog_relationship_rows(companies, groups_by_id, snapshot):
    """Format approved manual company and group links using current catalog inputs."""
    for link_record, company in companies:
        groups = [
            group_row
            for group_row in company_group_rows(company, groups_by_id, snapshot["as_of"])
            if group_row[0]["group_id"] is not None or group_row[0]["relationship_kind"] == "verified_no_affiliation"
        ]
        for group, status, reason, assertion_id in groups or [(None, "resolved", None, None)]:
            identity_parts = ["registry-manual", link_record["company_key"], link_record["network_id"]]
            if assertion_id is not None:
                identity_parts.append(assertion_id)
            relationship_by_field = {
                "relationship_id": hashlib.sha256(canonical_json(identity_parts).encode()).hexdigest(),
                "source_id": "registry-manual",
                "plan": None,
                "organization_reference": None,
                "company_role": None,
                "company": {"company_id": company["company_id"], "display_name": company["display_name"]},
                "group": group,
                "resolution_status": status,
                "resolution_reason": reason,
            }
            yield (
                snapshot["metadata_generation"],
                relationship_by_field["relationship_id"],
                link_record["network_id"],
                "registry-manual",
                snapshot["mapping_sha256"],
                None,
                UUID(company["company_id"]),
                UUID(group["group_id"]) if group is not None and group["group_id"] is not None else None,
                "\n".join((company["display_name"], *company.get("aliases", []))).lower(),
                "",
                canonical_json(relationship_by_field),
            )


def _organization(document):
    return FHIRCatalogOrganization(
        **{**document, "aliases": tuple(document["aliases"]), "identifiers": tuple(document["identifiers"])}
    )


def _edge(document):
    for field in ("plan_aliases", "plan_identifiers", "scope_identifiers", "scope_type_codes", "benefits", "geography"):
        document[field] = tuple(document[field])
    return FHIRCatalogReferenceEdge(**document)


async def stage_fhir_catalog_source(
    source_connection,
    loader,
    copy_target,
    disposition,
    *,
    loader_role,
    control_schema,
    reference_workspace=None,
    source_export=None,
):
    """Recheck an admitted source, then copy compact facts from its pinned read."""
    if type(disposition) is not FHIRCatalogSourceDisposition or disposition.status != "accepted":
        raise ValueError("registry_fhir_catalog_source_not_accepted")
    descriptor = disposition.descriptor
    generation = fhir_catalog_source_generation(descriptor)
    options_by_name = {"loader_role": loader_role, "control_schema": control_schema}
    counts_by_kind = {"source_rows": 0, "observation_rows": 0, "copy_batches": 0, "payload_bytes": 0}
    started = perf_counter()

    async def observations(observation_rows):
        """Copy one bounded observation batch and accumulate its receipt."""
        receipt = await write_fhir_catalog_rows(
            loader, copy_target, "registry_fhir_catalog_observation", observation_rows, **options_by_name
        )
        counts_by_kind["observation_rows"] += receipt["input_rows"]
        counts_by_kind["copy_batches"] += receipt["batches"]
        counts_by_kind["payload_bytes"] += receipt["payload_bytes"]

    if source_export is not None:
        counts_by_kind["source_rows"] = await source_export["replay"](descriptor, observations)
    else:
        if source_connection is None:
            raise ValueError("registry_fhir_catalog_source_reader_required")
        counts_by_kind["source_rows"] = await replay_fhir_catalog_observations(
            source_connection, descriptor, observations, reference_workspace=reference_workspace
        )
    await write_fhir_catalog_rows(
        loader,
        copy_target,
        "registry_fhir_catalog_source",
        [_staged_source_row(disposition, generation, counts_by_kind)],
        columns=TABLE_COLUMNS["registry_fhir_catalog_source"][:-1],
        **options_by_name,
    )
    counts_by_kind["seconds"] = perf_counter() - started
    return counts_by_kind


async def replay_fhir_catalog_observations(source_connection, descriptor, observations, *, reference_workspace=None):
    """Emit the same compact facts in primary-key order for staging and trusted replay."""
    descriptor = validate_fhir_catalog_source_descriptor(descriptor)
    coordinates = descriptor["coordinates"]
    read_dataset_id = coordinates["dataset_id"]
    retained_read = None
    if descriptor["profile"] == "retained_normalized":
        retained_read = await pin_retained_fhir_catalog_source(source_connection, descriptor)
        physical = retained_fhir_catalog_read_source(descriptor)
        read_schema, read_dataset_id = physical.read_schema_name, physical.dataset_id
    elif descriptor["admission"]["kind"] == "catalog_observation":
        read_schema = coordinates["dataset_schema"]
    else:
        await require_current_fhir_catalog_sources(source_connection, (descriptor,))
        read_schema = coordinates["dataset_schema"]
    published_read = await _published_read(source_connection, descriptor)
    source_read = retained_read if retained_read is not None else published_read
    generation = fhir_catalog_source_generation(descriptor)
    count = await _stage_plan_observations(
        source_connection,
        read_schema,
        read_dataset_id,
        descriptor,
        source_read,
        observations,
        generation,
    )
    count += await _stage_reference_observations(
        source_connection,
        read_schema,
        read_dataset_id,
        descriptor,
        source_read,
        reference_workspace,
        observations,
        generation,
    )
    if retained_read is not None:
        await retained_read.verify(source_connection, retained_read.capture)
    if published_read is not None:
        await published_read.verify(source_connection, descriptor=descriptor)
    return count


def _staged_source_row(disposition, generation, counts_by_kind):
    descriptor = disposition.descriptor
    return (
        generation,
        descriptor["coordinates"]["source_id"],
        canonical_json(descriptor),
        descriptor["profile"],
        counts_by_kind["source_rows"],
        counts_by_kind["observation_rows"],
        disposition.accepted_at,
    )


async def _stage_plan_observations(
    source_connection,
    read_schema,
    dataset_id,
    descriptor,
    source_read,
    observations,
    generation,
):
    source_count = 0
    async for resource_batch in iter_fhir_catalog_pages(
        source_connection,
        schema_name=read_schema,
        dataset_id=dataset_id,
        resource_types=("Organization", "InsurancePlan"),
        source_read=source_read,
    ):
        batch = extract_fhir_catalog_batch(
            resource_batch, profile=descriptor["profile"], source_uri=descriptor["source_uri"]
        )
        source_count += batch.source_rows
        observation_rows = [
            (generation, "Organization", org.resource_id, "organization", 0, canonical_json(asdict(org)))
            for org in batch.organizations
        ]
        ordinals_by_resource = {}
        for edge in batch.edges:
            key = (edge.owner_resource_type, edge.owner_resource_id)
            ordinal = ordinals_by_resource.get(key, 0)
            observation_rows.append((generation, *key, "edge", ordinal, canonical_json(asdict(edge))))
            ordinals_by_resource[key] = ordinal + 1
        await observations(sorted(observation_rows, key=lambda row: row[:5]))
    return source_count


async def _stage_reference_observations(
    source_connection,
    read_schema,
    dataset_id,
    descriptor,
    source_read,
    reference_workspace,
    observations,
    generation,
):
    async with staged_fhir_catalog_references(
        source_connection,
        schema_name=read_schema,
        dataset_id=dataset_id,
        source_uri=descriptor["source_uri"],
        source_read=source_read,
        workspace=reference_workspace,
    ) as references:
        async for batch in references.iter_batches():
            await observations(
                [
                    (
                        generation,
                        ref.edge.owner_resource_type,
                        ref.edge.owner_resource_id,
                        "edge",
                        ref.reference_ordinal,
                        canonical_json(asdict(ref.edge)),
                    )
                    for ref in batch
                ]
            )
    return references.accounting.source_rows


async def _approved_maps(connection, control_schema, revision):
    records = await connection.fetch(
        f"SELECT record_kind,record_json::text AS document FROM {storage_identifier(control_schema)}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind=ANY($2::text[]) "
        "ORDER BY record_kind,record_key LIMIT 2001",
        revision,
        ["company", "group"],
    )
    by_kind = {"company": [], "group": []}
    for record in records:
        by_kind[record["record_kind"]].append(json.loads(record["document"]))
    if any(len(values) > PAGE_ROWS for values in by_kind.values()):
        raise ValueError("registry_fhir_catalog_mapping_batch_bounds")
    return tuple(by_kind["company"]), tuple(by_kind["group"])


async def project_fhir_catalog_source(
    connection, copy_target, disposition, snapshot, *, grant, loader_role, source_connection=None, source_export=None
):
    """Allocate identities and project one staged source without row-wise SQL."""
    descriptor = disposition.descriptor
    generation = fhir_catalog_source_generation(descriptor)
    namespace = storage_identifier(copy_target.schema_name)
    started = perf_counter()
    counts_by_kind = {"created_networks": 0, "networks": 0, "relationships": 0, "batches": 0}
    published_read = await _published_read(source_connection, descriptor) if source_export is None else None
    await connection.execute(
        "CREATE TEMP TABLE fhir_catalog_network_targets(resource_id text PRIMARY KEY) ON COMMIT DROP"
    )
    await connection.execute(
        f"INSERT INTO pg_temp.fhir_catalog_network_targets SELECT DISTINCT document_json->>'network_resource_id' "
        f"FROM {namespace}.registry_fhir_catalog_observation WHERE source_generation=$1 AND observation_kind='edge' "
        "AND document_json->>'network_resource_id' IS NOT NULL",
        generation,
    )
    after = ""
    while True:
        organization_records = await _network_organization_page(connection, namespace, generation, after)
        if not organization_records:
            break
        legacy_by_organization = await _legacy_identity_inputs(
            None if descriptor["profile"] == "retained_normalized" else source_connection,
            connection,
            descriptor,
            tuple(organization_record["resource_id"] for organization_record in organization_records),
            storage_identifier(grant.control_schema),
            published_read=published_read,
            **({"source_export": source_export} if source_export is not None else {}),
        )
        created, count = await _project_network_page(
            connection,
            copy_target,
            disposition,
            snapshot,
            organization_records,
            grant,
            loader_role,
            legacy_by_organization,
        )
        counts_by_kind["created_networks"] += created
        counts_by_kind["networks"] += count
        counts_by_kind["batches"] += 1
        after = organization_records[-1]["resource_id"]
    await connection.execute("DROP TABLE pg_temp.fhir_catalog_network_targets")
    counts_by_kind["relationships"] = await project_fhir_catalog_relationships(
        connection,
        copy_target,
        disposition,
        snapshot,
        grant=grant,
        loader_role=loader_role,
        status="last_good" if descriptor["profile"] == "retained_normalized" else "current",
    )
    counts_by_kind["seconds"] = perf_counter() - started
    return counts_by_kind


async def _network_organization_page(connection, namespace, generation, after):
    return await connection.fetch(
        f"SELECT o.resource_id,o.document_json::text AS document FROM {namespace}.registry_fhir_catalog_observation o "
        "WHERE o.source_generation=$1 AND o.resource_type='Organization' AND o.observation_kind='organization' "
        "AND o.resource_id>$2 COLLATE \"C\" AND (o.document_json->'declared_network'='true'::jsonb "
        "OR EXISTS(SELECT FROM pg_temp.fhir_catalog_network_targets n WHERE n.resource_id=o.resource_id)) "
        'ORDER BY o.resource_id COLLATE "C" LIMIT $3',
        generation,
        after,
        PAGE_ROWS,
    )


def _source_projection_context(disposition, snapshot, generation, status):
    descriptor = disposition.descriptor
    return SourceCatalogContext(
        RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
        generation,
        snapshot["metadata_generation"],
        disposition.accepted_at,
        descriptor["profile"],
        status=status,
        capture_sha256=descriptor.get("capture_sha256"),
    )


async def _project_network_page(
    connection,
    copy_target,
    disposition,
    snapshot,
    organization_records,
    grant,
    loader_role,
    legacy_by_organization,
):
    """Resolve one organization page and write its canonical names and bindings."""
    descriptor = disposition.descriptor
    generation = fhir_catalog_source_generation(descriptor)
    namespace = storage_identifier(copy_target.schema_name)
    control = storage_identifier(grant.control_schema)
    revision = snapshot["metadata_approved_revision"]
    context = _source_projection_context(
        disposition, snapshot, generation, "last_good" if descriptor["profile"] == "retained_normalized" else "current"
    )
    organizations = tuple(
        _organization(json.loads(organization_record["document"])) for organization_record in organization_records
    )
    allocation_by_field = await allocate_fhir_catalog_network_ids(
        connection,
        _network_identity_inputs(organizations, descriptor, generation, legacy_by_organization),
        approved_revision=revision,
        control_schema=grant.control_schema,
    )
    if any(allocation_record["conflict"] for allocation_record in allocation_by_field["rows"]):
        raise ValueError("registry_fhir_catalog_identity_conflict")
    ids = tuple(
        (allocation_record["organization_id"], allocation_record["network_id"])
        for allocation_record in allocation_by_field["rows"]
    )
    canonical_records = await connection.fetch(
        f"SELECT record_json::text FROM {control}.registry_approved_record WHERE approved_revision=$1 "
        "AND record_kind='network' AND record_key=ANY($2::text[])",
        revision,
        [str(key) for _, key in ids],
    )
    projection = format_fhir_catalog_projection(
        organizations,
        (),
        ids,
        context,
        canonical_networks=tuple(json.loads(allocation_record[0]) for allocation_record in canonical_records),
    )
    await _replace_observed_bindings(connection, namespace, descriptor, [key for key, _ in ids])
    await _write_network_projection(connection, copy_target, projection, loader_role, grant.control_schema)
    return allocation_by_field["created_networks"], len(ids)


def _network_identity_inputs(organizations, descriptor, generation, legacy_by_organization):
    return tuple(
        FHIRNetworkIdentityInput(
            organization.resource_id,
            descriptor["endpoint_id"],
            tuple(descriptor["source_aliases"]),
            f"{generation}:Organization/{organization.resource_id}",
            *legacy_by_organization.get(organization.resource_id, (None, None)),
        )
        for organization in organizations
    )


async def _write_network_projection(connection, copy_target, projection, loader_role, control_schema):
    for table, projection_rows in (
        ("registry_fhir_catalog_network", projection.network_rows),
        ("registry_fhir_catalog_binding", projection.binding_rows),
    ):
        await write_fhir_catalog_rows(
            connection, copy_target, table, projection_rows, loader_role=loader_role, control_schema=control_schema
        )


async def refresh_carried_fhir_catalog_relationships(
    connection, copy_target, snapshot, *, current_generations, grant, loader_role
):
    """Refresh cached enrichment through surviving bindings without restoring old identities."""
    namespace = storage_identifier(copy_target.schema_name)
    after, relationships = "", 0
    while True:
        source_record = await connection.fetchrow(
            f"SELECT source_generation,descriptor_json::text AS descriptor,accepted_at "
            f"FROM {namespace}.registry_fhir_catalog_source "
            'WHERE source_generation>$1 COLLATE "C" AND NOT(source_generation=ANY($2::text[])) '
            'ORDER BY source_generation COLLATE "C" LIMIT 1',
            after,
            current_generations,
        )
        if source_record is None:
            return relationships
        descriptor = json.loads(source_record["descriptor"])
        disposition = FHIRCatalogSourceDisposition(
            descriptor["endpoint_id"],
            descriptor["coordinates"]["dataset_id"],
            tuple(descriptor["source_aliases"]),
            len(descriptor["source_aliases"]),
            "accepted",
            None,
            descriptor,
            source_record["accepted_at"],
        )
        relationships += await project_fhir_catalog_relationships(
            connection, copy_target, disposition, snapshot, grant=grant, loader_role=loader_role, status="last_good"
        )
        after = source_record["source_generation"]


async def project_fhir_catalog_relationships(
    connection, copy_target, disposition, snapshot, *, grant, loader_role, status
):
    """Replace derived relationships using only the source's surviving exact bindings."""
    descriptor = disposition.descriptor
    generation = fhir_catalog_source_generation(descriptor)
    namespace = storage_identifier(copy_target.schema_name)
    options_by_name = {"loader_role": loader_role, "control_schema": grant.control_schema}
    await replace_fhir_catalog_relationships(connection, copy_target, (generation,), **options_by_name)
    companies, groups = await _approved_maps(connection, grant.control_schema, snapshot["metadata_approved_revision"])
    context = _source_projection_context(disposition, snapshot, generation, status)
    enrichment = FHIRCatalogEnrichmentContext(
        context.coordinates.source_id,
        snapshot["as_of"],
        companies,
        groups,
        descriptor["endpoint_id"],
        grant.crosswalk_for(context.coordinates.source_id),
        descriptor["source_uri"],
    )
    count = 0
    cursor_values = ("", "", -1)
    while True:
        edge_records = await connection.fetch(
            f"SELECT resource_type,resource_id,ordinal,document_json::text AS document "
            f"FROM {namespace}.registry_fhir_catalog_observation o "
            "WHERE source_generation=$1 AND observation_kind='edge' "
            f"AND EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_binding b "
            "WHERE b.document_json->>'evidence_id'=o.source_generation||':Organization/'||"
            "(o.document_json->>'network_resource_id')) "
            'AND (resource_type COLLATE "C",resource_id COLLATE "C",ordinal)>($2 COLLATE "C",$3 COLLATE "C",$4) '
            'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",ordinal LIMIT $5',
            generation,
            *cursor_values,
            EDGE_ROWS,
        )
        if not edge_records:
            break
        edges = tuple(_edge(json.loads(edge_record["document"])) for edge_record in edge_records)
        organizations, ids = await _edge_organizations(connection, namespace, generation, edges)
        relationships = enrich_fhir_catalog_edges(organizations, edges, enrichment)
        # Keep projection's linkage checks without rewriting names or surviving bindings.
        projection = format_fhir_catalog_projection(organizations, relationships, ids, context)
        await write_fhir_catalog_rows(
            connection,
            copy_target,
            "registry_fhir_catalog_relationship",
            projection.relationship_rows,
            **options_by_name,
        )
        count += len(projection.relationship_rows)
        cursor_values = tuple(edge_records[-1][key] for key in ("resource_type", "resource_id", "ordinal"))
    return count


async def _published_read(connection, descriptor):
    if descriptor["profile"] == "retained_normalized":
        return None
    profile = parse_fhir_catalog_source_profile(descriptor["native_read_profile"])
    if profile.published_read_scope is None:
        return None
    if connection is None:
        raise ValueError("registry_fhir_catalog_published_reader_required")
    return await pin_fhir_catalog_published_read(
        connection,
        profile,
        endpoint_id=descriptor["endpoint_id"],
        dataset_id=descriptor["coordinates"]["dataset_id"],
        require_current=descriptor["admission"]["kind"] != "catalog_observation",
        descriptor=descriptor,
    )


async def _legacy_identity_inputs(
    source_connection, loader, descriptor, organization_ids, control, *, published_read=None, source_export=None
):
    """Read genuine source UUIDs and existing scoped aliases in two batch queries."""
    if source_export is not None:
        return await _source_owned_legacy_identity_inputs(source_export, loader, descriptor, organization_ids, control)
    if source_connection is None:
        return {}
    binding_relation = (
        f"{storage_identifier(descriptor['coordinates']['dataset_schema'])}"
        ".provider_directory_insurance_network_source_binding"
    )
    if descriptor["native_read_profile"].get("published_read_scope") is not None:
        if published_read is None:
            raise ValueError("registry_fhir_catalog_published_reader_required")
        await published_read.verify(source_connection, descriptor=descriptor)
        binding_relation = published_read.binding_relation
    source_bindings = await source_connection.fetch(
        f"SELECT resource_id,network_id::text FROM {binding_relation} "
        "WHERE source_id=ANY($1::text[]) AND resource_id=ANY($2::text[])",
        descriptor["source_aliases"],
        list(organization_ids),
    )
    if not source_bindings:
        return {}
    identities_by_organization = {}
    await _merge_legacy_identity_inputs(loader, descriptor, source_bindings, control, identities_by_organization)
    return identities_by_organization


async def _source_owned_legacy_identity_inputs(source_export, loader, descriptor, organization_ids, control):
    identities_by_organization = {}
    async for bindings in iter_source_owned_catalog_legacy_bindings(source_export, descriptor, organization_ids):
        await _merge_legacy_identity_inputs(loader, descriptor, bindings, control, identities_by_organization)
    return identities_by_organization


async def _merge_legacy_identity_inputs(loader, descriptor, source_bindings, control, identities_by_organization):
    aliases = await loader.fetch(
        f"SELECT alias_value,scope_key,network_id FROM {control}.network_registry_alias "
        "WHERE source_system='fhir' AND source_id=ANY($1::text[]) AND alias_type='legacy_fhir_uuid' "
        'AND alias_value=ANY($2::text[]) ORDER BY scope_key COLLATE "C"',
        descriptor["source_aliases"],
        sorted({source_binding["network_id"] for source_binding in source_bindings}),
    )
    by_uuid = {}
    for alias in aliases:
        by_uuid.setdefault(alias["alias_value"], []).append(alias)
    for source_binding in source_bindings:
        matches = by_uuid.get(source_binding["network_id"], [])
        if not matches:
            continue
        if len({alias["network_id"] for alias in matches}) != 1:
            raise ValueError("registry_fhir_catalog_identity_conflict")
        identity_pair = (source_binding["network_id"], matches[0]["scope_key"])
        if (
            source_binding["resource_id"] in identities_by_organization
            and identities_by_organization[source_binding["resource_id"]] != identity_pair
        ):
            raise ValueError("registry_fhir_catalog_identity_conflict")
        identities_by_organization[source_binding["resource_id"]] = identity_pair


async def _replace_observed_bindings(connection, namespace, descriptor, organization_ids):
    """Replace this source's observed networks only; missing networks keep old rows."""
    arguments = (organization_ids, descriptor["endpoint_id"], descriptor["coordinates"]["dataset_schema"])
    selection = (
        f"SELECT b.binding_id FROM {namespace}.registry_fhir_catalog_binding b "
        f"JOIN {namespace}.registry_fhir_catalog_source s ON "
        "b.document_json->>'evidence_id'=s.source_generation||':Organization/'||(b.document_json->>'source_key') "
        "WHERE b.document_json->>'source_key'=ANY($1::text[]) AND s.descriptor_json->>'endpoint_id'=$2 "
        "AND s.descriptor_json->'coordinates'->>'dataset_schema'=$3"
    )
    await connection.execute(
        f"DELETE FROM {namespace}.registry_fhir_catalog_relationship WHERE binding_id IN ({selection})", *arguments
    )
    await connection.execute(
        f"DELETE FROM {namespace}.registry_fhir_catalog_binding WHERE binding_id IN ({selection})", *arguments
    )


async def _edge_organizations(connection, namespace, generation, edges):
    needed_ids = {edge.network_resource_id for edge in edges if edge.network_resource_id is not None}
    for edge in edges:
        for reference in (edge.owner_ref, edge.administrator_ref):
            if reference:
                needed_ids.add(urlsplit(reference).path.rstrip("/").rsplit("/", 1)[-1])
    rows = await connection.fetch(
        f"SELECT o.document_json::text AS document,b.network_id FROM {namespace}.registry_fhir_catalog_observation o "
        f"LEFT JOIN {namespace}.registry_fhir_catalog_binding b ON b.document_json->>'evidence_id'="
        "o.source_generation||':Organization/'||o.resource_id "
        "WHERE o.source_generation=$1 AND o.resource_type='Organization' AND o.observation_kind='organization' "
        "AND (o.resource_id=ANY($2::text[]) OR o.resource_id IN ("
        f"SELECT regexp_replace(parent.document_json->>'part_of_ref','^.*/','') FROM {namespace}.registry_fhir_catalog_observation parent "
        "WHERE parent.source_generation=$1 AND parent.resource_type='Organization' "
        "AND parent.observation_kind='organization' AND parent.resource_id=ANY($2::text[]))) "
        'ORDER BY o.resource_id COLLATE "C"',
        generation,
        sorted(needed_ids),
    )
    organizations = tuple(_organization(json.loads(row["document"])) for row in rows)
    ids = tuple(
        (org.resource_id, row["network_id"])
        for org, row in zip(organizations, rows, strict=True)
        if row["network_id"] is not None
    )
    return organizations, ids
