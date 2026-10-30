# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare serving artifacts and apply them inside a caller-owned publication."""

from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from typing import Any, AsyncIterator

from sqlalchemy import text

from process.provider_directory_serving_semantic_inputs import validated_semantic_inputs as validated_semantic_inputs


@dataclass(frozen=True)
class CapturedServingInputs:
    """Original server observations; detached property copies convey no write authority."""

    database: Any = field(repr=False, compare=False)
    _engine: Any = field(repr=False, compare=False)
    _execution: Any = field(repr=False, compare=False)
    _fence: Any = field(repr=False, compare=False)
    _payload_json: str = field(repr=False)
    _limits_json: str = field(repr=False)
    _factory: Any = field(default=None, repr=False, compare=False)

    @property
    def scope_payload(self):
        """Return the complete server-captured identity and reviewed budget ceilings."""
        return json.loads(self._payload_json)["scope"]

    @property
    def address_task(self):
        """Return the exact existing partial recipe, or no task for an empty scope."""
        return json.loads(self._payload_json)["affected_scope"]["address_task"]

    @property
    def affected_scope(self):
        """Return Source intent plus the mandatory server-detected changes and pins."""
        return json.loads(self._payload_json)["affected_scope"]

    @property
    def overlay_fence(self):
        """Keep only refreshed desired snapshots; empty means no Overlay input reads."""
        refreshed_ids = set(self.affected_scope["refresh_source_ids"])
        datasets = tuple(pin for pin in self._fence.datasets if pin.source_id in refreshed_ids)
        _require_capture({pin.source_id for pin in datasets} == refreshed_ids)
        return replace(self._fence, datasets=datasets)

    @property
    def retention_plan(self):
        """Return the fixed serving roots and their permitted retention modes."""
        return json.loads(self._payload_json)["retention_plan"]

    @property
    def initial_targets(self):
        """Return the authentic original v1 baseline, preserving its historical absence claim."""
        from process import provider_directory_profile_initial_contract as initial

        predecessor = self.predecessor
        if predecessor["kind"] != "initial":
            return None
        target = initial.validated_target_state(predecessor["profile"])
        if target["contract_id"] != initial.TARGET_CONTRACT:
            target = target["recovered_initial_publication"]["initial_targets"]
        _require_capture(target["contract_id"] == initial.TARGET_CONTRACT)
        return initial.validated_target_state(target)

    @property
    def semantic_inputs(self):
        """Return input-only facts; no-op also requires a qualified current common receipt."""
        return validated_semantic_inputs(json.loads(self._payload_json)["predecessor"]["native"]["semantic_inputs"])

    @property
    def predecessor(self):
        """Return genuine published state or authentic initial targets and history."""
        return json.loads(self._payload_json)["predecessor"]

    @property
    def is_unchanged(self):
        """Report an empty requested and mandatory scope, without claiming output freshness."""
        return not self.affected_scope["source_ids"]

    def _require_owner(self, fhir):
        from process import provider_directory_generic_source_policy as source

        _require_capture(fhir.db is self.database and fhir.db.engine is self._engine)
        _require_capture(source._FACTORY.get() is self._factory)
        if source.is_source_storage_required():
            _require_capture(self._factory is not None)
        if self._factory is not None:
            _require_capture(self._factory.database is fhir.db)
            self._factory._require_current_policy()

    async def recheck(self, fhir):
        """Repeat real observations on the current owner before any serving writes."""
        from process.provider_directory_generic_source_policy import is_source_storage_required
        from process.provider_directory_profile_scope_copy import preflight_transaction

        self._require_owner(fhir)
        if fhir.db._transaction_binding() is not None:
            current = await _capture_serving_inputs(
                fhir,
                self._execution,
                self._fence,
                json.loads(self._limits_json),
                self.scope_payload["requested_source_ids"],
            )
        else:
            async with preflight_transaction(fhir, isolation="repeatable read", readonly=True) as session:
                if not is_source_storage_required():
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                current = await _capture_serving_inputs(
                    fhir,
                    self._execution,
                    self._fence,
                    json.loads(self._limits_json),
                    self.scope_payload["requested_source_ids"],
                )
        _require_capture(current._payload_json == self._payload_json)


def _require_capture(condition):
    if not condition:
        raise RuntimeError("provider_directory_serving_capture_changed")


def _capture_digest(domain, value):
    from process.provider_directory_serving_capacity_contract import canonical

    return hashlib.sha256(domain.encode("ascii") + b"\0" + canonical(value).encode("ascii")).hexdigest()


async def _capture_builder(fhir):
    """Resolve the original configured Builder, even while SOURCE owns the read binding."""
    from process import provider_directory_generic_source_policy as source

    engine = fhir.db.engine
    _require_capture(engine is not None and isinstance(engine.url.username, str) and bool(engine.url.username))
    factory = source._FACTORY.get()
    if source.is_source_storage_required():
        _require_capture(factory is not None)
    if factory is not None:
        _require_capture(factory.database is fhir.db)
        factory._require_current_policy()
    row = await fhir.db.first(
        "SELECT role.oid::bigint AS owner_oid,namespace.oid::bigint AS schema_oid,"
        "role.rolcanlogin AND has_schema_privilege(role.oid,namespace.oid,'USAGE') "
        "AND has_schema_privilege(role.oid,namespace.oid,'CREATE') AS can_create "
        "FROM pg_roles role,pg_namespace namespace WHERE role.rolname=:role AND namespace.nspname=:schema",
        role=engine.url.username,
        schema=fhir._schema(),
    )
    _require_capture(row is not None)
    owner_by_field = dict(fhir._pagination_checkpoint_row_mapping(row))
    _require_capture(owner_by_field.pop("can_create", None) is True)
    return owner_by_field


async def _capture_predecessor(fhir, execution):
    """Use a real serving vector or authentic undated initial targets, never adoption."""
    from process import provider_directory_profile_initial as initial
    from process import provider_directory_profile_initial_contract as initial_contract

    state = await fhir._provider_directory_profile_serving_state(fhir._schema())
    if state is not None:
        _require_capture(state.status == "published" and state.operation == "publish")
        _require_capture(not initial_contract.execution_initial_requested(execution))
        return {"kind": "published", "profile": asdict(state)}, state
    _require_capture(initial_contract.execution_initial_requested(execution))
    targets = await initial.capture_targets(fhir, fhir._schema())
    return {"kind": "initial", "profile": targets.payload}, targets


async def _capture_affected_scope(fhir, execution, fence, predecessor, state, requested):
    """Derive the exact changed Source vector using the existing Profile delta rules."""
    from process.entity_address_preparation_admission import inference_enabled

    desired = fhir._provider_directory_profile_source_vector(
        (pin["source_id"], pin["dataset_id"]) for pin in execution.attestation.pairs
    )
    _selected, _retained, contexts = await fhir._provider_directory_profile_scope_source_ids(
        fhir._schema(), {source_id for source_id, _dataset in desired}
    )
    desired_context = fhir._provider_directory_profile_source_context_vector(contexts)
    if predecessor["kind"] == "initial":
        history = state.payload["historical_publication"]
        previous = (
            ()
            if history is None
            else fhir._provider_directory_profile_source_vector(
                (pin["source_id"], pin["dataset_id"]) for pin in history["result"]["pairs"]
            )
        )
        refreshed_ids = tuple(source_id for source_id, _dataset in desired)
        removed_ids = tuple(sorted(set(dict(previous)) - set(dict(desired))))
    else:
        previous = state.source_vector
        refreshed_ids, removed_ids = fhir._provider_directory_profile_delta_sources(state, desired, desired_context)
        if execution.attestation.desired_profile_as_of != state.profile_as_of:
            refreshed_ids = tuple(source_id for source_id, _dataset in desired)
    _require_capture(set(requested) <= set(dict(desired)) | set(dict(previous)))
    refreshed_ids = tuple(sorted(set(refreshed_ids) | set(requested).intersection(dict(desired))))
    source_ids = sorted(set(refreshed_ids) | set(removed_ids))
    pins = [
        {
            "source_id": pin.source_id,
            "endpoint_id": pin.endpoint_id,
            "dataset_id": pin.dataset_id,
            "dataset_hash": pin.dataset_hash,
            "acquisition_root_run_id": pin.evidence_run_id,
        }
        for pin in fence.datasets
        if pin.source_id in set(refreshed_ids)
    ]
    task_by_field = _captured_address_task(source_ids, refreshed_ids, removed_ids, pins)
    return {
        "contract_id": "provider-directory-ordinary-affected-scope.v1",
        "source_ids": source_ids,
        "requested_source_ids": requested,
        "refresh_source_ids": list(refreshed_ids),
        "removed_source_ids": list(removed_ids),
        "previous_pairs": [list(pair) for pair in previous if pair[0] in source_ids],
        "desired_pins": pins,
        "desired_context": [list(pair) for pair in desired_context],
        "desired_profile_as_of": execution.attestation.desired_profile_as_of,
        "address_task": task_by_field,
        "inference_enabled": inference_enabled(False),
    }


def _captured_address_task(source_ids, refreshed_ids, removed_ids, pins):
    if not source_ids:
        return {}
    task_by_field = {
        "refresh_mode": "provider-directory-partial",
        "serving_only_refresh": True,
        "test_mode": False,
        "provider_directory_source_ids": source_ids,
        "provider_directory_partial_scope": "all",
    }
    if len(refreshed_ids) == 1 and not removed_ids:
        _require_capture(len(pins) == 1)
        task_by_field.update(
            provider_directory_dataset_id=pins[0]["dataset_id"],
            provider_directory_run_id=pins[0]["acquisition_root_run_id"],
            provider_directory_partial_scope="latest-run",
        )
    return task_by_field


async def _capture_serving_roots(fhir, predecessor):
    """Read fixed model roots and preserve their actual ownership and native structure."""
    from process import provider_directory_ordinary_retention_capacity as retention
    from process.provider_directory_cms_preparation import OwnedRelation

    roots = []
    for family, (relation_name, _key) in retention._FAMILIES.items():
        row = await fhir.db.first(
            "SELECT c.oid::bigint AS oid,c.relowner::bigint AS owner_oid,c.relnamespace::bigint AS schema_oid,"
            "pg_total_relation_size(c.oid)::bigint AS total_bytes,c.relpersistence::text AS persistence "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=:schema AND c.relname=:name AND c.relkind='r' AND NOT c.relrowsecurity "
            "AND NOT c.relforcerowsecurity AND NOT c.relispartition",
            schema=fhir._schema(),
            name=relation_name,
        )
        _require_capture(row is not None)
        root_by_field = dict(fhir._pagination_checkpoint_row_mapping(row))
        _require_capture(root_by_field.pop("persistence") == "p")
        relation = OwnedRelation(fhir._schema(), relation_name, root_by_field["oid"], root_by_field["total_bytes"], "p")
        root_by_field.update(family=family, schema=fhir._schema(), relation=relation_name)
        root_by_field["layout_sha256"] = await retention.retention_layout_sha256(fhir, relation)
        if family in {"profile", "evidence"}:
            _require_capture(root_by_field["oid"] == predecessor["profile"][family + "_target_oid"])
        roots.append(root_by_field)
    _require_capture(len({root_by_field["oid"] for root_by_field in roots}) == len(roots))
    return roots


def _captured_retention_plan(predecessor, roots, affected_hash):
    from process.provider_directory_ordinary_retention_capacity import _FAMILIES, _PLAN_CONTRACT

    entries, resident = [], 0
    for root_by_field in roots:
        is_initial = predecessor["kind"] == "initial" and root_by_field["family"] in {"profile", "evidence"}
        if is_initial:
            _require_capture(
                root_by_field["total_bytes"] == predecessor["profile"][root_by_field["family"] + "_target_bytes"]
            )
            resident += root_by_field["total_bytes"]
        entries.append(
            {
                "family": root_by_field["family"],
                "source_schema": root_by_field["schema"],
                "source_relation": root_by_field["relation"],
                "source_oid": root_by_field["oid"],
                "source_layout_sha256": root_by_field["layout_sha256"],
                "key_columns": list(_FAMILIES[root_by_field["family"]][1]),
                "scope_kind": "initial_full_root"
                if is_initial
                else (
                    "affected_npi"
                    if root_by_field["family"] in {"profile", "evidence", "overlay"}
                    else "affected_location"
                ),
                "retention_mode": "existing_initial_target" if is_initial else "owned_copy",
            }
        )
    return {
        "contract_id": _PLAN_CONTRACT,
        "affected_scope_sha256": affected_hash,
        "families": entries,
    }, resident


async def _capture_native_serving(fhir, session, *, inference_enabled):
    """Reuse ordinary native serving, alias and geo authority without CMS registration."""
    from process import entity_address_dependency_bindings as dependencies
    from process import entity_address_result_generation as generation
    from process import entity_address_snapshot_serving as serving
    from process.provider_directory_ordinary_facility_inputs import capture_facility_inputs
    from process.provider_directory_profile_selection_contract import configured_node_id
    from process.provider_directory_serving_semantic_inputs import capture_semantic_dependencies

    observed = await serving._read_observed_serving(
        session, fhir._schema(), require_current_dependencies=False, allow_unpublished_geo=True
    )
    authority = await generation.read_entity_address_result_generation_authority(session, schema_name=fhir._schema())
    selected = await dependencies.capture_selected_publication_bindings(
        session, fhir._schema(), node_id=configured_node_id()
    )
    selected_relations, bindings = (
        selected
        if selected is not None
        else ({}, await dependencies.capture_publication_dependency_bindings(session, fhir._schema(), {}))
    )
    facility_recipe, facility_inputs = await capture_facility_inputs(
        session, fhir._schema(), selected_relations, inference_enabled=inference_enabled
    )
    return {
        "facility_recipe": facility_recipe,
        "facility_inputs": facility_inputs,
        "observed": observed.as_dict(),
        "authority": authority.as_dict(),
        "selected_relations": selected_relations,
        "current_dependency_bindings": bindings,
        "semantic_dependency_proofs": await capture_semantic_dependencies(session, fhir._schema(), selected_relations),
        "inference_inputs": (
            await dependencies.capture_inference_inputs(session, fhir._schema(), selected_relations)
            if inference_enabled
            else None
        ),
    }


async def _capture_selection_binding(fhir, execution, fence):
    """Verify the original selection and immutable Source pins before observing outputs."""
    binding = fhir.db._transaction_binding()
    _require_capture(binding is not None)
    selected = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
    _require_capture(selected is None or selected is execution)
    await fhir.assert_profile_selection_current_in_transaction(
        execution.attestation, fhir._provider_directory_profile_selection_catalog()
    )
    fhir._assert_profile_selection_matches_artifact_fence(execution, fence)
    await fhir._verify_provider_directory_artifact_dataset_fence(fence)
    return binding


async def _capture_serving_inputs(fhir, execution, fence, retention_limits, refresh_source_ids):
    """Capture the complete ordinary scope from one original database transaction."""
    from process import provider_directory_generic_source_policy as source
    from process import provider_directory_serving_capacity_contract as contract
    from process.provider_directory_cms_preparation import desired_fence_hash
    from process.provider_directory_ordinary_retention_capacity import retention_plan_sha256
    from process.provider_directory_serving_semantic_inputs import semantic_inputs

    limits = contract.validate_retention_limits(retention_limits)
    factory = source._FACTORY.get()
    requested = contract.validate_refresh_source_ids(refresh_source_ids)
    contract.require_ordinary_selection(execution.attestation.payload)
    binding = await _capture_selection_binding(fhir, execution, fence)
    builder = await _capture_builder(fhir)
    predecessor, state = await _capture_predecessor(fhir, execution)
    affected = await _capture_affected_scope(fhir, execution, fence, predecessor, state, requested)
    roots = await _capture_serving_roots(fhir, predecessor)
    native = await _capture_native_serving(fhir, binding.session, inference_enabled=affected["inference_enabled"])
    _require_capture(
        native["observed"]["relations"][0]["relation_oid"]
        == next(root["oid"] for root in roots if root["family"] == "unified")
    )
    affected["facility_recipe"] = native.pop("facility_recipe")
    native["semantic_inputs"] = semantic_inputs(execution, affected, native)
    affected_hash = _capture_digest("provider-directory-ordinary-affected-scope.v1", affected)
    plan, resident = _captured_retention_plan(predecessor, roots, affected_hash)
    _require_capture(resident <= limits["max_retention_data_bytes"])
    native_hash = _capture_digest(
        "provider-directory-ordinary-address-native-input.v1",
        {"serving": native, "affected_scope_sha256": affected_hash, "desired_fence_hash": desired_fence_hash(fence)},
    )
    predecessor.update(roots=roots, native=native)
    scope_by_field = {
        "contract_id": contract.SCOPE,
        **builder,
        **limits,
        "resident_retention_data_bytes": resident,
        "requested_source_ids": requested,
        "source_vector_sha256": contract.source_vector_sha256(execution.attestation.payload["pairs"]),
        "predecessor_sha256": _capture_digest("provider-directory-ordinary-predecessor.v1", predecessor),
        "affected_scope_sha256": affected_hash,
        "retention_plan_sha256": retention_plan_sha256(plan),
        "native_address_input_hash": native_hash,
        "retention_kind": "snapshot" if predecessor["kind"] == "initial" else "delta_preimage",
    }
    scope_by_field["input_sha256"] = contract.input_sha256(
        {name: scope_by_field[name] for name in contract.INPUT_FIELDS}
    )
    captured_by_field = {
        "scope": contract.validate_scope(scope_by_field),
        "predecessor": predecessor,
        "affected_scope": affected,
        "retention_plan": plan,
    }
    _require_capture(fhir.db._transaction_binding() is binding and source._FACTORY.get() is factory)
    payload_json, limits_json = map(contract.canonical, (captured_by_field, limits))
    return CapturedServingInputs(fhir.db, fhir.db.engine, execution, fence, payload_json, limits_json, factory)


async def capture_serving_inputs(fhir, execution, fence, *, retention_limits, refresh_source_ids=()):
    """Observe real inputs in the caller's original repeatable snapshot before any build."""
    binding = fhir.db._transaction_binding()
    _require_capture(binding is not None)
    isolation = await binding.session.scalar(text("SHOW transaction_isolation"))
    _require_capture(isolation in {"repeatable read", "serializable"})
    return await _capture_serving_inputs(fhir, execution, fence, retention_limits, refresh_source_ids)


@asynccontextmanager
async def prepare_artifact_bundle(
    fhir: Any,
    fence: Any,
    request: Any,
    *,
    artifact_resource_types: frozenset[str],
    resource_fence: Any = None,
) -> AsyncIterator[tuple[Any, dict[str, Any]]]:
    """Keep an immutable serving bundle staged until its owner publishes it."""
    resource_fence = resource_fence or await fhir._provider_directory_profile_resource_scope_fence(
        fence, request.publish_artifacts_targets
    )
    async with fhir._provider_directory_artifact_dataset_scope(
        run_id=request.run_id,
        source_ids=request.source_ids,
        fence=fence,
        resource_fence=resource_fence,
        metrics=request.metrics,
        resource_types=artifact_resource_types,
    ):
        async with fhir._provider_directory_artifact_bundle_scope() as artifact_bundle:
            fhir._attach_artifact_fence_metrics(request.metrics, fence)
            publish_metrics = await fhir._publish_provider_directory_artifacts(
                replace(
                    request,
                    address_key_run_id=None,
                    publish_scope_run_id=None,
                    source_ids=[dataset.source_id for dataset in fence.datasets],
                )
            )
            fhir._assert_candidate_artifact_bundle_complete(
                fence,
                publish_metrics,
                artifact_bundle,
                publish_corroboration=request.publish_corroboration,
                publish_artifacts_targets=request.publish_artifacts_targets,
            )
            yield artifact_bundle, publish_metrics


async def apply_prepared_artifact_bundle(
    fhir: Any,
    stages: tuple[Any, ...],
    *,
    profile_delta: Any = None,
    cutover_timeout: Any = None,
    settings_configured: bool = False,
    before_swaps: Any = None,
    extra_live_relations: tuple[str, ...] = (),
) -> Any:
    """Apply prepared relations without committing the owner's publication."""
    if fhir.db._transaction_binding() is None:
        raise RuntimeError("provider_directory_artifact_bundle_requires_transaction")
    ordered_stages = fhir._ordered_provider_directory_artifact_bundle(stages)
    if not ordered_stages and profile_delta is None:
        if extra_live_relations:
            await fhir._lock_provider_directory_artifact_live_tables(fhir._schema(), extra_live_relations)
        if before_swaps is not None:
            await before_swaps()
        return
    schema, relation_names, lock_timeout, statement_timeout = fhir._provider_directory_artifact_bundle_context(
        ordered_stages, profile_delta
    )
    capacity_admission = (
        fhir._provider_directory_profile_capacity_admission()
        if settings_configured
        else await fhir._configure_provider_directory_artifact_promotion(lock_timeout, statement_timeout)
    )
    await fhir.profile_initial.lock_metadata(fhir, ordered_stages, profile_delta)
    if any(
        stage.build_fence is not None and stage.build_fence.alias_generation is not None for stage in ordered_stages
    ):
        await fhir.db.scalar(fhir.address_alias_sql.alias_advisory_xact_lock_sql())
    await fhir._lock_provider_directory_artifact_bundle_targets(ordered_stages, schema, profile_delta)
    active_fence = await fhir._reserve_provider_directory_artifact_cutover_budget(profile_delta, capacity_admission)
    return await fhir._apply_locked_provider_directory_artifact_bundle(
        ordered_stages,
        schema,
        relation_names,
        profile_delta,
        active_fence,
        cutover_timeout,
        before_swaps=before_swaps,
        extra_live_relations=extra_live_relations,
    )


async def apply_prepared_output_bundle(
    fhir,
    capability,
    prepared_receipts,
    *,
    artifact_stages=(),
    profile_delta=None,
    before_swaps=None,
    after_output_publish,
):
    """Keep every family's publication and its final journal in one original savepoint."""
    from process import provider_directory_output_snapshot_custody as custody
    from process.provider_directory_artifact_read_access import preserve_artifact_read_access
    from process.provider_directory_generic_source_policy import _savepoint
    from process.provider_directory_output_snapshot_retention import retain_prepared_outputs

    await custody._require_capability(capability)
    binding = fhir.db._transaction_binding()
    if binding is None or binding.session is not capability.session or not callable(after_output_publish):
        raise RuntimeError("provider_directory_output_bundle_original_session_required")
    preparations = tuple(prepared_receipts)
    artifact_stages = tuple(artifact_stages)
    await custody.recheck_output_cut(capability, preparations)
    output_targets = {receipt.incumbent.target for receipt in preparations}
    if output_targets.intersection(stage.target_relation for stage in artifact_stages):
        raise ValueError("provider_directory_output_bundle_duplicate_target")
    retained_results = []

    async def publish_outputs():
        """Publish outputs after the common helper locks every live family."""
        await preserve_artifact_read_access(capability, artifact_stages)
        if before_swaps is not None:
            await before_swaps()
        retained_results.extend(await retain_prepared_outputs(capability, preparations))

    async with _savepoint(capability.connection):
        await fhir._lock_provider_directory_artifact_tables(
            capability.policy.schema, tuple(receipt.successor.relation for receipt in preparations)
        )
        forecast = await apply_prepared_artifact_bundle(
            fhir,
            artifact_stages,
            profile_delta=profile_delta,
            before_swaps=publish_outputs,
            extra_live_relations=tuple(sorted(output_targets)),
        )
        await custody._require_capability(capability)
        if len(retained_results) != len(preparations):
            raise RuntimeError("provider_directory_output_bundle_publication_incomplete")
        await after_output_publish(capability, tuple(retained_results))
    return forecast, tuple(retained_results)


async def validate_profile_delta_total_wal(fhir, capacity_admission, capacity_forecast) -> None:
    """Keep the signed whole-admission bound and original commit envelope at the owner boundary."""
    if capacity_admission.geometry.bounded_admission:
        await fhir._assert_provider_directory_profile_wal_budget(capacity_admission)
    final_wal_bytes = await fhir._provider_directory_profile_current_wal_bytes(capacity_admission)
    maximum_wal_bytes = capacity_admission.geometry.reservation_bytes_by_storage_class["wal"]
    commit_envelope_bytes = capacity_forecast.metadata_projection.commit_envelope_bytes
    if final_wal_bytes + commit_envelope_bytes > maximum_wal_bytes:
        raise RuntimeError(
            "provider_directory_profile_capacity_final_wal_exceeded:"
            f"observed={final_wal_bytes}:"
            f"commit_envelope={commit_envelope_bytes}:"
            f"maximum={maximum_wal_bytes}"
        )
